/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package migration

import (
	"context"
	"fmt"
	"strings"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// controlPlaneRoles are the cluster's own roles. They exist on every instance already, they
// are never a tenant's to carry, and one of them - the replication role - holds grants on the
// source for the duration of a migration that must not become part of the tenant's schema.
var controlPlaneRoles = []string{
	"postgres",
	provision.OpsRole,
	provision.ReplicationRole,
	provision.RewindRole,
}

// RoleSpec is one role a tenant's database depends on, as the source describes it.
//
// Deliberately no password field. A verifier is the operator's to mint per instance, never
// migration's to copy between them: reading pg_authid to move a credential across an instance
// boundary would make every migration a credential-replication event, and a single mistake in
// the enumeration below would write one tenant's credential onto another tenant's role.
type RoleSpec struct {
	Name       string
	CanLogin   bool
	Inherit    bool
	ConnLimit  int64
	ValidUntil string
}

// roleEnumerationQuery finds every role the tenant's database actually depends on: the owners
// of its objects, everyone named in any of its ACLs, and the role graph around them.
//
// The closure over pg_auth_members runs in both directions on purpose. A group role that owns
// nothing and appears in no ACL, but which one of the tenant's roles is a member of, is
// invisible to a search that only looks at owners and grantees - and losing it silently
// changes what the tenant's users can do on the far side.
//
// grantee 0 is PUBLIC, which is not a role and carries in the ACL text itself, so it is
// dropped here. Predefined pg_* roles and the control-plane roles are excluded because they
// exist on the target already; carrying them would mean trying to create roles that cannot be
// created, and in the replication role's case would bake a control-plane credential's read
// access into the tenant's schema.
const roleEnumerationQuery = `
WITH acl_sources AS (
    SELECT c.relacl AS acl FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind IN ('r','p','v','m','S','f') AND %[1]s
    UNION ALL
    SELECT a.attacl FROM pg_attribute a
      JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE a.attnum > 0 AND NOT a.attisdropped AND %[1]s
    UNION ALL SELECT n.nspacl FROM pg_namespace n WHERE %[1]s
    UNION ALL SELECT p.proacl FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace WHERE %[1]s
    UNION ALL SELECT t.typacl FROM pg_type t
      JOIN pg_namespace n ON n.oid = t.typnamespace WHERE %[1]s
    UNION ALL SELECT d.defaclacl FROM pg_default_acl d
    UNION ALL SELECT db.datacl FROM pg_database db WHERE db.datname = current_database()
),
referenced AS (
    SELECT (aclexplode(acl)).grantee AS oid FROM acl_sources WHERE acl IS NOT NULL
    UNION SELECT (aclexplode(acl)).grantor FROM acl_sources WHERE acl IS NOT NULL
    UNION SELECT c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE %[1]s
    UNION SELECT n.nspowner FROM pg_namespace n WHERE %[1]s
    UNION SELECT p.proowner FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE %[1]s
    UNION SELECT t.typowner FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
     WHERE %[1]s
    UNION SELECT d.defaclrole FROM pg_default_acl d
    UNION SELECT db.datdba FROM pg_database db WHERE db.datname = current_database()
),
walk AS (
    SELECT oid FROM referenced WHERE oid <> 0
),
closure AS (
    SELECT w.oid FROM walk w
    UNION SELECT m.roleid FROM pg_auth_members m JOIN walk w ON w.oid = m.member
    UNION SELECT m.member FROM pg_auth_members m JOIN walk w ON w.oid = m.roleid
)
SELECT r.rolname,
       r.rolcanlogin::text,
       r.rolinherit::text,
       r.rolconnlimit::text,
       coalesce(r.rolvaliduntil::text, '')
  FROM pg_roles r JOIN closure c ON c.oid = r.oid
 WHERE r.rolname NOT LIKE 'pg\_%%' AND r.rolname <> ALL (%[2]s)
 ORDER BY 1`

// membershipQuery reads the memberships among a carried set, with the option flags PostgreSQL
// 16 made separable. A membership carried without them is not the same membership.
const membershipQuery = `
SELECT g.rolname, m.rolname, a.admin_option::text, a.inherit_option::text, a.set_option::text
  FROM pg_auth_members a
  JOIN pg_roles g ON g.oid = a.roleid
  JOIN pg_roles m ON m.oid = a.member
 WHERE g.rolname = ANY (%[1]s) AND m.rolname = ANY (%[1]s)
 ORDER BY 1, 2`

// EnumerateTenantRoles reports the roles the tenant's database depends on, read from the
// source's own catalogs.
func EnumerateTenantRoles(ctx context.Context, sql SQL, source Endpoint) ([]RoleSpec, error) {
	rows, err := sql.Query(ctx, source, fmt.Sprintf(
		roleEnumerationQuery, UserSchemaPredicate, textArray(controlPlaneRoles)))
	if err != nil {
		return nil, fmt.Errorf("enumerating the roles the tenant's database depends on: %w", err)
	}
	specs := make([]RoleSpec, 0, len(rows))
	for _, row := range rows {
		if len(row) < 5 {
			continue
		}
		specs = append(specs, RoleSpec{
			Name:       strings.TrimSpace(row[0]),
			CanLogin:   strings.TrimSpace(row[1]) == "t",
			Inherit:    strings.TrimSpace(row[2]) == "t",
			ConnLimit:  parseInt64(strings.TrimSpace(row[3]), -1),
			ValidUntil: strings.TrimSpace(row[4]),
		})
	}
	return specs, nil
}

// EnsureTenantRoles creates the carried roles on the target, then their memberships.
//
// It must run before the database is created and before the schema is applied. The dump now
// carries ownership and privileges, so it emits ALTER ... OWNER TO and GRANT ... TO naming
// these roles inside a single-transaction apply: one role missing fails the entire copy, not
// merely the statement that named it.
//
// Every role is created without a password and without a single privileged attribute. A
// migration is not a route by which a tenant acquires SUPERUSER, and CREATEROLE in particular
// would let a tenant mint cluster-global roles of its own choosing on the far side.
func EnsureTenantRoles(ctx context.Context, sql SQL, postgres Endpoint, roles []RoleSpec) error {
	for _, role := range roles {
		name := QuoteIdentifier(role.Name)
		create := fmt.Sprintf(
			`CREATE ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, name)
		if role.CanLogin {
			create += " LOGIN"
		} else {
			create += " NOLOGIN"
		}
		if !role.Inherit {
			create += " NOINHERIT"
		}
		if role.ConnLimit >= 0 {
			create += fmt.Sprintf(" CONNECTION LIMIT %d", role.ConnLimit)
		}
		if role.ValidUntil != "" {
			create += " VALID UNTIL " + QuoteLiteral(role.ValidUntil)
		}
		if err := execIfAbsent(ctx, sql, postgres,
			fmt.Sprintf(`SELECT count(*)::text FROM pg_roles WHERE rolname = %s`,
				QuoteLiteral(role.Name)),
			create); err != nil {
			return fmt.Errorf("creating role %s on the target: %w", role.Name, err)
		}
	}
	return nil
}

// CarryMemberships reproduces the memberships among the carried roles, with their option
// flags. Reads the source, writes the target.
func CarryMemberships(
	ctx context.Context, sql SQL, source, postgres Endpoint, roles []RoleSpec,
) error {
	if len(roles) == 0 {
		return nil
	}
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	rows, err := sql.Query(ctx, source, fmt.Sprintf(membershipQuery, textArray(names)))
	if err != nil {
		return fmt.Errorf("reading the tenant's role memberships: %w", err)
	}
	for _, row := range rows {
		if len(row) < 5 {
			continue
		}
		group, member := QuoteIdentifier(strings.TrimSpace(row[0])), QuoteIdentifier(strings.TrimSpace(row[1]))
		grant := fmt.Sprintf(`GRANT %s TO %s WITH ADMIN %s, INHERIT %s, SET %s`,
			group, member,
			optionWord(row[2]), optionWord(row[3]), optionWord(row[4]))
		if err := sql.Exec(ctx, postgres, grant); err != nil {
			return fmt.Errorf("carrying membership of %s in %s: %w", row[1], row[0], err)
		}
	}
	return nil
}

// databaseGrantsFor is the database ACL the target must end up with, issued by the engine
// because pg_dump emits a database's own ACL only under --create, which neither path uses.
//
// PUBLIC loses CONNECT here, which is the point. The default datacl grants CONNECT and
// TEMPORARY to PUBLIC, so on an instance where tenant roles can authenticate, every tenant's
// role could open a session on every other tenant's database. Revoking it and granting back
// only this tenant's own roles - plus the ops role, which the control plane reaches the
// database as - is what confines a tenant to its own database.
func databaseGrantsFor(database, owner string, roles []RoleSpec) []string {
	name := QuoteIdentifier(database)
	statements := []string{fmt.Sprintf(`REVOKE ALL ON DATABASE %s FROM PUBLIC`, name)}
	granted := map[string]bool{}
	for _, role := range append([]RoleSpec{{Name: owner}}, roles...) {
		if role.Name == "" || granted[role.Name] {
			continue
		}
		granted[role.Name] = true
		statements = append(statements, fmt.Sprintf(
			`GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s`, name, QuoteIdentifier(role.Name)))
	}
	return append(statements, fmt.Sprintf(
		`GRANT CONNECT ON DATABASE %s TO %s`, name, QuoteIdentifier(provision.OpsRole)))
}

// revokeReplicationGrantsSQL takes the replication role's reads back off the target.
//
// GrantSourceReads opens CONNECT and per-schema SELECT on the source so pg_dump can read it,
// and those grants are in force while the dump runs. Now that the dump carries privileges,
// they are captured and applied to the target - permanently granting a credential that lives
// in every member's environment read access to the tenant's data. They cannot be revoked
// before the dump, because the dump needs them; so they are removed on the far side.
func revokeReplicationGrantsSQL() string {
	return fmt.Sprintf(`DO $pgelastic$ DECLARE s text; BEGIN
  FOR s IN SELECT n.nspname FROM pg_namespace n WHERE %s
  LOOP
    EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA %%I FROM %%I', s, %s);
    EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA %%I FROM %%I', s, %s);
    EXECUTE format('REVOKE ALL ON SCHEMA %%I FROM %%I', s, %s);
  END LOOP;
END $pgelastic$;`,
		UserSchemaPredicate,
		QuoteLiteral(provision.ReplicationRole),
		QuoteLiteral(provision.ReplicationRole),
		QuoteLiteral(provision.ReplicationRole))
}

// SettleTargetGrants applies the database ACL and removes the replication role's temporary
// reads, for the path that has no schema-apply transaction to fold them into.
//
// The online path appends these to the dump file so they commit with the schema and the stamp.
// Offline restores with pg_restore --jobs, which is not one transaction, so its equivalent runs
// here as ordinary statements against the target.
// HoldTenantOut keeps a tenant's own roles out of its database while the database is being
// rewritten underneath them.
//
// It is not FenceSource. A migration fences a source it is about to abandon, so
// ALTER DATABASE ... ALLOW_CONNECTIONS false costs it nothing. A restore-in-place is rewriting
// the database it fenced, and that setting admits nobody at all - pg_restore included, which
// fails with "database is not currently accepting connections" before it writes a row.
//
// Revoking CONNECT stops the tenant's roles specifically. The restore runs as the superuser
// over the Unix socket, and a superuser bypasses privilege checks, so it still gets in. The
// backends already connected are terminated, because a revoke does not close a session that
// is already open.
func HoldTenantOut(ctx context.Context, sql SQL, target Endpoint, roles []RoleSpec) error {
	postgres := target.WithDatabase("postgres")
	name := QuoteIdentifier(target.Database)
	for _, role := range roles {
		if role.Name == "" {
			continue
		}
		if err := sql.Exec(ctx, postgres, fmt.Sprintf(
			`REVOKE CONNECT, TEMPORARY ON DATABASE %s FROM %s`,
			name, QuoteIdentifier(role.Name))); err != nil {
			return fmt.Errorf("holding %q out of %q: %w", role.Name, target.Database, err)
		}
	}
	return sql.Exec(ctx, postgres, fmt.Sprintf(
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()`,
		QuoteLiteral(target.Database)))
}

// ReadmitTenant puts back what HoldTenantOut took away.
//
// It runs on every exit from the copy, successful or not: a tenant left unable to connect
// after a restore that failed half way is an outage caused by the recovery rather than by
// whatever the recovery was for.
func ReadmitTenant(ctx context.Context, sql SQL, target Endpoint, roles []RoleSpec) error {
	postgres := target.WithDatabase("postgres")
	name := QuoteIdentifier(target.Database)
	for _, role := range roles {
		if role.Name == "" {
			continue
		}
		if err := sql.Exec(ctx, postgres, fmt.Sprintf(
			`GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s`,
			name, QuoteIdentifier(role.Name))); err != nil {
			return fmt.Errorf("readmitting %q to %q: %w", role.Name, target.Database, err)
		}
	}
	return nil
}

// RevokeReplicationReads takes back the reads GrantSourceReads made, on the far side.
//
// The far side is the point. pg_dump captures ACLs and pg_restore writes them into the
// database it loads, so the grants made on the source to let the dump read ride the dump into
// the copy. They do not die with the source, however throwaway it was. Leaving them behind
// permanently gives a credential that lives in every member's environment read access to the
// tenant's data.
func RevokeReplicationReads(ctx context.Context, sql SQL, target Endpoint) error {
	if err := sql.Exec(ctx, target, revokeReplicationGrantsSQL()); err != nil {
		return fmt.Errorf("revoking the replication role's reads on the target: %w", err)
	}
	return nil
}

func SettleTargetGrants(ctx context.Context, sql SQL, plan Plan, owner string) error {
	roles, err := EnumerateTenantRoles(ctx, sql, plan.Source)
	if err != nil {
		return err
	}
	postgres := plan.Target.WithDatabase("postgres")
	for _, statement := range databaseGrantsFor(plan.Target.Database, owner, roles) {
		if err := sql.Exec(ctx, postgres, statement); err != nil {
			return fmt.Errorf("applying the target database's ACL: %w", err)
		}
	}
	if err := RevokeReplicationReads(ctx, sql, plan.Target); err != nil {
		return err
	}
	return nil
}

// textArray renders a Go slice as a PostgreSQL text[] literal.
func textArray(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, QuoteLiteral(value))
	}
	return "ARRAY[" + strings.Join(quoted, ", ") + "]::text[]"
}

func optionWord(value string) string {
	if strings.TrimSpace(value) == "t" {
		return "TRUE"
	}
	return "FALSE"
}

func parseInt64(value string, fallback int64) int64 {
	var parsed int64
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}
