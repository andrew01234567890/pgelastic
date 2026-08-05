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

// Package tenantuser creates, inspects and removes the one PostgreSQL ROLE that backs a
// PgTenantUser.
//
// Its own package rather than more of internal/tenantdb, because that package's doc says it
// owns "the one DATABASE and the one ROLE that back a PgTenant" and that stays true. The two
// write disjoint objects: the tenant controller owns the database, the owner role and the
// REVOKE ALL ... FROM PUBLIC that confines it; this owns one login's role and an additive
// GRANT CONNECT. The only shared surface is pg_database.datacl, and tenantdb only ever revokes
// from PUBLIC, never from a named role - which is what makes two writers safe here. An edit
// that turned that revoke into an exact-ACL assertion would break this silently, so it is
// written down in both places.
//
// Every statement is re-enterable and every observation is read from the catalog rather than
// remembered, for the reason tenantdb gives: a reconcile that trusts its own memory reports
// success for work a previous attempt did not finish.
package tenantuser

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// SQL and Endpoint are migration's, for the reason tenantdb borrows them too: a role this
// package creates and a role a migration carries must be the same object.
type SQL = migration.SQL

// Endpoint names the PgInstance and database a statement runs against.
type Endpoint = migration.Endpoint

// Condition reasons this package produces, spelled here rather than in the shared vocabulary
// because they describe this package's steps - exactly as tenantdb and migration spell theirs.
const (
	// ReasonProvisioningFailed means PostgreSQL refused a step, and the message says which.
	ReasonProvisioningFailed = "ProvisioningFailed"
	// ReasonReclaimFailed means the role could not be dropped, so the finalizer stays on.
	ReasonReclaimFailed = "ReclaimFailed"
)

// maintenanceDatabase is where role DDL and the catalog reads are issued. It cannot be the
// tenant's own database: a login may be provisioned before anything has connected to that one,
// and pg_roles is shared anyway.
const maintenanceDatabase = "postgres"

// absentRole is what the observation reports for a role that does not exist, distinguishing it
// from one that exists and may not log in.
const absentRole = -1

// Spec is what one login must look like on the instance hosting its tenant.
type Spec struct {
	// Role is the cluster-global role name, from migration.TenantUserRoleName.
	Role string
	// Database is the tenant's database, which this login is granted CONNECT on and nothing
	// else. Everything beyond that is the tenant's own DBA's to grant, because this kind has
	// no field with which to ask for more.
	Database string
	// Login is whether the role may open a session at all.
	Login bool
	// Verifier is the SCRAM-SHA-256 credential in PostgreSQL's stored form, empty for a role
	// that authenticates nobody. The verifier rather than the password, so no plaintext
	// crosses the exec channel.
	Verifier string
	// MemberOf is the roles this one is granted membership in. It is the only privilege
	// mechanism this kind offers.
	MemberOf []string
	// Owned is every role this controller owns for the same tenant, and it is a fence rather
	// than a list: memberships are revoked inside it and never outside, so a grant a DBA made
	// by hand from a role we do not own is left exactly where it is.
	Owned []string
	// StatementTimeout and TempFileLimit are the tenant's tier-2 limits, in PostgreSQL's own
	// spelling, applied to this login's role as well as to the tenant's owner role.
	//
	// Both, because a contained user dials as *its own* backend role. A limit that reached only
	// the owner would be one every login escapes, and the class calls these hard limits.
	StatementTimeout string
	TempFileLimit    string
}

// roleSettings is the tier-2 limits as GUC name and value, in a fixed order so two passes over
// one spec issue the same statements.
func (s Spec) roleSettings() []struct{ Name, Value string } {
	return []struct{ Name, Value string }{
		{"statement_timeout", s.StatementTimeout},
		{"temp_file_limit", s.TempFileLimit},
	}
}

// parseRoleConfig turns pg_roles.rolconfig - a list of "name=value" strings - into a map.
//
// Joined with the ASCII record separator rather than a newline, because the transport splits
// rows on a newline: a value carrying one is read back as a second row and the answer is
// rejected.
func parseRoleConfig(raw string) map[string]string {
	settings := map[string]string{}
	for entry := range strings.SplitSeq(raw, "\x1e") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, value, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		settings[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
	}
	return settings
}

// State is what the catalog currently says about the login.
type State struct {
	// Exists reports a row in pg_roles.
	Exists bool
	// CanLogin is rolcanlogin.
	CanLogin bool
	// CredentialCurrent reports that rolpassword already equals the spec's verifier. Compared
	// rather than re-issued, unlike tenantdb's credential: a login's role is created
	// passwordless by a migration that carries it to another instance, so the steady state has
	// to be able to notice a role that exists and cannot authenticate. Reading it out of
	// pg_authid discloses nothing that is not already crossing this channel in the ALTER it
	// guards.
	CredentialCurrent bool
	// MayConnect is has_database_privilege on the tenant's database.
	MayConnect bool
	// RoleSettings is pg_roles.rolconfig as a map, which is where ALTER ROLE ... SET lands.
	RoleSettings map[string]string
	// MemberOf is the memberships this login holds *within* Owned, sorted.
	MemberOf []string
}

// Settled reports whether the catalog already matches the spec, so a second pass issues no DDL.
func (s State) Settled(spec Spec) bool {
	return s.Exists &&
		s.CanLogin == spec.Login &&
		s.MayConnect &&
		(spec.Verifier == "" || s.CredentialCurrent) &&
		slices.Equal(s.MemberOf, sortedCopy(spec.MemberOf)) &&
		s.settingsCurrent(spec)
}

// observeQuery reads everything in one round trip, because each round trip is an exec into a
// Pod and asking four questions separately costs four of them.
//
// Booleans are projected through ::int::text rather than ::text: psql displays a boolean as
// "t" while boolean::text is "true", and a caller that assumes the displayed spelling reads
// every answer as false. That is a defect this tree has already had once.
//
// An absent role answers -1 for rolcanlogin, which is what separates "not created yet" from
// "created NOLOGIN".
//
// CONNECT is asked by OID and not by name, and that is the difference between an observation
// and an error. has_database_privilege(name, ...) raises 42704 for a role that does not exist
// - which is the state this query exists to report - so the name form makes the first
// observation of every login fail, Ensure return before CREATE ROLE, and the login retry for
// ever. Joining pg_roles instead means an absent role contributes no row and coalesce answers.
const observeQuery = `SELECT coalesce((SELECT r.rolcanlogin::int::text FROM pg_roles r WHERE r.rolname = %[1]s), '-1'),
 coalesce((SELECT (a.rolpassword = %[2]s)::int::text FROM pg_authid a WHERE a.rolname = %[1]s), '0'),
 coalesce((SELECT has_database_privilege(r.oid, d.oid, 'CONNECT')::int::text
   FROM pg_roles r, pg_database d WHERE r.rolname = %[1]s AND d.datname = %[3]s), '0'),
 coalesce((SELECT string_agg(g.rolname, ',' ORDER BY g.rolname) FROM pg_auth_members m
   JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles v ON v.oid = m.member
   WHERE v.rolname = %[1]s AND g.rolname = ANY (%[4]s)), ''),
 coalesce((SELECT array_to_string(r.rolconfig, e'\x1e') FROM pg_roles r WHERE r.rolname = %[1]s), '')`

// settingsCurrent reports whether every declared tier-2 limit is already on the role. An
// undeclared one is not compared: leaving a parameter alone is not the same as agreeing with it.
func (s State) settingsCurrent(spec Spec) bool {
	for _, setting := range spec.roleSettings() {
		if setting.Value != "" && s.RoleSettings[setting.Name] != setting.Value {
			return false
		}
	}
	return true
}

// Observe reads the login out of the catalog.
func Observe(ctx context.Context, sql SQL, at Endpoint, spec Spec) (State, error) {
	if err := spec.validate(); err != nil {
		return State{}, err
	}
	statement := fmt.Sprintf(observeQuery,
		migration.QuoteLiteral(spec.Role),
		migration.QuoteLiteral(spec.Verifier),
		migration.QuoteLiteral(spec.Database),
		roleArray(spec.Owned))

	rows, err := sql.Query(ctx, at.WithDatabase(maintenanceDatabase), statement)
	if err != nil {
		return State{}, fmt.Errorf("reading the catalog on %s/%s: %w", at.Namespace, at.Instance, err)
	}
	if len(rows) != 1 || len(rows[0]) != 5 {
		return State{}, fmt.Errorf("unreadable catalog answer for role %q: %v", spec.Role, rows)
	}

	canLogin, err := strconv.Atoi(strings.TrimSpace(rows[0][0]))
	if err != nil {
		return State{}, fmt.Errorf("unreadable login attribute for role %q: %w", spec.Role, err)
	}
	return State{
		Exists:            canLogin != absentRole,
		CanLogin:          canLogin == 1,
		CredentialCurrent: strings.TrimSpace(rows[0][1]) == "1",
		MayConnect:        strings.TrimSpace(rows[0][2]) == "1",
		MemberOf:          splitRoles(rows[0][3]),
		RoleSettings:      parseRoleConfig(rows[0][4]),
	}, nil
}

// Ensure brings the login into the shape the spec asks for and returns what the catalog says
// afterwards. It issues only the difference, so a settled login costs one query and no DDL.
func Ensure(ctx context.Context, sql SQL, at Endpoint, spec Spec) (State, error) {
	state, err := Observe(ctx, sql, at, spec)
	if err != nil {
		return State{}, err
	}
	if state.Settled(spec) {
		return state, nil
	}
	postgres := at.WithDatabase(maintenanceDatabase)

	if !state.Exists {
		if err := create(ctx, sql, postgres, spec); err != nil {
			return state, err
		}
	} else if state.CanLogin != spec.Login {
		if err := exec(ctx, sql, postgres, spec.Role, "%s %s",
			alter(spec.Role), loginClause(spec.Login)); err != nil {
			return state, err
		}
	}

	if err := credential(ctx, sql, postgres, spec, state); err != nil {
		return state, err
	}
	if !state.MayConnect {
		// Additive, and never paired with a revoke. The reasoning is the one HoldTenantOut
		// already states: granting a fixed set back would hand a role access an owner had
		// deliberately taken away, so what this may do is add what the spec asks for.
		if err := exec(ctx, sql, postgres, spec.Role, `GRANT CONNECT ON DATABASE %s TO %s`,
			migration.QuoteIdentifier(spec.Database), migration.QuoteIdentifier(spec.Role)); err != nil {
			return state, err
		}
	}
	if err := memberships(ctx, sql, postgres, spec, state); err != nil {
		return state, err
	}
	if err := roleSettings(ctx, sql, postgres, spec, state); err != nil {
		return state, err
	}
	return Observe(ctx, sql, at, spec)
}

// roleSettings brings the tenant's tier-2 limits onto this login's role.
//
// The same limits the tenant's owner role carries, because a contained user dials as its own
// backend role: a bound that reached only the owner would be one every login escapes, and the
// workload class calls these hard limits rather than hints.
//
// An undeclared limit leaves the parameter alone rather than issuing RESET, exactly as the
// tenant's own role treats it.
func roleSettings(ctx context.Context, sql SQL, postgres Endpoint, spec Spec, state State) error {
	for _, setting := range spec.roleSettings() {
		if setting.Value == "" || state.RoleSettings[setting.Name] == setting.Value {
			continue
		}
		// The name is a compile-time constant and never client text; the value is a quoted
		// literal, which is what PostgreSQL accepts here for both of these.
		if err := exec(ctx, sql, postgres, spec.Role, "%s SET "+setting.Name+" = %s",
			alter(spec.Role), migration.QuoteLiteral(setting.Value)); err != nil {
			return err
		}
	}
	return nil
}

// create makes the role with every privileged attribute spelled out and denied.
//
// Spelled out rather than left to the default for the reason tenantdb gives about the tenant's
// own role, and more sharply here: this role is handed to whoever a tenant's operators say, so
// NOCREATEROLE is what stops one minting cluster-global roles of its own and defeating the
// namespacing that keeps two tenants' identities apart.
func create(ctx context.Context, sql SQL, postgres Endpoint, spec Spec) error {
	statement := fmt.Sprintf(
		`CREATE ROLE %s NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS INHERIT %s`,
		migration.QuoteIdentifier(spec.Role), loginClause(spec.Login))
	if err := sql.Exec(ctx, postgres, statement); err != nil && !alreadyExists(err) {
		return fmt.Errorf("creating the login role %q: %w", spec.Role, err)
	}
	return nil
}

// credential re-establishes the login's password when the catalog does not already hold it.
//
// Compared before writing, unlike tenantdb's, and the difference is load bearing: a migration
// recreates a tenant's carried roles *passwordless* on the target, deliberately, so a login
// that exists and cannot authenticate is an ordinary steady state to heal rather than an
// impossible one. Re-issuing unconditionally would instead write a credential into the
// instance's log on every pass.
func credential(ctx context.Context, sql SQL, postgres Endpoint, spec Spec, state State) error {
	if spec.Verifier == "" || (state.Exists && state.CredentialCurrent) {
		return nil
	}
	return exec(ctx, sql, postgres, spec.Role, `ALTER ROLE %s PASSWORD %s`,
		migration.QuoteIdentifier(spec.Role), migration.QuoteLiteral(spec.Verifier))
}

// memberships grants what the spec asks for and revokes what it no longer asks for, inside the
// fence of roles this controller owns.
//
// The GRANT options are the whole of what this kind promises. INHERIT TRUE because the field
// says the login "inherits" the privileges; SET FALSE because it does not say the login may
// *become* the other role, and on PostgreSQL 16+ SET defaults to TRUE - which would hand
// SET ROLE and let a login create objects owned by another, defeating the attribution the
// design is sold on. ADMIN FALSE so a member cannot re-grant its own membership onwards.
func memberships(ctx context.Context, sql SQL, postgres Endpoint, spec Spec, state State) error {
	want := sortedCopy(spec.MemberOf)
	for _, role := range want {
		if slices.Contains(state.MemberOf, role) {
			continue
		}
		if err := exec(ctx, sql, postgres, spec.Role,
			`GRANT %s TO %s WITH ADMIN FALSE, INHERIT TRUE, SET FALSE`,
			migration.QuoteIdentifier(role), migration.QuoteIdentifier(spec.Role)); err != nil {
			return err
		}
	}
	for _, role := range state.MemberOf {
		if slices.Contains(want, role) {
			continue
		}
		if err := exec(ctx, sql, postgres, spec.Role, `REVOKE %s FROM %s`,
			migration.QuoteIdentifier(role), migration.QuoteIdentifier(spec.Role)); err != nil {
			return err
		}
	}
	return nil
}

// Drop removes the login's role, moving anything it owns to the tenant's owner first.
//
// DROP ROLE fails while a role owns an object or holds a privilege, and nothing stops a
// tenant's owner granting this login CREATE. REASSIGN OWNED is therefore the first step and
// not an optimisation - and its destination is the tenant's own owner role, which already
// controls the database, so nothing is lost and nothing is escalated.
//
// DROP OWNED then clears the privileges granted *to* the role, which REASSIGN does not move.
// It is skipped when there is no owner to reassign to, because running it first would destroy
// the objects rather than rehome them.
func Drop(ctx context.Context, sql SQL, at Endpoint, spec Spec, reassignTo string) error {
	if strings.TrimSpace(spec.Role) == "" {
		return fmt.Errorf("dropping a login needs a role name")
	}
	role := migration.QuoteIdentifier(spec.Role)
	postgres := at.WithDatabase(maintenanceDatabase)

	if spec.Database != "" && reassignTo != "" {
		inTenant := at.WithDatabase(spec.Database)
		for _, statement := range []string{
			fmt.Sprintf(`REASSIGN OWNED BY %s TO %s`, role, migration.QuoteIdentifier(reassignTo)),
			fmt.Sprintf(`DROP OWNED BY %s`, role),
		} {
			if err := sql.Exec(ctx, inTenant, statement); err != nil && !missingObject(err) {
				return fmt.Errorf("clearing what login role %q owns: %w", spec.Role, err)
			}
		}
	}
	if err := sql.Exec(ctx, postgres, fmt.Sprintf(`DROP ROLE IF EXISTS %s`, role)); err != nil {
		return fmt.Errorf("dropping the login role %q: %w", spec.Role, err)
	}
	return nil
}

func exec(ctx context.Context, sql SQL, at Endpoint, role, format string, args ...any) error {
	if err := sql.Exec(ctx, at, fmt.Sprintf(format, args...)); err != nil {
		return fmt.Errorf("configuring login role %q: %w", role, err)
	}
	return nil
}

func alter(role string) string { return `ALTER ROLE ` + migration.QuoteIdentifier(role) }

func loginClause(login bool) string {
	if login {
		return "LOGIN"
	}
	return "NOLOGIN"
}

// roleArray renders the fence as a text[] literal for `= ANY (...)`.
func roleArray(roles []string) string {
	if len(roles) == 0 {
		return `'{}'::text[]`
	}
	quoted := make([]string, 0, len(roles))
	for _, role := range roles {
		quoted = append(quoted, migration.QuoteLiteral(role))
	}
	return `ARRAY[` + strings.Join(quoted, ",") + `]::text[]`
}

func splitRoles(value string) []string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	roles := strings.Split(trimmed, ",")
	for i := range roles {
		roles[i] = strings.TrimSpace(roles[i])
	}
	slices.Sort(roles)
	return roles
}

func sortedCopy(roles []string) []string {
	if len(roles) == 0 {
		return nil
	}
	copied := slices.Clone(roles)
	slices.Sort(copied)
	return copied
}

func (s Spec) validate() error {
	if strings.TrimSpace(s.Role) == "" {
		return fmt.Errorf("a login spec needs a role name")
	}
	if strings.TrimSpace(s.Database) == "" {
		return fmt.Errorf("a login spec needs the database its tenant owns")
	}
	return nil
}

// alreadyExists recognises the one failure that is not one: another actor created the role
// between the catalog read and the CREATE. PostgreSQL has no CREATE ROLE IF NOT EXISTS.
func alreadyExists(err error) bool {
	return strings.Contains(err.Error(), "already exists")
}

// missingObject tolerates a reassign or drop against a database or role that has already gone,
// which is the ordinary shape of a tenant being deleted underneath its logins.
func missingObject(err error) bool {
	message := err.Error()
	return strings.Contains(message, "does not exist")
}
