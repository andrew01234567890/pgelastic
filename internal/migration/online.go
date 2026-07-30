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

// socketDir is where the superuser is reachable inside every member's container. Nothing
// in this package ever reaches PostgreSQL as the superuser over TCP: there is no password
// to do it with, by design.
const socketDir = provision.SocketDir

// Plan is the set of physical objects one migration owns, plus the two endpoints it works
// between. It is derived once and recorded in status, so the cleanup ladder and the orphan
// sweeper can reap by name even after everything else about the migration is gone.
type Plan struct {
	Source Endpoint
	Target Endpoint

	Publication  string
	Slot         string
	Subscription string

	// SchemaStamp is the comment the schema copy leaves on the target database to record
	// that it committed.
	SchemaStamp string

	// SourceConnInfo is the libpq string the subscriber dials the source with. It carries a
	// password, so it is never written to status or to a log line.
	SourceConnInfo string
	// Concurrency is the -j given to pg_dump and pg_restore on the offline path.
	Concurrency int32
	// DumpDir is where the offline path writes its directory-format dump, on the target.
	DumpDir string
}

// CopyProgress is the initial table sync's state.
type CopyProgress struct {
	Copied int32
	Total  int32
}

// Done reports whether every relation has finished its initial sync.
func (p CopyProgress) Done() bool { return p.Total > 0 && p.Copied >= p.Total }

// Resettable says the tenant is provably still served by the source, so a target left over
// from an earlier attempt is wreckage rather than a database anybody is reading.
type Resettable bool

// ProvisionTarget creates the tenant's database on the target with the source's collation
// tuple, and copies the schema when the caller asks for it.
//
// Logical replication carries no DDL, so the online path's target schema has to exist before
// the subscription starts or the initial sync fails one relation at a time. The offline path
// must NOT have it: its dump carries the schema, and restoring a schema into a database that
// already has one fails on the first object that exists twice.
//
// Every step here is re-enterable, because Provisioning is retried: a target that already
// carries the schema is a satisfied precondition rather than an error. What makes that
// judgement safe is that the copy is applied in one transaction and stamps the database from
// inside it, so the stamp is present if and only if the whole schema is.
func ProvisionTarget(
	ctx context.Context, sql SQL, shell Shell, plan Plan, owner string, copySchema bool,
	mayReset Resettable,
) error {
	postgres := plan.Target.WithDatabase("postgres")
	if err := ensureOwnerRole(ctx, sql, postgres, owner); err != nil {
		return err
	}
	// Every role the tenant's database depends on has to exist before anything names it. The
	// database is created OWNER <owner>, and the schema apply carries ALTER ... OWNER TO and
	// GRANT ... TO for the rest - inside one transaction, so a single missing role fails the
	// whole copy rather than the one statement that mentioned it.
	roles, err := EnumerateTenantRoles(ctx, sql, plan.Source)
	if err != nil {
		return err
	}
	if err := EnsureTenantRoles(ctx, sql, postgres, roles); err != nil {
		return err
	}
	if err := CarryMemberships(ctx, sql, plan.Source, postgres, roles); err != nil {
		return err
	}
	exists, err := scalarInt64(ctx, sql, postgres, fmt.Sprintf(
		`SELECT count(*)::text FROM pg_database WHERE datname = %s`, QuoteLiteral(plan.Target.Database)))
	if err != nil {
		return err
	}
	if exists != 0 && copySchema && bool(mayReset) {
		discarded, err := resetUnstampedTarget(ctx, sql, plan, owner)
		if err != nil {
			return err
		}
		if discarded {
			exists = 0
		}
	}
	if exists == 0 {
		create := fmt.Sprintf(`CREATE DATABASE %s TEMPLATE template0`, QuoteIdentifier(plan.Target.Database))
		if owner != "" {
			create += " OWNER " + QuoteIdentifier(owner)
		}
		if err := sql.Exec(ctx, postgres, create); err != nil {
			return fmt.Errorf("creating the target database: %w", err)
		}
	}

	if !copySchema {
		return nil
	}
	copied, err := SchemaCopied(ctx, sql, plan)
	if err != nil {
		return err
	}
	if copied {
		return nil
	}
	output, err := shell.Run(ctx, plan.Target,
		[]string{"sh", "-c", schemaCopyCommand(plan, owner, roles)})
	if err != nil {
		return fmt.Errorf("copying the schema onto the target: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

// resetUnstampedTarget discards a target that carries objects but no stamp, and reports
// whether it did.
//
// That combination has exactly one cause: an earlier attempt that applied part or all of a
// schema and then lost its stamp - or never committed one. Nothing else produces it, and
// nothing recovers from it on its own: the copy is skipped only when the stamp is present, so
// every later attempt re-applies the same schema onto its own objects and dies on "already
// exists" until the retry budget is gone.
//
// Dropped and recreated rather than emptied, because the drop is the only action that also
// clears default privileges, extensions, non-public schemas and database-level grants. The
// same reasoning is already written down for the offline path's --clean --if-exists.
//
// The caller has established that the tenant is still served by the source, so nothing is
// reading this database. A stamped target is a satisfied precondition and is never touched:
// after a successful cutover the ladder clears the stamp deliberately and the tenant is
// serving from exactly this database, which is why "unstamped and non-empty" alone would be a
// rule that deletes live tenants.
func resetUnstampedTarget(ctx context.Context, sql SQL, plan Plan, owner string) (bool, error) {
	stamped, err := SchemaCopied(ctx, sql, plan)
	if err != nil || stamped {
		return false, err
	}
	// Only a database this migration would itself have created is ever discarded. Ownership is
	// the evidence: ProvisionTarget creates the target OWNER the tenant, so a database owned by
	// anybody else is one somebody else made - a human staging a restore, most likely - and
	// dropping it would destroy work nobody asked us to touch. Such a target is left alone and
	// the copy fails on it loudly, which is the correct outcome for a name collision.
	if owner != "" {
		ours, err := scalarInt64(ctx, sql, plan.Target.WithDatabase("postgres"), fmt.Sprintf(
			`SELECT count(*)::text FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba
			 WHERE d.datname = %s AND r.rolname = %s`,
			QuoteLiteral(plan.Target.Database), QuoteLiteral(owner)))
		if err != nil || ours == 0 {
			return false, err
		}
	}
	objects, err := scalarInt64(ctx, sql, plan.Target, fmt.Sprintf(
		`SELECT count(*)::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relkind IN ('r','p','v','m','S','f') AND %s`, UserSchemaPredicate))
	if err != nil || objects == 0 {
		return false, err
	}
	if err := DropTargetDatabase(ctx, sql, plan.Target); err != nil {
		return false, fmt.Errorf(
			"discarding an unstamped target carrying %d object(s) from an earlier attempt: %w",
			objects, err)
	}
	return true, nil
}

// schemaStampQuery asks whether the target carries a stamp, rather than asking what the
// stamp is.
//
// It is asked of the postgres database rather than of the target, because pg_shdescription
// is a shared catalog and the target may not exist at all; the aggregate is what makes an
// absent database answer at all. It answers 0 or 1 rather than the stamp itself because a
// text answer of "" and no answer are the same bytes over this port, so an unstamped
// database would come back as a query that returned no rows.
const schemaStampQuery = `SELECT starts_with(
  coalesce(max(shobj_description(oid, 'pg_database')), ''), %s)::int::text
FROM pg_database WHERE datname = %s`

// SchemaCopied reports whether the target already carries a complete copy of the source's
// schema.
//
// The stamp is written by the last statement of the transaction that applies the schema, so
// its absence means no part of a previous attempt survived: an interrupted copy rolls the
// whole apply back, and a database that is half-way through one cannot exist to be mistaken
// for a finished one.
func SchemaCopied(ctx context.Context, sql SQL, plan Plan) (bool, error) {
	stamped, err := scalarInt64(ctx, sql, plan.Target.WithDatabase("postgres"),
		fmt.Sprintf(schemaStampQuery, QuoteLiteral(SchemaStampPrefix), QuoteLiteral(plan.Target.Database)))
	if err != nil {
		return false, err
	}
	return stamped == 1, nil
}

// ensureOwnerRole creates the tenant's owner role on the target if it is not there.
//
// Roles are per-cluster, so a tenant moving to an instance it has never lived on arrives
// somewhere its owner does not exist. The role is created with LOGIN and no password on
// purpose: password_encryption is scram-sha-256 throughout, and a role with no password
// cannot authenticate over TCP at all, so this creates the ownership the database needs
// without minting a credential that is not this controller's to mint.
func ensureOwnerRole(ctx context.Context, sql SQL, postgres Endpoint, owner string) error {
	if owner == "" {
		return nil
	}
	return execIfAbsent(ctx, sql, postgres,
		fmt.Sprintf(`SELECT count(*)::text FROM pg_roles WHERE rolname = %s`, QuoteLiteral(owner)),
		fmt.Sprintf(`CREATE ROLE %s LOGIN`, QuoteIdentifier(owner)))
}

// schemaCopyCommand dumps the source's schema to a file on the target and then applies it.
//
// Both ends run inside the target's container: it is the only place that can reach the
// source over the pod network and the target over its own Unix socket at the same time. It
// is deliberately two steps through a file rather than one pipe, because a pipe's exit
// status is the last command's and the shell in this image has no pipefail - a pg_dump that
// died halfway would be reported as a successful psql over a truncated schema.
//
// The apply is one transaction, and the stamp is appended to the dump so that it is the last
// statement of that transaction. Together those two make the schema copy an atom: it either
// leaves the whole schema and the stamp, or it leaves the database exactly as it found it.
// Without them a copy interrupted part-way would leave objects behind that the retry then
// failed on - which is not a retry at all, only a slower way to exhaust the budget.
//
// The dump no longer strips owners and privileges, so the tenant's objects arrive owned by the
// roles that owned them and carrying the grants they carried. Two sections are appended ahead
// of the stamp, inside the same transaction, so that they are part of the same atom: the
// database's own ACL, which pg_dump emits only under --create, and the removal of the
// replication role's temporary reads, which the dump would otherwise have made permanent.
func schemaCopyCommand(plan Plan, owner string, roles []RoleSpec) string {
	file := plan.DumpDir + ".schema.sql"
	trailer := strings.Join(append(
		append(databaseGrantsFor(plan.Target.Database, owner, roles), revokeReplicationGrantsSQL()),
		fmt.Sprintf(`COMMENT ON DATABASE %s IS %s`,
			QuoteIdentifier(plan.Target.Database), QuoteLiteral(plan.SchemaStamp)),
	), ";\n") + ";"
	return fmt.Sprintf(
		`set -e; mkdir -p %s; pg_dump --schema-only `+
			`--quote-all-identifiers --file=%s --dbname=%s; printf '%%s\n' %s >> %s; `+
			`psql --set=ON_ERROR_STOP=1 --single-transaction --quiet --host=%s --username=postgres `+
			`--dbname=%s --file=%s; rm -f %s`,
		shellQuote(ScratchDir), shellQuote(file), shellQuote(plan.SourceConnInfo),
		shellQuote(trailer), shellQuote(file),
		shellQuote(socketDir), shellQuote(plan.Target.Database), shellQuote(file), shellQuote(file))
}

// GrantSourceReads opens the source's user schemas to the replication role for the
// duration of the migration.
//
// The subscriber's initial sync runs COPY as this role, and the offline path's pg_dump
// dials as it too. The grants are scoped to the tenant's own schemas and revoked by the
// cleanup ladder: handing the replication role a cluster-wide read predefined role instead
// would leave every other tenant on the instance readable by a credential that lives in
// every member's environment.
func GrantSourceReads(ctx context.Context, sql SQL, source Endpoint, role string) error {
	schemas, err := userSchemas(ctx, sql, source)
	if err != nil {
		return err
	}
	statements := []string{fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s`,
		QuoteIdentifier(source.Database), QuoteIdentifier(role))}
	for _, schema := range schemas {
		statements = append(statements,
			fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, QuoteIdentifier(schema), QuoteIdentifier(role)),
			fmt.Sprintf(`GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s`,
				QuoteIdentifier(schema), QuoteIdentifier(role)),
			fmt.Sprintf(`GRANT SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s`,
				QuoteIdentifier(schema), QuoteIdentifier(role)))
	}
	for _, statement := range statements {
		if err := sql.Exec(ctx, source, statement); err != nil {
			return fmt.Errorf("granting source reads: %w", err)
		}
	}
	return nil
}

// StartReplication creates the publication, the failover-enabled slot and the subscription
// that consumes it, in that order.
//
// The slot is created explicitly rather than by the subscription so that failover => true
// is set at creation: a slot created without it is not synchronized to the standbys, and a
// failover mid-migration destroys it.
func StartReplication(ctx context.Context, sql SQL, plan Plan) error {
	publication := QuoteIdentifier(plan.Publication)
	if err := execIfAbsent(ctx, sql, plan.Source,
		fmt.Sprintf(`SELECT count(*)::text FROM pg_publication WHERE pubname = %s`,
			QuoteLiteral(plan.Publication)),
		`CREATE PUBLICATION `+publication+` FOR ALL TABLES`); err != nil {
		return fmt.Errorf("creating the publication: %w", err)
	}

	if err := execIfAbsent(ctx, sql, plan.Source,
		fmt.Sprintf(`SELECT count(*)::text FROM pg_replication_slots WHERE slot_name = %s`,
			QuoteLiteral(plan.Slot)),
		fmt.Sprintf(`SELECT pg_create_logical_replication_slot(%s, 'pgoutput', false, false, true)`,
			QuoteLiteral(plan.Slot))); err != nil {
		return fmt.Errorf("creating the failover-enabled logical slot: %w", err)
	}

	return execIfAbsent(ctx, sql, plan.Target,
		fmt.Sprintf(`SELECT count(*)::text FROM pg_subscription WHERE subname = %s`,
			QuoteLiteral(plan.Subscription)),
		fmt.Sprintf(`CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s `+
			`WITH (create_slot = false, slot_name = %s, copy_data = true, failover = true, streaming = on)`,
			QuoteIdentifier(plan.Subscription), QuoteLiteral(plan.SourceConnInfo),
			publication, QuoteLiteral(plan.Slot)))
}

const copyProgressQuery = `SELECT count(*) FILTER (WHERE r.srsubstate IN ('r', 's'))::text || ' ' || count(*)::text
FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid WHERE s.subname = %s`

// ReadCopyProgress reports how far the initial table sync has got.
func ReadCopyProgress(ctx context.Context, sql SQL, plan Plan) (CopyProgress, error) {
	value, err := scalar(ctx, sql, plan.Target, fmt.Sprintf(copyProgressQuery, QuoteLiteral(plan.Subscription)))
	if err != nil {
		return CopyProgress{}, err
	}
	var copied, total int32
	if _, err := fmt.Sscanf(strings.TrimSpace(value), "%d %d", &copied, &total); err != nil {
		return CopyProgress{}, fmt.Errorf("unreadable copy progress %q: %w", value, err)
	}
	return CopyProgress{Copied: copied, Total: total}, nil
}

// ReadLagBytes is the WAL distance between what the subscriber has confirmed and where the
// source's primary is now.
func ReadLagBytes(ctx context.Context, sql SQL, plan Plan) (int64, error) {
	return scalarInt64(ctx, sql, plan.Source.WithDatabase("postgres"), fmt.Sprintf(
		`SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint::text
		 FROM pg_replication_slots WHERE slot_name = %s`, QuoteLiteral(plan.Slot)))
}

// CurrentWALLSN is the source's write position, which is the position the subscriber has to
// reach before the routing table may be flipped.
func CurrentWALLSN(ctx context.Context, sql SQL, plan Plan) (string, error) {
	return scalar(ctx, sql, plan.Source.WithDatabase("postgres"), `SELECT pg_current_wal_lsn()::text`)
}

// ConfirmedThrough reports whether the subscriber has confirmed everything up to an LSN.
func ConfirmedThrough(ctx context.Context, sql SQL, plan Plan, lsn string) (bool, error) {
	value, err := scalarInt64(ctx, sql, plan.Source.WithDatabase("postgres"), fmt.Sprintf(
		`SELECT (coalesce(confirmed_flush_lsn, '0/0'::pg_lsn) >= %s::pg_lsn)::int::text
		 FROM pg_replication_slots WHERE slot_name = %s`, QuoteLiteral(lsn), QuoteLiteral(plan.Slot)))
	if err != nil {
		return false, err
	}
	return value == 1, nil
}

// FenceSource refuses new connections to the source database without dropping it, which is
// what makes the rollback window a real option rather than a promise. It is deliberately
// not a DROP: the whole point of the window is that the data is still there.
func FenceSource(ctx context.Context, sql SQL, source Endpoint) error {
	postgres := source.WithDatabase("postgres")
	name := QuoteIdentifier(source.Database)
	if err := sql.Exec(ctx, postgres,
		fmt.Sprintf(`ALTER DATABASE %s WITH ALLOW_CONNECTIONS false`, name)); err != nil {
		return err
	}
	return sql.Exec(ctx, postgres, fmt.Sprintf(
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()`,
		QuoteLiteral(source.Database)))
}

// UnfenceSource readmits connections, which is the first thing a rollback has to do.
func UnfenceSource(ctx context.Context, sql SQL, source Endpoint) error {
	return sql.Exec(ctx, source.WithDatabase("postgres"),
		fmt.Sprintf(`ALTER DATABASE %s WITH ALLOW_CONNECTIONS true`, QuoteIdentifier(source.Database)))
}

// DropSourceDatabase is the last act of a migration, run only once the rollback window has
// closed.
func DropSourceDatabase(ctx context.Context, sql SQL, source Endpoint) error {
	return dropDatabase(ctx, sql, source)
}

// DropTargetDatabase removes the copy a migration was building. It runs on every departure
// from the happy path, because a half-built target is a database that stopped receiving
// changes at an arbitrary instant and looks exactly like a complete one.
//
// Any subscription still defined in the target is removed first. WITH (FORCE) terminates
// backends, which is what people expect it to solve, but PostgreSQL refuses to drop a database
// that still owns a subscription whatever the connections are doing - dropdb counts
// pg_subscription and raises object_in_use. So a cleanup ladder that failed to drop the
// subscription, for any reason including one flaky exec, would otherwise leave a database that
// can never be dropped and can never be re-provisioned.
func DropTargetDatabase(ctx context.Context, sql SQL, target Endpoint) error {
	if err := dropSubscriptionsIn(ctx, sql, target); err != nil {
		return err
	}
	return dropDatabase(ctx, sql, target)
}

// dropSubscriptionsIn detaches and drops every subscription defined in a database, in the
// order the ladder uses: a subscription dropped without SET (slot_name = NONE) first can block
// indefinitely trying to reach a publisher that may no longer be there.
func dropSubscriptionsIn(ctx context.Context, sql SQL, at Endpoint) error {
	names, err := firstColumn(ctx, sql, at, `SELECT subname FROM pg_subscription ORDER BY 1`)
	if err != nil {
		// A database that cannot be read is one that may not exist, which is the ordinary case
		// on a path that is about to issue DROP DATABASE IF EXISTS.
		return nil //nolint:nilerr // the drop itself reports anything that actually matters
	}
	for _, name := range names {
		quoted := QuoteIdentifier(name)
		for _, statement := range []string{
			`ALTER SUBSCRIPTION ` + quoted + ` DISABLE`,
			`ALTER SUBSCRIPTION ` + quoted + ` SET (slot_name = NONE)`,
			`DROP SUBSCRIPTION ` + quoted,
		} {
			if err := sql.Exec(ctx, at, statement); err != nil {
				return fmt.Errorf("removing subscription %s before dropping %s: %w",
					name, at.Database, err)
			}
		}
	}
	return nil
}

func dropDatabase(ctx context.Context, sql SQL, at Endpoint) error {
	return sql.Exec(ctx, at.WithDatabase("postgres"),
		fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, QuoteIdentifier(at.Database)))
}

func userSchemas(ctx context.Context, sql SQL, at Endpoint) ([]string, error) {
	return firstColumn(ctx, sql, at,
		`SELECT n.nspname FROM pg_namespace n WHERE `+UserSchemaPredicate+` ORDER BY 1`)
}

// execIfAbsent runs a statement only when a count query says the object is not there yet,
// which is what makes every provisioning step safe to re-enter after a requeue.
func execIfAbsent(ctx context.Context, sql SQL, at Endpoint, countQuery, statement string) error {
	count, err := scalarInt64(ctx, sql, at, countQuery)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	return sql.Exec(ctx, at, statement)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
