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

// Package tenantdb creates, inspects and removes the one PostgreSQL DATABASE and the one
// ROLE that back a PgTenant.
//
// It exists so that the tenant lifecycle can say Ready only about objects it has seen in
// pg_database and pg_roles. Every statement is re-enterable and every observation is read
// from the catalog rather than remembered, because a reconcile that trusts its own memory
// reports success for work a previous attempt did not finish.
//
// A tenant provisioned here and a tenant delivered by internal/migration must be the same
// object: same creation route, same template, same ownership. That is why the port below
// is migration's rather than one of this package's own.
package tenantdb

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// SQL is the single port every statement reaches PostgreSQL through: the bootstrap
// superuser, over the hosting member's Unix socket, through the API server's exec
// subresource. There is no TCP alternative, because that superuser has no password by
// design and creating a database needs one.
type SQL = migration.SQL

// Endpoint names the PgInstance and the database a statement runs against. An endpoint
// with no member names the instance's current primary, which is the only correct place
// for a CREATE.
type Endpoint = migration.Endpoint

// Condition reasons this package produces. They are spelled here rather than in the shared
// API vocabulary because they describe this package's steps, exactly as the migration
// package spells its own.
const (
	// ReasonProvisioning means the database is not there yet and nothing has failed.
	ReasonProvisioning = "Provisioning"
	// ReasonProvisioningFailed means PostgreSQL refused a step, and the message says which.
	ReasonProvisioningFailed = "ProvisioningFailed"
	// ReasonReclaimFailed means the reclaim policy's action did not complete, so the
	// tenant's finalizer stays on.
	ReasonReclaimFailed = "ReclaimFailed"
)

// maintenanceDatabase is where CREATE DATABASE and the catalog reads are issued. It cannot
// be the tenant's own database: that is the object being created, and a session inside a
// database is what stops it being dropped.
const maintenanceDatabase = "postgres"

// NoConnectionLimit is PostgreSQL's spelling of an unlimited rolconnlimit.
const NoConnectionLimit int32 = -1

// Spec is what one tenant's database and role must look like on the instance hosting it.
type Spec struct {
	// Database is the DATABASE created for the tenant.
	Database string
	// Owner is the ROLE that owns it. It is created first, because a database cannot be
	// given an owner that does not exist.
	Owner string
	// ConnectionLimit mirrors the tenant's effective burstable ceiling onto the role as
	// an in-database backstop. The proxy is the enforcement point; this only bounds what
	// a client that got past the proxy can hold. NoConnectionLimit leaves it uncapped.
	//
	// Set to NoConnectionLimit once the proxy authenticates as this role: every backend the
	// fleet opens then counts against it, and N replicas each holding up to burstable would
	// breach it by a factor of N with "too many connections for role". The proxy's own ledger
	// is the ceiling that matters.
	ConnectionLimit int32
	// Verifier is the SCRAM-SHA-256 credential the role authenticates with, in PostgreSQL's
	// own stored form. Empty leaves the role passwordless, which is what it was before the
	// proxy dialled backends as the tenant.
	//
	// The verifier rather than the password, so the plaintext never crosses the exec channel
	// and the same bytes land on every instance this tenant is provisioned on - which is what
	// lets a migration re-establish the credential on a target without copying pg_authid.
	Verifier string
	// Readers are the tenant's other roles that must be able to reach the database. The owner
	// is always granted; this is everything else.
	Readers []string
}

// State is what the catalog currently says about a tenant's objects.
type State struct {
	// RoleExists reports a row in pg_roles.
	RoleExists bool
	// DatabaseOID is the row in pg_database, or zero when there is none. The OID rather
	// than the name is what distinguishes this database from a later one that reuses the
	// name.
	DatabaseOID int64
	// AllowsConnections is datallowconn. A database fenced by a migration exists without
	// serving anybody.
	AllowsConnections bool
	// ConnectionLimit is the role's rolconnlimit as PostgreSQL currently holds it.
	ConnectionLimit int32
}

// Serving reports whether both objects exist and the database admits connections.
func (s State) Serving() bool {
	return s.RoleExists && s.DatabaseOID != 0 && s.AllowsConnections
}

// Ensure brings the tenant's role and database into existence and proves the database is
// connectable, returning what the catalog says afterwards.
//
// The order is fixed: the role first, because the database is created OWNER of it. Every
// step is guarded by a catalog read and every creation additionally tolerates the object
// having appeared underneath it, so a second pass over a provisioned tenant issues no DDL
// at all and a lost race is not mistaken for a failure.
func Ensure(ctx context.Context, sql SQL, at Endpoint, spec Spec) (State, error) {
	if err := spec.validate(); err != nil {
		return State{}, err
	}
	postgres := at.WithDatabase(maintenanceDatabase)

	state, err := Observe(ctx, sql, postgres, spec)
	if err != nil {
		return State{}, err
	}

	changed, err := ensureRole(ctx, sql, postgres, spec, state)
	if err != nil {
		return state, err
	}
	created, err := ensureDatabase(ctx, sql, postgres, spec, state)
	if err != nil {
		return state, err
	}
	// Issued only when something was actually created. Both are idempotent, but a reconcile
	// loop that re-issued them would write a credential into the instance's log on every pass
	// and turn a steady-state reconcile into a stream of DDL - and "a second pass issues no
	// statement that changes anything" is a property worth keeping.
	//
	// The cost is that a tenant provisioned before either existed keeps its old posture until
	// something recreates its role or database. The migration path applies both on the target,
	// so a move heals one; nothing else does yet.
	if changed || created {
		if err := ensureCredential(ctx, sql, postgres, spec); err != nil {
			return state, err
		}
		if err := ensureDatabaseGrants(ctx, sql, postgres, spec); err != nil {
			return state, err
		}
	}

	if changed || created {
		if state, err = Observe(ctx, sql, postgres, spec); err != nil {
			return State{}, err
		}
	}
	if state.ConnectionLimit != spec.ConnectionLimit {
		if err := sql.Exec(ctx, postgres, fmt.Sprintf(`ALTER ROLE %s CONNECTION LIMIT %d`,
			migration.QuoteIdentifier(spec.Owner), spec.ConnectionLimit)); err != nil {
			return state, fmt.Errorf("setting the connection limit on role %q: %w", spec.Owner, err)
		}
		state.ConnectionLimit = spec.ConnectionLimit
	}

	if !state.Serving() {
		return state, fmt.Errorf("%w: %s", ErrNotServing, state.describe(spec))
	}
	return state, connectable(ctx, sql, at, spec)
}

// ErrNotServing reports objects that the catalog does not agree exist and admit
// connections after a provisioning pass. It is an error rather than a retry signal
// because every step that could have created them has already run.
var ErrNotServing = fmt.Errorf("the tenant's database is not serving")

// Drop removes the tenant's database and then its role, in that order, and is what
// reclaimPolicy Delete means. It is irreversible.
//
// WITH (FORCE) terminates whatever is still connected. Without it a single idle client
// holds the drop off indefinitely, and a reclaim that cannot finish is a finalizer that
// never releases.
func Drop(ctx context.Context, sql SQL, at Endpoint, spec Spec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	postgres := at.WithDatabase(maintenanceDatabase)
	if err := sql.Exec(ctx, postgres, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`,
		migration.QuoteIdentifier(spec.Database))); err != nil {
		return fmt.Errorf("dropping the tenant database %q: %w", spec.Database, err)
	}
	// The role is dropped after the database rather than with it, because a role that
	// still owns a database cannot be dropped at all.
	if err := sql.Exec(ctx, postgres, fmt.Sprintf(`DROP ROLE IF EXISTS %s`,
		migration.QuoteIdentifier(spec.Owner))); err != nil {
		return fmt.Errorf("dropping the tenant role %q: %w", spec.Owner, err)
	}
	return nil
}

// observeQuery reads both objects in one round trip. Each round trip is an exec into a
// Pod, so asking four questions separately costs four of them.
//
// Absent objects answer with a value rather than with no row: the outer SELECT has no FROM,
// so it produces exactly one row whatever the catalog holds, and a missing row would
// otherwise be indistinguishable from a failed query.
//
// datallowconn is projected through int rather than through text. psql displays a boolean
// as "t", but boolean::text is "true", and a client that assumes the displayed spelling
// reads every database as refusing connections.
const observeQuery = `SELECT (SELECT count(*)::text FROM pg_roles WHERE rolname = %[1]s),
 coalesce((SELECT d.oid::text FROM pg_database d WHERE d.datname = %[2]s), '0'),
 coalesce((SELECT d.datallowconn::int::text FROM pg_database d WHERE d.datname = %[2]s), '0'),
 coalesce((SELECT r.rolconnlimit::text FROM pg_roles r WHERE r.rolname = %[1]s), '-1')`

// Observe reads the tenant's objects out of the catalog. The endpoint is expected to name
// a maintenance database; pg_roles is shared, and pg_database has to be readable when the
// tenant's own database does not exist.
func Observe(ctx context.Context, sql SQL, at Endpoint, spec Spec) (State, error) {
	rows, err := sql.Query(ctx, at, fmt.Sprintf(observeQuery,
		migration.QuoteLiteral(spec.Owner), migration.QuoteLiteral(spec.Database)))
	if err != nil {
		return State{}, fmt.Errorf("reading the catalog on %s/%s: %w", at.Namespace, at.Instance, err)
	}
	if len(rows) != 1 || len(rows[0]) != 4 {
		return State{}, fmt.Errorf("unreadable catalog answer for database %q: %v", spec.Database, rows)
	}

	roles, err := strconv.ParseInt(strings.TrimSpace(rows[0][0]), 10, 32)
	if err != nil {
		return State{}, fmt.Errorf("unreadable role count for %q: %w", spec.Owner, err)
	}
	oid, err := strconv.ParseInt(strings.TrimSpace(rows[0][1]), 10, 64)
	if err != nil {
		return State{}, fmt.Errorf("unreadable oid for database %q: %w", spec.Database, err)
	}
	limit, err := strconv.ParseInt(strings.TrimSpace(rows[0][3]), 10, 32)
	if err != nil {
		return State{}, fmt.Errorf("unreadable connection limit for role %q: %w", spec.Owner, err)
	}
	return State{
		RoleExists:        roles > 0,
		DatabaseOID:       oid,
		AllowsConnections: strings.TrimSpace(rows[0][2]) == "1",
		ConnectionLimit:   int32(limit),
	}, nil
}

func ensureRole(ctx context.Context, sql SQL, postgres Endpoint, spec Spec, state State) (bool, error) {
	if state.RoleExists {
		return false, nil
	}
	// Every privileged attribute is spelled out and denied rather than left to the default,
	// because the tenant's role is now the whole privilege surface a client reaches: nothing
	// runs as pgelastic_ops on a tenant's behalf any more. NOCREATEROLE is the sharpest of
	// them - a role that holds it can mint cluster-global roles of its own, which defeats the
	// namespacing that keeps two tenants' identities apart.
	err := sql.Exec(ctx, postgres, fmt.Sprintf(
		`CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`,
		migration.QuoteIdentifier(spec.Owner)))
	if err != nil && !alreadyExists(err) {
		return false, fmt.Errorf("creating the tenant role %q: %w", spec.Owner, err)
	}
	return true, nil
}

// ensureCredential sets the role's password from a pre-computed verifier.
//
// Re-issued on every pass rather than compared first: the stored verifier is not readable
// without superuser access to pg_authid, and asking for it would make this a place where
// credentials are read rather than written. ALTER ROLE ... PASSWORD with an identical verifier
// is a no-op in effect, so the cost of re-issuing is one statement.
func ensureCredential(ctx context.Context, sql SQL, postgres Endpoint, spec Spec) error {
	if spec.Verifier == "" {
		return nil
	}
	statement := fmt.Sprintf(`ALTER ROLE %s PASSWORD %s`,
		migration.QuoteIdentifier(spec.Owner), migration.QuoteLiteral(spec.Verifier))
	if err := sql.Exec(ctx, postgres, statement); err != nil {
		return fmt.Errorf("setting the credential on role %q: %w", spec.Owner, err)
	}
	return nil
}

// ensureDatabaseGrants confines the tenant's database to the tenant's own roles.
//
// The default datacl grants CONNECT and TEMPORARY to PUBLIC. Roles are cluster-global, so once
// tenant roles can authenticate that means every tenant's role can open a session on every
// other tenant's database. Revoking it and granting back only this tenant's roles - plus the
// ops role, which the control plane reaches the database as - is what confines a tenant.
//
// Issued every pass. It is idempotent, and a tenant whose database was created before this
// existed would otherwise keep the open posture for ever.
func ensureDatabaseGrants(ctx context.Context, sql SQL, postgres Endpoint, spec Spec) error {
	database := migration.QuoteIdentifier(spec.Database)
	statements := []string{fmt.Sprintf(`REVOKE ALL ON DATABASE %s FROM PUBLIC`, database)}
	for _, role := range append([]string{spec.Owner}, spec.Readers...) {
		if role == "" {
			continue
		}
		statements = append(statements, fmt.Sprintf(
			`GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s`, database, migration.QuoteIdentifier(role)))
	}
	statements = append(statements, fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s`,
		database, migration.QuoteIdentifier(provision.OpsRole)))

	for _, statement := range statements {
		if err := sql.Exec(ctx, postgres, statement); err != nil {
			return fmt.Errorf("confining database %q to its own roles: %w", spec.Database, err)
		}
	}
	return nil
}

func ensureDatabase(ctx context.Context, sql SQL, postgres Endpoint, spec Spec, state State) (bool, error) {
	if state.DatabaseOID != 0 {
		return false, nil
	}
	// TEMPLATE template0 and no LOCALE clause, which is what the migration path issues.
	// Naming a locale here would let a freshly provisioned tenant differ from a migrated
	// one in the tuple that decides index ordering, and that difference produces wrong
	// results rather than an error.
	statement := fmt.Sprintf(`CREATE DATABASE %s TEMPLATE template0 OWNER %s`,
		migration.QuoteIdentifier(spec.Database), migration.QuoteIdentifier(spec.Owner))
	if err := sql.Exec(ctx, postgres, statement); err != nil && !alreadyExists(err) {
		return false, fmt.Errorf("creating the tenant database %q: %w", spec.Database, err)
	}
	return true, nil
}

// connectable opens a session on the tenant's own database and asks it to name itself.
//
// This is the difference between Ready meaning "a CREATE returned zero" and Ready meaning
// "a client can get in". A database can exist, be owned correctly and still refuse every
// connection, which is precisely the state a migration leaves its source in.
func connectable(ctx context.Context, sql SQL, at Endpoint, spec Spec) error {
	rows, err := sql.Query(ctx, at.WithDatabase(spec.Database), `SELECT current_database()`)
	if err != nil {
		return fmt.Errorf("connecting to the tenant database %q: %w", spec.Database, err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || strings.TrimSpace(rows[0][0]) != spec.Database {
		return fmt.Errorf("a session opened on %q reported itself as %v", spec.Database, rows)
	}
	return nil
}

// alreadyExists recognises the one failure that is not one: another actor created the
// object between the catalog read and the CREATE. PostgreSQL has no CREATE ROLE IF NOT
// EXISTS, so the race cannot be closed in SQL, and a concurrent reconcile of the same
// tenant is a normal event rather than an error worth reporting.
func alreadyExists(err error) bool {
	return strings.Contains(err.Error(), "already exists")
}

func (s Spec) validate() error {
	if s.Database == "" {
		return fmt.Errorf("a tenant spec needs a database name")
	}
	if s.Owner == "" {
		return fmt.Errorf("a tenant spec needs an owning role for database %q", s.Database)
	}
	return nil
}

func (s State) describe(spec Spec) string {
	parts := make([]string, 0, 3)
	if !s.RoleExists {
		parts = append(parts, fmt.Sprintf("role %q does not exist", spec.Owner))
	}
	if s.DatabaseOID == 0 {
		parts = append(parts, fmt.Sprintf("database %q does not exist", spec.Database))
	} else if !s.AllowsConnections {
		parts = append(parts, fmt.Sprintf("database %q refuses connections", spec.Database))
	}
	return strings.Join(parts, " and ")
}
