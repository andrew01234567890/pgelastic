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

package tenantuser

import (
	"context"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgelastic/internal/migration"
)

const (
	loginRole     = "pgtu_acme_app_1a2b3c4d"
	ownerRole     = "pgt_acme_c0ffee11"
	database      = "acme_prod"
	verifier      = "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy"
	reportingRole = "pgtu_acme_reporting_9f9f9f9f"
)

func at() Endpoint {
	return Endpoint{Namespace: "tenants", Instance: "pg-live", Database: database}
}

// settled is the catalog answer for a login that already matches its spec.
//
// The rolconfig column carries the read-only posture, because Ensure always writes that one:
// it is computed from a measurement that moves, so it has to be lifted as well as applied, and
// a login whose catalogue does not carry it yet is one that still owes a statement.
func settled(members string) migration.Row {
	return migration.Row{"1", "1", "1", members, "default_transaction_read_only=off"}
}

func spec() Spec {
	return Spec{Role: loginRole, Database: database, Login: true, Verifier: verifier}
}

// A second pass over a provisioned login must issue no DDL at all. A reconcile loop that
// re-issued its statements would write a credential into the instance's log on every pass and
// turn a steady state into a stream of ALTERs.
func TestASettledLoginIssuesNoStatements(t *testing.T) {
	sql := newFakeSQL().answer(settled(""))

	state, err := Ensure(context.Background(), sql, at(), spec())

	if err != nil {
		t.Fatalf("ensuring: %v", err)
	}
	if !state.Settled(spec()) {
		t.Fatalf("state = %+v, want settled", state)
	}
	if len(sql.statements) != 0 {
		t.Errorf("a settled login issued DDL: %v", sql.statements)
	}
}

// The role is the whole privilege surface a client reaches, so every privileged attribute is
// denied by name rather than left to the default.
func TestANewLoginIsCreatedWithEveryPrivilegedAttributeDenied(t *testing.T) {
	sql := newFakeSQL().answer(migration.Row{"-1", "0", "0", "", ""})

	if _, err := Ensure(context.Background(), sql, at(), spec()); err != nil {
		t.Fatalf("ensuring: %v", err)
	}

	create := sql.matching("CREATE ROLE")
	if create == "" {
		t.Fatal("the role was never created")
	}
	for _, denied := range []string{
		"NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOREPLICATION", "NOBYPASSRLS", "LOGIN",
	} {
		if !strings.Contains(create, denied) {
			t.Errorf("CREATE ROLE does not spell out %s: %s", denied, create)
		}
	}
}

// connectionLimit is the proxy's ledger or it is nothing. Mirroring it onto the role would make
// every backend the fleet opens count against rolconnlimit, so N replicas would breach a cap of
// N by a factor of N - which is why the tenant's own role is uncapped for the same reason.
func TestALoginsRoleIsNeverGivenAConnectionLimit(t *testing.T) {
	sql := newFakeSQL().answer(migration.Row{"-1", "0", "0", "", ""})

	if _, err := Ensure(context.Background(), sql, at(), spec()); err != nil {
		t.Fatalf("ensuring: %v", err)
	}

	if strings.Contains(sql.joined(), "CONNECTION LIMIT") {
		t.Errorf("a login's role was capped: %s", sql.joined())
	}
}

// A group role authenticates nobody, so it gets NOLOGIN and no credential at all - there is
// nothing to steal and nothing for the proxy to prove.
func TestAGroupRoleIsCreatedNologinAndPasswordless(t *testing.T) {
	group := Spec{Role: loginRole, Database: database, Login: false}
	sql := newFakeSQL().answer(migration.Row{"-1", "0", "0", "", ""})

	if _, err := Ensure(context.Background(), sql, at(), group); err != nil {
		t.Fatalf("ensuring: %v", err)
	}

	if !strings.Contains(sql.matching("CREATE ROLE"), "NOLOGIN") {
		t.Errorf("a group role may open a session: %s", sql.matching("CREATE ROLE"))
	}
	if strings.Contains(sql.joined(), "PASSWORD") {
		t.Errorf("a group role was given a credential: %s", sql.joined())
	}
}

// A migration recreates a tenant's carried roles passwordless on the target, deliberately. So a
// role that exists and cannot authenticate is an ordinary state to heal, and the only thing
// that notices is comparing the stored verifier rather than trusting that a created role was
// also credentialed.
func TestALoginCarriedToAnotherInstanceHasItsCredentialReapplied(t *testing.T) {
	sql := newFakeSQL().answer(migration.Row{"1", "0", "1", "", ""})

	if _, err := Ensure(context.Background(), sql, at(), spec()); err != nil {
		t.Fatalf("ensuring: %v", err)
	}

	if !strings.Contains(sql.joined(), "ALTER ROLE") || !strings.Contains(sql.joined(), "PASSWORD") {
		t.Errorf("the carried role was left unable to authenticate: %s", sql.joined())
	}
}

// The GRANT options are the whole of what memberOf promises. PostgreSQL 16+ defaults SET to
// TRUE, which would let a member SET ROLE to the group and create objects owned by it - more
// than "inherits its privileges" says, and it would defeat the attribution the design sells.
func TestAMembershipInheritsWithoutHandingOverTheIdentity(t *testing.T) {
	want := spec()
	want.MemberOf = []string{reportingRole}
	want.Owned = []string{reportingRole}
	sql := newFakeSQL().answer(settled(""))

	if _, err := Ensure(context.Background(), sql, at(), want); err != nil {
		t.Fatalf("ensuring: %v", err)
	}

	grant := sql.matching("GRANT \"pgtu_acme_reporting")
	if grant == "" {
		t.Fatalf("the membership was never granted: %s", sql.joined())
	}
	for _, option := range []string{"ADMIN FALSE", "INHERIT TRUE", "SET FALSE"} {
		if !strings.Contains(grant, option) {
			t.Errorf("the grant does not pin %s: %s", option, grant)
		}
	}
}

// Shrinking memberOf has to revoke, or the field only ever adds - which is the half an additive
// implementation gets right by accident and the half it gets wrong.
func TestAMembershipTheSpecNoLongerAsksForIsRevoked(t *testing.T) {
	want := spec()
	want.Owned = []string{reportingRole}
	sql := newFakeSQL().answer(settled(reportingRole))

	if _, err := Ensure(context.Background(), sql, at(), want); err != nil {
		t.Fatalf("ensuring: %v", err)
	}

	if !strings.Contains(sql.joined(), `REVOKE "pgtu_acme_reporting_9f9f9f9f"`) {
		t.Errorf("the dropped membership was left in place: %s", sql.joined())
	}
}

// The fence is the point: a membership granted by hand from a role this controller does not own
// is somebody's deliberate act, and reconciling it away would be this controller overwriting a
// decision it was never told about.
func TestAMembershipOutsideWhatWeOwnIsNeverRevoked(t *testing.T) {
	want := spec()
	want.Owned = []string{reportingRole}
	sql := newFakeSQL().answer(settled(""))

	if _, err := Ensure(context.Background(), sql, at(), want); err != nil {
		t.Fatalf("ensuring: %v", err)
	}

	if strings.Contains(sql.joined(), "REVOKE") {
		t.Errorf("a membership outside the fence was revoked: %s", sql.joined())
	}
	if !strings.Contains(sql.query, "ARRAY[") {
		t.Errorf("the observation did not fence the membership read: %s", sql.query)
	}
}

// DROP ROLE fails while a role owns anything, and nothing stops a tenant's owner granting this
// login CREATE. The objects go to the tenant's own owner, which already controls the database.
func TestDroppingALoginRehomesWhatItOwnsBeforeRemovingIt(t *testing.T) {
	sql := newFakeSQL()

	if err := Drop(context.Background(), sql, at(), spec(), ownerRole); err != nil {
		t.Fatalf("dropping: %v", err)
	}

	order := []string{"REASSIGN OWNED BY", "DROP OWNED BY", "DROP ROLE IF EXISTS"}
	position := -1
	for _, want := range order {
		next := indexOfStatement(sql.statements, want)
		if next < 0 {
			t.Fatalf("%s was never issued: %v", want, sql.statements)
		}
		if next < position {
			t.Fatalf("statements ran out of order: %v", sql.statements)
		}
		position = next
	}
}

// DROP OWNED without a reassign destroys the objects rather than rehoming them, so a tenant
// whose owner role has already gone must lose the role and nothing else.
func TestDroppingALoginWhoseOwnerHasGoneDoesNotDestroyWhatItOwned(t *testing.T) {
	sql := newFakeSQL()

	if err := Drop(context.Background(), sql, at(), spec(), ""); err != nil {
		t.Fatalf("dropping: %v", err)
	}

	if strings.Contains(sql.joined(), "DROP OWNED BY") {
		t.Errorf("objects were dropped with nowhere to reassign them: %s", sql.joined())
	}
	if !strings.Contains(sql.joined(), "DROP ROLE IF EXISTS") {
		t.Errorf("the role was left behind: %s", sql.joined())
	}
}

func indexOfStatement(statements []string, fragment string) int {
	for i, statement := range statements {
		if strings.Contains(statement, fragment) {
			return i
		}
	}
	return -1
}

// fakeSQL records what was issued and answers the one observation query. Its own double rather
// than tenantdbtest's Cluster, which models pg_roles and pg_database but knows nothing of
// pg_auth_members, has_database_privilege or rolpassword.
type fakeSQL struct {
	statements []string
	query      string
	row        migration.Row
}

func newFakeSQL() *fakeSQL {
	return &fakeSQL{}
}

// answer sets what the single observation query returns.
func (f *fakeSQL) answer(row migration.Row) *fakeSQL {
	f.row = row
	return f
}

func (f *fakeSQL) Exec(_ context.Context, _ migration.Endpoint, statement string) error {
	f.statements = append(f.statements, statement)
	return nil
}

func (f *fakeSQL) Query(_ context.Context, _ migration.Endpoint, statement string) ([]migration.Row, error) {
	f.query = statement
	if f.row == nil {
		return []migration.Row{{"-1", "0", "0", ""}}, nil
	}
	return []migration.Row{f.row}, nil
}

func (f *fakeSQL) joined() string { return strings.Join(f.statements, "\n") }

func (f *fakeSQL) matching(fragment string) string {
	for _, statement := range f.statements {
		if strings.Contains(statement, fragment) {
			return statement
		}
	}
	return ""
}
