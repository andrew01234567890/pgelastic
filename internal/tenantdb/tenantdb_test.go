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

package tenantdb_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/tenantdb"
	"github.com/andrew01234567890/pgelastic/internal/tenantdb/tenantdbtest"
)

const (
	tenantDatabase = "acme_prod"
	tenantRole     = "acme_prod"
)

func endpoint() tenantdb.Endpoint {
	return migration.Endpoint{Namespace: "saas", Instance: "pg-a", Database: tenantDatabase}
}

func spec() tenantdb.Spec {
	return tenantdb.Spec{Database: tenantDatabase, Owner: tenantRole, ConnectionLimit: 60}
}

// The tier-2 limits as PostgreSQL spells them, which is what the role carries.
const (
	thirtySeconds = "30000ms"
	oneGigabyte   = "1048576kB"
)

func TestProvisioningCreatesTheRoleBeforeTheDatabaseThatIsOwnedByIt(t *testing.T) {
	cluster := tenantdbtest.NewCluster()

	state, err := tenantdb.Ensure(context.Background(), cluster, endpoint(), spec())
	if err != nil {
		t.Fatalf("provisioning a fresh tenant failed: %v", err)
	}

	statements := cluster.Statements()
	role, database := indexOf(statements, "CREATE ROLE"), indexOf(statements, "CREATE DATABASE")
	if role < 0 || database < 0 {
		t.Fatalf("provisioning issued neither a CREATE ROLE nor a CREATE DATABASE: %v", statements)
	}
	if role > database {
		t.Fatalf("the database was created before the role that owns it: %v", statements)
	}
	if !cluster.HasDatabase(tenantDatabase) || !cluster.HasRole(tenantRole) {
		t.Fatalf("the tenant reports provisioned with database=%v role=%v",
			cluster.HasDatabase(tenantDatabase), cluster.HasRole(tenantRole))
	}
	if owner := cluster.OwnerOf(tenantDatabase); owner != tenantRole {
		t.Fatalf("the tenant database is owned by %q rather than by the tenant role", owner)
	}
	if state.DatabaseOID == 0 {
		t.Fatal("the returned state carries no database oid, so a recreated database is indistinguishable")
	}
}

func TestTheDatabaseIsCreatedFromTemplateZeroLikeTheMigrationPathCreatesIt(t *testing.T) {
	cluster := tenantdbtest.NewCluster()

	if _, err := tenantdb.Ensure(context.Background(), cluster, endpoint(), spec()); err != nil {
		t.Fatalf("provisioning a fresh tenant failed: %v", err)
	}

	create := ""
	for _, statement := range cluster.Statements() {
		if strings.HasPrefix(statement, "CREATE DATABASE") {
			create = statement
		}
	}
	if !strings.Contains(create, "TEMPLATE template0") {
		t.Fatalf("a provisioned database and a migrated one would not share a template: %q", create)
	}
	if strings.Contains(create, "LOCALE") || strings.Contains(create, "ENCODING") {
		t.Fatalf("naming a locale here lets a provisioned tenant differ from a migrated one: %q", create)
	}
}

func TestASecondPassOverAProvisionedTenantIssuesNoDDL(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	ctx := context.Background()

	first, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec())
	if err != nil {
		t.Fatalf("the first pass failed: %v", err)
	}
	cluster.Forget()

	second, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec())
	if err != nil {
		t.Fatalf("the second pass failed rather than being a no-op: %v", err)
	}

	for _, forbidden := range []string{"CREATE", "ALTER", "DROP"} {
		if cluster.Ran(forbidden) != 0 {
			t.Fatalf("the second pass issued %s: %v", forbidden, cluster.Statements())
		}
	}
	if second.DatabaseOID != first.DatabaseOID {
		t.Fatalf("the second pass reports oid %d where the first reported %d",
			second.DatabaseOID, first.DatabaseOID)
	}
}

func TestAnObjectThatAppearsUnderneathTheCreateIsNotAFailure(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	ctx := context.Background()
	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec()); err != nil {
		t.Fatalf("provisioning failed: %v", err)
	}

	// The catalog read answers absent and both CREATEs then answer the way PostgreSQL
	// answers a lost race, which is what a second concurrent reconcile of the same tenant
	// sees.
	cluster.ConcealOnce(tenantRole, tenantDatabase)

	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec()); err != nil {
		t.Fatalf("a lost race was reported as a provisioning failure: %v", err)
	}
}

func TestTheConnectionLimitMirrorsTheTenantsCeilingAndIsNotReappliedOnceItMatches(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	ctx := context.Background()

	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec()); err != nil {
		t.Fatalf("provisioning failed: %v", err)
	}
	if limit := cluster.ConnectionLimit(tenantRole); limit != 60 {
		t.Fatalf("the role carries a connection limit of %d against a burstable ceiling of 60", limit)
	}

	cluster.Forget()
	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec()); err != nil {
		t.Fatalf("the second pass failed: %v", err)
	}
	if cluster.Ran("CONNECTION LIMIT") != 0 {
		t.Fatal("an unchanged connection limit was reapplied")
	}

	raised := spec()
	raised.ConnectionLimit = 90
	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), raised); err != nil {
		t.Fatalf("raising the ceiling failed: %v", err)
	}
	if limit := cluster.ConnectionLimit(tenantRole); limit != 90 {
		t.Fatalf("a raised ceiling left the role at %d", limit)
	}
}

// spec.limits.statementTimeout and spec.limits.tempFileLimit were resolved by
// policy.EffectiveFor and published in status.effective, and applied by nothing. The one place
// proxy.Tenant is built dropped them, and no ALTER ROLE SET existed anywhere.
func TestTheTierTwoLimitsReachTheRoleAndAreNotReappliedOnceTheyMatch(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	ctx := context.Background()

	limited := spec()
	limited.StatementTimeout = thirtySeconds
	limited.TempFileLimit = oneGigabyte
	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), limited); err != nil {
		t.Fatalf("provisioning failed: %v", err)
	}
	if got := cluster.RoleSetting(tenantRole, "statement_timeout"); got != thirtySeconds {
		t.Fatalf("the role carries statement_timeout %q", got)
	}
	if got := cluster.RoleSetting(tenantRole, "temp_file_limit"); got != oneGigabyte {
		t.Fatalf("the role carries temp_file_limit %q", got)
	}

	cluster.Forget()
	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), limited); err != nil {
		t.Fatalf("the second pass failed: %v", err)
	}
	if cluster.Ran("SET statement_timeout") != 0 || cluster.Ran("SET temp_file_limit") != 0 {
		t.Fatal("an unchanged limit was reapplied, so every reconcile writes to the catalog")
	}

	raised := limited
	raised.StatementTimeout = "60000ms"
	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), raised); err != nil {
		t.Fatalf("raising the timeout failed: %v", err)
	}
	if got := cluster.RoleSetting(tenantRole, "statement_timeout"); got != "60000ms" {
		t.Fatalf("a raised timeout left the role at %q", got)
	}
}

// A class that declares no statement timeout is not a class that overrides whatever the
// instance sets. Only one that declared a value and then removed it would be, and that is a
// change worth making explicit rather than inferring from an absence.
func TestAnUndeclaredLimitLeavesTheParameterAlone(t *testing.T) {
	cluster := tenantdbtest.NewCluster()

	if _, err := tenantdb.Ensure(context.Background(), cluster, endpoint(), spec()); err != nil {
		t.Fatalf("provisioning failed: %v", err)
	}
	if cluster.Ran("ALTER ROLE") == 0 {
		t.Fatal("the fixture should still have applied a connection limit")
	}
	if cluster.Ran("SET statement_timeout") != 0 || cluster.Ran("SET temp_file_limit") != 0 {
		t.Fatal("a tenant that declared no tier-2 limit had one written to its role")
	}
}

func TestADatabaseThatExistsButRefusesConnectionsIsNotServing(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	ctx := context.Background()

	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec()); err != nil {
		t.Fatalf("provisioning failed: %v", err)
	}
	cluster.Fence(tenantDatabase)

	state, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec())
	if err == nil {
		t.Fatal("a fenced database was reported as serving")
	}
	if !errors.Is(err, tenantdb.ErrNotServing) {
		t.Fatalf("a fenced database produced %v rather than ErrNotServing", err)
	}
	if state.Serving() {
		t.Fatal("the state of a fenced database says it is serving")
	}
}

func TestAFailedCreateIsReportedWithTheStatementThatFailed(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	cluster.FailOn("CREATE DATABASE", errors.New("permission denied to create database"))

	_, err := tenantdb.Ensure(context.Background(), cluster, endpoint(), spec())
	if err == nil {
		t.Fatal("a refused CREATE DATABASE was reported as success")
	}
	if !strings.Contains(err.Error(), tenantDatabase) ||
		!strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("the failure names neither the database nor what PostgreSQL said: %v", err)
	}
}

func TestAnUnreachableInstanceIsAFailureRatherThanAnAbsentDatabase(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	cluster.FailOn("pg_roles", errors.New("container not found"))

	if _, err := tenantdb.Ensure(context.Background(), cluster, endpoint(), spec()); err == nil {
		t.Fatal("an instance that could not be reached was reported as provisioned")
	}
}

func TestDroppingRemovesTheDatabaseAndThenTheRoleThatOwnedIt(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	ctx := context.Background()
	if _, err := tenantdb.Ensure(ctx, cluster, endpoint(), spec()); err != nil {
		t.Fatalf("provisioning failed: %v", err)
	}
	cluster.Forget()

	if err := tenantdb.Drop(ctx, cluster, endpoint(), spec()); err != nil {
		t.Fatalf("dropping the tenant failed: %v", err)
	}

	if cluster.HasDatabase(tenantDatabase) || cluster.HasRole(tenantRole) {
		t.Fatalf("the drop left database=%v role=%v behind",
			cluster.HasDatabase(tenantDatabase), cluster.HasRole(tenantRole))
	}
	statements := cluster.Statements()
	if indexOf(statements, "DROP DATABASE") > indexOf(statements, "DROP ROLE") {
		t.Fatalf("the role was dropped while it still owned the database: %v", statements)
	}
	if !strings.Contains(statements[indexOf(statements, "DROP DATABASE")], "FORCE") {
		t.Fatal("a single idle client would hold the drop off forever without FORCE")
	}
}

func TestDroppingATenantThatWasNeverProvisionedSucceeds(t *testing.T) {
	cluster := tenantdbtest.NewCluster()

	if err := tenantdb.Drop(context.Background(), cluster, endpoint(), spec()); err != nil {
		t.Fatalf("dropping absent objects failed rather than being a no-op: %v", err)
	}
}

func indexOf(statements []string, fragment string) int {
	for index, statement := range statements {
		if strings.Contains(statement, fragment) {
			return index
		}
	}
	return -1
}

// The default datacl grants CONNECT and TEMPORARY to PUBLIC. Roles are cluster-global, so once
// tenant roles can authenticate that means every tenant's role can open a session on every
// other tenant's database.
func TestTheDatabaseIsConfinedToTheTenantsOwnRoles(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	spec := spec()
	spec.Readers = []string{"acme_reader"}
	if _, err := tenantdb.Ensure(context.Background(), cluster, endpoint(), spec); err != nil {
		t.Fatal(err)
	}
	statements := strings.Join(cluster.Statements(), "\n")
	for _, want := range []string{
		`REVOKE ALL ON DATABASE "acme_prod" FROM PUBLIC`,
		`GRANT CONNECT, TEMPORARY ON DATABASE "acme_prod" TO "acme_prod"`,
		`GRANT CONNECT, TEMPORARY ON DATABASE "acme_prod" TO "acme_reader"`,
		`GRANT CONNECT ON DATABASE "acme_prod" TO "pgelastic_ops"`,
	} {
		if !strings.Contains(statements, want) {
			t.Fatalf("provisioning never issued %q:\n%s", want, statements)
		}
	}
}

// The verifier rather than the password, so the plaintext never crosses the exec channel and
// the same bytes land on every instance this tenant is ever provisioned on.
func TestTheRoleIsGivenItsCredentialAsAStoredVerifier(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	spec := spec()
	spec.Verifier = "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy"
	if _, err := tenantdb.Ensure(context.Background(), cluster, endpoint(), spec); err != nil {
		t.Fatal(err)
	}
	statements := strings.Join(cluster.Statements(), "\n")
	if !strings.Contains(statements, `ALTER ROLE "acme_prod" PASSWORD 'SCRAM-SHA-256$4096:`) {
		t.Fatalf("the role was never given its credential:\n%s", statements)
	}
}

// The tenant's role is now the whole privilege surface a client reaches, so every privileged
// attribute is denied explicitly rather than left to the default.
func TestTheTenantRoleIsCreatedWithNoPrivilegedAttribute(t *testing.T) {
	cluster := tenantdbtest.NewCluster()
	if _, err := tenantdb.Ensure(context.Background(), cluster, endpoint(), spec()); err != nil {
		t.Fatal(err)
	}
	var create string
	for _, statement := range cluster.Statements() {
		if strings.HasPrefix(statement, "CREATE ROLE") {
			create = statement
		}
	}
	for _, want := range []string{
		"NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOREPLICATION", "NOBYPASSRLS",
	} {
		if !strings.Contains(create, want) {
			t.Fatalf("the tenant role was created without %s: %s", want, create)
		}
	}
}

// The fake returns already-structured rows, so it cannot by itself catch a projection whose
// text the real transport mis-splits. This asserts the property the transport actually needs:
// whatever separator the query joins rolconfig with must survive migration.parseRows, which
// splits rows on a newline and columns on \x1f.
//
// The first version of this feature joined with a newline. A role carrying two settings then
// came back as two rows, Observe rejected the answer, and because rolconfig is durable and the
// failure happens before anything could undo it, the tenant failed every reconcile afterwards.
func TestTheRoleConfigProjectionSurvivesTheTransportsRowSplitting(t *testing.T) {
	joined := tenantdb.RoleConfigSeparator
	if strings.ContainsAny(joined, "\n\x1f") {
		t.Fatalf("rolconfig is joined with %q, which the transport reads as a row or column "+
			"break, so a role with two settings is unreadable", joined)
	}

	settings := tenantdb.ParseRoleConfig(
		"statement_timeout=" + thirtySeconds + joined + "temp_file_limit=" + oneGigabyte)
	if settings["statement_timeout"] != thirtySeconds || settings["temp_file_limit"] != oneGigabyte {
		t.Fatalf("two settings did not round-trip: %v", settings)
	}
}
