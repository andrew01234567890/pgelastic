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
	"strings"
	"testing"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// stampQueryFragment is long enough to outrank the plainer pg_database fragments the other
// fakes answer, so a spec that scripts a stamp gets its own answer rather than a count.
const stampQueryFragment = "shobj_description(oid, 'pg_database')"

// roleEnumerationFragment identifies the query that finds the roles a tenant's database
// depends on, so a spec can script the answer.
const roleEnumerationFragment = "FROM pg_roles r JOIN closure c ON c.oid = r.oid"

// testReader is a role of the tenant's own: neither the owner nor the superuser, so a grant
// to it is one the carry has to actually move.
const testReader = "shop_reader"

func TestTheSchemaCopyCommitsTheWholeSchemaAndItsStampOrNeither(t *testing.T) {
	plan := testPlan()
	plan.DumpDir = "/var/lib/postgresql/data/pgelastic-migration/shop_move"
	command := schemaCopyCommand(plan, "shop", []RoleSpec{{Name: testReader}})

	if !strings.Contains(command, "--single-transaction") {
		t.Fatalf("the schema is applied statement by statement, so an interrupted copy leaves "+
			"objects a retry then fails on: %s", command)
	}
	stampAt := strings.Index(command, "COMMENT ON DATABASE")
	applyAt := strings.Index(command, "psql ")
	if stampAt < 0 {
		t.Fatalf("the copy leaves no record that it finished: %s", command)
	}
	if stampAt > applyAt {
		t.Fatalf("the stamp is written outside the transaction that applies the schema, so it "+
			"can survive an apply that did not: %s", command)
	}
	file := shellQuote(plan.DumpDir + ".schema.sql")
	if !strings.Contains(command, ">> "+file+"; psql ") || !strings.Contains(command, "--file="+file) {
		t.Fatalf("the stamp is not appended to the file psql applies: %s", command)
	}
}

// TestASecondProvisioningPassLeavesTheSchemaAlone is the defect the nightly found: a phase
// that cannot survive its own retry is not retryable, and the retry budget behind it is
// decoration.
func TestASecondProvisioningPassLeavesTheSchemaAlone(t *testing.T) {
	sql, shell := runningSQL(), &fakeShell{}
	engine := testEngine(sql, &fakeRouter{routed: sourceInstance}, shell)
	run := testRun(provisioning, online)

	if fault := engine.Step(context.Background(), run).Observation.Fault; fault != nil {
		t.Fatal(fault)
	}
	sql.scalarAnswer(stampQueryFragment, "1")

	result := engine.Step(context.Background(), run)
	if result.Observation.Fault != nil {
		t.Fatalf("provisioning a target it had already provisioned failed: %v", result.Observation.Fault)
	}
	if !result.Observation.Provisioned {
		t.Fatal("a target that already carries the schema was not reported provisioned")
	}
	if copies := strings.Count(shell.joined(), "pg_dump --schema-only"); copies != 1 {
		t.Fatalf("the schema was copied %d times onto the same target", copies)
	}
}

// TestAnInterruptedSchemaCopyIsCopiedAgainRatherThanTakenAsDone is the other half: the
// target of a copy that did not commit is unstamped, and an unstamped target is copied onto
// from scratch rather than assumed to be half-built and left.
func TestAnInterruptedSchemaCopyIsCopiedAgainRatherThanTakenAsDone(t *testing.T) {
	sql, shell := runningSQL(), &fakeShell{err: errFake}
	engine := testEngine(sql, &fakeRouter{routed: sourceInstance}, shell)
	run := testRun(provisioning, online)

	if engine.Step(context.Background(), run).Observation.Fault == nil {
		t.Fatal("a schema copy that failed was reported as a success")
	}

	shell.err = nil
	result := engine.Step(context.Background(), run)
	if result.Observation.Fault != nil {
		t.Fatal(result.Observation.Fault)
	}
	if copies := strings.Count(shell.joined(), "pg_dump --schema-only"); copies != 2 {
		t.Fatalf("the retry ran %d copies; an interrupted copy has to be redone in full", copies)
	}
	if !result.Observation.Provisioned {
		t.Fatal("the retry did not finish provisioning")
	}
}

// The defect the demo found: every relation arrived owned by postgres with an empty ACL, and
// the first write through the proxy was refused with 42501.
func TestTheSchemaCopyCarriesOwnershipAndPrivileges(t *testing.T) {
	command := schemaCopyCommand(testPlan(), "shop", []RoleSpec{{Name: testReader}})
	for _, unwanted := range []string{"--no-owner", "--no-privileges"} {
		if strings.Contains(command, unwanted) {
			t.Fatalf("the schema copy still passes %s: %s", unwanted, command)
		}
	}
}

// The database's own ACL is the one thing pg_dump cannot bring: it emits datacl only under
// --create, which neither path uses. Left alone, PUBLIC keeps the CONNECT the default grants
// it, and on an instance where tenant roles can authenticate that is every tenant able to open
// a session on every other tenant's database.
func TestTheSchemaCopyConfinesTheTargetDatabaseToItsOwnRoles(t *testing.T) {
	command := schemaCopyCommand(testPlan(), "shop", []RoleSpec{{Name: testReader}})
	for _, want := range []string{
		"REVOKE ALL ON DATABASE",
		`GRANT CONNECT, TEMPORARY ON DATABASE "acme" TO "shop"`,
		`GRANT CONNECT, TEMPORARY ON DATABASE "acme" TO "` + testReader + `"`,
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("the target database's ACL is missing %q: %s", want, command)
		}
	}
	if !strings.Contains(command, `GRANT CONNECT ON DATABASE "acme" TO "pgelastic_ops"`) {
		t.Fatal("the control plane cannot reach the database it just built")
	}
}

// GrantSourceReads opens SELECT to the replication role so pg_dump can read the source, and
// those grants are live while the dump runs. Now that privileges are carried, they would be
// applied to the target and become permanent - handing a credential that lives in every
// member's environment standing read access to the tenant's data.
func TestTheReplicationRolesTemporaryReadsDoNotBecomePermanent(t *testing.T) {
	command := schemaCopyCommand(testPlan(), "shop", nil)
	if !strings.Contains(command, "REVOKE ALL ON ALL TABLES IN SCHEMA") ||
		!strings.Contains(command, provision.ReplicationRole) {
		t.Fatalf("the dump's copy of the migration's own grants is never taken back: %s", command)
	}
	revokeAt := strings.Index(command, "REVOKE ALL ON ALL TABLES IN SCHEMA")
	stampAt := strings.Index(command, "COMMENT ON DATABASE")
	if revokeAt > stampAt {
		t.Fatal("the revoke lands after the stamp, so a copy could be marked complete with the " +
			"replication role still holding reads on the tenant's tables")
	}
}

// Every carried role has to exist before anything names it. The apply is one transaction, so
// a single missing grantee fails the whole copy rather than the statement that mentioned it.
func TestCarriedRolesAreCreatedBeforeTheDatabaseAndTheSchema(t *testing.T) {
	sql, shell := runningSQL(), &fakeShell{}
	sql.answer(roleEnumerationFragment, Row{testReader, "f", "t", "-1", ""}).
		answer("FROM pg_auth_members a")
	engine := testEngine(sql, &fakeRouter{routed: sourceInstance}, shell)

	if fault := engine.Step(context.Background(), testRun(provisioning, online)).Observation.Fault; fault != nil {
		t.Fatal(fault)
	}
	created := sql.ran(`CREATE ROLE "` + testReader + `"`)
	if created < 0 {
		t.Fatal("a role the tenant's objects depend on was never created on the target")
	}
	if database := sql.ran("CREATE DATABASE"); database >= 0 && created > database {
		t.Fatal("the roles were created after the database that is owned by one of them")
	}
	statement := sql.statement[created]
	for _, forbidden := range []string{"SUPERUSER", "CREATEROLE", "CREATEDB", "REPLICATION", "BYPASSRLS"} {
		if strings.Contains(statement, " "+forbidden) {
			t.Fatalf("a migration handed a tenant role %s: %s", forbidden, statement)
		}
	}
}

// objectCountFragment is the count of relations in the target's user schemas, which is what
// separates a database an earlier attempt half-built from one that was only ever created.
const objectCountFragment = "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"

// A target carrying objects but no stamp has exactly one cause: an attempt that applied a
// schema and then lost its stamp. Nothing recovers from it on its own, because the copy is
// skipped only when the stamp is there - so every later attempt re-applies the same schema
// onto its own objects and dies on "already exists" until the retry budget is gone. That is
// the state the 2026-07-29 nightly reached and could not leave.
func TestAnUnstampedTargetCarryingObjectsIsRebuiltRatherThanCopiedOnto(t *testing.T) {
	sql, shell := runningSQL(), &fakeShell{}
	sql.scalarAnswer("FROM pg_database WHERE datname", "1").
		scalarAnswer(objectCountFragment, "7")
	engine := testEngine(sql, &fakeRouter{routed: sourceInstance}, shell)

	result := engine.Step(context.Background(), testRun(provisioning, online))
	if result.Observation.Fault != nil {
		t.Fatalf("an unstamped target with objects on it was not healed: %v", result.Observation.Fault)
	}
	if sql.ran("DROP DATABASE") < 0 {
		t.Fatal("the wreckage of an earlier attempt was copied onto rather than discarded")
	}
	if sql.ran("CREATE DATABASE") < 0 {
		t.Fatal("the target was dropped and never recreated")
	}
}

// The guard that stops this rule deleting live tenants. After a successful cutover the ladder
// clears the stamp deliberately and the tenant serves from exactly that database, so
// "unstamped and non-empty" on its own would describe a paying tenant.
func TestATargetTheTenantIsServedFromIsNeverDiscarded(t *testing.T) {
	sql, shell := runningSQL(), &fakeShell{}
	sql.scalarAnswer("FROM pg_database WHERE datname", "1").
		scalarAnswer(objectCountFragment, "7")
	engine := testEngine(sql, &fakeRouter{routed: targetInstance}, shell)

	engine.Step(context.Background(), testRun(provisioning, online))
	if sql.ran("DROP DATABASE") >= 0 {
		t.Fatal("a database the tenant is being served from was dropped")
	}
}

func TestAStampedTargetIsNeverDiscarded(t *testing.T) {
	sql, shell := runningSQL(), &fakeShell{}
	sql.scalarAnswer("FROM pg_database WHERE datname", "1").
		scalarAnswer(objectCountFragment, "7").
		scalarAnswer(stampQueryFragment, "1")
	engine := testEngine(sql, &fakeRouter{routed: sourceInstance}, shell)

	engine.Step(context.Background(), testRun(provisioning, online))
	if sql.ran("DROP DATABASE") >= 0 {
		t.Fatal("a target whose schema copy had committed was discarded")
	}
}

// PostgreSQL refuses to drop a database that still owns a subscription however the connections
// are behaving, so WITH (FORCE) is not enough on its own.
func TestTheTargetsSubscriptionsAreRemovedBeforeItIsDropped(t *testing.T) {
	sql := newFakeSQL().answer("SELECT subname FROM pg_subscription", Row{"pgelastic_sub_move"})
	if err := DropTargetDatabase(context.Background(), sql, targetAt); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"DISABLE", "slot_name = NONE", "DROP SUBSCRIPTION"} {
		if index := sql.ran(fragment); index < 0 || index > sql.ran("DROP DATABASE") {
			t.Fatalf("%q did not run before the database was dropped", fragment)
		}
	}
}

func TestTheCleanupLadderTakesTheStampOffTheDatabaseItHandsOver(t *testing.T) {
	plan := testPlan()
	sql := cleanableSQL().scalarAnswer(stampQueryFragment, "1")
	if err := Cleanup(context.Background(), sql, plan, provision.ReplicationRole, false); err != nil {
		t.Fatal(err)
	}
	if sql.ran("COMMENT ON DATABASE") < 0 {
		t.Fatal("the tenant was handed a database still carrying the migration's mark")
	}
}

func TestAnUnstampedTargetIsLeftAloneByTheCleanupLadder(t *testing.T) {
	sql := cleanableSQL().scalarAnswer(stampQueryFragment, "0")
	if err := Cleanup(context.Background(), sql, testPlan(), provision.ReplicationRole, false); err != nil {
		t.Fatal(err)
	}
	if sql.ran("COMMENT ON DATABASE") >= 0 {
		t.Fatal("the ladder rewrote the comment on a database it had never stamped")
	}
}
