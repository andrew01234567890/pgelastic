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

	"k8s.io/utils/ptr"
)

var sourceAt = Endpoint{Namespace: namespaceName, Instance: sourceInstance, Database: tenantDatabase}
var targetAt = Endpoint{Namespace: namespaceName, Instance: targetInstance, Database: tenantDatabase}

// passingSource answers every preflight query the way a source that may be moved would.
func passingSource() *fakeSQL {
	return newFakeSQL().
		answer(replicaIdentityQuery).
		answer("FROM pg_prepared_xacts").
		scalarAnswer("FROM pg_largeobject_metadata", "0").
		answer("SELECT extname FROM pg_extension", Row{AlwaysInstalledExtensions[0]}).
		scalarAnswer("FROM pg_database d WHERE d.datname = current_database()", "UTF8|C|C|b|C.UTF-8|").
		scalarAnswer("pg_database_size", "1048576").
		scalarAnswer("FROM pg_stat_activity WHERE backend_type", "12").
		answer(roleEnumerationFragment).
		answer("CASE WHEN r.rolsuper THEN 'SUPERUSER' END").
		scalarAnswer("e.grantee = 0 AND e.privilege_type = 'CONNECT'", "0").
		scalarAnswer(serverVersionQuery, "18")
}

// A migration must not be a route by which a tenant acquires an attribute it was never
// granted. CREATEROLE is the sharpest of them: a role that holds it can mint cluster-global
// roles of its own on the far side, which defeats every naming rule above it.
func TestPreflightRefusesToCarryAPrivilegedRole(t *testing.T) {
	sql := passingSource().
		answer(roleEnumerationFragment, Row{"shop_admin", "t", "t", "-1", ""}).
		answer("CASE WHEN r.rolsuper THEN 'SUPERUSER' END", Row{"shop_admin:CREATEROLE"})

	result := RunPreflight(context.Background(), sql, passingInput())
	if result.Passed() {
		t.Fatal("a role holding CREATEROLE was carried onto another instance")
	}
	if !strings.Contains(result.Message(), "shop_admin") {
		t.Fatalf("the refusal does not name the role that caused it: %s", result.Message())
	}
}

// Roles are cluster-global, so PUBLIC CONNECT on a tenant's database means every other
// tenant's role on the target instance can open a session on it. Refused here rather than
// carried forward.
func TestPreflightRefusesASourceThatLetsPublicConnect(t *testing.T) {
	sql := passingSource().
		scalarAnswer("e.grantee = 0 AND e.privilege_type = 'CONNECT'", "1")

	result := RunPreflight(context.Background(), sql, passingInput())
	if result.Passed() {
		t.Fatal("a database every role could connect to was accepted for migration")
	}
	if !strings.Contains(result.Message(), "PUBLIC") {
		t.Fatalf("the refusal does not say what is wrong: %s", result.Message())
	}
}

func passingInput() PreflightInput {
	return PreflightInput{
		Source: sourceAt, Target: targetAt,
		RequireReplicaIdentity: true, ForbidPreparedTransactions: true, RequireColdTenant: true,
		MaxSourceUtilizationPercent: 65,
		TenantIsCold:                ptr.To(true),
		TargetFreeBytes:             10 << 20,
	}
}

func TestPreflightPassesACleanSource(t *testing.T) {
	result := RunPreflight(context.Background(), passingSource(), passingInput())
	if !result.Passed() {
		t.Fatalf("a clean source was refused: %s", result.Message())
	}
}

func TestPreflightRefusesATableWithNoReplicaIdentityAndNamesIt(t *testing.T) {
	sql := passingSource().answer(replicaIdentityQuery, Row{offendingTable}, Row{"public.audit"})
	result := RunPreflight(context.Background(), sql, passingInput())
	if result.Passed() {
		t.Fatal("a table with no replica identity was admitted")
	}
	message := result.Message()
	for _, want := range []string{offendingTable, "public.audit", "REPLICA IDENTITY FULL"} {
		if !strings.Contains(message, want) {
			t.Fatalf("the refusal does not mention %q, so nobody can act on it: %s", want, message)
		}
	}
}

func TestPreflightRefusesAnOpenPreparedTransaction(t *testing.T) {
	sql := passingSource().answer("FROM pg_prepared_xacts", Row{"txn-42"})
	result := RunPreflight(context.Background(), sql, passingInput())
	if result.Passed() {
		t.Fatal("a prepared transaction was admitted")
	}
	if !strings.Contains(result.Message(), "txn-42") {
		t.Fatalf("the refusal does not name the prepared transaction: %s", result.Message())
	}
}

func TestPreflightRefusesAMismatchedCollationTuple(t *testing.T) {
	sql := passingSource()
	// The two sides are asked with the same statement, so a differing answer has to be
	// keyed on the endpoint. A second fake stands in for the target.
	target := newFakeSQL().
		scalarAnswer("FROM pg_database d WHERE d.datname = current_database()", "UTF8|en_US.utf8|en_US.utf8|c||")
	result := RunPreflight(context.Background(), splitSQL{source: sql, target: target}, passingInput())
	if result.Passed() {
		t.Fatal("a collation difference was admitted")
	}
	if !strings.Contains(result.Message(), "heap ordering") {
		t.Fatalf("the refusal does not say why a collation difference matters: %s", result.Message())
	}
}

func TestPreflightRefusesWhenTheTargetHasNoRoomForTwoCopies(t *testing.T) {
	input := passingInput()
	input.TargetFreeBytes = 1024
	result := RunPreflight(context.Background(), passingSource(), input)
	if result.Passed() {
		t.Fatal("a target with no headroom was admitted")
	}
	if !strings.Contains(result.Message(), "exist on both instances") {
		t.Fatalf("the refusal does not explain the two copies: %s", result.Message())
	}
}

func TestPreflightRefusesWhenTheTargetHeadroomIsUnknown(t *testing.T) {
	input := passingInput()
	input.TargetFreeBytes = 0
	result := RunPreflight(context.Background(), passingSource(), input)
	if result.Passed() {
		t.Fatal("unknown headroom was treated as sufficient headroom")
	}
}

func TestPreflightRefusesABusySource(t *testing.T) {
	sql := passingSource().scalarAnswer("FROM pg_stat_activity WHERE backend_type", "80")
	result := RunPreflight(context.Background(), sql, passingInput())
	if result.Passed() {
		t.Fatal("a source above the utilization ceiling was admitted")
	}
}

func TestPreflightRefusesAHotTenant(t *testing.T) {
	input := passingInput()
	input.TenantIsCold = ptr.To(false)
	result := RunPreflight(context.Background(), passingSource(), input)
	if result.Passed() {
		t.Fatal("a hot tenant was admitted")
	}
}

func TestPreflightTreatsAnUnobservedTenantAsNotCold(t *testing.T) {
	input := passingInput()
	input.TenantIsCold = nil
	result := RunPreflight(context.Background(), passingSource(), input)
	if result.Passed() {
		t.Fatal("a tenant whose coldness nobody established was treated as cold")
	}
	if !strings.Contains(result.Message(), "unestablished") {
		t.Fatalf("the refusal conflates unknown with false: %s", result.Message())
	}
}

func TestPreflightReportsAnUnlistedExtensionAsAnAdmissionHole(t *testing.T) {
	sql := passingSource().answer("SELECT extname FROM pg_extension", Row{AlwaysInstalledExtensions[0]}, Row{"postgis"})
	result := RunPreflight(context.Background(), sql, passingInput())
	if result.Passed() {
		t.Fatal("an extension outside the allowlist was admitted")
	}
	if !strings.Contains(result.Message(), "postgis") {
		t.Fatalf("the refusal does not name the extension: %s", result.Message())
	}
}

func TestPreflightAdmitsAnAllowlistedExtension(t *testing.T) {
	sql := passingSource().answer("SELECT extname FROM pg_extension", Row{AlwaysInstalledExtensions[0]}, Row{"pg_trgm"})
	input := passingInput()
	input.AllowedExtensions = []string{"pg_trgm"}
	result := RunPreflight(context.Background(), sql, input)
	if !result.Passed() {
		t.Fatalf("an allowlisted extension was refused: %s", result.Message())
	}
}

func TestPreflightReportsEveryFailureRatherThanTheFirst(t *testing.T) {
	sql := passingSource().
		answer(replicaIdentityQuery, Row{offendingTable}).
		answer("FROM pg_prepared_xacts", Row{"txn-1"})
	input := passingInput()
	input.TenantIsCold = ptr.To(false)
	result := RunPreflight(context.Background(), sql, input)
	if got := len(result.Failures()); got != 3 {
		t.Fatalf("%d failures reported, wanted 3; fixing one refusal only to meet the next "+
			"teaches the truth one round trip at a time: %s", got, result.Message())
	}
}

func TestAnEmptyResultDoesNotPass(t *testing.T) {
	if (PreflightResult{}).Passed() {
		t.Fatal("a gate that has not run reported a pass")
	}
}

// splitSQL routes queries to a different fake depending on the instance, which is how the
// two sides of a comparison can disagree.
type splitSQL struct {
	source *fakeSQL
	target *fakeSQL
}

func (s splitSQL) pick(at Endpoint) *fakeSQL {
	if at.Instance == targetAt.Instance {
		return s.target
	}
	return s.source
}

func (s splitSQL) Exec(ctx context.Context, at Endpoint, statement string) error {
	return s.pick(at).Exec(ctx, at, statement)
}

func (s splitSQL) Query(ctx context.Context, at Endpoint, statement string) ([]Row, error) {
	return s.pick(at).Query(ctx, at, statement)
}

// Logical replication carries neither an unlogged table nor a materialized view, and nothing
// downstream notices: the verifier's relation inventory excludes relkind 'm' entirely and
// nothing reads relispopulated, so an unlogged table surfaces only as a row-count mismatch at
// cutover and a matview surfaces as the tenant's first query failing. Refusing at preflight is
// what turns two silent data losses into one legible sentence.
func TestPreflightRefusesRelationsLogicalReplicationCannotCarry(t *testing.T) {
	sql := newFakeSQL().answer("relpersistence", Row{"public.report (materialized view)"})

	check := checkPublishableRelations(context.Background(), sql, publishableSource())

	if check.Passed {
		t.Fatal("a source holding a materialized view was admitted to the online path")
	}
	for _, want := range []string{"materialized view", "Offline", "has not been populated"} {
		if !strings.Contains(check.Detail, want) {
			t.Errorf("the refusal does not mention %q: %s", want, check.Detail)
		}
	}
}

func TestPreflightAdmitsASourceOfOrdinaryTables(t *testing.T) {
	sql := newFakeSQL().answer("relpersistence")

	check := checkPublishableRelations(context.Background(), sql, publishableSource())

	if !check.Passed {
		t.Fatalf("an ordinary source was refused: %s", check.Detail)
	}
}

// pg_dump defaults to --no-statistics, so without the flag every migrated tenant lands with an
// empty pg_statistic and the planner guesses - immediately after a cutover sold on a
// sub-second pause.
func publishableSource() Endpoint {
	return Endpoint{Namespace: "tenants", Instance: liveInstance, Database: tenantDatabase}
}

func TestTheOfflineDumpCarriesStatistics(t *testing.T) {
	command := offlineDumpCommand(Plan{
		DumpDir:        "/scratch/dump",
		SourceConnInfo: "host=/tmp dbname=acme",
	}, DefaultDumpJobs)

	if !strings.Contains(command, "--statistics") {
		t.Errorf("the dump leaves the tenant with no optimizer statistics: %s", command)
	}
}

// Both dumps run in the target's container, so the binary is always the target's, and
// pg_dump refuses outright to read a server newer than itself. Without this check that
// refusal arrives as a shell exit code inside a retry budget, which spends itself re-running
// a command that could never have worked.
func TestAMoveOntoAnOlderMajorIsRefusedHereRatherThanInsidePgDump(t *testing.T) {
	sql := passingSource().scalarAnswer(serverVersionQuery, "19")
	sql.endpointAnswer(targetAt.WithDatabase("postgres"), serverVersionQuery, "18")

	check := checkVersionCompatibility(t.Context(), sql, passingInput())

	if check.Passed {
		t.Fatal("a move from PostgreSQL 19 onto 18 passed preflight")
	}
	if !strings.Contains(check.Detail, "refuses to read a server newer than itself") {
		t.Errorf("the refusal does not say why: %q", check.Detail)
	}
}

// The recommended upgrade route is to stand up an instance on the new major and migrate
// tenants onto it one at a time. Refusing the newer target would refuse exactly that.
func TestAMoveOntoTheNextMajorIsTheSupportedDirection(t *testing.T) {
	sql := passingSource().scalarAnswer(serverVersionQuery, "18")
	sql.endpointAnswer(targetAt.WithDatabase("postgres"), serverVersionQuery, "19")

	check := checkVersionCompatibility(t.Context(), sql, passingInput())

	if !check.Passed {
		t.Fatalf("a move from 18 onto 19 was refused: %s", check.Detail)
	}
}

// One major at a time is the path that gets tested. Two is a combination nobody has run, and
// it is something an operator drifts into rather than chooses.
func TestAMultiMajorJumpIsRefused(t *testing.T) {
	sql := passingSource().scalarAnswer(serverVersionQuery, "18")
	sql.endpointAnswer(targetAt.WithDatabase("postgres"), serverVersionQuery, "20")

	check := checkVersionCompatibility(t.Context(), sql, passingInput())

	if check.Passed {
		t.Fatal("a two-major jump passed preflight")
	}
	if !strings.Contains(check.Detail, "jump of 2") {
		t.Errorf("the refusal does not name the size of the jump: %q", check.Detail)
	}
}

// Large objects are not publishable, so an online move would arrive with the rows that
// reference them and without the objects. `pg_dump` carries them, so the offline strategy is
// unaffected - and refusing there refused a move that works.
//
// It refused there. The check was unconditional, and preflight cannot fault, so a tenant that
// had ever called `lo_from_bytea` - a function PostgreSQL grants to PUBLIC and this tree never
// revokes - parked in Preflight for ever under every strategy. The drain finalizer on its
// instance held with it, so one large object made a PgInstance permanently undrainable.
func TestALargeObjectRefusesOnlyTheOnlinePath(t *testing.T) {
	for _, probe := range []struct {
		name    string
		input   PreflightInput
		refused bool
	}{
		{"offline carries them", PreflightInput{Online: false, ForbidLargeObjects: true}, false},
		{"online without the toggle", PreflightInput{Online: true, ForbidLargeObjects: false}, false},
		{"online with the toggle", PreflightInput{Online: true, ForbidLargeObjects: true}, true},
	} {
		t.Run(probe.name, func(t *testing.T) {
			sql := passingSource().
				scalarAnswer("FROM pg_largeobject_metadata", "3")
			in := probe.input
			in.Source = sourceAt
			in.Target = targetAt

			result := RunPreflight(context.Background(), sql, in)

			// Asked of this check alone. The whole verdict folds in gates the fake source
			// cannot answer, so it would say nothing about the one under test.
			var found *Check
			for i := range result.Checks {
				if result.Checks[i].Name == CheckLargeObjects {
					found = &result.Checks[i]
				}
			}
			switch {
			case !probe.refused && found != nil:
				t.Fatalf("the large-object gate ran where it should not have: %s", found.Detail)
			case probe.refused && found == nil:
				t.Fatal("the large-object gate did not run on the online path")
			case probe.refused && found.Passed:
				t.Fatal("three large objects passed the online gate")
			case probe.refused && !strings.Contains(found.Detail, "Offline strategy"):
				t.Fatalf("the refusal does not name the way out: %s", found.Detail)
			}
		})
	}
}
