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
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"k8s.io/utils/ptr"
)

var frozen = time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

// testDumpDir is where the offline path stages its directory-format dump, on the target.
const testDumpDir = "/var/lib/postgresql/data/pgelastic-migration/shop_move"

// runningSQL answers every question a healthy online migration asks.
func runningSQL() *fakeSQL {
	sql := passingSource()
	completeStackInto(sql)
	return sql.
		answer("FROM pg_namespace n WHERE", Row{userSchema}).
		scalarAnswer("FROM pg_database WHERE datname", "0").
		scalarAnswer("shobj_description(oid, 'pg_database')", "0").
		scalarAnswer("FROM pg_publication WHERE pubname", "0").
		scalarAnswer("SELECT count(*)::text FROM pg_replication_slots WHERE slot_name", "0").
		scalarAnswer("FROM pg_subscription WHERE subname", "0").
		scalarAnswer("FROM pg_subscription_rel", "4 4").
		scalarAnswer("pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)", "0").
		scalarAnswer("SELECT pg_current_wal_lsn()", "0/3000000").
		scalarAnswer("coalesce(confirmed_flush_lsn, '0/0'::pg_lsn) >=", "1").
		scalarAnswer("FROM pg_stat_activity\nWHERE datname", "0").
		answer("FROM pg_sequences ORDER BY schemaname").
		scalarAnswer("string_agg(entry", "aaaa").
		answer(relationsQuery).
		scalarAnswer("SELECT count(*)::text FROM", "0")
}

func completeStackInto(sql *fakeSQL) {
	sql.scalarAnswer("SHOW wal_level", "logical").
		scalarAnswer("SHOW synchronized_standby_slots", namedSlots()).
		answer("WHERE slot_type = 'physical'", standbySlots()...).
		answer("FROM pg_stat_replication WHERE sync_state", Row{sourceStandby}, Row{secondStandby}).
		scalarAnswer("SHOW hot_standby_feedback", "on").
		scalarAnswer("SHOW sync_replication_slots", "on").
		scalarAnswer("SHOW primary_slot_name", "pgelastic_pg_a_1").
		scalarAnswer("SHOW primary_conninfo", "dbname=postgres")
}

func testRun(phase Phase, strategy Strategy) Run {
	return Run{
		Migration:       TenantRef{Namespace: namespaceName, Name: "move-acme"},
		Tenant:          TenantRef{Namespace: namespaceName, Name: tenantDatabase},
		Phase:           phase,
		Strategy:        strategy,
		Plan:            testPlan(),
		Preflight:       passingInput(),
		Sequences:       SequencePlan{Mode: pgelasticv1alpha1.SequenceHandlingSetvalWithGap, SafetyGap: 1000},
		Verification:    VerifySchema,
		ReplicationRole: provision.ReplicationRole,
		Owner:           tenantDatabase,
		DrainTimeout:    30 * time.Second,
		CutoverTimeout:  60 * time.Second,
	}
}

func testEngine(sql SQL, router *fakeRouter, shell Shell) Engine {
	return Engine{SQL: sql, Router: router, Shell: shell, Now: func() time.Time { return frozen }}
}

func TestProvisioningCreatesTheDatabaseGrantsAndReplicationObjects(t *testing.T) {
	sql, router, shell := runningSQL(), &fakeRouter{routed: sourceInstance}, &fakeShell{}
	result := testEngine(sql, router, shell).
		Step(context.Background(), testRun(provisioning, online))
	if result.Observation.Fault != nil {
		t.Fatal(result.Observation.Fault)
	}
	if !result.Observation.Provisioned {
		t.Fatal("provisioning did not report itself finished")
	}
	if err := sql.sawAll(
		"GRANT CONNECT ON DATABASE", "GRANT SELECT ON ALL TABLES IN SCHEMA",
		"CREATE DATABASE", "CREATE PUBLICATION", "pg_create_logical_replication_slot",
		"CREATE SUBSCRIPTION",
	); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(shell.joined(), "pg_dump --schema-only") {
		t.Fatalf("the target's schema was never copied: %s", shell.joined())
	}
}

// TestTheMigrationSlotIsCreatedWithFailoverEnabled is the difference between a migration
// that survives a failover of the source and one whose slot is destroyed by it.
func TestTheMigrationSlotIsCreatedWithFailoverEnabled(t *testing.T) {
	sql, router := runningSQL(), &fakeRouter{routed: sourceInstance}
	testEngine(sql, router, &fakeShell{}).Step(context.Background(), testRun(provisioning, online))
	index := sql.ran("pg_create_logical_replication_slot")
	if index < 0 {
		t.Fatal("no slot was created")
	}
	statement := sql.statement[index]
	if !strings.Contains(statement, "'pgoutput', false, false, true") {
		t.Fatalf("the slot was not created with failover enabled: %s", statement)
	}
}

func TestOfflineProvisioningDoesNotOpenAReplicationSlot(t *testing.T) {
	sql, router := runningSQL(), &fakeRouter{routed: sourceInstance}
	testEngine(sql, router, &fakeShell{}).Step(context.Background(), testRun(provisioning, offline))
	if sql.ran("pg_create_logical_replication_slot") >= 0 {
		t.Fatal("the offline path opened a logical slot it will never consume")
	}
}

func TestCopyingReportsInitialSyncProgress(t *testing.T) {
	sql := runningSQL().scalarAnswer("FROM pg_subscription_rel", "2 5")
	result := testEngine(sql, &fakeRouter{}, &fakeShell{}).
		Step(context.Background(), testRun(copying, online))
	if result.Copied == nil || *result.Copied != 2 || result.Total == nil || *result.Total != 5 {
		t.Fatalf("copy progress was reported as %v of %v", result.Copied, result.Total)
	}
	if result.Observation.CopyComplete {
		t.Fatal("a half-finished initial sync reported itself complete")
	}
}

func TestOfflineCopyingRunsAParallelDumpAndRestore(t *testing.T) {
	shell := &fakeShell{}
	run := testRun(copying, offline)
	run.Plan.Concurrency = 6
	run.Plan.DumpDir = testDumpDir
	run.Plan.SourceConnInfo = "host=pg-a-rw.default.svc port=5432 user=pgelastic_repl dbname=acme"
	result := testEngine(runningSQL(), &fakeRouter{}, shell).Step(context.Background(), run)
	if result.Observation.Fault != nil {
		t.Fatal(result.Observation.Fault)
	}
	commands := shell.joined()
	for _, want := range []string{
		"pg_dump --format=directory --jobs=6", "pg_restore --jobs=6", "--exit-on-error", "rm -rf",
	} {
		if !strings.Contains(commands, want) {
			t.Fatalf("the offline copy never ran %q: %s", want, commands)
		}
	}
}

// A dump with no source connection string reads the target's own local database.
//
// Both commands run inside the target's container, so SourceConnInfo is the only thing that
// makes the copy a copy of somewhere else. Empty renders an empty --dbname, which libpq
// treats as "use the defaults" rather than as an error, and the whole thing then succeeds:
// pg_dump dumps the target and pg_restore loads it back over the top. A tenant restore
// shipped that way, reported Completed, and restored nothing.
func TestAnOfflineCopyWithNoSourceIsRefused(t *testing.T) {
	shell := &fakeShell{}
	run := testRun(copying, offline)
	run.Plan.DumpDir = testDumpDir
	run.Plan.SourceConnInfo = ""

	result := testEngine(runningSQL(), &fakeRouter{}, shell).Step(context.Background(), run)
	if result.Observation.Fault == nil {
		t.Fatal("the copy was allowed to run against no source at all")
	}
	if commands := shell.joined(); strings.Contains(commands, "pg_dump") {
		t.Errorf("pg_dump ran anyway: %s", commands)
	}
}

func TestCatchupComparesLagAgainstTheConfiguredThreshold(t *testing.T) {
	sql := runningSQL().scalarAnswer("pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)", "4096")
	run := testRun(catchup, online)
	result := testEngine(sql, &fakeRouter{}, &fakeShell{}).Step(context.Background(), run)
	if result.Observation.CaughtUp {
		t.Fatal("a lagging subscription was declared caught up with no threshold set")
	}
	run.MaxLagBytes = ptr.To(int64(8192))
	result = testEngine(sql, &fakeRouter{}, &fakeShell{}).Step(context.Background(), run)
	if !result.Observation.CaughtUp {
		t.Fatal("a subscription inside its lag threshold was not declared caught up")
	}
}

// TestQuiescingReadsDrainFromPostgresRatherThanTheProxy keeps the two claims apart: a proxy
// that believes it has drained and a database with a backend still in a transaction are
// different things, and it is the second that the cutover depends on.
func TestQuiescingReadsDrainFromPostgresRatherThanTheProxy(t *testing.T) {
	sql := runningSQL().scalarAnswer("FROM pg_stat_activity\nWHERE datname", "1")
	router := &fakeRouter{routed: sourceInstance}
	result := testEngine(sql, router, &fakeShell{}).Step(context.Background(), testRun(quiescing, online))
	if !router.quiesced {
		t.Fatal("the proxy was never asked to queue the tenant's clients")
	}
	if result.Observation.Drained {
		t.Fatal("a source with a backend still in a transaction was reported drained")
	}
}

// The gate is the other half of the same claim. PostgreSQL can say that no backend is
// currently inside a transaction; only the gate can say that no further one may start, and
// a replica still admitting traffic makes the first answer true for an instant and useless.
func TestQuiescingIsNotDrainedWhileAReplicaStillAdmitsTraffic(t *testing.T) {
	sql := runningSQL().scalarAnswer("FROM pg_stat_activity\nWHERE datname", "0")
	router := &fakeRouter{routed: sourceInstance}
	router.gate = DrainStatus{Known: true, Quiesced: false, Drained: false, Queued: 4}
	result := testEngine(sql, router, &fakeShell{}).Step(context.Background(), testRun(quiescing, online))
	if result.Observation.Drained {
		t.Fatal("a fleet with a replica still admitting the tenant's traffic was reported drained")
	}
	if result.Queued == nil || *result.Queued != 4 {
		t.Fatalf("the queued clients were not published: %+v", result.Queued)
	}
}

func TestQuiescingIsDrainedWhenTheGateAndTheDatabaseAgree(t *testing.T) {
	sql := runningSQL().scalarAnswer("FROM pg_stat_activity\nWHERE datname", "0")
	router := &fakeRouter{routed: sourceInstance}
	router.gate = DrainStatus{Known: true, Quiesced: true, Drained: true}
	result := testEngine(sql, router, &fakeShell{}).Step(context.Background(), testRun(quiescing, online))
	if !result.Observation.Drained {
		t.Fatal("a closed gate over a quiet source was not reported drained")
	}
}

// Resume commits and release abandons, and applying the wrong one is how a successful
// cutover gets rolled back by its own lease expiring a moment later.
func TestASuccessfulCutoverResumesRatherThanMerelyReleasing(t *testing.T) {
	sql := runningSQL().scalarAnswer("FROM pg_subscription WHERE subname", "1")
	router := &fakeRouter{routed: targetInstance}
	engine := testEngine(sql, router, &fakeShell{})
	decision := Decide(cutover, Observation{Strategy: online, CutoverComplete: true})
	if err := engine.Apply(context.Background(), testRun(cutover, online), decision); err != nil {
		t.Fatal(err)
	}
	if !router.resumed {
		t.Fatal("a committed cutover never resumed, so an expiring lease could still undo it")
	}
}

func TestAnAbortReleasesTheHoldWithoutCommittingIt(t *testing.T) {
	sql := runningSQL().scalarAnswer("FROM pg_subscription WHERE subname", "1")
	router := &fakeRouter{routed: targetInstance, quiesced: true}
	run := testRun(quiescing, online)
	run.AbortRequested = true
	engine := testEngine(sql, router, &fakeShell{})
	step := engine.Step(context.Background(), run)
	decision := Decide(quiescing, step.Observation)
	if err := engine.Apply(context.Background(), run, decision); err != nil {
		t.Fatal(err)
	}
	if router.resumed {
		t.Fatal("an abort committed the flip it was abandoning")
	}
	if !router.released {
		t.Fatal("an abort left the tenant's clients queued")
	}
}

func TestCutoverFlipsRoutingOnlyAfterVerificationPasses(t *testing.T) {
	sql, router := runningSQL(), &fakeRouter{routed: sourceInstance}
	run := testRun(cutover, online)
	run.QuiesceStartedAt = ptr.To(frozen.Add(-300 * time.Millisecond))
	result := testEngine(sql, router, &fakeShell{}).Step(context.Background(), run)
	if result.Observation.Fault != nil {
		t.Fatal(result.Observation.Fault)
	}
	if router.routed != targetInstance {
		t.Fatalf("routing was left on %q", router.routed)
	}
	if sql.ran("ALLOW_CONNECTIONS false") < 0 {
		t.Fatal("the source was left admitting connections after the flip")
	}
	if result.PauseMillis == nil || *result.PauseMillis != 300 {
		t.Fatalf("the pause was measured as %v", result.PauseMillis)
	}
}

func TestCutoverRefusesToFlipWhenVerificationDisagrees(t *testing.T) {
	sql := runningSQL()
	router := &fakeRouter{routed: sourceInstance}
	// The target's schema fingerprint differs from the source's.
	target := newFakeSQL().
		scalarAnswer("string_agg(entry", "bbbb").
		answer(relationsQuery).
		answer("FROM pg_sequences ORDER BY schemaname")
	result := testEngine(splitSQL{source: sql, target: target}, router, &fakeShell{}).
		Step(context.Background(), testRun(cutover, online))
	if result.Observation.Fault == nil {
		t.Fatal("a non-equivalent target was flipped to")
	}
	if router.routed != sourceInstance {
		t.Fatalf("routing moved to %q despite verification refusing", router.routed)
	}
}

func TestCutoverWaitsForTheSubscriberToConfirmTheSourcesFinalLSN(t *testing.T) {
	sql := runningSQL().scalarAnswer("coalesce(confirmed_flush_lsn, '0/0'::pg_lsn) >=", "0")
	router := &fakeRouter{routed: sourceInstance}
	result := testEngine(sql, router, &fakeShell{}).Step(context.Background(), testRun(cutover, online))
	if result.Observation.CutoverComplete {
		t.Fatal("the cutover completed before the subscriber had confirmed the source's last write")
	}
	if router.routed != sourceInstance {
		t.Fatal("routing moved before the subscriber had caught up")
	}
}

func TestSequencesAreCarriedAcrossInsideTheCutover(t *testing.T) {
	sql := runningSQL().answer("FROM pg_sequences ORDER BY schemaname", Row{userSchema, ordersSequence, "900"})
	testEngine(sql, &fakeRouter{routed: sourceInstance}, &fakeShell{}).
		Step(context.Background(), testRun(cutover, online))
	if sql.ran("setval") < 0 {
		t.Fatal("the cutover flipped routing without carrying the sequences across")
	}
}

// TestEveryAbortAppliedByTheEngineRestoresTheSource is the invariant checked through the
// effects rather than through the decision: whatever phase a migration is stopped in, the
// tenant ends up routed to the instance it started on with its database readmitting
// connections.
func TestEveryAbortAppliedByTheEngineRestoresTheSource(t *testing.T) {
	for _, strategy := range []Strategy{online, offline} {
		for _, phase := range PhaseOrder(strategy) {
			if phase == completed {
				continue
			}
			sql := runningSQL().
				scalarAnswer("FROM pg_subscription WHERE subname", "1")
			// The tenant has already been flipped, which is the worst case an abort can meet.
			router := &fakeRouter{routed: targetInstance, quiesced: true}
			run := testRun(phase, strategy)
			run.AbortRequested = true
			engine := testEngine(sql, router, &fakeShell{})
			step := engine.Step(context.Background(), run)
			decision := Decide(phase, step.Observation)
			if err := engine.Apply(context.Background(), run, decision); err != nil {
				t.Fatalf("%s aborting from %s: %v", strategy, phase, err)
			}
			if router.routed != sourceInstance {
				t.Fatalf("%s aborting from %s left the tenant routed to %q",
					strategy, phase, router.routed)
			}
			if sql.ran("ALLOW_CONNECTIONS true") < 0 {
				t.Fatalf("%s aborting from %s left the source refusing connections", strategy, phase)
			}
			if sql.ran("pg_drop_replication_slot") < 0 {
				t.Fatalf("%s aborting from %s left the slot pinning the source's WAL", strategy, phase)
			}
			if Quiesced(phase, strategy) && !router.released {
				t.Fatalf("%s aborting from %s left the tenant's clients queued", strategy, phase)
			}
			if sql.ran("DROP DATABASE") < 0 {
				t.Fatalf("%s aborting from %s left a half-built copy on the target", strategy, phase)
			}
		}
	}
}

func TestApplyingASuccessfulCutoverDoesNotTouchTheSourceRouting(t *testing.T) {
	sql := runningSQL().scalarAnswer("FROM pg_subscription WHERE subname", "1")
	router := &fakeRouter{routed: targetInstance}
	engine := testEngine(sql, router, &fakeShell{})
	run := testRun(cutover, online)
	decision := Decide(cutover, Observation{Strategy: online, CutoverComplete: true})
	if err := engine.Apply(context.Background(), run, decision); err != nil {
		t.Fatal(err)
	}
	if router.routed != targetInstance {
		t.Fatalf("a successful cutover was undone, routing is on %q", router.routed)
	}
	if sql.ran("ALLOW_CONNECTIONS true") >= 0 {
		t.Fatal("a successful cutover readmitted connections to the source it had just fenced")
	}
	if sql.ran("pg_drop_replication_slot") < 0 {
		t.Fatal("a successful cutover left the slot pinning the source primary's WAL")
	}
}

func TestClosingTheRollbackWindowDropsTheSourceDatabase(t *testing.T) {
	sql := runningSQL()
	engine := testEngine(sql, &fakeRouter{routed: targetInstance}, &fakeShell{})
	decision := Decide(completed, Observation{Strategy: online, RollbackWindowClosed: true})
	if err := engine.Apply(context.Background(), testRun(completed, online), decision); err != nil {
		t.Fatal(err)
	}
	if sql.ran("DROP DATABASE") < 0 {
		t.Fatal("the source database was kept past its rollback window")
	}
}

func TestTheDrainBudgetIsObservedAcrossAControllerRestart(t *testing.T) {
	run := testRun(quiescing, online)
	run.QuiesceStartedAt = ptr.To(frozen.Add(-90 * time.Second))
	sql := runningSQL().scalarAnswer("FROM pg_stat_activity\nWHERE datname", "1")
	result := testEngine(sql, &fakeRouter{routed: sourceInstance}, &fakeShell{}).Step(context.Background(), run)
	if !result.Observation.DrainDeadlineExceeded {
		t.Fatal("a drain budget spent while the controller was down was not observed on its return")
	}
}

func TestAnAbortSkipsTheEffectOfThePhaseItStops(t *testing.T) {
	sql := runningSQL()
	run := testRun(provisioning, online)
	run.AbortRequested = true
	testEngine(sql, &fakeRouter{routed: sourceInstance}, &fakeShell{}).Step(context.Background(), run)
	if sql.ran("CREATE SUBSCRIPTION") >= 0 {
		t.Fatal("an aborted migration created the subscription it was told not to")
	}
}

// An empty answer and an absent one are different facts, and psql tells them apart by
// printing a newline for the first and nothing for the second. Collapsing them is how a
// tenant database with no sequences fails its own schema fingerprint - which coalesces to
// the empty string deliberately - in the middle of a cutover.
func TestAnEmptyAnswerIsNotAnAbsentOne(t *testing.T) {
	if rows := parseRows([]byte("")); rows != nil {
		t.Fatalf("a query that returned nothing produced %#v", rows)
	}
	rows := parseRows([]byte("\n"))
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "" {
		t.Fatalf("one row whose only column is empty parsed as %#v", rows)
	}
	rows = parseRows([]byte("a" + fieldSeparator + "b\nc" + fieldSeparator + "d\n"))
	if len(rows) != 2 || rows[1][1] != "d" {
		t.Fatalf("two rows parsed as %#v", rows)
	}
}

// A cutover fences the source and then flips the routing, as two calls. If the flip never
// lands, the tenant is left on a source that admits nobody - ALLOW_CONNECTIONS false refuses
// the superuser too - and the routing still names it. Gating the unfence on the routing
// having moved reads that state as "nothing to undo" and returns, on every retry, for ever.
func TestASourceFencedWithoutTheFlipIsReadmitted(t *testing.T) {
	sql := runningSQL().scalarAnswer("FROM pg_subscription WHERE subname", "1")
	router := &fakeRouter{routed: sourceInstance, quiesced: true}
	run := testRun(cutover, online)
	run.AbortRequested = true
	engine := testEngine(sql, router, &fakeShell{})

	step := engine.Step(context.Background(), run)
	if err := engine.Apply(context.Background(), run, Decide(cutover, step.Observation)); err != nil {
		t.Fatal(err)
	}

	if sql.ran("ALLOW_CONNECTIONS true") < 0 {
		t.Fatal("the source was left refusing every connection, including the superuser's, " +
			"because the routing had not moved and the unfence was gated on that")
	}
	if router.routed != sourceInstance {
		t.Fatalf("the tenant ended up routed to %q", router.routed)
	}
}

// Nothing stops two migrations of one tenant being active at once. A→B can complete and take
// writes on B while A→C is still in flight; when A→C settles, its terminal decision serves
// from the source it captured and would overwrite the binding with A.
func TestAMigrationDoesNotRerouteATenantSomebodyElseHasMoved(t *testing.T) {
	const elsewhere = "pg-elsewhere"
	sql := runningSQL().scalarAnswer("FROM pg_subscription WHERE subname", "1")
	router := &fakeRouter{routed: elsewhere, quiesced: true}
	run := testRun(cutover, online)
	run.AbortRequested = true
	engine := testEngine(sql, router, &fakeShell{})

	step := engine.Step(context.Background(), run)
	if err := engine.Apply(context.Background(), run, Decide(cutover, step.Observation)); err != nil {
		t.Fatal(err)
	}

	if router.routed != elsewhere {
		t.Fatalf("a settled migration routed the tenant back to %q, which stopped being "+
			"current the moment another migration took a write on %q", router.routed, elsewhere)
	}
	if sql.ran("ALLOW_CONNECTIONS true") < 0 {
		t.Fatal("the source was left fenced; the unfence is this migration's own to undo " +
			"whoever the tenant is routed to now")
	}
}

// The mirror of the test above. Closing only the backward flip leaves the forward one able to
// fence and take offline an instance this migration has no claim to any more.
func TestACutoverRefusesToFenceATenantSomebodyElseHasMoved(t *testing.T) {
	const elsewhere = "pg-elsewhere"
	sql := runningSQL().scalarAnswer("FROM pg_subscription WHERE subname", "1")
	router := &fakeRouter{routed: elsewhere}
	engine := testEngine(sql, router, &fakeShell{})

	err := engine.cutover(context.Background(), testRun(cutover, online), &StepResult{})

	if err == nil {
		t.Fatal("the cutover fenced and reflagged a tenant another migration had moved")
	}
	if sql.ran("ALLOW_CONNECTIONS false") >= 0 {
		t.Fatal("the source was fenced before the ownership of the routing was checked, so " +
			"the refusal left an instance offline that this migration does not own")
	}
	if router.routed != elsewhere {
		t.Fatalf("the cutover flipped the routing to %q anyway", router.routed)
	}
}
