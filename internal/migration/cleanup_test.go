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

func testPlan() Plan {
	return Plan{
		Source:       sourceAt,
		Target:       targetAt,
		Publication:  "pgelastic_pub_move_deadbeef",
		Slot:         "pgelastic_mig_move_deadbeef",
		Subscription: "pgelastic_sub_move_deadbeef",
		SchemaStamp:  SchemaStamp(namespaceName, "move-acme"),
	}
}

func cleanableSQL() *fakeSQL {
	return newFakeSQL().
		scalarAnswer("FROM pg_subscription WHERE subname", "1").
		scalarAnswer("shobj_description(oid, 'pg_database')", "0").
		answer("FROM pg_namespace n WHERE", Row{userSchema})
}

func TestTheCleanupLadderRunsInTheOneOrderThatCannotHang(t *testing.T) {
	sql := cleanableSQL()
	if err := Cleanup(context.Background(), sql, testPlan(), provision.ReplicationRole); err != nil {
		t.Fatal(err)
	}
	sequence := []string{
		"ALTER SUBSCRIPTION",
		"slot_name = NONE",
		"DROP SUBSCRIPTION",
		"pg_drop_replication_slot",
		"DROP PUBLICATION",
	}
	previous := -1
	for _, fragment := range sequence {
		index := sql.ran(fragment)
		if index < 0 {
			t.Fatalf("the ladder never ran %q", fragment)
		}
		if index <= previous {
			t.Fatalf("%q ran at %d, out of order after %d", fragment, index, previous)
		}
		previous = index
	}
}

func TestTheSlotIsDetachedBeforeTheSubscriptionIsDropped(t *testing.T) {
	sql := cleanableSQL()
	if err := Cleanup(context.Background(), sql, testPlan(), provision.ReplicationRole); err != nil {
		t.Fatal(err)
	}
	// DROP SUBSCRIPTION on a still-attached subscription tries to drop the slot over the
	// subscription's own connection, which hangs whenever the source is what went wrong.
	if sql.ran("slot_name = NONE") >= sql.ran("DROP SUBSCRIPTION") {
		t.Fatal("the subscription was dropped while it still owned its slot")
	}
}

// TestTheLadderStillDropsTheSlotAfterAnEarlierRungFails is the property that keeps an
// abandoned slot from pinning the source primary's WAL. The rung that matters is near the
// end, so a ladder that aborted on the first error would routinely never reach it.
func TestTheLadderStillDropsTheSlotAfterAnEarlierRungFails(t *testing.T) {
	sql := cleanableSQL().fail("ALTER SUBSCRIPTION", errFake)
	err := Cleanup(context.Background(), sql, testPlan(), provision.ReplicationRole)
	if err == nil {
		t.Fatal("a failing rung was reported as success")
	}
	if sql.ran("pg_drop_replication_slot") < 0 {
		t.Fatal("the slot was left behind because an earlier rung failed")
	}
	if sql.ran("DROP PUBLICATION") < 0 {
		t.Fatal("the publication was left behind because an earlier rung failed")
	}
}

func TestTheLadderIsANoOpWhenTheSubscriptionIsAlreadyGone(t *testing.T) {
	sql := newFakeSQL().
		scalarAnswer("FROM pg_subscription WHERE subname", "0").
		scalarAnswer("shobj_description(oid, 'pg_database')", "0").
		answer("FROM pg_namespace n WHERE", Row{userSchema})
	if err := Cleanup(context.Background(), sql, testPlan(), provision.ReplicationRole); err != nil {
		t.Fatal(err)
	}
	if sql.ran("DROP SUBSCRIPTION") >= 0 {
		t.Fatal("the ladder dropped a subscription that did not exist")
	}
	if sql.ran("pg_drop_replication_slot") < 0 {
		t.Fatal("a missing subscription stopped the ladder reaching the slot")
	}
}

func TestTheLadderRevokesTheReadsItOpened(t *testing.T) {
	sql := cleanableSQL()
	if err := Cleanup(context.Background(), sql, testPlan(), provision.ReplicationRole); err != nil {
		t.Fatal(err)
	}
	if err := sql.sawAll(
		"REVOKE SELECT ON ALL TABLES IN SCHEMA",
		"REVOKE SELECT ON ALL SEQUENCES IN SCHEMA",
		"REVOKE USAGE ON SCHEMA",
	); err != nil {
		t.Fatal(err)
	}
}

// TestTheLadderSkipsTheFencedSourceWithoutReportingAFailure covers the state a successful
// cutover leaves behind: the source database refuses connections, so the two rungs that run
// inside it cannot, and the slot rung - the one that matters - still must.
func TestTheLadderSkipsTheFencedSourceWithoutReportingAFailure(t *testing.T) {
	sql := cleanableSQL().scalarAnswer("bool_or(datallowconn)", "false")
	if err := Cleanup(context.Background(), sql, testPlan(), provision.ReplicationRole); err != nil {
		t.Fatalf("a fenced source was reported as a cleanup failure: %v", err)
	}
	if sql.ran("pg_drop_replication_slot") < 0 {
		t.Fatal("a fenced source stopped the ladder dropping the slot")
	}
	if sql.ran("DROP PUBLICATION") >= 0 {
		t.Fatal("the ladder opened a session on a database that refuses connections")
	}
}

func TestLadderOrderMatchesTheDocumentedContract(t *testing.T) {
	want := []LadderStep{
		StepDisableSubscription, StepDetachSlot, StepDropSubscription,
		StepDropSlot, StepDropPublication, StepRevokeGrants, StepClearSchemaStamp,
	}
	if len(LadderOrder) != len(want) {
		t.Fatalf("the ladder has %d rungs, wanted %d", len(LadderOrder), len(want))
	}
	for index, step := range want {
		if LadderOrder[index] != step {
			t.Fatalf("rung %d is %s, wanted %s", index, LadderOrder[index], step)
		}
	}
}

func sweepableSQL() *fakeSQL {
	return newFakeSQL().
		answer("FROM pg_replication_slots WHERE slot_name LIKE",
			Row{liveSlotName}, Row{"pgelastic_mig_dead_11111111"}).
		answer("FROM pg_subscription s JOIN pg_database",
			Row{"pgelastic_sub_dead_11111111", tenantDatabase}).
		answer("FROM pg_database WHERE datallowconn", Row{tenantDatabase}).
		answer("FROM pg_publication WHERE pubname LIKE", Row{"pgelastic_pub_dead_11111111"})
}

func TestTheSweeperLeavesALiveMigrationAlone(t *testing.T) {
	live := map[string]bool{liveSlotName: true}
	orphans, err := FindOrphans(context.Background(), sweepableSQL(), sourceAt, live)
	if err != nil {
		t.Fatal(err)
	}
	for _, orphan := range orphans {
		if orphan.Name == liveSlotName {
			t.Fatal("the sweeper claimed a slot a running migration still owns")
		}
	}
	if len(orphans) != 3 {
		t.Fatalf("%d orphans found, wanted the slot, the subscription and the publication: %#v",
			len(orphans), orphans)
	}
}

func TestTheSweeperFindsASubscriptionInItsOwnDatabase(t *testing.T) {
	orphans, err := FindOrphans(context.Background(), sweepableSQL(), sourceAt, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, orphan := range orphans {
		if orphan.Kind != OrphanSubscription {
			continue
		}
		if orphan.At.Database != tenantDatabase {
			t.Fatalf("the subscription was located in %q; DROP SUBSCRIPTION only works in the "+
				"database that owns it", orphan.At.Database)
		}
		return
	}
	t.Fatal("no subscription orphan was found")
}

func TestSweepingDropsTheSubscriptionBeforeTheSlot(t *testing.T) {
	sql := sweepableSQL()
	orphans, err := FindOrphans(context.Background(), sql, sourceAt, nil)
	if err != nil {
		t.Fatal(err)
	}
	sweep := newFakeSQL()
	if err := SweepOrphans(context.Background(), sweep, orphans); err != nil {
		t.Fatal(err)
	}
	if sweep.ran("DROP SUBSCRIPTION") >= sweep.ran("pg_drop_replication_slot") {
		t.Fatal("the sweeper dropped a slot a subscription was still attached to")
	}
}

func TestLiveObjectNamesClaimsAllThreeObjectsOfAMigration(t *testing.T) {
	live := LiveObjectNames([][2]string{{namespaceName, "move-acme"}})
	for _, name := range []string{
		SlotName(namespaceName, "move-acme"),
		PublicationName(namespaceName, "move-acme"),
		SubscriptionName(namespaceName, "move-acme"),
	} {
		if !live[name] {
			t.Fatalf("%q is not claimed, so the sweeper would reap a running migration's object", name)
		}
	}
}

func TestIsAlreadyGoneRecognisesAMissingObject(t *testing.T) {
	if !IsAlreadyGone(errFakeMissing) {
		t.Fatal("a missing object was not recognised")
	}
	if IsAlreadyGone(nil) || IsAlreadyGone(errFake) {
		t.Fatal("an unrelated error was treated as a missing object")
	}
}

var errFakeMissing = errorString(`ERROR:  replication slot "pgelastic_mig_x" does not exist`)

type errorString string

func (e errorString) Error() string { return string(e) }

func TestGeneratedNamesFitPostgresIdentifierLimits(t *testing.T) {
	long := strings.Repeat("a-very-long-migration-name", 8)
	for _, name := range []string{
		PublicationName("a-namespace-that-is-also-long", long),
		SlotName("a-namespace-that-is-also-long", long),
		SubscriptionName("a-namespace-that-is-also-long", long),
	} {
		if len(name) > 63 {
			t.Fatalf("%q is %d bytes, past the 63-byte identifier limit", name, len(name))
		}
		for _, character := range name {
			legal := character >= 'a' && character <= 'z' ||
				character >= '0' && character <= '9' || character == '_'
			if !legal {
				t.Fatalf("%q contains %q, which a slot name may not hold", name, character)
			}
		}
	}
}

func TestTwoMigrationsOfTheSameNameInDifferentNamespacesGetDifferentSlots(t *testing.T) {
	if SlotName(namespaceName, "move") == SlotName("warehouse", "move") {
		t.Fatal("two namespaces collide on one slot, so cleanup of one would drop the other's")
	}
}
