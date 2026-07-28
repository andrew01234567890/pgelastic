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

var standbyMembers = []string{sourceStandby, secondStandby}

func standbySlots() []Row {
	rows := make([]Row, 0, len(standbyMembers))
	for _, member := range standbyMembers {
		rows = append(rows, Row{provision.ReplicationSlotName(member)})
	}
	return rows
}

func namedSlots() string {
	names := make([]string, 0, len(standbyMembers))
	for _, member := range standbyMembers {
		names = append(names, provision.ReplicationSlotName(member))
	}
	return strings.Join(names, ", ")
}

// completeStack answers exactly as an instance carrying the whole PG18 failover-slot
// contract would.
func completeStack() *fakeSQL {
	return newFakeSQL().
		scalarAnswer("SHOW wal_level", "logical").
		scalarAnswer("SHOW synchronized_standby_slots", namedSlots()).
		answer("WHERE slot_type = 'physical'", standbySlots()...).
		answer("FROM pg_stat_replication WHERE sync_state", Row{sourceStandby}, Row{secondStandby}).
		scalarAnswer("SHOW hot_standby_feedback", "on").
		scalarAnswer("SHOW sync_replication_slots", "on").
		scalarAnswer("SHOW primary_slot_name", "pgelastic_pg_a_1").
		scalarAnswer("SHOW primary_conninfo", "host=pg-a-1 user=pgelastic_repl dbname=postgres")
}

func TestACompleteFailoverSlotStackPasses(t *testing.T) {
	check := CheckFailoverSlots(context.Background(), completeStack(), sourceAt, standbyMembers)
	if !check.Passed {
		t.Fatalf("a complete stack was refused: %s", check.Detail)
	}
}

func TestASourceWithoutLogicalWALIsRefused(t *testing.T) {
	sql := completeStack().scalarAnswer("SHOW wal_level", "replica")
	check := CheckFailoverSlots(context.Background(), sql, sourceAt, standbyMembers)
	if check.Passed {
		t.Fatal("a source with wal_level = replica was admitted to the online path")
	}
}

// TestAMissingSynchronizedStandbySlotIsRefused is the check that guards the silent failure:
// without the standby's slot named there, the subscriber can consume changes the standby
// has not flushed, and promoting that standby loses them with no error on either side.
func TestAMissingSynchronizedStandbySlotIsRefused(t *testing.T) {
	sql := completeStack().
		scalarAnswer("SHOW synchronized_standby_slots", provision.ReplicationSlotName(sourceStandby))
	check := CheckFailoverSlots(context.Background(), sql, sourceAt, standbyMembers)
	if check.Passed {
		t.Fatal("a synchronous standby missing from synchronized_standby_slots was admitted")
	}
	if !strings.Contains(check.Detail, "lose them on promotion") {
		t.Fatalf("the refusal does not explain the silent loss: %s", check.Detail)
	}
}

func TestAStandbyWithNoPhysicalSlotIsRefused(t *testing.T) {
	sql := completeStack().answer("WHERE slot_type = 'physical'", standbySlots()[0])
	check := CheckFailoverSlots(context.Background(), sql, sourceAt, standbyMembers)
	if check.Passed {
		t.Fatal("a synchronous standby with no physical slot was admitted")
	}
}

func TestASourceWithNoSynchronousStandbyIsRefused(t *testing.T) {
	sql := completeStack().answer("FROM pg_stat_replication WHERE sync_state")
	check := CheckFailoverSlots(context.Background(), sql, sourceAt, standbyMembers)
	if check.Passed {
		t.Fatal("a source with no synchronous standby was admitted to the online path")
	}
}

// TestAPrimaryConninfoWithoutDbnameIsRefused covers the failure that leaves replication
// looking perfectly healthy while the slot synchronization worker errors out on a loop.
func TestAPrimaryConninfoWithoutDbnameIsRefused(t *testing.T) {
	sql := completeStack().scalarAnswer("SHOW primary_conninfo", "host=pg-a-1 user=pgelastic_repl")
	check := CheckFailoverSlots(context.Background(), sql, sourceAt, standbyMembers)
	if check.Passed {
		t.Fatal("a standby whose primary_conninfo has no dbname= was admitted")
	}
	if !strings.Contains(check.Detail, "slot synchronization worker") {
		t.Fatalf("the refusal does not name the worker that breaks: %s", check.Detail)
	}
}

func TestStandbyGUCsAreEachRequired(t *testing.T) {
	for _, broken := range []struct {
		guc   string
		value string
	}{
		{"SHOW hot_standby_feedback", "off"},
		{"SHOW sync_replication_slots", "off"},
		{"SHOW primary_slot_name", ""},
	} {
		sql := completeStack().scalarAnswer(broken.guc, broken.value)
		check := CheckFailoverSlots(context.Background(), sql, sourceAt, standbyMembers)
		if check.Passed {
			t.Fatalf("a standby with %s = %q was admitted", broken.guc, broken.value)
		}
	}
}

func TestSplitSlotListIgnoresWhitespaceAndEmptyEntries(t *testing.T) {
	slots := splitSlotList(" a , , b ")
	if len(slots) != 2 || slots[0] != "a" || slots[1] != "b" {
		t.Fatalf("splitSlotList produced %#v", slots)
	}
}
