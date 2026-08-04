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

package metering

import (
	"testing"
	"time"
)

// The names the readings helper generates, and the instance every test stages them on.
const (
	firstDatabase  = "a_db"
	secondDatabase = "b_db"
	testInstance   = "pg-a"
)

func readings(counters ...int64) map[string]DatabaseStats {
	byDatabase := map[string]DatabaseStats{}
	for i, value := range counters {
		byDatabase[string(rune('a'+i))+"_db"] = DatabaseStats{
			DatabaseOID: int64(20000 + i),
			Counters:    map[Stat]int64{StatXactCommit: value},
		}
	}
	return byDatabase
}

func TestAReadingIsHeldUntilThePoolsRoundPicksItUp(t *testing.T) {
	collector := NewCollector(Options{}, nil)
	collector.RecordDatabaseStats(testNamespace, testInstance, epoch, readings(100))

	key := ReadingKey{Namespace: testNamespace, Instance: testInstance, Database: firstDatabase}
	stats, ok := collector.DatabaseStatsFor(key, epoch.Add(time.Second))
	if !ok || stats.Counters[StatXactCommit] != 100 {
		t.Fatalf("reading = (%+v, %v), want the staged one", stats, ok)
	}
}

// Past the bound a reading is absent rather than replayed. Replaying it would fold a level
// that has stopped moving into every round as though it were current, which is exactly what a
// live-but-idle tenant looks like - so an instance that had stopped answering would be
// indistinguishable from one with nothing to do.
func TestAReadingOlderThanTheBoundIsAbsentRatherThanReplayed(t *testing.T) {
	collector := NewCollector(Options{}, nil)
	collector.RecordDatabaseStats(testNamespace, testInstance, epoch, readings(100))

	key := ReadingKey{Namespace: testNamespace, Instance: testInstance, Database: firstDatabase}
	if _, ok := collector.DatabaseStatsFor(key, epoch.Add(ReadingTTL+time.Second)); ok {
		t.Error("a reading older than the bound was served, so a silent instance reads as an " +
			"idle one")
	}
}

// A database that has been dropped has to stop being reported, and a scrape that no longer
// mentions it is the only signal there is.
func TestARecordReplacesTheInstancesWholePreviousScrape(t *testing.T) {
	collector := NewCollector(Options{}, nil)
	collector.RecordDatabaseStats(testNamespace, testInstance, epoch, readings(100, 200))
	if got := collector.Readings(); got != 2 {
		t.Fatalf("the collector holds %d readings, want 2", got)
	}

	collector.RecordDatabaseStats(testNamespace, testInstance, epoch, readings(150))
	if got := collector.Readings(); got != 1 {
		t.Errorf("the collector holds %d readings after a scrape that named one database, "+
			"want 1: the dropped database is still being reported", got)
	}
	dropped := ReadingKey{Namespace: testNamespace, Instance: testInstance, Database: secondDatabase}
	if _, ok := collector.DatabaseStatsFor(dropped, epoch); ok {
		t.Error("a database the latest scrape does not mention is still being served")
	}
}

// One operator serves every namespace, and two customers are entitled to name their instance
// and their database the same thing.
func TestTwoNamespacesSameNamesAreDifferentReadings(t *testing.T) {
	collector := NewCollector(Options{}, nil)
	collector.RecordDatabaseStats("left", testInstance, epoch, readings(100))
	collector.RecordDatabaseStats("right", testInstance, epoch, readings(900))

	left, _ := collector.DatabaseStatsFor(
		ReadingKey{Namespace: "left", Instance: testInstance, Database: firstDatabase}, epoch)
	right, _ := collector.DatabaseStatsFor(
		ReadingKey{Namespace: "right", Instance: testInstance, Database: firstDatabase}, epoch)
	if left.Counters[StatXactCommit] != 100 || right.Counters[StatXactCommit] != 900 {
		t.Errorf("left = %d and right = %d, want 100 and 900: one namespace is being answered "+
			"with another's readings",
			left.Counters[StatXactCommit], right.Counters[StatXactCommit])
	}
}

// Held state has to be bounded by the instances that exist rather than by the instances that
// ever existed, which is the same bound the store and the label set are held to.
func TestForgettingAnInstanceReleasesExactlyItsReadings(t *testing.T) {
	collector := NewCollector(Options{}, nil)
	collector.RecordDatabaseStats(testNamespace, "kept", epoch, readings(100, 200))
	collector.RecordDatabaseStats(testNamespace, "removed", epoch, readings(300, 400))

	collector.ForgetInstance(testNamespace, "removed")
	if got := collector.Readings(); got != 2 {
		t.Errorf("the collector holds %d readings after forgetting one of two instances, want 2",
			got)
	}
	kept := ReadingKey{Namespace: testNamespace, Instance: "kept", Database: firstDatabase}
	if _, ok := collector.DatabaseStatsFor(kept, epoch); !ok {
		t.Error("forgetting one instance took the other's readings with it")
	}
}
