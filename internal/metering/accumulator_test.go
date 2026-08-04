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

// The fixtures name one tenant and one database throughout; the facts under test are about
// counter arithmetic, not about names.
const (
	acmeTenant   = "acme"
	acmeDatabase = "acme_prod"
)

func statsOf(oid int64, commits int64) DatabaseStats {
	return DatabaseStats{
		DatabaseOID: oid,
		Counters:    map[Stat]int64{StatXactCommit: commits},
	}
}

func totalKeyOf() TotalKey {
	return TotalKey{Key: tenantKey(acmeTenant), Database: acmeDatabase, Role: RolePrimary}
}

func TestFreeingAPoolObjectDoesNotDecreaseATotal(t *testing.T) {
	accumulator := NewAccumulator()
	key := totalKeyOf()

	// A baseline, then 100 observed while watching.
	accumulator.Observe(key, "pg-a", statsOf(24591, 0))
	accumulator.Observe(key, "pg-a", statsOf(24591, 100))
	if total := accumulator.Total(key, StatXactCommit); total != 100 {
		t.Fatalf("total = %d, want 100", total)
	}

	// The proxy reclaims the tenant's idle pool object; the next one counts from zero.
	accumulator.PoolObjectFreed(key)
	accumulator.Observe(key, "pg-a", statsOf(24591, 30))

	if total := accumulator.Total(key, StatXactCommit); total != 130 {
		t.Errorf("total = %d after a pool object was freed and 30 more commits ran, want 130. "+
			"Anything below 100 is a decreasing counter, which every consumer downstream reads as a reset", total)
	}
}

func TestACounterGoingBackwardsIsTreatedAsAResetNotASubtraction(t *testing.T) {
	accumulator := NewAccumulator()
	key := totalKeyOf()

	// A baseline, then 500 observed, then the counter goes backwards - which means the
	// source restarted its count, so the 7 is accrual and never a subtraction.
	accumulator.Observe(key, "pg-a", statsOf(24591, 0))
	accumulator.Observe(key, "pg-a", statsOf(24591, 500))
	accumulator.Observe(key, "pg-a", statsOf(24591, 7))

	if total := accumulator.Total(key, StatXactCommit); total != 507 {
		t.Errorf("total = %d, want 507", total)
	}
}

func TestStatsResetIsARestartEvenWhenTheCountersRose(t *testing.T) {
	accumulator := NewAccumulator()
	key := totalKeyOf()

	first := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)

	before := statsOf(24591, 100)
	before.StatsReset = &first
	accumulator.Observe(key, "pg-a", before)

	after := statsOf(24591, 140)
	after.StatsReset = &second
	accumulator.Observe(key, "pg-a", after)

	// 140 rather than 240: the first reading is a baseline that accrues nothing, and then a
	// new stats_reset means the 140 is counting from a new origin - so the delta is the whole
	// 140 and not 40.
	if total := accumulator.Total(key, StatXactCommit); total != 140 {
		t.Errorf("total = %d, want 140: after a reset the whole value has accrued since the "+
			"new origin, so the delta is 140 and not 40", total)
	}
}

func TestPrimaryAndReplicaCountersAreNeverDifferencedAgainstEachOther(t *testing.T) {
	accumulator := NewAccumulator()
	primary := TotalKey{Key: tenantKey(acmeTenant), Database: acmeDatabase, Role: RolePrimary}
	replica := TotalKey{Key: tenantKey(acmeTenant), Database: acmeDatabase, Role: RoleReplica}

	// Baselines first - a first reading accrues nothing - then one real step on each side.
	// The replica's step is the smaller of the two on purpose: differenced against the
	// primary's it would be a decrease, and the whole point is that it never is.
	accumulator.Observe(primary, "pg-a", statsOf(24591, 900))
	accumulator.Observe(replica, "pg-a", statsOf(24591, 12))
	accumulator.Observe(primary, "pg-a", statsOf(24591, 940))
	accumulator.Observe(replica, "pg-a", statsOf(24591, 15))

	if total := accumulator.Total(replica, StatXactCommit); total != 3 {
		t.Errorf("replica total = %d, want 3", total)
	}
	if total := accumulator.Total(primary, StatXactCommit); total != 40 {
		t.Errorf("primary total = %d, want 40", total)
	}
}

func TestADatabaseRecreatedUnderTheSameNameRestartsItsCount(t *testing.T) {
	accumulator := NewAccumulator()
	key := totalKeyOf()

	// The first reading is a baseline: that 400 accrued on the postmaster before anybody
	// here was watching, and counting it would put the server's lifetime into the total on
	// every operator restart. The 50 after it did accrue while watching.
	accumulator.Observe(key, "pg-a", statsOf(24591, 400))
	accumulator.Observe(key, "pg-a", statsOf(24591, 450))
	// A new dbid is a new counter sequence, so its whole value is accrual rather than a
	// decrease to be swallowed.
	accumulator.Observe(key, "pg-a", statsOf(31002, 9))

	if total := accumulator.Total(key, StatXactCommit); total != 59 {
		t.Errorf("total = %d, want 59 (50 observed on the first database, then 9 on its "+
			"replacement); a new dbid is a new counter sequence, not a decrease", total)
	}
}

// The distinction the accumulator exists to draw, stated on its own because a missing cursor
// looks identical in all three cases. Reading a counter for the first time accrues nothing;
// every source that appears afterwards accrues its whole value.
func TestTheFirstReadingOfASourceIsABaselineRatherThanItsWholeLifetime(t *testing.T) {
	accumulator := NewAccumulator()
	key := totalKeyOf()

	accumulator.Observe(key, "pg-a", statsOf(24591, 1_000_000_000_000))

	if total := accumulator.Total(key, StatXactCommit); total != 0 {
		t.Errorf("total = %d after one reading, want 0: the counter is as old as the "+
			"postmaster and none of it accrued while this process was watching", total)
	}

	accumulator.Observe(key, "pg-a", statsOf(24591, 1_000_000_000_005))
	if total := accumulator.Total(key, StatXactCommit); total != 5 {
		t.Errorf("total = %d, want 5", total)
	}

	// A freed pool object is a source that went away, so the next reading is a fresh count
	// whose whole value is real - the same rule as a new dbid, and not the baseline rule.
	accumulator.PoolObjectFreed(key)
	accumulator.Observe(key, "pg-a", statsOf(24591, 7))
	if total := accumulator.Total(key, StatXactCommit); total != 12 {
		t.Errorf("total = %d, want 12: a reading after the source was freed is a fresh count", total)
	}
}

func TestForgetIsTheOnlyWayATotalDisappears(t *testing.T) {
	accumulator := NewAccumulator()
	key := totalKeyOf()

	accumulator.Observe(key, "pg-a", statsOf(24591, 100))
	accumulator.PoolObjectFreed(key)
	if accumulator.Len() != 1 {
		t.Errorf("accumulator holds %d totals after a pool object was freed, want 1", accumulator.Len())
	}

	accumulator.Forget(key)
	if accumulator.Len() != 0 {
		t.Errorf("accumulator holds %d totals after Forget, want 0", accumulator.Len())
	}
}

func TestNegativeDeltasAreRefusedRatherThanClamped(t *testing.T) {
	accumulator := NewAccumulator()
	key := totalKeyOf()

	accumulator.Add(key, map[Stat]int64{StatXactCommit: 50})
	applied := accumulator.Add(key, map[Stat]int64{StatXactCommit: -10})

	if len(applied) != 0 {
		t.Errorf("applied %v, want nothing", applied)
	}
	if total := accumulator.Total(key, StatXactCommit); total != 50 {
		t.Errorf("total = %d, want 50", total)
	}
}
