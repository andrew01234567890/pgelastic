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
	"maps"
	"time"
)

// Stat names one cumulative counter carried in a pg_stat_database row. The set is closed
// and small on purpose: it is a metric label value, so every entry added here costs one
// series per pool per role, permanently.
type Stat string

const (
	StatXactCommit   Stat = "xact_commit"
	StatXactRollback Stat = "xact_rollback"
	StatBlksRead     Stat = "blks_read"
	StatBlksHit      Stat = "blks_hit"
	StatTupReturned  Stat = "tup_returned"
	StatTupFetched   Stat = "tup_fetched"
	StatTupModified  Stat = "tup_modified"
	StatDeadlocks    Stat = "deadlocks"
)

// Stats is every stat, in a fixed order, so the metric label set can be materialised up
// front rather than appearing the first time a tenant happens to deadlock.
var Stats = [8]Stat{
	StatXactCommit, StatXactRollback,
	StatBlksRead, StatBlksHit,
	StatTupReturned, StatTupFetched, StatTupModified,
	StatDeadlocks,
}

// Role is which side of a replication pair produced a reading. It is part of the
// accumulator key because the same database has a different read profile on a standby, and
// it is a bounded enum so it is affordable as a label.
type Role string

const (
	RolePrimary Role = "primary"
	RoleReplica Role = "replica"
)

// Roles is every role, for the same reason Stats exists.
var Roles = [2]Role{RolePrimary, RoleReplica}

// DatabaseStats is one pg_stat_database row, aggregated by dbid and by nothing else.
//
// Per-query, per-user and per-relation breakdowns are all available in PostgreSQL and all
// deliberately unused: they would multiply the number of series held per tenant by a factor
// nobody can bound, and no placement or autoscaling decision reads them.
type DatabaseStats struct {
	// DatabaseOID is the dbid the row was aggregated by. It disambiguates a database that
	// was dropped and recreated under the same name.
	DatabaseOID int64
	// NumBackends is a level, not a counter, and so is never differenced.
	NumBackends int32
	// Counters holds the cumulative values as PostgreSQL reported them.
	Counters map[Stat]int64
	// StatsReset is pg_stat_database.stats_reset. A change in it is a reset even when every
	// counter happens to have gone up, because the new values are counting from a different
	// origin.
	StatsReset *time.Time
	// SizeBytes is pg_database_size and Relations is the count of pg_class entries the
	// tenant owns. Both are levels.
	SizeBytes int64
	Relations int32
}

// cursorKey identifies one cumulative counter source. The instance is part of it because
// the same database read from a primary and from a standby is two independent counter
// sequences, and differencing across them would produce nonsense.
type cursorKey struct {
	Key      Key
	Instance string
	Role     Role
	OID      int64
}

// cursor remembers the previous reading so the next one can be turned into a delta.
type cursor struct {
	counters   map[Stat]int64
	statsReset *time.Time
	seen       bool
}

// delta returns the non-negative change since the previous reading.
//
// Any decrease, and any change of stats_reset, means the source restarted its count: the
// new reading is then the whole delta, because it is what has accrued since the restart.
// This is what keeps a total monotonic across a pool object being freed, a database being
// recreated, or somebody calling pg_stat_reset().
func (c *cursor) delta(next DatabaseStats) map[Stat]int64 {
	deltas := make(map[Stat]int64, len(next.Counters))
	reset := !c.seen || !sameInstant(c.statsReset, next.StatsReset)
	for _, stat := range Stats {
		value, present := next.Counters[stat]
		if !present {
			continue
		}
		previous := c.counters[stat]
		switch {
		case reset || value < previous:
			deltas[stat] = value
		default:
			deltas[stat] = value - previous
		}
	}
	c.counters = make(map[Stat]int64, len(next.Counters))
	maps.Copy(c.counters, next.Counters)
	c.statsReset = next.StatsReset
	c.seen = true
	return deltas
}

func sameInstant(a, b *time.Time) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Equal(*b)
	}
}

// baseline records a reading without accruing any of it.
//
// It is what the very first observation of a source does. A counter read for the first time
// is as old as the postmaster that has been incrementing it, and none of that accrued while
// anybody here was watching; adding it would put the server's whole lifetime into the total
// on every operator restart and every leader election, and the dashboard's rate() over that
// counter would draw a spike that never happened.
//
// Deliberately not the same as a reset. A source that restarted its count, a database
// recreated under a new OID and a pool object that was freed have all genuinely accrued their
// whole value since they appeared - which is why the accumulator, not the cursor, is what
// tells the two situations apart.
func (c *cursor) baseline(next DatabaseStats) {
	c.counters = make(map[Stat]int64, len(next.Counters))
	maps.Copy(c.counters, next.Counters)
	c.statsReset = next.StatsReset
	c.seen = true
}
