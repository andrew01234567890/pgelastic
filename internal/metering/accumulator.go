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

import "sync"

// TotalKey identifies one monotonic total. It is finer than the histogram store's Key
// because monotonicity is claimed per (tenant, database, role) — a tenant whose database is
// recreated, or whose reads move to a standby, must not make any total go backwards.
type TotalKey struct {
	Key      Key
	Database string
	Role     Role
}

// Accumulator holds the retained total for every (tenant, database, role) and the last
// cumulative value each source reported, so a source that restarts its count is folded in
// rather than subtracted.
//
// It deliberately outlives the pool objects it meters. The proxy frees a per-tenant pool
// object when the tenant goes idle and the next one starts counting at zero; an accumulator
// that lived and died with the pool object would publish a decrease, and a decreasing
// counter is a reset to every consumer downstream.
type Accumulator struct {
	mu      sync.Mutex
	totals  map[TotalKey]map[Stat]int64
	cursors map[cursorKey]*cursor
	// observing records that this process has read this counter source before, and it
	// deliberately outlives the cursors. The three cases it separates look identical from a
	// missing cursor alone: a database recreated under a new OID and a pool object that was
	// freed are both new sources whose whole value has genuinely accrued since, while an
	// operator that has only just started observing is looking at a counter as old as the
	// postmaster and has accrued none of it.
	observing map[observerKey]bool
}

// observerKey is one counter source without its OID: the thing that stays the same when a
// database is dropped and recreated underneath it.
type observerKey struct {
	Key      Key
	Instance string
	Role     Role
}

// NewAccumulator returns an empty accumulator.
func NewAccumulator() *Accumulator {
	return &Accumulator{
		totals:    map[TotalKey]map[Stat]int64{},
		cursors:   map[cursorKey]*cursor{},
		observing: map[observerKey]bool{},
	}
}

// Add folds an already-differenced, non-negative delta into a total. Negative entries are
// dropped rather than clamped at the total, because a caller that computed a negative delta
// has a reset it failed to detect and hiding it inside a max() would make that invisible.
func (a *Accumulator) Add(key TotalKey, deltas map[Stat]int64) map[Stat]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.addLocked(key, deltas)
}

func (a *Accumulator) addLocked(key TotalKey, deltas map[Stat]int64) map[Stat]int64 {
	applied := make(map[Stat]int64, len(deltas))
	totals := a.totals[key]
	if totals == nil {
		totals = map[Stat]int64{}
		a.totals[key] = totals
	}
	for _, stat := range Stats {
		delta, present := deltas[stat]
		if !present || delta <= 0 {
			continue
		}
		totals[stat] += delta
		applied[stat] = delta
	}
	return applied
}

// Observe turns one cumulative reading from one source into a delta and folds it into the
// total for the (tenant, database, role) it belongs to. It returns the delta that was
// applied, which is what a metrics exporter adds to its counters.
func (a *Accumulator) Observe(key TotalKey, instance string, stats DatabaseStats) map[Stat]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	source := cursorKey{Key: key.Key, Instance: instance, Role: key.Role, OID: stats.DatabaseOID}
	observer := observerKey{Key: key.Key, Instance: instance, Role: key.Role}
	// The first reading this process ever takes of a source is a baseline, not an accrual.
	// Everything after it - a new OID, a freed pool object - is a genuinely new counter
	// sequence whose value really did accrue since it appeared.
	firstEver := !a.observing[observer]
	a.observing[observer] = true

	entry := a.cursors[source]
	if entry == nil {
		entry = &cursor{}
		a.cursors[source] = entry
	}
	if firstEver {
		entry.baseline(stats)
		return a.addLocked(key, nil)
	}
	return a.addLocked(key, entry.delta(stats))
}

// PoolObjectFreed records that the counter source behind this key has gone away — an idle
// pool object reclaimed, a database connection closed, a standby that stopped answering.
// The retained totals survive; only the differencing cursors are dropped, so the next
// reading is treated as a fresh count from zero rather than as a decrease.
func (a *Accumulator) PoolObjectFreed(key TotalKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for source := range a.cursors {
		if source.Key == key.Key && source.Role == key.Role {
			delete(a.cursors, source)
		}
	}
}

// Total reports the retained total for one key and stat.
func (a *Accumulator) Total(key TotalKey, stat Stat) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.totals[key][stat]
}

// Len is how many (tenant, database, role) totals are retained.
func (a *Accumulator) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.totals)
}

// Forget drops a key entirely, for a tenant that has been deleted. It is the only operation
// that may make a total disappear, and it exists so that retained state is bounded by the
// tenants that exist rather than by the tenants that ever existed.
func (a *Accumulator) Forget(key TotalKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.totals, key)
	for source := range a.cursors {
		if source.Key == key.Key && source.Role == key.Role {
			delete(a.cursors, source)
		}
	}
	for observer := range a.observing {
		if observer.Key == key.Key && observer.Role == key.Role {
			delete(a.observing, observer)
		}
	}
}
