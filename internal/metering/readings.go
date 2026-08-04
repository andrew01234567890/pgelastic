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

import "time"

// ReadingTTL is how old a pg_stat_database reading may be and still be folded into the
// round that finds it.
//
// The two sides run on different clocks: the instance controller records a reading whenever
// it polls a member, and the pool controller folds it whenever it reconciles the pool. This
// bridges them, and it has to be generous enough to span both cadences and mean enough to
// notice an instance that has stopped answering. Past it, the reading is treated as absent
// rather than replayed - which is what puts the tenant into the pool's stale count instead of
// silently reporting a level that has stopped moving as if it were current.
const ReadingTTL = 2 * time.Minute

// ReadingKey identifies one database's counters on one instance.
//
// The namespace is part of it because one operator serves every namespace in the cluster, and
// two tenants of two customers are entitled to name their instance and their database the
// same thing.
type ReadingKey struct {
	Namespace string
	Instance  string
	Database  string
}

type reading struct {
	stats      DatabaseStats
	observedAt time.Time
}

// RecordDatabaseStats stores one instance's whole scrape, replacing whatever it held before.
//
// Wholesale rather than per database, because a database that has been dropped has to stop
// being reported: a reading left behind would go on being folded as an unchanging level, and
// an unchanging level is exactly what a live-but-idle tenant looks like.
//
// It is deliberately not a second Observe. The readings arrive on the instance controller's
// cadence and are folded on the pool controller's, and folding them where they arrive would
// mean the pool's own gauges - its ledger, its tenant population - were written by whichever
// instance reported last.
func (c *Collector) RecordDatabaseStats(namespace, instance string, at time.Time, byDatabase map[string]DatabaseStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.readings {
		if key.Namespace == namespace && key.Instance == instance {
			delete(c.readings, key)
		}
	}
	if len(byDatabase) == 0 {
		return
	}
	if c.readings == nil {
		c.readings = map[ReadingKey]reading{}
	}
	for database, stats := range byDatabase {
		c.readings[ReadingKey{Namespace: namespace, Instance: instance, Database: database}] =
			reading{stats: stats, observedAt: at}
	}
}

// DatabaseStatsFor returns the reading for one database, if there is one and it is recent
// enough to describe this round.
func (c *Collector) DatabaseStatsFor(key ReadingKey, now time.Time) (DatabaseStats, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	held, ok := c.readings[key]
	if !ok || now.Sub(held.observedAt) > ReadingTTL {
		return DatabaseStats{}, false
	}
	return held.stats, true
}

// ForgetInstance drops every reading an instance left behind, for an instance that has been
// deleted. Without it the readings of a decommissioned instance are held until the process
// restarts, and the tenants that were on it keep a reading that describes a server that is
// gone.
func (c *Collector) ForgetInstance(namespace, instance string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.readings {
		if key.Namespace == namespace && key.Instance == instance {
			delete(c.readings, key)
		}
	}
}

// Readings is how many readings are held, which is what a test asserting the staging area is
// bounded by the databases that exist reads.
func (c *Collector) Readings() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.readings)
}
