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
	"sync"
	"time"
)

// PoolObservation is one round of pool-level ledger readings.
type PoolObservation struct {
	Namespace string
	Pool      string

	InUse          int32
	Reserved       int32
	Allocatable    int32
	CommittedBurst int32

	Bound   int32
	Pending int32
}

// TenantObservation is one round of readings for one tenant.
//
// The proxy side and the PostgreSQL side arrive together because they are two halves of the
// same fact: how many backend connections the tenant is holding, and what it did with them.
type TenantObservation struct {
	Key      Key
	Database string
	Instance string
	Role     Role

	// BackendConnections is the proxy-side reading, and the only one that gets a histogram.
	BackendConnections float64
	// PoolObjectFreed reports that the proxy released the tenant's pool object since the
	// last round, so the counters in Stats are counting from zero again.
	PoolObjectFreed bool
	// Stats is the PostgreSQL-side reading, aggregated by dbid. Nil when the instance did
	// not answer this round, which is a gap rather than a zero.
	Stats *DatabaseStats
	// Cold reports the tenant as below the pool's hot threshold for the whole window, which
	// is what makes it eligible to be moved.
	Cold bool
}

// Collector folds one round of observations into the store, the monotonic totals and the
// bounded metrics.
type Collector struct {
	Store       *Store
	Accumulator *Accumulator
	Metrics     *Metrics

	// pools records when each pool was last read at all, which is a different fact from when
	// its newest tenant sample was taken: a pool with no tenants is measurable and a pool
	// whose tenants have all stopped reporting is not.
	mu    sync.RWMutex
	pools map[poolKey]time.Time
	// readings is where the instance agents' pg_stat_database scrapes wait to be folded.
	// They arrive on the instance controller's cadence and are folded on the pool
	// controller's, so something has to hold them across the gap.
	readings map[ReadingKey]reading
}

type poolKey struct{ Namespace, Pool string }

// NewCollector wires a collector over a fresh store and accumulator. Metrics may be nil,
// which is what a unit test that only cares about the recommenders wants.
func NewCollector(options Options, metrics *Metrics) *Collector {
	return &Collector{
		Store:       NewStore(options),
		Accumulator: NewAccumulator(),
		Metrics:     metrics,
	}
}

// Observe records one round.
func (c *Collector) Observe(pool PoolObservation, tenants []TenantObservation, at time.Time) {
	c.mu.Lock()
	if c.pools == nil {
		c.pools = map[poolKey]time.Time{}
	}
	c.pools[poolKey{pool.Namespace, pool.Pool}] = at
	c.mu.Unlock()

	var p95Sum, p95Max, peakSum, peakMax float64
	cold, stale := 0, 0

	for _, tenant := range tenants {
		if tenant.PoolObjectFreed {
			c.Accumulator.PoolObjectFreed(TotalKey{Key: tenant.Key, Database: tenant.Database, Role: tenant.Role})
		}

		sample := Sample{BackendConnections: tenant.BackendConnections}
		if tenant.Stats != nil {
			sample.StorageBytes = tenant.Stats.SizeBytes
			sample.Relations = tenant.Stats.Relations

			total := TotalKey{Key: tenant.Key, Database: tenant.Database, Role: tenant.Role}
			applied := c.Accumulator.Observe(total, tenant.Instance, *tenant.Stats)
			if c.Metrics != nil {
				c.Metrics.AddDatabaseStats(pool.Namespace, pool.Pool, tenant.Role, applied)
			}
		} else {
			stale++
		}
		c.Store.Observe(tenant.Key, sample, at)

		if observation, ok := c.Store.Observation(tenant.Key, at); ok {
			p95Sum += observation.P95
			p95Max = max(p95Max, observation.P95)
			peakSum += observation.Peak
			peakMax = max(peakMax, observation.Peak)
		}
		if tenant.Cold {
			cold++
		}
	}

	if c.Metrics == nil {
		return
	}
	c.Metrics.SetPoolConnections(pool.Namespace, pool.Pool, ConnectionsInUse, float64(pool.InUse))
	c.Metrics.SetPoolConnections(pool.Namespace, pool.Pool, ConnectionsReserved, float64(pool.Reserved))
	c.Metrics.SetPoolConnections(pool.Namespace, pool.Pool, ConnectionsAllocatable, float64(pool.Allocatable))
	c.Metrics.SetPoolConnections(pool.Namespace, pool.Pool, ConnectionsCommittedBurst, float64(pool.CommittedBurst))

	c.Metrics.SetTenantConnections(pool.Namespace, pool.Pool, TenantP95Sum, p95Sum)
	c.Metrics.SetTenantConnections(pool.Namespace, pool.Pool, TenantP95Max, p95Max)
	c.Metrics.SetTenantConnections(pool.Namespace, pool.Pool, TenantPeakSum, peakSum)
	c.Metrics.SetTenantConnections(pool.Namespace, pool.Pool, TenantPeakMax, peakMax)

	c.Metrics.SetTenants(pool.Namespace, pool.Pool, TenantsTotal, float64(len(tenants)))
	c.Metrics.SetTenants(pool.Namespace, pool.Pool, TenantsBound, float64(pool.Bound))
	c.Metrics.SetTenants(pool.Namespace, pool.Pool, TenantsPending, float64(pool.Pending))
	c.Metrics.SetTenants(pool.Namespace, pool.Pool, TenantsCold, float64(cold))
	c.Metrics.SetTenants(pool.Namespace, pool.Pool, TenantsStale, float64(stale))

	c.Metrics.AddSamples(pool.Namespace, pool.Pool, len(tenants))
	c.Metrics.SetTenantSeries(pool.Namespace, pool.Pool, c.seriesFor(pool.Namespace, pool.Pool))
}

// seriesFor counts the tenant histograms held for one pool.
func (c *Collector) seriesFor(namespace, pool string) int {
	count := 0
	for _, key := range c.Store.Keys() {
		if key.Namespace == namespace && key.Pool == pool {
			count++
		}
	}
	return count
}

// Age reports how stale the pool's readings are, and whether there are any.
//
// It is the worse of two ages: how long since the collector last read the pool at all, and
// how long since its most recently sampled tenant reported. Taking the worse of the two is
// what makes both failure modes visible — a collector that has stopped running, and a
// collector that runs every minute and is handed nothing by the data plane.
//
// The autoscaler's stale-metric fallback is driven from this, so it must be read before the
// current round is folded in; afterwards every age is zero by construction.
func (c *Collector) Age(namespace, pool string, now time.Time) (time.Duration, bool) {
	c.mu.RLock()
	observedAt, observed := c.pools[poolKey{namespace, pool}]
	c.mu.RUnlock()

	age := time.Duration(0)
	seen := false
	if observed {
		age = now.Sub(observedAt)
		seen = true
	}
	for _, key := range c.Store.Keys() {
		if key.Namespace != namespace || key.Pool != pool {
			continue
		}
		observation, ok := c.Store.Observation(key, now)
		if !ok {
			continue
		}
		age = max(age, now.Sub(observation.LastSampleAt))
		seen = true
	}
	if !seen {
		return 0, false
	}
	return age, true
}

// ForgetPool drops everything held for a pool that no longer exists: its series, its
// retained totals and its metrics. It is the only path by which a counter here goes away,
// and it fires on the pool being deleted rather than on any tenant of it going idle.
func (c *Collector) ForgetPool(namespace, pool string) {
	c.mu.Lock()
	delete(c.pools, poolKey{namespace, pool})
	c.mu.Unlock()

	for _, key := range c.Store.Keys() {
		if key.Namespace == namespace && key.Pool == pool {
			c.Store.Forget(key)
		}
	}
	if c.Metrics != nil {
		c.Metrics.ForgetPool(namespace, pool)
	}
}

// ForgetDeparted drops the series of every tenant of a pool that is not in present.
//
// Age is the worst age across the pool's series, so a tenant that has been deleted keeps
// answering with the moment it was last sampled and drags the whole pool stale — which
// refuses every autoscaling action, including the storage expansion that stops a filling
// volume. Prune eventually removes it, but eventually is the retention window, and a week
// of refused autoscaling is not a recovery.
func (c *Collector) ForgetDeparted(namespace, pool string, present []Key) {
	live := make(map[Key]struct{}, len(present))
	for _, key := range present {
		live[key] = struct{}{}
	}
	for _, key := range c.Store.Keys() {
		if key.Namespace != namespace || key.Pool != pool {
			continue
		}
		if _, ok := live[key]; !ok {
			c.Store.Forget(key)
		}
	}
}

// Forget drops every trace of one tenant.
func (c *Collector) Forget(key Key, database string) {
	c.Store.Forget(key)
	for _, role := range Roles {
		c.Accumulator.Forget(TotalKey{Key: key, Database: database, Role: role})
	}
}
