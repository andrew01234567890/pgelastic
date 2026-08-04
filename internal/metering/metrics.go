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
	"github.com/prometheus/client_golang/prometheus"
)

// Label names, spelled once.
const (
	labelNamespace = "namespace"
	labelPool      = "pool"
	labelStat      = "stat"
	labelRole      = "role"
	labelStatistic = "statistic"
	labelState     = "state"
	// labelTenant appears on exactly one metric, and only when the pool has asked for it.
	// Every other metric in this file is unlabelled by tenant on purpose.
	labelTenant = "tenant"
)

// ConnectionStatistic and TenantState are closed label-value sets. Every value listed here
// costs one series per pool, and nothing outside these lists is ever emitted — which is the
// whole mechanism by which the exported series count is independent of tenant count.
type ConnectionStatistic string

const (
	ConnectionsInUse          ConnectionStatistic = "in_use"
	ConnectionsReserved       ConnectionStatistic = "reserved"
	ConnectionsAllocatable    ConnectionStatistic = "allocatable"
	ConnectionsCommittedBurst ConnectionStatistic = "committed_burst"
)

// connectionStatistics is the pool-level connection gauge's label values.
var connectionStatistics = []ConnectionStatistic{
	ConnectionsInUse, ConnectionsReserved, ConnectionsAllocatable, ConnectionsCommittedBurst,
}

// TenantStatistic aggregates the per-tenant recommenders. The per-tenant numbers themselves
// are published on each tenant's own status; what leaves as metrics is their sum and their
// maximum, which is what a dashboard needs and what an alert can be written against.
type TenantStatistic string

const (
	TenantP95Sum  TenantStatistic = "p95_sum"
	TenantP95Max  TenantStatistic = "p95_max"
	TenantPeakSum TenantStatistic = "peak_sum"
	TenantPeakMax TenantStatistic = "peak_max"
)

// tenantStatistics is the tenant-aggregate gauge's label values.
var tenantStatistics = []TenantStatistic{TenantP95Sum, TenantP95Max, TenantPeakSum, TenantPeakMax}

// TenantState counts the tenant population by a bounded set of states.
type TenantState string

const (
	TenantsTotal   TenantState = "total"
	TenantsBound   TenantState = "bound"
	TenantsPending TenantState = "pending"
	TenantsCold    TenantState = "cold"
	TenantsStale   TenantState = "stale"
)

// tenantStates is the population gauge's label values.
var tenantStates = []TenantState{TenantsTotal, TenantsBound, TenantsPending, TenantsCold, TenantsStale}

// SeriesPerPool is the exact number of Prometheus series this package emits for one pool,
// whether that pool has one tenant or a thousand.
//
// It is a documented bound rather than an emergent property: metering_cardinality_test.go
// gathers the registry at both extremes and fails if the count moves. The number is the sum
// of the closed label-value sets below.
const SeriesPerPool = len(Stats)*len(Roles) + // database_stats_total
	4 + // pool_connections
	4 + // tenant_connections
	5 + // tenants
	1 + // samples_total
	1 + // stale
	1 // tenant_series

// SeriesPerTenant is what one tenant adds on top of SeriesPerPool when the pool has turned
// spec.observability.perTenantMetrics on, and it is the whole price of that field: at the
// design point of ~200 tenants a pool goes from 32 series to 3,232.
//
// It is only the database counters that gain a tenant label. The rest of this file is
// pool-level facts - allocatable connections, the tenant population, the pool's staleness -
// which have no per-tenant reading to give, so labelling them by tenant would multiply the
// series count without adding a number anybody could read.
const SeriesPerTenant = len(Stats) * len(Roles)

// Metrics is the bounded exposition of everything this package meters.
//
// Every metric here is labelled by namespace and pool and by a closed enum, and by nothing
// else. There is deliberately no database, query or relation label anywhere, and no tenant
// label on any metric a pool has not explicitly asked for one on: at the design point of ~200
// tenants per pool a per-tenant label costs a hundredfold, and the per-tenant numbers are
// already published where an operator actually looks for them, on the tenant's own CR.
//
// tenantDatabaseStats is the single exception, and it is inert unless the pool sets
// spec.observability.perTenantMetrics. Nothing else in this struct gains a tenant label under
// that field, because nothing else in this struct has a per-tenant reading to give.
type Metrics struct {
	databaseStats       *prometheus.CounterVec
	tenantDatabaseStats *prometheus.CounterVec
	poolConns           *prometheus.GaugeVec
	tenantConns         *prometheus.GaugeVec
	tenants             *prometheus.GaugeVec
	samples             *prometheus.CounterVec
	stale               *prometheus.GaugeVec
	tenantSeries        *prometheus.GaugeVec
}

// NewMetrics builds the metric vectors and registers them. Taking a Registerer rather than
// reaching for the global one is what lets a test gather its own registry and count what
// this package actually emits.
func NewMetrics(registerer prometheus.Registerer) (*Metrics, error) {
	metrics := &Metrics{
		databaseStats: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgelastic_metering_database_stats_total",
			Help: "Cumulative pg_stat_database counters, summed over the pool's tenants. " +
				"Monotonic per tenant, database and role, so freeing an idle pool object " +
				"does not read as a counter reset.",
		}, []string{labelNamespace, labelPool, labelStat, labelRole}),

		tenantDatabaseStats: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgelastic_metering_tenant_database_stats_total",
			Help: "The same pg_stat_database counters broken down by tenant. Emitted only " +
				"for pools that set spec.observability.perTenantMetrics, because it costs " +
				"16 series per tenant rather than 16 per pool.",
		}, []string{labelNamespace, labelPool, labelTenant, labelStat, labelRole}),

		poolConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgelastic_metering_pool_connections",
			Help: "The pool's backend-connection ledger.",
		}, []string{labelNamespace, labelPool, labelStatistic}),

		tenantConns: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgelastic_metering_tenant_connections",
			Help: "Aggregate of the per-tenant trailing-window recommenders. Per-tenant " +
				"values are published on each PgTenant's status, never as a label here.",
		}, []string{labelNamespace, labelPool, labelStatistic}),

		tenants: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgelastic_metering_tenants",
			Help: "The pool's tenant population by state.",
		}, []string{labelNamespace, labelPool, labelState}),

		samples: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgelastic_metering_samples_total",
			Help: "Tenant observations recorded into the trailing-window store.",
		}, []string{labelNamespace, labelPool}),

		stale: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgelastic_metering_stale",
			Help: "1 while the pool's metrics are older than the autoscaler's staleness " +
				"threshold, which is the condition that forces the DoNothing fallback.",
		}, []string{labelNamespace, labelPool}),

		tenantSeries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgelastic_metering_tenant_series",
			Help: "Tenant histograms held in memory for this pool. This is the cardinality " +
				"that is deliberately kept out of the labels.",
		}, []string{labelNamespace, labelPool}),
	}

	for _, collector := range metrics.collectors() {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return metrics, nil
}

func (m *Metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.databaseStats, m.tenantDatabaseStats, m.poolConns, m.tenantConns, m.tenants,
		m.samples, m.stale, m.tenantSeries,
	}
}

// RegisterPool materialises every series for a pool up front.
//
// A metric that springs into existence the first time a tenant deadlocks cannot be alerted
// on, because absent and zero are different to every query language. Creating the whole
// closed label set at once also makes SeriesPerPool an assertable number rather than a
// number that depends on which events happened to occur.
func (m *Metrics) RegisterPool(namespace, pool string) {
	for _, stat := range Stats {
		for _, role := range Roles {
			m.databaseStats.WithLabelValues(namespace, pool, string(stat), string(role))
		}
	}
	for _, statistic := range connectionStatistics {
		m.poolConns.WithLabelValues(namespace, pool, string(statistic))
	}
	for _, statistic := range tenantStatistics {
		m.tenantConns.WithLabelValues(namespace, pool, string(statistic))
	}
	for _, state := range tenantStates {
		m.tenants.WithLabelValues(namespace, pool, string(state))
	}
	m.samples.WithLabelValues(namespace, pool)
	m.stale.WithLabelValues(namespace, pool)
	m.tenantSeries.WithLabelValues(namespace, pool)
}

// RegisterTenant materialises the per-tenant counters for one tenant, for the same reason
// RegisterPool materialises the pool's: a series that springs into existence the first time a
// tenant deadlocks cannot be alerted on, because absent and zero are different to every query
// language. It is called only while the pool has perTenantMetrics on.
func (m *Metrics) RegisterTenant(namespace, pool, tenant string) {
	for _, stat := range Stats {
		for _, role := range Roles {
			m.tenantDatabaseStats.WithLabelValues(
				namespace, pool, tenant, string(stat), string(role))
		}
	}
}

// AddTenantDatabaseStats adds one already-differenced, non-negative delta under the tenant's
// own label.
func (m *Metrics) AddTenantDatabaseStats(
	namespace, pool, tenant string,
	role Role,
	deltas map[Stat]int64,
) {
	for stat, delta := range deltas {
		if delta <= 0 {
			continue
		}
		m.tenantDatabaseStats.
			WithLabelValues(namespace, pool, tenant, string(stat), string(role)).
			Add(float64(delta))
	}
}

// ForgetTenant releases one tenant's per-tenant series, for a tenant that has left the pool.
func (m *Metrics) ForgetTenant(namespace, pool, tenant string) {
	m.tenantDatabaseStats.DeletePartialMatch(prometheus.Labels{
		labelNamespace: namespace, labelPool: pool, labelTenant: tenant,
	})
}

// ForgetTenantStats releases every per-tenant series a pool holds.
//
// It is what makes the field reversible. Turning perTenantMetrics off would otherwise leave
// the series that were already created behind for the life of the process - so the setting
// would take effect on the way up and not on the way down, which is the direction somebody
// flips it in when their Prometheus is falling over.
func (m *Metrics) ForgetTenantStats(namespace, pool string) {
	m.tenantDatabaseStats.DeletePartialMatch(prometheus.Labels{
		labelNamespace: namespace, labelPool: pool,
	})
}

// ForgetPool removes every series for a pool that no longer exists. It is the only path by
// which a counter here goes away, and it fires on the pool being deleted rather than on any
// tenant of it going idle.
func (m *Metrics) ForgetPool(namespace, pool string) {
	labels := prometheus.Labels{labelNamespace: namespace, labelPool: pool}
	m.databaseStats.DeletePartialMatch(labels)
	m.tenantDatabaseStats.DeletePartialMatch(labels)
	m.poolConns.DeletePartialMatch(labels)
	m.tenantConns.DeletePartialMatch(labels)
	m.tenants.DeletePartialMatch(labels)
	m.samples.DeletePartialMatch(labels)
	m.stale.DeletePartialMatch(labels)
	m.tenantSeries.DeletePartialMatch(labels)
}

// AddDatabaseStats adds one already-differenced, non-negative delta.
func (m *Metrics) AddDatabaseStats(namespace, pool string, role Role, deltas map[Stat]int64) {
	for stat, delta := range deltas {
		if delta <= 0 {
			continue
		}
		m.databaseStats.WithLabelValues(namespace, pool, string(stat), string(role)).Add(float64(delta))
	}
}

// SetPoolConnections publishes the pool's ledger.
func (m *Metrics) SetPoolConnections(namespace, pool string, statistic ConnectionStatistic, value float64) {
	m.poolConns.WithLabelValues(namespace, pool, string(statistic)).Set(value)
}

// SetTenantConnections publishes one aggregate of the per-tenant recommenders.
func (m *Metrics) SetTenantConnections(namespace, pool string, statistic TenantStatistic, value float64) {
	m.tenantConns.WithLabelValues(namespace, pool, string(statistic)).Set(value)
}

// SetTenants publishes one tenant-population count.
func (m *Metrics) SetTenants(namespace, pool string, state TenantState, value float64) {
	m.tenants.WithLabelValues(namespace, pool, string(state)).Set(value)
}

// AddSamples counts observations recorded.
func (m *Metrics) AddSamples(namespace, pool string, count int) {
	if count > 0 {
		m.samples.WithLabelValues(namespace, pool).Add(float64(count))
	}
}

// SetStale publishes whether the pool's metrics are too old to act on.
func (m *Metrics) SetStale(namespace, pool string, stale bool) {
	value := float64(0)
	if stale {
		value = 1
	}
	m.stale.WithLabelValues(namespace, pool).Set(value)
}

// SetTenantSeries publishes how many tenant histograms are held for the pool.
func (m *Metrics) SetTenantSeries(namespace, pool string, count int) {
	m.tenantSeries.WithLabelValues(namespace, pool).Set(float64(count))
}
