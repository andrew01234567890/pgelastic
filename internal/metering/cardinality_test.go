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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// gatheredSeries counts the time series a registry would expose, which is the number that
// actually costs money in Prometheus: one per metric name per distinct label set.
func gatheredSeries(t *testing.T, registry *prometheus.Registry) int {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gathering the registry: %v", err)
	}
	total := 0
	for _, family := range families {
		total += len(family.GetMetric())
	}
	return total
}

// labelNames collects every label name in use, so a per-tenant label added by accident is
// named in the failure rather than only showing up as a larger number.
func labelNames(t *testing.T, registry *prometheus.Registry) []string {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gathering the registry: %v", err)
	}
	seen := map[string]struct{}{}
	names := []string{}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if _, ok := seen[pair.GetName()]; !ok {
					seen[pair.GetName()] = struct{}{}
					names = append(names, pair.GetName())
				}
			}
		}
	}
	return names
}

// meterTenants runs one full round of observations for a pool of the given size.
func meterTenants(t *testing.T, collector *Collector, namespace, pool string, tenants int) {
	t.Helper()
	observations := make([]TenantObservation, 0, tenants)
	for i := range tenants {
		name := fmt.Sprintf("tenant-%03d", i)
		stats := DatabaseStats{
			DatabaseOID: int64(20000 + i),
			Counters: map[Stat]int64{
				StatXactCommit:  int64(100 * (i + 1)),
				StatBlksHit:     int64(9000 * (i + 1)),
				StatTupReturned: int64(42 * (i + 1)),
				StatDeadlocks:   int64(i % 3),
			},
			SizeBytes: int64(i) * (1 << 30),
			Relations: int32(20 + i),
		}
		observations = append(observations, TenantObservation{
			Key:                Key{Namespace: namespace, Pool: pool, Tenant: name},
			Database:           name,
			Instance:           fmt.Sprintf("pg-%d", i%3),
			Role:               RolePrimary,
			BackendConnections: float64(i%17 + 1),
			Stats:              &stats,
			Cold:               i%2 == 0,
		})
	}
	collector.Observe(PoolObservation{
		Namespace:   namespace,
		Pool:        pool,
		InUse:       int32(tenants),
		Allocatable: 675,
		Bound:       int32(tenants),
	}, observations, epoch)
}

// This is the hard requirement of M8 stated as a test: the exported series count is a
// property of the pool, not of the tenant population. Two hundred tenants must cost exactly
// what one tenant costs.
func TestExportedSeriesDoNotScaleWithTenantCount(t *testing.T) {
	measure := func(tenants int) (int, []string) {
		registry := prometheus.NewRegistry()
		metrics, err := NewMetrics(registry)
		if err != nil {
			t.Fatalf("registering metrics: %v", err)
		}
		metrics.RegisterPool("saas-prod", "saas-pool")
		collector := &Collector{Store: NewStore(Options{}), Accumulator: NewAccumulator(), Metrics: metrics}
		meterTenants(t, collector, "saas-prod", "saas-pool", tenants)
		return gatheredSeries(t, registry), labelNames(t, registry)
	}

	one, oneLabels := measure(1)
	many, manyLabels := measure(200)

	if one != SeriesPerPool {
		t.Errorf("one tenant emits %d series, want the documented bound of %d", one, SeriesPerPool)
	}
	if many != one {
		t.Errorf("200 tenants emit %d series and one tenant emits %d: cardinality scales with tenant count, "+
			"which is exactly what this package exists to prevent", many, one)
	}
	if strings.Join(manyLabels, ",") != strings.Join(oneLabels, ",") {
		t.Errorf("label names differ between 1 tenant (%v) and 200 tenants (%v)", oneLabels, manyLabels)
	}
	for _, name := range manyLabels {
		switch name {
		case "tenant", "database", "dbid", "datname", "query", "queryid", "relation", "user", "usename":
			t.Errorf("label %q is per-tenant or finer and would multiply every series by the tenant count", name)
		}
	}
}

func TestSeriesPerPoolIsPerPoolAndNotPerNamespace(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("registering metrics: %v", err)
	}
	collector := &Collector{Store: NewStore(Options{}), Accumulator: NewAccumulator(), Metrics: metrics}

	const pools = 4
	for i := range pools {
		namespace := fmt.Sprintf("ns-%d", i)
		metrics.RegisterPool(namespace, "saas-pool")
		meterTenants(t, collector, namespace, "saas-pool", 50)
	}

	if got := gatheredSeries(t, registry); got != pools*SeriesPerPool {
		t.Errorf("%d pools of 50 tenants emit %d series, want %d", pools, got, pools*SeriesPerPool)
	}
}

func TestForgettingAPoolReleasesExactlyItsSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewMetrics(registry)
	if err != nil {
		t.Fatalf("registering metrics: %v", err)
	}
	collector := &Collector{Store: NewStore(Options{}), Accumulator: NewAccumulator(), Metrics: metrics}

	metrics.RegisterPool("saas-prod", "kept")
	metrics.RegisterPool("saas-prod", "removed")
	meterTenants(t, collector, "saas-prod", "kept", 10)
	meterTenants(t, collector, "saas-prod", "removed", 10)

	metrics.ForgetPool("saas-prod", "removed")
	if got := gatheredSeries(t, registry); got != SeriesPerPool {
		t.Errorf("after forgetting one of two pools the registry holds %d series, want %d", got, SeriesPerPool)
	}
}

// The store is where per-tenant detail is allowed to live, and it is bounded by the tenants
// that exist rather than by the tenants that ever existed.
func TestStoreCardinalityIsBoundedByLivingTenants(t *testing.T) {
	collector := NewCollector(Options{Window: 24 * time.Hour, Resolution: time.Hour}, nil)

	meterTenants(t, collector, "saas-prod", "saas-pool", 200)
	if collector.Store.Len() != 200 {
		t.Fatalf("store holds %d series, want 200", collector.Store.Len())
	}
	if collector.Accumulator.Len() != 200 {
		t.Fatalf("accumulator holds %d totals, want 200", collector.Accumulator.Len())
	}

	collector.Store.Prune(epoch.Add(72 * time.Hour))
	if collector.Store.Len() != 0 {
		t.Errorf("store holds %d series three days after the last observation, want 0", collector.Store.Len())
	}
}
