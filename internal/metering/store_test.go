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

var epoch = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

func tenantKey(name string) Key {
	return Key{Namespace: "saas-prod", Pool: "saas-pool", Tenant: name}
}

func TestPercentileReportsTheBurstAndTheMeanWouldNot(t *testing.T) {
	store := NewStore(Options{HalfLife: 0})
	now := epoch

	for i := range 100 {
		connections := float64(1)
		if i%10 == 0 {
			connections = 100
		}
		store.Observe(tenantKey("acme"), Sample{BackendConnections: connections}, now)
		now = now.Add(time.Minute)
	}

	observation, ok := store.Observation(tenantKey("acme"), now)
	if !ok {
		t.Fatal("the tenant was observed a hundred times and the store has no series for it")
	}
	if observation.P95 < 100 {
		t.Errorf("p95 = %v, want at least the 100-connection burst that ten percent of samples reached", observation.P95)
	}
	if observation.Peak < 100 {
		t.Errorf("peak = %v, want 100", observation.Peak)
	}

	mean := (90*1.0 + 10*100.0) / 100
	if observation.P95 <= mean*2 {
		t.Errorf("p95 %v is indistinguishable from the mean %v, which is the failure this store exists to prevent",
			observation.P95, mean)
	}
}

func TestQuantileIsConservativeAtTheBucketBoundary(t *testing.T) {
	store := NewStore(Options{HalfLife: 0})
	for range 100 {
		store.Observe(tenantKey("acme"), Sample{BackendConnections: 40}, epoch)
	}

	observation, _ := store.Observation(tenantKey("acme"), epoch)
	if observation.P95 < 40 {
		t.Errorf("p95 = %v, want no less than the observed 40: under-stating demand overfills an instance",
			observation.P95)
	}
	if observation.P95 > 40*bucketRatio {
		t.Errorf("p95 = %v, want within one bucket of 40", observation.P95)
	}
}

func TestObservationsOutsideTheWindowAreDroppedNotDecayed(t *testing.T) {
	store := NewStore(Options{Window: 24 * time.Hour, Resolution: time.Hour, HalfLife: 0})

	store.Observe(tenantKey("acme"), Sample{BackendConnections: 500}, epoch)
	later := epoch.Add(48 * time.Hour)
	store.Observe(tenantKey("acme"), Sample{BackendConnections: 3}, later)

	observation, _ := store.Observation(tenantKey("acme"), later)
	if observation.Peak > 3 {
		t.Errorf("peak = %v two days after a 500-connection burst in a 24h window, want 3", observation.Peak)
	}
	if observation.P95 > 3*bucketRatio {
		t.Errorf("p95 = %v, want the burst outside the window to contribute nothing", observation.P95)
	}
}

// The half-life moves the body of the distribution, which is what a median shows and what a
// 95th percentile is built not to show. The peak must not move at all: it is the largest
// value in the window, and ageing it would make the store answer a question nobody asked.
func TestDecayWeightsRecentSlotsAbove(t *testing.T) {
	decayed := NewStore(Options{Window: 24 * time.Hour, Resolution: time.Hour, HalfLife: 2 * time.Hour})
	flat := NewStore(Options{Window: 24 * time.Hour, Resolution: time.Hour, HalfLife: 0})

	now := epoch
	for hour := range 20 {
		connections := float64(80)
		if hour >= 17 {
			connections = 2
		}
		for _, store := range []*Store{decayed, flat} {
			store.Observe(tenantKey("acme"), Sample{BackendConnections: connections}, now)
		}
		now = now.Add(time.Hour)
	}
	now = now.Add(-time.Hour)

	decayedMedian, _ := decayed.Quantile(tenantKey("acme"), 0.5, now)
	flatMedian, _ := flat.Quantile(tenantKey("acme"), 0.5, now)
	if decayedMedian >= flatMedian {
		t.Errorf("decayed median %v is not below the undecayed %v, so the half-life does nothing",
			decayedMedian, flatMedian)
	}

	decayedObservation, _ := decayed.Observation(tenantKey("acme"), now)
	flatObservation, _ := flat.Observation(tenantKey("acme"), now)
	if decayedObservation.Peak != flatObservation.Peak {
		t.Errorf("decayed peak %v differs from undecayed %v: the peak is the largest value in the window, "+
			"and weighting must not move it", decayedObservation.Peak, flatObservation.Peak)
	}
}

func TestCoversRequiresEvidenceSpanningTheWindow(t *testing.T) {
	store := NewStore(Options{})
	store.Observe(tenantKey("acme"), Sample{BackendConnections: 4}, epoch)
	store.Observe(tenantKey("acme"), Sample{BackendConnections: 4}, epoch.Add(100*time.Hour))

	observation, _ := store.Observation(tenantKey("acme"), epoch.Add(100*time.Hour))
	if observation.Covers(168 * time.Hour) {
		t.Error("100 hours of evidence claims to cover a 168-hour window, which would unlock scale-in early")
	}

	store.Observe(tenantKey("acme"), Sample{BackendConnections: 4}, epoch.Add(169*time.Hour))
	observation, _ = store.Observation(tenantKey("acme"), epoch.Add(169*time.Hour))
	if !observation.Covers(168 * time.Hour) {
		t.Error("169 hours of evidence does not cover a 168-hour window")
	}
}

func TestUnobservedTenantIsDistinguishableFromAnObservedZero(t *testing.T) {
	store := NewStore(Options{})
	if _, ok := store.Observation(tenantKey("never-seen"), epoch); ok {
		t.Error("a tenant that was never observed reports an observation")
	}

	store.Observe(tenantKey("idle"), Sample{BackendConnections: 0}, epoch)
	observation, ok := store.Observation(tenantKey("idle"), epoch)
	if !ok {
		t.Fatal("a tenant observed holding zero connections has no observation")
	}
	if observation.Samples != 1 {
		t.Errorf("samples = %d, want 1", observation.Samples)
	}
}

func TestPruneDropsSeriesWithNothingInTheWindow(t *testing.T) {
	store := NewStore(Options{Window: 24 * time.Hour, Resolution: time.Hour})
	store.Observe(tenantKey("gone"), Sample{BackendConnections: 1}, epoch)
	store.Observe(tenantKey("here"), Sample{BackendConnections: 1}, epoch.Add(30*time.Hour))

	if pruned := store.Prune(epoch.Add(30 * time.Hour)); pruned != 1 {
		t.Errorf("pruned %d series, want 1", pruned)
	}
	if store.Len() != 1 {
		t.Errorf("store holds %d series after pruning, want 1", store.Len())
	}
}

func TestQuantileHonoursTheRequestedPercentile(t *testing.T) {
	store := NewStore(Options{HalfLife: 0})
	for i := range 100 {
		connections := float64(2)
		if i >= 60 {
			connections = 50
		}
		store.Observe(tenantKey("acme"), Sample{BackendConnections: connections}, epoch)
	}

	p50, _ := store.Quantile(tenantKey("acme"), 0.50, epoch)
	p95, _ := store.Quantile(tenantKey("acme"), 0.95, epoch)
	if p50 > 3 {
		t.Errorf("p50 = %v, want the quiet majority near 2", p50)
	}
	if p95 < 50 {
		t.Errorf("p95 = %v, want the busy 40 percent near 50", p95)
	}
}
