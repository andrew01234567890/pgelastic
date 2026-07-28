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
	"math"
	"sync"
	"time"
)

// Store defaults. The window is the trailing seven days every placement and rebalance
// policy in the API defaults to; the resolution is the granularity a slot of that window is
// kept at; the half-life is how fast an old slot's contribution fades.
const (
	DefaultWindow     = 168 * time.Hour
	DefaultResolution = 4 * time.Hour
	DefaultHalfLife   = 24 * time.Hour
)

// Key identifies one metered tenant. Nothing below the tenant is a key: per-database and
// per-role detail is folded into the tenant's series, because a key that carried them would
// multiply the number of histograms held per tenant for no decision anybody makes.
type Key struct {
	Namespace string
	Pool      string
	Tenant    string
}

// Sample is one observation of a tenant at one instant.
type Sample struct {
	// BackendConnections is what the proxy reports the tenant holding. It is the capacity
	// unit, so it is the only field that gets a histogram.
	BackendConnections float64
	// StorageBytes is the tenant database's allocated size, carried as a level rather than
	// a distribution: nothing decides on the 95th percentile of a monotone quantity.
	StorageBytes int64
	// Relations is the tenant's relation count, which bounds catalogue and autovacuum cost
	// on the instance and is a packing dimension in its own right.
	Relations int32
}

// Options configures a Store.
type Options struct {
	// Window is the trailing period a percentile and a peak are computed over. Data older
	// than this is dropped outright rather than decayed to insignificance, so "p95 of a
	// trailing 7-day window" means exactly that.
	Window time.Duration
	// Resolution is the width of one slot of the window.
	Resolution time.Duration
	// HalfLife is how long it takes a slot's weight to halve. Zero weights every slot in
	// the window equally.
	HalfLife time.Duration
}

func (o Options) withDefaults() Options {
	if o.Window <= 0 {
		o.Window = DefaultWindow
	}
	if o.Resolution <= 0 {
		o.Resolution = DefaultResolution
	}
	if o.Resolution > o.Window {
		o.Resolution = o.Window
	}
	if o.HalfLife < 0 {
		o.HalfLife = 0
	}
	return o
}

// slots is how many ring entries the window is divided into.
func (o Options) slots() int {
	return max(int(o.Window/o.Resolution), 1)
}

// slot is one resolution-wide bucket of the ring.
type slot struct {
	// ordinal is the slot's position on the global timeline, which is what makes reuse
	// self-describing: a ring entry whose ordinal is not the one being asked for holds data
	// from a previous lap and is stale by definition.
	ordinal int64
	hist    histogram
	peak    float64
}

// series is one tenant's history.
type series struct {
	slots []slot

	current       float64
	storageBytes  int64
	relations     int32
	samples       int64
	firstSampleAt time.Time
	lastSampleAt  time.Time
}

// Observation is what a recommender reads back out of the store.
type Observation struct {
	// P95 is the packing statistic: the 95th percentile of held connections over the
	// window. Placement packs on this and never on the mean, because the mean of a bursty
	// tenant is a number no instance ever actually has to serve.
	P95 float64
	// Peak is the largest single observation still inside the window.
	Peak float64
	// Current is the most recent observation.
	Current float64
	// StorageBytes and Relations are the most recent levels.
	StorageBytes int64
	Relations    int32
	// Samples counts observations recorded for this tenant, over all time rather than over
	// the window: it is how a caller tells "quiet" from "never measured".
	Samples int64
	// FirstSampleAt and LastSampleAt bound the evidence. Scale-in is gated on the first,
	// staleness on the second.
	FirstSampleAt time.Time
	LastSampleAt  time.Time
}

// Covers reports whether the evidence spans at least the given duration, which is the
// question the 168-hour scale-in gate asks.
func (o Observation) Covers(window time.Duration) bool {
	return o.Samples > 0 && !o.FirstSampleAt.IsZero() &&
		o.LastSampleAt.Sub(o.FirstSampleAt) >= window
}

// Store keeps one decayed histogram ring per tenant.
//
// It is safe for concurrent use: the pool controller writes it from its reconcile loop
// while the tenant controller reads recommendations out of it from another.
type Store struct {
	mu      sync.RWMutex
	options Options
	series  map[Key]*series
}

// NewStore returns a store with the given options, defaulted.
func NewStore(options Options) *Store {
	return &Store{options: options.withDefaults(), series: map[Key]*series{}}
}

// Options reports the effective options, after defaulting.
func (s *Store) Options() Options { return s.options }

// Observe records one sample for one tenant.
func (s *Store) Observe(key Key, sample Sample, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.series[key]
	if entry == nil {
		entry = &series{slots: make([]slot, s.options.slots()), firstSampleAt: at}
		for i := range entry.slots {
			entry.slots[i].ordinal = -1
		}
		s.series[key] = entry
	}

	ordinal := s.ordinalOf(at)
	target := &entry.slots[ringIndex(ordinal, len(entry.slots))]
	if target.ordinal != ordinal {
		target.ordinal = ordinal
		target.hist.reset()
		target.peak = 0
	}
	target.hist.add(sample.BackendConnections, 1)
	target.peak = max(target.peak, sample.BackendConnections)

	entry.current = sample.BackendConnections
	entry.storageBytes = sample.StorageBytes
	entry.relations = sample.Relations
	entry.samples++
	if entry.firstSampleAt.IsZero() || at.Before(entry.firstSampleAt) {
		entry.firstSampleAt = at
	}
	if at.After(entry.lastSampleAt) {
		entry.lastSampleAt = at
	}
}

// Observation reports the recommenders for one tenant as of now. The second return is false
// when the tenant has never been observed, which is a different fact from an observed zero
// and drives a different decision.
func (s *Store) Observation(key Key, now time.Time) (Observation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.series[key]
	if entry == nil {
		return Observation{}, false
	}
	return s.observationOf(entry, now), true
}

// Quantile reports an arbitrary quantile of a tenant's window, for the pool policies that
// pack on something other than the 95th percentile.
func (s *Store) Quantile(key Key, q float64, now time.Time) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.series[key]
	if entry == nil {
		return 0, false
	}
	windowed, _ := s.window(entry, now)
	return windowed.quantile(q), true
}

// Len is how many tenant series are held. It is published as a single gauge per pool, which
// is the honest way to expose a cardinality that is deliberately not in the labels.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.series)
}

// Keys returns every key currently held.
func (s *Store) Keys() []Key {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]Key, 0, len(s.series))
	for key := range s.series {
		keys = append(keys, key)
	}
	return keys
}

// Forget drops one tenant's history, for a tenant that has been deleted.
func (s *Store) Forget(key Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.series, key)
}

// Prune drops every series with no observation inside the window ending at now. Without it
// a store grows with every tenant that has ever existed rather than with the tenants that
// exist, which is the same unbounded growth in memory that the label bound prevents in
// Prometheus.
func (s *Store) Prune(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	pruned := 0
	for key, entry := range s.series {
		if now.Sub(entry.lastSampleAt) > s.options.Window {
			delete(s.series, key)
			pruned++
		}
	}
	return pruned
}

func (s *Store) ordinalOf(at time.Time) int64 {
	return at.UnixNano() / int64(s.options.Resolution)
}

// ringIndex maps a timeline ordinal onto a ring position, for ordinals either side of the
// epoch.
func ringIndex(ordinal int64, size int) int {
	index := ordinal % int64(size)
	if index < 0 {
		index += int64(size)
	}
	return int(index)
}

// window sums the live slots into one histogram, each weighted by its age, and returns the
// peak alongside.
func (s *Store) window(entry *series, now time.Time) (histogram, float64) {
	newest := s.ordinalOf(now)
	oldest := newest - int64(len(entry.slots)) + 1

	var summed histogram
	peak := float64(0)
	for i := range entry.slots {
		slot := &entry.slots[i]
		if slot.ordinal < oldest || slot.ordinal > newest {
			continue
		}
		age := time.Duration(newest-slot.ordinal) * s.options.Resolution
		summed.addScaled(&slot.hist, decayWeight(age, s.options.HalfLife))
		peak = max(peak, slot.peak)
	}
	return summed, peak
}

func (s *Store) observationOf(entry *series, now time.Time) Observation {
	windowed, peak := s.window(entry, now)
	return Observation{
		P95:           windowed.quantile(0.95),
		Peak:          peak,
		Current:       entry.current,
		StorageBytes:  entry.storageBytes,
		Relations:     entry.relations,
		Samples:       entry.samples,
		FirstSampleAt: entry.firstSampleAt,
		LastSampleAt:  entry.lastSampleAt,
	}
}

// decayWeight is 2^(-age/halfLife), or 1 when decay is disabled.
func decayWeight(age, halfLife time.Duration) float64 {
	if halfLife <= 0 || age <= 0 {
		return 1
	}
	return math.Exp2(-age.Seconds() / halfLife.Seconds())
}
