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

// Package metering turns what the proxy and PostgreSQL report about a tenant into the two
// numbers placement and autoscaling are allowed to act on: a trailing-window percentile and
// a trailing-window peak.
//
// Two constraints shape everything here.
//
// The first is cardinality. Around two hundred tenants must not become two hundred times N
// Prometheus series, so per-tenant history lives in this package's in-memory store and is
// published per tenant only onto the tenant's own CR status. What leaves the process as
// metrics is aggregated to a fixed, enumerated set of series per pool — see SeriesPerPool.
//
// The second is monotonicity. The counters a proxy reports are per pool object, and a pool
// object is freed when it goes idle; the next one starts counting from zero. A collector
// that published the pool object's own counter would emit a decrease, which Prometheus reads
// as a process restart and turns into a spurious spike. Accumulator therefore holds the
// retained total per (tenant, database, role) and only ever adds non-negative deltas.
package metering

import "math"

// The histogram's buckets are exponential, in the shape VPA uses: the resolution that
// matters is relative, because the difference between 2 and 3 held connections is a
// different kind of fact from the difference between 200 and 201.
const (
	// bucketCount covers 0 up to firstBucketBound * bucketRatio^(bucketCount-1), which at
	// the ratio below is a little over twelve thousand connections.
	bucketCount = 100
	// bucketRatio is the growth factor between consecutive bucket upper bounds, and so
	// also the worst-case relative over-estimate of any percentile read out of it.
	bucketRatio = 1.10
	// firstBucketBound is the upper bound of bucket zero. Everything at or below one
	// connection is the same fact.
	firstBucketBound = 1.0
)

// bucketBounds holds each bucket's inclusive upper bound. The last bucket saturates: a
// value above the final bound is counted there and reported as that bound, which
// under-reports rather than fabricating a number, and the bound is high enough that
// reaching it means the connection model has already been violated.
var bucketBounds = func() [bucketCount]float64 {
	var bounds [bucketCount]float64
	bound := firstBucketBound
	for i := range bounds {
		bounds[i] = bound
		bound *= bucketRatio
	}
	return bounds
}()

// bucketFor maps a value onto the index of the bucket whose upper bound first covers it.
func bucketFor(value float64) int {
	if value <= firstBucketBound || math.IsNaN(value) {
		return 0
	}
	index := int(math.Ceil(math.Log(value) / math.Log(bucketRatio)))
	if index >= bucketCount {
		return bucketCount - 1
	}
	return index
}

// histogram is a weighted bucket count. Weights are float32 because a store holding a
// thousand tenants keeps one histogram per tenant per slot, and the precision of a decayed
// sample weight is not what limits the accuracy of a percentile estimated from 64 buckets.
type histogram struct {
	weights [bucketCount]float32
}

// add records one observation.
func (h *histogram) add(value float64, weight float64) {
	h.weights[bucketFor(value)] += float32(weight)
}

// reset empties the histogram so a ring slot can be reused without allocating.
func (h *histogram) reset() {
	h.weights = [bucketCount]float32{}
}

// addScaled folds another histogram in, multiplied by factor. It is how a trailing window
// is summed out of its slots with each slot weighted by its age.
func (h *histogram) addScaled(other *histogram, factor float64) {
	scale := float32(factor)
	for i := range h.weights {
		h.weights[i] += other.weights[i] * scale
	}
}

// total is the summed weight.
func (h *histogram) total() float64 {
	sum := float64(0)
	for _, weight := range h.weights {
		sum += float64(weight)
	}
	return sum
}

// quantile estimates the q-th quantile and returns the covering bucket's upper bound.
//
// Returning the upper bound rather than the lower one is deliberate. This number is fed to
// a bin packer, and a percentile that under-states demand packs a tenant onto an instance
// that cannot hold it; one that over-states it wastes a few connections.
func (h *histogram) quantile(q float64) float64 {
	total := h.total()
	if total == 0 {
		return 0
	}
	target := q * total
	cumulative := float64(0)
	for i, weight := range h.weights {
		cumulative += float64(weight)
		if cumulative >= target {
			return bucketBounds[i]
		}
	}
	return bucketBounds[bucketCount-1]
}
