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

package bench

import (
	"fmt"
	"math"
	"slices"
)

// Reproducible asks whether repeating a measurement reproduces it.
//
// Every verdict this package issues rests on an unstated assumption: that running the same
// arm again would give the same answer. Repetitions test that inside one invocation and
// routinely agree to within 0-2%. Between invocations the same rig has been seen to drift
// 8-13% - wider than several of the gaps the criteria are asked to adjudicate, and wide
// enough that a 25% claim resting on two runs a day apart is not supported by them.
//
// The threshold is MaxP99SpreadRatio, reused rather than chosen. The drift is already
// measured, so any new number would be picked in view of the result it had to produce, which
// is the exact failure the pre-registration comment in criteria.go exists to prevent. Some
// rows will genuinely fail at 0.10. That is the finding, not a calibration error.
func Reproducible(axis Axis, reports []Report, concurrency int, of func(Point) Sample) Result {
	medians := make([]float64, 0, len(reports))
	runs := map[string]bool{}
	anonymous := 0
	for _, report := range reports {
		point, ok := byConcurrency(report)[concurrency]
		if !ok {
			continue
		}
		sample := of(point)
		if sample.Runs == 0 {
			continue
		}
		medians = append(medians, sample.Median)
		if report.RunID == "" {
			anonymous++
			continue
		}
		runs[report.RunID] = true
	}

	if len(medians) < 2 {
		return Result{
			Axis: axis, Concurrency: concurrency, Verdict: Inconclusive,
			Reason: fmt.Sprintf("%d invocations measured %d clients; reproducibility needs at least 2",
				len(medians), concurrency),
		}
	}
	// Anonymous reports predate run identity. They might be separate invocations or the same
	// one, and a reproducibility check that cannot tell has to say so rather than pick.
	if anonymous > 0 {
		return Result{
			Axis: axis, Concurrency: concurrency, Verdict: Inconclusive,
			Reason: fmt.Sprintf("%d of these reports carry no run id, so whether they are "+
				"separate invocations cannot be established", anonymous),
		}
	}
	if len(runs) < 2 {
		return Result{
			Axis: axis, Concurrency: concurrency, Verdict: Inconclusive,
			Reason: "these reports share a run id, so they are repetitions of one invocation " +
				"rather than separate ones, and cannot speak to drift between invocations",
		}
	}

	slices.Sort(medians)
	low, high := medians[0], medians[len(medians)-1]
	middle := median(medians)
	if middle == 0 {
		return Result{
			Axis: axis, Concurrency: concurrency, Verdict: Inconclusive,
			Reason: "the median across invocations is zero, so a spread ratio is undefined",
		}
	}

	spread := (high - low) / math.Abs(middle)
	result := Result{
		Axis: axis, Concurrency: concurrency, Ratio: spread,
		Reason: fmt.Sprintf("%d invocations at %d clients spread %.1f%% (%.0f to %.0f)",
			len(medians), concurrency, spread*100, low, high),
	}
	if spread > MaxP99SpreadRatio {
		result.Verdict = Fail
		result.Reason += fmt.Sprintf(", wider than the %.0f%% a repeated measurement is allowed "+
			"to move; a verdict drawn across these invocations would be reporting the rig",
			MaxP99SpreadRatio*100)
		return result
	}
	result.Verdict = Pass
	return result
}

// median of an already-sorted slice, averaging the middle pair when the count is even.
//
// Two invocations is the common case and the one an upper-middle "median" would get wrong: it
// would divide the spread by the larger of the two and quietly under-report the drift.
func median(sorted []float64) float64 {
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}
