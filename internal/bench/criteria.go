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

import "fmt"

// The pre-registered thresholds.
//
// Committed before the Go proxy exists, which is the only thing that makes them
// pre-registered. Their value is not that they are the right numbers - reasonable people
// would pick differently - but that they were fixed before anyone could see which way they
// would cut. Changing one after a result exists is a decision to stop measuring and start
// arguing, and the diff will say so.
const (
	// MinRatio is the floor for the axes where more is better: a Go proxy delivering at
	// least three quarters of the Rust proxy's rate clears the bar the user set, because a
	// single-language repository is worth a quarter of the throughput.
	MinRatio = 0.75
	// MaxRatio is the ceiling for the axes where less is better, the same 25% expressed the
	// other way round.
	MaxRatio = 1.25
	// MaxTailRatio gives p99.9 more room than p99. A garbage-collected runtime buys its
	// throughput with occasional pauses, and refusing it any extra tail slack would be
	// choosing the answer rather than measuring it.
	MaxTailRatio = 1.50

	// MaxP99SpreadRatio is Gate 0. Run-to-run p99 spread wider than this means the rig
	// cannot resolve the effect being measured, so no latency verdict from it is worth
	// anything.
	MaxP99SpreadRatio = 0.10

	// MinRuns is how many repetitions a sample needs before it is allowed to decide
	// anything. One run is an anecdote.
	MinRuns = 5

	// MinResolvableAddedMicros is the smallest proxy-added latency worth expressing as a
	// ratio. The proxy adds tens of microseconds; below this, the denominator is noise and
	// the quotient is a random number with a decimal point.
	MinResolvableAddedMicros = 20.0

	// MaxLoadGenCPU is the share of its own cores the load generator may burn before its
	// measurements describe the load generator rather than the proxy.
	MaxLoadGenCPU = 0.70
)

// Axis is one dimension of the comparison.
type Axis string

const (
	AxisChurn      Axis = "churn"
	AxisThroughput Axis = "throughput"
	AxisBulk       Axis = "bulk"
	AxisDensity    Axis = "density"
	AxisP99        Axis = "p99"
	AxisP999       Axis = "p99.9"
)

// Verdict is what a row of the results table says.
type Verdict string

const (
	Pass Verdict = "PASS"
	Fail Verdict = "FAIL"
	// Inconclusive is a first-class outcome, not a failure to produce one. A rig that cannot
	// resolve a difference should say so; reporting PASS or FAIL from noise is the specific
	// failure mode this whole package exists to prevent.
	Inconclusive Verdict = "INCONCLUSIVE"
	// VerdictReference is a measurement that was never a test. It carries a ratio against an
	// external implementation and no judgement, because that implementation is not doing the
	// same job and a threshold against it would be scoring two different programs.
	VerdictReference Verdict = "REFERENCE"
)

// Sample is one measured quantity across repetitions of the same cell.
//
// Median rather than mean because a descheduled vCPU produces outliers that a mean carries
// and a median does not. Min and Max are kept because the spread is what decides whether a
// difference between two samples is real.
type Sample struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Runs   int     `json:"runs"`
}

// Overlaps reports whether two samples' observed ranges intersect.
//
// When they do, the measurement did not separate them, and any statement that one is faster
// than the other is a statement about which run happened to land where.
func (s Sample) Overlaps(other Sample) bool {
	return s.Min <= other.Max && other.Min <= s.Max
}

// SpreadRatio is the observed range as a fraction of the median.
func (s Sample) SpreadRatio() float64 {
	if s.Median == 0 {
		return 0
	}
	return (s.Max - s.Min) / s.Median
}

// Result is one row of the decision table.
type Result struct {
	Axis      Axis    `json:"axis"`
	Verdict   Verdict `json:"verdict"`
	Ratio     float64 `json:"ratio"`
	Threshold float64 `json:"threshold"`
	Reason    string  `json:"reason"`
}

// HigherIsBetter judges an axis where Go must reach a fraction of Rust's number: churn rate,
// query throughput, bulk bytes per second.
func HigherIsBetter(axis Axis, rust, golang Sample) Result {
	result := Result{Axis: axis, Threshold: MinRatio}
	if reason, ok := undersampled(rust, golang); !ok {
		return inconclusive(result, reason)
	}
	if rust.Median == 0 {
		return inconclusive(result, "the Rust arm measured zero, so there is nothing to be a fraction of")
	}

	result.Ratio = golang.Median / rust.Median
	if result.Ratio >= MinRatio {
		return pass(result, fmt.Sprintf("Go reached %.1f%% of Rust", result.Ratio*100))
	}
	if rust.Overlaps(golang) {
		return inconclusive(result, fmt.Sprintf(
			"Go's median is %.1f%% of Rust's, but the run-to-run ranges overlap, so the shortfall is not resolved",
			result.Ratio*100))
	}
	return fail(result, fmt.Sprintf("Go reached %.1f%% of Rust, below the %.0f%% floor, with no overlap",
		result.Ratio*100, MinRatio*100))
}

// LowerIsBetter judges an axis where Go must stay under a multiple of Rust's number. Density
// is the one that matters: resident bytes per idle connection.
func LowerIsBetter(axis Axis, rust, golang Sample) Result {
	result := Result{Axis: axis, Threshold: MaxRatio}
	if reason, ok := undersampled(rust, golang); !ok {
		return inconclusive(result, reason)
	}
	if rust.Median == 0 {
		return inconclusive(result, "the Rust arm measured zero, so there is nothing to be a multiple of")
	}

	result.Ratio = golang.Median / rust.Median
	if result.Ratio <= MaxRatio {
		return pass(result, fmt.Sprintf("Go used %.2fx Rust", result.Ratio))
	}
	if rust.Overlaps(golang) {
		return inconclusive(result, fmt.Sprintf(
			"Go's median is %.2fx Rust's, but the run-to-run ranges overlap, so the excess is not resolved",
			result.Ratio))
	}
	return fail(result, fmt.Sprintf("Go used %.2fx Rust, above the %.2fx ceiling, with no overlap",
		result.Ratio, MaxRatio))
}

// AddedLatency judges a tail-latency percentile.
//
// Deliberately not a ratio of the two proxies' absolute latencies: almost all of that number
// is PostgreSQL, and dividing one by the other would compare the database to itself and
// report a reassuring 1.01x no matter how bad the proxy was. What is being compared is the
// latency each proxy *adds* over talking to PostgreSQL directly.
//
// Four separate ways this refuses to answer, because all four occur in practice:
//   - samples whose own run-to-run spread exceeds the Gate 0 ceiling, measured rather than
//     assumed from what kind of machine this is
//   - an added latency too small to divide, where the quotient is noise
//   - a negative added latency, which happens once client count exceeds pool size and the
//     pooler starts beating direct connections. That is the pooler working, not a
//     measurement error, and a ratio of a negative denominator is meaningless.
//   - too few repetitions to have a spread at all
//
// The spread gate replaced an earlier veto keyed on Rig, which refused every latency verdict
// on a virtualised machine. Measurement showed that too blunt: on the rig this was written
// for, p99 repeats to within 5-8% while p99.9 swings 14-19%, so a rig-wide veto discards a
// decidable axis to protect an undecidable one. Deciding on the observed spread keeps the
// same pre-registered ceiling and applies it to evidence instead of to a proxy for evidence.
// Rig stays in every result file as provenance.
func AddedLatency(rig Rig, axis Axis, direct, rust, golang Sample) Result {
	threshold := MaxRatio
	if axis == AxisP999 {
		threshold = MaxTailRatio
	}
	result := Result{Axis: axis, Threshold: threshold}

	if reason, ok := undersampled(direct, rust, golang); !ok {
		return inconclusive(result, reason)
	}
	if spread, ok := steady(rust, golang); !ok {
		return inconclusive(result, fmt.Sprintf(
			"run-to-run spread reaches %.1f%%, above the %.0f%% Gate 0 ceiling, on rig %q: "+
				"this cell cannot resolve a %.0f%% difference",
			spread*100, MaxP99SpreadRatio*100, rig, (threshold-1)*100))
	}

	addedRust := rust.Median - direct.Median
	addedGo := golang.Median - direct.Median

	if addedRust < 0 {
		return inconclusive(result, fmt.Sprintf(
			"the Rust proxy is %.1fus faster than a direct connection at this concurrency, which is the pooler doing its job; "+
				"added latency cannot be a denominator here", -addedRust))
	}
	if addedRust < MinResolvableAddedMicros {
		return inconclusive(result, fmt.Sprintf(
			"the Rust proxy adds only %.1fus, below the %.0fus this rig can resolve, so the ratio would be noise",
			addedRust, MinResolvableAddedMicros))
	}

	result.Ratio = addedGo / addedRust
	if result.Ratio <= threshold {
		return pass(result, fmt.Sprintf("Go adds %.1fus against Rust's %.1fus (%.2fx)", addedGo, addedRust, result.Ratio))
	}
	if rust.Overlaps(golang) {
		return inconclusive(result, fmt.Sprintf(
			"Go adds %.2fx Rust's latency, but the run-to-run ranges overlap, so the excess is not resolved", result.Ratio))
	}
	return fail(result, fmt.Sprintf("Go adds %.1fus against Rust's %.1fus (%.2fx), above the %.2fx ceiling",
		addedGo, addedRust, result.Ratio, threshold))
}

// NoiseFloor is Gate 0: whether this rig resolves latency finely enough to decide anything.
//
// Run before any Go code is written. A rig that fails it can still decide throughput,
// density and allocation counts; it just cannot decide tails, and the report says which.
func NoiseFloor(p99 Sample) Result {
	result := Result{Axis: AxisP99, Threshold: MaxP99SpreadRatio}
	if p99.Runs < MinRuns {
		return inconclusive(result, fmt.Sprintf("%d runs is fewer than the %d needed to see a spread", p99.Runs, MinRuns))
	}

	result.Ratio = p99.SpreadRatio()
	if result.Ratio <= MaxP99SpreadRatio {
		return pass(result, fmt.Sprintf("p99 spread is %.1f%% of the median", result.Ratio*100))
	}
	return fail(result, fmt.Sprintf(
		"p99 spread is %.1f%% of the median, above the %.0f%% Gate 0 ceiling: this rig cannot decide a 25%% tail question",
		result.Ratio*100, MaxP99SpreadRatio*100))
}

// LoadGenSaturated reports whether the load generator ran out of CPU, which makes every
// measurement in the cell a description of the load generator instead of the proxy.
func LoadGenSaturated(cpuShare float64) bool { return cpuShare > MaxLoadGenCPU }

// steady reports the worst run-to-run spread among the samples and whether it clears Gate 0.
//
// Applied to the arms being compared rather than to the direct baseline: the baseline sets
// the zero point, and a noisy zero point moves both arms together instead of separating them.
func steady(samples ...Sample) (float64, bool) {
	worst := 0.0
	for _, sample := range samples {
		if spread := sample.SpreadRatio(); spread > worst {
			worst = spread
		}
	}
	return worst, worst <= MaxP99SpreadRatio
}

func undersampled(samples ...Sample) (string, bool) {
	for _, sample := range samples {
		if sample.Runs < MinRuns {
			return fmt.Sprintf("only %d of the required %d runs", sample.Runs, MinRuns), false
		}
	}
	return "", true
}

func pass(result Result, reason string) Result {
	result.Verdict, result.Reason = Pass, reason
	return result
}

func fail(result Result, reason string) Result {
	result.Verdict, result.Reason = Fail, reason
	return result
}

func inconclusive(result Result, reason string) Result {
	result.Verdict, result.Reason = Inconclusive, reason
	return result
}
