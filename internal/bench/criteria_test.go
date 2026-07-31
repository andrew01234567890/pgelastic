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

import "testing"

func sample(median, minimum, maximum float64) Sample {
	return Sample{Median: median, Min: minimum, Max: maximum, Runs: MinRuns}
}

func TestHigherIsBetterPassesWhenGoClearsThreeQuarters(t *testing.T) {
	rust := sample(1000, 980, 1020)
	golang := sample(800, 780, 820)

	got := HigherIsBetter(AxisThroughput, rust, golang)

	if got.Verdict != Pass {
		t.Fatalf("80%% of Rust should pass a 75%% floor, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestHigherIsBetterFailsOnlyWhenTheShortfallIsResolved(t *testing.T) {
	rust := sample(1000, 990, 1010)
	golang := sample(500, 490, 510)

	got := HigherIsBetter(AxisChurn, rust, golang)

	if got.Verdict != Fail {
		t.Fatalf("half of Rust with disjoint ranges should fail, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestHigherIsBetterWithdrawsWhenRangesOverlap(t *testing.T) {
	rust := sample(1000, 400, 1600)
	golang := sample(500, 200, 1400)

	got := HigherIsBetter(AxisChurn, rust, golang)

	if got.Verdict != Inconclusive {
		t.Fatalf("overlapping ranges cannot support a failure, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestHigherIsBetterRefusesASingleRun(t *testing.T) {
	rust := Sample{Median: 1000, Min: 1000, Max: 1000, Runs: 1}
	golang := Sample{Median: 900, Min: 900, Max: 900, Runs: 1}

	got := HigherIsBetter(AxisBulk, rust, golang)

	if got.Verdict != Inconclusive {
		t.Fatalf("one run is an anecdote, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestLowerIsBetterPassesWithinAQuarterMore(t *testing.T) {
	rust := sample(100, 98, 102)
	golang := sample(120, 118, 122)

	got := LowerIsBetter(AxisDensity, rust, golang)

	if got.Verdict != Pass {
		t.Fatalf("1.20x should pass a 1.25x ceiling, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestLowerIsBetterFailsWhenGoUsesFarMoreMemory(t *testing.T) {
	rust := sample(100, 99, 101)
	golang := sample(400, 390, 410)

	got := LowerIsBetter(AxisDensity, rust, golang)

	if got.Verdict != Fail {
		t.Fatalf("4x memory with disjoint ranges should fail, got %s: %s", got.Verdict, got.Reason)
	}
}

// A sample that does not repeat cannot decide anything, whatever machine produced it. This
// is measured rather than inferred from the rig: p99 on a virtualised box repeats to within
// 5-8% while p99.9 on the same box swings 14-19%, so one verdict is supportable and the
// other is not, and a rig-wide veto cannot tell them apart.
func TestAddedLatencyRefusesASampleThatDoesNotRepeat(t *testing.T) {
	direct := sample(1000, 990, 1010)
	rust := Sample{Median: 1100, Min: 950, Max: 1400, Runs: MinRuns}
	golang := Sample{Median: 1500, Min: 1300, Max: 1800, Runs: MinRuns}

	got := AddedLatency(RigWSL2NonIsolated, AxisP99, direct, rust, golang)

	if got.Verdict != Inconclusive {
		t.Fatalf("a sample spanning 40%% of its median cannot decide a 25%% question, got %s: %s",
			got.Verdict, got.Reason)
	}
}

// The counterpart: a virtualised rig whose samples demonstrably repeat is allowed to decide,
// because the evidence is the spread and not the hypervisor.
func TestAddedLatencyAcceptsASteadySampleFromAVirtualisedRig(t *testing.T) {
	direct, rust, golang := sample(1000, 995, 1005), sample(1100, 1095, 1105), sample(1120, 1115, 1125)

	got := AddedLatency(RigWSL2NonIsolated, AxisP99, direct, rust, golang)

	if got.Verdict != Pass {
		t.Fatalf("a steady sample should decide regardless of rig, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestAddedLatencyComparesAddedRatherThanAbsolute(t *testing.T) {
	// PostgreSQL dominates both absolute numbers; only the proxy's contribution differs.
	direct := sample(1000, 995, 1005)
	rust := sample(1100, 1095, 1105)
	golang := sample(1120, 1115, 1125)

	got := AddedLatency(RigIsolated, AxisP99, direct, rust, golang)

	if got.Verdict != Pass {
		t.Fatalf("120us added against 100us is 1.2x and should pass, got %s: %s", got.Verdict, got.Reason)
	}
	if got.Ratio < 1.1 || got.Ratio > 1.3 {
		t.Fatalf("ratio should be computed from added latency, got %.2f", got.Ratio)
	}
}

func TestAddedLatencyFailsWhenGoAddsFarMore(t *testing.T) {
	direct := sample(1000, 995, 1005)
	rust := sample(1100, 1095, 1105)
	golang := sample(1400, 1395, 1405)

	got := AddedLatency(RigIsolated, AxisP99, direct, rust, golang)

	if got.Verdict != Fail {
		t.Fatalf("400us added against 100us is 4x and should fail, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestAddedLatencyWithdrawsWhenThePoolerBeatsDirect(t *testing.T) {
	// Above pool size this is the expected shape: queueing at the proxy costs less than
	// making PostgreSQL serve every client its own backend.
	direct := sample(5000, 4900, 5100)
	rust := sample(1100, 1090, 1110)
	golang := sample(1200, 1190, 1210)

	got := AddedLatency(RigIsolated, AxisP99, direct, rust, golang)

	if got.Verdict != Inconclusive {
		t.Fatalf("a negative denominator cannot produce a verdict, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestAddedLatencyWithdrawsBelowTheResolvableDelta(t *testing.T) {
	direct := sample(1000, 999, 1001)
	rust := sample(1005, 1004, 1006)
	golang := sample(1009, 1008, 1010)

	got := AddedLatency(RigIsolated, AxisP99, direct, rust, golang)

	if got.Verdict != Inconclusive {
		t.Fatalf("5us added is below the resolvable floor, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestAddedLatencyGivesTheDeepTailMoreRoom(t *testing.T) {
	direct := sample(1000, 995, 1005)
	rust := sample(1100, 1095, 1105)
	golang := sample(1140, 1135, 1145)

	p99 := AddedLatency(RigIsolated, AxisP99, direct, rust, golang)
	p999 := AddedLatency(RigIsolated, AxisP999, direct, rust, golang)

	if p99.Threshold != MaxRatio {
		t.Fatalf("p99 should use the %.2fx ceiling, got %.2f", MaxRatio, p99.Threshold)
	}
	if p999.Threshold != MaxTailRatio {
		t.Fatalf("p99.9 should use the %.2fx ceiling, got %.2f", MaxTailRatio, p999.Threshold)
	}
}

func TestNoiseFloorGatesTheRig(t *testing.T) {
	quiet := Sample{Median: 1000, Min: 980, Max: 1040, Runs: MinRuns}
	noisy := Sample{Median: 1000, Min: 700, Max: 1900, Runs: MinRuns}

	if got := NoiseFloor(quiet); got.Verdict != Pass {
		t.Fatalf("6%% spread should clear Gate 0, got %s: %s", got.Verdict, got.Reason)
	}
	if got := NoiseFloor(noisy); got.Verdict != Fail {
		t.Fatalf("120%% spread should fail Gate 0, got %s: %s", got.Verdict, got.Reason)
	}
}

func TestLoadGenSaturation(t *testing.T) {
	if LoadGenSaturated(0.5) {
		t.Fatal("half a core is headroom, not saturation")
	}
	if !LoadGenSaturated(0.95) {
		t.Fatal("95%% CPU means the measurement describes the load generator")
	}
}

func TestThresholdsAreTheOnesThatWerePreRegistered(t *testing.T) {
	// This test exists to make a threshold change impossible to land quietly: editing a
	// constant to fit a result has to edit this file too, and that shows up in review.
	cases := map[string]struct{ got, want float64 }{
		"MinRatio":                 {MinRatio, 0.75},
		"MaxRatio":                 {MaxRatio, 1.25},
		"MaxTailRatio":             {MaxTailRatio, 1.50},
		"MaxP99SpreadRatio":        {MaxP99SpreadRatio, 0.10},
		"MinResolvableAddedMicros": {MinResolvableAddedMicros, 20.0},
		"MaxLoadGenCPU":            {MaxLoadGenCPU, 0.70},
		"MinRuns":                  {MinRuns, 5},
	}
	for name, c := range cases {
		if c.got != c.want {
			t.Errorf("%s is %v, pre-registered as %v", name, c.got, c.want)
		}
	}
}
