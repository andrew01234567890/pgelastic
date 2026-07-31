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
	"strings"
	"testing"
)

// The client count every drift case uses. One point is enough: the check pairs by
// concurrency, and a second would only repeat the same arithmetic.
const driftClients = 64

const (
	runA = "run-a"
	runB = "run-b"
)

func invocation(id string, median float64) Report {
	return Report{
		RunID:    id,
		Workload: WorkloadThroughput,
		Points: []Point{{
			Concurrency: driftClients,
			Throughput:  sample(median, median*0.99, median*1.01),
		}},
	}
}

func throughputOf(point Point) Sample { return point.Throughput }

// The measured drift on this rig is 8-13% between invocations against 0-2% within one. At the
// reused 10% gate that is precisely the interesting boundary: some real runs pass and some
// real runs fail, which is the point of measuring it rather than asserting it.
func TestInvocationsThatDriftWiderThanTheGateFail(t *testing.T) {
	reports := []Report{
		invocation(runA, 15000),
		invocation(runB, 16800), // 12% above, inside the measured drift band
	}

	got := Reproducible(AxisThroughput, reports, 64, throughputOf)

	if got.Verdict != Fail {
		t.Fatalf("verdict = %s, want FAIL: 12%% is wider than the %.0f%% gate",
			got.Verdict, MaxP99SpreadRatio*100)
	}
	if !strings.Contains(got.Reason, "reporting the rig") {
		t.Errorf("the reason has to say what a verdict across these would actually measure, got %q",
			got.Reason)
	}
}

func TestInvocationsThatAgreeReproduce(t *testing.T) {
	reports := []Report{
		invocation(runA, 15000),
		invocation(runB, 15300),
		invocation("run-c", 15100),
	}

	got := Reproducible(AxisThroughput, reports, 64, throughputOf)

	if got.Verdict != Pass {
		t.Fatalf("verdict = %s, want PASS: 2%% is inside the gate. reason: %s", got.Verdict, got.Reason)
	}
}

// Five repetitions of one invocation say nothing about drift between invocations, and would
// pass every time - which would turn the reproducibility check into a rubber stamp.
func TestRepetitionsOfOneInvocationCannotSpeakToDrift(t *testing.T) {
	reports := []Report{
		invocation(runA, 15000),
		invocation(runA, 15100),
	}

	got := Reproducible(AxisThroughput, reports, 64, throughputOf)

	if got.Verdict != Inconclusive {
		t.Fatalf("verdict = %s, want INCONCLUSIVE", got.Verdict)
	}
}

// Reports predating run identity might be separate invocations or the same one. A check that
// cannot tell has to say so, rather than reading two anonymous reports as one run and claiming
// they cannot speak to drift for a reason that is not the real one.
func TestReportsWithoutARunIdentityCannotBeCheckedForDrift(t *testing.T) {
	reports := []Report{
		{Workload: WorkloadThroughput, Points: []Point{{Concurrency: 64, Throughput: sample(15000, 14900, 15100)}}},
		{Workload: WorkloadThroughput, Points: []Point{{Concurrency: 64, Throughput: sample(16800, 16700, 16900)}}},
	}

	got := Reproducible(AxisThroughput, reports, 64, throughputOf)

	if got.Verdict != Inconclusive {
		t.Fatalf("verdict = %s, want INCONCLUSIVE", got.Verdict)
	}
	if !strings.Contains(got.Reason, "no run id") {
		t.Errorf("the reason must name the actual obstacle, got %q", got.Reason)
	}
}

func TestASingleInvocationIsNotAReproducibilityCheck(t *testing.T) {
	got := Reproducible(AxisThroughput, []Report{invocation(runA, 15000)}, 64, throughputOf)

	if got.Verdict != Inconclusive {
		t.Errorf("verdict = %s, want INCONCLUSIVE", got.Verdict)
	}
}

// A concurrency none of the reports visited must not silently produce a verdict from the
// points that happen to be present.
func TestAConcurrencyNobodyMeasuredIsInconclusive(t *testing.T) {
	reports := []Report{invocation(runA, 15000), invocation(runB, 15100)}

	got := Reproducible(AxisThroughput, reports, 256, throughputOf)

	if got.Verdict != Inconclusive {
		t.Errorf("verdict = %s, want INCONCLUSIVE", got.Verdict)
	}
}

// Arms measured a day apart are still comparable on throughput - the reference arm is often
// measured once and kept - but the reader has to be told, on every row.
func TestArmsFromSeparateInvocationsCarryTheWarningOnEveryRow(t *testing.T) {
	point := Point{Concurrency: 64, Throughput: sample(15000, 14900, 15100)}
	rust := Report{RunID: runA, Workload: WorkloadThroughput, Points: []Point{point}}
	golang := Report{RunID: runB, Workload: WorkloadThroughput, Points: []Point{point}}

	results := Compare(Report{}, rust, golang)

	if len(results) == 0 {
		t.Fatal("no rows")
	}
	for _, result := range results {
		if !strings.Contains(result.Reason, "separate invocations") {
			t.Errorf("row %s does not mention the run boundary: %q", result.Axis, result.Reason)
		}
	}
}

// The latency axes subtract the direct arm from the pooled ones. Across a run boundary that
// subtraction crosses drift larger than the added latency being measured, so the difference
// would be mostly the gap between two invocations reported as a property of the proxy.
func TestLatencyRowsAreWithheldAcrossARunBoundary(t *testing.T) {
	point := Point{
		Concurrency: 64,
		P99Micros:   sample(4000, 3900, 4100),
		P999Micros:  sample(5000, 4900, 5100),
	}
	direct := Report{RunID: runA, Workload: WorkloadLatency, Points: []Point{point}}
	rust := Report{RunID: runB, Workload: WorkloadLatency, Points: []Point{point}}
	golang := Report{RunID: runB, Workload: WorkloadLatency, Points: []Point{point}}

	results := Compare(direct, rust, golang)

	found := 0
	for _, result := range results {
		if result.Axis != AxisP99 && result.Axis != AxisP999 {
			continue
		}
		found++
		if result.Verdict != Inconclusive {
			t.Errorf("%s = %s, want INCONCLUSIVE: the subtraction crosses the drift boundary",
				result.Axis, result.Verdict)
		}
		if strings.Count(result.Reason, "separate invocations") != 1 {
			t.Errorf("%s says it once or not at all, got %q", result.Axis, result.Reason)
		}
	}
	if found != 2 {
		t.Fatalf("got %d latency rows, want 2", found)
	}
}

func TestOneInvocationComparesWithoutAWarning(t *testing.T) {
	point := Point{Concurrency: 64, Throughput: sample(15000, 14900, 15100)}
	rust := Report{RunID: runA, Workload: WorkloadThroughput, Points: []Point{point}}
	golang := Report{RunID: runA, Workload: WorkloadThroughput, Points: []Point{point}}

	for _, result := range Compare(Report{}, rust, golang) {
		if strings.Contains(result.Reason, "separate invocations") {
			t.Errorf("row %s warns about a boundary that is not there: %q", result.Axis, result.Reason)
		}
	}
}

// A report written before run identity existed cannot be shown to have been measured alongside
// the others. Silence would imply it was, so the row says the identity is unknown instead.
func TestAnArmWithoutARunIdentityIsFlaggedRatherThanAssumed(t *testing.T) {
	point := Point{Concurrency: 64, Throughput: sample(15000, 14900, 15100)}
	rust := Report{RunID: runA, Workload: WorkloadThroughput, Points: []Point{point}}
	legacy := Report{Workload: WorkloadThroughput, Points: []Point{point}}

	results := Compare(Report{}, rust, legacy)

	if len(results) == 0 {
		t.Fatal("no rows")
	}
	for _, result := range results {
		if !strings.Contains(result.Reason, "predates run identity") {
			t.Errorf("row %s implies these were measured together: %q", result.Axis, result.Reason)
		}
	}
}

// The absent-arm placeholder is the zero Report, and must not be mistaken for an arm that was
// measured without an identity - every two-arm comparison would carry a spurious warning.
func TestTheAbsentArmPlaceholderIsNotMistakenForAnUnidentifiedRun(t *testing.T) {
	point := Point{Concurrency: 64, Throughput: sample(15000, 14900, 15100)}
	rust := Report{RunID: runA, Workload: WorkloadThroughput, Points: []Point{point}}
	golang := Report{RunID: runA, Workload: WorkloadThroughput, Points: []Point{point}}

	for _, result := range Compare(Report{}, rust, golang) {
		if strings.Contains(result.Reason, "predates run identity") {
			t.Errorf("the missing direct arm was counted as an unidentified run: %q", result.Reason)
		}
	}
}
