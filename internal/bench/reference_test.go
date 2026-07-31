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

// The reference arm exists to say whether the Rust-versus-Go question matters at all. It
// must never turn into a threshold: pgbouncer has no epoch fence, no tenant routing, no
// capacity allocator and no quiesce, so passing or failing against it would be scoring two
// different programs.
func TestTheReferenceArmProducesNoVerdict(t *testing.T) {
	pgbouncer := sample(100000, 99000, 101000)
	ours := sample(50000, 49000, 51000)

	got := Against(AxisThroughput, pgbouncer, ours)

	if got.Verdict != VerdictReference {
		t.Fatalf("verdict = %s, want %s: the reference arm is not a gate", got.Verdict, VerdictReference)
	}
	if got.Verdict == Fail {
		t.Fatal("being slower than pgbouncer is a finding, not a failure")
	}
	if got.Ratio < 0.49 || got.Ratio > 0.51 {
		t.Errorf("ratio = %.3f, want 0.5", got.Ratio)
	}
}

func TestTheReferenceArmSurvivesAZeroMeasurement(t *testing.T) {
	got := Against(AxisThroughput, Sample{}, sample(100, 99, 101))

	if got.Verdict != VerdictReference {
		t.Errorf("verdict = %s", got.Verdict)
	}
	if got.Ratio != 0 {
		t.Errorf("ratio = %v, want 0 rather than an infinity", got.Ratio)
	}
}

func TestReferenceRowsFollowTheWorkloadsOwnAxis(t *testing.T) {
	cases := map[WorkloadName]Axis{
		WorkloadThroughput: AxisThroughput,
		WorkloadChurn:      AxisChurn,
		WorkloadBulk:       AxisBulk,
	}
	for workload, want := range cases {
		t.Run(string(workload), func(t *testing.T) {
			point := Point{
				Concurrency: 64,
				Throughput:  sample(1000, 990, 1010),
				MBPerSec:    sample(500, 495, 505),
			}
			reference := Report{Workload: workload, Points: []Point{point}}
			arm := Report{Workload: workload, Points: []Point{point}}

			rows := ReferenceRows(reference, arm)

			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if rows[0].Axis != want {
				t.Errorf("axis = %s, want %s", rows[0].Axis, want)
			}
			if rows[0].Concurrency != 64 {
				t.Errorf("row describes %d clients, want 64: a table of rows all labelled "+
					"identically cannot be read", rows[0].Concurrency)
			}
		})
	}
}

// Rows are paired by client count, not by position in the list. Two sweeps that visited
// different concurrencies would otherwise be lined up against each other in order, reporting
// 64 clients of one arm against 256 of the other.
func TestReferenceRowsPairByConcurrencyRatherThanPosition(t *testing.T) {
	at := func(clients int) Point {
		return Point{Concurrency: clients, Throughput: sample(1000, 990, 1010)}
	}
	reference := Report{Workload: WorkloadThroughput, Points: []Point{at(1), at(8), at(64)}}
	// Same points, opposite order, and one the reference never visited.
	arm := Report{Workload: WorkloadThroughput, Points: []Point{at(64), at(8), at(256)}}

	rows := ReferenceRows(reference, arm)

	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2: only 8 and 64 were swept by both", len(rows))
	}
	for _, row := range rows {
		if row.Concurrency != 8 && row.Concurrency != 64 {
			t.Errorf("row at %d clients: neither arm swept that against the other", row.Concurrency)
		}
	}
}

func TestReferenceRowsRefuseADifferentWorkload(t *testing.T) {
	point := Point{Concurrency: 1, Throughput: sample(1000, 990, 1010)}
	reference := Report{Workload: WorkloadChurn, Points: []Point{point}}
	arm := Report{Workload: WorkloadThroughput, Points: []Point{point}}

	rows := ReferenceRows(reference, arm)

	if len(rows) != 1 || rows[0].Verdict != Inconclusive {
		t.Fatalf("comparing churn against throughput must refuse, got %+v", rows)
	}
}
