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
	"context"
	"testing"
	"time"
)

func TestCountingACpuSetList(t *testing.T) {
	cases := map[string]int{
		"6-9":       4,
		"6-9,22-25": 8,
		"3":         1,
		"0,2,4":     3,
		"":          0,
		"nonsense":  0,
		"9-6":       0,
	}
	for list, want := range cases {
		if got := countCPUs(list); got != want {
			t.Errorf("countCPUs(%q) = %d, want %d", list, got, want)
		}
	}
}

// A probe that could not find its cgroup returns nil and says why, rather than falling back to
// a differently-defined source. Two arms measured by two instruments would produce a
// comparison that looks valid and is not.
func TestAProbeWithNoCgroupIsAbsentRatherThanApproximate(t *testing.T) {
	probe, why := NewResourceProbe(context.Background(), "")
	if probe != nil {
		t.Fatal("naming no container must not produce a probe")
	}
	if why == "" {
		t.Error("an absent probe has to say why, or the gap is invisible in the report")
	}

	missing, why := NewResourceProbe(context.Background(), "pgebench-no-such-container")
	if missing != nil {
		t.Fatal("a container that does not exist must not produce a probe")
	}
	if why == "" {
		t.Error("an absent probe has to say why")
	}
}

// Every method has to be safe on a nil probe, because the direct arm has no pooler to sample
// and every workload calls these unconditionally.
func TestANilProbeIsUsableEverywhere(t *testing.T) {
	var probe *ResourceProbe

	probe.Stop()
	if got := probe.CPUSet(); got != "" {
		t.Errorf("CPUSet = %q, want empty", got)
	}
	if got := probe.WorkingSet(); got != 0 {
		t.Errorf("WorkingSet = %d, want 0", got)
	}
	if got := probe.Segment(time.Now(), time.Now(), 10, 0); got != nil {
		t.Errorf("Segment = %+v, want nil", got)
	}
}

// An unsampled arm produces a Sample with no runs, and the criteria turn that into a visible
// INCONCLUSIVE rather than a zero that reads as a measurement.
func TestAnUnsampledArmIsInconclusiveRatherThanZero(t *testing.T) {
	result := LowerIsBetter(AxisCPUPerOp, Sample{}, Sample{})

	if result.Verdict != Inconclusive {
		t.Fatalf("verdict = %s, want INCONCLUSIVE", result.Verdict)
	}
}

// A cell whose working set sits below its own floor reports no per-connection cost at all.
//
// This is the measured failure, not a hypothetical: reading the floor moments after the
// previous repetition closed 256 connections put it above the steady state that followed, and
// the subtraction produced -13490 bytes per connection. A negative is obvious enough to catch,
// but the same contamination shrinks a positive figure silently, so the arm with the dirtiest
// baseline would win the density axis.
func TestACellBelowItsFloorReportsNoPerConnectionCost(t *testing.T) {
	probe := &ResourceProbe{stop: make(chan struct{}), done: make(chan struct{})}
	start := time.Now()
	probe.samples = []cgroupSample{
		{at: start, cpuUsec: 0, workingSet: 3_000_000},
		{at: start.Add(time.Second), cpuUsec: 1_000_000, workingSet: 3_000_000},
	}

	// A floor above the cell, which is what a baseline read during teardown looks like.
	got := probe.Segment(start, start.Add(2*time.Second), 256, 5_500_000)

	if got == nil {
		t.Fatal("the cell still has CPU and memory figures worth reporting")
	}
	if got.BytesPerConn != 0 {
		t.Errorf("bytesPerConnection = %v, want absent: a pooler cannot use less than nothing",
			got.BytesPerConn)
	}
	if got.IdleWorkingSet != 5_500_000 {
		t.Error("the floor has to survive into the artifact, or the absence cannot be explained")
	}
}

func TestACellAboveItsFloorDividesByConnectionsEstablished(t *testing.T) {
	probe := &ResourceProbe{stop: make(chan struct{}), done: make(chan struct{})}
	start := time.Now()
	probe.samples = []cgroupSample{
		{at: start, workingSet: 3_000_000},
		{at: start.Add(time.Second), workingSet: 3_000_000},
	}

	// 256 asked for, 128 established: the refused half must not read as cheap memory.
	got := probe.Segment(start, start.Add(2*time.Second), 128, 2_000_000)

	if got.BytesPerConn != (3_000_000-2_000_000)/128.0 {
		t.Errorf("bytesPerConnection = %v, want the delta over the 128 that connected",
			got.BytesPerConn)
	}
}
