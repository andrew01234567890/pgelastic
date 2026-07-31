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
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// The property the whole latency workload rests on: operation n is due at start+n*interval
// no matter when operation n-1 was actually served. If the schedule drifted with the
// workers, a system that had stopped keeping up would report the same percentiles it did
// when it was healthy, which is the definition of coordinated omission.
func TestTheScheduleIsAbsoluteRatherThanRelativeToTheLastCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	const rate = 1000 // one per millisecond
	due := make(chan time.Time, 1024)
	generate(ctx, rate, due, NewRecorder())
	close(due)

	var scheduled []time.Time
	for at := range due {
		scheduled = append(scheduled, at)
	}
	if len(scheduled) < 10 {
		t.Fatalf("expected the generator to emit a schedule, got %d entries", len(scheduled))
	}

	for i := 1; i < len(scheduled); i++ {
		gap := scheduled[i].Sub(scheduled[i-1])
		if gap != time.Millisecond {
			t.Fatalf("entry %d is %v after its predecessor, want exactly 1ms: the schedule drifted", i, gap)
		}
	}
}

// A full buffer means every worker is busy. That has to be counted, because a cell that
// could not be offered the load it was configured for is not a measurement of that load.
func TestAnUnservableScheduleIsCountedRatherThanSilentlyDropped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	recorder := NewRecorder()
	due := make(chan time.Time, 2)
	generate(ctx, 5000, due, recorder)

	if got := recorder.Snapshot().Overruns; got == 0 {
		t.Fatal("a two-deep buffer offered 5000/s with no consumer should have recorded overruns")
	}
}

func TestWarmupSamplesAreDiscarded(t *testing.T) {
	measuring := afterWarmup(50 * time.Millisecond)

	if measuring() {
		t.Fatal("samples during the warmup window must not be recorded")
	}
	time.Sleep(60 * time.Millisecond)
	if !measuring() {
		t.Fatal("samples after the warmup window must be recorded")
	}
}

func TestAnUnknownWorkloadIsRefusedRatherThanSilentlyProducingZero(t *testing.T) {
	_, err := Run(context.Background(), RunConfig{Workload: "nonsense"})

	if err == nil {
		t.Fatal("an unknown workload should be an error, not an empty result that reads as a measurement")
	}
}

func TestTheLatencyWorkloadRefusesAClosedLoop(t *testing.T) {
	_, err := Run(context.Background(), RunConfig{Workload: WorkloadLatency, Rate: 0})

	if err == nil {
		t.Fatal("a latency measurement without an offered rate is a throughput measurement wearing a percentile")
	}
}

// A pooler under load refuses work with a SQLSTATE, and the connection carrying that refusal
// is healthy. Retiring the worker on it would understate throughput for the rest of the run
// and read as the pooler being slow rather than as the pooler doing its job.
func TestAServerSideRefusalDoesNotRetireTheWorker(t *testing.T) {
	cases := map[string]struct {
		err  error
		want bool
	}{
		"too many clients":    {&pgconn.PgError{Code: "53300"}, true},
		"admission denied":    {&pgconn.PgError{Code: "53400"}, true},
		"write stalled":       {&pgconn.PgError{Code: "57P03"}, true},
		"connection reset":    {errors.New("read: connection reset by peer"), false},
		"backend disappeared": {errors.New("unexpected EOF"), false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := recoverable(c.err); got != c.want {
				t.Errorf("recoverable(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestErrorsAreKeyedBySQLStateWhereThereIsOne(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"too many clients":  {&pgconn.PgError{Code: "53300"}, "53300"},
		"backend went away": {&pgconn.PgError{Code: "08006"}, "08006"},
		"deadline":          {context.DeadlineExceeded, "timeout"},
		"cancelled":         {context.Canceled, "canceled"},
		"anything else":     {errors.New("connection reset"), "transport"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := classifyError(c.err); got != c.want {
				t.Errorf("classifyError(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

func TestTheRecorderReportsPercentilesAndRate(t *testing.T) {
	recorder := NewRecorder()
	for i := range 1000 {
		recorder.Observe(time.Duration(i+1) * time.Microsecond)
	}
	recorder.Fail(&pgconn.PgError{Code: "53300"})
	recorder.Done()

	cell := recorder.Snapshot()

	if cell.Ops != 1000 {
		t.Errorf("ops = %d, want 1000", cell.Ops)
	}
	if cell.Errors["53300"] != 1 {
		t.Errorf("errors = %v, want one 53300", cell.Errors)
	}
	if cell.P50Micros < 450 || cell.P50Micros > 550 {
		t.Errorf("p50 = %.0fus, want roughly 500 for a uniform 1..1000 spread", cell.P50Micros)
	}
	if cell.P99Micros <= cell.P50Micros {
		t.Errorf("p99 (%.0f) should exceed p50 (%.0f)", cell.P99Micros, cell.P50Micros)
	}
	if cell.Throughput <= 0 {
		t.Error("throughput should be computed from elapsed time")
	}
}
