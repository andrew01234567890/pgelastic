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
	"maps"
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/jackc/pgx/v5/pgconn"
)

// Latency is recorded in microseconds across a range wide enough to hold both a cached
// SELECT and a connection that gave up.
//
// Three significant figures rather than more: the extra buckets cost memory per recorder and
// buy precision the rig cannot resolve anyway.
const (
	latencyMinMicros = 1
	latencyMaxMicros = 60 * 1000 * 1000
	latencyPrecision = 3

	// outcomeNone labels a nil error, so a caller that reports success through Fail is
	// visible in the result rather than silently counted as a transport failure.
	outcomeNone = "none"
)

// Recorder accumulates the outcomes of one cell: one workload, one target, one concurrency.
//
// Safe for concurrent use because every workload here is many goroutines against one
// recorder. The lock is uncontended enough at these rates to stay out of the measurement,
// and a per-goroutine histogram merged at the end would trade that for the risk of losing a
// worker's samples when it exits early.
type Recorder struct {
	mu       sync.Mutex
	latency  *hdr.Histogram
	ops      int64
	bytes    int64
	overruns int64
	errors   map[string]int64
	started  time.Time
	elapsed  time.Duration
}

func NewRecorder() *Recorder {
	return &Recorder{
		latency: hdr.New(latencyMinMicros, latencyMaxMicros, latencyPrecision),
		errors:  map[string]int64{},
		started: time.Now(),
	}
}

// Observe records one successful operation.
//
// The caller decides what the duration is measured from, and for open-loop workloads that
// must be the moment the operation was *scheduled*, not the moment a worker picked it up.
// Measuring from pickup is coordinated omission: it hides exactly the latency that appears
// when the system stops keeping up.
func (r *Recorder) Observe(d time.Duration) {
	micros := max(d.Microseconds(), latencyMinMicros)
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.latency.RecordValue(micros)
	r.ops++
}

// ObserveBytes records payload moved, for the workloads whose unit is bandwidth.
func (r *Recorder) ObserveBytes(n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bytes += n
}

// Fail records an operation that did not complete, keyed by SQLSTATE where PostgreSQL gave
// one. The proxy's own refusals carry SQLSTATEs on purpose, so a run that hits admission
// limits reports 53300 rather than a shapeless error count.
func (r *Recorder) Fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors[classifyError(err)]++
}

// Overrun records that the schedule could not be met because every worker was busy.
//
// Reported rather than smoothed away: a cell with overruns was offered more load than it
// could take, and its latency percentiles describe a saturated system.
func (r *Recorder) Overrun() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.overruns++
}

// Done stops the clock. Throughput is computed from this rather than from the configured
// duration, so a run cut short reports the rate it actually achieved.
func (r *Recorder) Done() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.elapsed = time.Since(r.started)
}

// Cell is one measured point, ready to be written to a result file.
type Cell struct {
	Ops        int64            `json:"ops"`
	Errors     map[string]int64 `json:"errors,omitempty"`
	Overruns   int64            `json:"overruns"`
	Bytes      int64            `json:"bytes,omitempty"`
	ElapsedMs  float64          `json:"elapsedMs"`
	Throughput float64          `json:"opsPerSecond"`
	MBPerSec   float64          `json:"mbPerSecond,omitempty"`
	P50Micros  float64          `json:"p50Micros"`
	P90Micros  float64          `json:"p90Micros"`
	P99Micros  float64          `json:"p99Micros"`
	P999Micros float64          `json:"p999Micros"`
	MaxMicros  float64          `json:"maxMicros"`
}

// Snapshot freezes what has been recorded so far.
func (r *Recorder) Snapshot() Cell {
	r.mu.Lock()
	defer r.mu.Unlock()

	elapsed := r.elapsed
	if elapsed == 0 {
		elapsed = time.Since(r.started)
	}
	seconds := elapsed.Seconds()

	cell := Cell{
		Ops:        r.ops,
		Overruns:   r.overruns,
		Bytes:      r.bytes,
		ElapsedMs:  float64(elapsed.Microseconds()) / 1000,
		P50Micros:  float64(r.latency.ValueAtQuantile(50)),
		P90Micros:  float64(r.latency.ValueAtQuantile(90)),
		P99Micros:  float64(r.latency.ValueAtQuantile(99)),
		P999Micros: float64(r.latency.ValueAtQuantile(99.9)),
		MaxMicros:  float64(r.latency.Max()),
	}
	if seconds > 0 {
		cell.Throughput = float64(r.ops) / seconds
		cell.MBPerSec = float64(r.bytes) / seconds / (1 << 20)
	}
	if len(r.errors) > 0 {
		cell.Errors = make(map[string]int64, len(r.errors))
		maps.Copy(cell.Errors, r.errors)
	}
	return cell
}

// classifyError reduces an error to the label a result file should carry.
//
// SQLSTATE where there is one, because that is the proxy's own vocabulary for refusing work
// and the difference between "too many clients" (53300) and "the backend went away" (08006)
// is the difference between a working admission control and a broken proxy.
func classifyError(err error) string {
	if err == nil {
		return outcomeNone
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code != "" {
		return pgErr.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "transport"
}
