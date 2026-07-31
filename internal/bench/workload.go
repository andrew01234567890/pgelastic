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
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// scheduleDepth bounds how far the open-loop generator may run ahead of the workers.
//
// Deep enough that a brief stall queues rather than counting as an overrun, shallow enough
// that a genuinely saturated cell is reported as saturated instead of accumulating an
// unbounded backlog that makes the last percentile meaningless.
const scheduleDepth = 4096

// WorkloadName identifies one of the five shapes the comparison is made of.
type WorkloadName string

const (
	// WorkloadChurn opens a connection, authenticates, runs one statement and closes.
	// The axis where TLS handshakes and PBKDF2 dominate.
	WorkloadChurn WorkloadName = "churn"
	// WorkloadThroughput holds connections open and runs cached statements as fast as it
	// can. Closed loop, so its latency figures describe saturation and nothing else.
	WorkloadThroughput WorkloadName = "throughput"
	// WorkloadLatency offers a fixed rate and measures from the moment each operation was
	// due. The only workload whose percentiles mean what percentiles usually mean.
	WorkloadLatency WorkloadName = "latency"
	// WorkloadBulk moves large result sets, to catch a relay that copies more than it must.
	WorkloadBulk WorkloadName = "bulk"
	// WorkloadDensity holds many connections idle and measures what they cost.
	WorkloadDensity WorkloadName = "density"
)

// RunConfig is one cell of the experiment matrix.
type RunConfig struct {
	Workload    WorkloadName
	DSN         string
	Concurrency int
	Duration    time.Duration
	Warmup      time.Duration
	// Rate is operations per second offered, for WorkloadLatency only. Zero means closed
	// loop, which is correct for throughput and wrong for latency.
	Rate float64
	// Query is the statement under test. Kept configurable so the same driver can measure a
	// cached point SELECT and a wide scan without a second code path.
	Query string
	// BulkRows sizes the result set for WorkloadBulk.
	BulkRows int
	// SimpleProtocol sends each statement as a single Query message instead of the
	// Parse/Bind/Execute/Sync the driver prefers.
	//
	// Not a performance knob - a comparability one. Under the extended protocol the driver
	// caches a prepared statement per client connection, so a pooler that hands the next
	// transaction to a different backend must notice and re-prepare it there. The Rust proxy
	// does that by rewriting statement names, pgbouncer does it above 1.21, and a small
	// spike does neither. Measuring all three under the simple protocol removes the
	// capability from the comparison rather than crediting whoever happens to have it.
	SimpleProtocol bool
}

// EffectiveQuery is the statement this cell actually runs.
//
// Bulk synthesises its own unless told otherwise, and that is why `Query` has to be able to be
// unset: the driver defaulted it to a literal and passed it unconditionally, so the bulk
// workload drained `SELECT 1` and `--bulk-rows` did nothing at all. The report records what
// comes back from here rather than the flag, or the artifact keeps the same lie.
func (c RunConfig) EffectiveQuery() string {
	if c.Query != "" {
		return c.Query
	}
	if c.Workload == WorkloadBulk {
		return fmt.Sprintf("SELECT repeat('x', 512) FROM generate_series(1, %d)", max(c.BulkRows, 1))
	}
	return "SELECT 1"
}

// connect opens one client connection honouring the configured protocol.
func connect(ctx context.Context, cfg RunConfig) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}
	if cfg.SimpleProtocol {
		config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}
	return pgx.ConnectConfig(ctx, config)
}

// Probe runs one statement and reports whether the whole path worked.
//
// Used as readiness for poolers with no health endpoint. A listening socket only proves the
// pooler started; this proves it can reach PostgreSQL and authenticate on both legs, which
// is what a measurement started against it depends on.
// Deliberately the simple protocol. A readiness check has no business requiring
// prepared-statement support: under the extended protocol the driver Parses on one backend and
// Binds on whichever backend the pooler hands out next, so a pooler that does not rewrite
// statement names fails the probe and gets reported as never having started - which is a
// statement about the probe rather than about the pooler.
func Probe(ctx context.Context, dsn string) error {
	return oneConnectCycle(ctx, RunConfig{DSN: dsn, Query: "SELECT 1", SimpleProtocol: true})
}

// Run executes one cell and returns what it measured.
func Run(ctx context.Context, cfg RunConfig) (Cell, error) {
	switch cfg.Workload {
	case WorkloadChurn:
		return runChurn(ctx, cfg)
	case WorkloadThroughput:
		return runThroughput(ctx, cfg)
	case WorkloadLatency:
		return runLatency(ctx, cfg)
	case WorkloadBulk:
		return runBulk(ctx, cfg)
	case WorkloadDensity:
		return runDensity(ctx, cfg)
	default:
		return Cell{}, fmt.Errorf("unknown workload %q", cfg.Workload)
	}
}

// runChurn measures complete connection lifecycles.
//
// Deliberately not pooled on the client side: the quantity under test is what the proxy
// spends accepting, negotiating TLS, running SCRAM and tearing down, and a client-side pool
// would amortise away exactly that.
func runChurn(ctx context.Context, cfg RunConfig) (Cell, error) {
	recorder := NewRecorder()
	deadline, cancel := context.WithTimeout(ctx, cfg.Warmup+cfg.Duration)
	defer cancel()

	measuring := afterWarmup(cfg.Warmup)
	var wait sync.WaitGroup
	for range cfg.Concurrency {
		wait.Go(func() {
			for deadline.Err() == nil {
				started := time.Now()
				err := oneConnectCycle(deadline, cfg)
				if aborted(deadline) {
					return
				}
				if !measuring() {
					continue
				}
				if err != nil {
					recorder.Fail(err)
					continue
				}
				recorder.Observe(time.Since(started))
			}
		})
	}
	wait.Wait()
	recorder.Done()
	return recorder.Snapshot(), nil
}

func oneConnectCycle(ctx context.Context, cfg RunConfig) error {
	conn, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	var scratch int
	return conn.QueryRow(ctx, cfg.EffectiveQuery()).Scan(&scratch)
}

// runThroughput holds connections open and asks for as much work as the system will take.
//
// Closed loop on purpose: this is the workload that answers "how much can it do", and its
// latency numbers are recorded but labelled, because in a closed loop they are a restatement
// of the throughput rather than an independent fact.
func runThroughput(ctx context.Context, cfg RunConfig) (Cell, error) {
	recorder := NewRecorder()
	deadline, cancel := context.WithTimeout(ctx, cfg.Warmup+cfg.Duration)
	defer cancel()

	measuring := afterWarmup(cfg.Warmup)
	var wait sync.WaitGroup
	for range cfg.Concurrency {
		wait.Go(func() {
			conn, err := connect(deadline, cfg)
			if err != nil {
				recorder.Fail(err)
				return
			}
			defer func() { _ = conn.Close(context.WithoutCancel(deadline)) }()

			for deadline.Err() == nil {
				started := time.Now()
				var scratch int
				err := conn.QueryRow(deadline, cfg.EffectiveQuery()).Scan(&scratch)
				if aborted(deadline) {
					return
				}
				if !measuring() {
					continue
				}
				if err != nil {
					recorder.Fail(err)
					if !recoverable(err) {
						return
					}
					continue
				}
				recorder.Observe(time.Since(started))
			}
		})
	}
	wait.Wait()
	recorder.Done()
	return recorder.Snapshot(), nil
}

// recoverable reports whether the connection is still usable after an error.
//
// The distinction matters more than it looks. A pooler under load refuses work with a
// SQLSTATE - too many clients, a write stall, a superseded backend - and the connection
// carrying that refusal is perfectly healthy. Treating those as fatal would retire a worker
// on the first refusal and quietly understate throughput for the rest of the run, which
// reads as the pooler being slow rather than as the pooler doing exactly what it should.
// A transport error has no such guarantee, so it still ends the worker.
func recoverable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr)
}

// runLatency offers a fixed rate and measures each operation from when it was due.
//
// The schedule is absolute - the nth operation is due at start+n/rate - so a worker that
// falls behind does not push the schedule out with it. That is the whole defence against
// coordinated omission: the latency of a system that has stopped keeping up grows without
// bound here, where a closed loop would simply issue fewer operations and report the same
// cheerful percentile it did when the system was healthy.
func runLatency(ctx context.Context, cfg RunConfig) (Cell, error) {
	if cfg.Rate <= 0 {
		return Cell{}, fmt.Errorf("the latency workload needs a positive offered rate")
	}

	recorder := NewRecorder()
	deadline, cancel := context.WithTimeout(ctx, cfg.Warmup+cfg.Duration)
	defer cancel()

	measuring := afterWarmup(cfg.Warmup)
	due := make(chan time.Time, scheduleDepth)

	var wait sync.WaitGroup
	for range cfg.Concurrency {
		wait.Go(func() {
			conn, err := connect(deadline, cfg)
			if err != nil {
				recorder.Fail(err)
				return
			}
			defer func() { _ = conn.Close(context.WithoutCancel(deadline)) }()

			for scheduled := range due {
				var scratch int
				err := conn.QueryRow(deadline, cfg.EffectiveQuery()).Scan(&scratch)
				if aborted(deadline) {
					return
				}
				if !measuring() {
					continue
				}
				if err != nil {
					recorder.Fail(err)
					continue
				}
				recorder.Observe(time.Since(scheduled))
			}
		})
	}

	generate(deadline, cfg.Rate, due, recorder)
	close(due)
	wait.Wait()
	recorder.Done()
	return recorder.Snapshot(), nil
}

// generate emits operation start times on an absolute schedule.
//
// When the buffer is full every worker is busy, and the operation is counted as an overrun
// rather than being dropped silently or allowed to delay the schedule.
func generate(ctx context.Context, rate float64, due chan<- time.Time, recorder *Recorder) {
	interval := time.Duration(float64(time.Second) / rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	start := time.Now()

	for n := int64(0); ctx.Err() == nil; n++ {
		scheduled := start.Add(time.Duration(n) * interval)
		if wait := time.Until(scheduled); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
		select {
		case due <- scheduled:
		default:
			recorder.Overrun()
		}
	}
}

// runBulk moves large result sets to expose a relay that buffers or copies more than it must.
//
// Every row is read and its bytes counted rather than discarded by the driver, because a
// result set nobody drains measures the proxy's write buffer instead of its throughput.
func runBulk(ctx context.Context, cfg RunConfig) (Cell, error) {
	recorder := NewRecorder()
	deadline, cancel := context.WithTimeout(ctx, cfg.Warmup+cfg.Duration)
	defer cancel()

	query := cfg.EffectiveQuery()

	measuring := afterWarmup(cfg.Warmup)
	var wait sync.WaitGroup
	for range cfg.Concurrency {
		wait.Go(func() {
			conn, err := connect(deadline, cfg)
			if err != nil {
				recorder.Fail(err)
				return
			}
			defer func() { _ = conn.Close(context.WithoutCancel(deadline)) }()

			for deadline.Err() == nil {
				started := time.Now()
				moved, err := drain(deadline, conn, query)
				if aborted(deadline) {
					return
				}
				if !measuring() {
					continue
				}
				if err != nil {
					recorder.Fail(err)
					if !recoverable(err) {
						return
					}
					continue
				}
				recorder.Observe(time.Since(started))
				recorder.ObserveBytes(moved)
			}
		})
	}
	wait.Wait()
	recorder.Done()
	return recorder.Snapshot(), nil
}

func drain(ctx context.Context, conn *pgx.Conn, query string) (int64, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var moved int64
	for rows.Next() {
		for _, value := range rows.RawValues() {
			moved += int64(len(value))
		}
	}
	return moved, rows.Err()
}

// runDensity opens connections, holds them idle, and reports how many it sustained.
//
// The number this workload exists to produce is not here: it is the proxy's resident memory,
// which has to be read from outside the process while these connections are held. The Ops
// count is the witness that the connections really were established, so a proxy that refused
// half of them cannot be credited with cheap memory.
func runDensity(ctx context.Context, cfg RunConfig) (Cell, error) {
	recorder := NewRecorder()
	deadline, cancel := context.WithTimeout(ctx, cfg.Warmup+cfg.Duration)
	defer cancel()

	held := make([]*pgx.Conn, 0, cfg.Concurrency)
	var mu sync.Mutex
	var wait sync.WaitGroup

	for range cfg.Concurrency {
		wait.Go(func() {
			started := time.Now()
			conn, err := connect(deadline, cfg)
			if err != nil {
				recorder.Fail(err)
				return
			}
			recorder.Observe(time.Since(started))
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		})
	}
	wait.Wait()

	<-deadline.Done()

	mu.Lock()
	defer mu.Unlock()
	for _, conn := range held {
		_ = conn.Close(context.WithoutCancel(ctx))
	}
	recorder.Done()
	return recorder.Snapshot(), nil
}

// aborted reports that an error is the run ending rather than the system failing.
//
// Without this every cell records one failure per worker: the operations in flight when the
// duration expires are cancelled by the harness itself, and counting those as errors would
// put a handful of spurious timeouts in every result file and make a healthy run look like a
// failing one.
func aborted(ctx context.Context) bool { return ctx.Err() != nil }

// afterWarmup returns a predicate that is false until the warmup has elapsed.
//
// Warmup samples are discarded rather than recorded and subtracted, because the first
// connection to a cold proxy pays for the pool filling and PostgreSQL faulting its buffers
// in, and averaging that into a steady-state percentile is how a benchmark reports a
// regression that is really a cold start.
func afterWarmup(warmup time.Duration) func() bool {
	until := time.Now().Add(warmup)
	return func() bool { return !time.Now().Before(until) }
}
