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

package verify

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// WorkloadConfig describes one run of the writer fleet.
type WorkloadConfig struct {
	// DSN is the endpoint under test: the proxy, or PostgreSQL directly for calibration.
	DSN       string
	Table     string
	Writers   int
	Duration  time.Duration
	OpTimeout time.Duration
	// FirstValue is where the monotonic sequence resumes. A run that reuses values from a
	// prior ledger would make R ⊆ ATTEMPTED unfalsifiable for those values.
	FirstValue int64
	// Interval optionally paces each writer between inserts.
	Interval time.Duration
}

// Stats counts what the workload did. They are diagnostics; the ledger is the evidence.
type Stats struct {
	Attempted     int64 `json:"attempted"`
	Committed     int64 `json:"committed"`
	Indeterminate int64 `json:"indeterminate"`
	Failed        int64 `json:"failed"`
	ConnectErrors int64 `json:"connectErrors"`
}

type counters struct {
	attempted     atomic.Int64
	committed     atomic.Int64
	indeterminate atomic.Int64
	failed        atomic.Int64
	connectErrors atomic.Int64
}

func (c *counters) snapshot() Stats {
	return Stats{
		Attempted:     c.attempted.Load(),
		Committed:     c.committed.Load(),
		Indeterminate: c.indeterminate.Load(),
		Failed:        c.failed.Load(),
		ConnectErrors: c.connectErrors.Load(),
	}
}

const (
	defaultOpTimeout   = 5 * time.Second
	reconnectBackoff   = 200 * time.Millisecond
	defaultWriterCount = 1
)

// NextValue returns the first value a resumed run may use without colliding with a
// previous one.
func NextValue(recs []Record) int64 {
	var highest int64
	for _, rec := range recs {
		if rec.Value > highest {
			highest = rec.Value
		}
	}
	return highest + 1
}

// RunWorkload drives Writers goroutines, each inserting monotonically increasing values
// on its own connection, until Duration elapses or ctx is done. Every insert is recorded
// in the ledger before it is issued and classified after it returns.
//
// Connections are deliberately unpooled and never retried: a retry would turn an
// ambiguous outcome into a second, differently-ambiguous one, and the point of the tool
// is to observe the ambiguity rather than paper over it.
func RunWorkload(ctx context.Context, cfg WorkloadConfig, ledger *Ledger) (Stats, error) {
	if cfg.Writers <= 0 {
		cfg.Writers = defaultWriterCount
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = defaultOpTimeout
	}
	if cfg.Table == "" {
		cfg.Table = DefaultTable
	}
	ident, err := quoteIdentifier(cfg.Table)
	if err != nil {
		return Stats{}, err
	}

	runCtx := ctx
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	next := &atomic.Int64{}
	next.Store(cfg.FirstValue)
	stats := &counters{}
	errs := make([]error, cfg.Writers)

	var wg sync.WaitGroup
	for i := range cfg.Writers {
		wg.Go(func() {
			w := &writer{
				cfg:    cfg,
				sql:    "INSERT INTO " + ident + " (value) VALUES ($1)",
				next:   next,
				ledger: ledger,
				stats:  stats,
			}
			errs[i] = w.run(runCtx)
		})
	}
	wg.Wait()

	return stats.snapshot(), errors.Join(errs...)
}

type writer struct {
	cfg    WorkloadConfig
	sql    string
	next   *atomic.Int64
	ledger *Ledger
	stats  *counters
}

func (w *writer) run(ctx context.Context) error {
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
		}
	}()

	for ctx.Err() == nil {
		if conn == nil {
			c, err := pgx.Connect(ctx, w.cfg.DSN)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				w.stats.connectErrors.Add(1)
				sleep(ctx, reconnectBackoff)
				continue
			}
			conn = c
		}

		broken, err := w.insert(ctx, conn)
		if err != nil {
			return err
		}
		if broken {
			_ = conn.Close(context.WithoutCancel(ctx))
			conn = nil
		}
		if w.cfg.Interval > 0 {
			sleep(ctx, w.cfg.Interval)
		}
	}
	return nil
}

// insert performs one attempt. It returns whether the connection must be discarded, and
// an error only when the ledger itself failed — at which point the run must stop, because
// an unrecorded outcome invalidates the whole result.
func (w *writer) insert(ctx context.Context, conn *pgx.Conn) (broken bool, err error) {
	v := w.next.Add(1) - 1
	if err := w.ledger.Attempt(v); err != nil {
		return false, fmt.Errorf("ledger ATTEMPTED %d: %w", v, err)
	}
	w.stats.attempted.Add(1)

	opCtx, cancel := context.WithTimeout(ctx, w.cfg.OpTimeout)
	_, execErr := conn.Exec(opCtx, w.sql, v)
	cancel()

	class := Classify(execErr)
	switch class.Outcome {
	case OutcomeCommitted:
		if err := w.ledger.Commit(v); err != nil {
			return false, fmt.Errorf("ledger COMMITTED %d: %w", v, err)
		}
		w.stats.committed.Add(1)
	case OutcomeIndeterminate:
		if err := w.ledger.Indeterminate(v); err != nil {
			return false, fmt.Errorf("ledger INDETERMINATE %d: %w", v, err)
		}
		w.stats.indeterminate.Add(1)
	case OutcomeFailed:
		w.stats.failed.Add(1)
	}

	// A transport-level failure leaves no SQLSTATE and no usable connection.
	return conn.IsClosed() || (execErr != nil && class.Code == ""), nil
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
