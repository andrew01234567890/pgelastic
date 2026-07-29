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

// Command demo-writer is the live-migration demo's client.
//
// It writes continuously through the pool's Service - not to an instance - while a tenant
// is migrated underneath it, and reports the two things the demo is about: whether any
// committed write was lost, which the internal/verify ledger answers, and what the pause
// cost the client, which only the client can answer.
//
// The pause is reported as the largest gap between two consecutive successful writes rather
// than as a duration the operator claims about itself. A queued client shows one long gap
// and no error; a dropped one shows errors.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgelastic/internal/verify"
)

func main() {
	os.Exit(run())
}

// exitOperational matches the oracle's own code for "the run could not be completed",
// which is a different thing from a run that completed and found a violation.
const exitOperational = verify.ExitOperational

func run() int {
	dsn := flag.String("dsn", "", "libpq DSN of the pool's Service")
	ledgerPath := flag.String("ledger", "demo-ledger.log", "durable append-only ledger")
	table := flag.String("table", verify.DefaultTable, "relation to write to and read back")
	writers := flag.Int("writers", 4, "concurrent writer connections")
	duration := flag.Duration("duration", 2*time.Minute, "how long to write for")
	interval := flag.Duration("interval", 20*time.Millisecond, "pause between writes per writer")
	opTimeout := flag.Duration("op-timeout", 60*time.Second,
		"per-write deadline; it has to exceed the pause a queued client is expected to wait")
	baseline := flag.Duration("baseline", 20*time.Second,
		"how long the run is quiet before the migration starts, which is what the cutover "+
			"window is compared against")
	journal := flag.String("journal", "", "optional CSV of every attempt: start,micros,outcome")
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "--dsn is required")
		return exitOperational
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := ensureTable(ctx, *dsn, *table); err != nil {
		fmt.Fprintf(os.Stderr, "preparing %s: %v\n", *table, err)
		return exitOperational
	}

	ledger, prior, err := verify.Open(*ledgerPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening ledger: %v\n", err)
		return exitOperational
	}
	defer func() { _ = ledger.Close() }()

	collector := &attempts{}
	started := time.Now()
	stats, err := verify.RunWorkload(ctx, verify.WorkloadConfig{
		DSN:        *dsn,
		Table:      *table,
		Writers:    *writers,
		Duration:   *duration,
		OpTimeout:  *opTimeout,
		Interval:   *interval,
		FirstValue: verify.NextValue(prior),
		Observe:    collector.add,
	}, ledger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workload: %v\n", err)
		return exitOperational
	}

	report := collector.summarize(started.Add(*baseline))
	report.Stats = stats
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "encoding report: %v\n", err)
		return exitOperational
	}
	fmt.Println(string(encoded))

	if *journal != "" {
		if err := collector.writeJournal(*journal); err != nil {
			fmt.Fprintf(os.Stderr, "writing the journal: %v\n", err)
			return exitOperational
		}
	}
	return verify.ExitPass
}

// attempts collects what every write cost. It is separate from the ledger on purpose: the
// ledger answers whether a write survived and has no clock in it, and this answers what the
// client waited and has no opinion about durability.
type attempts struct {
	mu      sync.Mutex
	records []verify.Attempt
}

func (a *attempts) add(attempt verify.Attempt) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, attempt)
}

// Report is what the demo publishes. Every duration is in milliseconds.
type Report struct {
	Attempts int `json:"attempts"`
	Errors   int `json:"errors"`
	// MaxCommitGapMillis is the longest a client went without a successful write. It is the
	// number the product's claim is about, and it is measured between two writes that both
	// succeeded, so a gap that ended in an error is not counted as a pause.
	MaxCommitGapMillis float64 `json:"maxCommitGapMillis"`
	// MaxWriteMillis is the single longest write, which for a queued client is the one that
	// was waiting at the gate when the flip happened.
	MaxWriteMillis float64 `json:"maxWriteMillis"`

	BeforeP50Millis float64 `json:"beforeP50Millis"`
	BeforeP99Millis float64 `json:"beforeP99Millis"`
	BeforeCount     int     `json:"beforeCount"`
	DuringP50Millis float64 `json:"duringP50Millis"`
	DuringP99Millis float64 `json:"duringP99Millis"`
	DuringCount     int     `json:"duringCount"`

	Stats verify.Stats `json:"stats"`
}

// summarize splits the run at the instant the migration was created.
func (a *attempts) summarize(cutoverFrom time.Time) Report {
	a.mu.Lock()
	defer a.mu.Unlock()

	ordered := slices.Clone(a.records)
	slices.SortFunc(ordered, func(x, y verify.Attempt) int { return x.Started.Compare(y.Started) })

	var before, during []time.Duration
	var commits []time.Time
	report := Report{Attempts: len(ordered)}
	for _, attempt := range ordered {
		if attempt.Outcome == verify.OutcomeFailed {
			report.Errors++
		}
		if attempt.Outcome == verify.OutcomeCommitted {
			commits = append(commits, attempt.Started.Add(attempt.Elapsed))
		}
		if attempt.Started.Before(cutoverFrom) {
			before = append(before, attempt.Elapsed)
		} else {
			during = append(during, attempt.Elapsed)
		}
		report.MaxWriteMillis = max(report.MaxWriteMillis, millis(attempt.Elapsed))
	}

	slices.SortFunc(commits, func(x, y time.Time) int { return x.Compare(y) })
	for index := 1; index < len(commits); index++ {
		report.MaxCommitGapMillis = max(report.MaxCommitGapMillis,
			millis(commits[index].Sub(commits[index-1])))
	}

	report.BeforeCount, report.DuringCount = len(before), len(during)
	report.BeforeP50Millis, report.BeforeP99Millis = percentile(before, 50), percentile(before, 99)
	report.DuringP50Millis, report.DuringP99Millis = percentile(during, 50), percentile(during, 99)
	return report
}

func (a *attempts) writeJournal(path string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if _, err := fmt.Fprintln(file, "startedUnixMicros,elapsedMicros,outcome"); err != nil {
		return err
	}
	for _, attempt := range a.records {
		if _, err := fmt.Fprintf(file, "%d,%d,%s\n",
			attempt.Started.UnixMicro(), attempt.Elapsed.Microseconds(), attempt.Outcome); err != nil {
			return err
		}
	}
	return nil
}

func percentile(samples []time.Duration, which int) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	index := min((which*len(sorted))/100, len(sorted)-1)
	return millis(sorted[index])
}

func millis(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// ensureTable creates the relation the oracle reads back. It is the same shape the verify
// command uses, because the ledger is checked with that command afterwards.
//
// The first connection is retried, and only the first. A forward that has published its
// local port a moment before the fleet will accept on it is the normal start-up race, and
// failing the whole run on it would throw away the migration it was there to watch. Every
// connection the workload itself opens is deliberately not retried: that is the measurement.
func ensureTable(ctx context.Context, dsn, table string) error {
	conn, err := connectWithRetry(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err = conn.Exec(execCtx,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (value bigint PRIMARY KEY)",
			pgx.Identifier{table}.Sanitize()))
	return err
}

func connectWithRetry(ctx context.Context, dsn string) (*pgx.Conn, error) {
	var last error
	for attempt := range 60 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err := pgx.Connect(attemptCtx, dsn)
		cancel()
		if err == nil {
			return conn, nil
		}
		last = err
	}
	return nil, last
}
