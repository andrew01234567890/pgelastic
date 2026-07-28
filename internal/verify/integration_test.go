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

package verify_test

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgelastic/internal/verify"
	"github.com/andrew01234567890/pgelastic/internal/verify/pgtest"
)

const (
	workloadDuration = 2 * time.Second
	workloadWriters  = 4
	minimumCommits   = 5
)

// TestOracleAgainstPostgres calibrates the oracle against a single, healthy PostgreSQL:
// with no chaos it must be silent, and with a violation manufactured behind its back it
// must name the offending value. An oracle never shown to fail is not an oracle.
func TestOracleAgainstPostgres(t *testing.T) {
	dsn := pgtest.Start(t)

	t.Run("a healthy run is clean", func(t *testing.T) {
		f := newRun(t, dsn, "set_clean")

		report := f.check(t)
		if report.Verdict != verify.VerdictPass {
			t.Fatalf("verdict = %s, want PASS:\n%s", report.Verdict, report.Text())
		}
		if report.Counts.Committed < minimumCommits {
			t.Fatalf("only %d commits; the workload did not exercise anything", report.Counts.Committed)
		}
		if report.Counts.Observed != report.Counts.Committed+report.Counts.Recovered {
			t.Fatalf("observed %d rows, but %d committed + %d recovered: the accounting does not close",
				report.Counts.Observed, report.Counts.Committed, report.Counts.Recovered)
		}
		if report.ExitCode() != verify.ExitPass {
			t.Fatalf("exit code = %d, want %d", report.ExitCode(), verify.ExitPass)
		}
	})

	t.Run("a manufactured lost commit is caught and named", func(t *testing.T) {
		f := newRun(t, dsn, "set_lost_commit")

		victim := f.aCommittedValue(t)
		f.exec(t, "DELETE FROM set_lost_commit WHERE value = $1", victim)

		report := f.check(t)
		if !report.DurabilityViolation {
			t.Fatalf("deleting committed value %d did not trip the durability assertion:\n%s", victim, report.Text())
		}
		if !slices.Contains(report.LostCommitted, victim) {
			t.Fatalf("LostCommitted = %v, want it to contain %d", report.LostCommitted, victim)
		}
		if report.Verdict != verify.VerdictFail {
			t.Fatalf("verdict = %s, want FAIL", report.Verdict)
		}
		if report.ExitCode() != verify.ExitDurabilityViolation {
			t.Fatalf("exit code = %d, want %d", report.ExitCode(), verify.ExitDurabilityViolation)
		}
	})

	t.Run("a manufactured phantom write is caught and named", func(t *testing.T) {
		f := newRun(t, dsn, "set_phantom")

		phantom := verify.NextValue(f.records) + 1_000_000
		f.exec(t, "INSERT INTO set_phantom (value) VALUES ($1)", phantom)

		report := f.check(t)
		if !report.UnexpectedWrites {
			t.Fatalf("inserting never-attempted value %d did not trip R ⊆ ATTEMPTED:\n%s", phantom, report.Text())
		}
		if !slices.Contains(report.Unexpected, phantom) {
			t.Fatalf("Unexpected = %v, want it to contain %d", report.Unexpected, phantom)
		}
		if report.DurabilityViolation {
			t.Fatalf("a phantom write must not be reported as a lost commit:\n%s", report.Text())
		}
		if report.ExitCode() != verify.ExitUnexpectedWrite {
			t.Fatalf("exit code = %d, want %d", report.ExitCode(), verify.ExitUnexpectedWrite)
		}
	})

	t.Run("terminated backends produce indeterminates, never lost commits", func(t *testing.T) {
		table := "set_terminated"
		f := newFixture(t, dsn, table)

		chaos, stopChaos := context.WithCancel(context.Background())
		defer stopChaos()
		go terminateBackends(chaos, dsn)

		f.write(t)
		stopChaos()

		report := f.check(t)
		if report.Counts.Indeterminate == 0 {
			t.Fatal("no indeterminate outcomes; the chaos loop never interrupted a writer")
		}
		if report.Verdict != verify.VerdictPass {
			t.Fatalf("verdict = %s, want PASS — terminating a backend must not lose a commit:\n%s",
				report.Verdict, report.Text())
		}
	})

	t.Run("a second run resumes the ledger of the first", func(t *testing.T) {
		f := newRun(t, dsn, "set_resumed")
		first := verify.NextValue(f.records)

		f.write(t)
		if verify.NextValue(f.records) <= first {
			t.Fatalf("resumed run reused values: next was %d, still %d", first, verify.NextValue(f.records))
		}

		report := f.check(t)
		if report.Verdict != verify.VerdictPass {
			t.Fatalf("verdict = %s, want PASS:\n%s", report.Verdict, report.Text())
		}
		if report.Counts.Attempted <= f.firstRunAttempts {
			t.Fatalf("second run attempted %d values in total, no more than the first run's %d",
				report.Counts.Attempted, f.firstRunAttempts)
		}
	})
}

type fixture struct {
	dsn              string
	table            string
	ledgerPath       string
	records          []verify.Record
	firstRunAttempts int
}

func newFixture(t *testing.T, dsn, table string) *fixture {
	t.Helper()
	f := &fixture{dsn: dsn, table: table, ledgerPath: filepath.Join(t.TempDir(), "ledger.log")}

	conn := connect(t, dsn)
	if err := verify.EnsureSchema(t.Context(), conn, table); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return f
}

func newRun(t *testing.T, dsn, table string) *fixture {
	t.Helper()
	f := newFixture(t, dsn, table)
	f.write(t)
	f.firstRunAttempts = len(verify.Summarize(f.records).Attempted)
	return f
}

func (f *fixture) write(t *testing.T) {
	t.Helper()
	ledger, prior, err := verify.Open(f.ledgerPath)
	if err != nil {
		t.Fatalf("opening ledger: %v", err)
	}
	stats, err := verify.RunWorkload(t.Context(), verify.WorkloadConfig{
		DSN:        f.dsn,
		Table:      f.table,
		Writers:    workloadWriters,
		Duration:   workloadDuration,
		OpTimeout:  2 * time.Second,
		FirstValue: verify.NextValue(prior),
	}, ledger)
	if err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("closing ledger: %v", err)
	}
	if stats.Attempted == 0 {
		t.Fatal("the workload attempted nothing")
	}

	records, err := verify.ReadFile(f.ledgerPath)
	if err != nil {
		t.Fatalf("replaying ledger: %v", err)
	}
	f.records = records
}

func (f *fixture) check(t *testing.T) verify.Report {
	t.Helper()
	conn := connect(t, f.dsn)
	observed, err := verify.ReadSet(t.Context(), conn, f.table, true)
	if err != nil {
		t.Fatalf("ReadSet: %v", err)
	}
	return verify.Check(verify.Summarize(f.records), observed, verify.CheckOptions{})
}

func (f *fixture) aCommittedValue(t *testing.T) int64 {
	t.Helper()
	committed := verify.Summarize(f.records).Committed
	values := make([]int64, 0, len(committed))
	for v := range committed {
		values = append(values, v)
	}
	if len(values) < minimumCommits {
		t.Fatalf("only %d committed values to choose a victim from", len(values))
	}
	slices.Sort(values)
	return values[len(values)/2]
}

func (f *fixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	conn := connect(t, f.dsn)
	tag, err := conn.Exec(t.Context(), sql, args...)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("%s affected %d rows, want 1", sql, tag.RowsAffected())
	}
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	return conn
}

// terminateBackends is the stand-in for the chaos harness: it repeatedly kills the
// workload's server-side backends, which is what a failover looks like from a client.
func terminateBackends(ctx context.Context, dsn string) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	const kill = `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	              WHERE datname = current_database() AND pid <> pg_backend_pid()`
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := conn.Exec(ctx, kill); err != nil {
				return
			}
		}
	}
}
