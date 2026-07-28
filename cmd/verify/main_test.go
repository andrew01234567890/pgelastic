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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgelastic/internal/verify"
	"github.com/andrew01234567890/pgelastic/internal/verify/pgtest"
)

const (
	flagDSN    = "--dsn"
	flagLedger = "--ledger"
	flagTable  = "--table"
	flagJSON   = "--json"
)

func TestCLIUsageErrors(t *testing.T) {
	ledger := filepath.Join(t.TempDir(), "ledger.log")

	tests := []struct {
		name string
		args []string
	}{
		{name: "no subcommand", args: nil},
		{name: "unknown subcommand", args: []string{"frobnicate"}},
		{name: "check without a dsn", args: []string{cmdCheck, flagLedger, ledger}},
		{name: "run without a dsn", args: []string{cmdRun, flagLedger, ledger}},
		{name: "check against an empty ledger", args: []string{cmdCheck, flagDSN, "postgres:///nowhere", flagLedger, ledger}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if code, _ := invoke(t, tc.args); code != verify.ExitOperational {
				t.Fatalf("exit code = %d, want %d", code, verify.ExitOperational)
			}
		})
	}
}

func TestCLIHelpSucceeds(t *testing.T) {
	code, stdout := invoke(t, []string{"--help"})
	if code != verify.ExitPass {
		t.Fatalf("exit code = %d, want %d", code, verify.ExitPass)
	}
	if !strings.Contains(stdout, "durability oracle") {
		t.Fatalf("help text = %q", stdout)
	}
}

func TestCLIAgainstPostgres(t *testing.T) {
	dsn := pgtest.Start(t)

	t.Run("run --check exits zero on a healthy database", func(t *testing.T) {
		ledger := filepath.Join(t.TempDir(), "ledger.log")
		code, stdout := invoke(t, workloadArgs(dsn, ledger, "set_cli_clean", "--check"))

		report := decodeReport(t, stdout)
		if report.Verdict != verify.VerdictPass {
			t.Fatalf("verdict = %s, want PASS: %s", report.Verdict, stdout)
		}
		if code != verify.ExitPass {
			t.Fatalf("exit code = %d, want %d", code, verify.ExitPass)
		}
		if report.Counts.Committed == 0 {
			t.Fatal("the workload committed nothing")
		}
	})

	t.Run("check exits one and names a lost commit", func(t *testing.T) {
		const table = "set_cli_lost"
		ledger := filepath.Join(t.TempDir(), "ledger.log")
		if code, _ := invoke(t, workloadArgs(dsn, ledger, table)); code != verify.ExitPass {
			t.Fatalf("workload exit code = %d", code)
		}

		victim := aCommittedValue(t, ledger)
		execOne(t, dsn, "DELETE FROM "+table+" WHERE value = $1", victim)

		code, stdout := invoke(t, []string{cmdCheck, flagDSN, dsn, flagLedger, ledger, flagTable, table, flagJSON})
		report := decodeReport(t, stdout)
		if !slices.Contains(report.LostCommitted, victim) {
			t.Fatalf("LostCommitted = %v, want it to contain the deleted value %d", report.LostCommitted, victim)
		}
		if !report.DurabilityViolation || report.Verdict != verify.VerdictFail {
			t.Fatalf("report did not fail: %s", stdout)
		}
		if code != verify.ExitDurabilityViolation {
			t.Fatalf("exit code = %d, want %d", code, verify.ExitDurabilityViolation)
		}
	})

	t.Run("check exits two on a phantom write", func(t *testing.T) {
		const table = "set_cli_phantom"
		ledger := filepath.Join(t.TempDir(), "ledger.log")
		if code, _ := invoke(t, workloadArgs(dsn, ledger, table)); code != verify.ExitPass {
			t.Fatalf("workload exit code = %d", code)
		}

		records, err := verify.ReadFile(ledger)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		phantom := verify.NextValue(records) + 1_000_000
		execOne(t, dsn, "INSERT INTO "+table+" (value) VALUES ($1)", phantom)

		code, stdout := invoke(t, []string{cmdCheck, flagDSN, dsn, flagLedger, ledger, flagTable, table, flagJSON})
		report := decodeReport(t, stdout)
		if !slices.Contains(report.Unexpected, phantom) {
			t.Fatalf("Unexpected = %v, want it to contain %d", report.Unexpected, phantom)
		}
		if code != verify.ExitUnexpectedWrite {
			t.Fatalf("exit code = %d, want %d", code, verify.ExitUnexpectedWrite)
		}
	})

	t.Run("a killed run is resumed and checked by a second invocation", func(t *testing.T) {
		const table = "set_cli_resumed"
		ledger := filepath.Join(t.TempDir(), "ledger.log")
		if code, _ := invoke(t, workloadArgs(dsn, ledger, table)); code != verify.ExitPass {
			t.Fatalf("first workload exit code = %d", code)
		}
		appendTornRecord(t, ledger)

		code, stdout := invoke(t, workloadArgs(dsn, ledger, table, "--check"))
		report := decodeReport(t, stdout)
		if report.Verdict != verify.VerdictPass {
			t.Fatalf("verdict = %s, want PASS: %s", report.Verdict, stdout)
		}
		if code != verify.ExitPass {
			t.Fatalf("exit code = %d, want %d", code, verify.ExitPass)
		}
	})
}

func workloadArgs(dsn, ledger, table string, extra ...string) []string {
	args := make([]string, 0, 12+len(extra))
	args = append(args,
		cmdRun,
		flagDSN, dsn,
		flagLedger, ledger,
		flagTable, table,
		"--writers", "4",
		"--duration", "2s",
		flagJSON,
	)
	return append(args, extra...)
}

func invoke(t *testing.T, args []string) (int, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	t.Logf("args=%v exit=%d stderr=%s", args, code, stderr.String())
	return code, stdout.String()
}

func decodeReport(t *testing.T, stdout string) verify.Report {
	t.Helper()
	var report verify.Report
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("decoding report %q: %v", stdout, err)
	}
	return report
}

func aCommittedValue(t *testing.T, ledgerPath string) int64 {
	t.Helper()
	records, err := verify.ReadFile(ledgerPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	values := make([]int64, 0, len(records))
	for v := range verify.Summarize(records).Committed {
		values = append(values, v)
	}
	if len(values) == 0 {
		t.Fatal("the ledger holds no committed value to delete")
	}
	slices.Sort(values)
	return values[len(values)/2]
}

// appendTornRecord stands in for the verifier being killed part-way through an append.
func appendTornRecord(t *testing.T, ledgerPath string) {
	t.Helper()
	f, err := os.OpenFile(ledgerPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("opening ledger: %v", err)
	}
	if _, err := f.WriteString("ATTEMPTED 99"); err != nil {
		t.Fatalf("appending: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("closing ledger: %v", err)
	}
}

func execOne(t *testing.T, dsn, sql string, args ...any) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(t.Context())) }()

	tag, err := conn.Exec(t.Context(), sql, args...)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("%s affected %d rows, want 1", sql, tag.RowsAffected())
	}
}
