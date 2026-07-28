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

// Command pgelastic-verify is the durability oracle: a Patroni-Jepsen `patroni-set`
// checker. See cmd/verify/README.md.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgelastic/internal/verify"
)

const (
	cmdRun   = "run"
	cmdCheck = "check"
)

const usage = `pgelastic-verify — the pgelastic durability oracle

  run    drive the writer fleet, recording every attempt in a durable ledger,
         then optionally check the result
  check  replay an existing ledger and check it against the surviving primary

Exit codes: 0 pass, 1 lost committed transaction, 2 unexpected write, 3 operational error.
`

func printf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type commonFlags struct {
	dsn            string
	ledger         string
	table          string
	jsonOut        bool
	allowReplica   bool
	connectTimeout time.Duration
}

func (c *commonFlags) bind(fs *flag.FlagSet) {
	fs.StringVar(&c.dsn, "dsn", "", "libpq DSN of the endpoint under test (the proxy, or PostgreSQL directly)")
	fs.StringVar(&c.ledger, "ledger", "verify-ledger.log", "path to the durable append-only ledger")
	fs.StringVar(&c.table, "table", verify.DefaultTable, "relation to write to and read back")
	fs.BoolVar(&c.jsonOut, "json", false, "emit only the machine-readable report on stdout")
	fs.BoolVar(&c.allowReplica, "allow-replica", false,
		"permit reading R from a node in recovery (unsafe: lag looks like a lost commit)")
	fs.DurationVar(&c.connectTimeout, "connect-timeout", 30*time.Second, "deadline for the check's own connection")
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printf(stderr, "%s", usage)
		return verify.ExitOperational
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case cmdRun:
		return runWorkload(ctx, args[1:], stdout, stderr)
	case cmdCheck:
		return runCheck(ctx, args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printf(stdout, "%s", usage)
		return verify.ExitPass
	default:
		printf(stderr, "unknown subcommand %q\n\n%s", args[0], usage)
		return verify.ExitOperational
	}
}

func runWorkload(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(cmdRun, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var common commonFlags
	common.bind(fs)
	writers := fs.Int("writers", 8, "number of concurrent writer goroutines")
	duration := fs.Duration("duration", 30*time.Second, "how long to write for")
	opTimeout := fs.Duration("op-timeout", 5*time.Second, "per-INSERT deadline")
	interval := fs.Duration("interval", 0, "optional pause between inserts per writer")
	check := fs.Bool("check", false, "check the ledger against the primary once writing stops")
	if err := fs.Parse(args); err != nil {
		return verify.ExitOperational
	}
	if common.dsn == "" {
		printf(stderr, "--dsn is required\n")
		return verify.ExitOperational
	}

	ledger, prior, err := verify.Open(common.ledger)
	if err != nil {
		printf(stderr, "opening ledger: %v\n", err)
		return verify.ExitOperational
	}
	defer func() { _ = ledger.Close() }()

	if err := ensureSchema(ctx, common); err != nil {
		printf(stderr, "preparing schema: %v\n", err)
		return verify.ExitOperational
	}

	stats, err := verify.RunWorkload(ctx, verify.WorkloadConfig{
		DSN:        common.dsn,
		Table:      common.table,
		Writers:    *writers,
		Duration:   *duration,
		OpTimeout:  *opTimeout,
		Interval:   *interval,
		FirstValue: verify.NextValue(prior),
	}, ledger)
	if err != nil {
		printf(stderr, "workload: %v\n", err)
		return verify.ExitOperational
	}
	if !common.jsonOut {
		printf(stdout, "workload finished: attempted=%d committed=%d indeterminate=%d failed=%d\n",
			stats.Attempted, stats.Committed, stats.Indeterminate, stats.Failed)
	}
	if !*check {
		return verify.ExitPass
	}
	return checkLedger(ctx, common, stdout, stderr)
}

func runCheck(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(cmdCheck, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var common commonFlags
	common.bind(fs)
	if err := fs.Parse(args); err != nil {
		return verify.ExitOperational
	}
	if common.dsn == "" {
		printf(stderr, "--dsn is required\n")
		return verify.ExitOperational
	}
	return checkLedger(ctx, common, stdout, stderr)
}

func checkLedger(ctx context.Context, common commonFlags, stdout, stderr io.Writer) int {
	records, err := verify.ReadFile(common.ledger)
	if err != nil {
		printf(stderr, "replaying ledger: %v\n", err)
		return verify.ExitOperational
	}
	if len(records) == 0 {
		printf(stderr, "ledger %s is empty; nothing to check\n", common.ledger)
		return verify.ExitOperational
	}

	observed, err := readObserved(ctx, common)
	if err != nil {
		printf(stderr, "reading R: %v\n", err)
		return verify.ExitOperational
	}

	report := verify.Check(verify.Summarize(records), observed, verify.CheckOptions{})
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		printf(stderr, "encoding report: %v\n", err)
		return verify.ExitOperational
	}
	if common.jsonOut {
		printf(stdout, "%s\n", encoded)
	} else {
		printf(stdout, "%s%s\n", report.Text(), encoded)
	}
	return report.ExitCode()
}

func ensureSchema(ctx context.Context, common commonFlags) error {
	return withConn(ctx, common, func(ctx context.Context, conn *pgx.Conn) error {
		return verify.EnsureSchema(ctx, conn, common.table)
	})
}

func readObserved(ctx context.Context, common commonFlags) ([]int64, error) {
	var observed []int64
	err := withConn(ctx, common, func(ctx context.Context, conn *pgx.Conn) error {
		var err error
		observed, err = verify.ReadSet(ctx, conn, common.table, !common.allowReplica)
		return err
	})
	return observed, err
}

func withConn(ctx context.Context, common commonFlags, fn func(context.Context, *pgx.Conn) error) error {
	ctx, cancel := context.WithTimeout(ctx, common.connectTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, common.dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	return fn(ctx, conn)
}
