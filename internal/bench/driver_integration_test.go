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

package bench_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/andrew01234567890/pgelastic/internal/bench"
	"github.com/andrew01234567890/pgelastic/internal/verify/pgtest"
)

// These run against a real PostgreSQL because the thing being checked is that the driver
// measures a database rather than measuring itself. A mocked connection would agree with
// whatever the driver believed.
//
// The numbers here are deliberately not asserted tightly: this is a correctness test for the
// harness, not a benchmark. What it establishes is that each workload connects, does the work
// it claims, and reports it in the units it claims.
//
// One container for the whole package rather than one per test. Starting five in sequence
// fails reproducibly on the third under Docker Desktop, and these specs all want the same
// thing - a PostgreSQL to point a workload at - so sharing it removes a failure mode instead
// of papering over one.

const querySelectOne = "SELECT 1"

var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(withPostgres(m))
}

func withPostgres(m *testing.M) int {
	if os.Getenv(pgtest.SkipEnvVar) != "" {
		fmt.Fprintf(os.Stderr, "%s is set, driver specs will skip\n", pgtest.SkipEnvVar)
		return m.Run()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := postgres.Run(ctx, pgtest.Image,
		postgres.WithDatabase("bench"),
		postgres.WithUsername("bench"),
		postgres.WithPassword("bench"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "no PostgreSQL for the driver specs: %v\n", err)
		return m.Run()
	}
	defer func() {
		_ = container.Terminate(context.WithoutCancel(ctx))
	}()

	sharedDSN, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "no connection string for the driver specs: %v\n", err)
	}
	return m.Run()
}

func benchDSN(t *testing.T) string {
	t.Helper()
	if sharedDSN == "" {
		t.Skip("no container runtime available")
	}
	return sharedDSN
}

func TestChurnMeasuresCompleteConnectionLifecycles(t *testing.T) {
	dsn := benchDSN(t)

	cell, err := bench.Run(context.Background(), bench.RunConfig{
		Workload:    bench.WorkloadChurn,
		DSN:         dsn,
		Concurrency: 4,
		Warmup:      200 * time.Millisecond,
		Duration:    2 * time.Second,
		Query:       querySelectOne,
	})
	if err != nil {
		t.Fatal(err)
	}

	if cell.Ops == 0 {
		t.Fatalf("no connection cycles completed; errors: %v", cell.Errors)
	}

	// A timeout and a rejection are not the same finding, and this test used to fail on both.
	// Churn is deliberately unbounded -- it opens connections as fast as the machine allows --
	// so on a shared runner some cycles are still establishing when the measurement window
	// closes. That is a fact about the runner. A PostgreSQL error code or a transport failure
	// against a database that is up is a fact about the code, and stays fatal.
	timeouts := cell.Errors["timeout"] + cell.Errors["canceled"]
	rejected := map[string]int64{}
	for class, count := range cell.Errors {
		if class != "timeout" && class != "canceled" {
			rejected[class] = count
		}
	}
	if len(rejected) > 0 {
		t.Errorf("connection cycles were refused by a healthy database: %v", rejected)
	}
	// Tolerated, but not unboundedly: if most cycles cannot finish, the workload is measuring
	// the clock rather than the connection lifecycle and its percentiles mean nothing.
	if timeouts > cell.Ops {
		t.Errorf("%d cycles timed out against %d that completed, so the window rather than the "+
			"connection lifecycle is what was measured", timeouts, cell.Ops)
	}

	if cell.P50Micros <= 0 {
		t.Error("a connection cycle cannot take zero time")
	}
	t.Logf("churn: %.0f conn/s, p50 %.0fus, p99 %.0fus", cell.Throughput, cell.P50Micros, cell.P99Micros)
}

func TestThroughputBeatsChurnBecauseItKeepsItsConnections(t *testing.T) {
	dsn := benchDSN(t)
	ctx := context.Background()

	shared := bench.RunConfig{
		DSN:         dsn,
		Concurrency: 4,
		Warmup:      200 * time.Millisecond,
		Duration:    2 * time.Second,
		Query:       querySelectOne,
	}

	churn := shared
	churn.Workload = bench.WorkloadChurn
	churnCell, err := bench.Run(ctx, churn)
	if err != nil {
		t.Fatal(err)
	}

	steady := shared
	steady.Workload = bench.WorkloadThroughput
	steadyCell, err := bench.Run(ctx, steady)
	if err != nil {
		t.Fatal(err)
	}

	if steadyCell.Ops == 0 {
		t.Fatalf("no queries completed; errors: %v", steadyCell.Errors)
	}
	// Not a performance claim, a wiring check: if reconnecting per query were not slower
	// than reusing a connection, the churn workload would not be opening connections.
	if steadyCell.Throughput <= churnCell.Throughput {
		t.Errorf("holding connections open (%.0f ops/s) should beat reopening them (%.0f ops/s)",
			steadyCell.Throughput, churnCell.Throughput)
	}
	t.Logf("throughput: %.0f q/s, p50 %.0fus", steadyCell.Throughput, steadyCell.P50Micros)
}

// The open-loop generator has to actually offer the rate it was asked for. If it undershot,
// every latency percentile in the results would describe a lighter load than the one the
// report claims was applied.
func TestTheOfferedRateIsTheRateAchievedWhenTheSystemKeepsUp(t *testing.T) {
	dsn := benchDSN(t)

	const offered = 200.0
	cell, err := bench.Run(context.Background(), bench.RunConfig{
		Workload:    bench.WorkloadLatency,
		DSN:         dsn,
		Concurrency: 8,
		Warmup:      500 * time.Millisecond,
		Duration:    3 * time.Second,
		Rate:        offered,
		Query:       querySelectOne,
	})
	if err != nil {
		t.Fatal(err)
	}

	if cell.Ops == 0 {
		t.Fatalf("no operations completed; errors: %v", cell.Errors)
	}
	if cell.Overruns > 0 {
		t.Logf("note: %d overruns, so this cell was saturated", cell.Overruns)
	}
	if cell.Throughput < offered*0.8 || cell.Throughput > offered*1.2 {
		t.Errorf("achieved %.0f ops/s against an offered %.0f: the generator is not holding its schedule",
			cell.Throughput, offered)
	}
	t.Logf("latency at %.0f/s: p50 %.0fus, p99 %.0fus, p99.9 %.0fus",
		offered, cell.P50Micros, cell.P99Micros, cell.P999Micros)
}

func TestBulkCountsTheBytesItDrained(t *testing.T) {
	dsn := benchDSN(t)

	cell, err := bench.Run(context.Background(), bench.RunConfig{
		Workload:    bench.WorkloadBulk,
		DSN:         dsn,
		Concurrency: 2,
		Warmup:      200 * time.Millisecond,
		Duration:    2 * time.Second,
		BulkRows:    20000,
	})
	if err != nil {
		t.Fatal(err)
	}

	if cell.Bytes == 0 {
		t.Fatalf("no bytes drained; errors: %v", cell.Errors)
	}
	if cell.MBPerSec <= 0 {
		t.Error("bandwidth should be reported for the bulk workload")
	}
	t.Logf("bulk: %.1f MB/s over %d ops", cell.MBPerSec, cell.Ops)
}

func TestDensityHoldsEveryConnectionItOpened(t *testing.T) {
	dsn := benchDSN(t)

	const connections = 20
	cell, err := bench.Run(context.Background(), bench.RunConfig{
		Workload:    bench.WorkloadDensity,
		DSN:         dsn,
		Concurrency: connections,
		Duration:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	if cell.Ops != connections {
		t.Errorf("held %d of %d connections; a proxy that refused some must not be credited "+
			"with cheap memory. errors: %v", cell.Ops, connections, cell.Errors)
	}
}
