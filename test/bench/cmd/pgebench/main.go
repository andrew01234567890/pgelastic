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

// Command pgebench measures a PostgreSQL proxy and says what the measurement is worth.
//
// The second half is the point. A benchmark that reports a number without reporting whether
// the machine could resolve it is how a rewrite gets justified by noise.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/andrew01234567890/pgelastic/internal/bench"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "doctor":
		os.Exit(doctor(os.Args[2:]))
	case "run":
		os.Exit(run(os.Args[2:]))
	case "compare":
		os.Exit(compare(os.Args[2:]))
	case "probe":
		os.Exit(probe(os.Args[2:]))
	case "table":
		os.Exit(table(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: pgebench <subcommand> [flags]

subcommands:
  doctor    report what this machine is and which axes it can decide
  run       measure one target across one workload and write a report
  compare   apply the pre-registered criteria to direct/rust/go reports
  probe     run one query against a DSN and exit non-zero if it fails
  table     render stored reports as a markdown table, one column per arm
`)
}

// table renders the stored reports side by side.
//
// Every cell carries its spread, not just its median. A number whose repetitions disagreed
// should look different on the page from one that repeated, because the reader is being asked
// to draw a conclusion from it.
func table(args []string) int {
	flags := flag.NewFlagSet("table", flag.ExitOnError)
	dir := flags.String("dir", "docs/bench", "directory holding the reports")
	workload := flags.String("workload", "throughput", "which workload to tabulate")
	arms := flags.String("arms", "direct,rust,rust-fence-on,rust-session,rust-1worker,pgbouncer",
		"comma-separated arms, in column order")
	metric := flags.String("metric", "throughput", "throughput, p50, p99 or p999")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	var columns []column
	for name := range strings.SplitSeq(*arms, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		report, err := readReport(fmt.Sprintf("%s/%s-%s.json", *dir, name, *workload))
		if err != nil {
			fmt.Fprintf(os.Stderr, "skipping %s: %v\n", name, err)
			continue
		}
		columns = append(columns, column{name: name, report: report})
	}
	if len(columns) == 0 {
		fmt.Fprintln(os.Stderr, "no reports found")
		return 1
	}

	fmt.Printf("| clients | %s |\n", strings.Join(namesOf(columns), " | "))
	fmt.Printf("|---%s|\n", strings.Repeat("|---", len(columns)))

	for row := range columns[0].report.Points {
		cells := make([]string, 0, len(columns))
		concurrency := columns[0].report.Points[row].Concurrency
		for _, col := range columns {
			if row >= len(col.report.Points) {
				cells = append(cells, "—")
				continue
			}
			cells = append(cells, format(sampleFor(col.report.Points[row], *metric), *metric))
		}
		fmt.Printf("| %d | %s |\n", concurrency, strings.Join(cells, " | "))
	}
	return 0
}

// column is one arm's report, rendered as one column of the table.
type column struct {
	name   string
	report bench.Report
}

func namesOf(columns []column) []string {
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, col.name)
	}
	return names
}

func sampleFor(point bench.Point, metric string) bench.Sample {
	switch metric {
	case "p50":
		return point.P50Micros
	case "p99":
		return point.P99Micros
	case "p999":
		return point.P999Micros
	case "mb":
		return point.MBPerSec
	default:
		return point.Throughput
	}
}

// format marks a cell whose repetitions disagreed, so a reader cannot mistake a number the
// rig could not resolve for one it could.
func format(sample bench.Sample, metric string) string {
	spread := sample.SpreadRatio() * 100
	marker := ""
	if sample.SpreadRatio() > bench.MaxP99SpreadRatio {
		marker = " ⚠"
	}
	if metric == "throughput" {
		return fmt.Sprintf("%.0f (±%.0f%%)%s", sample.Median, spread/2, marker)
	}
	return fmt.Sprintf("%.0f µs (±%.0f%%)%s", sample.Median, spread/2, marker)
}

// probe establishes that a pooler is actually serving.
//
// Readiness for the arms that have no health endpoint. A listening socket is not the same as
// a working backend leg, and starting a measurement against a pooler that cannot reach
// PostgreSQL produces a report full of errors that looks like a finding.
func probe(args []string) int {
	flags := flag.NewFlagSet("probe", flag.ExitOnError)
	dsn := flags.String("dsn", "", "connection string to probe")
	timeout := flags.Duration("timeout", 5*time.Second, "give up after this long")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "probe needs --dsn")
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := bench.Probe(ctx, *dsn); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	return 0
}

// run measures one target and writes a report.
//
// One target per invocation on purpose: the arms have to be started and pinned separately,
// and a driver that switched between them inside one process would be measuring whichever
// one warmed the page cache first.
func run(args []string) int {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	target := flags.String("target", "", "direct, rust or go")
	dsn := flags.String("dsn", "", "connection string for this target")
	workload := flags.String("workload", "throughput", "churn, throughput, latency, bulk or density")
	clients := flags.String("concurrency", "1,8,64,256", "comma-separated client counts to sweep")
	duration := flags.Duration("duration", 120*time.Second, "measured window per repetition")
	warmup := flags.Duration("warmup", 30*time.Second, "discarded window before each repetition")
	repetitions := flags.Int("repetitions", bench.MinRuns, "runs per concurrency point")
	rate := flags.Float64("rate", 0, "offered operations per second (latency workload only)")
	query := flags.String("query", "",
		"statement under test; unset lets the workload choose, which is how bulk gets its own")
	bulkRows := flags.Int("bulk-rows", 20000, "rows per result set for the bulk workload")
	simple := flags.Bool("simple-protocol", false,
		"send statements as Query rather than Parse/Bind/Execute/Sync, "+
			"taking prepared-statement handling out of the comparison")
	pooler := flags.String("pooler", "",
		"container whose CPU and memory to sample; empty for the direct arm, which has none")
	out := flags.String("out", "", "write the JSON report here (default stdout summary only)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *target == "" || *dsn == "" {
		fmt.Fprintln(os.Stderr, "run needs --target and --dsn")
		return 2
	}

	concurrencies, err := parseConcurrency(*clients)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}

	ctx := context.Background()
	env := bench.Capture(ctx)

	report, err := bench.SweepWithProbe(ctx, env, *target, bench.RunConfig{
		Workload:       bench.WorkloadName(*workload),
		DSN:            *dsn,
		Duration:       *duration,
		Warmup:         *warmup,
		Rate:           *rate,
		Query:          *query,
		BulkRows:       *bulkRows,
		SimpleProtocol: *simple,
	}, concurrencies, *repetitions, os.Stderr, *pooler)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}

	bench.WriteSummary(os.Stdout, report)

	if *out != "" {
		if err := writeJSON(*out, report); err != nil {
			fmt.Fprintf(os.Stderr, "writing %s: %v\n", *out, err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "\nwrote %s\n", *out)
	}
	return 0
}

// compare is where the pre-registered thresholds are applied, and the only place a verdict
// is produced. Kept separate from run so a verdict can be recomputed from stored reports
// without re-measuring, and so re-measuring cannot quietly change a threshold.
func compare(args []string) int {
	flags := flag.NewFlagSet("compare", flag.ExitOnError)
	directPath := flags.String("direct", "", "report for the no-proxy baseline (required)")
	rustPath := flags.String("rust", "", "report for the Rust proxy")
	goPath := flags.String("go", "", "report for the Go proxy")
	referencePath := flags.String("pgbouncer", "", "report for the pgbouncer reference arm")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *directPath == "" {
		fmt.Fprintln(os.Stderr, "compare needs --direct: without it the proxy numbers are a ratio between two unknowns")
		return 2
	}

	loaded := map[string]bench.Report{}
	for name, path := range map[string]string{
		"direct": *directPath, "rust": *rustPath, "go": *goPath, "pgbouncer": *referencePath,
	} {
		if path == "" {
			continue
		}
		report, err := readReport(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 1
		}
		loaded[name] = report
	}

	var verdicts, references []bench.Result
	rust, haveRust := loaded["rust"]
	golang, haveGo := loaded["go"]
	reference, haveReference := loaded["pgbouncer"]

	if haveRust && haveGo {
		verdicts = bench.Compare(loaded["direct"], rust, golang)
	}
	if haveReference {
		for _, arm := range []struct {
			name    string
			report  bench.Report
			present bool
		}{{"rust", rust, haveRust}, {"go", golang, haveGo}} {
			if !arm.present {
				continue
			}
			for _, row := range bench.ReferenceRows(reference, arm.report) {
				row.Reason = arm.name + " " + row.Reason
				references = append(references, row)
			}
		}
	}

	if len(verdicts) == 0 && len(references) == 0 {
		fmt.Fprintln(os.Stderr,
			"nothing to compare: supply --rust and --go for a verdict, or --pgbouncer for reference rows")
		return 1
	}

	failed := false
	if len(verdicts) > 0 {
		fmt.Println("pre-registered criteria (Rust versus Go):")
		fmt.Printf("  %-14s %-14s %8s %10s  %s\n", "axis", "verdict", "ratio", "threshold", "reason")
		for _, result := range verdicts {
			fmt.Printf("  %-14s %-14s %8.2f %10.2f  %s\n",
				result.Axis, result.Verdict, result.Ratio, result.Threshold, result.Reason)
			if result.Verdict == bench.Fail {
				failed = true
			}
		}
	}
	if len(references) > 0 {
		fmt.Println("\nreference only, no verdict (pgbouncer does strictly less work):")
		fmt.Printf("  %-14s %8s  %s\n", "axis", "ratio", "detail")
		for _, result := range references {
			fmt.Printf("  %-14s %8.2f  %s\n", result.Axis, result.Ratio, result.Reason)
		}
	}
	if failed {
		return 1
	}
	return 0
}

func parseConcurrency(spec string) ([]int, error) {
	var counts []int
	for field := range strings.SplitSeq(spec, ",") {
		value, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || value < 1 {
			return nil, fmt.Errorf("%q is not a client count", field)
		}
		counts = append(counts, value)
	}
	if len(counts) == 0 {
		return nil, fmt.Errorf("no client counts given")
	}
	return counts, nil
}

func writeJSON(path string, report bench.Report) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func readReport(path string) (bench.Report, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return bench.Report{}, err
	}
	var report bench.Report
	if err := json.Unmarshal(content, &report); err != nil {
		return bench.Report{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	return report, nil
}

// doctor reports the rig and refuses to be silent about what it cannot decide.
//
// Exits non-zero only for conditions that stop a run outright. A rig that cannot decide
// latency still exits zero, because it can decide throughput and density, and the criteria
// package will withhold the latency rows on its own.
func doctor(args []string) int {
	flags := flag.NewFlagSet("doctor", flag.ExitOnError)
	asJSON := flags.Bool("json", false, "emit the environment block as JSON")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	env := bench.Capture(context.Background())

	if *asJSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(env); err != nil {
			fmt.Fprintf(os.Stderr, "encoding the environment: %v\n", err)
			return 1
		}
		return blockers(env)
	}

	report(env)
	return blockers(env)
}

func report(env bench.Environment) {
	fmt.Printf("rig             %s\n", env.Rig)
	fmt.Printf("cpu             %s\n", env.CPUModel)
	fmt.Printf("cores           %d physical / %d logical\n", env.PhysicalCores, env.LogicalCPUs)
	fmt.Printf("memory          %.1f GiB\n", float64(env.MemTotalBytes)/(1<<30))
	fmt.Printf("swap            %.1f GiB total, %.1f GiB in use\n",
		float64(env.SwapTotalBytes)/(1<<30), float64(env.SwapUsedBytes)/(1<<30))
	fmt.Printf("virtualization  %s\n", env.Virtualization)
	fmt.Printf("cgroup          v%d\n", env.CgroupVersion)
	fmt.Printf("go              %s\n", orNone(env.Toolchain.Go))
	fmt.Printf("rustc           %s\n", orNone(env.Toolchain.Rustc))
	fmt.Printf("docker          %s\n", orNone(env.Toolchain.Docker))
	fmt.Printf("commit          %s%s\n", orNone(env.GitSHA), dirtySuffix(env.GitDirty))

	fmt.Println()
	fmt.Println("decidable on this rig:")
	for _, axis := range []bench.Axis{bench.AxisChurn, bench.AxisThroughput, bench.AxisBulk, bench.AxisDensity} {
		fmt.Printf("  %-12s yes\n", axis)
	}
	sampling := "yes"
	if why := bench.SamplingAvailable(); why != "" {
		sampling = "no -- " + why
	}
	fmt.Printf("  %-12s %s\n", "cpu / memory", sampling)

	expectation := "yes"
	if !env.Rig.DecidesLatency() {
		expectation = "only where the measured spread clears the " +
			fmt.Sprintf("%.0f%% gate", bench.MaxP99SpreadRatio*100)
	}
	fmt.Printf("  %-12s %s\n", "p99 / p99.9", expectation)

	if len(env.Warnings) > 0 {
		fmt.Println()
		fmt.Println("warnings:")
		for _, warning := range env.Warnings {
			fmt.Printf("  - %s\n", warning)
		}
	}
}

// blockers are the conditions that stop a run rather than qualify it.
func blockers(env bench.Environment) int {
	code := 0
	if env.Toolchain.Docker == "" {
		fmt.Fprintln(os.Stderr, "\nblocked: docker is required for the pinned container stack")
		code = 1
	}
	if env.PhysicalCores < 14 {
		fmt.Fprintf(os.Stderr,
			"\nblocked: the core budget needs 14 physical cores (loadgen 4, proxy 4, postgres 6), found %d\n",
			env.PhysicalCores)
		code = 1
	}
	return code
}

func orNone(value string) string {
	if value == "" {
		return "(not found)"
	}
	return value
}

func dirtySuffix(dirty bool) string {
	if dirty {
		return " (dirty)"
	}
	return ""
}
