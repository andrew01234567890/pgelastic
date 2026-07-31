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
	"fmt"
	"io"
	"slices"
	"time"
)

// Target names which side of the comparison a report describes.
//
// TargetDirect is not optional. Without it the other two numbers are a ratio between two
// unknowns: almost all of a query's latency is PostgreSQL, and a proxy comparison that never
// measures PostgreSQL alone cannot say how much of what it saw was the proxy.
const (
	TargetDirect = "direct"
	TargetRust   = "rust"
	TargetGo     = "go"
	// TargetPgBouncer is the external reference. Rust versus Go on its own is a closed
	// comparison: it says which of the two is faster, not whether either is any good. It is
	// never a gate, because pgbouncer is not doing the same job - no epoch fence, no tenant
	// routing, no capacity allocator, no quiesce - and a threshold against it would be
	// scoring two different programs.
	TargetPgBouncer = "pgbouncer"
)

// Point is one concurrency in the sweep, measured over every repetition.
type Point struct {
	Concurrency int     `json:"concurrency"`
	Rate        float64 `json:"offeredRate,omitempty"`
	Repetitions []Cell  `json:"repetitions"`

	Throughput Sample `json:"throughput"`
	P50Micros  Sample `json:"p50Micros"`
	P99Micros  Sample `json:"p99Micros"`
	P999Micros Sample `json:"p999Micros"`
	MBPerSec   Sample `json:"mbPerSecond,omitempty"`

	// What the pooler spent. Empty when nothing was sampled, which `undersampled` turns into
	// a visible INCONCLUSIVE row rather than silence.
	CPUMicrosPerOp  Sample `json:"cpuMicrosPerOp,omitempty"`
	CPUCoresPeak    Sample `json:"cpuCoresPeak,omitempty"`
	WorkingSetBytes Sample `json:"workingSetBytes,omitempty"`
	BytesPerConn    Sample `json:"bytesPerConnection,omitempty"`

	// Errors and Overruns are summed across repetitions. A point with either is reported
	// with them rather than quietly averaged, because a saturated or failing cell describes
	// a different system than the one the sweep meant to measure.
	Errors   map[string]int64 `json:"errors,omitempty"`
	Overruns int64            `json:"overruns"`
}

// Report is one target measured across one workload. Written to disk verbatim.
type Report struct {
	Environment Environment `json:"environment"`
	// RunID names the invocation that produced this report, and is shared by every arm of one
	// run-arms.sh sweep.
	//
	// It exists because drift between invocations was measured at 8-13% against 0-2% within
	// one, which is wider than several of the gaps this harness is asked to adjudicate. Two
	// reports carrying different IDs were taken far enough apart that the machine, not the
	// code, may account for the difference between them - and until this field existed
	// nothing in the artifact recorded which was which.
	RunID     string    `json:"runId,omitempty"`
	StartedAt time.Time `json:"startedAt"`

	Target      string       `json:"target"`
	Workload    WorkloadName `json:"workload"`
	Query       string       `json:"query,omitempty"`
	DurationMs  int64        `json:"durationMs"`
	WarmupMs    int64        `json:"warmupMs"`
	Repetitions int          `json:"repetitions"`
	// SimpleProtocol records how the statements were sent.
	//
	// Two reports that differ here are not comparable and never were, but until this field
	// existed nothing in the artifact said so - they were separated only by a filename
	// convention that a caller could forget to apply.
	SimpleProtocol bool `json:"simpleProtocol"`
	// Pooler names the container that was sampled, and PoolerCPUSet the pinning it actually
	// got - read from its cgroup rather than from whatever the caller believes it asked for,
	// so "pinned to four cores" is a recorded fact rather than a claim.
	Pooler       string   `json:"pooler,omitempty"`
	PoolerCPUSet string   `json:"poolerCpuSet,omitempty"`
	Warnings     []string `json:"warnings,omitempty"`
	Points       []Point  `json:"points"`
}

// Sweep runs every concurrency point the requested number of times.
//
// Repetitions are the unit of trust here: one run of a cell is a number, five runs of it are
// a number with a spread, and only the second kind can support or refuse a 25% claim.
func Sweep(ctx context.Context, env Environment, target string, cfg RunConfig,
	concurrencies []int, repetitions int, progress io.Writer,
) (Report, error) {
	return SweepWithProbe(ctx, env, target, cfg, concurrencies, repetitions, progress, "")
}

// SweepWithProbe is Sweep with the pooler's resources sampled for the whole run.
//
// The probe's lifetime is the sweep's, not a cell's: an earlier CPU figure was wrong because
// sampling was something a person did once, at a moment that turned out to be the
// single-client phase. Cells attribute themselves out of it afterwards.
func SweepWithProbe(ctx context.Context, env Environment, target string, cfg RunConfig,
	concurrencies []int, repetitions int, progress io.Writer, pooler string,
) (Report, error) {
	if repetitions < MinRuns {
		return Report{}, fmt.Errorf(
			"%d repetitions is fewer than the %d the criteria require; a verdict from it would be withheld anyway",
			repetitions, MinRuns)
	}

	var warnings []string
	if pooler != "" {
		probe, why := NewResourceProbe(ctx, pooler)
		if probe == nil {
			warnings = append(warnings, "no resource sampling: "+why)
		}
		defer probe.Stop()
		cfg.Probe = probe
		// Read before the first cell opens anything, while the pooler is genuinely idle.
		cfg.IdleWorkingSet = probe.WorkingSet()
	}

	report := Report{
		Environment:    env,
		RunID:          cfg.RunID,
		StartedAt:      time.Now().UTC(),
		Target:         target,
		Workload:       cfg.Workload,
		Query:          cfg.EffectiveQuery(),
		SimpleProtocol: cfg.SimpleProtocol,
		DurationMs:     cfg.Duration.Milliseconds(),
		WarmupMs:       cfg.Warmup.Milliseconds(),
		Repetitions:    repetitions,
	}

	for _, concurrency := range concurrencies {
		point := Point{Concurrency: concurrency, Rate: cfg.Rate, Errors: map[string]int64{}}
		for run := range repetitions {
			cell := cfg
			cell.Concurrency = concurrency
			measured, err := Run(ctx, cell)
			if err != nil {
				return Report{}, fmt.Errorf("concurrency %d run %d: %w", concurrency, run+1, err)
			}
			point.Repetitions = append(point.Repetitions, measured)
			point.Overruns += measured.Overruns
			for code, count := range measured.Errors {
				point.Errors[code] += count
			}
			if progress != nil {
				_, _ = fmt.Fprintf(progress, "  %s c=%-5d run %d/%d  %.0f ops/s  p99 %.0fus\n",
					cfg.Workload, concurrency, run+1, repetitions, measured.Throughput, measured.P99Micros)
			}
		}
		summarizePoint(&point)
		if len(point.Errors) == 0 {
			point.Errors = nil
		}
		report.Points = append(report.Points, point)
	}
	if cfg.Probe != nil {
		report.Pooler = pooler
		report.PoolerCPUSet = cfg.Probe.CPUSet()
	}
	report.Warnings = warnings
	return report, nil
}

func summarizePoint(point *Point) {
	point.Throughput = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.Throughput }))
	point.P50Micros = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.P50Micros }))
	point.P99Micros = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.P99Micros }))
	point.P999Micros = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.P999Micros }))
	point.MBPerSec = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.MBPerSec }))
	point.CPUMicrosPerOp = summarizeResource(point.Repetitions, func(r *Resource) float64 { return r.CPUMicrosPerOp })
	point.CPUCoresPeak = summarizeResource(point.Repetitions, func(r *Resource) float64 { return r.CPUCoresPeak })
	point.WorkingSetBytes = summarizeResource(point.Repetitions, func(r *Resource) float64 { return r.WorkingSetMean })
	point.BytesPerConn = summarizeResource(point.Repetitions, func(r *Resource) float64 { return r.BytesPerConn })
}

// summarizeResource skips cells with no sampling, so a partly-sampled sweep reports the runs
// it has rather than a set of zeros mixed in with them.
func summarizeResource(cells []Cell, of func(*Resource) float64) Sample {
	values := make([]float64, 0, len(cells))
	for _, cell := range cells {
		if cell.Resource != nil {
			values = append(values, of(cell.Resource))
		}
	}
	return summarize(values)
}

func pluck(cells []Cell, of func(Cell) float64) []float64 {
	values := make([]float64, 0, len(cells))
	for _, cell := range cells {
		values = append(values, of(cell))
	}
	return values
}

// summarize reduces repetitions to a median and the range they spanned.
//
// Median rather than mean: on a rig where the hypervisor may deschedule a vCPU, one
// repetition in five can be arbitrarily bad, and a mean carries that into the reported
// number while a median does not. The range is kept because it is what decides whether two
// samples are distinguishable at all.
func summarize(values []float64) Sample {
	if len(values) == 0 {
		return Sample{}
	}
	ordered := slices.Clone(values)
	slices.Sort(ordered)

	sample := Sample{
		Min:  ordered[0],
		Max:  ordered[len(ordered)-1],
		Runs: len(ordered),
	}
	middle := len(ordered) / 2
	if len(ordered)%2 == 1 {
		sample.Median = ordered[middle]
	} else {
		sample.Median = (ordered[middle-1] + ordered[middle]) / 2
	}
	return sample
}

// Compare applies the pre-registered criteria to a pair of reports.
//
// Direct is required rather than optional, because the latency rows are expressed as the
// latency each proxy adds over talking to PostgreSQL itself.
func Compare(direct, rust, golang Report) []Result {
	if mismatch := incomparable(direct, rust, golang); mismatch != nil {
		return []Result{*mismatch}
	}

	boundary := crossRun(direct, rust, golang)
	var results []Result
	rig := rust.Environment.Rig
	goPoints := byConcurrency(golang)
	directPoints := byConcurrency(direct)

	for _, rustPoint := range rust.Points {
		// Paired by client count, not by position. Two sweeps that visited different
		// concurrencies would otherwise be compared point-for-point down the list, reporting
		// a verdict on 64 clients against 256.
		goPoint, ok := goPoints[rustPoint.Concurrency]
		if !ok {
			results = append(results, missingPoint(rust.Workload, rustPoint.Concurrency, "go"))
			continue
		}

		// Emitted for every workload, and unconditionally: an unsampled arm yields a Sample
		// with no runs, which `undersampled` reports as INCONCLUSIVE. A visible refusal is
		// right; silence is how a missing measurement gets forgotten.
		results = append(results,
			at(rustPoint, LowerIsBetter(AxisCPUPerOp, rustPoint.CPUMicrosPerOp, goPoint.CPUMicrosPerOp)),
			at(rustPoint, LowerIsBetter(AxisMemory, rustPoint.WorkingSetBytes, goPoint.WorkingSetBytes)))

		switch rust.Workload {
		case WorkloadChurn:
			results = append(results, at(rustPoint,
				HigherIsBetter(AxisChurn, rustPoint.Throughput, goPoint.Throughput)))
		case WorkloadThroughput:
			results = append(results, at(rustPoint,
				HigherIsBetter(AxisThroughput, rustPoint.Throughput, goPoint.Throughput)))
		case WorkloadBulk:
			results = append(results, at(rustPoint,
				HigherIsBetter(AxisBulk, rustPoint.MBPerSec, goPoint.MBPerSec)))
		case WorkloadDensity:
			// Resident bytes per idle connection, lower being better - which is what
			// criteria.go always documented this axis as and what the shipped path did not
			// do. It fed the establish rate into HigherIsBetter instead, and since both arms
			// establish over the same window that ratio was structurally ~1.00: a guaranteed
			// PASS carrying no information, which is worse than no row at all.
			results = append(results, at(rustPoint,
				LowerIsBetter(AxisDensity, rustPoint.BytesPerConn, goPoint.BytesPerConn)))
		case WorkloadLatency:
			directPoint, ok := directPoints[rustPoint.Concurrency]
			if !ok {
				results = append(results, missingPoint(rust.Workload, rustPoint.Concurrency, "direct"))
				continue
			}
			// The latency axes subtract the direct arm from the pooled ones. Across a run
			// boundary that subtraction crosses the drift, and drift is larger than the
			// added latency being measured - so the difference would be mostly the gap
			// between two invocations, reported as a property of the proxy.
			p99 := at(rustPoint, AddedLatency(rig, AxisP99,
				directPoint.P99Micros, rustPoint.P99Micros, goPoint.P99Micros))
			p999 := at(rustPoint, AddedLatency(rig, AxisP999,
				directPoint.P999Micros, rustPoint.P999Micros, goPoint.P999Micros))
			if boundary != "" {
				p99, p999 = withheld(p99), withheld(p999)
			}
			results = append(results, p99, p999)
		}
	}

	if boundary != "" {
		for i := range results {
			results[i].Reason = appendReason(results[i].Reason, boundary)
		}
	}
	return results
}

// crossRun reports that these arms came from different invocations, or "" if they did not.
//
// A note on every row rather than a refusal. Comparing arms across runs is a real thing to
// want - the reference arm is often measured once and kept - and the drift is a qualification
// on the number, not a reason to withhold it. The exception is the latency axes, which
// subtract across the boundary and are withheld outright.
func crossRun(reports ...Report) string {
	ids := map[string]bool{}
	anonymous := false
	for _, report := range reports {
		// An empty workload is the zero Report that Compare is given when an arm is absent,
		// not an arm that was measured without an identity.
		if report.Workload == "" {
			continue
		}
		if report.RunID == "" {
			anonymous = true
			continue
		}
		ids[report.RunID] = true
	}

	if len(ids) > 1 {
		return fmt.Sprintf("these arms come from %d separate invocations, between which this rig "+
			"drifts by more than it varies within one", len(ids))
	}
	// Silence here would imply these were measured together, which is exactly what cannot be
	// established: a report written before run identity existed carries no way to tell.
	if anonymous && len(ids) > 0 {
		return "one of these arms predates run identity, so whether it was measured alongside " +
			"the others cannot be established"
	}
	return ""
}

// withheld turns a result into a refusal, keeping both the ratio and the reason the axis gave
// so a reader can still see what it would have said and why.
//
// The cross-run note itself is not added here; every row gets it once, at the end.
func withheld(result Result) Result {
	result.Verdict = Inconclusive
	return result
}

func appendReason(existing, addition string) string {
	if existing == "" {
		return addition
	}
	return existing + "; " + addition
}

// incomparable refuses two reports that are not measuring the same thing.
//
// The package already promises that a result whose environment differs from another's is not
// comparable to it and that the report says so. Nothing checked it, so a churn report passed
// as one arm against a throughput report as the other produced verdicts.
func incomparable(direct, rust, golang Report) *Result {
	for _, other := range []Report{direct, golang} {
		if other.Workload != "" && other.Workload != rust.Workload {
			return &Result{
				Verdict: Inconclusive,
				Reason: fmt.Sprintf("these arms measured different workloads (%s and %s), "+
					"so there is nothing to compare", rust.Workload, other.Workload),
			}
		}
		if other.SimpleProtocol != rust.SimpleProtocol {
			return &Result{
				Verdict: Inconclusive,
				Reason: "these arms used different wire protocols, so one of them was doing " +
					"prepared-statement work the other was not",
			}
		}
	}
	return nil
}

func byConcurrency(report Report) map[int]Point {
	points := make(map[int]Point, len(report.Points))
	for _, point := range report.Points {
		points[point.Concurrency] = point
	}
	return points
}

func missingPoint(workload WorkloadName, concurrency int, arm string) Result {
	return Result{
		Concurrency: concurrency,
		Verdict:     Inconclusive,
		Reason: fmt.Sprintf("the %s arm did not sweep %d clients for %s",
			arm, concurrency, workload),
	}
}

// at labels a result with the client count it describes, so a table of them is readable.
func at(point Point, result Result) Result {
	result.Concurrency = point.Concurrency
	return result
}

// Against measures an arm against a reference implementation and deliberately produces no
// verdict.
//
// The ratio is worth knowing - it is what says whether the Rust-versus-Go question matters
// at all - but pgbouncer does strictly less work, so a pass or fail against it would be
// scoring two different programs against one threshold.
func Against(axis Axis, reference, arm Sample) Result {
	result := Result{Axis: axis, Verdict: VerdictReference}
	if reference.Median == 0 {
		result.Reason = "the reference arm measured zero"
		return result
	}
	result.Ratio = arm.Median / reference.Median
	result.Reason = fmt.Sprintf("%.2fx pgbouncer", result.Ratio)
	return result
}

// ReferenceRows compares one arm to the reference across whatever axis its workload measures.
func ReferenceRows(reference, arm Report) []Result {
	if reference.Workload != arm.Workload || reference.SimpleProtocol != arm.SimpleProtocol {
		return []Result{{
			Verdict: Inconclusive,
			Reason:  "the reference arm did not measure the same thing as this one",
		}}
	}

	var results []Result
	armPoints := byConcurrency(arm)
	for _, referencePoint := range reference.Points {
		// Paired by client count for the same reason Compare is: two sweeps that visited
		// different concurrencies would otherwise be lined up by position.
		armPoint, ok := armPoints[referencePoint.Concurrency]
		if !ok {
			continue
		}
		axis, refSample, armSample := AxisThroughput, referencePoint.Throughput, armPoint.Throughput
		switch reference.Workload {
		case WorkloadChurn:
			axis = AxisChurn
		case WorkloadBulk:
			axis = AxisBulk
			refSample, armSample = referencePoint.MBPerSec, armPoint.MBPerSec
		}
		results = append(results, at(referencePoint, Against(axis, refSample, armSample)))
	}
	return results
}

// WriteSummary prints a report in the shape a person reads before deciding anything.
func WriteSummary(w io.Writer, report Report) {
	_, _ = fmt.Fprintf(w, "\n%s / %s  (%d repetitions of %s, %s warmup)\n",
		report.Target, report.Workload, report.Repetitions,
		time.Duration(report.DurationMs)*time.Millisecond,
		time.Duration(report.WarmupMs)*time.Millisecond)
	_, _ = fmt.Fprintf(w, "rig: %s\n", report.Environment.Rig)
	_, _ = fmt.Fprintf(w, "%-8s %14s %12s %12s %12s %10s\n", "clients", "ops/s", "p50 us", "p99 us", "p99.9 us", "errors")

	for _, point := range report.Points {
		errors := int64(0)
		for _, count := range point.Errors {
			errors += count
		}
		_, _ = fmt.Fprintf(w, "%-8d %14.0f %12.0f %12.0f %12.0f %10d\n",
			point.Concurrency, point.Throughput.Median, point.P50Micros.Median,
			point.P99Micros.Median, point.P999Micros.Median, errors)
	}
}
