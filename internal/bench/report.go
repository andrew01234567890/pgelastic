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

	// Errors and Overruns are summed across repetitions. A point with either is reported
	// with them rather than quietly averaged, because a saturated or failing cell describes
	// a different system than the one the sweep meant to measure.
	Errors   map[string]int64 `json:"errors,omitempty"`
	Overruns int64            `json:"overruns"`
}

// Report is one target measured across one workload. Written to disk verbatim.
type Report struct {
	Environment Environment  `json:"environment"`
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
	SimpleProtocol bool    `json:"simpleProtocol"`
	Points         []Point `json:"points"`
}

// Sweep runs every concurrency point the requested number of times.
//
// Repetitions are the unit of trust here: one run of a cell is a number, five runs of it are
// a number with a spread, and only the second kind can support or refuse a 25% claim.
func Sweep(ctx context.Context, env Environment, target string, cfg RunConfig,
	concurrencies []int, repetitions int, progress io.Writer,
) (Report, error) {
	if repetitions < MinRuns {
		return Report{}, fmt.Errorf(
			"%d repetitions is fewer than the %d the criteria require; a verdict from it would be withheld anyway",
			repetitions, MinRuns)
	}

	report := Report{
		Environment:    env,
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
	return report, nil
}

func summarizePoint(point *Point) {
	point.Throughput = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.Throughput }))
	point.P50Micros = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.P50Micros }))
	point.P99Micros = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.P99Micros }))
	point.P999Micros = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.P999Micros }))
	point.MBPerSec = summarize(pluck(point.Repetitions, func(c Cell) float64 { return c.MBPerSec }))
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
			// Density is decided from resident memory read outside the process, which the
			// driver cannot see. The connection count is reported so a proxy that refused
			// connections is not credited with having held them cheaply.
			results = append(results, at(rustPoint,
				HigherIsBetter(AxisDensity, rustPoint.Throughput, goPoint.Throughput)))
		case WorkloadLatency:
			directPoint, ok := directPoints[rustPoint.Concurrency]
			if !ok {
				results = append(results, missingPoint(rust.Workload, rustPoint.Concurrency, "direct"))
				continue
			}
			results = append(results,
				at(rustPoint, AddedLatency(rig, AxisP99,
					directPoint.P99Micros, rustPoint.P99Micros, goPoint.P99Micros)),
				at(rustPoint, AddedLatency(rig, AxisP999,
					directPoint.P999Micros, rustPoint.P999Micros, goPoint.P999Micros)))
		}
	}
	return results
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
