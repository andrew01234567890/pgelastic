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
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// How often the pooler's cgroup is read, and how long a peak therefore describes.
//
// Two ~1 KB pseudo-file reads at this cadence are unmeasurable against a load generator doing
// fifteen thousand operations a second, and eighty samples in a twenty-second window is enough
// that a peak is a peak rather than a lucky draw.
const (
	sampleInterval  = 250 * time.Millisecond
	cgroupRoot      = "/sys/fs/cgroup"
	cgroupSearchMax = 4

	// A cell using this share of its pinned cores is called saturated. Not 1.0: a pooler never
	// reaches its exact quota, and the finding worth flagging is "this number describes the
	// pinning" rather than "this number describes a perfectly busy CPU".
	saturationFraction = 0.9
)

// Resource is what one cell cost the pooler.
//
// A pointer on [`Cell`], so a run with no sampling is visibly absent rather than a block of
// zeros that reads as a measurement of zero.
type Resource struct {
	Source           string `json:"source"`
	CgroupPath       string `json:"cgroupPath,omitempty"`
	Samples          int    `json:"samples"`
	SampleIntervalMs int    `json:"sampleIntervalMs"`

	// CPUCoreSeconds is exact rather than sampled: `cpu.stat`'s `usage_usec` is a cumulative
	// counter, so consumption over a window is a subtraction and carries no sampling error at
	// all. That is the single reason this reads cgroup files rather than `docker stats`,
	// whose CPU figure is already a percentage and cannot be turned back into a total.
	CPUCoreSeconds float64 `json:"cpuCoreSeconds"`
	CPUCoresMean   float64 `json:"cpuCoresMean"`
	CPUCoresPeak   float64 `json:"cpuCoresPeak"`
	CPUMicrosPerOp float64 `json:"cpuMicrosPerOp"`
	// Throttled means the cgroup hit its quota during this cell, so every latency figure in
	// it describes a quota rather than the pooler.
	Throttled bool `json:"throttled"`
	// CoresAvailable is how many CPUs the cpuset actually granted, and Saturated says the cell
	// spent most of them. A saturated cell's throughput is a property of the pinning, not of
	// the pooler, and reading it as the latter is how a ceiling gets attributed to the wrong
	// thing -- which happened once already in docs/bench.md.
	//
	// Saturated == false is NOT "this cell was not CPU-bound". It is only "the cpuset was not
	// the binding constraint". A runtime given fewer threads than the cpuset has cores pegs
	// well below this line: the proxy at 64 clients spends 2.18 of 8 pinned cores and reports
	// false, while being entirely bound by TOKIO_WORKER_THREADS=2. Both numbers are published
	// so that case is visible rather than inferred from the boolean alone.
	CoresAvailable int  `json:"coresAvailable,omitempty"`
	Saturated      bool `json:"saturated"`

	// WorkingSetMean follows `docker stats`' own cgroup-v2 definition -- `memory.current`
	// less `inactive_file` -- so these numbers are continuous with every figure already
	// published from that tool. It includes socket buffers, which are a real per-connection
	// cost and the thing density exists to measure.
	WorkingSetMean float64 `json:"workingSetBytesMean"`
	WorkingSetPeak int64   `json:"workingSetBytesPeak"`
	// IdleWorkingSet is the baseline before this cell's connections were opened, and
	// BytesPerConn the difference divided by the connections actually established. Recorded
	// rather than merely subtracted because it drifts upward across a sweep: a pool keeps its
	// backend links after the clients holding them disconnect, so the second cell starts from
	// a larger floor than the first and reads as cheaper. A rising baseline in the artifact is
	// how that gets noticed instead of being averaged into the answer.
	IdleWorkingSet int64   `json:"idleWorkingSetBytes,omitempty"`
	BytesPerConn   float64 `json:"bytesPerConnection,omitempty"`
}

// cgroupSample is one reading of the cgroup.
type cgroupSample struct {
	at         time.Time
	cpuUsec    uint64
	workingSet int64
	throttled  uint64
}

// ResourceProbe samples one container's cgroup for the length of a sweep.
//
// One probe spans the whole sweep and cells attribute themselves afterwards through
// [Segment]. That ordering is the point: an earlier CPU figure in docs/bench.md was wrong
// because sampling was a manual act somebody had to remember to perform at the right moment,
// and it was performed during the single-client phase. Nobody has to remember anything here.
type ResourceProbe struct {
	path   string
	cores  int
	cpuSet string

	mu      sync.Mutex
	samples []cgroupSample
	stop    chan struct{}
	done    chan struct{}
}

// NewResourceProbe starts sampling `container`, or returns nil if its cgroup cannot be found.
//
// Nil rather than a fallback to a differently-defined source: two arms measured by two
// instruments would produce a comparison that looks valid and is not, which is the failure
// this package exists to prevent. Absence is honest.
func NewResourceProbe(ctx context.Context, container string) (*ResourceProbe, string) {
	if container == "" {
		return nil, "no pooler container was named, so nothing was sampled"
	}
	id := strings.TrimSpace(run(ctx, "docker", "inspect", "--format", "{{.Id}}", container))
	if id == "" {
		return nil, fmt.Sprintf("docker could not identify %q", container)
	}
	path := findCgroup(id)
	if path == "" {
		return nil, fmt.Sprintf("no cgroup v2 directory for %s under %s", container, cgroupRoot)
	}

	probe := &ResourceProbe{
		path:   path,
		cpuSet: strings.TrimSpace(readFile(filepath.Join(path, "cpuset.cpus.effective"))),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	probe.cores = countCPUs(probe.cpuSet)
	go probe.loop()
	return probe, ""
}

// SamplingAvailable says whether this machine can be asked for CPU and memory at all.
//
// A precondition rather than a run-time surprise: without it a sweep completes, writes a report
// whose resource cells are all absent, and the absence is only noticed when the comparison comes
// back INCONCLUSIVE on two axes.
func SamplingAvailable() string {
	controllers := readFile(filepath.Join(cgroupRoot, "cgroup.controllers"))
	if controllers == "" {
		return "no cgroup v2 at " + cgroupRoot
	}
	for _, needed := range []string{"cpu", "memory"} {
		if !slices.Contains(strings.Fields(controllers), needed) {
			return fmt.Sprintf("the %s controller is not enabled at %s", needed, cgroupRoot)
		}
	}
	return ""
}

// findCgroup resolves a container id to its cgroup directory.
//
// By id string rather than through `docker inspect .State.Pid` and `/proc/<pid>/cgroup`, which
// is the usual answer and does not work under Docker Desktop: the engine runs in its own PID
// namespace, so the pid is meaningless here and `cgroup.procs` reads empty.
func findCgroup(id string) string {
	for _, candidate := range []string{
		filepath.Join(cgroupRoot, "docker", id),
		filepath.Join(cgroupRoot, "system.slice", "docker-"+id+".scope"),
	} {
		if _, err := os.Stat(filepath.Join(candidate, "cpu.stat")); err == nil {
			return candidate
		}
	}
	return searchCgroup(id)
}

func searchCgroup(id string) string {
	found := ""
	_ = filepath.WalkDir(cgroupRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil || found != "" || !entry.IsDir() {
			return nil //nolint:nilerr // an unreadable subtree is not a reason to stop looking
		}
		if strings.Count(strings.TrimPrefix(path, cgroupRoot), string(os.PathSeparator)) > cgroupSearchMax {
			return filepath.SkipDir
		}
		if strings.Contains(entry.Name(), id) {
			if _, statErr := os.Stat(filepath.Join(path, "cpu.stat")); statErr == nil {
				found = path
			}
		}
		return nil
	})
	return found
}

func (p *ResourceProbe) loop() {
	defer close(p.done)
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

	p.record()
	for {
		select {
		case <-p.stop:
			p.record()
			return
		case <-ticker.C:
			p.record()
		}
	}
}

func (p *ResourceProbe) record() {
	stat := parseKeyed(filepath.Join(p.path, "cpu.stat"))
	current, _ := strconv.ParseInt(strings.TrimSpace(readFile(filepath.Join(p.path, "memory.current"))), 10, 64)
	inactive := parseKeyed(filepath.Join(p.path, "memory.stat"))["inactive_file"]

	p.mu.Lock()
	defer p.mu.Unlock()
	p.samples = append(p.samples, cgroupSample{
		at:         time.Now(),
		cpuUsec:    stat["usage_usec"],
		workingSet: current - int64(inactive),
		throttled:  stat["nr_throttled"],
	})
}

// Stop ends sampling. Safe to call twice.
func (p *ResourceProbe) Stop() {
	if p == nil {
		return
	}
	select {
	case <-p.stop:
	default:
		close(p.stop)
		<-p.done
	}
}

// CPUSet is the pinning the container actually got, read from the cgroup rather than from
// whatever the caller believes it asked for.
func (p *ResourceProbe) CPUSet() string {
	if p == nil {
		return ""
	}
	return p.cpuSet
}

// Segment attributes the window [from, to) to a cell that completed `ops` operations.
//
// Retrospective, so no workload has to know a probe exists.
func (p *ResourceProbe) Segment(from, to time.Time, ops int64, idleBaseline int64) *Resource {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	var first, last *cgroupSample
	count, peakCores, peakSet := 0, 0.0, int64(0)
	var setTotal float64
	var throttledStart, throttledEnd uint64

	for i := range p.samples {
		current := &p.samples[i]
		if current.at.Before(from) || current.at.After(to) {
			continue
		}
		if first == nil {
			first, throttledStart = current, current.throttled
		}
		if last != nil {
			// A peak over one interval, which is what makes it comparable between runs.
			elapsed := current.at.Sub(last.at).Seconds()
			if elapsed > 0 {
				cores := float64(current.cpuUsec-last.cpuUsec) / 1e6 / elapsed
				peakCores = max(peakCores, cores)
			}
		}
		last, throttledEnd = current, current.throttled
		peakSet = max(peakSet, current.workingSet)
		setTotal += float64(current.workingSet)
		count++
	}
	if first == nil || last == nil || count == 0 {
		return nil
	}

	window := last.at.Sub(first.at).Seconds()
	resource := &Resource{
		Source:           "cgroup2",
		CgroupPath:       p.path,
		Samples:          count,
		SampleIntervalMs: int(sampleInterval / time.Millisecond),
		CPUCoreSeconds:   float64(last.cpuUsec-first.cpuUsec) / 1e6,
		CPUCoresPeak:     peakCores,
		WorkingSetMean:   setTotal / float64(count),
		WorkingSetPeak:   peakSet,
		Throttled:        throttledEnd > throttledStart,
		IdleWorkingSet:   idleBaseline,
		CoresAvailable:   p.cores,
	}
	if window > 0 {
		resource.CPUCoresMean = resource.CPUCoreSeconds / window
	}
	if p.cores > 0 {
		resource.Saturated = resource.CPUCoresMean >= saturationFraction*float64(p.cores)
	}
	if ops > 0 {
		resource.CPUMicrosPerOp = resource.CPUCoreSeconds * 1e6 / float64(ops)
		// Left absent rather than negative when the cell sits below its own floor. That is not a
		// pooler using less than nothing, it is a floor that was read at the wrong moment, and
		// publishing it as a number would make the cheapest arm the one whose baseline was most
		// contaminated.
		if float64(idleBaseline) > 0 && resource.WorkingSetMean > float64(idleBaseline) {
			// Divided by the connections actually established, not by the count requested: a
			// pooler that refused half of them must not be credited with cheap memory.
			resource.BytesPerConn = (resource.WorkingSetMean - float64(idleBaseline)) / float64(ops)
		}
	}
	return resource
}

// WorkingSet reads the current working set once, for a baseline taken before a cell starts.
func (p *ResourceProbe) WorkingSet() int64 {
	if p == nil {
		return 0
	}
	current, _ := strconv.ParseInt(strings.TrimSpace(readFile(filepath.Join(p.path, "memory.current"))), 10, 64)
	inactive := parseKeyed(filepath.Join(p.path, "memory.stat"))["inactive_file"]
	return current - int64(inactive)
}

// parseKeyed reads a cgroup file of "key value" lines.
func parseKeyed(path string) map[string]uint64 {
	values := map[string]uint64{}
	file, err := os.Open(path)
	if err != nil {
		return values
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, raw, found := strings.Cut(scanner.Text(), " ")
		if !found {
			continue
		}
		if parsed, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64); err == nil {
			values[key] = parsed
		}
	}
	return values
}

// countCPUs counts the CPUs in a cpuset list such as "6-9,22-25".
func countCPUs(list string) int {
	total := 0
	for part := range strings.SplitSeq(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		low, high, ranged := strings.Cut(part, "-")
		if !ranged {
			if _, err := strconv.Atoi(part); err == nil {
				total++
			}
			continue
		}
		from, fromErr := strconv.Atoi(low)
		to, toErr := strconv.Atoi(high)
		if fromErr == nil && toErr == nil && to >= from {
			total += to - from + 1
		}
	}
	return total
}
