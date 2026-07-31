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

// Package bench holds the parts of the proxy benchmark that have to be trusted: what the
// machine was when a number was taken, and what the number had to beat.
//
// Both live here rather than beside the driver because both are claims about git history. A
// threshold is only pre-registered if it was committed before the result it judges, and a
// result is only reproducible if the machine it came from is recorded next to it. Keeping
// them in a package the driver imports means neither can be edited to fit an outcome without
// the edit showing up in a diff.
package bench

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Rig is what a machine can be trusted to decide.
//
// Not a description of the hardware: a 5950X is a fine benchmarking CPU and a useless one,
// depending on whether anything can hold its clock still. The distinction that matters to a
// verdict is whether the sources of timing noise are controllable, so that is what is named.
type Rig string

const (
	// RigIsolated has a frequency governor, pinnable physical cores, and no hypervisor
	// rescheduling underneath it. Every axis is decidable.
	RigIsolated Rig = "isolated"
	// RigWSL2NonIsolated has no cpufreq tree at all, so clocks cannot be held and boost
	// cannot be disabled, and its "cores" are vCPUs the host may deschedule at any moment.
	// Throughput and memory survive that; a 25% verdict on p99.9 does not.
	RigWSL2NonIsolated Rig = "wsl2-nonisolated"
)

// DecidesLatency reports whether latency numbers from this rig can be trusted on sight.
//
// Advisory only, and deliberately so. It once gated AddedLatency outright, which turned out
// to discard decidable axes: on a virtualised box p99 repeats to within 5-8% while p99.9
// swings 14-19%, and a rig-wide veto refuses both. The verdict is now taken from each
// sample's measured spread against MaxP99SpreadRatio; this reports what to expect before a
// run, and what to caveat after one.
func (r Rig) DecidesLatency() bool { return r == RigIsolated }

// What Capture reports in Environment.Virtualization.
const (
	VirtWSL2 = "wsl2"
	VirtVM   = "vm"
	VirtNone = "none"
)

// Toolchain is the set of compilers and runtimes that produced the binaries under test.
type Toolchain struct {
	Go     string `json:"go"`
	Rustc  string `json:"rustc"`
	Cargo  string `json:"cargo"`
	Docker string `json:"docker"`
}

// Environment is everything about a machine that a result depends on.
//
// Embedded verbatim in every result file. A result whose Environment differs from another's
// is not comparable to it, and the report says so rather than averaging across the two.
type Environment struct {
	Rig            Rig       `json:"rig"`
	Kernel         string    `json:"kernel"`
	Virtualization string    `json:"virtualization"`
	CPUModel       string    `json:"cpuModel"`
	PhysicalCores  int       `json:"physicalCores"`
	LogicalCPUs    int       `json:"logicalCpus"`
	MemTotalBytes  uint64    `json:"memTotalBytes"`
	SwapTotalBytes uint64    `json:"swapTotalBytes"`
	SwapUsedBytes  uint64    `json:"swapUsedBytes"`
	CPUFreqPresent bool      `json:"cpuFreqPresent"`
	CgroupVersion  int       `json:"cgroupVersion"`
	Toolchain      Toolchain `json:"toolchain"`
	GitSHA         string    `json:"gitSha"`
	GitDirty       bool      `json:"gitDirty"`
	// Warnings are conditions that do not invalidate a run but change how it reads.
	Warnings []string `json:"warnings,omitempty"`
}

const (
	procCPUInfo    = "/proc/cpuinfo"
	procMemInfo    = "/proc/meminfo"
	procVersion    = "/proc/version"
	cpuFreqDir     = "/sys/devices/system/cpu/cpu0/cpufreq"
	cgroupV2Marker = "/sys/fs/cgroup/cgroup.controllers"
)

// Capture reads the machine.
//
// Never returns an error for a fact it could not establish: a missing rustc is a warning, not
// a reason to refuse to benchmark Go. The only thing it will not do is guess, because a
// fabricated environment block is worse than an incomplete one.
func Capture(ctx context.Context) Environment {
	env := Environment{
		Kernel:         strings.TrimSpace(readFile(procVersion)),
		CPUFreqPresent: dirExists(cpuFreqDir),
		CgroupVersion:  detectCgroupVersion(),
	}

	if cpu, err := os.Open(procCPUInfo); err == nil {
		defer func() { _ = cpu.Close() }()
		env.CPUModel, env.PhysicalCores, env.LogicalCPUs = parseCPUInfo(cpu)
	}
	if mem, err := os.Open(procMemInfo); err == nil {
		defer func() { _ = mem.Close() }()
		env.MemTotalBytes, env.SwapTotalBytes, env.SwapUsedBytes = parseMemInfo(mem)
	}

	env.Virtualization = detectVirtualization(env.Kernel)
	env.Rig = classify(env.CPUFreqPresent, env.Virtualization)
	env.Toolchain = captureToolchain(ctx)
	env.GitSHA, env.GitDirty = captureGit(ctx)
	env.Warnings = env.warn()

	return env
}

// classify decides what the machine may be trusted to say.
//
// Deliberately conservative in both inputs: an absent cpufreq tree is disqualifying on its
// own, because without it there is no way to even observe whether the clock moved, let alone
// hold it. Detecting WSL separately matters because its thread_siblings_list describes
// synthetic vCPUs rather than host physical pairing, so SMT-aware pinning is a fiction there.
func classify(cpuFreqPresent bool, virtualization string) Rig {
	if !cpuFreqPresent || virtualization == VirtWSL2 {
		return RigWSL2NonIsolated
	}
	return RigIsolated
}

func detectVirtualization(kernel string) string {
	lowered := strings.ToLower(kernel)
	switch {
	case strings.Contains(lowered, "microsoft") || strings.Contains(lowered, "wsl"):
		return VirtWSL2
	case strings.Contains(lowered, "hypervisor"):
		return VirtVM
	default:
		return VirtNone
	}
}

func (e Environment) warn() []string {
	var warnings []string
	if !e.CPUFreqPresent {
		warnings = append(warnings,
			"no cpufreq tree: the CPU governor and boost state are neither controllable nor observable")
	}
	if e.Virtualization == VirtWSL2 {
		warnings = append(warnings,
			"WSL2: reported core topology describes synthetic vCPUs, so SMT-aware pinning is not meaningful")
	}
	if e.SwapUsedBytes > 0 {
		warnings = append(warnings,
			fmt.Sprintf("swap already in use (%d bytes) before the run started", e.SwapUsedBytes))
	}
	if e.GitDirty {
		warnings = append(warnings,
			"working tree is dirty: this result cannot be tied to a commit")
	}
	if e.CgroupVersion == 0 {
		warnings = append(warnings, "no cgroup v2: per-container CPU and memory accounting is unavailable")
	}
	if e.Toolchain.Rustc == "" {
		warnings = append(warnings, "no rustc: the Rust arm cannot be built on this machine")
	}
	if e.Toolchain.Docker == "" {
		warnings = append(warnings, "no docker: the pinned container stack cannot be started")
	}
	return warnings
}

// parseCPUInfo returns the model, the physical core count and the logical CPU count.
//
// "cpu cores" is per-socket and repeats once per logical CPU, so it is read once rather than
// summed. The core budget in the bench plan is expressed in physical cores, and taking the
// logical count for it would oversubscribe every component by two.
func parseCPUInfo(r io.Reader) (model string, physical int, logical int) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), ":")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		switch key {
		case "processor":
			logical++
		case "model name":
			if model == "" {
				model = value
			}
		case "cpu cores":
			if physical == 0 {
				physical, _ = strconv.Atoi(value)
			}
		}
	}
	if physical == 0 {
		physical = logical
	}
	return model, physical, logical
}

// parseMemInfo returns total memory, total swap and swap in use, all in bytes.
func parseMemInfo(r io.Reader) (memTotal, swapTotal, swapUsed uint64) {
	var swapFree uint64
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), ":")
		if !found {
			continue
		}
		kb := parseKilobytes(value)
		switch strings.TrimSpace(key) {
		case "MemTotal":
			memTotal = kb
		case "SwapTotal":
			swapTotal = kb
		case "SwapFree":
			swapFree = kb
		}
	}
	if swapTotal > swapFree {
		swapUsed = swapTotal - swapFree
	}
	return memTotal, swapTotal, swapUsed
}

func parseKilobytes(value string) uint64 {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return 0
	}
	n, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0
	}
	return n * 1024
}

func captureToolchain(ctx context.Context) Toolchain {
	return Toolchain{
		Go:     firstLine(run(ctx, "go", "version")),
		Rustc:  firstLine(run(ctx, "rustc", "--version")),
		Cargo:  firstLine(run(ctx, "cargo", "--version")),
		Docker: firstLine(run(ctx, "docker", "version", "--format", "{{.Server.Version}}")),
	}
}

func captureGit(ctx context.Context) (sha string, dirty bool) {
	sha = firstLine(run(ctx, "git", "rev-parse", "HEAD"))
	return sha, strings.TrimSpace(run(ctx, "git", "status", "--porcelain")) != ""
}

func run(ctx context.Context, name string, args ...string) string {
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(line)
}

func readFile(path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(content)
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func detectCgroupVersion() int {
	if _, err := os.Stat(cgroupV2Marker); err == nil {
		return 2
	}
	if dirExists("/sys/fs/cgroup/cpu") {
		return 1
	}
	return 0
}
