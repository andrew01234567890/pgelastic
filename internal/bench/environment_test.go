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
	"strings"
	"testing"
)

const cpuInfoFixture = `processor	: 0
vendor_id	: AuthenticAMD
cpu family	: 25
model name	: AMD Ryzen 9 5950X 16-Core Processor
siblings	: 32
cpu cores	: 16

processor	: 1
model name	: AMD Ryzen 9 5950X 16-Core Processor
siblings	: 32
cpu cores	: 16

processor	: 2
model name	: AMD Ryzen 9 5950X 16-Core Processor
siblings	: 32
cpu cores	: 16
`

const memInfoFixture = `MemTotal:       16307040 kB
MemFree:          741852 kB
SwapTotal:      134217728 kB
SwapFree:       134217728 kB
`

func TestParseCPUInfoReadsPhysicalCoresNotLogical(t *testing.T) {
	model, physical, logical := parseCPUInfo(strings.NewReader(cpuInfoFixture))

	if model != "AMD Ryzen 9 5950X 16-Core Processor" {
		t.Errorf("model = %q", model)
	}
	if physical != 16 {
		t.Errorf("physical cores = %d, want 16 read once rather than summed per logical CPU", physical)
	}
	if logical != 3 {
		t.Errorf("logical CPUs = %d, want 3 (one per processor line in the fixture)", logical)
	}
}

func TestParseCPUInfoFallsBackToLogicalWhenCoreCountIsAbsent(t *testing.T) {
	_, physical, logical := parseCPUInfo(strings.NewReader("processor\t: 0\nprocessor\t: 1\n"))

	if physical != logical {
		t.Errorf("physical = %d, want it to fall back to logical = %d", physical, logical)
	}
}

func TestParseMemInfoComputesSwapInUse(t *testing.T) {
	memTotal, swapTotal, swapUsed := parseMemInfo(strings.NewReader(memInfoFixture))

	if memTotal != 16307040*1024 {
		t.Errorf("memTotal = %d bytes", memTotal)
	}
	if swapTotal != 134217728*1024 {
		t.Errorf("swapTotal = %d bytes", swapTotal)
	}
	if swapUsed != 0 {
		t.Errorf("swapUsed = %d, want 0 when SwapFree equals SwapTotal", swapUsed)
	}
}

func TestParseMemInfoReportsSwapAlreadyInUse(t *testing.T) {
	_, _, swapUsed := parseMemInfo(strings.NewReader("SwapTotal:  1000 kB\nSwapFree:  400 kB\n"))

	if swapUsed != 600*1024 {
		t.Errorf("swapUsed = %d bytes, want 600 kB worth", swapUsed)
	}
}

func TestClassifyDisqualifiesARigWithNoFrequencyControl(t *testing.T) {
	cases := []struct {
		name           string
		cpuFreqPresent bool
		virtualization string
		want           Rig
	}{
		{"bare metal with a governor", true, VirtNone, RigIsolated},
		{"no cpufreq tree at all", false, VirtNone, RigWSL2NonIsolated},
		{"WSL2 even if a cpufreq tree appears", true, VirtWSL2, RigWSL2NonIsolated},
		{"neither", false, VirtWSL2, RigWSL2NonIsolated},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classify(c.cpuFreqPresent, c.virtualization); got != c.want {
				t.Errorf("classify(%v, %q) = %q, want %q", c.cpuFreqPresent, c.virtualization, got, c.want)
			}
		})
	}
}

// DecidesLatency is advisory: it says what to expect before a run and what to caveat after
// one. The verdict itself comes from each sample's measured spread, so this only has to
// describe the rig honestly, not gate anything.
func TestTheRigAdvertisesWhetherItsLatencyNumbersNeedCaveats(t *testing.T) {
	if !RigIsolated.DecidesLatency() {
		t.Error("an isolated rig should advertise trustworthy latency")
	}
	if RigWSL2NonIsolated.DecidesLatency() {
		t.Error("a rig with no clock control should advertise that its latency needs caveats")
	}
}

func TestDetectVirtualization(t *testing.T) {
	cases := map[string]string{
		"Linux version 6.6.87.2-microsoft-standard-WSL2": VirtWSL2,
		"Linux version 6.1.0-generic":                    VirtNone,
	}
	for kernel, want := range cases {
		if got := detectVirtualization(kernel); got != want {
			t.Errorf("detectVirtualization(%q) = %q, want %q", kernel, got, want)
		}
	}
}

func TestWarningsNameEveryUncontrolledCondition(t *testing.T) {
	env := Environment{
		CPUFreqPresent: false,
		Virtualization: VirtWSL2,
		SwapUsedBytes:  1024,
		GitDirty:       true,
		CgroupVersion:  0,
	}

	warnings := strings.Join(env.warn(), "\n")

	for _, want := range []string{"cpufreq", "WSL2", "swap", "dirty", "cgroup", "rustc", "docker"} {
		if !strings.Contains(warnings, want) {
			t.Errorf("warnings should mention %q, got:\n%s", want, warnings)
		}
	}
}
