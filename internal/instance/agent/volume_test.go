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

package agent

import "testing"

// status.storage.used had no producer at all, so ObservedUtilizationPercent sat at zero and
// AutoActionStorageExpand - which requires a figure above zero - could never fire. The bytes
// were being measured on every tick and thrown away except for one boolean.
func TestUsedBytesCountsWhatPostgresCannotHaveBack(t *testing.T) {
	// Total minus *available*, not minus free: the blocks a filesystem reserves for root are
	// not room PostgreSQL can write into, so counting them as free reports a volume as
	// emptier than the postmaster can treat it. That is the same mistake Full() exists to
	// avoid, and it has to be the same arithmetic or the two disagree about one filesystem.
	usage := VolumeUsage{TotalBytes: 1000, FreeBytes: 250}
	if got := usage.UsedBytes(); got != 750 {
		t.Errorf("UsedBytes() = %d, want 750", got)
	}
}

// An unmeasurable volume reports zero rather than a wrong number, and the operator treats
// zero as "no measurement" rather than as an empty disk.
func TestAnUnmeasuredVolumeReportsNothingRatherThanEmpty(t *testing.T) {
	if got := (VolumeUsage{}).UsedBytes(); got != 0 {
		t.Errorf("UsedBytes() = %d for an unmeasured volume, want 0", got)
	}
}
