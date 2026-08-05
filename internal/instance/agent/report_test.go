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

// The agent measures all three of these on every observe pass and used to report them only
// over its HTTP surface, which nothing in the control plane calls. The pool's planner reads
// them off the CR: status.capacity.inUse is summed from clientBackends, so it answered zero
// for every instance however loaded it was, and storage autoscaling compares dataUsedBytes
// against the volume, so it could never fire whatever the disk was doing.
func TestTheMemberEntryCarriesWhatThePlannerReads(t *testing.T) {
	reporter := Reporter{Instance: "pg", Member: holderOne, Session: "s"}
	observation := MemberObservation{
		ClientBackends: 42,
		DataUsedBytes:  9_000_000,
		WALUsedBytes:   1_500_000,
	}

	entry := reporter.memberEntry(observation, true, "")

	for name, want := range map[string]any{
		"clientBackends": int64(42),
		"dataUsedBytes":  int64(9_000_000),
		"walUsedBytes":   int64(1_500_000),
	} {
		got, present := entry[name]
		if !present {
			t.Errorf("%s is never reported, so whatever reads it off the CR reads zero", name)
			continue
		}
		if got != want {
			t.Errorf("%s reported %v, want %v", name, got, want)
		}
	}
}
