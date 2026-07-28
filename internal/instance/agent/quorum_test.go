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

import (
	"slices"
	"testing"
)

// memberThree completes the three-member set the lease specs already name two of.
const memberThree = "pg-3"

var instanceMembers = []string{holderOne, holderTwo, memberThree}

func TestTheQuorumSetGrowsAsStandbysStartStreaming(t *testing.T) {
	converged := ConvergeSyncMembers(nil, []string{holderTwo}, instanceMembers, true)

	if !slices.Equal(converged, []string{holderTwo}) {
		t.Fatalf("converged to %v", converged)
	}
}

func TestARequiredQuorumSetNeverShrinks(t *testing.T) {
	// pg-3 has stopped streaming. Dropping it would turn a stalled commit into a silently
	// asynchronous one, and nobody outside the server could tell afterwards.
	converged := ConvergeSyncMembers([]string{holderTwo, memberThree}, []string{holderTwo}, instanceMembers, true)

	if !slices.Equal(converged, []string{holderTwo, memberThree}) {
		t.Fatalf("converged to %v", converged)
	}
}

func TestAPreferredQuorumSetFollowsWhatIsStreaming(t *testing.T) {
	converged := ConvergeSyncMembers([]string{holderTwo, memberThree}, []string{holderTwo}, instanceMembers, false)

	if !slices.Equal(converged, []string{holderTwo}) {
		t.Fatalf("degrading to asynchronous replication is what Preferred asks for, got %v", converged)
	}
}

func TestARetiredMemberDropsOutOfTheQuorumSet(t *testing.T) {
	converged := ConvergeSyncMembers([]string{holderTwo, "pg-9"}, []string{holderTwo}, instanceMembers, true)

	if !slices.Equal(converged, []string{holderTwo}) {
		t.Fatalf("a member the instance no longer has cannot be a voter, got %v", converged)
	}
}

func TestTheLoadedClauseIsParsedIntoTheGatesTerms(t *testing.T) {
	numSync, voters := ParseSyncStandbyNames(`ANY 1 ("pg-2","pg-3")`)

	if numSync != 1 {
		t.Fatalf("W was %d", numSync)
	}
	if !slices.Equal(voters, []string{holderTwo, memberThree}) {
		t.Fatalf("N was %v", voters)
	}
}

func TestAnUnparseableClauseYieldsNoQuorumAtAll(t *testing.T) {
	for _, clause := range []string{"", "ANY 1", "nonsense (a,b)"} {
		numSync, voters := ParseSyncStandbyNames(clause)
		if numSync != 0 || voters != nil {
			t.Fatalf("%q yielded W=%d N=%v; empty evidence must deny a failover", clause, numSync, voters)
		}
	}
}

func TestAWALVolumeIsFullBelowTheHeadroomFloor(t *testing.T) {
	const gibibyte = 1 << 30
	cases := map[string]struct {
		usage VolumeUsage
		full  bool
	}{
		"plenty of room": {VolumeUsage{TotalBytes: gibibyte, FreeBytes: gibibyte / 2}, false},
		"below the absolute segment floor on a small volume": {
			VolumeUsage{TotalBytes: gibibyte, FreeBytes: 4 * WALSegmentBytes}, true},
		"below the proportional floor on a large volume": {
			VolumeUsage{TotalBytes: 100 * gibibyte, FreeBytes: gibibyte}, true},
		"an unmeasurable volume is not a full one": {VolumeUsage{}, false},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			if got := testCase.usage.Full(); got != testCase.full {
				t.Fatalf("Full() = %v, want %v", got, testCase.full)
			}
		})
	}
}
