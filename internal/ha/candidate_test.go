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

package ha

import (
	"slices"
	"testing"
)

// The three members every spec in this package works with.
const (
	memberOne   = "pg-1"
	memberTwo   = "pg-2"
	memberThree = "pg-3"
)

// standby is an eligible member: reachable, in recovery, in the sync set and streaming when
// the primary was last seen.
func standby(name string, timeline int32, received, replay string) Member {
	return Member{
		Name:            name,
		Timeline:        timeline,
		ReceivedLSN:     MustParseLSN(received),
		ReplayLSN:       MustParseLSN(replay),
		InRecovery:      true,
		StatusReachable: true,
		PodReady:        true,
	}
}

func inputFor(members ...Member) CandidateInput {
	names := make([]string, 0, len(members))
	for _, member := range members {
		names = append(names, member.Name)
	}
	return CandidateInput{
		Members:              members,
		KnownPrimary:         memberOne,
		LastKnownTimeline:    LastKnownTimeline(members),
		SyncSet:              names,
		StreamingAtDetection: names,
	}
}

func reasonFor(result CandidateResult, member string) string {
	for _, disqualified := range result.Disqualified {
		if disqualified.Member == member {
			return disqualified.Reason
		}
	}
	return ""
}

func TestTimelineOutranksLSN(t *testing.T) {
	behind := standby(memberTwo, 4, "1/FFFFFFFF", "1/FFFFFFFF")
	ahead := standby(memberThree, 5, "1/00000010", "1/00000010")

	result := SelectCandidate(inputFor(behind, ahead))

	if result.Candidate != memberThree {
		t.Fatalf("the higher timeline must win regardless of LSN, got %q (%v)",
			result.Candidate, result.Ranked)
	}
	if reasonFor(result, memberTwo) != DisqualifiedBehindTimeline {
		t.Fatalf("pg-2 is below the last known timeline, got %q", reasonFor(result, memberTwo))
	}
}

func TestReceivedLSNOutranksReplayLSN(t *testing.T) {
	replayedMore := standby(memberTwo, 5, "2/00000000", "2/00000000")
	receivedMore := standby(memberThree, 5, "3/00000000", "1/00000000")

	result := SelectCandidate(inputFor(replayedMore, receivedMore))

	if result.Candidate != memberThree {
		t.Fatalf("received WAL is durable whether replayed or not; got %q", result.Candidate)
	}
}

func TestNameBreaksAnExactTie(t *testing.T) {
	first := standby(memberThree, 5, "2/00000000", "2/00000000")
	second := standby(memberTwo, 5, "2/00000000", "2/00000000")

	result := SelectCandidate(inputFor(first, second))

	if result.Candidate != memberTwo {
		t.Fatalf("an exact tie must resolve deterministically by name, got %q", result.Candidate)
	}
	if !slices.Equal(result.Ranked, []string{memberTwo, memberThree}) {
		t.Fatalf("ranking was %v", result.Ranked)
	}
}

func TestTextualLSNOrderingIsNotUsed(t *testing.T) {
	// "10/0" is ahead of "9/FFFFFFFF" numerically and behind it lexicographically.
	lexicographicallyLater := standby(memberTwo, 5, "9/FFFFFFFF", "9/FFFFFFFF")
	numericallyAhead := standby(memberThree, 5, "10/00000000", "10/00000000")

	result := SelectCandidate(inputFor(lexicographicallyLater, numericallyAhead))

	if result.Candidate != memberThree {
		t.Fatalf("LSNs must be compared numerically, got %q", result.Candidate)
	}
}

func TestDisqualifications(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Member)
		reason string
	}{
		{"an unreachable status endpoint", func(m *Member) { m.StatusReachable = false },
			DisqualifiedUnreachable},
		{"a member already out of recovery", func(m *Member) { m.InRecovery = false },
			DisqualifiedOutOfRecovery},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := standby(memberTwo, 5, "2/0", "2/0")
			testCase.mutate(&candidate)
			other := standby(memberThree, 5, "1/0", "1/0")

			result := SelectCandidate(inputFor(candidate, other))

			if result.Candidate != memberThree {
				t.Fatalf("expected pg-2 to be disqualified, candidate was %q", result.Candidate)
			}
			if got := reasonFor(result, memberTwo); got != testCase.reason {
				t.Fatalf("reason was %q, want %q", got, testCase.reason)
			}
		})
	}
}

func TestAMemberOutsideTheRecordedSyncSetIsDisqualified(t *testing.T) {
	furthestAhead := standby(memberTwo, 5, "9/0", "9/0")
	voter := standby(memberThree, 5, "1/0", "1/0")

	input := inputFor(furthestAhead, voter)
	input.SyncSet = []string{memberThree}
	input.StreamingAtDetection = []string{memberThree}

	result := SelectCandidate(input)

	if result.Candidate != memberThree {
		t.Fatalf("a member PostgreSQL never loaded as a voter cannot be promoted, got %q",
			result.Candidate)
	}
	if reasonFor(result, memberTwo) != DisqualifiedNotInSyncSet {
		t.Fatalf("reason was %q", reasonFor(result, memberTwo))
	}
}

func TestAMemberWithNoReceiverAtDetectionIsDisqualified(t *testing.T) {
	notStreaming := standby(memberTwo, 5, "9/0", "9/0")
	streaming := standby(memberThree, 5, "1/0", "1/0")

	input := inputFor(notStreaming, streaming)
	input.StreamingAtDetection = []string{memberThree}

	result := SelectCandidate(input)

	if result.Candidate != memberThree {
		t.Fatalf("candidate was %q", result.Candidate)
	}
	if reasonFor(result, memberTwo) != DisqualifiedNoReceiverAtDetection {
		t.Fatalf("reason was %q", reasonFor(result, memberTwo))
	}
}

func TestTheFailedPrimaryIsNeverACandidate(t *testing.T) {
	primary := Member{Name: memberOne, Timeline: 5, StatusReachable: true, PodReady: true}
	result := SelectCandidate(inputFor(primary, standby(memberTwo, 5, "1/0", "1/0")))

	if result.Candidate != memberTwo {
		t.Fatalf("candidate was %q", result.Candidate)
	}
	if reasonFor(result, memberOne) != DisqualifiedIsPrimary {
		t.Fatalf("reason was %q", reasonFor(result, memberOne))
	}
}

func TestEveryMemberDisqualifiedYieldsNoCandidate(t *testing.T) {
	unreachable := standby(memberTwo, 5, "1/0", "1/0")
	unreachable.StatusReachable = false

	result := SelectCandidate(inputFor(unreachable))

	if result.Candidate != "" {
		t.Fatalf("expected no candidate, got %q", result.Candidate)
	}
}

func TestLastKnownTimelineIgnoresUnreachableMembers(t *testing.T) {
	reachable := standby(memberTwo, 5, "1/0", "1/0")
	stale := standby(memberThree, 9, "1/0", "1/0")
	stale.StatusReachable = false

	if got := LastKnownTimeline([]Member{reachable, stale}); got != 5 {
		t.Fatalf("last known timeline was %d, want 5", got)
	}
}
