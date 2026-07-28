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

import "testing"

// forkedAtTimelineThree is the ancestry of a primary that promoted twice: timeline 2 left
// timeline 1 at 0/3000060, and timeline 3 left timeline 2 at 0/70000A0.
var forkedAtTimelineThree = []TimelineFork{
	{Parent: 1, SwitchLSN: MustParseLSN("0/3000060")},
	{Parent: 2, SwitchLSN: MustParseLSN("0/70000A0")},
}

func TestDetectDivergence(t *testing.T) {
	cases := []struct {
		name       string
		local      TimelinePosition
		primary    PrimaryHistory
		wantReason string
	}{
		{
			name:    "a standby merely behind on the primary's own timeline",
			local:   TimelinePosition{Timeline: 3, LSN: MustParseLSN("0/7000000")},
			primary: PrimaryHistory{Timeline: 3, HistoryTimeline: 3, Forks: forkedAtTimelineThree},
		},
		{
			name:    "a standby that stopped before the fork point",
			local:   TimelinePosition{Timeline: 2, LSN: MustParseLSN("0/6FFFFFF")},
			primary: PrimaryHistory{Timeline: 3, HistoryTimeline: 3, Forks: forkedAtTimelineThree},
		},
		{
			name:    "a standby exactly at the fork point",
			local:   TimelinePosition{Timeline: 2, LSN: MustParseLSN("0/70000A0")},
			primary: PrimaryHistory{Timeline: 3, HistoryTimeline: 3, Forks: forkedAtTimelineThree},
		},
		{
			name:       "a standby that replayed past the fork point",
			local:      TimelinePosition{Timeline: 2, LSN: MustParseLSN("0/80000A0")},
			primary:    PrimaryHistory{Timeline: 3, HistoryTimeline: 3, Forks: forkedAtTimelineThree},
			wantReason: DivergencePastForkPoint,
		},
		{
			name:       "a member holding a higher timeline than the primary",
			local:      TimelinePosition{Timeline: 4, LSN: MustParseLSN("0/1000000")},
			primary:    PrimaryHistory{Timeline: 3, HistoryTimeline: 3, Forks: forkedAtTimelineThree},
			wantReason: DivergenceAheadOfPrimary,
		},
		{
			name:  "a primary whose history never passed through this member's timeline",
			local: TimelinePosition{Timeline: 5, LSN: MustParseLSN("0/1000000")},
			primary: PrimaryHistory{Timeline: 9, HistoryTimeline: 9, Forks: []TimelineFork{
				{Parent: 1, SwitchLSN: MustParseLSN("0/3000060")},
			}},
			wantReason: DivergenceNoCommonAncestor,
		},
		{
			name:    "a member that once promoted and still holds the history file it wrote",
			local:   TimelinePosition{Timeline: 2, LSN: MustParseLSN("0/80000A0")},
			primary: PrimaryHistory{Timeline: 2, HistoryTimeline: 3, Forks: forkedAtTimelineThree},
		},
		{
			name:    "a primary whose timeline nobody has reported",
			local:   TimelinePosition{Timeline: 2, LSN: MustParseLSN("0/80000A0")},
			primary: PrimaryHistory{HistoryTimeline: 3, Forks: forkedAtTimelineThree},
		},
		{
			name:    "a member with no timeline history to compare against",
			local:   TimelinePosition{Timeline: 2, LSN: MustParseLSN("0/80000A0")},
			primary: PrimaryHistory{Timeline: 2},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			divergence := DetectDivergence(testCase.local, testCase.primary)
			if divergence.Diverged != (testCase.wantReason != "") {
				t.Fatalf("diverged = %v, want %v (%s)",
					divergence.Diverged, testCase.wantReason != "", divergence.Message)
			}
			if divergence.Reason != testCase.wantReason {
				t.Errorf("reason = %q, want %q", divergence.Reason, testCase.wantReason)
			}
			if divergence.Diverged && divergence.Message == "" {
				t.Error("a destructive verdict has to say what it was reached from")
			}
		})
	}
}
