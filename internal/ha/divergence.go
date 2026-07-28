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

import "fmt"

// TimelineFork is one entry of a timeline history: the timeline that history left behind,
// and the last position the two have in common.
type TimelineFork struct {
	// Parent is the timeline the history forked away from.
	Parent int32
	// SwitchLSN is the last position shared with Parent.
	SwitchLSN LSN
}

// TimelinePosition is how much WAL a member holds and which history it holds it on.
//
// Timeline must be the highest timeline the member has any WAL for, not merely the one its
// last replayed checkpoint was on: a standby writes a timeline switch into its control file
// only when it turns a replayed checkpoint into a restartpoint, so the control file alone
// reports a member that has already followed the new history as though it were still on the
// old one - and rewinding on that reading would discard a member with nothing wrong with it.
type TimelinePosition struct {
	Timeline int32
	LSN      LSN
}

// PrimaryHistory is everything a member knows about the history it is supposed to follow.
//
// The two timelines are separate fields because they answer separate questions and are
// sourced separately. Timeline is what the primary reports about itself, and it is the only
// thing that can prove a member is *ahead* of the primary. HistoryTimeline and Forks come
// from the newest .history file in this member's own WAL directory - fetched from the
// primary by its own WAL receiver before the streaming request was refused - and they are
// the only thing that can prove where the two histories parted.
type PrimaryHistory struct {
	// Timeline is the timeline the primary is on, zero when it is not known.
	Timeline int32
	// HistoryTimeline is the timeline the newest history file describes.
	HistoryTimeline int32
	// Forks is that file's ancestry.
	Forks []TimelineFork
}

// Reasons a member's history cannot be reconciled with the primary's by streaming.
const (
	// DivergenceAheadOfPrimary is a member holding a higher timeline than the primary. It
	// may never rejoin without a rewind or a re-clone, whatever its position says.
	DivergenceAheadOfPrimary = "TimelineAheadOfPrimary"
	// DivergenceNoCommonAncestor is a primary whose history never passed through this
	// member's timeline at all.
	DivergenceNoCommonAncestor = "NoCommonAncestor"
	// DivergencePastForkPoint is a member that replayed past the position the primary's
	// history forked at. The WAL it holds beyond that point exists on no other copy, and no
	// amount of streaming will ever bridge it.
	DivergencePastForkPoint = "ReplayedPastForkPoint"
)

// Divergence is the verdict on whether a member can still follow the primary by streaming.
type Divergence struct {
	// Diverged is true when it cannot, and a rewind or a re-clone is the only way back.
	Diverged bool
	// Reason names which of the three shapes this is, empty when the member is merely
	// behind.
	Reason string
	// Message states the arithmetic the verdict was reached by, because a rewind is
	// destructive and "why did this member rewind" has to be answerable afterwards.
	Message string
}

// DetectDivergence decides whether a member's own history can still be reconciled with the
// primary's by streaming.
//
// This is row four of the split-brain catalogue, and the important thing about it is that a
// member being *in recovery* proves nothing. A standby that received WAL past the point the
// new primary forked at holds records that exist nowhere else; its WAL receiver is refused
// for as long as it keeps asking, and it keeps asking forever.
//
// Every branch that returns Diverged does so from a position and a fork point, never from
// the shape of an error message. The fork analysis additionally refuses to run against a
// history file describing a timeline the primary has not reached, because a member that
// once promoted keeps the history file it wrote, and treating that as the primary's history
// would rewind a member for having been a candidate.
func DetectDivergence(local TimelinePosition, primary PrimaryHistory) Divergence {
	if local.Timeline <= 0 || primary.Timeline <= 0 {
		return Divergence{}
	}
	if local.Timeline > primary.Timeline {
		return Divergence{
			Diverged: true,
			Reason:   DivergenceAheadOfPrimary,
			Message: fmt.Sprintf("this member is on timeline %d while the primary is on %d",
				local.Timeline, primary.Timeline),
		}
	}
	if primary.HistoryTimeline <= local.Timeline || primary.HistoryTimeline > primary.Timeline {
		return Divergence{}
	}

	for _, fork := range primary.Forks {
		if fork.Parent != local.Timeline {
			continue
		}
		if local.LSN <= fork.SwitchLSN {
			return Divergence{}
		}
		return Divergence{
			Diverged: true,
			Reason:   DivergencePastForkPoint,
			Message: fmt.Sprintf(
				"this member holds WAL to %s on timeline %d, past the %s at which timeline %d forked",
				local.LSN, local.Timeline, fork.SwitchLSN, primary.HistoryTimeline),
		}
	}
	return Divergence{
		Diverged: true,
		Reason:   DivergenceNoCommonAncestor,
		Message: fmt.Sprintf("timeline %d never descended from timeline %d",
			primary.HistoryTimeline, local.Timeline),
	}
}
