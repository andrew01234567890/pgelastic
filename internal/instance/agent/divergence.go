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
	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
)

// HeldPosition is the furthest WAL this member holds and the timeline it is on.
//
// The three positions are taken together because each of them is unavailable in a state the
// other two survive: the received position is empty until a WAL receiver has succeeded once
// in this postmaster's lifetime, which is exactly what a diverged member can never do; the
// replay position moves only while recovery is making progress; and the control file's
// minimum recovery end is the only one of the three that is durable.
func (o MemberObservation) HeldPosition() ha.TimelinePosition {
	return ha.TimelinePosition{
		Timeline: o.Timeline,
		LSN: max(ha.MustParseLSN(o.ReceivedLSN), ha.MustParseLSN(o.ReplayLSN),
			ha.MustParseLSN(o.MinRecoveryEndLSN)),
	}
}

// StoppedPosition is the same reading taken from a data directory with no postmaster on it.
func StoppedPosition(data pgtool.ControlData) ha.TimelinePosition {
	return ha.TimelinePosition{
		Timeline: max(data.TimelineID, data.MinRecoveryEndTimeline),
		LSN:      ha.MustParseLSN(data.MinRecoveryEnd),
	}
}

// PrimaryTimeline reads the timeline the named member last reported itself on. Zero means
// the instance has no record of it, and a divergence verdict is never reached without one.
func PrimaryTimeline(instance *pgelasticv1alpha1.PgInstance, primary string) int32 {
	if instance == nil {
		return 0
	}
	for _, member := range instance.Status.Instances {
		if member.Name == primary {
			return member.Timeline
		}
	}
	return 0
}

// DetectDivergence answers whether this member can still follow the primary by streaming.
//
// The primary's history comes off this member's own WAL volume. A standby fetches every
// missing timeline history file from the primary before it asks to stream, and keeps them
// whether or not the streaming request is then refused - so the fork point is on local disk,
// written by PostgreSQL, in a file whose format is part of the on-disk contract. Reading it
// is evidence; matching the FATAL the refusal produces would be a guess about a message
// that is free to change.
func DetectDivergence(walDir string, local ha.TimelinePosition, primaryTimeline int32) (ha.Divergence, error) {
	history, found, err := pgtool.LatestTimelineHistory(walDir)
	if err != nil {
		return ha.Divergence{}, err
	}
	primary := ha.PrimaryHistory{Timeline: primaryTimeline}
	if found {
		primary.HistoryTimeline = history.Timeline
		for _, fork := range history.Forks {
			primary.Forks = append(primary.Forks, ha.TimelineFork{
				Parent:    fork.Parent,
				SwitchLSN: ha.MustParseLSN(fork.SwitchLSN),
			})
		}
	}
	return ha.DetectDivergence(local, primary), nil
}
