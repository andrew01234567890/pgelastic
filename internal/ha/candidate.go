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
	"cmp"
	"slices"
)

// Member is one instance member as the operator observes it, from the member's own report
// and from its Pod.
type Member struct {
	// Name is the Pod name, which is also the application_name
	// synchronous_standby_names refers to it by.
	Name string
	// Timeline is a first-class term in candidate selection, ordered ahead of every LSN.
	Timeline int32
	// ReceivedLSN is how far the WAL receiver has written; ReplayLSN is how far recovery
	// has replayed. They are ordered in that sequence because received WAL is durable on
	// this member whether or not it has been replayed yet, so the member holding more of it
	// is the one that loses less.
	ReceivedLSN LSN
	ReplayLSN   LSN
	// InRecovery is pg_is_in_recovery() as read by the member's own agent. A member
	// reporting false that is not the known primary is a split-brain alarm, never a
	// tiebreak.
	InRecovery bool
	// StatusReachable reports that the member's status endpoint answered at all. A member
	// whose endpoint errored is disqualified: not knowing where it is, is not the same as
	// knowing it is behind.
	StatusReachable bool
	// PodReady is the kubelet's verdict, which is a different question from whether the
	// status endpoint answered and is deliberately not conflated with it.
	PodReady bool
	// WALReceiverActive is checked at two instants with two meanings: a candidate must have
	// had a receiver at detection time, and every member must have its receiver down before
	// promotion may proceed.
	WALReceiverActive bool
	// WALVolumeFull refuses a candidate outright. Promoting onto a full pg_wal buys a
	// primary that PANICs on its first checkpoint.
	WALVolumeFull bool
}

// Disqualification records one member that cannot be promoted and why. Every rejection is
// recorded rather than merely filtered, because "why was that one not chosen" is asked
// after the fact and cannot be reconstructed from a decision that only kept the winner.
type Disqualification struct {
	Member string
	Reason string
}

// Reasons a member is disqualified from candidacy.
const (
	// DisqualifiedBehindTimeline is a member below the cluster's last known timeline. It
	// has not replayed a history file the rest of the cluster has, so promoting it discards
	// everything after the divergence.
	DisqualifiedBehindTimeline = "BelowLastKnownTimeline"
	// DisqualifiedOutOfRecovery is a member reporting pg_is_in_recovery() = false that is
	// not the known primary.
	DisqualifiedOutOfRecovery = "OutOfRecovery"
	// DisqualifiedUnreachable is a member whose status endpoint errored.
	DisqualifiedUnreachable = "StatusEndpointUnreachable"
	// DisqualifiedNotInSyncSet is a member PostgreSQL did not load as a quorum voter.
	DisqualifiedNotInSyncSet = "NotInRecordedSyncSet"
	// DisqualifiedNoReceiverAtDetection is a member that was not streaming when the primary
	// was last observed healthy, so nothing proves it ever held the last acknowledged
	// commit.
	DisqualifiedNoReceiverAtDetection = "NoWALReceiverAtDetection"
	// DisqualifiedIsPrimary is the failed primary itself.
	DisqualifiedIsPrimary = "IsTheFailedPrimary"
)

// CandidateInput is everything candidate selection is allowed to look at.
type CandidateInput struct {
	// Members is every member the operator has an observation for.
	Members []Member
	// KnownPrimary is status.currentPrimary: the one member permitted to report
	// pg_is_in_recovery() = false.
	KnownPrimary string
	// LastKnownTimeline disqualifies anything below it.
	LastKnownTimeline int32
	// SyncSet is the recorded quorum set, parsed out of the synchronous_standby_names
	// PostgreSQL actually loaded. A member outside it cannot be proven to have
	// acknowledged the last commit.
	SyncSet []string
	// StreamingAtDetection is the set of members the primary reported as streaming quorum
	// members the last time it was observed. It is the proof that a candidate had an active
	// WAL receiver at detection time: PostgreSQL counts a standby towards the quorum only
	// while it is streaming.
	StreamingAtDetection []string
}

// CandidateResult is the outcome of ranking the members.
type CandidateResult struct {
	// Candidate is the highest-ranked eligible member, or empty when none is eligible.
	Candidate string
	// Ranked is every eligible member in promotion order.
	Ranked []string
	// Disqualified records every member that was rejected, with its reason.
	Disqualified []Disqualification
}

// SelectCandidate ranks the members that may be promoted.
//
// The ordering is (timeline DESC, received_lsn DESC, replay_lsn DESC, name ASC). Timeline
// leads because a member on a higher timeline has replayed history the others have not, and
// treating that as merely a tiebreak - the usual implicit assumption - promotes a member
// that is about to be told to discard WAL it already has. The name is the final term so the
// answer is deterministic: two members at an identical position must not produce two
// different decisions on two reconciles.
func SelectCandidate(input CandidateInput) CandidateResult {
	syncSet := setOf(input.SyncSet)
	streaming := setOf(input.StreamingAtDetection)

	result := CandidateResult{}
	eligible := make([]Member, 0, len(input.Members))
	for _, member := range input.Members {
		if reason, ok := disqualify(member, input, syncSet, streaming); ok {
			result.Disqualified = append(result.Disqualified,
				Disqualification{Member: member.Name, Reason: reason})
			continue
		}
		eligible = append(eligible, member)
	}

	slices.SortFunc(eligible, func(a, b Member) int {
		if order := cmp.Compare(b.Timeline, a.Timeline); order != 0 {
			return order
		}
		if order := cmp.Compare(b.ReceivedLSN, a.ReceivedLSN); order != 0 {
			return order
		}
		if order := cmp.Compare(b.ReplayLSN, a.ReplayLSN); order != 0 {
			return order
		}
		return cmp.Compare(a.Name, b.Name)
	})
	slices.SortFunc(result.Disqualified, func(a, b Disqualification) int {
		return cmp.Compare(a.Member, b.Member)
	})

	for _, member := range eligible {
		result.Ranked = append(result.Ranked, member.Name)
	}
	if len(result.Ranked) > 0 {
		result.Candidate = result.Ranked[0]
	}
	return result
}

func disqualify(
	member Member,
	input CandidateInput,
	syncSet, streaming map[string]bool,
) (string, bool) {
	switch {
	case member.Name == input.KnownPrimary:
		return DisqualifiedIsPrimary, true
	case !member.StatusReachable:
		return DisqualifiedUnreachable, true
	case !member.InRecovery:
		return DisqualifiedOutOfRecovery, true
	case member.Timeline < input.LastKnownTimeline:
		return DisqualifiedBehindTimeline, true
	case !syncSet[member.Name]:
		return DisqualifiedNotInSyncSet, true
	case !streaming[member.Name]:
		return DisqualifiedNoReceiverAtDetection, true
	}
	return "", false
}

// LastKnownTimeline is the highest timeline any reachable member reports.
//
// It is derived rather than persisted so that it cannot go stale, and it is taken over
// reachable members only: a member nobody can talk to contributes no evidence about where
// the cluster's history has got to.
func LastKnownTimeline(members []Member) int32 {
	var highest int32
	for _, member := range members {
		if member.StatusReachable && member.Timeline > highest {
			highest = member.Timeline
		}
	}
	return highest
}

func setOf(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}
