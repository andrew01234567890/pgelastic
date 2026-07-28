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

// StartupAction is what a member must do to its data directory before any postmaster is
// allowed to start.
//
// It exists because the most dangerous moment in the whole design is a member coming back
// after a failover it did not witness. Its data directory still says "primary", its
// postmaster would start as one, and Kubernetes will happily route a connection to it.
type StartupAction struct {
	// Follow is the member this one must stream from, empty when it may start unchanged.
	Follow string
	// Rejoin means the data directory has diverged and must be rewound - or re-cloned -
	// before it may follow anything.
	Rejoin bool
	// Reason records why, so a rewind that turns out to have been unnecessary can be
	// explained afterwards rather than guessed at.
	Reason string
}

// Reasons a member is told what to do at start-up.
const (
	// StartupUnchanged is a member whose data directory already matches the instance's
	// view of it.
	StartupUnchanged = "AlreadyConsistent"
	// StartupFollowPrimary is a standby being pointed at whichever member is the primary
	// now. Its history is intact - it only ever received WAL - so nothing but
	// primary_conninfo needs rewriting, and rewriting it when it has not changed costs a
	// file write.
	StartupFollowPrimary = "FollowDesignatedPrimary"
	// StartupSupersededByTarget is a member that believes it is a primary while the
	// operator has named somebody else.
	StartupSupersededByTarget = "SupersededByTargetPrimary"
	// StartupSupersededByLease is a member that believes it is a primary while the
	// promotion Lease is demonstrably held by somebody else. This is the case that catches
	// a member which was deleted and recreated inside a failover, before any status field
	// had caught up with what happened to it.
	StartupSupersededByLease = "SupersededByLeaseHolder"
	// StartupSupersededByPrimary is a member that believes it is a primary while the
	// instance records a different one.
	StartupSupersededByPrimary = "SupersededByCurrentPrimary"
)

// StartupDecision decides what a member must do to its data directory before starting.
//
// The three inputs are deliberately independent, and are consulted in order of how directly
// each proves that this member is no longer the primary. targetPrimary is the operator's
// decision and is the strongest. The Lease is next, and it is the one that catches the
// nastiest case: a member deleted and recreated during a failover, whose own currentPrimary
// entry still names itself because nobody has written the new one yet. currentPrimary is
// last, because it is the field that lags furthest behind reality.
//
// A member already in recovery is never asked to rejoin *by this function*: nothing it is
// given can tell a standby that is merely behind from one that received WAL past the point
// the primary's history forked at. That second case is real and it does not resolve itself,
// so the caller compares this member's position against the primary's timeline history and
// escalates the answer from Follow to Rejoin when the two cannot be reconciled by
// streaming. See DetectDivergence.
func StartupDecision(self string, inRecovery bool, currentPrimary, targetPrimary, leaseHolder string) StartupAction {
	designated := currentPrimary
	if targetPrimary != "" && targetPrimary != TargetPrimaryPending {
		designated = targetPrimary
	}

	if inRecovery {
		if designated == "" || designated == self {
			return StartupAction{Reason: StartupUnchanged}
		}
		return StartupAction{Follow: designated, Reason: StartupFollowPrimary}
	}

	switch {
	case targetPrimary == self || leaseHolder == self:
		// This member is the primary, or is the one the operator has just named as the next
		// one. Its own history is the history everybody else is about to follow, and
		// rewinding it onto a predecessor would discard exactly the commits it was chosen
		// for holding.
		return StartupAction{Reason: StartupUnchanged}
	case targetPrimary != "" && targetPrimary != TargetPrimaryPending && targetPrimary != self:
		return StartupAction{Follow: targetPrimary, Rejoin: true, Reason: StartupSupersededByTarget}
	case leaseHolder != "" && leaseHolder != self:
		return StartupAction{Follow: leaseHolder, Rejoin: true, Reason: StartupSupersededByLease}
	case targetPrimary == TargetPrimaryPending:
		// A failover that has not chosen anybody yet. This member coming back as the
		// primary it already was is a recovery, not a split brain, and the operator will
		// withdraw the sentinel once it can see it again.
		return StartupAction{Reason: StartupUnchanged}
	case currentPrimary != "" && currentPrimary != self:
		return StartupAction{Follow: currentPrimary, Rejoin: true, Reason: StartupSupersededByPrimary}
	}
	return StartupAction{Reason: StartupUnchanged}
}
