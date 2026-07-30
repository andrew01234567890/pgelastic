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
	"fmt"
	"slices"
	"time"
)

// MaxEvidenceAge is how old quorum evidence may be, measured against the moment the primary
// was first observed failing rather than against now.
//
// Measuring it against now would be self-defeating: the primary is the only writer of this
// record, so once it dies the evidence stops being refreshed and would age out of validity
// during exactly the failover it exists to gate. What the age check is actually for is
// rejecting a record left behind by a previous incarnation of the instance, and the failing
// instant is the right reference for that.
const MaxEvidenceAge = 5 * time.Minute

// Evidence is what PostgreSQL actually loaded, as read back out of the live postmaster by
// the member that is both currentPrimary and targetPrimary.
//
// It is deliberately a separate record from the operator's decision. Sourcing N and W from
// the CR spec instead would let a reload that was applied to the file but never loaded by
// the postmaster satisfy a gate whose whole purpose is to prove the opposite.
type Evidence struct {
	// SynchronousStandbyNames is the loaded clause, verbatim.
	SynchronousStandbyNames string
	// NumSync is W: how many standbys PostgreSQL waits for before acknowledging a commit.
	NumSync int32
	// VotingMembers is N: the members named in the loaded clause.
	VotingMembers []string
	// StreamingMembers is the subset of the voters that pg_stat_replication reported as
	// actually streaming when the record was written.
	StreamingMembers []string
	// ReportedBy is the member that wrote the record.
	ReportedBy string
	// ObservedAt is when the value was read out of the postmaster.
	ObservedAt time.Time
}

// QuorumVerdict is the R + W > N gate's answer, with every term recorded so the arithmetic
// can be audited from the CR alone.
type QuorumVerdict struct {
	// Satisfied is the gate's answer. False denies the failover.
	Satisfied bool
	// N is the size of the loaded quorum set, W is the loaded numsync, and R is how many of
	// the voters the operator can currently reach.
	N int
	W int
	R int
	// ReachableVoters are the voters that answered.
	ReachableVoters []string
	// Reason is a short machine-readable cause when the gate denies.
	Reason string
	// Message explains the arithmetic in words.
	Message string
}

// Reasons the quorum gate denies a failover.
const (
	// QuorumEvidenceMissing is an absent or empty record. Empty evidence denies: a stalled
	// instance is recoverable and a lost acknowledged commit is not.
	QuorumEvidenceMissing = "QuorumEvidenceMissing"
	// QuorumEvidenceStale is a record older than MaxEvidenceAge at the failing instant.
	QuorumEvidenceStale = "QuorumEvidenceStale"
	// QuorumNotProven is a well-formed record whose arithmetic does not hold.
	QuorumNotProven = "QuorumNotProven"
	// QuorumProven is recorded on the satisfied verdict so the passing case carries a
	// reason too.
	QuorumProven = "QuorumProven"
)

// EvaluateQuorum decides whether a promotion can be proven not to lose an acknowledged
// commit.
//
// R + W > N is the standard read/write quorum overlap, evaluated against the loaded clause.
// With three replicas and "ANY 1" that is N = 2, W = 1, so R must be 2: both standbys have
// to be reachable. That is strict on purpose. With one standby visible there is no way to
// tell whether it is the one that acknowledged the last commit, and promoting the other
// discards it silently.
//
// The reference instant is when the primary was first seen failing, so that evidence which
// was current while the primary was healthy stays valid for the failover it has to gate.
func EvaluateQuorum(evidence Evidence, reachable []string, reference time.Time) QuorumVerdict {
	verdict := QuorumVerdict{
		N: len(evidence.VotingMembers),
		W: int(evidence.NumSync),
	}
	if verdict.N == 0 || verdict.W <= 0 || evidence.SynchronousStandbyNames == "" {
		verdict.Reason = QuorumEvidenceMissing
		verdict.Message = "PostgreSQL reported no loaded synchronous_standby_names, " +
			"and empty evidence denies a failover"
		return verdict
	}
	if evidence.ObservedAt.IsZero() || reference.Sub(evidence.ObservedAt) > MaxEvidenceAge {
		verdict.Reason = QuorumEvidenceStale
		verdict.Message = fmt.Sprintf(
			"the quorum evidence was last observed at %s, more than %s before the primary began failing",
			evidence.ObservedAt.UTC().Format(time.RFC3339), MaxEvidenceAge)
		return verdict
	}

	reachableSet := setOf(reachable)
	for _, voter := range evidence.VotingMembers {
		if reachableSet[voter] {
			verdict.ReachableVoters = append(verdict.ReachableVoters, voter)
		}
	}
	slices.Sort(verdict.ReachableVoters)
	verdict.R = len(verdict.ReachableVoters)

	verdict.Satisfied = verdict.R+verdict.W > verdict.N
	verdict.Reason = QuorumNotProven
	if verdict.Satisfied {
		verdict.Reason = QuorumProven
	}
	verdict.Message = fmt.Sprintf(
		"R=%d W=%d N=%d against the loaded clause %q", verdict.R, verdict.W, verdict.N,
		evidence.SynchronousStandbyNames)
	return verdict
}

// WriteStalled reports whether commits are currently blocked because fewer standbys are
// streaming than the loaded clause waits for.
//
// Under dataDurability Required that is the correct behaviour rather than a fault, but it
// has to be a named, alertable state: an instance whose commits stall silently pins every
// pooled backend, consumes every burst connection and cascades into tenants that have
// nothing to do with the failure.
func WriteStalled(evidence Evidence) bool {
	if evidence.NumSync <= 0 {
		return false
	}
	return len(evidence.StreamingMembers) < int(evidence.NumSync)
}

// WouldStall reports whether taking one member away would block commits.
//
// This is the question a rolling restart has, and it is not the one WriteStalled answers.
// WriteStalled is retrospective - it says commits are stalling *now* - and a roll that asks only
// that will happily remove the last streaming standby, because right up until the moment it does
// so nothing is stalling at all.
//
// The case is not hypothetical and it is not an edge. With ANY 1 over two standbys, NumSync is 1,
// so WriteStalled stays false while either standby streams. A member that has just been rolled is
// Ready long before it has caught up, so during that window exactly one standby streams -
// WriteStalled says fine, and removing that one takes the instance to zero. Under dataDurability
// Required every commit then blocks until a standby returns, which is a rolling restart that
// stops the database committing.
//
// Membership is by name and the target is excluded whether or not it is currently streaming, so
// the answer is about the state the instance would be left in rather than the one it is in.
func WouldStall(evidence Evidence, member string) bool {
	if evidence.NumSync <= 0 {
		return false
	}
	remaining := 0
	for _, streaming := range evidence.StreamingMembers {
		if streaming != member {
			remaining++
		}
	}
	return remaining < int(evidence.NumSync)
}

// EvidenceStale reports whether a quorum record is too old to decide anything with.
//
// Evidence is a photograph of the synchronous set at the instant a member reported it. Deciding
// to disrupt a member from a photograph taken long enough ago is deciding about a cluster that
// may have moved, and the decision this gates - remove a member - is the one whose cost is a
// stalled instance. Refusing is recoverable; the roll simply waits for a fresher record.
//
// A record with no timestamp is treated as stale rather than fresh, because "nobody said when"
// and "somebody said just now" must not be the same answer.
func EvidenceStale(evidence Evidence, now time.Time, maxAge time.Duration) bool {
	if evidence.ObservedAt.IsZero() {
		return true
	}
	return now.Sub(evidence.ObservedAt) > maxAge
}
