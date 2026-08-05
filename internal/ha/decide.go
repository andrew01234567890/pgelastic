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

// TargetPrimaryPending is the reserved sentinel written in phase one of a failover.
//
// It is never a member name, which is what makes "targetPrimary != currentPrimary" a total
// signal for "failover in progress, freeze everything" - one comparison, no tri-state, no
// separate flag that can disagree with it.
const TargetPrimaryPending = "pending"

// Phase is what the state machine decided this reconcile is.
type Phase string

const (
	// PhaseSteady is a healthy primary, or an instance that has not elected one yet.
	PhaseSteady Phase = "Steady"
	// PhaseDebouncing is an unhealthy primary inside failoverDelay.
	PhaseDebouncing Phase = "Debouncing"
	// PhaseSentinel is phase one: targetPrimary is being set to the sentinel and the role
	// label stripped, so Services stop selecting the old primary.
	PhaseSentinel Phase = "Sentinel"
	// PhaseWaitingWALReceivers is phase one holding, because some non-primary member still
	// reports an active WAL receiver.
	PhaseWaitingWALReceivers Phase = "WaitingForWALReceivers"
	// PhaseCandidateChosen is phase two: a candidate has passed the quorum gate and every
	// veto, and its name is being written to targetPrimary.
	PhaseCandidateChosen Phase = "CandidateChosen"
	// PhaseVetoed is a failover the operator refused, with a named reason.
	PhaseVetoed Phase = "Vetoed"
	// PhaseSplitBrain is two members reporting pg_is_in_recovery() = false. Everything
	// freezes.
	PhaseSplitBrain Phase = "SplitBrain"
	// PhasePromoting is a candidate already written to targetPrimary, waiting for that
	// member's own agent to finish promoting and write currentPrimary.
	PhasePromoting Phase = "Promoting"
	// PhasePlannedSwitchover is a healthy primary being handed over on purpose, because
	// something is about to disrupt the member currently holding the role.
	PhasePlannedSwitchover Phase = "PlannedSwitchover"
)

// Veto is one of the four named refusals. Each is its own CR condition and its own metric
// label, because "why did it not fail over" is the question asked at three in the morning
// and a single generic condition answers it uselessly.
type Veto string

const (
	// VetoNone is no veto.
	VetoNone Veto = ""
	// VetoOperatorIsolated is every ready member unreachable, which means the *operator* is
	// the partitioned party. It requeues and never fails over.
	VetoOperatorIsolated Veto = "OperatorIsolated"
	// VetoPrimaryUnobservable is a primary whose Pod the kubelet still calls Ready but
	// whose status endpoint is failing. The kubelet is closer to the truth than the
	// operator is, so this defers until it marks the Pod NotReady.
	VetoPrimaryUnobservable Veto = "PrimaryUnobservable"
	// VetoCandidateNotReady is a candidate answering over HTTP whose Pod is not Ready. It
	// waits rather than promoting into a Pod that Services will not select.
	VetoCandidateNotReady Veto = "CandidateNotReady"
	// VetoCandidateWALVolumeFull is a candidate whose pg_wal volume is full. Promoting onto
	// it buys a primary that PANICs at its first checkpoint.
	VetoCandidateWALVolumeFull Veto = "CandidateWALVolumeFull"
)

// Vetoes is every named veto, in the order they are evaluated. It exists so the metric and
// the condition set are enumerable rather than discovered by grep.
var Vetoes = []Veto{
	VetoOperatorIsolated,
	VetoPrimaryUnobservable,
	VetoCandidateNotReady,
	VetoCandidateWALVolumeFull,
}

// Additional reasons a decision refuses to progress that are not one of the four vetoes.
const (
	// ReasonNoEligibleCandidate is every member disqualified by candidate selection.
	ReasonNoEligibleCandidate = "NoEligibleCandidate"
	// ReasonSplitBrain is two members out of recovery at once.
	ReasonSplitBrain = "TwoPrimariesObserved"
	// ReasonSwitchoverChosen is a planned handover whose candidate passed every gate.
	ReasonSwitchoverChosen = "SwitchoverCandidateSelected"
	// ReasonSwitchoverNoCandidate is a planned handover with nowhere to hand the role to.
	// It is a refusal rather than a failure: the disruption simply does not happen, and
	// the primary keeps serving.
	ReasonSwitchoverNoCandidate = "SwitchoverHasNoEligibleCandidate"
)

// Observation is the whole input to one failover decision.
type Observation struct {
	// Members is every member, from its own report and from its Pod.
	Members []Member
	// CurrentPrimary and TargetPrimary are the two status fields the decision turns on.
	CurrentPrimary string
	TargetPrimary  string
	// Evidence is the quorum record the primary read out of its own postmaster.
	Evidence Evidence
	// FailingSince is the persisted debounce origin, zero when the primary is not currently
	// believed to be failing.
	FailingSince time.Time
	// FailoverDelay is how long an unhealthy primary is tolerated before phase one begins.
	FailoverDelay time.Duration
	// QuorumGateEnabled mirrors spec.highAvailability.failoverQuorum. Turning it off
	// permits promoting a standby that cannot be proven to hold the last acknowledged
	// commit, and is never the default.
	QuorumGateEnabled bool
	// Maintenance is the members a rolling operation is about to disrupt: restart for a
	// parameter that needs one, drain for a node that is going away, an explicit rollout.
	//
	// It is an input to this decision rather than a separate mechanism because a planned
	// handover and an unplanned failover choose their candidate against the same evidence
	// and must refuse for the same reasons. A member named here is disqualified from
	// candidacy - promoting onto a pod that is about to be restarted buys nothing - and a
	// primary named here is handed over before it is disrupted.
	Maintenance []string
	// Now is the operator's clock.
	Now time.Time
}

// UnderMaintenance reports whether a member is one the caller intends to disrupt.
func (o Observation) UnderMaintenance(member string) bool {
	return slices.Contains(o.Maintenance, member)
}

// Decision is what the reconcile should do about the failover, and nothing else.
type Decision struct {
	// Phase is the state the machine settled in.
	Phase Phase
	// TargetPrimary is the value to write to status.targetPrimary. Empty means leave it
	// exactly as it is.
	TargetPrimary string
	// DemoteMember is a member whose role label must be stripped so the read-write Service
	// stops selecting it.
	DemoteMember string
	// ServingPrimary is the member the read-write Service may select right now, empty when
	// nobody may be selected.
	ServingPrimary string
	// SetFailingSince and ClearFailingSince maintain the persisted debounce origin, which
	// lives on the CR so an operator restart does not restart the countdown.
	SetFailingSince   *time.Time
	ClearFailingSince bool
	// Veto is the named refusal, if any.
	Veto Veto
	// SplitBrain freezes all automated remediation.
	SplitBrain bool
	// Quorum records the gate's arithmetic whenever it was evaluated.
	Quorum QuorumVerdict
	// Candidate records the ranking and every disqualification.
	Candidate CandidateResult
	// Reason and Message describe the decision.
	Reason  string
	Message string
	// RequeueAfter asks for another look, for the states that are waiting on wall time
	// rather than on an event.
	RequeueAfter time.Duration
}

// FailoverInProgress is the one-comparison signal the sentinel exists to provide.
func FailoverInProgress(currentPrimary, targetPrimary string) bool {
	return targetPrimary != "" && targetPrimary != currentPrimary
}

// Decide runs the two-phase failover state machine.
//
// It writes nothing and promotes nothing. The operator's entire authority over a failover
// is status.targetPrimary; the promotion itself belongs to the chosen member's own agent,
// behind the Lease, so that a confused operator cannot promote anybody and a dead operator
// cannot promote anybody either.
func Decide(observation Observation) Decision {
	decision := decide(observation)
	decision.ServingPrimary = servingPrimary(observation, decision)
	return decision
}

// servingPrimary is the member the read-write Service may select, which is a different
// question from "is a failover in progress" and must not be answered with it.
//
// Freezing the role label for the whole of a failover leaves a healthy current primary
// unlabelled - and an <instance>-rw Service with no endpoints refuses every connection -
// for as long as the operator is merely *considering* a failover. What the freeze exists
// for is the demoted primary, and that is exactly the first clause below: the instant a
// different member is named in targetPrimary, nobody is serving until that member says it
// is.
func servingPrimary(observation Observation, decision Decision) string {
	primary := observation.CurrentPrimary
	if primary == "" || decision.SplitBrain || decision.DemoteMember == primary {
		return ""
	}
	target := observation.TargetPrimary
	if decision.TargetPrimary != "" {
		target = decision.TargetPrimary
	}
	if target != "" && target != TargetPrimaryPending && target != primary {
		return ""
	}
	// An unreachable member is not a demoted one. Under either of these two vetoes the
	// evidence says the operator cannot see the member rather than that the member has
	// stopped serving, and taking the label away on that reading would break a healthy
	// instance because the operator was having a bad day.
	if decision.Veto == VetoOperatorIsolated || decision.Veto == VetoPrimaryUnobservable {
		return primary
	}
	member, known := memberNamed(observation.Members, primary)
	if !known || !member.StatusReachable || member.InRecovery {
		return ""
	}
	return primary
}

func decide(observation Observation) Decision {
	if brain := splitBrain(observation); brain != nil {
		return *brain
	}
	if observation.CurrentPrimary == "" {
		return Decision{
			Phase:   PhaseSteady,
			Reason:  "NoPrimaryElectedYet",
			Message: "the instance has not finished bootstrapping",
		}
	}
	if isolated := operatorIsolated(observation); isolated != nil {
		return *isolated
	}
	// Waiting on a promotion is only waiting if there is something to wait for. Phase
	// Promoting requeues every second and returns before every election below it, so a target
	// that is not a member of the instance at all - deleted, renamed, or never created -
	// leaves the instance with no primary and no way to elect one, for ever, one second at a
	// time. A member that is merely slow or unreachable is still a member and is still
	// waited for: this only steps aside when there is nobody of that name to promote.
	if _, present := memberNamed(observation.Members, observation.TargetPrimary); present &&
		FailoverInProgress(observation.CurrentPrimary, observation.TargetPrimary) &&
		observation.TargetPrimary != TargetPrimaryPending {
		return Decision{
			Phase:        PhasePromoting,
			Reason:       "AwaitingPromotion",
			Message:      fmt.Sprintf("%s has been told to promote", observation.TargetPrimary),
			RequeueAfter: time.Second,
		}
	}

	primary, known := memberNamed(observation.Members, observation.CurrentPrimary)
	if known && primary.StatusReachable && !primary.InRecovery {
		return healthyPrimary(observation)
	}
	if known && primary.PodReady && !primary.StatusReachable {
		return vetoed(observation, VetoPrimaryUnobservable,
			fmt.Sprintf("the kubelet still reports %s Ready while its status endpoint is failing; "+
				"deferring until the kubelet disagrees", primary.Name))
	}
	return failing(observation)
}

// splitBrain is a dedicated alarm and never a tiebreak input. Two members out of recovery at
// once means somebody's writes are about to be discarded, and quietly picking one of them
// hides the loss instead of surfacing it.
func splitBrain(observation Observation) *Decision {
	var primaries []string
	for _, member := range observation.Members {
		if member.StatusReachable && !member.InRecovery {
			primaries = append(primaries, member.Name)
		}
	}
	if len(primaries) < 2 {
		return nil
	}
	slices.Sort(primaries)
	return &Decision{
		Phase:      PhaseSplitBrain,
		SplitBrain: true,
		Reason:     ReasonSplitBrain,
		Message: fmt.Sprintf(
			"%v all report pg_is_in_recovery() = false; every automated remediation is refused",
			primaries),
	}
}

// operatorIsolated is veto (a). If no member the kubelet calls Ready will answer the
// operator, the partitioned party is the operator, and failing over on that evidence would
// promote a candidate the rest of the world can already see is unnecessary.
func operatorIsolated(observation Observation) *Decision {
	var ready, reachable int
	for _, member := range observation.Members {
		if !member.PodReady {
			continue
		}
		ready++
		if member.StatusReachable {
			reachable++
		}
	}
	if ready == 0 || reachable > 0 {
		return nil
	}
	decision := vetoed(observation, VetoOperatorIsolated, fmt.Sprintf(
		"none of the %d Ready members answered; the operator is the isolated party, so no failover is started",
		ready))
	return &decision
}

func healthyPrimary(observation Observation) Decision {
	// The primary is reachable and out of recovery on every path from here, including the
	// maintenance one - plannedSwitchover exists precisely because the primary is alive. So a
	// debounce origin left by an earlier blip is stale whichever way this goes, and clearing
	// it only on the ordinary path meant a primary under maintenance kept one indefinitely.
	// failing() then reads that stale instant as the start of a new failure and can skip
	// failoverDelay entirely on the next real one.
	stale := !observation.FailingSince.IsZero()
	if observation.UnderMaintenance(observation.CurrentPrimary) {
		decision := plannedSwitchover(observation)
		decision.ClearFailingSince = decision.ClearFailingSince || stale
		return decision
	}
	decision := Decision{
		Phase:             PhaseSteady,
		ClearFailingSince: stale,
		Reason:            "PrimaryHealthy",
		Message:           observation.CurrentPrimary + " is serving",
	}
	if observation.TargetPrimary != observation.CurrentPrimary {
		decision.TargetPrimary = observation.CurrentPrimary
	}
	return decision
}

// plannedSwitchover hands the role over before the member holding it is disrupted.
//
// It is the healthy-primary path, not a second failover: the primary is answering, its
// quorum evidence is current, and the moment was chosen. What it shares with the failover
// is every refusal - the quorum gate and candidate selection are the same functions
// against the same evidence, because a handover that cannot be proven not to lose an
// acknowledged commit is exactly as unacceptable when it was planned.
//
// The two differences from chooseCandidate are both consequences of the primary being
// alive. There is no wait for every WAL receiver to go down, because they are streaming
// from a primary that has not stopped and never will while this decision is being made -
// waiting for that would deadlock the handover it is gating. And the quorum evidence is
// measured against now rather than against a failing instant, because there is no failing
// instant.
//
// A refusal here leaves the primary serving. That is the whole safety property: the worst
// outcome of a switchover that cannot be done safely is that the maintenance does not
// happen, which is a decision a human can act on, rather than an instance that lost its
// primary and could not replace it.
func plannedSwitchover(observation Observation) Decision {
	verdict := EvaluateQuorum(observation.Evidence, reachableMembers(observation.Members),
		observation.Now)
	if observation.QuorumGateEnabled && !verdict.Satisfied {
		return Decision{
			Phase:  PhaseVetoed,
			Quorum: verdict,
			Reason: verdict.Reason,
			Message: fmt.Sprintf(
				"the planned switchover away from %s is denied because %s; it keeps serving",
				observation.CurrentPrimary, verdict.Message),
			RequeueAfter: 5 * time.Second,
		}
	}

	candidates := SelectCandidate(CandidateInput{
		Members:              observation.Members,
		KnownPrimary:         observation.CurrentPrimary,
		LastKnownTimeline:    LastKnownTimeline(observation.Members),
		SyncSet:              observation.Evidence.VotingMembers,
		StreamingAtDetection: observation.Evidence.StreamingMembers,
		Maintenance:          observation.Maintenance,
	})
	if candidates.Candidate == "" {
		return Decision{
			Phase:     PhaseVetoed,
			Quorum:    verdict,
			Candidate: candidates,
			Reason:    ReasonSwitchoverNoCandidate,
			Message: fmt.Sprintf(
				"%s is due to be disrupted but every other member was disqualified: %v",
				observation.CurrentPrimary, candidates.Disqualified),
			RequeueAfter: 5 * time.Second,
		}
	}

	chosen, _ := memberNamed(observation.Members, candidates.Candidate)
	if chosen.WALVolumeFull {
		decision := vetoed(observation, VetoCandidateWALVolumeFull,
			chosen.Name+" has a full WAL volume; handing the role to it buys a primary that PANICs")
		decision.Candidate, decision.Quorum = candidates, verdict
		return decision
	}
	if !chosen.PodReady {
		decision := vetoed(observation, VetoCandidateNotReady,
			chosen.Name+" answers over HTTP but its Pod is not Ready; waiting rather than "+
				"handing the role to a Pod no Service will select")
		decision.Candidate, decision.Quorum = candidates, verdict
		return decision
	}

	return Decision{
		Phase:         PhasePlannedSwitchover,
		TargetPrimary: candidates.Candidate,
		Quorum:        verdict,
		Candidate:     candidates,
		Reason:        ReasonSwitchoverChosen,
		Message: fmt.Sprintf("%s is due to be disrupted, so %s takes the role; %s",
			observation.CurrentPrimary, candidates.Candidate, verdict.Message),
		RequeueAfter: time.Second,
	}
}

// failing runs the debounce and then the two phases.
func failing(observation Observation) Decision {
	if observation.FailingSince.IsZero() {
		when := observation.Now
		return Decision{
			Phase:           PhaseDebouncing,
			SetFailingSince: &when,
			Reason:          "PrimaryUnhealthy",
			Message: fmt.Sprintf("%s stopped answering; debouncing for %s",
				observation.CurrentPrimary, observation.FailoverDelay),
			RequeueAfter: observation.FailoverDelay,
		}
	}
	if elapsed := observation.Now.Sub(observation.FailingSince); elapsed < observation.FailoverDelay {
		return Decision{
			Phase:  PhaseDebouncing,
			Reason: "WithinFailoverDelay",
			Message: fmt.Sprintf("%s has been failing for %s of %s",
				observation.CurrentPrimary, elapsed.Truncate(time.Second), observation.FailoverDelay),
			RequeueAfter: observation.FailoverDelay - elapsed,
		}
	}

	if observation.TargetPrimary != TargetPrimaryPending {
		return Decision{
			Phase:         PhaseSentinel,
			TargetPrimary: TargetPrimaryPending,
			DemoteMember:  observation.CurrentPrimary,
			Reason:        "FailoverStarted",
			Message: fmt.Sprintf(
				"%s is unhealthy; the role label is stripped and targetPrimary is the sentinel",
				observation.CurrentPrimary),
			RequeueAfter: time.Second,
		}
	}
	return chooseCandidate(observation)
}

// chooseCandidate is phase two. It runs only once every non-primary member reports its WAL
// receiver down, because a receiver still streaming means WAL is still arriving from
// somewhere and the position being ranked on is still moving.
func chooseCandidate(observation Observation) Decision {
	if streaming := receiversStillUp(observation); len(streaming) > 0 {
		return Decision{
			Phase:        PhaseWaitingWALReceivers,
			Reason:       "WALReceiversStillActive",
			Message:      fmt.Sprintf("%v still report an active WAL receiver", streaming),
			RequeueAfter: time.Second,
		}
	}

	verdict := EvaluateQuorum(observation.Evidence, reachableMembers(observation.Members),
		quorumReference(observation))
	if observation.QuorumGateEnabled && !verdict.Satisfied {
		return Decision{
			Phase:   PhaseVetoed,
			Quorum:  verdict,
			Reason:  verdict.Reason,
			Message: "the failover is denied because " + verdict.Message,
			// A denied quorum gate is not a dead end: the missing standby may come back.
			RequeueAfter: 5 * time.Second,
		}
	}

	candidates := SelectCandidate(CandidateInput{
		Members:              observation.Members,
		KnownPrimary:         observation.CurrentPrimary,
		LastKnownTimeline:    LastKnownTimeline(observation.Members),
		SyncSet:              observation.Evidence.VotingMembers,
		StreamingAtDetection: observation.Evidence.StreamingMembers,
	})
	if candidates.Candidate == "" {
		return Decision{
			Phase:        PhaseVetoed,
			Quorum:       verdict,
			Candidate:    candidates,
			Reason:       ReasonNoEligibleCandidate,
			Message:      fmt.Sprintf("every member was disqualified: %v", candidates.Disqualified),
			RequeueAfter: 5 * time.Second,
		}
	}

	chosen, _ := memberNamed(observation.Members, candidates.Candidate)
	if chosen.WALVolumeFull {
		decision := vetoed(observation, VetoCandidateWALVolumeFull,
			chosen.Name+" has a full WAL volume; promoting onto it buys a primary that PANICs")
		decision.Candidate = candidates
		decision.Quorum = verdict
		return decision
	}
	if !chosen.PodReady {
		decision := vetoed(observation, VetoCandidateNotReady,
			chosen.Name+" answers over HTTP but its Pod is not Ready; waiting rather than "+
				"promoting into a Pod no Service will select")
		decision.Candidate = candidates
		decision.Quorum = verdict
		return decision
	}

	return Decision{
		Phase:         PhaseCandidateChosen,
		TargetPrimary: candidates.Candidate,
		Quorum:        verdict,
		Candidate:     candidates,
		Reason:        "CandidateSelected",
		Message:       fmt.Sprintf("%s was chosen; %s", candidates.Candidate, verdict.Message),
		RequeueAfter:  time.Second,
	}
}

// quorumReference is the instant the evidence's age is measured against.
func quorumReference(observation Observation) time.Time {
	if observation.FailingSince.IsZero() {
		return observation.Now
	}
	return observation.FailingSince
}

func receiversStillUp(observation Observation) []string {
	var streaming []string
	for _, member := range observation.Members {
		if member.Name == observation.CurrentPrimary || !member.StatusReachable {
			continue
		}
		if member.WALReceiverActive {
			streaming = append(streaming, member.Name)
		}
	}
	slices.Sort(streaming)
	return streaming
}

// reachableMembers is R: the members whose own status endpoint answered and reported a
// postmaster accepting connections.
//
// It deliberately does not consult the Pod's Ready condition. The kubelet's verdict and the
// member's own report are two different questions, and the case where they disagree is
// precisely veto (c) - which would be unreachable if a not-Ready Pod already failed the
// quorum gate under a different name.
func reachableMembers(members []Member) []string {
	var reachable []string
	for _, member := range members {
		if member.StatusReachable {
			reachable = append(reachable, member.Name)
		}
	}
	return reachable
}

func memberNamed(members []Member, name string) (Member, bool) {
	for _, member := range members {
		if member.Name == name {
			return member, true
		}
	}
	return Member{}, false
}

func vetoed(observation Observation, veto Veto, message string) Decision {
	return Decision{
		Phase:        PhaseVetoed,
		Veto:         veto,
		Reason:       string(veto),
		Message:      message,
		RequeueAfter: max(observation.FailoverDelay, 5*time.Second),
	}
}
