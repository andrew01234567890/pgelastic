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
	"testing"
	"time"
)

const failoverDelay = 10 * time.Second

var now = time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

func healthyInstance() Observation {
	primary := Member{
		Name: memberOne, Timeline: 5, StatusReachable: true, PodReady: true,
		ReceivedLSN: MustParseLSN("2/0"),
	}
	return Observation{
		Members: []Member{
			primary,
			standby(memberTwo, 5, "2/0", "2/0"),
			standby(memberThree, 5, "1/F0", "1/F0"),
		},
		CurrentPrimary:    memberOne,
		TargetPrimary:     memberOne,
		Evidence:          anyOneEvidence(),
		FailoverDelay:     failoverDelay,
		QuorumGateEnabled: true,
		Now:               now,
	}
}

// failedPrimary is a healthy instance whose primary has stopped answering and whose
// standbys have already noticed, which is the state phase two requires.
func failedPrimary() Observation {
	observation := healthyInstance()
	observation.Members[0].StatusReachable = false
	observation.Members[0].PodReady = false
	observation.Evidence.ObservedAt = now.Add(-time.Second)
	return observation
}

func TestAHealthyPrimaryIsSteady(t *testing.T) {
	decision := Decide(healthyInstance())

	if decision.Phase != PhaseSteady {
		t.Fatalf("phase was %s: %s", decision.Phase, decision.Message)
	}
	if decision.TargetPrimary != "" {
		t.Fatalf("nothing needs writing, got %q", decision.TargetPrimary)
	}
}

func TestTheDebounceOriginIsPersistedOnce(t *testing.T) {
	observation := failedPrimary()

	first := Decide(observation)
	if first.Phase != PhaseDebouncing || first.SetFailingSince == nil {
		t.Fatalf("decision was %+v", first)
	}
	if !first.SetFailingSince.Equal(now) {
		t.Fatalf("failingSince was %s", first.SetFailingSince)
	}

	observation.FailingSince = *first.SetFailingSince
	observation.Now = now.Add(time.Second)
	second := Decide(observation)
	if second.Phase != PhaseDebouncing {
		t.Fatalf("phase was %s", second.Phase)
	}
	if second.SetFailingSince != nil {
		t.Fatal("the debounce origin must survive rather than be restamped every reconcile")
	}
}

func TestAPrimaryThatRecoversInsideTheDebounceClearsTheOrigin(t *testing.T) {
	observation := healthyInstance()
	observation.FailingSince = now.Add(-time.Second)

	decision := Decide(observation)

	if decision.Phase != PhaseSteady || !decision.ClearFailingSince {
		t.Fatalf("decision was %+v", decision)
	}
}

func TestPhaseOneWritesTheSentinelAndStripsTheRoleLabel(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)

	decision := Decide(observation)

	if decision.Phase != PhaseSentinel {
		t.Fatalf("phase was %s: %s", decision.Phase, decision.Message)
	}
	if decision.TargetPrimary != TargetPrimaryPending {
		t.Fatalf("targetPrimary was %q, want the reserved sentinel", decision.TargetPrimary)
	}
	if decision.DemoteMember != memberOne {
		t.Fatalf("the old primary must lose its role label, got %q", decision.DemoteMember)
	}
	if !FailoverInProgress(observation.CurrentPrimary, decision.TargetPrimary) {
		t.Fatal("the sentinel must make targetPrimary != currentPrimary a total signal")
	}
}

func TestPhaseTwoWaitsForEveryWALReceiverToGoDown(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending
	observation.Members[1].WALReceiverActive = true

	decision := Decide(observation)

	if decision.Phase != PhaseWaitingWALReceivers {
		t.Fatalf("phase was %s: %s", decision.Phase, decision.Message)
	}
	if decision.TargetPrimary != "" {
		t.Fatal("no candidate may be written while WAL is still arriving")
	}
}

func TestPhaseTwoChoosesTheFurthestAheadCandidate(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending

	decision := Decide(observation)

	if decision.Phase != PhaseCandidateChosen {
		t.Fatalf("phase was %s: %s", decision.Phase, decision.Message)
	}
	if decision.TargetPrimary != memberTwo {
		t.Fatalf("targetPrimary was %q, want pg-2", decision.TargetPrimary)
	}
	if !decision.Quorum.Satisfied {
		t.Fatalf("quorum was %+v", decision.Quorum)
	}
}

func TestAFailoverIsDeniedWhenOnlyOneStandbyIsReachable(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending
	observation.Members[2].StatusReachable = false

	decision := Decide(observation)

	if decision.Phase != PhaseVetoed {
		t.Fatalf("phase was %s: %s", decision.Phase, decision.Message)
	}
	if decision.Reason != QuorumNotProven {
		t.Fatalf("reason was %q", decision.Reason)
	}
	if decision.TargetPrimary != "" {
		t.Fatal("a denied quorum gate must not write a candidate")
	}
}

func TestMissingEvidenceDeniesTheFailoverEntirely(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending
	observation.Evidence = Evidence{}

	decision := Decide(observation)

	if decision.Phase != PhaseVetoed || decision.Reason != QuorumEvidenceMissing {
		t.Fatalf("decision was %+v", decision)
	}
}

func TestDisablingTheQuorumGatePermitsAnUnprovenPromotion(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending
	observation.Members[2].StatusReachable = false
	observation.QuorumGateEnabled = false

	decision := Decide(observation)

	if decision.Phase != PhaseCandidateChosen || decision.TargetPrimary != memberTwo {
		t.Fatalf("decision was %+v", decision)
	}
}

func TestVetoOperatorIsolated(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending
	for i := range observation.Members {
		observation.Members[i].StatusReachable = false
		observation.Members[i].PodReady = true
	}

	decision := Decide(observation)

	if decision.Veto != VetoOperatorIsolated {
		t.Fatalf("veto was %q: %s", decision.Veto, decision.Message)
	}
	if decision.TargetPrimary != "" {
		t.Fatal("an isolated operator must never fail over")
	}
}

func TestVetoPrimaryUnobservable(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.Members[0].PodReady = true

	decision := Decide(observation)

	if decision.Veto != VetoPrimaryUnobservable {
		t.Fatalf("veto was %q: %s", decision.Veto, decision.Message)
	}
}

func TestVetoCandidateNotReady(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending
	observation.Members[1].PodReady = false
	observation.Members[2].PodReady = false

	decision := Decide(observation)

	if decision.Veto != VetoCandidateNotReady {
		t.Fatalf("veto was %q: %s", decision.Veto, decision.Message)
	}
	if decision.TargetPrimary != "" {
		t.Fatal("a candidate whose Pod is not Ready must be waited for, not promoted")
	}
}

func TestVetoCandidateWALVolumeFull(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending
	observation.Members[1].WALVolumeFull = true

	decision := Decide(observation)

	if decision.Veto != VetoCandidateWALVolumeFull {
		t.Fatalf("veto was %q: %s", decision.Veto, decision.Message)
	}
}

func TestTwoPrimariesFreezeEverything(t *testing.T) {
	observation := healthyInstance()
	observation.Members[1].InRecovery = false

	decision := Decide(observation)

	if decision.Phase != PhaseSplitBrain || !decision.SplitBrain {
		t.Fatalf("decision was %+v", decision)
	}
	if decision.TargetPrimary != "" || decision.DemoteMember != "" {
		t.Fatal("split brain must refuse every automated remediation, not pick a winner")
	}
	if decision.Reason != ReasonSplitBrain {
		t.Fatalf("reason was %q", decision.Reason)
	}
}

func TestAChosenCandidateIsNotReconsidered(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = memberTwo

	decision := Decide(observation)

	if decision.Phase != PhasePromoting {
		t.Fatalf("phase was %s", decision.Phase)
	}
	if decision.TargetPrimary != "" {
		t.Fatal("a committed decision must not be rewritten by a later reconcile")
	}
}

func TestNoCandidateSurvivesDisqualification(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending
	observation.Evidence.StreamingMembers = nil

	decision := Decide(observation)

	if decision.Phase != PhaseVetoed || decision.Reason != ReasonNoEligibleCandidate {
		t.Fatalf("decision was %+v", decision)
	}
}

// PhasePromoting requeues every second and returns before every election below it, so it is
// absorbing: nothing that follows can run while it holds. A target that is not a member of
// the instance at all leaves it with no primary and no way to elect one, indefinitely, one
// second at a time.
func TestAPromotionIsNotAwaitedFromAMemberThatDoesNotExist(t *testing.T) {
	observation := Observation{
		CurrentPrimary: memberOne,
		TargetPrimary:  "pg-9",
		Members: []Member{
			{Name: memberOne, PodReady: true, StatusReachable: true, InRecovery: false},
			{Name: memberTwo, PodReady: true, StatusReachable: true, InRecovery: true},
		},
	}

	decision := Decide(observation)

	if decision.Phase == PhasePromoting {
		t.Fatalf("the instance is waiting for %q to promote, and no such member exists",
			observation.TargetPrimary)
	}
}

// healthyPrimary cleared the persisted debounce origin, but returned into plannedSwitchover
// before doing so when the primary was under maintenance. The primary is alive on that path by
// definition, so an origin left by an earlier blip survived its recovery indefinitely - and
// failing() reads a stale instant as the start of a new failure, which can skip failoverDelay
// on the next real one.
func TestMaintenanceDoesNotPreserveAStaleFailureOrigin(t *testing.T) {
	now := time.Now()
	observation := Observation{
		CurrentPrimary: memberOne,
		TargetPrimary:  memberOne,
		FailingSince:   now.Add(-time.Hour),
		Now:            now,
		Maintenance:    []string{memberOne},
		Members: []Member{
			{Name: memberOne, PodReady: true, StatusReachable: true, InRecovery: false},
			{Name: memberTwo, PodReady: true, StatusReachable: true, InRecovery: true},
		},
	}

	decision := healthyPrimary(observation)

	if !decision.ClearFailingSince {
		t.Fatal("a primary under maintenance kept an hour-old failure origin, so the next " +
			"real failure is measured from it")
	}
}
