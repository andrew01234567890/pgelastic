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
	"time"
)

// primaryUnderMaintenance is a healthy instance whose primary is about to be disrupted:
// the input a rolling restart or a node drain produces.
func primaryUnderMaintenance() Observation {
	observation := healthyInstance()
	observation.Maintenance = []string{memberOne}
	observation.Evidence.ObservedAt = now.Add(-time.Second)
	return observation
}

func TestAHealthyPrimaryDueForDisruptionHandsTheRoleOver(t *testing.T) {
	decision := Decide(primaryUnderMaintenance())

	if decision.Phase != PhasePlannedSwitchover {
		t.Fatalf("phase was %s: %s", decision.Phase, decision.Message)
	}
	if decision.TargetPrimary != memberTwo {
		t.Fatalf("targetPrimary was %q, want the most advanced standby", decision.TargetPrimary)
	}
	if !decision.Quorum.Satisfied {
		t.Fatalf("the quorum arithmetic was not recorded: %+v", decision.Quorum)
	}
}

// The candidate is chosen by the same ranking an unplanned failover uses, and the ranking
// is what makes the handover lossless: memberThree is behind, so it must not be picked
// merely because it was named first.
func TestThePlannedCandidateIsRankedNotTakenInOrder(t *testing.T) {
	observation := primaryUnderMaintenance()
	observation.Members[1] = standby(memberTwo, 5, "1/A0", "1/A0")
	observation.Members[2] = standby(memberThree, 5, "2/0", "2/0")

	decision := Decide(observation)
	if decision.TargetPrimary != memberThree {
		t.Fatalf("targetPrimary was %q, want the member holding more WAL", decision.TargetPrimary)
	}
}

// The gate that must not be softened because the moment was chosen. With "ANY 1" over two
// voters the arithmetic needs both of them reachable, and one missing means there is no
// way to tell which standby acknowledged the last commit.
func TestAPlannedSwitchoverIsRefusedWhenQuorumCannotBeSatisfied(t *testing.T) {
	observation := primaryUnderMaintenance()
	observation.Members[2].StatusReachable = false

	decision := Decide(observation)
	if decision.Phase != PhaseVetoed {
		t.Fatalf("phase was %s: %s", decision.Phase, decision.Message)
	}
	if decision.TargetPrimary != "" {
		t.Fatalf("a refused switchover wrote targetPrimary %q", decision.TargetPrimary)
	}
	if decision.Quorum.Reason != QuorumNotProven {
		t.Fatalf("reason was %q, want the quorum gate's own", decision.Quorum.Reason)
	}
}

// A refused switchover leaves the primary serving. Losing the role label as well would
// turn "the maintenance does not happen" into "the instance has no primary", which is the
// opposite of what refusing is for.
func TestARefusedSwitchoverLeavesThePrimaryServing(t *testing.T) {
	observation := primaryUnderMaintenance()
	observation.Members[2].StatusReachable = false

	decision := Decide(observation)
	if decision.ServingPrimary != memberOne {
		t.Fatalf("servingPrimary was %q, want the primary that is still healthy",
			decision.ServingPrimary)
	}
}

func TestAPlannedSwitchoverProceedsWithoutTheQuorumGateWhenItIsTurnedOff(t *testing.T) {
	observation := primaryUnderMaintenance()
	observation.Members[2].StatusReachable = false
	observation.QuorumGateEnabled = false

	decision := Decide(observation)
	if decision.Phase != PhasePlannedSwitchover || decision.TargetPrimary != memberTwo {
		t.Fatalf("decision was %s/%q: %s", decision.Phase, decision.TargetPrimary, decision.Message)
	}
}

// Handing the role to a member that is itself next on the list means switching over twice,
// and the second switchover has one fewer member to choose from.
func TestAMemberAlsoDueForDisruptionIsNotACandidate(t *testing.T) {
	observation := primaryUnderMaintenance()
	observation.Maintenance = []string{memberOne, memberTwo}

	decision := Decide(observation)
	if decision.TargetPrimary != memberThree {
		t.Fatalf("targetPrimary was %q, want the member nothing is about to disrupt",
			decision.TargetPrimary)
	}
	if !slices.Contains(decision.Candidate.Disqualified,
		Disqualification{Member: memberTwo, Reason: DisqualifiedUnderMaintenance}) {
		t.Fatalf("disqualifications were %+v", decision.Candidate.Disqualified)
	}
}

func TestASwitchoverWithNowhereToGoIsRefusedAndTheInstanceKeepsItsPrimary(t *testing.T) {
	observation := primaryUnderMaintenance()
	observation.Maintenance = []string{memberOne, memberTwo, memberThree}

	decision := Decide(observation)
	if decision.Phase != PhaseVetoed || decision.Reason != ReasonSwitchoverNoCandidate {
		t.Fatalf("decision was %s/%s: %s", decision.Phase, decision.Reason, decision.Message)
	}
	if decision.ServingPrimary != memberOne {
		t.Fatalf("servingPrimary was %q", decision.ServingPrimary)
	}
}

func TestACandidateWithAFullWALVolumeIsRefusedForAPlannedSwitchoverToo(t *testing.T) {
	observation := primaryUnderMaintenance()
	observation.Members[1].WALVolumeFull = true
	observation.Members[2] = standby(memberThree, 5, "1/00", "1/00")
	observation.Evidence.VotingMembers = []string{memberTwo}
	observation.Evidence.StreamingMembers = []string{memberTwo}
	observation.Evidence.SynchronousStandbyNames = `ANY 1 ("pg-2")`

	decision := Decide(observation)
	if decision.Veto != VetoCandidateWALVolumeFull {
		t.Fatalf("veto was %q: %s", decision.Veto, decision.Message)
	}
}

// A member nobody is disrupting is the steady state, which is the guard that the planned
// branch is reached by the maintenance input and not by anything else.
func TestAnInstanceWithNoMaintenanceIsStillSteady(t *testing.T) {
	observation := healthyInstance()
	observation.Maintenance = []string{memberTwo}

	decision := Decide(observation)
	if decision.Phase != PhaseSteady {
		t.Fatalf("phase was %s: %s", decision.Phase, decision.Message)
	}
}

// An unplanned failover must not inherit the planned path's disqualification. Nothing is
// being disrupted on purpose, so an empty maintenance set has to leave selection exactly
// as it was.
func TestAnUnplannedFailoverIsUnaffectedByTheMaintenanceInput(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-failoverDelay)
	observation.TargetPrimary = TargetPrimaryPending

	decision := Decide(observation)
	if decision.Phase != PhaseCandidateChosen || decision.TargetPrimary != memberTwo {
		t.Fatalf("decision was %s/%q: %s", decision.Phase, decision.TargetPrimary, decision.Message)
	}
}

// The agent reads this to decide whether its stop is clean or immediate, so a malformed
// value must name nobody rather than name the wrong member.
func TestMaintenanceMembersParsesTheAnnotationAndIgnoresRubbish(t *testing.T) {
	for annotation, want := range map[string][]string{
		"":                           nil,
		"   ":                        nil,
		",,":                         nil,
		memberOne:                    {memberOne},
		" pg-2 , pg-1 ":              {memberOne, memberTwo},
		memberOne + ",pg-1":          {memberOne},
		"pg-3,\tpg-1\n,,   , pg-2  ": {memberOne, memberTwo, memberThree},
	} {
		annotations := map[string]string{AnnotationMaintenance: annotation}
		if got := MaintenanceMembers(annotations); !slices.Equal(got, want) {
			t.Errorf("MaintenanceMembers(%q) = %v, want %v", annotation, got, want)
		}
	}
	if MaintenanceMembers(nil) != nil {
		t.Error("an object with no annotations is disrupting nobody")
	}
	if !UnderMaintenance(map[string]string{AnnotationMaintenance: "pg-1,pg-2"}, memberTwo) {
		t.Error("a named member must be reported as under maintenance")
	}
	if UnderMaintenance(map[string]string{AnnotationMaintenance: "pg-1"}, memberTwo) {
		t.Error("an unnamed member must not be")
	}
}
