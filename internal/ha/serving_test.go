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

func TestAHealthyPrimaryIsServing(t *testing.T) {
	if serving := Decide(healthyInstance()).ServingPrimary; serving != memberOne {
		t.Fatalf("servingPrimary = %q, want %s", serving, memberOne)
	}
}

func TestAPrimaryStillServingKeepsTheReadWriteServiceWhileTheSentinelIsSet(t *testing.T) {
	observation := healthyInstance()
	observation.TargetPrimary = TargetPrimaryPending
	observation.FailingSince = now.Add(-2 * failoverDelay)

	decision := Decide(observation)

	if decision.ServingPrimary != memberOne {
		t.Fatalf("servingPrimary = %q: an endpoint-less read-write Service refuses every "+
			"connection a serving primary could have answered", decision.ServingPrimary)
	}
}

func TestANamedSuccessorTakesTheReadWriteServiceFromTheOldPrimaryAtOnce(t *testing.T) {
	observation := healthyInstance()
	observation.TargetPrimary = memberTwo

	if serving := Decide(observation).ServingPrimary; serving != "" {
		t.Fatalf("servingPrimary = %q: a member being demoted must stop being selected", serving)
	}
}

func TestAPrimaryNobodyCanReachStopsBeingServing(t *testing.T) {
	observation := failedPrimary()
	observation.FailingSince = now.Add(-2 * failoverDelay)

	decision := Decide(observation)

	if decision.ServingPrimary != "" {
		t.Fatalf("servingPrimary = %q, want nobody", decision.ServingPrimary)
	}
	if decision.DemoteMember != memberOne {
		t.Fatalf("demoteMember = %q, want %s", decision.DemoteMember, memberOne)
	}
}

func TestAnIsolatedOperatorTakesNobodysLabelAway(t *testing.T) {
	observation := healthyInstance()
	for i := range observation.Members {
		observation.Members[i].StatusReachable = false
	}

	decision := Decide(observation)

	if decision.Veto != VetoOperatorIsolated {
		t.Fatalf("veto = %q, want %s", decision.Veto, VetoOperatorIsolated)
	}
	if decision.ServingPrimary != memberOne {
		t.Fatalf("servingPrimary = %q: the operator being the partitioned party is not "+
			"evidence that the instance stopped serving", decision.ServingPrimary)
	}
}

func TestAPrimaryTheKubeletStillCallsReadyKeepsItsLabel(t *testing.T) {
	observation := healthyInstance()
	observation.Members[0].StatusReachable = false

	decision := Decide(observation)

	if decision.Veto != VetoPrimaryUnobservable {
		t.Fatalf("veto = %q, want %s", decision.Veto, VetoPrimaryUnobservable)
	}
	if decision.ServingPrimary != memberOne {
		t.Fatalf("servingPrimary = %q, want %s while the decision is deferred",
			decision.ServingPrimary, memberOne)
	}
}

func TestSplitBrainLeavesTheLabelAlone(t *testing.T) {
	observation := healthyInstance()
	observation.Members[1].InRecovery = false

	decision := Decide(observation)

	if !decision.SplitBrain {
		t.Fatal("two members out of recovery must raise the alarm")
	}
	if decision.ServingPrimary != "" {
		t.Fatalf("servingPrimary = %q: no automated remediation runs during a split brain",
			decision.ServingPrimary)
	}
}

func TestAPrimaryBackInRecoveryStopsBeingServing(t *testing.T) {
	observation := healthyInstance()
	observation.Members[0].InRecovery = true
	observation.Now = now.Add(time.Second)

	if serving := Decide(observation).ServingPrimary; serving != "" {
		t.Fatalf("servingPrimary = %q, want nobody", serving)
	}
}
