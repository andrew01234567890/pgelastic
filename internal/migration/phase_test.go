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

package migration

import (
	"errors"
	"slices"
	"testing"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const (
	preflight    = pgelasticv1alpha1.TenantMigrationPhasePreflight
	provisioning = pgelasticv1alpha1.TenantMigrationPhaseProvisioning
	preWarm      = pgelasticv1alpha1.TenantMigrationPhasePreWarm
	copying      = pgelasticv1alpha1.TenantMigrationPhaseCopying
	catchup      = pgelasticv1alpha1.TenantMigrationPhaseCatchup
	quiescing    = pgelasticv1alpha1.TenantMigrationPhaseQuiescing
	cutover      = pgelasticv1alpha1.TenantMigrationPhaseCutover
	completed    = pgelasticv1alpha1.TenantMigrationPhaseCompleted
	failedPhase  = pgelasticv1alpha1.TenantMigrationPhaseFailed
	aborted      = pgelasticv1alpha1.TenantMigrationPhaseAborted
	rolledBack   = pgelasticv1alpha1.TenantMigrationPhaseRolledBack

	online  = pgelasticv1alpha1.TenantMigrationOnline
	offline = pgelasticv1alpha1.TenantMigrationOffline
)

// ready builds an observation in which the given phase has finished its work.
func readyIn(phase Phase, strategy Strategy) Observation {
	observation := Observation{Strategy: strategy}
	switch phase {
	case preflight:
		observation.PreflightPassed = true
	case provisioning:
		observation.Provisioned = true
	case preWarm:
		observation.PreWarmed = true
	case copying:
		observation.CopyComplete = true
	case catchup:
		observation.CaughtUp = true
	case quiescing:
		observation.Drained = true
	case cutover:
		observation.CutoverComplete = true
	}
	return observation
}

func TestOnlineAdvancesThroughEveryPhaseInOrder(t *testing.T) {
	order := PhaseOrder(online)
	for index, phase := range order[:len(order)-1] {
		decision := Decide(phase, readyIn(phase, online))
		if decision.Phase != order[index+1] {
			t.Fatalf("from %s the machine went to %s, wanted %s", phase, decision.Phase, order[index+1])
		}
	}
}

func TestOfflineCopiesInsideThePauseAndSkipsCatchup(t *testing.T) {
	order := PhaseOrder(offline)
	if slices.Contains(order, catchup) {
		t.Fatal("the offline strategy has no replication stream and must not enter Catchup")
	}
	quiesceIndex := slices.Index(order, quiescing)
	copyIndex := slices.Index(order, copying)
	if copyIndex < quiesceIndex {
		t.Fatalf("offline copies at index %d, before the quiesce at %d; a dump taken while writes "+
			"continue is behind by whatever was written during it", copyIndex, quiesceIndex)
	}
	for index, phase := range order[:len(order)-1] {
		decision := Decide(phase, readyIn(phase, offline))
		if decision.Phase != order[index+1] {
			t.Fatalf("from %s the machine went to %s, wanted %s", phase, decision.Phase, order[index+1])
		}
	}
}

func TestAPhaseThatIsNotFinishedStaysPut(t *testing.T) {
	for _, phase := range PhaseOrder(online) {
		if phase == completed {
			continue
		}
		decision := Decide(phase, Observation{Strategy: online})
		if decision.Phase != phase {
			t.Fatalf("an unfinished %s advanced to %s", phase, decision.Phase)
		}
		if decision.Serving != ServingSource {
			t.Fatalf("an unfinished %s served from %s", phase, decision.Serving)
		}
	}
}

// TestEveryAbortPathLeavesTheTenantOnTheSource is the safety property the whole machine
// exists to hold. It is checked at every phase boundary of both strategies, for every way
// a migration can be stopped.
func TestEveryAbortPathLeavesTheTenantOnTheSource(t *testing.T) {
	// The two requests are applied to a phase that has just finished its work, because an
	// abort racing a legitimate advance is the ordering that matters. The two timeouts are
	// applied to a phase that has not finished, because that is the only state the engine
	// can observe them in: a drained phase is never past its drain budget.
	triggers := map[string]struct {
		ready bool
		apply func(*Observation)
	}{
		"abort requested": {ready: true, apply: func(o *Observation) { o.AbortRequested = true }},
		"fault past its budget": {ready: true, apply: func(o *Observation) {
			o.Fault = errors.New("source went away")
			o.FaultBudgetExceeded = true
		}},
		"fault inside its budget": {ready: true, apply: func(o *Observation) {
			o.Fault = errors.New("source went away")
		}},
		"drain timeout":   {apply: func(o *Observation) { o.DrainDeadlineExceeded = true }},
		"cutover timeout": {apply: func(o *Observation) { o.CutoverDeadlineExceeded = true }},
	}
	for _, strategy := range []Strategy{online, offline} {
		for _, phase := range PhaseOrder(strategy) {
			if phase == completed {
				continue
			}
			for name, trigger := range triggers {
				observation := Observation{Strategy: strategy}
				if trigger.ready {
					observation = readyIn(phase, strategy)
				}
				trigger.apply(&observation)
				decision := Decide(phase, observation)
				if decision.Serving != ServingSource {
					t.Fatalf("%s in %s under %s served from %s", strategy, phase, name, decision.Serving)
				}
			}
		}
	}
}

// TestEveryAbortDiscardsTheHalfBuiltTarget keeps a stopped migration from leaving a copy
// behind. That copy stopped receiving changes at an arbitrary instant and is
// indistinguishable from a complete one, so the next migration of the same tenant would
// either collide with it or replicate into stale data nobody knows is stale.
func TestEveryAbortDiscardsTheHalfBuiltTarget(t *testing.T) {
	for _, strategy := range []Strategy{online, offline} {
		for _, phase := range PhaseOrder(strategy) {
			if phase == completed {
				continue
			}
			for _, observation := range []Observation{
				{Strategy: strategy, AbortRequested: true},
				{Strategy: strategy, Fault: errors.New("boom"), FaultBudgetExceeded: true},
			} {
				decision := Decide(phase, observation)
				if !decision.DiscardTarget {
					t.Fatalf("%s stopping in %s left a half-built copy on the target", strategy, phase)
				}
			}
		}
	}
	if !Decide(completed, Observation{Strategy: online, RollbackRequested: true}).DiscardTarget {
		t.Fatal("a rollback left the stale target copy in place")
	}
}

func TestASuccessfulCutoverKeepsTheTarget(t *testing.T) {
	if Decide(cutover, readyIn(cutover, online)).DiscardTarget {
		t.Fatal("a successful cutover dropped the database the tenant now serves from")
	}
	if Decide(completed, Observation{Strategy: online, RollbackWindowClosed: true}).DiscardTarget {
		t.Fatal("closing the rollback window dropped the target rather than the source")
	}
}

func TestAnAbortRequestBeatsAReadyPhase(t *testing.T) {
	for _, phase := range PhaseOrder(online) {
		if phase == completed {
			continue
		}
		observation := readyIn(phase, online)
		observation.AbortRequested = true
		decision := Decide(phase, observation)
		if decision.Phase != aborted {
			t.Fatalf("a ready %s advanced to %s despite an abort request", phase, decision.Phase)
		}
		if !decision.Cleanup {
			t.Fatalf("the abort from %s did not run the cleanup ladder", phase)
		}
	}
}

// TestATransientFaultIsRetriedRatherThanFatal covers the failures that actually happen: a
// refused connection while a read-write Service reselects its endpoints, a member restarting.
// Ending a move on the first of those would make migration look unreliable for reasons that
// have nothing to do with migration.
func TestATransientFaultIsRetriedRatherThanFatal(t *testing.T) {
	for _, phase := range PhaseOrder(online) {
		if phase == completed {
			continue
		}
		observation := readyIn(phase, online)
		observation.Fault = errors.New("connection refused")
		decision := Decide(phase, observation)
		if decision.Phase != phase {
			t.Fatalf("a transient fault in %s ended the migration in %s", phase, decision.Phase)
		}
		if decision.Reason != ReasonRetrying {
			t.Fatalf("a retried fault in %s was reported as %s", phase, decision.Reason)
		}
		if decision.Serving != ServingSource {
			t.Fatalf("a retried fault in %s served from %s", phase, decision.Serving)
		}
		if decision.Cleanup || decision.DiscardTarget {
			t.Fatalf("a retried fault in %s tore down the work it is about to retry", phase)
		}
	}
}

func TestAFaultPastItsBudgetBeatsAReadyPhase(t *testing.T) {
	for _, phase := range PhaseOrder(online) {
		if phase == completed {
			continue
		}
		observation := readyIn(phase, online)
		observation.Fault = errors.New("boom")
		observation.FaultBudgetExceeded = true
		decision := Decide(phase, observation)
		if decision.Phase != failedPhase {
			t.Fatalf("a ready %s advanced to %s despite a fault", phase, decision.Phase)
		}
		if !decision.Cleanup {
			t.Fatalf("the failure from %s did not run the cleanup ladder", phase)
		}
	}
}

func TestAnAbortFromAQuiescedPhaseReleasesTheHeldClients(t *testing.T) {
	for _, strategy := range []Strategy{online, offline} {
		for _, phase := range PhaseOrder(strategy) {
			observation := Observation{Strategy: strategy, AbortRequested: true}
			decision := Decide(phase, observation)
			if Terminal(phase) {
				continue
			}
			if Quiesced(phase, strategy) != decision.ReleaseQuiesce {
				t.Fatalf("%s aborting from %s released=%t while quiesced=%t; a migration that "+
					"fails holding the sockets has turned a move into an outage",
					strategy, phase, decision.ReleaseQuiesce, Quiesced(phase, strategy))
			}
		}
	}
}

func TestTheDrainTimeoutOnlyAppliesToQuiescing(t *testing.T) {
	decision := Decide(catchup, Observation{Strategy: online, DrainDeadlineExceeded: true})
	if decision.Phase == failedPhase {
		t.Fatal("a drain timeout ended a migration that was not draining")
	}
	decision = Decide(quiescing, Observation{Strategy: online, DrainDeadlineExceeded: true})
	if decision.Phase != failedPhase || decision.Reason != ReasonDrainTimedOut {
		t.Fatalf("Quiescing past its drain timeout produced %s/%s", decision.Phase, decision.Reason)
	}
}

func TestTheCutoverBudgetAppliesToEveryQuiescedPhase(t *testing.T) {
	for _, strategy := range []Strategy{online, offline} {
		for _, phase := range PhaseOrder(strategy) {
			if !Quiesced(phase, strategy) {
				continue
			}
			decision := Decide(phase, Observation{Strategy: strategy, CutoverDeadlineExceeded: true})
			if decision.Phase != failedPhase {
				t.Fatalf("%s in %s ignored the cutover budget", strategy, phase)
			}
		}
	}
}

func TestTheCutoverBudgetDoesNotApplyBeforeTheQuiesce(t *testing.T) {
	for _, phase := range []Phase{preflight, provisioning, preWarm, copying, catchup} {
		decision := Decide(phase, Observation{Strategy: online, CutoverDeadlineExceeded: true})
		if decision.Phase == failedPhase {
			t.Fatalf("%s failed on a cutover budget that had not started", phase)
		}
	}
}

func TestTheOnlyDecisionServingTheTargetIsASuccessfulCutover(t *testing.T) {
	decision := Decide(cutover, readyIn(cutover, online))
	if decision.Phase != completed || decision.Serving != ServingTarget {
		t.Fatalf("a finished cutover produced %s serving %s", decision.Phase, decision.Serving)
	}
	if !decision.ReleaseQuiesce {
		t.Fatal("a finished cutover left the queued clients held")
	}
	if !decision.Cleanup {
		t.Fatal("a finished cutover left the slot holding the source primary's WAL")
	}
}

func TestACompletedMigrationRollsBackInsideItsWindow(t *testing.T) {
	decision := Decide(completed, Observation{Strategy: online, RollbackRequested: true})
	if decision.Phase != rolledBack {
		t.Fatalf("a rollback request inside the window produced %s", decision.Phase)
	}
	if decision.Serving != ServingSource {
		t.Fatalf("a rollback served from %s", decision.Serving)
	}
}

func TestACompletedMigrationRefusesToRollBackOnceTheWindowClosed(t *testing.T) {
	decision := Decide(completed, Observation{
		Strategy: online, RollbackRequested: true, RollbackWindowClosed: true})
	if decision.Phase != completed {
		t.Fatalf("a rollback after the window produced %s", decision.Phase)
	}
	if !decision.DropSource {
		t.Fatal("the closed window did not drop the source database")
	}
	if decision.Serving != ServingTarget {
		t.Fatalf("a migration past its rollback window served from %s", decision.Serving)
	}
}

// TestAFinishedMigrationNeverDropsTheSourceTwice guards a database, not a counter. Once the
// window has closed and the source is gone, the name it used to hold is free for another
// migration to recreate; a finished migration that went on dropping it every reconcile would
// be deleting a live tenant on a schedule.
func TestAFinishedMigrationNeverDropsTheSourceTwice(t *testing.T) {
	decision := Decide(completed, Observation{
		Strategy: online, RollbackWindowClosed: true, SourceDropped: true})
	if decision.DropSource {
		t.Fatal("a migration that had already dropped its source dropped it again")
	}
	if !decision.Settled {
		t.Fatal("a final migration did not settle, so it goes on acting on both instances")
	}
	if decision.Phase != completed || decision.Serving != ServingTarget {
		t.Fatalf("a final migration reported %s serving %s", decision.Phase, decision.Serving)
	}
}

func TestACompletedMigrationKeepsTheSourceUntilTheWindowCloses(t *testing.T) {
	decision := Decide(completed, Observation{Strategy: online})
	if decision.DropSource {
		t.Fatal("the source was dropped while the rollback window was still open")
	}
	if decision.Serving != ServingTarget {
		t.Fatalf("a completed migration served from %s", decision.Serving)
	}
}

func TestTerminalPhasesStayTerminal(t *testing.T) {
	for _, phase := range []Phase{failedPhase, aborted, rolledBack} {
		decision := Decide(phase, Observation{
			Strategy: online, PreflightPassed: true, Provisioned: true, CutoverComplete: true})
		if decision.Phase != phase {
			t.Fatalf("terminal %s moved to %s", phase, decision.Phase)
		}
		if decision.Serving != ServingSource {
			t.Fatalf("terminal %s served from %s", phase, decision.Serving)
		}
	}
}

func TestQuiescedSpansTheOfflineCopy(t *testing.T) {
	if !Quiesced(copying, offline) {
		t.Fatal("the offline copy runs inside the pause and must be counted in it")
	}
	if Quiesced(copying, online) {
		t.Fatal("the online copy runs while the tenant is still serving")
	}
}
