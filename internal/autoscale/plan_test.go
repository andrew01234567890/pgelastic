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

package autoscale

import (
	"fmt"
	"strings"
	"testing"
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/placement"
)

var now = time.Date(2026, 7, 27, 22, 0, 0, 0, time.UTC)

// The three member instances and the one tenant every fixture refers to.
const (
	instanceA  = "pg-a"
	instanceB  = "pg-b"
	instanceC  = "pg-c"
	someTenant = "acme"
)

func consolidatableFor(offset time.Duration, instances ...string) map[string]time.Time {
	stamped := make(map[string]time.Time, len(instances))
	for _, instance := range instances {
		stamped[instance] = now.Add(offset)
	}
	return stamped
}

func at(offset time.Duration) *time.Time {
	moment := now.Add(offset)
	return &moment
}

// basePolicy is a pool that has opted into everything, so that a test asserting on one
// guardrail is not accidentally passing because of the mode gate.
func basePolicy() Policy {
	policy := PolicyFor(nil)
	policy.Mode = pgelasticv1alpha1.AutoscalingAuto
	policy.AutoActions = ActionOrder[:]
	policy.MinInstances = 1
	policy.MaxInstances = 8
	policy.RebalanceEnabled = true
	policy.Placement = placement.Policy{PackOn: pgelasticv1alpha1.PercentileP95}
	return policy
}

// baseSignals is a healthy, freshly metered, three-instance pool with nothing in flight.
func baseSignals() Signals {
	return Signals{
		Now:          now,
		Namespace:    "saas-prod",
		Pool:         "saas-pool",
		MetricsSeen:  true,
		MetricsAge:   30 * time.Second,
		EvidenceSpan: 200 * time.Hour,
		Instances: []InstanceSignal{
			readyInstance(instanceA, 225, 100),
			readyInstance(instanceB, 225, 100),
			readyInstance(instanceC, 225, 100),
		},
		MigrationsFromSource: map[string]int32{},
	}
}

func readyInstance(name string, allocatable, inUse int32) InstanceSignal {
	return InstanceSignal{
		Name:                   name,
		Ready:                  true,
		Schedulable:            true,
		AllocatableConnections: allocatable,
		InUseConnections:       inUse,
	}
}

func tenantsAcross(instances []string, perInstance int, packed int32) []TenantSignal {
	tenants := make([]TenantSignal, 0, len(instances)*perInstance)
	for _, instance := range instances {
		for i := range perInstance {
			tenants = append(tenants, TenantSignal{
				Name:              fmt.Sprintf("%s-t%02d", instance, i),
				Instance:          instance,
				PackedConnections: packed,
				Cold:              true,
				MigrationAllowed:  true,
				EvidenceSpan:      200 * time.Hour,
			})
		}
	}
	return tenants
}

func actionOf(t *testing.T, plan Plan, class pgelasticv1alpha1.AutoAction) Action {
	t.Helper()
	action, ok := plan.ActionFor(class)
	if !ok {
		t.Fatalf("the plan proposes no %s; it proposes %v", class, classesOf(plan))
	}
	return action
}

func classesOf(plan Plan) []pgelasticv1alpha1.AutoAction {
	classes := make([]pgelasticv1alpha1.AutoAction, 0, len(plan.Actions))
	for _, action := range plan.Actions {
		classes = append(classes, action.Class)
	}
	return classes
}

// withFullVolume makes storage expansion the obvious proposal, so that a test about a
// guardrail can assert the guardrail refuses something rather than that nothing was proposed.
func withFullVolume(signals Signals) Signals {
	signals.Instances[0].StorageAllocatedBytes = 500 << 30
	signals.Instances[0].StorageUsedBytes = 450 << 30
	return signals
}

// ---------------------------------------------------------------- stale metrics

func TestStaleMetricsProduceDoNothing(t *testing.T) {
	signals := withFullVolume(baseSignals())
	signals.MetricsAge = time.Hour

	plan := Recommend(signals, basePolicy())

	if !plan.MetricsStale {
		t.Fatal("a one-hour-old sample against a five-minute threshold does not read as stale")
	}
	if _, ok := plan.Selected(); ok {
		t.Error("an action was selected for execution against stale metrics")
	}
	for _, action := range plan.Actions {
		if action.Permitted {
			t.Errorf("%s is permitted against stale metrics", action.Class)
		}
		if action.Reason != ReasonStaleMetrics {
			t.Errorf("%s refused with %q, want %q", action.Class, action.Reason, ReasonStaleMetrics)
		}
	}
}

func TestAPoolThatHasNeverBeenMeteredIsStale(t *testing.T) {
	signals := withFullVolume(baseSignals())
	signals.MetricsSeen = false

	plan := Recommend(signals, basePolicy())
	if !plan.MetricsStale {
		t.Fatal("a pool with no samples at all does not read as stale")
	}
	if _, ok := plan.Selected(); ok {
		t.Error("an action was selected for a pool that has never been metered")
	}
}

// Even storage expansion, the one action Recommend mode executes, is refused on stale
// metrics: the used-bytes figure is exactly the reading that would be stale.
func TestStaleMetricsAlsoStopStorageExpansion(t *testing.T) {
	signals := withFullVolume(baseSignals())
	signals.MetricsAge = time.Hour
	policy := basePolicy()
	policy.Mode = pgelasticv1alpha1.AutoscalingRecommend

	plan := Recommend(signals, policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionStorageExpand)
	if action.Permitted {
		t.Error("storage expansion is permitted against stale metrics")
	}
	if action.Reason != ReasonStaleMetrics {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonStaleMetrics)
	}
}

// ---------------------------------------------------------------- rollout

func TestNoActionIsTakenDuringARollout(t *testing.T) {
	signals := withFullVolume(baseSignals())
	signals.RolloutInProgress = true

	plan := Recommend(signals, basePolicy())
	if len(plan.Actions) == 0 {
		t.Fatal("the fixture proposes nothing, so it cannot show a rollout refusing anything")
	}
	if _, ok := plan.Selected(); ok {
		t.Error("an action was selected while an instance was rolling out")
	}
	for _, action := range plan.Actions {
		if action.Reason != ReasonRolloutInProgress {
			t.Errorf("%s refused with %q, want %q", action.Class, action.Reason, ReasonRolloutInProgress)
		}
	}
}

func TestNoActionIsTakenWhileThePoolIsPaused(t *testing.T) {
	signals := withFullVolume(baseSignals())
	signals.Paused = true

	plan := Recommend(signals, basePolicy())
	if _, ok := plan.Selected(); ok {
		t.Error("an action was selected on a paused pool")
	}
	if action := actionOf(t, plan, pgelasticv1alpha1.AutoActionStorageExpand); action.Reason != ReasonPoolPaused {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonPoolPaused)
	}
}

// ---------------------------------------------------------------- blackout

func TestABlackoutWindowRefusesEveryClass(t *testing.T) {
	policy := basePolicy()
	policy.BlackoutWindows = []pgelasticv1alpha1.TimeWindow{windowAt("0 20 * * *", 6*time.Hour, "UTC")}

	plan := Recommend(withFullVolume(baseSignals()), policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionStorageExpand)
	if action.Permitted {
		t.Error("storage expansion executed inside a blackout window")
	}
	if action.Reason != ReasonBlackoutWindow {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonBlackoutWindow)
	}
}

// ---------------------------------------------------------------- mode

func TestRecommendModeExecutesNothingButStorageExpansion(t *testing.T) {
	policy := basePolicy()
	policy.Mode = pgelasticv1alpha1.AutoscalingRecommend

	signals := withFullVolume(baseSignals())
	// Push utilization well past the target so a scale-out is also proposed.
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 220
	}

	plan := Recommend(signals, policy)

	storage := actionOf(t, plan, pgelasticv1alpha1.AutoActionStorageExpand)
	if !storage.Permitted {
		t.Errorf("storage expansion refused in Recommend mode: %s %s", storage.Reason, storage.Message)
	}
	scaleOut := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleOut)
	if scaleOut.Permitted {
		t.Error("scale-out executed in Recommend mode")
	}
	if scaleOut.Reason != ReasonRecommendMode {
		t.Errorf("scale-out refused with %q, want %q", scaleOut.Reason, ReasonRecommendMode)
	}

	selected, ok := plan.Selected()
	if !ok || selected.Class != pgelasticv1alpha1.AutoActionStorageExpand {
		t.Errorf("selected %v (present %v), want StorageExpand", selected.Class, ok)
	}
}

func TestRecommendModePersistsAPlanEvenWithNothingToExecute(t *testing.T) {
	policy := basePolicy()
	policy.Mode = pgelasticv1alpha1.AutoscalingRecommend

	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 220
	}
	signals.Tenants = tenantsAcross([]string{instanceA, instanceB, instanceC}, 5, 40)

	plan := Recommend(signals, policy)
	if plan.RecommendedInstances <= plan.ObservedInstances {
		t.Errorf("recommended %d instances against an observed %d at %d%% utilization",
			plan.RecommendedInstances, plan.ObservedInstances, plan.ObservedUtilizationPercent)
	}
	if len(plan.InstanceTargets) != 3 {
		t.Errorf("%d instance targets, want 3", len(plan.InstanceTargets))
	}
	if _, ok := plan.Selected(); ok {
		t.Error("Recommend mode selected an action with no storage expansion to do")
	}
	if plan.Summary == "" {
		t.Error("the plan has no summary")
	}
}

func TestAutoModeOptsInOneClassAtATime(t *testing.T) {
	policy := basePolicy()
	policy.AutoActions = []pgelasticv1alpha1.AutoAction{pgelasticv1alpha1.AutoActionScaleOut}

	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 220
	}
	signals.Tenants = tenantsAcross([]string{instanceA}, 6, 40)

	plan := Recommend(signals, policy)
	scaleOut := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleOut)
	if !scaleOut.Permitted {
		t.Errorf("scale-out refused although it is the opted-in class: %s %s", scaleOut.Reason, scaleOut.Message)
	}
	if rebalance, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionRebalance); ok {
		if rebalance.Permitted {
			t.Error("rebalance executed although it is not in autoActions")
		}
		if rebalance.Reason != ReasonNotOptedIn {
			t.Errorf("rebalance refused with %q, want %q", rebalance.Reason, ReasonNotOptedIn)
		}
	}
}

// ---------------------------------------------------------------- ordering

func TestAtMostOneClassExecutesAndItIsTheEarliestInTheOrder(t *testing.T) {
	policy := basePolicy()

	signals := withFullVolume(baseSignals())
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 220
	}
	signals.Tenants = tenantsAcross([]string{instanceA}, 6, 40)

	plan := Recommend(signals, policy)
	if len(plan.Actions) < 2 {
		t.Fatalf("the fixture proposes %v; the point of this test needs at least two classes", classesOf(plan))
	}

	selected, ok := plan.Selected()
	if !ok {
		t.Fatal("nothing was selected although several classes are permitted")
	}
	for _, action := range plan.Actions {
		if !action.Permitted || action.Class == selected.Class {
			continue
		}
		if indexOf(action.Class) < indexOf(selected.Class) {
			t.Errorf("%s comes before the selected %s in ActionOrder", action.Class, selected.Class)
		}
	}
	if selected.Class != pgelasticv1alpha1.AutoActionStorageExpand {
		t.Errorf("selected %s, want StorageExpand: it is the earliest permitted class here", selected.Class)
	}
}

func indexOf(class pgelasticv1alpha1.AutoAction) int {
	for i, candidate := range ActionOrder {
		if candidate == class {
			return i
		}
	}
	return -1
}

func TestScaleInIsLastInTheOrder(t *testing.T) {
	if ActionOrder[len(ActionOrder)-1] != pgelasticv1alpha1.AutoActionScaleIn {
		t.Errorf("the last action class is %s, want ScaleIn", ActionOrder[len(ActionOrder)-1])
	}
	if ActionOrder[0] != pgelasticv1alpha1.AutoActionTenantGucTune {
		t.Errorf("the first action class is %s, want TenantGucTune", ActionOrder[0])
	}
}

// ---------------------------------------------------------------- stabilization

func TestScaleUpIsHeldInsideTheStabilizationWindow(t *testing.T) {
	policy := basePolicy()
	policy.ScaleUpStabilization = 3 * time.Minute

	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 220
	}
	signals.LastScaleUpAt = at(-time.Minute)

	plan := Recommend(signals, policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleOut)
	if action.Permitted {
		t.Error("scaled up again one minute into a three-minute stabilization window")
	}
	if action.Reason != ReasonStabilizing {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonStabilizing)
	}

	signals.LastScaleUpAt = at(-10 * time.Minute)
	plan = Recommend(signals, policy)
	if action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleOut); !action.Permitted {
		t.Errorf("scale-out still refused ten minutes later: %s %s", action.Reason, action.Message)
	}
}

func TestUtilizationInsideTheToleranceProposesNothing(t *testing.T) {
	policy := basePolicy()
	policy.TargetUtilizationPercent = 70
	policy.TolerancePercent = 10

	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 170 // 75% of 225
	}

	plan := Recommend(signals, policy)
	if plan.RecommendedInstances != plan.ObservedInstances {
		t.Errorf("recommended %d instances at %d%% against a %d%% target with a %d%% tolerance",
			plan.RecommendedInstances, plan.ObservedUtilizationPercent,
			policy.TargetUtilizationPercent, policy.TolerancePercent)
	}
	if _, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleOut); ok {
		t.Error("a scale-out was proposed inside the tolerance band")
	}
}

// ---------------------------------------------------------------- measurable capacity

// A restored instance is the reachable case: pgrestore_controller gives it the source pool's
// poolRef and cordons it in the same breath, so a pool acquires unschedulable capacity with
// no operator action at all.
func restoredInstance(name string, allocatable int32) InstanceSignal {
	instance := readyInstance(name, allocatable, 0)
	instance.Schedulable = false
	return instance
}

func TestCapacityNothingMayBePlacedOnIsNotMeasured(t *testing.T) {
	policy := basePolicy()
	policy.TargetUtilizationPercent = 70

	signals := baseSignals()
	signals.Instances = []InstanceSignal{
		readyInstance(instanceA, 225, 100),
		readyInstance(instanceB, 225, 100),
		restoredInstance(instanceC, 225),
	}

	plan := Recommend(signals, policy)
	if want := int32(44); plan.ObservedUtilizationPercent != want {
		t.Errorf("utilization is %d%%, want %d%% — 200 in use over the 450 connections a tenant "+
			"could actually be placed on", plan.ObservedUtilizationPercent, want)
	}
	if action, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleIn); ok {
		t.Errorf("a pool at 44%% of a 70%% target proposed %s: %s", action.Class, action.Detail)
	}
}

func TestAPoolWithNoPlaceableCapacityIsUnmeasuredRatherThanEmpty(t *testing.T) {
	policy := basePolicy()
	policy.TargetUtilizationPercent = 70
	policy.MinInstances = 1

	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].Schedulable = false
	}

	plan := Recommend(signals, policy)
	// The count is not the assertion; what the pool does about it is. A pool nothing can be
	// read through proposes neither growth nor consolidation, because both would be acting on
	// a reading that was never taken.
	if action, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleIn); ok {
		t.Errorf("proposed %s against a pool nothing can be placed on: %s", action.Class, action.Detail)
	}
	if action, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleOut); ok {
		t.Errorf("proposed %s against a pool nothing can be placed on: %s", action.Class, action.Detail)
	}
	// Taking no action and saying the pool is idle are the same outcome told two ways, and
	// only one of them sends an operator to look at why the members are cordoned.
	if !strings.Contains(plan.Summary, "utilization is unknown") {
		t.Errorf("the summary reads %q, which does not distinguish an unreadable pool from an idle one",
			plan.Summary)
	}
}

// The count the ratio scales must be the count the ratio measured, or a pool that is one
// third unschedulable is sized as though the third were carrying its share.
func TestTheRecommendationIsScaledFromTheMeasuredCount(t *testing.T) {
	policy := basePolicy()
	policy.TargetUtilizationPercent = 50

	signals := baseSignals()
	signals.Instances = []InstanceSignal{
		readyInstance(instanceA, 100, 100),
		readyInstance(instanceB, 100, 100),
		restoredInstance(instanceC, 100),
	}

	plan := Recommend(signals, policy)
	// Two measured instances at 100%, halved to a 50% target, needs four that can serve. The
	// unschedulable member is not one of them and is not added on top of them either.
	if want := int32(4); plan.RecommendedInstances != want {
		t.Errorf("recommended %d instances from %d measured at %d%%, want %d",
			plan.RecommendedInstances, plan.MeasuredInstances, plan.ObservedUtilizationPercent, want)
	}
}

// The recommendation must not be a function of the count it is compared against. A member
// that exists without becoming measurable - one still coming up, one cordoned - would
// otherwise raise the recommendation by one and so ask for another, for ever.
func TestTheRecommendationDoesNotChaseTheMembersItAsksFor(t *testing.T) {
	policy := basePolicy()
	policy.TargetUtilizationPercent = 40
	policy.MaxInstances = 32

	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 135 // 60% of 225
	}
	first := Recommend(signals, policy).RecommendedInstances
	if first <= signals.InstanceCount() {
		t.Fatalf("the fixture recommends %d against %d members, so it cannot show a runaway",
			first, signals.InstanceCount())
	}

	// The members it asked for arrive and are not serving yet, which is every member for the
	// minutes between being created and finishing initdb.
	for len(signals.Instances) < int(first) {
		signals.Instances = append(signals.Instances, InstanceSignal{
			Name: fmt.Sprintf("pg-new-%d", len(signals.Instances)), Schedulable: true,
		})
	}

	if second := Recommend(signals, policy).RecommendedInstances; second != first {
		t.Errorf("recommended %d after the %d it asked for appeared unready; a recommendation "+
			"that grows with the membership walks to maxInstances", second, first)
	}
}

// The proposal and whatever applies it have now disagreed about which count "current" means
// three times, in three different pairs of numbers, and each time the symptom was identical: a
// class proposed, permitted and selected on every pass that applies nothing, and every class
// beneath it in ActionOrder never running again.
//
// So the plan states the count its recommendation is in, and this asserts that a permitted
// ScaleOut is one the executor will also find worth applying - across a fixture set that
// includes every shape that has caused the disagreement so far.
func TestAPermittedScaleOutIsAlwaysOneItsExecutorWouldApply(t *testing.T) {
	shapes := map[string]func(Signals) Signals{
		"all serving":  func(s Signals) Signals { return s },
		"one cordoned": func(s Signals) Signals { s.Instances[2].Schedulable = false; return s },
		"two cordoned": func(s Signals) Signals {
			s.Instances[1].Schedulable = false
			s.Instances[2].Schedulable = false
			return s
		},
		"one still coming up":    func(s Signals) Signals { s.Instances[2].Ready = false; return s },
		"cordoned and coming up": func(s Signals) Signals { s.Instances[1].Schedulable = false; s.Instances[2].Ready = false; return s },
	}
	loads := []int32{20, 100, 155, 214, 225}

	for name, shape := range shapes {
		for _, inUse := range loads {
			signals := baseSignals()
			for i := range signals.Instances {
				signals.Instances[i].InUseConnections = inUse
			}
			signals = shape(signals)

			plan := Recommend(signals, basePolicy())
			action, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleOut)
			if !ok || !action.Permitted {
				continue
			}
			// What the executor asks before it writes anything.
			if plan.RecommendedInstances <= plan.ServingInstances {
				t.Errorf("%s at %d in use: ScaleOut is permitted with recommended=%d against "+
					"serving=%d, so it is selected every pass and applies nothing",
					name, inUse, plan.RecommendedInstances, plan.ServingInstances)
			}
		}
	}
}

// A pool whose members are all out of service reads as needing more of them - the floor puts
// the recommendation above a serving count of zero - so growth has to be refused on the
// measurement rather than on the arithmetic.
func TestAPoolWithNothingMeasurableDoesNotProposeGrowth(t *testing.T) {
	policy := basePolicy()
	policy.TargetUtilizationPercent = 70
	policy.MinInstances = 4

	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].Schedulable = false
	}

	plan := Recommend(signals, policy)
	if plan.RecommendedInstances <= signals.ServingInstances() {
		t.Fatalf("the fixture recommends %d against %d serving, so the guard under test is not "+
			"what stops the proposal", plan.RecommendedInstances, signals.ServingInstances())
	}
	if action, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleOut); ok {
		t.Errorf("proposed %s from a pool nothing could be measured through: %s",
			action.Class, action.Detail)
	}
}

// A pool sitting on its target proposes nothing, and holding a member that is out of service
// does not change that. The recommendation and the count it is compared against have to be
// the same unit: expressed in members and compared with servable ones, an on-target pool with
// one cordoned member proposed raising its own count from three to three - inert, permitted,
// selected, and starving every class below it, for ever.
func TestAnOnTargetPoolWithACordonedMemberProposesNothing(t *testing.T) {
	policy := basePolicy()
	policy.TargetUtilizationPercent = 70
	policy.TolerancePercent = 10

	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 155 // 68% of 225, inside the band
	}
	signals.Instances[2].Schedulable = false

	plan := Recommend(signals, policy)
	if action, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleOut); ok {
		t.Errorf("an on-target pool proposed %s: %s", action.Class, action.Detail)
	}
}

// The state scale-in leaves behind on the pass after it cordons its victim: the victim stops
// counting as capacity, so the utilization over what is left rises. Read against the members
// that remain in service, that is not a pool that needs to grow - and proposing growth here
// abandons the scale-in half-done, with the victim cordoned, empty and never reclaimed.
func TestCordoningTheScaleInVictimDoesNotTurnIntoAScaleOut(t *testing.T) {
	policy := basePolicy()
	policy.TargetUtilizationPercent = 70

	signals := scaleInSignals()
	signals.Instances[2].Schedulable = false
	signals.Instances[2].Tenants = 0

	plan := Recommend(signals, policy)
	if action, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleOut); ok {
		t.Errorf("proposed %s on the pass after scale-in cordoned its victim: %s",
			action.Class, action.Detail)
	}
}

// ------------------------------------------------- a scale-out that never landed

// An over-target pool whose tenants sit unevenly, so ScaleOut and Rebalance are both
// proposed and the order between them decides which one ever runs.
func crowdedAndSkewedSignals() Signals {
	signals := baseSignals()
	for i := range signals.Instances {
		signals.Instances[i].InUseConnections = 220
	}
	signals.Tenants = tenantsAcross([]string{instanceA}, 6, 40)
	signals.DeclaredInstances = int32(len(signals.Instances))
	return signals
}

func TestAScaleOutIsRefusedWhileTheLastOneIsUnrealised(t *testing.T) {
	policy := basePolicy()
	signals := crowdedAndSkewedSignals()
	// The pool has already asked for a fourth member and has three. Nothing provisions one,
	// so this is where a pool that has ever scaled out permanently sits.
	signals.DeclaredInstances = 4

	plan := Recommend(signals, policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleOut)
	if action.Permitted {
		t.Fatalf("permitted a fourth scale-out while the third is unrealised: %s", action.Detail)
	}
	if action.Reason != ReasonScaleOutUnrealised {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonScaleOutUnrealised)
	}
	if !strings.Contains(action.Message, "4") || !strings.Contains(action.Message, "3") {
		t.Errorf("the refusal reads %q and names neither count", action.Message)
	}
}

// The starvation this exists to stop: ScaleOut sits above Rebalance and ScaleIn in
// ActionOrder, and Selected takes the earliest proposed-and-permitted class. A ScaleOut that
// is permitted for ever and applies nothing is therefore not one stuck action, it is every
// action below it never running again.
func TestAnUnrealisedScaleOutDoesNotStarveTheClassesBelowIt(t *testing.T) {
	policy := basePolicy()
	signals := crowdedAndSkewedSignals()
	signals.DeclaredInstances = 4

	plan := Recommend(signals, policy)
	selected, ok := plan.Selected()
	if !ok {
		t.Fatalf("nothing was selected; the plan proposes %v", classesOf(plan))
	}
	if selected.Class == pgelasticv1alpha1.AutoActionScaleOut {
		t.Fatal("selected the ScaleOut that cannot be applied, so nothing below it will ever run")
	}
	if selected.Class != pgelasticv1alpha1.AutoActionRebalance {
		t.Errorf("selected %s, want Rebalance: it is the earliest class that can actually act", selected.Class)
	}
}

func TestAScaleOutIsPermittedOnceThePoolHasTheMembersItDeclares(t *testing.T) {
	policy := basePolicy()
	signals := crowdedAndSkewedSignals()

	plan := Recommend(signals, policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleOut)
	if !action.Permitted {
		t.Errorf("refused a scale-out on a pool holding every member it declares: %s / %s",
			action.Reason, action.Message)
	}
}

// A pool holding more members than it declares has not asked for anything it has not got, so
// the gate above it must not fire. Adoption makes this reachable.
func TestAPoolHoldingMoreMembersThanItDeclaresMayStillScaleOut(t *testing.T) {
	policy := basePolicy()
	signals := crowdedAndSkewedSignals()
	signals.DeclaredInstances = 1

	plan := Recommend(signals, policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleOut)
	if !action.Permitted {
		t.Errorf("refused a scale-out on a pool with more members than it declares: %s / %s",
			action.Reason, action.Message)
	}
}

// ---------------------------------------------------------------- scale-in

func scaleInSignals() Signals {
	signals := baseSignals()
	// The pool is running at a twentieth of its capacity, so a repack fits every tenant onto
	// fewer instances than it has and leaves at least one with nothing on it.
	signals.Instances[0].InUseConnections, signals.Instances[0].Tenants = 20, 2
	signals.Instances[1].InUseConnections, signals.Instances[1].Tenants = 20, 2
	signals.Instances[2].InUseConnections, signals.Instances[2].Tenants = 0, 0
	signals.Tenants = tenantsAcross([]string{instanceA, instanceB}, 2, 5)
	signals.ConsolidatableSince = consolidatableFor(-48*time.Hour, instanceA, instanceB, instanceC)
	signals.LastScaleDownAt = at(-72 * time.Hour)
	return signals
}

func TestScaleInNeedsAWeekOfEvidence(t *testing.T) {
	policy := basePolicy()
	signals := scaleInSignals()
	signals.EvidenceSpan = 100 * time.Hour

	plan := Recommend(signals, policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleIn)
	if action.Permitted {
		t.Error("scaled in on 100 hours of evidence against a 168-hour requirement")
	}
	if action.Reason != ReasonInsufficientEvidence {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonInsufficientEvidence)
	}
}

func TestScaleInNeedsTheConsolidationToHaveDwelled(t *testing.T) {
	policy := basePolicy()
	signals := scaleInSignals()
	signals.ConsolidatableSince = consolidatableFor(-time.Hour, instanceA, instanceB, instanceC)

	plan := Recommend(signals, policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleIn)
	if action.Permitted {
		t.Error("scaled in one hour into a 24-hour dwell time")
	}
	if action.Reason != ReasonDwellTime {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonDwellTime)
	}

	signals.ConsolidatableSince = nil
	plan = Recommend(signals, policy)
	if action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleIn); action.Reason != ReasonDwellTime {
		t.Errorf("with no observed dwell at all the refusal is %q, want %q", action.Reason, ReasonDwellTime)
	}
}

// The dwell time belongs to the instance that is actually going to be reclaimed. Reading
// the longest-waiting candidate's timestamp instead lets an instance that emptied a minute
// ago be scaled in on another instance's patience, which is the whole of the anti-flapping
// guard.
func TestTheDwellTimeIsReadOffTheInstanceBeingReclaimed(t *testing.T) {
	policy := basePolicy()
	signals := scaleInSignals()

	plan := Recommend(signals, policy)
	victim := plan.ConsolidationTarget
	if victim == "" {
		t.Fatal("no consolidation target was chosen, so there is nothing to gate")
	}

	// Every other candidate has waited out the dwell time; the victim has just become
	// consolidatable.
	signals.ConsolidatableSince = consolidatableFor(-48*time.Hour, instanceA, instanceB, instanceC)
	signals.ConsolidatableSince[victim] = now.Add(-time.Minute)

	action := actionOf(t, Recommend(signals, policy), pgelasticv1alpha1.AutoActionScaleIn)
	if action.Permitted {
		t.Errorf("%s was scaled in a minute after becoming consolidatable, on another instance's dwell time", victim)
	}
	if action.Reason != ReasonDwellTime {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonDwellTime)
	}
	if !strings.Contains(action.Message, victim) {
		t.Errorf("message = %q, want it to name %s", action.Message, victim)
	}
}

func TestScaleInSpendsTheMigrationBudget(t *testing.T) {
	policy := basePolicy()
	signals := scaleInSignals()
	signals.InFlightMigrations = 1

	plan := Recommend(signals, policy)
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleIn)
	if action.Permitted {
		t.Error("scaled in with a migration already in flight against a budget of one")
	}
	if action.Reason != ReasonMigrationBudget {
		t.Errorf("refused with %q, want %q", action.Reason, ReasonMigrationBudget)
	}
}

func TestScaleInStopsAtMinInstances(t *testing.T) {
	policy := basePolicy()
	policy.MinInstances = 3

	plan := Recommend(scaleInSignals(), policy)
	if action, ok := plan.ActionFor(pgelasticv1alpha1.AutoActionScaleIn); ok && action.Permitted {
		t.Error("scaled a three-instance pool in against a floor of three")
	}
}

func TestScaleInIsPermittedOnceEveryGateIsSatisfied(t *testing.T) {
	plan := Recommend(scaleInSignals(), basePolicy())
	action := actionOf(t, plan, pgelasticv1alpha1.AutoActionScaleIn)
	if !action.Permitted {
		t.Errorf("scale-in refused with every gate satisfied: %s %s", action.Reason, action.Message)
	}
	if plan.ConsolidationTarget == "" {
		t.Fatal("scale-in is permitted with no instance named to reclaim")
	}
	for _, target := range plan.InstanceTargets {
		if target.Name == plan.ConsolidationTarget && target.Tenants != 0 {
			t.Errorf("the consolidation target %q still holds %d tenants in the plan",
				target.Name, target.Tenants)
		}
	}
}

// ---------------------------------------------------------------- vertical resize

func TestVerticalResizeStaysHumanGatedEvenWhenOptedIn(t *testing.T) {
	guard := Guard{Policy: basePolicy(), Signals: baseSignals()}
	verdict := guard.Evaluate(pgelasticv1alpha1.AutoActionVerticalResize)
	if verdict.permitted {
		t.Error("VerticalResize was permitted; it restarts a postmaster and no switchover proof exists")
	}
	if verdict.reason != ReasonHumanGated {
		t.Errorf("refused with %q, want %q", verdict.reason, ReasonHumanGated)
	}
}

// ---------------------------------------------------------------- migration budget

func TestMigrationWindowsScopeWhenMovesMayStart(t *testing.T) {
	policy := basePolicy()
	policy.MigrationBudget.Windows = []pgelasticv1alpha1.TimeWindow{windowAt("0 2 * * *", time.Hour, "UTC")}

	guard := Guard{Policy: policy, Signals: baseSignals()}
	verdict := guard.Evaluate(pgelasticv1alpha1.AutoActionRebalance)
	if verdict.permitted {
		t.Error("a move was permitted at 22:00 against a 02:00 window")
	}
	if verdict.reason != ReasonMigrationWindow {
		t.Errorf("refused with %q, want %q", verdict.reason, ReasonMigrationWindow)
	}

	inWindow := baseSignals()
	inWindow.Now = time.Date(2026, 7, 28, 2, 30, 0, 0, time.UTC)
	guard = Guard{Policy: policy, Signals: inWindow}
	if verdict := guard.Evaluate(pgelasticv1alpha1.AutoActionRebalance); !verdict.permitted {
		t.Errorf("a move inside the window was refused: %s %s", verdict.reason, verdict.message)
	}
}

func TestMigrationRateCapIsSpentOverTheWindow(t *testing.T) {
	policy := basePolicy()
	policy.MigrationBudget.MaxPerWindow = 4

	signals := baseSignals()
	signals.MigrationsStartedInWindow = 4
	guard := Guard{Policy: policy, Signals: signals}

	verdict := guard.Evaluate(pgelasticv1alpha1.AutoActionRebalance)
	if verdict.permitted {
		t.Error("a fifth move was permitted against a per-window cap of four")
	}
	if verdict.reason != ReasonMigrationBudget {
		t.Errorf("refused with %q, want %q", verdict.reason, ReasonMigrationBudget)
	}
}

func TestRebalanceIsRefusedWhenRebalancingIsOff(t *testing.T) {
	policy := basePolicy()
	policy.RebalanceEnabled = false

	guard := Guard{Policy: policy, Signals: baseSignals()}
	verdict := guard.Evaluate(pgelasticv1alpha1.AutoActionRebalance)
	if verdict.reason != ReasonRebalancingDisabled {
		t.Errorf("refused with %q, want %q", verdict.reason, ReasonRebalancingDisabled)
	}
}

// ---------------------------------------------------------------- move eligibility

func TestAHotTenantIsNotMoved(t *testing.T) {
	guard := Guard{Policy: basePolicy(), Signals: baseSignals()}
	tenant := TenantSignal{Name: someTenant, Cold: false, MigrationAllowed: true}

	eligible, _, why := guard.MoveEligible(tenant, readyInstance(instanceA, 225, 10))
	if eligible {
		t.Error("a hot tenant was made eligible to move")
	}
	if why == "" {
		t.Error("the refusal carries no explanation")
	}
}

func TestATenantIsNotMovedOffABusySource(t *testing.T) {
	policy := basePolicy()
	policy.ForbidMoveAboveSourcePercent = 65

	guard := Guard{Policy: policy, Signals: baseSignals()}
	tenant := TenantSignal{Name: someTenant, Cold: true, MigrationAllowed: true}

	if eligible, _, _ := guard.MoveEligible(tenant, readyInstance(instanceA, 100, 90)); eligible {
		t.Error("a move was allowed off a source at 90% utilization: logical decoding would consume " +
			"exactly the capacity the move is meant to relieve")
	}
}

func TestASecondMoveOffTheSameSourceWaits(t *testing.T) {
	signals := baseSignals()
	signals.MigrationsFromSource = map[string]int32{instanceA: 1}

	guard := Guard{Policy: basePolicy(), Signals: signals}
	tenant := TenantSignal{Name: someTenant, Cold: true, MigrationAllowed: true}

	if eligible, _, _ := guard.MoveEligible(tenant, readyInstance(instanceA, 225, 10)); eligible {
		t.Error("a second move off pg-a was allowed while the first was still decoding")
	}
	if eligible, _, _ := guard.MoveEligible(tenant, readyInstance(instanceB, 225, 10)); !eligible {
		t.Error("a move off pg-b was blocked by an in-flight move off pg-a")
	}
}

func TestATenantWhoseClassForbidsAutomaticMigrationIsNotMoved(t *testing.T) {
	guard := Guard{Policy: basePolicy(), Signals: baseSignals()}
	tenant := TenantSignal{Name: "premium", Cold: true, MigrationAllowed: false}

	if eligible, _, _ := guard.MoveEligible(tenant, readyInstance(instanceA, 225, 10)); eligible {
		t.Error("a tenant whose workload class requires approval was moved automatically")
	}
}

// ---------------------------------------------------------------- atomic plan

func TestEvictionAndDestinationAreOneDecision(t *testing.T) {
	signals := baseSignals()
	signals.Instances = []InstanceSignal{
		readyInstance(instanceA, 100, 95),
		readyInstance(instanceB, 100, 5),
	}
	signals.Tenants = []TenantSignal{
		{Name: "big", Instance: instanceA, PackedConnections: 80, Cold: true, MigrationAllowed: true},
		{Name: "small", Instance: instanceA, PackedConnections: 40, Cold: true, MigrationAllowed: true},
	}

	plan := Recommend(signals, basePolicy())
	if len(plan.Moves) == 0 {
		t.Fatal("no move was planned although pg-a cannot hold both its tenants")
	}
	for _, move := range plan.Moves {
		if move.From == "" || move.To == "" {
			t.Errorf("move of %q is %q -> %q: a plan that evicts without naming a destination is half a plan",
				move.Tenant, move.From, move.To)
		}
		if move.ExpectedImprovementPercent <= 0 {
			t.Errorf("move of %q claims no improvement; a move that cannot state one should not be made",
				move.Tenant)
		}
	}
}

func TestEligibleMovesAreOrderedByImprovement(t *testing.T) {
	plan := Plan{Moves: []Move{
		{Tenant: "a", Eligible: true, ExpectedImprovementPercent: 5},
		{Tenant: "b", Eligible: true, ExpectedImprovementPercent: 30},
		{Tenant: "c", Eligible: false, ExpectedImprovementPercent: 90},
	}}
	moves := plan.EligibleMoves()
	if len(moves) != 2 {
		t.Fatalf("%d eligible moves, want 2", len(moves))
	}
	if moves[0].Tenant != "b" {
		t.Errorf("first move is %q, want the 30%% one", moves[0].Tenant)
	}
}

// ---------------------------------------------------------------- storage

func TestStorageExpansionOnlyEverGrowsAndOnlyWhenFull(t *testing.T) {
	policy := basePolicy()
	policy.StorageExpandAtPercent = 80
	policy.StorageExpandToPercent = 60

	quiet := InstanceSignal{Name: instanceA, StorageAllocatedBytes: 100 << 30, StorageUsedBytes: 50 << 30}
	if _, ok := storageTarget(quiet, policy); ok {
		t.Error("a volume at 50% was proposed for expansion")
	}

	full := InstanceSignal{Name: instanceA, StorageAllocatedBytes: 100 << 30, StorageUsedBytes: 90 << 30}
	size, ok := storageTarget(full, policy)
	if !ok {
		t.Fatal("a volume at 90% was not proposed for expansion")
	}
	if size <= full.StorageAllocatedBytes {
		t.Errorf("expansion target %d is not larger than the current %d", size, full.StorageAllocatedBytes)
	}
	if size != 150<<30 {
		t.Errorf("expansion target %d bytes, want 150GiB: 90GiB used restored to 60%%", size)
	}
}

func TestStorageExpansionIsCappedByMaxSize(t *testing.T) {
	policy := basePolicy()
	policy.StorageMaxBytes = 120 << 30

	full := InstanceSignal{Name: instanceA, StorageAllocatedBytes: 100 << 30, StorageUsedBytes: 90 << 30}
	size, ok := storageTarget(full, policy)
	if !ok {
		t.Fatal("no expansion proposed")
	}
	if size != 120<<30 {
		t.Errorf("expansion target %d bytes, want the 120GiB cap", size)
	}
}

func TestAnInstanceWithNoPublishedStorageIsNotExpanded(t *testing.T) {
	if _, ok := storageTarget(InstanceSignal{Name: instanceA}, basePolicy()); ok {
		t.Error("an instance that has published no storage figures was proposed for expansion")
	}
}

// ---------------------------------------------------------------- policy resolution

func TestPolicyDefaultsToRecommendAndDoNothing(t *testing.T) {
	policy := PolicyFor(&pgelasticv1alpha1.PgElasticPool{})
	if policy.Mode != pgelasticv1alpha1.AutoscalingRecommend {
		t.Errorf("mode = %q, want Recommend", policy.Mode)
	}
	if policy.StaleFallback != pgelasticv1alpha1.StaleMetricDoNothing {
		t.Errorf("stale fallback = %q, want DoNothing", policy.StaleFallback)
	}
	if len(policy.AutoActions) != 0 {
		t.Errorf("autoActions = %v, want none", policy.AutoActions)
	}
	if policy.ScaleInEvidenceWindow != 168*time.Hour {
		t.Errorf("scale-in evidence window = %s, want 168h", policy.ScaleInEvidenceWindow)
	}
}

func TestARebalancingBlackoutIsAlsoAnAutoscalingBlackout(t *testing.T) {
	pool := &pgelasticv1alpha1.PgElasticPool{}
	pool.Spec.Rebalancing = &pgelasticv1alpha1.PoolRebalancing{
		BlackoutWindows: []pgelasticv1alpha1.TimeWindow{windowAt("0 8 * * 1-5", 10*time.Hour, "UTC")},
	}
	policy := PolicyFor(pool)
	if len(policy.BlackoutWindows) != 1 {
		t.Errorf("%d blackout windows, want the rebalancing one carried over", len(policy.BlackoutWindows))
	}
}
