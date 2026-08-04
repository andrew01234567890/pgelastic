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
	"slices"
	"strings"
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/placement"
)

// InstanceSignal is one member instance as the planner sees it.
type InstanceSignal struct {
	Name                   string
	Ready                  bool
	Schedulable            bool
	Progressing            bool
	AllocatableConnections int32
	InUseConnections       int32
	ReservedConnections    int32
	Tenants                int32
	StorageAllocatedBytes  int64
	StorageUsedBytes       int64
}

// UtilizationPercent is connections in use over allocatable. An instance with no published
// allocatable capacity reports zero rather than dividing by it.
func (i InstanceSignal) UtilizationPercent() int32 {
	if i.AllocatableConnections <= 0 {
		return 0
	}
	return int32(int64(i.InUseConnections) * 100 / int64(i.AllocatableConnections))
}

// StorageUsedPercent is used over allocated on the data volume.
func (i InstanceSignal) StorageUsedPercent() int32 {
	if i.StorageAllocatedBytes <= 0 {
		return 0
	}
	return int32(i.StorageUsedBytes * 100 / i.StorageAllocatedBytes)
}

// TenantSignal is one tenant as the planner sees it.
type TenantSignal struct {
	Name     string
	Instance string

	GuaranteedConnections int32
	BurstableConnections  int32
	// PackedConnections is the trailing-window percentile the pool packs on, already
	// resolved from the metering store by the caller.
	PackedConnections int32
	PeakConnections   int32
	StorageBytes      int64
	Relations         int32

	Cold             bool
	MigrationAllowed bool
	AntiAffinity     map[string]string
	PinnedInstance   string
	// EvidenceSpan is how much history exists for this tenant.
	EvidenceSpan time.Duration
}

// Signals is everything one planning pass reads.
type Signals struct {
	Now       time.Time
	Namespace string
	Pool      string
	Paused    bool

	Instances []InstanceSignal
	Tenants   []TenantSignal

	// MetricsSeen and MetricsAge drive the stale-metric fallback.
	MetricsSeen bool
	MetricsAge  time.Duration
	// EvidenceSpan is the shortest per-tenant history in the pool, which is what scale-in's
	// 168-hour gate is measured against: one tenant nobody has watched for a week is enough
	// to make consolidating around it a guess.
	EvidenceSpan time.Duration

	// RolloutInProgress is true while any member instance is not converged. No action is
	// taken while it holds.
	RolloutInProgress bool

	InFlightMigrations        int32
	MigrationsStartedInWindow int32
	// MigrationsFromSource counts in-flight moves per source instance, so a second move off
	// the same instance is refused while the first is decoding.
	MigrationsFromSource map[string]int32

	LastScaleUpAt   *time.Time
	LastScaleDownAt *time.Time
	// ConsolidatableSince is when each instance was first found consolidatable, keyed by
	// instance name. The dwell time is measured against the instance scale-in has actually
	// chosen: collapsing these to one timestamp lets an instance that emptied a minute ago
	// inherit the patience of one that emptied a day ago, which is the whole of the
	// anti-flapping guard.
	ConsolidatableSince map[string]time.Time
}

// InstanceCount is how many instances the pool has.
func (s Signals) InstanceCount() int32 { return int32(len(s.Instances)) }

// Action is one proposed change class.
type Action struct {
	Class     pgelasticv1alpha1.AutoAction
	Target    string
	Detail    string
	Permitted bool
	Reason    string
	Message   string
}

// Move is one tenant relocation the plan implies.
type Move struct {
	Tenant                     string
	From                       string
	To                         string
	ExpectedImprovementPercent int32
	Reason                     string
	Eligible                   bool
	// Blocker is why the move is ineligible, in a form a caller can branch on. BlockedBy is
	// the same fact in a sentence a person reads; both are kept because a status field that
	// only a human can interpret cannot be acted on, and a code without a sentence tells an
	// operator nothing.
	Blocker   Blocker
	BlockedBy string
}

// Blocker names what is holding a move back.
//
// The distinction that matters is between a refusal about *safety* and a refusal about
// *worth*. Decoding a move off an overloaded source, or starting a second move from a source
// that is already streaming one, are physical limits and hold whatever the reason for the
// move. Whether a tenant is hot enough to be worth rebalancing is a question about the
// rebalancer's own objective, and an instance being drained has already answered it: the
// operator said empty this, and heat is not a reason to leave a tenant behind.
type Blocker string

const (
	// BlockedByWorkloadClass means the tenant's class forbids automatic migration outright.
	BlockedByWorkloadClass Blocker = "WorkloadClass"
	// BlockedByHeat means the tenant is not cold and the pool only rebalances cold tenants.
	BlockedByHeat Blocker = "Heat"
	// BlockedBySourceLoad means the source is too busy to decode a move off it.
	BlockedBySourceLoad Blocker = "SourceLoad"
	// BlockedByInFlight means a migration from that source is already running.
	BlockedByInFlight Blocker = "InFlight"
)

// InstanceTarget is what one instance looks like once the plan is applied.
type InstanceTarget struct {
	Name                   string
	UtilizationPercent     int32
	PackedConnections      int32
	AllocatableConnections int32
	Tenants                int32
	StorageUsedPercent     int32
	RecommendedStorageByes int64
	Consolidatable         bool
}

// Plan is the whole recommendation.
type Plan struct {
	ComputedAt                 time.Time
	Mode                       pgelasticv1alpha1.AutoscalingMode
	MetricsStale               bool
	ObservedInstances          int32
	RecommendedInstances       int32
	ObservedUtilizationPercent int32
	TargetUtilizationPercent   int32
	// MigrationsPermitted is the disruption budget alone - concurrency cap, rate cap and
	// migration windows - with no opinion about whether any particular move is worth making.
	// It is what an evacuation asks, because a drain has already been decided by an operator
	// and only needs to know whether the pool can carry a move right now.
	MigrationsPermitted      bool
	MigrationsRefusedBecause string
	Summary                  string

	InstanceTargets []InstanceTarget
	Moves           []Move
	Actions         []Action
	// ConsolidationTarget is the instance scale-in would reclaim, empty when none is
	// consolidatable. It is part of the plan rather than recomputed at execution time,
	// because the eviction and the destination are one decision.
	ConsolidationTarget string
}

// ActionFor returns the proposed action of a class.
func (p Plan) ActionFor(class pgelasticv1alpha1.AutoAction) (Action, bool) {
	for _, action := range p.Actions {
		if action.Class == class {
			return action, true
		}
	}
	return Action{}, false
}

// Selected is the single action this pass may execute: the earliest in ActionOrder that is
// both proposed and permitted. Nothing else runs, whatever else the plan proposes.
func (p Plan) Selected() (Action, bool) {
	for _, class := range ActionOrder {
		if action, ok := p.ActionFor(class); ok && action.Permitted {
			return action, true
		}
	}
	return Action{}, false
}

// EligibleMoves is the movement the plan may actually spend, in the order it should be
// spent: largest improvement first.
func (p Plan) EligibleMoves() []Move {
	moves := make([]Move, 0, len(p.Moves))
	for _, move := range p.Moves {
		if move.Eligible {
			moves = append(moves, move)
		}
	}
	slices.SortStableFunc(moves, func(a, b Move) int {
		switch {
		case a.ExpectedImprovementPercent > b.ExpectedImprovementPercent:
			return -1
		case b.ExpectedImprovementPercent > a.ExpectedImprovementPercent:
			return 1
		default:
			return strings.Compare(a.Tenant, b.Tenant)
		}
	})
	return moves
}

// Recommend computes the whole plan. It never executes anything; the caller applies at most
// Plan.Selected().
func Recommend(signals Signals, policy Policy) Plan {
	plan := Plan{
		ComputedAt:               signals.Now,
		Mode:                     policy.Mode,
		ObservedInstances:        signals.InstanceCount(),
		TargetUtilizationPercent: policy.TargetUtilizationPercent,
	}
	inUse, allocatable := int64(0), int64(0)
	for _, instance := range signals.Instances {
		if !instance.Ready {
			continue
		}
		inUse += int64(instance.InUseConnections)
		allocatable += int64(instance.AllocatableConnections)
	}
	if allocatable > 0 {
		plan.ObservedUtilizationPercent = int32(inUse * 100 / allocatable)
	}

	plan.RecommendedInstances = recommendedInstances(plan.ObservedUtilizationPercent, signals, policy)
	packing := repack(signals, policy)
	plan.InstanceTargets = instanceTargets(signals, policy, packing)
	plan.ConsolidationTarget = consolidationTarget(plan.InstanceTargets, policy)

	// The guard is built once the victim is known, because the dwell time it enforces
	// belongs to that instance and to no other.
	guard := Guard{Policy: policy, Signals: signals, ConsolidationTarget: plan.ConsolidationTarget}
	plan.Moves = moves(signals, guard, packing)

	// The staleness verdict is computed once and stamped on the plan, so a reader can tell a
	// plan that proposes nothing because nothing is wrong from one that proposes nothing
	// because it cannot see.
	plan.MetricsStale = !guard.staleness().permitted
	// The disruption budget on its own, separate from whether rebalancing is switched on.
	// An evacuation is an instruction rather than a recommendation, so it is not subject to
	// the rebalancer being enabled - but it is subject to the concurrency cap, the rate cap
	// and the migration windows, because those exist to protect the tenants being moved and
	// do not care why a move was decided.
	migrations := guard.migrationBudget()
	plan.MigrationsPermitted = migrations.permitted
	plan.MigrationsRefusedBecause = migrations.message

	plan.Actions = actions(signals, policy, guard, plan)
	plan.Summary = summarise(plan)
	return plan
}

// recommendedInstances is the HPA ratio, with the same dead band: a utilization within
// tolerance of the target changes nothing at all.
func recommendedInstances(observed int32, signals Signals, policy Policy) int32 {
	current := signals.InstanceCount()
	if current == 0 || policy.TargetUtilizationPercent <= 0 {
		return max(current, policy.MinInstances)
	}
	deviation := observed - policy.TargetUtilizationPercent
	if deviation < 0 {
		deviation = -deviation
	}
	if deviation <= policy.TolerancePercent {
		return clamp(current, policy.MinInstances, policy.MaxInstances)
	}
	desired := (int64(current)*int64(observed) + int64(policy.TargetUtilizationPercent) - 1) /
		int64(policy.TargetUtilizationPercent)
	return clamp(int32(desired), policy.MinInstances, policy.MaxInstances)
}

// repack runs the placement algorithm over the whole pool, which is what makes eviction and
// destination one decision: the moves are read off a single packing rather than chosen one
// at a time and then given somewhere to go.
func repack(signals Signals, policy Policy) placement.Result {
	tenants := make([]placement.Tenant, 0, len(signals.Tenants))
	for _, tenant := range signals.Tenants {
		tenants = append(tenants, placement.Tenant{
			Name: tenant.Name,
			Demand: placement.Demand{
				GuaranteedConnections: tenant.GuaranteedConnections,
				ObservedConnections:   tenant.PackedConnections,
				StorageBytes:          tenant.StorageBytes,
				Relations:             tenant.Relations,
			},
			AntiAffinity:   tenant.AntiAffinity,
			PinnedInstance: tenant.PinnedInstance,
			BoundInstance:  tenant.Instance,
		})
	}
	instances := make([]placement.Instance, 0, len(signals.Instances))
	for _, instance := range signals.Instances {
		instances = append(instances, placement.Instance{
			Name:        instance.Name,
			Capacity:    placement.Capacity{Connections: instance.AllocatableConnections},
			Schedulable: instance.Schedulable,
			Ready:       instance.Ready,
		})
	}
	result, _ := placement.Pack(tenants, instances, policy.Placement)
	return result
}

func instanceTargets(signals Signals, policy Policy, packing placement.Result) []InstanceTarget {
	targets := make([]InstanceTarget, 0, len(signals.Instances))
	for _, instance := range signals.Instances {
		target := InstanceTarget{
			Name:                   instance.Name,
			UtilizationPercent:     instance.UtilizationPercent(),
			AllocatableConnections: instance.AllocatableConnections,
			StorageUsedPercent:     instance.StorageUsedPercent(),
			PackedConnections:      packing.PerInstance[instance.Name].Connections,
			Tenants:                packing.TenantsPerInstance[instance.Name],
		}
		if expanded, ok := storageTarget(instance, policy); ok {
			target.RecommendedStorageByes = expanded
		}
		// An instance the repack put nothing on is one the rest of the pool can already
		// absorb, which is the only precondition scale-in may act on. An instance that
		// merely looks idle is not: idle is a moment, and the packing is the whole window.
		target.Consolidatable = target.Tenants == 0
		targets = append(targets, target)
	}
	slices.SortFunc(targets, func(a, b InstanceTarget) int { return strings.Compare(a.Name, b.Name) })
	return targets
}

// storageTarget is the size a volume should grow to, and whether it should grow at all.
// It only ever grows: a PVC cannot shrink, so a recommendation to shrink is a
// recommendation to recreate the instance, which is not a storage action.
func storageTarget(instance InstanceSignal, policy Policy) (int64, bool) {
	if instance.StorageAllocatedBytes <= 0 || instance.StorageUsedBytes <= 0 {
		return 0, false
	}
	if instance.StorageUsedPercent() < policy.StorageExpandAtPercent {
		return 0, false
	}
	target := policy.StorageExpandToPercent
	if target <= 0 {
		target = DefaultStorageExpandToPercent
	}
	wanted := instance.StorageUsedBytes * 100 / int64(target)
	wanted = roundUpToGiB(wanted)
	if policy.StorageMaxBytes > 0 {
		wanted = min(wanted, policy.StorageMaxBytes)
	}
	if wanted <= instance.StorageAllocatedBytes {
		return 0, false
	}
	return wanted, true
}

const gibibyte = int64(1) << 30

func roundUpToGiB(bytes int64) int64 {
	return (bytes + gibibyte - 1) / gibibyte * gibibyte
}

// consolidationTarget is the instance a repack emptied, which is the only instance scale-in
// may consider. An instance the packer still put tenants on is not consolidatable however
// idle it looks.
func consolidationTarget(targets []InstanceTarget, policy Policy) string {
	if int32(len(targets)) <= policy.MinInstances {
		return ""
	}
	for _, target := range targets {
		if target.Consolidatable {
			return target.Name
		}
	}
	return ""
}

func moves(signals Signals, guard Guard, packing placement.Result) []Move {
	instances := make(map[string]InstanceSignal, len(signals.Instances))
	for _, instance := range signals.Instances {
		instances[instance.Name] = instance
	}
	tenants := make(map[string]TenantSignal, len(signals.Tenants))
	for _, tenant := range signals.Tenants {
		tenants[tenant.Name] = tenant
	}

	planned := make([]Move, 0, len(packing.Moves()))
	for _, assignment := range packing.Moves() {
		tenant := tenants[assignment.Tenant]
		source := instances[assignment.From]
		move := Move{
			Tenant: assignment.Tenant,
			From:   assignment.From,
			To:     assignment.Instance,
			Reason: fmt.Sprintf("packing on %s puts %s on %s",
				guard.Policy.Placement.PackOn, assignment.Tenant, assignment.Instance),
			ExpectedImprovementPercent: improvement(tenant, source),
		}
		move.Eligible, move.Blocker, move.BlockedBy = guard.MoveEligible(tenant, source)
		planned = append(planned, move)
	}
	return planned
}

// improvement is how many percentage points of the source's utilization the move gives
// back. A move that cannot state a number here is a move that should not be made.
func improvement(tenant TenantSignal, source InstanceSignal) int32 {
	if source.AllocatableConnections <= 0 {
		return 0
	}
	demand := max(tenant.GuaranteedConnections, tenant.PackedConnections)
	return int32(int64(demand) * 100 / int64(source.AllocatableConnections))
}

// actions proposes one entry per class that has something to do, and asks the guard about
// each. A class with nothing to do is absent rather than present-and-refused, so the plan
// distinguishes "no work" from "work the guardrails forbid".
func actions(signals Signals, policy Policy, guard Guard, plan Plan) []Action {
	proposals := make([]Action, 0, len(ActionOrder))

	for _, target := range plan.InstanceTargets {
		if target.RecommendedStorageByes > 0 {
			proposals = append(proposals, Action{
				Class:  pgelasticv1alpha1.AutoActionStorageExpand,
				Target: target.Name,
				Detail: fmt.Sprintf("grow the data volume of %s to %d bytes; it is %d%% full",
					target.Name, target.RecommendedStorageByes, target.StorageUsedPercent),
			})
			break
		}
	}

	if plan.RecommendedInstances > plan.ObservedInstances {
		proposals = append(proposals, Action{
			Class:  pgelasticv1alpha1.AutoActionScaleOut,
			Target: signals.Pool,
			Detail: fmt.Sprintf("raise instances.replicas from %d to %d; the pool is at %d%% of a %d%% target",
				plan.ObservedInstances, plan.RecommendedInstances,
				plan.ObservedUtilizationPercent, policy.TargetUtilizationPercent),
		})
	}

	if imbalance := imbalancePercent(plan.InstanceTargets); len(plan.Moves) > 0 &&
		imbalance >= policy.MinImbalancePercent {
		proposals = append(proposals, Action{
			Class:  pgelasticv1alpha1.AutoActionRebalance,
			Target: signals.Pool,
			Detail: fmt.Sprintf("%d tenant moves close a %d%% spread between the fullest and emptiest instance",
				len(plan.Moves), imbalance),
		})
	}

	if plan.ConsolidationTarget != "" && plan.RecommendedInstances < plan.ObservedInstances {
		proposals = append(proposals, Action{
			Class:  pgelasticv1alpha1.AutoActionScaleIn,
			Target: plan.ConsolidationTarget,
			Detail: fmt.Sprintf("evacuate and reclaim %s, taking the pool from %d instances to %d",
				plan.ConsolidationTarget, plan.ObservedInstances, plan.RecommendedInstances),
		})
	}

	for i := range proposals {
		verdict := guard.Evaluate(proposals[i].Class)
		proposals[i].Permitted = verdict.permitted
		proposals[i].Reason = verdict.reason
		proposals[i].Message = verdict.message
	}
	slices.SortFunc(proposals, func(a, b Action) int {
		return slices.Index(ActionOrder[:], a.Class) - slices.Index(ActionOrder[:], b.Class)
	})
	return proposals
}

// imbalancePercent is the spread between the fullest and emptiest instance, which is what
// minImbalancePercent is compared against: below it, the cost of moving exceeds the benefit.
func imbalancePercent(targets []InstanceTarget) int32 {
	if len(targets) < 2 {
		return 0
	}
	lowest, highest := int32(-1), int32(0)
	for _, target := range targets {
		utilization := packedUtilization(target)
		if lowest < 0 || utilization < lowest {
			lowest = utilization
		}
		highest = max(highest, utilization)
	}
	return highest - lowest
}

func packedUtilization(target InstanceTarget) int32 {
	if target.AllocatableConnections <= 0 {
		return 0
	}
	return int32(int64(target.PackedConnections) * 100 / int64(target.AllocatableConnections))
}

func summarise(plan Plan) string {
	if plan.MetricsStale {
		return "metrics are stale; no action will be taken"
	}
	if len(plan.Actions) == 0 {
		return fmt.Sprintf("%d instances at %d%% of a %d%% target; nothing to change",
			plan.ObservedInstances, plan.ObservedUtilizationPercent, plan.TargetUtilizationPercent)
	}
	classes := make([]string, 0, len(plan.Actions))
	for _, action := range plan.Actions {
		classes = append(classes, string(action.Class))
	}
	if selected, ok := plan.Selected(); ok {
		return fmt.Sprintf("proposing %s; executing %s", strings.Join(classes, ", "), selected.Class)
	}
	return fmt.Sprintf("proposing %s; executing nothing", strings.Join(classes, ", "))
}

func clamp(value, low, high int32) int32 {
	if high < low {
		high = low
	}
	return min(max(value, low), high)
}
