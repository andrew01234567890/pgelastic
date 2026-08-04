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
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Guardrail reasons. Every refusal names one, and the name is API surface: it lands in
// status.autoscaling.actions[].reason and in the Event, and an operator branches on it.
const (
	ReasonAllowed              = "Allowed"
	ReasonStaleMetrics         = "StaleMetrics"
	ReasonRolloutInProgress    = "RolloutInProgress"
	ReasonBlackoutWindow       = "BlackoutWindow"
	ReasonRecommendMode        = "RecommendModeOnly"
	ReasonNotOptedIn           = "NotOptedIn"
	ReasonStabilizing          = "StabilizationWindow"
	ReasonMigrationBudget      = "MigrationBudgetExhausted"
	ReasonMigrationWindow      = "OutsideMigrationWindow"
	ReasonDwellTime            = "DwellTimeNotElapsed"
	ReasonInsufficientEvidence = "InsufficientEvidence"
	ReasonAtInstanceBound      = "AtInstanceBound"
	ReasonHumanGated           = "HumanGated"
	ReasonPoolPaused           = "PoolPaused"
	ReasonScheduleInvalid      = "ScheduleInvalid"
	ReasonRebalancingDisabled  = "RebalancingDisabled"
	ReasonNoExecutor           = "NoExecutorAvailable"
	ReasonNotProposed          = "NotProposed"
)

// ActionOrder is the order action classes are considered in, and the order the API
// documents them being opted into: cheapest and most reversible first, scale-in last.
//
// At most one class executes per planning pass. That is what "one class at a time" means in
// practice — not that a pool may only ever enable one, but that a single pass never both
// grows a volume and moves a tenant, because the second decision was computed against a
// world the first one has just changed.
var ActionOrder = [...]pgelasticv1alpha1.AutoAction{
	pgelasticv1alpha1.AutoActionTenantGucTune,
	pgelasticv1alpha1.AutoActionStorageExpand,
	pgelasticv1alpha1.AutoActionScaleOut,
	pgelasticv1alpha1.AutoActionRebalance,
	pgelasticv1alpha1.AutoActionVerticalResize,
	pgelasticv1alpha1.AutoActionScaleIn,
}

// verdict is one guardrail's answer.
type verdict struct {
	permitted bool
	reason    string
	message   string
}

func allow() verdict { return verdict{permitted: true, reason: ReasonAllowed} }

func refuse(reason, format string, args ...any) verdict {
	return verdict{reason: reason, message: fmt.Sprintf(format, args...)}
}

// Guard evaluates one action class against every guardrail, in the order that puts the
// cheapest and most categorical checks first.
type Guard struct {
	Policy  Policy
	Signals Signals
	// ConsolidationTarget is the instance scale-in would reclaim. The dwell time is read
	// off this instance's own timestamp, never off whichever candidate has waited longest.
	ConsolidationTarget string
}

// Evaluate answers whether one action class may execute now.
func (g Guard) Evaluate(class pgelasticv1alpha1.AutoAction) verdict {
	if g.Signals.Paused {
		return refuse(ReasonPoolPaused, "the pool is paused")
	}
	if verdict := g.staleness(); !verdict.permitted {
		return verdict
	}
	if g.Signals.RolloutInProgress {
		return refuse(ReasonRolloutInProgress,
			"an instance is rolling out; acting now would compound a change already in flight")
	}
	if verdict := g.blackout(); !verdict.permitted {
		return verdict
	}
	if verdict := g.mode(class); !verdict.permitted {
		return verdict
	}
	return g.perClass(class)
}

// staleness is the KEDA-shaped fallback. The reading most likely to be stale is the one
// taken while the thing being measured was already failing, so the default is to do nothing
// at all rather than to act on the last number that happened to arrive.
func (g Guard) staleness() verdict {
	if !g.Signals.MetricsSeen {
		return refuse(ReasonStaleMetrics, "the pool has never been metered")
	}
	if g.Policy.StaleThreshold > 0 && g.Signals.MetricsAge > g.Policy.StaleThreshold {
		return refuse(ReasonStaleMetrics, "the newest sample is %s old, past the %s threshold",
			g.Signals.MetricsAge.Round(time.Second), g.Policy.StaleThreshold)
	}
	return allow()
}

func (g Guard) blackout() verdict {
	open, err := WindowOpen(g.Signals.Now, g.Policy.BlackoutWindows)
	if err != nil {
		return refuse(ReasonScheduleInvalid, "%v", err)
	}
	if open {
		return refuse(ReasonBlackoutWindow, "a blackout window is open")
	}
	return allow()
}

// mode applies the Recommend/Auto split. Recommend executes storage expansion and nothing
// else: it is online, it is the only remedy for a volume that is filling, and a full volume
// is an outage that no amount of human gating prevents.
func (g Guard) mode(class pgelasticv1alpha1.AutoAction) verdict {
	if g.Policy.Mode != pgelasticv1alpha1.AutoscalingAuto {
		if class == pgelasticv1alpha1.AutoActionStorageExpand {
			return allow()
		}
		return refuse(ReasonRecommendMode,
			"mode is %s: the plan is published and only StorageExpand is executed", g.Policy.Mode)
	}
	if !g.Policy.Allows(class) {
		return refuse(ReasonNotOptedIn, "%s is not in autoActions", class)
	}
	return allow()
}

func (g Guard) perClass(class pgelasticv1alpha1.AutoAction) verdict {
	switch class {
	case pgelasticv1alpha1.AutoActionStorageExpand:
		return allow()
	case pgelasticv1alpha1.AutoActionTenantGucTune:
		return allow()
	case pgelasticv1alpha1.AutoActionScaleOut:
		return g.scaleOut()
	case pgelasticv1alpha1.AutoActionRebalance:
		return g.rebalance()
	case pgelasticv1alpha1.AutoActionVerticalResize:
		// shared_buffers cannot change without a restart, and a restart is only transparent
		// if switchover through the proxy has been proven so. Until that proof exists this
		// class is refused even when it has been opted into, because opting in is not
		// evidence.
		return refuse(ReasonHumanGated,
			"VerticalResize restarts a postmaster and is human-gated until switchover is proven transparent")
	case pgelasticv1alpha1.AutoActionScaleIn:
		return g.scaleIn()
	default:
		return refuse(ReasonNotOptedIn, "unknown action class %q", class)
	}
}

func (g Guard) scaleOut() verdict {
	if g.Signals.InstanceCount() >= g.Policy.MaxInstances {
		return refuse(ReasonAtInstanceBound, "the pool is already at maxInstances %d", g.Policy.MaxInstances)
	}
	if elapsed, ok := since(g.Signals.Now, g.Signals.LastScaleUpAt); ok && elapsed < g.Policy.ScaleUpStabilization {
		return refuse(ReasonStabilizing, "scaled up %s ago, inside the %s scale-up window",
			elapsed.Round(time.Second), g.Policy.ScaleUpStabilization)
	}
	return allow()
}

func (g Guard) rebalance() verdict {
	if !g.Policy.RebalanceEnabled {
		return refuse(ReasonRebalancingDisabled, "rebalancing is not enabled on this pool")
	}
	return g.migrationBudget()
}

// migrationBudget is the Karpenter-shaped disruption budget: a concurrency cap, a rate cap
// over the window, and cron windows outside which the budget is zero.
func (g Guard) migrationBudget() verdict {
	budget := g.Policy.MigrationBudget
	if len(budget.Windows) > 0 {
		open, err := WindowOpen(g.Signals.Now, budget.Windows)
		if err != nil {
			return refuse(ReasonScheduleInvalid, "%v", err)
		}
		if !open {
			if next, ok := NextWindow(g.Signals.Now, budget.Windows); ok {
				return refuse(ReasonMigrationWindow, "the next migration window opens at %s",
					next.UTC().Format(time.RFC3339))
			}
			return refuse(ReasonMigrationWindow, "no migration window is open")
		}
	}
	if budget.MaxConcurrent <= 0 {
		return refuse(ReasonMigrationBudget, "maxConcurrent is 0, so no move may start")
	}
	if g.Signals.InFlightMigrations >= budget.MaxConcurrent {
		return refuse(ReasonMigrationBudget, "%d migrations in flight against a cap of %d",
			g.Signals.InFlightMigrations, budget.MaxConcurrent)
	}
	if budget.MaxPerWindow > 0 && g.Signals.MigrationsStartedInWindow >= budget.MaxPerWindow {
		return refuse(ReasonMigrationBudget, "%d migrations already started in this window against a cap of %d",
			g.Signals.MigrationsStartedInWindow, budget.MaxPerWindow)
	}
	return allow()
}

// scaleIn is the most dangerous action in the system and carries the most gates: the
// scale-down stabilization window, a dwell time on the specific consolidation decision, a
// week of per-tenant evidence, and the migration budget that has to pay for the evacuation.
func (g Guard) scaleIn() verdict {
	if g.Signals.InstanceCount() <= g.Policy.MinInstances {
		return refuse(ReasonAtInstanceBound, "the pool is already at minInstances %d", g.Policy.MinInstances)
	}
	if elapsed, ok := since(g.Signals.Now, g.Signals.LastScaleDownAt); ok && elapsed < g.Policy.ScaleDownStabilization {
		return refuse(ReasonStabilizing, "scaled down %s ago, inside the %s scale-down window",
			elapsed.Round(time.Second), g.Policy.ScaleDownStabilization)
	}
	if g.Signals.EvidenceSpan < g.Policy.ScaleInEvidenceWindow {
		return refuse(ReasonInsufficientEvidence,
			"the pool has %s of per-tenant history against the %s scale-in requires",
			g.Signals.EvidenceSpan.Round(time.Hour), g.Policy.ScaleInEvidenceWindow)
	}
	elapsed, ok := g.consolidationDwell()
	if !ok {
		return refuse(ReasonDwellTime,
			"%s has not been observed consolidatable for any dwell time yet", g.ConsolidationTarget)
	}
	if elapsed < g.Policy.ConsolidationDwell {
		return refuse(ReasonDwellTime, "%s has been consolidatable for %s of the required %s",
			g.ConsolidationTarget, elapsed.Round(time.Minute), g.Policy.ConsolidationDwell)
	}
	return g.migrationBudget()
}

// MoveEligible reports whether one specific tenant may be moved off one specific instance.
// It is separate from the class-level budget because the eviction and the destination are
// decided as one plan, and a plan whose evictions are individually forbidden is not a plan
// that can be partially executed.
func (g Guard) MoveEligible(tenant TenantSignal, source InstanceSignal) (bool, Blocker, string) {
	if !tenant.MigrationAllowed {
		return false, BlockedByWorkloadClass,
			"the tenant's workload class does not permit automatic migration"
	}
	if g.Policy.RebalanceColdOnly && !tenant.Cold {
		return false, BlockedByHeat, fmt.Sprintf(
			"the tenant is hot: moving it would consume exactly the capacity "+
				"the move is meant to relieve (threshold %d%%)", g.Policy.HotTenantPercent)
	}
	if source.UtilizationPercent() > g.Policy.ForbidMoveAboveSourcePercent {
		return false, BlockedBySourceLoad, fmt.Sprintf(
			"source %s is at %d%% utilization, above the %d%% ceiling for decoding a move",
			source.Name, source.UtilizationPercent(), g.Policy.ForbidMoveAboveSourcePercent)
	}
	if g.Signals.MigrationsFromSource[source.Name] > 0 {
		return false, BlockedByInFlight,
			fmt.Sprintf("a migration from %s is already in flight", source.Name)
	}
	return true, "", ""
}

// consolidationDwell is how long the instance scale-in has chosen has been consolidatable.
// A target that is not named, or that has no timestamp of its own, has waited for nothing.
func (g Guard) consolidationDwell() (time.Duration, bool) {
	if g.ConsolidationTarget == "" {
		return 0, false
	}
	at, ok := g.Signals.ConsolidatableSince[g.ConsolidationTarget]
	if !ok {
		return 0, false
	}
	return since(g.Signals.Now, &at)
}

func since(now time.Time, at *time.Time) (time.Duration, bool) {
	if at == nil || at.IsZero() {
		return 0, false
	}
	return now.Sub(*at), true
}
