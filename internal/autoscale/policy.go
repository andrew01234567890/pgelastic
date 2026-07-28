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

// Package autoscale computes a pool's whole capacity plan and decides, against a set of
// guardrails, which single part of it may be executed now.
//
// The default is mode Recommend: the plan is computed and published in full, Events are
// emitted, and nothing is executed except storage expansion. Auto mode is opted into one
// action class at a time, and within any one pass at most one class executes — the earliest
// in ActionOrder that is both proposed and permitted, which puts the cheap reversible
// changes ahead of the ones that move data and scale-in last of all.
//
// Every refusal is a named reason rather than a silent no-op, because "why did the pool not
// scale" is a question asked in an incident and answered badly by absence.
package autoscale

import (
	"slices"
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/placement"
)

// CRD defaults, restated so a pool stored before a field existed resolves the same way a
// freshly defaulted one does.
const (
	DefaultTargetUtilizationPercent = int32(70)
	DefaultTolerancePercent         = int32(10)
	DefaultScaleUpStabilization     = 3 * time.Minute
	DefaultScaleDownStabilization   = 30 * time.Minute
	DefaultStaleThreshold           = 5 * time.Minute
	DefaultConsolidationDwell       = 24 * time.Hour
	DefaultScaleInEvidenceWindow    = 168 * time.Hour
	DefaultMaxConcurrentMigrations  = int32(1)
	DefaultMaxMigrationsPerWindow   = int32(4)
	DefaultStorageExpandAtPercent   = int32(80)
	DefaultStorageExpandToPercent   = int32(60)
	DefaultMinImbalancePercent      = int32(20)
	DefaultForbidMoveAbovePercent   = int32(65)
	DefaultHotTenantPercent         = int32(15)
	DefaultMinInstances             = int32(1)
	DefaultMaxInstances             = int32(64)
)

// Policy is every knob the planner reads, resolved from the pool's spec.
type Policy struct {
	Mode        pgelasticv1alpha1.AutoscalingMode
	AutoActions []pgelasticv1alpha1.AutoAction

	MinInstances             int32
	MaxInstances             int32
	TargetUtilizationPercent int32
	TolerancePercent         int32

	ScaleUpStabilization   time.Duration
	ScaleDownStabilization time.Duration

	StaleThreshold time.Duration
	StaleFallback  pgelasticv1alpha1.StaleMetricPolicy

	ConsolidationDwell    time.Duration
	ScaleInEvidenceWindow time.Duration

	MigrationBudget MigrationBudget
	BlackoutWindows []pgelasticv1alpha1.TimeWindow

	StorageExpandAtPercent int32
	StorageExpandToPercent int32
	StorageMaxBytes        int64

	RebalanceEnabled             bool
	RebalanceColdOnly            bool
	MinImbalancePercent          int32
	HotTenantPercent             int32
	ForbidMoveAboveSourcePercent int32

	Placement placement.Policy
}

// MigrationBudget is the resolved movement budget.
type MigrationBudget struct {
	MaxConcurrent int32
	MaxPerWindow  int32
	Windows       []pgelasticv1alpha1.TimeWindow
}

// Allows reports whether a class is in the opt-in list.
func (p Policy) Allows(class pgelasticv1alpha1.AutoAction) bool {
	return slices.Contains(p.AutoActions, class)
}

// PolicyFor resolves a pool's autoscaling policy.
func PolicyFor(pool *pgelasticv1alpha1.PgElasticPool) Policy {
	policy := Policy{
		Mode:                         pgelasticv1alpha1.AutoscalingRecommend,
		MinInstances:                 DefaultMinInstances,
		MaxInstances:                 DefaultMaxInstances,
		TargetUtilizationPercent:     DefaultTargetUtilizationPercent,
		TolerancePercent:             DefaultTolerancePercent,
		ScaleUpStabilization:         DefaultScaleUpStabilization,
		ScaleDownStabilization:       DefaultScaleDownStabilization,
		StaleThreshold:               DefaultStaleThreshold,
		StaleFallback:                pgelasticv1alpha1.StaleMetricDoNothing,
		ConsolidationDwell:           DefaultConsolidationDwell,
		ScaleInEvidenceWindow:        DefaultScaleInEvidenceWindow,
		StorageExpandAtPercent:       DefaultStorageExpandAtPercent,
		StorageExpandToPercent:       DefaultStorageExpandToPercent,
		MinImbalancePercent:          DefaultMinImbalancePercent,
		HotTenantPercent:             DefaultHotTenantPercent,
		ForbidMoveAboveSourcePercent: DefaultForbidMoveAbovePercent,
		RebalanceColdOnly:            true,
		MigrationBudget: MigrationBudget{
			MaxConcurrent: DefaultMaxConcurrentMigrations,
			MaxPerWindow:  DefaultMaxMigrationsPerWindow,
		},
		Placement: placement.PolicyFor(pool),
	}
	if pool == nil {
		return policy
	}

	applyAutoscalingSpec(&policy, pool.Spec.Autoscaling)
	applyRebalancingSpec(&policy, pool.Spec.Rebalancing)
	return policy
}

func applyAutoscalingSpec(policy *Policy, spec *pgelasticv1alpha1.PoolAutoscaling) {
	if spec == nil {
		return
	}
	if spec.Mode != "" {
		policy.Mode = spec.Mode
	}
	policy.AutoActions = spec.AutoActions
	if spec.MinInstances != nil {
		policy.MinInstances = *spec.MinInstances
	}
	if spec.MaxInstances != nil {
		policy.MaxInstances = *spec.MaxInstances
	}
	if spec.TargetUtilizationPercent != nil {
		policy.TargetUtilizationPercent = *spec.TargetUtilizationPercent
	}
	if spec.TolerancePercent != nil {
		policy.TolerancePercent = *spec.TolerancePercent
	}
	if window := spec.StabilizationWindow; window != nil {
		if window.ScaleUp != nil {
			policy.ScaleUpStabilization = window.ScaleUp.Duration
		}
		if window.ScaleDown != nil {
			policy.ScaleDownStabilization = window.ScaleDown.Duration
		}
	}
	if spec.StaleMetricThreshold != nil {
		policy.StaleThreshold = spec.StaleMetricThreshold.Duration
	}
	if spec.StaleMetricPolicy != "" {
		policy.StaleFallback = spec.StaleMetricPolicy
	}
	if spec.ConsolidationDwellTime != nil {
		policy.ConsolidationDwell = spec.ConsolidationDwellTime.Duration
	}
	if spec.ScaleInEvidenceWindow != nil {
		policy.ScaleInEvidenceWindow = spec.ScaleInEvidenceWindow.Duration
	}
	policy.BlackoutWindows = spec.BlackoutWindows
	if budget := spec.MigrationBudget; budget != nil {
		if budget.MaxConcurrent != nil {
			policy.MigrationBudget.MaxConcurrent = *budget.MaxConcurrent
		}
		if budget.MaxPerWindow != nil {
			policy.MigrationBudget.MaxPerWindow = *budget.MaxPerWindow
		}
		policy.MigrationBudget.Windows = budget.Windows
	}
	if storage := spec.Storage; storage != nil {
		if storage.ExpandAtPercent != nil {
			policy.StorageExpandAtPercent = *storage.ExpandAtPercent
		}
		if storage.ExpandToPercent != nil {
			policy.StorageExpandToPercent = *storage.ExpandToPercent
		}
		if storage.MaxSize != nil {
			policy.StorageMaxBytes = storage.MaxSize.Value()
		}
	}
}

func applyRebalancingSpec(policy *Policy, spec *pgelasticv1alpha1.PoolRebalancing) {
	if spec == nil {
		return
	}
	if spec.Enabled != nil {
		policy.RebalanceEnabled = *spec.Enabled
	}
	if spec.Mode != "" {
		policy.RebalanceColdOnly = spec.Mode == pgelasticv1alpha1.RebalanceColdTenantsOnly
	}
	if spec.MinImbalancePercent != nil {
		policy.MinImbalancePercent = *spec.MinImbalancePercent
	}
	if spec.HotTenantUtilizationThresholdPercent != nil {
		policy.HotTenantPercent = *spec.HotTenantUtilizationThresholdPercent
	}
	if spec.ForbidMoveWhenSourceUtilizationAbovePercent != nil {
		policy.ForbidMoveAboveSourcePercent = *spec.ForbidMoveWhenSourceUtilizationAbovePercent
	}
	if spec.MaxConcurrentMigrations != nil {
		policy.MigrationBudget.MaxConcurrent = min(policy.MigrationBudget.MaxConcurrent, *spec.MaxConcurrentMigrations)
	}
	// A rebalancing blackout is also an autoscaling blackout: the pool is being left alone,
	// and growing it during a change freeze is no more welcome than moving a tenant.
	policy.BlackoutWindows = append(policy.BlackoutWindows, spec.BlackoutWindows...)
}
