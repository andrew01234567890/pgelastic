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

package controller

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/autoscale"
	"github.com/andrew01234567890/pgelastic/internal/metering"
	"github.com/andrew01234567890/pgelastic/internal/placement"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

// resolveEffective walks a tenant's workload class and returns the numbers in force. A
// tenant whose class does not resolve reserves nothing anybody can be held to, so it is
// skipped rather than being allowed to inflate the ledger.
func resolveEffective(
	ctx context.Context,
	resolver policy.Resolver,
	reader client.Reader,
	tenant *pgelasticv1alpha1.PgTenant,
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
	cache map[string]*pgelasticv1alpha1.PgWorkloadClass,
) (policy.Effective, bool, error) {
	name, err := resolver.WorkloadClassNameFor(ctx, tenant, pool, elasticClass)
	if err != nil {
		return policy.Effective{}, false, nil
	}
	class, cached := cache[name]
	if !cached {
		class = &pgelasticv1alpha1.PgWorkloadClass{}
		if err := reader.Get(ctx, types.NamespacedName{Name: name}, class); err != nil {
			if apierrors.IsNotFound(err) {
				cache[name] = nil
				return policy.Effective{}, false, nil
			}
			return policy.Effective{}, false, err
		}
		cache[name] = class
	}
	if class == nil {
		return policy.Effective{}, false, nil
	}
	return policy.EffectiveFor(tenant, class), true, nil
}

func meteringKeyOf(tenant *pgelasticv1alpha1.PgTenant) metering.Key {
	return metering.Key{
		Namespace: tenant.Namespace,
		Pool:      tenant.Spec.PoolRef.Name,
		Tenant:    tenant.Name,
	}
}

// currentConnectionsOf is the tenant's live backend-connection count as last published. It
// is what the proxy reports through the tenant's status, and it is the sample the trailing
// window is built from.
func currentConnectionsOf(tenant *pgelasticv1alpha1.PgTenant) int32 {
	utilization := tenant.Status.Utilization
	if utilization == nil || utilization.BackendConnections == nil ||
		utilization.BackendConnections.Current == nil {
		return 0
	}
	return *utilization.BackendConnections.Current
}

// packedConnectionsOf is the statistic the pool packs on, read out of the trailing-window
// store. A tenant nobody has metered yet packs on its guarantee alone: an unmeasured tenant
// is an absence of evidence, and treating it as an observed zero would let the packer fill
// an instance with tenants it knows nothing about.
func packedConnectionsOf(entry *tenantView) int32 {
	if !entry.metered || entry.observation.Samples == 0 {
		return entry.effective.Guaranteed
	}
	return int32(math.Ceil(entry.packed))
}

// isColdTenant reports the tenant as below the pool's hot threshold for the whole window,
// which is what makes it eligible to be moved. It is measured on the peak rather than on the
// current value: a tenant that is idle at this instant and busy every afternoon is not cold.
func isColdTenant(entry *tenantView, hotPercent int32) bool {
	if !entry.metered || entry.observation.Samples == 0 {
		return false
	}
	ceiling := entry.effective.Burstable
	if ceiling <= 0 {
		return false
	}
	return entry.observation.Peak*100 < float64(ceiling)*float64(hotPercent)
}

// evidenceSpanOf is the shortest per-tenant history in the pool. Scale-in's week-long
// evidence gate is measured against it because one tenant nobody has watched for a week is
// enough to make consolidating around it a guess.
func evidenceSpanOf(tenants []tenantView) time.Duration {
	shortest := time.Duration(0)
	for i := range tenants {
		entry := &tenants[i]
		if !entry.metered {
			return 0
		}
		span := entry.observation.LastSampleAt.Sub(entry.observation.FirstSampleAt)
		if i == 0 || span < shortest {
			shortest = span
		}
	}
	return shortest
}

func ledgerStatus(view *poolView) *pgelasticv1alpha1.CapacityLedger {
	ledger := view.ledger
	inUse := int32(0)
	for i := range view.instances {
		if capacity := view.instances[i].Status.Capacity; capacity != nil {
			inUse += capacity.InUse
		}
	}
	return &pgelasticv1alpha1.CapacityLedger{
		BackendConnections:              ledger.BackendConnections,
		Allocatable:                     ledger.Allocatable,
		Reserved:                        ledger.Reserved,
		Available:                       ledger.Available,
		CommittedBurst:                  ledger.CommittedBurst,
		ObservedOversubscription:        oversubscriptionOf(ledger),
		InUse:                           inUse,
		DerivedFrom:                     derivationOf(view),
		MaxTenantsAtCurrentReservations: maxTenantsOf(view),
	}
}

func oversubscriptionOf(ledger policy.Ledger) string {
	if ledger.Allocatable <= 0 {
		return "0.0"
	}
	return fmt.Sprintf("%.1f", float64(ledger.CommittedBurst)/float64(ledger.Allocatable))
}

// derivationOf explains where the allocatable number came from. Capacity is derived rather
// than invented, and the arithmetic has to be auditable from kubectl alone.
func derivationOf(view *poolView) string {
	ready := readyInstanceCount(view)
	allocatable := int32(0)
	for i := range view.instances {
		if capacity := view.instances[i].Status.Capacity; capacity != nil {
			allocatable += capacity.Allocatable
		}
	}
	return fmt.Sprintf("%d of %d instances ready, contributing %d allocatable connections; "+
		"pool budget %d less %d%% headroom is %d",
		ready, len(view.instances), allocatable,
		view.ledger.BackendConnections, view.ledger.HeadroomPercent, view.ledger.Allocatable)
}

// maxTenantsOf is how many more tenants of the pool's default shape could still be admitted.
// A pool where every tenant sets a safety floor performs worse than one with all floors at
// zero, and this is the number that makes that visible before it happens.
func maxTenantsOf(view *poolView) int32 {
	averageGuarantee := int32(0)
	if view.ledger.Tenants > 0 {
		averageGuarantee = view.ledger.Reserved / view.ledger.Tenants
	}
	if averageGuarantee <= 0 {
		return 0
	}
	return view.ledger.Available / averageGuarantee
}

func perInstanceStatus(view *poolView) []pgelasticv1alpha1.PoolInstanceStatus {
	reserved := map[string]int32{}
	tenants := map[string]int32{}
	for i := range view.tenants {
		entry := &view.tenants[i]
		if bound := placement.BoundInstanceFor(entry.tenant); bound != "" {
			reserved[bound] += entry.effective.Guaranteed
			tenants[bound]++
		}
	}

	rows := make([]pgelasticv1alpha1.PoolInstanceStatus, 0, len(view.instances))
	for i := range view.instances {
		instance := &view.instances[i]
		row := pgelasticv1alpha1.PoolInstanceStatus{
			Name:     instance.Name,
			Reserved: reserved[instance.Name],
			Tenants:  tenants[instance.Name],
			Ready:    instance.Status.Phase == pgelasticv1alpha1.InstancePhaseReady,
		}
		if capacity := instance.Status.Capacity; capacity != nil {
			row.Allocatable = capacity.Allocatable
			row.InUse = capacity.InUse
		}
		if instance.Status.CurrentPrimary != "" {
			row.Role = pgelasticv1alpha1.InstanceRolePrimary
		}
		rows = append(rows, row)
	}
	return rows
}

// planStatus projects the computed plan onto the CRD, carrying forward the two facts that
// only exist across passes: when each action class last executed, and how long the current
// consolidation candidate has been one.
func planStatus(
	pool *pgelasticv1alpha1.PgElasticPool,
	plan autoscale.Plan,
	applied executed,
	now time.Time,
) *pgelasticv1alpha1.AutoscalingPlan {
	previous := pool.Status.Autoscaling
	status := &pgelasticv1alpha1.AutoscalingPlan{
		Mode:                       plan.Mode,
		ComputedAt:                 &metav1.Time{Time: plan.ComputedAt},
		MetricsStale:               plan.MetricsStale,
		ObservedInstances:          plan.ObservedInstances,
		MeasuredInstances:          plan.MeasuredInstances,
		RecommendedInstances:       plan.RecommendedInstances,
		ObservedUtilizationPercent: plan.ObservedUtilizationPercent,
		TargetUtilizationPercent:   plan.TargetUtilizationPercent,
		Summary:                    plan.Summary,
	}

	for _, target := range plan.InstanceTargets {
		row := pgelasticv1alpha1.InstanceTarget{
			Name:                   target.Name,
			UtilizationPercent:     target.UtilizationPercent,
			PackedConnections:      target.PackedConnections,
			AllocatableConnections: target.AllocatableConnections,
			Tenants:                target.Tenants,
			StorageUsedPercent:     target.StorageUsedPercent,
			Consolidatable:         target.Consolidatable,
		}
		if target.RecommendedStorageByes > 0 {
			row.RecommendedStorage = resource.NewQuantity(target.RecommendedStorageByes, resource.BinarySI)
		}
		if target.Consolidatable {
			row.ConsolidatableSince = consolidatableSince(previous, target.Name, now)
		}
		status.PerInstance = append(status.PerInstance, row)
	}

	for _, move := range plan.Moves {
		status.Moves = append(status.Moves, pgelasticv1alpha1.PlannedMove{
			Name:                       move.Tenant,
			From:                       move.From,
			To:                         move.To,
			ExpectedImprovementPercent: move.ExpectedImprovementPercent,
			Reason:                     move.Reason,
			Eligible:                   move.Eligible,
			BlockedBy:                  move.BlockedBy,
		})
	}

	proposed := map[pgelasticv1alpha1.AutoAction]struct{}{}
	for _, action := range plan.Actions {
		proposed[action.Class] = struct{}{}
		entry := pgelasticv1alpha1.PlannedAction{
			Name:       action.Class,
			Target:     action.Target,
			Detail:     action.Detail,
			Permitted:  action.Permitted,
			Reason:     action.Reason,
			Message:    action.Message,
			ExecutedAt: executedAt(previous, action.Class),
		}
		if applied.applied && applied.class == action.Class {
			entry.ExecutedAt = &metav1.Time{Time: applied.at}
		}
		if applied.class == action.Class && applied.failReason != "" {
			entry.Permitted = false
			entry.Reason = applied.failReason
			entry.Message = "no executor for this class is wired into the operator"
		}
		status.Actions = append(status.Actions, entry)
	}

	// A class stops being proposed the moment its work is done — a grown volume needs no
	// second expansion — so an entry that only existed while proposed would take its
	// executedAt with it. That timestamp is what the stabilization windows are measured
	// from, and losing it is how a pool scales out twice in a row.
	for _, class := range autoscale.ActionOrder {
		if _, ok := proposed[class]; ok {
			continue
		}
		at := executedAt(previous, class)
		if at == nil {
			continue
		}
		status.Actions = append(status.Actions, pgelasticv1alpha1.PlannedAction{
			Name:       class,
			Reason:     autoscale.ReasonNotProposed,
			Message:    "nothing in this class needs doing; the timestamp is when it last did",
			ExecutedAt: at,
		})
	}
	slices.SortFunc(status.Actions, func(a, b pgelasticv1alpha1.PlannedAction) int {
		return slices.Index(autoscale.ActionOrder[:], a.Name) - slices.Index(autoscale.ActionOrder[:], b.Name)
	})
	return status
}

// consolidatableSince keeps the clock running from the first pass that found this instance
// consolidatable, and restarts it when the candidate changes. A dwell time that reset on
// every reconcile would never elapse; one that survived a change of candidate would credit
// the new one with the old one's patience.
func consolidatableSince(
	previous *pgelasticv1alpha1.AutoscalingPlan,
	instance string,
	now time.Time,
) *metav1.Time {
	if previous != nil {
		for _, target := range previous.PerInstance {
			if target.Name == instance && target.Consolidatable && target.ConsolidatableSince != nil {
				return target.ConsolidatableSince
			}
		}
	}
	return &metav1.Time{Time: now}
}

func executedAt(
	previous *pgelasticv1alpha1.AutoscalingPlan,
	class pgelasticv1alpha1.AutoAction,
) *metav1.Time {
	if previous == nil {
		return nil
	}
	for _, action := range previous.Actions {
		if action.Name == class {
			return action.ExecutedAt
		}
	}
	return nil
}

func readyInstanceCount(view *poolView) int {
	ready := 0
	for i := range view.instances {
		if view.instances[i].Status.Phase == pgelasticv1alpha1.InstancePhaseReady {
			ready++
		}
	}
	return ready
}

func acceptedReasonOf(view *poolView) string {
	if view.elasticClass == nil {
		return pgelasticv1alpha1.ReasonPending
	}
	return pgelasticv1alpha1.ReasonAccepted
}

func acceptedMessageOf(view *poolView, pool *pgelasticv1alpha1.PgElasticPool) string {
	if view.elasticClass == nil {
		return fmt.Sprintf("PgElasticClass %q does not exist", pool.Spec.ClassRef.Name)
	}
	return fmt.Sprintf("bound to PgElasticClass %q; %d allocatable connections against %d reserved",
		view.elasticClass.Name, view.ledger.Allocatable, view.ledger.Reserved)
}

func readyPoolReason(ready bool) string {
	if ready {
		return pgelasticv1alpha1.ReasonReady
	}
	return pgelasticv1alpha1.ReasonPending
}

func readyPoolMessage(view *poolView) string {
	return fmt.Sprintf("%d of %d member instances are ready", readyInstanceCount(view), len(view.instances))
}

// poolPhase is a pure function of the conditions, present only so kubectl output has one
// column to show.
func poolPhase(view *poolView, conditions []metav1.Condition) pgelasticv1alpha1.PoolPhase {
	switch {
	case !conditionTrue(conditions, pgelasticv1alpha1.ConditionAccepted):
		return pgelasticv1alpha1.PoolPhasePending
	case readyInstanceCount(view) == 0:
		return pgelasticv1alpha1.PoolPhasePending
	case readyInstanceCount(view) < len(view.instances):
		return pgelasticv1alpha1.PoolPhaseDegraded
	default:
		return pgelasticv1alpha1.PoolPhaseReady
	}
}

func conditionTrue(conditions []metav1.Condition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Status == metav1.ConditionTrue
		}
	}
	return false
}
