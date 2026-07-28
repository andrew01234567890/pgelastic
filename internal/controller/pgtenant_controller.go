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
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

// placementRetryInterval is how long a tenant waits before its unresolved references or
// its missing placement are looked at again. Both are states another actor has to leave,
// so retrying faster only burns API calls.
const placementRetryInterval = 30 * time.Second

// PgTenantReconciler reconciles a PgTenant object
type PgTenantReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants/finalizers,verbs=update
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgworkloadclasses,verbs=get;list;watch

// Reconcile resolves the tenant's effective policy, derives its QoS class from the
// resulting numbers and publishes both. Placement onto an instance is a separate
// concern, so Bound stays false until a scheduler has actually chosen one.
func (r *PgTenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	tenant := &pgelasticv1alpha1.PgTenant{}
	if err := r.Get(ctx, req.NamespacedName, tenant); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !tenant.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	status := pgelasticv1alpha1.PgTenantStatus{
		ObservedGeneration: tenant.Generation,
		Binding:            tenant.Status.Binding,
		Utilization:        tenant.Status.Utilization,
		Throttle:           tenant.Status.Throttle,
		Conditions:         tenant.Status.Conditions,
	}

	effective, reason, message, err := r.resolve(ctx, tenant)
	if err != nil {
		return ctrl.Result{}, err
	}

	accepted := reason == pgelasticv1alpha1.ReasonAccepted
	if accepted {
		status.QoSClass = effective.QoSClass
		status.Effective = &pgelasticv1alpha1.PgTenantEffectiveLimits{
			Guaranteed:       ptr.To(effective.Guaranteed),
			Burstable:        ptr.To(effective.Burstable),
			Weight:           ptr.To(effective.Weight),
			StatementTimeout: effective.StatementTimeout,
			TempFileLimit:    effective.TempFileLimit,
		}
	}

	setCondition(&status.Conditions, tenant.Generation, pgelasticv1alpha1.ConditionAccepted,
		conditionStatus(accepted), reason, message)
	setCondition(&status.Conditions, tenant.Generation, pgelasticv1alpha1.ConditionBound,
		metav1.ConditionFalse, pgelasticv1alpha1.ReasonPending,
		"no PgInstance has been selected for this tenant yet")
	setCondition(&status.Conditions, tenant.Generation, pgelasticv1alpha1.ConditionReady,
		metav1.ConditionFalse, pgelasticv1alpha1.ReasonPending,
		"the tenant database is not serving until the tenant is bound to an instance")
	status.Phase = tenantPhase(status.Conditions)

	if !equality.Semantic.DeepEqual(tenant.Status, status) {
		tenant.Status = status
		if err := r.Status().Update(ctx, tenant); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: placementRetryInterval}, nil
}

// resolve walks the pool and workload class references and returns the effective policy
// together with the Accepted reason and message the walk produced. A reference that does
// not resolve is reported, never treated as an error: the referent may simply not have
// been created yet.
func (r *PgTenantReconciler) resolve(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
) (policy.Effective, string, string, error) {
	resolver := policy.Resolver{Reader: r.Client}

	pool := &pgelasticv1alpha1.PgElasticPool{}
	poolKey := types.NamespacedName{Namespace: tenant.Namespace, Name: tenant.Spec.PoolRef.Name}
	if err := r.Get(ctx, poolKey, pool); err != nil {
		if !apierrors.IsNotFound(err) {
			return policy.Effective{}, "", "", err
		}
		return policy.Effective{}, pgelasticv1alpha1.ReasonPending,
			fmt.Sprintf("PgElasticPool %q does not exist in namespace %q", poolKey.Name, poolKey.Namespace), nil
	}

	elasticClass, err := resolver.ElasticClassFor(ctx, pool)
	if apierrors.IsNotFound(err) {
		elasticClass = nil
	} else if err != nil {
		return policy.Effective{}, "", "", err
	}

	workloadClassName, err := resolver.WorkloadClassNameFor(ctx, tenant, pool, elasticClass)
	if errors.Is(err, policy.ErrNoWorkloadClass) {
		return policy.Effective{}, pgelasticv1alpha1.ReasonPending, err.Error(), nil
	} else if err != nil {
		return policy.Effective{}, "", "", err
	}

	workloadClass := &pgelasticv1alpha1.PgWorkloadClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: workloadClassName}, workloadClass); err != nil {
		if !apierrors.IsNotFound(err) {
			return policy.Effective{}, "", "", err
		}
		return policy.Effective{}, pgelasticv1alpha1.ReasonPending,
			fmt.Sprintf("PgWorkloadClass %q does not exist", workloadClassName), nil
	}

	effective := policy.EffectiveFor(tenant, workloadClass)
	return effective, pgelasticv1alpha1.ReasonAccepted, fmt.Sprintf(
		"effective capacity resolved from PgWorkloadClass %q: guaranteed %d, burstable %d, weight %d",
		effective.WorkloadClassName, effective.Guaranteed, effective.Burstable, effective.Weight), nil
}

// tenantPhase is a pure function of the conditions, present only so kubectl output has a
// single column to show. Nothing may read it back as input.
func tenantPhase(conditions []metav1.Condition) pgelasticv1alpha1.PgTenantPhase {
	switch {
	case !meta.IsStatusConditionTrue(conditions, pgelasticv1alpha1.ConditionAccepted):
		return pgelasticv1alpha1.PgTenantPhasePending
	case meta.IsStatusConditionTrue(conditions, pgelasticv1alpha1.ConditionReady):
		return pgelasticv1alpha1.PgTenantPhaseReady
	default:
		return pgelasticv1alpha1.PgTenantPhaseBinding
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *PgTenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgTenant{}).
		Watches(&pgelasticv1alpha1.PgElasticPool{}, handler.EnqueueRequestsFromMapFunc(r.tenantsOfPool)).
		Watches(&pgelasticv1alpha1.PgWorkloadClass{}, handler.EnqueueRequestsFromMapFunc(r.tenantsOfWorkloadClass)).
		Named("pgtenant").
		Complete(r)
}

func (r *PgTenantReconciler) tenantsOfPool(ctx context.Context, object client.Object) []reconcile.Request {
	pool, ok := object.(*pgelasticv1alpha1.PgElasticPool)
	if !ok {
		return nil
	}
	tenants := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, tenants,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{index.TenantByPool: pool.Name}); err != nil {
		return nil
	}
	return requestsFor(tenants.Items)
}

// tenantsOfWorkloadClass enqueues the tenants that name the class outright as well as
// every tenant of a pool that defaults to it, because a class edit changes the effective
// capacity of both kinds equally.
func (r *PgTenantReconciler) tenantsOfWorkloadClass(ctx context.Context, object client.Object) []reconcile.Request {
	workloadClass, ok := object.(*pgelasticv1alpha1.PgWorkloadClass)
	if !ok {
		return nil
	}

	named := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, named,
		client.MatchingFields{index.TenantByWorkloadClass: workloadClass.Name}); err != nil {
		return nil
	}
	requests := requestsFor(named.Items)

	pools := &pgelasticv1alpha1.PgElasticPoolList{}
	if err := r.List(ctx, pools,
		client.MatchingFields{index.PoolByDefaultWorkloadClass: workloadClass.Name}); err != nil {
		return requests
	}
	for i := range pools.Items {
		requests = append(requests, r.tenantsOfPool(ctx, &pools.Items[i])...)
	}
	return requests
}

func requestsFor(tenants []pgelasticv1alpha1.PgTenant) []reconcile.Request {
	requests := make([]reconcile.Request, 0, len(tenants))
	for i := range tenants {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: tenants[i].Namespace, Name: tenants[i].Name},
		})
	}
	return requests
}
