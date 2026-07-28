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
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

// PgWorkloadClassReconciler reconciles a PgWorkloadClass object
type PgWorkloadClassReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgworkloadclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgworkloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgworkloadclasses/finalizers,verbs=update

// Reconcile validates a workload class against itself and against the cluster-wide
// single-global rule, and republishes how many tenants currently resolve to it.
func (r *PgWorkloadClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workloadClass := &pgelasticv1alpha1.PgWorkloadClass{}
	if err := r.Get(ctx, req.NamespacedName, workloadClass); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !workloadClass.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	status := pgelasticv1alpha1.PgWorkloadClassStatus{
		ObservedGeneration: workloadClass.Generation,
		Conditions:         workloadClass.Status.Conditions,
	}

	tenantCount, err := r.countTenants(ctx, workloadClass.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	status.TenantCount = tenantCount

	reason, message := pgelasticv1alpha1.ReasonAccepted, "workload class is self-consistent"
	if problems := policy.WorkloadClassProblems(workloadClass); len(problems) > 0 {
		reason, message = pgelasticv1alpha1.ReasonInvalidSpec, policy.JoinProblems(problems)
	} else {
		conflict, err := r.globalConflict(ctx, workloadClass)
		if err != nil {
			return ctrl.Result{}, err
		}
		if conflict != "" {
			reason, message = pgelasticv1alpha1.ReasonInvalidSpec, conflict
		}
	}

	setCondition(&status.Conditions, workloadClass.Generation, pgelasticv1alpha1.ConditionAccepted,
		conditionStatus(reason == pgelasticv1alpha1.ReasonAccepted), reason, message)

	if equality.Semantic.DeepEqual(workloadClass.Status, status) {
		return ctrl.Result{}, nil
	}
	workloadClass.Status = status
	return ctrl.Result{}, r.Status().Update(ctx, workloadClass)
}

// globalConflict reports the cluster-wide single-global violation this class is part of,
// or the empty string when there is none. The rule spans objects so the webhook is what
// enforces it; the reconciler reports it as well because a class that predates the
// webhook, or was written while it was unavailable, would otherwise look accepted while
// silently competing to be the cluster default.
func (r *PgWorkloadClassReconciler) globalConflict(
	ctx context.Context,
	workloadClass *pgelasticv1alpha1.PgWorkloadClass,
) (string, error) {
	if workloadClass.Spec.Global == nil || !*workloadClass.Spec.Global {
		return "", nil
	}
	globals, err := policy.Resolver{Reader: r.Client}.GlobalWorkloadClassNames(ctx)
	if err != nil {
		return "", err
	}
	others := slices.DeleteFunc(globals, func(name string) bool { return name == workloadClass.Name })
	if len(others) == 0 {
		return "", nil
	}
	return fmt.Sprintf("at most one PgWorkloadClass may set global; %s also does", strings.Join(others, ", ")), nil
}

// countTenants counts every tenant that resolves to the class, whether by naming it or
// by inheriting it as a default. The named ones come from a field index; the inheriting
// ones cannot, because no index can follow a tenant's pool reference through to that
// pool's default, so they are counted by walking the pools that default to this class.
func (r *PgWorkloadClassReconciler) countTenants(ctx context.Context, className string) (int32, error) {
	named := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, named, client.MatchingFields{index.TenantByWorkloadClass: className}); err != nil {
		return 0, err
	}
	total := int32(len(named.Items))

	pools := &pgelasticv1alpha1.PgElasticPoolList{}
	if err := r.List(ctx, pools); err != nil {
		return 0, err
	}
	resolver := policy.Resolver{Reader: r.Client}
	for i := range pools.Items {
		pool := &pools.Items[i]
		defaultName, err := r.defaultWorkloadClassName(ctx, resolver, pool)
		if err != nil {
			return 0, err
		}
		if defaultName != className {
			continue
		}
		inheriting, err := r.inheritingTenants(ctx, pool)
		if err != nil {
			return 0, err
		}
		total += inheriting
	}
	return total, nil
}

func (r *PgWorkloadClassReconciler) defaultWorkloadClassName(
	ctx context.Context,
	resolver policy.Resolver,
	pool *pgelasticv1alpha1.PgElasticPool,
) (string, error) {
	elasticClass, err := resolver.ElasticClassFor(ctx, pool)
	if apierrors.IsNotFound(err) {
		elasticClass = nil
	} else if err != nil {
		return "", err
	}
	name, err := resolver.DefaultWorkloadClassNameFor(ctx, pool, elasticClass)
	if errors.Is(err, policy.ErrNoWorkloadClass) {
		return "", nil
	}
	return name, err
}

func (r *PgWorkloadClassReconciler) inheritingTenants(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) (int32, error) {
	tenants := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, tenants,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{index.TenantByPool: pool.Name}); err != nil {
		return 0, err
	}
	var count int32
	for i := range tenants.Items {
		if name := tenants.Items[i].Spec.WorkloadClassName; name == nil || *name == "" {
			count++
		}
	}
	return count, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PgWorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgWorkloadClass{}).
		Watches(&pgelasticv1alpha1.PgTenant{}, handler.EnqueueRequestsFromMapFunc(r.classesForTenant)).
		Watches(&pgelasticv1alpha1.PgElasticPool{}, handler.EnqueueRequestsFromMapFunc(r.classesForPool)).
		Named("pgworkloadclass").
		Complete(r)
}

func (r *PgWorkloadClassReconciler) classesForTenant(ctx context.Context, object client.Object) []reconcile.Request {
	tenant, ok := object.(*pgelasticv1alpha1.PgTenant)
	if !ok {
		return nil
	}
	resolver := policy.Resolver{Reader: r.Client}
	pool := &pgelasticv1alpha1.PgElasticPool{}
	key := types.NamespacedName{Namespace: tenant.Namespace, Name: tenant.Spec.PoolRef.Name}
	if err := r.Get(ctx, key, pool); err != nil {
		pool = nil
	}
	var elasticClass *pgelasticv1alpha1.PgElasticClass
	if pool != nil {
		if resolved, err := resolver.ElasticClassFor(ctx, pool); err == nil {
			elasticClass = resolved
		}
	}
	name, err := resolver.WorkloadClassNameFor(ctx, tenant, pool, elasticClass)
	if err != nil {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
}

func (r *PgWorkloadClassReconciler) classesForPool(ctx context.Context, object client.Object) []reconcile.Request {
	pool, ok := object.(*pgelasticv1alpha1.PgElasticPool)
	if !ok {
		return nil
	}
	name, err := r.defaultWorkloadClassName(ctx, policy.Resolver{Reader: r.Client}, pool)
	if err != nil || name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
}
