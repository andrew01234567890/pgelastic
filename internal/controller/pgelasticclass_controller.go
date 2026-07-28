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
	"slices"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

// DefaultControllerName is the controllerName a PgElasticClass must carry for this
// controller to claim it.
const DefaultControllerName = "pgelastic.io/elastic-pool-controller"

// PoolsExistFinalizer keeps a PgElasticClass alive while pools still depend on the
// policy it publishes. Deleting a class out from under a live pool would leave the pool
// with defaults nobody can read and no way to recompute its own ledger.
const PoolsExistFinalizer = "pgelastic.io/pools-exist"

// supportedClassFeatures is what this controller genuinely implements today. It is
// published verbatim in status so a pool author can detect a capability gap from the
// API rather than from a rollout that silently does less than the spec asked for.
var supportedClassFeatures = []pgelasticv1alpha1.ClassFeature{
	"BidirectionalNamespaceConsent",
	"DerivedQoSClass",
	"GuaranteedReservationLedger",
	"WorkloadClassDefaulting",
}

// PgElasticClassReconciler reconciles a PgElasticClass object
type PgElasticClassReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// ControllerName is this controller's identity. A class naming a different
	// controller belongs to someone else and is left entirely alone.
	ControllerName string
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticclasses/finalizers,verbs=update

// Reconcile claims a PgElasticClass whose controllerName matches this controller,
// publishes what the controller supports and how many pools depend on the class, and
// holds the class open while any of them do.
func (r *PgElasticClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	elasticClass := &pgelasticv1alpha1.PgElasticClass{}
	if err := r.Get(ctx, req.NamespacedName, elasticClass); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// A class naming another controller is not an error and never becomes one: it is
	// simply not ours, so it gets no conditions, no finalizer and no status. This is the
	// GatewayClass rule, and it is what lets two controllers share one cluster.
	if elasticClass.Spec.ControllerName != r.controllerName() {
		logf.FromContext(ctx).V(1).Info("Ignoring PgElasticClass claimed by another controller",
			"controllerName", elasticClass.Spec.ControllerName)
		return ctrl.Result{}, nil
	}

	poolCount, err := r.countPools(ctx, elasticClass.Name)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !elasticClass.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, elasticClass, poolCount)
	}

	if controllerutil.AddFinalizer(elasticClass, PoolsExistFinalizer) {
		if err := r.Update(ctx, elasticClass); err != nil {
			return ctrl.Result{}, err
		}
	}

	status := pgelasticv1alpha1.PgElasticClassStatus{
		ObservedGeneration: elasticClass.Generation,
		PoolCount:          poolCount,
		SupportedFeatures:  slices.Clone(supportedClassFeatures),
		Conditions:         elasticClass.Status.Conditions,
	}

	reason, message := pgelasticv1alpha1.ReasonAccepted, "class is claimed by "+r.controllerName()
	if problems := policy.ElasticClassProblems(elasticClass); len(problems) > 0 {
		reason, message = pgelasticv1alpha1.ReasonInvalidSpec, policy.JoinProblems(problems)
	}
	accepted := reason == pgelasticv1alpha1.ReasonAccepted

	setCondition(&status.Conditions, elasticClass.Generation, pgelasticv1alpha1.ConditionAccepted,
		conditionStatus(accepted), reason, message)
	if accepted {
		setCondition(&status.Conditions, elasticClass.Generation, pgelasticv1alpha1.ConditionReady,
			metav1.ConditionTrue, pgelasticv1alpha1.ReasonReady,
			fmt.Sprintf("policy is available to %d pool(s)", poolCount))
	} else {
		setCondition(&status.Conditions, elasticClass.Generation, pgelasticv1alpha1.ConditionReady,
			metav1.ConditionFalse, pgelasticv1alpha1.ReasonInvalidSpec, message)
	}

	return ctrl.Result{}, r.writeStatus(ctx, elasticClass, status)
}

// reconcileDeletion releases the finalizer only once nothing depends on the class.
func (r *PgElasticClassReconciler) reconcileDeletion(
	ctx context.Context,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
	poolCount int32,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(elasticClass, PoolsExistFinalizer) {
		return ctrl.Result{}, nil
	}

	if poolCount > 0 {
		status := pgelasticv1alpha1.PgElasticClassStatus{
			ObservedGeneration: elasticClass.Generation,
			PoolCount:          poolCount,
			SupportedFeatures:  slices.Clone(supportedClassFeatures),
			Conditions:         elasticClass.Status.Conditions,
		}
		setCondition(&status.Conditions, elasticClass.Generation, pgelasticv1alpha1.ConditionReady,
			metav1.ConditionFalse, pgelasticv1alpha1.ReasonPending,
			fmt.Sprintf("deletion is held by the %s finalizer: %d pool(s) still reference this class",
				PoolsExistFinalizer, poolCount))
		return ctrl.Result{}, r.writeStatus(ctx, elasticClass, status)
	}

	controllerutil.RemoveFinalizer(elasticClass, PoolsExistFinalizer)
	return ctrl.Result{}, r.Update(ctx, elasticClass)
}

func (r *PgElasticClassReconciler) writeStatus(
	ctx context.Context,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
	status pgelasticv1alpha1.PgElasticClassStatus,
) error {
	if equality.Semantic.DeepEqual(elasticClass.Status, status) {
		return nil
	}
	elasticClass.Status = status
	return r.Status().Update(ctx, elasticClass)
}

func (r *PgElasticClassReconciler) countPools(ctx context.Context, className string) (int32, error) {
	pools := &pgelasticv1alpha1.PgElasticPoolList{}
	if err := r.List(ctx, pools, client.MatchingFields{index.PoolByElasticClass: className}); err != nil {
		return 0, err
	}
	return int32(len(pools.Items)), nil
}

func (r *PgElasticClassReconciler) controllerName() string {
	if r.ControllerName == "" {
		return DefaultControllerName
	}
	return r.ControllerName
}

// SetupWithManager sets up the controller with the Manager.
func (r *PgElasticClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgElasticClass{}).
		Watches(&pgelasticv1alpha1.PgElasticPool{}, handler.EnqueueRequestsFromMapFunc(elasticClassForPool)).
		Named("pgelasticclass").
		Complete(r)
}

func elasticClassForPool(_ context.Context, object client.Object) []reconcile.Request {
	pool, ok := object.(*pgelasticv1alpha1.PgElasticPool)
	if !ok || pool.Spec.ClassRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pool.Spec.ClassRef.Name}}}
}
