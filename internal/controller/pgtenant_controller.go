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
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/metering"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
	"github.com/andrew01234567890/pgelastic/internal/placement"
	"github.com/andrew01234567890/pgelastic/internal/policy"
	"github.com/andrew01234567890/pgelastic/internal/tenantdb"
)

// placementRetryInterval is how long a tenant waits before its unresolved references or
// its missing placement are looked at again. Both are states another actor has to leave,
// so retrying faster only burns API calls.
const placementRetryInterval = 30 * time.Second

// TenantDatabaseFinalizer blocks deletion of a PgTenant until its reclaimPolicy has been
// carried out on the instance hosting it.
//
// It is added before the first CREATE rather than after it, because a database created
// without the finalizer already persisted outlives every record that it exists.
const TenantDatabaseFinalizer = "pgelastic.io/tenant-database"

// PgTenantReconciler resolves a tenant's effective policy, places it on an instance,
// creates the database and role that back it, and publishes what it is observed consuming.
type PgTenantReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// SQL is how the tenant's database is created on the instance it is bound to: as the
	// bootstrap superuser, over that member's Unix socket, through the API server's exec
	// subresource. It is the same port the migration path uses, because a provisioned
	// tenant and a migrated one have to be the same object.
	//
	// A nil port provisions nothing, and a tenant it is asked about is Ready=False saying
	// exactly that rather than Ready=True saying nothing.
	SQL tenantdb.SQL

	// Metering supplies the trailing-window recommenders placement packs on. A nil collector
	// places on declared guarantees alone, which is what a pool with no history has anyway.
	Metering *metering.Collector

	// Rand seeds the power-of-two-choices sampling. Injectable so a test gets the same
	// placement twice; production leaves it nil and gets the global source.
	Rand *rand.Rand

	// APIReader reads the tenant itself straight from the API server. A binding is the one
	// piece of tenant status that is not recomputed from scratch: it is kept because moving
	// a placed tenant costs a live migration. Read from a cache that has not caught up with
	// the previous pass's write, the binding looks absent and the tenant is placed a second
	// time, possibly somewhere else.
	APIReader client.Reader

	// Now is the clock, injectable for the same reason.
	Now func() time.Time

	// ControllerName is this operator's identity. A tenant reaches a PgElasticClass through
	// its pool, and one naming a different controller is left entirely alone.
	ControllerName string
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants/finalizers,verbs=update
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pginstances,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgworkloadclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create

// Reconcile resolves the tenant's effective policy, derives its QoS class from the
// resulting numbers, places it on an instance, creates the database and role there, and
// publishes all of it.
func (r *PgTenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	tenant := &pgelasticv1alpha1.PgTenant{}
	if err := r.reader().Get(ctx, req.NamespacedName, tenant); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// Ahead of the deletion branch, because releasing a finalizer is a write and a tenant
	// this operator never claimed never carried one of ours to release.
	if result, stop, err := unclaimed(ctx, r.ownership(), r.Client, releaseOnly, tenant); stop {
		return result, err
	}
	if !tenant.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, tenant)
	}

	status := pgelasticv1alpha1.PgTenantStatus{
		ObservedGeneration: tenant.Generation,
		Binding:            tenant.Status.Binding,
		Utilization:        tenant.Status.Utilization,
		Throttle:           tenant.Status.Throttle,
		Conditions:         tenant.Status.Conditions,
	}

	resolved, err := r.resolve(ctx, tenant)
	if err != nil {
		return ctrl.Result{}, err
	}

	accepted := resolved.reason == pgelasticv1alpha1.ReasonAccepted
	if accepted {
		status.QoSClass = resolved.effective.QoSClass
		status.Effective = &pgelasticv1alpha1.PgTenantEffectiveLimits{
			Guaranteed:       ptr.To(resolved.effective.Guaranteed),
			Burstable:        ptr.To(resolved.effective.Burstable),
			Weight:           ptr.To(resolved.effective.Weight),
			StatementTimeout: resolved.effective.StatementTimeout,
			TempFileLimit:    resolved.effective.TempFileLimit,
		}
		status.Utilization = r.utilizationOf(tenant, resolved)
	}
	setCondition(&status.Conditions, tenant.Generation, pgelasticv1alpha1.ConditionAccepted,
		conditionStatus(accepted), resolved.reason, resolved.message)

	placed, err := r.place(ctx, tenant, resolved, &status)
	if err != nil {
		return ctrl.Result{}, err
	}
	if placed.rebound {
		status.Binding = placed.binding
	}

	if err := r.provision(ctx, tenant, resolved, placed.host, &status); err != nil {
		return ctrl.Result{}, err
	}

	status.Phase = tenantPhase(status.Conditions)

	if !equality.Semantic.DeepEqual(tenant.Status, status) {
		tenant.Status = status
		if err := r.Status().Update(ctx, tenant); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: placementRetryInterval}, nil
}

// resolution is the outcome of walking a tenant's references.
type resolution struct {
	pool      *pgelasticv1alpha1.PgElasticPool
	effective policy.Effective
	reason    string
	message   string
}

// resolve walks the pool and workload class references and returns the effective policy
// together with the Accepted reason and message the walk produced. A reference that does
// not resolve is reported, never treated as an error: the referent may simply not have
// been created yet.
func (r *PgTenantReconciler) resolve(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
) (resolution, error) {
	resolver := policy.Resolver{Reader: r.Client}

	pool := &pgelasticv1alpha1.PgElasticPool{}
	poolKey := types.NamespacedName{Namespace: tenant.Namespace, Name: tenant.Spec.PoolRef.Name}
	if err := r.Get(ctx, poolKey, pool); err != nil {
		if !apierrors.IsNotFound(err) {
			return resolution{}, err
		}
		return resolution{reason: pgelasticv1alpha1.ReasonPending, message: fmt.Sprintf(
			"PgElasticPool %q does not exist in namespace %q", poolKey.Name, poolKey.Namespace)}, nil
	}

	elasticClass, err := resolver.ElasticClassFor(ctx, pool)
	if apierrors.IsNotFound(err) {
		elasticClass = nil
	} else if err != nil {
		return resolution{}, err
	}

	workloadClassName, err := resolver.WorkloadClassNameFor(ctx, tenant, pool, elasticClass)
	if errors.Is(err, policy.ErrNoWorkloadClass) {
		return resolution{pool: pool, reason: pgelasticv1alpha1.ReasonPending, message: err.Error()}, nil
	} else if err != nil {
		return resolution{}, err
	}

	workloadClass := &pgelasticv1alpha1.PgWorkloadClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: workloadClassName}, workloadClass); err != nil {
		if !apierrors.IsNotFound(err) {
			return resolution{}, err
		}
		return resolution{pool: pool, reason: pgelasticv1alpha1.ReasonPending, message: fmt.Sprintf(
			"PgWorkloadClass %q does not exist", workloadClassName)}, nil
	}

	effective := policy.EffectiveFor(tenant, workloadClass)
	return resolution{
		pool:      pool,
		effective: effective,
		reason:    pgelasticv1alpha1.ReasonAccepted,
		message: fmt.Sprintf(
			"effective capacity resolved from PgWorkloadClass %q: guaranteed %d, burstable %d, weight %d",
			effective.WorkloadClassName, effective.Guaranteed, effective.Burstable, effective.Weight),
	}, nil
}

// placed is where a tenant ended up: the instance it is on, the binding to publish, and
// whether that binding is new.
type placed struct {
	host    *pgelasticv1alpha1.PgInstance
	binding *pgelasticv1alpha1.PgTenantBinding
	rebound bool
}

// place binds the tenant to an instance and sets the Bound condition.
//
// It says nothing about Ready. Where a tenant belongs and whether its database exists are
// different questions, and answering the second with the first is what let a tenant with
// no database at all report itself as being served.
//
// A tenant already bound to an instance that can still hold it stays where it is. Every
// rebinding costs a live migration, so placement here only ever answers the question "where
// does an unplaced tenant go"; moving a placed one is the rebalancer's decision and arrives
// as a PgTenantMigration.
func (r *PgTenantReconciler) place(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
	resolved resolution,
	status *pgelasticv1alpha1.PgTenantStatus,
) (placed, error) {
	if resolved.reason != pgelasticv1alpha1.ReasonAccepted {
		r.unbound(status, tenant.Generation, pgelasticv1alpha1.ReasonPending,
			"the tenant's policy has not resolved, so no instance can be chosen for it")
		return placed{}, nil
	}

	instances, err := r.instancesOf(ctx, resolved.pool)
	if err != nil {
		return placed{}, err
	}

	if existing := placement.BoundInstanceFor(tenant); existing != "" {
		host, ok := instanceNamed(instances, existing)
		if !ok {
			// The tenant's database is on the instance it is bound to and nowhere else.
			// Falling through to ordinary placement here would repoint the binding at an
			// instance that has never held this tenant's data, which reads to the client
			// as an empty database and leaves the real one stranded. A binding is only
			// ever moved by a migration that copies the data first.
			r.unbound(status, tenant.Generation, pgelasticv1alpha1.ReasonInstanceMissing,
				fmt.Sprintf("PgInstance %q holds this tenant's database and is not present "+
					"in pool %q; the binding is kept and no replacement is chosen, because "+
					"rebinding without a migration would serve an empty database",
					existing, resolved.pool.Name))
			return placed{binding: tenant.Status.Binding}, nil
		}
		r.boundTo(status, tenant.Generation, host.Name)
		return placed{host: host, binding: tenant.Status.Binding}, nil
	}

	candidates, err := r.candidatesFor(ctx, resolved, instances, tenant.Name)
	if err != nil {
		return placed{}, err
	}

	assignment, refusal := placement.Admit(
		r.tenantDemand(tenant, resolved), candidates, placement.PolicyFor(resolved.pool), r.Rand)
	if refusal != nil {
		r.unbound(status, tenant.Generation, pgelasticv1alpha1.ReasonUnplaceable,
			fmt.Sprintf("%s: %s", refusal.Reason, refusal.Message))
		return placed{}, nil
	}

	host, _ := instanceNamed(instances, assignment.Instance)
	r.boundTo(status, tenant.Generation, assignment.Instance)
	return placed{
		host: host,
		binding: &pgelasticv1alpha1.PgTenantBinding{
			InstanceRef: &corev1.LocalObjectReference{Name: assignment.Instance},
			BoundAt:     &metav1.Time{Time: r.now()},
		},
		rebound: true,
	}, nil
}

// provision creates the tenant's role and database on the instance it was placed on, and
// sets Ready from what PostgreSQL's catalog says afterwards rather than from the fact that
// a placement decision was reached.
//
// A failure is reported on the condition and not returned. The caller would only requeue,
// and a tenant whose CREATE DATABASE is being refused needs an operator to read the reason,
// not a faster retry loop.
func (r *PgTenantReconciler) provision(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
	resolved resolution,
	host *pgelasticv1alpha1.PgInstance,
	status *pgelasticv1alpha1.PgTenantStatus,
) error {
	if host == nil {
		return nil
	}
	database := tenant.Spec.DatabaseName
	if host.Status.Phase != pgelasticv1alpha1.InstancePhaseReady {
		r.notServing(status, tenant.Generation, pgelasticv1alpha1.ReasonPending,
			fmt.Sprintf("PgInstance %q is not ready yet, so database %q cannot be created on it",
				host.Name, database))
		return nil
	}
	if r.SQL == nil {
		r.notServing(status, tenant.Generation, tenantdb.ReasonProvisioning,
			fmt.Sprintf("no PostgreSQL transport is configured, so database %q has not been created "+
				"on PgInstance %q", database, host.Name))
		return nil
	}

	if err := r.holdForReclaim(ctx, tenant); err != nil {
		return err
	}

	// Minted before the role is created, because the role is created with it. A tenant whose
	// credential cannot be written is left unprovisioned rather than provisioned passwordless:
	// a role the proxy cannot authenticate as is a tenant nobody can reach, and it would look
	// exactly like a working one from the outside.
	credential, err := r.ensureBackendCredential(ctx, tenant, scramIterationsOf(resolved))
	if err != nil {
		return err
	}
	spec := tenantSpecOf(tenant, connectionLimitOf(resolved))
	spec.Verifier = credential.Verifier
	state, err := tenantdb.Ensure(ctx, r.SQL, tenantEndpoint(tenant, host.Name), spec)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Could not provision the tenant's database",
			"database", spec.Database, "instance", host.Name)
		r.notServing(status, tenant.Generation, tenantdb.ReasonProvisioningFailed, err.Error())
		return nil
	}

	binding := status.Binding.DeepCopy()
	if binding == nil {
		binding = &pgelasticv1alpha1.PgTenantBinding{
			InstanceRef: &corev1.LocalObjectReference{Name: host.Name},
			BoundAt:     &metav1.Time{Time: r.now()},
		}
	}
	binding.DatabaseOID = ptr.To(state.DatabaseOID)
	status.Binding = binding

	setCondition(&status.Conditions, tenant.Generation, pgelasticv1alpha1.ConditionReady,
		metav1.ConditionTrue, pgelasticv1alpha1.ReasonReady, fmt.Sprintf(
			"PgInstance %q is serving database %q (oid %d), owned by role %q with an in-database "+
				"connection limit of %d", host.Name, database, state.DatabaseOID,
			spec.Owner, state.ConnectionLimit))
	return nil
}

// holdForReclaim persists the finalizer before the first CREATE. A finalizer added after
// the database exists leaves a window in which deleting the tenant deletes the only record
// that there is anything to reclaim.
func (r *PgTenantReconciler) holdForReclaim(ctx context.Context, tenant *pgelasticv1alpha1.PgTenant) error {
	if !controllerutil.AddFinalizer(tenant, TenantDatabaseFinalizer) {
		return nil
	}
	return r.Update(ctx, tenant)
}

// finalize carries out the tenant's reclaimPolicy and only then releases the finalizer.
//
// The order is the whole point: the finalizer is what guarantees the policy runs at all,
// so removing it before the action has actually completed turns Delete into Retain and
// hides the difference.
func (r *PgTenantReconciler) finalize(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(tenant, TenantDatabaseFinalizer) {
		return ctrl.Result{}, nil
	}
	if err := r.reclaim(ctx, tenant); err != nil {
		if reportErr := r.reportReclaimFailure(ctx, tenant, err); reportErr != nil {
			return ctrl.Result{}, reportErr
		}
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(tenant, TenantDatabaseFinalizer)
	return ctrl.Result{}, r.Update(ctx, tenant)
}

// reclaim performs the action the tenant's reclaimPolicy names.
//
// Retain is the default and is a no-op by design: dropping a tenant's database is
// irreversible and nothing in the system can undo it, so the destructive branch is the one
// that has to be asked for.
func (r *PgTenantReconciler) reclaim(ctx context.Context, tenant *pgelasticv1alpha1.PgTenant) error {
	if ptr.Deref(tenant.Spec.ReclaimPolicy, pgelasticv1alpha1.ReclaimRetain) !=
		pgelasticv1alpha1.ReclaimDelete {
		return nil
	}
	bound := placement.BoundInstanceFor(tenant)
	if bound == "" {
		return nil
	}

	host := &pgelasticv1alpha1.PgInstance{}
	key := types.NamespacedName{Namespace: tenant.Namespace, Name: bound}
	if err := r.Get(ctx, key, host); err != nil {
		// A database cannot outlive the instance that held it, and PgInstance's own drain
		// finalizer is what stops one going away under a bound tenant. Holding this tenant
		// for an instance that is already gone would strand it forever with nothing left
		// to drop.
		return client.IgnoreNotFound(err)
	}
	if r.SQL == nil {
		return fmt.Errorf(
			"reclaimPolicy Delete has to drop database %q on PgInstance %q and no PostgreSQL "+
				"transport is configured", tenant.Spec.DatabaseName, bound)
	}
	return tenantdb.Drop(ctx, r.SQL, tenantEndpoint(tenant, bound),
		tenantSpecOf(tenant, tenantdb.NoConnectionLimit))
}

// reportReclaimFailure publishes why a deletion is not completing, because a tenant stuck
// on its finalizer is otherwise indistinguishable from one the controller has forgotten.
func (r *PgTenantReconciler) reportReclaimFailure(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
	cause error,
) error {
	status := *tenant.Status.DeepCopy()
	status.Phase = pgelasticv1alpha1.PgTenantPhaseTerminating
	setCondition(&status.Conditions, tenant.Generation, pgelasticv1alpha1.ConditionReady,
		metav1.ConditionFalse, tenantdb.ReasonReclaimFailed, cause.Error())
	if equality.Semantic.DeepEqual(tenant.Status, status) {
		return nil
	}
	tenant.Status = status
	return r.Status().Update(ctx, tenant)
}

// tenantEndpoint addresses the tenant's database on the instance hosting it. No member is
// named, which resolves to that instance's current primary: the only member a CREATE can
// be issued on.
func tenantEndpoint(tenant *pgelasticv1alpha1.PgTenant, instance string) tenantdb.Endpoint {
	return tenantdb.Endpoint{
		Namespace: tenant.Namespace,
		Instance:  instance,
		Database:  tenant.Spec.DatabaseName,
	}
}

func tenantSpecOf(tenant *pgelasticv1alpha1.PgTenant, connectionLimit int32) tenantdb.Spec {
	return tenantdb.Spec{
		Database: tenant.Spec.DatabaseName,
		// Derived from the tenant's identity rather than taken from spec.owner. Roles are
		// cluster-global, so two tenants that chose the same owner would otherwise share one -
		// and now that these roles carry credentials, sharing one is a merge of two identities
		// rather than an untidy privilege union. spec.owner survives as the readable prefix.
		Owner:           migration.BackendRoleName(tenant.Namespace, tenant.Name),
		ConnectionLimit: connectionLimit,
	}
}

// connectionLimitOf leaves the tenant's role uncapped, deliberately.
//
// It used to mirror the effective burstable ceiling as an in-database backstop, which was
// harmless while nothing ever logged in as that role. Now that the proxy authenticates as it,
// every backend the fleet opens counts against rolconnlimit - and N replicas each entitled to
// hold up to burstable would breach a limit of burstable by a factor of N, with
// "too many connections for role" arriving at whichever client happened to be last.
//
// The proxy's own ledger is the ceiling that means anything: it is fleet-wide, it is the number
// the capacity model is built on, and it is enforced before a backend is opened rather than
// after. A per-role limit that is only correct for a single-replica fleet is not a backstop,
// it is a second limiter that disagrees with the first.
func connectionLimitOf(_ resolution) int32 {
	return tenantdb.NoConnectionLimit
}

// candidatesFor charges each instance with the tenants already bound to it, so that an
// admission sees the capacity that is really left rather than the capacity the instance
// was built with.
func (r *PgTenantReconciler) candidatesFor(
	ctx context.Context,
	resolved resolution,
	instances []pgelasticv1alpha1.PgInstance,
	excluding string,
) ([]placement.Instance, error) {
	targets := make([]placement.Instance, 0, len(instances))
	for i := range instances {
		targets = append(targets, placement.InstanceFrom(&instances[i]))
	}

	siblings := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, siblings,
		client.InNamespace(resolved.pool.Namespace),
		client.MatchingFields{index.TenantByPool: resolved.pool.Name}); err != nil {
		return nil, err
	}

	bound := make([]placement.Tenant, 0, len(siblings.Items))
	for i := range siblings.Items {
		sibling := &siblings.Items[i]
		if sibling.Name == excluding {
			continue
		}
		host := placement.BoundInstanceFor(sibling)
		if host == "" {
			continue
		}
		bound = append(bound, placement.Tenant{
			Name:          sibling.Name,
			BoundInstance: host,
			AntiAffinity:  placement.AntiAffinityFor(sibling),
			Demand: placement.Demand{
				GuaranteedConnections: effectiveGuaranteeOf(sibling),
				ObservedConnections:   r.observedConnectionsOf(sibling, resolved.pool),
			},
		})
	}

	return placement.SeedBinsWithBoundTenants(targets, bound), nil
}

// tenantDemand is what the tenant needs, packed on the pool's trailing-window percentile
// rather than on its declared ceiling. A ceiling is what a tenant is allowed; a percentile
// is what it does.
func (r *PgTenantReconciler) tenantDemand(
	tenant *pgelasticv1alpha1.PgTenant,
	resolved resolution,
) placement.Tenant {
	return placement.Tenant{
		Name:           tenant.Name,
		AntiAffinity:   placement.AntiAffinityFor(tenant),
		PinnedInstance: placement.PinnedInstanceFor(tenant),
		BoundInstance:  placement.BoundInstanceFor(tenant),
		Demand: placement.Demand{
			GuaranteedConnections: resolved.effective.Guaranteed,
			ObservedConnections:   r.observedConnectionsOf(tenant, resolved.pool),
			StorageBytes:          storageBytesOf(tenant),
		},
	}
}

// observedConnectionsOf reads the packing statistic out of the metering store, falling back
// to what the tenant last published on its own status. An unmetered tenant contributes
// nothing observed, and its guarantee alone then decides where it can go.
func (r *PgTenantReconciler) observedConnectionsOf(
	tenant *pgelasticv1alpha1.PgTenant,
	pool *pgelasticv1alpha1.PgElasticPool,
) int32 {
	if r.Metering != nil {
		quantile := placement.QuantileFor(placement.PolicyFor(pool).PackOn)
		if value, ok := r.Metering.Store.Quantile(meteringKeyOf(tenant), quantile, r.now()); ok {
			return int32(math.Ceil(value))
		}
	}
	utilization := tenant.Status.Utilization
	if utilization == nil || utilization.BackendConnections == nil ||
		utilization.BackendConnections.P95_7d == nil {
		return 0
	}
	return *utilization.BackendConnections.P95_7d
}

// utilizationOf publishes the tenant's trailing-window numbers on its own object. This is
// where per-tenant history is allowed to live: as ~200 fields on ~200 objects rather than as
// ~200 label values multiplying every metric in the process.
func (r *PgTenantReconciler) utilizationOf(
	tenant *pgelasticv1alpha1.PgTenant,
	resolved resolution,
) *pgelasticv1alpha1.PgTenantUtilization {
	if r.Metering == nil {
		return tenant.Status.Utilization
	}
	observation, ok := r.Metering.Store.Observation(meteringKeyOf(tenant), r.now())
	if !ok {
		return tenant.Status.Utilization
	}

	hot := placement.DefaultHotThresholdPercent
	if resolved.pool != nil && resolved.pool.Spec.Rebalancing != nil &&
		resolved.pool.Spec.Rebalancing.HotTenantUtilizationThresholdPercent != nil {
		hot = *resolved.pool.Spec.Rebalancing.HotTenantUtilizationThresholdPercent
	}
	cold := resolved.effective.Burstable > 0 &&
		observation.Peak*100 < float64(resolved.effective.Burstable)*float64(hot)

	return &pgelasticv1alpha1.PgTenantUtilization{
		BackendConnections: &pgelasticv1alpha1.PgTenantConnectionUtilization{
			Current: ptr.To(int32(observation.Current)),
			P95_7d:  ptr.To(int32(math.Ceil(observation.P95))),
			Peak_7d: ptr.To(int32(observation.Peak)),
		},
		StorageBytes: ptr.To(observation.StorageBytes),
		IsCold:       ptr.To(cold),
	}
}

func (r *PgTenantReconciler) boundTo(
	status *pgelasticv1alpha1.PgTenantStatus,
	generation int64,
	instance string,
) {
	setCondition(&status.Conditions, generation, pgelasticv1alpha1.ConditionBound,
		metav1.ConditionTrue, pgelasticv1alpha1.ReasonPlaced,
		fmt.Sprintf("bound to PgInstance %q", instance))
}

func (r *PgTenantReconciler) unbound(
	status *pgelasticv1alpha1.PgTenantStatus,
	generation int64,
	reason, message string,
) {
	setCondition(&status.Conditions, generation, pgelasticv1alpha1.ConditionBound,
		metav1.ConditionFalse, reason, message)
	r.notServing(status, generation, pgelasticv1alpha1.ReasonPending,
		"the tenant database is not serving until the tenant is bound to an instance")
}

// notServing is the only way Ready is set to False, so that every reason a tenant is not
// being served passes through one place.
func (r *PgTenantReconciler) notServing(
	status *pgelasticv1alpha1.PgTenantStatus,
	generation int64,
	reason, message string,
) {
	setCondition(&status.Conditions, generation, pgelasticv1alpha1.ConditionReady,
		metav1.ConditionFalse, reason, message)
}

func (r *PgTenantReconciler) instancesOf(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) ([]pgelasticv1alpha1.PgInstance, error) {
	instances := &pgelasticv1alpha1.PgInstanceList{}
	if err := r.List(ctx, instances, client.InNamespace(pool.Namespace)); err != nil {
		return nil, err
	}
	members := make([]pgelasticv1alpha1.PgInstance, 0, len(instances.Items))
	for i := range instances.Items {
		if instances.Items[i].Spec.PoolRef.Name == pool.Name {
			members = append(members, instances.Items[i])
		}
	}
	slices.SortFunc(members, func(a, b pgelasticv1alpha1.PgInstance) int {
		return strings.Compare(a.Name, b.Name)
	})
	return members, nil
}

// reader is the API server for the tenant object itself, and the cache for everything else.
func (r *PgTenantReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *PgTenantReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func instanceNamed(
	instances []pgelasticv1alpha1.PgInstance,
	name string,
) (*pgelasticv1alpha1.PgInstance, bool) {
	for i := range instances {
		if instances[i].Name == name {
			return &instances[i], true
		}
	}
	return nil, false
}

func effectiveGuaranteeOf(tenant *pgelasticv1alpha1.PgTenant) int32 {
	if tenant.Status.Effective == nil || tenant.Status.Effective.Guaranteed == nil {
		return 0
	}
	return *tenant.Status.Effective.Guaranteed
}

func storageBytesOf(tenant *pgelasticv1alpha1.PgTenant) int64 {
	if tenant.Status.Utilization == nil || tenant.Status.Utilization.StorageBytes == nil {
		return 0
	}
	return *tenant.Status.Utilization.StorageBytes
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
func (r *PgTenantReconciler) ownership() ownership.Resolver {
	return ownership.Resolver{Reader: r.Client, ControllerName: r.ControllerName}
}

func (r *PgTenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgTenant{}).
		Watches(&pgelasticv1alpha1.PgElasticPool{}, handler.EnqueueRequestsFromMapFunc(r.tenantsOfPool)).
		Watches(&pgelasticv1alpha1.PgInstance{}, handler.EnqueueRequestsFromMapFunc(r.tenantsOfInstance)).
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

// tenantsOfInstance wakes the tenants of an instance's pool, because an instance becoming
// ready or being cordoned changes where an unplaced tenant may go and whether a placed one
// is serving.
func (r *PgTenantReconciler) tenantsOfInstance(ctx context.Context, object client.Object) []reconcile.Request {
	instance, ok := object.(*pgelasticv1alpha1.PgInstance)
	if !ok || instance.Spec.PoolRef.Name == "" {
		return nil
	}
	tenants := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, tenants,
		client.InNamespace(instance.Namespace),
		client.MatchingFields{index.TenantByPool: instance.Spec.PoolRef.Name}); err != nil {
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
