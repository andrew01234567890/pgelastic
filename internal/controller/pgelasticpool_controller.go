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
	"maps"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/autoscale"
	"github.com/andrew01234567890/pgelastic/internal/metering"
	migrationpkg "github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
	"github.com/andrew01234567890/pgelastic/internal/placement"
	"github.com/andrew01234567890/pgelastic/internal/policy"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
	"github.com/andrew01234567890/pgelastic/internal/tracing"
)

// poolResyncInterval paces the planner. Placement and autoscaling read trailing windows
// measured in days, so nothing is gained by recomputing more often than the shortest
// stabilization window can react to.
const poolResyncInterval = time.Minute

// proxyConvergenceInterval paces the wait for the fleet to report which configuration it is
// serving. It is short because it is the only thing being waited for.
const proxyConvergenceInterval = 3 * time.Second

// migrationRateWindow is how far back migrations are counted when the pool declares no
// migration windows of its own, so the per-window budget still means something.
const migrationRateWindow = 24 * time.Hour

// Event reasons emitted by the planner. They are the same strings the plan publishes in
// status, so an Event and a status entry about one decision are greppable together.
const (
	eventAutoscalePlan    = "AutoscalePlan"
	eventActionExecuted   = "AutoscaleActionExecuted"
	eventActionRefused    = "AutoscaleActionRefused"
	eventMigrationEmitted = "TenantMigrationEmitted"
	eventDrainStalled     = "InstanceDrainStalled"
)

// PgElasticPoolReconciler publishes the pool's reservation ledger and its capacity plan.
//
// It computes the whole plan on every pass and executes at most one action from it, chosen
// by the guardrails in internal/autoscale. In the default Recommend mode the only thing it
// will ever apply is a volume expansion.
type PgElasticPoolReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// Metering is the trailing-window store both this controller and the tenant controller
	// read their recommenders out of. A nil collector means the pool is unmetered, which the
	// planner treats as stale rather than as quiet.
	Metering *metering.Collector

	// APIReader reads the pool itself straight from the API server, bypassing the informer
	// cache. The plan carries facts that exist only across passes — when each action class
	// last executed, how long a consolidation candidate has been one — and it is rewritten
	// wholesale every pass. Read from a cache that has not yet caught up with the previous
	// pass's write, those facts are silently dropped and never come back. Everything else
	// this controller reads is cached, because everything else is recomputed from scratch.
	APIReader client.Reader

	// ProxyImage carries the proxy binary the fleet runs. Empty falls back to the
	// PGELASTIC_PROXY_IMAGE environment variable and then to the development tag.
	ProxyImage string

	// Now is the clock, injectable so a test can drive a dwell time without waiting a day.
	Now func() time.Time

	// ControllerName is this operator's identity. A pool whose PgElasticClass names a
	// different controller belongs to that controller and is left entirely alone.
	ControllerName string
}

// envProxyImage names the proxy image, so a deployment can pin it without rebuilding the
// operator.
const envProxyImage = "PGELASTIC_PROXY_IMAGE"

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=pgelastic.io,resources=pginstances,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantusers,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantmigrations,verbs=get;list;watch;create
// The recorder is client-go's events.EventRecorder, which writes events.k8s.io/v1 rather
// than core/v1. Granting only the core group leaves every automatic-action and refusal
// Event rejected with Forbidden, which removes exactly the audit trail an operator reads
// after an unattended scale-in.
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile publishes the ledger and the plan, and applies at most one action.
func (r *PgElasticPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pool := &pgelasticv1alpha1.PgElasticPool{}
	if err := r.reader().Get(ctx, req.NamespacedName, pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if result, stop, err := unclaimed(ctx, r.ownership(), r.Client, finalizeAnyway, pool); stop {
		return result, err
	}
	if !pool.DeletionTimestamp.IsZero() {
		if r.Metering != nil {
			r.Metering.ForgetPool(pool.Namespace, pool.Name)
		}
		return ctrl.Result{}, nil
	}

	view, err := r.observe(ctx, pool)
	if err != nil {
		return ctrl.Result{}, err
	}

	plan := autoscale.Recommend(view.signals, view.policy)
	// A drain and the autoscaler spend the same migration budget, and both read the count
	// this pass started with. Without telling execute what the drain already did, one
	// reconcile emits two migrations against a cap of one - and both off the same source,
	// which is the pair the per-source guard exists to prevent.
	evacuated, err := r.evacuateDraining(ctx, pool, view, plan)
	if err != nil {
		return ctrl.Result{}, err
	}
	executed, err := r.execute(ctx, pool, view, plan, evacuated)
	if err != nil {
		return ctrl.Result{}, err
	}

	// The fleet is reconciled before the status is published, so one pass writes both the
	// configuration the proxies read and the report of what they are running.
	view.proxy, err = r.reconcileProxy(ctx, pool, view)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.publish(ctx, pool, view, plan, executed); err != nil {
		return ctrl.Result{}, err
	}
	// A replica reports the configuration it has applied by annotating its own Pod, and a
	// Pod annotation is not something this controller watches. Waiting a whole resync to
	// notice would make a converged fleet look unconverged for a minute after it was.
	if view.proxy != nil && view.proxy.ConfigVersion == "" {
		return ctrl.Result{RequeueAfter: proxyConvergenceInterval}, nil
	}
	return ctrl.Result{RequeueAfter: poolResyncInterval}, nil
}

// poolView is everything one pass reads, resolved once.
type poolView struct {
	elasticClass *pgelasticv1alpha1.PgElasticClass
	instances    []pgelasticv1alpha1.PgInstance
	tenants      []tenantView
	ledger       policy.Ledger
	signals      autoscale.Signals
	policy       autoscale.Policy
	boundCount   int32
	pendingCount int32
	byQoS        pgelasticv1alpha1.QoSClassCounts
	metricsAge   time.Duration
	metricsSeen  bool
	proxy        *pgelasticv1alpha1.ProxyStatus
}

// tenantView is one tenant with its resolved policy and its metered demand.
type tenantView struct {
	tenant      *pgelasticv1alpha1.PgTenant
	effective   policy.Effective
	observation metering.Observation
	metered     bool
	// packed is the trailing-window percentile the pool's placement policy names, read out
	// of the store at the quantile that policy asks for rather than always at the 95th.
	packed float64
}

// reader is the API server for the pool object itself, and the cache for everything else.
func (r *PgElasticPoolReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *PgElasticPoolReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// observe reads the pool's members and tenants, meters them, and assembles the planner's
// inputs.
func (r *PgElasticPoolReconciler) observe(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) (*poolView, error) {
	resolver := policy.Resolver{Reader: r.Client}
	view := &poolView{policy: autoscale.PolicyFor(pool)}

	elasticClass, err := resolver.ElasticClassFor(ctx, pool)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	view.elasticClass = elasticClass

	view.instances, err = r.instancesOf(ctx, pool)
	if err != nil {
		return nil, err
	}
	view.tenants, err = r.tenantsOf(ctx, pool, elasticClass, resolver, view.policy.Placement.PackOn)
	if err != nil {
		return nil, err
	}

	view.ledger = r.ledgerOf(pool, elasticClass, view)
	// The staleness of the readings is read before this round is folded in. Afterwards every
	// age is zero by construction, and the guardrail that exists to refuse action on an old
	// reading would be answering a question about its own write.
	view.metricsAge, view.metricsSeen = r.metricsAge(pool)
	r.meter(pool, view)
	view.signals, err = r.signalsOf(ctx, pool, view)
	if err != nil {
		return nil, err
	}
	return view, nil
}

// metricsAge is how stale the pool's readings were when this pass started.
func (r *PgElasticPoolReconciler) metricsAge(pool *pgelasticv1alpha1.PgElasticPool) (time.Duration, bool) {
	if r.Metering == nil {
		return 0, false
	}
	return r.Metering.Age(pool.Namespace, pool.Name, r.now())
}

func (r *PgElasticPoolReconciler) instancesOf(
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

func (r *PgElasticPoolReconciler) tenantsOf(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
	resolver policy.Resolver,
	packOn pgelasticv1alpha1.Percentile,
) ([]tenantView, error) {
	list := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, list, client.InNamespace(pool.Namespace)); err != nil {
		return nil, err
	}

	classes := map[string]*pgelasticv1alpha1.PgWorkloadClass{}
	views := make([]tenantView, 0, len(list.Items))
	for i := range list.Items {
		tenant := &list.Items[i]
		if tenant.Spec.PoolRef.Name != pool.Name || !tenant.DeletionTimestamp.IsZero() {
			continue
		}
		effective, ok, err := resolveEffective(ctx, resolver, r.Client, tenant, pool, elasticClass, classes)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		view := tenantView{tenant: tenant, effective: effective}
		if r.Metering != nil {
			key := meteringKeyOf(tenant)
			view.observation, view.metered = r.Metering.Store.Observation(key, r.now())
			view.packed, _ = r.Metering.Store.Quantile(key, placement.QuantileFor(packOn), r.now())
		}
		views = append(views, view)
	}
	slices.SortFunc(views, func(a, b tenantView) int {
		return strings.Compare(a.tenant.Name, b.tenant.Name)
	})
	return views, nil
}

// ledgerOf sums the pool's reservations. It counts every tenant whose policy resolves,
// bound or not: a guarantee is a credit taken at admission, so a tenant that has been
// accepted and not yet placed is still holding capacity nobody else may be promised.
func (r *PgElasticPoolReconciler) ledgerOf(
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
	view *poolView,
) policy.Ledger {
	headroom := policy.HeadroomPercent(pool, elasticClass)
	ledger := policy.Ledger{
		BackendConnections: pool.Spec.Capacity.BackendConnections,
		HeadroomPercent:    headroom,
		Allocatable:        policy.Allocatable(pool.Spec.Capacity.BackendConnections, headroom),
	}
	for i := range view.tenants {
		entry := &view.tenants[i]
		ledger.Reserved += entry.effective.Guaranteed
		ledger.CommittedBurst += entry.effective.Burstable
		ledger.Tenants++

		switch entry.effective.QoSClass {
		case pgelasticv1alpha1.QoSGuaranteed:
			view.byQoS.Guaranteed++
		case pgelasticv1alpha1.QoSBurstable:
			view.byQoS.Burstable++
		default:
			view.byQoS.BestEffort++
		}
		if placement.BoundInstanceFor(entry.tenant) != "" {
			view.boundCount++
		} else {
			view.pendingCount++
		}
	}
	ledger.Available = max(ledger.Allocatable-ledger.Reserved, 0)
	return ledger
}

// meter folds this pass's readings into the trailing-window store. The pool's own ledger and
// each tenant's connection count are what the operator can see from here; the
// pg_stat_database side is staged by the instance controller from the members' own scrapes
// and picked up by database name on the instance the tenant is bound to.
func (r *PgElasticPoolReconciler) meter(pool *pgelasticv1alpha1.PgElasticPool, view *poolView) {
	if r.Metering == nil {
		return
	}
	if r.Metering.Metrics != nil {
		r.Metering.Metrics.RegisterPool(pool.Namespace, pool.Name)
	}

	inUse := int32(0)
	for i := range view.instances {
		if capacity := view.instances[i].Status.Capacity; capacity != nil {
			inUse += capacity.InUse
		}
	}

	observations := make([]metering.TenantObservation, 0, len(view.tenants))
	present := make([]metering.Key, 0, len(view.tenants))
	for i := range view.tenants {
		entry := &view.tenants[i]
		present = append(present, meteringKeyOf(entry.tenant))
		bound := placement.BoundInstanceFor(entry.tenant)
		observation := metering.TenantObservation{
			Key:                meteringKeyOf(entry.tenant),
			Database:           entry.tenant.Spec.DatabaseName,
			Instance:           bound,
			Role:               metering.RolePrimary,
			BackendConnections: float64(currentConnectionsOf(entry.tenant)),
			Cold:               isColdTenant(entry, view.policy.HotTenantPercent),
		}
		// Left nil for a tenant with no reading, which is a gap rather than a zero: the
		// collector counts it as stale, and a zero would fold a counter that has not been
		// read into the totals as if the tenant had gone quiet.
		if stats, ok := r.Metering.DatabaseStatsFor(metering.ReadingKey{
			Namespace: pool.Namespace,
			Instance:  bound,
			Database:  entry.tenant.Spec.DatabaseName,
		}, r.now()); ok {
			observation.Stats = &stats
		}
		observations = append(observations, observation)
	}

	// A tenant that has been deleted stops being observed but does not stop occupying a ring
	// of histograms, so the store is swept every pass. Held state is then bounded by the
	// tenants that exist rather than by the tenants that ever existed, which is the same
	// bound the label set gives the metrics.
	//
	// The sweep is by pool membership rather than by age alone. Pool freshness is the worst
	// age across the pool's series, so a departed tenant left in the store until its window
	// expires holds every autoscaling action for that whole window.
	r.Metering.ForgetDeparted(pool.Namespace, pool.Name, present)
	defer r.Metering.Store.Prune(r.now())

	r.Metering.Observe(metering.PoolObservation{
		Namespace:        pool.Namespace,
		Pool:             pool.Name,
		InUse:            inUse,
		Reserved:         view.ledger.Reserved,
		Allocatable:      view.ledger.Allocatable,
		CommittedBurst:   view.ledger.CommittedBurst,
		Bound:            view.boundCount,
		Pending:          view.pendingCount,
		PerTenantMetrics: perTenantMetrics(pool),
	}, observations, r.now())
}

// perTenantMetrics reads spec.observability.perTenantMetrics, which is the only switch that
// lets a tenant label out of the metering package.
//
// Absent is off. The field has a false default in the API, so an object that went through the
// API server carries one either way; the fallback is for an object built in memory, and it
// falls the way that cannot multiply a pool's series by its tenant count without anybody
// having asked.
func perTenantMetrics(pool *pgelasticv1alpha1.PgElasticPool) bool {
	observability := pool.Spec.Observability
	return observability != nil && observability.PerTenantMetrics != nil &&
		*observability.PerTenantMetrics
}

// signalsOf assembles the planner's inputs, including the three facts that can only be read
// from other objects: whether a rollout is in flight, what migrations are running, and when
// each action class last executed.
func (r *PgElasticPoolReconciler) signalsOf(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
) (autoscale.Signals, error) {
	now := r.now()
	signals := autoscale.Signals{
		Now:                  now,
		Namespace:            pool.Namespace,
		Pool:                 pool.Name,
		Paused:               pool.Spec.Paused != nil && *pool.Spec.Paused,
		MigrationsFromSource: map[string]int32{},
		EvidenceSpan:         evidenceSpanOf(view.tenants),
		DeclaredInstances:    declaredInstances(pool),
	}

	for i := range view.instances {
		instance := &view.instances[i]
		signal := autoscale.InstanceSignal{
			Name:        instance.Name,
			Major:       placement.MajorOf(instance),
			Ready:       instance.Status.Phase == pgelasticv1alpha1.InstancePhaseReady,
			Schedulable: placement.InstanceFrom(instance).Schedulable,
			Progressing: instanceProgressing(instance),
		}
		if capacity := instance.Status.Capacity; capacity != nil {
			signal.AllocatableConnections = capacity.Allocatable
			signal.InUseConnections = capacity.InUse
		}
		if storage := instance.Status.Storage; storage != nil {
			if storage.Allocated != nil {
				signal.StorageAllocatedBytes = storage.Allocated.Value()
			}
			if storage.Used != nil {
				signal.StorageUsedBytes = storage.Used.Value()
			}
		}
		signal.Tenants = instance.Status.Tenants
		if signal.Progressing {
			signals.RolloutInProgress = true
		}
		signals.Instances = append(signals.Instances, signal)
	}

	for i := range view.tenants {
		signals.Tenants = append(signals.Tenants, r.tenantSignalOf(&view.tenants[i], view))
	}

	if err := r.migrationSignals(ctx, pool, &signals); err != nil {
		return autoscale.Signals{}, err
	}
	r.historySignals(pool, &signals)

	signals.MetricsSeen = view.metricsSeen
	signals.MetricsAge = view.metricsAge
	return signals, nil
}

func (r *PgElasticPoolReconciler) tenantSignalOf(entry *tenantView, view *poolView) autoscale.TenantSignal {
	return autoscale.TenantSignal{
		Name:                  entry.tenant.Name,
		Instance:              placement.BoundInstanceFor(entry.tenant),
		GuaranteedConnections: entry.effective.Guaranteed,
		BurstableConnections:  entry.effective.Burstable,
		PackedConnections:     packedConnectionsOf(entry),
		PeakConnections:       int32(entry.observation.Peak),
		StorageBytes:          entry.observation.StorageBytes,
		Relations:             entry.observation.Relations,
		Cold:                  isColdTenant(entry, view.policy.HotTenantPercent),
		MigrationAllowed:      entry.effective.AutomaticMigrationAllowed,
		AntiAffinity:          placement.AntiAffinityFor(entry.tenant),
		PinnedInstance:        placement.PinnedInstanceFor(entry.tenant),
		EvidenceSpan:          entry.observation.LastSampleAt.Sub(entry.observation.FirstSampleAt),
	}
}

// migrationSignals counts what is already moving. Two numbers matter: how many moves are in
// flight in total, which is the concurrency cap, and how many are in flight off each
// instance, which is what stops a second tenant being decoded off a source that is already
// paying for the first.
func (r *PgElasticPoolReconciler) migrationSignals(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	signals *autoscale.Signals,
) error {
	migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
	if err := r.List(ctx, migrations, client.InNamespace(pool.Namespace)); err != nil {
		return err
	}
	windowStart := signals.Now.Add(-migrationRateWindow)
	for i := range migrations.Items {
		migration := &migrations.Items[i]
		if migrationSettled(migration) || preflightRefused(migration) {
			if migration.CreationTimestamp.After(windowStart) {
				signals.MigrationsStartedInWindow++
			}
			continue
		}
		signals.InFlightMigrations++
		if migration.CreationTimestamp.After(windowStart) {
			signals.MigrationsStartedInWindow++
		}
		if source := migration.Status.SourceInstanceRef; source != nil {
			signals.MigrationsFromSource[source.Name]++
		}
	}
	return nil
}

// historySignals recovers when each action class last executed, and how long the current
// consolidation candidate has been a candidate, from the plan the last pass published.
// Keeping them in status rather than in memory is what makes a dwell time survive an
// operator restart instead of silently resetting.
func (r *PgElasticPoolReconciler) historySignals(
	pool *pgelasticv1alpha1.PgElasticPool,
	signals *autoscale.Signals,
) {
	previous := pool.Status.Autoscaling
	if previous == nil {
		return
	}
	for _, action := range previous.Actions {
		if action.ExecutedAt == nil {
			continue
		}
		executed := action.ExecutedAt.Time
		switch action.Name {
		case pgelasticv1alpha1.AutoActionScaleOut:
			signals.LastScaleUpAt = &executed
		case pgelasticv1alpha1.AutoActionScaleIn:
			signals.LastScaleDownAt = &executed
		}
	}
	for _, target := range previous.PerInstance {
		if target.Consolidatable && target.ConsolidatableSince != nil {
			if signals.ConsolidatableSince == nil {
				signals.ConsolidatableSince = map[string]time.Time{}
			}
			signals.ConsolidatableSince[target.Name] = target.ConsolidatableSince.Time
		}
	}
}

// executed records what one pass actually applied.
type executed struct {
	class      pgelasticv1alpha1.AutoAction
	at         time.Time
	applied    bool
	detail     string
	failReason string
}

// execute applies at most one action: the single one the guardrails selected.
func (r *PgElasticPoolReconciler) execute(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
	plan autoscale.Plan,
	evacuated bool,
) (executed, error) {
	action, ok := plan.Selected()
	if !ok {
		return executed{}, nil
	}
	// The drain has already spent this pass's migration budget, so the two classes that
	// would spend it again are refused rather than silently doubling it. Said out loud in
	// the plan, because an operator reading "no rebalance happened" is owed the reason.
	if evacuated && (action.Class == pgelasticv1alpha1.AutoActionRebalance ||
		action.Class == pgelasticv1alpha1.AutoActionScaleIn) {
		return executed{
			class:      action.Class,
			at:         r.now(),
			detail:     action.Detail,
			failReason: autoscale.ReasonMigrationBudget,
		}, nil
	}

	result := executed{class: action.Class, at: r.now(), detail: action.Detail}
	var err error
	switch action.Class {
	case pgelasticv1alpha1.AutoActionStorageExpand:
		result.applied, err = r.expandStorage(ctx, view, plan, action)
	case pgelasticv1alpha1.AutoActionScaleOut:
		result.applied, err = r.scaleOut(ctx, pool, plan)
	case pgelasticv1alpha1.AutoActionRebalance:
		result.applied, err = r.emitMigrations(ctx, pool, plan.EligibleMoves(), 1, nil)
	case pgelasticv1alpha1.AutoActionScaleIn:
		result.applied, err = r.scaleIn(ctx, pool, view, plan)
	default:
		// TenantGucTune needs a SQL session and VerticalResize needs a restart proven
		// transparent through the proxy. Neither is reachable from here, so the plan says so
		// rather than reporting an execution that did not happen.
		result.failReason = autoscale.ReasonNoExecutor
	}
	if err != nil {
		return executed{}, err
	}

	if result.applied {
		r.event(pool, corev1.EventTypeNormal, eventActionExecuted, actionExecute,
			"%s: %s", action.Class, action.Detail)
	}
	return result, nil
}

// expandStorage grows one instance's data volume. It only ever grows: a PVC cannot shrink,
// so a smaller recommendation is a no-op rather than a shrink request that would be rejected
// by the API server with a less obvious message.
func (r *PgElasticPoolReconciler) expandStorage(
	ctx context.Context,
	view *poolView,
	plan autoscale.Plan,
	action autoscale.Action,
) (bool, error) {
	for _, target := range plan.InstanceTargets {
		if target.Name != action.Target || target.RecommendedStorageByes <= 0 {
			continue
		}
		for i := range view.instances {
			instance := &view.instances[i]
			if instance.Name != target.Name {
				continue
			}
			wanted := resource.NewQuantity(target.RecommendedStorageByes, resource.BinarySI)
			if instance.Spec.Storage.Size.Cmp(*wanted) >= 0 {
				return false, nil
			}
			patch := client.MergeFrom(instance.DeepCopy())
			instance.Spec.Storage.Size = *wanted
			return true, r.Patch(ctx, instance, patch)
		}
	}
	return false, nil
}

// declaredInstances is the member count the pool's spec asks for. The CRD defaults it, so an
// unset pointer is a pool whose spec predates the default rather than a pool that wants none.
func declaredInstances(pool *pgelasticv1alpha1.PgElasticPool) int32 {
	if pool.Spec.Instances.Replicas == nil {
		return 1
	}
	return *pool.Spec.Instances.Replicas
}

// scaleOut raises the pool's declared member count. The autoscaler moves the desired-count
// knob and nothing else; provisioning the member is the instance lifecycle's job, exactly as
// an HPA writes replicas and leaves the Deployment controller to make Pods.
//
// The count it raises is the one the plan was computed against - the members that exist -
// rather than the number already declared. Those are the same only while every declared
// member has been made; the moment they diverge, raising the declared count again asks for a
// member on top of one that never arrived, and the pool's own guardrail refuses the class
// before this is reached.
func (r *PgElasticPoolReconciler) scaleOut(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	plan autoscale.Plan,
) (bool, error) {
	if plan.RecommendedInstances <= plan.ObservedInstances {
		return false, nil
	}
	// One instance at a time, so the stabilization window gets to observe the effect of
	// each addition before the next is considered.
	patch := client.MergeFrom(pool.DeepCopy())
	pool.Spec.Instances.Replicas = ptr.To(plan.ObservedInstances + 1)
	return true, r.Patch(ctx, pool, patch)
}

// scaleIn evacuates the consolidation target and then takes it out of service.
//
// The two halves are one plan and are applied in order: while the victim still holds
// tenants, the only thing that happens is the migrations that empty it, and the member count
// only drops once it holds none. Reversing that order would strand databases on an instance
// the pool no longer believes in.
func (r *PgElasticPoolReconciler) scaleIn(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
	plan autoscale.Plan,
) (bool, error) {
	target := plan.ConsolidationTarget
	if target == "" {
		return false, nil
	}

	evacuation := make([]autoscale.Move, 0, len(plan.Moves))
	for _, move := range plan.EligibleMoves() {
		if move.From == target {
			evacuation = append(evacuation, move)
		}
	}
	if len(evacuation) > 0 {
		return r.emitMigrations(ctx, pool, evacuation, 1, nil)
	}

	for i := range view.instances {
		instance := &view.instances[i]
		if instance.Name != target {
			continue
		}
		if cordoned(instance) {
			break
		}
		patch := client.MergeFrom(instance.DeepCopy())
		if instance.Spec.Admission == nil {
			instance.Spec.Admission = &pgelasticv1alpha1.InstanceAdmission{}
		}
		instance.Spec.Admission.Cordoned = ptr.To(true)
		instance.Spec.Drain = &pgelasticv1alpha1.InstanceDrain{
			Mode: ptr.To(pgelasticv1alpha1.InstanceDrainRequested),
		}
		if err := r.Patch(ctx, instance, patch); err != nil {
			return false, err
		}
	}

	current := int32(len(view.instances))
	if pool.Spec.Instances.Replicas != nil {
		current = *pool.Spec.Instances.Replicas
	}
	if current <= plan.RecommendedInstances {
		return false, nil
	}
	patch := client.MergeFrom(pool.DeepCopy())
	pool.Spec.Instances.Replicas = ptr.To(current - 1)
	return true, r.Patch(ctx, pool, patch)
}

// evacuateDraining carries out spec.drain on every instance that asks for it.
//
// `mode: Requested` documents itself as cordoning the instance *and* emitting a
// PgTenantMigration per bound tenant. Only the first half existed: the sole reader set
// Schedulable=false, so a drained instance stopped receiving new tenants and kept every
// tenant it already had, for ever. An operator retiring hardware had no way to say "move
// these off, now" and no way to see that nothing was happening.
//
// A drain is an instruction rather than a recommendation, so it is not gated behind the
// Rebalance action class being enabled - the operator has already decided. It goes through
// the same emitter and the same eligibility as every other move, because the concurrency
// limit and the blackout windows protect the tenants being moved and do not care why they
// are moving. One per pass, like the rest: a drain that emitted every migration at once
// would move the whole instance's population through logical replication simultaneously.
func (r *PgElasticPoolReconciler) evacuateDraining(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
	plan autoscale.Plan,
) (bool, error) {
	draining := map[string]bool{}
	for index := range view.instances {
		if drainRequested(&view.instances[index]) {
			draining[view.instances[index].Name] = true
		}
	}
	if len(draining) == 0 {
		return false, nil
	}
	// The disruption budget, and only that: the concurrency cap, the rate cap and the
	// migration windows. Not `rebalance()`, which would also require the rebalancer to be
	// switched on - an operator draining an instance has already decided, and a pool that
	// never rebalances still has to be able to evacuate hardware.
	if !plan.EvacuationPermitted {
		for _, name := range slices.Sorted(maps.Keys(draining)) {
			if stranded := boundTo(view, name); stranded > 0 {
				r.event(pool, corev1.EventTypeNormal, eventDrainStalled, actionEmitMigration,
					"%s is draining with %d tenant(s) still on it, and the pool is not "+
						"admitting a move right now: %s", name, stranded, plan.EvacuationRefusedBecause)
			}
		}
		return false, nil
	}

	// Re-evaluated with the heat rule disabled rather than filtered on the blocker the plan
	// recorded. MoveEligible reports the FIRST refusal it finds, so "blocked by heat" says
	// nothing about whether the source is also too loaded to decode a move off, or already
	// streaming one - and admitting on that basis would have let a drain do exactly what this
	// function's own comment promises it never does.
	evacuation := make([]autoscale.Move, 0, len(plan.Moves))
	for _, move := range plan.EvacuationMoves(view.signals) {
		if draining[move.From] {
			evacuation = append(evacuation, move)
		}
	}
	if len(evacuation) == 0 {
		// Said out loud rather than left as silence. A drain with nowhere to send its
		// tenants is the state an operator most needs to see: the instance is cordoned, the
		// tenants are staying, and without this the pool looks like it is draining.
		for _, name := range slices.Sorted(maps.Keys(draining)) {
			if stranded := boundTo(view, name); stranded > 0 {
				r.event(pool, corev1.EventTypeWarning, eventDrainStalled, actionEmitMigration,
					"%s is draining and %d tenant(s) are still on it, with no move the pool "+
						"can make right now: no other instance both fits them and admits them, "+
						"or the guardrails are holding every move back", name, stranded)
			}
		}
		return false, nil
	}

	// The override has to travel on the object, not merely be applied here. The migration's
	// own preflight defaults requireColdTenant to true and then reads the same coldness
	// verdict the pool just overruled, so a drain that emitted a bare spec would emit a
	// migration guaranteed to be refused - the override would look applied and do nothing.
	return r.emitMigrations(ctx, pool, evacuation, 1,
		&pgelasticv1alpha1.TenantMigrationPreflight{RequireColdTenant: ptr.To(false)})
}

// boundTo counts the tenants still bound to one instance.
func boundTo(view *poolView, instance string) int {
	bound := 0
	for _, tenant := range view.tenants {
		if placement.BoundInstanceFor(tenant.tenant) == instance {
			bound++
		}
	}
	return bound
}

// drainRequested reports the instruction, which is separate from being cordoned: a cordoned
// instance keeps its tenants deliberately, and a draining one is being emptied.
func drainRequested(instance *pgelasticv1alpha1.PgInstance) bool {
	drain := instance.Spec.Drain
	return drain != nil && drain.Mode != nil &&
		*drain.Mode == pgelasticv1alpha1.InstanceDrainRequested
}

// emitMigrations creates PgTenantMigration objects for the moves the plan may afford.
//
// The migration machinery is consumed as an API and never as a function call: this
// controller states the intent and the migration controller owns every phase of carrying it
// out, including refusing it at preflight.
func (r *PgElasticPoolReconciler) emitMigrations(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	moves []autoscale.Move,
	limit int,
	preflight *pgelasticv1alpha1.TenantMigrationPreflight,
) (bool, error) {
	emitted := false
	for _, move := range moves {
		if limit <= 0 {
			break
		}
		migration := &pgelasticv1alpha1.PgTenantMigration{
			ObjectMeta: metav1.ObjectMeta{
				Name:      migrationNameFor(move),
				Namespace: pool.Namespace,
				Labels: map[string]string{
					"pgelastic.io/pool":   pool.Name,
					"pgelastic.io/tenant": move.Tenant,
				},
			},
			Spec: pgelasticv1alpha1.PgTenantMigrationSpec{
				TenantRef:         corev1.LocalObjectReference{Name: move.Tenant},
				TargetInstanceRef: corev1.LocalObjectReference{Name: move.To},
				Preflight:         preflight.DeepCopy(),
			},
		}
		if err := r.Create(ctx, migration); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return emitted, err
		}
		r.event(pool, corev1.EventTypeNormal, eventMigrationEmitted, actionEmitMigration,
			"moving %s from %s to %s, worth %d%% of the source's utilization",
			move.Tenant, move.From, move.To, move.ExpectedImprovementPercent)
		emitted = true
		limit--
	}
	return emitted, nil
}

// publish writes the ledger and the plan, and emits one Event per pass describing what was
// decided. Recommend mode publishes exactly the same plan Auto mode would act on, which is
// the whole point: the evidence for enabling an action class is the record of what it would
// have done.
func (r *PgElasticPoolReconciler) publish(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
	plan autoscale.Plan,
	applied executed,
) error {
	status := pgelasticv1alpha1.PgElasticPoolStatus{
		ObservedGeneration: pool.Generation,
		Selector:           poolSelector(pool),
		Proxy:              view.proxy,
		Conditions:         pool.Status.Conditions,
		Capacity:           ledgerStatus(view),
		PerInstance:        perInstanceStatus(view),
		Tenants: &pgelasticv1alpha1.PoolTenantCounts{
			Total:      view.ledger.Tenants,
			Bound:      view.boundCount,
			Pending:    view.pendingCount,
			ByQosClass: &view.byQoS,
		},
		Autoscaling: planStatus(pool, plan, applied, r.now()),
	}
	status.Phase = poolPhase(view, status.Conditions)

	r.recordPlan(pool, plan)
	setCondition(&status.Conditions, pool.Generation, pgelasticv1alpha1.ConditionAccepted,
		conditionStatus(view.elasticClass != nil), acceptedReasonOf(view),
		acceptedMessageOf(view, pool))
	ready := view.elasticClass != nil && readyInstanceCount(view) > 0
	setCondition(&status.Conditions, pool.Generation, pgelasticv1alpha1.ConditionReady,
		conditionStatus(ready), readyPoolReason(ready), readyPoolMessage(view))
	status.Phase = poolPhase(view, status.Conditions)

	// The plan is recomputed every pass and is usually identical to the last one. Stamping a
	// fresh computedAt on an unchanged plan would make every reconcile a write, and every
	// write its own watch event: the controller would spin at full speed on a pool where
	// nothing is happening. So the timestamp only advances when something else did.
	if previous := pool.Status.Autoscaling; previous != nil && status.Autoscaling != nil {
		status.Autoscaling.ComputedAt = previous.ComputedAt
	}
	if equality.Semantic.DeepEqual(pool.Status, status) {
		return nil
	}
	if status.Autoscaling != nil {
		status.Autoscaling.ComputedAt = &metav1.Time{Time: r.now()}
	}
	// A merge patch rather than an update: this controller reads the pool from the informer
	// cache, and an update carries the resource version it read, so it loses a race with its
	// own previous write and spends a reconcile on a conflict it has no disagreement about.
	patch := client.MergeFrom(pool.DeepCopy())
	pool.Status = status
	return r.Status().Patch(ctx, pool, patch)
}

// recordPlan emits the plan as Events. One summary Event per pass, plus one per refused
// action, because the refusals are what an operator is looking for when the pool did not do
// the thing they expected.
func (r *PgElasticPoolReconciler) recordPlan(pool *pgelasticv1alpha1.PgElasticPool, plan autoscale.Plan) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(pool, nil, corev1.EventTypeNormal, eventAutoscalePlan, actionPlan, "%s", plan.Summary)
	for _, action := range plan.Actions {
		if action.Permitted {
			continue
		}
		r.Recorder.Eventf(pool, nil, corev1.EventTypeNormal, eventActionRefused, actionRefuse,
			"%s not executed: %s (%s)", action.Class, action.Reason, action.Message)
	}
}

func (r *PgElasticPoolReconciler) event(
	pool *pgelasticv1alpha1.PgElasticPool,
	eventType, reason, action, format string,
	args ...any,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(pool, nil, eventType, reason, action, format, args...)
}

// The events API takes an action alongside the reason: the reason is why, the action is
// what was done. Keeping them distinct is the whole point of the newer API.
const (
	actionPlan          = "Plan"
	actionRefuse        = "Refuse"
	actionExecute       = "Execute"
	actionEmitMigration = "EmitMigration"
)

// SetupWithManager sets up the controller with the Manager.
func (r *PgElasticPoolReconciler) ownership() ownership.Resolver {
	return ownership.Resolver{Reader: r.Client, ControllerName: r.ControllerName}
}

func (r *PgElasticPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("pgelasticpool")
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgElasticPool{}).
		Owns(&appsv1.Deployment{}).
		Watches(&pgelasticv1alpha1.PgInstance{}, handler.EnqueueRequestsFromMapFunc(poolOfInstance)).
		Watches(&pgelasticv1alpha1.PgTenant{}, handler.EnqueueRequestsFromMapFunc(poolOfTenant)).
		Named("pgelasticpool").
		Complete(tracing.Wrap("PgElasticPool", r))
}

// poolSelector is the label selector the scale subresource resolves replicas through. It
// names the proxy fleet because that is what scaling a pool's front end means; a pool with
// no fleet declares no selector rather than one that matches nothing.
func poolSelector(pool *pgelasticv1alpha1.PgElasticPool) string {
	if pool.Spec.Proxy == nil {
		return ""
	}
	return proxy.SelectorString(pool.Name)
}

func poolOfInstance(_ context.Context, object client.Object) []reconcile.Request {
	instance, ok := object.(*pgelasticv1alpha1.PgInstance)
	if !ok || instance.Spec.PoolRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{
		Namespace: instance.Namespace, Name: instance.Spec.PoolRef.Name,
	}}}
}

func poolOfTenant(_ context.Context, object client.Object) []reconcile.Request {
	tenant, ok := object.(*pgelasticv1alpha1.PgTenant)
	if !ok || tenant.Spec.PoolRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{
		Namespace: tenant.Namespace, Name: tenant.Spec.PoolRef.Name,
	}}}
}

func migrationNameFor(move autoscale.Move) string {
	name := fmt.Sprintf("%s-to-%s", move.Tenant, move.To)
	if len(name) > 253 {
		name = name[:253]
	}
	return name
}

func migrationSettled(migration *pgelasticv1alpha1.PgTenantMigration) bool {
	switch migration.Status.Phase {
	case pgelasticv1alpha1.TenantMigrationPhaseCompleted,
		pgelasticv1alpha1.TenantMigrationPhaseAborted,
		pgelasticv1alpha1.TenantMigrationPhaseFailed,
		pgelasticv1alpha1.TenantMigrationPhaseRolledBack:
		return true
	default:
		return false
	}
}

// preflightRefused is a migration parked at the gate rather than one in flight.
//
// It is not settled, and deliberately so: the object stays, with the reason on it, until
// somebody acts on it. But it is holding no replication slot, no subscription and no backend,
// so counting it against maxConcurrentMigrations is counting a resource nobody is using.
//
// That distinction became load-bearing the moment a refusal existed that no amount of waiting
// can clear. A materialized view or an unlogged table refuses an ONLINE move by the shape of
// the tenant's own schema, and there is no waiver - so one such object would sit at the gate
// and consume a budget whose default is 1, permanently stalling every rebalance, every
// evacuation and every scale-in in the pool. A refusal that stops other work is a much bigger
// refusal than the one that was written.
func preflightRefused(object *pgelasticv1alpha1.PgTenantMigration) bool {
	return object.Status.Phase == pgelasticv1alpha1.TenantMigrationPhasePreflight &&
		meta.IsStatusConditionFalse(object.Status.Conditions, migrationpkg.ConditionPreflightPassed)
}

func cordoned(instance *pgelasticv1alpha1.PgInstance) bool {
	admission := instance.Spec.Admission
	return admission != nil && admission.Cordoned != nil && *admission.Cordoned
}

// instanceProgressing reports a rollout in flight on one member. It is the condition rather
// than the phase because Progressing is written from the instance's own convergence check,
// and the phase is documented as display-only.
func instanceProgressing(instance *pgelasticv1alpha1.PgInstance) bool {
	for _, condition := range instance.Status.Conditions {
		if condition.Type == pgelasticv1alpha1.ConditionProgressing {
			return condition.Status == metav1.ConditionTrue
		}
	}
	return false
}
