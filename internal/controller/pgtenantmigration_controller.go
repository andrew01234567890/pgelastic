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
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
)

const (
	// migrationPollInterval is how often an unfinished phase is looked at again. Copying and
	// Catchup are the phases it governs, and both are bounded by how fast the source can
	// emit rather than by how often anyone asks.
	migrationPollInterval = 5 * time.Second
	// migrationPausePollInterval governs the quiesced phases. Every millisecond spent here
	// is a millisecond of the pause clients were promised, so it is as short as a reconcile
	// can usefully be.
	migrationPausePollInterval = 250 * time.Millisecond
	// migrationSettledInterval is how often a finished migration is looked at, which it
	// needs at all only so the rollback window can close.
	migrationSettledInterval = 30 * time.Second
	// migrationRetryBudget is how long one phase may go on failing before the migration gives
	// up. It exists because the failures that actually happen are transient: a refused
	// connection while a read-write Service reselects its endpoints, a member restarting under
	// a rolling change. Ending a move on the first of those would make migration look
	// unreliable for reasons that have nothing to do with migration.
	migrationRetryBudget = 2 * time.Minute
)

const (
	// AnnotationAbort asks a running migration to stop. Every abort path leaves the tenant
	// serving from the source, so it is safe to set at any phase.
	AnnotationAbort = "pgelastic.io/abort"
	// AnnotationRollback asks a completed migration to put the tenant back on the source. It
	// is honoured only while the source database still exists, which rollbackWindow bounds.
	AnnotationRollback = "pgelastic.io/rollback"
)

// PgTenantMigrationReconciler reconciles a PgTenantMigration object.
type PgTenantMigrationReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// APIReader reads the migration itself straight from the API server, bypassing the
	// informer cache.
	//
	// A reconcile that observed anything new ends in a status update, and an update carries
	// the resourceVersion it read. The cache lags this controller's own last write by however
	// long the watch event takes to arrive, so reading the object through it makes the
	// controller lose an optimistic-concurrency race with nobody but itself - and losing one
	// is not a harmless retry, because the phase's whole effect runs before the status is
	// published and so runs again.
	APIReader client.Reader

	// SQL, Shell and Router are the three ports the migration engine acts through. An
	// operator started without them can resolve and report, and nothing else.
	SQL    migration.SQL
	Shell  migration.Shell
	Router migration.Router

	// Now is injectable so the pause clock and the rollback deadline can be driven in tests.
	Now func() time.Time

	// ControllerName is this operator's identity. A migration reaches a PgElasticClass
	// through its tenant's pool, and one naming a different controller is left entirely
	// alone - which matters more here than anywhere else, because two operators running the
	// same move would each quiesce and cut over the same tenant.
	ControllerName string
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantmigrations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantmigrations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenantmigrations/finalizers,verbs=update
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgtenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pginstances,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgelasticpools,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=pods;services,verbs=get;list;watch

// Reconcile advances one migration by at most one phase.
//
// The transition is decided by migration.Decide from what the engine observed on this
// reconcile, never from anything already in status. That separation is what makes the
// safety property checkable: every decision other than a successful cutover names the source
// as the instance the tenant serves from, and the controller applies that naming before it
// publishes the phase.
func (r *PgTenantMigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	object := &pgelasticv1alpha1.PgTenantMigration{}
	if err := r.reader().Get(ctx, req.NamespacedName, object); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if result, stop, err := unclaimed(ctx, r.ownership(), r.Client, finalizeAnyway, object); stop {
		return result, err
	}
	if !object.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	status := *object.Status.DeepCopy()
	status.ObservedGeneration = object.Generation
	if status.Phase == "" {
		status.Phase = pgelasticv1alpha1.TenantMigrationPhasePreflight
	}
	if status.StartedAt == nil {
		status.StartedAt = ptr.To(metav1.NewTime(r.now()))
	}

	run, err := r.resolve(ctx, object, &status)
	if err != nil {
		setCondition(&status.Conditions, object.Generation, pgelasticv1alpha1.ConditionAccepted,
			metav1.ConditionFalse, migration.ReasonUnresolved, err.Error())
		return r.publish(ctx, object, status, ctrl.Result{RequeueAfter: migrationPollInterval})
	}
	setCondition(&status.Conditions, object.Generation, pgelasticv1alpha1.ConditionAccepted,
		metav1.ConditionTrue, pgelasticv1alpha1.ReasonAccepted,
		fmt.Sprintf("moving tenant %s from %s to %s by the %s strategy",
			run.Tenant.Name, run.Plan.Source.Instance, run.Plan.Target.Instance, run.Strategy))

	engine := migration.Engine{SQL: r.SQL, Shell: r.Shell, Router: r.Router, Now: r.now}
	step := engine.Step(ctx, run)
	decision := migration.Decide(run.Phase, step.Observation)

	if err := engine.Apply(ctx, run, decision); err != nil {
		// A cleanup that could not finish must not stop the object reaching its terminal
		// phase: the orphan sweeper is what reaps whatever is left, and it can only find the
		// litter of a migration that has actually stopped.
		log.Error(err, "Could not apply the migration decision in full", "phase", string(decision.Phase))
	}
	// Read after Apply, because Apply is where the hold ends: the router can only report the
	// pause once it has stopped imposing it.
	step.ClientPause = r.clientPause(run.Tenant)

	r.record(&status, object.Generation, run, step, decision)
	return r.publish(ctx, object, status, ctrl.Result{RequeueAfter: requeueFor(decision.Phase, run.Strategy)})
}

// resolve turns the CR and everything it references into the engine's inputs.
func (r *PgTenantMigrationReconciler) resolve(
	ctx context.Context,
	object *pgelasticv1alpha1.PgTenantMigration,
	status *pgelasticv1alpha1.PgTenantMigrationStatus,
) (migration.Run, error) {
	if r.SQL == nil || r.Router == nil {
		return migration.Run{}, fmt.Errorf(
			"this operator was started without the SQL and proxy ports a migration acts through")
	}

	tenant := &pgelasticv1alpha1.PgTenant{}
	tenantKey := types.NamespacedName{Namespace: object.Namespace, Name: object.Spec.TenantRef.Name}
	if err := r.Get(ctx, tenantKey, tenant); err != nil {
		return migration.Run{}, fmt.Errorf("PgTenant %q: %w", tenantKey.Name, err)
	}

	// The source is resolved once and then treated as fixed. A binding rewritten by another
	// actor mid-migration must not change which instance an abort restores the tenant to.
	if status.SourceInstanceRef == nil {
		if tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef == nil {
			return migration.Run{}, fmt.Errorf("PgTenant %q is not bound to an instance yet", tenantKey.Name)
		}
		status.SourceInstanceRef = tenant.Status.Binding.InstanceRef.DeepCopy()
	}
	if status.SourceInstanceRef.Name == object.Spec.TargetInstanceRef.Name {
		return migration.Run{}, fmt.Errorf("the tenant is already bound to %q", status.SourceInstanceRef.Name)
	}

	source, err := r.instance(ctx, object.Namespace, status.SourceInstanceRef.Name)
	if err != nil {
		return migration.Run{}, err
	}
	target, err := r.instance(ctx, object.Namespace, object.Spec.TargetInstanceRef.Name)
	if err != nil {
		return migration.Run{}, err
	}

	pool := &pgelasticv1alpha1.PgElasticPool{}
	poolKey := types.NamespacedName{Namespace: object.Namespace, Name: tenant.Spec.PoolRef.Name}
	if err := r.Get(ctx, poolKey, pool); err != nil {
		if !apierrors.IsNotFound(err) {
			return migration.Run{}, fmt.Errorf("PgElasticPool %q: %w", poolKey.Name, err)
		}
		pool = nil
	}

	password, err := replicationPassword(ctx, r.Client, object.Namespace, source.Name)
	if err != nil {
		return migration.Run{}, err
	}

	run := migration.Run{
		Migration: migration.TenantRef{Namespace: object.Namespace, Name: object.Name},
		Tenant:    migration.TenantRef{Namespace: tenant.Namespace, Name: tenant.Name},
		Phase:     status.Phase,
		Strategy:  resolveStrategy(object.Spec.Strategy, pool),
		Plan: migration.Plan{
			Source: migration.Endpoint{
				Namespace: object.Namespace, Instance: source.Name, Database: tenant.Spec.DatabaseName},
			Target: migration.Endpoint{
				Namespace: object.Namespace, Instance: target.Name, Database: tenant.Spec.DatabaseName},
			Publication:    migration.PublicationName(object.Namespace, object.Name),
			Slot:           migration.SlotName(object.Namespace, object.Name),
			Subscription:   migration.SubscriptionName(object.Namespace, object.Name),
			SchemaStamp:    migration.SchemaStamp(object.Namespace, object.Name),
			SourceConnInfo: sourceConnInfo(source, tenant.Spec.DatabaseName, password),
			Concurrency:    provision.ConcurrentDumps(source.Spec),
			DumpDir:        migration.DumpDir(object.Namespace, object.Name),
		},
		ReplicationRole:      provision.ReplicationRole,
		Owner:                ptr.Deref(tenant.Spec.Owner, tenant.Spec.DatabaseName),
		AbortRequested:       object.Annotations[AnnotationAbort] != "",
		AbortMessage:         object.Annotations[AnnotationAbort],
		RollbackRequested:    object.Annotations[AnnotationRollback] != "",
		RollbackWindowClosed: rollbackWindowClosed(status, r.now()),
		SourceDropped:        sourceDropped(status),
		QuiesceStartedAt:     quiesceStart(status),
		FaultingSince:        faultingSince(status),
		RetryBudget:          migrationRetryBudget,
		DrainTimeout:         durationOr(object.Spec.DrainTimeout, 30*time.Second),
		CutoverTimeout:       durationOr(object.Spec.CutoverTimeout, 60*time.Second),
		RollbackWindow:       rollbackWindow(object, pool),
		Verification:         migration.VerificationLevelFor(poolVerification(pool)),
		Sequences:            sequencePlan(object.Spec.SequenceHandling),
	}
	if preflight := object.Spec.Preflight; preflight != nil {
		run.MaxLagBytes = preflight.MaxSourceLagBytes
	}
	run.Preflight = preflightInput(object, tenant, source, target, pool, run)
	return run, nil
}

func (r *PgTenantMigrationReconciler) instance(
	ctx context.Context, namespace, name string,
) (*pgelasticv1alpha1.PgInstance, error) {
	instance := &pgelasticv1alpha1.PgInstance{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, instance); err != nil {
		return nil, fmt.Errorf("PgInstance %q: %w", name, err)
	}
	if instance.Status.CurrentPrimary == "" {
		return nil, fmt.Errorf("PgInstance %q has no current primary", name)
	}
	return instance, nil
}

// record projects one step onto the status. Everything published here is evidence the
// engine gathered on this reconcile; nothing is carried forward on faith.
func (r *PgTenantMigrationReconciler) record(
	status *pgelasticv1alpha1.PgTenantMigrationStatus,
	generation int64,
	run migration.Run,
	step migration.StepResult,
	decision migration.Decision,
) {
	status.ReplicationSlotName = run.Plan.Slot
	status.PublicationName = run.Plan.Publication
	status.SubscriptionName = run.Plan.Subscription

	online := run.Strategy == pgelasticv1alpha1.TenantMigrationOnline
	setCondition(&status.Conditions, generation, migration.ConditionOnline,
		conditionStatus(online), onlineReason(online), strategyMessage(run.Strategy))

	// The retry clock starts at the first failure of a run and is cleared by the first
	// success, so a phase that fails, recovers and fails again gets the whole budget again.
	// Its transition time is stamped from the controller's clock rather than the condition
	// helper's, because that timestamp is what the budget is measured against.
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               migration.ConditionRetrying,
		Status:             conditionStatus(step.Observation.Fault != nil),
		Reason:             retryingReason(step.Observation.Fault),
		Message:            retryingMessage(step.Observation.Fault),
		ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(r.now()),
	})

	if step.Preflight != nil {
		setCondition(&status.Conditions, generation, migration.ConditionPreflightPassed,
			conditionStatus(step.Preflight.Passed()), preflightReason(step.Preflight.Passed()),
			step.Preflight.Message())
	}
	if step.Verification != nil {
		status.Verification = step.Verification.APIVerification()
		status.Verification.VerifiedAt = ptr.To(metav1.NewTime(r.now()))
		setCondition(&status.Conditions, generation, migration.ConditionVerified,
			conditionStatus(step.Verification.Equivalent()),
			verifiedReason(step.Verification.Equivalent()), step.Verification.Message())
	}
	if step.LagBytes != nil {
		status.LagBytes = step.LagBytes
	}
	if step.Copied != nil {
		status.CopiedTables, status.TotalTables = step.Copied, step.Total
	}
	if step.PauseMillis != nil {
		status.PauseDurationMillis = step.PauseMillis
		migrationPauseSeconds.WithLabelValues(run.Tenant.Namespace, string(run.Strategy)).
			Observe(float64(*step.PauseMillis) / 1000)
	}
	if step.ClientPause != nil {
		status.ClientPauseMillis = ptr.To(step.ClientPause.Milliseconds())
	}
	if step.Queued != nil {
		status.QueuedClients = step.Queued
	}

	if decision.Phase == pgelasticv1alpha1.TenantMigrationPhaseQuiescing && quiesceStart(status) == nil {
		// The transition time is stamped from the controller's own clock rather than left to
		// the condition helper's, because this one timestamp is the start of the pause the
		// product is measured on. Two clocks would make the published pause the difference
		// between them.
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               migration.ConditionQuiesced,
			Status:             metav1.ConditionTrue,
			Reason:             migration.ReasonProgressing,
			Message:            "the tenant's clients are queued at the proxy with their sockets held open",
			ObservedGeneration: generation,
			LastTransitionTime: metav1.NewTime(r.now()),
		})
	}
	if decision.Phase == pgelasticv1alpha1.TenantMigrationPhaseCompleted && status.RollbackDeadline == nil {
		status.RollbackDeadline = ptr.To(metav1.NewTime(r.now().Add(run.RollbackWindow)))
	}
	if migration.Terminal(decision.Phase) && status.CompletedAt == nil {
		status.CompletedAt = ptr.To(metav1.NewTime(r.now()))
	}

	previousPhase := status.Phase
	status.Phase = decision.Phase
	// A settled decision leaves the conditions exactly as the reconcile that ended the
	// migration wrote them. The message saying why it failed is the most valuable thing it
	// leaves behind, and rewriting it every thirty seconds with "this migration is finished"
	// destroys the only record of the cause.
	if !decision.Settled {
		setCondition(&status.Conditions, generation, pgelasticv1alpha1.ConditionProgressing,
			conditionStatus(!migration.Terminal(decision.Phase)), decision.Reason, decision.Message)
		setCondition(&status.Conditions, generation, migration.ConditionSucceeded,
			conditionStatus(decision.Phase == pgelasticv1alpha1.TenantMigrationPhaseCompleted),
			decision.Reason, servingMessage(decision, run))
	}
	recordMigrationPhase(run.Migration.Namespace, run.Migration.Name, previousPhase, status,
		run.Plan, r.now())
}

func (r *PgTenantMigrationReconciler) publish(
	ctx context.Context,
	object *pgelasticv1alpha1.PgTenantMigration,
	status pgelasticv1alpha1.PgTenantMigrationStatus,
	result ctrl.Result,
) (ctrl.Result, error) {
	if equality.Semantic.DeepEqual(object.Status, status) {
		return result, nil
	}
	object.Status = status
	if err := r.Status().Update(ctx, object); err != nil {
		if apierrors.IsConflict(err) {
			// Somebody else wrote the object between the read and here, so this status was
			// computed against a revision that no longer exists. Returning the conflict as an
			// error would requeue at the rate limiter's five-millisecond base and re-run the
			// phase's whole effect against a cache that has not moved either; waiting for the
			// phase's own poll interval costs one interval and re-reads something newer.
			return ctrl.Result{RequeueAfter: result.RequeueAfter}, nil
		}
		return ctrl.Result{}, err
	}
	return result, nil
}

// reader is the API server wherever one was supplied, and the client itself otherwise: a
// caller that hands this reconciler an uncached client has already answered the question
// APIReader exists to answer.
func (r *PgTenantMigrationReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// clientPause asks the router how long it held the tenant's clients, when it is a router
// that holds anybody. A BindingRouter queues nobody and reports nothing, which is why this
// is an optional capability rather than a method every Router has to answer dishonestly.
func (r *PgTenantMigrationReconciler) clientPause(tenant migration.TenantRef) *time.Duration {
	reporter, ok := r.Router.(migration.PauseReporter)
	if !ok {
		return nil
	}
	held, reported := reporter.ClientPause(tenant)
	if !reported {
		return nil
	}
	return &held
}

func (r *PgTenantMigrationReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// SetupWithManager sets up the controller with the Manager.
func (r *PgTenantMigrationReconciler) ownership() ownership.Resolver {
	return ownership.Resolver{Reader: r.Client, ControllerName: r.ControllerName}
}

func (r *PgTenantMigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgTenantMigration{}).
		Named("pgtenantmigration").
		Complete(r)
}
