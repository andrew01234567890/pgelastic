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

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
)

// restoreRequeue paces a restore that is waiting for its target instance to finish
// recovering. Pulling a base backup down and replaying WAL takes minutes to hours, and
// nothing about it is made faster by asking more often.
const restoreRequeue = 15 * time.Second

// RecoveryInstanceFinalizer holds a tenant-scoped PgRestore open until the throwaway instance
// it recovered into has been removed.
//
// Without it finalize is unreachable: an object with no finalizers is deleted by the API
// server immediately, the reconciler observes NotFound rather than a deletion timestamp, and
// the recovery instance - a full, running, readable copy of every tenant on the source -
// outlives everything that knew about it. That matters most for a restore that failed, which
// deliberately leaves the instance standing as evidence.
const RecoveryInstanceFinalizer = "pgelastic.io/recovery-instance"

// PgRestoreReconciler puts an instance back, into a new instance.
//
// Recovery is never performed in place. The instance being recovered from is very often the
// only copy of the data, and a restore that ran over it would destroy the thing it was
// asked to save at the moment it went wrong.
type PgRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// SQL and Shell are the two ports a tenant-scoped restore copies through. They are the
	// migration engine's own ports, because lifting a database out of a recovered instance
	// and loading it over a live one is the offline migration copy with different ends.
	// Nil refuses a tenant-scoped restore and leaves instance-scoped ones working.
	SQL   migration.SQL
	Shell migration.Shell

	// ControllerName is this operator's identity.
	ControllerName string
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile converges one PgRestore.
func (r *PgRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	restore := &pgelasticv1alpha1.PgRestore{}
	if err := r.Get(ctx, req.NamespacedName, restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if result, stop, err := unclaimed(ctx, r.ownership(), r.Client, releaseOnly, restore); stop {
		return result, err
	}
	if !restore.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, restore)
	}
	// Before anything creates a recovery instance, so that deleting the restore can never
	// destroy the only record that there is one to clean up.
	if restore.Spec.Scope == pgelasticv1alpha1.RestoreScopeTenant &&
		controllerutil.AddFinalizer(restore, RecoveryInstanceFinalizer) {
		if err := r.Update(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
	}

	status := restore.Status.DeepCopy()
	status.ObservedGeneration = restore.Generation
	status.TargetInstance = restoreTargetInstance(restore)
	if restore.Spec.Scope == pgelasticv1alpha1.RestoreScopeTenant {
		status.TargetInstance = recoveryInstanceName(restore)
	}

	requeue, err := r.converge(ctx, restore, status)
	if err != nil {
		return ctrl.Result{}, err
	}

	status.Conditions = restoreConditions(restore, status)
	status.Phase = restorePhase(status)
	if err := r.publish(ctx, restore, status); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// publish writes the status out.
//
// Every other controller here does this once, at the end of the pass. A tenant restore also
// calls it in the middle of one: the record that a copy was cleared to overwrite the live
// tenant has to be on the API server before the copy runs, and the pass that writes it is the
// pass that runs it, so it cannot wait for the end.
func (r *PgRestoreReconciler) publish(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
	status *pgelasticv1alpha1.PgRestoreStatus,
) error {
	if equality.Semantic.DeepEqual(&restore.Status, status) {
		return nil
	}
	restore.Status = *status
	return r.Status().Update(ctx, restore)
}

// converge creates the target instance if it is not there and reports on it if it is.
//
// Every failure lands on the status rather than being returned. A restore whose source
// backup does not exist needs somebody to read the reason, not a faster retry loop.
func (r *PgRestoreReconciler) converge(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
	status *pgelasticv1alpha1.PgRestoreStatus,
) (time.Duration, error) {
	// A restore is a one-shot fact with an immutable spec, so once it has ended there is
	// nothing left to converge towards. Two things depend on saying so before anything else:
	// a target deleted after the restore finished is somebody's decision about an instance
	// rather than a regression of the restore that produced it, and - the reason this guard
	// is above the scope switch rather than below it - a finished tenant restore reached
	// here again on the very status write that recorded its success, and rebuilt the
	// recovery instance and loaded the dump back over the live tenant every time.
	if isTerminalRestore(status.Phase) {
		return 0, nil
	}

	// A copy was cleared to start and the restore never reached an ending, so the pass that
	// was running it did not survive: a rolled pod, a lost lease, a conflict on the terminal
	// write. The clearance is written and flushed by the same pass that then runs the copy,
	// so seeing it here at the top of a later pass means that copy did not finish. What it
	// managed to do to the live tenant before it went is not knowable from here - pg_restore
	// --clean drops as it goes - so this refuses rather than starting over. The recovery
	// instance is left up because it holds the intended contents, and the operator can finish
	// by hand or take another restore from it.
	if restore.Spec.Scope == pgelasticv1alpha1.RestoreScopeTenant && status.CopyStartedAt != nil {
		status.Phase = pgelasticv1alpha1.RestorePhaseFailed
		status.Error = fmt.Sprintf(
			"a copy into this tenant began at %s and did not report an ending, so it is not "+
				"known how much of the database it had already replaced. Refusing to run it "+
				"again, because it loads with --clean and would drop whatever has been "+
				"written since. PgInstance %q still holds the recovered copy",
			status.CopyStartedAt.UTC().Format(time.RFC3339), recoveryInstanceName(restore))
		return 0, nil
	}

	if restore.Spec.Scope == pgelasticv1alpha1.RestoreScopeTenant {
		return r.reconcileTenantRestore(ctx, restore, status)
	}

	target := &pgelasticv1alpha1.PgInstance{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: restore.Namespace, Name: status.TargetInstance,
	}, target)
	switch {
	case err == nil:
		return r.observeTarget(status, target), nil
	case !apierrors.IsNotFound(err):
		return 0, err
	}

	plan, reason, err := r.planRestore(ctx, restore)
	if err != nil {
		return 0, err
	}
	if reason != "" {
		status.Error = reason
		return restoreRequeue, nil
	}

	status.Error = ""
	status.BackupID = plan.Spec.Restore.BackupID
	if err := r.Create(ctx, plan); err != nil && !apierrors.IsAlreadyExists(err) {
		status.Error = err.Error()
		return restoreRequeue, nil
	}
	if err := r.handOverCredentials(ctx, restore.Spec.SourceInstanceRef.Name, plan); err != nil {
		status.Error = err.Error()
		return restoreRequeue, nil
	}
	return restoreRequeue, nil
}

// observeTarget reports what the instance being recovered into is doing.
func (r *PgRestoreReconciler) observeTarget(
	status *pgelasticv1alpha1.PgRestoreStatus,
	target *pgelasticv1alpha1.PgInstance,
) time.Duration {
	if meta.IsStatusConditionTrue(target.Status.Conditions, pgelasticv1alpha1.ConditionReady) {
		status.Phase = pgelasticv1alpha1.RestorePhaseCompleted
		status.Error = ""
		return 0
	}
	status.Phase = pgelasticv1alpha1.RestorePhaseRecovering
	return restoreRequeue
}

// planRestore builds the instance to recover into, and says why it cannot when it cannot.
//
// The repository, the stanza and the enforced-parameter floor all come from the backup
// rather than from the source instance, because a backup outlives its instance and the case
// a restore exists for is the case where the source is gone.
func (r *PgRestoreReconciler) planRestore(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
) (*pgelasticv1alpha1.PgInstance, string, error) {
	source := &pgelasticv1alpha1.PgInstance{}
	sourceErr := r.Get(ctx, types.NamespacedName{
		Namespace: restore.Namespace, Name: restore.Spec.SourceInstanceRef.Name,
	}, source)
	if sourceErr != nil && !apierrors.IsNotFound(sourceErr) {
		return nil, "", sourceErr
	}
	sourceExists := sourceErr == nil

	backup, reason, err := r.resolveBackup(ctx, restore, source, sourceExists)
	if err != nil || reason != "" {
		return nil, reason, err
	}

	repository := backup.Status.Repository
	if repository == nil && sourceExists && source.Spec.Backup != nil {
		repository = &source.Spec.Backup.ObjectStore
	}
	if repository == nil {
		return nil, "the backup does not record which repository it was written to, and " +
			"the source instance is gone, so there is nowhere to restore from", nil
	}
	if backup.Status.Stanza == "" {
		return nil, "the backup does not record its repository stanza, so the archive it " +
			"belongs to cannot be addressed", nil
	}
	if !sourceExists {
		return nil, "", fmt.Errorf(
			"restoring without the source instance needs its sizing, which is not yet "+
				"recorded on a backup; recreate %s first", restore.Spec.SourceInstanceRef.Name)
	}
	// The recovered instance inherits the source's role passwords, because the catalogue it
	// restores is the source's. Checking here means a missing Secret is a reason an operator
	// can read on the restore, rather than an instance that provisions and never comes up.
	credentials := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: restore.Namespace,
		Name:      provision.CredentialsSecretName(source.Name),
	}, credentials); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, "", err
		}
		return nil, fmt.Sprintf(
			"%s has no credentials Secret, and a restored instance's roles keep the "+
				"passwords its catalogue was backed up with, so there would be nothing able "+
				"to reach the recovered instance", source.Name), nil
	}

	// The backup records where it was written but not what to authenticate with: the agent
	// is configured with pgBackRest's own settings and never learns which Secret they were
	// projected from. Without this the recovered instance was given a repository it could
	// not open, and its bootstrap container died on a missing accessKeyID until the restore
	// timed out.
	if repository.CredentialsSecretRef.Name == "" && source.Spec.Backup != nil {
		withCredentials := *repository
		withCredentials.CredentialsSecretRef = source.Spec.Backup.ObjectStore.CredentialsSecretRef
		repository = &withCredentials
	}
	if repository.CredentialsSecretRef.Name == "" {
		return nil, fmt.Sprintf("neither the backup nor %s says which Secret holds the "+
			"object store's credentials, so the recovered instance could not read the "+
			"repository", source.Name), nil
	}

	instance := &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoreTargetInstance(restore),
			Namespace: restore.Namespace,
		},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef:          source.Spec.PoolRef,
			Class:            source.Spec.Class,
			PostgresVersion:  source.Spec.PostgresVersion,
			HighAvailability: source.Spec.HighAvailability,
			Storage:          source.Spec.Storage,
			Resources:        source.Spec.Resources,
			Parameters:       source.Spec.Parameters,
			// The recovering instance reads the source's archive through exactly the
			// configuration an archiving instance writes it with, because read access and
			// write refusal are the same credential. What stops it writing is behavioural:
			// the instance manager declines to archive at all while spec.restore is set.
			Backup: &pgelasticv1alpha1.InstanceBackup{
				ObjectStore:   *repository,
				Retention:     retentionOf(source),
				BackupStandby: backupStandbyOf(source),
			},
			Restore: &pgelasticv1alpha1.InstanceRestore{
				SourceInstanceName:     restore.Spec.SourceInstanceRef.Name,
				Stanza:                 backup.Status.Stanza,
				BackupID:               backup.Status.BackupID,
				Target:                 restore.Spec.Target,
				EnforcedParameterFloor: backup.Status.SourceEnforcedParameters,
			},
			// A restored instance is evidence until somebody has looked at it, not
			// capacity. Placing tenants onto one that has just replayed to an arbitrary
			// point would move live customers onto a copy of their own past.
			Admission: &pgelasticv1alpha1.InstanceAdmission{Schedulable: ptr.To(false)},
		},
	}
	return instance, "", nil
}

// handOverCredentials gives the instance this restore just created the source's role
// passwords, which are the ones its restored catalogue holds.
//
// It lives here rather than in the instance controller because of who is allowed to ask.
// spec.restore is a plain field on a namespaced object, so an instance controller that
// copied the Secret it names would hand anybody who can create a PgInstance the replication,
// ops and rewind passwords of any instance they cared to name - all three live against the
// instance they came from, and reachable over the network from anywhere pg_hba admits. This
// path is reached only after resolveBackup has found a completed backup belonging to that
// same source.
//
// The Secret is owned by the instance, so it is collected with it - which matters most for
// the throwaway instance a tenant restore recovers into.
func (r *PgRestoreReconciler) handOverCredentials(
	ctx context.Context,
	source string,
	instance *pgelasticv1alpha1.PgInstance,
) error {
	target := provision.CredentialsSecretName(instance.Name)
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Namespace: instance.Namespace, Name: target}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	from := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: instance.Namespace, Name: provision.CredentialsSecretName(source),
	}, from); err != nil {
		return fmt.Errorf("reading %s's credentials, which %s's restored catalogue holds: %w",
			source, instance.Name, err)
	}

	handed := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: target, Namespace: instance.Namespace},
		Type:       from.Type,
		Data:       from.Data,
	}
	if err := controllerutil.SetControllerReference(instance, handed, r.Scheme); err != nil {
		return err
	}
	return client.IgnoreAlreadyExists(r.Create(ctx, handed))
}

// resolveBackup finds the base backup this restore starts from.
//
// A named backup is used as named. An unnamed one is chosen from the instance's completed
// backups as the newest that finished before the target time - which is why the API refuses
// an unnamed backup for any target that cannot be ordered.
func (r *PgRestoreReconciler) resolveBackup(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
	source *pgelasticv1alpha1.PgInstance,
	sourceExists bool,
) (*pgelasticv1alpha1.PgBackup, string, error) {
	if named := restore.Spec.BackupRef; named != nil {
		backup := &pgelasticv1alpha1.PgBackup{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: restore.Namespace, Name: named.Name,
		}, backup)
		switch {
		case apierrors.IsNotFound(err):
			return nil, fmt.Sprintf("no backup named %s exists in this namespace", named.Name), nil
		case err != nil:
			return nil, "", err
		case backup.Status.Phase != pgelasticv1alpha1.BackupPhaseCompleted:
			return nil, fmt.Sprintf("%s is %s rather than Completed, so there is nothing to "+
				"restore from", named.Name, backup.Status.Phase), nil
		case backup.Spec.InstanceRef.Name != restore.Spec.SourceInstanceRef.Name:
			// The unnamed branch filters by instance; naming one skipped the check entirely.
			// A backup of somebody else carries somebody else's stanza and repository, so the
			// recovered instance would be a copy of an instance this restore never named -
			// and the credentials handed to it are the named source's, which its restored
			// catalogue has never seen.
			return nil, fmt.Sprintf("%s is a backup of %s, not of %s", named.Name,
				backup.Spec.InstanceRef.Name, restore.Spec.SourceInstanceRef.Name), nil
		}
		return backup, "", nil
	}

	if !sourceExists {
		return nil, fmt.Sprintf("no backup was named and %s no longer exists, so there is "+
			"no catalogue to choose one from", restore.Spec.SourceInstanceRef.Name), nil
	}
	list := &pgelasticv1alpha1.PgBackupList{}
	if err := r.List(ctx, list, client.InNamespace(restore.Namespace)); err != nil {
		return nil, "", err
	}
	chosen := newestBackupBefore(list.Items, source.Name, restoreTargetTime(restore))
	if chosen == nil {
		return nil, fmt.Sprintf("no completed backup of %s ends before the requested target",
			source.Name), nil
	}
	return chosen, "", nil
}

// newestBackupBefore picks the last backup that finished before the target.
//
// Before, not nearest. A backup that ended after the target cannot be replayed backwards to
// reach it, so "closest" would silently choose one that lands past the moment asked for.
func newestBackupBefore(
	backups []pgelasticv1alpha1.PgBackup,
	instance string,
	target *time.Time,
) *pgelasticv1alpha1.PgBackup {
	var chosen *pgelasticv1alpha1.PgBackup
	for i := range backups {
		candidate := &backups[i]
		if candidate.Spec.InstanceRef.Name != instance ||
			candidate.Status.Phase != pgelasticv1alpha1.BackupPhaseCompleted ||
			candidate.Status.StoppedAt == nil {
			continue
		}
		if target != nil && !candidate.Status.StoppedAt.Time.Before(*target) {
			continue
		}
		if chosen == nil || candidate.Status.StoppedAt.After(chosen.Status.StoppedAt.Time) {
			chosen = candidate
		}
	}
	return chosen
}

// restoreTargetTime reads the target time, if the target is one. Anything else has no
// ordering against the catalogue and reaches here only with a named backup.
func restoreTargetTime(restore *pgelasticv1alpha1.PgRestore) *time.Time {
	if restore.Spec.Target == nil || restore.Spec.Target.Time == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, restore.Spec.Target.Time)
	if err != nil {
		return nil
	}
	return &parsed
}

// backupStandbyOf guards the same nil retentionOf does. A source whose spec.backup was
// removed after its backups were taken still has a repository recorded on those backups, so
// this is reachable the moment anything records the credentials reference there too.
func backupStandbyOf(source *pgelasticv1alpha1.PgInstance) *bool {
	if source.Spec.Backup == nil {
		return nil
	}
	return source.Spec.Backup.BackupStandby
}

func retentionOf(source *pgelasticv1alpha1.PgInstance) *pgelasticv1alpha1.RetentionPolicy {
	if source.Spec.Backup == nil {
		return nil
	}
	return source.Spec.Backup.Retention
}

// targetInstanceName is what the recovered instance is called, defaulting to the restore's
// own name so that two restores cannot collide on one instance without saying so.
func restoreTargetInstance(restore *pgelasticv1alpha1.PgRestore) string {
	if restore.Spec.TargetInstanceName != "" {
		return restore.Spec.TargetInstanceName
	}
	return restore.Name
}

func restoreConditions(
	restore *pgelasticv1alpha1.PgRestore,
	status *pgelasticv1alpha1.PgRestoreStatus,
) []metav1.Condition {
	// A restore that was planned and then failed part-way through the copy was accepted.
	// What went wrong belongs on Ready, because reporting it as a preflight refusal would
	// send whoever reads it back to check a spec that was never the problem.
	failed := status.Phase == pgelasticv1alpha1.RestorePhaseFailed
	accepted := status.Error == "" || failed
	reason, message := pgelasticv1alpha1.ReasonAccepted, "the restore can be planned"
	if !accepted {
		reason, message = pgelasticv1alpha1.ReasonPreflightFailed, status.Error
	}

	done := isTerminalRestore(status.Phase)
	conditions := make([]metav1.Condition, 0, 3)
	conditions = append(conditions,
		newCondition(status.Conditions, pgelasticv1alpha1.ConditionAccepted, accepted,
			restore.Generation, reason, message),
		newCondition(status.Conditions, pgelasticv1alpha1.ConditionProgressing,
			accepted && !done, restore.Generation, progressingRestoreReason(status),
			fmt.Sprintf("%s is recovering", status.TargetInstance)),
		newCondition(status.Conditions, pgelasticv1alpha1.ConditionReady,
			status.Phase == pgelasticv1alpha1.RestorePhaseCompleted,
			restore.Generation, readyRestoreReason(status), readyRestoreMessage(status)),
	)
	return conditions
}

func progressingRestoreReason(status *pgelasticv1alpha1.PgRestoreStatus) string {
	if status.Phase == pgelasticv1alpha1.RestorePhaseRecovering {
		return pgelasticv1alpha1.ReasonRunning
	}
	return pgelasticv1alpha1.ReasonPending
}

func readyRestoreReason(status *pgelasticv1alpha1.PgRestoreStatus) string {
	switch status.Phase {
	case pgelasticv1alpha1.RestorePhaseCompleted:
		return pgelasticv1alpha1.ReasonReady
	case pgelasticv1alpha1.RestorePhaseFailed:
		return pgelasticv1alpha1.ReasonRestoreFailed
	default:
		return pgelasticv1alpha1.ReasonPending
	}
}

func readyRestoreMessage(status *pgelasticv1alpha1.PgRestoreStatus) string {
	if status.Phase == pgelasticv1alpha1.RestorePhaseFailed {
		return status.Error
	}
	return fmt.Sprintf("%s is serving on its own timeline", status.TargetInstance)
}

// isTerminalRestore reports whether a restore has ended, either way. A restore is a one-shot
// fact and its spec is immutable, so trying again means creating another one.
func isTerminalRestore(phase pgelasticv1alpha1.RestorePhase) bool {
	return phase == pgelasticv1alpha1.RestorePhaseCompleted ||
		phase == pgelasticv1alpha1.RestorePhaseFailed
}

// restorePhase derives the display phase. A restore that cannot be planned is Preflight
// with the reason on its Accepted condition rather than Failed, because every one of those
// reasons is something an operator can put right without starting again - unlike a copy that
// failed half way, which is terminal and says so.
func restorePhase(status *pgelasticv1alpha1.PgRestoreStatus) pgelasticv1alpha1.RestorePhase {
	switch {
	case isTerminalRestore(status.Phase):
		return status.Phase
	case status.Error != "":
		return pgelasticv1alpha1.RestorePhasePreflight
	case status.Phase == "":
		return pgelasticv1alpha1.RestorePhasePreflight
	default:
		return status.Phase
	}
}

// finalize tears the throwaway recovery instance down.
//
// A tenant restore abandoned halfway leaves a full copy of every tenant on the source
// instance running and readable, and nothing else would ever remove it.
func (r *PgRestoreReconciler) finalize(
	ctx context.Context,
	restore *pgelasticv1alpha1.PgRestore,
) error {
	if restore.Spec.Scope != pgelasticv1alpha1.RestoreScopeTenant {
		return nil
	}
	if err := r.deleteRecoveryInstance(ctx, restore); err != nil {
		return err
	}
	if !controllerutil.RemoveFinalizer(restore, RecoveryInstanceFinalizer) {
		return nil
	}
	return r.Update(ctx, restore)
}

func (r *PgRestoreReconciler) ownership() ownership.Resolver {
	return ownership.Resolver{Reader: r.Client, ControllerName: r.ControllerName}
}

// SetupWithManager wires the controller. It owns the instance it creates in the watch sense
// only: the instance carries no owner reference, because deleting the restore record must
// not delete the instance it recovered.
func (r *PgRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgRestore{}).
		Named("pgrestore").
		Complete(r)
}
