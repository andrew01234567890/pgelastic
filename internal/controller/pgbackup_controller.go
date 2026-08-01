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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
)

// backupRequeue paces a backup waiting to be taken. It is a floor rather than the
// mechanism: a member claiming one writes the status, and the watch carries that back.
const backupRequeue = 30 * time.Second

// PgBackupReconciler reports on one physical backup.
//
// It never takes one. The backup runs inside the member's Pod, claimed by that member's own
// agent, and this controller's whole job is to say what is true about it - which member has
// it, whether anybody still does, and why it has not started.
type PgBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// ControllerName is this operator's identity. A backup reaches a PgElasticClass through
	// its instance's pool, and one naming a different controller is left entirely alone.
	ControllerName string
}

// +kubebuilder:rbac:groups=pgelastic.io,resources=pgbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgelastic.io,resources=pgbackups/status,verbs=get;update;patch

// Reconcile converges one PgBackup's status.
func (r *PgBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	backup := &pgelasticv1alpha1.PgBackup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if result, stop, err := unclaimed(ctx, r.ownership(), r.Client, finalizeAnyway, backup); stop {
		return result, err
	}
	if !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	instance := &pgelasticv1alpha1.PgInstance{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: backup.Namespace, Name: backup.Spec.InstanceRef.Name,
	}, instance)
	switch {
	case apierrors.IsNotFound(err):
		instance = nil
	case err != nil:
		return ctrl.Result{}, err
	}

	status := backup.Status.DeepCopy()
	status.ObservedGeneration = backup.Generation
	r.orphanIfNobodyIsTakingIt(status, instance)
	status.Conditions = backupConditions(backup, status, instance)
	status.Phase = backupPhase(status)

	if !equality.Semantic.DeepEqual(&backup.Status, status) {
		backup.Status = *status
		if err := r.Status().Update(ctx, backup); err != nil {
			return ctrl.Result{}, err
		}
	}
	if status.Phase == pgelasticv1alpha1.BackupPhasePending {
		return ctrl.Result{RequeueAfter: backupRequeue}, nil
	}
	return ctrl.Result{}, nil
}

// orphanIfNobodyIsTakingIt fails a backup whose taker no longer exists.
//
// A backup runs as a goroutine inside a process that can restart, and a restarted agent has
// no memory of what its predecessor was doing. Without this, an agent that died mid-backup
// leaves the object Running forever: it never completes, never fails, and - because a
// running backup holds the election - no later backup is ever taken either. That is the
// bug CloudNativePG shipped a session identifier to fix, and it is worth having before
// rather than after.
//
// The evidence is the member's own published session against the one recorded when the
// backup was claimed. A member that is not reporting at all is not evidence: an unreachable
// agent is very often a working agent behind a broken network, and failing its backup would
// be a decision made on silence.
func (r *PgBackupReconciler) orphanIfNobodyIsTakingIt(
	status *pgelasticv1alpha1.PgBackupStatus,
	instance *pgelasticv1alpha1.PgInstance,
) {
	if status.Phase != pgelasticv1alpha1.BackupPhaseRunning || status.AgentSession == "" {
		return
	}
	if instance == nil {
		return
	}
	for _, member := range instance.Status.Instances {
		if member.Name != status.Member {
			continue
		}
		if member.AgentSession == "" || member.AgentSession == status.AgentSession {
			return
		}
		status.Phase = pgelasticv1alpha1.BackupPhaseFailed
		status.Error = fmt.Sprintf(
			"the instance manager on %s restarted while this backup was running; "+
				"a backup does not survive the process taking it", status.Member)
		return
	}
}

// backupConditions says what is true about a backup, and in the pending case why.
//
// The reason matters more than the phase here, because Pending covers two situations that
// call for entirely different actions: nobody has picked the backup up yet, which resolves
// itself, and archiving is not working, which does not.
func backupConditions(
	backup *pgelasticv1alpha1.PgBackup,
	status *pgelasticv1alpha1.PgBackupStatus,
	instance *pgelasticv1alpha1.PgInstance,
) []metav1.Condition {
	generation := backup.Generation
	accepted, acceptedReason, acceptedMessage := backupAccepted(backup, instance)

	conditions := make([]metav1.Condition, 0, 3)
	conditions = append(conditions,
		newCondition(status.Conditions, pgelasticv1alpha1.ConditionAccepted, accepted,
			generation, acceptedReason, acceptedMessage))

	done := status.Phase == pgelasticv1alpha1.BackupPhaseCompleted ||
		status.Phase == pgelasticv1alpha1.BackupPhaseFailed
	conditions = append(conditions, newCondition(status.Conditions,
		pgelasticv1alpha1.ConditionProgressing, accepted && !done, generation,
		progressReason(status, instance), progressMessage(status, instance)))

	conditions = append(conditions, newCondition(status.Conditions,
		pgelasticv1alpha1.ConditionReady,
		status.Phase == pgelasticv1alpha1.BackupPhaseCompleted, generation,
		readyBackupReason(status), readyBackupMessage(status)))
	return conditions
}

func backupAccepted(
	backup *pgelasticv1alpha1.PgBackup,
	instance *pgelasticv1alpha1.PgInstance,
) (bool, string, string) {
	switch {
	case instance == nil:
		return false, pgelasticv1alpha1.ReasonInstanceMissing,
			fmt.Sprintf("no instance named %s exists in this namespace",
				backup.Spec.InstanceRef.Name)
	case instance.Spec.Backup == nil:
		return false, pgelasticv1alpha1.ReasonNoRepository,
			fmt.Sprintf("%s has no repository to back up into", instance.Name)
	default:
		return true, pgelasticv1alpha1.ReasonAccepted, "the instance has a repository"
	}
}

func progressReason(
	status *pgelasticv1alpha1.PgBackupStatus,
	instance *pgelasticv1alpha1.PgInstance,
) string {
	switch {
	case status.Phase == pgelasticv1alpha1.BackupPhaseRunning:
		return pgelasticv1alpha1.ReasonRunning
	case instance != nil && !archivingWorks(instance):
		return pgelasticv1alpha1.ReasonArchiveDegraded
	default:
		return pgelasticv1alpha1.ReasonPending
	}
}

// progressMessage says what a backup is waiting for.
//
// The archiving case is the one worth spelling out. A base backup needs every WAL segment
// from its own start position to reach consistency, so one taken while archiving is broken
// is not a backup at all - it is an object in a bucket that no restore can use, and taking
// it would be worse than not, because it would look like progress.
func progressMessage(
	status *pgelasticv1alpha1.PgBackupStatus,
	instance *pgelasticv1alpha1.PgInstance,
) string {
	switch {
	case status.Phase == pgelasticv1alpha1.BackupPhaseRunning:
		return fmt.Sprintf("%s is taking this backup", status.Member)
	case instance != nil && !archivingWorks(instance):
		return "waiting for WAL archiving to work: a base backup taken now could not be " +
			"replayed to consistency, because the WAL it needs is not reaching the repository"
	case instance != nil && instance.Status.PendingBackup != nil &&
		instance.Status.PendingBackup.Name != "":
		return fmt.Sprintf("waiting for %s to claim it",
			instance.Status.PendingBackup.Member)
	default:
		return "waiting for a member to be elected"
	}
}

func readyBackupReason(status *pgelasticv1alpha1.PgBackupStatus) string {
	switch status.Phase {
	case pgelasticv1alpha1.BackupPhaseCompleted:
		return pgelasticv1alpha1.ReasonBackupCompleted
	case pgelasticv1alpha1.BackupPhaseFailed:
		return pgelasticv1alpha1.ReasonBackupFailed
	default:
		return pgelasticv1alpha1.ReasonPending
	}
}

func readyBackupMessage(status *pgelasticv1alpha1.PgBackupStatus) string {
	switch status.Phase {
	case pgelasticv1alpha1.BackupPhaseCompleted:
		return fmt.Sprintf("%s covers WAL %s through %s",
			status.BackupID, status.BeginWAL, status.EndWAL)
	case pgelasticv1alpha1.BackupPhaseFailed:
		return status.Error
	default:
		return "this backup has not been taken"
	}
}

// backupPhase carries forward whatever the member wrote. The member is the only thing that
// knows whether a backup is running, and a controller that recomputed the phase from its own
// view would overwrite the terminal status an agent had just written - which is the shape of
// every stuck-forever bug CloudNativePG fixed across two releases.
func backupPhase(status *pgelasticv1alpha1.PgBackupStatus) pgelasticv1alpha1.BackupPhase {
	if status.Phase == "" {
		return pgelasticv1alpha1.BackupPhasePending
	}
	return status.Phase
}

// newCondition builds one condition, preserving the transition time across a status that
// did not change.
func newCondition(
	existing []metav1.Condition,
	conditionType string,
	ok bool,
	generation int64,
	reason, message string,
) metav1.Condition {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus(ok),
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.Now(),
	}
	if previous := findCondition(existing, conditionType); previous != nil &&
		previous.Status == condition.Status && !previous.LastTransitionTime.IsZero() {
		condition.LastTransitionTime = previous.LastTransitionTime
	}
	return condition
}

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func (r *PgBackupReconciler) ownership() ownership.Resolver {
	return ownership.Resolver{Reader: r.Client, ControllerName: r.ControllerName}
}

// SetupWithManager wires the controller.
//
// It watches PgInstance as well as PgBackup, because most of what a backup is waiting for is
// recorded on its instance: the election, and whether archiving works. Without the watch a
// backup's own message would only refresh on its requeue heartbeat, and would spend most of
// its life saying something that had stopped being true.
func (r *PgBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pgelasticv1alpha1.PgBackup{}).
		Watches(&pgelasticv1alpha1.PgInstance{},
			handler.EnqueueRequestsFromMapFunc(r.backupsOfInstance)).
		Named("pgbackup").
		Complete(r)
}

func (r *PgBackupReconciler) backupsOfInstance(
	ctx context.Context,
	object client.Object,
) []reconcile.Request {
	list := &pgelasticv1alpha1.PgBackupList{}
	if err := r.List(ctx, list,
		client.InNamespace(object.GetNamespace()),
		client.MatchingFields{index.BackupByInstance: object.GetName()},
	); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&list.Items[i]),
		})
	}
	return requests
}
