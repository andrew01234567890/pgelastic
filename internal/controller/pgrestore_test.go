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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
)

func completedAt(name string, stopped time.Time) pgelasticv1alpha1.PgBackup {
	at := metav1.NewTime(stopped)
	return pgelasticv1alpha1.PgBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: pgelasticv1alpha1.PgBackupSpec{
			InstanceRef: corev1.LocalObjectReference{Name: "pg-a"},
		},
		Status: pgelasticv1alpha1.PgBackupStatus{
			Phase:     pgelasticv1alpha1.BackupPhaseCompleted,
			StoppedAt: &at,
		},
	}
}

// Before, not nearest. A backup that ended after the target cannot be replayed backwards to
// reach it, so choosing the closest would silently land past the moment asked for - and a
// restore that overshoots is a restore that did not undo what it was asked to undo.
func TestTheBaseBackupChosenIsTheLastOneThatEndedBeforeTheTarget(t *testing.T) {
	base := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	backups := []pgelasticv1alpha1.PgBackup{
		completedAt("far-before", base.Add(-4*time.Hour)),
		completedAt("just-before", base.Add(-time.Hour)),
		completedAt("just-after", base.Add(time.Minute)),
	}
	target := base
	chosen := newestBackupBefore(backups, "pg-a", &target)
	if chosen == nil || chosen.Name != "just-before" {
		t.Fatalf("chose %v, want the last backup that ended before the target", chosen)
	}
}

func TestNoBaseBackupWhenEveryOneEndedAfterTheTarget(t *testing.T) {
	base := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	target := base.Add(-time.Hour)
	if chosen := newestBackupBefore(
		[]pgelasticv1alpha1.PgBackup{completedAt("after", base)}, "pg-a", &target,
	); chosen != nil {
		t.Fatalf("chose %s, want nothing restorable to that moment", chosen.Name)
	}
}

// A restore with no target replays everything the archive holds, so the newest backup is
// the right place to start and there is no time to compare against.
func TestWithNoTargetTheNewestBackupIsChosen(t *testing.T) {
	base := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	chosen := newestBackupBefore([]pgelasticv1alpha1.PgBackup{
		completedAt("older", base.Add(-time.Hour)),
		completedAt("newer", base),
	}, "pg-a", nil)
	if chosen == nil || chosen.Name != "newer" {
		t.Fatalf("chose %v, want the newest", chosen)
	}
}

func TestBackupsOfAnotherInstanceAreNeverChosen(t *testing.T) {
	base := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	somebodyElse := completedAt("somebody-else", base)
	somebodyElse.Spec.InstanceRef.Name = "pg-b"
	if chosen := newestBackupBefore(
		[]pgelasticv1alpha1.PgBackup{somebodyElse}, "pg-a", nil,
	); chosen != nil {
		t.Fatalf("chose %s, a backup of a different instance", chosen.Name)
	}
}

// A backup that never completed has no base backup in the repository behind it. Restoring
// from one would fail after the download rather than before it.
func TestUnfinishedBackupsAreNeverChosen(t *testing.T) {
	base := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	running := completedAt("running", base)
	running.Status.Phase = pgelasticv1alpha1.BackupPhaseRunning
	failed := completedAt("failed", base)
	failed.Status.Phase = pgelasticv1alpha1.BackupPhaseFailed

	if chosen := newestBackupBefore(
		[]pgelasticv1alpha1.PgBackup{running, failed}, "pg-a", nil,
	); chosen != nil {
		t.Fatalf("chose %s, which was never written to the repository", chosen.Name)
	}
}

// Only a time can be ordered against the catalogue. Anything else reaches the planner with
// a named backup, so reading a target time out of an LSN or a restore point would be
// comparing a moment against something that is not one.
func TestOnlyATimeTargetOrdersAgainstTheCatalogue(t *testing.T) {
	moment := "2026-07-31T12:00:00Z"
	withTime := &pgelasticv1alpha1.PgRestore{
		Spec: pgelasticv1alpha1.PgRestoreSpec{
			Target: &pgelasticv1alpha1.RecoveryTarget{Time: moment},
		},
	}
	if got := restoreTargetTime(withTime); got == nil || got.Format(time.RFC3339) != moment {
		t.Errorf("target time = %v, want %s", got, moment)
	}

	for _, target := range []*pgelasticv1alpha1.RecoveryTarget{
		nil,
		{LSN: "0/2000028"},
		{Name: "before-the-migration"},
		{XID: "4711"},
		{Time: "half past three"},
	} {
		restore := &pgelasticv1alpha1.PgRestore{
			Spec: pgelasticv1alpha1.PgRestoreSpec{Target: target},
		}
		if got := restoreTargetTime(restore); got != nil {
			t.Errorf("target %+v produced a time %v", target, got)
		}
	}
}

// The name has to be stable and derivable, because it is what the reconcile looks the
// target instance up by on every pass. A generated one would create a second instance every
// time the restore was reconciled.
func TestTheTargetInstanceNameDefaultsToTheRestoresOwn(t *testing.T) {
	restore := &pgelasticv1alpha1.PgRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "recover-friday"},
	}
	if got := restoreTargetInstance(restore); got != "recover-friday" {
		t.Errorf("target = %q, want the restore's own name", got)
	}
	restore.Spec.TargetInstanceName = "pg-a-recovered"
	if got := restoreTargetInstance(restore); got != "pg-a-recovered" {
		t.Errorf("target = %q, want the name that was asked for", got)
	}
}

// A restore that cannot be planned is Preflight with the reason on its Accepted condition,
// not Failed. Every one of those reasons - a backup that does not exist, one that never
// finished, a repository the backup did not record - is something an operator can put right
// without starting the restore again.
func TestAnUnplannableRestoreIsPreflightRatherThanFailed(t *testing.T) {
	status := &pgelasticv1alpha1.PgRestoreStatus{Error: "no backup named friday exists"}
	if got := restorePhase(status); got != pgelasticv1alpha1.RestorePhasePreflight {
		t.Fatalf("phase = %q, want Preflight", got)
	}

	restore := &pgelasticv1alpha1.PgRestore{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	conditions := restoreConditions(restore, status)
	accepted := findCondition(conditions, pgelasticv1alpha1.ConditionAccepted)
	if accepted == nil || accepted.Status != metav1.ConditionFalse {
		t.Fatalf("accepted = %v, want False", accepted)
	}
	if accepted.Message != status.Error {
		t.Errorf("message = %q, want the reason it could not be planned", accepted.Message)
	}
	if accepted.Reason != pgelasticv1alpha1.ReasonPreflightFailed {
		t.Errorf("reason = %q", accepted.Reason)
	}
}

// A restore whose target instance was deleted afterwards stays Completed. Deleting a
// recovered instance is a decision about that instance, not a regression of the restore
// that produced it, and flipping back would make the record lie about what happened.
func TestACompletedRestoreStaysCompleted(t *testing.T) {
	status := &pgelasticv1alpha1.PgRestoreStatus{
		Phase: pgelasticv1alpha1.RestorePhaseCompleted,
	}
	if got := restorePhase(status); got != pgelasticv1alpha1.RestorePhaseCompleted {
		t.Fatalf("phase = %q, want it to stay Completed", got)
	}
}

// An instance recovering from a repository has that repository configured so it can read
// from it. It carries its source's system identifier, so it addresses the source's stanza
// while running on a forked timeline: a backup taken from here lands in somebody else's
// archive and leaves neither instance restorable.
func TestARecoveringInstanceIsNeverScheduledForBackups(t *testing.T) {
	reconciler := &PgInstanceReconciler{}
	instance := &pgelasticv1alpha1.PgInstance{
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			Backup:  &pgelasticv1alpha1.InstanceBackup{},
			Restore: &pgelasticv1alpha1.InstanceRestore{Stanza: "pgelastic-99"},
		},
		Status: pgelasticv1alpha1.PgInstanceStatus{
			CurrentPrimary: "pg-a-1",
			ArchiveHealth:  &pgelasticv1alpha1.ArchiveHealthStatus{Healthy: true},
		},
	}
	state := reconciler.reconcileBackups(t.Context(), instance, ha.Decision{ServingPrimary: "pg-a-1"})
	if state.pending != nil {
		t.Fatalf("elected %+v, want a recovering instance left alone", state.pending)
	}
}
