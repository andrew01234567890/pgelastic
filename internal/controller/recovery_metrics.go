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
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// restorePhaseNames is every phase a restore's series are written for.
var restorePhaseNames = []string{
	string(pgelasticv1alpha1.RestorePhasePreflight),
	string(pgelasticv1alpha1.RestorePhaseRecovering),
	string(pgelasticv1alpha1.RestorePhaseExtracting),
	string(pgelasticv1alpha1.RestorePhaseLoading),
	string(pgelasticv1alpha1.RestorePhaseCompleted),
	string(pgelasticv1alpha1.RestorePhaseFailed),
}

// backupPhaseNames is every phase a backup's series are written for.
var backupPhaseNames = []string{
	string(pgelasticv1alpha1.BackupPhasePending),
	string(pgelasticv1alpha1.BackupPhaseRunning),
	string(pgelasticv1alpha1.BackupPhaseCompleted),
	string(pgelasticv1alpha1.BackupPhaseFailed),
}

// recordRestorePhase reports a restore as a transition. A restore is the other thing a pool
// does that takes minutes and moves a database somewhere - it is the same shape as a
// migration and belongs on the same timeline.
//
// The route runs from the instance the backup was taken from to the instance being recovered
// onto, which is the pair somebody watching a recovery wants to see. The target is empty
// until the restore has decided on one, and Route declines to write a half-empty pair, so a
// restore advertises no route until it has both.
//
// No duration is reported: PgRestoreStatus carries no start time for the restore as a whole,
// only copyStartedAt for the tenant-scope copy. Zero is how the histogram is told to skip an
// observation, which is the honest answer rather than a fabricated one.
func recordRestorePhase(
	namespace, name string,
	previous pgelasticv1alpha1.RestorePhase,
	status *pgelasticv1alpha1.PgRestoreStatus,
	source string,
	now time.Time,
) {
	recordTransition(transition{
		Namespace: namespace,
		Kind:      kindRestore,
		Name:      name,
		Previous:  string(previous),
		Current:   string(status.Phase),
		Phases:    restorePhaseNames,
		From:      source,
		To:        status.TargetInstance,
	}, func(phase string) bool {
		return isTerminalRestore(pgelasticv1alpha1.RestorePhase(phase))
	}, now)
}

// recordBackupPhase reports a backup as a transition.
//
// A backup carries no route. It does not move a database between two places, and labelling
// it with the instance it was taken from twice would be a route in shape and not in meaning.
// The instance is already on every other backup series through the object's own name.
func recordBackupPhase(
	namespace, name string,
	previous pgelasticv1alpha1.BackupPhase,
	status *pgelasticv1alpha1.PgBackupStatus,
	now time.Time,
) {
	recordTransition(transition{
		Namespace: namespace,
		Kind:      kindBackup,
		Name:      name,
		Previous:  string(previous),
		Current:   string(status.Phase),
		Phases:    backupPhaseNames,
		Took:      backupDuration(status),
	}, func(phase string) bool {
		return isTerminalBackup(pgelasticv1alpha1.BackupPhase(phase))
	}, now)
}

func isTerminalBackup(phase pgelasticv1alpha1.BackupPhase) bool {
	return phase == pgelasticv1alpha1.BackupPhaseCompleted ||
		phase == pgelasticv1alpha1.BackupPhaseFailed
}

// backupDuration is how long the backup itself ran. Both times are read back from the
// pgBackRest catalogue rather than measured by the operator, so this is the backup's own
// duration and not the reconcile loop's view of it.
func backupDuration(status *pgelasticv1alpha1.PgBackupStatus) time.Duration {
	if status.StartedAt == nil || status.StoppedAt == nil {
		return 0
	}
	return status.StoppedAt.Sub(status.StartedAt.Time)
}
