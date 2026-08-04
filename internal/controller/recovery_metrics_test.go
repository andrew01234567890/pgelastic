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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// A restore is the other thing a pool does that takes minutes and puts a database somewhere
// new. Somebody watching one wants the same two answers a migration gives: how far it has
// got, and which instance it is landing on.
func TestARestoreReportsItsProgressAndWhereItIsLanding(t *testing.T) {
	const name = "restore-acme"
	status := &pgelasticv1alpha1.PgRestoreStatus{
		Phase:          pgelasticv1alpha1.RestorePhaseRecovering,
		TargetInstance: "gp-recovered",
	}

	recordRestorePhase(metricNamespace, name,
		pgelasticv1alpha1.RestorePhasePreflight, status, "gp-live", time.Unix(1000, 0))

	if labels := routeLabels(t, name); labels[labelFrom] != "gp-live" ||
		labels[labelTo] != "gp-recovered" {
		t.Errorf("the restore reads from=%q to=%q, want gp-live to gp-recovered",
			labels[labelFrom], labels[labelTo])
	}

	before := outcomeTotalFor(t, kindRestore, "Completed")
	status.Phase = pgelasticv1alpha1.RestorePhaseCompleted
	recordRestorePhase(metricNamespace, name,
		pgelasticv1alpha1.RestorePhaseLoading, status, "gp-live", time.Unix(1200, 0))

	if after := outcomeTotalFor(t, kindRestore, "Completed"); after != before+1 {
		t.Errorf("a finished restore counted %v, want one", after-before)
	}
	if labels := routeLabels(t, name); labels != nil {
		t.Errorf("a finished restore still advertises a route: %v", labels)
	}
}

// A backup does not go anywhere. Labelling it with the instance it was taken from at both
// ends would be a route in shape and not in meaning, and it would put a series on the "what
// is moving between instances" panel that is not moving between instances.
func TestABackupIsATransitionWithNoRoute(t *testing.T) {
	const name = "backup-nightly"
	status := &pgelasticv1alpha1.PgBackupStatus{
		Phase:     pgelasticv1alpha1.BackupPhaseRunning,
		StartedAt: ptr.To(metav1.NewTime(time.Unix(1000, 0))),
		StoppedAt: ptr.To(metav1.NewTime(time.Unix(1420, 0))),
	}

	recordBackupPhase(metricNamespace, name,
		pgelasticv1alpha1.BackupPhasePending, status, time.Unix(1000, 0))

	if labels := routeLabels(t, name); labels != nil {
		t.Errorf("a backup advertises a route: %v", labels)
	}

	before := outcomeTotalFor(t, kindBackup, "Failed")
	status.Phase = pgelasticv1alpha1.BackupPhaseFailed
	recordBackupPhase(metricNamespace, name,
		pgelasticv1alpha1.BackupPhaseRunning, status, time.Unix(1420, 0))

	if after := outcomeTotalFor(t, kindBackup, "Failed"); after != before+1 {
		t.Errorf("a failed backup counted %v, want one", after-before)
	}
}
