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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

func archivingInstance(
	backup *pgelasticv1alpha1.InstanceBackup,
	health *pgelasticv1alpha1.ArchiveHealthStatus,
) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: primaryInstanceName, Namespace: "tenants", Generation: 4},
		Spec:       pgelasticv1alpha1.PgInstanceSpec{Backup: backup},
		Status:     pgelasticv1alpha1.PgInstanceStatus{ArchiveHealth: health},
	}
}

func configuredBackup() *pgelasticv1alpha1.InstanceBackup {
	return &pgelasticv1alpha1.InstanceBackup{
		ObjectStore: pgelasticv1alpha1.ObjectStore{
			Path:                 "s3://backups/prod",
			CredentialsSecretRef: corev1.LocalObjectReference{Name: "prod-object-store"},
		},
	}
}

// An absent condition and a False one are different claims. Reporting False for an instance
// that never asked for an archive would alarm on every instance in a fleet that has not
// opted in, which is the fastest way to make the alarm ignored.
func TestNoArchivingConditionWithoutARepository(t *testing.T) {
	if got := archivingCondition(archivingInstance(nil, nil)); got != nil {
		t.Fatalf("condition = %v, want none at all", got)
	}
}

// A repository was asked for and no member has said whether WAL is reaching it. That is not
// evidence of health, and publishing nothing would leave the instance looking like one with
// no repository at all.
func TestArchivingIsFalseWhileNobodyHasReported(t *testing.T) {
	condition := archivingCondition(archivingInstance(configuredBackup(), nil))
	if condition["status"] != string(metav1.ConditionFalse) {
		t.Errorf("status = %v, want False", condition["status"])
	}
	if condition["reason"] != pgelasticv1alpha1.ReasonArchiveDegraded {
		t.Errorf("reason = %v", condition["reason"])
	}
}

func TestArchivingFollowsWhatThePrimaryPublished(t *testing.T) {
	healthy := archivingCondition(archivingInstance(configuredBackup(),
		&pgelasticv1alpha1.ArchiveHealthStatus{
			Healthy:         true,
			LastArchivedWAL: "000000010000000000000009",
		}))
	if healthy["status"] != string(metav1.ConditionTrue) {
		t.Errorf("status = %v, want True", healthy["status"])
	}
	if healthy["reason"] != pgelasticv1alpha1.ReasonArchiveHealthy {
		t.Errorf("reason = %v", healthy["reason"])
	}
	if message, _ := healthy["message"].(string); !strings.Contains(message, "000000010000000000000009") {
		t.Errorf("message = %q, want the segment it archived through", message)
	}

	degraded := archivingCondition(archivingInstance(configuredBackup(),
		&pgelasticv1alpha1.ArchiveHealthStatus{Healthy: false}))
	if degraded["status"] != string(metav1.ConditionFalse) {
		t.Errorf("status = %v, want False", degraded["status"])
	}
	if degraded["reason"] != pgelasticv1alpha1.ReasonArchiveDegraded {
		t.Errorf("reason = %v", degraded["reason"])
	}
}

// The two ways archiving fails call for different actions - a repository refusing writes is
// a credential or a bucket, and a queue that will not drain behind a last success is an
// archive_command that stopped returning - so the message has to say which one happened.
func TestTheMessageSaysWhichWayArchivingIsFailing(t *testing.T) {
	refused := archivingCondition(archivingInstance(configuredBackup(),
		&pgelasticv1alpha1.ArchiveHealthStatus{
			LastFailureMessage: "the bucket refused the upload",
		}))
	if message, _ := refused["message"].(string); message != "the bucket refused the upload" {
		t.Errorf("message = %q, want the recorded reason", message)
	}

	stalled := archivingCondition(archivingInstance(configuredBackup(),
		&pgelasticv1alpha1.ArchiveHealthStatus{ReadyBacklog: 41}))
	message, _ := stalled["message"].(string)
	if !strings.Contains(message, "41") {
		t.Errorf("message = %q, want the size of the queue that is not draining", message)
	}
}

func TestTheArchivingConditionIsStampedWithTheObservedGeneration(t *testing.T) {
	condition := archivingCondition(archivingInstance(configuredBackup(),
		&pgelasticv1alpha1.ArchiveHealthStatus{Healthy: true}))
	if condition["observedGeneration"] != int64(4) {
		t.Fatalf("observedGeneration = %v, want the instance's generation",
			condition["observedGeneration"])
	}
}
