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

package agent

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const (
	backupNamespace = "shop"
	backupName      = "pg-a-20260801t0200"
	backupMember    = "pg-a-1"
)

func backupScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	built := runtime.NewScheme()
	if err := pgelasticv1alpha1.AddToScheme(built); err != nil {
		t.Fatalf("adding pgelastic types: %v", err)
	}
	return built
}

func runningBackup() *pgelasticv1alpha1.PgBackup {
	return &pgelasticv1alpha1.PgBackup{
		ObjectMeta: metav1.ObjectMeta{Namespace: backupNamespace, Name: backupName},
		Status: pgelasticv1alpha1.PgBackupStatus{
			Phase:  pgelasticv1alpha1.BackupPhaseRunning,
			Member: backupMember,
		},
	}
}

// The write that records how a backup ended cannot go through the copy the claim was made
// on. Claiming sets the phase to Running, which wakes the PgBackup controller, which rewrites
// the conditions to say so - and the backup being described still has minutes to hours to
// run. Every terminal write therefore lost to a conflict: the backup stayed Running for ever
// and, because a running backup holds the election, no later backup was taken either.
func TestTheTerminalStatusSurvivesAConcurrentWrite(t *testing.T) {
	scheme := backupScheme(t)
	conflicts := 2
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(runningBackup()).
		WithStatusSubresource(&pgelasticv1alpha1.PgBackup{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(
				ctx context.Context, c client.Client, subResource string,
				object client.Object, options ...client.SubResourceUpdateOption,
			) error {
				if conflicts == 0 {
					return c.Status().Update(ctx, object, options...)
				}
				conflicts--

				// The stored object is genuinely moved on, the way the PgBackup controller
				// moves it while a backup runs, and the conflict is then the fake client's
				// own. Fabricating one instead would leave resourceVersion untouched, so an
				// implementation that never re-read would sail through its second attempt
				// and the test would prove only that a retry happened - not that it read
				// again, which is the entire fix.
				moved := &pgelasticv1alpha1.PgBackup{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(object), moved); err != nil {
					return err
				}
				meta.SetStatusCondition(&moved.Status.Conditions, metav1.Condition{
					Type:    pgelasticv1alpha1.ConditionProgressing,
					Status:  metav1.ConditionTrue,
					Reason:  pgelasticv1alpha1.ReasonRunning,
					Message: backupMember + " is taking this backup",
				})
				if err := c.Status().Update(ctx, moved); err != nil {
					return err
				}
				return c.Status().Update(ctx, object, options...)
			},
		}).Build()

	options := Options{Client: kube, Namespace: backupNamespace, Member: backupMember}
	err := writeTerminalStatus(context.Background(), options, backupName,
		func(status *pgelasticv1alpha1.PgBackupStatus) {
			status.Phase = pgelasticv1alpha1.BackupPhaseCompleted
			status.BackupID = "20260801-020000F"
		})
	if err != nil {
		t.Fatalf("recording how the backup ended: %v", err)
	}
	if conflicts != 0 {
		t.Fatalf("%d conflicts were never provoked, so the retry was not exercised", conflicts)
	}

	got := &pgelasticv1alpha1.PgBackup{}
	if err := kube.Get(context.Background(), types.NamespacedName{
		Namespace: backupNamespace, Name: backupName,
	}, got); err != nil {
		t.Fatalf("reading the backup back: %v", err)
	}
	if got.Status.Phase != pgelasticv1alpha1.BackupPhaseCompleted {
		t.Errorf("phase = %q, want Completed; a backup stuck at Running holds the election "+
			"and no later backup is ever taken", got.Status.Phase)
	}
	if got.Status.BackupID != "20260801-020000F" {
		t.Errorf("backupID = %q, want what the repository reported", got.Status.BackupID)
	}
	// The other half of the contract: the terminal write is applied on top of what the
	// controller wrote, not over it.
	if meta.FindStatusCondition(got.Status.Conditions,
		pgelasticv1alpha1.ConditionProgressing) == nil {
		t.Error("the concurrent writer's condition was clobbered by the terminal write")
	}
}

// A backup deleted while it ran has nowhere to record what happened. That is somebody's
// decision about an object, not a failure of the backup, and it must not be reported as one.
func TestADeletedBackupIsNotAnError(t *testing.T) {
	scheme := backupScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&pgelasticv1alpha1.PgBackup{}).Build()

	options := Options{Client: kube, Namespace: backupNamespace, Member: backupMember}
	if err := writeTerminalStatus(context.Background(), options, backupName,
		func(status *pgelasticv1alpha1.PgBackupStatus) {
			status.Phase = pgelasticv1alpha1.BackupPhaseCompleted
		}); err != nil {
		t.Fatalf("a backup that no longer exists was reported as a failure: %v", err)
	}
}
