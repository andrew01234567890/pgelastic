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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

const (
	credentialNamespace = "shop"
	sourceInstanceName  = "pg-a"
	sourcePassword      = "the-password-the-catalogue-was-backed-up-with"
	backupName          = "friday"
	repositoryPath      = "/pgelastic"
	objectStoreSecret   = "object-store-credentials"
)

func credentialScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	built := runtime.NewScheme()
	if err := corev1.AddToScheme(built); err != nil {
		t.Fatalf("adding core types: %v", err)
	}
	if err := pgelasticv1alpha1.AddToScheme(built); err != nil {
		t.Fatalf("adding pgelastic types: %v", err)
	}
	return built
}

func sourceCredentials() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: credentialNamespace,
			Name:      provision.CredentialsSecretName(sourceInstanceName),
		},
		Data: map[string][]byte{
			provision.SecretKeyReplicationPassword: []byte(sourcePassword),
			provision.SecretKeyOpsPassword:         []byte("ops"),
			provision.SecretKeyRewindPassword:      []byte("rewind"),
		},
	}
}

func recoveredInstance(name string) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: name},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			Restore: &pgelasticv1alpha1.InstanceRestore{
				SourceInstanceName: sourceInstanceName,
			},
		},
	}
}

// pgbackrest restores pg_authid along with everything else, so a recovered instance's roles
// keep the source's passwords. A fresh Secret would describe a cluster that has never
// existed: every standby fails SCRAM cloning from its own primary, and nothing on the
// network can reach it - while the Secret itself looks perfectly well-formed.
func TestARestoredInstanceInheritsItsSourcesCredentials(t *testing.T) {
	scheme := credentialScheme(t)
	restored := recoveredInstance("pg-a-recovered")
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(sourceCredentials(), restored).Build()
	reconciler := &PgInstanceReconciler{Client: kube, Scheme: scheme}

	if err := reconciler.ensureCredentials(context.Background(), restored); err != nil {
		t.Fatalf("ensuring credentials: %v", err)
	}

	got := &corev1.Secret{}
	if err := kube.Get(context.Background(), types.NamespacedName{
		Namespace: credentialNamespace,
		Name:      provision.CredentialsSecretName("pg-a-recovered"),
	}, got); err != nil {
		t.Fatalf("reading the restored instance's credentials: %v", err)
	}

	for _, key := range []string{
		provision.SecretKeyReplicationPassword,
		provision.SecretKeyOpsPassword,
		provision.SecretKeyRewindPassword,
	} {
		if string(got.Data[key]) != string(sourceCredentials().Data[key]) {
			t.Errorf("%s = %q, want the source's", key, got.Data[key])
		}
	}
}

// Failing is the only safe answer. Minting fresh passwords instead would produce an instance
// that provisions cleanly, reports itself healthy, and cannot be reached by anything -
// discovered during the restore it was needed for.
func TestARestoreWithNoSourceCredentialsFailsLoudly(t *testing.T) {
	scheme := credentialScheme(t)
	restored := recoveredInstance("pg-a-recovered")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(restored).Build()
	reconciler := &PgInstanceReconciler{Client: kube, Scheme: scheme}

	err := reconciler.ensureCredentials(context.Background(), restored)
	if err == nil {
		t.Fatal("credentials were invented for an instance whose catalogue has its own")
	}
	if !strings.Contains(err.Error(), sourceInstanceName) {
		t.Errorf("error = %q, want it to name the source whose Secret is missing", err)
	}

	secrets := &corev1.SecretList{}
	if err := kube.List(context.Background(), secrets,
		client.InNamespace(credentialNamespace)); err != nil {
		t.Fatalf("listing: %v", err)
	}
	if len(secrets.Items) != 0 {
		t.Errorf("wrote %d Secret(s) anyway", len(secrets.Items))
	}
}

// A backup records where it was written but not what to authenticate with: the agent is
// configured with pgBackRest's own settings and never learns which Secret they came from.
// planRestore prefers that recorded repository over the source's spec, so without this the
// recovered instance was handed a repository it could not open at all - its bootstrap
// container died on a missing accessKeyID until the restore timed out.
//
// This drives planRestore itself. An earlier version of this test re-implemented the two
// lines of the fix in the test body and asserted on its own copy, which passed with the fix
// reverted.
func TestARestoreCarriesTheObjectStoreCredentialsForward(t *testing.T) {
	scheme := credentialScheme(t)
	stopped := metav1.NewTime(time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC))
	backup := &pgelasticv1alpha1.PgBackup{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: backupName},
		Spec: pgelasticv1alpha1.PgBackupSpec{
			InstanceRef: corev1.LocalObjectReference{Name: sourceInstanceName},
		},
		Status: pgelasticv1alpha1.PgBackupStatus{
			Phase:     pgelasticv1alpha1.BackupPhaseCompleted,
			StoppedAt: &stopped,
			Stanza:    "pgelastic-7668815305197002786",
			BackupID:  "20260801-020000F",
			// What the agent records: where, and nothing about what to authenticate with.
			Repository: &pgelasticv1alpha1.ObjectStore{
				Path:        repositoryPath,
				EndpointURL: "objectstore.shop.svc",
				Region:      "us-east-1",
			},
		},
	}
	source := &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: sourceInstanceName},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			Backup: &pgelasticv1alpha1.InstanceBackup{
				ObjectStore: pgelasticv1alpha1.ObjectStore{
					Path: repositoryPath,
					CredentialsSecretRef: corev1.LocalObjectReference{
						Name: objectStoreSecret,
					},
				},
			},
		},
	}
	restore := &pgelasticv1alpha1.PgRestore{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: "put-it-back"},
		Spec: pgelasticv1alpha1.PgRestoreSpec{
			SourceInstanceRef: corev1.LocalObjectReference{Name: sourceInstanceName},
			BackupRef:         &corev1.LocalObjectReference{Name: backupName},
		},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(sourceCredentials(), backup, source, restore).Build()
	reconciler := &PgRestoreReconciler{Client: kube, Scheme: scheme}

	plan, reason, err := reconciler.planRestore(context.Background(), restore)
	if err != nil {
		t.Fatalf("planning: %v", err)
	}
	if reason != "" {
		t.Fatalf("refused: %s", reason)
	}
	if got := plan.Spec.Backup.ObjectStore.CredentialsSecretRef.Name; got != objectStoreSecret {
		t.Errorf("credentialsSecretRef = %q, want the source's; without it the recovered "+
			"instance cannot authenticate to the repository it is told to read", got)
	}
	// The backup's own record is a shared pointer until it is copied. Mutating it would put a
	// credentials reference on a status that never had one.
	if backup.Status.Repository.CredentialsSecretRef.Name != "" {
		t.Error("the backup's recorded repository was modified in place")
	}
}

// Neither the backup nor the source says which Secret holds the object store's credentials,
// so there is nothing the recovered instance could authenticate with. A named reason beats an
// instance that provisions and then crash-loops on a missing accessKeyID.
func TestARestoreWithNoObjectStoreCredentialsIsRefused(t *testing.T) {
	scheme := credentialScheme(t)
	stopped := metav1.NewTime(time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC))
	backup := &pgelasticv1alpha1.PgBackup{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: backupName},
		Spec: pgelasticv1alpha1.PgBackupSpec{
			InstanceRef: corev1.LocalObjectReference{Name: sourceInstanceName},
		},
		Status: pgelasticv1alpha1.PgBackupStatus{
			Phase:      pgelasticv1alpha1.BackupPhaseCompleted,
			StoppedAt:  &stopped,
			Stanza:     "pgelastic-7668815305197002786",
			Repository: &pgelasticv1alpha1.ObjectStore{Path: repositoryPath},
		},
	}
	source := &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: sourceInstanceName},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			Backup: &pgelasticv1alpha1.InstanceBackup{
				ObjectStore: pgelasticv1alpha1.ObjectStore{Path: repositoryPath},
			},
		},
	}
	restore := &pgelasticv1alpha1.PgRestore{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: "put-it-back"},
		Spec: pgelasticv1alpha1.PgRestoreSpec{
			SourceInstanceRef: corev1.LocalObjectReference{Name: sourceInstanceName},
			BackupRef:         &corev1.LocalObjectReference{Name: backupName},
		},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(sourceCredentials(), backup, source, restore).Build()
	reconciler := &PgRestoreReconciler{Client: kube, Scheme: scheme}

	plan, reason, err := reconciler.planRestore(context.Background(), restore)
	if err != nil {
		t.Fatalf("planning: %v", err)
	}
	if reason == "" {
		t.Fatalf("planned %v with no object store credentials at all", plan)
	}
	if !strings.Contains(reason, sourceInstanceName) {
		t.Errorf("reason = %q, want it to name where the credentials were looked for", reason)
	}
}

// An ordinary instance still gets passwords of its own. initdb creates its catalogue from
// nothing, so there is no earlier cluster for it to agree with.
func TestAnOrdinaryInstanceGetsFreshCredentials(t *testing.T) {
	scheme := credentialScheme(t)
	fresh := &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: "pg-b"},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(sourceCredentials(), fresh).Build()
	reconciler := &PgInstanceReconciler{Client: kube, Scheme: scheme}

	if err := reconciler.ensureCredentials(context.Background(), fresh); err != nil {
		t.Fatalf("ensuring credentials: %v", err)
	}

	got := &corev1.Secret{}
	if err := kube.Get(context.Background(), types.NamespacedName{
		Namespace: credentialNamespace,
		Name:      provision.CredentialsSecretName("pg-b"),
	}, got); err != nil {
		t.Fatalf("reading: %v", err)
	}
	// Minted through StringData, which only a real API server folds into Data, so both are
	// read here rather than asserting on the encoding the fake client happens to keep.
	password := got.StringData[provision.SecretKeyReplicationPassword]
	if password == "" {
		password = string(got.Data[provision.SecretKeyReplicationPassword])
	}
	if password == sourcePassword {
		t.Error("an instance that was not restored took another instance's password")
	}
	if password == "" {
		t.Error("no replication password was minted at all")
	}
}
