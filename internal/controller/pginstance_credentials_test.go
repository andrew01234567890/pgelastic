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
