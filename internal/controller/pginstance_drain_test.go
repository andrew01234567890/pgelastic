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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/andrew01234567890/pgelastic/internal/index"
)

const drainInstanceName = "pg-busy"

func boundTenant(name, instance string) *pgelasticv1alpha1.PgTenant {
	return &pgelasticv1alpha1.PgTenant{
		ObjectMeta: metav1.ObjectMeta{Namespace: credentialNamespace, Name: name},
		Status: pgelasticv1alpha1.PgTenantStatus{
			Binding: &pgelasticv1alpha1.PgTenantBinding{
				InstanceRef: &corev1.LocalObjectReference{Name: instance},
			},
		},
	}
}

func deletingInstance() *pgelasticv1alpha1.PgInstance {
	deleting := metav1.Now()
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         credentialNamespace,
			Name:              drainInstanceName,
			DeletionTimestamp: &deleting,
			Finalizers:        []string{pgelasticv1alpha1.PgInstanceDrainTenantsFinalizer},
		},
	}
}

// Every PVC of an instance carries an owner reference to it, so deleting the object is
// deleting the data of every tenant living on it - immediately, and with no confirmation
// beyond the one kubectl already asked for. The tenants that make an instance not idle are
// invisible from the instance itself, which is what makes `kubectl delete pginstance` a
// plausible thing to type at a cluster somebody believes is empty.
func TestAnInstanceWithTenantsOnItIsNotDeleted(t *testing.T) {
	scheme := credentialScheme(t)
	instance := deletingInstance()
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(instance, boundTenant("acme", drainInstanceName),
			boundTenant("globex", drainInstanceName)).
		WithIndex(&pgelasticv1alpha1.PgTenant{}, index.TenantByInstance,
			func(object client.Object) []string {
				tenant, ok := object.(*pgelasticv1alpha1.PgTenant)
				if !ok || tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef == nil {
					return nil
				}
				return []string{tenant.Status.Binding.InstanceRef.Name}
			}).
		WithStatusSubresource(&pgelasticv1alpha1.PgInstance{}).Build()
	reconciler := &PgInstanceReconciler{Client: kube, Scheme: scheme}

	result, err := reconciler.finalize(context.Background(), instance)
	if err != nil {
		t.Fatalf("finalizing: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("the deletion was not held open, so nothing will look again")
	}
	if !controllerutil.ContainsFinalizer(instance,
		pgelasticv1alpha1.PgInstanceDrainTenantsFinalizer) {
		t.Fatal("the finalizer was released while two tenants were still on the instance, " +
			"so their volumes are about to be garbage-collected")
	}

	// Holding the deletion open is only half of it. A deletion that hangs with nothing said
	// about why is a deletion somebody debugs by removing the finalizer, which is the exact
	// outcome the guard exists to prevent - so the names go in the condition, sorted, because
	// an unstable message rewrites itself on every recheck.
	held := &pgelasticv1alpha1.PgInstance{}
	if err := kube.Get(context.Background(),
		client.ObjectKeyFromObject(instance), held); err != nil {
		t.Fatalf("reading the instance back: %v", err)
	}
	if held.Status.Phase != pgelasticv1alpha1.InstancePhaseTerminating {
		t.Errorf("phase is %q, so the instance does not look like it is going away",
			held.Status.Phase)
	}
	ready := apimeta.FindStatusCondition(held.Status.Conditions,
		pgelasticv1alpha1.ConditionReady)
	switch {
	case ready == nil:
		t.Fatal("no Ready condition, so kubectl describe says nothing about the held deletion")
	case ready.Status != metav1.ConditionFalse:
		t.Errorf("Ready is %q while the deletion is blocked", ready.Status)
	case ready.Reason != pgelasticv1alpha1.ReasonTenantsStillBound:
		t.Errorf("Ready reason is %q, not %q",
			ready.Reason, pgelasticv1alpha1.ReasonTenantsStillBound)
	}
	if !strings.Contains(ready.Message, "acme, globex") {
		t.Errorf("the condition does not name both tenants in a stable order: %q", ready.Message)
	}
	if !strings.Contains(ready.Message, "2 tenant(s)") {
		t.Errorf("the condition does not count the tenants: %q", ready.Message)
	}
}

// The other half: an instance nobody is on must not be held for ever.
func TestAnEmptyInstanceIsReleased(t *testing.T) {
	scheme := credentialScheme(t)
	instance := deletingInstance()
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(instance, boundTenant("acme", "somebody-else")).
		WithIndex(&pgelasticv1alpha1.PgTenant{}, index.TenantByInstance,
			func(object client.Object) []string {
				tenant, ok := object.(*pgelasticv1alpha1.PgTenant)
				if !ok || tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef == nil {
					return nil
				}
				return []string{tenant.Status.Binding.InstanceRef.Name}
			}).
		WithStatusSubresource(&pgelasticv1alpha1.PgInstance{}).Build()
	reconciler := &PgInstanceReconciler{Client: kube, Scheme: scheme}

	if _, err := reconciler.finalize(context.Background(), instance); err != nil {
		t.Fatalf("finalizing: %v", err)
	}
	if controllerutil.ContainsFinalizer(instance,
		pgelasticv1alpha1.PgInstanceDrainTenantsFinalizer) {
		t.Error("an instance with no tenants on it was held open anyway")
	}
}

// The drain guard is a refusal, not a cleanup, so it has to survive an ownership verdict that
// releases the finalizers which are cleanups. A pool carries no finalizer of its own and its
// webhook does not refuse deletion, so `kubectl delete pgelasticpool` succeeds at once and
// every instance under it stops resolving back to a class from that moment.
func TestAnOrphanedInstanceStillRefusesToDrop(t *testing.T) {
	scheme := credentialScheme(t)
	instance := deletingInstance()
	instance.Spec.PoolRef = corev1.LocalObjectReference{Name: "a-pool-that-is-gone"}
	kube := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(instance, boundTenant("acme", drainInstanceName)).
		WithIndex(&pgelasticv1alpha1.PgTenant{}, index.TenantByInstance,
			func(object client.Object) []string {
				tenant, ok := object.(*pgelasticv1alpha1.PgTenant)
				if !ok || tenant.Status.Binding == nil || tenant.Status.Binding.InstanceRef == nil {
					return nil
				}
				return []string{tenant.Status.Binding.InstanceRef.Name}
			}).
		WithStatusSubresource(&pgelasticv1alpha1.PgInstance{}).Build()
	reconciler := &PgInstanceReconciler{Client: kube, Scheme: scheme}

	if _, err := reconciler.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(instance),
	}); err != nil {
		t.Fatalf("reconciling: %v", err)
	}

	held := &pgelasticv1alpha1.PgInstance{}
	if err := kube.Get(context.Background(),
		client.ObjectKeyFromObject(instance), held); err != nil {
		t.Fatalf("reading the instance back: %v", err)
	}
	if !controllerutil.ContainsFinalizer(held,
		pgelasticv1alpha1.PgInstanceDrainTenantsFinalizer) {
		t.Fatal("an instance whose pool has gone was released while a tenant was still on it, " +
			"so that tenant's volumes are about to be garbage-collected")
	}
}
