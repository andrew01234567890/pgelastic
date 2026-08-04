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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const otherControllerName = "example.com/some-other-controller"

func ensureNamespace(name string) {
	GinkgoHelper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, namespace))).To(Succeed())
}

func makeElasticClass(name, controller string) *pgelasticv1alpha1.PgElasticClass {
	return &pgelasticv1alpha1.PgElasticClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       pgelasticv1alpha1.PgElasticClassSpec{ControllerName: controller},
	}
}

func makePool(namespace, name, className string, backendConnections int32) *pgelasticv1alpha1.PgElasticPool {
	return &pgelasticv1alpha1.PgElasticPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: pgelasticv1alpha1.PgElasticPoolSpec{
			ClassRef: pgelasticv1alpha1.ClassReference{
				APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
				Kind:     elasticClassKind,
				Name:     className,
			},
			Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: backendConnections},
			Instances: pgelasticv1alpha1.PoolInstances{
				// Declared, rather than left to the CRD's default of three. These fixtures
				// hand-write the members they want; a pool that declares three and is given
				// one now provisions the other two, which is correct and is not what any of
				// them is about.
				Replicas: ptrTo(int32(1)),
				Template: pgelasticv1alpha1.PgInstanceTemplate{
					Class: instanceClassName,
					Storage: pgelasticv1alpha1.InstanceStorage{
						Size:      *quantity("100Gi"),
						WALVolume: pgelasticv1alpha1.WALVolume{Size: *quantity("20Gi")},
					},
				},
			},
		},
	}
}

// claimPool gives a namespace a PgElasticClass this controller claims and a pool bound to
// it, so instances, tenants and migrations under that pool resolve to this controller.
// Ownership is inherited by reference and nothing else, so a spec that skips this gets an
// object the reconciler is required to leave alone.
func claimPool(namespace, className, poolName string) {
	GinkgoHelper()
	elasticClass := makeElasticClass(className, defaultControllerName)
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, elasticClass))).To(Succeed())
	pool := makePool(namespace, poolName, className, 100)
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, pool))).To(Succeed())
	awaitCached(elasticClass, pool)
}

func makeWorkloadClass(name string, guaranteed, burstable int32) *pgelasticv1alpha1.PgWorkloadClass {
	return &pgelasticv1alpha1.PgWorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
			Priority: 1000,
			Capacity: pgelasticv1alpha1.WorkloadCapacity{
				Guaranteed: ptrTo(guaranteed),
				Burstable:  burstable,
			},
		},
	}
}

func makeTenant(namespace, name, pool, database string) *pgelasticv1alpha1.PgTenant {
	return &pgelasticv1alpha1.PgTenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: pgelasticv1alpha1.PgTenantSpec{
			PoolRef:      corev1.LocalObjectReference{Name: pool},
			DatabaseName: database,
		},
	}
}

func requestFor(object client.Object) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKeyFromObject(object)}
}

func conditionOf(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	GinkgoHelper()
	condition := meta.FindStatusCondition(conditions, conditionType)
	Expect(condition).NotTo(BeNil(), "expected a %s condition", conditionType)
	return condition
}

// reconcileNow lets the informer cache catch up with whatever the spec just wrote and
// then drives one reconcile. The wait is the price of giving reconcilers a cached client:
// only a cache carries the field indexes they select on.
func reconcileNow(reconciler reconcile.Reconciler, object client.Object) reconcile.Result {
	GinkgoHelper()
	awaitCached(object)
	result, err := reconciler.Reconcile(ctx, requestFor(object))
	Expect(err).NotTo(HaveOccurred())
	return result
}

func refetch[T any, PT interface {
	*T
	client.Object
}](object PT) PT {
	GinkgoHelper()
	fetched := PT(new(T))
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(object), fetched)).To(Succeed())
	return fetched
}

func present(object client.Object) bool {
	GinkgoHelper()
	err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object.DeepCopyObject().(client.Object))
	if err == nil {
		return true
	}
	Expect(client.IgnoreNotFound(err)).To(Succeed())
	return false
}

// deleteAndAwait removes objects without depending on a running controller to release
// their finalizers.
func deleteAndAwait(objects ...client.Object) {
	GinkgoHelper()
	for _, object := range objects {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, object))).To(Succeed())
		Eventually(func() error {
			fetched, ok := object.DeepCopyObject().(client.Object)
			Expect(ok).To(BeTrue())
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), fetched); err != nil {
				return client.IgnoreNotFound(err)
			}
			if len(fetched.GetFinalizers()) == 0 {
				return nil
			}
			fetched.SetFinalizers(nil)
			return k8sClient.Update(ctx, fetched)
		}).Should(Succeed())
	}
	awaitCachedGone(objects...)
}
