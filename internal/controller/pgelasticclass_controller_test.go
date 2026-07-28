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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

var _ = Describe("PgElasticClass controller", func() {
	const namespace = "pgec-controller"

	var reconciler *PgElasticClassReconciler

	BeforeEach(func() {
		ensureNamespace(namespace)
		reconciler = &PgElasticClassReconciler{
			Client:         cachedClient,
			Scheme:         cachedClient.Scheme(),
			ControllerName: defaultControllerName,
		}
	})

	It("leaves a class belonging to another controller entirely alone", func() {
		elasticClass := makeElasticClass("pgec-foreign", otherControllerName)
		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(elasticClass) })
		awaitCached(elasticClass)

		result := reconcileNow(reconciler, elasticClass)

		Expect(result.IsZero()).To(BeTrue())
		fetched := refetch(elasticClass)
		Expect(fetched.Status.Conditions).To(BeEmpty())
		Expect(fetched.Status.SupportedFeatures).To(BeEmpty())
		Expect(fetched.Finalizers).To(BeEmpty())
	})

	It("accepts a class it owns and publishes the features it implements", func() {
		elasticClass := makeElasticClass("pgec-owned", defaultControllerName)
		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(elasticClass) })
		awaitCached(elasticClass)

		reconcileNow(reconciler, elasticClass)

		fetched := refetch(elasticClass)
		Expect(fetched.Finalizers).To(ContainElement(PoolsExistFinalizer))
		Expect(fetched.Status.ObservedGeneration).To(Equal(fetched.Generation))
		Expect(fetched.Status.SupportedFeatures).To(ContainElement(pgelasticv1alpha1.ClassFeature("DerivedQoSClass")))

		accepted := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionAccepted)
		Expect(accepted.Status).To(Equal(metav1.ConditionTrue))
		Expect(accepted.Reason).To(Equal(pgelasticv1alpha1.ReasonAccepted))
		Expect(accepted.ObservedGeneration).To(Equal(fetched.Generation))
	})

	It("refuses a class whose own headroom defaults leave a migrating pool nothing", func() {
		elasticClass := makeElasticClass("pgec-no-headroom", defaultControllerName)
		elasticClass.Spec.Defaults = &pgelasticv1alpha1.ElasticClassDefaults{
			HeadroomPercent:          ptrTo(int32(50)),
			MigrationHeadroomPercent: ptrTo(int32(50)),
		}
		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(elasticClass) })
		awaitCached(elasticClass)

		reconcileNow(reconciler, elasticClass)

		accepted := conditionOf(refetch(elasticClass).Status.Conditions, pgelasticv1alpha1.ConditionAccepted)
		Expect(accepted.Status).To(Equal(metav1.ConditionFalse))
		Expect(accepted.Reason).To(Equal(pgelasticv1alpha1.ReasonInvalidSpec))
		Expect(accepted.Message).To(ContainSubstring("migrationHeadroomPercent"))
	})

	Describe("the pools-exist finalizer", Ordered, func() {
		var elasticClass *pgelasticv1alpha1.PgElasticClass
		var pool *pgelasticv1alpha1.PgElasticPool

		BeforeAll(func() {
			ensureNamespace(namespace)
			elasticClass = makeElasticClass("pgec-guarded", defaultControllerName)
			pool = makePool(namespace, "pgec-guarded-pool", elasticClass.Name, 100)
			Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			awaitCached(elasticClass, pool)
		})

		It("counts the pools bound to the class", func() {
			reconcileNow(reconciler, elasticClass)

			fetched := refetch(elasticClass)
			Expect(fetched.Status.PoolCount).To(Equal(int32(1)))
			Expect(fetched.Finalizers).To(ContainElement(PoolsExistFinalizer))
		})

		It("holds the class open while the pool still needs its policy", func() {
			Expect(k8sClient.Delete(ctx, elasticClass)).To(Succeed())
			Eventually(func() bool {
				return !refetch(elasticClass).DeletionTimestamp.IsZero()
			}).Should(BeTrue())

			reconcileNow(reconciler, elasticClass)

			fetched := refetch(elasticClass)
			Expect(fetched.Finalizers).To(ContainElement(PoolsExistFinalizer))
			ready := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionReady)
			Expect(ready.Status).To(Equal(metav1.ConditionFalse))
			Expect(ready.Reason).To(Equal(pgelasticv1alpha1.ReasonPending))
			Expect(ready.Message).To(ContainSubstring(PoolsExistFinalizer))
		})

		It("releases the class once the last pool is gone", func() {
			deleteAndAwait(pool)

			reconcileNow(reconciler, elasticClass)

			Expect(present(elasticClass)).To(BeFalse())
		})
	})
})
