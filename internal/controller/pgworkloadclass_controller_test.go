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

var _ = Describe("PgWorkloadClass controller", func() {
	const namespace = "pgwc-controller"

	var reconciler *PgWorkloadClassReconciler

	BeforeEach(func() {
		ensureNamespace(namespace)
		reconciler = &PgWorkloadClassReconciler{Client: cachedClient, Scheme: cachedClient.Scheme()}
	})

	It("accepts a self-consistent class and stamps the generation on the condition", func() {
		workloadClass := makeWorkloadClass("pgwc-plain", 0, 8)
		Expect(k8sClient.Create(ctx, workloadClass)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(workloadClass) })

		reconcileNow(reconciler, workloadClass)

		fetched := refetch(workloadClass)
		Expect(fetched.Status.ObservedGeneration).To(Equal(fetched.Generation))
		accepted := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionAccepted)
		Expect(accepted.Status).To(Equal(metav1.ConditionTrue))
		Expect(accepted.ObservedGeneration).To(Equal(fetched.Generation))
	})

	It("refuses a class whose statement timeout is not a timeout at all", func() {
		workloadClass := makeWorkloadClass("pgwc-no-deadline", 0, 8)
		workloadClass.Spec.Limits = &pgelasticv1alpha1.TenantLimits{StatementTimeout: duration(0)}
		Expect(k8sClient.Create(ctx, workloadClass)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(workloadClass) })

		reconcileNow(reconciler, workloadClass)

		accepted := conditionOf(refetch(workloadClass).Status.Conditions, pgelasticv1alpha1.ConditionAccepted)
		Expect(accepted.Status).To(Equal(metav1.ConditionFalse))
		Expect(accepted.Reason).To(Equal(pgelasticv1alpha1.ReasonInvalidSpec))
		Expect(accepted.Message).To(ContainSubstring("spec.limits.statementTimeout"))
	})

	It("refuses a class that would evict a tenant holding no surplus", func() {
		workloadClass := makeWorkloadClass("pgwc-nothing-to-evict", 8, 8)
		workloadClass.Spec.OnBudgetExhaustion = ptrTo(pgelasticv1alpha1.BudgetExhaustionEvict)
		Expect(k8sClient.Create(ctx, workloadClass)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(workloadClass) })

		reconcileNow(reconciler, workloadClass)

		accepted := conditionOf(refetch(workloadClass).Status.Conditions, pgelasticv1alpha1.ConditionAccepted)
		Expect(accepted.Status).To(Equal(metav1.ConditionFalse))
		Expect(accepted.Message).To(ContainSubstring("onBudgetExhaustion"))
	})

	It("reports a cluster-wide global conflict rather than pretending to be the default", func() {
		first := makeWorkloadClass("pgwc-global-a", 0, 8)
		first.Spec.Global = ptrTo(true)
		second := makeWorkloadClass("pgwc-global-b", 0, 8)
		second.Spec.Global = ptrTo(true)
		Expect(k8sClient.Create(ctx, first)).To(Succeed())
		Expect(k8sClient.Create(ctx, second)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(first, second) })
		awaitCached(first, second)

		reconcileNow(reconciler, second)

		accepted := conditionOf(refetch(second).Status.Conditions, pgelasticv1alpha1.ConditionAccepted)
		Expect(accepted.Status).To(Equal(metav1.ConditionFalse))
		Expect(accepted.Reason).To(Equal(pgelasticv1alpha1.ReasonInvalidSpec))
		Expect(accepted.Message).To(ContainSubstring(first.Name))
		Expect(accepted.Message).NotTo(ContainSubstring(second.Name))
	})

	Describe("the tenant count", Ordered, func() {
		var elasticClass *pgelasticv1alpha1.PgElasticClass
		var workloadClass *pgelasticv1alpha1.PgWorkloadClass
		var pool *pgelasticv1alpha1.PgElasticPool

		BeforeAll(func() {
			ensureNamespace(namespace)
			elasticClass = makeElasticClass("pgwc-count-class", defaultControllerName)
			workloadClass = makeWorkloadClass("pgwc-counted", 0, 8)
			pool = makePool(namespace, "pgwc-count-pool", elasticClass.Name, 100)
			pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{
				DefaultWorkloadClassName: workloadClass.Name,
			}
			Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
			Expect(k8sClient.Create(ctx, workloadClass)).To(Succeed())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			DeferCleanup(func() { deleteAndAwait(pool, workloadClass, elasticClass) })
			awaitCached(elasticClass, workloadClass, pool)
		})

		It("is zero before any tenant claims the class", func() {
			reconcileNow(reconciler, workloadClass)

			Expect(refetch(workloadClass).Status.TenantCount).To(BeZero())
		})

		It("counts tenants that name the class and tenants that inherit it from their pool", func() {
			naming := makeTenant(namespace, "pgwc-count-naming", pool.Name, "naming")
			naming.Spec.WorkloadClassName = ptrTo(workloadClass.Name)
			inheriting := makeTenant(namespace, "pgwc-count-inheriting", pool.Name, "inheriting")
			elsewhere := makeTenant(namespace, "pgwc-count-elsewhere", pool.Name, "elsewhere")
			elsewhere.Spec.WorkloadClassName = ptrTo("pgwc-plain")
			Expect(k8sClient.Create(ctx, naming)).To(Succeed())
			Expect(k8sClient.Create(ctx, inheriting)).To(Succeed())
			Expect(k8sClient.Create(ctx, elsewhere)).To(Succeed())
			DeferCleanup(func() { deleteAndAwait(naming, inheriting, elsewhere) })
			awaitCached(naming, inheriting, elsewhere)

			reconcileNow(reconciler, workloadClass)

			Expect(refetch(workloadClass).Status.TenantCount).To(Equal(int32(2)))
		})
	})
})
