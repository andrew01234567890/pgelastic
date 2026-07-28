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
	"github.com/andrew01234567890/pgelastic/internal/placement"
)

var _ = Describe("PgTenant controller", Ordered, func() {
	const (
		namespace     = "pgt-controller"
		poolName      = "pgt-pool"
		bestEffort    = "pgt-best-effort"
		burstable     = "pgt-burstable"
		guaranteedAll = "pgt-guaranteed"
	)

	var reconciler *PgTenantReconciler

	BeforeAll(func() {
		ensureNamespace(namespace)
		elasticClass := makeElasticClass("pgt-class", defaultControllerName)
		pool := makePool(namespace, poolName, elasticClass.Name, 900)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: bestEffort}
		classes := []*pgelasticv1alpha1.PgWorkloadClass{
			makeWorkloadClass(bestEffort, 0, 8),
			makeWorkloadClass(burstable, 4, 40),
			makeWorkloadClass(guaranteedAll, 8, 8),
		}

		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		for _, class := range classes {
			Expect(k8sClient.Create(ctx, class)).To(Succeed())
		}
		DeferCleanup(func() {
			deleteAndAwait(pool, elasticClass)
			for _, class := range classes {
				deleteAndAwait(class)
			}
		})
		awaitCached(elasticClass, pool)
		for _, class := range classes {
			awaitCached(class)
		}
	})

	BeforeEach(func() {
		reconciler = &PgTenantReconciler{Client: cachedClient, Scheme: cachedClient.Scheme()}
	})

	createTenant := func(name, database string, mutate func(*pgelasticv1alpha1.PgTenant)) *pgelasticv1alpha1.PgTenant {
		GinkgoHelper()
		tenant := makeTenant(namespace, name, poolName, database)
		if mutate != nil {
			mutate(tenant)
		}
		Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(tenant) })
		return tenant
	}

	DescribeTable("derives the QoS class from the effective numbers, never from a declaration",
		func(name, database string, mutate func(*pgelasticv1alpha1.PgTenant), expected pgelasticv1alpha1.QoSClass,
			guaranteed, burst int32) {
			tenant := createTenant(name, database, mutate)

			reconcileNow(reconciler, tenant)

			fetched := refetch(tenant)
			Expect(fetched.Status.QoSClass).To(Equal(expected))
			Expect(fetched.Status.Effective).NotTo(BeNil())
			Expect(*fetched.Status.Effective.Guaranteed).To(Equal(guaranteed))
			Expect(*fetched.Status.Effective.Burstable).To(Equal(burst))
		},
		Entry("a class with no floor", "pgt-qos-besteffort", "qos_besteffort",
			func(tenant *pgelasticv1alpha1.PgTenant) { tenant.Spec.WorkloadClassName = ptrTo(bestEffort) },
			pgelasticv1alpha1.QoSBestEffort, int32(0), int32(8)),
		Entry("a class whose floor is below its ceiling", "pgt-qos-burstable", "qos_burstable",
			func(tenant *pgelasticv1alpha1.PgTenant) { tenant.Spec.WorkloadClassName = ptrTo(burstable) },
			pgelasticv1alpha1.QoSBurstable, int32(4), int32(40)),
		Entry("a class whose floor is its ceiling", "pgt-qos-guaranteed", "qos_guaranteed",
			func(tenant *pgelasticv1alpha1.PgTenant) { tenant.Spec.WorkloadClassName = ptrTo(guaranteedAll) },
			pgelasticv1alpha1.QoSGuaranteed, int32(8), int32(8)),
		Entry("a tenant override that raises the floor to the ceiling", "pgt-qos-override", "qos_override",
			func(tenant *pgelasticv1alpha1.PgTenant) {
				tenant.Spec.WorkloadClassName = ptrTo(burstable)
				tenant.Spec.Capacity = &pgelasticv1alpha1.PgTenantCapacity{
					Guaranteed: ptrTo(int32(40)),
				}
			},
			pgelasticv1alpha1.QoSGuaranteed, int32(40), int32(40)),
	)

	It("falls back to the pool's default class when the tenant names none", func() {
		tenant := createTenant("pgt-inherits", "inherits", nil)

		reconcileNow(reconciler, tenant)

		fetched := refetch(tenant)
		Expect(fetched.Status.QoSClass).To(Equal(pgelasticv1alpha1.QoSBestEffort))
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionAccepted).Message).
			To(ContainSubstring(bestEffort))
	})

	It("stamps the observed generation on the object and on every condition", func() {
		tenant := createTenant("pgt-generation", "generation", func(tenant *pgelasticv1alpha1.PgTenant) {
			tenant.Spec.WorkloadClassName = ptrTo(burstable)
		})

		reconcileNow(reconciler, tenant)

		fetched := refetch(tenant)
		Expect(fetched.Status.ObservedGeneration).To(Equal(fetched.Generation))
		Expect(fetched.Status.Conditions).To(HaveLen(3))
		for _, condition := range fetched.Status.Conditions {
			Expect(condition.ObservedGeneration).To(Equal(fetched.Generation),
				"condition %s carries a stale generation", condition.Type)
		}
	})

	It("names the missing instance rather than faking a placement", func() {
		tenant := createTenant("pgt-unbound", "unbound", nil)

		result := reconcileNow(reconciler, tenant)

		Expect(result.RequeueAfter).To(Equal(placementRetryInterval))
		fetched := refetch(tenant)
		Expect(fetched.Status.Binding).To(BeNil())
		Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.PgTenantPhaseBinding))

		bound := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionBound)
		Expect(bound.Status).To(Equal(metav1.ConditionFalse))
		Expect(bound.Reason).To(Equal(pgelasticv1alpha1.ReasonUnplaceable))
		Expect(bound.Message).To(ContainSubstring(placement.ReasonNoInstances))
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionReady).Status).
			To(Equal(metav1.ConditionFalse))
	})

	It("reports an unresolved pool as pending rather than as an error", func() {
		tenant := makeTenant(namespace, "pgt-no-pool", "pgt-pool-that-is-not-there", "no_pool")
		Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(tenant) })

		reconcileNow(reconciler, tenant)

		fetched := refetch(tenant)
		Expect(fetched.Status.Effective).To(BeNil())
		Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.PgTenantPhasePending))
		accepted := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionAccepted)
		Expect(accepted.Status).To(Equal(metav1.ConditionFalse))
		Expect(accepted.Reason).To(Equal(pgelasticv1alpha1.ReasonPending))
		Expect(accepted.Message).To(ContainSubstring("pgt-pool-that-is-not-there"))
	})
})
