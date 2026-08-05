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
	"math"
	"time"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
	"github.com/andrew01234567890/pgelastic/internal/placement"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		// Accepted, Ready, Placed and Throttled: the last is the storage cap's current-state
		// flag, set on every pass so that lifting it is as visible as applying it.
		Expect(fetched.Status.Conditions).To(HaveLen(4))
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

	It("leaves a tenant whose pool is gone untouched and comes back for it", func() {
		tenant := makeTenant(namespace, "pgt-no-pool", "pgt-pool-that-is-not-there", "no_pool")
		Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(tenant) })
		before := refetch(tenant).ResourceVersion

		result := reconcileNow(reconciler, tenant)

		Expect(result.RequeueAfter).To(Equal(ownership.RetryUnresolved))
		Expect(refetch(tenant).ResourceVersion).To(Equal(before))
	})
})

// statement_timeout's bare-number unit is milliseconds, and a Go duration prints spellings
// like "1m30s" that PostgreSQL will not parse. temp_file_limit's unit is kilobytes, and a
// limit rounded down to zero would mean "no temporary files at all" - the opposite of a small
// allowance - while -1 would mean no limit.
var _ = Describe("rendering a tenant's tier-2 limits for ALTER ROLE", func() {
	It("spells a statement timeout in milliseconds", func() {
		Expect(durationSetting(&metav1.Duration{Duration: 90 * time.Second})).To(Equal("90000ms"))
		Expect(durationSetting(&metav1.Duration{Duration: 1500 * time.Millisecond})).
			To(Equal("1500ms"))
	})

	// 0 is PostgreSQL's spelling of "no timeout at all", so truncating a sub-millisecond limit
	// to 0ms would turn the tightest bound an operator can ask for into no bound. Its sibling
	// already floored at 1kB for the same reason; this one did not.
	It("floors a sub-millisecond timeout rather than truncating it to no timeout", func() {
		Expect(durationSetting(&metav1.Duration{Duration: 500 * time.Microsecond})).To(Equal("1ms"))
		Expect(durationSetting(&metav1.Duration{Duration: time.Nanosecond})).To(Equal("1ms"))
	})

	// Past INT_MAX PostgreSQL refuses the ALTER outright with "value exceeds integer range",
	// which fails the whole provisioning pass rather than this one setting.
	It("caps both limits at PostgreSQL's own integer range", func() {
		Expect(durationSetting(&metav1.Duration{Duration: 1000 * time.Hour})).To(Equal("2147483647ms"))
		big := resource.NewQuantity(1<<62, resource.BinarySI)
		Expect(quantitySetting(big)).To(Equal("2147483647kB"))
	})

	// The rounding is done in kilobytes, because adding 1023 to the largest quantity the API
	// accepts overflows int64 - and an overflowed sum turned the most generous allowance an
	// operator can express into the most restrictive.
	It("does not invert the largest allowance into the smallest", func() {
		Expect(quantitySetting(resource.NewQuantity(math.MaxInt64, resource.BinarySI))).
			To(Equal("2147483647kB"))
	})

	It("treats an absent or non-positive timeout as undeclared", func() {
		Expect(durationSetting(nil)).To(BeEmpty())
		Expect(durationSetting(&metav1.Duration{})).To(BeEmpty())
		Expect(durationSetting(&metav1.Duration{Duration: -time.Second})).To(BeEmpty())
	})

	It("spells a temp file limit in kilobytes, rounding up", func() {
		Expect(quantitySetting(resource.NewQuantity(1<<30, resource.BinarySI))).
			To(Equal("1048576kB"))
		// Anything under a kilobyte is still an allowance, not a prohibition.
		Expect(quantitySetting(resource.NewQuantity(1, resource.BinarySI))).To(Equal("1kB"))
		Expect(quantitySetting(resource.NewQuantity(1025, resource.BinarySI))).To(Equal("2kB"))
	})

	It("treats an absent, zero or negative limit as undeclared", func() {
		Expect(quantitySetting(nil)).To(BeEmpty())
		Expect(quantitySetting(resource.NewQuantity(0, resource.BinarySI))).To(BeEmpty())
		Expect(quantitySetting(resource.NewQuantity(-1, resource.BinarySI))).To(BeEmpty())
	})
})

// The storage cap on ElasticClassDensity had zero production readers. A tenant past it now has
// its roles opened read-only by default, which is PostgreSQL deciding what a write is rather
// than the proxy guessing from statement text — an approach that was written, measured against
// a real backend, and withdrawn because BEGIN;…;COMMIT walked straight past it.
var _ = Describe("deciding whether a tenant is over its storage cap", func() {
	class := func(quota *resource.Quantity) *pgelasticv1alpha1.PgElasticClass {
		return &pgelasticv1alpha1.PgElasticClass{
			Spec: pgelasticv1alpha1.PgElasticClassSpec{
				Density: &pgelasticv1alpha1.ElasticClassDensity{MaxStoragePerTenant: quota},
			},
		}
	}
	tenantUsing := func(bytes *int64) *pgelasticv1alpha1.PgTenant {
		return &pgelasticv1alpha1.PgTenant{
			Status: pgelasticv1alpha1.PgTenantStatus{
				Utilization: &pgelasticv1alpha1.PgTenantUtilization{StorageBytes: bytes},
			},
		}
	}

	It("is over only when the measurement exceeds the cap", func() {
		quota := resource.NewQuantity(1000, resource.BinarySI)
		Expect(overStorageQuota(tenantUsing(ptrTo(int64(1001))), class(quota))).To(BeTrue())
		Expect(overStorageQuota(tenantUsing(ptrTo(int64(1000))), class(quota))).To(BeFalse())
		Expect(overStorageQuota(tenantUsing(ptrTo(int64(0))), class(quota))).To(BeFalse())
	})

	// Refusing until a figure arrives would refuse every tenant for the first scrape interval
	// of its life, and a cap that fires before anything is stored is not a cap.
	It("treats an unmeasured tenant as under its cap", func() {
		quota := resource.NewQuantity(1000, resource.BinarySI)
		Expect(overStorageQuota(tenantUsing(nil), class(quota))).To(BeFalse())
		Expect(overStorageQuota(&pgelasticv1alpha1.PgTenant{}, class(quota))).To(BeFalse())
	})

	// A class that draws no line, or whose class has gone, is one nobody has capped.
	It("treats an undeclared or absent cap as no cap", func() {
		Expect(overStorageQuota(tenantUsing(ptrTo(int64(1<<40))), class(nil))).To(BeFalse())
		Expect(overStorageQuota(tenantUsing(ptrTo(int64(1<<40))), nil)).To(BeFalse())
		zero := resource.NewQuantity(0, resource.BinarySI)
		Expect(overStorageQuota(tenantUsing(ptrTo(int64(1<<40))), class(zero))).To(BeFalse())
	})
})
