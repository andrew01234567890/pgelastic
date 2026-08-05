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

package v1alpha1

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

var _ = Describe("the PgTenant reservation ledger", Ordered, func() {
	const (
		namespace = "wh-ledger"
		poolName  = "wh-ledger-pool"
		half      = "wh-guaranteed-50"
		quarter   = "wh-guaranteed-25"
		single    = "wh-guaranteed-1"
	)

	BeforeAll(func() {
		ensureNamespace(namespace, nil)
		elasticClass := makeElasticClass("wh-ledger-class")
		mustCreate(elasticClass)
		mustCreate(makeWorkloadClass(half, 50, 50), makeWorkloadClass(quarter, 25, 25),
			makeWorkloadClass(single, 1, 1))
		mustCreate(makePool(namespace, poolName, elasticClass.Name))
	})

	It("admits a guarantee that fits inside allocatable capacity", func() {
		tenant := makeTenant(namespace, "wh-ledger-first", poolName, "ledger_first", half)

		mustCreate(tenant)
	})

	It("admits a guarantee that fits exactly, leaving nothing over", func() {
		tenant := makeTenant(namespace, "wh-ledger-boundary", poolName, "ledger_boundary", quarter)

		mustCreate(tenant)
	})

	It("refuses the guarantee that would take the pool past what it can honour", func() {
		tenant := makeTenant(namespace, "wh-ledger-overcommit", poolName, "ledger_overcommit", single)

		err := k8sClient.Create(ctx, tenant)

		Expect(err).To(MatchError(ContainSubstring("reserved 75")))
		Expect(err).To(MatchError(ContainSubstring("available 0")))
		Expect(err).To(MatchError(ContainSubstring("allocatable 75")))
	})

	It("admits a best-effort tenant into a fully reserved pool, because it reserves nothing", func() {
		mustCreate(makeWorkloadClass("wh-besteffort", 0, 8))
		tenant := makeTenant(namespace, "wh-ledger-besteffort", poolName, "ledger_besteffort", "wh-besteffort")

		mustCreate(tenant)
	})
})

var _ = Describe("PgTenant admission", func() {
	It("refuses a tenant naming a pool that does not exist", func() {
		ensureNamespace("wh-dangling", nil)
		tenant := makeTenant("wh-dangling", "wh-dangling-tenant", "wh-absent-pool", "dangling", "")

		err := k8sClient.Create(ctx, tenant)

		Expect(err).To(MatchError(ContainSubstring("no PgElasticPool of that name exists")))
	})

	It("refuses a workload class the pool's allowlist does not carry", func() {
		const namespace = "wh-allowlist"
		ensureNamespace(namespace, nil)
		elasticClass := makeElasticClass("wh-allowlist-class")
		mustCreate(elasticClass)
		mustCreate(makeWorkloadClass("wh-allowlist-permitted", 0, 8),
			makeWorkloadClass("wh-allowlist-forbidden", 0, 8))
		pool := makePool(namespace, "wh-allowlist-pool", elasticClass.Name)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{
			DefaultWorkloadClassName:  "wh-allowlist-permitted",
			AllowedWorkloadClassNames: []string{"wh-allowlist-permitted"},
		}
		mustCreate(pool)

		err := k8sClient.Create(ctx, makeTenant(namespace, "wh-allowlist-tenant", pool.Name,
			"allowlist", "wh-allowlist-forbidden"))

		Expect(err).To(MatchError(ContainSubstring("wh-allowlist-forbidden")))
		Expect(err).To(MatchError(ContainSubstring("supported values")))
	})

	It("refuses a tenant when no class names it, no default applies and no class is global", func() {
		const namespace = "wh-no-class"
		ensureNamespace(namespace, nil)
		elasticClass := makeElasticClass("wh-no-class-class")
		mustCreate(elasticClass)
		mustCreate(makePool(namespace, "wh-no-class-pool", elasticClass.Name))

		err := k8sClient.Create(ctx, makeTenant(namespace, "wh-no-class-tenant", "wh-no-class-pool",
			"no_class", ""))

		Expect(err).To(MatchError(ContainSubstring("no PgWorkloadClass is global")))
	})
})

var _ = Describe("bidirectional namespace consent", func() {
	const (
		goldLabel = "pgelastic.io/tier"
		goldTier  = "gold"
	)

	It("refuses a tenant whose namespace the pool's own selector does not admit", func() {
		const namespace = "wh-pool-consent"
		ensureNamespace(namespace, nil)
		elasticClass := makeElasticClass("wh-pool-consent-class")
		mustCreate(elasticClass, makeWorkloadClass("wh-pool-consent-workload", 0, 8))
		pool := makePool(namespace, "wh-pool-consent-pool", elasticClass.Name)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{
			DefaultWorkloadClassName: "wh-pool-consent-workload",
			AdmittedNamespaces: &pgelasticv1alpha1.NamespaceAdmission{
				From:     pgelasticv1alpha1.NamespaceFromSelector,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{goldLabel: goldTier}},
			},
		}
		mustCreate(pool)

		err := k8sClient.Create(ctx, makeTenant(namespace, "wh-pool-consent-tenant", pool.Name, "pool_consent", ""))

		Expect(err).To(MatchError(ContainSubstring("PgElasticPool \"wh-pool-consent-pool\" does not admit")))
	})

	It("refuses a tenant the pool admits but the class no longer does", func() {
		const namespace = "wh-class-consent"
		ensureNamespace(namespace, nil)
		elasticClass := makeElasticClass("wh-class-consent-class")
		elasticClass.Spec.AdmittedNamespaces = &pgelasticv1alpha1.NamespaceAdmission{
			From: pgelasticv1alpha1.NamespaceFromAll,
		}
		mustCreate(elasticClass, makeWorkloadClass("wh-class-consent-workload", 0, 8))
		pool := makePool(namespace, "wh-class-consent-pool", elasticClass.Name)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{
			DefaultWorkloadClassName: "wh-class-consent-workload",
			AdmittedNamespaces:       &pgelasticv1alpha1.NamespaceAdmission{From: pgelasticv1alpha1.NamespaceFromAll},
		}
		mustCreate(pool)

		By("tightening the class after the pool already exists")
		elasticClass.Spec.AdmittedNamespaces = &pgelasticv1alpha1.NamespaceAdmission{
			From:     pgelasticv1alpha1.NamespaceFromSelector,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{goldLabel: "platinum"}},
		}
		Expect(k8sClient.Update(ctx, elasticClass)).To(Succeed())

		err := k8sClient.Create(ctx, makeTenant(namespace, "wh-class-consent-tenant", pool.Name, "class_consent", ""))

		Expect(err).To(MatchError(ContainSubstring("PgElasticClass \"wh-class-consent-class\" does not admit")))
	})

	It("admits a tenant both the pool and the class select", func() {
		const namespace = "wh-both-consent"
		ensureNamespace(namespace, map[string]string{goldLabel: goldTier})
		elasticClass := makeElasticClass("wh-both-consent-class")
		elasticClass.Spec.AdmittedNamespaces = &pgelasticv1alpha1.NamespaceAdmission{
			From:     pgelasticv1alpha1.NamespaceFromSelector,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{goldLabel: goldTier}},
		}
		mustCreate(elasticClass, makeWorkloadClass("wh-both-consent-workload", 0, 8))
		pool := makePool(namespace, "wh-both-consent-pool", elasticClass.Name)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{
			DefaultWorkloadClassName: "wh-both-consent-workload",
			AdmittedNamespaces: &pgelasticv1alpha1.NamespaceAdmission{
				From:     pgelasticv1alpha1.NamespaceFromSelector,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{goldLabel: goldTier}},
			},
		}
		mustCreate(pool)

		mustCreate(makeTenant(namespace, "wh-both-consent-tenant", pool.Name, "both_consent", ""))
	})
})

// A database name is not a label. tenantdb adopts a database that already exists under the
// name rather than creating one, and then grants the second tenant's backend role CONNECT on
// it - so admitting the pair is admitting one tenant into the other's database. On separate
// instances it fails differently and worse: the routing table is keyed on the name, and a key
// written twice is a document no proxy replica can parse.
var _ = Describe("two PgTenants claiming one database name", Ordered, func() {
	const (
		namespace = "wh-dbname"
		poolName  = "wh-dbname-pool"
		otherPool = "wh-dbname-other"
		workload  = "wh-dbname-workload"
		database  = "shared_db"
	)

	BeforeAll(func() {
		ensureNamespace(namespace, nil)
		elasticClass := makeElasticClass("wh-dbname-class")
		mustCreate(elasticClass)
		mustCreate(makeWorkloadClass(workload, 0, 8))
		mustCreate(makePool(namespace, poolName, elasticClass.Name))
		mustCreate(makePool(namespace, otherPool, elasticClass.Name))
		mustCreate(makeTenant(namespace, "wh-dbname-holder", poolName, database, workload))
	})

	It("refuses the second, naming the tenant that already holds it", func() {
		second := makeTenant(namespace, "wh-dbname-second", poolName, database, workload)

		err := k8sClient.Create(ctx, second)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("wh-dbname-holder"))
		Expect(err.Error()).To(ContainSubstring("spec.databaseName"))
	})

	// The claim is over one pool's databases, not the namespace's: two pools are two sets of
	// instances, so the same name in each is two databases that never meet.
	It("admits the same name in a different pool", func() {
		mustCreate(makeTenant(namespace, "wh-dbname-elsewhere", otherPool, database, workload))
	})

	It("admits an update to the tenant that holds it, which collides only with itself", func() {
		held := &pgelasticv1alpha1.PgTenant{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace, Name: "wh-dbname-holder",
		}, held)).To(Succeed())
		held.Spec.Capacity = &pgelasticv1alpha1.PgTenantCapacity{Guaranteed: ptrTo(int32(1))}

		Expect(k8sClient.Update(ctx, held)).To(Succeed())
	})
})

var _ = Describe("PgTenant webhook during deletion", func() {
	It("admits a write to a tenant being deleted whose pool is already gone", func() {
		validator := &PgTenantCustomValidator{Reader: k8sClient}
		tenant := &pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "doomed",
				Namespace:         "default",
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
				Finalizers:        []string{"pgelastic.io/tenant-database"},
			},
			Spec: pgelasticv1alpha1.PgTenantSpec{
				PoolRef:      corev1.LocalObjectReference{Name: "pool-that-does-not-exist"},
				DatabaseName: "doomed",
			},
		}

		_, err := validator.ValidateUpdate(ctx, tenant, tenant)
		Expect(err).NotTo(HaveOccurred())
	})
})

// A tenant's capacity is the class merged with the override, field by field, so raising
// guaranteed alone - or lowering burstable alone - produces a floor above its own ceiling.
// Nothing refused it, and the component that found out was the proxy: the allocator refuses
// the claim when it loads the document, which lands on a pool-wide reload rather than on the
// object that caused it.
var _ = Describe("a tenant guaranteed more than it may ever hold", Ordered, func() {
	const (
		namespace = "wh-incoherent"
		poolName  = "wh-incoherent-pool"
		workload  = "wh-incoherent-class"
	)

	BeforeAll(func() {
		ensureNamespace(namespace, nil)
		elasticClass := makeElasticClass("wh-incoherent-elastic")
		mustCreate(elasticClass)
		mustCreate(makeWorkloadClass(workload, 2, 8))
		mustCreate(makePool(namespace, poolName, elasticClass.Name))
	})

	It("refuses an override that raises the floor above the class ceiling", func() {
		tenant := makeTenant(namespace, "wh-incoherent-floor", poolName, "incoherent_floor", workload)
		tenant.Spec.Capacity = &pgelasticv1alpha1.PgTenantCapacity{Guaranteed: ptrTo(int32(9))}

		err := k8sClient.Create(ctx, tenant)

		Expect(err).To(MatchError(ContainSubstring("spec.capacity.guaranteed")))
		Expect(err).To(MatchError(ContainSubstring("above the burstable ceiling")))
	})

	It("refuses an override that lowers the ceiling under the class floor", func() {
		tenant := makeTenant(namespace, "wh-incoherent-ceiling", poolName, "incoherent_ceiling", workload)
		tenant.Spec.Capacity = &pgelasticv1alpha1.PgTenantCapacity{Burstable: ptrTo(int32(1))}

		err := k8sClient.Create(ctx, tenant)

		Expect(err).To(MatchError(ContainSubstring("spec.capacity.burstable")))
	})

	It("admits an override that keeps the two coherent", func() {
		tenant := makeTenant(namespace, "wh-incoherent-ok", poolName, "incoherent_ok", workload)
		tenant.Spec.Capacity = &pgelasticv1alpha1.PgTenantCapacity{Guaranteed: ptrTo(int32(8))}

		mustCreate(tenant)
	})

})

// The reconciler's version of this rule skips a sibling carrying a deletion timestamp; this
// validator did not, and it runs on update as well as create. So while a duplicate was
// reclaiming its database - which takes as long as the reclaim takes - the tenant that is
// keeping the name could not be edited at all, and the refusal named an object on its way out.
var _ = Describe("a duplicate database name held by a tenant that is going away", Ordered, func() {
	const (
		namespace = "wh-dup-terminating"
		className = "wh-dup-terminating-workload"
		shared    = "shared"
	)

	var leaving *pgelasticv1alpha1.PgTenant

	BeforeAll(func() {
		ensureNamespace(namespace, nil)
		mustCreate(makeElasticClass("wh-dup-terminating-class"),
			makeWorkloadClass(className, 1, 4))
		mustCreate(makePool(namespace, "wh-dup-terminating-pool", "wh-dup-terminating-class"))

		leaving = makeTenant(namespace, "wh-dup-leaving", "wh-dup-terminating-pool",
			shared, className)
		leaving.Finalizers = []string{"pgelastic.io/test-hold"}
		mustCreate(leaving)

		// Deleted, but held by the finalizer - which is exactly the window a reclaim runs in.
		Expect(k8sClient.Delete(ctx, leaving)).To(Succeed())
		Expect(k8sClient.Get(ctx, keyIn(namespace, leaving.Name), leaving)).To(Succeed())
		Expect(leaving.DeletionTimestamp).NotTo(BeNil())

		DeferCleanup(func() {
			if err := k8sClient.Get(ctx, keyIn(namespace, leaving.Name), leaving); err == nil {
				leaving.Finalizers = nil
				Expect(k8sClient.Update(ctx, leaving)).To(Succeed())
			}
		})
	})

	It("lets the name be claimed again", func() {
		successor := makeTenant(namespace, "wh-dup-successor", "wh-dup-terminating-pool",
			shared, className)

		Expect(k8sClient.Create(ctx, successor)).To(Succeed())
	})
})
