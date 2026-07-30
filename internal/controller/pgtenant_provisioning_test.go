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
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/tenantdb"
	"github.com/andrew01234567890/pgelastic/internal/tenantdb/tenantdbtest"
)

var _ = Describe("PgTenant database provisioning", Ordered, func() {
	const (
		namespace    = "pgt-provisioning"
		poolName     = "prov-pool"
		className    = "prov-class"
		workloadName = "prov-standard"
		burstable    = int32(40)
		instanceName = "prov-a"
	)

	var (
		reconciler *PgTenantReconciler
		cluster    *tenantdbtest.Cluster
	)

	BeforeAll(func() {
		ensureNamespace(namespace)
		elasticClass := makeElasticClass(className, defaultControllerName)
		pool := makePool(namespace, poolName, className, 900)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workloadName}
		class := makeWorkloadClass(workloadName, 2, burstable)

		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Create(ctx, class)).To(Succeed())
		instance := makeReadyInstance(namespace, instanceName, poolName, 225, 40)

		DeferCleanup(func() { deleteAndAwait(instance, pool, elasticClass, class) })
		awaitCached(elasticClass, pool, class, instance)
	})

	BeforeEach(func() {
		cluster = tenantdbtest.NewCluster()
		reconciler = &PgTenantReconciler{Client: cachedClient, Scheme: cachedClient.Scheme(), SQL: cluster}
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

	readyOf := func(tenant *pgelasticv1alpha1.PgTenant) *metav1.Condition {
		GinkgoHelper()
		return conditionOf(refetch(tenant).Status.Conditions, pgelasticv1alpha1.ConditionReady)
	}

	It("creates the role, then the database it owns, and only then reports Ready", func() {
		tenant := createTenant("prov-happy", "prov_happy", nil)

		reconcileNow(reconciler, tenant)

		// The role is derived from the tenant's identity rather than named after its
		// database, so two tenants that chose the same spec.owner cannot share one - which,
		// now that these roles carry credentials, would be a merge of two identities.
		role := migration.BackendRoleName(namespace, "prov-happy")
		Expect(cluster.HasRole(role)).To(BeTrue())
		Expect(cluster.HasDatabase("prov_happy")).To(BeTrue())
		Expect(cluster.OwnerOf("prov_happy")).To(Equal(role))

		fetched := refetch(tenant)
		ready := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionReady)
		Expect(ready.Status).To(Equal(metav1.ConditionTrue))
		Expect(ready.Reason).To(Equal(pgelasticv1alpha1.ReasonReady))
		Expect(ready.Message).To(ContainSubstring("prov_happy"))
		Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.PgTenantPhaseReady))
		Expect(fetched.Status.Binding.DatabaseOID).NotTo(BeNil())
		Expect(*fetched.Status.Binding.DatabaseOID).To(BeNumerically(">", int64(0)))
	})

	It("holds the tenant with a finalizer before it creates anything", func() {
		tenant := createTenant("prov-held", "prov_held", nil)

		reconcileNow(reconciler, tenant)

		Expect(refetch(tenant).Finalizers).To(ContainElement(TenantDatabaseFinalizer))
	})

	// Deliberately uncapped. rolconnlimit was a harmless backstop while nothing logged in as
	// the tenant's role; now that the proxy authenticates as it, every backend the fleet opens
	// counts against it, and N replicas each entitled to burstable would breach a limit of
	// burstable by a factor of N. The proxy's own ledger is the ceiling that means anything.
	It("leaves the role uncapped, because the fleet is what bounds it", func() {
		tenant := createTenant("prov-limit", "prov_limit", nil)

		reconcileNow(reconciler, tenant)

		role := migration.BackendRoleName(namespace, "prov-limit")
		Expect(cluster.ConnectionLimit(role)).To(Equal(tenantdb.NoConnectionLimit))
	})

	It("issues no statement that changes anything on a second pass", func() {
		tenant := createTenant("prov-idempotent", "prov_idempotent", nil)

		reconcileNow(reconciler, tenant)
		cluster.Forget()
		reconcileNow(reconciler, tenant)

		for _, ddl := range []string{"CREATE", "ALTER", "DROP"} {
			Expect(cluster.Ran(ddl)).To(Equal(0),
				"the second pass issued %s: %v", ddl, cluster.Statements())
		}
		Expect(readyOf(tenant).Status).To(Equal(metav1.ConditionTrue))
	})

	It("is not Ready while the database does not exist, and says why", func() {
		cluster.FailOn("CREATE DATABASE", errors.New("permission denied to create database"))
		tenant := createTenant("prov-refused", "prov_refused", nil)

		reconcileNow(reconciler, tenant)

		Expect(cluster.HasDatabase("prov_refused")).To(BeFalse())
		fetched := refetch(tenant)
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionBound).Status).
			To(Equal(metav1.ConditionTrue))

		ready := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionReady)
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(tenantdb.ReasonProvisioningFailed))
		Expect(ready.Message).To(ContainSubstring("permission denied"))
		Expect(ready.Message).To(ContainSubstring("prov_refused"))
		Expect(fetched.Status.Phase).NotTo(Equal(pgelasticv1alpha1.PgTenantPhaseReady))
	})

	It("becomes Ready once the failure that was reported goes away", func() {
		cluster.FailOn("CREATE DATABASE", errors.New("permission denied to create database"))
		tenant := createTenant("prov-recovers", "prov_recovers", nil)
		reconcileNow(reconciler, tenant)
		Expect(readyOf(tenant).Status).To(Equal(metav1.ConditionFalse))

		cluster.Heal("CREATE DATABASE")
		reconcileNow(reconciler, tenant)

		Expect(cluster.HasDatabase("prov_recovers")).To(BeTrue())
		Expect(readyOf(tenant).Status).To(Equal(metav1.ConditionTrue))
	})

	It("refuses to call a database Ready when no transport can reach it", func() {
		reconciler.SQL = nil
		tenant := createTenant("prov-no-transport", "prov_no_transport", nil)

		reconcileNow(reconciler, tenant)

		ready := readyOf(tenant)
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(tenantdb.ReasonProvisioning))
		Expect(refetch(tenant).Finalizers).To(BeEmpty(),
			"a tenant that created nothing must not be held for a reclaim")
	})

	It("keeps the database when the reclaim policy is the default", func() {
		tenant := createTenant("prov-retain", "prov_retain", nil)
		reconcileNow(reconciler, tenant)
		Expect(refetch(tenant).Spec.ReclaimPolicy).To(HaveValue(Equal(pgelasticv1alpha1.ReclaimRetain)))

		Expect(k8sClient.Delete(ctx, tenant)).To(Succeed())
		reconcileNow(reconciler, tenant)

		Expect(cluster.HasDatabase("prov_retain")).To(BeTrue())
		Expect(cluster.HasRole(migration.BackendRoleName(namespace, "prov-retain"))).To(BeTrue())
		Expect(cluster.Ran("DROP")).To(Equal(0))
		Eventually(func() bool { return present(tenant) }).Should(BeFalse())
	})

	It("drops the database and the role when the reclaim policy is Delete", func() {
		tenant := createTenant("prov-delete", "prov_delete", func(tenant *pgelasticv1alpha1.PgTenant) {
			tenant.Spec.ReclaimPolicy = ptrTo(pgelasticv1alpha1.ReclaimDelete)
		})
		reconcileNow(reconciler, tenant)
		Expect(cluster.HasDatabase("prov_delete")).To(BeTrue())

		Expect(k8sClient.Delete(ctx, tenant)).To(Succeed())
		reconcileNow(reconciler, tenant)

		Expect(cluster.HasDatabase("prov_delete")).To(BeFalse())
		Expect(cluster.HasRole(migration.BackendRoleName(namespace, "prov-delete"))).To(BeFalse())
		Eventually(func() bool { return present(tenant) }).Should(BeFalse())
	})

	It("keeps the finalizer until the drop it was holding for actually happened", func() {
		tenant := createTenant("prov-stuck", "prov_stuck", func(tenant *pgelasticv1alpha1.PgTenant) {
			tenant.Spec.ReclaimPolicy = ptrTo(pgelasticv1alpha1.ReclaimDelete)
		})
		reconcileNow(reconciler, tenant)
		cluster.FailOn("DROP DATABASE", errors.New("cannot drop the currently open database"))

		Expect(k8sClient.Delete(ctx, tenant)).To(Succeed())
		awaitCached(tenant)
		_, err := reconciler.Reconcile(ctx, requestFor(tenant))
		Expect(err).To(HaveOccurred())

		fetched := refetch(tenant)
		Expect(fetched.Finalizers).To(ContainElement(TenantDatabaseFinalizer))
		Expect(cluster.HasDatabase("prov_stuck")).To(BeTrue())
		ready := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionReady)
		Expect(ready.Reason).To(Equal(tenantdb.ReasonReclaimFailed))
		Expect(ready.Message).To(ContainSubstring("cannot drop the currently open database"))

		cluster.Heal("DROP DATABASE")
		reconcileNow(reconciler, tenant)

		Expect(cluster.HasDatabase("prov_stuck")).To(BeFalse())
		Eventually(func() bool { return present(tenant) }).Should(BeFalse())
	})
})
