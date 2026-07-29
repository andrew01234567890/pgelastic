//go:build e2e

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

package placement

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

const (
	// provisioningClass is the development sizing tier: three postmasters fit on one node
	// and it publishes 50 allocatable connections, which is the budget below.
	provisioningClass = "dev-1"
	// tenantBurstable is the ceiling mirrored onto the tenant's role.
	tenantBurstable = 25
	// instanceReadyTimeout covers initdb, two pg_basebackups and the quorum coming up.
	instanceReadyTimeout = 12 * time.Minute
	// tenantReadyTimeout covers a handful of exec round trips, no more.
	tenantReadyTimeout = 3 * time.Minute
)

// The `postgres` label is what keeps a real three-node instance out of the fast placement
// run. `make test-e2e-placement` filters it out; `make test-e2e-tenantdb` selects it.
var _ = Describe("provisioning a tenant's database on a real instance", Ordered, Label("postgres"), func() {
	const (
		instanceName  = "pgt-e2e"
		poolName      = "tenantdb-pool"
		className     = "tenantdb-class"
		workloadName  = "tenantdb-standard"
		reclaimedName = "e2e-reclaimed"
		reclaimedDB   = "e2e_reclaimed"
		reclaimedRole = "e2e_reclaimed_owner"
		retainedName  = "e2e-retained"
		retainedDB    = "e2e_retained"
	)

	var (
		reclaimed *pgelasticv1alpha1.PgTenant
		retained  *pgelasticv1alpha1.PgTenant
		primary   string
	)

	BeforeAll(func() {
		Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: provisioningNamespace},
		})).To(Succeed())

		elasticClass := &pgelasticv1alpha1.PgElasticClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec: pgelasticv1alpha1.PgElasticClassSpec{
				ControllerName: envOr("PGELASTIC_CONTROLLER_NAME", "pgelastic.io/elastic-pool-controller"),
			},
		}
		Expect(k8sClient.Create(suiteCtx, elasticClass)).To(Succeed())

		workloadClass := &pgelasticv1alpha1.PgWorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: workloadName},
			Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
				Priority: 1000,
				Capacity: pgelasticv1alpha1.WorkloadCapacity{
					Guaranteed: ptr.To(int32(2)),
					Burstable:  tenantBurstable,
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, workloadClass)).To(Succeed())

		pool := &pgelasticv1alpha1.PgElasticPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: provisioningNamespace},
			Spec: pgelasticv1alpha1.PgElasticPoolSpec{
				ClassRef: pgelasticv1alpha1.ClassReference{
					APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
					Kind:     "PgElasticClass",
					Name:     className,
				},
				Capacity:  pgelasticv1alpha1.PoolCapacity{BackendConnections: 50},
				Instances: pgelasticv1alpha1.PoolInstances{Template: instanceTemplate()},
				Admission: &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workloadName},
			},
		}
		Expect(k8sClient.Create(suiteCtx, pool)).To(Succeed())

		instance := &pgelasticv1alpha1.PgInstance{
			ObjectMeta: metav1.ObjectMeta{Name: instanceName, Namespace: provisioningNamespace},
			Spec: pgelasticv1alpha1.PgInstanceSpec{
				PoolRef: corev1.LocalObjectReference{Name: poolName},
				Class:   provisioningClass,
				Storage: pgelasticv1alpha1.InstanceStorage{
					Size:      resource.MustParse("1Gi"),
					WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("1Gi")},
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, instance)).To(Succeed())

		DeferCleanup(func() {
			for _, tenant := range []*pgelasticv1alpha1.PgTenant{reclaimed, retained} {
				if tenant != nil {
					Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, tenant))).To(Succeed())
				}
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, instance))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: provisioningNamespace},
			}))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, elasticClass))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, workloadClass))).To(Succeed())
		})
	})

	It("brings up a real PostgreSQL instance for the tenants to land on", func() {
		Eventually(func(g Gomega) {
			fetched := &pgelasticv1alpha1.PgInstance{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: provisioningNamespace, Name: instanceName,
			}, fetched)).To(Succeed())
			g.Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady))
			g.Expect(fetched.Status.CurrentPrimary).NotTo(BeEmpty())
			primary = fetched.Status.CurrentPrimary
		}).WithTimeout(instanceReadyTimeout).Should(Succeed())

		Expect(psql(primary, "postgres", "SELECT 1")).To(Equal("1"))
	})

	It("reports Ready only once PostgreSQL holds the database", func() {
		reclaimed = &pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{Name: reclaimedName, Namespace: provisioningNamespace},
			Spec: pgelasticv1alpha1.PgTenantSpec{
				PoolRef:       corev1.LocalObjectReference{Name: poolName},
				DatabaseName:  reclaimedDB,
				Owner:         ptr.To(reclaimedRole),
				ReclaimPolicy: ptr.To(pgelasticv1alpha1.ReclaimDelete),
			},
		}
		Expect(k8sClient.Create(suiteCtx, reclaimed)).To(Succeed())

		retained = &pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{Name: retainedName, Namespace: provisioningNamespace},
			Spec: pgelasticv1alpha1.PgTenantSpec{
				PoolRef:      corev1.LocalObjectReference{Name: poolName},
				DatabaseName: retainedDB,
			},
		}
		Expect(k8sClient.Create(suiteCtx, retained)).To(Succeed())

		for _, tenant := range []*pgelasticv1alpha1.PgTenant{reclaimed, retained} {
			Eventually(func(g Gomega) {
				fetched := &pgelasticv1alpha1.PgTenant{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: provisioningNamespace, Name: tenant.Name,
				}, fetched)).To(Succeed())

				ready := meta.FindStatusCondition(fetched.Status.Conditions,
					pgelasticv1alpha1.ConditionReady)
				g.Expect(ready).NotTo(BeNil(), "%s has no Ready condition yet", tenant.Name)
				g.Expect(ready.Status).To(Equal(metav1.ConditionTrue),
					"%s is not being served: %s / %s", tenant.Name, ready.Reason, ready.Message)
				g.Expect(ready.Reason).To(Equal(pgelasticv1alpha1.ReasonReady))
			}).WithTimeout(tenantReadyTimeout).Should(Succeed())
		}
	})

	// This is the assertion the CR could not make. The tenant reported Ready above; here
	// PostgreSQL is asked directly whether there is anything behind that claim.
	It("has both databases and both roles in PostgreSQL's own catalog", func() {
		for database, owner := range map[string]string{
			reclaimedDB: reclaimedRole,
			retainedDB:  retainedDB,
		} {
			Expect(psql(primary, "postgres", countDatabase(database))).To(Equal("1"),
				"database %q is not in pg_database on %s", database, primary)
			Expect(psql(primary, "postgres", countRole(owner))).To(Equal("1"),
				"role %q is not in pg_roles on %s", owner, primary)
			Expect(psql(primary, "postgres", fmt.Sprintf(
				`SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '%s'`, database))).
				To(Equal(owner), "database %q is owned by the wrong role", database)
		}
	})

	It("admits a connection to the tenant's own database", func() {
		Expect(psql(primary, reclaimedDB, "SELECT current_database()")).To(Equal(reclaimedDB))
	})

	It("mirrors the tenant's burstable ceiling onto the role as an in-database backstop", func() {
		Expect(psql(primary, "postgres", fmt.Sprintf(
			`SELECT rolconnlimit FROM pg_roles WHERE rolname = '%s'`, reclaimedRole))).
			To(Equal(fmt.Sprintf("%d", tenantBurstable)))
	})

	It("publishes the oid PostgreSQL actually assigned", func() {
		oid, err := psql(primary, "postgres", fmt.Sprintf(
			`SELECT oid FROM pg_database WHERE datname = '%s'`, reclaimedDB))
		Expect(err).NotTo(HaveOccurred())

		binding := fetchTenant(reclaimedName).Status.Binding
		Expect(binding).NotTo(BeNil())
		Expect(binding.DatabaseOID).NotTo(BeNil())
		Expect(fmt.Sprintf("%d", *binding.DatabaseOID)).To(Equal(oid))
	})

	It("keeps the database of a Retain tenant, which is the default", func() {
		Expect(k8sClient.Delete(suiteCtx, retained)).To(Succeed())
		Eventually(func() bool { return tenantGone(retainedName) }).Should(BeTrue())

		Expect(psql(primary, "postgres", countDatabase(retainedDB))).To(Equal("1"),
			"Retain dropped the database it was supposed to leave alone")
		Expect(psql(primary, "postgres", countRole(retainedDB))).To(Equal("1"))
	})

	It("drops the database and the role of a Delete tenant", func() {
		Expect(k8sClient.Delete(suiteCtx, reclaimed)).To(Succeed())
		Eventually(func() bool { return tenantGone(reclaimedName) }).Should(BeTrue())

		Expect(psql(primary, "postgres", countDatabase(reclaimedDB))).To(Equal("0"),
			"Delete left the database behind while releasing the finalizer")
		Expect(psql(primary, "postgres", countRole(reclaimedRole))).To(Equal("0"))
	})
})

func instanceTemplate() pgelasticv1alpha1.PgInstanceTemplate {
	return pgelasticv1alpha1.PgInstanceTemplate{
		Class: provisioningClass,
		Storage: pgelasticv1alpha1.InstanceStorage{
			Size:      resource.MustParse("1Gi"),
			WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("1Gi")},
		},
	}
}

func countDatabase(name string) string {
	return fmt.Sprintf(`SELECT count(*) FROM pg_database WHERE datname = '%s'`, name)
}

func countRole(name string) string {
	return fmt.Sprintf(`SELECT count(*) FROM pg_roles WHERE rolname = '%s'`, name)
}

func fetchTenant(name string) *pgelasticv1alpha1.PgTenant {
	GinkgoHelper()
	tenant := &pgelasticv1alpha1.PgTenant{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: provisioningNamespace, Name: name,
	}, tenant)).To(Succeed())
	return tenant
}

// tenantGone reports the object being fully removed, which only happens once the finalizer
// has been released — and the finalizer is only released once the reclaim policy's action
// has actually run.
func tenantGone(name string) bool {
	GinkgoHelper()
	err := k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: provisioningNamespace, Name: name,
	}, &pgelasticv1alpha1.PgTenant{})
	Expect(client.IgnoreNotFound(err)).To(Succeed())
	return err != nil
}
