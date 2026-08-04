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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Everything else about provisioning is asserted on object shape, and object shape is where
// a provisioner can be wrong in the way that looks right: a PgInstance carrying the
// template's fields proves the pool built the declaration, not that the declaration builds
// PostgreSQL. That is the assertion a reviewer would otherwise assume the cheap specs made.
//
// So this one declares a member, waits for the pool to make it, and then asks the postmaster
// the pool never touched whether the tenant's database is there.
var _ = Describe("a member the pool made", Ordered, Label("postgres"), func() {
	const (
		poolName     = "made-pool"
		className    = "made-class"
		workloadName = "made-standard"
		tenantName   = "e2e-made-tenant"
		tenantDB     = "e2e_made_tenant"
	)

	var member string

	// The one namespace this suite runs a real instance controller in, and the one psql
	// reaches. A pool anywhere else would have its members created and never reconciled,
	// which is a spec that cannot pass rather than one that fails.
	namespace := func() string { return provisioningNamespace }

	BeforeAll(func() {
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace()},
		}))).To(Succeed())

		elasticClass := &pgelasticv1alpha1.PgElasticClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec:       pgelasticv1alpha1.PgElasticClassSpec{ControllerName: suiteControllerName},
		}
		// Cleanup is registered as each object is made, not once at the end: these two are
		// cluster-scoped, and a BeforeAll that fails halfway leaves whatever it made behind to
		// fail the next run in a way that looks nothing like the original failure.
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, elasticClass))).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, elasticClass))).To(Succeed())
		})

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
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, workloadClass))).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, workloadClass))).To(Succeed())
		})

		// No instance is written here. That is the whole spec.
		Expect(k8sClient.Create(suiteCtx, &pgelasticv1alpha1.PgElasticPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: namespace()},
			Spec: pgelasticv1alpha1.PgElasticPoolSpec{
				ClassRef: pgelasticv1alpha1.ClassReference{
					APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
					Kind:     elasticClassKind,
					Name:     className,
				},
				Capacity:  pgelasticv1alpha1.PoolCapacity{BackendConnections: 50},
				Instances: pgelasticv1alpha1.PoolInstances{Replicas: ptr.To(int32(1)), Template: instanceTemplate()},
				Admission: &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workloadName},
			},
		})).To(Succeed())

		// The namespace is shared with the container that stands up the suite's other real
		// instance, so this cleans up what it made and leaves the namespace to the suite.
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &pgelasticv1alpha1.PgTenant{
				ObjectMeta: metav1.ObjectMeta{Name: tenantName, Namespace: namespace()},
			}))).To(Succeed())
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: namespace(), Name: tenantName,
				}, &pgelasticv1alpha1.PgTenant{}))
			}, "5m", "2s").Should(BeTrue())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: namespace()},
			}))).To(Succeed())
		})
	})

	It("becomes a PostgreSQL that answers", func() {
		var instance pgelasticv1alpha1.PgInstance
		Eventually(func(g Gomega) {
			list := &pgelasticv1alpha1.PgInstanceList{}
			g.Expect(k8sClient.List(suiteCtx, list, client.InNamespace(namespace()))).To(Succeed())
			members := make([]pgelasticv1alpha1.PgInstance, 0, len(list.Items))
			for i := range list.Items {
				if list.Items[i].Spec.PoolRef.Name == poolName {
					members = append(members, list.Items[i])
				}
			}
			g.Expect(members).To(HaveLen(1), "the pool declared a member and made %d", len(members))
			instance = members[0]
			g.Expect(instance.Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady))
			g.Expect(instance.Status.CurrentPrimary).NotTo(BeEmpty())
		}).WithTimeout(instanceReadyTimeout).Should(Succeed())

		member = instance.Status.CurrentPrimary
		Expect(psql(member, "postgres", "SELECT 1")).To(Equal("1"))
	})

	It("carries a tenant's database, asked of the postmaster", func() {
		Expect(k8sClient.Create(suiteCtx, &pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{Name: tenantName, Namespace: namespace()},
			Spec: pgelasticv1alpha1.PgTenantSpec{
				PoolRef:      corev1.LocalObjectReference{Name: poolName},
				DatabaseName: tenantDB,
			},
		})).To(Succeed())

		Eventually(func(g Gomega) {
			fetched := &pgelasticv1alpha1.PgTenant{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: namespace(), Name: tenantName,
			}, fetched)).To(Succeed())
			g.Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.PgTenantPhaseReady))
		}).WithTimeout(tenantReadyTimeout).Should(Succeed())

		Expect(psql(member, "postgres", countDatabase(tenantDB))).To(Equal("1"),
			"the tenant reports Ready but the database is not in the catalog of the member "+
				"the pool provisioned")
	})
})
