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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// The CRD says of the instance template: "pgelastic provisions these; they are not adopted
// from elsewhere." Nothing in the tree called Create for a PgInstance, so every member in
// every suite and in the demo was hand-written, and spec.instances.replicas was a number the
// autoscaler moved and nobody acted on.
//
// Object shape only: what is being proved is that a declaration becomes members carrying the
// template's shape. Whether those members become PostgreSQL is the postgres-labelled spec's
// job, and it costs an order of magnitude more to run.
var _ = Describe("a pool provisioning the members it declares", Ordered, func() {
	var (
		namespace string
		poolName  = "e2e-owned-pool"
		className = "e2e-owned-class"
	)

	declare := func(name string, replicas int32) *pgelasticv1alpha1.PgElasticPool {
		GinkgoHelper()
		pool := &pgelasticv1alpha1.PgElasticPool{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: pgelasticv1alpha1.PgElasticPoolSpec{
				ClassRef: pgelasticv1alpha1.ClassReference{
					APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
					Kind:     elasticClassKind,
					Name:     className,
				},
				Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 240},
				Instances: pgelasticv1alpha1.PoolInstances{
					Replicas: ptr.To(replicas),
					Template: pgelasticv1alpha1.PgInstanceTemplate{
						Class: memberClass,
						Storage: pgelasticv1alpha1.InstanceStorage{
							Size:      resource.MustParse("11Gi"),
							WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("3Gi")},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, pool)).To(Succeed())
		return pool
	}

	membersOf := func(pool string) []pgelasticv1alpha1.PgInstance {
		GinkgoHelper()
		list := &pgelasticv1alpha1.PgInstanceList{}
		Expect(k8sClient.List(suiteCtx, list, client.InNamespace(namespace))).To(Succeed())
		members := make([]pgelasticv1alpha1.PgInstance, 0, len(list.Items))
		for i := range list.Items {
			if list.Items[i].Spec.PoolRef.Name == pool {
				members = append(members, list.Items[i])
			}
		}
		return members
	}

	BeforeAll(func() {
		namespace = uniqueNamespace("pgelastic-owned")
		Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())

		elasticClass := &pgelasticv1alpha1.PgElasticClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec:       pgelasticv1alpha1.PgElasticClassSpec{ControllerName: suiteControllerName},
		}
		Expect(k8sClient.Create(suiteCtx, elasticClass)).To(Succeed())

		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespace},
			}))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, elasticClass))).To(Succeed())
		})
	})

	It("creates the members it declares, carrying the template's shape", func() {
		declare(poolName, 2)

		Eventually(func(g Gomega) {
			members := membersOf(poolName)
			g.Expect(members).To(HaveLen(2))
			for i := range members {
				g.Expect(members[i].Spec.Class).To(Equal(memberClass))
				g.Expect(members[i].Spec.Storage.Size).To(Equal(resource.MustParse("11Gi")),
					"%s does not carry the template's data volume", members[i].Name)
				g.Expect(members[i].Spec.Storage.WALVolume.Size).To(Equal(resource.MustParse("3Gi")))
				g.Expect(metav1.GetControllerOf(&members[i])).NotTo(BeNil(),
					"%s is not owned by the pool that made it", members[i].Name)
			}
		}).Should(Succeed())
	})

	// The members this container makes are real PgInstances, and every one of them puts a
	// StatefulSet and two volumes on the API server for a suite that provisions no PostgreSQL
	// and needs neither. Left standing they are load the specs below inherit, and the tenant
	// skew this suite asserts elsewhere is a distribution that load skews. Everything else about
	// provisioning - the count settling, adoption, the declared-vs-actual report - is asserted
	// against a real API server in internal/controller, where it costs nothing to run.
	AfterAll(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &pgelasticv1alpha1.PgElasticPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: namespace},
		}))).To(Succeed())
	})
})
