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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/metering"
)

func newPoolReconciler() *PgElasticPoolReconciler {
	return &PgElasticPoolReconciler{
		Client:   k8sClient,
		Scheme:   k8sClient.Scheme(),
		Metering: metering.NewCollector(metering.Options{}, nil),
	}
}

// makeMember is an instance somebody wrote by hand and pointed at a pool, which is how every
// member in this tree came to exist before the pool could make one.
func makeMember(namespace, name, pool string) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef: corev1.LocalObjectReference{Name: pool},
			Class:   instanceClassName,
			Storage: pgelasticv1alpha1.InstanceStorage{
				Size:      *quantity("100Gi"),
				WALVolume: pgelasticv1alpha1.WALVolume{Size: *quantity("20Gi")},
			},
		},
	}
}

var ownedPoolNamespaces int

var _ = Describe("a pool provisioning its own members", func() {
	var (
		namespace string
		poolName  = "owned-pool"
		className = "owned-class"
	)

	// membersOf reads straight from the API server rather than from the cache, because what
	// is being asserted is how many objects exist and a stale list would answer a different
	// question.
	membersOf := func() []pgelasticv1alpha1.PgInstance {
		GinkgoHelper()
		list := &pgelasticv1alpha1.PgInstanceList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace(namespace))).To(Succeed())
		members := make([]pgelasticv1alpha1.PgInstance, 0, len(list.Items))
		for i := range list.Items {
			if list.Items[i].Spec.PoolRef.Name == poolName {
				members = append(members, list.Items[i])
			}
		}
		return members
	}

	// settle reconciles until the member count stops moving, since one pass creates one
	// member and the controller relies on its own watch to be called again.
	settle := func(pool *pgelasticv1alpha1.PgElasticPool) {
		GinkgoHelper()
		reconciler := newPoolReconciler()
		for range 10 {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pool),
			})
			Expect(err).NotTo(HaveOccurred())
		}
	}

	declare := func(replicas int32) *pgelasticv1alpha1.PgElasticPool {
		GinkgoHelper()
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx,
			makeElasticClass(className, defaultControllerName)))).To(Succeed())
		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Instances.Replicas = ptrTo(replicas)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)
		return pool
	}

	BeforeEach(func() {
		ownedPoolNamespaces++
		namespace = fmt.Sprintf("owned-pool-%d", ownedPoolNamespaces)
		ensureNamespace(namespace)
	})

	It("materializes the members its spec declares", func() {
		pool := declare(2)
		settle(pool)

		members := membersOf()
		Expect(members).To(HaveLen(2))
		for i := range members {
			Expect(members[i].Spec.Class).To(Equal(instanceClassName))
			Expect(members[i].Spec.Storage.Size.String()).To(Equal("100Gi"),
				"%s was not stamped from the template", members[i].Name)
			Expect(members[i].Spec.Storage.WALVolume.Size.String()).To(Equal("20Gi"))
		}
	})

	It("owns what it made, so the members are reclaimed with it", func() {
		pool := declare(1)
		settle(pool)

		members := membersOf()
		Expect(members).To(HaveLen(1))
		owner := metav1.GetControllerOf(&members[0])
		Expect(owner).NotTo(BeNil(), "the member the pool created carries no controller reference")
		Expect(owner.Name).To(Equal(poolName))
		Expect(owner.Kind).To(Equal("PgElasticPool"))
	})

	It("stops at the declared count rather than growing every pass", func() {
		pool := declare(2)
		settle(pool)
		Expect(membersOf()).To(HaveLen(2))

		settle(pool)
		Expect(membersOf()).To(HaveLen(2),
			"a second settling made more members, so the count is written rather than observed")
	})

	// The membership rule is poolRef and has always been poolRef. Every existing suite and
	// the demo hand-write their members, so a provisioner that counted only what it owned
	// would double every one of them.
	It("adopts a hand-written member instead of duplicating it", func() {
		handWritten := makeMember(namespace, "hand-written", poolName)
		Expect(k8sClient.Create(ctx, handWritten)).To(Succeed())
		awaitCached(handWritten)

		pool := declare(2)
		settle(pool)

		members := membersOf()
		Expect(members).To(HaveLen(2), "the pool did not count the member it did not make")

		names := make([]string, 0, len(members))
		for i := range members {
			names = append(names, members[i].Name)
		}
		Expect(names).To(ContainElement("hand-written"))

		By("checking the adopted member was not seized")
		adopted := &pgelasticv1alpha1.PgInstance{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{
			Namespace: namespace, Name: "hand-written",
		}, adopted)).To(Succeed())
		Expect(metav1.GetControllerOf(adopted)).To(BeNil(),
			"the pool took ownership of an instance somebody else created, so deleting the "+
				"pool would now delete it")
	})

	// A pool and the members somebody wrote for it arrive together and nothing orders them, so
	// a pool reconciled in between looks exactly like a pool with none. Members made in that
	// window cannot be taken back.
	It("makes nothing on the first sighting of a spec", func() {
		pool := declare(2)

		reconciler := newPoolReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(pool),
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(membersOf()).To(BeEmpty(),
			"the pool provisioned on the pass that first saw its spec; an apply that lands the "+
				"members a moment later now has more than it asked for")
	})

	// The class is where a platform team bounds what a pool may command the operator to build.
	It("stops at the ceiling its class sets, whatever the pool declares", func() {
		class := makeElasticClass(className, defaultControllerName)
		class.Spec.Density = &pgelasticv1alpha1.ElasticClassDensity{
			MaxInstancesPerPool: ptrTo(int32(1)),
		}
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, class))).To(Succeed())
		fresh := &pgelasticv1alpha1.PgElasticClass{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(class), fresh)).To(Succeed())
		fresh.Spec.Density = &pgelasticv1alpha1.ElasticClassDensity{
			MaxInstancesPerPool: ptrTo(int32(1)),
		}
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())
		awaitCached(fresh)

		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Instances.Replicas = ptrTo(int32(6))
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)
		settle(pool)

		Expect(membersOf()).To(HaveLen(1),
			"the pool built past the ceiling its class sets, so the cap is advice")
	})

	It("reports the members it declares, not just the ones it has", func() {
		pool := declare(3)
		reconciler := newPoolReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(pool),
		})
		Expect(err).NotTo(HaveOccurred())

		fetched := &pgelasticv1alpha1.PgElasticPool{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), fetched)).To(Succeed())
		Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.PoolPhaseProvisioning))

		ready := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionReady)
		Expect(ready.Message).To(ContainSubstring("3"),
			"the pool reports %q, which never mentions the count it was asked for", ready.Message)
	})
})
