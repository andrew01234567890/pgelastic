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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// A member carries the pool's ownerReference, so deleting the pool is a cascade - and a
// member here is not a replica of anything. It is a primary holding up to a couple of hundred
// tenants' databases with no copy elsewhere, which makes `kubectl delete pgelasticpool` one
// tab-completion away from the most destructive thing in the product.
var _ = Describe("deleting a pool that made its own members", func() {
	var (
		namespace string
		poolName  = "doomed-pool"
		className = "doomed-class"
	)

	BeforeEach(func() {
		doomedNamespaces++
		namespace = fmt.Sprintf("doomed-%d", doomedNamespaces)
		ensureNamespace(namespace)
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx,
			makeElasticClass(className, defaultControllerName)))).To(Succeed())
	})

	settle := func(pool *pgelasticv1alpha1.PgElasticPool) {
		GinkgoHelper()
		reconciler := newPoolReconciler()
		for range 6 {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pool),
			})
			Expect(err).NotTo(HaveOccurred())
		}
	}

	provisioned := func() (*pgelasticv1alpha1.PgElasticPool, *pgelasticv1alpha1.PgInstance) {
		GinkgoHelper()
		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Instances.Replicas = ptrTo(int32(1))
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)
		settle(pool)

		list := &pgelasticv1alpha1.PgInstanceList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace(namespace))).To(Succeed())
		Expect(list.Items).To(HaveLen(1))
		return refetch(pool), &list.Items[0]
	}

	// Real tenants, bound the way the tenant controller binds them. The status field that
	// looks like this count is written by nothing, so a fixture stamping it would prove the
	// hold works while the hold was in fact finding every member empty.
	holding := func(instance *pgelasticv1alpha1.PgInstance, count int) []*pgelasticv1alpha1.PgTenant {
		GinkgoHelper()
		fresh := refetch(instance)
		fresh.Status.Phase = pgelasticv1alpha1.InstancePhaseReady
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
		awaitCached(fresh)

		held := make([]*pgelasticv1alpha1.PgTenant, 0, count)
		for i := range count {
			name := fmt.Sprintf("held-%d-%d", doomedNamespaces, i)
			tenant := makeTenant(namespace, name, poolName, strings.ReplaceAll(name, "-", "_"))
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
			bound := refetch(tenant)
			bound.Status.Binding = &pgelasticv1alpha1.PgTenantBinding{
				InstanceRef: &corev1.LocalObjectReference{Name: instance.Name},
				BoundAt:     &metav1.Time{Time: time.Now()},
			}
			Expect(k8sClient.Status().Update(ctx, bound)).To(Succeed())
			awaitCached(bound)
			held = append(held, bound)
		}
		return held
	}

	release := func(held []*pgelasticv1alpha1.PgTenant) {
		GinkgoHelper()
		for _, tenant := range held {
			deleteAndAwait(tenant)
		}
	}

	It("holds the deletion while a member it made still holds tenants", func() {
		pool, member := provisioned()
		held := holding(member, 3)
		DeferCleanup(func() { release(held) })

		Expect(k8sClient.Delete(ctx, refetch(pool))).To(Succeed())
		settle(pool)

		fetched := &pgelasticv1alpha1.PgElasticPool{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), fetched)).To(Succeed(),
			"the pool finished deleting, and garbage collection is now reclaiming a member "+
				"holding three tenants' databases")
		Expect(fetched.Finalizers).To(ContainElement(PgElasticPoolMembersFinalizer))
	})

	It("names the members and the tenants it is holding for", func() {
		Expect(occupiedMembersOf(&pgelasticv1alpha1.PgElasticPool{}, nil, nil)).To(BeEmpty())

		message := heldMessage([]occupiedMember{{name: "some-member", tenants: 3}})
		Expect(message).To(ContainSubstring("some-member"))
		Expect(message).To(ContainSubstring("3"))
	})

	It("completes once the members it made hold nothing", func() {
		pool, member := provisioned()
		held := holding(member, 3)

		Expect(k8sClient.Delete(ctx, refetch(pool))).To(Succeed())
		settle(pool)

		release(held)
		settle(pool)

		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx,
				client.ObjectKeyFromObject(pool), &pgelasticv1alpha1.PgElasticPool{}))
		}).Should(BeTrue(), "the pool never finished deleting once its member was empty")
	})

	// A member somebody else wrote is not deleted with the pool, so it cannot be a reason to
	// refuse - holding the pool open for it would be a hold nothing can release.
	It("is not held by a member it did not make", func() {
		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Instances.Replicas = ptrTo(int32(1))
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)

		handWritten := makeMember(namespace, "hand-written", poolName)
		Expect(k8sClient.Create(ctx, handWritten)).To(Succeed())
		awaitCached(handWritten)
		settle(pool)
		held := holding(handWritten, 5)
		DeferCleanup(func() { release(held) })

		Expect(k8sClient.Delete(ctx, refetch(pool))).To(Succeed())
		settle(pool)

		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx,
				client.ObjectKeyFromObject(pool), &pgelasticv1alpha1.PgElasticPool{}))
		}).Should(BeTrue(),
			"the pool is held open by an instance deleting it was never going to touch")
	})
})

var doomedNamespaces int
