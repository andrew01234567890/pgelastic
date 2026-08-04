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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/metering"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
)

// countingRecorder is the only way to prove no Event was emitted: an Event is written to a
// different object than the one under test, so watching resourceVersion cannot see it.
type countingRecorder struct{ count int }

func (r *countingRecorder) Eventf(
	_ runtime.Object, _ runtime.Object, _, _, _, _ string, _ ...any,
) {
	r.count++
}

var _ = Describe("ownership of the object graph", Ordered, func() {
	const (
		namespace    = "ownership-scoping"
		foreignClass = "ownership-foreign-class"
		foreignPool  = "ownership-foreign-pool"
		ownedClass   = "ownership-owned-class"
		ownedPool    = "ownership-owned-pool"
	)

	BeforeAll(func() {
		ensureNamespace(namespace)
		objects := []client.Object{
			makeElasticClass(foreignClass, otherControllerName),
			makeElasticClass(ownedClass, defaultControllerName),
			makePool(namespace, foreignPool, foreignClass, 100),
			makePool(namespace, ownedPool, ownedClass, 100),
		}
		for _, object := range objects {
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
		}
		DeferCleanup(func() { deleteAndAwait(objects...) })
		awaitCached(objects...)
	})

	// untouched is the assertion that actually proves the guard: a reconcile that writes
	// nothing leaves resourceVersion where it was. Asserting only that no error came back
	// would pass just as happily for a reconcile that rewrote the whole object.
	live := func(object client.Object) client.Object {
		GinkgoHelper()
		fetched, ok := object.DeepCopyObject().(client.Object)
		Expect(ok).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(object), fetched)).To(Succeed())
		return fetched
	}

	untouched := func(reconciler reconcile.Reconciler, object client.Object) {
		GinkgoHelper()
		before := live(object).GetResourceVersion()

		result, err := reconciler.Reconcile(ctx, requestFor(object))

		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
		after := live(object)
		Expect(after.GetResourceVersion()).To(Equal(before))
		Expect(after.GetFinalizers()).To(BeEmpty())
	}

	Context("PgElasticPool", func() {
		It("leaves a pool bound to another controller's class alone", func() {
			pool := &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: metav1.ObjectMeta{Name: foreignPool, Namespace: namespace},
			}
			recorder := &countingRecorder{}
			reconciler := &PgElasticPoolReconciler{
				Client: cachedClient, Scheme: cachedClient.Scheme(),
				Metering: metering.NewCollector(metering.Options{}, nil), Recorder: recorder,
			}

			untouched(reconciler, pool)

			Expect(recorder.count).To(BeZero())
		})

		It("still reconciles a pool bound to its own class", func() {
			pool := &pgelasticv1alpha1.PgElasticPool{
				ObjectMeta: metav1.ObjectMeta{Name: ownedPool, Namespace: namespace},
			}
			reconciler := &PgElasticPoolReconciler{
				Client: cachedClient, Scheme: cachedClient.Scheme(),
				Metering: metering.NewCollector(metering.Options{}, nil),
				Recorder: &countingRecorder{},
			}
			before := refetch(pool).ResourceVersion

			reconcileNow(reconciler, pool)

			Expect(refetch(pool).ResourceVersion).NotTo(Equal(before))
		})
	})

	Context("PgInstance", func() {
		It("leaves an instance under another controller's pool alone, and builds it nothing", func() {
			instance := makeInstance("ownership-instance")
			instance.Namespace = namespace
			instance.Spec.PoolRef = corev1.LocalObjectReference{Name: foreignPool}
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			DeferCleanup(func() { deleteAndAwait(instance) })
			awaitCached(instance)

			untouched(newInstanceReconciler(), instance)

			Expect(podsOf(instance)).To(BeEmpty())
			Expect(claimsOf(instance)).To(BeEmpty())
		})
	})

	Context("PgTenant", func() {
		It("leaves a tenant under another controller's pool alone", func() {
			tenant := makeTenant(namespace, "ownership-tenant", foreignPool, "ownership_tenant")
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
			DeferCleanup(func() { deleteAndAwait(tenant) })
			awaitCached(tenant)

			untouched(&PgTenantReconciler{Client: cachedClient, Scheme: cachedClient.Scheme()}, tenant)
		})
	})

	Context("PgTenantMigration", func() {
		It("leaves a migration of another controller's tenant alone", func() {
			tenant := makeTenant(namespace, "ownership-moving", foreignPool, "ownership_moving")
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
			object := &pgelasticv1alpha1.PgTenantMigration{
				ObjectMeta: metav1.ObjectMeta{Name: "ownership-move", Namespace: namespace},
				Spec: pgelasticv1alpha1.PgTenantMigrationSpec{
					TenantRef:         corev1.LocalObjectReference{Name: tenant.Name},
					TargetInstanceRef: corev1.LocalObjectReference{Name: "ownership-target"},
					Strategy:          pgelasticv1alpha1.TenantMigrationOnline,
				},
			}
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
			DeferCleanup(func() { deleteAndAwait(object, tenant) })
			awaitCached(tenant, object)

			untouched(&PgTenantMigrationReconciler{
				Client: cachedClient, Scheme: cachedClient.Scheme(),
				Router: migration.BindingRouter{Client: cachedClient},
			}, object)
		})
	})

	Context("a reference that does not resolve", func() {
		It("claims nothing and comes back for it, rather than defaulting to itself", func() {
			pool := makePool(namespace, "ownership-classless-pool", "ownership-class-that-is-gone", 100)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			DeferCleanup(func() { deleteAndAwait(pool) })
			awaitCached(pool)
			before := refetch(pool).ResourceVersion

			reconciler := &PgElasticPoolReconciler{
				Client: cachedClient, Scheme: cachedClient.Scheme(),
				Metering: metering.NewCollector(metering.Options{}, nil),
				Recorder: &countingRecorder{},
			}
			result, err := reconciler.Reconcile(ctx, requestFor(pool))

			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(ownership.RetryUnresolved))
			Expect(refetch(pool).ResourceVersion).To(Equal(before))
		})
	})

	Context("a reference that stops resolving while the object is being deleted", func() {
		It("still releases the finalizer, rather than stranding the object forever", func() {
			pool := makePool(namespace, "ownership-vanishing-pool", ownedClass, 100)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			tenant := makeTenant(namespace, "ownership-stranded",
				pool.Name, "ownership_stranded")
			tenant.Finalizers = []string{TenantDatabaseFinalizer}
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
			awaitCached(pool, tenant)

			// Deleting a namespace deletes the pool and the tenant at once, so this is the
			// ordering the API server produces on its own, not a contrived one.
			Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
			awaitCachedGone(pool)
			Expect(k8sClient.Delete(ctx, tenant)).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tenant),
				&pgelasticv1alpha1.PgTenant{})).To(Succeed())

			reconcileNow(&PgTenantReconciler{
				Client: cachedClient, Scheme: cachedClient.Scheme(),
			}, tenant)

			Expect(apierrors.IsNotFound(k8sClient.Get(ctx,
				client.ObjectKeyFromObject(tenant),
				&pgelasticv1alpha1.PgTenant{}))).To(BeTrue())
		})

		// reclaimPolicy Delete is the case worth pinning, because it is the one that acts on
		// the world. This tenant is bound and asks for its database to be dropped, and the
		// reconciler is given no PostgreSQL transport - so if the release went through the
		// normal finalize path it would fail with "no PostgreSQL transport is configured",
		// keep the finalizer, and strand the object exactly as before.
		It("releases without reclaiming, rather than dropping what it cannot prove is its own", func() {
			pool := makePool(namespace, "ownership-doomed-pool", ownedClass, 100)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			// A host that really exists: reclaim returns early on an instance that is already
			// gone, which would take the spec past the branch it is here to pin.
			host := makeInstance("ownership-host")
			host.Namespace = namespace
			host.Spec.PoolRef = corev1.LocalObjectReference{Name: pool.Name}
			Expect(k8sClient.Create(ctx, host)).To(Succeed())
			DeferCleanup(func() { deleteAndAwait(host) })

			tenant := makeTenant(namespace, "ownership-reclaimer", pool.Name, "ownership_reclaimer")
			tenant.Finalizers = []string{TenantDatabaseFinalizer}
			tenant.Spec.ReclaimPolicy = ptr.To(pgelasticv1alpha1.ReclaimDelete)
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
			tenant.Status.Binding = &pgelasticv1alpha1.PgTenantBinding{
				InstanceRef: &corev1.LocalObjectReference{Name: host.Name},
			}
			Expect(k8sClient.Status().Update(ctx, tenant)).To(Succeed())
			awaitCached(pool, host, tenant)

			Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
			awaitCachedGone(pool)
			Expect(k8sClient.Delete(ctx, tenant)).To(Succeed())

			reconcileNow(&PgTenantReconciler{
				Client: cachedClient, Scheme: cachedClient.Scheme(),
			}, tenant)

			Expect(apierrors.IsNotFound(k8sClient.Get(ctx,
				client.ObjectKeyFromObject(tenant),
				&pgelasticv1alpha1.PgTenant{}))).To(BeTrue())
		})
	})

	Context("an operator configured with a second identity", func() {
		It("swaps which pools it claims without any object changing", func() {
			mine := ownership.Resolver{Reader: cachedClient, ControllerName: otherControllerName}
			theirs := ownership.Resolver{Reader: cachedClient}
			foreign := &pgelasticv1alpha1.PgElasticPool{}
			Expect(cachedClient.Get(ctx,
				client.ObjectKey{Namespace: namespace, Name: foreignPool}, foreign)).To(Succeed())

			Expect(mine.Of(ctx, foreign)).To(Equal(ownership.Mine))
			Expect(theirs.Of(ctx, foreign)).To(Equal(ownership.Foreign))
		})
	})

	// A login resolves through the tenant it belongs to. Without a case of its own it fell to
	// the default arm, which answers Foreign *and* an error - so every reconcile of one would
	// have failed on its first line, before any of the rest of this kind could be reached.
	Context("a login belonging to a tenant of a claimed pool", func() {
		It("resolves through its tenant to the pool's class", func() {
			tenant := makeTenant(namespace, "ownership-login-host", ownedPool, "ownership_login")
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
			DeferCleanup(func() { deleteAndAwait(tenant) })
			awaitCached(tenant)

			resolver := ownership.Resolver{Reader: cachedClient}
			login := &pgelasticv1alpha1.PgTenantUser{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "own-login"},
				Spec: pgelasticv1alpha1.PgTenantUserSpec{
					TenantRef: corev1.LocalObjectReference{Name: tenant.Name},
					UserName:  "app",
				},
			}

			verdict, err := resolver.Of(ctx, login)

			Expect(err).NotTo(HaveOccurred())
			Expect(verdict).To(Equal(ownership.Mine))
		})

		// A tenant that has gone is a state of the cluster, not a fault. Answering Foreign
		// would let whichever operator asked next adopt a login it never provisioned.
		It("reports a login whose tenant has gone as unresolved rather than foreign", func() {
			resolver := ownership.Resolver{Reader: cachedClient}
			login := &pgelasticv1alpha1.PgTenantUser{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "orphan-login"},
				Spec: pgelasticv1alpha1.PgTenantUserSpec{
					TenantRef: corev1.LocalObjectReference{Name: "a-tenant-that-never-existed"},
					UserName:  "app",
				},
			}

			verdict, err := resolver.Of(ctx, login)

			Expect(err).NotTo(HaveOccurred())
			Expect(verdict).To(Equal(ownership.Unresolved))
		})
	})
})
