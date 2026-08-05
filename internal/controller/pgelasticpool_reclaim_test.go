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
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/autoscale"
)

// Scale-in cordoned its victim, asked it to drain, decremented the count and stopped. The
// machine stayed: running, holding its volumes, counted by the ledger it no longer serves,
// and reachable by nothing. An operator was left to notice and delete it by hand, and the
// pool's own count said it was already gone.
var _ = Describe("reclaiming the member scale-in emptied", func() {
	var (
		namespace string
		poolName  = "reclaim-pool"
		className = "reclaim-class"
	)

	BeforeEach(func() {
		reclaimNamespaces++
		namespace = fmt.Sprintf("reclaim-%d", reclaimNamespaces)
		ensureNamespace(namespace)
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx,
			makeElasticClass(className, defaultControllerName)))).To(Succeed())
	})

	// survives reconciles the pool while it watches, because a negative assertion made after a
	// fixed number of passes proves nothing: the state it is asserting about may not have
	// reached the controller's cache before the last of them.
	survives := func(pool *pgelasticv1alpha1.PgElasticPool, instance *pgelasticv1alpha1.PgInstance, why string) {
		GinkgoHelper()
		reconciler := newPoolReconciler()
		Consistently(func(g Gomega) {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pool),
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(instance),
				&pgelasticv1alpha1.PgInstance{})).To(Succeed())
		}, "5s", "100ms").Should(Succeed(), why)
	}

	// poolWithMembers declares `replicas` and provisions them, so the members carry the
	// pool's ownerReference exactly as a real one would.
	poolWithMembers := func(replicas int32) (*pgelasticv1alpha1.PgElasticPool, []pgelasticv1alpha1.PgInstance) {
		GinkgoHelper()
		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Instances.Replicas = ptrTo(replicas)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)

		reconciler := newPoolReconciler()
		Eventually(func(g Gomega) {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
			g.Expect(err).NotTo(HaveOccurred())
			list := &pgelasticv1alpha1.PgInstanceList{}
			g.Expect(k8sClient.List(ctx, list, client.InNamespace(namespace))).To(Succeed())
			g.Expect(list.Items).To(HaveLen(int(replicas)))
			// Longer than the suite's default: a pool makes one member per pass and will not
			// make its first until it has seen its own published status, so this waits on a
			// status round-trip through the informer cache per member.
		}, "20s", "50ms").Should(Succeed())

		list := &pgelasticv1alpha1.PgInstanceList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace(namespace))).To(Succeed())
		return refetch(pool), list.Items
	}

	// boundTenant creates a real PgTenant and binds it to a member, which is the only record
	// of where a tenant lives. PgInstanceStatus.Tenants looks like that record and is written
	// by nothing, so a fixture that stamps it proves a guard reading it works while the guard
	// is in fact always answering "empty".
	boundTenant := func(name, instance string) *pgelasticv1alpha1.PgTenant {
		GinkgoHelper()
		tenant := makeTenant(namespace, name, poolName, strings.ReplaceAll(name, "-", "_"))
		Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
		fresh := refetch(tenant)
		fresh.Status.Binding = &pgelasticv1alpha1.PgTenantBinding{
			InstanceRef: &corev1.LocalObjectReference{Name: instance},
			BoundAt:     &metav1.Time{Time: time.Now()},
		}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
		awaitCached(fresh)
		DeferCleanup(func() { deleteAndAwait(fresh) })
		return fresh
	}

	// emptied puts a member in the state scale-in leaves it in: marked for reclaim, cordoned,
	// drained, and holding nothing.
	emptied := func(instance *pgelasticv1alpha1.PgInstance) {
		GinkgoHelper()
		fresh := refetch(instance)
		patch := client.MergeFrom(fresh.DeepCopy())
		fresh.Spec.Admission = &pgelasticv1alpha1.InstanceAdmission{
			Cordoned:    ptrTo(true),
			Schedulable: ptrTo(false),
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[ReclaimWhenEmptyAnnotation] = ReclaimWhenEmpty
		Expect(k8sClient.Patch(ctx, fresh, patch)).To(Succeed())

		fresh = refetch(instance)
		fresh.Status.Phase = pgelasticv1alpha1.InstancePhaseReady
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
		awaitCached(fresh)
	}

	// The link between the two halves: the reclaim acts on a mark, and only scale-in writes
	// it. Without this, both halves can be right and the feature still never fire.
	It("marks the victim it cordons, so emptying it finishes the job", func() {
		pool, members := poolWithMembers(2)
		victim := members[1]

		reconciler := newPoolReconciler()
		view, err := reconciler.observe(ctx, refetch(pool))
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.scaleIn(ctx, refetch(pool), view, autoscale.Plan{
			ConsolidationTarget:  victim.Name,
			RecommendedInstances: 1,
		})
		Expect(err).NotTo(HaveOccurred())

		marked := refetch(&victim)
		Expect(marked.Annotations).To(HaveKeyWithValue(ReclaimWhenEmptyAnnotation, ReclaimWhenEmpty))
		Expect(cordoned(marked)).To(BeTrue())
	})

	It("deletes the member it emptied", func() {
		pool, members := poolWithMembers(2)
		victim := members[1]
		emptied(&victim)

		reconciler := newPoolReconciler()
		Eventually(func(g Gomega) {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(pool),
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: namespace, Name: victim.Name,
			}, &pgelasticv1alpha1.PgInstance{}))).To(BeTrue())
		}, "20s", "50ms").Should(Succeed(),
			"the emptied member is still there, and the pool's count says it is not")
	})

	// The victim is chosen by name and acted on many passes later, so by the time the delete
	// is issued the name can mean a different object - one somebody recreated by hand while
	// the drain was running, holding data nobody asked to lose.
	It("refuses to delete an object that is no longer the one it emptied", func() {
		pool, members := poolWithMembers(2)
		victim := members[1]
		emptied(&victim)

		By("replacing the victim with a different object under the same name")
		Expect(k8sClient.Delete(ctx, &victim)).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(&victim),
				&pgelasticv1alpha1.PgInstance{}))
		}).Should(BeTrue())
		impostor := makeMember(namespace, victim.Name, poolName)
		Expect(k8sClient.Create(ctx, impostor)).To(Succeed())
		awaitCached(impostor)

		survives(pool, impostor, "an object that merely inherited the victim's name was deleted")
	})

	// The pool owns what it made. An instance somebody else created is somebody else's to
	// delete, and it holds a primary's worth of tenant data - there is no replica of it.
	It("will not delete a member it did not make", func() {
		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Instances.Replicas = ptrTo(int32(2))
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)

		handWritten := makeMember(namespace, "hand-written", poolName)
		Expect(k8sClient.Create(ctx, handWritten)).To(Succeed())
		awaitCached(handWritten)
		emptied(handWritten)

		survives(pool, handWritten, "the pool deleted a machine it did not create")
	})

	// An operator retiring hardware sets exactly the fields scale-in sets - cordoned, drain
	// requested - and expects the machine to still be there when the tenants have gone.
	// Reclaiming on that state alone would delete a member nobody asked to lose.
	It("leaves a member the operator drained, rather than one scale-in chose", func() {
		pool, members := poolWithMembers(2)
		drained := members[1]

		fresh := refetch(&drained)
		patch := client.MergeFrom(fresh.DeepCopy())
		fresh.Spec.Admission = &pgelasticv1alpha1.InstanceAdmission{
			Cordoned:    ptrTo(true),
			Schedulable: ptrTo(false),
		}
		Expect(k8sClient.Patch(ctx, fresh, patch)).To(Succeed())
		fresh = refetch(&drained)
		fresh.Status.Phase = pgelasticv1alpha1.InstancePhaseReady
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
		awaitCached(fresh)

		survives(pool, &drained,
			"an instance the operator drained was deleted; only scale-in's own victim may be")
	})

	// Cutover rebinds the tenant and leaves the source database in place for the rollback
	// window, so "no tenant is bound here" is true of a member holding every retained source
	// database the drain just produced.
	It("waits for the rollback window before taking the source databases with it", func() {
		pool, members := poolWithMembers(2)
		victim := members[1]
		emptied(&victim)

		moved := makeTenant(namespace, "moved-on", poolName, "moved_on")
		Expect(k8sClient.Create(ctx, moved)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(moved) })

		migration := &pgelasticv1alpha1.PgTenantMigration{
			ObjectMeta: metav1.ObjectMeta{Name: "moved-on-migration", Namespace: namespace},
			Spec: pgelasticv1alpha1.PgTenantMigrationSpec{
				TenantRef:         corev1.LocalObjectReference{Name: moved.Name},
				TargetInstanceRef: corev1.LocalObjectReference{Name: members[0].Name},
			},
		}
		Expect(k8sClient.Create(ctx, migration)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(migration) })
		fresh := &pgelasticv1alpha1.PgTenantMigration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(migration), fresh)).To(Succeed())
		fresh.Status.SourceInstanceRef = &corev1.LocalObjectReference{Name: victim.Name}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
		awaitCached(fresh)

		survives(pool, &victim,
			"the member holding a just-migrated tenant's retained source database was deleted, "+
				"and the rollback window with it")
	})

	It("leaves a member that still holds tenants alone", func() {
		pool, members := poolWithMembers(2)
		victim := members[1]
		emptied(&victim)

		boundTenant("still-here", victim.Name)

		survives(pool, &victim,
			"a member still holding a tenant was deleted, and the tenant with it")
	})
})

var reclaimNamespaces int

// A migration's name is a function of the tenant and the target, and nothing deletes a
// settled one - no finalizer, no TTL, no owner reference. So the record of one move is the
// name the next identical move needs, and a tenant that goes A to B, back to A, and is asked
// for B again finds its own history in the way. The Create fails with AlreadyExists, which
// used to be swallowed whole: no event, no log, no retry, and a drain waiting on that move
// waited for ever with the instance cordoned and un-reclaimable.
var _ = Describe("emitting a move whose name a finished migration already holds", Ordered, func() {
	const namespace = "emit-again"

	var reconciler *PgElasticPoolReconciler
	var pool *pgelasticv1alpha1.PgElasticPool

	BeforeAll(func() {
		ensureNamespace(namespace)
		elasticClass := makeElasticClass("emit-again-class", defaultControllerName)
		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		pool = makePool(namespace, "emit-again-pool", elasticClass.Name, 900)
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		DeferCleanup(func() { deleteAndAwait(pool, elasticClass) })
		awaitCached(elasticClass, pool)
	})

	BeforeEach(func() {
		reconciler = &PgElasticPoolReconciler{
			Client: cachedClient, Scheme: cachedClient.Scheme(),
			Recorder: events.NewFakeRecorder(64),
		}
	})

	settledMigration := func(move autoscale.Move, phase pgelasticv1alpha1.TenantMigrationPhase) {
		GinkgoHelper()
		existing := &pgelasticv1alpha1.PgTenantMigration{
			ObjectMeta: metav1.ObjectMeta{Name: migrationNameFor(move), Namespace: namespace},
			Spec: pgelasticv1alpha1.PgTenantMigrationSpec{
				TenantRef:         corev1.LocalObjectReference{Name: move.Tenant},
				TargetInstanceRef: corev1.LocalObjectReference{Name: move.To},
			},
		}
		Expect(k8sClient.Create(ctx, existing)).To(Succeed())
		fetched := &pgelasticv1alpha1.PgTenantMigration{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(existing), fetched)).To(Succeed())
		fetched.Status.Phase = phase
		Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
		awaitCached(fetched)
	}

	It("clears the finished record so the move can be made again", func() {
		move := autoscale.Move{Tenant: "again-completed", From: "m-a", To: "m-b"}
		settledMigration(move, pgelasticv1alpha1.TenantMigrationPhaseCompleted)

		progressed, err := reconciler.emitMigrations(ctx, pool, []autoscale.Move{move}, nil)

		Expect(err).NotTo(HaveOccurred())
		Expect(progressed).To(BeTrue(),
			"the pass reported no progress, so the drain that asked for this move waits for ever")
		Eventually(func() bool {
			err := k8sClient.Get(ctx, client.ObjectKey{
				Namespace: namespace, Name: migrationNameFor(move),
			}, &pgelasticv1alpha1.PgTenantMigration{})
			return apierrors.IsNotFound(err)
		}).Should(BeTrue(), "the settled record still holds the name the new move needs")
	})

	// The ordinary idempotent case: one already in flight is the reason the Create is allowed
	// to fail at all, and deleting it would abandon a migration mid-flight.
	It("leaves a migration that is still running exactly where it is", func() {
		move := autoscale.Move{Tenant: "again-running", From: "m-a", To: "m-b"}
		settledMigration(move, pgelasticv1alpha1.TenantMigrationPhaseCatchup)

		progressed, err := reconciler.emitMigrations(ctx, pool, []autoscale.Move{move}, nil)

		Expect(err).NotTo(HaveOccurred())
		Expect(progressed).To(BeFalse())
		Expect(k8sClient.Get(ctx, client.ObjectKey{
			Namespace: namespace, Name: migrationNameFor(move),
		}, &pgelasticv1alpha1.PgTenantMigration{})).To(Succeed())
	})
})
