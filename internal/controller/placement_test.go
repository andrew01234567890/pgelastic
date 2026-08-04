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
	"math/rand/v2"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/autoscale"
	"github.com/andrew01234567890/pgelastic/internal/metering"
	"github.com/andrew01234567890/pgelastic/internal/tenantdb/tenantdbtest"
)

// reconcilePool drives two passes.
//
// The first pass measures and, by the stale-metric guardrail, executes nothing: the age of
// the readings is deliberately read before the pass folds its own observations in, so a
// controller that has just started has no evidence yet and is not allowed to act on it. The
// second pass is the first one entitled to do anything.
func reconcilePool(reconciler *PgElasticPoolReconciler, pool *pgelasticv1alpha1.PgElasticPool) {
	GinkgoHelper()
	reconcileNow(reconciler, refetch(pool))
	reconcileNow(reconciler, refetch(pool))
}

// placementClock is a fixed instant so that a dwell time or a stabilization window can be
// crossed by arithmetic rather than by waiting.
var placementClock = time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

// makeReadyInstance creates a PgInstance and publishes the capacity a placement decision
// reads. Placement never asks what an instance was configured with; it asks what the
// instance says it can hold.
func makeReadyInstance(namespace, name, pool string, allocatable, inUse int32) *pgelasticv1alpha1.PgInstance {
	GinkgoHelper()
	instance := &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef: corev1.LocalObjectReference{Name: pool},
			Class:   instanceClassName,
			Storage: pgelasticv1alpha1.InstanceStorage{
				Size:      resource.MustParse("100Gi"),
				WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("20Gi")},
			},
		},
	}
	Expect(k8sClient.Create(ctx, instance)).To(Succeed())

	instance.Status = pgelasticv1alpha1.PgInstanceStatus{
		Phase:          pgelasticv1alpha1.InstancePhaseReady,
		CurrentPrimary: name + "-1",
		Capacity: &pgelasticv1alpha1.InstanceCapacityStatus{
			MaxConnections: allocatable + 20,
			Allocatable:    allocatable,
			InUse:          inUse,
		},
		Storage: &pgelasticv1alpha1.InstanceStorageStatus{
			Allocated: ptr.To(resource.MustParse("100Gi")),
			Used:      ptr.To(resource.MustParse("10Gi")),
		},
	}
	Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
	return instance
}

var _ = Describe("tenant placement", Ordered, func() {
	const (
		namespace = "pgt-placement"
		poolName  = "placement-pool"
		className = "placement-class"
		workload  = "placement-standard"
	)

	var (
		reconciler *PgTenantReconciler
		collector  *metering.Collector
		instances  []*pgelasticv1alpha1.PgInstance
	)

	BeforeAll(func() {
		ensureNamespace(namespace)
		elasticClass := makeElasticClass(className, defaultControllerName)
		pool := makePool(namespace, poolName, className, 900)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workload}
		class := makeWorkloadClass(workload, 2, 40)

		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Create(ctx, class)).To(Succeed())

		instances = []*pgelasticv1alpha1.PgInstance{
			makeReadyInstance(namespace, "place-a", poolName, 225, 40),
			makeReadyInstance(namespace, "place-b", poolName, 225, 40),
			makeReadyInstance(namespace, "place-c", poolName, 225, 40),
		}

		DeferCleanup(func() {
			for _, instance := range instances {
				deleteAndAwait(instance)
			}
			deleteAndAwait(pool, elasticClass, class)
		})
		awaitCached(elasticClass, pool, class)
		for _, instance := range instances {
			awaitCached(instance)
		}
	})

	BeforeEach(func() {
		collector = metering.NewCollector(metering.Options{}, nil)
		reconciler = &PgTenantReconciler{
			Client:   cachedClient,
			Scheme:   cachedClient.Scheme(),
			Metering: collector,
			Rand:     rand.New(rand.NewPCG(11, 13)),
			Now:      func() time.Time { return placementClock },
			// Placement decides where a tenant goes; Ready decides whether its database is
			// there. These specs are about the first, and they get a PostgreSQL that answers
			// so that the second does not silently become the thing being asserted.
			SQL: tenantdbtest.NewCluster(),
		}
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

	It("binds the tenant to a real instance and says which one", func() {
		tenant := createTenant("bind-me", "bind_me", nil)

		reconcileNow(reconciler, tenant)

		fetched := refetch(tenant)
		Expect(fetched.Status.Binding).NotTo(BeNil())
		Expect(fetched.Status.Binding.InstanceRef).NotTo(BeNil())
		Expect(fetched.Status.Binding.InstanceRef.Name).To(BeElementOf("place-a", "place-b", "place-c"))
		Expect(fetched.Status.Binding.BoundAt).NotTo(BeNil())

		bound := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionBound)
		Expect(bound.Status).To(Equal(metav1.ConditionTrue))
		Expect(bound.Reason).To(Equal(pgelasticv1alpha1.ReasonPlaced))
		Expect(bound.Message).To(ContainSubstring(fetched.Status.Binding.InstanceRef.Name))
		Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.PgTenantPhaseReady))
	})

	It("leaves an already-bound tenant where it is", func() {
		tenant := createTenant("stay-put", "stay_put", nil)

		reconcileNow(reconciler, tenant)
		first := refetch(tenant).Status.Binding.InstanceRef.Name
		boundAt := refetch(tenant).Status.Binding.BoundAt

		for range 3 {
			reconcileNow(reconciler, refetch(tenant))
		}

		fetched := refetch(tenant)
		Expect(fetched.Status.Binding.InstanceRef.Name).To(Equal(first),
			"the tenant was rebound, and every rebinding costs a live migration")
		Expect(fetched.Status.Binding.BoundAt).To(Equal(boundAt))
	})

	It("honours an instance pin and refuses one it cannot honour", func() {
		pinned := createTenant("pinned", "pinned_db", func(tenant *pgelasticv1alpha1.PgTenant) {
			tenant.Spec.Placement = &pgelasticv1alpha1.PgTenantPlacement{
				InstanceRef: &corev1.LocalObjectReference{Name: "place-c"},
			}
		})
		reconcileNow(reconciler, pinned)
		Expect(refetch(pinned).Status.Binding.InstanceRef.Name).To(Equal("place-c"))

		elsewhere := createTenant("pinned-nowhere", "pinned_nowhere", func(tenant *pgelasticv1alpha1.PgTenant) {
			tenant.Spec.Placement = &pgelasticv1alpha1.PgTenantPlacement{
				InstanceRef: &corev1.LocalObjectReference{Name: "place-z"},
			}
		})
		reconcileNow(reconciler, elsewhere)

		fetched := refetch(elsewhere)
		Expect(fetched.Status.Binding).To(BeNil())
		bound := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionBound)
		Expect(bound.Status).To(Equal(metav1.ConditionFalse))
		Expect(bound.Message).To(ContainSubstring("place-z"))
	})

	It("keeps correlated tenants off the same instance", func() {
		const key = "saas.example.com/customer-shard"
		shard := func(name, database string) *pgelasticv1alpha1.PgTenant {
			return createTenant(name, database, func(tenant *pgelasticv1alpha1.PgTenant) {
				tenant.Labels = map[string]string{key: "acme"}
				tenant.Spec.Placement = &pgelasticv1alpha1.PgTenantPlacement{
					AntiAffinityLabelKeys: []string{key},
				}
			})
		}
		first, second := shard("shard-one", "shard_one"), shard("shard-two", "shard_two")

		reconcileNow(reconciler, first)
		// Anti-affinity is a fact about where the *sibling* already is, and the placer reads
		// its siblings through the informer cache. Reconciling the second tenant before that
		// cache has the first one's binding asks the placer to avoid a tenant it cannot see,
		// and it puts them together perfectly correctly - so the assertion below has to wait
		// for the precondition rather than assume the write is instantly readable.
		awaitCached(refetch(first))

		reconcileNow(reconciler, refetch(second))

		firstInstance := refetch(first).Status.Binding.InstanceRef.Name
		secondBinding := refetch(second).Status.Binding
		Expect(secondBinding).NotTo(BeNil())
		Expect(secondBinding.InstanceRef.Name).NotTo(Equal(firstInstance),
			"two tenants of the same customer shard landed together, which is the correlation the "+
				"oversubscription bet cannot survive")
	})

	It("refuses to rebind a tenant whose bound instance has gone", func() {
		tenant := createTenant("stranded", "stranded_db", nil)
		reconcileNow(reconciler, tenant)

		bound := refetch(tenant).Status.Binding
		Expect(bound).NotTo(BeNil())
		original := bound.InstanceRef.Name

		// The tenant's database lives on this instance and nowhere else, so a binding that
		// moved without a migration would point at an empty database.
		vanished := &pgelasticv1alpha1.PgInstance{ObjectMeta: metav1.ObjectMeta{
			Name: original, Namespace: namespace}}
		deleteAndAwait(vanished)
		DeferCleanup(func() {
			restored := makeReadyInstance(namespace, original, poolName, 225, 40)
			awaitCached(restored)
			for i, instance := range instances {
				if instance.Name == original {
					instances[i] = restored
				}
			}
		})

		reconcileNow(reconciler, refetch(tenant))

		fetched := refetch(tenant)
		Expect(fetched.Status.Binding).NotTo(BeNil())
		Expect(fetched.Status.Binding.InstanceRef.Name).To(Equal(original),
			"the binding was repointed at an instance that has never held this tenant's data")
		condition := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionBound)
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal(pgelasticv1alpha1.ReasonInstanceMissing))
		Expect(condition.Message).To(ContainSubstring(original))
		Expect(fetched.Status.Phase).NotTo(Equal(pgelasticv1alpha1.PgTenantPhaseReady))
	})

	It("publishes the trailing-window numbers on the tenant rather than as metric labels", func() {
		tenant := createTenant("metered", "metered_db", nil)

		key := metering.Key{Namespace: namespace, Pool: poolName, Tenant: tenant.Name}
		for i := range 100 {
			connections := float64(1)
			if i%10 == 0 {
				connections = 30
			}
			collector.Store.Observe(key, metering.Sample{
				BackendConnections: connections,
				StorageBytes:       12 << 30,
			}, placementClock.Add(-time.Duration(100-i)*time.Minute))
		}

		reconcileNow(reconciler, tenant)

		utilization := refetch(tenant).Status.Utilization
		Expect(utilization).NotTo(BeNil())
		Expect(utilization.BackendConnections).NotTo(BeNil())
		Expect(*utilization.BackendConnections.P95_7d).To(BeNumerically(">=", 30))
		Expect(*utilization.BackendConnections.Peak_7d).To(BeNumerically(">=", 30))
		Expect(*utilization.StorageBytes).To(Equal(int64(12 << 30)))
	})
})

var _ = Describe("pool capacity planning", Ordered, func() {
	const (
		namespace = "pool-planning"
		poolName  = "planning-pool"
		className = "planning-class"
		workload  = "planning-standard"
	)

	var (
		reconciler *PgElasticPoolReconciler
		recorder   *events.FakeRecorder
		collector  *metering.Collector
		pool       *pgelasticv1alpha1.PgElasticPool
		instances  []*pgelasticv1alpha1.PgInstance
	)

	BeforeAll(func() {
		ensureNamespace(namespace)
		elasticClass := makeElasticClass(className, defaultControllerName)
		pool = makePool(namespace, poolName, className, 900)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workload}
		class := makeWorkloadClass(workload, 2, 40)

		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Create(ctx, class)).To(Succeed())

		instances = []*pgelasticv1alpha1.PgInstance{
			makeReadyInstance(namespace, "plan-a", poolName, 225, 200),
			makeReadyInstance(namespace, "plan-b", poolName, 225, 200),
			makeReadyInstance(namespace, "plan-c", poolName, 225, 200),
		}

		DeferCleanup(func() {
			for _, instance := range instances {
				deleteAndAwait(instance)
			}
			deleteAndAwait(pool, elasticClass, class)
		})
		awaitCached(elasticClass, pool, class)
		for _, instance := range instances {
			awaitCached(instance)
		}
	})

	BeforeEach(func() {
		collector = metering.NewCollector(metering.Options{}, nil)
		recorder = events.NewFakeRecorder(64)
		reconciler = &PgElasticPoolReconciler{
			Client:   cachedClient,
			Scheme:   cachedClient.Scheme(),
			Recorder: recorder,
			Metering: collector,
			Now:      func() time.Time { return placementClock },
		}
	})

	It("publishes the ledger and a plan in Recommend mode", func() {
		reconcileNow(reconciler, refetch(pool))

		fetched := refetch(pool)
		Expect(fetched.Status.Capacity).NotTo(BeNil())
		Expect(fetched.Status.Capacity.Allocatable).To(Equal(int32(675)))
		Expect(fetched.Status.Capacity.DerivedFrom).To(ContainSubstring("3 instances ready"))
		Expect(fetched.Status.PerInstance).To(HaveLen(3))

		plan := fetched.Status.Autoscaling
		Expect(plan).NotTo(BeNil())
		Expect(plan.Mode).To(Equal(pgelasticv1alpha1.AutoscalingRecommend))
		Expect(plan.ObservedInstances).To(Equal(int32(3)))
		Expect(plan.ObservedUtilizationPercent).To(Equal(int32(88)))
		Expect(plan.RecommendedInstances).To(BeNumerically(">", 3),
			"the pool is at 88%% of a 70%% target and the plan does not say to grow")
		Expect(plan.Summary).NotTo(BeEmpty())
	})

	It("refuses to execute a scale-out in Recommend mode and names the reason", func() {
		reconcilePool(reconciler, pool)

		fetched := refetch(pool)
		scaleOut := plannedAction(fetched, pgelasticv1alpha1.AutoActionScaleOut)
		Expect(scaleOut).NotTo(BeNil())
		Expect(scaleOut.Permitted).To(BeFalse())
		Expect(scaleOut.Reason).To(Equal(autoscale.ReasonRecommendMode))
		Expect(scaleOut.ExecutedAt).To(BeNil())

		replicas := int32(3)
		if fetched.Spec.Instances.Replicas != nil {
			replicas = *fetched.Spec.Instances.Replicas
		}
		Expect(replicas).To(Equal(int32(3)), "Recommend mode changed the pool's member count")
	})

	It("emits an Event for the plan and for each refused action", func() {
		reconcileNow(reconciler, refetch(pool))

		var events []string
		for len(recorder.Events) > 0 {
			events = append(events, <-recorder.Events)
		}
		Expect(events).NotTo(BeEmpty())
		Expect(events).To(ContainElement(ContainSubstring(eventAutoscalePlan)))
		Expect(events).To(ContainElement(ContainSubstring(eventActionRefused)))
	})
})

func plannedAction(
	pool *pgelasticv1alpha1.PgElasticPool,
	class pgelasticv1alpha1.AutoAction,
) *pgelasticv1alpha1.PlannedAction {
	if pool.Status.Autoscaling == nil {
		return nil
	}
	for i := range pool.Status.Autoscaling.Actions {
		if pool.Status.Autoscaling.Actions[i].Name == class {
			return &pool.Status.Autoscaling.Actions[i]
		}
	}
	return nil
}

var _ = Describe("storage expansion", Ordered, func() {
	const (
		namespace = "pool-storage"
		poolName  = "storage-pool"
		className = "storage-class-policy"
		workload  = "storage-standard"
	)

	var (
		reconciler *PgElasticPoolReconciler
		pool       *pgelasticv1alpha1.PgElasticPool
		instance   *pgelasticv1alpha1.PgInstance
	)

	BeforeAll(func() {
		ensureNamespace(namespace)
		elasticClass := makeElasticClass(className, defaultControllerName)
		pool = makePool(namespace, poolName, className, 300)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workload}
		class := makeWorkloadClass(workload, 2, 40)

		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Create(ctx, class)).To(Succeed())
		instance = makeReadyInstance(namespace, "storage-a", poolName, 225, 10)

		fresh := refetch(instance)
		fresh.Status.Storage = &pgelasticv1alpha1.InstanceStorageStatus{
			Allocated: ptr.To(resource.MustParse("100Gi")),
			Used:      ptr.To(resource.MustParse("92Gi")),
		}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())

		DeferCleanup(func() { deleteAndAwait(instance, pool, elasticClass, class) })
		awaitCached(elasticClass, pool, class, instance)
	})

	BeforeEach(func() {
		reconciler = &PgElasticPoolReconciler{
			Client:   cachedClient,
			Scheme:   cachedClient.Scheme(),
			Recorder: events.NewFakeRecorder(64),
			Metering: metering.NewCollector(metering.Options{}, nil),
			Now:      func() time.Time { return placementClock },
		}
	})

	// Recommend mode executes storage expansion and nothing else. A full volume is an outage
	// that no amount of human gating prevents, and growing one is online.
	It("grows a full volume even in Recommend mode", func() {
		reconcilePool(reconciler, pool)

		grown := refetch(instance)
		Expect(grown.Spec.Storage.Size.Value()).To(BeNumerically(">", int64(100)<<30),
			"a volume at 92%% was left at its original size")

		action := plannedAction(refetch(pool), pgelasticv1alpha1.AutoActionStorageExpand)
		Expect(action).NotTo(BeNil())
		Expect(action.Permitted).To(BeTrue())
		Expect(action.ExecutedAt).NotTo(BeNil())
		Expect(action.Target).To(Equal("storage-a"))
	})

	It("does not grow a volume twice", func() {
		reconcilePool(reconciler, pool)
		first := refetch(instance).Spec.Storage.Size

		reconcilePool(reconciler, pool)
		Expect(refetch(instance).Spec.Storage.Size).To(Equal(first))
	})
})

var _ = Describe("stale metrics", Ordered, func() {
	const (
		namespace = "pool-stale"
		poolName  = "stale-pool"
		className = "stale-class"
		workload  = "stale-standard"
	)

	var (
		pool     *pgelasticv1alpha1.PgElasticPool
		instance *pgelasticv1alpha1.PgInstance
	)

	BeforeAll(func() {
		ensureNamespace(namespace)
		elasticClass := makeElasticClass(className, defaultControllerName)
		pool = makePool(namespace, poolName, className, 300)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workload}
		class := makeWorkloadClass(workload, 2, 40)

		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Create(ctx, class)).To(Succeed())
		instance = makeReadyInstance(namespace, "stale-a", poolName, 225, 10)

		fresh := refetch(instance)
		fresh.Status.Storage = &pgelasticv1alpha1.InstanceStorageStatus{
			Allocated: ptr.To(resource.MustParse("100Gi")),
			Used:      ptr.To(resource.MustParse("95Gi")),
		}
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())

		DeferCleanup(func() { deleteAndAwait(instance, pool, elasticClass, class) })
		awaitCached(elasticClass, pool, class, instance)
	})

	// The pool is metered once and then the clock is moved an hour forward. Nothing may be
	// executed on a reading that old, including the volume expansion that would otherwise be
	// the one thing Recommend mode does.
	It("executes nothing once the newest sample is older than the threshold", func() {
		collector := metering.NewCollector(metering.Options{}, nil)
		collector.Store.Observe(
			metering.Key{Namespace: namespace, Pool: poolName, Tenant: "ghost"},
			metering.Sample{BackendConnections: 1}, placementClock.Add(-time.Hour))

		reconciler := &PgElasticPoolReconciler{
			Client:   cachedClient,
			Scheme:   cachedClient.Scheme(),
			Recorder: events.NewFakeRecorder(64),
			Metering: collector,
			Now:      func() time.Time { return placementClock },
		}

		before := refetch(instance).Spec.Storage.Size
		reconcileNow(reconciler, refetch(pool))

		fetched := refetch(pool)
		Expect(fetched.Status.Autoscaling).NotTo(BeNil())
		Expect(fetched.Status.Autoscaling.MetricsStale).To(BeTrue())
		for _, action := range fetched.Status.Autoscaling.Actions {
			Expect(action.Permitted).To(BeFalse(),
				"%s executed against a one-hour-old reading", action.Name)
			Expect(action.Reason).To(Equal(autoscale.ReasonStaleMetrics))
		}
		Expect(refetch(instance).Spec.Storage.Size).To(Equal(before))
	})
})

// bindTenant records a placement without running the placer, for the specs that are about
// what happens to tenants that are already placed.
func bindTenant(tenant *pgelasticv1alpha1.PgTenant, instance string) {
	GinkgoHelper()
	fresh := refetch(tenant)
	fresh.Status.Binding = &pgelasticv1alpha1.PgTenantBinding{
		InstanceRef: &corev1.LocalObjectReference{Name: instance},
		BoundAt:     &metav1.Time{Time: placementClock.Add(-time.Hour)},
	}
	Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
}

var _ = Describe("auto mode", Ordered, func() {
	const (
		namespace = "pool-auto"
		poolName  = "auto-pool"
		className = "auto-class"
		workload  = "auto-standard"
	)

	var (
		reconciler *PgElasticPoolReconciler
		collector  *metering.Collector
		pool       *pgelasticv1alpha1.PgElasticPool
		tenants    []*pgelasticv1alpha1.PgTenant
	)

	BeforeAll(func() {
		ensureNamespace(namespace)
		elasticClass := makeElasticClass(className, defaultControllerName)
		pool = makePool(namespace, poolName, className, 400)
		pool.Spec.Instances.Replicas = ptr.To(int32(2))
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workload}
		pool.Spec.Rebalancing = &pgelasticv1alpha1.PoolRebalancing{Enabled: ptr.To(true)}
		pool.Spec.Autoscaling = &pgelasticv1alpha1.PoolAutoscaling{
			Mode:         pgelasticv1alpha1.AutoscalingAuto,
			MinInstances: ptr.To(int32(1)),
			MaxInstances: ptr.To(int32(8)),
			AutoActions:  []pgelasticv1alpha1.AutoAction{pgelasticv1alpha1.AutoActionRebalance},
		}
		class := makeWorkloadClass(workload, 2, 40)

		Expect(k8sClient.Create(ctx, elasticClass)).To(Succeed())
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Expect(k8sClient.Create(ctx, class)).To(Succeed())

		// A small instance next to a large, nearly empty one: best-fit packs the tenants onto
		// the small one and leaves the large one to be reclaimed.
		small := makeReadyInstance(namespace, "rb-a", poolName, 30, 0)
		large := makeReadyInstance(namespace, "rb-b", poolName, 225, 10)

		for i := range 4 {
			tenant := makeTenant(namespace, fmt.Sprintf("rb-t%d", i), poolName, fmt.Sprintf("rb_t%d", i))
			Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
			bindTenant(tenant, "rb-b")
			tenants = append(tenants, tenant)
		}

		DeferCleanup(func() {
			for _, tenant := range tenants {
				deleteAndAwait(tenant)
			}
			deleteAndAwait(small, large, pool, elasticClass, class)
		})
		awaitCached(elasticClass, pool, class, small, large)
		for _, tenant := range tenants {
			awaitCached(tenant)
		}
	})

	BeforeEach(func() {
		collector = metering.NewCollector(metering.Options{}, nil)
		for _, tenant := range tenants {
			key := metering.Key{Namespace: namespace, Pool: poolName, Tenant: tenant.Name}
			for i := range 60 {
				collector.Store.Observe(key, metering.Sample{BackendConnections: 5},
					placementClock.Add(-time.Duration(60-i)*time.Minute))
			}
		}
		reconciler = &PgElasticPoolReconciler{
			Client:   cachedClient,
			Scheme:   cachedClient.Scheme(),
			Recorder: events.NewFakeRecorder(64),
			Metering: collector,
			Now:      func() time.Time { return placementClock },
		}
	})

	AfterEach(func() {
		migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
		Expect(k8sClient.List(ctx, migrations, client.InNamespace(namespace))).To(Succeed())
		for i := range migrations.Items {
			deleteAndAwait(&migrations.Items[i])
		}
	})

	// Rebalancing consumes migration as an API. The planner states the intent and the
	// migration controller owns every phase of carrying it out, including refusing it.
	It("emits a PgTenantMigration rather than moving anything itself", func() {
		reconcilePool(reconciler, pool)

		migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
		Expect(k8sClient.List(ctx, migrations, client.InNamespace(namespace))).To(Succeed())
		Expect(migrations.Items).To(HaveLen(1),
			"the migration budget caps concurrent moves at one, and the plan spent exactly that")

		emitted := migrations.Items[0]
		Expect(emitted.Spec.TargetInstanceRef.Name).To(Equal("rb-a"))
		Expect(emitted.Spec.TenantRef.Name).To(HavePrefix("rb-t"))

		plan := refetch(pool).Status.Autoscaling
		Expect(plan.Moves).NotTo(BeEmpty())
		for _, move := range plan.Moves {
			Expect(move.From).To(Equal("rb-b"))
			Expect(move.To).To(Equal("rb-a"))
			Expect(move.Eligible).To(BeTrue(), "blocked by %s", move.BlockedBy)
		}
	})

	It("does not start a second move while the first is in flight", func() {
		reconcilePool(reconciler, pool)
		reconcilePool(reconciler, pool)

		migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
		Expect(k8sClient.List(ctx, migrations, client.InNamespace(namespace))).To(Succeed())
		Expect(migrations.Items).To(HaveLen(1),
			"a second move started while the first was still decoding off the same source")

		rebalance := plannedAction(refetch(pool), pgelasticv1alpha1.AutoActionRebalance)
		Expect(rebalance).NotTo(BeNil())
		Expect(rebalance.Permitted).To(BeFalse())
		Expect(rebalance.Reason).To(Equal(autoscale.ReasonMigrationBudget))
	})

	It("takes no action at all while an instance is rolling out", func() {
		instance := &pgelasticv1alpha1.PgInstance{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "rb-b"}, instance)).To(Succeed())
		instance.Status.Conditions = []metav1.Condition{{
			Type:               pgelasticv1alpha1.ConditionProgressing,
			Status:             metav1.ConditionTrue,
			Reason:             pgelasticv1alpha1.ReasonRecloning,
			Message:            "re-cloning a replica",
			LastTransitionTime: metav1.Time{Time: placementClock},
		}}
		Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
		DeferCleanup(func() {
			fresh := refetch(instance)
			fresh.Status.Conditions = nil
			Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
			awaitCached(fresh)
		})
		awaitCached(instance)

		reconcilePool(reconciler, pool)

		migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
		Expect(k8sClient.List(ctx, migrations, client.InNamespace(namespace))).To(Succeed())
		Expect(migrations.Items).To(BeEmpty(), "a tenant was moved during a rollout")

		for _, action := range refetch(pool).Status.Autoscaling.Actions {
			Expect(action.Permitted).To(BeFalse())
			Expect(action.Reason).To(Equal(autoscale.ReasonRolloutInProgress))
		}
	})
})
