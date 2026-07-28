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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/autoscale"
	"github.com/andrew01234567890/pgelastic/internal/metering"
)

const (
	// tenantCount is the population the design point calls for at pool scale, scaled down to
	// what a single API server should place in seconds rather than minutes.
	tenantCount = 20
	// instanceCount is the pool's member count.
	instanceCount = 3
	// allocatablePerInstance is what each member publishes as its usable connection budget.
	allocatablePerInstance = 60
	// guaranteedPerTenant is each tenant's floor. Twenty of them is 40 connections, which
	// fits inside one instance's budget and so cannot mask a packing failure as a capacity
	// failure.
	guaranteedPerTenant = 2
	// maxSkewTenants is deliberately tight, so a distribution assertion means something.
	maxSkewTenants = 3
	// shardLabelKey is the anti-affinity key the correlated tenants declare.
	shardLabelKey = "saas.example.com/customer-shard"
)

var _ = Describe("placing a tenant population across a pool", Ordered, func() {
	var (
		namespace    string
		poolName     = "e2e-pool"
		className    = "e2e-placement-class"
		workloadName = "e2e-placement-standard"

		instances []string
		tenants   []*pgelasticv1alpha1.PgTenant
	)

	BeforeAll(func() {
		namespace = uniqueNamespace("pgelastic-placement")
		Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
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
					Guaranteed: ptr.To(int32(guaranteedPerTenant)),
					Burstable:  40,
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, workloadClass)).To(Succeed())

		pool := &pgelasticv1alpha1.PgElasticPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: namespace},
			Spec: pgelasticv1alpha1.PgElasticPoolSpec{
				ClassRef: pgelasticv1alpha1.ClassReference{
					APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
					Kind:     "PgElasticClass",
					Name:     className,
				},
				Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 240},
				Instances: pgelasticv1alpha1.PoolInstances{
					Replicas: ptr.To(int32(instanceCount)),
					Template: pgelasticv1alpha1.PgInstanceTemplate{
						Class: "gp-8",
						Storage: pgelasticv1alpha1.InstanceStorage{
							Size:      resource.MustParse("100Gi"),
							WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("20Gi")},
						},
					},
				},
				Admission: &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workloadName},
				Placement: &pgelasticv1alpha1.PoolPlacement{
					PackOnPercentile: pgelasticv1alpha1.PercentileP95,
					MaxSkewTenants:   ptr.To(int32(maxSkewTenants)),
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, pool)).To(Succeed())

		for i := range instanceCount {
			name := fmt.Sprintf("e2e-pg-%c", 'a'+i)
			instances = append(instances, name)
			createReadyInstance(namespace, name, poolName, allocatablePerInstance)
		}

		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespace},
			}))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, elasticClass))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, workloadClass))).To(Succeed())
		})
	})

	It("binds every tenant to a real instance", func() {
		By("seeding a trailing window so placement packs on observed demand, not on declarations")
		for i := range tenantCount {
			name := fmt.Sprintf("e2e-tenant-%02d", i)
			tenant := &pgelasticv1alpha1.PgTenant{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: pgelasticv1alpha1.PgTenantSpec{
					PoolRef:      corev1.LocalObjectReference{Name: poolName},
					DatabaseName: fmt.Sprintf("e2e_tenant_%02d", i),
				},
			}
			// Three tenants belong to the same customer shard and declare an anti-affinity
			// key on it. Three is the most a three-instance pool can satisfy, and asking for
			// a fourth would be testing that the pool refuses the impossible rather than that
			// it separates the possible.
			if i%8 == 0 {
				tenant.Labels = map[string]string{shardLabelKey: "acme"}
				tenant.Spec.Placement = &pgelasticv1alpha1.PgTenantPlacement{
					AntiAffinityLabelKeys: []string{shardLabelKey},
				}
			}
			Expect(k8sClient.Create(suiteCtx, tenant)).To(Succeed())
			tenants = append(tenants, tenant)

			seedWindow(namespace, poolName, name, float64(3+i%4))
		}

		By("waiting for every tenant to report a binding")
		Eventually(func(g Gomega) {
			for _, tenant := range tenants {
				fetched := &pgelasticv1alpha1.PgTenant{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(tenant), fetched)).To(Succeed())
				g.Expect(fetched.Status.Binding).NotTo(BeNil(), "%s has no binding: %s",
					tenant.Name, boundMessage(fetched))
				g.Expect(fetched.Status.Binding.InstanceRef).NotTo(BeNil())
				g.Expect(fetched.Status.Binding.InstanceRef.Name).To(BeElementOf(instances))
				g.Expect(fetched.Status.Binding.BoundAt).NotTo(BeNil())
			}
		}).Should(Succeed())
	})

	It("reports Bound with a reason instead of the Pending it used to", func() {
		fetched := &pgelasticv1alpha1.PgTenant{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: namespace, Name: "e2e-tenant-00",
		}, fetched)).To(Succeed())

		bound := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionBound)
		Expect(bound.Status).To(Equal(metav1.ConditionTrue))
		Expect(bound.Reason).To(Equal(pgelasticv1alpha1.ReasonPlaced))
		Expect(bound.Message).To(ContainSubstring(fetched.Status.Binding.InstanceRef.Name))
		Expect(bound.ObservedGeneration).To(Equal(fetched.Generation))
		Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.PgTenantPhaseReady))
	})

	It("respects every guarantee and the tenant-count skew bound", func() {
		byInstance := bindings(namespace, tenants)

		reserved := map[string]int32{}
		counts := map[string]int32{}
		for name, hosted := range byInstance {
			for range hosted {
				reserved[name] += guaranteedPerTenant
				counts[name]++
			}
		}

		total := int32(0)
		for _, name := range instances {
			total += counts[name]
			Expect(reserved[name]).To(BeNumerically("<=", int32(allocatablePerInstance)),
				"instance %s was sold %d guaranteed connections against %d allocatable",
				name, reserved[name], allocatablePerInstance)
		}
		Expect(total).To(Equal(int32(tenantCount)))

		lowest, highest := int32(1<<30), int32(0)
		for _, name := range instances {
			lowest = min(lowest, counts[name])
			highest = max(highest, counts[name])
		}
		By(fmt.Sprintf("distribution across the pool: %v, reserved connections: %v", counts, reserved))
		Expect(highest-lowest).To(BeNumerically("<=", int32(maxSkewTenants)),
			"tenant counts %v skew by %d against a bound of %d", counts, highest-lowest, maxSkewTenants)
	})

	It("keeps correlated tenants apart", func() {
		byInstance := bindings(namespace, tenants)

		shardTenants := map[string]int{}
		for name, hosted := range byInstance {
			for _, tenant := range hosted {
				if tenant.Labels[shardLabelKey] == "acme" {
					shardTenants[name]++
				}
			}
		}
		for name, count := range shardTenants {
			Expect(count).To(Equal(1),
				"instance %s hosts %d tenants of the acme shard; correlated workloads defeat the "+
					"oversubscription bet the capacity model rests on", name, count)
		}
	})

	It("publishes the pool's ledger and per-instance breakdown", func() {
		Eventually(func(g Gomega) {
			pool := fetchPool(namespace, poolName)
			g.Expect(pool.Status.Capacity).NotTo(BeNil())
			g.Expect(pool.Status.Capacity.Reserved).To(Equal(int32(tenantCount * guaranteedPerTenant)))
			g.Expect(pool.Status.Capacity.DerivedFrom).NotTo(BeEmpty())
			g.Expect(pool.Status.PerInstance).To(HaveLen(instanceCount))
			g.Expect(pool.Status.Tenants).NotTo(BeNil())
			g.Expect(pool.Status.Tenants.Bound).To(Equal(int32(tenantCount)))

			placed := int32(0)
			for _, row := range pool.Status.PerInstance {
				placed += row.Tenants
			}
			g.Expect(placed).To(Equal(int32(tenantCount)))
		}).Should(Succeed())
	})

	// Recommend is the default, and the whole point of it is that the plan is written down in
	// full while nothing but a volume expansion is applied.
	It("persists a whole plan in Recommend mode and executes none of it", func() {
		Eventually(func(g Gomega) {
			plan := fetchPool(namespace, poolName).Status.Autoscaling
			g.Expect(plan).NotTo(BeNil())
			g.Expect(plan.Mode).To(Equal(pgelasticv1alpha1.AutoscalingRecommend))
			g.Expect(plan.ComputedAt).NotTo(BeNil())
			g.Expect(plan.Summary).NotTo(BeEmpty())
			g.Expect(plan.PerInstance).To(HaveLen(instanceCount))
			g.Expect(plan.MetricsStale).To(BeFalse(), "the pool has been metered every reconcile")
		}).Should(Succeed())

		By("checking that no member's declared size or member count moved")
		pool := fetchPool(namespace, poolName)
		Expect(*pool.Spec.Instances.Replicas).To(Equal(int32(instanceCount)))
		for _, name := range instances {
			instance := fetchInstance(namespace, name)
			Expect(instance.Spec.Storage.Size).To(Equal(resource.MustParse("100Gi")),
				"Recommend mode resized %s", name)
			// The CRD defaults admission to {schedulable: true, cordoned: false}, so its
			// presence proves nothing; what would prove a cordon is the flag being set.
			if instance.Spec.Admission != nil && instance.Spec.Admission.Cordoned != nil {
				Expect(*instance.Spec.Admission.Cordoned).To(BeFalse(),
					"Recommend mode cordoned %s", name)
			}
			if instance.Spec.Drain != nil && instance.Spec.Drain.Mode != nil {
				Expect(*instance.Spec.Drain.Mode).To(Equal(pgelasticv1alpha1.InstanceDrainNever),
					"Recommend mode asked %s to drain", name)
			}
		}

		for _, action := range pool.Status.Autoscaling.Actions {
			if action.Name == pgelasticv1alpha1.AutoActionStorageExpand {
				continue
			}
			Expect(action.Permitted).To(BeFalse(),
				"Recommend mode permitted %s", action.Name)
			Expect(action.Reason).To(Equal(autoscale.ReasonRecommendMode))
			Expect(action.ExecutedAt).To(BeNil())
		}
	})

	// Storage expansion is the one exception, because a full volume is an outage that no
	// amount of human gating prevents and growing one is online.
	It("expands a filling volume even in Recommend mode", func() {
		target := instances[0]
		setStorageUsage(namespace, target, "100Gi", "93Gi")

		Eventually(func(g Gomega) {
			instance := fetchInstance(namespace, target)
			g.Expect(instance.Spec.Storage.Size.Value()).To(BeNumerically(">", int64(100)<<30),
				"a volume at 93%% was left at its original size")
		}).Should(Succeed())

		// The expansion is recorded in the plan and stays recorded once the class stops being
		// proposed, because that timestamp is what the stabilization windows are measured from.
		Eventually(func(g Gomega) {
			plan := fetchPool(namespace, poolName).Status.Autoscaling
			var expand *pgelasticv1alpha1.PlannedAction
			for i := range plan.Actions {
				if plan.Actions[i].Name == pgelasticv1alpha1.AutoActionStorageExpand {
					expand = &plan.Actions[i]
				}
			}
			g.Expect(expand).NotTo(BeNil())
			g.Expect(expand.ExecutedAt).NotTo(BeNil())
		}).Should(Succeed())
	})

	It("emits no migration objects at all while in Recommend mode", func() {
		migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
		Expect(k8sClient.List(suiteCtx, migrations, client.InNamespace(namespace))).To(Succeed())
		Expect(migrations.Items).To(BeEmpty(),
			"Recommend mode moved a tenant, which is the one thing it must never do")
	})
})

// createReadyInstance creates a PgInstance and publishes the capacity a placement decision
// reads. Placement never asks what an instance was configured with; it asks what the
// instance says it can hold, which is why the status is what this fixture writes.
func createReadyInstance(namespace, name, pool string, allocatable int32) {
	GinkgoHelper()
	instance := &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef: corev1.LocalObjectReference{Name: pool},
			Class:   "gp-8",
			Storage: pgelasticv1alpha1.InstanceStorage{
				Size:      resource.MustParse("100Gi"),
				WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("20Gi")},
			},
		},
	}
	Expect(k8sClient.Create(suiteCtx, instance)).To(Succeed())

	instance.Status = pgelasticv1alpha1.PgInstanceStatus{
		Phase:          pgelasticv1alpha1.InstancePhaseReady,
		CurrentPrimary: name + "-1",
		Capacity: &pgelasticv1alpha1.InstanceCapacityStatus{
			MaxConnections: allocatable + 20,
			Allocatable:    allocatable,
			InUse:          4,
		},
		Storage: &pgelasticv1alpha1.InstanceStorageStatus{
			Allocated: ptr.To(resource.MustParse("100Gi")),
			Used:      ptr.To(resource.MustParse("10Gi")),
		},
	}
	Expect(k8sClient.Status().Update(suiteCtx, instance)).To(Succeed())
}

func setStorageUsage(namespace, name, allocated, used string) {
	GinkgoHelper()
	instance := fetchInstance(namespace, name)
	instance.Status.Storage = &pgelasticv1alpha1.InstanceStorageStatus{
		Allocated: ptr.To(resource.MustParse(allocated)),
		Used:      ptr.To(resource.MustParse(used)),
	}
	Expect(k8sClient.Status().Update(suiteCtx, instance)).To(Succeed())
}

// seedWindow fills a tenant's trailing window with a steady demand, so the packing statistic
// is a measured percentile rather than a declared ceiling.
func seedWindow(namespace, pool, tenant string, connections float64) {
	key := metering.Key{Namespace: namespace, Pool: pool, Tenant: tenant}
	now := time.Now()
	for i := range 40 {
		collector.Store.Observe(key,
			metering.Sample{BackendConnections: connections, StorageBytes: 4 << 30},
			now.Add(-time.Duration(40-i)*time.Minute))
	}
}

func fetchPool(namespace, name string) *pgelasticv1alpha1.PgElasticPool {
	GinkgoHelper()
	pool := &pgelasticv1alpha1.PgElasticPool{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, pool)).To(Succeed())
	return pool
}

func fetchInstance(namespace, name string) *pgelasticv1alpha1.PgInstance {
	GinkgoHelper()
	instance := &pgelasticv1alpha1.PgInstance{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, instance)).To(Succeed())
	return instance
}

// bindings groups the tenants by the instance they were placed on, read straight from the
// API server rather than from any cache.
func bindings(namespace string, tenants []*pgelasticv1alpha1.PgTenant) map[string][]*pgelasticv1alpha1.PgTenant {
	GinkgoHelper()
	byInstance := map[string][]*pgelasticv1alpha1.PgTenant{}
	for _, tenant := range tenants {
		fetched := &pgelasticv1alpha1.PgTenant{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: namespace, Name: tenant.Name,
		}, fetched)).To(Succeed())
		Expect(fetched.Status.Binding).NotTo(BeNil(), "%s is unbound", tenant.Name)
		host := fetched.Status.Binding.InstanceRef.Name
		byInstance[host] = append(byInstance[host], fetched)
	}
	return byInstance
}

// boundMessage is what the controller said about a tenant it did not place, so a failure
// names the refused constraint rather than only the absence of a binding.
func boundMessage(tenant *pgelasticv1alpha1.PgTenant) string {
	for _, condition := range tenant.Status.Conditions {
		if condition.Type == pgelasticv1alpha1.ConditionBound {
			return fmt.Sprintf("%s: %s", condition.Reason, condition.Message)
		}
	}
	return "no Bound condition at all"
}

func conditionOf(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	GinkgoHelper()
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	Fail(fmt.Sprintf("expected a %s condition", conditionType))
	return nil
}
