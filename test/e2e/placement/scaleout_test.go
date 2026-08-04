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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/autoscale"
)

const (
	// jammedTenantCount is enough tenants that a repack has something to move and few enough
	// that they all bind in seconds.
	jammedTenantCount = 12
	// jammedAllocatable is each member's published budget.
	jammedAllocatable = 60
)

// A pool that has ever scaled out declares a member nothing makes. From then on the plan is
// computed against the members that exist and executed against the number declared, so the
// same scale-out is proposed on every pass, permitted on every pass, and applies nothing.
//
// That is not one stuck action. ScaleOut sits above Rebalance and ScaleIn in ActionOrder and
// at most one class executes per pass, so a scale-out that can never land is every class
// below it never running again - on a pool that is over its target and visibly skewed.
var _ = Describe("a pool whose declared member count outran its members", Ordered, func() {
	var (
		namespace    string
		poolName     = "e2e-jammed-pool"
		className    = "e2e-jammed-class"
		workloadName = "e2e-jammed-standard"
		instances    []string
	)

	BeforeAll(func() {
		namespace = uniqueNamespace("pgelastic-jammed")
		Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())

		elasticClass := &pgelasticv1alpha1.PgElasticClass{
			ObjectMeta: metav1.ObjectMeta{Name: className},
			Spec:       pgelasticv1alpha1.PgElasticClassSpec{ControllerName: suiteControllerName},
		}
		Expect(k8sClient.Create(suiteCtx, elasticClass)).To(Succeed())

		workloadClass := &pgelasticv1alpha1.PgWorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: workloadName},
			Spec: pgelasticv1alpha1.PgWorkloadClassSpec{
				Priority: 1000,
				Capacity: pgelasticv1alpha1.WorkloadCapacity{
					Guaranteed: ptr.To(int32(2)),
					Burstable:  20,
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
					// Four declared against the three that exist: the state a pool reaches by
					// scaling out once, and never leaves.
					Replicas: ptr.To(int32(4)),
					Template: pgelasticv1alpha1.PgInstanceTemplate{
						Class: "gp-8",
						Storage: pgelasticv1alpha1.InstanceStorage{
							Size:      resource.MustParse("100Gi"),
							WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("20Gi")},
						},
					},
				},
				Admission: &pgelasticv1alpha1.PoolAdmission{DefaultWorkloadClassName: workloadName},
				Autoscaling: &pgelasticv1alpha1.PoolAutoscaling{
					Mode: pgelasticv1alpha1.AutoscalingAuto,
					AutoActions: []pgelasticv1alpha1.AutoAction{
						pgelasticv1alpha1.AutoActionScaleOut,
						pgelasticv1alpha1.AutoActionRebalance,
					},
					// Low enough that the fixture's load is over it, so a scale-out is
					// genuinely proposed rather than merely refused.
					TargetUtilizationPercent: ptr.To(int32(40)),
					MinInstances:             ptr.To(int32(1)),
					MaxInstances:             ptr.To(int32(8)),
				},
				Rebalancing: &pgelasticv1alpha1.PoolRebalancing{
					Enabled: ptr.To(true),
					// The source-load ceiling and the cold-tenant threshold are separate
					// guardrails with their own tests. Lift both, so that what this spec
					// observes is the ordering between classes rather than one of them firing.
					ForbidMoveWhenSourceUtilizationAbovePercent: ptr.To(int32(95)),
					HotTenantUtilizationThresholdPercent:        ptr.To(int32(50)),
				},
			},
		}
		Expect(k8sClient.Create(suiteCtx, pool)).To(Succeed())

		for i := range 3 {
			name := fmt.Sprintf("e2e-jam-%c", 'a'+i)
			instances = append(instances, name)
			createReadyInstance(namespace, name, poolName, jammedAllocatable)
		}

		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespace},
			}))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, elasticClass))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, workloadClass))).To(Succeed())
		})
	})

	It("crowds every tenant onto one member, so a repack has somewhere to move them", func() {
		By("cordoning the other two members while the tenants arrive")
		for _, name := range instances[1:] {
			instance := fetchInstance(namespace, name)
			patch := client.MergeFrom(instance.DeepCopy())
			instance.Spec.Admission = &pgelasticv1alpha1.InstanceAdmission{Schedulable: ptr.To(false)}
			Expect(k8sClient.Patch(suiteCtx, instance, patch)).To(Succeed())
		}

		var tenants []*pgelasticv1alpha1.PgTenant
		for i := range jammedTenantCount {
			name := fmt.Sprintf("e2e-jam-tenant-%02d", i)
			tenant := &pgelasticv1alpha1.PgTenant{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: pgelasticv1alpha1.PgTenantSpec{
					PoolRef:      corev1.LocalObjectReference{Name: poolName},
					DatabaseName: fmt.Sprintf("e2e_jam_tenant_%02d", i),
				},
			}
			Expect(k8sClient.Create(suiteCtx, tenant)).To(Succeed())
			tenants = append(tenants, tenant)
			// Admitted small, because admission would refuse the later tenants outright if the
			// only schedulable member could not already hold them all.
			seedWindow(namespace, poolName, name, 2)
		}

		Eventually(func(g Gomega) {
			for _, tenant := range tenants {
				fetched := &pgelasticv1alpha1.PgTenant{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(tenant), fetched)).To(Succeed())
				g.Expect(fetched.Status.Binding).NotTo(BeNil(), "%s never bound", tenant.Name)
				g.Expect(fetched.Status.Binding.InstanceRef.Name).To(Equal(instances[0]),
					"the cordon did not hold the population on one member")
			}
		}).Should(Succeed())

		By("publishing enough load that the pool wants a member it will never get")
		// Uneven across the members, and over the 40% target either way: a pool small enough
		// to want a fourth member, and a spread wide enough to be worth rebalancing once the
		// other two members come back.
		setInUse(namespace, instances[0], 39)
		setInUse(namespace, instances[1], 39)
		setInUse(namespace, instances[2], 21)

		// The jam has to be established before a rebalance is possible at all, or the spec
		// below can be satisfied by a move made in the window before this load was visible -
		// which passes without ever exercising the ordering it exists to prove.
		Eventually(func(g Gomega) {
			plan := fetchPool(namespace, poolName).Status.Autoscaling
			g.Expect(plan).NotTo(BeNil())
			g.Expect(plan.Actions).To(ContainElement(HaveField("Name", pgelasticv1alpha1.AutoActionScaleOut)),
				"the fixture is not over its target, so there is no unrealised scale-out to jam on: %s",
				plan.Summary)
		}).Should(Succeed())

		By("checking nothing has been moved yet, so the spec below measures what it claims to")
		migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
		Expect(k8sClient.List(suiteCtx, migrations, client.InNamespace(namespace))).To(Succeed())
		Expect(migrations.Items).To(BeEmpty())
	})

	It("refuses to declare a fifth member while the fourth has never been made", func() {
		Eventually(func(g Gomega) {
			pool := fetchPool(namespace, poolName)
			g.Expect(pool.Status.Autoscaling).NotTo(BeNil())

			var scaleOut *pgelasticv1alpha1.PlannedAction
			for i := range pool.Status.Autoscaling.Actions {
				if pool.Status.Autoscaling.Actions[i].Name == pgelasticv1alpha1.AutoActionScaleOut {
					scaleOut = &pool.Status.Autoscaling.Actions[i]
				}
			}
			g.Expect(scaleOut).NotTo(BeNil(),
				"the fixture is not over target, so there is no scale-out to refuse: %s",
				pool.Status.Autoscaling.Summary)
			g.Expect(scaleOut.Permitted).To(BeFalse())
			g.Expect(scaleOut.Reason).To(Equal(autoscale.ReasonScaleOutUnrealised))
		}).Should(Succeed())

		By("checking the declared count did not ratchet again")
		Expect(*fetchPool(namespace, poolName).Spec.Instances.Replicas).To(Equal(int32(4)),
			"the pool asked for a fifth member while the fourth had never been made")
	})

	// The whole point of the layer. Rebalance sits below ScaleOut, so it only ever runs if
	// the scale-out above it stops being selected.
	It("lets the rebalance below it run", func() {
		By("returning the other two members to service")
		for _, name := range instances[1:] {
			instance := fetchInstance(namespace, name)
			patch := client.MergeFrom(instance.DeepCopy())
			instance.Spec.Admission = nil
			Expect(k8sClient.Patch(suiteCtx, instance, patch)).To(Succeed())
		}

		By("growing the tenants' observed demand past what one member can hold")
		// The repack is a fresh best-fit over the whole pool, so tenants that still fit where
		// they are stay where they are - packing tightly is the point of it. What makes a
		// rebalance the right answer is a population that no longer fits on one member:
		// twelve tenants at eight connections is 96 against a 60-connection member. Raised
		// here rather than at admission, which would have refused the later tenants outright.
		for i := range jammedTenantCount {
			seedWindow(namespace, poolName, fmt.Sprintf("e2e-jam-tenant-%02d", i), 8)
		}

		Eventually(func(g Gomega) {
			migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
			g.Expect(k8sClient.List(suiteCtx, migrations, client.InNamespace(namespace))).To(Succeed())
			g.Expect(migrations.Items).NotTo(BeEmpty(),
				"no tenant was ever moved off the crowded member; the plan says: %s",
				fetchPool(namespace, poolName).Status.Autoscaling.Summary)
		}, "3m", "2s").Should(Succeed())

		By("checking the move goes where the repack wanted it, off the crowded member")
		// Asserted on the migration rather than on the plan's executedAt: once a move is in
		// flight the budget is spent, so the Rebalance action stops being proposed and the
		// timestamp with it. The object it left behind is the durable evidence.
		migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
		Expect(k8sClient.List(suiteCtx, migrations, client.InNamespace(namespace))).To(Succeed())
		for i := range migrations.Items {
			Expect(migrations.Items[i].Spec.TargetInstanceRef.Name).To(BeElementOf(instances[1:]),
				"a tenant was moved onto the member it was already crowded onto")
		}
	})
})

// setInUse publishes a member's connection load, which is what both the pool's utilization
// and the imbalance between members are read from.
func setInUse(namespace, name string, inUse int32) {
	GinkgoHelper()
	instance := fetchInstance(namespace, name)
	instance.Status.Capacity.InUse = inUse
	Expect(k8sClient.Status().Update(suiteCtx, instance)).To(Succeed())
}
