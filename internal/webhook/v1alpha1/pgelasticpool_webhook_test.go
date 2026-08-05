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

package v1alpha1

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

var _ = Describe("PgElasticPool admission", func() {
	It("refuses a pool bound to a class that does not exist", func() {
		const namespace = "wh-pool-dangling"
		ensureNamespace(namespace, nil)

		err := k8sClient.Create(ctx, makePool(namespace, "wh-pool-dangling-pool", "wh-absent-class"))

		Expect(err).To(MatchError(ContainSubstring("no PgElasticClass of that name exists")))
	})

	It("refuses a pool whose default workload class its own allowlist forbids", func() {
		const namespace = "wh-pool-allowlist"
		ensureNamespace(namespace, nil)
		elasticClass := makeElasticClass("wh-pool-allowlist-class")
		mustCreate(elasticClass)
		pool := makePool(namespace, "wh-pool-allowlist-pool", elasticClass.Name)
		pool.Spec.Admission = &pgelasticv1alpha1.PoolAdmission{
			DefaultWorkloadClassName:  "wh-pool-allowlist-absent",
			AllowedWorkloadClassNames: []string{"wh-pool-allowlist-permitted"},
		}

		err := k8sClient.Create(ctx, pool)

		Expect(err).To(MatchError(ContainSubstring("spec.admission.defaultWorkloadClassName")))
	})

	Describe("shrinking a pool that has already made guarantees", Ordered, func() {
		const (
			namespace = "wh-pool-shrink"
			poolName  = "wh-pool-shrink-pool"
			className = "wh-pool-shrink-workload"
		)

		var pool *pgelasticv1alpha1.PgElasticPool

		BeforeAll(func() {
			ensureNamespace(namespace, nil)
			elasticClass := makeElasticClass("wh-pool-shrink-class")
			mustCreate(elasticClass, makeWorkloadClass(className, 40, 40))
			pool = makePool(namespace, poolName, elasticClass.Name)
			mustCreate(pool)
			mustCreate(makeTenant(namespace, "wh-pool-shrink-tenant", poolName, "shrink", className))
		})

		It("refuses a budget that no longer covers what the pool already promised", func() {
			Expect(k8sClient.Get(ctx, keyIn(namespace, poolName), pool)).To(Succeed())
			pool.Spec.Capacity.BackendConnections = 40

			err := k8sClient.Update(ctx, pool)

			Expect(err).To(MatchError(ContainSubstring("already guaranteed")))
			Expect(err).To(MatchError(ContainSubstring("allocatable 30")))
		})

		It("admits a budget that still covers them", func() {
			Expect(k8sClient.Get(ctx, keyIn(namespace, poolName), pool)).To(Succeed())
			pool.Spec.Capacity.BackendConnections = 60

			Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		})
	})

	// Every replica reads one configuration document carrying the undivided budget, so the
	// fleet's worst case is the replica count times the sum of every tenant's ceiling. The
	// reservation ledger above cannot see this: it sums guarantees and has never heard of
	// spec.proxy.replicas.
	//
	// The pool here sets maxOversubscriptionRatio to 1, which is the strict reading: the
	// fleet may not commit past allocatable at all. 75 allocatable and a ceiling of 25 means
	// three replicas fit exactly and a fourth does not.
	Describe("a fleet that would multiply the budget past what the pool committed to",
		Ordered, func() {
			const (
				namespace = "wh-pool-fleet"
				poolName  = "wh-pool-fleet-pool"
				className = "wh-pool-fleet-workload"
			)

			var pool *pgelasticv1alpha1.PgElasticPool

			BeforeAll(func() {
				ensureNamespace(namespace, nil)
				elasticClass := makeElasticClass("wh-pool-fleet-class")
				mustCreate(elasticClass, makeWorkloadClass(className, 0, 25))
				pool = makePool(namespace, poolName, elasticClass.Name)
				pool.Spec.Capacity.MaxOversubscriptionRatio = "1"
				pool.Spec.Proxy = &pgelasticv1alpha1.ProxySpec{Replicas: ptrTo(int32(3))}
				mustCreate(pool)
			})

			It("admits the tenant whose ceiling fills the fleet's budget exactly", func() {
				Expect(k8sClient.Create(ctx,
					makeTenant(namespace, "wh-pool-fleet-a", poolName, "fleeta", className))).
					To(Succeed())
			})

			It("refuses a replica count that overcommits, naming every figure it computed", func() {
				Expect(k8sClient.Get(ctx, keyIn(namespace, poolName), pool)).To(Succeed())
				pool.Spec.Proxy.Replicas = ptrTo(int32(4))

				err := k8sClient.Update(ctx, pool)

				Expect(err).To(MatchError(ContainSubstring("spec.proxy.replicas")))
				Expect(err).To(MatchError(ContainSubstring("4 x 25 = 100")))
				Expect(err).To(MatchError(ContainSubstring("ceiling of 75")))
				Expect(err).To(MatchError(ContainSubstring("allocatable 75")))
				Expect(err).To(MatchError(ContainSubstring("maxOversubscriptionRatio 1")))
			})

			// The gate would be decorative if it only ran when a pool was edited: a tenant
			// added afterwards raises the same sum without the pool changing at all.
			It("refuses a second tenant that pushes the same worst case over", func() {
				err := k8sClient.Create(ctx,
					makeTenant(namespace, "wh-pool-fleet-b", poolName, "fleetb", className))

				Expect(err).To(MatchError(ContainSubstring("3 x 50 = 150")))
				Expect(err).To(MatchError(ContainSubstring("spec.workloadClassName")))
			})

			It("still admits a tenant small enough to fit beside the first", func() {
				Expect(k8sClient.Get(ctx, keyIn(namespace, poolName), pool)).To(Succeed())
				pool.Spec.Capacity.BackendConnections = 200
				Expect(k8sClient.Update(ctx, pool)).To(Succeed())

				Expect(k8sClient.Create(ctx,
					makeTenant(namespace, "wh-pool-fleet-c", poolName, "fleetc", className))).
					To(Succeed())
			})
		})
})

// spec.proxy.template.spec is a whole corev1.PodSpec, strategically merged over the pod the
// controller generates - and a strategic merge adds rather than replaces. The proxy pod is
// mounted with the rendered configuration, which carries every tenant's password and backend
// SCRAM keys, so a container or volume added here is a second identity inside that mount.
var _ = Describe("the proxy pod template escape hatch", Ordered, func() {
	const namespace = "wh-podspec"

	var poolNumber int

	BeforeAll(func() {
		ensureNamespace(namespace, nil)
		mustCreate(makeElasticClass("wh-podspec-class"))
	})

	// Every template must carry containers - the PodSpec schema requires it - so the
	// legitimate shape names the generated container and patches it. The image is never
	// pulled: the merge takes the generated container's, and no pod is created here.
	const unusedImage = "ignored"
	patchesTheProxy := []corev1.Container{{Name: "proxy", Image: unusedImage}}

	poolWith := func(spec *corev1.PodSpec) *pgelasticv1alpha1.PgElasticPool {
		if len(spec.Containers) == 0 {
			spec.Containers = patchesTheProxy
		}
		poolNumber++
		pool := makePool(namespace, fmt.Sprintf("wh-podspec-%d", poolNumber), "wh-podspec-class")
		pool.Spec.Proxy = &pgelasticv1alpha1.ProxySpec{
			Template: &pgelasticv1alpha1.ProxyPodTemplate{Spec: spec},
		}
		return pool
	}

	DescribeTable("refuses what would put a second identity in the proxy's pod",
		func(spec *corev1.PodSpec, expected string) {
			err := k8sClient.Create(ctx, poolWith(spec))

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(expected))
		},
		Entry("a container beside the proxy", &corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "proxy", Image: unusedImage},
				{Name: "sidecar", Image: "busybox"},
			},
		}, "containers[1].name"),
		Entry("an init container", &corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "init", Image: "busybox"}},
		}, "template.spec.initContainers"),
		Entry("a volume naming any Secret in the namespace", &corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "loot",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: "px-pool-proxy-config"},
				},
			}},
		}, "template.spec.volumes"),
		Entry("another service account", &corev1.PodSpec{
			ServiceAccountName: "something-wider",
		}, "template.spec.serviceAccountName"),
		Entry("the node's namespaces", &corev1.PodSpec{
			HostNetwork: true,
		}, "template.spec.hostNetwork"),
	)

	// The hatch exists for placement, and that half stays open.
	It("admits the scheduling fields the hatch is for", func() {
		mustCreate(poolWith(&corev1.PodSpec{
			Containers:        patchesTheProxy,
			NodeSelector:      map[string]string{"kubernetes.io/os": "linux"},
			PriorityClassName: "system-cluster-critical",
			Tolerations:       []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
		}))
	})
})

// The only writes left on a pool being deleted are the controller clearing its own
// finalizer. Validating those against rules the pool now fails - a class that has since been
// deleted, a ledger its tenants have outgrown - rejects them for ever, and the pool then
// keeps its finalizer and the namespace never leaves Terminating. The tenant and tenant-user
// validators already carried this bypass; the pool's did not.
var _ = Describe("PgElasticPool webhook during deletion", func() {
	It("admits a write to a pool being deleted whose class is already gone", func() {
		validator := &PgElasticPoolCustomValidator{Reader: k8sClient}
		pool := makePool("default", "doomed-pool", "class-that-does-not-exist")
		pool.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		pool.Finalizers = []string{"pgelastic.io/pool-members"}

		_, err := validator.ValidateUpdate(ctx, pool, pool)

		Expect(err).NotTo(HaveOccurred())
	})
})
