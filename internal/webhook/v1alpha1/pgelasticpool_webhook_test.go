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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
