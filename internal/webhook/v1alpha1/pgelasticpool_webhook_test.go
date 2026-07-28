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
})
