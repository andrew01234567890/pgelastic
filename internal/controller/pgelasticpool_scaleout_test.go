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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/andrew01234567890/pgelastic/internal/autoscale"
	"github.com/andrew01234567890/pgelastic/internal/metering"
)

var scaleOutNamespaces int

// Membership is poolRef and nothing else, so the members a pool can see include instances that
// are not its to keep. A PgRestore's recovery instance is handed the source pool's poolRef,
// reaches Ready while a tenant is extracted from it, and is deleted when the restore finishes.
var _ = Describe("raising a pool's declared member count", func() {
	var (
		namespace string
		poolName  = "scaleout-pool"
		className = "scaleout-class"
	)

	BeforeEach(func() {
		scaleOutNamespaces++
		namespace = fmt.Sprintf("scaleout-%d", scaleOutNamespaces)
		ensureNamespace(namespace)
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx,
			makeElasticClass(className, defaultControllerName)))).To(Succeed())
	})

	// The proposal is made against the members that serve. An executor asking a different
	// question refuses what the planner permitted, and because at most one class executes per
	// pass and ScaleOut sits above Rebalance and ScaleIn, a permitted-and-refused ScaleOut is
	// every class beneath it never running again.
	It("applies a scale-out the planner permitted while a member is out of service", func() {
		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Instances.Replicas = ptrTo(int32(3))
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)

		reconciler := &PgElasticPoolReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			Metering: metering.NewCollector(metering.Options{}, nil),
		}
		// Three members, one of them cordoned - a scale-in victim mid-drain, or an instance
		// recovered from a backup that nobody has looked at yet.
		applied, err := reconciler.scaleOut(ctx, refetch(pool), autoscale.Plan{
			ObservedInstances:    3,
			ServingInstances:     2,
			RecommendedInstances: 3,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(applied).To(BeTrue(),
			"the planner permitted this scale-out and the executor declined it, so the pool "+
				"sits over its target for ever and nothing below ScaleOut ever runs")
		Expect(*refetch(pool).Spec.Instances.Replicas).To(Equal(int32(4)))
	})

	It("adds to what the pool declares, not to what it happens to see", func() {
		pool := makePool(namespace, poolName, className, 300)
		pool.Spec.Instances.Replicas = ptrTo(int32(3))
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		awaitCached(pool)

		reconciler := &PgElasticPoolReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			Metering: metering.NewCollector(metering.Options{}, nil),
		}
		// Four members seen against three declared, which is the shape a pool takes while one
		// of its tenants is being restored.
		applied, err := reconciler.scaleOut(ctx, refetch(pool), autoscale.Plan{
			ObservedInstances:    4,
			RecommendedInstances: 6,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(applied).To(BeTrue())

		Expect(*refetch(pool).Spec.Instances.Replicas).To(Equal(int32(4)),
			"the pool declared against a member it does not keep, so it will provision one "+
				"more primary than the operator asked for when that member goes")
	})
})
