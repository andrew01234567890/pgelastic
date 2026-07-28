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

var _ = Describe("the single global PgWorkloadClass", Ordered, func() {
	const (
		firstGlobal  = "wh-global-first"
		secondGlobal = "wh-global-second"
	)

	BeforeAll(func() {
		// Being global is cluster-wide state, so it must not outlive this container:
		// another spec asserts on what happens when no class is global at all.
		DeferCleanup(func() {
			mustDelete(makeWorkloadClass(firstGlobal, 0, 8), makeWorkloadClass("wh-global-promoted", 0, 8))
		})
	})

	It("admits the first class to claim it", func() {
		workloadClass := makeWorkloadClass(firstGlobal, 0, 8)
		workloadClass.Spec.Global = ptrTo(true)

		mustCreate(workloadClass)
	})

	It("refuses a second class claiming it", func() {
		workloadClass := makeWorkloadClass(secondGlobal, 0, 8)
		workloadClass.Spec.Global = ptrTo(true)

		err := k8sClient.Create(ctx, workloadClass)

		Expect(err).To(MatchError(ContainSubstring("at most one PgWorkloadClass cluster-wide may be global")))
		Expect(err).To(MatchError(ContainSubstring(firstGlobal)))
	})

	It("refuses a non-global class that is later promoted to global", func() {
		workloadClass := makeWorkloadClass("wh-global-promoted", 0, 8)
		mustCreate(workloadClass)

		workloadClass.Spec.Global = ptrTo(true)
		err := k8sClient.Update(ctx, workloadClass)

		Expect(err).To(MatchError(ContainSubstring(firstGlobal)))
	})

	It("keeps admitting updates to the class that already holds it", func() {
		workloadClass := &pgelasticv1alpha1.PgWorkloadClass{}
		Expect(k8sClient.Get(ctx, keyOf(firstGlobal), workloadClass)).To(Succeed())

		workloadClass.Spec.Capacity.Burstable = 16

		Expect(k8sClient.Update(ctx, workloadClass)).To(Succeed())
	})
})

var _ = Describe("PgWorkloadClass self-consistency at admission", func() {
	It("refuses a class whose declared timeout is not a limit", func() {
		workloadClass := makeWorkloadClass("wh-selfcheck", 0, 8)
		workloadClass.Spec.Limits = &pgelasticv1alpha1.TenantLimits{StatementTimeout: duration(0)}

		err := k8sClient.Create(ctx, workloadClass)

		Expect(err).To(MatchError(ContainSubstring("spec.limits.statementTimeout")))
	})
})
