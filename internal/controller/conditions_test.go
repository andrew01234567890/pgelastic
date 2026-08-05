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
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Every reconciler in this package builds a fresh status, mutates its conditions, and writes it
// only if it differs from the object's. That last step is the one this protects: meta's setter
// mutates an existing condition in place, so a status seeded with the object's own slice shares
// its backing array and the comparison is against the change itself.
var _ = Describe("carrying an object's conditions into the status a pass is building", func() {
	existing := func() []metav1.Condition {
		return []metav1.Condition{{
			Type:    pgelasticv1alpha1.ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  "Pending",
			Message: "not yet",
		}}
	}

	It("leaves the object's own conditions untouched when the copy is written to", func() {
		object := existing()
		carried := carriedConditions(object)

		setCondition(&carried, 1, pgelasticv1alpha1.ConditionReady,
			metav1.ConditionTrue, "Ready", "serving")

		Expect(object[0].Status).To(Equal(metav1.ConditionFalse),
			"the pass wrote through to the object it was comparing against")
		Expect(carried[0].Status).To(Equal(metav1.ConditionTrue))
	})

	// The property the reconcilers actually depend on, stated as they state it: a pass whose
	// only change is a condition must be publishable. Without the copy this comparison is true
	// and the update is skipped - not deferred, skipped, until some unrelated field moves.
	It("makes a condition-only change visible to the publish gate", func() {
		object := pgelasticv1alpha1.PgTenantStatus{Conditions: existing()}
		status := pgelasticv1alpha1.PgTenantStatus{
			Conditions: carriedConditions(object.Conditions),
		}

		setCondition(&status.Conditions, 1, pgelasticv1alpha1.ConditionReady,
			metav1.ConditionTrue, "Ready", "serving")

		Expect(equality.Semantic.DeepEqual(object, status)).To(BeFalse(),
			"a condition-only change compares equal, so it is never written to the API server")
	})

	It("carries a nil condition list without inventing one", func() {
		Expect(carriedConditions(nil)).To(BeNil())
	})
})
