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
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// setCondition upserts one condition, stamping it with the generation it was computed
// from. Without the per-condition generation a reader cannot tell a condition that was
// reaffirmed against the current spec from one left over from a previous edit.
func setCondition(
	conditions *[]metav1.Condition,
	generation int64,
	conditionType string,
	status metav1.ConditionStatus,
	reason, message string,
) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

// carriedConditions copies an object's conditions into a status a reconcile is building.
//
// A copy, never the slice itself. `meta.SetStatusCondition` mutates an existing condition in
// place, so a status seeded with the object's own slice shares its backing array - and by the
// time the `DeepEqual` gate every one of these reconcilers ends with compares the two, the
// change is present on both sides. A pass whose only change is a condition then reaches the API
// server never: not late, not on the next pass, never, until some other field happens to move.
//
// The elements are values, so one clone is enough; nothing inside a `metav1.Condition` is
// shared.
func carriedConditions(conditions []metav1.Condition) []metav1.Condition {
	return slices.Clone(conditions)
}

// conditionStatus maps a boolean outcome onto a condition status.
func conditionStatus(ok bool) metav1.ConditionStatus {
	if ok {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
