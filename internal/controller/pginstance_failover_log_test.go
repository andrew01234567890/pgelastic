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
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
)

// A decision that has not changed is not news. This runs on every reconcile of every
// instance, and the e2e suites - the ones whose logs most need reading - enable V(1), so
// logging unconditionally buried every line that explained a failure under thousands that
// said nothing had happened.
func TestTheFailoverDecisionIsLoggedOnlyWhenItChanges(t *testing.T) {
	steady := ha.Decision{Phase: ha.PhaseSteady, Message: "the primary is healthy"}
	instance := &pgelasticv1alpha1.PgInstance{}

	// Nothing recorded yet: the first decision is always worth saying.
	if !failoverDecisionIsNews(instance, steady) {
		t.Error("the first decision was suppressed, so a fresh instance says nothing at all")
	}

	// What the previous pass would have written.
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:    pgelasticv1alpha1.ConditionFailingOver,
		Status:  metav1.ConditionFalse,
		Reason:  failoverReason(steady),
		Message: steady.Message,
	})
	if failoverDecisionIsNews(instance, steady) {
		t.Error("an unchanged decision was logged again")
	}

	if moved := (ha.Decision{Phase: ha.PhaseSteady, Message: "something else entirely"}); !failoverDecisionIsNews(instance, moved) {
		t.Error("a changed message was suppressed, which is the line somebody needs")
	}
}
