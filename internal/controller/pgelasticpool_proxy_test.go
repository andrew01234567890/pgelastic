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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

// A tenant whose PgWorkloadClass cannot be read has unknown capacity, not a cancelled
// existence. It used to be dropped from the pool view entirely, which took it out of the
// rendered routing table AND the login table - so deleting one PgWorkloadClass stopped every
// tenant that named it from being routed anywhere or authenticating at all, and the fix for
// that is somewhere the operator has to think of rather than something the fleet reports.
func TestATenantWhoseClassIsGoneIsStillServed(t *testing.T) {
	named := func(name string) *pgelasticv1alpha1.PgTenant {
		return &pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec:       pgelasticv1alpha1.PgTenantSpec{DatabaseName: name},
		}
	}
	view := &poolView{
		tenants: []tenantView{{
			tenant:    named("resolved"),
			effective: policy.Effective{WorkloadClassName: "gold", Burstable: 8},
		}},
		unresolved: []*pgelasticv1alpha1.PgTenant{named("orphaned")},
	}

	served := view.allTenants()
	names := make([]string, 0, len(served))
	for _, tenant := range served {
		names = append(names, tenant.Name)
	}

	if len(names) != 2 {
		t.Fatalf("the pool serves %v; a tenant whose class went missing was dropped from "+
			"everything the document is rendered from", names)
	}
}
