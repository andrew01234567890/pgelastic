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

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// primaryMember is the member every test here elects.
const primaryMember = "pg-1"

func instanceHolding(primary string, members ...pgelasticv1alpha1.InstanceMemberStatus) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		Status: pgelasticv1alpha1.PgInstanceStatus{
			CurrentPrimary: primary,
			Instances:      members,
		},
	}
}

// status.capacity.inUse is summed by the pool in four places and, until this, written by
// nobody. A permanent zero is not a harmless gap: it makes ObservedUtilizationPercent zero,
// so the autoscaler computes desired = 0 and recommends scaling to the floor for ever, and
// it classifies every tenant Cold - which is the permissive side of the automatic-migration
// gate, so the check meant to protect busy tenants from being moved passes everything.
func TestTheInstanceReportsTheConnectionsItIsCarrying(t *testing.T) {
	instance := instanceHolding(primaryMember,
		pgelasticv1alpha1.InstanceMemberStatus{Name: primaryMember, ClientBackends: 137},
		pgelasticv1alpha1.InstanceMemberStatus{Name: "pg-2", ClientBackends: 4},
	)

	if got := inUseOf(instance); got != 137 {
		t.Errorf("inUse = %d, want the primary's 137", got)
	}
}

// Allocatable is derived once from the sizing class and describes what one member sells.
// Summing three members against it would read as saturation at a third of the real load.
func TestOnlyThePrimarysConnectionsCountAgainstTheBudget(t *testing.T) {
	instance := instanceHolding(primaryMember,
		pgelasticv1alpha1.InstanceMemberStatus{Name: primaryMember, ClientBackends: 100},
		pgelasticv1alpha1.InstanceMemberStatus{Name: "pg-2", ClientBackends: 100},
		pgelasticv1alpha1.InstanceMemberStatus{Name: "pg-3", ClientBackends: 100},
	)

	if got := inUseOf(instance); got != 100 {
		t.Errorf("inUse = %d, want 100 rather than a sum across members", got)
	}
}

// An instance mid-failover has no primary to ask. Zero is the honest answer to "how many
// connections is the primary carrying" when there is no primary, and it is what the
// staleness brake is there to notice.
func TestAnInstanceWithNoPrimaryReportsNoConnections(t *testing.T) {
	instance := instanceHolding("",
		pgelasticv1alpha1.InstanceMemberStatus{Name: primaryMember, ClientBackends: 137},
	)

	if got := inUseOf(instance); got != 0 {
		t.Errorf("inUse = %d with no primary elected, want 0", got)
	}
}
