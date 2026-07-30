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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// servingMember is the member holding the role in this file's fixtures. Every row but the
// half-built one names it, because "has served at least once" is what separates an instance
// being rolled from one being built.
const servingMember = "pg-a-1"

// The table this file exists to be.
//
// status.capacity.allocatable is part of the proxy fleet's structural configuration: the
// rendered document carries it, the structural half of that document is the fleet
// Deployment's Pod template annotation, and a changed template replaces every replica. With
// maxSurge zero a replica is deleted before its replacement exists, so every move of this
// number drops every client on the pool - on every tenant of every instance, including the
// ones nothing was happening to.
//
// It has been got wrong twice, both times by writing down which states publish the capacity
// instead of which ones withhold it, and both times the same way: a state nobody had thought
// of fell through to the zero. So the rule is stated here as data. A new InstancePhase that
// is not in this table is published, deliberately, because an unrecognised state is not
// evidence that an instance has stopped carrying its tenants - and getting that wrong costs
// a status field being briefly wrong, where the other direction costs every client on the
// pool.
//
// The mutation test: restore the allow-list of serving phases in allocatableOf, or drop the
// roll.active clause, and the rolling rows go red naming the number and the state.
func TestAllocatableIsWithheldOnlyWhereTheInstanceIsNotCarryingTenants(t *testing.T) {
	const rated int32 = 50
	capacity := pgconf.Capacity{Allocatable: rated}

	rolling := rollState{active: true, member: "pg-a-2"}
	settled := rollState{}

	for _, tc := range []struct {
		name    string
		primary string
		phase   pgelasticv1alpha1.InstancePhase
		roll    rollState
		brain   bool
		want    int32
		why     string
	}{
		{
			name:  "half-built, has never served",
			phase: pgelasticv1alpha1.InstancePhaseBootstrapping,
			roll:  settled,
			want:  0,
			why: "an instance that has not yet carried a tenant has no headroom to sell; " +
				"currentPrimary is empty until a member takes the role and is never cleared " +
				"afterwards, so it is the durable form of that fact",
		},
		{
			name:    "a member's Pod is gone because the roll deleted it",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseBootstrapping,
			roll:    rolling,
			want:    rated,
			why: "this is the defect: phaseOf reports fewer Pods than replicas as " +
				"Bootstrapping before it looks at readiness, so a roll spends part of every " +
				"member restart in a phase no list of serving states would contain",
		},
		{
			name:    "a member's Pod is gone and no roll is running",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseBootstrapping,
			roll:    settled,
			want:    rated,
			why: "the instance is serving its tenants on the members it has left, which is " +
				"the same answer Degraded gets; withholding here would move the number for a " +
				"Pod nobody is coming back for either",
		},
		{
			name:    "serving every member",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseReady,
			roll:    settled,
			want:    rated,
		},
		{
			name:    "one member down",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseDegraded,
			roll:    settled,
			want:    rated,
			why:     "the instance is carrying every tenant it had, on the same connections",
		},
		{
			name:    "handing the role over",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseFailingOver,
			roll:    rolling,
			want:    rated,
			why:     "every roll passes through this on its way to restarting the primary",
		},
		{
			name:    "a member rebuilding itself, outside a roll",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseRecloning,
			roll:    settled,
			want:    0,
			why: "the minutes-to-hours case the zero was written for: reduced redundancy " +
				"with no bounded end and no operator action pacing it",
		},
		{
			name:    "a member rebuilding itself during a roll",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseRecloning,
			roll:    rolling,
			want:    rated,
			why:     "one member being restarted on purpose, one at a time, is not that case",
		},
		{
			name:    "being drained",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseDraining,
			roll:    settled,
			want:    0,
		},
		{
			name:    "being taken apart",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseTerminating,
			roll:    settled,
			want:    0,
		},
		{
			name:    "split brain outranks the roll",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhaseReady,
			roll:    rolling,
			brain:   true,
			want:    0,
			why: "two members that both believe they hold the role are not an instance " +
				"carrying its tenants, whatever else is happening to it",
		},
		{
			name:    "a phase this function has not been taught",
			primary: servingMember,
			phase:   pgelasticv1alpha1.InstancePhase("SomethingAddedLater"),
			roll:    settled,
			want:    rated,
			why: "publishing is the safe direction. A wrong status field is a display fault; " +
				"a wrong zero rolls the fleet and drops every client on the pool",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance := &pgelasticv1alpha1.PgInstance{
				Status: pgelasticv1alpha1.PgInstanceStatus{CurrentPrimary: tc.primary},
			}
			decision := ha.Decision{SplitBrain: tc.brain}

			got := allocatableOf(instance, tc.phase, capacity, tc.roll, decision)
			if got != tc.want {
				t.Fatalf("allocatable was %d, want %d: %s", got, tc.want, tc.why)
			}
		})
	}
}

// The premise the table above rests on, asserted rather than assumed.
//
// If phaseOf ever stops reporting a missing Pod as Bootstrapping, the row that matters most
// in that table stops describing anything real - and it would keep passing, because it takes
// the phase as a parameter. This is what stops the two drifting apart.
func TestAMissingMemberPodReadsAsBootstrapping(t *testing.T) {
	instance := &pgelasticv1alpha1.PgInstance{
		Status: pgelasticv1alpha1.PgInstanceStatus{CurrentPrimary: servingMember},
	}
	groups := []provision.Group{{Serial: 1}, {Serial: 2}, {Serial: 3}}
	present := []corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: servingMember}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pg-a-3"}},
	}

	phase := phaseOf(instance, groups, present, ha.Decision{}, 3)
	if phase != pgelasticv1alpha1.InstancePhaseBootstrapping {
		t.Fatalf("two Pods against three replicas read as %q, so the roll no longer passes "+
			"through the state the capacity table's rolling rows are about", phase)
	}
}
