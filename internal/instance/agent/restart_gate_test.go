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

package agent

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
)

// The three members of the instance every case in this file is about.
const (
	rolePrimary = "demo-1"
	roleStandby = "demo-2"
	roleOther   = "demo-3"
)

// steady is the instance every one of these cases starts from: demo-1 holds the role, it
// is the target as well, and the annotation names whichever members a roll is disrupting.
func steady(namedMembers string) *pgelasticv1alpha1.PgInstance {
	instance := &pgelasticv1alpha1.PgInstance{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	if namedMembers != "" {
		instance.Annotations = map[string]string{ha.AnnotationMaintenance: namedMembers}
	}
	instance.Status.CurrentPrimary = rolePrimary
	instance.Status.TargetPrimary = rolePrimary
	return instance
}

func TestARestartWaitsForTheOperatorToNameThisMember(t *testing.T) {
	supervisor := NewSupervisor(Options{Member: roleStandby})

	for name, instance := range map[string]*pgelasticv1alpha1.PgInstance{
		"nothing is being rolled":        steady(""),
		"another member is being rolled": steady(roleOther),
	} {
		if supervisor.restartPermitted(instance) {
			t.Errorf("%s: demo-2 restarted its own postmaster without being named. One "+
				"ConfigMap reaches all three members, so unnamed restarts are three at once",
				name)
		}
	}

	if !supervisor.restartPermitted(steady(roleStandby)) {
		t.Error("a member the operator named has to take its turn or the roll never moves")
	}
}

func TestAMemberBeingHandedItsRoleAwayDoesNotRestartInPlace(t *testing.T) {
	supervisor := NewSupervisor(Options{Member: rolePrimary})

	handingOver := steady(rolePrimary)
	handingOver.Status.TargetPrimary = roleStandby
	if supervisor.restartPermitted(handingOver) {
		t.Error("demo-1 is being switched away; restarting in place brings back the primary " +
			"the operator has already decided against, and the handover has to ask twice")
	}

	sentinel := steady(rolePrimary)
	sentinel.Status.TargetPrimary = ha.TargetPrimaryPending
	if !supervisor.restartPermitted(sentinel) {
		t.Error("the sentinel names nobody, so it is not another member holding this one back")
	}
}

func TestAnUnreadableInstanceRefusesTheRestart(t *testing.T) {
	supervisor := NewSupervisor(Options{Member: roleStandby})

	if supervisor.restartPermitted(nil) {
		t.Error("a member that cannot read the instance cannot know it is its turn")
	}
}
