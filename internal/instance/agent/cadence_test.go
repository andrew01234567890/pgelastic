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
)

func handingOver(current, target string) *pgelasticv1alpha1.PgInstance {
	instance := &pgelasticv1alpha1.PgInstance{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	instance.Status.CurrentPrimary = current
	instance.Status.TargetPrimary = target
	return instance
}

func TestTheCadenceIsChosenFromWhetherARoleChangeIsInFlight(t *testing.T) {
	supervisor := NewSupervisor(Options{Member: roleStandby})

	if got := supervisor.observeCadence(); got != observeInterval {
		t.Errorf("a supervisor that has observed nothing yet polled every %s, want %s: "+
			"polling hard before there is anything to see is chatter, not speed", got, observeInterval)
	}

	supervisor.noteRoleChange(roleChangeInFlight(handingOver(rolePrimary, roleStandby)))
	if got := supervisor.observeCadence(); got != handoverInterval {
		t.Errorf("the agent polled every %s during a handover, want %s: this loop is on the "+
			"critical path three times per switchover and every client is held for the sum",
			got, handoverInterval)
	}

	supervisor.noteRoleChange(roleChangeInFlight(handingOver(roleStandby, roleStandby)))
	if got := supervisor.observeCadence(); got != observeInterval {
		t.Errorf("the agent stayed at %s after the handover settled, want %s: the fast cadence "+
			"is paid for by the window being bounded and rare", got, observeInterval)
	}
}

func TestARoleChangeIsOnlyInFlightWhenTheCRSaysSo(t *testing.T) {
	for name, instance := range map[string]*pgelasticv1alpha1.PgInstance{
		"the CR could not be read": nil,
		"nothing has been decided": handingOver("", ""),
		"the holder is the target": handingOver(rolePrimary, rolePrimary),
	} {
		if roleChangeInFlight(instance) {
			t.Errorf("%s: read as a handover in flight. The fast cadence would then be the "+
				"steady state, which is the chatter the slow one exists to avoid", name)
		}
	}

	for name, instance := range map[string]*pgelasticv1alpha1.PgInstance{
		"the role is moving to another member": handingOver(rolePrimary, roleStandby),
		"the sentinel is set":                  handingOver(rolePrimary, "pending"),
		"nobody holds it yet":                  handingOver("", rolePrimary),
	} {
		if !roleChangeInFlight(instance) {
			t.Errorf("%s: read as settled. The agent would then take up to %s to notice each "+
				"of the three steps of a switchover", name, observeInterval)
		}
	}
}
