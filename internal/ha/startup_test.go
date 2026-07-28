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

package ha

import "testing"

func TestStartupDecision(t *testing.T) {
	const inRecovery, outOfRecovery = true, false

	cases := []struct {
		name       string
		self       string
		recovery   bool
		current    string
		target     string
		lease      string
		wantFollow string
		wantRejoin bool
		wantReason string
	}{
		{
			name: "the first member of a brand new instance", self: memberOne,
			recovery: outOfRecovery, wantReason: StartupUnchanged,
		},
		{
			name: "the primary restarting in place", self: memberOne, recovery: outOfRecovery,
			current: memberOne, target: memberOne, lease: memberOne, wantReason: StartupUnchanged,
		},
		{
			name: "a standby restarting in place", self: memberTwo, recovery: inRecovery,
			current: memberOne, target: memberOne, lease: memberOne,
			wantFollow: memberOne, wantReason: StartupFollowPrimary,
		},
		{
			name: "a standby whose primary changed underneath it", self: memberThree,
			recovery: inRecovery, current: memberTwo, target: memberTwo, lease: memberTwo,
			wantFollow: memberTwo, wantReason: StartupFollowPrimary,
		},
		{
			name: "a demoted primary the operator has replaced", self: memberOne,
			recovery: outOfRecovery, current: memberOne, target: memberTwo, lease: memberTwo,
			wantFollow: memberTwo, wantRejoin: true, wantReason: StartupSupersededByTarget,
		},
		{
			// The member was deleted and recreated inside a failover. Nothing in status has
			// caught up with what happened to it, and the Lease is the only thing that has.
			name: "a recreated primary whose status has not caught up", self: memberOne,
			recovery: outOfRecovery, current: memberOne, target: TargetPrimaryPending,
			lease: memberTwo, wantFollow: memberTwo, wantRejoin: true,
			wantReason: StartupSupersededByLease,
		},
		{
			name: "the old primary coming back during the sentinel phase", self: memberOne,
			recovery: outOfRecovery, current: memberOne, target: TargetPrimaryPending,
			lease: memberOne, wantReason: StartupUnchanged,
		},
		{
			name: "a stale primary the instance has moved on from", self: memberOne,
			recovery: outOfRecovery, current: memberThree, target: memberThree,
			wantFollow: memberThree, wantRejoin: true, wantReason: StartupSupersededByTarget,
		},
		{
			name: "a promoted member before it has written currentPrimary", self: memberTwo,
			recovery: outOfRecovery, current: memberOne, target: memberTwo, lease: memberTwo,
			wantReason: StartupUnchanged,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			action := StartupDecision(testCase.self, testCase.recovery,
				testCase.current, testCase.target, testCase.lease)

			if action.Follow != testCase.wantFollow || action.Rejoin != testCase.wantRejoin ||
				action.Reason != testCase.wantReason {
				t.Fatalf("got %+v, want follow=%q rejoin=%v reason=%q",
					action, testCase.wantFollow, testCase.wantRejoin, testCase.wantReason)
			}
		})
	}
}
