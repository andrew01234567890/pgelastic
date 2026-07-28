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

import (
	"testing"
	"time"
)

var evidenceObservedAt = time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

func anyOneEvidence() Evidence {
	return Evidence{
		SynchronousStandbyNames: `ANY 1 ("pg-2","pg-3")`,
		NumSync:                 1,
		VotingMembers:           []string{memberTwo, memberThree},
		StreamingMembers:        []string{memberTwo, memberThree},
		ReportedBy:              memberOne,
		ObservedAt:              evidenceObservedAt,
	}
}

func TestAnyOneNeedsBothStandbysReachable(t *testing.T) {
	verdict := EvaluateQuorum(anyOneEvidence(), []string{memberTwo, memberThree}, evidenceObservedAt)

	if !verdict.Satisfied {
		t.Fatalf("R=2 W=1 N=2 satisfies R+W>N: %s", verdict.Message)
	}
	if verdict.N != 2 || verdict.W != 1 || verdict.R != 2 {
		t.Fatalf("arithmetic was R=%d W=%d N=%d", verdict.R, verdict.W, verdict.N)
	}
}

func TestOneReachableStandbyDeniesFailover(t *testing.T) {
	verdict := EvaluateQuorum(anyOneEvidence(), []string{memberTwo}, evidenceObservedAt)

	if verdict.Satisfied {
		t.Fatal("with one standby visible nothing proves it acknowledged the last commit")
	}
	if verdict.Reason != QuorumNotProven {
		t.Fatalf("reason was %q", verdict.Reason)
	}
}

func TestReachabilityOutsideTheVotingSetDoesNotCount(t *testing.T) {
	verdict := EvaluateQuorum(anyOneEvidence(), []string{memberTwo, "pg-9"}, evidenceObservedAt)

	if verdict.Satisfied {
		t.Fatal("a reachable non-voter contributes nothing to R")
	}
	if verdict.R != 1 {
		t.Fatalf("R was %d, want 1", verdict.R)
	}
}

func TestMissingEvidenceDeniesFailover(t *testing.T) {
	for name, evidence := range map[string]Evidence{
		"an entirely absent record": {},
		"an empty loaded clause": {
			NumSync:    1,
			ObservedAt: evidenceObservedAt,
		},
		"a clause naming nobody": {
			SynchronousStandbyNames: "ANY 1 ()",
			NumSync:                 1,
			ObservedAt:              evidenceObservedAt,
		},
		"a zero numsync": {
			SynchronousStandbyNames: `ANY 0 ("pg-2","pg-3")`,
			VotingMembers:           []string{memberTwo, memberThree},
			ObservedAt:              evidenceObservedAt,
		},
	} {
		t.Run(name, func(t *testing.T) {
			verdict := EvaluateQuorum(evidence, []string{memberTwo, memberThree}, evidenceObservedAt)
			if verdict.Satisfied {
				t.Fatal("empty or missing evidence must deny the failover")
			}
			if verdict.Reason != QuorumEvidenceMissing {
				t.Fatalf("reason was %q", verdict.Reason)
			}
		})
	}
}

func TestEvidenceOlderThanTheMaximumIsTreatedAsMissing(t *testing.T) {
	evidence := anyOneEvidence()
	failingSince := evidenceObservedAt.Add(MaxEvidenceAge + time.Second)

	verdict := EvaluateQuorum(evidence, []string{memberTwo, memberThree}, failingSince)

	if verdict.Satisfied {
		t.Fatal("stale evidence is treated as missing evidence")
	}
	if verdict.Reason != QuorumEvidenceStale {
		t.Fatalf("reason was %q", verdict.Reason)
	}
}

func TestEvidenceIsAgedAgainstTheFailingInstantNotAgainstNow(t *testing.T) {
	// The primary is the only writer of the record, so it stops being refreshed the moment
	// the primary dies. Ageing it against now would deny every failover that took longer
	// than MaxEvidenceAge to be noticed.
	evidence := anyOneEvidence()
	failingSince := evidenceObservedAt.Add(2 * time.Second)

	verdict := EvaluateQuorum(evidence, []string{memberTwo, memberThree}, failingSince)

	if !verdict.Satisfied {
		t.Fatalf("evidence current at the failing instant must stay valid: %s", verdict.Message)
	}
}

func TestWriteStalledWhenFewerStandbysStreamThanNumSync(t *testing.T) {
	evidence := anyOneEvidence()
	evidence.StreamingMembers = nil

	if !WriteStalled(evidence) {
		t.Fatal("ANY 1 with nothing streaming stalls every commit and must be surfaced")
	}

	evidence.StreamingMembers = []string{memberThree}
	if WriteStalled(evidence) {
		t.Fatal("one streaming standby satisfies ANY 1")
	}
}

func TestWriteStalledIsFalseWithoutASynchronousQuorum(t *testing.T) {
	if WriteStalled(Evidence{}) {
		t.Fatal("an instance with no synchronous quorum cannot stall on one")
	}
}
