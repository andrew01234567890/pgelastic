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
	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// This file is the only place in the package that knows about the API types. The decision
// logic itself stays a pure function of plain Go values, so that a promotion can be
// reproduced in a unit test without a cluster.

// EvidenceFrom lifts the CR's quorum record into the form the gate evaluates. A missing
// record yields zero evidence, and zero evidence denies a failover.
func EvidenceFrom(record *pgelasticv1alpha1.QuorumEvidence) Evidence {
	if record == nil {
		return Evidence{}
	}
	evidence := Evidence{
		SynchronousStandbyNames: record.SynchronousStandbyNames,
		NumSync:                 record.NumSync,
		VotingMembers:           record.VotingMembers,
		StreamingMembers:        record.StreamingMembers,
		ReportedBy:              record.ReportedBy,
	}
	if record.ObservedAt != nil {
		evidence.ObservedAt = record.ObservedAt.Time
	}
	return evidence
}
