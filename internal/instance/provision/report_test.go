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

package provision

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// The limit and the number of databases a member may carry are two constants that have to
// agree, and they live in different files - one of them a CRD marker Go cannot read. So the
// agreement is asserted rather than remembered.
//
// The failure this prevents is not a metrics gap. A report truncated at the limit fails to
// decode, FetchMemberReport returns the error, the caller records no member at all, and the
// member's own instance sees "unreachable, timeline 0" - a failover input. The largest
// instances in an estate would be the ones silently reclassified as unhealthy.
// maxLSN is the widest an LSN renders, because the report has to fit in the worst case.
const maxLSN = "FFFFFFFF/FFFFFFFF"

func TestAFullMemberReportFitsUnderTheLimitItIsReadWith(t *testing.T) {
	report := MemberReport{
		Member:                  "pgelastic-instance-with-a-long-enough-name-3",
		Role:                    "primary",
		LSN:                     maxLSN,
		ReceivedLSN:             maxLSN,
		ReplayLSN:               maxLSN,
		SynchronousStandbyNames: "ANY 1 (pgelastic-instance-with-a-long-enough-name-2, pgelastic-instance-with-a-long-enough-name-3)",
	}
	// A name at PostgreSQL's identifier limit and counters at full width, because the report
	// has to fit in the worst case rather than the typical one.
	name := strings.Repeat("d", 63)
	for i := range MaxDatabasesPerReport {
		report.Databases = append(report.Databases, DatabaseReport{
			Name: fmt.Sprintf("%s%04d", name[:59], i), OID: 9223372036854775807,
			NumBackends: 2147483647,
			XactCommit:  9223372036854775807, XactRollback: 9223372036854775807,
			BlksRead: 9223372036854775807, BlksHit: 9223372036854775807,
			TupReturned: 9223372036854775807, TupFetched: 9223372036854775807,
			TupModified: 9223372036854775807, Deadlocks: 9223372036854775807,
			SizeBytes: 9223372036854775807,
		})
	}

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshalling a full report: %v", err)
	}
	if len(encoded) >= reportSizeLimit {
		t.Errorf("a member carrying %d databases encodes to %d bytes, at or past the %d-byte "+
			"limit it is read with; the report would be truncated and the member would read "+
			"as unreachable", MaxDatabasesPerReport, len(encoded), reportSizeLimit)
	}

	// And the limit is not so wide that it has stopped bounding anything.
	if reportSizeLimit > 4*len(encoded) {
		t.Errorf("the limit is %d bytes against a worst case of %d, which is no longer a bound",
			reportSizeLimit, len(encoded))
	}
}
