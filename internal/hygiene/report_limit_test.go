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

package hygiene

import (
	"fmt"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// The member report is sized against the most databases a member may carry, and that number
// lives in a CRD validation marker Go cannot read. Raising the marker without raising the
// constant would put the largest instances back over the decode limit - where a truncated
// report is not a metrics gap but a member that reads as unreachable, which is a failover
// input.
func TestTheReportLimitKnowsHowManyDatabasesAreAllowed(t *testing.T) {
	source := read(t, "api/v1alpha1/pgelasticclass_types.go")

	marker := fmt.Sprintf("// +kubebuilder:validation:Maximum=%d", provision.MaxDatabasesPerReport)
	density := source[strings.Index(source, "maxTenantsPerInstance bounds"):]
	if cut := strings.Index(density, "MaxTenantsPerInstance"); cut > 0 {
		density = density[:cut]
	}
	if !strings.Contains(density, marker) {
		t.Errorf("maxTenantsPerInstance no longer caps at %d, which is what the member report "+
			"is sized for; provision.MaxDatabasesPerReport and reportSizeLimit have to move with it",
			provision.MaxDatabasesPerReport)
	}
}
