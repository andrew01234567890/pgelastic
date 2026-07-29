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
	"slices"
	"strings"
)

// AnnotationMaintenance names the members a rolling operation is about to disrupt, as a
// comma-separated list of Pod names.
//
// It is an annotation rather than a status field because it is an intent the control plane
// publishes about itself, not an observation of PostgreSQL, and because it has to be
// readable by two processes that do not talk to each other: the operator, which decides who
// is next, and the member's own agent, which has to know that the shutdown it is about to be
// asked for was chosen rather than forced. That distinction is the difference between a
// clean stop the next start rewinds from and an immediate stop the next start crash-recovers
// from, and no other signal carries it - the primary epoch bumps identically either way.
//
// An empty or absent annotation means nothing is being disrupted, which is the steady state.
const AnnotationMaintenance = "pgelastic.io/maintenance-members"

// MaintenanceMembers reads the members a rolling operation intends to disrupt.
//
// Unparseable input yields no members, so a typo costs a switchover that does not happen
// rather than one that happens to the wrong member.
func MaintenanceMembers(annotations map[string]string) []string {
	raw := annotations[AnnotationMaintenance]
	if raw == "" {
		return nil
	}
	var members []string
	for field := range strings.SplitSeq(raw, ",") {
		if trimmed := strings.TrimSpace(field); trimmed != "" && !slices.Contains(members, trimmed) {
			members = append(members, trimmed)
		}
	}
	slices.Sort(members)
	return members
}

// UnderMaintenance reports whether one member is named in the annotation.
func UnderMaintenance(annotations map[string]string, member string) bool {
	return slices.Contains(MaintenanceMembers(annotations), member)
}
