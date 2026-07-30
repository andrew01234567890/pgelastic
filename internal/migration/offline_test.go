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

package migration

import (
	"strings"
	"testing"
)

// Ownership and privileges have to survive both commands. The flag is accepted by pg_dump and
// pg_restore alike, so stripping on either end loses them - which is why removing it from only
// one would have changed nothing a test could see.
func TestNeitherOfflineCommandStripsOwnershipOrPrivileges(t *testing.T) {
	for name, command := range map[string]string{
		"dump":    offlineDumpCommand(testPlan(), DefaultDumpJobs),
		"restore": offlineRestoreCommand(testPlan(), DefaultDumpJobs),
	} {
		for _, unwanted := range []string{"--no-owner", "--no-privileges"} {
			if strings.Contains(command, unwanted) {
				t.Fatalf("the offline %s still passes %s, so the tenant's objects arrive owned "+
					"by postgres with no grants: %s", name, unwanted, command)
			}
		}
	}
}

// TestTheOfflineRestoreDropsWhateverAPreviousAttemptLeft is the schema copy's defect on the
// path that carries data rather than schema. A restore that died part-way leaves objects
// behind, and a second attempt that did not drop them first could only ever fail on them.
func TestTheOfflineRestoreDropsWhateverAPreviousAttemptLeft(t *testing.T) {
	command := offlineRestoreCommand(testPlan(), DefaultDumpJobs)
	for _, want := range []string{"--clean", "--if-exists", "--exit-on-error"} {
		if !strings.Contains(command, want) {
			t.Fatalf("the offline restore cannot survive its own retry without %s: %s", want, command)
		}
	}
}
