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
