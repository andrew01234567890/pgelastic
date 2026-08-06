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
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// perTenantLogicalBackup once published a cron schedule, a retention and a dump timeout, all
// defaulted, none of them read by anything. So `kubectl explain` described a nightly backup
// that no code takes, and an operator who enabled it found that out at the moment they needed
// a dump to restore from.
//
// The published schema is the contract, so the assertion is against the CRD rather than the Go
// type: only what the operator actually reads may appear there. A field added back here needs
// a reader added with it, and this test is where that is noticed.
func TestThePerTenantLogicalBackupSchemaDeclaresOnlyWhatIsRead(t *testing.T) {
	// The readers, and the whole of them: ConcurrentDumps in
	// internal/instance/provision/objects.go reads exactly these two.
	want := []string{"enabled", "maxConcurrentDumps"}

	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema map[string]any `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	raw := read(t, "config/crd/bases/pgelastic.io_pginstances.yaml")
	if err := yaml.Unmarshal([]byte(raw), &crd); err != nil {
		t.Fatalf("parsing the PgInstance CRD: %v", err)
	}
	if len(crd.Spec.Versions) != 1 {
		t.Fatalf("the CRD serves %d versions; this test assumes the single-version shape "+
			"that makes deleting a field safe", len(crd.Spec.Versions))
	}

	node := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	for _, step := range []string{"properties", "spec", "properties", "perTenantLogicalBackup"} {
		next, ok := node[step].(map[string]any)
		if !ok {
			t.Fatalf("the schema has no %q under perTenantLogicalBackup's path", step)
		}
		node = next
	}
	properties, ok := node["properties"].(map[string]any)
	if !ok {
		t.Fatal("perTenantLogicalBackup declares no properties at all")
	}

	got := make([]string, 0, len(properties))
	for name := range properties {
		got = append(got, name)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("perTenantLogicalBackup publishes [%s]; the operator reads [%s]. A field here "+
			"that nothing reads is a promise the product does not keep, and this one promised "+
			"a nightly dump that no code takes",
			strings.Join(got, " "), strings.Join(want, " "))
	}
}
