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
	"os"
	"path/filepath"
	"testing"
)

// spec.postgresVersion and the image the postmaster comes from are set independently, and
// nothing related them: the version is a property of the instance and the image is an
// operator-global environment variable. A 19 image under the CRD's default of "18" rendered
// 18's literals onto a 19 postmaster - max_locks_per_transaction alone differs by a factor of
// two - on a field that is immutable and so cannot be corrected afterwards.
func TestABootstrapRefusesADataDirectoryFromAnotherMajor(t *testing.T) {
	dataDir := t.TempDir()
	write := func(t *testing.T, version string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte(version), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	optionsFor := func(major int) Options {
		options := Options{DataDir: dataDir}
		options.Config.Postgres.Major = major
		return options
	}

	write(t, "19\n")
	if err := checkMajorMatchesTheData(optionsFor(18)); err == nil {
		t.Error("a 19 data directory was accepted for a configuration rendered for 18")
	}
	if err := checkMajorMatchesTheData(optionsFor(19)); err != nil {
		t.Errorf("a 19 data directory was refused for a 19 configuration: %v", err)
	}

	// An unset version is the tree's default rather than zero, so an 18 directory agrees
	// with it - which is every instance that exists today.
	write(t, "18\n")
	if err := checkMajorMatchesTheData(optionsFor(0)); err != nil {
		t.Errorf("an 18 data directory was refused for an unset version: %v", err)
	}

	// A first boot has no PG_VERSION at all, and refusing there would refuse every new
	// member: initdb has not run yet and is about to create the file from the image.
	if err := os.Remove(filepath.Join(dataDir, "PG_VERSION")); err != nil {
		t.Fatal(err)
	}
	if err := checkMajorMatchesTheData(optionsFor(19)); err != nil {
		t.Errorf("a data directory that does not exist yet was refused: %v", err)
	}
}
