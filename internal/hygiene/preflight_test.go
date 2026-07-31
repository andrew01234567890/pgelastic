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

// Package hygiene holds checks about the repository itself rather than about what it builds.
package hygiene

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The commands CI runs that a local check has no other way to discover.
//
// This repository is half Rust and half Go, and the two halves are checked by different tools
// with different entry points. `make test` runs the Go tests and nothing else; `cargo test`
// runs the Rust tests and nothing else. There is no arrangement of habit under which a person
// reliably remembers both plus the linter, which is why `make preflight` exists and why this
// test exists to keep it honest.
var mustRunLocally = []string{
	"cargo fmt --all --check",
	"cargo clippy --workspace --all-targets --all-features",
	"cargo test --workspace --all-features",
}

func preflightRecipe(t *testing.T) string {
	t.Helper()
	return makefileRecipe(t, "preflight")
}

// makefileRecipe returns one target's recipe: the tab-indented block after the target line,
// ending at the first line that is neither indented nor blank.
func makefileRecipe(t *testing.T, target string) string {
	t.Helper()
	raw, err := os.ReadFile("../../Makefile")
	if err != nil {
		t.Fatalf("reading the Makefile: %v", err)
	}

	start := strings.Index(string(raw), "\n"+target+":")
	if start < 0 {
		t.Fatalf("the Makefile has no %s target", target)
	}
	rest := string(raw)[start+1:]
	var recipe []string
	for line := range strings.SplitSeq(rest, "\n") {
		if strings.HasPrefix(line, target+":") {
			continue
		}
		if line != "" && !strings.HasPrefix(line, "\t") {
			break
		}
		recipe = append(recipe, strings.TrimPrefix(line, "\t"))
	}
	return strings.Join(recipe, "\n")
}

// Every command CI runs has to be reachable from one local command.
//
// The failure this prevents is specific and has happened repeatedly: a change is made, the
// half of the test suite belonging to the language that changed is run, it passes, and CI
// then fails on the other half or on the linter. A green local run that does not mean a green
// CI is worse than no local run, because it is trusted.
func TestPreflightRunsWhatCiRuns(t *testing.T) {
	recipe := preflightRecipe(t)

	for _, command := range mustRunLocally {
		if !strings.Contains(recipe, command) {
			t.Errorf("make preflight does not run %q, so a local pass does not imply a CI pass",
				command)
		}
	}

	// The Go half is delegated rather than inlined, so that preflight cannot drift from the
	// targets CI itself invokes.
	for _, delegated := range []string{"$(MAKE) test", "$(MAKE) lint"} {
		if !strings.Contains(recipe, delegated) {
			t.Errorf("make preflight does not run %q", delegated)
		}
	}
}

// If CI grows a cargo step, preflight has to grow it too.
//
// Without this the drift is silent and one-directional: CI gets stricter, the local command
// stays where it was, and the gap is only discovered by a red build. Reading the workflow
// rather than a second hand-maintained list is the point -- a list would drift the same way.
func TestNoCargoStepInCiIsMissingFromPreflight(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/rust.yml")
	if err != nil {
		t.Fatalf("reading the Rust workflow: %v", err)
	}

	// `run: cargo ...`, single line, which is how every step in that workflow is written.
	steps := regexp.MustCompile(`(?m)^\s*run:\s*(cargo\s+[^\n]+?)\s*$`).FindAllStringSubmatch(string(raw), -1)
	if len(steps) == 0 {
		t.Fatal("found no cargo steps in the Rust workflow, so this test is not checking anything")
	}

	recipe := preflightRecipe(t)
	for _, step := range steps {
		command := step[1]
		// Fuzzing is a scheduled smoke run with its own toolchain and corpus; it is not part of
		// what a person runs before pushing, and demanding it here would make preflight so slow
		// that it stopped being run at all.
		if strings.Contains(command, "fuzz") {
			continue
		}
		if !strings.Contains(recipe, command) {
			t.Errorf("CI runs %q and make preflight does not; add it, or the next person's "+
				"clean local run will still be a red build", command)
		}
	}
}
