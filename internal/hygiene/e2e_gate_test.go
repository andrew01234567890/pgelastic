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
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// The e2e suites deliberately not on the merge gate, and the reason each one is not.
//
// An entry here is a decision. The absence of an entry, for a suite no pull-request job
// runs, is what this file exists to make impossible: on 2026-07-31 four PRs of backup and
// PITR work merged green against a suite that had never executed once, because adding the
// Makefile target and a nightly step reads exactly like adding coverage and is not.
var ungated = map[string]string{
	"test-e2e-migration": "two full three-node instances through logical replication, and it " +
		"needs cert-manager and a Rust proxy release build; nightly",
	"test-e2e-proxy": "two three-node instances and a fleet in front of them, on the same " +
		"prerequisites as migration; nightly",
	"test-e2e-restart": "recreates every member Pod of a real instance and switches the " +
		"primary away, so it is minutes of real restarts; nightly",
	"test-e2e-placement": "runs in no CI job at all. Decided on 2026-08-01 to record that " +
		"rather than fix it in the same change as the backup gate",
	"test-e2e-tenantdb": "runs in no CI job at all, except under E2E_HEAVY=true which " +
		"nothing sets. Recorded on 2026-08-01 rather than fixed",
	"test-e2e-coexistence": "runs in no CI job at all. Recorded on 2026-08-01 rather than fixed",
}

// A suite that no merge-blocking job runs is a suite that does not run.
//
// It reads both files rather than checking a list, because a list drifts in exactly the
// direction that hurts: somebody adds a target, adds a nightly step, and the gap is invisible
// until the feature it was supposed to cover is found broken in production.
func TestEveryE2ESuiteIsGatedOrExcusedInWriting(t *testing.T) {
	targets := e2eTargets(t)
	if len(targets) == 0 {
		t.Fatal("found no test-e2e-* targets in the Makefile, so this test is checking nothing")
	}
	gated := targetsOnTheMergeGate(t)

	for _, target := range targets {
		reason, excused := ungated[target]
		switch {
		case gated[target] && excused:
			t.Errorf("%s is listed as ungated and is in fact run by a pull-request job; "+
				"delete its entry so the list keeps meaning something", target)
		case gated[target]:
		case !excused:
			t.Errorf("no pull-request job runs %s, so nothing it asserts can block a merge. "+
				"Put it on a merge-blocking job in .github/workflows/test-e2e.yml, or add it "+
				"to `ungated` with the reason it cannot be", target)
		case strings.TrimSpace(reason) == "":
			t.Errorf("%s is excused with an empty reason; the reason is the whole point of "+
				"the list", target)
		}
	}

	for target := range ungated {
		if !slices.Contains(targets, target) {
			t.Errorf("`ungated` excuses %s, which the Makefile no longer has", target)
		}
	}
}

// The suite this whole file was written for. Stated separately so that a change deleting the
// backup job fails with the reason rather than with a generic one.
func TestTheBackupSuiteIsOnTheMergeGate(t *testing.T) {
	if !targetsOnTheMergeGate(t)["test-e2e-backup"] {
		t.Error("no pull-request job runs test-e2e-backup. It was nightly-only until " +
			"2026-08-01, in which time it never ran once, and four PRs of backup and PITR " +
			"work merged green on a suite that could not have passed")
	}
}

func e2eTargets(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile("../../Makefile")
	if err != nil {
		t.Fatalf("reading the Makefile: %v", err)
	}
	pattern := regexp.MustCompile(`(?m)^(test-e2e-[a-z0-9-]+):`)

	var targets []string
	for _, match := range pattern.FindAllStringSubmatch(string(raw), -1) {
		if !slices.Contains(targets, match[1]) {
			targets = append(targets, match[1])
		}
	}
	return targets
}

// targetsOnTheMergeGate is every e2e target reachable from a job that runs on a pull request.
func targetsOnTheMergeGate(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile("../../.github/workflows/test-e2e.yml")
	if err != nil {
		t.Fatalf("reading the e2e workflow: %v", err)
	}

	var workflow struct {
		// A bare `on:` is read as the boolean true by every YAML 1.1 parser, so the triggers
		// arrive under the key "true" rather than "on". GitHub reads the same file as the
		// trigger block regardless; this is a quirk of parsing it, not of the workflow.
		On       map[string]any `json:"on"`
		OnAsTrue map[string]any `json:"true"`
		Jobs     map[string]struct {
			If    string `json:"if"`
			Steps []struct {
				Name string `json:"name"`
				If   string `json:"if"`
				Run  string `json:"run"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatalf("parsing the e2e workflow: %v", err)
	}
	triggers := workflow.On
	if triggers == nil {
		triggers = workflow.OnAsTrue
	}
	if _, ok := triggers["pull_request"]; !ok {
		t.Fatal("the e2e workflow has no pull_request trigger, so none of it blocks a merge")
	}

	invoked := regexp.MustCompile(`\bmake\s+(test-e2e[a-z0-9-]*)`)
	reached := map[string]bool{}
	for name, job := range workflow.Jobs {
		if !runsOnPullRequest(t, name, job.If) {
			continue
		}
		for _, step := range job.Steps {
			// A step condition counts as much as a job condition. `if: github.event_name ==
			// 'push'` on the step that runs the suite leaves the job itself pull-request
			// triggered and the suite gating nothing, and reading only the job would call
			// that gated.
			if !runsOnPullRequest(t, name+"/"+step.Name, step.If) {
				continue
			}
			for _, match := range invoked.FindAllStringSubmatch(step.Run, -1) {
				for _, target := range append([]string{match[1]}, chainedBy(t, match[1])...) {
					reached[target] = true
				}
			}
		}
	}
	return reached
}

// chainedBy returns the e2e targets a target invokes unconditionally.
//
// Unconditionally is the operative word: `test-e2e` chains four more suites inside
// `ifeq ($(E2E_HEAVY),true)`, and the pull-request job passes E2E_HEAVY=false. Counting
// those as gated would excuse exactly the suites that do not run.
func chainedBy(t *testing.T, target string) []string {
	t.Helper()
	recipe := makefileRecipe(t, target)
	if conditional := strings.Index(recipe, "ifeq"); conditional >= 0 {
		recipe = recipe[:conditional]
	}

	matches := regexp.MustCompile(`\$\(MAKE\)\s+(test-e2e[a-z0-9-]*)`).
		FindAllStringSubmatch(recipe, -1)
	chained := make([]string, 0, len(matches))
	for _, match := range matches {
		chained = append(chained, match[1])
	}
	return chained
}

// runsOnPullRequest evaluates a job's `if:` for a pull_request event.
//
// It understands only the conditions this workflow actually uses, and fails on anything
// else. A checker that guessed at an expression it did not recognise would report a suite as
// gated on the strength of its own optimism, which is the failure this whole file is about.
func runsOnPullRequest(t *testing.T, job, condition string) bool {
	t.Helper()
	switch strings.TrimSpace(condition) {
	case "":
		return true
	case "github.event_name != 'schedule'":
		return true
	case "github.event_name == 'schedule' || inputs.chaos":
		return false
	// Step conditions about cancellation and failure say nothing about which event triggered
	// the run, so a step carrying one still runs on a pull request.
	case "${{ !cancelled() }}", "!cancelled()", "always()", "${{ always() }}":
		return true
	default:
		t.Fatalf("job %q carries an `if:` this check cannot evaluate (%q). Teach it that "+
			"condition rather than leaving it to guess", job, condition)
		return false
	}
}
