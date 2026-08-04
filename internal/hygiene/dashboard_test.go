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
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// A dashboard panel querying a metric nobody emits renders as "No data", which is
// indistinguishable from a quiet system. This project has shipped five inert
// spec.observability fields and a metrics substrate with no producer; a dashboard is the
// easiest place yet to promise something the tree does not deliver, because nothing compiles
// it and nobody runs it.
//
// So the dashboard is checked against the exposition rather than trusted to review. It reads
// both directions: a metric that is queried must be emitted, and the check is cheap enough to
// keep running as either side changes.
func TestEveryDashboardPanelQueriesAMetricThatExists(t *testing.T) {
	emitted := metricsEmittedByTheTree(t)

	for _, source := range []struct{ what, path string }{
		{"the dashboard", filepath.Join("config", "grafana", "dashboard.json")},
		{"an alert", filepath.Join("config", "grafana", "alerts.yaml")},
	} {
		queried := metricsQueriedIn(t, source.path)
		if len(queried) == 0 {
			t.Errorf("no metric names were extracted from %s, so this proves nothing about it",
				source.path)
			continue
		}
		for _, name := range queried {
			if !slices.Contains(emitted, name) {
				t.Errorf("%s queries %s, which nothing in this tree emits - it would render "+
					"No data, which reads exactly like a quiet system", source.what, name)
			}
		}
	}
}

var (
	// A metric reference in PromQL: the name, optionally followed by a label selector. Names
	// here are all pgelastic_-prefixed, which keeps PromQL functions and label names out.
	metricReference = regexp.MustCompile(`pgelastic_[a-z0-9_]+`)
	// A registered metric name in Go, and in the Rust proxy's exposition.
	goMetricName   = regexp.MustCompile(`Name:\s*"(pgelastic_[a-z0-9_]+)"`)
	rustMetricName = regexp.MustCompile(`"(pgelastic_proxy_[a-z0-9_]+)"`)

	// Histogram and summary families are registered under a base name and scraped as three
	// derived series. A dashboard querying the _bucket of a histogram that exists is correct.
	derivedSuffixes = []string{"_bucket", "_sum", "_count"}
)

func metricsQueriedIn(t *testing.T, path string) []string {
	t.Helper()
	raw := read(t, path)

	// Parsed rather than grepped, so that a dashboard which is not valid JSON fails here
	// rather than being silently uploaded and rejected by Grafana.
	if strings.HasSuffix(path, ".json") {
		var dashboard any
		if err := json.Unmarshal([]byte(raw), &dashboard); err != nil {
			t.Fatalf("the dashboard is not valid JSON: %v", err)
		}
	}

	found := map[string]struct{}{}
	for _, name := range metricReference.FindAllString(raw, -1) {
		found[name] = struct{}{}
	}
	return sorted(found)
}

func metricsEmittedByTheTree(t *testing.T) []string {
	t.Helper()
	emitted := map[string]struct{}{}

	for _, dir := range []string{"internal", "api", "cmd"} {
		walkGo(t, dir, func(source string) {
			for _, match := range goMetricName.FindAllStringSubmatch(source, -1) {
				emitted[match[1]] = struct{}{}
			}
		})
	}
	proxy := read(t, filepath.Join("crates", "pgelastic-proxy", "src", "metrics.rs"))
	for _, match := range rustMetricName.FindAllStringSubmatch(proxy, -1) {
		emitted[match[1]] = struct{}{}
	}

	// Every derived series of a family that exists is itself queryable.
	for name := range maps(emitted) {
		for _, suffix := range derivedSuffixes {
			emitted[name+suffix] = struct{}{}
		}
	}
	return sorted(emitted)
}

func maps(from map[string]struct{}) map[string]struct{} {
	copied := make(map[string]struct{}, len(from))
	for name := range from {
		copied[name] = struct{}{}
	}
	return copied
}

func walkGo(t *testing.T, dir string, visit func(string)) {
	t.Helper()
	root := filepath.Join("..", "..", dir)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		source, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		visit(string(source))
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
}

func sorted(from map[string]struct{}) []string {
	names := make([]string, 0, len(from))
	for name := range from {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
