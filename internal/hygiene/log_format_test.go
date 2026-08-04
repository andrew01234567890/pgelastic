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
	"strings"
	"testing"
)

// Four processes write logs for one product: the operator, the instance agent, the Rust proxy
// and PostgreSQL. They spent a long time disagreeing - console text at Debug from the operator,
// JSON from the agent, text from the proxy, CSV from PostgreSQL - while
// spec.observability.logFormat declared Json and was read by nothing.
//
// Nothing about that was visible from any one file, which is why it is checked here rather
// than trusted to review.
func TestEveryBinaryLogsInTheSameShape(t *testing.T) {
	t.Run("the operator is not in zap's development mode", func(t *testing.T) {
		source := read(t, "cmd/main.go")
		// Development mode decides three things at once, and all three are wrong here:
		// console encoding rather than JSON, DebugLevel rather than Info, and a stacktrace
		// on every Warn.
		if strings.Contains(source, "Development: true") {
			t.Error("cmd/main.go builds its logger in zap development mode, which emits " +
				"console text at Debug - while spec.observability.logFormat declares Json")
		}
	})

	t.Run("the instance agent is not in zap's development mode", func(t *testing.T) {
		source := read(t, "cmd/instance/main.go")
		if strings.Contains(source, "UseDevMode(true)") {
			t.Error("cmd/instance/main.go builds its logger in zap development mode")
		}
	})

	t.Run("the proxy can emit JSON at all", func(t *testing.T) {
		manifest := read(t, "crates/pgelastic-proxy/Cargo.toml")
		// Without the feature the builder has no .json() to call, so this is not a
		// preference that drifted - it is a capability the crate did not have.
		if !strings.Contains(manifest, `"json"`) {
			t.Error("tracing-subscriber is built without its json feature, so the proxy " +
				"cannot emit structured logs however it is configured")
		}
		if source := read(t, "crates/pgelastic-proxy/src/main.rs"); !strings.Contains(source, ".json()") {
			t.Error("the proxy builds a subscriber that never selects JSON")
		}
	})

	// PostgreSQL rewrites a ".log" suffix to the destination's own extension, so the FIFO the
	// agent drains has to be named for the destination it is configured with. Get these two
	// out of step and the collector writes a regular file *beside* an untouched FIFO: an
	// emptyDir filling up with logs nobody reads, and a container whose stdout stays silent.
	t.Run("the log FIFO is named for the destination PostgreSQL is given", func(t *testing.T) {
		destination := valueOf(t, read(t, "internal/instance/pgconf/gucs.go"), `"log_destination":`)
		fifo := valueOf(t, read(t, "internal/instance/provision/layout.go"), "LogFIFOName =")

		want := map[string]string{"jsonlog": ".json", "csvlog": ".csv", "stderr": ".log"}[destination]
		if want == "" {
			t.Fatalf("log_destination is %q, which this test does not know the suffix for", destination)
		}
		if !strings.HasSuffix(fifo, want) {
			t.Errorf("log_destination is %q so PostgreSQL writes %q, but the FIFO the agent "+
				"drains is %q - the collector would write past it into a regular file",
				destination, want, fifo)
		}
	})
}

// valueOf pulls the first double-quoted string from the line carrying a marker.
// The operator names the variable that carries the log format to the proxy, and the proxy
// reads it. They are a matched pair across a language boundary, which means a rename on
// either side compiles, passes both test suites, and silently falls back to the default -
// the field would go inert again exactly the way it was inert before.
func TestTheProxyReadsTheVariableTheOperatorSets(t *testing.T) {
	operator := valueOf(t, read(t, "internal/proxy/names.go"), "EnvLogFormat =")
	proxy := valueOf(t, read(t, "crates/pgelastic-proxy/src/main.rs"), "LOG_FORMAT_ENV: &str =")

	if operator != proxy {
		t.Errorf("the operator sets %q and the proxy reads %q, so the pool's logFormat reaches "+
			"nothing", operator, proxy)
	}
}

func valueOf(t *testing.T, source, marker string) string {
	t.Helper()
	for line := range strings.SplitSeq(source, "\n") {
		if !strings.Contains(line, marker) {
			continue
		}
		if _, rest, found := strings.Cut(line[strings.Index(line, marker)+len(marker):], `"`); found {
			if value, _, ok := strings.Cut(rest, `"`); ok {
				return value
			}
		}
	}
	t.Fatalf("found no quoted value on any line containing %q", marker)
	return ""
}
