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

package tracing

import (
	"testing"
)

// An operator with no collector configured must not pay for tracing, and must not fail to
// start because of it. Presence of the standard endpoint variable is the whole switch: there
// is no pgelastic-specific field to declare, which is deliberate - a field that declares a
// capability nobody wired is the failure this project keeps repeating.
func TestTracingIsOffWithNoEndpointConfigured(t *testing.T) {
	t.Setenv(EndpointEnv, "")

	shutdown, err := Start(t.Context(), "test")
	if err != nil {
		t.Fatalf("starting with no endpoint failed: %v", err)
	}
	if shutdown == nil {
		t.Fatal("no shutdown returned, so the caller would nil-panic on exit")
	}
	if err := shutdown(t.Context()); err != nil {
		t.Errorf("shutting down a disabled tracer failed: %v", err)
	}
}

// Tracer is used by packages initialised before Start runs. The global provider is a no-op
// until one is installed, so this must be safe rather than nil.
func TestATracerIsUsableBeforeAnyProviderIsInstalled(t *testing.T) {
	ctx, span := Tracer().Start(t.Context(), "before-start")
	defer span.End()

	if ctx == nil {
		t.Fatal("starting a span before the provider is installed returned no context")
	}
	span.SetAttributes(Object("PgTenantMigration", "tenants", "move-acme")...)
}

// The attribute names are the vocabulary the metrics already use, so a trace and a dashboard
// panel can be read side by side without a translation step.
func TestObjectAttributesMatchTheMetricVocabulary(t *testing.T) {
	attributes := Object("PgRestore", "tenants", "restore-acme")

	want := map[string]string{
		"pgelastic.kind":      "PgRestore",
		"pgelastic.namespace": "tenants",
		"pgelastic.name":      "restore-acme",
	}
	if len(attributes) != len(want) {
		t.Fatalf("got %d attributes, want %d", len(attributes), len(want))
	}
	for _, attribute := range attributes {
		if got := attribute.Value.AsString(); got != want[string(attribute.Key)] {
			t.Errorf("%s = %q, want %q", attribute.Key, got, want[string(attribute.Key)])
		}
	}
}
