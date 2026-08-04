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
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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

// The decorator is what stops a controller added later being the one that is not traced, so
// what it records is asserted rather than assumed - including on the error path, where a span
// that ended without saying it failed would be the misleading half of the record.
func TestAReconcilePassIsOneSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })

	failing := errors.New("the instance said no")
	traced := Wrap("PgInstance", reconcile.Func(
		func(_ context.Context, _ ctrl.Request) (ctrl.Result, error) {
			return ctrl.Result{RequeueAfter: time.Minute}, failing
		}))

	_, err := traced.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "pg"}})
	if !errors.Is(err, failing) {
		t.Fatalf("the decorator swallowed the reconciler's error: %v", err)
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("one pass produced %d spans, want exactly one", len(spans))
	}
	span := spans[0]
	if span.Name() != "PgInstance.Reconcile" {
		t.Errorf("span name is %q", span.Name())
	}
	if span.Status().Code != codes.Error {
		t.Errorf("a pass that returned an error is recorded as %s", span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Error("the error was not recorded on the span")
	}
	var requeued, named bool
	for _, attribute := range span.Attributes() {
		switch {
		case string(attribute.Key) == "pgelastic.requeued":
			requeued = attribute.Value.AsBool()
		case attribute.Value.AsString() == "pg":
			named = true
		}
	}
	if !requeued {
		t.Error("a pass returning RequeueAfter is not recorded as requeued")
	}
	if !named {
		t.Error("the span does not name the object the pass was about")
	}
}
