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

// Package tracing wires OpenTelemetry trace export for the operator.
//
// The long multi-phase operations are what a trace actually illuminates here - a tenant
// migration across its eleven phases, a restore across its six - because a span per phase is
// a faithful representation of something the objects already record rather than an invention.
// It is the same data the transitions metrics put on a dashboard, viewed as a waterfall
// instead of a timeline.
//
// What this package deliberately does not attempt is following a span *into* PostgreSQL.
// PostgreSQL has no notion of a trace context, and the two routes to giving it one - stamping
// the context into application_name, or the pg_tracing extension, which is a
// shared_preload_libraries change and therefore a restart - are a decision rather than
// plumbing. Everything here is the plumbing.
package tracing

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// EndpointEnv is the standard OTLP variable. Its absence is what switches tracing off, which
// is deliberate: there is no pgelastic-specific flag to learn, and an operator that already
// runs a collector gets traces by pointing at it the same way everything else does.
const EndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"

// TracesEndpointEnv is the signal-specific override, and it has to be read here as well as by
// the exporter.
//
// The exporter honours it on its own, so a deployment that sends traces and metrics to
// different collectors sets only this one - and the gate below, reading only the general
// variable, would then decide tracing was switched off and never build the exporter at all.
// Traces would silently not appear, with the configuration that asks for them present and
// correct.
const TracesEndpointEnv = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"

// ServiceName names the operator in the collector.
const ServiceName = "pgelastic-operator"

// Tracer is the operator's tracer. It is safe to use before Start: the global provider is a
// no-op until one is installed, so a span taken by a package initialised earlier costs a
// couple of nil checks rather than panicking.
func Tracer() trace.Tracer {
	return otel.Tracer("github.com/andrew01234567890/pgelastic")
}

// Shutdown flushes whatever is buffered. Returning it rather than registering an atexit hook
// keeps the ownership where the process lifetime is known.
type Shutdown func(context.Context) error

// Start installs a trace provider when an OTLP endpoint is configured, and does nothing when
// one is not.
//
// Enabled by the presence of the endpoint rather than by a flag, because the failure this
// avoids is the one this project keeps hitting: a field that declares a capability nobody
// wired. There is nothing to declare here - either traces have somewhere to go or they do
// not, and the answer is the same variable every other OTLP producer reads.
//
// A version is required rather than defaulted. A trace whose service.version is "unknown" is
// the trace you cannot correlate with a rollout, which is the main thing you want it for.
func Start(ctx context.Context, version string) (Shutdown, error) {
	// Read for the gate and for the error message only. Which endpoint the exporter actually
	// uses, and how the two variables take precedence, is the exporter's own business.
	endpoint := cmp.Or(os.Getenv(TracesEndpointEnv), os.Getenv(EndpointEnv))
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("building the OTLP trace exporter for %s: %w", endpoint, err)
	}

	// Schemaless, and that is a correctness requirement rather than a shortcut.
	// resource.Merge refuses to merge two resources carrying *different* schema URLs, and
	// resource.Default() carries whichever schema the SDK was built against. Naming a schema
	// here pins this file to that choice, so the two disagree the moment the SDK is upgraded -
	// Merge returns ErrSchemaURLConflict, Start returns an error, and main exits. The operator
	// would then CrashLoopBackOff for exactly as long as somebody had OTEL_EXPORTER_OTLP_ENDPOINT
	// set, which is the one configuration where tracing was wanted.
	//
	// Pinning a matching version would fix it once and reopen it on the next upgrade. A
	// resource with no schema URL merges with any of them, which is what this needs: the
	// attribute keys still come from semconv, and only the version claim is dropped.
	attributes, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(ServiceName),
		semconv.ServiceVersion(version),
	))
	if err != nil {
		return nil, fmt.Errorf("describing this process to the collector: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(attributes),
	)
	otel.SetTracerProvider(provider)
	// Both formats, because the collector on the other end is not this project's to choose
	// and a context that does not propagate makes every span a root.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	return func(stopping context.Context) error {
		// Bounded rather than inheriting the caller's deadline, which on shutdown is usually
		// already cancelled - and a cancelled context makes the flush a no-op, losing exactly
		// the spans describing whatever went wrong on the way out.
		flushing, cancel := context.WithTimeout(context.WithoutCancel(stopping), 5*time.Second)
		defer cancel()
		return provider.Shutdown(flushing)
	}, nil
}

// Object names the thing a span is about, in the vocabulary the metrics already use so that a
// trace and a dashboard panel can be put side by side without a translation step.
func Object(kind, namespace, name string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("pgelastic.kind", kind),
		attribute.String("pgelastic.namespace", namespace),
		attribute.String("pgelastic.name", name),
	}
}
