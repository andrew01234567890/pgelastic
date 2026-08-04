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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Reconciler wraps one reconciler in a span per pass.
//
// A decorator rather than a line inside each Reconcile, for two reasons. It cannot be
// forgotten by a controller added later - the wrap is at the single place a reconciler is
// handed to the manager - and it sees the returned error and requeue, which a span taken
// inside the function body would have to be careful to record on every path out.
//
// The span it opens is the parent of everything the pass does, so a trace of one reconcile is
// a tree rather than a list: work the reconcile delegates inherits this context and hangs off
// it. That is the whole reason to trace an operator whose interesting operations span many
// passes - the passes are what you have.
//
// It costs a couple of nil checks when no collector is configured, because the global
// provider is a no-op until Start installs one.
type Reconciler struct {
	// Kind names the resource, and becomes the span name. It is passed rather than derived
	// from the request, which carries only a namespace and a name.
	Kind string
	// Inner is the reconciler being wrapped.
	Inner reconcile.Reconciler
}

// Wrap returns a traced reconciler.
func Wrap(kind string, inner reconcile.Reconciler) reconcile.Reconciler {
	return Reconciler{Kind: kind, Inner: inner}
}

// Reconcile implements reconcile.Reconciler.
func (r Reconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	ctx, span := Tracer().Start(ctx, r.Kind+".Reconcile",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(Object(r.Kind, request.Namespace, request.Name)...))
	defer span.End()

	result, err := r.Inner.Reconcile(ctx, request)

	// Recorded even though controller-runtime logs it: a trace that showed a pass ending
	// without saying it failed would be the misleading half of two records rather than the
	// second half of one.
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	// A pass that asks to be looked at again is the ordinary shape here - almost every
	// reconcile in this tree returns a RequeueAfter - so the interesting question a trace can
	// answer is which passes did not. Only RequeueAfter is read: the boolean beside it is
	// deprecated, and this tree returns an interval everywhere it wants another pass.
	span.SetAttributes(attribute.Bool("pgelastic.requeued", result.RequeueAfter > 0))
	return result, err
}
