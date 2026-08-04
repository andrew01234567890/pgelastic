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

package controller

import (
	"context"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
)

// eventTimeoutNowEnforced is emitted once per bound, on the upgrade that starts enforcing it.
const eventTimeoutNowEnforced = "PoolTimeoutNowEnforced"

const actionEnforce = "Enforce"

// enforcedBounds are the timeouts whose stored default becomes real the moment the operator
// that reads them is deployed, in the order they are reported.
var enforcedBounds = []struct {
	key   string
	field string
	unit  string
	costs string
}{
	{
		key:   "queryDeadlineSeconds",
		field: "spec.timeouts.query",
		unit:  "s",
		costs: "a statement running longer than this is cancelled and its connection closed",
	},
	{
		key:   "clientIdleInTransactionSeconds",
		field: "spec.timeouts.clientIdleInTransaction",
		unit:  "s",
		costs: "a transaction left open and idle for longer than this has its connection closed",
	},
	{
		key:   "maxPinnedPercent",
		field: "spec.pooling.maxPinnedFractionPercent",
		unit:  "%",
		costs: "a client whose session state would pin a backend past this share of the " +
			"budget is closed rather than given a shared link",
	},
}

// warnNewlyEnforcedTimeouts reports a bound that this operator will start enforcing and the
// one before it did not.
//
// These fields were documented, defaulted and stored on every pool long before anything read
// them, so deploying the operator that reads them turns them on across an estate with no spec
// change by anybody. An application with a legitimately long statement, or long think-time
// inside a transaction, starts being cut off — and the only signal would otherwise be the
// application's own errors.
//
// The previous document is the evidence, because it is the only durable record of what the
// last operator to run here was capable of. A document with no such key was written by an
// operator that could not enforce the bound; one that has the key already carries whatever
// value the pool asked for, and enforcing it is not news.
//
// Best-effort by design: this is a warning about a behaviour change, and failing to publish a
// warning must not stop the change being reconciled. A pool whose Secret cannot be read is a
// pool whose Secret is about to be written anyway.
func (r *PgElasticPoolReconciler) warnNewlyEnforcedTimeouts(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	document string,
) {
	if r.Recorder == nil {
		return
	}
	// Every one of these bounds lives in the transaction-mode relay. A session-mode pool hands
	// each client its own backend for life and runs none of them, so telling its operator that
	// their statements are now cancelled at 120s would send them auditing for a limit that does
	// not exist. The rendered document still carries the keys, so if the pool is later switched
	// to transaction pooling this stays silent - which is the right trade: a mode change is a
	// deliberate act, and an upgrade is not.
	if pool.Spec.Pooling != nil && pool.Spec.Pooling.Mode == pgelasticv1alpha1.PoolModeSession {
		return
	}
	previous, ok := r.previousProxyDocument(ctx, pool)
	if !ok {
		// No previous document means a pool being created now, which nobody can be surprised
		// by: it has never run under an operator that did not enforce these.
		return
	}
	for _, bound := range enforcedBounds {
		if _, existed := renderedSeconds(previous, bound.key); existed {
			continue
		}
		seconds, present := renderedSeconds(document, bound.key)
		if !present || seconds == 0 {
			continue
		}
		r.Recorder.Eventf(pool, nil, corev1.EventTypeWarning, eventTimeoutNowEnforced,
			actionEnforce,
			"%s is now enforced at %d%s by the proxy: %s. It was stored but unread before "+
				"this upgrade. Set it to 0 to leave it unbounded.",
			bound.field, seconds, bound.unit, bound.costs)
	}
}

// previousProxyDocument returns the configuration this pool's fleet is running now, and
// whether there was one to read.
func (r *PgElasticPoolReconciler) previousProxyDocument(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) (string, bool) {
	var secret corev1.Secret
	key := types.NamespacedName{
		Namespace: pool.Namespace,
		Name:      proxy.ConfigSecretName(pool.Name),
	}
	if err := r.Get(ctx, key, &secret); err != nil {
		if !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).V(1).Info("could not read the current proxy configuration to compare "+
				"against", "error", err)
		}
		return "", false
	}
	document, ok := secret.Data[proxy.ConfigKey]
	return string(document), ok
}

// renderedSeconds reads one top-level integer key out of a rendered document.
//
// A scan rather than a TOML parse: the operator writes this document itself, one
// "key = value" line at a time, and taking a parser dependency to read back two integers it
// wrote would be a larger surface than the question needs. The key is matched whole so
// queryDeadlineSeconds is not found inside a longer name.
func renderedSeconds(document, key string) (int64, bool) {
	for line := range strings.SplitSeq(document, "\n") {
		name, value, found := strings.Cut(line, "=")
		if !found || strings.TrimSpace(name) != key {
			continue
		}
		seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return 0, false
		}
		return seconds, true
	}
	return 0, false
}
