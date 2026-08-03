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

package metering

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Transition labels. The object is named because a transition is a thing that happens to one
// object and an operator watching one wants to find it, not a rate.
const (
	labelKind  = "kind"
	labelName  = "name"
	labelPhase = "phase"
	labelFrom  = "from"
	labelTo    = "to"
)

// Transitions is what is moving in a pool right now: which object, of what kind, in which
// phase, and - for the two that move a database between instances - from where to where.
//
// This is the one part of the exposition that carries a per-object label, and it is affordable
// for a reason the tenant labels are not. A transition is rare, bounded and *ephemeral*: one
// migration per tenant at a time, one restore per request, and every one of them reaches a
// terminal phase and stops. The series are dropped when it does, so the steady state of a pool
// that is not moving anything is zero series rather than one per tenant for ever.
//
// Dropping them is therefore not tidiness - it is the whole reason the label is allowed. A
// Forget that stopped being called would turn this into exactly the cardinality the rest of
// this package refuses.
type Transitions struct {
	phase    *prometheus.GaugeVec
	since    *prometheus.GaugeVec
	total    *prometheus.CounterVec
	duration *prometheus.HistogramVec

	mu    sync.Mutex
	known map[transitionKey]string
}

type transitionKey struct {
	kind      string
	namespace string
	name      string
}

// NewTransitions builds and registers the transition vectors.
func NewTransitions(registerer prometheus.Registerer) (*Transitions, error) {
	transitions := &Transitions{
		phase: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgelastic_transition_phase",
			Help: "1 for the phase an in-flight transition is in, 0 for the phases it is " +
				"not. One series per phase so a dashboard can draw a state timeline; the " +
				"whole set is dropped when the transition reaches a terminal phase.",
		}, []string{labelNamespace, labelKind, labelName, labelPhase}),

		since: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgelastic_transition_phase_since_seconds",
			Help: "Unix time at which the current phase was entered. A timestamp rather " +
				"than an age, so the value does not depend on when it was scraped.",
		}, []string{labelNamespace, labelKind, labelName}),

		total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgelastic_transitions_total",
			Help: "Transitions that have reached a terminal phase, by outcome. Survives " +
				"the per-object series being dropped, which is what makes a rate possible.",
		}, []string{labelNamespace, labelKind, labelTo}),

		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "pgelastic_transition_duration_seconds",
			Help: "How long a transition took, observed once when it terminates.",
			// A tenant migration is minutes to hours and a backup can be longer; the top
			// bucket is deliberately past anything anybody would wait for, because a
			// migration that has run over is exactly the one worth seeing.
			Buckets: []float64{10, 30, 60, 300, 900, 1800, 3600, 7200, 21600},
		}, []string{labelNamespace, labelKind, labelTo}),

		known: map[transitionKey]string{},
	}
	for _, collector := range []prometheus.Collector{
		transitions.phase, transitions.since, transitions.total, transitions.duration,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, err
		}
	}
	return transitions, nil
}

// Observe records that one object is in one phase, out of the phases its kind can be in.
//
// Every phase of the kind is written, not just the current one, because a state-timeline panel
// needs the zeroes: a series that simply vanishes is indistinguishable from one that is being
// scraped late, and the gap reads as an outage rather than as a phase that ended.
func (t *Transitions) Observe(namespace, kind, name, phase string, phases []string, entered time.Time) {
	if t == nil {
		return
	}
	key := transitionKey{kind: kind, namespace: namespace, name: name}

	t.mu.Lock()
	previous, seen := t.known[key]
	t.known[key] = phase
	t.mu.Unlock()

	for _, candidate := range phases {
		value := 0.0
		if candidate == phase {
			value = 1
		}
		t.phase.WithLabelValues(namespace, kind, name, candidate).Set(value)
	}
	// Only on a change, so a reconcile that observes the same phase again does not keep
	// resetting the clock an operator is reading to ask "how long has this been stuck?".
	if !seen || previous != phase {
		t.since.WithLabelValues(namespace, kind, name).Set(float64(entered.Unix()))
	}
}

// Forget drops one object's per-object series and records how it ended.
//
// Called when a transition reaches a terminal phase or the object goes away. The counter and
// the histogram deliberately outlive the gauges: a rate of migrations by outcome is what a
// dashboard actually plots, and it cannot be computed from series that disappear.
func (t *Transitions) Forget(namespace, kind, name, outcome string, took time.Duration, phases []string) {
	if t == nil {
		return
	}
	key := transitionKey{kind: kind, namespace: namespace, name: name}

	t.mu.Lock()
	_, seen := t.known[key]
	delete(t.known, key)
	t.mu.Unlock()

	for _, candidate := range phases {
		t.phase.DeleteLabelValues(namespace, kind, name, candidate)
	}
	t.since.DeleteLabelValues(namespace, kind, name)

	// Counted once. Forget is reached on every reconcile after a transition terminates - the
	// object sits in its terminal phase until somebody deletes it - so counting
	// unconditionally would turn one migration into one per reconcile for ever.
	if !seen {
		return
	}
	t.total.WithLabelValues(namespace, kind, outcome).Inc()
	if took > 0 {
		t.duration.WithLabelValues(namespace, kind, outcome).Observe(took.Seconds())
	}
}

// InFlight is how many objects currently hold per-object series, so the cardinality this
// package allows itself is observable rather than assumed.
func (t *Transitions) InFlight() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.known)
}
