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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/andrew01234567890/pgelastic/internal/metering"
)

// Kinds that transition, spelled once. These are label values on a shared metric rather
// than a metric per kind, so a dashboard can draw everything a pool is moving in one panel
// and still filter down to one sort of move.
const (
	kindMigration = "PgTenantMigration"
	kindRestore   = "PgRestore"
	kindBackup    = "PgBackup"
)

// transition is one object's move, as the exporter needs to see it. It is a struct because
// the call takes nine values and a positional list of that length is how a from and a to end
// up the wrong way round.
type transition struct {
	Namespace string
	Kind      string
	Name      string

	// Previous is the phase the last reconcile left behind and Current is the one this
	// reconcile decided. Both are needed: which of the two is terminal is what says whether
	// this pass is the arrival, and only the arrival may count.
	Previous string
	Current  string
	Phases   []string

	// From and To are where the object is being moved between, when that means anything for
	// the kind. Empty for a backup, which does not go anywhere.
	From string
	To   string

	// Took is how long the whole transition ran, and is only read on arrival. Zero when the
	// kind does not record enough to say, which the histogram treats as "do not observe"
	// rather than as an instant transition.
	Took time.Duration
}

// recordTransition reports where one object has got to and retires it when it arrives.
//
// The ordering is the part worth reading. Arriving at a terminal phase observes and then
// immediately forgets, in that order, because Forget counts only what it has already seen -
// so a transition that fails on its very first pass is still counted, and one that sits in a
// terminal phase being reconciled for a week is counted once. An object already terminal
// when this process first saw it is neither observed nor forgotten: an operator restart must
// not re-count history, nor resurrect series for something that finished last Tuesday.
//
// Every kind goes through here rather than repeating that reasoning, because the failure it
// prevents - a rate panel showing migrations on an idle pool - is invisible in review.
func recordTransition(move transition, terminal func(string) bool, now time.Time) {
	if terminal(move.Previous) {
		return
	}
	transitions.Observe(move.Namespace, move.Kind, move.Name, move.Current, move.Phases, now)
	transitions.Route(move.Namespace, move.Kind, move.Name, move.From, move.To)
	if !terminal(move.Current) {
		return
	}
	transitions.Forget(move.Namespace, move.Kind, move.Name, move.Current, move.Took, move.Phases)
}

// transitions is the operator's one exporter for everything currently moving in a pool.
//
// It is a package-level singleton because a Prometheus registry refuses a duplicate metric
// name: an instance per reconciler would fail at the second controller to register rather
// than at compile time. The panic matches the MustRegister the rest of this package's
// metrics use - a metric that cannot be registered is a programming error found at start-up.
var transitions = mustExportTransitions()

func mustExportTransitions() *metering.Transitions {
	exported, err := metering.NewTransitions(metrics.Registry)
	if err != nil {
		panic(err)
	}
	return exported
}
