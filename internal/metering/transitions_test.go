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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var migrationPhases = []string{"Preflight", "Copying", "Cutover", "Completed", "Failed"}

func transitions(t *testing.T) *Transitions {
	t.Helper()
	built, err := NewTransitions(prometheus.NewPedanticRegistry())
	if err != nil {
		t.Fatalf("registering: %v", err)
	}
	return built
}

// A state-timeline panel needs the zeroes. A series that simply vanishes when a phase ends is
// indistinguishable from one being scraped late, and the gap reads as an outage rather than as
// a phase that finished.
func TestEveryPhaseIsWrittenSoTheTimelineHasNoHoles(t *testing.T) {
	metrics := transitions(t)

	metrics.Observe("tenants", "PgTenantMigration", "move-acme", "Copying", migrationPhases, time.Unix(1000, 0))

	if got := testutil.CollectAndCount(metrics.phase); got != len(migrationPhases) {
		t.Errorf("wrote %d phase series, want one per phase (%d)", got, len(migrationPhases))
	}
}

// The per-object label is affordable only because these series are ephemeral. A Forget that
// stopped being called would leave one series per phase per object for ever - exactly the
// cardinality the rest of this package refuses.
func TestATerminatedTransitionLeavesNoPerObjectSeriesBehind(t *testing.T) {
	metrics := transitions(t)
	metrics.Observe("tenants", "PgTenantMigration", "move-acme", "Copying", migrationPhases, time.Unix(1000, 0))

	metrics.Forget("tenants", "PgTenantMigration", "move-acme", "Completed", 90*time.Second, migrationPhases)

	if got := testutil.CollectAndCount(metrics.phase); got != 0 {
		t.Errorf("%d phase series survived the transition ending", got)
	}
	if got := testutil.CollectAndCount(metrics.since); got != 0 {
		t.Errorf("%d since series survived the transition ending", got)
	}
	if got := metrics.InFlight(); got != 0 {
		t.Errorf("InFlight() = %d after the only transition ended", got)
	}
}

// Forget is reached on every reconcile after a transition terminates, because the object sits
// in its terminal phase until somebody deletes it. Counting unconditionally would turn one
// migration into one per reconcile, for ever, and the rate a dashboard plots would be a
// function of the resync interval rather than of anything that happened.
func TestATerminatedTransitionIsCountedOnceHoweverOftenItIsReconciled(t *testing.T) {
	metrics := transitions(t)
	metrics.Observe("tenants", "PgTenantMigration", "move-acme", "Cutover", migrationPhases, time.Unix(1000, 0))

	for range 5 {
		metrics.Forget("tenants", "PgTenantMigration", "move-acme", "Completed", time.Minute, migrationPhases)
	}

	if got := testutil.ToFloat64(metrics.total.WithLabelValues("tenants", "PgTenantMigration", "Completed")); got != 1 {
		t.Errorf("one migration was counted %v times", got)
	}
}

// The timestamp is what an operator reads to ask "how long has this been stuck?". Rewriting it
// on a reconcile that saw no change would reset that clock and make a stalled phase look fresh.
func TestReobservingTheSamePhaseDoesNotResetItsClock(t *testing.T) {
	metrics := transitions(t)
	metrics.Observe("tenants", "PgRestore", "put-back", "Recovering", migrationPhases, time.Unix(1000, 0))
	metrics.Observe("tenants", "PgRestore", "put-back", "Recovering", migrationPhases, time.Unix(9999, 0))

	if got := testutil.ToFloat64(metrics.since.WithLabelValues("tenants", "PgRestore", "put-back")); got != 1000 {
		t.Errorf("the phase clock was reset to %v by a reconcile that saw no change", got)
	}
}
