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
	"maps"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// acmeRoute is the move every test here reports: off gp-old, onto gp-new.
var acmeRoute = migration.Plan{
	Source: migration.Endpoint{Namespace: metricNamespace, Instance: "gp-old"},
	Target: migration.Endpoint{Namespace: metricNamespace, Instance: "gp-new"},
}

const (
	phaseSeries     = "pgelastic_transition_phase"
	routeSeries     = "pgelastic_transition_route"
	totalSeries     = "pgelastic_transitions_total"
	metricNamespace = "tenants"

	// The metering package owns these label names. A test reading a scrape has to spell
	// them the same way a dashboard query would.
	labelName = "name"
	labelFrom = "from"
	labelTo   = "to"
)

func migrationStatus(phase pgelasticv1alpha1.TenantMigrationPhase) *pgelasticv1alpha1.PgTenantMigrationStatus {
	return &pgelasticv1alpha1.PgTenantMigrationStatus{
		Phase:       phase,
		StartedAt:   ptr.To(metav1.NewTime(time.Unix(1000, 0))),
		CompletedAt: ptr.To(metav1.NewTime(time.Unix(1090, 0))),
	}
}

func gathered(t *testing.T, name string) int {
	t.Helper()
	count, err := testutil.GatherAndCount(metrics.Registry, name)
	if err != nil {
		t.Fatalf("gathering %s: %v", name, err)
	}
	return count
}

// outcomeTotalFor reads one counter out of the registry by its labels. The Transitions vectors
// are the metering package's own, so a test in this package can only reach them the way a
// scrape does.
func outcomeTotalFor(t *testing.T, kind, outcome string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gathering: %v", err)
	}
	want := map[string]string{
		labelNamespace: metricNamespace,
		labelKind:      kind,
		"to":           outcome,
	}
	for _, family := range families {
		if family.GetName() != totalSeries {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if maps.Equal(labels, want) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// The per-object series carry the migration's own name, which is the label the rest of the
// metering package refuses everywhere else. It is affordable here only because a migration
// ends: one that finished last Tuesday must not still be occupying series today.
//
// The metric this replaced never deleted anything, so every migration a pool had ever run
// left its phases behind for the lifetime of the process.
func TestAFinishedMigrationStopsOccupyingSeries(t *testing.T) {
	const namespace, name = metricNamespace, "move-acme-retire"

	recordMigrationPhase(namespace, name,
		pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
		migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseCopying),
		acmeRoute, time.Unix(1030, 0))

	running := gathered(t, phaseSeries)
	if running < len(migrationPhaseNames) {
		t.Fatalf("a running migration holds %d series, want at least one per phase (%d)",
			running, len(migrationPhaseNames))
	}

	recordMigrationPhase(namespace, name,
		pgelasticv1alpha1.TenantMigrationPhaseCutover,
		migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseCompleted),
		acmeRoute, time.Unix(1090, 0))

	if finished := gathered(t, phaseSeries); finished != running-len(migrationPhaseNames) {
		t.Errorf("a completed migration left %d series behind, want its own %d dropped (was %d)",
			finished, len(migrationPhaseNames), running)
	}
}

// A terminal migration is reconciled for as long as it exists - it sits in Completed until
// somebody deletes it. Counting on every one of those passes would turn a single move into
// an ever-climbing total, which is worse than not counting it at all: the rate panel would
// show migrations happening on an idle pool.
func TestAMigrationIsCountedOnceHoweverOftenItIsReconciled(t *testing.T) {
	const namespace, name = metricNamespace, "move-acme-once"

	recordMigrationPhase(namespace, name,
		pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
		migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseCopying),
		acmeRoute, time.Unix(1030, 0))

	before := outcomeTotalFor(t, kindMigration, "Completed")

	recordMigrationPhase(namespace, name,
		pgelasticv1alpha1.TenantMigrationPhaseCutover,
		migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseCompleted),
		acmeRoute, time.Unix(1090, 0))
	for range 5 {
		recordMigrationPhase(namespace, name,
			pgelasticv1alpha1.TenantMigrationPhaseCompleted,
			migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseCompleted),
			acmeRoute, time.Unix(1200, 0))
	}

	if after := outcomeTotalFor(t, kindMigration, "Completed"); after != before+1 {
		t.Errorf("six reconciles of one finished migration counted %v, want exactly one", after-before)
	}
}

// A migration refused by its own preflight reaches a terminal phase having never been seen
// in a running one. It is still a migration that happened, and an operator alerting on
// failures is alerting on exactly these.
func TestAMigrationThatFailsImmediatelyIsStillCounted(t *testing.T) {
	const namespace, name = metricNamespace, "move-acme-refused"

	before := outcomeTotalFor(t, kindMigration, "Failed")

	recordMigrationPhase(namespace, name,
		"",
		migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseFailed),
		acmeRoute, time.Unix(1000, 0))

	if after := outcomeTotalFor(t, kindMigration, "Failed"); after != before+1 {
		t.Errorf("a migration that failed on its first pass counted %v, want one", after-before)
	}
	if gathered(t, totalSeries) == 0 {
		t.Error("the outcome counter carries no series at all")
	}
}

// A dashboard has to be able to say where a tenant is going, not just that it is going. The
// route is one series per in-flight migration naming both instances, joined to the phase
// timeline by the migration's own name.
func TestAMigrationSaysWhichInstanceItIsLeavingAndWhichItIsJoining(t *testing.T) {
	const name = "move-acme-route"

	recordMigrationPhase(metricNamespace, name,
		pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
		migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseCopying),
		acmeRoute, time.Unix(1030, 0))

	labels := routeLabels(t, name)
	if labels[labelFrom] != "gp-old" || labels[labelTo] != "gp-new" {
		t.Errorf("the route reads from=%q to=%q, want gp-old to gp-new",
			labels[labelFrom], labels[labelTo])
	}

	// A migration learns its source one reconcile after its target. Re-reporting must move the
	// series rather than leave the half-known one alongside it.
	recordMigrationPhase(metricNamespace, name,
		pgelasticv1alpha1.TenantMigrationPhaseCopying,
		migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseCatchup),
		migration.Plan{Target: migration.Endpoint{Instance: "gp-newer"}}, time.Unix(1040, 0))

	if labels := routeLabels(t, name); labels[labelTo] != "gp-newer" {
		t.Errorf("a re-reported route reads to=%q, want the new one", labels[labelTo])
	}

	recordMigrationPhase(metricNamespace, name,
		pgelasticv1alpha1.TenantMigrationPhaseCutover,
		migrationStatus(pgelasticv1alpha1.TenantMigrationPhaseCompleted),
		acmeRoute, time.Unix(1090, 0))

	if labels := routeLabels(t, name); labels != nil {
		t.Errorf("a finished migration still advertises a route: %v", labels)
	}
}

// routeLabels returns the labels of the one route series for a migration, or nil when it has
// none. More than one is a failure in itself: the whole point is that an object has one route.
func routeLabels(t *testing.T, name string) map[string]string {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gathering: %v", err)
	}
	var found []map[string]string
	for _, family := range families {
		if family.GetName() != routeSeries {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels[labelName] == name {
				found = append(found, labels)
			}
		}
	}
	if len(found) > 1 {
		t.Fatalf("one migration advertises %d routes at once: %v", len(found), found)
	}
	if len(found) == 0 {
		return nil
	}
	return found[0]
}
