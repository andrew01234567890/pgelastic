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

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// migrationPauseSeconds is a product commitment rather than a diagnostic. The target is a
// p99 below one second with clients queued and never dropped, and the buckets are placed
// around that: the comparable managed-service elastic pool move drops connections and
// relies on client retry, so this histogram is the number the claim is checked against.
//
// The offline strategy shares the metric with a label of its own, because its pause is
// measured in tens of seconds by design and averaging the two would hide both.
var migrationPauseSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "pgelastic_migration_pause_seconds",
	Help:    "How long a tenant's clients were queued across the cutover, by strategy.",
	Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120},
}, []string{labelNamespace, "strategy"})

// migrationPhaseNames is every phase a migration's series are written for. The exporter
// writes all of them on every observation, not just the current one, so that leaving a
// phase drives its series back to zero: a gauge latched at its last value is how a finished
// migration goes on looking like a running one.
var migrationPhaseNames = phaseNames(
	pgelasticv1alpha1.TenantMigrationPhasePreflight,
	pgelasticv1alpha1.TenantMigrationPhaseProvisioning,
	pgelasticv1alpha1.TenantMigrationPhasePreWarm,
	pgelasticv1alpha1.TenantMigrationPhaseCopying,
	pgelasticv1alpha1.TenantMigrationPhaseCatchup,
	pgelasticv1alpha1.TenantMigrationPhaseQuiescing,
	pgelasticv1alpha1.TenantMigrationPhaseCutover,
	pgelasticv1alpha1.TenantMigrationPhaseCompleted,
	pgelasticv1alpha1.TenantMigrationPhaseFailed,
	pgelasticv1alpha1.TenantMigrationPhaseAborted,
	pgelasticv1alpha1.TenantMigrationPhaseRolledBack,
)

func phaseNames(phases ...pgelasticv1alpha1.TenantMigrationPhase) []string {
	names := make([]string, 0, len(phases))
	for _, phase := range phases {
		names = append(names, string(phase))
	}
	return names
}

func init() {
	metrics.Registry.MustRegister(migrationPauseSeconds)
}

// recordMigrationPhase reports where one migration has got to, and retires it when it
// arrives. previous is the phase the last reconcile left behind, and it is what makes both
// halves fire exactly once.
//
// Arriving at a terminal phase is observed and then immediately forgotten, in that order.
// The observation is what makes the exporter count the migration - it counts what it
// watched finish - and it matters for a migration that fails its very first preflight,
// which would otherwise reach a terminal phase having never been seen at all. The forget is
// what drops the per-object series, which is the whole reason the name label is affordable.
//
// A migration that was already terminal when this process first saw it is deliberately
// neither: an operator restart must not re-count every migration in the namespace's history.
func recordMigrationPhase(
	namespace, name string,
	previous pgelasticv1alpha1.TenantMigrationPhase,
	status *pgelasticv1alpha1.PgTenantMigrationStatus,
	route migration.Plan,
	now time.Time,
) {
	recordTransition(transition{
		Namespace: namespace,
		Kind:      kindMigration,
		Name:      name,
		Previous:  string(previous),
		Current:   string(status.Phase),
		Phases:    migrationPhaseNames,
		From:      route.Source.Instance,
		To:        route.Target.Instance,
		Took:      migrationDuration(status),
	}, func(phase string) bool {
		return migration.Terminal(pgelasticv1alpha1.TenantMigrationPhase(phase))
	}, now)
}

// migrationDuration is how long the migration ran, or zero when it cannot be said. Zero is
// not observed into the histogram, because a migration recorded as having taken no time is
// worse than one that is missing from the distribution.
func migrationDuration(status *pgelasticv1alpha1.PgTenantMigrationStatus) time.Duration {
	if status.StartedAt == nil || status.CompletedAt == nil {
		return 0
	}
	return status.CompletedAt.Sub(status.StartedAt.Time)
}

// migrationOrphansSwept counts the physical objects the sweeper reaped. An abandoned slot
// pins the source primary's WAL until max_slot_wal_keep_size invalidates it, so a non-zero
// rate here is evidence of migrations dying without running their own cleanup ladder.
var migrationOrphansSwept = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "pgelastic_migration_orphans_swept_total",
	Help: "Migration-owned publications, slots and subscriptions reaped by the orphan sweeper.",
}, []string{labelNamespace, labelInstance, labelKind})

func init() {
	metrics.Registry.MustRegister(migrationOrphansSwept)
}

func recordOrphansSwept(orphans []migration.Orphan) {
	for _, orphan := range orphans {
		migrationOrphansSwept.WithLabelValues(orphan.At.Namespace, orphan.At.Instance, orphan.Kind).Inc()
	}
}
