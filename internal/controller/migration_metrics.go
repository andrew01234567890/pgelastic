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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

const labelMigration = "migration"

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

// migrationPhase carries one series per phase so that a phase being left drives its series
// back to zero. A gauge latched at its last value is how a finished migration goes on
// looking like a running one.
var migrationPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "pgelastic_migration_phase",
	Help: "1 for the phase a migration is currently in.",
}, []string{labelNamespace, labelMigration, fieldPhase})

// migrationPhases is every phase the gauge can report.
var migrationPhases = []pgelasticv1alpha1.TenantMigrationPhase{
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
}

func init() {
	metrics.Registry.MustRegister(migrationPauseSeconds, migrationPhase)
}

func recordMigrationPhase(namespace, name string, phase pgelasticv1alpha1.TenantMigrationPhase) {
	for _, candidate := range migrationPhases {
		migrationPhase.WithLabelValues(namespace, name, string(candidate)).
			Set(boolValue(candidate == phase))
	}
}

// migrationOrphansSwept counts the physical objects the sweeper reaped. An abandoned slot
// pins the source primary's WAL until max_slot_wal_keep_size invalidates it, so a non-zero
// rate here is evidence of migrations dying without running their own cleanup ladder.
var migrationOrphansSwept = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "pgelastic_migration_orphans_swept_total",
	Help: "Migration-owned publications, slots and subscriptions reaped by the orphan sweeper.",
}, []string{labelNamespace, labelInstance, "kind"})

func init() {
	metrics.Registry.MustRegister(migrationOrphansSwept)
}

func recordOrphansSwept(orphans []migration.Orphan) {
	for _, orphan := range orphans {
		migrationOrphansSwept.WithLabelValues(orphan.At.Namespace, orphan.At.Instance, orphan.Kind).Inc()
	}
}
