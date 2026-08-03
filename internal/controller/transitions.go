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
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/andrew01234567890/pgelastic/internal/metering"
)

// Kinds that transition, spelled once. These are label values on a shared metric rather
// than a metric per kind, so a dashboard can draw everything a pool is moving in one panel
// and still filter down to migrations.
const kindMigration = "PgTenantMigration"

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
