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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	"github.com/andrew01234567890/pgelastic/internal/ownership"
)

// DefaultSweepInterval is how often abandoned migration objects are reaped.
//
// It is minutes rather than seconds because the thing being reaped is already bounded:
// max_slot_wal_keep_size invalidates an abandoned slot before it can fill the primary's
// disk. The sweeper exists because an invalidated slot is still an object nobody will ever
// drop, and because a publication or a subscription left behind confuses the next migration
// of the same tenant.
const DefaultSweepInterval = 5 * time.Minute

// MigrationSweeper reaps the physical objects of migrations that no longer exist.
//
// A migration that runs to any of its terminal phases runs its own cleanup ladder. This is
// for the ones that never got there: an object deleted mid-flight, a controller that died
// between creating a slot and recording its name, a namespace removed under a running move.
// Because every generated name is a pure function of the migration's namespace and name,
// the sweeper can decide what is claimed without trusting any status it might have lost.
type MigrationSweeper struct {
	client.Client
	SQL      migration.SQL
	Interval time.Duration

	// ControllerName is this operator's identity. The sweep drops physical objects inside
	// PostgreSQL, so it is confined to the instances this operator governs; the set of live
	// claims it compares against stays cluster-wide, because an object another operator's
	// migration is using is not an orphan whoever is looking at it.
	ControllerName string
}

var _ manager.LeaderElectionRunnable = &MigrationSweeper{}

// Start runs the sweep on a ticker until the manager stops.
func (s *MigrationSweeper) Start(ctx context.Context) error {
	interval := s.Interval
	if interval <= 0 {
		interval = DefaultSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.Sweep(ctx); err != nil {
				logf.FromContext(ctx).Error(err, "Could not sweep abandoned migration objects")
			}
		}
	}
}

// NeedLeaderElection keeps one sweeper running per cluster. Two of them racing on the same
// slot would have one of them report a failure for an object the other had just dropped.
func (s *MigrationSweeper) NeedLeaderElection() bool { return true }

// Sweep reaps every unclaimed migration object on every instance.
func (s *MigrationSweeper) Sweep(ctx context.Context) error {
	if s.SQL == nil {
		return nil
	}
	log := logf.FromContext(ctx)

	migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
	if err := s.List(ctx, migrations); err != nil {
		return err
	}
	claims := make([][2]string, 0, len(migrations.Items))
	for i := range migrations.Items {
		object := &migrations.Items[i]
		if migration.Terminal(object.Status.Phase) {
			continue
		}
		claims = append(claims, [2]string{object.Namespace, object.Name})
	}
	live := migration.LiveObjectNames(claims)

	instances := &pgelasticv1alpha1.PgInstanceList{}
	if err := s.List(ctx, instances); err != nil {
		return err
	}
	resolver := ownership.Resolver{Reader: s.Client, ControllerName: s.ControllerName}
	for i := range instances.Items {
		instance := &instances.Items[i]
		if instance.Status.CurrentPrimary == "" {
			continue
		}
		verdict, err := resolver.Of(ctx, instance)
		if err != nil {
			return err
		}
		if verdict != ownership.Mine {
			continue
		}
		at := migration.Endpoint{
			Namespace: instance.Namespace, Instance: instance.Name, Database: "postgres"}
		orphans, err := migration.FindOrphans(ctx, s.SQL, at, live)
		if err != nil {
			log.Error(err, "Could not list abandoned migration objects", "instance", instance.Name)
			continue
		}
		if len(orphans) == 0 {
			continue
		}
		log.Info("Sweeping abandoned migration objects",
			"instance", instance.Name, "count", len(orphans))
		if err := migration.SweepOrphans(ctx, s.SQL, orphans); err != nil {
			log.Error(err, "Could not sweep abandoned migration objects", "instance", instance.Name)
			continue
		}
		recordOrphansSwept(orphans)
	}
	return nil
}
