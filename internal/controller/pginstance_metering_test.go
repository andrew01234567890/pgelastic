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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/metering"
)

// The names every spec in this file is written against.
const (
	meteredDatabase = "tenant-alpha"
	meteredTenant   = "alpha"
	successor       = "pg-a-2"
)

func reportOf(name string, databases ...provision.DatabaseReport) provision.MemberReport {
	return provision.MemberReport{Member: name, Healthy: true, Databases: databases}
}

// A counter the mapping forgets is not a zero to the accumulator: it is skipped, so the total
// freezes at its last value and goes on being exported as a number that has stopped moving.
// Every entry of metering.Stats has to be written, including the ones that are zero.
func TestEveryMeteredCounterIsCarriedOffTheReport(t *testing.T) {
	byName := databaseStatsByName([]provision.DatabaseReport{{
		Name: meteredDatabase, OID: 16385,
		XactCommit: 1, XactRollback: 2, BlksRead: 3, BlksHit: 4,
		TupReturned: 5, TupFetched: 6, TupModified: 7, Deadlocks: 0,
	}})

	stats, ok := byName[meteredDatabase]
	if !ok {
		t.Fatalf("the mapping produced %v, want an entry for the tenant's database", byName)
	}
	for _, stat := range metering.Stats {
		if _, present := stats.Counters[stat]; !present {
			t.Errorf("%s is missing from the mapped counters, so its total would freeze "+
				"rather than stop rising", stat)
		}
	}
	if stats.DatabaseOID != 16385 {
		t.Errorf("databaseOID = %d, want the oid that tells a recreated database apart",
			stats.DatabaseOID)
	}
}

func TestAResetInstantSurvivesTheMappingAndAnAbsentOneStaysAbsent(t *testing.T) {
	reset := metav1.NewTime(time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC))
	byName := databaseStatsByName([]provision.DatabaseReport{
		{Name: "was-reset", StatsReset: &reset},
		{Name: "never-reset"},
	})

	if got := byName["was-reset"].StatsReset; got == nil || !got.Equal(reset.Time) {
		t.Errorf("statsReset = %v, want %v: a reset the accumulator cannot see reads as "+
			"every counter decreasing at once", got, reset.Time)
	}
	if got := byName["never-reset"].StatsReset; got != nil {
		t.Errorf("statsReset = %v on a database nobody has reset, want nil", got)
	}
}

// Counters taken from a member that has been demoted are a different instance's history, and
// status.currentPrimary is the operator's own answer to who holds the role. A member's report
// of itself is only consulted before anybody has claimed it, which is bootstrap.
func TestTheCountersComeFromTheMemberTheOperatorCallsThePrimary(t *testing.T) {
	demoted := reportOf(testMember, provision.DatabaseReport{Name: meteredDatabase, XactCommit: 10})
	demoted.InRecovery = false
	promoted := reportOf(successor, provision.DatabaseReport{Name: meteredDatabase, XactCommit: 90})
	promoted.InRecovery = false

	instance := &pgelasticv1alpha1.PgInstance{
		Status: pgelasticv1alpha1.PgInstanceStatus{CurrentPrimary: successor},
	}
	chosen := reportOfPrimary(instance, []provision.MemberReport{demoted, promoted})
	if chosen == nil || chosen.Member != successor {
		t.Fatalf("chose %v, want the member status.currentPrimary names", chosen)
	}

	// Nobody holds the role yet, which is the bootstrap window.
	bootstrapping := &pgelasticv1alpha1.PgInstance{}
	standby := reportOf("pg-a-3")
	standby.InRecovery = true
	chosen = reportOfPrimary(bootstrapping, []provision.MemberReport{standby, promoted})
	if chosen == nil || chosen.Member != successor {
		t.Fatalf("chose %v, want the only member not in recovery", chosen)
	}

	// A primary that did not answer this round leaves no reading at all, rather than one
	// taken from a standby.
	if chosen := reportOfPrimary(instance, []provision.MemberReport{standby}); chosen != nil {
		t.Errorf("chose %v, want nothing: the primary did not answer", chosen)
	}
}

// The whole point of staging: readings recorded from the instance controller reach the pool
// controller's round, and the accumulator differences them rather than adding each cumulative
// reading to the total again.
func TestAStagedReadingIsDifferencedIntoThePoolsTotals(t *testing.T) {
	const (
		namespace = "saas-prod"
		pool      = "saas-pool"
		instance  = "pg-a"
		database  = "tenant_alpha"
	)
	collector := metering.NewCollector(metering.Options{}, nil)
	reconciler := &PgInstanceReconciler{Metering: collector}
	held := &pgelasticv1alpha1.PgInstance{}
	held.Namespace, held.Name = namespace, instance
	held.Status.CurrentPrimary = testMember

	key := metering.TotalKey{
		Key:      metering.Key{Namespace: namespace, Pool: pool, Tenant: meteredTenant},
		Database: database,
		Role:     metering.RolePrimary,
	}
	fold := func(at time.Time) {
		stats, ok := collector.DatabaseStatsFor(metering.ReadingKey{
			Namespace: namespace, Instance: instance, Database: database,
		}, at)
		if !ok {
			t.Fatalf("no reading was staged for %s on %s", database, instance)
		}
		collector.Observe(
			metering.PoolObservation{Namespace: namespace, Pool: pool},
			[]metering.TenantObservation{{
				Key: key.Key, Database: database, Instance: instance,
				Role: metering.RolePrimary, Stats: &stats,
			}}, at)
	}

	at := time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC)
	reconciler.Now = func() time.Time { return at }
	reconciler.meterDatabases(held, []provision.MemberReport{
		reportOf(testMember, provision.DatabaseReport{Name: database, OID: 16385, XactCommit: 100}),
	})
	fold(at)
	// The first reading is a baseline. That 100 accrued on the postmaster before this
	// process was watching, and counting it would put the server's lifetime into the total
	// on every operator restart.
	if got := collector.Accumulator.Total(key, metering.StatXactCommit); got != 0 {
		t.Fatalf("xact_commit total = %d after the first reading, want 0", got)
	}

	later := at.Add(time.Minute)
	reconciler.Now = func() time.Time { return later }
	reconciler.meterDatabases(held, []provision.MemberReport{
		reportOf(testMember, provision.DatabaseReport{Name: database, OID: 16385, XactCommit: 130}),
	})
	fold(later)
	if got := collector.Accumulator.Total(key, metering.StatXactCommit); got != 30 {
		t.Errorf("xact_commit total = %d after a second cumulative reading of 130, want 30: "+
			"the readings are being added rather than differenced", got)
	}
}
