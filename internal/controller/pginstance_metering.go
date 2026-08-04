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
	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/metering"
)

// meterDatabases stages this instance's pg_stat_database scrape for the pool controller to
// fold.
//
// It stages rather than folds because folding needs a pool, and this controller has an
// instance: the collector's round is one pool's tenants and one pool's ledger together, and a
// round written from here would publish whichever instance reported last as if it were the
// pool's own state.
//
// Only the primary's reading is taken. A standby's pg_stat_database counts the reads that
// standby served, which is a real fact and one nothing in this tree produces: no tenant read
// is routed to a replica today. That is the same choice, for the same reason, that inUseOf
// makes about connections - and it is the line that has to change on the day something does
// route reads to standbys.
func (r *PgInstanceReconciler) meterDatabases(
	instance *pgelasticv1alpha1.PgInstance,
	reports []provision.MemberReport,
) {
	if r.Metering == nil {
		return
	}
	primary := reportOfPrimary(instance, reports)
	if primary == nil {
		return
	}
	r.Metering.RecordDatabaseStats(instance.Namespace, instance.Name, r.now(),
		databaseStatsByName(primary.Databases))
}

// reportOfPrimary picks the member whose report belongs to the instance's primary.
//
// status.currentPrimary is the authority, and the report's own InRecovery is only a fallback
// for the bootstrap window before anybody has claimed the role. Trusting InRecovery first
// would take counters from a demoted member that has not finished becoming a standby, which
// is the same mistake reconcileRoleLabels exists to avoid with the Service selector.
func reportOfPrimary(
	instance *pgelasticv1alpha1.PgInstance,
	reports []provision.MemberReport,
) *provision.MemberReport {
	if current := instance.Status.CurrentPrimary; current != "" {
		for i := range reports {
			if reports[i].Member == current {
				return &reports[i]
			}
		}
		return nil
	}
	for i := range reports {
		if !reports[i].InRecovery {
			return &reports[i]
		}
	}
	return nil
}

// databaseStatsByName turns the wire report into what the accumulator differences.
//
// Every entry of metering.Stats is written, including the ones that are zero. A counter left
// out of the map is not a zero to the delta: it is skipped entirely, so a stat that stopped
// being reported would freeze at its last value rather than stop rising.
func databaseStatsByName(reports []provision.DatabaseReport) map[string]metering.DatabaseStats {
	if len(reports) == 0 {
		return nil
	}
	byName := make(map[string]metering.DatabaseStats, len(reports))
	for _, report := range reports {
		stats := metering.DatabaseStats{
			DatabaseOID: report.OID,
			NumBackends: report.NumBackends,
			SizeBytes:   report.SizeBytes,
			Counters: map[metering.Stat]int64{
				metering.StatXactCommit:   report.XactCommit,
				metering.StatXactRollback: report.XactRollback,
				metering.StatBlksRead:     report.BlksRead,
				metering.StatBlksHit:      report.BlksHit,
				metering.StatTupReturned:  report.TupReturned,
				metering.StatTupFetched:   report.TupFetched,
				metering.StatTupModified:  report.TupModified,
				metering.StatDeadlocks:    report.Deadlocks,
			},
		}
		if report.StatsReset != nil {
			at := report.StatsReset.Time
			stats.StatsReset = &at
		}
		byName[report.Name] = stats
	}
	return byName
}
