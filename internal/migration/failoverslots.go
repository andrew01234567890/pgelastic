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

package migration

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// standbyRequirements is the standby half of the PG18 failover-slot contract, as GUC name
// and the value it has to hold. primary_conninfo is checked separately because what
// matters about it is a substring rather than a value.
var standbyRequirements = []struct {
	guc  string
	want string
	why  string
}{
	{"hot_standby_feedback", "on", "without it the primary can vacuum away rows the synced slot still needs"},
	{"sync_replication_slots", "on", "this is the switch that runs the slot synchronization worker at all"},
}

// CheckFailoverSlots asserts the whole PG18 failover-slot stack on the source instance.
//
// The stack is not a nicety. A migration slot that is not synchronized is destroyed by a
// failover mid-migration, and the migration then fails in a way that is at least loud. The
// quiet failure is the other one: without synchronized_standby_slots naming every
// synchronous standby's physical slot, the walsender feeding the subscriber is free to
// send changes the standbys have not flushed yet. Promote one of those standbys and the
// synced logical slot is now *behind* the subscriber, so replay resumes from a position
// the subscriber has already passed - and the rows in between are never delivered. No
// error is raised on either side; the migration completes and the tenant is missing rows.
func CheckFailoverSlots(ctx context.Context, sql SQL, source Endpoint, standbys []string) Check {
	primary := source.WithDatabase("postgres")

	walLevel, err := scalar(ctx, sql, primary, `SHOW wal_level`)
	if err != nil {
		return failed(CheckFailoverSlotStack, "could not read the source's wal_level: "+err.Error())
	}
	if walLevel != "logical" {
		return failed(CheckFailoverSlotStack, fmt.Sprintf(
			"the source runs with wal_level = %q; logical replication needs 'logical', and changing it "+
				"costs a postmaster restart, so the online path is unavailable on this instance", walLevel))
	}

	syncStandbySlots, err := scalar(ctx, sql, primary, `SHOW synchronized_standby_slots`)
	if err != nil {
		return failed(CheckFailoverSlotStack, "could not read synchronized_standby_slots: "+err.Error())
	}
	named := splitSlotList(syncStandbySlots)

	physical, err := firstColumn(ctx, sql, primary,
		`SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'physical' ORDER BY 1`)
	if err != nil {
		return failed(CheckFailoverSlotStack, "could not list the source's physical slots: "+err.Error())
	}

	quorum, err := firstColumn(ctx, sql, primary,
		`SELECT application_name FROM pg_stat_replication WHERE sync_state IN ('sync', 'quorum') ORDER BY 1`)
	if err != nil {
		return failed(CheckFailoverSlotStack, "could not list the source's synchronous standbys: "+err.Error())
	}
	if len(quorum) == 0 {
		return failed(CheckFailoverSlotStack,
			"no standby is counted towards the source's synchronous quorum, so a failover during this "+
				"migration would have no synchronized slot to carry it")
	}

	var problems []string
	for _, standby := range quorum {
		slot := slotFor(standby, physical)
		switch {
		case slot == "":
			problems = append(problems, fmt.Sprintf(
				"synchronous standby %q has no physical slot on the primary", standby))
		case !slices.Contains(named, slot):
			problems = append(problems, fmt.Sprintf(
				"synchronized_standby_slots does not name %q, so the subscriber could consume changes "+
					"%q has not flushed and lose them on promotion", slot, standby))
		}
	}
	problems = append(problems, checkStandbyMembers(ctx, sql, source, standbys)...)

	if len(problems) > 0 {
		return failed(CheckFailoverSlotStack, strings.Join(problems, "; ")+
			". The online path is refused rather than run without the stack, because the failure it "+
			"produces is silent row loss after a failover")
	}
	return passed(CheckFailoverSlotStack, fmt.Sprintf(
		"wal_level is logical, %d synchronous standby slot(s) are named in synchronized_standby_slots, "+
			"and every standby carries the slot-sync contract", len(quorum)))
}

func checkStandbyMembers(ctx context.Context, sql SQL, source Endpoint, standbys []string) []string {
	var problems []string
	for _, member := range standbys {
		at := source.WithDatabase("postgres").WithMember(member)
		for _, requirement := range standbyRequirements {
			value, err := scalar(ctx, sql, at, "SHOW "+requirement.guc)
			if err != nil {
				problems = append(problems, fmt.Sprintf("could not read %s on %s: %s",
					requirement.guc, member, err.Error()))
				continue
			}
			if value != requirement.want {
				problems = append(problems, fmt.Sprintf("%s has %s = %q, wanted %q: %s",
					member, requirement.guc, value, requirement.want, requirement.why))
			}
		}
		if slot, err := scalar(ctx, sql, at, `SHOW primary_slot_name`); err != nil {
			problems = append(problems, "could not read primary_slot_name on "+member+": "+err.Error())
		} else if slot == "" {
			problems = append(problems, member+" streams without a primary_slot_name, so its position "+
				"does not hold WAL on the primary and slot synchronization has nothing to anchor to")
		}
		conninfo, err := scalar(ctx, sql, at, `SHOW primary_conninfo`)
		switch {
		case err != nil:
			problems = append(problems, "could not read primary_conninfo on "+member+": "+err.Error())
		case !strings.Contains(conninfo, "dbname="):
			// The slot synchronization worker opens an ordinary connection to the database
			// named here, not a replication one. Without dbname= it errors out on every
			// attempt while streaming replication carries on looking perfectly healthy.
			problems = append(problems, member+" has a primary_conninfo with no dbname=, which makes "+
				"the slot synchronization worker error out while replication still looks healthy")
		}
	}
	return problems
}

// slotFor maps a standby's application_name onto its physical slot. The name is derived
// rather than read from pg_stat_replication because a standby that has just reconnected can
// be streaming before its slot column is populated.
func slotFor(standby string, physical []string) string {
	candidate := provision.ReplicationSlotName(standby)
	if slices.Contains(physical, candidate) {
		return candidate
	}
	return ""
}

func splitSlotList(value string) []string {
	parts := strings.Split(value, ",")
	slots := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			slots = append(slots, trimmed)
		}
	}
	return slots
}
