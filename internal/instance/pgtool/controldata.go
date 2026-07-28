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

// Package pgtool wraps the PostgreSQL binaries the instance manager drives locally:
// initdb, pg_ctl, pg_basebackup, pg_isready and pg_controldata. Everything here runs
// inside the Postgres pod, as the postgres user, against a data directory nothing else
// may touch.
package pgtool

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

// ControlData is the subset of pg_controldata the instance manager acts on.
type ControlData struct {
	// SystemIdentifier keys the archive stanza. Keying it on the instance name instead
	// would let an instance recreated under a reused name interleave its WAL into a
	// predecessor's archive and quietly destroy the ability to restore either.
	SystemIdentifier string
	// ClusterState is the "Database cluster state" line.
	ClusterState string
	// TimelineID is a first-class term in candidate selection, ordered ahead of LSN.
	TimelineID int32
	// MinRecoveryEnd and MinRecoveryEndTimeline are how far a standby had replayed when it
	// last flushed, and on which timeline. They are the only durable record of a stopped
	// standby's position: the latest checkpoint's timeline lags a timeline switch by a whole
	// restartpoint, so a member that had already followed a new history looks, from the
	// checkpoint alone, as though it were still on the old one.
	MinRecoveryEnd         string
	MinRecoveryEndTimeline int32
	// WALSegmentSize is part of the collation contract: two instances that disagree
	// cannot exchange a physical backup.
	WALSegmentSize int64
	// DataPageChecksumVersion is non-zero when data checksums are on, which is what makes
	// wal_log_hints redundant on PG18.
	DataPageChecksumVersion int32
	// EnforcedSettings holds the five parameters a standby must not start below, keyed by
	// their GUC name rather than by pg_controldata's own labels.
	EnforcedSettings map[string]int32
}

// controlDataKeys maps pg_controldata's labels onto GUC names for the five enforced
// parameters. The labels are not the GUC names, and getting that mapping wrong shows up
// only as a FATAL at the start of recovery.
var controlDataKeys = map[string]string{
	"max_connections setting":      pgconf.GUCMaxConnections,
	"max_worker_processes setting": pgconf.GUCMaxWorkerProcesses,
	"max_wal_senders setting":      pgconf.GUCMaxWALSenders,
	"max_prepared_xacts setting":   pgconf.GUCMaxPreparedTransactions,
	"max_locks_per_xact setting":   pgconf.GUCMaxLocksPerTransaction,
}

// ParseControlData parses pg_controldata output. A parse failure is meaningful in itself:
// a pre-existing data directory whose control file does not parse is not a PostgreSQL
// data directory pgelastic may adopt.
func ParseControlData(output string) (ControlData, error) {
	data := ControlData{EnforcedSettings: map[string]int32{}}
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		label, value, found := strings.Cut(scanner.Text(), ":")
		if !found {
			continue
		}
		label, value = strings.TrimSpace(label), strings.TrimSpace(value)
		if guc, ok := controlDataKeys[label]; ok {
			number, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return ControlData{}, fmt.Errorf("pg_controldata %q: %w", label, err)
			}
			data.EnforcedSettings[guc] = int32(number)
			continue
		}
		switch label {
		case "Database system identifier":
			data.SystemIdentifier = value
		case "Database cluster state":
			data.ClusterState = value
		case "Latest checkpoint's TimeLineID":
			number, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return ControlData{}, fmt.Errorf("pg_controldata %q: %w", label, err)
			}
			data.TimelineID = int32(number)
		case "Minimum recovery ending location":
			data.MinRecoveryEnd = value
		case "Min recovery ending loc's timeline":
			number, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return ControlData{}, fmt.Errorf("pg_controldata %q: %w", label, err)
			}
			data.MinRecoveryEndTimeline = int32(number)
		case "Bytes per WAL segment":
			number, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ControlData{}, fmt.Errorf("pg_controldata %q: %w", label, err)
			}
			data.WALSegmentSize = number
		case "Data page checksum version":
			number, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return ControlData{}, fmt.Errorf("pg_controldata %q: %w", label, err)
			}
			data.DataPageChecksumVersion = int32(number)
		}
	}
	if err := scanner.Err(); err != nil {
		return ControlData{}, err
	}
	if data.SystemIdentifier == "" {
		return ControlData{}, fmt.Errorf("pg_controldata output has no system identifier")
	}
	return data, nil
}

// EnforcedFloor raises each of the five enforced parameters to max(desired, controldata).
// It runs on every non-first start of a standby, because a primary that was restarted with
// a higher value leaves the standby unable to begin recovery at all until it matches.
func EnforcedFloor(desired map[string]int32, data ControlData) map[string]int32 {
	floored := make(map[string]int32, len(desired))
	for name, value := range desired {
		floored[name] = max(value, data.EnforcedSettings[name])
	}
	return floored
}

// QuarantineSuffix builds the suffix appended to a pre-existing data directory that is
// being moved aside. Renaming rather than deleting is the whole point: a directory that
// parses as PostgreSQL data is somebody's database until proven otherwise, and the cost of
// keeping it is disk, while the cost of deleting it is unbounded.
func QuarantineSuffix(now time.Time) string {
	return ".pgelastic-quarantine-" + now.UTC().Format("20060102T150405Z")
}
