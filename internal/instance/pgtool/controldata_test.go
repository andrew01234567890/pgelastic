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

package pgtool

import (
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

const sampleControlData = `pg_control version number:            1800
Catalog version number:               202506291
Database system identifier:           7521834562341234567
Database cluster state:               in production
pg_control last modified:             Tue 28 Jul 2026 02:00:11 PM UTC
Latest checkpoint location:           0/3A9F1C8
Latest checkpoint's TimeLineID:       3
Latest checkpoint's full_page_writes: on
max_connections setting:              422
max_worker_processes setting:         16
max_wal_senders setting:              6
max_prepared_xacts setting:           0
max_locks_per_xact setting:           64
track_commit_timestamp setting:       on
Maximum data alignment:               8
Database block size:                  8192
Bytes per WAL segment:                16777216
Data page checksum version:           1
`

func TestParseControlData(t *testing.T) {
	data, err := ParseControlData(sampleControlData)
	if err != nil {
		t.Fatalf("ParseControlData = %v", err)
	}
	if data.SystemIdentifier != "7521834562341234567" {
		t.Errorf("system identifier = %q", data.SystemIdentifier)
	}
	if data.ClusterState != "in production" {
		t.Errorf("cluster state = %q", data.ClusterState)
	}
	if data.TimelineID != 3 {
		t.Errorf("timeline = %d, want 3", data.TimelineID)
	}
	if data.WALSegmentSize != 16777216 {
		t.Errorf("wal segment size = %d, want 16MiB", data.WALSegmentSize)
	}
	if data.DataPageChecksumVersion != 1 {
		t.Errorf("checksum version = %d, want 1", data.DataPageChecksumVersion)
	}
}

func TestParseControlDataMapsLabelsOntoGUCNames(t *testing.T) {
	data, err := ParseControlData(sampleControlData)
	if err != nil {
		t.Fatalf("ParseControlData = %v", err)
	}
	want := map[string]int32{
		pgconf.GUCMaxConnections:          422,
		pgconf.GUCMaxWorkerProcesses:      16,
		pgconf.GUCMaxWALSenders:           6,
		pgconf.GUCMaxPreparedTransactions: 0,
		pgconf.GUCMaxLocksPerTransaction:  64,
	}
	for name, value := range want {
		if data.EnforcedSettings[name] != value {
			t.Errorf("%s = %d, want %d", name, data.EnforcedSettings[name], value)
		}
	}
	if len(data.EnforcedSettings) != len(controlDataKeys) {
		t.Errorf("enforced settings = %v, want all five: starting recovery below any of them FATALs",
			data.EnforcedSettings)
	}
}

func TestParseControlDataRejectsOutputWithNoSystemIdentifier(t *testing.T) {
	if _, err := ParseControlData("Database cluster state: in production\n"); err == nil {
		t.Fatal("output with no system identifier is not a control file this may adopt")
	}
}

func TestEnforcedFloorRaisesToTheControlFile(t *testing.T) {
	data, err := ParseControlData(sampleControlData)
	if err != nil {
		t.Fatal(err)
	}
	floored := EnforcedFloor(map[string]int32{
		pgconf.GUCMaxConnections:     100,
		pgconf.GUCMaxWorkerProcesses: 32,
	}, data)
	if floored[pgconf.GUCMaxConnections] != 422 {
		t.Errorf("max_connections = %d, want the control file's higher 422",
			floored[pgconf.GUCMaxConnections])
	}
	if floored[pgconf.GUCMaxWorkerProcesses] != 32 {
		t.Errorf("max_worker_processes = %d, want the higher desired 32",
			floored[pgconf.GUCMaxWorkerProcesses])
	}
}

func TestQuarantineSuffixIsSortableAndUnambiguous(t *testing.T) {
	suffix := QuarantineSuffix(time.Date(2026, 7, 28, 14, 5, 9, 0, time.UTC))
	if suffix != ".pgelastic-quarantine-20260728T140509Z" {
		t.Errorf("suffix = %q", suffix)
	}
	if !strings.HasPrefix(suffix, ".") {
		t.Error("the suffix must not turn the directory into a sibling PGDATA candidate")
	}
}
