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

package pgbackrest

import (
	"slices"
	"testing"
)

// A trimmed but otherwise verbatim `pgbackrest info --output=json`, carrying the two
// backups and the two database histories a stanza has after a restore has forked it.
const sampleInfo = `[
  {
    "name": "pgelastic-7521834562341234567",
    "db": [
      {"id": 1, "system-id": 7521834562341234567, "version": "18"},
      {"id": 2, "system-id": 7409988776655443322, "version": "18"}
    ],
    "backup": [
      {
        "label": "20260731-020000F",
        "type": "full",
        "timestamp": {"start": 1785463200, "stop": 1785463320},
        "lsn": {"start": "0/2000028", "stop": "0/2000138"},
        "archive": {"start": "000000010000000000000002", "stop": "000000010000000000000002"},
        "info": {"size": 25000000, "repository": {"size": 3100000, "delta": 3100000}},
        "database": {"id": 1, "repo-key": 1},
        "annotation": {"pgelastic-backup": "pg-a-20260731t0200"}
      },
      {
        "label": "20260731-020000F_20260731-140000D",
        "type": "diff",
        "timestamp": {"start": 1785506400, "stop": 1785506430},
        "lsn": {"start": "0/4000028", "stop": "0/4000138"},
        "archive": {"start": "000000010000000000000004", "stop": "000000010000000000000004"},
        "info": {"size": 25000000, "repository": {"size": 90000, "delta": 90000}},
        "database": {"id": 2, "repo-key": 1},
        "annotation": {"pgelastic-backup": "on-demand"}
      }
    ]
  }
]`

func TestParseInfoReadsWhatARestoreNeeds(t *testing.T) {
	backups, err := ParseInfo(sampleInfo, "pgelastic-7521834562341234567")
	if err != nil {
		t.Fatalf("ParseInfo = %v", err)
	}
	if len(backups) != 2 {
		t.Fatalf("backups = %d, want two", len(backups))
	}

	full := backups[0]
	if full.Label != "20260731-020000F" || full.Type != BackupFull {
		t.Errorf("full = %+v", full)
	}
	// The LSN range is what a restore replays between, and the WAL names are what a restore
	// checks are present before it spends an hour pulling the base backup down.
	if full.BeginLSN != "0/2000028" || full.EndLSN != "0/2000138" {
		t.Errorf("lsn range = %s..%s", full.BeginLSN, full.EndLSN)
	}
	if full.BeginWAL != "000000010000000000000002" || full.EndWAL != "000000010000000000000002" {
		t.Errorf("wal range = %s..%s", full.BeginWAL, full.EndWAL)
	}
	// The repository size, not the database size: what a backup costs is what it occupies
	// after compression, and reporting the uncompressed figure would overstate the bill by
	// an order of magnitude.
	if full.SizeBytes != 3100000 {
		t.Errorf("sizeBytes = %d, want the compressed repository size", full.SizeBytes)
	}
	if full.Started.IsZero() || !full.Stopped.After(full.Started) {
		t.Errorf("times = %s..%s", full.Started, full.Stopped)
	}
}

// A stanza holds more than one database history after a restore forks it, and each backup
// names the one it belongs to. Resolving that per backup rather than taking the stanza's
// first entry is what stops a restore being planned against a database the backup was
// never taken from.
func TestEachBackupCarriesItsOwnDatabaseHistory(t *testing.T) {
	backups, err := ParseInfo(sampleInfo, "pgelastic-7521834562341234567")
	if err != nil {
		t.Fatal(err)
	}
	if backups[0].SystemIdentifier != "7521834562341234567" {
		t.Errorf("first system identifier = %q", backups[0].SystemIdentifier)
	}
	if backups[1].SystemIdentifier != "7409988776655443322" {
		t.Errorf("second system identifier = %q", backups[1].SystemIdentifier)
	}
}

// Matching by annotation rather than by "the newest one" is what stops a scheduled backup
// and an on-demand one that overlapped from each claiming the other's result. The LSN range
// recorded against a backup is what a restore trusts.
func TestABackupIsFoundByWhatAskedForIt(t *testing.T) {
	backups, err := ParseInfo(sampleInfo, "pgelastic-7521834562341234567")
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(backups, func(backup Backup) bool {
		return backup.Annotation[BackupAnnotation] == "on-demand"
	})
	if index < 0 {
		t.Fatal("the on-demand backup could not be found by its annotation")
	}
	if backups[index].Label != "20260731-020000F_20260731-140000D" {
		t.Errorf("label = %q", backups[index].Label)
	}
}

func TestAMissingStanzaIsAnError(t *testing.T) {
	if _, err := ParseInfo(sampleInfo, "pgelastic-somebody-else"); err == nil {
		t.Fatal("ParseInfo accepted a stanza the repository does not hold")
	}
}

func TestAnEmptyRepositoryIsAnErrorNotAnEmptyList(t *testing.T) {
	// pgBackRest reports an empty array when the repository holds nothing at all. Returning
	// no backups and no error would read as "this stanza exists and is empty", which is a
	// different claim from "this stanza is not there".
	if _, err := ParseInfo(`[]`, "pgelastic-7521834562341234567"); err == nil {
		t.Fatal("an empty repository was reported as a stanza with no backups")
	}
}

func TestLatestIsTheNewestByStopTime(t *testing.T) {
	backups, err := ParseInfo(sampleInfo, "pgelastic-7521834562341234567")
	if err != nil {
		t.Fatal(err)
	}
	// Reversed, because the catalogue's own order must not be what makes this right.
	slices.Reverse(backups)
	latest, ok := Latest(backups)
	if !ok || latest.Label != "20260731-020000F_20260731-140000D" {
		t.Fatalf("latest = %+v, want the newest by stop time", latest)
	}
	if _, ok := Latest(nil); ok {
		t.Error("an empty catalogue reported a latest backup")
	}
}

func TestFindByLabelIsExact(t *testing.T) {
	backups, err := ParseInfo(sampleInfo, "pgelastic-7521834562341234567")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := FindByLabel(backups, "20260731-020000F"); !ok {
		t.Error("an existing label was not found")
	}
	// A prefix match would find the full backup when asked for the differential that
	// descends from it, and a restore pointed at the wrong one loses everything after it.
	if _, ok := FindByLabel(backups, "20260731-020000"); ok {
		t.Error("a partial label matched, so a restore could be pointed at the wrong backup")
	}
}

func TestBackupCommandsCarryTheTypeAndTheRequester(t *testing.T) {
	invocation := Invocation{ConfigFile: testConfigFile, Stanza: testStanza}
	args := invocation.Backup(BackupDifferential, "pg-a-20260731t0200").Args
	if !slices.Contains(args, "--type=diff") {
		t.Errorf("args = %v, want the requested type", args)
	}
	if !slices.Contains(args, "--annotation="+BackupAnnotation+"=pg-a-20260731t0200") {
		t.Errorf("args = %v, want the backup annotated with what asked for it", args)
	}
}
