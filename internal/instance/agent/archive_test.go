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

package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgbackrest"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// testRetentionWindow is the documented default, and the shortest thing that renders.
const testRetentionWindow = "30d"

func at(offset time.Duration) *time.Time {
	moment := time.Now().Add(offset)
	return &moment
}

func archivingConfig() provision.AgentConfig {
	config := agentConfig()
	config.Backup = &pgbackrest.Repository{
		Path:          "s3://backups/prod",
		RetentionFull: testRetentionWindow,
		RetentionWAL:  testRetentionWindow,
	}
	return config
}

// Three states, not two. An archive nothing has ever been written to is neither working nor
// failing, and it is the only state from which a primary should switch a segment to find
// out which of the two it is.
func TestArchiveStateIsThreeWay(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		archived *time.Time
		failed   *time.Time
		want     ArchiveState
	}{
		{"nothing has ever happened", nil, nil, ArchiveNeverRun},
		{"only successes", at(-time.Minute), nil, ArchiveWorking},
		{"only failures", nil, at(-time.Minute), ArchiveFailing},
		{"succeeded since the last failure", at(-time.Minute), at(-time.Hour), ArchiveWorking},
		{"failed since the last success", at(-time.Hour), at(-time.Minute), ArchiveFailing},
	} {
		if got := archiveState(testCase.archived, testCase.failed); got != testCase.want {
			t.Errorf("%s: state = %q, want %q", testCase.name, got, testCase.want)
		}
	}
}

// An idle instance archives nothing because archive_timeout only switches a segment when
// there has been WAL activity since the last switch. Calling that a fault would report a
// broken archive on every quiet instance in the fleet.
func TestAnIdleArchiveIsHealthyHoweverStaleItLooks(t *testing.T) {
	observation := ArchiveObservation{
		State:          ArchiveWorking,
		LastArchivedAt: at(-24 * time.Hour),
		ReadyBacklog:   0,
	}
	if !observation.Healthy(time.Now()) {
		t.Fatal("an instance with nothing to archive was reported as failing to archive")
	}
}

// The failure this catches is the one pg_stat_archiver cannot see: an archive_command that
// hangs rather than exits records neither a success nor a failure, so the last success ages
// exactly as it would on an idle instance. The queue is what tells them apart.
func TestAQueueThatIsNotDrainingIsAStall(t *testing.T) {
	stalled := ArchiveObservation{
		State:          ArchiveWorking,
		LastArchivedAt: at(-(provision.ArchiveStallAfter + time.Minute)),
		ReadyBacklog:   12,
	}
	if stalled.Healthy(time.Now()) {
		t.Error("a queue that has not moved for longer than the stall window was called healthy")
	}

	busy := ArchiveObservation{
		State:          ArchiveWorking,
		LastArchivedAt: at(-time.Minute),
		ReadyBacklog:   12,
	}
	if !busy.Healthy(time.Now()) {
		t.Error("an archive that is merely behind was called stalled")
	}
}

func TestAFailingArchiveIsNeverHealthy(t *testing.T) {
	for _, state := range []ArchiveState{ArchiveFailing, ArchiveNeverRun} {
		observation := ArchiveObservation{State: state, LastArchivedAt: at(-time.Second)}
		if observation.Healthy(time.Now()) {
			t.Errorf("state %q was reported healthy", state)
		}
	}
}

// A queue with no successful archive behind it has no age to compare, and the queue is
// precisely what makes that a stall rather than an instance that has not started yet.
func TestAQueueWithNoSuccessBehindItIsAStall(t *testing.T) {
	observation := ArchiveObservation{State: ArchiveWorking, ReadyBacklog: 3}
	if observation.Healthy(time.Now()) {
		t.Fatal("segments queued behind an archive that has never succeeded were called healthy")
	}
}

// restore_command is what lets a standby that fell off the primary's wal_keep_size read the
// missing segments out of the repository instead of re-cloning itself. It is decided in
// WriteConfig rather than by its four callers, so this asserts every path gets it.
func TestEveryConfigurationPathGetsARestoreCommand(t *testing.T) {
	config := archivingConfig()
	for name, replication := range map[string]pgconf.ReplicationConfig{
		"a primary at initdb": {},
		"a standby joining": {
			Standby:         true,
			PrimaryConnInfo: testPrimaryConnInfo,
			PrimarySlotName: testPrimarySlot,
		},
		"a member being promoted": {SynchronousStandbyNames: "ANY 1 (pg-a-2)"},
	} {
		dir := t.TempDir()
		if _, err := WriteConfig(config, "pg-a-2", replication, dir, nil); err != nil {
			t.Fatalf("%s: WriteConfig = %v", name, err)
		}
		got := pgconf.ParseSettings(readFile(t, dir, pgconf.OverrideConfFile))["restore_command"]
		if !strings.Contains(got, "wal-restore") {
			t.Errorf("%s: restore_command = %q", name, got)
		}
		for _, placeholder := range []string{"%f", "%p"} {
			if !strings.Contains(got, placeholder) {
				t.Errorf("%s: restore_command %q is missing %s", name, got, placeholder)
			}
		}
	}
}

// Without a repository there is nowhere to restore from, and a restore_command that always
// fails would make PostgreSQL log an archive lookup failure for every segment a standby
// needs while it is perfectly able to stream them.
func TestNoRestoreCommandWithoutARepository(t *testing.T) {
	dir := t.TempDir()
	if _, err := WriteConfig(agentConfig(), "pg-a-2", pgconf.ReplicationConfig{}, dir, nil); err != nil {
		t.Fatalf("WriteConfig = %v", err)
	}
	if got := pgconf.ParseSettings(readFile(t, dir, pgconf.OverrideConfFile))["restore_command"]; got != "" {
		t.Fatalf("restore_command = %q, want nothing", got)
	}
}

func TestArchiveBacklogCountsOnlyWhatIsWaiting(t *testing.T) {
	walDir := t.TempDir()
	status := filepath.Join(walDir, "archive_status")
	if err := os.MkdirAll(status, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"000000010000000000000001.ready",
		"000000010000000000000002.ready",
		"000000010000000000000003.done",
	} {
		if err := os.WriteFile(filepath.Join(status, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got := ArchiveBacklog(walDir); got != 2 {
		t.Fatalf("backlog = %d, want the two segments still waiting", got)
	}
}

// A member whose pg_wal has no archive_status directory at all has nothing queued, which is
// not the same as a measurement that failed - but reporting a fault for it would alarm on
// every member that has not started PostgreSQL yet.
func TestArchiveBacklogOfAMissingQueueIsZero(t *testing.T) {
	if got := ArchiveBacklog(t.TempDir()); got != 0 {
		t.Fatalf("backlog = %d, want zero", got)
	}
}

// The recorded failure outlives the failure it describes. Reporting a stale message beside
// a recovered archive would be worse than reporting no message at all, so it is only used
// for the segment pg_stat_archiver says actually failed.
func TestTheRecordedFailureIsOnlyUsedForTheSegmentThatFailed(t *testing.T) {
	path := writeArchiveFailure(t, "000000010000000000000007", "the bucket refused the upload")

	if got := LastArchiveFailure(path, "000000010000000000000007"); got != "the bucket refused the upload" {
		t.Errorf("message = %q", got)
	}
	if got := LastArchiveFailure(path, "000000010000000000000009"); got != "" {
		t.Errorf("a message from a different segment was reported: %q", got)
	}
	if got := LastArchiveFailure(path, ""); got != "" {
		t.Errorf("a message was reported for no failure at all: %q", got)
	}
}

// The rendered configuration is cached for the life of the Pod so that the stanza name,
// which comes from pg_controldata, does not cost a fork per archived segment. Rotating a
// credential has to invalidate it, or archiving keeps using the key that stopped working.
func TestTheCachedConfigurationIsInvalidatedByARotatedCredential(t *testing.T) {
	repository := pgbackrest.Repository{
		Path: "s3://backups/prod", RetentionFull: testRetentionWindow,
		RetentionWAL: testRetentionWindow,
	}
	original := pgbackrest.Credentials{AccessKeyID: "one", SecretAccessKey: "two"}
	rotated := pgbackrest.Credentials{AccessKeyID: "one", SecretAccessKey: "three"}

	fingerprint := repositoryFingerprint(repository, original)
	path := filepath.Join(t.TempDir(), "pgbackrest.conf")
	if err := os.WriteFile(path,
		[]byte(configMarker+fingerprint+" pgelastic-99\n[global]\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if stanza, ok := cachedStanza(path, fingerprint); !ok || stanza != "pgelastic-99" {
		t.Fatalf("cachedStanza = %q, %v; want the cached stanza to be reused", stanza, ok)
	}
	if _, ok := cachedStanza(path, repositoryFingerprint(repository, rotated)); ok {
		t.Fatal("a rotated credential still served the configuration rendered from the old one")
	}
}

func TestAnAbsentOrTruncatedConfigurationIsNotServedFromCache(t *testing.T) {
	dir := t.TempDir()
	if _, ok := cachedStanza(filepath.Join(dir, "absent.conf"), "whatever"); ok {
		t.Error("a configuration that does not exist was served from cache")
	}

	truncated := filepath.Join(dir, "truncated.conf")
	if err := os.WriteFile(truncated, []byte(configMarker+"onlyafingerprint\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := cachedStanza(truncated, "onlyafingerprint"); ok {
		t.Error("a marker with no stanza was served from cache")
	}
}

func writeArchiveFailure(t *testing.T, segment, message string) string {
	t.Helper()
	document, err := json.Marshal(ArchiveFailure{
		Segment: segment,
		At:      time.Now().UTC(),
		Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "archive-status.json")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
