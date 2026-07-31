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
	"strings"
	"testing"
)

// testRetention is the documented default window, and testConfigFile is where the agent
// renders the configuration inside the Pod.
const (
	testRetention  = "30d"
	testConfigFile = "/agent/pgbackrest.conf"
	testStanza     = "pgelastic-99"
)

func testLayout() Layout {
	return Layout{
		DataDir:   "/var/lib/postgresql/data/pgdata",
		SocketDir: "/var/run/postgresql",
		Port:      5432,
		SpoolPath: "/var/lib/postgresql/wal/pgbackrest-spool",
		LogPath:   "/agent/pgbackrest-log",
	}
}

func testRepository() Repository {
	return Repository{
		Path:          "s3://backups/pgelastic/prod",
		RetentionFull: testRetention,
		RetentionWAL:  testRetention,
	}
}

func render(t *testing.T, repository Repository, credentials Credentials) string {
	t.Helper()
	body, err := Render(repository, credentials, testLayout(), "pgelastic-7521834562341234567")
	if err != nil {
		t.Fatalf("Render = %v", err)
	}
	return body
}

// The stanza is the archive's identity, and it has to be the data directory's identity
// rather than the instance's. An instance deleted and recreated under a reused name is a
// different database with a different history; sharing one stanza between them interleaves
// two WAL streams and leaves neither restorable.
func TestStanzaNameFollowsTheSystemIdentifierNotTheInstance(t *testing.T) {
	recreated := StanzaName("7521834562341234567")
	predecessor := StanzaName("7409988776655443322")
	if recreated == predecessor {
		t.Fatalf("two system identifiers produced one stanza %q", recreated)
	}
}

// A restored instance copies its source's control file, so it addresses its source's
// stanza. This is asserted rather than merely known, because it is the reason a recovery
// instance has to be kept out of the archive by other means, and a future change that made
// the stanza unique per instance would silently remove that requirement's justification
// while breaking restore.
func TestARestoredInstanceAddressesItsSourceStanza(t *testing.T) {
	source := StanzaName("7521834562341234567")
	restored := StanzaName("7521834562341234567")
	if source != restored {
		t.Fatalf("stanza %q for the source and %q for a restore of it", source, restored)
	}
}

func TestStanzaNameRefusesPathSeparators(t *testing.T) {
	stanza := StanzaName("../../etc/passwd")
	if strings.ContainsAny(stanza, "./") {
		t.Fatalf("stanza %q carries a path separator into the command line", stanza)
	}
}

func TestPathSplitsIntoBucketAndPrefix(t *testing.T) {
	body := render(t, testRepository(), Credentials{})
	for _, want := range []string{
		"repo1-s3-bucket=backups",
		"repo1-path=/pgelastic/prod",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the rendered configuration does not contain %q", want)
		}
	}
}

func TestBucketRootIsARepositoryPathOfItsOwn(t *testing.T) {
	repository := testRepository()
	repository.Path = "s3://backups"
	body := render(t, repository, Credentials{})
	if !strings.Contains(body, "repo1-path=/\n") {
		t.Errorf("a bucket with no prefix did not render the root path:\n%s", body)
	}
}

func TestPathIsRefusedWhenItNamesNoBucketOrIsNotS3(t *testing.T) {
	for _, path := range []string{"", "s3://", "https://backups/prefix", "/backups/prefix"} {
		repository := testRepository()
		repository.Path = path
		if _, err := Render(repository, Credentials{}, testLayout(), "stanza"); err == nil {
			t.Errorf("Render accepted the object store path %q", path)
		}
	}
}

// Asynchronous archiving is not a tuning knob here. pgelastic packs many tenants onto one
// instance, and a synchronous archive_command pushes one segment at a time.
func TestArchivingIsAsynchronousAndSpooledOffTheDataVolume(t *testing.T) {
	body := render(t, testRepository(), Credentials{})
	if !strings.Contains(body, "archive-async=y") {
		t.Error("archiving is synchronous")
	}
	if !strings.Contains(body, "spool-path="+testLayout().SpoolPath) {
		t.Error("the spool is not on the WAL volume")
	}
}

// archive-push-queue-max makes archive_command report success and discard the segment once
// the queue is over its limit. That trades a full pg_wal, which is an outage somebody
// notices, for a hole in the archive, which is an unrestorable instance nobody finds out
// about until they try to restore it.
func TestTheQueueLimitThatDiscardsWALIsNeverSet(t *testing.T) {
	body := render(t, testRepository(), Credentials{})
	if strings.Contains(body, "archive-push-queue-max") {
		t.Fatal("archive-push-queue-max is set, which lets archiving drop segments silently")
	}
}

func TestCredentialsAreWrittenIntoTheConfigurationNotLeftOut(t *testing.T) {
	body := render(t, testRepository(), Credentials{
		AccessKeyID:     "objectstore",
		SecretAccessKey: "objectstore123",
		CABundlePath:    "/etc/pgelastic-backup/ca.crt",
	})
	for _, want := range []string{
		"repo1-s3-key=objectstore",
		"repo1-s3-key-secret=objectstore123",
		"repo1-storage-ca-file=/etc/pgelastic-backup/ca.crt",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the rendered configuration does not contain %q", want)
		}
	}
}

func TestNoCAFileIsEmittedWhenTheStoreNeedsNone(t *testing.T) {
	body := render(t, testRepository(), Credentials{AccessKeyID: "a", SecretAccessKey: "b"})
	if strings.Contains(body, "repo1-storage-ca-file") {
		t.Error("a CA file was configured without one being supplied")
	}
}

// A custom endpoint means an S3-compatible store, and those are addressed path-style:
// virtual-host style needs a wildcard DNS record per bucket, which a store running inside
// the cluster does not have.
func TestACustomEndpointSelectsPathStyleAndSplitsOutThePort(t *testing.T) {
	repository := testRepository()
	repository.EndpointURL = "https://objectstore.pgelastic-e2e.svc:9000"
	body := render(t, repository, Credentials{})
	for _, want := range []string{
		"repo1-s3-endpoint=objectstore.pgelastic-e2e.svc",
		"repo1-s3-uri-style=path",
		"repo1-storage-port=9000",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the rendered configuration does not contain %q", want)
		}
	}
}

func TestNoEndpointLeavesTheProviderDefault(t *testing.T) {
	body := render(t, testRepository(), Credentials{})
	if strings.Contains(body, "repo1-s3-endpoint") || strings.Contains(body, "repo1-s3-uri-style") {
		t.Error("an endpoint was configured for a repository that named none")
	}
}

func TestRetentionIsExpressedAsARecoveryWindowInDays(t *testing.T) {
	for _, testCase := range []struct {
		window string
		days   string
	}{
		{testRetention, "30"},
		{"4w", "28"},
		{"6m", "180"},
	} {
		repository := testRepository()
		repository.RetentionFull = testCase.window
		repository.RetentionWAL = testCase.window
		body := render(t, repository, Credentials{})
		if !strings.Contains(body, "repo1-retention-full-type=time") {
			t.Errorf("%s: retention is not time based", testCase.window)
		}
		if !strings.Contains(body, "repo1-retention-full="+testCase.days+"\n") {
			t.Errorf("%s: retention is not %s days:\n%s", testCase.window, testCase.days, body)
		}
	}
}

// WAL expiring before the full backup it belongs to leaves a base backup that cannot be
// replayed to consistency. The repository looks healthy right up until somebody restores.
func TestWALRetentionShorterThanFullRetentionIsRefused(t *testing.T) {
	repository := testRepository()
	repository.RetentionFull = testRetention
	repository.RetentionWAL = "7d"
	if _, err := Render(repository, Credentials{}, testLayout(), "stanza"); err == nil {
		t.Fatal("Render accepted a WAL window shorter than the full-backup window")
	}
}

func TestRetentionRefusesWhatItCannotMean(t *testing.T) {
	for _, window := range []string{"", "30", "d", "0d", "-5d", "30y", "thirty days"} {
		if _, err := ParseRetention(window); err == nil {
			t.Errorf("ParseRetention accepted %q", window)
		}
	}
}

func TestEveryCommandCarriesItsConfigAndStanza(t *testing.T) {
	invocation := Invocation{ConfigFile: testConfigFile, Stanza: testStanza}
	for name, command := range map[string]Command{
		"stanza-create": invocation.StanzaCreate(),
		"check":         invocation.Check(),
		"archive-push":  invocation.ArchivePush("pg_wal/000000010000000000000003"),
		"archive-get":   invocation.ArchiveGet("000000010000000000000003", "pg_wal/RECOVERYXLOG", true),
		"info":          invocation.Info(),
	} {
		if !slices.Contains(command.Args, "--config="+testConfigFile) {
			t.Errorf("%s does not name the configuration file: %v", name, command.Args)
		}
		if !slices.Contains(command.Args, "--stanza="+testStanza) {
			t.Errorf("%s does not name the stanza: %v", name, command.Args)
		}
	}
}

// pg_rewind walks WAL backwards in a single pass, so every prefetched segment is fetched
// and discarded, and the miss that ends the walk has to be reported at once rather than
// after the prefetcher has finished.
func TestPrefetchIsDisabledForARewindAndOnlyForARewind(t *testing.T) {
	invocation := Invocation{ConfigFile: testConfigFile, Stanza: testStanza}

	rewind := invocation.ArchiveGet("000000010000000000000003", "/pgdata/pg_wal/RECOVERYXLOG", false)
	if !slices.Contains(rewind.Args, "--no-archive-async") {
		t.Errorf("a rewind fetch still prefetches: %v", rewind.Args)
	}

	recovery := invocation.ArchiveGet("000000010000000000000003", "/pgdata/pg_wal/RECOVERYXLOG", true)
	if slices.Contains(recovery.Args, "--no-archive-async") {
		t.Errorf("a recovery fetch does not prefetch: %v", recovery.Args)
	}
}

// The order matters: pgBackRest reads archive-get's operands as %f then %p, and swapping
// them writes the segment name into a file named after the destination.
func TestArchiveGetPassesTheSegmentBeforeTheDestination(t *testing.T) {
	invocation := Invocation{ConfigFile: testConfigFile, Stanza: testStanza}
	args := invocation.ArchiveGet("000000010000000000000003", "/pgdata/pg_wal/RECOVERYXLOG", true).Args
	segment := slices.Index(args, "000000010000000000000003")
	destination := slices.Index(args, "/pgdata/pg_wal/RECOVERYXLOG")
	if segment < 0 || destination < 0 || segment > destination {
		t.Fatalf("archive-get operands are out of order: %v", args)
	}
}
