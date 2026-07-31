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
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgbackrest"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// ArchiveWAL is archive_command.
//
// WAL archiving and physical backup are pgBackRest's job, behind this package's repository
// helpers, because the correctness surface - multipart uploads, retry idempotency,
// partial-object detection, timeline history, .partial segments, manifests, checksum
// verification, expiry safety - is large and every bug in it is a silent data-loss bug
// found only at restore time.
//
// Until a repository is configured there is nothing to push to. archive_mode is still on,
// because it is PGC_POSTMASTER and turning it on later costs a restart that drops every
// tenant connection on the instance; so this reports success and says plainly that nothing
// was archived, rather than failing and letting pg_wal fill up behind an instance that has
// no archive to fill.
func ArchiveWAL(ctx context.Context, options Options, segment, name string) error {
	log := logf.FromContext(ctx)
	if segment == "" || name == "" {
		return fmt.Errorf("archive_command needs both %%p and %%f")
	}

	// A restored instance carries its source's system identifier, because a physical
	// restore copies the control file verbatim, so it addresses its source's stanza while
	// running on a forked timeline. Archiving from here would interleave two histories into
	// one archive and leave neither restorable.
	//
	// This is the only guard. archive_command is rendered identically for every member,
	// restored or not - it has to be, because pgBackRest refuses to take a base backup
	// unless archive_command names it - so nothing upstream of here stops a recovery
	// instance archiving. The operator's own refusal to schedule backups against a
	// recovering instance is a separate protection for a separate hazard.
	//
	// It returns success rather than failing for the same reason as the case below: a
	// throwaway instance must still be able to recycle its own WAL, and a failing
	// archive_command fills pg_wal until the postmaster PANICs.
	if options.Config.Recovering {
		log.Info("this member was restored from a repository and does not archive",
			"segment", name, "instance", options.Instance)
		return nil
	}

	invocation, configured, err := ensureRepository(ctx, options)
	if err != nil {
		recordArchiveFailure(provision.ArchiveStatusFile, name, err)
		return err
	}
	if !configured {
		log.Info("no WAL repository is configured; the segment was not archived",
			"segment", name, "instance", options.Instance)
		return nil
	}

	// PostgreSQL passes %p relative to PGDATA and runs archive_command with PGDATA as the
	// working directory. Resolving it here means the same command also works when a person
	// runs it by hand from somewhere else, which is exactly when they are least able to
	// diagnose a path that silently referred to the wrong file.
	if !filepath.IsAbs(segment) {
		segment = filepath.Join(options.DataDir, segment)
	}

	if _, err := (pgbackrest.Runner{}).Run(ctx, invocation.ArchivePush(segment)); err != nil {
		recordArchiveFailure(provision.ArchiveStatusFile, name, err)
		return err
	}
	return nil
}

// RestoreWAL is restore_command. The rewind variant disables prefetch because pg_rewind
// walks WAL backwards in a single pass, so read-ahead is pure waste there.
//
// Unlike archiving, this is not disabled on a restored instance: fetching WAL out of the
// source's archive is the whole of what a restored instance is doing.
func RestoreWAL(ctx context.Context, options Options, name, target string, rewind bool) error {
	log := logf.FromContext(ctx)
	if name == "" || target == "" {
		return fmt.Errorf("restore_command needs both %%f and %%p")
	}

	invocation, configured, err := ensureRepository(ctx, options)
	if err != nil {
		return err
	}
	if !configured {
		log.Info("no WAL repository is configured; the segment cannot be restored",
			"segment", name, "rewind", rewind, "instance", options.Instance)
		// A non-zero exit is what tells PostgreSQL the segment is not in the archive, which
		// during recovery means "carry on with what is in pg_wal" rather than "fail".
		return fmt.Errorf("segment %s is not available: no WAL repository is configured", name)
	}

	if !filepath.IsAbs(target) {
		target = filepath.Join(options.DataDir, target)
	}
	_, err = (pgbackrest.Runner{}).Run(ctx, invocation.ArchiveGet(name, target, !rewind))
	return err
}

// ensureRepository resolves the pgBackRest invocation for this member, rendering the
// configuration file first if it is absent or no longer describes the current inputs.
//
// The rendered file is cached in the agent's emptyDir rather than rebuilt per segment. The
// stanza name is derived from pg_controldata, so rebuilding it every time would fork
// pg_controldata once per archived segment; caching it turns that into once per Pod, and
// once more whenever the credentials are rotated.
func ensureRepository(ctx context.Context, options Options) (pgbackrest.Invocation, bool, error) {
	return ensureRepositoryForStanza(ctx, options, "")
}

// ensureRepositoryForStanza is the same, for a caller that already knows the stanza.
//
// A restore is the caller that does. The stanza is named after the system identifier, and
// at restore time the data directory is empty, so there is no control file to read one out
// of - the stanza has to come from the backup being restored instead. Passing it in also
// means a restore addresses the source's stanza rather than deriving one from a data
// directory it is about to overwrite.
func ensureRepositoryForStanza(
	ctx context.Context,
	options Options,
	stanza string,
) (pgbackrest.Invocation, bool, error) {
	repository := options.Config.Backup
	if repository == nil || !repository.Configured() {
		return pgbackrest.Invocation{}, false, nil
	}

	credentials, err := loadBackupCredentials()
	if err != nil {
		return pgbackrest.Invocation{}, false, err
	}
	fingerprint := repositoryFingerprint(*repository, credentials)

	if cached, ok := cachedStanza(provision.BackupConfigFile, fingerprint); ok &&
		(stanza == "" || cached == stanza) {
		return pgbackrest.Invocation{
			ConfigFile: provision.BackupConfigFile,
			Stanza:     cached,
		}, true, nil
	}
	return renderRepository(ctx, options, *repository, credentials, fingerprint, stanza)
}

// configMarker is the first line of the generated file: the fingerprint of the inputs it
// was rendered from, and the stanza it names. Carrying the stanza here is what lets the
// cached case skip pg_controldata entirely rather than parse the file back.
const configMarker = "# pgelastic "

func cachedStanza(path, fingerprint string) (string, bool) {
	file, err := os.Open(path) // #nosec G304 -- a constant path in the agent's own emptyDir
	if err != nil {
		return "", false
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return "", false
	}
	fields := strings.Fields(strings.TrimPrefix(scanner.Text(), configMarker))
	if len(fields) != 2 || fields[0] != fingerprint {
		return "", false
	}
	return fields[1], true
}

func renderRepository(
	ctx context.Context,
	options Options,
	repository pgbackrest.Repository,
	credentials pgbackrest.Credentials,
	fingerprint, stanza string,
) (pgbackrest.Invocation, bool, error) {
	restoring := stanza != ""
	if stanza == "" {
		controlData, err := toolchain(options).ControlData(ctx, options.DataDir)
		if err != nil {
			return pgbackrest.Invocation{}, false, fmt.Errorf(
				"the archive stanza is named after the system identifier and this member's "+
					"control file could not be read: %w", err)
		}
		stanza = pgbackrest.StanzaName(controlData.SystemIdentifier)
	}

	layout := pgbackrest.Layout{
		DataDir:   options.DataDir,
		SocketDir: options.SocketDir,
		Port:      options.Config.Postgres.Port,
		SpoolPath: provision.BackupSpoolPath,
		LogPath:   provision.BackupLogPath,
	}
	// pgBackRest creates neither of these, and the failure it reports when they are missing
	// names the path without saying that nothing was going to create it.
	for _, directory := range []string{layout.SpoolPath, layout.LogPath} {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			return pgbackrest.Invocation{}, false, err
		}
	}

	body, err := pgbackrest.Render(repository, credentials, layout, stanza)
	if err != nil {
		return pgbackrest.Invocation{}, false, err
	}
	marker := configMarker + fingerprint + " " + stanza + "\n"
	if err := writeFileAtomically(provision.BackupConfigFile, marker+body); err != nil {
		return pgbackrest.Invocation{}, false, err
	}

	invocation := pgbackrest.Invocation{
		ConfigFile: provision.BackupConfigFile,
		Stanza:     stanza,
	}
	// Creating the stanza is safe to repeat: pgBackRest accepts a stanza whose recorded
	// system identifier matches this one and refuses a stanza that belongs to a different
	// database, which is the guard against two instances sharing one archive.
	//
	// It is skipped entirely when a restore supplied the stanza. That stanza belongs to the
	// source and already exists; running stanza-create against it from a member whose data
	// directory is empty is at best a no-op and at worst a write into somebody else's
	// repository from an instance that has no business writing to it at all.
	if !restoring {
		if _, err := (pgbackrest.Runner{}).Run(ctx, invocation.StanzaCreate()); err != nil {
			return pgbackrest.Invocation{}, false, err
		}
	}
	return invocation, true, nil
}

// repositoryFingerprint identifies the inputs the configuration was rendered from, so a
// rotated credential produces a rewrite instead of a file that quietly no longer works.
func repositoryFingerprint(
	repository pgbackrest.Repository,
	credentials pgbackrest.Credentials,
) string {
	digest := sha256.New()
	encoder := json.NewEncoder(digest)
	// Both values are structs of strings, so encoding cannot fail; the error is checked
	// anyway rather than discarded, because a silent failure here would fingerprint every
	// input identically and the file would never be rewritten.
	if err := encoder.Encode(repository); err != nil {
		return ""
	}
	if err := encoder.Encode(credentials); err != nil {
		return ""
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// loadBackupCredentials reads the mounted object store Secret.
//
// The CA bundle is optional and its absence is not an error: a store presenting a
// certificate the image already trusts needs none.
func loadBackupCredentials() (pgbackrest.Credentials, error) {
	read := func(key string) (string, error) {
		value, err := os.ReadFile(filepath.Join(provision.BackupCredentialsMountPath, key)) // #nosec G304 -- a constant mount path
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(value)), nil
	}

	accessKeyID, err := read(provision.SecretKeyBackupAccessKeyID)
	if err != nil {
		return pgbackrest.Credentials{}, fmt.Errorf(
			"a WAL repository is configured but its credentials are not mounted: %w", err)
	}
	secretAccessKey, err := read(provision.SecretKeyBackupSecretAccessKey)
	if err != nil {
		return pgbackrest.Credentials{}, fmt.Errorf(
			"a WAL repository is configured but its credentials are not mounted: %w", err)
	}

	credentials := pgbackrest.Credentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}
	caBundle := filepath.Join(provision.BackupCredentialsMountPath, provision.SecretKeyBackupCABundle)
	if _, err := os.Stat(caBundle); err == nil {
		credentials.CABundlePath = caBundle
	}
	return credentials, nil
}

// ArchiveFailure is what archive_command records about its own last failure.
//
// pg_stat_archiver records that a failure happened and which segment it was, never why.
// PostgreSQL discards archive_command's output, so without this file the reason a WAL
// repository stopped accepting segments would exist only in the postmaster log, and the
// instance's status would report a degraded archive with nothing to act on.
type ArchiveFailure struct {
	Segment string    `json:"segment"`
	At      time.Time `json:"at"`
	Message string    `json:"message"`
}

// recordArchiveFailure is best effort by construction. The archive attempt has already
// failed and its error is on its way back to PostgreSQL; failing to write the explanation
// as well must not turn one problem into two.
func recordArchiveFailure(path, segment string, cause error) {
	document, err := json.Marshal(ArchiveFailure{
		Segment: segment,
		At:      time.Now().UTC(),
		Message: cause.Error(),
	})
	if err != nil {
		return
	}
	_ = writeFileAtomically(path, string(document))
}

// LastArchiveFailure reads back what archive_command last recorded, for the segment
// pg_stat_archiver says failed.
//
// The segment is matched rather than trusted: the file outlives the failure it describes,
// and reporting a stale message beside a healthy archiver would be worse than reporting no
// message at all.
func LastArchiveFailure(path, segment string) string {
	if segment == "" {
		return ""
	}
	document, err := os.ReadFile(path) // #nosec G304 -- a constant path in the agent's own emptyDir
	if err != nil {
		return ""
	}
	var failure ArchiveFailure
	if err := json.Unmarshal(document, &failure); err != nil {
		return ""
	}
	if failure.Segment != segment {
		return ""
	}
	return failure.Message
}

// ArchiveBacklog counts the segments queued for archiving.
//
// Measured from the filesystem rather than asked of PostgreSQL, for the same reason the WAL
// volume's fullness is: the number matters most when the postmaster has already stopped,
// and pg_stat_archiver reports what has happened rather than what is waiting to.
func ArchiveBacklog(walDir string) int32 {
	entries, err := os.ReadDir(filepath.Join(walDir, "archive_status"))
	if err != nil {
		return 0
	}
	var ready int32
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".ready") {
			ready++
		}
	}
	return ready
}

// StatusEndpoint is a member's failsafe endpoint, addressed directly.
func StatusEndpoint(member string, options Options) string {
	return fmt.Sprintf("%s:%d", PeerHost(member, options.PeerService, options.Namespace),
		provision.StatusPort)
}
