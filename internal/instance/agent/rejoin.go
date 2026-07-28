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
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgtool"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// Rewind retry schedule: four attempts, backing off five seconds and doubling. Transient
// failures here are network and I/O ones, which is worth a handful of retries; anything
// that survives all four is a divergence a rewind cannot bridge, and the answer to that is
// a re-clone rather than a fifth attempt.
const (
	rewindAttempts    = 4
	rewindBackoff     = 5 * time.Second
	rewindTimeout     = 30 * time.Minute
	cleanStopTimeout  = 10 * time.Minute
	controlFileBackup = "global/pg_control.pgelastic-backup"
)

// controlFile is the file whose loss is unrecoverable, which is why it is guarded.
const controlFile = "global/pg_control"

// postmasterLockFile records the PID of the postmaster that owns a data directory.
const postmasterLockFile = "postmaster.pid"

// Rejoin brings a member that used to be a primary back as a standby of the new one.
//
// It always tries pg_rewind and always has an automatic re-clone behind it. A rewind is
// minutes of WAL where a re-clone is the whole data directory, but a rewind that cannot
// reach a common ancestor is not a failure to be retried - it is a divergence, and the only
// correct answer to a divergence is to take the new primary's history wholesale.
func Rejoin(ctx context.Context, options Options, tools pgtool.Toolchain, primary string) error {
	log := logf.FromContext(ctx)
	host := PeerHost(primary, options.PeerService, options.Namespace)

	// Anything still waiting in archive_status belongs to the postmaster that just died. It
	// is archived before the rewind because a rewind removes exactly the segments that have
	// not been archived, and once removed they are gone from every copy that will ever
	// exist.
	if err := archivePendingWAL(ctx, options); err != nil {
		log.Error(err, "could not drain the archive backlog before rewinding")
	}

	if err := ensureCleanShutdown(ctx, options, tools); err != nil {
		log.Error(err, "the data directory could not be shut down cleanly; re-cloning instead")
		return join(ctx, options, tools, primary)
	}

	if err := guardControlFile(options.DataDir); err != nil {
		return err
	}
	err := rewindWithRetries(ctx, options, tools, host)
	restored, restoreErr := restoreControlFileIfEmpty(options.DataDir)
	if restoreErr != nil {
		return restoreErr
	}
	if restored {
		// pg_rewind can leave a zero-length pg_control behind, which no amount of
		// PostgreSQL machinery can reconstruct. Putting the backup back is what turns that
		// from an unrecoverable data directory into a failed attempt.
		log.Info("pg_rewind left an empty control file; the guarded copy was restored")
	}
	if err != nil {
		log.Error(err, "pg_rewind did not succeed; re-cloning from the primary", "primary", primary)
		return join(ctx, options, tools, primary)
	}
	_ = os.Remove(filepath.Join(options.DataDir, controlFileBackup))

	log.Info("rewound onto the new primary's history", "primary", primary)
	return followPrimary(options, host)
}

func rewindWithRetries(
	ctx context.Context,
	options Options,
	tools pgtool.Toolchain,
	host string,
) error {
	log := logf.FromContext(ctx)
	connInfo := RewindConnInfo(host, options.RewindPassword)
	backoff := rewindBackoff

	var lastErr error
	for attempt := 1; attempt <= rewindAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, rewindTimeout)
		err := runWithPassword(attemptCtx, options.RewindPassword, func(ctx context.Context) error {
			return tools.Rewind(ctx, connInfo)
		})
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		log.Info("pg_rewind failed", "attempt", attempt, "error", err.Error())
		if attempt == rewindAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return lastErr
}

// RewindConnInfo is the connection string pg_rewind dials the new primary with. The password
// is passed through the environment rather than on the command line, so this carries only
// the parts that are safe in a process listing.
func RewindConnInfo(host, _ string) string {
	return fmt.Sprintf("host=%s port=%d user=%s dbname=postgres",
		host, provision.PostgresPort, provision.RewindRole)
}

// ensureCleanShutdown gets the data directory into a state pg_rewind will accept.
//
// A member whose postmaster was killed is "in production" as far as its control file is
// concerned, and pg_rewind refuses that outright. Starting it privately and stopping it
// again runs the crash recovery that makes the control file honest, over a socket nothing
// outside this pod can reach and with archiving off so a half-recovered instance cannot
// push anything into the archive.
func ensureCleanShutdown(ctx context.Context, options Options, tools pgtool.Toolchain) error {
	data, err := tools.ControlData(ctx, options.DataDir)
	if err != nil {
		return err
	}
	if pgtool.ClusterStateShutDown(data.ClusterState) {
		return nil
	}
	log := logf.FromContext(ctx)
	log.Info("running crash recovery before the rewind", "clusterState", data.ClusterState)
	if err := os.MkdirAll(bootstrapSocketDir, 0o700); err != nil {
		return err
	}
	// The lock file belongs to a postmaster that is definitively dead: this runs before any
	// postmaster of ours exists, on a data directory inherited from a Pod that is gone.
	// Leaving it means PostgreSQL compares the recorded PID against this container's own
	// namespace, finds something unrelated holding it, and refuses to start at all.
	if err := os.Remove(filepath.Join(options.DataDir, postmasterLockFile)); err != nil &&
		!os.IsNotExist(err) {
		return err
	}

	// The postmaster is spawned directly rather than through pg_ctl start -o, because
	// pg_ctl splits that option string on whitespace and an empty value has to survive the
	// split. Getting it wrong does not fail loudly: the postmaster refuses the parameter,
	// pg_ctl waits out its whole timeout, and the rejoin looks like a hang.
	command := exec.CommandContext(ctx, filepath.Join(options.BinDir, postmasterExecutable),
		"-D", options.DataDir,
		"-c", "listen_addresses=",
		"-c", "unix_socket_directories="+bootstrapSocketDir,
		"-c", "archive_mode=off",
		"-c", "logging_collector=off",
		"-c", "log_destination=stderr",
		"-c", "synchronous_standby_names=",
	)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Start(); err != nil {
		return err
	}

	waitCtx, cancel := context.WithTimeout(ctx, cleanStopTimeout)
	defer cancel()
	conn, err := WaitForPostmaster(waitCtx, bootstrapSocketDir, provision.PostgresPort)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return fmt.Errorf("crash recovery never finished: %w", err)
	}
	_ = conn.Close(waitCtx)

	if err := tools.Stop(ctx, pgtool.StopFast, cleanStopTimeout); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return err
	}
	_ = command.Wait()
	log.Info("crash recovery finished; the data directory is shut down cleanly")
	return nil
}

// guardControlFile copies global/pg_control aside before anything is allowed to rewrite it.
func guardControlFile(dataDir string) error {
	contents, err := os.ReadFile(filepath.Join(dataDir, controlFile))
	if err != nil {
		return fmt.Errorf("reading the control file before rewinding: %w", err)
	}
	if len(contents) == 0 {
		return fmt.Errorf("the control file in %s is already empty", dataDir)
	}
	return os.WriteFile(filepath.Join(dataDir, controlFileBackup), contents, 0o600)
}

// restoreControlFileIfEmpty puts the guarded copy back when the rewind left nothing behind.
func restoreControlFileIfEmpty(dataDir string) (bool, error) {
	path := filepath.Join(dataDir, controlFile)
	info, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}
	if err == nil && info.Size() > 0 {
		return false, nil
	}
	backup, err := os.ReadFile(filepath.Join(dataDir, controlFileBackup))
	if err != nil {
		return false, err
	}
	return true, os.WriteFile(path, backup, 0o600)
}

// archivePendingWAL pushes every segment still marked ready by the dead postmaster.
//
// The segments in archive_status are the ones that were written but never archived. A
// rewind is about to discard whichever of them belong to the diverged history, and there is
// no other copy: the primary that produced them is this node, and it is about to stop being
// the authority on its own past.
func archivePendingWAL(ctx context.Context, options Options) error {
	statusDir := filepath.Join(options.WALDir, "archive_status")
	entries, err := os.ReadDir(statusDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	log := logf.FromContext(ctx)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".ready") {
			continue
		}
		segment := strings.TrimSuffix(name, ".ready")
		if err := ArchiveWAL(ctx, options, filepath.Join(options.WALDir, segment), segment); err != nil {
			return fmt.Errorf("archiving %s before the rewind: %w", segment, err)
		}
		if err := os.Rename(filepath.Join(statusDir, name),
			filepath.Join(statusDir, segment+".done")); err != nil {
			return err
		}
		log.Info("archived a segment the dead postmaster left behind", "segment", segment)
	}
	return nil
}

// followPrimary rewrites this member's replication configuration to stream from the new
// primary and puts it back into recovery.
func followPrimary(options Options, host string) error {
	replication := pgconf.ReplicationConfig{
		Standby:         true,
		PrimaryConnInfo: PrimaryConnInfo(host, options.Member, options.ReplicationPassword),
		PrimarySlotName: provision.ReplicationSlotName(options.Member),
	}
	if err := EnsureIncludes(options.DataDir); err != nil {
		return err
	}
	if _, err := WriteConfig(options.Config, options.Member,
		replication, options.DataDir, nil); err != nil {
		return err
	}
	return SetStandbySignal(options.DataDir, true)
}
