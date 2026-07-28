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
	"path/filepath"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// promoteTimeout bounds pg_ctl promote itself. The check that follows it has its own,
// longer deadline.
const promoteTimeout = 60 * time.Second

// Promote takes a standby out of recovery.
//
// This is the local half only. The full promotion sequence - acquire the Lease, re-verify
// the quorum evidence, promote, CHECKPOINT, rewrite synchronous_standby_names before
// accepting writes, bump the epoch, terminate stale backends, write currentPrimary - is
// the failover state machine's, and is deliberately not started from here: a promotion
// that can be triggered locally is a promotion that can happen without the quorum gate.
func Promote(ctx context.Context, options Options) error {
	log := logf.FromContext(ctx)
	tools := toolchain(options)

	if err := tools.Promote(ctx, promoteTimeout); err != nil {
		return err
	}

	// PostgreSQL removes standby.signal itself. It is never deleted by hand, and its
	// continued existence past the deadline is a hard failure rather than something to be
	// tidied away: the file still being there means the promotion did not happen.
	deadline := time.Now().Add(promoteTimeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(options.DataDir, StandbySignal)); os.IsNotExist(err) {
			log.Info("promotion completed", "member", options.Member)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("standby.signal still exists %s after promoting %s", promoteTimeout, options.Member)
}

// ArchiveWAL is archive_command.
//
// WAL archiving and physical backup are pgBackRest's job, behind the WalArchive and
// BackupStore interfaces, because the correctness surface - multipart uploads, retry
// idempotency, partial-object detection, timeline history, .partial segments, manifests,
// checksum verification, expiry safety - is large and every bug in it is a silent
// data-loss bug found only at restore time.
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
	log.Info("no WAL repository is configured; the segment was not archived",
		"segment", name, "instance", options.Instance)
	return nil
}

// RestoreWAL is restore_command. The rewind variant disables prefetch because pg_rewind
// walks WAL backwards in a single pass, so read-ahead is pure waste there.
func RestoreWAL(ctx context.Context, options Options, name, target string, rewind bool) error {
	log := logf.FromContext(ctx)
	if name == "" || target == "" {
		return fmt.Errorf("restore_command needs both %%f and %%p")
	}
	log.Info("no WAL repository is configured; the segment cannot be restored",
		"segment", name, "rewind", rewind, "instance", options.Instance)
	// A non-zero exit is what tells PostgreSQL the segment is not in the archive, which
	// during recovery means "carry on with what is in pg_wal" rather than "fail".
	return fmt.Errorf("segment %s is not available: no WAL repository is configured", name)
}

// StatusEndpoint is a member's failsafe endpoint, addressed directly.
func StatusEndpoint(member string, options Options) string {
	return fmt.Sprintf("%s:%d", PeerHost(member, options.PeerService, options.Namespace),
		provision.StatusPort)
}
