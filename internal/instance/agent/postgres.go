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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// LocalDSN addresses the postmaster over the Unix socket as the bootstrap superuser.
// There is no password anywhere in it, and there is none to find: the superuser is never
// given one, and peer authentication over a socket in an emptyDir is the only way in.
func LocalDSN(socketDir string, port int32, database string) string {
	return fmt.Sprintf("host=%s port=%d user=postgres dbname=%s", socketDir, port, database)
}

// Connect opens one short-lived superuser connection over the Unix socket.
func Connect(ctx context.Context, socketDir string, port int32) (*pgx.Conn, error) {
	return pgx.Connect(ctx, LocalDSN(socketDir, port, "postgres"))
}

// MemberObservation is what one member's agent knows about itself. It is the agent that
// reports it, because the agent is the only party that can read pg_is_in_recovery() on
// this member without a network hop that the failure being diagnosed may have removed.
type MemberObservation struct {
	Role Role
	// LSN is the member's headline position: the current insert position on a primary, the
	// replay position on a standby.
	LSN string
	// ReceivedLSN and ReplayLSN are reported separately because candidate selection orders
	// on both, received first. WAL that has been received is durable on this member whether
	// or not recovery has replayed it yet.
	ReceivedLSN string
	ReplayLSN   string
	// Timeline is the highest timeline this member holds WAL for: the control file's, the
	// one recovery last replayed on, or the one its WAL receiver is streaming on, whichever
	// is furthest ahead. The control file alone lags a timeline switch by a whole
	// restartpoint, which would make a standby that has already received the new history
	// look like one that had been left behind on the old.
	Timeline int32
	// MinRecoveryEndLSN is pg_control_recovery()'s durable record of how far recovery has
	// got. It is carried alongside the two live positions because it is the only one that
	// survives a postmaster restart, and divergence has to be decidable on a member whose
	// WAL receiver has never once succeeded in this postmaster's lifetime.
	MinRecoveryEndLSN string
	ReplayLag         time.Duration
	WALReceiverActive bool
	// WALVolumeFull is measured from the filesystem rather than read from PostgreSQL, so
	// that the answer survives a postmaster that has already stopped - which is precisely
	// the state a candidate is in when the veto has to be evaluated.
	WALVolumeFull bool
	// DataUsedBytes and WALUsedBytes are what the two volumes are using, from the same
	// measurement that decides WALVolumeFull. Carried rather than dropped because
	// status.storage.used has no other producer, and the autoscaler cannot expand a volume
	// whose usage it cannot see.
	DataUsedBytes int64
	WALUsedBytes  int64
	// SyncStandbys and StreamingStandbys come from pg_stat_replication and describe what is
	// happening right now.
	SyncStandbys      []string
	StreamingStandbys []string
	// SyncStandbyNames is the clause the postmaster loaded, and VotingMembers and NumSync
	// are parsed out of it. They are the quorum gate's N and W, and they deliberately come
	// from the loaded value rather than from pg_stat_replication: a member PostgreSQL never
	// loaded as a voter is not a voter, however healthily it happens to be streaming.
	SyncStandbyNames string
	NumSync          int32
	VotingMembers    []string
	ConfigSHA256     string
	PendingRestart   bool
	MaxConnections   int32
	// ClientBackends is how many client connections the postmaster is holding right now,
	// counted as PostgreSQL counts them rather than as the proxy believes it opened them.
	ClientBackends int32
	PrimaryEpoch   int64
	// Archive is this member's view of its own WAL archiving.
	Archive ArchiveObservation
}

// ArchiveState is the answer to "is WAL archiving working", and it has three values rather
// than two.
//
// An archive nothing has ever been written to is not a failing archive. Collapsing that
// case into failure would make every freshly bootstrapped primary report a fault it does
// not have; collapsing it into success would report an archive as healthy before anything
// had ever proved it was reachable. It is its own state because it is the only one from
// which a primary can bootstrap the archive by switching a segment.
type ArchiveState string

const (
	// ArchiveWorking means the last archive attempt succeeded more recently than the last
	// failure.
	ArchiveWorking ArchiveState = "working"
	// ArchiveFailing means the reverse.
	ArchiveFailing ArchiveState = "failing"
	// ArchiveNeverRun means no segment has ever been archived or failed to archive.
	ArchiveNeverRun ArchiveState = "neverRun"
)

// ArchiveObservation is pg_stat_archiver as this member reports it, plus the two things
// pg_stat_archiver does not record: how many segments are queued, and why the last failure
// happened.
type ArchiveObservation struct {
	State           ArchiveState
	LastArchivedWAL string
	LastArchivedAt  *time.Time
	FailedCount     int64
	LastFailedWAL   string
	LastFailureAt   *time.Time
	// LastFailureMessage comes from what archive_command recorded, because
	// pg_stat_archiver records that a failure happened and never what it was.
	LastFailureMessage string
	// ReadyBacklog is how many segments are waiting to be archived, counted from the
	// filesystem so that the answer survives a postmaster that has already stopped.
	ReadyBacklog int32
}

// Healthy folds the three inputs into the summary the admission and migration gates read.
//
// State alone is not enough, because the worst failure does not set it. An archive_command
// that hangs rather than fails records neither a success nor a failure, so
// pg_stat_archiver goes on reporting the last success and an archive that has stopped
// working is indistinguishable from one that is merely idle.
//
// What separates them is the queue. Segments waiting while nothing has been archived for a
// long time is a stall; no segments waiting is an instance with nothing to archive, which
// is not a fault. Staleness on its own is never evidence: archive_timeout only switches a
// segment when there has been WAL activity since the last switch, so a quiet instance
// archives nothing for as long as it stays quiet and is perfectly healthy while it does.
func (o ArchiveObservation) Healthy(now time.Time) bool {
	if o.State != ArchiveWorking {
		return false
	}
	if o.ReadyBacklog == 0 {
		return true
	}
	// A queue that has never archived anything at all has no last-success to age, and a
	// queue is exactly what makes that a stall rather than an idle instance.
	if o.LastArchivedAt == nil {
		return false
	}
	return now.Sub(*o.LastArchivedAt) < provision.ArchiveStallAfter
}

// archiveState folds the two timestamps into the three-way answer.
func archiveState(archived, failed *time.Time) ArchiveState {
	switch {
	case archived == nil && failed == nil:
		return ArchiveNeverRun
	case failed == nil:
		return ArchiveWorking
	case archived == nil:
		return ArchiveFailing
	case archived.After(*failed):
		return ArchiveWorking
	default:
		return ArchiveFailing
	}
}

// Observe reads the member's own state out of the running postmaster.
func Observe(ctx context.Context, conn *pgx.Conn) (MemberObservation, error) {
	observation := MemberObservation{Role: RoleUnknown}

	var inRecovery bool
	if err := conn.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery); err != nil {
		return observation, err
	}
	observation.Role = RolePrimary
	if inRecovery {
		observation.Role = RoleReplica
	}

	const stateQuery = `
		SELECT COALESCE(CASE WHEN pg_is_in_recovery()
		                     THEN pg_last_wal_replay_lsn()
		                     ELSE pg_current_wal_lsn() END::text, ''),
		       COALESCE(CASE WHEN pg_is_in_recovery()
		                     THEN pg_last_wal_receive_lsn()
		                     ELSE pg_current_wal_lsn() END::text, ''),
		       COALESCE(CASE WHEN pg_is_in_recovery()
		                     THEN pg_last_wal_replay_lsn()
		                     ELSE pg_current_wal_lsn() END::text, ''),
		       GREATEST((SELECT timeline_id FROM pg_control_checkpoint()),
		                (SELECT min_recovery_end_timeline FROM pg_control_recovery()),
		                COALESCE((SELECT received_tli FROM pg_stat_wal_receiver), 0)),
		       COALESCE((SELECT min_recovery_end_lsn::text FROM pg_control_recovery()), ''),
		       COALESCE(EXTRACT(epoch FROM now() - pg_last_xact_replay_timestamp()), 0)::float8,
		       EXISTS (SELECT 1 FROM pg_stat_wal_receiver),
		       COALESCE(current_setting('pgelastic.config_sha256', true), ''),
		       EXISTS (SELECT 1 FROM pg_settings WHERE pending_restart),
		       current_setting('max_connections')::int,
		       (SELECT count(*) FROM pg_stat_activity
		         WHERE backend_type = 'client backend')::int,
		       current_setting('synchronous_standby_names'),
		       COALESCE(current_setting('pgelastic.primary_epoch', true), '0')::bigint,
		       COALESCE((SELECT last_archived_wal FROM pg_stat_archiver), ''),
		       (SELECT last_archived_time FROM pg_stat_archiver),
		       COALESCE((SELECT failed_count FROM pg_stat_archiver), 0),
		       COALESCE((SELECT last_failed_wal FROM pg_stat_archiver), ''),
		       (SELECT last_failed_time FROM pg_stat_archiver)`
	var lagSeconds float64
	err := conn.QueryRow(ctx, stateQuery).Scan(
		&observation.LSN,
		&observation.ReceivedLSN,
		&observation.ReplayLSN,
		&observation.Timeline,
		&observation.MinRecoveryEndLSN,
		&lagSeconds,
		&observation.WALReceiverActive,
		&observation.ConfigSHA256,
		&observation.PendingRestart,
		&observation.MaxConnections,
		&observation.ClientBackends,
		&observation.SyncStandbyNames,
		&observation.PrimaryEpoch,
		&observation.Archive.LastArchivedWAL,
		&observation.Archive.LastArchivedAt,
		&observation.Archive.FailedCount,
		&observation.Archive.LastFailedWAL,
		&observation.Archive.LastFailureAt,
	)
	if err != nil {
		return observation, err
	}
	observation.ReplayLag = time.Duration(lagSeconds * float64(time.Second))
	observation.Archive.State = archiveState(
		observation.Archive.LastArchivedAt, observation.Archive.LastFailureAt)
	observation.NumSync, observation.VotingMembers = ParseSyncStandbyNames(observation.SyncStandbyNames)

	if observation.Role != RolePrimary {
		return observation, nil
	}
	rows, err := conn.Query(ctx,
		`SELECT application_name, state, sync_state FROM pg_stat_replication ORDER BY application_name`)
	if err != nil {
		return observation, err
	}
	defer rows.Close()
	for rows.Next() {
		var name, state, syncState string
		if err := rows.Scan(&name, &state, &syncState); err != nil {
			return observation, err
		}
		if state != "streaming" {
			continue
		}
		observation.StreamingStandbys = append(observation.StreamingStandbys, name)
		if syncState == "sync" || syncState == "quorum" {
			observation.SyncStandbys = append(observation.SyncStandbys, name)
		}
	}
	return observation, rows.Err()
}

// CollationContract reads the immutable text-handling and on-disk identity tuple.
//
// It is recorded so that a migration or a pool join whose tuple differs can be refused
// outright. Restoring under a different collation produces indexes silently inconsistent
// with their heap ordering: no error, wrong results, discovered by a customer.
func CollationContract(ctx context.Context, conn *pgx.Conn) (Contract, error) {
	const query = `
		SELECT pg_encoding_to_char(d.encoding),
		       d.datlocprovider::text,
		       COALESCE(d.datlocale, ''),
		       d.datcollate,
		       d.datctype,
		       COALESCE(d.daticurules, ''),
		       (SELECT system_identifier::text FROM pg_control_system()),
		       (SELECT setting::bigint FROM pg_settings WHERE name = 'wal_segment_size'),
		       current_setting('data_checksums')::boolean
		FROM pg_database d WHERE d.datname = current_database()`
	var contract Contract
	err := conn.QueryRow(ctx, query).Scan(
		&contract.Encoding,
		&contract.LocaleProvider,
		&contract.Locale,
		&contract.Collate,
		&contract.Ctype,
		&contract.ICURules,
		&contract.SystemIdentifier,
		&contract.WALSegmentSize,
		&contract.DataChecksums,
	)
	return contract, err
}

// Contract is the collation contract as read from the running instance.
type Contract struct {
	Encoding         string
	LocaleProvider   string
	Locale           string
	Collate          string
	Ctype            string
	ICURules         string
	WALSegmentSize   int64
	DataChecksums    bool
	SystemIdentifier string
}

// BootstrapRoles creates the two non-superuser roles the control plane and the standbys
// use.
//
// Every statement runs with synchronous_commit set to local for the transaction. Bootstrap
// happens before any standby exists, so a synchronous commit here would wait forever for a
// quorum that cannot yet be formed - a deadlock produced by the durability setting that
// makes the product correct in every other circumstance.
func BootstrapRoles(ctx context.Context, conn *pgx.Conn, replicationPassword, opsPassword, rewindPassword string) error {
	statements := make([]string, 0, 9)
	statements = append(statements,
		`SET LOCAL synchronous_commit = 'local'`,
		upsertRole(provision.ReplicationRole, replicationPassword, "REPLICATION"),
		upsertRole(provision.OpsRole, opsPassword, ""),
		fmt.Sprintf(`GRANT pg_monitor, pg_signal_backend, pg_use_reserved_connections TO %s`,
			provision.OpsRole),
		upsertRole(provision.RewindRole, rewindPassword, ""),
	)
	// The maintenance databases stop admitting PUBLIC.
	//
	// Harmless while tenant roles were passwordless and could not open a session anywhere.
	// Once the proxy authenticates as them it is a cross-tenant leak: a tenant role locked out
	// of every tenant database could still connect to postgres and read pg_database for every
	// tenant's database name, pg_roles for every tenant's role names, pg_shdescription for the
	// migration stamps that name them, and pg_stat_activity for who is connected where. None
	// of that is its own data, and none of it needs a single privilege to read.
	//
	// template1 is included because a database created from it inherits its ACL, so leaving it
	// open would hand the same opening to every database made afterwards.
	for _, database := range []string{"postgres", "template1"} {
		statements = append(statements,
			fmt.Sprintf(`REVOKE CONNECT ON DATABASE %s FROM PUBLIC`, database),
			fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s, %s, %s`,
				database, provision.OpsRole, provision.ReplicationRole, provision.RewindRole),
		)
	}
	// pg_rewind reads the source's data directory over an ordinary connection, so the role
	// it dials as needs exactly these four functions and nothing else. Granting it a
	// predefined role instead, or superuser, would hand file-read access far wider than the
	// data directory to a credential that lives in every member's environment.
	for _, function := range []string{
		"pg_ls_dir(text,boolean,boolean)",
		"pg_stat_file(text,boolean)",
		"pg_read_binary_file(text)",
		"pg_read_binary_file(text,bigint,bigint,boolean)",
	} {
		statements = append(statements,
			fmt.Sprintf(`GRANT EXECUTE ON FUNCTION %s TO %s`, function, provision.RewindRole))
	}

	transaction, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = transaction.Rollback(ctx) }()
	for _, statement := range statements {
		if _, err := transaction.Exec(ctx, statement); err != nil {
			return fmt.Errorf("bootstrap statement failed: %w", err)
		}
	}
	return transaction.Commit(ctx)
}

// Checkpoint forces a checkpoint. On a primary about to stop it is not optional: without
// it, a pg_rewind against this node after somebody else is promoted computes the wrong
// divergence point.
func Checkpoint(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, "CHECKPOINT")
	return err
}

// SynchronousStandbyNames builds the clause from the standbys that are actually streaming
// right now.
//
// It grows rather than being written once at bootstrap, and that ordering is the whole
// point: a primary that starts alone with both standbys already named would block every
// commit under dataDurability Required, including the bootstrap statements that create the
// roles the standbys need in order to connect. Patroni's rule is to increase the quorum set
// before numsync and to decrease numsync before the quorum set, never both in one reload,
// and each step is a millisecond SIGHUP.
func SynchronousStandbyNames(quorum string, streaming []string) string {
	if len(streaming) == 0 {
		return ""
	}
	members := slices.Clone(streaming)
	slices.Sort(members)
	quoted := make([]string, 0, len(members))
	for _, member := range members {
		quoted = append(quoted, quoteIdentifier(member))
	}
	return fmt.Sprintf("%s (%s)", quorum, strings.Join(quoted, ","))
}

// ConvergeSyncMembers decides the quorum set for the next reload.
//
// Under dataDurability Required the answer is the union of what PostgreSQL already loaded
// and what is streaming now, intersected with the members this instance is allowed to have.
// Growing is safe - a named standby that has not connected yet blocks commits, which is the
// declared contract - while shrinking is not: dropping a standby the moment it stops
// streaming turns a stalled commit into a silently asynchronous one, and nobody outside the
// server can tell the difference afterwards.
//
// Under Preferred the set follows what is streaming, because degrading to asynchronous
// replication is precisely what that setting asks for.
func ConvergeSyncMembers(loaded, streaming, members []string, required bool) []string {
	allowed := make(map[string]bool, len(members))
	for _, member := range members {
		allowed[member] = true
	}
	keep := make(map[string]bool, len(streaming))
	for _, member := range streaming {
		if allowed[member] {
			keep[member] = true
		}
	}
	if required {
		for _, member := range loaded {
			if allowed[member] {
				keep[member] = true
			}
		}
	}
	converged := make([]string, 0, len(keep))
	for member := range keep {
		converged = append(converged, member)
	}
	slices.Sort(converged)
	return converged
}

// ParseSyncStandbyNames extracts W and the voting members from a loaded
// synchronous_standby_names clause.
//
// The value it is given must come from the running postmaster, never from the spec: the
// R + W > N gate exists to be evaluated against what PostgreSQL actually loaded, so that a
// partially applied reload cannot fool it. An empty or unparseable clause yields a zero
// numsync, and empty evidence denies failover rather than permitting it.
func ParseSyncStandbyNames(value string) (int32, []string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	open := strings.Index(value, "(")
	closing := strings.LastIndex(value, ")")
	if open < 0 || closing < open {
		return 0, nil
	}
	prefix := strings.Fields(strings.TrimSpace(value[:open]))
	numSync := int64(1)
	if len(prefix) > 0 {
		parsed, err := strconv.ParseInt(prefix[len(prefix)-1], 10, 32)
		if err != nil {
			return 0, nil
		}
		numSync = parsed
	}
	var members []string
	for member := range strings.SplitSeq(value[open+1:closing], ",") {
		member = strings.TrimSpace(member)
		member = strings.TrimSuffix(strings.TrimPrefix(member, `"`), `"`)
		if member != "" {
			members = append(members, strings.ReplaceAll(member, `""`, `"`))
		}
	}
	return int32(numSync), members
}

// upsertRole creates or re-passwords one login role. It is idempotent because bootstrap
// runs in an init container, and an init container runs again on every Pod restart.
func upsertRole(name, password, attributes string) string {
	return fmt.Sprintf(
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s) THEN
		     CREATE ROLE %s WITH LOGIN %s PASSWORD %s;
		   ELSE
		     ALTER ROLE %s WITH LOGIN %s PASSWORD %s;
		   END IF;
		 END $$`,
		quoteLiteral(name), name, attributes, quoteLiteral(password),
		name, attributes, quoteLiteral(password))
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
