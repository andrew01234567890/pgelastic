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
	Role              Role
	LSN               string
	Timeline          int32
	ReplayLag         time.Duration
	WALReceiverActive bool
	SyncStandbys      []string
	StreamingStandbys []string
	SyncStandbyNames  string
	NumSync           int32
	ConfigSHA256      string
	PendingRestart    bool
	MaxConnections    int32
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
		       (SELECT timeline_id FROM pg_control_checkpoint()),
		       COALESCE(EXTRACT(epoch FROM now() - pg_last_xact_replay_timestamp()), 0)::float8,
		       EXISTS (SELECT 1 FROM pg_stat_wal_receiver),
		       COALESCE(current_setting('pgelastic.config_sha256', true), ''),
		       EXISTS (SELECT 1 FROM pg_settings WHERE pending_restart),
		       current_setting('max_connections')::int,
		       current_setting('synchronous_standby_names')`
	var lagSeconds float64
	err := conn.QueryRow(ctx, stateQuery).Scan(
		&observation.LSN,
		&observation.Timeline,
		&lagSeconds,
		&observation.WALReceiverActive,
		&observation.ConfigSHA256,
		&observation.PendingRestart,
		&observation.MaxConnections,
		&observation.SyncStandbyNames,
	)
	if err != nil {
		return observation, err
	}
	observation.ReplayLag = time.Duration(lagSeconds * float64(time.Second))
	observation.NumSync, _ = ParseSyncStandbyNames(observation.SyncStandbyNames)

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
func BootstrapRoles(ctx context.Context, conn *pgx.Conn, replicationPassword, opsPassword string) error {
	statements := []string{
		`SET LOCAL synchronous_commit = 'local'`,
		fmt.Sprintf(
			`DO $$ BEGIN
			   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s) THEN
			     CREATE ROLE %s WITH LOGIN REPLICATION PASSWORD %s;
			   ELSE
			     ALTER ROLE %s WITH LOGIN REPLICATION PASSWORD %s;
			   END IF;
			 END $$`,
			quoteLiteral(provision.ReplicationRole), provision.ReplicationRole,
			quoteLiteral(replicationPassword), provision.ReplicationRole,
			quoteLiteral(replicationPassword)),
		fmt.Sprintf(
			`DO $$ BEGIN
			   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s) THEN
			     CREATE ROLE %s WITH LOGIN PASSWORD %s;
			   ELSE
			     ALTER ROLE %s WITH LOGIN PASSWORD %s;
			   END IF;
			 END $$`,
			quoteLiteral(provision.OpsRole), provision.OpsRole,
			quoteLiteral(opsPassword), provision.OpsRole, quoteLiteral(opsPassword)),
		fmt.Sprintf(`GRANT pg_monitor, pg_signal_backend, pg_use_reserved_connections TO %s`,
			provision.OpsRole),
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

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
