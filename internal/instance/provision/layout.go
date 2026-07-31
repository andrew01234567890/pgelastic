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

package provision

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/andrew01234567890/pgelastic/internal/instance/pgbackrest"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

// Filesystem layout inside a Postgres pod. The agent and the operator both compile against
// these constants, so a path can never be spelled two different ways.
const (
	// DataMountPath is where the PG_DATA volume is mounted.
	DataMountPath = "/var/lib/postgresql/data"
	// DataDir is PGDATA, a subdirectory of the mount rather than the mount itself, so a
	// filesystem's own bookkeeping entries are not inside the data directory.
	DataDir = DataMountPath + "/pgdata"
	// WALMountPath is where the PG_WAL volume is mounted.
	WALMountPath = "/var/lib/postgresql/wal"
	// WALDir is the pg_wal directory initdb is pointed at.
	WALDir = WALMountPath + "/pg_wal"
	// AgentMountPath is the emptyDir the instance manager is copied into by an init
	// container, which is what lets the agent be upgraded without rebuilding the
	// PostgreSQL image.
	AgentMountPath = "/agent"
	// AgentBinaryName is the file the init container copies.
	AgentBinaryName = "pgelastic-instance"
	// AgentBinary is the copy the postgres container executes.
	AgentBinary = AgentMountPath + "/" + AgentBinaryName
	// SourceAgentBinary is where the binary sits in the agent image.
	SourceAgentBinary = "/" + AgentBinaryName
	// SourceVerifyBinary is where the durability oracle sits in the same image. It travels
	// with the agent so a chaos run can drive it from inside the cluster, against the
	// read-write Service, which is the route a tenant's connection takes.
	SourceVerifyBinary = "/pgelastic-verify"
	// ConfigMountPath holds the generated configuration.
	ConfigMountPath = "/etc/pgelastic"
	// ConfigFileName is the operator's decisions, rendered by the agent.
	ConfigFileName = "config.json"
	// SocketDir holds the Unix socket the superuser is reached on. It is an emptyDir so
	// that peer access to the postmaster does not depend on the data volume.
	SocketDir = "/var/run/postgresql"
	// LogDir holds the FIFO the logging collector writes into.
	LogDir = "/var/log/postgresql"
	// LogFileName is the log_filename PostgreSQL is given. It has to end in ".log":
	// with log_destination = csvlog, PostgreSQL replaces a ".log" suffix with ".csv" and
	// otherwise *appends* ".csv", so naming the file after the FIFO directly produces a
	// "postgresql.csv.csv" regular file beside an untouched FIFO - an emptyDir quietly
	// filling up with logs nobody is reading.
	LogFileName = "postgresql.log"
	// LogFIFOName is what the logging collector actually writes, and the FIFO the agent
	// creates and drains to the container's stdout before any postmaster exists.
	LogFIFOName = "postgresql.csv"
)

// Backup paths. The generated pgBackRest configuration carries the repository credentials,
// so it lives in the agent's emptyDir at 0600 beside override.conf, which already carries
// the replication password, rather than anywhere a second process could read it.
const (
	// BackupCredentialsMountPath is where the object store's Secret is mounted. It is a
	// path of its own rather than a subdirectory of ConfigMountPath, because nesting one
	// volume mount inside another is legal and unreadable.
	BackupCredentialsMountPath = "/etc/pgelastic-backup"
	// BackupConfigFile is the generated pgbackrest.conf.
	BackupConfigFile = AgentMountPath + "/pgbackrest.conf"
	// BackupSpoolPath backs asynchronous archiving. It sits beside pg_wal on the WAL volume,
	// not inside it and not on the data volume: it holds WAL-sized files, and the volume
	// already sized for WAL is the one whose worst case is understood.
	BackupSpoolPath = WALMountPath + "/pgbackrest-spool"
	// BackupLogPath is pgBackRest's own log directory. File logging is off, but pgBackRest
	// still insists the path be writable.
	BackupLogPath = AgentMountPath + "/pgbackrest-log"
	// ArchiveTimeout is how long PostgreSQL waits before switching a segment so that a
	// low-traffic instance still bounds how much committed work is not yet archived.
	//
	// Note what it does not do: it only switches when there has been WAL activity since the
	// last switch, so an idle instance archives nothing for as long as it stays idle. That
	// is why the age of the last successful archive is never on its own evidence of a fault.
	ArchiveTimeout = "5min"
	// ArchiveStallAfter is how long a non-empty archive queue may sit without anything
	// being archived before archiving is called stalled rather than slow.
	//
	// Three archive_timeouts. Two would fire on a single slow upload; longer would let a
	// hung archive_command - which fails no differently from an idle instance, because a
	// command that never returns records neither a success nor a failure - go unnoticed for
	// most of an hour.
	ArchiveStallAfter = 15 * time.Minute
	// ArchiveStatusFile is where archive_command records its own last outcome.
	//
	// It exists because archive_command runs as a separate short-lived process: it cannot
	// hand an error to the agent in memory, and PostgreSQL discards its output. Without
	// this the only evidence of why a segment failed would be an exit code, and
	// pg_stat_archiver records that a failure happened without recording what it was.
	ArchiveStatusFile = AgentMountPath + "/archive-status.json"
)

// Secret keys of the object store credentials.
const (
	// SecretKeyBackupAccessKeyID and SecretKeyBackupSecretAccessKey are the S3 key pair.
	SecretKeyBackupAccessKeyID     = "accessKeyID"
	SecretKeyBackupSecretAccessKey = "secretAccessKey"
	// SecretKeyBackupCABundle is optional, and is the CA that signed an S3-compatible
	// store's certificate when it is not one the image already trusts.
	SecretKeyBackupCABundle = "ca.crt"
)

// Ports.
const (
	// PostgresPort is the postmaster's TCP port.
	PostgresPort int32 = 5432
	// StatusPort carries the three probes and the failsafe peer endpoint. It is separate
	// from 5432 on purpose: the liveness probe must be answerable when PostgreSQL is not.
	StatusPort int32 = 8008
)

// Roles created at bootstrap. The postgres superuser is not among them because it is never
// given a password at all.
const (
	// ReplicationRole is what standbys stream as.
	ReplicationRole = "pgelastic_repl"
	// OpsRole is the control plane's non-superuser role, holding pg_monitor,
	// pg_signal_backend and pg_use_reserved_connections and nothing else.
	OpsRole = "pgelastic_ops"
	// RewindRole is what pg_rewind connects to the new primary as. It is a role of its own
	// rather than a reuse of OpsRole because it needs exactly four function grants and
	// nothing else: a rewind connection can read any file in the data directory, so the
	// role that does it must be able to do nothing more than that.
	RewindRole = "pgelastic_rewind"
)

// Secret keys.
const (
	// SecretKeyReplicationPassword is the replication role's password.
	SecretKeyReplicationPassword = "replication-password"
	// SecretKeyOpsPassword is the ops role's password.
	SecretKeyOpsPassword = "ops-password"
	// SecretKeyRewindPassword is the rewind role's password.
	SecretKeyRewindPassword = "rewind-password"
)

// Environment variables the agent reads. They are declared here rather than in the agent
// so the operator that writes them and the agent that reads them cannot drift.
const (
	EnvInstance       = "PGELASTIC_INSTANCE"
	EnvNamespace      = "PGELASTIC_NAMESPACE"
	EnvMember         = "PGELASTIC_MEMBER"
	EnvSerial         = "PGELASTIC_SERIAL"
	EnvPodIP          = "PGELASTIC_POD_IP"
	EnvDataDir        = "PGDATA"
	EnvWALDir         = "PGELASTIC_WAL_DIR"
	EnvConfigFile     = "PGELASTIC_CONFIG_FILE"
	EnvSocketDir      = "PGELASTIC_SOCKET_DIR"
	EnvLogDir         = "PGELASTIC_LOG_DIR"
	EnvStatusPort     = "PGELASTIC_STATUS_PORT"
	EnvPeerService    = "PGELASTIC_PEER_SERVICE"
	EnvReplPassword   = "PGELASTIC_REPLICATION_PASSWORD"
	EnvOpsPassword    = "PGELASTIC_OPS_PASSWORD"
	EnvRewindPassword = "PGELASTIC_REWIND_PASSWORD"
	EnvBinDir         = "PGELASTIC_PG_BIN_DIR"
)

// AgentConfig is everything the operator decided, handed to the agent as one document.
//
// The agent renders the configuration files itself rather than mounting them ready-made,
// because two of the values are member-local: cluster_name, and the floors the five
// enforced parameters have to be raised to from this member's own pg_controldata.
type AgentConfig struct {
	// Instance is the PgInstance name.
	Instance string `json:"instance"`
	// Namespace is the PgInstance namespace.
	Namespace string `json:"namespace"`
	// Replicas is the member count including the primary.
	Replicas int32 `json:"replicas"`
	// Postgres carries every operator-computed parameter except the member-local ones.
	Postgres pgconf.InstanceConfig `json:"postgres"`
	// HBA is the input to the generated pg_hba.conf.
	HBA pgconf.HBAConfig `json:"hba"`
	// Quorum is the leading clause of synchronous_standby_names, for example "ANY 1".
	Quorum string `json:"quorum"`
	// DataDurability decides whether losing quorum stalls commits or degrades to
	// asynchronous replication.
	DataDurability string `json:"dataDurability"`
	// Lease parameterises the promotion Lease the agent holds.
	Lease LeaseTimings `json:"lease"`
	// SwitchoverTimeout bounds a stop the control plane asked for at a moment it chose,
	// which is spec.highAvailability.switchoverTimeout. It travels here rather than being
	// hardcoded in the agent because it is the deadline a planned role change is measured
	// against, and a value the API accepts and nothing reads is worse than no field.
	SwitchoverTimeout metav1.Duration `json:"switchoverTimeout"`
	// PeerService is the headless Service that gives members stable DNS names.
	PeerService string `json:"peerService"`
	// CollationContract is what initdb was pinned to, so a member can refuse to join a
	// pool whose contract differs rather than discovering the difference at restore time.
	CollationContract CollationContract `json:"collationContract"`
	// Backup is the WAL and base-backup repository, absent until one is configured. When it
	// is absent archive_command still runs and still succeeds, because archive_mode is
	// PGC_POSTMASTER and turning it on later would cost a restart that drops every tenant
	// connection on the instance.
	Backup *pgbackrest.Repository `json:"backup,omitempty"`
	// Recovering marks an instance restored from a repository rather than provisioned.
	//
	// Such a member carries its source's system identifier - a physical restore copies the
	// control file verbatim - and therefore addresses its source's stanza, while running on
	// a forked timeline. Archiving from it would interleave two histories into one archive
	// and leave neither restorable, so it does not archive at all.
	Recovering bool `json:"recovering,omitempty"`
}

// LeaseTimings are the promotion Lease's four durations, handed to the agent because the
// agent, not the operator, is what holds the lease.
//
// They are also the proxy's fencing deadline, which is why they travel as one struct:
// shortening the lease duration shortens the deadline in lockstep, and a deployment where
// the two disagree has a window in which a new primary exists and the old one is still
// being written to.
type LeaseTimings struct {
	LeaseDuration         metav1.Duration `json:"leaseDuration"`
	RenewDeadline         metav1.Duration `json:"renewDeadline"`
	RetryPeriod           metav1.Duration `json:"retryPeriod"`
	ReleasedLeaseDuration metav1.Duration `json:"releasedLeaseDuration"`
}

// CollationContract is the initdb side of the immutable tuple published in status.
type CollationContract struct {
	Encoding       string `json:"encoding"`
	LocaleProvider string `json:"localeProvider"`
	Locale         string `json:"locale"`
	WALSegmentSize int64  `json:"walSegmentSize"`
	DataChecksums  bool   `json:"dataChecksums"`
}
