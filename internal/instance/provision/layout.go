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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	// PeerService is the headless Service that gives members stable DNS names.
	PeerService string `json:"peerService"`
	// CollationContract is what initdb was pinned to, so a member can refuse to join a
	// pool whose contract differs rather than discovering the difference at restore time.
	CollationContract CollationContract `json:"collationContract"`
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
