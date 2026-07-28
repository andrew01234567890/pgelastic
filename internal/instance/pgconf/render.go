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

package pgconf

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// File names inside PGDATA. postgresql.conf is written once, by initdb, and then touched
// exactly once more to add two include directives; everything pgelastic decides lives in
// the two files it owns outright.
const (
	// CustomConfFile holds every operator-owned parameter.
	CustomConfFile = "custom.conf"
	// OverrideConfFile holds replication and recovery settings and is rewritten wholesale
	// on every role change, so a stale line from a previous role can never survive.
	OverrideConfFile = "override.conf"
	// HBAFile is generated wholesale every reconcile.
	HBAFile = "pg_hba.conf"
	// IdentFile is generated wholesale alongside it.
	IdentFile = "pg_ident.conf"
	// PostgresqlConfFile is initdb's own output.
	PostgresqlConfFile = "postgresql.conf"
)

// IncludeDirectives are the only lines pgelastic ever adds to postgresql.conf. They are
// appended once, at initdb, and the ordering matters: override.conf is included last so
// the replication and recovery settings for the member's current role win over anything
// custom.conf says.
const IncludeDirectives = "include_if_exists = '" + CustomConfFile + "'\n" +
	"include_if_exists = '" + OverrideConfFile + "'\n"

// Setting is one rendered parameter.
type Setting struct {
	Name  string
	Value string
}

// InstanceConfig is everything the operator computes for one member.
type InstanceConfig struct {
	// MemberName is the pod name, published as cluster_name so a backend can be traced
	// back to the member that served it.
	MemberName string
	// Capacity is the max_connections split.
	Capacity Capacity
	// SocketDirectory holds the Unix socket the superuser is reached on. It is an
	// emptyDir, never the data volume, so a stuck volume cannot take peer access with it.
	SocketDirectory string
	// Port is the TCP port the postmaster listens on.
	Port int32
	// LogDirectory and LogFilename address the FIFO the logging collector writes into.
	LogDirectory string
	LogFilename  string
	// ArchiveCommand is the shim that hands a segment to pgBackRest.
	ArchiveCommand string
	// ArchiveTimeout bounds RPO.
	ArchiveTimeout string
	// WALVolumeBytes sizes slot retention.
	WALVolumeBytes int64
	// SynchronousCommit is the instance-wide level.
	SynchronousCommit string
	// AutovacuumWorkerSlots is sized once at creation because it is PGC_POSTMASTER,
	// unlike autovacuum_max_workers which PG18 made reloadable.
	AutovacuumWorkerSlots int32
	// ActiveReplicationOrigins is deliberately over-provisioned: PG18 no longer derives
	// max_active_replication_origins from max_replication_slots, and under-sizing it
	// means CREATE SUBSCRIPTION fails with no fix but a restart that drops every tenant
	// connection on the instance.
	ActiveReplicationOrigins int32
	// SharedBuffers and EffectiveCacheSize are derived from the pod's memory allocation.
	// They are omitted when the pod declares none, so the boot defaults stand rather than
	// a number invented from nothing.
	SharedBuffers      string
	EffectiveCacheSize string
	// PrimaryEpoch is the fence token bound into the postmaster.
	PrimaryEpoch int64
	// UserParameters are the tenant-settable parameters that survived UserParameters.
	UserParameters map[string]string
}

// slotRetentionNumerator and slotRetentionDenominator hold max_slot_wal_keep_size to two
// fifths of the WAL volume. Together with wal_keep_size at one tenth that leaves a fifth
// of the volume for archive backlog and checkpoint headroom, under the 0.7 ceiling that
// keeps a full pg_wal - which PANICs the primary and vaporizes every tenant's guarantee
// at once - out of reach.
const (
	slotRetentionNumerator   = 2
	slotRetentionDenominator = 5
	walKeepDenominator       = 10
	mebibyte                 = 1 << 20
)

// RenderCustomConf renders every operator-owned parameter, then the user's own.
//
// Owned parameters are emitted explicitly even when the value equals the boot default,
// notably shared_preload_libraries: pending_restart is set only when a parameter appears
// in or disappears from the configuration file, so a parameter left implicit is a
// parameter whose restart requirement PostgreSQL will never report.
func RenderCustomConf(config InstanceConfig) []Setting {
	settings := map[string]string{
		GUCMaxConnections:                strconv.Itoa(int(config.Capacity.MaxConnections)),
		"superuser_reserved_connections": strconv.Itoa(int(config.Capacity.SuperuserReserved)),
		"reserved_connections":           strconv.Itoa(int(config.Capacity.Reserved)),
		GUCMaxWALSenders:                 strconv.Itoa(int(config.Capacity.WALSenders)),
		"max_replication_slots":          strconv.Itoa(int(config.Capacity.ReplicationSlots)),
		"max_active_replication_origins": strconv.Itoa(int(config.ActiveReplicationOrigins)),
		GUCMaxWorkerProcesses:            "16",
		GUCMaxPreparedTransactions:       "0",
		GUCMaxLocksPerTransaction:        "64",
		"autovacuum_worker_slots":        strconv.Itoa(int(config.AutovacuumWorkerSlots)),

		"listen_addresses":        "*",
		"port":                    strconv.Itoa(int(config.Port)),
		"unix_socket_directories": config.SocketDirectory,
		GUCClusterName:            config.MemberName,
		GUCSharedPreloadLibraries: "",
		"ssl":                     valueOff,

		"log_directory": config.LogDirectory,
		"log_filename":  config.LogFilename,

		GUCArchiveCommand: config.ArchiveCommand,
		"archive_timeout": config.ArchiveTimeout,

		"max_slot_wal_keep_size": mebibytes(
			config.WALVolumeBytes * slotRetentionNumerator / slotRetentionDenominator),
		"wal_keep_size": mebibytes(config.WALVolumeBytes / walKeepDenominator),

		"synchronous_commit": config.SynchronousCommit,

		GUCPrimaryEpoch: strconv.FormatInt(config.PrimaryEpoch, 10),
	}
	maps.Copy(settings, BlockedDefaults())

	if config.SharedBuffers != "" {
		settings["shared_buffers"] = config.SharedBuffers
	}
	if config.EffectiveCacheSize != "" {
		settings["effective_cache_size"] = config.EffectiveCacheSize
	}
	for name, value := range config.UserParameters {
		if IsOwned(name) {
			continue
		}
		settings[name] = value
	}
	return sortedSettings(settings)
}

// ReplicationConfig is what override.conf is rewritten from. Every field is role
// dependent, which is why the file is replaced wholesale rather than patched.
type ReplicationConfig struct {
	// Standby puts the member into recovery. The primary writes the same file with this
	// false, so a demoted primary's old primary_conninfo cannot linger.
	Standby bool
	// PrimaryConnInfo must carry dbname=, without which slot synchronisation errors out.
	PrimaryConnInfo string
	// PrimarySlotName is the persistent slot this standby streams from.
	PrimarySlotName string
	// SynchronousStandbyNames is the full clause, quorum prefix included.
	SynchronousStandbyNames string
	// SynchronizedStandbySlots names the physical slots a logical walsender must wait on.
	// Without it a subscriber can consume changes a standby has not flushed, and after a
	// promotion the synced slot is behind the subscriber: the migration then either errors
	// on missing WAL or silently loses rows.
	SynchronizedStandbySlots string
	// RestoreCommand is the shim that fetches an archived segment.
	RestoreCommand string
}

// RenderOverrideConf renders the replication and recovery settings.
func RenderOverrideConf(config ReplicationConfig) []Setting {
	settings := map[string]string{
		"primary_conninfo":           config.PrimaryConnInfo,
		"primary_slot_name":          config.PrimarySlotName,
		GUCSynchronousStandbyNames:   config.SynchronousStandbyNames,
		"synchronized_standby_slots": config.SynchronizedStandbySlots,
		"restore_command":            config.RestoreCommand,
	}
	return sortedSettings(settings)
}

// FormatSettings writes settings as postgresql.conf lines. Every value is quoted, which
// PostgreSQL accepts for numeric parameters too, so a value containing a space or an
// equals sign cannot change the meaning of the line it appears on.
func FormatSettings(header string, settings []Setting) string {
	var builder strings.Builder
	builder.WriteString("# " + header + "\n")
	builder.WriteString("# Generated by pgelastic. Edits are overwritten on the next reconcile.\n")
	for _, setting := range settings {
		fmt.Fprintf(&builder, "%s = '%s'\n", setting.Name, strings.ReplaceAll(setting.Value, "'", "''"))
	}
	return builder.String()
}

// HBAConfig is the input to a generated pg_hba.conf.
type HBAConfig struct {
	// ProxySources are the CIDRs the proxy's pods dial from. Tenant roles are admitted
	// from these and nowhere else, so nothing can bypass connection accounting by dialling
	// 5432 directly.
	ProxySources []string
	// PeerSources are the CIDRs the instance's own members dial from, for replication.
	PeerSources []string
	// ReplicationRole is the non-superuser role standbys stream as.
	ReplicationRole string
	// OpsRole is the control plane's non-superuser role.
	OpsRole string
	// RewindRole is the role pg_rewind dials the new primary as.
	RewindRole string
}

// RenderHBA generates pg_hba.conf wholesale. The trailing catch-all initdb writes is
// replaced with deny-by-default, because a rule that admits everything is indistinguishable
// from having no accounting at all.
func RenderHBA(config HBAConfig) string {
	var builder strings.Builder
	builder.WriteString("# Generated by pgelastic. Edits are overwritten on the next reconcile.\n")
	builder.WriteString("local all postgres peer\n")
	builder.WriteString("local all all peer\n")
	builder.WriteString("local replication all peer\n")
	for _, source := range config.PeerSources {
		fmt.Fprintf(&builder, "host replication %s %s scram-sha-256\n", config.ReplicationRole, source)
	}
	// The replication role also needs an ordinary connection, not only a replication one:
	// slot synchronisation dials the database named in primary_conninfo's dbname=, and
	// admitting only "replication" leaves every standby logging an authentication FATAL on
	// a loop while the synchronised slots silently never advance.
	for _, source := range config.PeerSources {
		fmt.Fprintf(&builder, "host all %s %s scram-sha-256\n", config.ReplicationRole, source)
	}
	for _, source := range config.PeerSources {
		fmt.Fprintf(&builder, "host all %s %s scram-sha-256\n", config.OpsRole, source)
	}
	for _, source := range config.PeerSources {
		fmt.Fprintf(&builder, "host all %s %s scram-sha-256\n", config.RewindRole, source)
	}
	for _, source := range config.ProxySources {
		fmt.Fprintf(&builder, "host all all %s scram-sha-256\n", source)
	}
	// One catch-all is enough: a "host" record matches both plain and SSL connections, so
	// a second "hostssl" line would add nothing but a startup HINT while SSL is off.
	builder.WriteString("host all all all reject\n")
	return builder.String()
}

// RenderIdent generates pg_ident.conf. It is empty of maps until certificate
// authentication lands, but is still generated so the file is owned rather than inherited.
func RenderIdent() string {
	return "# Generated by pgelastic. Edits are overwritten on the next reconcile.\n"
}

// Hash is the identity of one rendered configuration. It is injected back into the
// postmaster as a custom GUC and read with current_setting(), never from
// pg_show_all_file_settings(), so that the hash and pending_restart always describe the
// same reload rather than two different instants.
func Hash(parts ...string) string {
	digest := sha256.New()
	for _, part := range parts {
		digest.Write([]byte(part))
		digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// HashLine is the configuration-identity setting appended to custom.conf after the hash
// has been computed over the file bodies that exclude it.
func HashLine(hash string) string {
	return fmt.Sprintf("%s = '%s'\n", GUCConfigSHA256, hash)
}

func sortedSettings(settings map[string]string) []Setting {
	out := make([]Setting, 0, len(settings))
	for _, name := range slices.Sorted(maps.Keys(settings)) {
		out = append(out, Setting{Name: name, Value: settings[name]})
	}
	return out
}

func mebibytes(bytes int64) string {
	return strconv.FormatInt(max(bytes/mebibyte, 0), 10) + "MB"
}

// ParseSettings reads back a file this package rendered. It exists so that a file which is
// replaced wholesale can be replaced knowing all of its current contents, rather than
// having part of it silently dropped by a rewrite made for an unrelated reason.
func ParseSettings(contents string) map[string]string {
	values := map[string]string{}
	for line := range strings.SplitSeq(contents, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
			value = strings.ReplaceAll(value[1:len(value)-1], "''", "'")
		}
		values[strings.TrimSpace(name)] = value
	}
	return values
}
