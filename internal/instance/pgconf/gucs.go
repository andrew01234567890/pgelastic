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

// Package pgconf owns every PostgreSQL parameter pgelastic decides for itself: which
// ones the operator owns, what values it computes for them, and how the resulting
// configuration files are rendered and hashed.
//
// It is shared between the operator and the in-pod instance manager on purpose. The
// admission webhook rejects an owned parameter and the config generator drops it again,
// so a stale object that was admitted before a parameter became owned still cannot
// poison a pod that reads it later.
package pgconf

import (
	"maps"
	"slices"
	"strings"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// SettingContext mirrors pg_settings.context. It is what decides whether applying a new
// value costs a reload, a restart of the postmaster, or nothing at all.
type SettingContext string

const (
	// ContextInternal cannot change after initdb.
	ContextInternal SettingContext = "internal"
	// ContextPostmaster needs a full postmaster restart, which drops every tenant
	// connection on the instance.
	ContextPostmaster SettingContext = "postmaster"
	// ContextSighup is applied by a reload.
	ContextSighup SettingContext = "sighup"
	// ContextSuperuserBackend is fixed for the lifetime of a backend, settable only by a
	// superuser at connection time.
	ContextSuperuserBackend SettingContext = "superuser-backend"
	// ContextBackend is fixed for the lifetime of a backend.
	ContextBackend SettingContext = "backend"
	// ContextSuperuser is settable at run time by a superuser.
	ContextSuperuser SettingContext = "superuser"
	// ContextUser is settable at run time by any client, which is why every Tier 2
	// control is advisory rather than enforced.
	ContextUser SettingContext = "user"
)

// Ownership says who decides a parameter's value.
type Ownership string

const (
	// OwnershipFixed means the operator computes the value from the instance's own shape
	// - its class, its volumes, its topology - and any user-supplied value is dropped.
	OwnershipFixed Ownership = "Fixed"
	// OwnershipBlocked means the parameter must stay at the operator's constant value.
	// Blocking is about safety rather than sizing: these are the parameters that let a
	// user route around the operator or invalidate a guarantee the product sells.
	OwnershipBlocked Ownership = "Blocked"
	// OwnershipTuned means the operator computes a value from the instance's own shape and
	// the user may replace it. It is the level for a parameter whose default should follow
	// the container rather than PostgreSQL's boot constant, but whose wrong value costs the
	// tenant that chose it and nobody else.
	//
	// A parameter belongs here only if all three hold. It is not one of EnforcedParameters,
	// because a standby raises those to the primary's value and would silently discard the
	// override - an override that appears accepted and does not take effect is worse than one
	// refused. It does not denominate capacity, because admission, the reservation ledger and
	// chargeback are all sold in those units. And over-setting it cannot take the postmaster
	// down, because ~200 tenants share it and restart_after_crash is Blocked off.
	//
	// max_connections fails all three and is the canonical example of what does not belong.
	OwnershipTuned Ownership = "Tuned"
	// OwnershipUser means the parameter is the tenant's to set.
	OwnershipUser Ownership = "User"
)

// Owned describes one operator-owned parameter.
type Owned struct {
	// Ownership distinguishes a computed value from a constant one.
	Ownership Ownership
	// Context is the pg_settings.context this parameter is classified by.
	Context SettingContext
	// Value is the constant emitted for a blocked parameter. It is empty for fixed
	// parameters, whose value is computed per instance, and for the one blocked
	// parameter that is deliberately never emitted at all.
	Value string
	// Omit records that the parameter is owned but must not appear in the config file.
	// The only member is wal_log_hints: PG18 turns data checksums on by default, which
	// makes it redundant for pg_rewind while still costing WAL volume. Blocking it
	// without emitting it is what stops a user turning that cost back on.
	Omit bool
}

// EnforcedParameters are the five parameters whose value on a standby must be at least
// the primary's, in the exact set PostgreSQL checks. Starting recovery below any of them
// FATALs with "recovery aborted because of insufficient parameter settings", so a replica
// raises each to max(desired, pg_controldata) on every non-first start.
var EnforcedParameters = []string{
	GUCMaxConnections,
	GUCMaxPreparedTransactions,
	GUCMaxWALSenders,
	GUCMaxWorkerProcesses,
	GUCMaxLocksPerTransaction,
}

// CustomGUCPrefix namespaces the parameters pgelastic defines itself. They are readable
// with a plain SHOW over any backend connection, which is what binds them to the running
// postmaster rather than to a file somebody may have rewritten since.
const CustomGUCPrefix = "pgelastic."

const (
	// GUCPrimaryEpoch carries the fence token into the postmaster so the proxy can read
	// it back off any backend connection.
	GUCPrimaryEpoch = CustomGUCPrefix + "primary_epoch"
	// GUCConfigSHA256 identifies the configuration the postmaster actually loaded. It is
	// read back with current_setting() and never from pg_show_all_file_settings(), so
	// that the hash and pending_restart always describe the same reload.
	GUCConfigSHA256 = CustomGUCPrefix + "config_sha256"
)

// GUC names pgelastic refers to by name in more than one place. Spelling a parameter name
// twice is how a rename becomes a silent no-op, so the ones that matter are named here.
const (
	GUCMaxConnections          = "max_connections"
	GUCMaxPreparedTransactions = "max_prepared_transactions"
	GUCMaxWALSenders           = "max_wal_senders"
	GUCMaxWorkerProcesses      = "max_worker_processes"
	GUCMaxParallelWorkers      = "max_parallel_workers"
	GUCMaxLocksPerTransaction  = "max_locks_per_transaction"
	GUCWALLevel                = "wal_level"
	GUCWALLogHints             = "wal_log_hints"
	GUCTrackCommitTimestamp    = "track_commit_timestamp"
	GUCSynchronousStandbyNames = "synchronous_standby_names"
	GUCArchiveMode             = "archive_mode"
	GUCArchiveCommand          = "archive_command"
	GUCAllowAlterSystem        = "allow_alter_system"
	GUCRestartAfterCrash       = "restart_after_crash"
	GUCIOMethod                = "io_method"
	GUCLoggingCollector        = "logging_collector"
	GUCSharedPreloadLibraries  = "shared_preload_libraries"
	GUCClusterName             = "cluster_name"
	GUCSynchronousCommit       = "synchronous_commit"
)

// valueOff and valueOn are the two boolean literals PostgreSQL accepts in a config file.
const (
	valueOff = "off"
	valueOn  = "on"
)

// ownedParameters is the classification table. Everything absent from it is the user's.
var ownedParameters = map[string]Owned{
	// Capacity. max_connections is the number the whole product rests on, and it is
	// PGC_POSTMASTER, so it is monotonically non-decreasing within an instance
	// generation: capacity is reclaimed by migrating tenants away, never by shrinking it.
	GUCMaxConnections:                {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"superuser_reserved_connections": {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"reserved_connections":           {Ownership: OwnershipFixed, Context: ContextPostmaster},

	// Replication and live migration. Retrofitting either of the first two costs a
	// fleet-wide rolling restart, so both are set at bootstrap and never changed.
	GUCWALLevel:                      {Ownership: OwnershipBlocked, Context: ContextPostmaster, Value: "logical"},
	GUCTrackCommitTimestamp:          {Ownership: OwnershipBlocked, Context: ContextPostmaster, Value: valueOn},
	GUCMaxWALSenders:                 {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"max_replication_slots":          {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"max_active_replication_origins": {Ownership: OwnershipFixed, Context: ContextPostmaster},
	GUCMaxWorkerProcesses:            {Ownership: OwnershipFixed, Context: ContextPostmaster},
	// The first Tuned parameter, and the level's whole point: the operator computes a value
	// from the instance's own CPU and the tenant may replace it. Over-setting it costs the
	// tenant that chose it a share of one bounded pool and cannot take the postmaster down,
	// it denominates no capacity the product sells, and it is not one of EnforcedParameters -
	// which are the three tests a parameter has to pass to be Tuned rather than Fixed.
	GUCMaxParallelWorkers:        {Ownership: OwnershipTuned, Context: ContextUser},
	GUCMaxPreparedTransactions:   {Ownership: OwnershipFixed, Context: ContextPostmaster},
	GUCMaxLocksPerTransaction:    {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"hot_standby":                {Ownership: OwnershipBlocked, Context: ContextPostmaster, Value: valueOn},
	"hot_standby_feedback":       {Ownership: OwnershipBlocked, Context: ContextSighup, Value: valueOn},
	"sync_replication_slots":     {Ownership: OwnershipBlocked, Context: ContextSighup, Value: valueOn},
	"synchronized_standby_slots": {Ownership: OwnershipFixed, Context: ContextSighup},
	GUCSynchronousStandbyNames:   {Ownership: OwnershipFixed, Context: ContextSighup},
	GUCSynchronousCommit:         {Ownership: OwnershipFixed, Context: ContextUser},
	"primary_conninfo":           {Ownership: OwnershipFixed, Context: ContextSighup},
	"primary_slot_name":          {Ownership: OwnershipFixed, Context: ContextSighup},
	"restore_command":            {Ownership: OwnershipFixed, Context: ContextSighup},
	"recovery_target_time":       {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"recovery_target_lsn":        {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"recovery_target_name":       {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"recovery_target_action":     {Ownership: OwnershipFixed, Context: ContextPostmaster},

	// WAL retention, derived from the WAL volume: losing a slot costs a bounded replica
	// rebuild, losing the primary costs every tenant on the instance their guarantee.
	"max_slot_wal_keep_size": {Ownership: OwnershipFixed, Context: ContextSighup},
	"wal_keep_size":          {Ownership: OwnershipFixed, Context: ContextSighup},

	// pg_rewind viability. Blocked but never emitted, see Owned.Omit.
	GUCWALLogHints: {Ownership: OwnershipBlocked, Context: ContextPostmaster, Omit: true},

	// Archiving. archive_mode is PGC_POSTMASTER and is therefore on from bootstrap even
	// before a repository exists, because turning it on later is a restart that drops
	// every tenant connection.
	GUCArchiveMode:    {Ownership: OwnershipBlocked, Context: ContextPostmaster, Value: valueOn},
	GUCArchiveCommand: {Ownership: OwnershipFixed, Context: ContextSighup},
	"archive_timeout": {Ownership: OwnershipFixed, Context: ContextSighup},

	// Routing around the operator, and self-healing that hides a fault.
	GUCAllowAlterSystem: {Ownership: OwnershipBlocked, Context: ContextSighup, Value: valueOff},
	GUCRestartAfterCrash: {
		Ownership: OwnershipBlocked, Context: ContextSighup, Value: valueOff,
	},
	"fsync":            {Ownership: OwnershipBlocked, Context: ContextSighup, Value: valueOn},
	"full_page_writes": {Ownership: OwnershipBlocked, Context: ContextSighup, Value: valueOn},

	// io_uring needs a bespoke seccomp profile, so PG18's default worker method stands
	// and io_workers is the tuning knob instead.
	GUCIOMethod: {Ownership: OwnershipBlocked, Context: ContextPostmaster, Value: "worker"},

	// Logging. logging_collector is what creates the syslogger, which is what makes the
	// scoped reaper necessary: syslogger.c sets SIG_IGN on SIGINT/SIGTERM/SIGQUIT, so the
	// collector always outlives the postmaster.
	GUCLoggingCollector:        {Ownership: OwnershipBlocked, Context: ContextPostmaster, Value: valueOn},
	"log_destination":          {Ownership: OwnershipBlocked, Context: ContextSighup, Value: "jsonlog"},
	"log_directory":            {Ownership: OwnershipFixed, Context: ContextSighup},
	"log_filename":             {Ownership: OwnershipFixed, Context: ContextSighup},
	"log_rotation_age":         {Ownership: OwnershipBlocked, Context: ContextSighup, Value: "0"},
	"log_rotation_size":        {Ownership: OwnershipBlocked, Context: ContextSighup, Value: "0"},
	"log_truncate_on_rotation": {Ownership: OwnershipBlocked, Context: ContextSighup, Value: valueOff},

	// Connectivity. Nothing may reach 5432 except through the proxy, and the superuser is
	// reachable only over the Unix socket by peer authentication.
	"listen_addresses":        {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"port":                    {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"unix_socket_directories": {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"password_encryption":     {Ownership: OwnershipBlocked, Context: ContextSighup, Value: "scram-sha-256"},
	"ssl":                     {Ownership: OwnershipFixed, Context: ContextSighup},
	"ssl_cert_file":           {Ownership: OwnershipFixed, Context: ContextSighup},
	"ssl_key_file":            {Ownership: OwnershipFixed, Context: ContextSighup},
	"ssl_ca_file":             {Ownership: OwnershipFixed, Context: ContextSighup},
	"hba_file":                {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"ident_file":              {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"data_directory":          {Ownership: OwnershipFixed, Context: ContextPostmaster},
	GUCClusterName:            {Ownership: OwnershipFixed, Context: ContextPostmaster},
	GUCSharedPreloadLibraries: {Ownership: OwnershipFixed, Context: ContextPostmaster},

	// PG18 moved autovacuum_max_workers to PGC_SIGHUP, so the slot count is what has to
	// be sized once at creation and the worker count stays tunable as tenant density
	// changes.
	"autovacuum_worker_slots": {Ownership: OwnershipFixed, Context: ContextPostmaster},

	// Memory, computed from the pod's own limits rather than left at a boot default that
	// bears no relation to the container it is running in.
	"shared_buffers":       {Ownership: OwnershipFixed, Context: ContextPostmaster},
	"effective_cache_size": {Ownership: OwnershipFixed, Context: ContextUser},
}

// Classify reports how a parameter is owned. Unknown parameters belong to the user: the
// table enumerates what pgelastic takes, not what PostgreSQL offers.
func Classify(name string) Owned {
	if owned, ok := ownedParameters[name]; ok {
		return owned
	}
	return Owned{Ownership: OwnershipUser, Context: ContextUser}
}

// IsOwned reports whether the operator has an opinion about a parameter at all - whether by
// computing its value, by pinning it, or by tuning a default the user may replace.
//
// This is the *membership* question, and it is deliberately not the authorization one. The two
// had the same answer for every entry while every owned parameter was pinned, which is why one
// predicate served both; the moment a computed value became overridable they diverged, and a
// call site that asks the wrong one either drops an override that should have won or refuses a
// parameter the user is entitled to set.
func IsOwned(name string) bool {
	_, ok := ownedParameters[name]
	return ok
}

// IsPinned reports whether a user-supplied value for a parameter is refused.
//
// The *authorization* question, and the one every rejection path must ask. Fixed and Blocked
// are pinned; Tuned is not, because the whole point of Tuned is that the computed value is a
// default rather than a decision.
func IsPinned(name string) bool {
	switch Classify(name).Ownership {
	case OwnershipFixed, OwnershipBlocked:
		return true
	default:
		return false
	}
}

// PinnedNames lists every parameter a user may not set, in sorted order.
//
// This rather than OwnedNames is what an admission error quotes: naming a Tuned parameter in a
// refusal would tell an operator the opposite of the truth.
func PinnedNames() []string {
	pinned := make([]string, 0, len(ownedParameters))
	for name := range ownedParameters {
		if IsPinned(name) {
			pinned = append(pinned, name)
		}
	}
	slices.Sort(pinned)
	return pinned
}

// OwnedNames lists every operator-owned parameter in sorted order.
func OwnedNames() []string {
	return slices.Sorted(maps.Keys(ownedParameters))
}

// UserParameters drops every operator-owned parameter from a spec's parameter map, and
// every parameter that cannot be rendered as one configuration line, and reports which ones
// were dropped. The webhook rejects them too; this second pass is what makes an object
// admitted under an older classification harmless.
func UserParameters(parameters map[string]pgelasticv1alpha1.GUCValue) (map[string]string, []string) {
	kept := make(map[string]string, len(parameters))
	var dropped []string
	for name, value := range parameters {
		// IsPinned, not IsOwned: a Tuned parameter is owned and is still the user's to set,
		// so it has to survive into the rendered configuration to overwrite the computed
		// value. Malformed names stay refused at every level - well-formedness is a separate
		// axis from ownership and folding the two together is how `fsync = off` gets in.
		if IsPinned(name) || !RenderableParameter(name, string(value)) {
			dropped = append(dropped, name)
			continue
		}
		kept[name] = string(value)
	}
	slices.Sort(dropped)
	return kept, dropped
}

// RenderableParameter reports whether a parameter can be written as exactly one
// postgresql.conf line.
//
// The ownership table matches a parameter by its exact name, and the renderer writes that
// name verbatim. A name or a value carrying a line break therefore escapes the line it was
// meant to occupy and becomes a configuration directive of its own - which is how a
// parameter nobody owns turns into `fsync = off`, three characters past a denylist that
// never saw the string `fsync`. PostgreSQL's configuration file is line-oriented, so the
// only sound answer is to refuse anything that is not one line.
func RenderableParameter(name, value string) bool {
	return validGUCName(name) && !strings.ContainsAny(value, "\n\r")
}

// validGUCName accepts the grammar PostgreSQL itself accepts for a configuration parameter:
// an identifier, optionally qualified by an extension prefix. Everything else - whitespace,
// `#`, `=`, quotes, control characters - is refused rather than escaped, because a name is
// not quotable in a configuration file the way a value is.
func validGUCName(name string) bool {
	if name == "" {
		return false
	}
	prefix, rest, qualified := strings.Cut(name, ".")
	if qualified {
		return validGUCIdentifier(prefix) && validGUCIdentifier(rest)
	}
	return validGUCIdentifier(name)
}

func validGUCIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}
	for index, char := range identifier {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char == '_':
		case index > 0 && char >= '0' && char <= '9':
		default:
			return false
		}
	}
	return true
}

// BlockedDefaults returns the constant values for every blocked parameter that is emitted
// at all, keyed by name.
func BlockedDefaults() map[string]string {
	values := make(map[string]string)
	for name, owned := range ownedParameters {
		if owned.Ownership == OwnershipBlocked && !owned.Omit {
			values[name] = owned.Value
		}
	}
	return values
}

// logicalReplicationWorkers is the slot budget the ONLINE migration path needs, and the
// reason max_worker_processes cannot simply be a literal.
//
// bgworker.c keeps ONE global pool, shared by parallel query, parallel maintenance and
// logical replication. So a max_worker_processes that does not account for the apply and
// tablesync workers a migration starts is a migration that cannot start - it fails on a
// resource nobody can see from the migration's own configuration.
const logicalReplicationWorkers = 4

// backgroundWorkerReserve is what the instance itself runs that is neither parallel query nor
// logical replication: the autovacuum launcher, the logical replication launcher and room for
// the extensions a tenant may load.
const backgroundWorkerReserve = 8

// minWorkerProcesses is the literal this tree used before anything derived it, kept as a
// floor so no instance shape can end up with fewer workers than the tree has always had.
const minWorkerProcesses = 16

// WorkerProcesses is max_worker_processes: the envelope every other worker count sits inside.
//
// Fixed rather than Tuned, and the reason is the pool it governs. A user who lowered it below
// what the parallel and logical-replication settings already promise would not get a smaller
// instance; they would get migrations that cannot start and parallel plans that silently run
// serially, with nothing saying why.
func WorkerProcesses(parallelWorkers int32) int32 {
	return workerProcesses(parallelWorkers)
}

func workerProcesses(parallelWorkers int32) int32 {
	return max(minWorkerProcesses,
		parallelWorkers+logicalReplicationWorkers+backgroundWorkerReserve)
}

// minParallelWorkers keeps a small instance able to run a parallel plan at all. Below two
// the setting stops meaning "fewer workers" and starts meaning "no parallelism", which is a
// different decision and not one the operator should make on a tenant's behalf.
const minParallelWorkers = 2

// parallelWorkers is max_parallel_workers, in workers, from a CPU allocation in millicores.
//
// One worker per core, which is where pgtune, timescaledb-tune and CNPG all land for a
// dedicated server. It is bounded above by the Fixed max_worker_processes that is derived
// from it, so a Tuned override cannot escape the envelope the operator sized.
func parallelWorkers(declared int32) int32 {
	if declared <= 0 {
		return minParallelWorkers
	}
	return max(minParallelWorkers, declared)
}

// ParallelWorkersForCPU turns a CPU allocation in millicores into a worker count.
func ParallelWorkersForCPU(millis int64) int32 {
	if millis <= 0 {
		return minParallelWorkers
	}
	return parallelWorkers(int32(millis / 1000))
}

// DefaultMajor is the PostgreSQL major a configuration is rendered for when the caller names
// none. It is what every caller meant before the tree could express more than one.
const DefaultMajor = 18

// maxLocksPerTransaction is the one operator-computed value that changes with the major.
//
// PostgreSQL 19 doubled the default from 64 to 128, and its release note is explicit that
// settings "must now be doubled to match their capacity in previous releases" - the lock
// table is sized as max_locks_per_transaction x (max_connections + max_prepared_transactions),
// and 19 changed what one unit buys. Rendering 18's literal on a 19 postmaster would
// therefore halve the lock capacity of every instance, silently, and the first symptom would
// be "out of shared memory" on a tenant doing nothing unusual.
//
// It is also an EnforcedParameters member, so a standby that came up on the smaller value
// would raise it to the primary's anyway - which means a mixed-major pair would disagree
// about the number and only the primary's would take effect. One more reason the value has
// to follow the major rather than the tree.
func maxLocksPerTransaction(major int) string {
	if major >= 19 {
		return "128"
	}
	return "64"
}
