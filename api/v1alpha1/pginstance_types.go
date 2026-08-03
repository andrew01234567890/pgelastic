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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PgInstanceDrainTenantsFinalizer blocks deletion of a PgInstance until every tenant
// bound to it has been migrated away or explicitly released. Deleting the object while
// tenants are still bound would strand databases whose reclaim policy is Retain and
// leave the pool ledger claiming reservations against storage nobody owns.
const PgInstanceDrainTenantsFinalizer = "pgelastic.io/drain-tenants"

// AnnotationRestartedAt asks for one rolling restart of every member, and is the same
// idiom `kubectl rollout restart` teaches: set it to a value different from the one
// already there and the instance rolls once.
//
// The value is opaque and never parsed. It is compared - against the value each Pod was
// last rolled for, which is recorded on the Pod itself - so a roll survives an operator
// restart and resumes at the member it had reached rather than starting again, and two
// edits made while one roll is running are still one roll. A timestamp is the conventional
// value because it is unique and readable, but nothing here requires one.
const AnnotationRestartedAt = "pgelastic.io/restartedAt"

// InstanceRollReason is why a rolling restart is happening. It is published rather than
// inferred because the three causes have different remedies: a configuration change waits
// for nothing, an explicit request was somebody's decision, and a draining node is a
// deadline the cluster imposed.
// +kubebuilder:validation:Enum=ConfigurationChanged;RestartRequested;NodeDraining
type InstanceRollReason string

const (
	// RollReasonConfigChanged is a parameter that PostgreSQL can only adopt at startup.
	RollReasonConfigChanged InstanceRollReason = "ConfigurationChanged"
	// RollReasonRestartRequested is AnnotationRestartedAt.
	RollReasonRestartRequested InstanceRollReason = "RestartRequested"
	// RollReasonNodeDraining is the primary's node having been made unschedulable. Without
	// this the primary PodDisruptionBudget blocks the eviction and the drain hangs, because
	// nothing else in the system ever initiates the switchover it is waiting for.
	RollReasonNodeDraining InstanceRollReason = "NodeDraining"
)

// InstanceRollStep is what the roll is doing to the member it names, so that a roll that
// has stopped moving says where it stopped.
// +kubebuilder:validation:Enum=Quiescing;SwitchingOver;Restarting;Blocked;Stalled
type InstanceRollStep string

const (
	// RollStepQuiescing is holding this instance's clients at the proxy, before a role
	// change they must not be dropped by.
	RollStepQuiescing InstanceRollStep = "Quiescing"
	// RollStepSwitchingOver is waiting for another member to take the primary role.
	RollStepSwitchingOver InstanceRollStep = "SwitchingOver"
	// RollStepRestarting is waiting for one member's Pod to be recreated and come back
	// Ready on the new configuration.
	RollStepRestarting InstanceRollStep = "Restarting"
	// RollStepBlocked is a roll that has work to do and may not do it yet: the instance is
	// short of a member, or a failover is in flight. It clears itself, and the message
	// says what it is waiting for.
	RollStepBlocked InstanceRollStep = "Blocked"
	// RollStepStalled is a roll that gave up rather than kept waiting, and gave the
	// clients back when it did.
	//
	// It is a separate step from Blocked because it does not clear itself. Something on
	// the instance is holding a backend that will never be returned - a session with
	// temporary tables, a LISTEN registration or a session advisory lock - and until it
	// ends, every attempt queues every other client behind a handover that cannot happen.
	// So the roll waits a long time between attempts instead of a short one, and says so.
	RollStepStalled InstanceRollStep = "Stalled"
)

// DataDurability selects what happens to commits when the synchronous quorum cannot be
// satisfied.
//
// Required stalls commits until quorum returns, which is a first-class alertable state
// rather than a hang: the proxy compares pg_stat_replication against the loaded num_sync
// and fails fast so a write-stalled instance cannot pin every pooled backend. Preferred
// degrades to asynchronous replication, trading acknowledged-commit durability for
// availability.
// +kubebuilder:validation:Enum=Required;Preferred
type DataDurability string

const (
	DataDurabilityRequired  DataDurability = "Required"
	DataDurabilityPreferred DataDurability = "Preferred"
)

// SynchronousCommitLevel is the synchronous_commit value applied instance-wide.
//
// The values below "remote_write" are deliberately absent: "local" and "off" acknowledge
// a commit no standby has seen, which makes the quorum-gated failover check a lie.
// +kubebuilder:validation:Enum=on;remote_apply;remote_write
type SynchronousCommitLevel string

const (
	SynchronousCommitOn          SynchronousCommitLevel = "on"
	SynchronousCommitRemoteApply SynchronousCommitLevel = "remote_apply"
	SynchronousCommitRemoteWrite SynchronousCommitLevel = "remote_write"
)

// InstanceDrainMode controls whether the operator is actively evacuating tenants off an
// instance.
// +kubebuilder:validation:Enum=Never;Requested
type InstanceDrainMode string

const (
	InstanceDrainNever     InstanceDrainMode = "Never"
	InstanceDrainRequested InstanceDrainMode = "Requested"
)

// InstanceRole is the replication role of a single member pod.
// +kubebuilder:validation:Enum=primary;replica;unknown
type InstanceRole string

const (
	InstanceRolePrimary InstanceRole = "primary"
	InstanceRoleReplica InstanceRole = "replica"
	InstanceRoleUnknown InstanceRole = "unknown"
)

// InstancePhase is a display-only summary of the conditions, present for kubectl get
// ergonomics. Controllers must branch on conditions, never on this.
// +kubebuilder:validation:Enum=Pending;Bootstrapping;Ready;FailingOver;Recloning;Degraded;Draining;Terminating
type InstancePhase string

const (
	InstancePhasePending       InstancePhase = "Pending"
	InstancePhaseBootstrapping InstancePhase = "Bootstrapping"
	InstancePhaseReady         InstancePhase = "Ready"
	InstancePhaseFailingOver   InstancePhase = "FailingOver"
	InstancePhaseRecloning     InstancePhase = "Recloning"
	InstancePhaseDegraded      InstancePhase = "Degraded"
	InstancePhaseDraining      InstancePhase = "Draining"
	InstancePhaseTerminating   InstancePhase = "Terminating"
)

// BackupType distinguishes the pgBackRest backup kinds.
// +kubebuilder:validation:Enum=Full;Differential;Incremental
type BackupType string

const (
	BackupTypeFull         BackupType = "Full"
	BackupTypeDifferential BackupType = "Differential"
	BackupTypeIncremental  BackupType = "Incremental"
)

// TargetPrimaryPending is the reserved sentinel written to status.targetPrimary in phase
// one of a failover, before a candidate has been chosen. It exists so that
// "targetPrimary != currentPrimary" is a *total* signal for "failover in progress, freeze
// everything", decidable with a single comparison and no tri-state.
const TargetPrimaryPending = "pending"

// GUCValue is a PostgreSQL parameter value as written to custom.conf.
//
// A line break is refused rather than escaped. postgresql.conf is line-oriented, so a value
// carrying one stops being a value and becomes a directive of its own - which is how a
// parameter nobody owns turns into `fsync = off`, past a denylist that matches by name.
// +kubebuilder:validation:MaxLength=1024
// +kubebuilder:validation:Pattern=`^[^\n\r]*$`
type GUCValue string

// PgInstanceSpec is the desired state of one provisioned PostgreSQL instance and its
// replica set.
//
// Whether the restore marker is set is fixed for the life of the instance, and that has to
// be said at this level rather than on the field. A CEL rule written on an optional field is
// not evaluated when the field is absent, so `self == oldSelf` on restore catches an edit to
// it and catches neither its deletion nor its arrival - which are the two edits somebody
// would actually make.
//
// Both directions corrupt something outside this object, in opposite ways. A restored
// instance carries its source's system identifier verbatim, so its stanza IS the source's,
// and the marker is the only thing stopping its agent archiving: clear it and the instance
// pushes a forked timeline into the production archive it was recovered from, leaving
// neither history restorable. Add it to an ordinary instance and the same switch goes the
// other way - the agent starts returning success from archive_command without archiving, so
// PostgreSQL recycles WAL that never reached the repository and the recovery window is gone
// with no error anywhere. Neither is an edit; an instance is one kind or the other for life.
// +kubebuilder:validation:XValidation:rule="has(oldSelf.restore) == has(self.restore)",message="whether restore is set cannot change: this marker decides whether the agent archives at all, so adding it silently stops archiving and removing it starts a forked timeline in the source's own stanza"
type PgInstanceSpec struct {
	// poolRef names the PgElasticPool in the same namespace that owns this instance.
	// It is immutable: the pool holds the reservation ledger that this instance's
	// allocatable capacity feeds, and reparenting would silently invalidate both sides.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="poolRef is immutable"
	// +required
	PoolRef corev1.LocalObjectReference `json:"poolRef"`

	// class names the PgElasticClass sizing tier. It is immutable because max_connections
	// is derived from it and is PGC_POSTMASTER; changing tiers is an instance-add plus a
	// tenant migration, not an edit.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="class is immutable"
	// +required
	Class string `json:"class"`

	// postgresVersion is pinned to 18 for the whole of v1. There is no major-version
	// upgrade path, so the field exists only to make the assumption explicit and to leave
	// room for one later.
	// +kubebuilder:validation:Enum="18"
	// +kubebuilder:default="18"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="postgresVersion is immutable"
	// +optional
	PostgresVersion *string `json:"postgresVersion,omitempty"`

	// highAvailability configures the replica set, the synchronous quorum and the
	// promotion lease.
	// +kubebuilder:default={}
	// +optional
	HighAvailability *InstanceHighAvailability `json:"highAvailability,omitempty"`

	// storage configures the two mandatory PVC roles, PG_DATA and PG_WAL.
	// +required
	Storage InstanceStorage `json:"storage"`

	// resources is applied to the postgres container of every member pod.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// parameters carries user-settable PostgreSQL GUCs only.
	//
	// The operator maintains two lists derived from pg_settings.context at build time:
	// FIXED parameters, whose value the operator computes, and BLOCKED parameters, which
	// must stay at the operator's value. Naming either here is rejected by the validating
	// webhook and additionally dropped by the config generator, so a stale object cannot
	// poison a pod that reads it later.
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(name, name.matches('^[a-zA-Z_][a-zA-Z0-9_]*([.][a-zA-Z_][a-zA-Z0-9_]*)?$'))",message="a parameter name must be a PostgreSQL identifier, optionally qualified by an extension prefix"
	// +optional
	Parameters map[string]GUCValue `json:"parameters,omitempty"`

	// restore marks this instance as one created by recovering a repository rather than by
	// initdb, and carries what recovery needs.
	//
	// It is immutable and only ever read at bootstrap: an instance that has finished
	// recovering is an ordinary instance, and the record is kept so that where it came from
	// is answerable afterwards rather than folklore.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore is immutable"
	// +optional
	Restore *InstanceRestore `json:"restore,omitempty"`

	// backup configures physical backup and WAL archiving, both executed by shelling out
	// to pgBackRest.
	// +optional
	Backup *InstanceBackup `json:"backup,omitempty"`

	// perTenantLogicalBackup configures the nightly per-database pg_dump that covers the
	// "a tenant deleted their own data" case, which physical backup cannot address at
	// tenant granularity without a scratch instance.
	// +optional
	PerTenantLogicalBackup *PerTenantLogicalBackup `json:"perTenantLogicalBackup,omitempty"`

	// admission controls whether the placement scheduler may bind new tenants here.
	// +kubebuilder:default={}
	// +optional
	Admission *InstanceAdmission `json:"admission,omitempty"`

	// drain requests evacuation of every tenant bound to this instance.
	// +kubebuilder:default={}
	// +optional
	Drain *InstanceDrain `json:"drain,omitempty"`
}

// InstanceHighAvailability describes the replica set and the rules that govern promotion.
type InstanceHighAvailability struct {
	// replicas is the total member count including the primary. Three is the only
	// topology the quorum gate is designed around: with quorum "ANY 1" the failover gate
	// R + W > N needs both standbys reachable.
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:validation:Maximum=5
	// +kubebuilder:default=3
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// synchronousCommit is applied instance-wide.
	// +kubebuilder:default=on
	// +optional
	SynchronousCommit *SynchronousCommitLevel `json:"synchronousCommit,omitempty"`

	// quorum is the leading clause of synchronous_standby_names, for example "ANY 1".
	// The member list is appended by the operator and rewritten using Patroni's ordering
	// rule: grow the quorum set before numsync, shrink numsync before the quorum set,
	// never both in one reload.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:default="ANY 1"
	// +optional
	Quorum *string `json:"quorum,omitempty"`

	// dataDurability selects stall-on-quorum-loss versus degrade-to-async.
	// +kubebuilder:default=Required
	// +optional
	DataDurability *DataDurability `json:"dataDurability,omitempty"`

	// failoverQuorum gates automatic promotion on quorum evidence read out of the live
	// postmaster rather than out of this spec. Turning it off permits promoting a standby
	// that cannot be proven to hold the last acknowledged commit.
	// +kubebuilder:default=true
	// +optional
	FailoverQuorum *bool `json:"failoverQuorum,omitempty"`

	// switchoverTimeout bounds a planned, operator-initiated role change end to end.
	// +kubebuilder:default="60s"
	// +optional
	SwitchoverTimeout *metav1.Duration `json:"switchoverTimeout,omitempty"`

	// failoverDelay debounces an unhealthy primary before an unplanned failover starts,
	// and is deliberately non-zero. A spurious failover costs a timeline bump, a rewind or
	// full re-clone, a window at 2/3 redundancy during which failover is impossible, and a
	// connection reset for every tenant on the instance. All of that is far more expensive
	// than ten extra seconds of downtime. The debounce origin is persisted in
	// status.currentPrimaryFailingSince so it survives an operator restart.
	// +kubebuilder:default="10s"
	// +optional
	FailoverDelay *metav1.Duration `json:"failoverDelay,omitempty"`

	// primaryLease tunes the coordination.k8s.io Lease that provides mutual exclusion
	// between primaries.
	// +kubebuilder:default={}
	// +optional
	PrimaryLease *PrimaryLeaseSpec `json:"primaryLease,omitempty"`
}

// PrimaryLeaseSpec parameterises the promotion Lease, which is held by the in-pod agent
// rather than the operator so that a dead operator cannot cause an unnecessary failover.
//
// These values also set the proxy's fencing deadline: a candidate cannot take over a held
// lease until leaseDuration has elapsed without renewal, and the proxy must sever
// old-epoch sockets within one retryPeriod. Shortening leaseDuration therefore shortens
// the fence deadline in lockstep, which is why both live in one struct and are validated
// together.
// The rules are has()-guarded because the enclosing field defaults to {} and the API
// server evaluates a declared default against them at CRD registration, before per-field
// defaults exist. An absent field is satisfied by its own default, so skipping the
// comparison here loses nothing.
// +kubebuilder:validation:XValidation:rule="!has(self.leaseDuration) || !has(self.renewDeadline) || duration(self.leaseDuration) > duration(self.renewDeadline)",message="leaseDuration must be greater than renewDeadline"
// +kubebuilder:validation:XValidation:rule="!has(self.renewDeadline) || !has(self.retryPeriod) || duration(self.renewDeadline).getMilliseconds() * 5 > duration(self.retryPeriod).getMilliseconds() * 6",message="5 * renewDeadline must be greater than 6 * retryPeriod"
type PrimaryLeaseSpec struct {
	// leaseDuration is how long a lease stays valid without renewal. Take-over compares
	// RenewTime for equality only, never ordering, so the previous holder's clock is never
	// trusted.
	// +kubebuilder:default="15s"
	// +optional
	LeaseDuration *metav1.Duration `json:"leaseDuration,omitempty"`

	// renewDeadline is how long the holder keeps trying to renew before treating the loss
	// as terminal and stopping its postmaster.
	// +kubebuilder:default="10s"
	// +optional
	RenewDeadline *metav1.Duration `json:"renewDeadline,omitempty"`

	// retryPeriod is the interval between renewal and acquisition attempts. Per-attempt
	// acquire timeout is leaseDuration + 3 * retryPeriod.
	// +kubebuilder:default="2s"
	// +optional
	RetryPeriod *metav1.Duration `json:"retryPeriod,omitempty"`

	// releasedLeaseDuration is the short validity stamped on a lease released
	// cooperatively, so a planned switchover does not wait out a full leaseDuration.
	// +kubebuilder:default="1s"
	// +optional
	ReleasedLeaseDuration *metav1.Duration `json:"releasedLeaseDuration,omitempty"`
}

// InstanceStorage describes the two PVC roles. There are exactly two; declarative
// tablespaces are out of scope for v1.
// +kubebuilder:validation:XValidation:rule="quantity(self.size).compareTo(quantity(oldSelf.size)) >= 0",message="storage size cannot be decreased"
type InstanceStorage struct {
	// size is the PG_DATA volume size. It may only grow: the underlying StorageClass must
	// have allowVolumeExpansion, and PGDATA growth is a legitimate autoscaling input.
	// +required
	Size resource.Quantity `json:"size"`

	// className is the StorageClass for PG_DATA. Certified storage is network block
	// (EBS / PD / Azure Disk) with ReadWriteOncePod. NFS and network filesystems are
	// unsupported: their fsync honesty cannot be validated by the operator and a lie
	// surfaces only as corruption after a node loss.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ClassName *string `json:"className,omitempty"`

	// walVolume is MANDATORY, not optional, and is why this field is not a pointer.
	// pgelastic sells connection availability; a full pg_wal PANICs the primary and
	// vaporizes every tenant's guarantee on that instance at once. Keeping WAL on its own
	// volume also lets it be resized on its own trigger, because pg_wal growth means
	// archive backlog and auto-expanding it would hide the fault rather than surface it.
	// +required
	WALVolume WALVolume `json:"walVolume"`
}

// WALVolume describes the PG_WAL PVC.
// +kubebuilder:validation:XValidation:rule="quantity(self.size).compareTo(quantity(oldSelf.size)) >= 0",message="walVolume size cannot be decreased"
type WALVolume struct {
	// size bounds max_slot_wal_keep_size, which the operator derives from it so that
	// slot retention plus wal_keep_size plus archive backlog plus checkpoint headroom stay
	// under 70 percent of the volume. Losing a replication slot costs a bounded replica
	// rebuild; losing the primary costs every tenant's guarantee.
	// +required
	Size resource.Quantity `json:"size"`

	// className is the StorageClass for PG_WAL.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ClassName *string `json:"className,omitempty"`
}

// InstanceRestore is what an instance being recovered from a repository needs to know.
//
// The repository itself is not here: it is spec.backup.objectStore, because a recovering
// instance reads the source's archive through exactly the configuration an archiving
// instance writes it with. Read access and write refusal are the same credential, so the
// refusal is behavioural - the instance manager declines to archive at all while this is
// set - rather than a matter of withholding anything.
type InstanceRestore struct {
	// sourceInstanceName is where the repository came from, recorded for provenance.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	SourceInstanceName string `json:"sourceInstanceName,omitempty"`

	// stanza is the repository stanza to restore from. It is named after the source's
	// system identifier, which a restored instance inherits: a restore copies the control
	// file verbatim, so this instance will address the same stanza while running on a
	// forked timeline, and must never archive into it.
	// +kubebuilder:validation:MaxLength=128
	// +required
	Stanza string `json:"stanza"`

	// backupID is the base backup to start from. Empty lets the repository choose, which
	// only works for a target that can be ordered against the catalogue.
	// +kubebuilder:validation:MaxLength=128
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// target is where recovery stops. Omitted replays everything the archive holds.
	// +optional
	Target *RecoveryTarget `json:"target,omitempty"`

	// enforcedParameterFloor are the source's values for the five settings PostgreSQL
	// refuses to begin recovery below.
	//
	// Without them a restore into a smaller instance FATALs at start-up with a message that
	// names the parameter and not the cause, after it has already pulled the whole base
	// backup down.
	// +optional
	EnforcedParameterFloor map[string]int32 `json:"enforcedParameterFloor,omitempty"`
}

// InstanceBackup configures physical backup and WAL archiving.
type InstanceBackup struct {
	// objectStore locates the repository. The stanza path underneath it is keyed on the
	// PostgreSQL system identifier rather than the instance name, so an instance recreated
	// under a reused name cannot interleave its WAL into a predecessor's archive.
	// +required
	ObjectStore ObjectStore `json:"objectStore"`

	// retention bounds full-backup and WAL history.
	// +kubebuilder:default={}
	// +optional
	Retention *RetentionPolicy `json:"retention,omitempty"`

	// schedule is a five-field cron expression for the full backup.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:default="0 2 * * *"
	// +optional
	Schedule *string `json:"schedule,omitempty"`

	// backupStandby takes the backup from a standby so the primary's connection budget and
	// I/O stay untouched. For a product selling connection guarantees this is the cheapest
	// large win available, which is why it defaults on.
	// +kubebuilder:default=true
	// +optional
	BackupStandby *bool `json:"backupStandby,omitempty"`
}

// PerTenantLogicalBackup configures nightly per-database dumps.
type PerTenantLogicalBackup struct {
	// enabled turns the nightly dump sweep on.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// schedule is a five-field cron expression for the sweep.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:default="0 4 * * *"
	// +optional
	Schedule *string `json:"schedule,omitempty"`

	// retention is how long dumps are kept.
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:default="14d"
	// +optional
	Retention *string `json:"retention,omitempty"`

	// maxConcurrentDumps caps simultaneous dumps on this instance. Each open dump holds
	// back the instance-wide xmin horizon, which blocks vacuum for every tenant on the
	// instance, not just the one being dumped. Dump connections are additionally charged to
	// a reserved pool held outside every tenant's guarantee and burst budget.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=16
	// +kubebuilder:default=4
	// +optional
	MaxConcurrentDumps *int32 `json:"maxConcurrentDumps,omitempty"`

	// dumpTimeout aborts and reschedules an overrunning dump rather than letting it hold
	// the xmin horizon open indefinitely.
	// +kubebuilder:default="2h"
	// +optional
	DumpTimeout *metav1.Duration `json:"dumpTimeout,omitempty"`
}

// InstanceAdmission controls tenant placement onto this instance.
type InstanceAdmission struct {
	// schedulable allows the placement scheduler to bind new tenants here.
	// +kubebuilder:default=true
	// +optional
	Schedulable *bool `json:"schedulable,omitempty"`

	// cordoned blocks new bindings while leaving existing tenants in place, mirroring
	// kubectl cordon. The operator also sets this itself while the instance is re-cloning
	// or its archive is degraded.
	// +kubebuilder:default=false
	// +optional
	Cordoned *bool `json:"cordoned,omitempty"`
}

// InstanceDrain requests evacuation of this instance.
type InstanceDrain struct {
	// mode set to Requested cordons the instance and emits a PgTenantMigration per bound
	// tenant, subject to the pool's concurrency limit and blackout windows.
	// +kubebuilder:default=Never
	// +optional
	Mode *InstanceDrainMode `json:"mode,omitempty"`
}

// PgInstanceStatus is the observed state of a PgInstance.
type PgInstanceStatus struct {
	// observedGeneration is the metadata.generation this status was computed from.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase is a display-only pure function of the conditions.
	// +kubebuilder:default=Pending
	// +optional
	Phase InstancePhase `json:"phase,omitempty"`

	// primaryEpoch is the fence token.
	//
	// It is a monotonic counter derived from the promotion Lease's LeaderTransitions field
	// and published into the custom GUC pgelastic.primary_epoch, which makes it readable
	// with a plain SHOW over any backend connection and unable to drift from the running
	// postmaster. Every bump must accompany a timeline bump, cross-checked against
	// pg_control_checkpoint().timeline_id.
	//
	// The Rust proxy consumes it because Kubernetes Services do not tear down established
	// TCP connections: kube-proxy never touches ESTABLISHED conntrack entries, so a demoted
	// primary keeps serving writes that pg_rewind is about to discard. Every client socket
	// terminates at the proxy and every backend socket originates from it, so the proxy can
	// sever rather than merely deregister. It learns the epoch by three independent paths,
	// acting on whichever fires first: a watch on this field, a push from the promoting
	// agent, and - the only path that is safe under partition, and therefore mandatory - a
	// GUC_REPORT ParameterStatus carried on every backend connection. The proxy's in-memory
	// epoch never decreases: observing a lower one is a fence trigger, not new information.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PrimaryEpoch int64 `json:"primaryEpoch,omitempty"`

	// currentPrimary is the member that has actually completed promotion. It is written by
	// the promoted pod itself, last in the promotion sequence, after the checkpoint, the
	// synchronous_standby_names rewrite and the epoch bump.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	CurrentPrimary string `json:"currentPrimary,omitempty"`

	// targetPrimary is the operator's decision, written before any promotion begins.
	//
	// On detecting an unhealthy primary the operator first writes the reserved sentinel
	// "pending" (TargetPrimaryPending) and strips the role label so Services stop selecting
	// the old primary; only once every non-primary member reports its WAL receiver down
	// does it write a real candidate name. Because the sentinel is never a member name,
	// targetPrimary != currentPrimary is a total signal meaning "failover in progress,
	// freeze everything", decidable with one comparison.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	TargetPrimary string `json:"targetPrimary,omitempty"`

	// currentPrimaryFailingSince is when the primary was first observed unhealthy. It is
	// persisted so the failoverDelay debounce is not reset by an operator restart.
	// +optional
	CurrentPrimaryFailingSince *metav1.Time `json:"currentPrimaryFailingSince,omitempty"`

	// instances reports every member pod.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=8
	// +optional
	Instances []InstanceMemberStatus `json:"instances,omitempty"`

	// quorumEvidence records what PostgreSQL actually loaded, not what the spec asked for.
	// +optional
	QuorumEvidence *QuorumEvidence `json:"quorumEvidence,omitempty"`

	// roll reports a rolling restart in progress, and is absent when none is.
	// +optional
	Roll *InstanceRollStatus `json:"roll,omitempty"`

	// capacity is this instance's contribution to the pool's connection budget.
	// +optional
	Capacity *InstanceCapacityStatus `json:"capacity,omitempty"`

	// tenants is the number of tenant databases currently bound here.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Tenants int32 `json:"tenants,omitempty"`

	// storage reports allocated and used bytes for both volumes.
	// +optional
	Storage *InstanceStorageStatus `json:"storage,omitempty"`

	// collationContract is the instance's text-handling and on-disk identity, recorded at
	// bootstrap. Any migration or pool-join whose contract differs is refused: restoring
	// under a different collation produces indexes silently inconsistent with their heap
	// ordering, which yields wrong results and no error.
	// +optional
	CollationContract *CollationContract `json:"collationContract,omitempty"`

	// pendingBackup names the backup a member has been asked to take, and which member.
	//
	// The operator elects; the member's own agent acts. That split is the same one
	// targetPrimary encodes, and for the same reason: the agent already reads this object
	// on every observe tick, so an election costs no new watch and no new transport, and
	// the operator never runs anything inside a Pod.
	// +optional
	PendingBackup *PendingBackup `json:"pendingBackup,omitempty"`

	// lastBackup summarises the most recent successful physical backup.
	// +optional
	LastBackup *BackupSummary `json:"lastBackup,omitempty"`

	// lastRestoreRehearsal records the most recent end-to-end restore into a throwaway
	// namespace. A backup pipeline never exercised end to end has an unknown success rate,
	// not a good one.
	// +optional
	LastRestoreRehearsal *RestoreRehearsalSummary `json:"lastRestoreRehearsal,omitempty"`

	// archiveHealth drives operator behaviour, not just dashboards: a degraded archive
	// stops new tenant admission and blocks migrations into this instance. Archiving is
	// never auto-disabled to relieve pressure; archive-push-queue-max makes that decision
	// explicit and recorded instead.
	// +optional
	ArchiveHealth *ArchiveHealthStatus `json:"archiveHealth,omitempty"`

	// conditions represent the current state of the PgInstance.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// InstanceMemberStatus reports one member pod, as seen by that pod's own agent.
type InstanceMemberStatus struct {
	// name is the member pod name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// role is the observed role. Two members simultaneously reporting primary is a
	// dedicated alarm that freezes write admission and refuses all automated remediation,
	// never a tiebreak input: silent recovery from split brain hides the data loss.
	// +kubebuilder:default=unknown
	// +optional
	Role InstanceRole `json:"role,omitempty"`

	// agentSession identifies the instance manager process currently running on this
	// member. It changes on every agent start, including a container restart in place.
	//
	// It is published so that work a member claimed can be told apart from work nobody is
	// doing any more: a backup runs as a goroutine inside a process that can die, and
	// without this a backup left Running by a dead agent is indistinguishable from one
	// still in progress.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	AgentSession string `json:"agentSession,omitempty"`

	// lsn is the member's latest WAL position.
	// +kubebuilder:validation:MaxLength=32
	// +optional
	LSN string `json:"lsn,omitempty"`

	// receivedLSN is how far this member's WAL receiver has written, and replayLSN how far
	// recovery has replayed. They are separate fields because candidate selection orders on
	// both, received first: WAL that has been received is durable on this member whether or
	// not it has been replayed yet, so the member holding more of it is the one that loses
	// less on promotion.
	// +kubebuilder:validation:MaxLength=32
	// +optional
	ReceivedLSN string `json:"receivedLSN,omitempty"`

	// replayLSN is how far recovery has replayed.
	// +kubebuilder:validation:MaxLength=32
	// +optional
	ReplayLSN string `json:"replayLSN,omitempty"`

	// timeline is a first-class term in candidate selection, ordered ahead of LSN. Any
	// member below the cluster's last known timeline is disqualified outright.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Timeline int32 `json:"timeline,omitempty"`

	// healthy reports whether the member's status endpoint answered and its postmaster is
	// accepting connections.
	// +optional
	Healthy bool `json:"healthy,omitempty"`

	// inSyncSet reports whether the primary counts this member towards the synchronous
	// quorum. Members outside the recorded sync set are disqualified as candidates.
	// +optional
	InSyncSet bool `json:"inSyncSet,omitempty"`

	// walReceiverActive is checked at two distinct instants with two distinct meanings: a
	// candidate must have had an active receiver when the failure was detected, and every
	// member must have its receiver down before promotion may proceed.
	// +optional
	WALReceiverActive bool `json:"walReceiverActive,omitempty"`

	// walVolumeFull refuses this member as a promotion candidate outright. A primary whose
	// pg_wal cannot grow PANICs at its first checkpoint and takes every tenant on the
	// instance with it, so a full WAL volume is a refusal rather than a degraded state.
	// +optional
	WALVolumeFull bool `json:"walVolumeFull,omitempty"`

	// dataUsedBytes and walUsedBytes are the two volumes' usage as this member measured
	// them, from the same statfs that decides walVolumeFull.
	//
	// Per member rather than per instance because that is where they can be measured: only
	// the agent is inside the volume. The instance's own status.storage.used is the primary's
	// figure, because a standby's is a replica of the same data and averaging them would
	// describe no real filesystem.
	// +optional
	DataUsedBytes int64 `json:"dataUsedBytes,omitempty"`
	// +optional
	WALUsedBytes int64 `json:"walUsedBytes,omitempty"`

	// rejoining names the path this member is taking back onto the primary's history after
	// its own diverged: "rewinding" for pg_rewind, "recloning" for the pg_basebackup
	// fallback. It is empty the rest of the time.
	//
	// It is reported rather than merely logged because a re-clone runs for minutes to
	// hours, holds a connection and a replication slot for all of them, and leaves the
	// instance at reduced redundancy - during which quorum-gated failover is impossible.
	// The instance's burst headroom must not be counted as available while it is set.
	// +kubebuilder:validation:Enum=rewinding;recloning
	// +optional
	Rejoining string `json:"rejoining,omitempty"`
}

// InstanceRollStatus is the roll's own account of itself: which member it is disrupting,
// why, what it is doing to that member, and how many are still to come.
//
// It exists because a rolling restart is the one operation that is both routine and slow
// enough to be watched, and because its two interesting failures - a client set that will
// not drain, and an instance that is not fit to lose a member - are indistinguishable from
// "still working" without it.
type InstanceRollStatus struct {
	// member is the Pod being rolled right now.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Member string `json:"member,omitempty"`

	// reason is what asked for the roll.
	// +optional
	Reason InstanceRollReason `json:"reason,omitempty"`

	// step is what is being done to member.
	// +optional
	Step InstanceRollStep `json:"step,omitempty"`

	// pending is how many members are still to be rolled, including member itself. It
	// reaches zero on the reconcile that clears this whole record.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Pending int32 `json:"pending,omitempty"`

	// startedAt is when this member's step began, which is what makes a stalled roll
	// visible as one rather than as a slow one.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// message says in one sentence what is being waited for.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Message string `json:"message,omitempty"`
}

// QuorumEvidence is written only by the member that is both currentPrimary and
// targetPrimary, and records the synchronous_standby_names the postmaster actually
// loaded, read back out of a custom GUC rather than taken from the spec. Keeping it
// separate from the operator's decision is what lets the operator veto itself: a
// partially applied reload cannot fool the R + W > N gate, and empty or missing evidence
// denies failover outright.
type QuorumEvidence struct {
	// synchronousStandbyNames is the loaded value, verbatim.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	SynchronousStandbyNames string `json:"synchronousStandbyNames,omitempty"`

	// numSync is W in the R + W > N gate.
	// +kubebuilder:validation:Minimum=0
	// +optional
	NumSync int32 `json:"numSync,omitempty"`

	// votingMembers is N in the R + W > N gate. It is parsed out of the loaded clause, not
	// out of pg_stat_replication: a member PostgreSQL never loaded as a voter is not a
	// voter, however healthily it happens to be streaming.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	// +optional
	VotingMembers []string `json:"votingMembers,omitempty"`

	// streamingMembers is the subset of the voters pg_stat_replication reported as actually
	// streaming when this record was written. It carries two meanings: fewer of them than
	// numSync means commits are stalling right now, and membership is the proof that a
	// candidate had an active WAL receiver at detection time, since PostgreSQL counts a
	// standby towards the quorum only while it is streaming.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	// +optional
	StreamingMembers []string `json:"streamingMembers,omitempty"`

	// reportedBy is the member that wrote this record.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ReportedBy string `json:"reportedBy,omitempty"`

	// observedAt is when the value was read from the postmaster. Stale evidence is treated
	// as missing evidence.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// InstanceCapacityStatus publishes the max_connections split that the pool's capacity
// budget is summed from.
//
// max_connections = allocatable + reservedForAdmin + replicationSlots + agent overhead,
// where allocatable is the only part tenants may draw on. The number is monotonically
// non-decreasing within an instance generation: capacity is reclaimed by migrating tenants
// and retiring instances, never by shrinking it, because max_connections is PGC_POSTMASTER
// and a decrease costs every tenant on the instance their connections.
type InstanceCapacityStatus struct {
	// maxConnections is the value PostgreSQL is actually running with.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxConnections int32 `json:"maxConnections,omitempty"`

	// reservedForAdmin covers superuser_reserved_connections plus reserved_connections.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReservedForAdmin int32 `json:"reservedForAdmin,omitempty"`

	// replicationSlots covers streaming standbys plus operator-owned migration slots.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReplicationSlots int32 `json:"replicationSlots,omitempty"`

	// allocatable is what tenants may draw on. A re-cloning instance publishes zero here:
	// its headroom must not count as available while quorum-gated failover is impossible.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Allocatable int32 `json:"allocatable,omitempty"`

	// inUse is the observed backend connection count against allocatable.
	// +kubebuilder:validation:Minimum=0
	// +optional
	InUse int32 `json:"inUse,omitempty"`
}

// InstanceStorageStatus reports volume consumption. Scale-down eligibility is judged on
// allocated, never on used.
type InstanceStorageStatus struct {
	// allocated is the requested PG_DATA volume size.
	// +optional
	Allocated *resource.Quantity `json:"allocated,omitempty"`

	// used is the PG_DATA filesystem usage.
	// +optional
	Used *resource.Quantity `json:"used,omitempty"`

	// walUsed is PG_WAL filesystem usage. Growth here means archive backlog, so it is an
	// alerting input rather than an autoscaling one.
	// +optional
	WALUsed *resource.Quantity `json:"walUsed,omitempty"`
}

// BackupSummary describes the most recent successful physical backup.
type BackupSummary struct {
	// at is when the backup completed.
	// +optional
	At *metav1.Time `json:"at,omitempty"`

	// type distinguishes full, differential and incremental.
	// +optional
	Type BackupType `json:"type,omitempty"`

	// sizeBytes is the repository size of this backup after block-incremental and bundling.
	// +kubebuilder:validation:Minimum=0
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// verified reports whether pgbackrest verify has validated this backup's content.
	// Verification is part of the backup, not an afterthought.
	// +optional
	Verified bool `json:"verified,omitempty"`

	// sourceMaxConnections is the max_connections recorded in the backup catalog. Recovery
	// FATALs unless the restore target's enforced parameters are at least the source's, so
	// a restore target must be sized before it is provisioned.
	// +kubebuilder:validation:Minimum=0
	// +optional
	SourceMaxConnections int32 `json:"sourceMaxConnections,omitempty"`
}

// RestoreRehearsalSummary records the periodic real restore into a throwaway namespace.
type RestoreRehearsalSummary struct {
	// at is when the rehearsal last succeeded end to end, including pg_verifybackup and a
	// smoke query.
	// +optional
	At *metav1.Time `json:"at,omitempty"`

	// durationSeconds is the measured RTO for a whole-instance restore.
	// +kubebuilder:validation:Minimum=0
	// +optional
	DurationSeconds int64 `json:"durationSeconds,omitempty"`
}

// PendingBackup is the operator's election of a member to take a named backup.
//
// It is a command carried in status, which is unusual and deliberate: targetPrimary is the
// same shape, and for the same reason. The agent is the only thing that may act inside its
// own Pod, and this object is the only channel it already reads.
type PendingBackup struct {
	// name is the PgBackup in this namespace.
	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// member is the Pod elected to take it. Only that member acts on this.
	// +kubebuilder:validation:MaxLength=253
	// +required
	Member string `json:"member"`

	// requestedAt is when the election was made, so a member that never claims one can be
	// distinguished from one that has only just been asked.
	// +required
	RequestedAt metav1.Time `json:"requestedAt"`
}

// ArchiveHealthStatus reports WAL archiving, assembled from three inputs because none is
// sufficient alone: the pg_stat_archiver failure rate, the age of the last successful
// archive against archive_timeout, and a filesystem count of pg_wal/archive_status/*.ready
// (pg_stat_archiver has no backlog column).
type ArchiveHealthStatus struct {
	// healthy is the summary the admission and migration gates read.
	// +optional
	Healthy bool `json:"healthy,omitempty"`

	// failedCount is pg_stat_archiver.failed_count.
	// +kubebuilder:validation:Minimum=0
	// +optional
	FailedCount int64 `json:"failedCount,omitempty"`

	// lastArchivedWAL is the last segment successfully pushed.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	LastArchivedWAL string `json:"lastArchivedWAL,omitempty"`

	// lastArchivedAt is when that push completed.
	// +optional
	LastArchivedAt *metav1.Time `json:"lastArchivedAt,omitempty"`

	// readyBacklog counts segments waiting in pg_wal/archive_status.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReadyBacklog int32 `json:"readyBacklog,omitempty"`

	// lastFailureAt is when archiving last failed.
	// +optional
	LastFailureAt *metav1.Time `json:"lastFailureAt,omitempty"`

	// lastFailureMessage is the truncated error from the last failed push.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	LastFailureMessage string `json:"lastFailureMessage,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:categories=pgelastic,shortName=pgi
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.currentPrimary`
// +kubebuilder:printcolumn:name="Epoch",type=integer,JSONPath=`.status.primaryEpoch`
// +kubebuilder:printcolumn:name="Tenants",type=integer,JSONPath=`.status.tenants`
// +kubebuilder:printcolumn:name="Allocatable",type=integer,JSONPath=`.status.capacity.allocatable`
// +kubebuilder:printcolumn:name="In-Use",type=integer,JSONPath=`.status.capacity.inUse`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.status.targetPrimary`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PgInstance is one provisioned PostgreSQL instance and its replica set: HA topology,
// storage, backup, rated capacity and cordon/drain state.
//
// The object carries the finalizer pgelastic.io/drain-tenants
// (PgInstanceDrainTenantsFinalizer), which blocks deletion until every bound tenant has
// been migrated away or released.
type PgInstance struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PgInstance
	// +required
	Spec PgInstanceSpec `json:"spec"`

	// status defines the observed state of PgInstance
	// +optional
	Status PgInstanceStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PgInstanceList contains a list of PgInstance
type PgInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgInstance `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgInstance{}, &PgInstanceList{})
		return nil
	})
}
