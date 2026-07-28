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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TenantMigrationStrategy selects how the tenant's data is carried to the target instance.
//
// Online uses logical replication and is the norm: its cutover pause is a queued
// sub-second stall, so it is permitted at any time including business hours, which is
// what makes reactive rebalancing and autoscaling viable. Offline quiesces the tenant
// and moves it with pg_dump/pg_restore, pausing for tens of seconds; it is confined to
// the pool's nightly window and exists only as the fallback for tenants Online cannot
// carry. Auto lets the controller pick Online whenever preflight passes.
// +kubebuilder:validation:Enum=Auto;Online;Offline
type TenantMigrationStrategy string

const (
	TenantMigrationAuto    TenantMigrationStrategy = "Auto"
	TenantMigrationOnline  TenantMigrationStrategy = "Online"
	TenantMigrationOffline TenantMigrationStrategy = "Offline"
)

// TenantMigrationPhase is the display-only projection of the migration's conditions.
// The full state machine is documented on PgTenantMigration.
// +kubebuilder:validation:Enum=Preflight;Provisioning;PreWarm;Copying;Catchup;Quiescing;Cutover;Completed;Failed;Aborted;RolledBack
type TenantMigrationPhase string

const (
	// TenantMigrationPhasePreflight runs the blocking gate: replica identity, prepared
	// transactions, large objects, collation contract, storage headroom, source
	// utilization and tenant coldness.
	TenantMigrationPhasePreflight TenantMigrationPhase = "Preflight"
	// TenantMigrationPhaseProvisioning creates the target database, role and grants, and
	// the publication, slot and subscription named in status.
	TenantMigrationPhaseProvisioning TenantMigrationPhase = "Provisioning"
	// TenantMigrationPhasePreWarm opens and warms target backend connections so the
	// cutover pause is not spent on connection establishment.
	TenantMigrationPhasePreWarm TenantMigrationPhase = "PreWarm"
	// TenantMigrationPhaseCopying is the logical replication initial table sync.
	TenantMigrationPhaseCopying TenantMigrationPhase = "Copying"
	// TenantMigrationPhaseCatchup streams changes until lagBytes falls under
	// preflight.maxSourceLagBytes.
	TenantMigrationPhaseCatchup TenantMigrationPhase = "Catchup"
	// TenantMigrationPhaseQuiescing queues new transactions at the proxy, holds the client
	// sockets open and drains what is in flight.
	TenantMigrationPhaseQuiescing TenantMigrationPhase = "Quiescing"
	// TenantMigrationPhaseCutover waits for confirmed_flush_lsn to reach the source's
	// current WAL LSN, applies sequence handling, verifies, then flips the routing table.
	TenantMigrationPhaseCutover TenantMigrationPhase = "Cutover"
	// TenantMigrationPhaseCompleted means the tenant serves from the target. The source
	// database stays connection-refusing until rollbackDeadline, then is dropped.
	TenantMigrationPhaseCompleted TenantMigrationPhase = "Completed"
	// TenantMigrationPhaseFailed is a terminal stop with the cleanup ladder run and the
	// tenant still serving from the source.
	TenantMigrationPhaseFailed TenantMigrationPhase = "Failed"
	// TenantMigrationPhaseAborted is the same terminal state reached by request rather
	// than by error.
	TenantMigrationPhaseAborted TenantMigrationPhase = "Aborted"
	// TenantMigrationPhaseRolledBack means routing was flipped back to the source inside
	// rollbackWindow.
	TenantMigrationPhaseRolledBack TenantMigrationPhase = "RolledBack"
)

// SequenceHandlingMode selects what happens to the tenant's sequences at cutover.
// +kubebuilder:validation:Enum=SetvalWithGap;Skip
type SequenceHandlingMode string

const (
	// SequenceHandlingSetvalWithGap advances every target sequence past the source's last
	// value plus safetyGap during the cutover pause.
	SequenceHandlingSetvalWithGap SequenceHandlingMode = "SetvalWithGap"
	// SequenceHandlingSkip leaves target sequences untouched and is only ever correct for
	// a tenant with no sequences at all.
	SequenceHandlingSkip SequenceHandlingMode = "Skip"
)

// TenantMigrationPreflight is a hard gate, not a warning: a failing check stops the
// migration in Preflight rather than degrading it. Large objects and prepared
// transactions have no workaround in logical replication, so the corresponding checks
// are also enforced at tenant admission to keep Online the normal path.
type TenantMigrationPreflight struct {
	// requireReplicaIdentity fails the migration unless every table carries a PRIMARY KEY
	// or an explicit REPLICA IDENTITY. Without one, logical replication silently drops
	// UPDATE and DELETE rows on the target.
	// +kubebuilder:default=true
	// +optional
	RequireReplicaIdentity *bool `json:"requireReplicaIdentity,omitempty"`

	// forbidLargeObjects fails the migration when the tenant holds any pg_largeobject
	// entry, which logical replication does not carry.
	// +kubebuilder:default=true
	// +optional
	ForbidLargeObjects *bool `json:"forbidLargeObjects,omitempty"`

	// forbidPreparedTransactions fails the migration while any prepared transaction is
	// open on the tenant's database: it pins the source's oldest xmin and can neither be
	// replicated nor drained.
	// +kubebuilder:default=true
	// +optional
	ForbidPreparedTransactions *bool `json:"forbidPreparedTransactions,omitempty"`

	// maxSourceLagBytes is the replication lag under which Catchup may advance to
	// Quiescing. Leave unset to require the subscription to be fully caught up, which
	// trades a longer Catchup for the shortest possible pause.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxSourceLagBytes *int64 `json:"maxSourceLagBytes,omitempty"`

	// requireColdTenant refuses to move a tenant whose observed utilization is above the
	// pool's hot threshold. Moving a hot tenant consumes exactly the resource that is
	// already scarce on the source.
	// +kubebuilder:default=true
	// +optional
	RequireColdTenant *bool `json:"requireColdTenant,omitempty"`

	// forbidMoveWhenSourceUtilizationAbovePercent refuses to start while the source
	// instance is this busy, independently of how cold the tenant itself is.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=65
	// +optional
	ForbidMoveWhenSourceUtilizationAbovePercent *int32 `json:"forbidMoveWhenSourceUtilizationAbovePercent,omitempty"`
}

// TenantMigrationSequenceHandling controls the sequence reconciliation performed inside
// the cutover pause.
//
// Logical replication carries no sequence state through PostgreSQL 18; sequence
// synchronization lands in PostgreSQL 19. A target sequence therefore sits at its
// creation-time value, and skipping the setval step produces duplicate key violations
// hours later, once the tenant's inserts catch up with rows already copied.
type TenantMigrationSequenceHandling struct {
	// mode selects whether sequences are advanced at cutover.
	// +kubebuilder:default=SetvalWithGap
	// +optional
	Mode SequenceHandlingMode `json:"mode,omitempty"`

	// safetyGap is added to each source sequence's last value before it is applied to the
	// target, covering values cached in source backends that were never written to WAL.
	// It must exceed the largest CACHE setting the tenant uses.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1000
	// +optional
	SafetyGap *int64 `json:"safetyGap,omitempty"`
}

// TenantMigrationVerification records the equivalence evidence gathered before routing
// is flipped. Verification runs while the tenant is quiesced, so every check here is
// inside the measured pause and is chosen for cost accordingly.
type TenantMigrationVerification struct {
	// schemaFingerprintMatch reports whether the ordered catalog digests agree.
	// +optional
	SchemaFingerprintMatch *bool `json:"schemaFingerprintMatch,omitempty"`

	// rowCountsMatch reports whether per-relation row counts agree.
	// +optional
	RowCountsMatch *bool `json:"rowCountsMatch,omitempty"`

	// checksumsMatch reports whether the sampled content checksums agree. Unset when the
	// migration did not run content verification.
	// +optional
	ChecksumsMatch *bool `json:"checksumsMatch,omitempty"`

	// verifiedAt is when the last check completed.
	// +optional
	VerifiedAt *metav1.Time `json:"verifiedAt,omitempty"`
}

// PgTenantMigrationSpec defines the desired state of PgTenantMigration.
//
// The whole spec is immutable: this is a one-shot job object, and the phase machine
// commits to physical objects on the source and the target from Provisioning onward.
// Changing your mind means aborting and creating a new migration.
type PgTenantMigrationSpec struct {
	// tenantRef names the PgTenant being moved, in this namespace.
	// +required
	TenantRef corev1.LocalObjectReference `json:"tenantRef"`

	// targetInstanceRef names the destination PgInstance, in this namespace. It must
	// belong to the tenant's pool and present a byte-identical collation contract.
	// +required
	TargetInstanceRef corev1.LocalObjectReference `json:"targetInstanceRef"`

	// strategy selects the data movement mechanism.
	// +kubebuilder:default=Auto
	// +optional
	Strategy TenantMigrationStrategy `json:"strategy,omitempty"`

	// preflight tunes the blocking gate run before anything is provisioned.
	// +optional
	Preflight *TenantMigrationPreflight `json:"preflight,omitempty"`

	// sequenceHandling controls how sequences are carried across at cutover.
	// +optional
	SequenceHandling *TenantMigrationSequenceHandling `json:"sequenceHandling,omitempty"`

	// drainTimeout bounds how long Quiescing waits for in-flight transactions to finish
	// before the migration aborts back to the source.
	// +kubebuilder:default="30s"
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`

	// cutoverTimeout bounds the whole Cutover phase, including the final LSN wait,
	// sequence handling and verification. Exceeding it aborts back to the source, so it
	// is the upper bound on the pause clients can observe.
	// +kubebuilder:default="60s"
	// +optional
	CutoverTimeout *metav1.Duration `json:"cutoverTimeout,omitempty"`

	// rollbackWindow is how long the source database is kept intact and
	// connection-refusing after a successful cutover, during which routing can be flipped
	// back without restoring a backup. It is dropped once the window closes.
	// +kubebuilder:default="1h"
	// +optional
	RollbackWindow *metav1.Duration `json:"rollbackWindow,omitempty"`

	// approvedBy records the human who authorized this move. Required for tenants whose
	// PgWorkloadClass sets migration.requireApproval; the admission webhook rejects the
	// object when it is absent.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:MinLength=1
	// +optional
	ApprovedBy *string `json:"approvedBy,omitempty"`
}

// PgTenantMigrationStatus defines the observed state of PgTenantMigration.
type PgTenantMigrationStatus struct {
	// observedGeneration is the .metadata.generation this status was computed from.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase is a display-only pure function of the conditions.
	// +kubebuilder:default=Preflight
	// +optional
	Phase TenantMigrationPhase `json:"phase,omitempty"`

	// startedAt is when the controller began Preflight.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// completedAt is when the migration reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// sourceInstanceRef is resolved once at Preflight and then treated as fixed, so an
	// abort has an unambiguous instance to leave the tenant serving from even if the
	// tenant's binding is concurrently rewritten.
	// +optional
	SourceInstanceRef *corev1.LocalObjectReference `json:"sourceInstanceRef,omitempty"`

	// lagBytes is the source WAL distance between the slot's confirmed_flush_lsn and the
	// primary's current LSN.
	// +optional
	LagBytes *int64 `json:"lagBytes,omitempty"`

	// copiedTables counts relations whose initial sync has finished.
	// +kubebuilder:validation:Minimum=0
	// +optional
	CopiedTables *int32 `json:"copiedTables,omitempty"`

	// totalTables is the relation count preflight found on the source.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TotalTables *int32 `json:"totalTables,omitempty"`

	// pauseDurationMillis is how long clients were queued across Quiescing and Cutover.
	// It is also exported as a Prometheus histogram; the target is a p99 below one second
	// with clients queued and never dropped. Dropping connections and relying on client
	// retry is what the comparable managed-service elastic pool move does, so this number
	// is a product commitment rather than a diagnostic.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PauseDurationMillis *int64 `json:"pauseDurationMillis,omitempty"`

	// verification is the equivalence evidence gathered before the routing flip.
	// +optional
	Verification *TenantMigrationVerification `json:"verification,omitempty"`

	// replicationSlotName is the source slot backing the subscription. Recorded so the
	// cleanup ladder and the orphan sweeper can reap it by name after the subscription
	// object is gone; an abandoned slot otherwise pins the source primary's WAL.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	ReplicationSlotName string `json:"replicationSlotName,omitempty"`

	// publicationName is the publication created on the source.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	PublicationName string `json:"publicationName,omitempty"`

	// subscriptionName is the subscription created on the target.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	SubscriptionName string `json:"subscriptionName,omitempty"`

	// rollbackDeadline is when the source database stops being recoverable and is
	// dropped. Empty before a successful cutover.
	// +optional
	RollbackDeadline *metav1.Time `json:"rollbackDeadline,omitempty"`

	// conditions represent the current state of the PgTenantMigration resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:categories=pgelastic,shortName=pgtm
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=".spec.tenantRef.name"
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=".spec.targetInstanceRef.name"
// +kubebuilder:printcolumn:name="Strategy",type=string,JSONPath=".spec.strategy"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Lag",type=integer,JSONPath=".status.lagBytes"
// +kubebuilder:printcolumn:name="Pause",type=integer,JSONPath=".status.pauseDurationMillis"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// PgTenantMigration is one non-repeatable move of a single tenant from the instance it
// is bound to onto another instance in the same pool.
//
// The phase machine advances Preflight -> Provisioning -> PreWarm -> Copying -> Catchup
// -> Quiescing -> Cutover -> Completed. Every phase may leave for Failed on error or
// Aborted on request; Completed may leave for RolledBack until rollbackDeadline passes,
// after which the migration is final.
//
// Every departure from the happy path leaves the tenant serving from the SOURCE. The
// routing table is flipped exactly once, at the end of Cutover, after verification has
// passed; before that point an abort only has to run the cleanup ladder (disable the
// subscription, detach its slot, drop the subscription, drop the slot, drop the
// publication) and release the quiesce, and after that point a rollback flips routing
// back to a source database that has been kept intact for precisely this reason.
type PgTenantMigration struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PgTenantMigration
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
	Spec PgTenantMigrationSpec `json:"spec"`

	// status defines the observed state of PgTenantMigration
	// +optional
	Status PgTenantMigrationStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PgTenantMigrationList contains a list of PgTenantMigration
type PgTenantMigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgTenantMigration `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgTenantMigration{}, &PgTenantMigrationList{})
		return nil
	})
}
