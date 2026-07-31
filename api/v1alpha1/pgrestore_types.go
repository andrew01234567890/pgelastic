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

// RestoreScope is what is being put back.
// +kubebuilder:validation:Enum=Instance;Tenant
type RestoreScope string

const (
	// RestoreScopeInstance restores a whole instance into a new one. Every tenant on it
	// goes back to the same moment, which is what makes it disaster recovery rather than
	// the per-customer restore a multi-tenant product is usually asked for.
	RestoreScopeInstance RestoreScope = "Instance"
	// RestoreScopeTenant restores one tenant and leaves its neighbours alone.
	//
	// There is no such thing as restoring one database out of a physical backup: WAL is
	// instance-wide, so replaying it to a moment replays every tenant on the instance to
	// that moment. What this does instead is recover the whole instance into one nobody can
	// reach, lift the one database out of it, load it over the live one, and throw the
	// recovery away. It costs a second instance for the duration and it is the restore a
	// multi-tenant product is actually asked for.
	RestoreScopeTenant RestoreScope = "Tenant"
)

// RestorePhase is the display-only projection of a restore's conditions.
// +kubebuilder:validation:Enum=Preflight;Recovering;Extracting;Loading;Completed;Failed
type RestorePhase string

const (
	// RestorePhasePreflight is checking that the restore can be planned at all.
	RestorePhasePreflight RestorePhase = "Preflight"
	// RestorePhaseRecovering is a target instance pulling the base backup down and
	// replaying WAL onto it.
	RestorePhaseRecovering RestorePhase = "Recovering"
	// RestorePhaseExtracting is lifting one tenant's database out of the recovered instance.
	RestorePhaseExtracting RestorePhase = "Extracting"
	// RestorePhaseLoading is writing it over the live one, with the tenant held still.
	RestorePhaseLoading RestorePhase = "Loading"
	// RestorePhaseCompleted is a target instance serving on a forked timeline.
	RestorePhaseCompleted RestorePhase = "Completed"
	// RestorePhaseFailed is terminal. The target instance is left in place rather than
	// deleted: a half-restored instance is evidence, and deleting it destroys the only copy
	// of what went wrong.
	RestorePhaseFailed RestorePhase = "Failed"
)

// RecoveryTarget is where recovery stops.
//
// Every field except timeline is mutually exclusive with the others, and the mutual
// exclusion is enforced by CEL rather than by a discriminated union so that an operator can
// write the field they mean rather than a type and a value.
//
// Only time and lsn are orderable against the repository catalogue, so only they let the
// base backup be chosen automatically. For name, xid and immediate there is nothing to
// search by, and backupRef becomes required - the rule CloudNativePG arrived at after
// shipping without it.
// +kubebuilder:validation:XValidation:rule="[has(self.time), has(self.lsn), has(self.name), has(self.xid), has(self.immediate)].filter(x, x).size() <= 1",message="at most one of time, lsn, name, xid or immediate may be set"
type RecoveryTarget struct {
	// time stops recovery at the first transaction that committed after this moment.
	//
	// A timestamp with no zone is read as UTC. Recovery fails outright if no transaction
	// committed after it, which is a surprising way to learn that the target is in the
	// future.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	Time string `json:"time,omitempty"`

	// lsn stops recovery at a write-ahead log position.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	LSN string `json:"lsn,omitempty"`

	// name stops recovery at a restore point created with pg_create_restore_point.
	//
	// PostgreSQL enforces MAXFNAMELEN on both ends, so a longer name is refused here rather
	// than silently truncated into one that matches nothing.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[^\x00-\x1f\x7f]*$`
	// +optional
	Name string `json:"name,omitempty"`

	// xid stops recovery at a transaction id.
	//
	// PostgreSQL casts this to TransactionId, which is 32 bits: a larger value would be
	// silently truncated into a different transaction, so the pattern refuses one outright.
	// +kubebuilder:validation:MaxLength=10
	// +kubebuilder:validation:Pattern=`^[0-9]+$`
	// +optional
	XID string `json:"xid,omitempty"`

	// immediate stops recovery as soon as the backup is consistent, without replaying any
	// further. It is the fastest restore and it lands at whatever moment the backup ended.
	// +optional
	Immediate *bool `json:"immediate,omitempty"`

	// exclusive stops just before the named target rather than just after it. It has no
	// meaning for immediate.
	// +optional
	Exclusive *bool `json:"exclusive,omitempty"`

	// timeline selects which history to follow, "latest" or a positive integer.
	//
	// It is not mutually exclusive with the rest: a target time can fall on more than one
	// timeline once an instance has been restored before, and picking the wrong one lands
	// on a history that diverged before the moment asked for.
	// +kubebuilder:validation:MaxLength=16
	// +kubebuilder:validation:Pattern=`^(latest|[1-9][0-9]*)$`
	// +optional
	Timeline string `json:"timeline,omitempty"`
}

// PgRestoreSpec is one request to put an instance back.
//
// The whole spec is immutable, and the target is required to name a backup unless it is
// orderable against the catalogue.
// +kubebuilder:validation:XValidation:rule="has(self.backupRef) || !has(self.target) || has(self.target.time) || has(self.target.lsn)",message="backupRef is required unless the target is a time or an LSN, because nothing else can be searched for in the repository catalogue"
// +kubebuilder:validation:XValidation:rule="!has(self.scope) || self.scope != 'Tenant' || has(self.tenantRef)",message="a tenant-scoped restore has to name the tenant it is putting back"
type PgRestoreSpec struct {
	// scope is what is being restored.
	// +kubebuilder:default=Instance
	// +optional
	Scope RestoreScope `json:"scope,omitempty"`

	// sourceInstanceRef names the instance whose repository is being read.
	//
	// The instance itself need not still exist. What is read from it when it does is its
	// repository and its sizing; when it does not, backupRef supplies both, which is why a
	// backup's status records them.
	// +required
	SourceInstanceRef corev1.LocalObjectReference `json:"sourceInstanceRef"`

	// backupRef names the base backup to start from. Required unless the target is a time
	// or an LSN.
	// +optional
	BackupRef *corev1.LocalObjectReference `json:"backupRef,omitempty"`

	// target is where recovery stops. Omitted means replay everything the archive holds.
	// +optional
	Target *RecoveryTarget `json:"target,omitempty"`

	// tenantRef names the tenant to put back, for a tenant-scoped restore.
	//
	// The tenant is restored where it already lives. Its neighbours on that instance are
	// untouched and served throughout, which is the whole point: the alternative is rolling
	// every customer on the instance back to one customer's bad afternoon.
	// +optional
	TenantRef *corev1.LocalObjectReference `json:"tenantRef,omitempty"`

	// targetInstanceName is the instance to create, for an instance-scoped restore.
	// Defaults to the restore's own name. It is ignored for a tenant-scoped one, which
	// always recovers into a throwaway instance named after the restore.
	//
	// It is always a new instance: restoring in place would destroy the only copy of the
	// thing being recovered, and a restore that went wrong would leave nothing to try again
	// from.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	TargetInstanceName string `json:"targetInstanceName,omitempty"`
}

// PgRestoreStatus is how the restore went.
type PgRestoreStatus struct {
	// phase is display only and carries no semantics of its own.
	// +optional
	Phase RestorePhase `json:"phase,omitempty"`

	// conditions carry the real state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the spec generation these conditions describe.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// targetInstance is the instance that was created.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	TargetInstance string `json:"targetInstance,omitempty"`

	// backupID is the base backup recovery actually started from, which for a
	// time-targeted restore is chosen from the catalogue rather than named in the spec.
	// +kubebuilder:validation:MaxLength=128
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// error is why it failed, when it did.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Error string `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:categories=pgelastic,shortName=pgr
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=".spec.sourceInstanceRef.name"
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=".status.targetInstance"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// PgRestore puts an instance back, into a new instance, at a moment of the operator's
// choosing.
//
// Recovery is never performed in place. The instance being recovered from is very often the
// only copy of the data, and a restore that ran over it would destroy the thing it was
// asked to save the moment it went wrong. The new instance is created unschedulable and
// joins no pool until somebody says so: a restored instance is evidence until it has been
// looked at, not capacity.
type PgRestore struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PgRestore
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
	Spec PgRestoreSpec `json:"spec"`

	// status defines the observed state of PgRestore
	// +optional
	Status PgRestoreStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PgRestoreList contains a list of PgRestore
type PgRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgRestore{}, &PgRestoreList{})
		return nil
	})
}
