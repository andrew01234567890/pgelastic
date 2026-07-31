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

// BackupTarget selects which member of an instance takes the backup.
//
// PreferStandby is the default and falls back to the primary when no standby will serve,
// because a backup that did not happen is worse than one that cost the primary some I/O.
// +kubebuilder:validation:Enum=Primary;PreferStandby
type BackupTarget string

const (
	// BackupTargetPrimary insists on the primary. It exists for the case a standby cannot
	// answer for - a backup taken immediately after a change that a standby has not
	// replayed yet is an incomplete backup that reports success.
	BackupTargetPrimary BackupTarget = "Primary"
	// BackupTargetPreferStandby spends a standby's I/O rather than the primary's.
	BackupTargetPreferStandby BackupTarget = "PreferStandby"
)

// BackupPhase is the display-only projection of a backup's conditions. Controllers must
// branch on conditions, never on this.
// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
type BackupPhase string

const (
	// BackupPhasePending is a backup that has not started. It covers two situations that
	// read the same from here and differ entirely in what to do about them - no member has
	// claimed it yet, and archiving is not working so it refuses to start - which is why
	// the reason lives on the conditions rather than in the phase.
	BackupPhasePending BackupPhase = "Pending"
	// BackupPhaseRunning is a backup a member has claimed and is taking.
	BackupPhaseRunning BackupPhase = "Running"
	// BackupPhaseCompleted is a backup present in the repository catalogue.
	BackupPhaseCompleted BackupPhase = "Completed"
	// BackupPhaseFailed is terminal. A backup is never retried in place: the record of an
	// attempt that failed is worth keeping, and the schedule mints a new one.
	BackupPhaseFailed BackupPhase = "Failed"
)

// PgBackupSpec is one request to take one physical backup.
//
// The whole spec is immutable. A backup request is a one-shot fact, and letting it be
// edited while a member is acting on it is a class of bug avoided for one line.
type PgBackupSpec struct {
	// instanceRef names the instance to back up, in this namespace.
	// +required
	InstanceRef corev1.LocalObjectReference `json:"instanceRef"`

	// type selects a full backup or one relative to the last.
	//
	// A differential is relative to the last full, and an incremental to the last backup of
	// any type. Both are useless without the full they descend from, which is why retention
	// is expressed as a recovery window rather than a count: expiring a full silently
	// invalidates every backup that depended on it.
	// +kubebuilder:default=Full
	// +optional
	Type BackupType `json:"type,omitempty"`

	// target selects which member takes it.
	// +kubebuilder:default=PreferStandby
	// +optional
	Target BackupTarget `json:"target,omitempty"`
}

// PgBackupStatus is what the backup turned out to be.
//
// It is deliberately self-describing: the repository, the stanza, the system identifier and
// the source's enforced parameters are all recorded here rather than looked up from the
// PgInstance when a restore needs them. A backup outlives its instance - that is most of
// the point of having one - and a restore that could only be planned while the source still
// existed would be no use in the case it exists for.
type PgBackupStatus struct {
	// phase is display only and carries no semantics of its own.
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// conditions carry the real state. Accepted, Progressing and Ready.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the spec generation these conditions describe.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// member is the Pod that took it.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Member string `json:"member,omitempty"`

	// agentSession identifies the instance manager process that claimed this backup.
	//
	// It changes on every agent start, including a container restart. A backup is taken by
	// a goroutine inside a process that can die, and without an epoch token a restarted
	// agent leaves its predecessor's backup marked Running forever, indistinguishable from
	// one still in progress.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	AgentSession string `json:"agentSession,omitempty"`

	// backupID is what the repository calls it, and what a restore names.
	// +kubebuilder:validation:MaxLength=128
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// type is what was actually taken, which is not always what was asked for: pgBackRest
	// promotes a differential or an incremental to a full when there is no full to descend
	// from.
	// +optional
	Type BackupType `json:"type,omitempty"`

	// startedAt and stoppedAt are the backup's own times, read back from the repository
	// catalogue rather than measured by the operator.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	StoppedAt *metav1.Time `json:"stoppedAt,omitempty"`

	// beginLSN, endLSN, beginWAL and endWAL bound the WAL a restore of this backup has to
	// replay to reach consistency.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	BeginLSN string `json:"beginLSN,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	// +optional
	EndLSN string `json:"endLSN,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	// +optional
	BeginWAL string `json:"beginWAL,omitempty"`
	// +kubebuilder:validation:MaxLength=64
	// +optional
	EndWAL string `json:"endWAL,omitempty"`

	// sizeBytes is the repository size of this backup after compression.
	// +kubebuilder:validation:Minimum=0
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// systemIdentifier and stanza address the repository this backup lives in. The stanza
	// is derived from the system identifier rather than from the instance name, so a
	// recreated instance cannot address a predecessor's archive.
	// +kubebuilder:validation:MaxLength=32
	// +optional
	SystemIdentifier string `json:"systemIdentifier,omitempty"`
	// +kubebuilder:validation:MaxLength=128
	// +optional
	Stanza string `json:"stanza,omitempty"`

	// repository is where it was written.
	// +optional
	Repository *ObjectStore `json:"repository,omitempty"`

	// sourceEnforcedParameters are the five settings PostgreSQL refuses to begin recovery
	// below.
	//
	// Recorded here because they are the source's, and the restore target's own
	// configuration is not evidence about them: a restore into an instance whose
	// max_connections is lower than the source's FATALs at start-up with a message that
	// names the parameter and not the cause.
	// +optional
	SourceEnforcedParameters map[string]int32 `json:"sourceEnforcedParameters,omitempty"`

	// timeline is the timeline the backup was taken on. A restore that means to land before
	// a promotion has to know which history it is asking for.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Timeline int32 `json:"timeline,omitempty"`

	// error is why it failed, when it did.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Error string `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:categories=pgelastic,shortName=pgb
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=".spec.instanceRef.name"
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=".status.type"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Member",type=string,JSONPath=".status.member"
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=".status.sizeBytes"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// PgBackup is one physical backup of one instance, and the record of how to restore from it.
//
// It carries no owner reference to its PgInstance, deliberately. Deleting an instance must
// not delete the record of how to get it back, and the case a backup exists for is exactly
// the case where the instance is gone.
type PgBackup struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PgBackup
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
	Spec PgBackupSpec `json:"spec"`

	// status defines the observed state of PgBackup
	// +optional
	Status PgBackupStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PgBackupList contains a list of PgBackup
type PgBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgBackup{}, &PgBackupList{})
		return nil
	})
}
