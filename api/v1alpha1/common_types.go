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
)

// Condition types shared across pgelastic resources. They are adjectives describing
// an aspect of the object, following the Gateway API convention.
const (
	// ConditionAccepted reports whether the controller has claimed the object and
	// found its spec self-consistent.
	ConditionAccepted = "Accepted"
	// ConditionReady reports whether the object is serving its purpose.
	ConditionReady = "Ready"
	// ConditionBound reports whether a claim has been bound to a backing resource.
	ConditionBound = "Bound"
	// ConditionProgressing reports whether the controller is actively converging.
	ConditionProgressing = "Progressing"
	// ConditionDegraded reports whether the object is serving in a reduced capacity.
	ConditionDegraded = "Degraded"
	// ConditionThrottled reports whether admission is currently denying or queuing work.
	ConditionThrottled = "Throttled"
	// ConditionMigrating reports whether a tenant move is in flight.
	ConditionMigrating = "Migrating"
)

// Condition reasons. Every reason a controller can set is enumerated here so the set
// is greppable and stable; unlisted reasons are a bug.
const (
	ReasonAccepted            = "Accepted"
	ReasonPending             = "Pending"
	ReasonReady               = "Ready"
	ReasonStable              = "Stable"
	ReasonInvalidSpec         = "InvalidSpec"
	ReasonNotClaimed          = "NotClaimed"
	ReasonPlaced              = "Placed"
	ReasonUnplaceable         = "Unplaceable"
	ReasonWithinLimits        = "WithinLimits"
	ReasonTenantCapReached    = "TenantCapacityReached"
	ReasonPoolCapReached      = "PoolCapacityReached"
	ReasonOverCommitted       = "OverCommitted"
	ReasonQuorumHealthy       = "QuorumHealthy"
	ReasonQuorumLost          = "QuorumLost"
	ReasonWriteStalled        = "WriteStalled"
	ReasonFailingOver         = "FailingOver"
	ReasonSplitBrainSuspected = "SplitBrainSuspected"
	ReasonArchiveDegraded     = "ArchiveDegraded"
	ReasonArchiveHealthy      = "ArchiveHealthy"
	ReasonRecloning           = "Recloning"
	ReasonPreflightFailed     = "PreflightFailed"
	ReasonCutoverComplete     = "CutoverComplete"
	ReasonRolledBack          = "RolledBack"
)

// QoSClass is derived by the controller from the relationship between a tenant's
// guaranteed and burstable capacity, exactly as the kubelet derives Pod QoS. It is
// never set by a user.
// +kubebuilder:validation:Enum=Guaranteed;Burstable;BestEffort
type QoSClass string

const (
	// QoSGuaranteed means guaranteed == burstable and both are non-zero.
	QoSGuaranteed QoSClass = "Guaranteed"
	// QoSBurstable means 0 < guaranteed < burstable.
	QoSBurstable QoSClass = "Burstable"
	// QoSBestEffort means guaranteed == 0; the tenant draws only from burst credit.
	QoSBestEffort QoSClass = "BestEffort"
)

// PoolMode selects when a backend connection is returned to the pool.
//
// Session holds the backend for the client's whole session. Transaction returns it at
// every ReadyForQuery whose transaction-status byte is 'I'. Statement is Transaction
// plus a hard error when a client opens an explicit transaction block.
// +kubebuilder:validation:Enum=Session;Transaction;Statement
type PoolMode string

const (
	PoolModeSession     PoolMode = "Session"
	PoolModeTransaction PoolMode = "Transaction"
	PoolModeStatement   PoolMode = "Statement"
)

// ResetPolicy selects how much session state is scrubbed before a backend is reused.
//
// DirtyTracked scrubs only what the proxy observed being set (taint tracking).
// SmartDiscard runs a prepared-statement-preserving hygiene script. DiscardAll runs
// DISCARD ALL unconditionally. Verified additionally samples the result to prove the
// reset took effect.
// +kubebuilder:validation:Enum=None;DirtyTracked;SmartDiscard;DiscardAll;Verified
type ResetPolicy string

const (
	ResetNone         ResetPolicy = "None"
	ResetDirtyTracked ResetPolicy = "DirtyTracked"
	ResetSmartDiscard ResetPolicy = "SmartDiscard"
	ResetDiscardAll   ResetPolicy = "DiscardAll"
	ResetVerified     ResetPolicy = "Verified"
)

// StartupParameterPolicy selects what happens when a client sends a startup parameter
// the proxy does not track.
//
// PoolKey folds the parameter into the pool key, which is the only option that is both
// safe and compatible; Reject breaks lib/pq and Rails clients; Ignore is a correctness hole.
// +kubebuilder:validation:Enum=Reject;Ignore;PoolKey
type StartupParameterPolicy string

const (
	StartupParameterReject  StartupParameterPolicy = "Reject"
	StartupParameterIgnore  StartupParameterPolicy = "Ignore"
	StartupParameterPoolKey StartupParameterPolicy = "PoolKey"
)

// PinningPolicy selects what happens when a client creates backend session state that
// cannot be scrubbed, such as LISTEN, a WITH HOLD cursor or a session advisory lock.
//
// Pin is the safe default: the client keeps that backend for its lifetime. Close drops
// the backend on release. Error refuses the statement.
// +kubebuilder:validation:Enum=Pin;Close;Error
type PinningPolicy string

const (
	PinningPin   PinningPolicy = "Pin"
	PinningClose PinningPolicy = "Close"
	PinningError PinningPolicy = "Error"
)

// TLSMode mirrors libpq sslmode.
// +kubebuilder:validation:Enum=Disable;Allow;Prefer;Require;VerifyCA;VerifyFull
type TLSMode string

const (
	TLSDisable    TLSMode = "Disable"
	TLSAllow      TLSMode = "Allow"
	TLSPrefer     TLSMode = "Prefer"
	TLSRequire    TLSMode = "Require"
	TLSVerifyCA   TLSMode = "VerifyCA"
	TLSVerifyFull TLSMode = "VerifyFull"
)

// AuthMode selects how the proxy authenticates a client and then the backend.
//
// ScramPassthrough derives the backend proof from the client's, which requires pgelastic
// to own password provisioning so salt and iteration count match byte for byte.
// +kubebuilder:validation:Enum=ScramPassthrough;BackendServiceAccount;AuthQuery
type AuthMode string

const (
	AuthScramPassthrough      AuthMode = "ScramPassthrough"
	AuthBackendServiceAccount AuthMode = "BackendServiceAccount"
	AuthQuery                 AuthMode = "AuthQuery"
)

// ReclaimPolicy selects what happens to backing data when the owning object is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type ReclaimPolicy string

const (
	ReclaimRetain ReclaimPolicy = "Retain"
	ReclaimDelete ReclaimPolicy = "Delete"
)

// NamespaceFrom selects which namespaces may bind to a policy or pool. One-way
// selection is a tenancy escape, so consent is bidirectional: both the class and the
// pool must admit the namespace.
// +kubebuilder:validation:Enum=All;Selector;Same
type NamespaceFrom string

const (
	NamespaceFromAll      NamespaceFrom = "All"
	NamespaceFromSelector NamespaceFrom = "Selector"
	NamespaceFromSame     NamespaceFrom = "Same"
)

// NamespaceAdmission expresses which namespaces may bind, in the shape of the Gateway
// API allowedRoutes.namespaces field.
type NamespaceAdmission struct {
	// from selects the namespace-matching strategy.
	// +kubebuilder:default=Same
	// +optional
	From NamespaceFrom `json:"from,omitempty"`

	// selector matches namespaces by label. Only consulted when from is Selector.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
}

// ClassReference points at a cluster-scoped pgelastic policy object. The apiGroup and
// kind are constrained by CEL at the referring field so a reference cannot silently
// point at an unrelated resource.
type ClassReference struct {
	// apiGroup of the referent. Always pgelastic.io.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:default="pgelastic.io"
	// +optional
	APIGroup string `json:"apiGroup,omitempty"`

	// kind of the referent.
	// +kubebuilder:validation:MaxLength=63
	// +required
	Kind string `json:"kind"`

	// name of the referent.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:MinLength=1
	// +required
	Name string `json:"name"`
}

// CollationContract is the immutable identity of a PostgreSQL instance's text handling
// and on-disk format. Two instances may only exchange tenants when their contracts are
// byte-identical: restoring under a different collation produces indexes that are
// silently inconsistent with their heap ordering, which yields wrong results and no error.
type CollationContract struct {
	// encoding is the database encoding, always UTF8.
	// +optional
	Encoding string `json:"encoding,omitempty"`

	// localeProvider is the datlocprovider value. pgelastic pins this to "builtin" so
	// neither ICU nor glibc version drift can differ between instances in a pool.
	// +optional
	LocaleProvider string `json:"localeProvider,omitempty"`

	// locale is the datlocale value, pinned to C.UTF-8.
	// +optional
	Locale string `json:"locale,omitempty"`

	// collate is the datcollate value.
	// +optional
	Collate string `json:"collate,omitempty"`

	// ctype is the datctype value.
	// +optional
	Ctype string `json:"ctype,omitempty"`

	// icuRules is the daticurules value, empty under the builtin provider.
	// +optional
	ICURules string `json:"icuRules,omitempty"`

	// walSegmentSize in bytes, fixed at initdb.
	// +optional
	WALSegmentSize int64 `json:"walSegmentSize,omitempty"`

	// dataChecksums reports whether page checksums are enabled. Always true.
	// +optional
	DataChecksums bool `json:"dataChecksums,omitempty"`

	// systemIdentifier is the PostgreSQL system identifier. Archive and backup paths are
	// keyed on this rather than on an instance name, so a recreated instance reusing a
	// name cannot interleave its WAL into a predecessor's archive.
	// +optional
	SystemIdentifier string `json:"systemIdentifier,omitempty"`
}

// TLSConfig configures one side of a TLS relationship.
type TLSConfig struct {
	// mode selects the libpq-equivalent sslmode.
	// +kubebuilder:default=VerifyFull
	// +optional
	Mode TLSMode `json:"mode,omitempty"`

	// certificateSecretRef names a kubernetes.io/tls Secret holding the serving certificate.
	// +optional
	CertificateSecretRef *corev1.LocalObjectReference `json:"certificateSecretRef,omitempty"`

	// caSecretRef names a Secret holding the CA bundle used to verify the peer.
	// +optional
	CASecretRef *corev1.LocalObjectReference `json:"caSecretRef,omitempty"`

	// protocols restricts the acceptable TLS versions.
	// +kubebuilder:validation:MaxItems=4
	// +kubebuilder:default={"tlsv1.2","tlsv1.3"}
	// +optional
	Protocols []string `json:"protocols,omitempty"`
}

// ObjectStore locates a bucket and the credentials to reach it.
type ObjectStore struct {
	// path is the destination URL, for example s3://bucket/prefix.
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:MinLength=1
	// +required
	Path string `json:"path"`

	// credentialsSecretRef names a Secret holding the object-store credentials.
	// +required
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`

	// endpointURL overrides the provider's default endpoint, for S3-compatible stores.
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	EndpointURL string `json:"endpointURL,omitempty"`

	// region for providers that require one.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Region string `json:"region,omitempty"`
}

// RetentionPolicy bounds how long backups and WAL are kept. WAL retention must always
// cover the oldest restorable full backup plus every incremental depending on it.
type RetentionPolicy struct {
	// full is how long full backups are kept.
	// +kubebuilder:default="30d"
	// +optional
	Full string `json:"full,omitempty"`

	// wal is how long archived WAL is kept.
	// +kubebuilder:default="30d"
	// +optional
	WAL string `json:"wal,omitempty"`
}

// TimeWindow is a recurring window expressed as a cron start plus a duration.
type TimeWindow struct {
	// schedule is a standard five-field cron expression giving the window start.
	// +kubebuilder:validation:MaxLength=256
	// +required
	Schedule string `json:"schedule"`

	// duration is how long the window stays open once started.
	// +required
	Duration metav1.Duration `json:"duration"`

	// timeZone is an IANA zone name. Defaults to UTC.
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:default="UTC"
	// +optional
	TimeZone string `json:"timeZone,omitempty"`
}

// TenantLimits are the per-tenant guardrails. Everything here except the proxy-enforced
// deadline is advisory: the underlying GUCs are PGC_USERSET, so a client can SET them
// back. The proxy's own deadline is the authoritative one.
type TenantLimits struct {
	// statementTimeout bounds a single statement.
	// +optional
	StatementTimeout *metav1.Duration `json:"statementTimeout,omitempty"`

	// idleInTransactionSessionTimeout bounds how long a transaction may sit idle.
	// +optional
	IdleInTransactionSessionTimeout *metav1.Duration `json:"idleInTransactionSessionTimeout,omitempty"`

	// idleSessionTimeout bounds how long a session may sit idle outside a transaction.
	// +optional
	IdleSessionTimeout *metav1.Duration `json:"idleSessionTimeout,omitempty"`

	// lockTimeout bounds how long a statement waits for a lock.
	// +optional
	LockTimeout *metav1.Duration `json:"lockTimeout,omitempty"`

	// tempFileLimit caps temporary file bytes. Note this is enforced per PostgreSQL
	// process, so a session using parallel workers can consume a multiple of it.
	// +optional
	TempFileLimit *resource.Quantity `json:"tempFileLimit,omitempty"`

	// maxResultBytes caps the bytes the proxy will relay for a single result set.
	// +optional
	MaxResultBytes *resource.Quantity `json:"maxResultBytes,omitempty"`

	// rateLimit caps the tenant's request and byte rate at the proxy.
	// +optional
	RateLimit *RateLimit `json:"rateLimit,omitempty"`
}

// RateLimit caps a tenant's throughput at the proxy.
type RateLimit struct {
	// transactionsPerSecond caps committed and rolled-back transactions per second.
	// +kubebuilder:validation:Minimum=1
	// +optional
	TransactionsPerSecond *int32 `json:"transactionsPerSecond,omitempty"`

	// bytesPerSecond caps bytes relayed to the client per second.
	// +optional
	BytesPerSecond *resource.Quantity `json:"bytesPerSecond,omitempty"`
}
