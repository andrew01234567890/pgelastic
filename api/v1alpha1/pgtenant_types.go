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

// PgTenantPhase is a display-only summary of the conditions, present for kubectl
// ergonomics. It is a pure function of status.conditions and must never be read by a
// controller as input.
// +kubebuilder:validation:Enum=Pending;Binding;Ready;Throttled;Migrating;Degraded;Terminating
type PgTenantPhase string

const (
	PgTenantPhasePending     PgTenantPhase = "Pending"
	PgTenantPhaseBinding     PgTenantPhase = "Binding"
	PgTenantPhaseReady       PgTenantPhase = "Ready"
	PgTenantPhaseThrottled   PgTenantPhase = "Throttled"
	PgTenantPhaseMigrating   PgTenantPhase = "Migrating"
	PgTenantPhaseDegraded    PgTenantPhase = "Degraded"
	PgTenantPhaseTerminating PgTenantPhase = "Terminating"
)

// PgTenantThrottleCode identifies an admission or quota rejection. The codes are API
// surface, not log text: clients branch their retry behaviour on them, so the split
// between "you hit your own ceiling" and "the pool is full" must survive into status.
// +kubebuilder:validation:Enum=PGE1928;PGE1936;PGE1929;PGE0544;PGE1024;PGE1613
type PgTenantThrottleCode string

const (
	// ThrottleTenantCap (SQLSTATE 53300) means the tenant reached its own burstable
	// ceiling while the pool still had headroom. The fix is raising burstable.
	ThrottleTenantCap PgTenantThrottleCode = "PGE1928"
	// ThrottlePoolCapacity (SQLSTATE 53400) means the pool budget is exhausted. The fix
	// is scaling the pool, not the tenant.
	ThrottlePoolCapacity PgTenantThrottleCode = "PGE1936"
	// ThrottlePoolBusy (SQLSTATE 53400) means the request was within every limit but the
	// pool was momentarily saturated. Retryable.
	ThrottlePoolBusy PgTenantThrottleCode = "PGE1929"
	// ThrottleStorageQuota (SQLSTATE 53100) means the storage cap was reached. Writes
	// fail; SELECT and DELETE keep working so the tenant can recover unaided.
	ThrottleStorageQuota PgTenantThrottleCode = "PGE0544"
	// ThrottleQueueTimeout (SQLSTATE 53400) means the client waited out maxWaitSeconds
	// in the admission queue.
	ThrottleQueueTimeout PgTenantThrottleCode = "PGE1024"
	// ThrottleMigrationCutover (SQLSTATE 57P01) means the connection was ended by a
	// tenant move.
	ThrottleMigrationCutover PgTenantThrottleCode = "PGE1613"
)

// PgTenantCapacity overrides the workload class's capacity triple for this one tenant.
//
// Per-tenant floors are the capability Azure elastic pools explicitly lack ("customizing
// min and max vCores for individual databases in the pool isn't supported"). A guarantee
// that cannot be honoured against the pool's remaining headroom is rejected at admission
// rather than silently degraded, so setting guaranteed here can fail the apply.
// +kubebuilder:validation:XValidation:rule="!has(self.guaranteed) || !has(self.burstable) || self.guaranteed <= self.burstable",message="guaranteed must not exceed burstable"
type PgTenantCapacity struct {
	// guaranteed is the number of backend connections reserved for this tenant. The
	// reservation is a credit, not a pre-warmed connection, and it is strictly
	// non-work-conserving: an idle tenant's unused guarantee is not lent to others.
	// Zero means the tenant draws only from burst credit, which derives qosClass
	// BestEffort.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	// +optional
	Guaranteed *int32 `json:"guaranteed,omitempty"`

	// burstable is the ceiling on concurrently held backend connections. It is a cap and
	// explicitly not a guarantee; exceeding it yields PGE1928 rather than a queue slot.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10000
	// +optional
	Burstable *int32 `json:"burstable,omitempty"`

	// storage caps the tenant's database size. WAL and temporary files are excluded, so
	// the enforced number is not the tenant's total disk footprint.
	// +optional
	Storage *resource.Quantity `json:"storage,omitempty"`
}

// PgTenantAuth overrides the pool's authentication posture for this tenant.
type PgTenantAuth struct {
	// mode selects how the proxy authenticates this tenant's clients. Defaults to the
	// pool's auth.mode.
	// +optional
	Mode *AuthMode `json:"mode,omitempty"`

	// credentialsSecretRef names a Secret holding the tenant role's password. pgelastic
	// owns password provisioning end to end because ScramPassthrough requires the salt
	// and iteration count to match the backend verifier byte for byte.
	// +optional
	CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`
}

// PgTenantPlacement constrains which PgInstance in the pool may host this tenant.
type PgTenantPlacement struct {
	// instanceRef pins the tenant to one PgInstance. Omit to let the placement scheduler
	// choose. Pinning removes the tenant from rebalancing and can make a pool
	// unschedulable, so it is an escape hatch rather than a tuning knob.
	// +optional
	InstanceRef *corev1.LocalObjectReference `json:"instanceRef,omitempty"`

	// antiAffinityLabelKeys names label keys on this PgTenant whose values must not be
	// shared with co-located tenants. Correlated workloads defeat the oversubscription
	// bet that the whole capacity model rests on.
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:items:MaxLength=317
	// +listType=atomic
	// +optional
	AntiAffinityLabelKeys []string `json:"antiAffinityLabelKeys,omitempty"`
}

// PgTenantSpec is the complete desired state of a tenant claim. The minimum viable spec
// is poolRef plus databaseName; every other field is an override used by a small minority
// of tenants.
type PgTenantSpec struct {
	// poolRef names the PgElasticPool in this namespace that supplies the capacity
	// budget. Changing it would mean moving the data to a different capacity boundary,
	// which is what PgTenantMigration exists for.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="poolRef is immutable"
	// +required
	PoolRef corev1.LocalObjectReference `json:"poolRef"`

	// databaseName is the PostgreSQL DATABASE created for this tenant. It is immutable
	// because renaming would invalidate every issued connection string and every backup
	// path keyed on it.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_]*$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="databaseName is immutable"
	// +required
	DatabaseName string `json:"databaseName"`

	// workloadClassName selects the PgWorkloadClass supplying the capacity triple, limits
	// and SLO. Changing this one string is the entire tier-change operation. Defaults to
	// the pool's admission.defaultWorkloadClassName.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	WorkloadClassName *string `json:"workloadClassName,omitempty"`

	// capacity overrides the workload class's capacity triple for this tenant alone.
	// +optional
	Capacity *PgTenantCapacity `json:"capacity,omitempty"`

	// owner is the PostgreSQL role owning the database. Defaults to databaseName.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_]*$`
	// +optional
	Owner *string `json:"owner,omitempty"`

	// auth overrides the pool's authentication posture for this tenant.
	// +optional
	Auth *PgTenantAuth `json:"auth,omitempty"`

	// placement constrains where the tenant may be hosted.
	// +optional
	Placement *PgTenantPlacement `json:"placement,omitempty"`

	// extensions requests PostgreSQL extensions in the tenant's database. Entries must
	// come from the pool's curated allowlist and are installed at identical versions on
	// every instance in the pool: a version skew between a migration's source and target
	// silently produces a divergent schema. Large objects are forbidden product-wide and
	// no extension can re-enable them, because they have no representation in logical
	// replication and would permanently strand the tenant on the offline migration path.
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:validation:items:Pattern=`^[a-z_][a-z0-9_]*$`
	// +listType=atomic
	// +optional
	Extensions []string `json:"extensions,omitempty"`

	// reclaimPolicy decides the fate of the database when this object is deleted.
	// +kubebuilder:default=Retain
	// +optional
	ReclaimPolicy *ReclaimPolicy `json:"reclaimPolicy,omitempty"`
}

// PgTenantBinding records the instance the tenant was placed on.
type PgTenantBinding struct {
	// instanceRef names the PgInstance currently hosting the database.
	// +optional
	InstanceRef *corev1.LocalObjectReference `json:"instanceRef,omitempty"`

	// databaseOid is the PostgreSQL OID of the created database. It disambiguates a
	// recreated database that reuses the same name.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4294967295
	// +optional
	DatabaseOID *int64 `json:"databaseOid,omitempty"`

	// boundAt is when the current binding was established.
	// +optional
	BoundAt *metav1.Time `json:"boundAt,omitempty"`
}

// PgTenantEffectiveLimits publishes the limits actually in force after the workload class
// defaults, the per-tenant overrides and the pool's ceilings have all been applied. It is
// the analogue of sys.dm_user_db_resource_governance: the one place an operator can read
// what the governor is really enforcing, rather than inferring it from three objects.
type PgTenantEffectiveLimits struct {
	// guaranteed is the reserved backend-connection floor in force.
	// +optional
	Guaranteed *int32 `json:"guaranteed,omitempty"`

	// burstable is the backend-connection ceiling in force.
	// +optional
	Burstable *int32 `json:"burstable,omitempty"`

	// weight is this tenant's share of contended surplus in the cross-tenant scheduler.
	// +optional
	Weight *int32 `json:"weight,omitempty"`

	// statementTimeout is the deadline the proxy enforces. The matching GUC is set on the
	// role as defense in depth only; being PGC_USERSET a client can SET it back, so this
	// value is the authoritative one.
	// +optional
	StatementTimeout *metav1.Duration `json:"statementTimeout,omitempty"`

	// tempFileLimit is the per-process temporary file cap in force. PostgreSQL applies it
	// per backend process, so a session with parallel workers can consume a multiple of it.
	// +optional
	TempFileLimit *resource.Quantity `json:"tempFileLimit,omitempty"`
}

// PgTenantConnectionUtilization reports observed backend-connection usage. Placement and
// rebalancing pack on the trailing percentile, never on the mean.
type PgTenantConnectionUtilization struct {
	// current is the live count of held backend connections.
	// +optional
	Current *int32 `json:"current,omitempty"`

	// p95_7d is the 95th percentile over a trailing 7-day window.
	// +optional
	P95_7d *int32 `json:"p95_7d,omitempty"`

	// peak_7d is the maximum observed over a trailing 7-day window.
	// +optional
	Peak_7d *int32 `json:"peak_7d,omitempty"`
}

// PgTenantUtilization reports what the tenant is actually consuming.
type PgTenantUtilization struct {
	// backendConnections reports connection usage against the tenant's ceiling.
	// +optional
	BackendConnections *PgTenantConnectionUtilization `json:"backendConnections,omitempty"`

	// storageBytes is the database's allocated size. Scale-down decisions read allocated
	// rather than used space, because unused space inside the database is not reclaimable
	// without a rewrite.
	// +optional
	StorageBytes *int64 `json:"storageBytes,omitempty"`

	// isCold reports whether utilization has stayed below the pool's
	// hotTenantUtilizationThresholdPercent for the whole observation window. Only cold
	// tenants are eligible for rebalancing: moving a hot tenant consumes exactly the
	// resource that is scarce when rebalancing is worth doing.
	// +optional
	IsCold *bool `json:"isCold,omitempty"`
}

// PgTenantThrottleCounts is the last 24 hours of admission rejections broken out by code.
// The breakdown is the point: PGE1928 and PGE1936 have opposite remediations.
type PgTenantThrottleCounts struct {
	// PGE1928 counts rejections at the tenant's own burstable ceiling.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PGE1928 *int32 `json:"PGE1928,omitempty"`

	// PGE1936 counts rejections because the pool budget was exhausted.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PGE1936 *int32 `json:"PGE1936,omitempty"`

	// PGE1929 counts retryable rejections while within every limit.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PGE1929 *int32 `json:"PGE1929,omitempty"`

	// PGE0544 counts write rejections against the storage quota.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PGE0544 *int32 `json:"PGE0544,omitempty"`

	// PGE1024 counts admission queue timeouts.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PGE1024 *int32 `json:"PGE1024,omitempty"`

	// PGE1613 counts connections ended by a migration cutover.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PGE1613 *int32 `json:"PGE1613,omitempty"`
}

// PgTenantThrottleEvent is the most recent rejection.
type PgTenantThrottleEvent struct {
	// code identifies which limit rejected the request.
	// +optional
	Code *PgTenantThrottleCode `json:"code,omitempty"`

	// at is when the rejection happened.
	// +optional
	At *metav1.Time `json:"at,omitempty"`
}

// PgTenantThrottleStatus is the tenant's recent rejection history.
type PgTenantThrottleStatus struct {
	// last24h counts rejections over a trailing 24-hour window.
	// +optional
	Last24h *PgTenantThrottleCounts `json:"last24h,omitempty"`

	// lastEvent is the most recent rejection.
	// +optional
	LastEvent *PgTenantThrottleEvent `json:"lastEvent,omitempty"`
}

// PgTenantStatus is the observed state of a PgTenant.
type PgTenantStatus struct {
	// observedGeneration is the metadata.generation this status was computed from.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase summarises the conditions for kubectl output and carries no semantics of its
	// own.
	// +kubebuilder:default=Pending
	// +optional
	Phase PgTenantPhase `json:"phase,omitempty"`

	// qosClass is DERIVED by the controller from the effective guaranteed-to-burstable
	// relationship, exactly as the kubelet derives Pod QoS. It is never declared by a
	// user and never accepted from a spec field: guaranteed == burstable and both
	// non-zero is Guaranteed, 0 < guaranteed < burstable is Burstable, guaranteed == 0 is
	// BestEffort. Writing it anywhere but here is a bug.
	// +optional
	QoSClass QoSClass `json:"qosClass,omitempty"`

	// binding records which instance hosts the tenant's database.
	// +optional
	Binding *PgTenantBinding `json:"binding,omitempty"`

	// effective publishes the limits actually being enforced.
	// +optional
	Effective *PgTenantEffectiveLimits `json:"effective,omitempty"`

	// utilization reports observed consumption.
	// +optional
	Utilization *PgTenantUtilization `json:"utilization,omitempty"`

	// throttle reports recent admission rejections.
	// +optional
	Throttle *PgTenantThrottleStatus `json:"throttle,omitempty"`

	// conditions represent the current state of the PgTenant resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:categories=pgelastic,shortName=pgt
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.poolRef.name`
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.databaseName`
// +kubebuilder:printcolumn:name="QoS",type=string,JSONPath=`.status.qosClass`
// +kubebuilder:printcolumn:name="Instance",type=string,JSONPath=`.status.binding.instanceRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PgTenant is one tenant's claim on a PgElasticPool: one PostgreSQL DATABASE and one
// ROLE. It is the only object an application team owns, and a working spec is a pool
// reference plus a database name.
type PgTenant struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PgTenant
	// +required
	Spec PgTenantSpec `json:"spec"`

	// status defines the observed state of PgTenant
	// +optional
	Status PgTenantStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PgTenantList contains a list of PgTenant
type PgTenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgTenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgTenant{}, &PgTenantList{})
		return nil
	})
}
