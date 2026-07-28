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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PreemptionPolicy selects whether a class may displace lower-priority tenants that
// already hold burst credit, or may only be ordered ahead of them in the queue.
// +kubebuilder:validation:Enum=Never;PreemptLowerPriority
type PreemptionPolicy string

const (
	// PreemptionNever waits its turn: a higher-priority tenant is served first when
	// credit frees up, but nothing is revoked to make that happen.
	PreemptionNever PreemptionPolicy = "Never"
	// PreemptionLowerPriority revokes surplus burst credit from lower-priority tenants,
	// closing their least-recently-used idle backends first.
	PreemptionLowerPriority PreemptionPolicy = "PreemptLowerPriority"
)

// BudgetExhaustionPolicy selects what the proxy does to a tenant of this class once the
// pool's capacity budget is fully committed and no credit can be revoked.
// +kubebuilder:validation:Enum=Throttle;Reject;Evict
type BudgetExhaustionPolicy string

const (
	// BudgetExhaustionThrottle queues the client under the cross-tenant scheduler until
	// credit frees up or the admission queue times out.
	BudgetExhaustionThrottle BudgetExhaustionPolicy = "Throttle"
	// BudgetExhaustionReject fails the checkout immediately rather than queuing, which
	// keeps latency bounded for clients that would rather retry than wait.
	BudgetExhaustionReject BudgetExhaustionPolicy = "Reject"
	// BudgetExhaustionEvict closes the tenant's own surplus backends down to its
	// guarantee, freeing capacity for the rest of the pool.
	BudgetExhaustionEvict BudgetExhaustionPolicy = "Evict"
)

// WorkloadCapacity is the guaranteed/burstable/weight triple every tenant of the class
// inherits, in units of backend connections.
//
// In transaction pooling a held backend connection is exactly one unit of
// work-in-progress, so this triple is both the capacity model and the pool sizing.
// +kubebuilder:validation:XValidation:rule="self.guaranteed <= self.burstable",message="capacity.guaranteed must not exceed capacity.burstable"
type WorkloadCapacity struct {
	// guaranteed is the per-tenant floor of backend connections, honoured as a strict
	// non-work-conserving reservation: an idle tenant's unused guarantee is not lent out,
	// otherwise the guarantee decays into a promise plus eviction latency. Zero means the
	// tenant draws only from burst credit, which is what BestEffort is.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100000
	// +kubebuilder:default=0
	// +optional
	Guaranteed *int32 `json:"guaranteed,omitempty"`

	// burstable is the per-tenant ceiling of backend connections. It is a cap, never a
	// guarantee: the sum of burstable across a pool's tenants may exceed the pool budget,
	// and that oversubscription ratio is the product.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	// +required
	Burstable int32 `json:"burstable"`

	// weight orders tenants of equal guarantee deficit when the pool budget is exhausted,
	// as a share of the contended surplus.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10000
	// +kubebuilder:default=100
	// +optional
	Weight *int32 `json:"weight,omitempty"`

	// storage caps the tenant database's allocated size. WAL and temporary files are
	// excluded, so the effective disk footprint is larger than this number.
	// +optional
	Storage *resource.Quantity `json:"storage,omitempty"`

	// maxClientConnections caps client-side connections, a currency independent of
	// backend connections and bounded by file descriptors rather than max_connections.
	// In Session pool mode the two currencies collapse and this is clamped to burstable.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	// +optional
	MaxClientConnections *int32 `json:"maxClientConnections,omitempty"`
}

// WorkloadSLO is the service level the platform admin publishes for the class. It is
// reported against in tenant status; it does not itself configure enforcement.
type WorkloadSLO struct {
	// checkoutWaitP99 is the target 99th-percentile wait to obtain a backend connection.
	// +optional
	CheckoutWaitP99 *metav1.Duration `json:"checkoutWaitP99,omitempty"`

	// admissionErrorBudgetPercent is the tolerated share of checkouts denied or timed
	// out, as a decimal string so fractions below one percent are expressible.
	// +kubebuilder:validation:Pattern=`^(100(\.0+)?|[0-9]{1,2}(\.[0-9]+)?)$`
	// +kubebuilder:validation:MaxLength=16
	// +optional
	AdmissionErrorBudgetPercent *string `json:"admissionErrorBudgetPercent,omitempty"`
}

// QuarantinePolicy makes Azure's prose advice — observe an uncharacterised workload
// before letting it share a budget — enforceable at tenant admission.
type QuarantinePolicy struct {
	// required forces new tenants of this class through an observation period in the
	// pool's quarantine class before their declared capacity takes effect.
	// +kubebuilder:default=false
	// +optional
	Required *bool `json:"required,omitempty"`

	// duration is the observation window. Promotion is driven off a high percentile of
	// the tenant's usage over this window, never off the mean.
	// +kubebuilder:default="168h"
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`
}

// WorkloadAdmission gates how tenants of this class enter a pool.
type WorkloadAdmission struct {
	// quarantine requires an observation period before a new tenant's capacity is trusted.
	// +optional
	Quarantine *QuarantinePolicy `json:"quarantine,omitempty"`
}

// WorkloadMigrationPolicy governs whether the rebalancer may move tenants of this class
// between instances on its own initiative.
type WorkloadMigrationPolicy struct {
	// allowAutomatic lets the rebalancer emit PgTenantMigration objects for tenants of
	// this class without a human asking.
	// +kubebuilder:default=true
	// +optional
	AllowAutomatic *bool `json:"allowAutomatic,omitempty"`

	// requireApproval holds every migration of a tenant of this class at the preflight
	// phase until a human approves it, including migrations a human started.
	// +kubebuilder:default=false
	// +optional
	RequireApproval *bool `json:"requireApproval,omitempty"`
}

// AutoPausePolicy is RESERVED and unimplemented in v1. It is declared now so that
// shipping it later is an additive change rather than a new field on a stable API.
//
// The intended semantics mirror the Azure ARM contract: after delay with no client
// activity the tenant's backends are released and its capacity returned to the pool,
// and a negative delay disables auto-pause entirely. ARM spells "disabled" as the
// integer -1; because this field is a duration, express it as any negative duration
// such as "-1s". A controller in v1 accepts the field, reports it in status, and
// otherwise ignores it.
type AutoPausePolicy struct {
	// delay is the inactivity period before a tenant is paused. Negative disables.
	// +optional
	Delay *metav1.Duration `json:"delay,omitempty"`
}

// PgWorkloadClassSpec defines the desired state of PgWorkloadClass.
type PgWorkloadClassSpec struct {
	// priority orders admission across tenants of different classes, in the same sense as
	// a Kubernetes PriorityClass value: larger wins.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000000
	// +required
	Priority int32 `json:"priority"`

	// preemptionPolicy decides whether higher priority may revoke credit already held by
	// lower-priority tenants, or only jump the queue for credit that frees up naturally.
	// +kubebuilder:default=Never
	// +optional
	PreemptionPolicy *PreemptionPolicy `json:"preemptionPolicy,omitempty"`

	// description is free text shown to the tenant owner choosing a class.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Description *string `json:"description,omitempty"`

	// global marks this class as the one applied to tenants that name no class and whose
	// pool sets no default. At most one PgWorkloadClass cluster-wide may set it; the
	// constraint spans objects, so it is enforced by the validating webhook rather than
	// by CEL, and a second global class is rejected at admission.
	// +kubebuilder:default=false
	// +optional
	Global *bool `json:"global,omitempty"`

	// capacity is the guaranteed/burstable/weight triple tenants of this class inherit.
	// +required
	Capacity WorkloadCapacity `json:"capacity"`

	// limits are the per-tenant guardrails. Only the proxy-enforced deadline and byte
	// caps are hard; the GUC-backed entries are advisory because a client can SET them
	// back.
	// +optional
	Limits *TenantLimits `json:"limits,omitempty"`

	// slo publishes the service level tenants of this class are held to.
	// +optional
	SLO *WorkloadSLO `json:"slo,omitempty"`

	// onBudgetExhaustion selects the behaviour when the pool budget is fully committed
	// and no credit can be revoked.
	// +kubebuilder:default=Throttle
	// +optional
	OnBudgetExhaustion *BudgetExhaustionPolicy `json:"onBudgetExhaustion,omitempty"`

	// admission gates how tenants of this class enter a pool.
	// +optional
	Admission *WorkloadAdmission `json:"admission,omitempty"`

	// migration governs automatic tenant moves for this class.
	// +optional
	Migration *WorkloadMigrationPolicy `json:"migration,omitempty"`

	// autoPause is RESERVED and unimplemented in v1. See AutoPausePolicy.
	// +optional
	AutoPause *AutoPausePolicy `json:"autoPause,omitempty"`
}

// PgWorkloadClassStatus defines the observed state of PgWorkloadClass.
type PgWorkloadClassStatus struct {
	// observedGeneration is the spec generation the controller last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// tenantCount is how many PgTenant objects cluster-wide currently resolve to this
	// class. It is the blast radius of editing the spec.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TenantCount int32 `json:"tenantCount,omitempty"`

	// conditions report whether the class is accepted and usable by tenants.
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=8
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,categories=pgelastic,shortName=pgwc
// +kubebuilder:printcolumn:name="Priority",type=integer,JSONPath=`.spec.priority`
// +kubebuilder:printcolumn:name="Guaranteed",type=integer,JSONPath=`.spec.capacity.guaranteed`
// +kubebuilder:printcolumn:name="Burstable",type=integer,JSONPath=`.spec.capacity.burstable`
// +kubebuilder:printcolumn:name="Weight",type=integer,JSONPath=`.spec.capacity.weight`
// +kubebuilder:printcolumn:name="Global",type=boolean,JSONPath=`.spec.global`
// +kubebuilder:printcolumn:name="Tenants",type=integer,JSONPath=`.status.tenantCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PgWorkloadClass is the PriorityClass analogue for tenant workloads: a named,
// cluster-scoped bundle of priority, capacity, limits and SLO that a tenant adopts by
// naming it in one mutable string. Changing tier is editing that string.
//
// The class never declares a QoS class. The controller derives status.qosClass on each
// PgTenant from the guaranteed-versus-burstable relationship of that tenant's effective
// capacity, exactly as the kubelet derives Pod QoS:
//
//	guaranteed == burstable, both > 0   ->  Guaranteed
//	0 < guaranteed < burstable          ->  Burstable
//	guaranteed == 0                     ->  BestEffort
//
// Deriving rather than declaring means the QoS class cannot contradict the numbers that
// actually drive admission, and a per-tenant capacity override re-derives it for free.
type PgWorkloadClass struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PgWorkloadClass
	// +required
	Spec PgWorkloadClassSpec `json:"spec"`

	// status defines the observed state of PgWorkloadClass
	// +optional
	Status PgWorkloadClassStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PgWorkloadClassList contains a list of PgWorkloadClass
type PgWorkloadClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgWorkloadClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgWorkloadClass{}, &PgWorkloadClassList{})
		return nil
	})
}
