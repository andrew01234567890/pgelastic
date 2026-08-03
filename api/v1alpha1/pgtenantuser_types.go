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

// PgTenantUserSpec is one login inside a tenant's database.
//
// This is Azure SQL's contained database user, reproduced on PostgreSQL - where it cannot exist
// natively, because pg_authid is a shared catalog and there is no such thing as a user scoped
// to one database. Azure states the property this kind exists to give back: "The identity is
// confined to the database. The identity is independent (in all aspects) from a user who has
// the same name and the same password in another database in the same server."
//
// pgelastic reproduces it above PostgreSQL rather than inside it. The proxy is already the
// authentication boundary, so the credential lives in pgelastic's control plane scoped to a
// PgTenant, and the PostgreSQL role it maps to is named after the tenant's identity so two
// tenants' users of the same name cannot become one role.
//
// **What is not here is the point.** There is no field for SUPERUSER, CREATEDB, CREATEROLE,
// REPLICATION or BYPASSRLS, and no way to name a role outside this tenant. A PgTenantUser is
// structurally incapable of granting access beyond its own tenant, which is what makes it safe
// to let a tenant's own operators create them: the containment is the API's shape rather than a
// rule a webhook has to keep enforcing correctly for ever.
//
// Azure's own caveat is why that matters: "database owners and database users who have the
// ALTER ANY USER permission can grant access to the database... reduces the access control of
// highly privileged server logins and expands the access control to include highly privileged
// database users". Containment moves the trust boundary onto whoever can mint users, so here it
// is bounded by Kubernetes RBAC on this kind rather than by an in-database permission.
type PgTenantUserSpec struct {
	// TenantRef is the tenant this login belongs to, in the same namespace. A login is
	// meaningless without one: the tenant is what decides which database it reaches and which
	// PostgreSQL role it becomes.
	//
	// Immutable. The PostgreSQL role is derived from the tenant's identity, so moving a login
	// between tenants would leave the role it was provisioned under standing in pg_authid with
	// nothing referring to it - and would point a credential somebody already holds at a
	// database it was never granted.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="tenantRef is immutable: the PostgreSQL role is derived from the tenant, so moving a login would orphan the role it was provisioned under and hand an existing credential a database it was never granted"
	TenantRef corev1.LocalObjectReference `json:"tenantRef"`

	// UserName is the name a client sends in its startup packet.
	//
	// Unique within the tenant rather than within the cluster, which is the containment
	// property: two tenants may each have a user called `app`, and they are different
	// identities with different credentials that cannot reach each other's data.
	//
	// Immutable, for the same reason as tenantRef: it is an input to the derived role, and a
	// rename would strand the old one.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z_][a-z0-9_]*$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="userName is immutable: it is an input to the derived PostgreSQL role, so renaming a login would strand the role it was provisioned under"
	UserName string `json:"userName"`

	// CredentialsSecretRef holds the login's password, in the same namespace.
	//
	// A login with no credentials Secret authenticates nobody and is reported not ready rather
	// than admitted: a user the proxy cannot challenge is one it would have to let in.
	// +optional
	CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`

	// Login is whether this user may open a session. False makes it a group role - something to
	// grant to, and to hold grants, without anybody authenticating as it.
	// +optional
	// +kubebuilder:default=true
	Login *bool `json:"login,omitempty"`

	// MemberOf names other users **of the same tenant** whose privileges this one inherits.
	//
	// Same-tenant is enforced rather than conventional. A membership reaching outside the
	// tenant is the one thing this kind could otherwise express that would breach containment,
	// so it is refused at admission.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=32
	MemberOf []string `json:"memberOf,omitempty"`

	// ConnectionLimit bounds the sessions this login may hold. The proxy's ledger is the
	// enforcement point and the only one; -1 leaves it unbounded, which is the default.
	//
	// Deliberately **not** mirrored onto the PostgreSQL role. Once the proxy authenticates as
	// that role, every backend the fleet opens counts against rolconnlimit, so N replicas each
	// entitled to this many would breach it N-fold and deliver "too many connections for role"
	// to whichever client happened to be last. The tenant's own role is left uncapped for
	// exactly this reason - see connectionLimitOf in the tenant controller.
	// +optional
	// +kubebuilder:validation:Minimum=-1
	ConnectionLimit *int32 `json:"connectionLimit,omitempty"`
}

// PgTenantUserStatus is what the login actually is on the instance hosting its tenant.
type PgTenantUserStatus struct {
	// ObservedGeneration is the spec generation this status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// RoleName is the PostgreSQL role this login maps to.
	//
	// Published rather than left to be recomputed, because it is what appears in pg_stat_activity,
	// in log_line_prefix and in relowner - so anybody reading those needs to be able to get from
	// the role back to the object without knowing the derivation.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	RoleName string `json:"roleName,omitempty"`

	// Phase is a display-only summary of the conditions.
	// +optional
	// +kubebuilder:default=Pending
	Phase PgTenantUserPhase `json:"phase,omitempty"`

	// Conditions carry Accepted and Ready.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PgTenantUserPhase is the display-only summary.
// +kubebuilder:validation:Enum=Pending;Ready;Failed
type PgTenantUserPhase string

const (
	// PgTenantUserPhasePending means the role does not exist yet.
	PgTenantUserPhasePending PgTenantUserPhase = "Pending"
	// PgTenantUserPhaseReady means the role exists and the proxy can authenticate it.
	PgTenantUserPhaseReady PgTenantUserPhase = "Ready"
	// PgTenantUserPhaseFailed means the login was refused, and a condition says why.
	PgTenantUserPhaseFailed PgTenantUserPhase = "Failed"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:categories=pgelastic,shortName=pgtu
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantRef.name`
// +kubebuilder:printcolumn:name="User",type=string,JSONPath=`.spec.userName`
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.status.roleName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PgTenantUser is one login inside a tenant's database.
type PgTenantUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PgTenantUserSpec   `json:"spec,omitempty"`
	Status PgTenantUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PgTenantUserList contains a list of PgTenantUser.
type PgTenantUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PgTenantUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgTenantUser{}, &PgTenantUserList{})
		metav1.AddToGroupVersion(s, SchemeGroupVersion)
		return nil
	})
}
