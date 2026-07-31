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

// ElasticClassTier is a coarse service-tier label. It selects no behaviour on its own:
// every knob a tier implies is spelled out in this object's other fields. It exists so
// pools, dashboards and chargeback reports can be grouped and sorted without parsing names.
// +kubebuilder:validation:Enum=Development;GeneralPurpose;BusinessCritical
type ElasticClassTier string

const (
	// TierDevelopment is non-production: no SLO is offered and capacity may be reclaimed.
	TierDevelopment ElasticClassTier = "Development"
	// TierGeneralPurpose is the default production tier.
	TierGeneralPurpose ElasticClassTier = "GeneralPurpose"
	// TierBusinessCritical offers the tightest checkout-wait SLO and the lowest density caps.
	TierBusinessCritical ElasticClassTier = "BusinessCritical"
)

// TenancyModel declares how many tenants may share one provisioned PostgreSQL instance.
// +kubebuilder:validation:Enum=SharedInstance;DedicatedInstance
type TenancyModel string

const (
	// TenancySharedInstance places many tenant databases on each instance. Isolation is
	// partial by construction: shared_buffers, WAL, the checkpointer and base/pgsql_tmp
	// are common to every tenant on the instance.
	TenancySharedInstance TenancyModel = "SharedInstance"
	// TenancyDedicatedInstance places at most one tenant on each instance. Tenants still
	// share the pool's capacity budget and proxy fleet.
	TenancyDedicatedInstance TenancyModel = "DedicatedInstance"
)

// CapacityUnit names the resource the elastic budget is denominated in.
// +kubebuilder:validation:Enum=BackendConnections
type CapacityUnit string

// CapacityBackendConnections is the only unit pgelastic offers. In transaction pooling a
// held backend connection is exactly one unit of work-in-progress, so the elastic budget
// and the pooler's connection limit are the same number and cannot disagree.
const CapacityBackendConnections CapacityUnit = "BackendConnections"

// CapacityEnforcement names the primitive that actually makes the budget true.
// +kubebuilder:validation:Enum=ProxyLease;ProxyLeaseWithDatabaseBackstop
type CapacityEnforcement string

const (
	// EnforcementProxyLease grants every backend connection from the pool's lease ledger.
	// This is the only mechanism backing a guarantee.
	EnforcementProxyLease CapacityEnforcement = "ProxyLease"
	// EnforcementProxyLeaseWithDatabaseBackstop additionally mirrors each tenant's burst
	// ceiling into ALTER ROLE / ALTER DATABASE CONNECTION LIMIT. The backstop is
	// approximate and catches only direct connections; the proxy remains authoritative.
	EnforcementProxyLeaseWithDatabaseBackstop CapacityEnforcement = "ProxyLeaseWithDatabaseBackstop"
)

// PlacementDimension names a measurable property of a tenant or instance. Dimensions other
// than BackendConnections are observations, never guarantees.
// +kubebuilder:validation:Enum=BackendConnections;CPUSeconds;StorageBytes;RelationCount;WriteBytes;TransactionRate
type PlacementDimension string

const (
	PlacementDimensionBackendConnections PlacementDimension = "BackendConnections"
	PlacementDimensionCPUSeconds         PlacementDimension = "CPUSeconds"
	PlacementDimensionStorageBytes       PlacementDimension = "StorageBytes"
	PlacementDimensionRelationCount      PlacementDimension = "RelationCount"
	PlacementDimensionWriteBytes         PlacementDimension = "WriteBytes"
	PlacementDimensionTransactionRate    PlacementDimension = "TransactionRate"
)

// Tier1Control names a limit enforced by the proxy itself.
// +kubebuilder:validation:Enum=MaxBackendConnections;GuaranteedFloor;AdmissionQueue;QueryDeadline;MaxResultBytes;TransactionRateLimit;ByteRateLimit;MaxClientConnections;CancelBurstCredit
type Tier1Control string

// Tier2Control names a PostgreSQL GUC applied with ALTER ROLE ... SET.
// +kubebuilder:validation:Enum=StatementTimeout;IdleInTransactionSessionTimeout;IdleSessionTimeout;LockTimeout;TempFileLimit;WorkMem
type Tier2Control string

// Tier3Control names an operating-system control.
// +kubebuilder:validation:Enum=CPUWeight;CPUMax;MemoryMax;MemoryHigh;IOMax;IOWeight
type Tier3Control string

// ClassFeature is a capability name published in status so a client can detect what the
// running controller actually implements without inspecting its version.
// +kubebuilder:validation:MaxLength=64
type ClassFeature string

// PgElasticClassSpec is the cluster-scoped policy a platform admin sets once and every
// PgElasticPool referencing it inherits.
type PgElasticClassSpec struct {
	// controllerName is the controller that reconciles pools bound to this class, in the
	// form "example.com/controller-name". Following the GatewayClass precedent, a
	// controller whose own name does not match this value ignores the object silently: it
	// sets no conditions and raises no error, because an unmatched class belongs to a
	// different controller and is not a failure.
	//
	// The claim is inherited by the whole object graph beneath the class, because no other
	// kind carries one: a PgElasticPool through its classRef, a PgInstance and a PgTenant
	// through their poolRef, and a PgTenantMigration through its tenant. An object whose
	// route to a class cannot be resolved is claimed by nobody, so that two operators
	// looking at the same dangling reference do not both claim it and then rewrite it under
	// each other.
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*\/[A-Za-z0-9\/\-._~%!$&'()*+,;=:]+$`
	// +kubebuilder:default="pgelastic.io/elastic-pool-controller"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="controllerName is immutable"
	// +required
	ControllerName string `json:"controllerName"`

	// tier groups classes for reporting and chargeback.
	// +kubebuilder:default=GeneralPurpose
	// +optional
	Tier ElasticClassTier `json:"tier,omitempty"`

	// tenancyModel is immutable because changing it retroactively reinterprets the
	// isolation promise made to every tenant already bound to a pool of this class.
	// +kubebuilder:default=SharedInstance
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="tenancyModel is immutable"
	// +optional
	TenancyModel TenancyModel `json:"tenancyModel,omitempty"`

	// description is shown in `kubectl describe` and in the platform catalogue.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Description *string `json:"description,omitempty"`

	// capacityModel declares what the elastic budget is denominated in and how it is
	// enforced.
	// +optional
	CapacityModel *ElasticClassCapacityModel `json:"capacityModel,omitempty"`

	// density caps how much a single instance or pool may be packed. These are hard
	// admission limits, not scheduler hints.
	// +optional
	Density *ElasticClassDensity `json:"density,omitempty"`

	// governance publishes which limits are actually enforced and which are advisory.
	// It is a deliberate API surface: an operator who mistakes tier 2 for enforcement
	// will build a capacity plan on limits any client can SET away.
	// +optional
	Governance *ElasticClassGovernance `json:"governance,omitempty"`

	// defaults are applied to pools of this class that leave the corresponding field
	// unset. A pool may override any of them; the class does not clamp.
	// +optional
	Defaults *ElasticClassDefaults `json:"defaults,omitempty"`

	// runtime configures the proxy binary the controller deploys for pools of this class.
	// +optional
	Runtime *ElasticClassRuntime `json:"runtime,omitempty"`

	// errorCodes is the published mapping from an admission outcome to the SQLSTATE and
	// retry hint a client sees. It is API surface, not an implementation detail: client
	// retry logic can only distinguish "raise my ceiling" from "the pool is full" if the
	// codes are stable and documented.
	// +optional
	ErrorCodes *ElasticClassErrorCodes `json:"errorCodes,omitempty"`

	// admittedNamespaces selects which namespaces may create pools bound to this class.
	// Consent is bidirectional: a pool must also admit the tenant's namespace. One-way
	// selection, where naming an object is enough to use it, is a tenancy escape.
	// +optional
	AdmittedNamespaces *NamespaceAdmission `json:"admittedNamespaces,omitempty"`

	// reclaimPolicy decides the fate of provisioned databases, volumes and backups when a
	// pool of this class is deleted.
	// +kubebuilder:default=Retain
	// +optional
	ReclaimPolicy ReclaimPolicy `json:"reclaimPolicy,omitempty"`
}

// ElasticClassCapacityModel fixes the unit of the elastic budget and the way it is held.
type ElasticClassCapacityModel struct {
	// unit is immutable: every reservation, ledger entry and status number recorded under
	// this class is denominated in it, and there is no conversion that preserves a
	// guarantee already granted.
	// +kubebuilder:default=BackendConnections
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="unit is immutable"
	// +optional
	Unit CapacityUnit `json:"unit,omitempty"`

	// enforcement names the primitive that makes the budget real.
	// +kubebuilder:default=ProxyLease
	// +optional
	Enforcement CapacityEnforcement `json:"enforcement,omitempty"`

	// minFloor is the smallest non-zero guaranteed reservation a tenant may hold. A
	// guarantee between 1 and this value is rejected at admission rather than rounded,
	// because a floor too small to survive one client's connection burst is a promise the
	// pool cannot be held to. A guarantee of exactly 0 is always allowed and means
	// BestEffort.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1024
	// +kubebuilder:default=1
	// +optional
	MinFloor *int32 `json:"minFloor,omitempty"`

	// scoringDimensions are consulted ONLY when scoring a placement candidate or an
	// autoscaling recommendation. Nothing here is enforced, reserved or guaranteed at any
	// tier: CPU, storage throughput and relation count are observed after the fact, and a
	// tenant that exceeds its observed profile is not throttled for it. The single
	// enforced dimension is `unit`.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:default={{name:BackendConnections,weight:1000},{name:StorageBytes,weight:400},{name:RelationCount,weight:100}}
	// +optional
	ScoringDimensions []CapacityScoringDimension `json:"scoringDimensions,omitempty"`
}

// CapacityScoringDimension weights one observed dimension in placement and autoscaling
// scoring.
type CapacityScoringDimension struct {
	// name of the observed dimension.
	// +required
	Name PlacementDimension `json:"name"`

	// weight is relative to the other listed dimensions; the score is normalised across
	// whatever is present, so absolute magnitudes carry no meaning.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	// +kubebuilder:default=100
	// +optional
	Weight *int32 `json:"weight,omitempty"`
}

// ElasticClassDensity caps packing. Every value is enforced at tenant or pool admission.
type ElasticClassDensity struct {
	// maxTenantsPerInstance bounds tenant databases on one PostgreSQL instance.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5000
	// +kubebuilder:default=250
	// +optional
	MaxTenantsPerInstance *int32 `json:"maxTenantsPerInstance,omitempty"`

	// maxTenantsPerPool bounds the pool's tenant ledger. Beyond roughly a thousand
	// entries the cross-tenant scheduler's ordering pass needs re-analysis, so this is a
	// hard stop rather than a slow degradation.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=20000
	// +kubebuilder:default=1000
	// +optional
	MaxTenantsPerPool *int32 `json:"maxTenantsPerPool,omitempty"`

	// maxInstancesPerPool bounds how far a pool may scale out.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	// +kubebuilder:default=16
	// +optional
	MaxInstancesPerPool *int32 `json:"maxInstancesPerPool,omitempty"`

	// maxBackendConnectionsPerInstance caps the allocatable connections derived from an
	// instance class. PostgreSQL's per-snapshot and ProcArray costs grow with
	// max_connections, so raising this trades pool headroom for latency on every backend.
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=5000
	// +kubebuilder:default=500
	// +optional
	MaxBackendConnectionsPerInstance *int32 `json:"maxBackendConnectionsPerInstance,omitempty"`

	// maxRelationsPerInstance bounds the catalogue. Relation count, not data volume, is
	// what governs autovacuum sweep time, relcache memory and file-descriptor pressure,
	// and it is the density limit tenants reach first.
	// +kubebuilder:validation:Minimum=1000
	// +kubebuilder:default=200000
	// +optional
	MaxRelationsPerInstance *int32 `json:"maxRelationsPerInstance,omitempty"`

	// maxStoragePerTenant caps a single tenant's data size, excluding WAL and temporary
	// files.
	// +optional
	MaxStoragePerTenant *resource.Quantity `json:"maxStoragePerTenant,omitempty"`
}

// ElasticClassGovernance is the honesty table: which limits hold against a hostile client,
// which merely discourage one, and which do not exist at all.
type ElasticClassGovernance struct {
	// tier1Proxy is enforced by the proxy and is the only tier that can back a guarantee.
	// +optional
	Tier1Proxy *Tier1ProxyGovernance `json:"tier1Proxy,omitempty"`

	// tier2Postgres is advisory only.
	// +optional
	Tier2Postgres *Tier2PostgresGovernance `json:"tier2Postgres,omitempty"`

	// tier3OS is what the operating system can and cannot attribute to a tenant.
	// +optional
	Tier3OS *Tier3OSGovernance `json:"tier3OS,omitempty"`

	// storage is a soft quota with a hard terminal action.
	// +optional
	Storage *StorageGovernance `json:"storage,omitempty"`
}

// Tier1ProxyGovernance lists the hard limits. A tenant cannot exceed these by any means
// short of bypassing the proxy, which pg_hba.conf denies.
type Tier1ProxyGovernance struct {
	// enforced controls are applied on the admission path before a backend connection is
	// leased, so exceeding one is an error to the client rather than a debt to the pool.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:default={MaxBackendConnections,GuaranteedFloor,AdmissionQueue,QueryDeadline,MaxResultBytes,TransactionRateLimit,ByteRateLimit,MaxClientConnections,CancelBurstCredit}
	// +optional
	Enforced []Tier1Control `json:"enforced,omitempty"`
}

// Tier2PostgresGovernance lists limits applied with ALTER ROLE ... SET. Every useful GUC
// here except temp_file_limit is PGC_USERSET, so a client can SET it back to anything it
// likes and GRANT SET ON PARAMETER can only grant that ability, never revoke it. These are
// defence in depth; the proxy's deadline is the authoritative one.
type Tier2PostgresGovernance struct {
	// advisory controls are applied on connect and may be overridden by the client.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:default={StatementTimeout,IdleInTransactionSessionTimeout,IdleSessionTimeout,LockTimeout,TempFileLimit}
	// +optional
	Advisory []Tier2Control `json:"advisory,omitempty"`

	// reapplyOnCheckout re-issues the advisory SETs each time a pooled backend is handed
	// to a client, which bounds how long a client's own override survives to one
	// checkout.
	// +kubebuilder:default=true
	// +optional
	ReapplyOnCheckout *bool `json:"reapplyOnCheckout,omitempty"`
}

// Tier3OSGovernance covers cgroup v2 controls on per-tenant backend subgroups. Only CPU is
// attributable to a tenant.
type Tier3OSGovernance struct {
	// enforced controls are written to the tenant's cgroup.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:default={CPUWeight,CPUMax}
	// +optional
	Enforced []Tier3Control `json:"enforced,omitempty"`

	// neverPerBackend controls are refused by design, and the refusal is published so
	// nobody plans capacity around them.
	//
	// Memory: the page cache is charged to whichever backend first touches a page and is
	// never re-charged, so a per-backend memory.max attributes shared data to an arbitrary
	// tenant; an OOM kill inside a critical section crash-restarts the whole postmaster.
	//
	// IO: a per-backend io.max misses the checkpointer, bgwriter, walwriter and
	// autovacuum, which are most of the write traffic. PG18's default io_method=worker
	// additionally moves reads into shared IO workers, so read attribution is gone too.
	// Restoring it would require io_method=io_uring behind a bespoke seccomp profile, so
	// per-backend IO attribution is simply unavailable in v1 — which is consistent with a
	// capacity model denominated in connections.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:default={MemoryMax,MemoryHigh,IOMax,IOWeight}
	// +optional
	NeverPerBackend []Tier3Control `json:"neverPerBackend,omitempty"`
}

// StorageGovernance configures the per-tenant storage quota.
type StorageGovernance struct {
	// diskQuota is the soft quota implementation.
	// +optional
	DiskQuota *DiskQuotaGovernance `json:"diskQuota,omitempty"`
}

// DiskQuotaGovernance configures the diskquota extension. The quota is soft: usage is
// sampled, so a tenant can overshoot by at most naptime multiplied by its write rate
// before the terminal action lands. WAL and temporary files are outside the quota.
type DiskQuotaGovernance struct {
	// enabled turns per-tenant storage accounting on.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// naptime is the sampling interval, and therefore the overshoot bound. Shortening it
	// tightens the bound at the cost of a catalogue scan per interval.
	// +kubebuilder:default="2s"
	// +optional
	Naptime *metav1.Duration `json:"naptime,omitempty"`

	// warnAtPercent raises the Throttled condition and an Event before writes stop.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=90
	// +optional
	WarnAtPercent *int32 `json:"warnAtPercent,omitempty"`

	// hardAction is taken at 100% of quota. ReadOnly sets
	// default_transaction_read_only = on for the tenant, so writes fail while SELECT and
	// DELETE keep working — a tenant over quota must be able to delete its way out.
	// +kubebuilder:validation:Enum=ReadOnly;WarnOnly
	// +kubebuilder:default=ReadOnly
	// +optional
	HardAction string `json:"hardAction,omitempty"`
}

// ElasticClassDefaults are the policy defaults inherited by pools of this class.
type ElasticClassDefaults struct {
	// headroomPercent is withheld from the pool total before any guarantee is granted:
	// allocatable = total * (1 - headroomPercent/100). The webhook enforces
	// sum(guaranteed) <= allocatable, so this is what keeps a fully-reserved pool from
	// having nothing left for a failover or a rolling restart.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +kubebuilder:default=25
	// +optional
	HeadroomPercent *int32 `json:"headroomPercent,omitempty"`

	// migrationHeadroomPercent is withheld on top of headroomPercent while a tenant
	// migration is in flight, because during cutover the tenant's data and connections
	// exist on both the source and the target instance at once.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +kubebuilder:default=10
	// +optional
	MigrationHeadroomPercent *int32 `json:"migrationHeadroomPercent,omitempty"`

	// admission defaults for the pool's connection admission ladder.
	// +optional
	Admission *ElasticClassAdmissionDefaults `json:"admission,omitempty"`

	// pooling defaults for the proxy's connection pool behaviour.
	// +optional
	Pooling *ElasticClassPoolingDefaults `json:"pooling,omitempty"`

	// placement defaults for the tenant scheduler.
	// +optional
	Placement *ElasticClassPlacementDefaults `json:"placement,omitempty"`

	// rebalancing defaults for reactive tenant movement.
	// +optional
	Rebalancing *ElasticClassRebalancingDefaults `json:"rebalancing,omitempty"`

	// migration defaults for tenant moves.
	// +optional
	Migration *ElasticClassMigrationDefaults `json:"migration,omitempty"`
}

// ElasticClassAdmissionDefaults configures how connection requests are ordered and queued.
type ElasticClassAdmissionDefaults struct {
	// strategy orders waiters once the budget is exhausted. WeightedDeficit serves the
	// largest guarantee deficit first, then the least-satisfied burst fraction, weighted
	// by workload class. Fifo ignores guarantees entirely and exists only as an escape
	// hatch for debugging.
	// +kubebuilder:validation:Enum=WeightedDeficit;Fifo
	// +kubebuilder:default=WeightedDeficit
	// +optional
	Strategy string `json:"strategy,omitempty"`

	// queueDepthPerTenant bounds waiters per tenant so one tenant's backlog cannot
	// exhaust proxy memory or starve the scheduler's ordering pass.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10000
	// +kubebuilder:default=64
	// +optional
	QueueDepthPerTenant *int32 `json:"queueDepthPerTenant,omitempty"`

	// maxWait bounds time in the admission queue before the request is failed.
	// +kubebuilder:default="30s"
	// +optional
	MaxWait *metav1.Duration `json:"maxWait,omitempty"`

	// notifyAfter is when a waiting client receives a NoticeResponse telling it the
	// connection is queued rather than hung. Without it a slow admission is
	// indistinguishable from a dead proxy.
	// +kubebuilder:default="5s"
	// +optional
	NotifyAfter *metav1.Duration `json:"notifyAfter,omitempty"`

	// reservationMode decides whether an idle tenant's unused guarantee may be lent out.
	// Strict does not lend, matching Azure: lending makes every guaranteed request
	// trigger a revocation, which decays the guarantee into a promise plus an eviction
	// latency. Tenants wanting lendable capacity set guaranteed to 0.
	// +kubebuilder:validation:Enum=Strict;Lendable
	// +kubebuilder:default=Strict
	// +optional
	ReservationMode string `json:"reservationMode,omitempty"`

	// defaultWorkloadClassName is applied to a tenant that names no workload class.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	DefaultWorkloadClassName *string `json:"defaultWorkloadClassName,omitempty"`

	// allowedWorkloadClassNames restricts which workload classes tenants of this class
	// may select. Empty means every class is allowed.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=253
	// +optional
	AllowedWorkloadClassNames []string `json:"allowedWorkloadClassNames,omitempty"`

	// requireQuarantine forces a new tenant into an observation window on a restricted
	// workload class before it can hold production capacity, so an uncharacterised
	// workload cannot be admitted straight into a reservation it will not fit.
	// +kubebuilder:default=true
	// +optional
	RequireQuarantine *bool `json:"requireQuarantine,omitempty"`

	// quarantineWindow is how long a new tenant is observed before promotion.
	// +kubebuilder:default="168h"
	// +optional
	QuarantineWindow *metav1.Duration `json:"quarantineWindow,omitempty"`
}

// ElasticClassPoolingDefaults configures connection pooling behaviour.
type ElasticClassPoolingDefaults struct {
	// mode selects when a backend is returned to the pool.
	// +kubebuilder:default=Transaction
	// +optional
	Mode PoolMode `json:"mode,omitempty"`

	// preparedStatements selects protocol-level prepared statement support. Extended
	// keeps transaction mode usable by every mainstream driver, all of which auto-prepare
	// by default. Full additionally rewrites SQL-level PREPARE, which is not implemented
	// in v1.
	// +kubebuilder:validation:Enum=Disabled;Extended;Full
	// +kubebuilder:default=Extended
	// +optional
	PreparedStatements string `json:"preparedStatements,omitempty"`

	// preparedStatementsLimit bounds the per-backend statement cache, evicted LRU.
	//
	// A statement here is a plan the backend holds for the life of the link. Until the cache
	// could outlive a transaction none of these were ever allocated -- the cache was wiped on
	// every release -- so 1000 was a number that had never been paid for. It is paid for now,
	// per link: at 100 backends that is 100,000 plans resident in PostgreSQL. 200 matches
	// pgbouncer's max_prepared_statements and is the largest figure with a precedent behind it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100000
	// +kubebuilder:default=200
	// +optional
	PreparedStatementsLimit *int32 `json:"preparedStatementsLimit,omitempty"`

	// globalStatementsLimit caps the distinct statement texts one proxy instance interns before
	// its LRU evicts.
	//
	// A different quantity from preparedStatementsLimit, which bounds what a single backend link
	// has parsed. This bounds what the proxy has named, shared across every client on the
	// instance so two clients sending the same text get one identifier and the link parses once.
	// The entry owns the query text, so an unbounded table makes proxy memory a function of how
	// much distinct SQL the applications behind it contain rather than of anything configured.
	//
	// Raising it costs memory proportional to the average statement length; lowering it past an
	// application's working set of distinct statements costs backend parses, which
	// pgelastic_proxy_statements_evicted_total makes visible.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	// +kubebuilder:default=2048
	// +optional
	GlobalStatementsLimit *int32 `json:"globalStatementsLimit,omitempty"`

	// serverIdleTimeout closes idle backends. It is suppressed at or below a tenant's
	// guaranteed floor, since reaping a reserved connection only forces it to be
	// re-established.
	// +kubebuilder:default="600s"
	// +optional
	ServerIdleTimeout *metav1.Duration `json:"serverIdleTimeout,omitempty"`

	// serverLifetime recycles a backend regardless of activity.
	// +kubebuilder:default="3600s"
	// +optional
	ServerLifetime *metav1.Duration `json:"serverLifetime,omitempty"`

	// serverLifetimeJitter spreads recycling. Without it a pool established in one burst
	// tears itself down in one burst an hour later, which is a self-inflicted outage.
	// +kubebuilder:default="300s"
	// +optional
	ServerLifetimeJitter *metav1.Duration `json:"serverLifetimeJitter,omitempty"`

	// idleSelection picks which idle backend is handed out next. Lifo keeps the working
	// set small so serverIdleTimeout can actually retire the tail.
	// +kubebuilder:validation:Enum=Lifo;Fifo
	// +kubebuilder:default=Lifo
	// +optional
	IdleSelection string `json:"idleSelection,omitempty"`

	// resetMode selects how much session state is scrubbed before reuse.
	// +kubebuilder:default=DirtyTracked
	// +optional
	ResetMode ResetPolicy `json:"resetMode,omitempty"`

	// trackExtraParameters are GUC_REPORT parameters added to the variable cache beyond
	// the built-in set, so a difference between client and backend is corrected rather
	// than silently inherited from the previous client.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=63
	// +optional
	TrackExtraParameters []string `json:"trackExtraParameters,omitempty"`

	// ignoreStartupParameters are startup-packet parameters excluded from the pool key.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:default={extra_float_digits,options}
	// +optional
	IgnoreStartupParameters []string `json:"ignoreStartupParameters,omitempty"`

	// startupParameterPolicy handles a startup parameter the proxy does not track.
	// PoolKey is the only setting that is both safe and compatible: RESET ALL restores
	// session-start values, so startup parameters are part of session identity and cannot
	// be scrubbed away.
	// +kubebuilder:default=PoolKey
	// +optional
	StartupParameterPolicy StartupParameterPolicy `json:"startupParameterPolicy,omitempty"`

	// pinningPolicy handles session state that cannot be scrubbed, such as LISTEN, a WITH
	// HOLD cursor or setseed(). Pinning costs throughput; scrubbing incorrectly leaks one
	// tenant's data to another.
	// +kubebuilder:default=Pin
	// +optional
	PinningPolicy PinningPolicy `json:"pinningPolicy,omitempty"`

	// maxPinnedFractionPercent bounds how much of a pool may be pinned. Pinned backends
	// are removed from the elastic budget and gauged separately, so an unbounded pinned
	// fraction makes the pool's effective ceiling unexplainable.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=20
	// +optional
	MaxPinnedFractionPercent *int32 `json:"maxPinnedFractionPercent,omitempty"`

	// maxPinDuration closes a pinned backend that has held its client's state for too
	// long.
	// +kubebuilder:default="1h"
	// +optional
	MaxPinDuration *metav1.Duration `json:"maxPinDuration,omitempty"`
}

// ElasticClassPlacementDefaults configures the tenant scheduler.
type ElasticClassPlacementDefaults struct {
	// strategy selects the bin-packing algorithm. PowerOfTwoChoices is used for admitting
	// a single new tenant, where a full scan buys nothing.
	// +kubebuilder:validation:Enum=BestFitDecreasing;PowerOfTwoChoices;Spread
	// +kubebuilder:default=BestFitDecreasing
	// +optional
	Strategy string `json:"strategy,omitempty"`

	// packOnPercentile selects the statistic of the trailing observation window that
	// placement packs against. Never the mean: bursty tenants are the premise of an
	// elastic pool, and a mean hides exactly the peaks that collide.
	// +kubebuilder:validation:Enum=P50;P95;P99;Peak
	// +kubebuilder:default=P95
	// +optional
	PackOnPercentile string `json:"packOnPercentile,omitempty"`

	// observationWindow is the trailing window the percentile is computed over. A week
	// covers the weekly cycle most business workloads have.
	// +kubebuilder:default="168h"
	// +optional
	ObservationWindow *metav1.Duration `json:"observationWindow,omitempty"`

	// maxSkewTenants bounds the tenant-count difference between the fullest and emptiest
	// instance before placement prefers the emptier one regardless of fit.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	// +kubebuilder:default=15
	// +optional
	MaxSkewTenants *int32 `json:"maxSkewTenants,omitempty"`
}

// ElasticClassRebalancingDefaults configures reactive tenant movement between instances.
type ElasticClassRebalancingDefaults struct {
	// enabled turns automatic rebalancing on.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// mode restricts which tenants may be moved. Moving a hot tenant consumes exactly the
	// resources that are scarce on an instance that is already the reason to rebalance.
	// +kubebuilder:validation:Enum=ColdTenantsOnly;AnyTenant;Disabled
	// +kubebuilder:default=ColdTenantsOnly
	// +optional
	Mode string `json:"mode,omitempty"`

	// evaluationInterval is how often imbalance is scored.
	// +kubebuilder:default="15m"
	// +optional
	EvaluationInterval *metav1.Duration `json:"evaluationInterval,omitempty"`

	// minImbalancePercent is the spread between instances below which a move is not worth
	// its risk.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=20
	// +optional
	MinImbalancePercent *int32 `json:"minImbalancePercent,omitempty"`

	// maxConcurrentMigrations bounds in-flight moves per pool. Each move holds a
	// replication slot and duplicates the tenant's storage.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=16
	// +kubebuilder:default=1
	// +optional
	MaxConcurrentMigrations *int32 `json:"maxConcurrentMigrations,omitempty"`

	// hotTenantUtilizationThresholdPercent is the utilization at or above which a tenant
	// counts as hot and is excluded under ColdTenantsOnly.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=15
	// +optional
	HotTenantUtilizationThresholdPercent *int32 `json:"hotTenantUtilizationThresholdPercent,omitempty"`

	// forbidMoveWhenSourceUtilizationAbovePercent blocks moves off an instance that is
	// too busy to also run logical replication decoding for the departing tenant.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=65
	// +optional
	ForbidMoveWhenSourceUtilizationAbovePercent *int32 `json:"forbidMoveWhenSourceUtilizationAbovePercent,omitempty"`

	// blackoutWindows are recurring periods during which no automatic move starts. A move
	// already in flight when a window opens runs to completion, because aborting mid-
	// cutover is riskier than finishing.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=16
	// +optional
	BlackoutWindows []TimeWindow `json:"blackoutWindows,omitempty"`
}

// ElasticClassMigrationDefaults configures tenant moves.
type ElasticClassMigrationDefaults struct {
	// strategy selects the default move mechanism. Online uses logical replication and
	// queues clients through a sub-second cutover. Offline dumps and restores, pausing for
	// tens of seconds, and is confined to a maintenance window.
	// +kubebuilder:validation:Enum=Online;Offline
	// +kubebuilder:default=Online
	// +optional
	Strategy string `json:"strategy,omitempty"`

	// allowAutomatic lets the rebalancer and autoscaler start moves without a human.
	// +kubebuilder:default=true
	// +optional
	AllowAutomatic *bool `json:"allowAutomatic,omitempty"`

	// requireApproval holds every move at the preflight gate until a human approves it.
	// +kubebuilder:default=false
	// +optional
	RequireApproval *bool `json:"requireApproval,omitempty"`

	// maxPause is the cutover budget: if clients would be queued longer than this, the
	// cutover is abandoned and the tenant keeps serving from the source.
	// +kubebuilder:default="1s"
	// +optional
	MaxPause *metav1.Duration `json:"maxPause,omitempty"`

	// rollbackWindow is how long the source database is kept, connections refused, after a
	// successful cutover. Dropping it immediately makes an undetected cutover defect
	// unrecoverable.
	// +kubebuilder:default="1h"
	// +optional
	RollbackWindow *metav1.Duration `json:"rollbackWindow,omitempty"`

	// offlineWindow is when an Offline move may run.
	// +optional
	OfflineWindow *TimeWindow `json:"offlineWindow,omitempty"`
}

// ElasticClassRuntime configures the proxy processes the controller deploys.
type ElasticClassRuntime struct {
	// proxyImage overrides the controller's compiled-in proxy image. The controller
	// refuses an image whose reported protocol capabilities do not cover this class's
	// posture, rather than starting a fleet that will reject connections.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	ProxyImage *string `json:"proxyImage,omitempty"`

	// imagePullPolicy for the proxy image.
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	// +kubebuilder:default=IfNotPresent
	// +optional
	ImagePullPolicy *corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// workers is the proxy's runtime thread count. It is deliberately not derived from
	// the host CPU count: in a CPU-limited pod, spawning one worker per visible core makes
	// the runtime consume its whole CFS quota in the first few milliseconds of each period
	// and stall for the remainder.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	// +kubebuilder:default=2
	// +optional
	Workers *int32 `json:"workers,omitempty"`

	// memoryBuffers sizes the proxy's per-connection and per-packet allocations, which
	// together with the client connection cap determine its memory ceiling.
	// +optional
	MemoryBuffers *ProxyMemoryBuffers `json:"memoryBuffers,omitempty"`

	// protocol is the wire-protocol posture offered to clients.
	// +optional
	Protocol *ProxyProtocolPosture `json:"protocol,omitempty"`
}

// ProxyMemoryBuffers sizes the proxy's buffers. Total proxy memory is roughly
// maxClientConnections multiplied by perConnectionBufferLimit, so these are capacity
// planning inputs, not micro-optimisations.
type ProxyMemoryBuffers struct {
	// packetBufferSize is the read buffer used per socket for message framing.
	// +kubebuilder:default="4Ki"
	// +optional
	PacketBufferSize *resource.Quantity `json:"packetBufferSize,omitempty"`

	// maxPacketSize rejects a single protocol message larger than this. PostgreSQL's own
	// message length field is a signed 32-bit count, so 1Gi is the protocol ceiling.
	// +kubebuilder:default="1Gi"
	// +optional
	MaxPacketSize *resource.Quantity `json:"maxPacketSize,omitempty"`

	// perConnectionBufferLimit is the backpressure high-water mark: once this much
	// unwritten data is queued toward a peer, the proxy stops reading from the other side
	// rather than buffering a slow client's result set into an OOM kill.
	// +kubebuilder:default="1Mi"
	// +optional
	PerConnectionBufferLimit *resource.Quantity `json:"perConnectionBufferLimit,omitempty"`

	// listenBacklog is the accept queue depth.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=128
	// +optional
	ListenBacklog *int32 `json:"listenBacklog,omitempty"`
}

// ProxyProtocolPosture declares what the proxy offers on the wire before authentication.
// These are protocol-compatibility decisions, not tuning: getting one wrong makes some
// client library fail to connect at all.
type ProxyProtocolPosture struct {
	// cancelKeySizeBytes is the length of the proxy-generated cancel key. Protocol 3.2
	// allows 4 to 256 bytes, so the key is a byte string and never a 32-bit integer. The
	// proxy embeds a routing identifier and a hop TTL in it, because with several proxy
	// replicas behind one Service a CancelRequest arrives at an arbitrary pod that would
	// otherwise have no way to find the query. Widening it later is a wire break, so it is
	// sized generously on day one.
	// +kubebuilder:validation:Minimum=4
	// +kubebuilder:validation:Maximum=256
	// +kubebuilder:default=32
	// +optional
	CancelKeySizeBytes *int32 `json:"cancelKeySizeBytes,omitempty"`

	// directTLS accepts a TLS ClientHello as the first bytes on the socket, skipping the
	// SSLRequest round trip. PostgreSQL 17 and later require ALPN "postgresql" on this
	// path, and the proxy enforces it.
	// +kubebuilder:default=true
	// +optional
	DirectTLS *bool `json:"directTLS,omitempty"`

	// gssEncryption offers GSSAPI transport encryption. Disabled means the proxy answers
	// a GSSENCRequest with 'N' and the client falls back, which every supported client
	// does cleanly.
	// +kubebuilder:default=false
	// +optional
	GSSEncryption *bool `json:"gssEncryption,omitempty"`

	// replicationConnections admits physical and logical replication connections. They are
	// forced into session mode unconditionally regardless of the pool's mode, because a
	// multiplexed replication stream corrupts silently.
	// +kubebuilder:default=true
	// +optional
	ReplicationConnections *bool `json:"replicationConnections,omitempty"`

	// channelBinding offers SCRAM-SHA-256-PLUS bound to the proxy's own certificate via
	// tls-server-end-point. End-to-end binding to the backend's certificate is
	// structurally impossible behind a TLS-terminating proxy; Require here means the
	// client is bound to the proxy, and nothing more.
	// +kubebuilder:validation:Enum=Disable;Prefer;Require
	// +kubebuilder:default=Prefer
	// +optional
	ChannelBinding string `json:"channelBinding,omitempty"`

	// startupPacketMaxBytes caps the startup packet. PostgreSQL's own
	// MAX_STARTUP_PACKET_LENGTH is 10000 and clients are built against it, so raising this
	// admits packets no real server would accept.
	// +kubebuilder:validation:Minimum=512
	// +kubebuilder:validation:Maximum=10000
	// +kubebuilder:default=10000
	// +optional
	StartupPacketMaxBytes *int32 `json:"startupPacketMaxBytes,omitempty"`

	// negotiateGrease tolerates libpq's deliberately invalid protocol version probe
	// (3.9999) by answering NegotiateProtocolVersion instead of erroring.
	// +kubebuilder:default=true
	// +optional
	NegotiateGrease *bool `json:"negotiateGrease,omitempty"`

	// echoUnknownPqOptions lists every unrecognised _pq_. startup option back in the
	// NegotiateProtocolVersion reply, which the protocol requires. Erroring on them
	// instead is what permanently burned protocol 3.1.
	// +kubebuilder:default=true
	// +optional
	EchoUnknownPqOptions *bool `json:"echoUnknownPqOptions,omitempty"`
}

// ElasticClassErrorCodes maps each admission and quota outcome to what the client sees.
// The three-way split between "your ceiling", "the pool's ceiling" and "transiently busy"
// is what makes correct client retry logic possible; collapsing them into one code makes
// every failure look like a reason to retry harder.
type ElasticClassErrorCodes struct {
	// tenantCapReached fires when a tenant is below the pool's ceiling but at its own.
	// The fix is to raise the tenant's burstable, so retrying is pointless.
	// +kubebuilder:default={code:"PGE1928",sqlState:"53300",retryable:false}
	// +optional
	TenantCapReached *ErrorCodeMapping `json:"tenantCapReached,omitempty"`

	// poolCapacityExhausted fires when the pool's whole budget is committed. The fix is to
	// scale the pool.
	// +kubebuilder:default={code:"PGE1936",sqlState:"53400",retryable:true,retryAfter:"5s"}
	// +optional
	PoolCapacityExhausted *ErrorCodeMapping `json:"poolCapacityExhausted,omitempty"`

	// poolBusy fires when the request is within every configured limit but the pool cannot
	// serve it right now. This is the only one of the three where retrying is the correct
	// client behaviour.
	// +kubebuilder:default={code:"PGE1929",sqlState:"53400",retryable:true,retryAfter:"1s"}
	// +optional
	PoolBusy *ErrorCodeMapping `json:"poolBusy,omitempty"`

	// storageQuotaExceeded fires on a write by a tenant at its storage quota. SELECT and
	// DELETE keep working so the tenant can recover without operator involvement.
	// +kubebuilder:default={code:"PGE0544",sqlState:"53100",retryable:false}
	// +optional
	StorageQuotaExceeded *ErrorCodeMapping `json:"storageQuotaExceeded,omitempty"`

	// admissionQueueTimeout fires when a queued client exceeds admission.maxWait.
	// +kubebuilder:default={code:"PGE1024",sqlState:"53400",retryable:true,retryAfter:"5s"}
	// +optional
	AdmissionQueueTimeout *ErrorCodeMapping `json:"admissionQueueTimeout,omitempty"`

	// migrationCutover fires on the sessions that cannot be queued through a tenant move.
	// It uses the admin-shutdown SQLSTATE because every driver already treats that as
	// "reconnect", which is exactly the required behaviour.
	// +kubebuilder:default={code:"PGE1613",sqlState:"57P01",retryable:true,retryAfter:"1s"}
	// +optional
	MigrationCutover *ErrorCodeMapping `json:"migrationCutover,omitempty"`
}

// ErrorCodeMapping is one row of the published error taxonomy.
type ErrorCodeMapping struct {
	// code is the pgelastic identifier carried in the error message. Changing it breaks
	// client retry logic that matches on it.
	// +kubebuilder:validation:MaxLength=16
	// +kubebuilder:validation:Pattern=`^PGE[0-9]{4}$`
	// +required
	Code string `json:"code"`

	// sqlState is the five-character SQLSTATE sent in the ErrorResponse. It is overridable
	// because some client frameworks key their retry policy on a fixed set of SQLSTATEs
	// and cannot be taught new ones.
	// +kubebuilder:validation:MaxLength=5
	// +kubebuilder:validation:MinLength=5
	// +kubebuilder:validation:Pattern=`^[0-9A-Z]{5}$`
	// +required
	SQLState string `json:"sqlState"`

	// retryable advertises whether retrying the same request can succeed without a
	// configuration change.
	// +kubebuilder:default=false
	// +optional
	Retryable *bool `json:"retryable,omitempty"`

	// retryAfter is the delay published to clients and exporters for a retryable code.
	// +optional
	RetryAfter *metav1.Duration `json:"retryAfter,omitempty"`

	// messageTemplate overrides the client-facing message. The proxy substitutes only the
	// documented placeholders and never interpolates tenant-supplied text.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	MessageTemplate *string `json:"messageTemplate,omitempty"`
}

// PgElasticClassStatus is the observed state of PgElasticClass.
type PgElasticClassStatus struct {
	// observedGeneration is the spec generation the conditions below were computed from.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// poolCount is how many pools reference this class. It is the value the
	// pgelastic.io/pools-exist finalizer guards: deletion blocks while it is non-zero.
	// +kubebuilder:validation:Minimum=0
	// +optional
	PoolCount int32 `json:"poolCount,omitempty"`

	// supportedFeatures is what the reconciling controller actually implements, so a pool
	// author can detect a capability gap from the API instead of from a failed rollout.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=64
	// +optional
	SupportedFeatures []ClassFeature `json:"supportedFeatures,omitempty"`

	// conditions describe the current state of the PgElasticClass.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,categories=pgelastic,shortName=pgec
// +kubebuilder:printcolumn:name="Tier",type=string,JSONPath=`.spec.tier`
// +kubebuilder:printcolumn:name="Tenancy",type=string,JSONPath=`.spec.tenancyModel`
// +kubebuilder:printcolumn:name="Pools",type=integer,JSONPath=`.status.poolCount`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Controller",type=string,JSONPath=`.spec.controllerName`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PgElasticClass is the cluster-scoped policy object a platform admin owns. It binds a
// controller, fixes the capacity model and density caps, publishes the governance honesty
// table, and supplies the defaults every PgElasticClass-bound pool inherits.
//
// The controller adds the finalizer pgelastic.io/pools-exist while any pool references the
// class, so deleting a class cannot orphan a running pool's policy.
type PgElasticClass struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PgElasticClass
	// +required
	Spec PgElasticClassSpec `json:"spec"`

	// status defines the observed state of PgElasticClass
	// +optional
	Status PgElasticClassStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PgElasticClassList contains a list of PgElasticClass
type PgElasticClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgElasticClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgElasticClass{}, &PgElasticClassList{})
		return nil
	})
}
