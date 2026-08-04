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
	"k8s.io/apimachinery/pkg/util/intstr"
)

// AdmissionStrategy orders clients waiting for a backend connection once the pool
// budget is exhausted.
//
// WeightedDeficit orders across tenants by guarantee deficit first, then by burst
// saturation weighted by the workload class weight; within a tenant the order is always
// strict FIFO so per-client wait time stays bounded and explainable.
// +kubebuilder:validation:Enum=WeightedDeficit;Fifo
type AdmissionStrategy string

const (
	AdmissionWeightedDeficit AdmissionStrategy = "WeightedDeficit"
	AdmissionFifo            AdmissionStrategy = "Fifo"
)

// ReservationMode selects whether an idle tenant's guaranteed credits may be lent out.
//
// Strict matches Azure: a guarantee is non-work-conserving, so reserved credits are not
// available to other tenants even while unused. Elastic lends them and revokes on demand,
// which turns a guarantee into a promise plus an eviction latency.
// +kubebuilder:validation:Enum=Strict;Elastic
type ReservationMode string

const (
	ReservationStrict  ReservationMode = "Strict"
	ReservationElastic ReservationMode = "Elastic"
)

// PreparedStatementsMode selects how far the proxy goes to keep named prepared
// statements working under transaction pooling.
//
// Extended covers the protocol-level Parse/Bind path every mainstream driver uses by
// default. Full additionally rewrites SQL-level PREPARE/EXECUTE/DEALLOCATE and is not
// implemented in v1.
// +kubebuilder:validation:Enum=Disabled;Extended;Full
type PreparedStatementsMode string

const (
	PreparedStatementsDisabled PreparedStatementsMode = "Disabled"
	PreparedStatementsExtended PreparedStatementsMode = "Extended"
	PreparedStatementsFull     PreparedStatementsMode = "Full"
)

// IdleSelection picks which idle backend is handed to the next client. lifo keeps a small
// working set warm and lets the rest age out against serverIdleTimeout.
// +kubebuilder:validation:Enum=lifo;fifo
type IdleSelection string

const (
	IdleSelectionLIFO IdleSelection = "lifo"
	IdleSelectionFIFO IdleSelection = "fifo"
)

// Percentile names a summary statistic over a trailing observation window. Packing and
// promotion decisions are always taken on a high percentile, never the mean: a mean hides
// exactly the bursts an oversubscribed pool has to survive.
// +kubebuilder:validation:Enum=P50;P75;P90;P95;P99
type Percentile string

const (
	PercentileP50 Percentile = "P50"
	PercentileP75 Percentile = "P75"
	PercentileP90 Percentile = "P90"
	PercentileP95 Percentile = "P95"
	PercentileP99 Percentile = "P99"
)

// PlacementStrategy selects the bin-packing algorithm used to choose an instance for a
// newly admitted tenant.
// +kubebuilder:validation:Enum=BestFitDecreasing
type PlacementStrategy string

const (
	PlacementBestFitDecreasing PlacementStrategy = "BestFitDecreasing"
)

// RebalanceMode bounds which tenants the rebalancer is allowed to move.
//
// ColdTenantsOnly is the safe setting: moving a hot tenant consumes exactly the resource
// that is scarce when the pool is imbalanced.
// +kubebuilder:validation:Enum=ColdTenantsOnly;AllTenants
type RebalanceMode string

const (
	RebalanceColdTenantsOnly RebalanceMode = "ColdTenantsOnly"
	RebalanceAllTenants      RebalanceMode = "AllTenants"
)

// MigrationStrategy selects how a tenant is moved between instances.
//
// Online uses logical replication and holds client sockets queued across a sub-second
// cutover. Offline dumps and restores, pausing for tens of seconds.
// +kubebuilder:validation:Enum=Online;Offline
type MigrationStrategy string

const (
	MigrationOnline  MigrationStrategy = "Online"
	MigrationOffline MigrationStrategy = "Offline"
)

// MigrationVerification selects how much of the moved data is proven equivalent before
// the source copy is dropped. Equivalence is not correctness: ctid, TOAST layout, index
// physical structure and planner statistics are never verified.
// +kubebuilder:validation:Enum=Schema;RowCounts;Checksums
type MigrationVerification string

const (
	MigrationVerifySchema    MigrationVerification = "Schema"
	MigrationVerifyRowCounts MigrationVerification = "RowCounts"
	MigrationVerifyChecksums MigrationVerification = "Checksums"
)

// AutoscalingMode selects whether the autoscaler executes its plan or only publishes it.
//
// Recommend is the default because every scaling action except storage expansion either
// restarts a database or moves tenant data.
// +kubebuilder:validation:Enum=Recommend;Auto
type AutoscalingMode string

const (
	AutoscalingRecommend AutoscalingMode = "Recommend"
	AutoscalingAuto      AutoscalingMode = "Auto"
)

// AutoAction is one class of change the autoscaler may execute without human approval.
// They are listed in the order they are meant to be opted into: earlier entries are
// cheaper and more reversible than later ones.
// +kubebuilder:validation:Enum=TenantGucTune;StorageExpand;ScaleOut;Rebalance;VerticalResize;ScaleIn
type AutoAction string

const (
	AutoActionTenantGucTune  AutoAction = "TenantGucTune"
	AutoActionStorageExpand  AutoAction = "StorageExpand"
	AutoActionScaleOut       AutoAction = "ScaleOut"
	AutoActionRebalance      AutoAction = "Rebalance"
	AutoActionVerticalResize AutoAction = "VerticalResize"
	AutoActionScaleIn        AutoAction = "ScaleIn"
)

// StaleMetricPolicy selects what the planner does when the metrics it would act on are
// older than the pool's staleness threshold.
//
// DoNothing is the default and is the KEDA-shaped answer: acting on a stale reading is how
// an autoscaler amplifies an incident, because the reading most likely to be stale is the
// one taken while the thing being measured was already failing.
// +kubebuilder:validation:Enum=DoNothing;ScaleToMinimum
type StaleMetricPolicy string

const (
	StaleMetricDoNothing      StaleMetricPolicy = "DoNothing"
	StaleMetricScaleToMinimum StaleMetricPolicy = "ScaleToMinimum"
)

// TenantDiscriminator is one input the proxy uses to decide which tenant a new client
// connection belongs to.
// +kubebuilder:validation:Enum=SNI;StartupOptions;DatabaseName
type TenantDiscriminator string

const (
	DiscriminatorSNI            TenantDiscriminator = "SNI"
	DiscriminatorStartupOptions TenantDiscriminator = "StartupOptions"
	DiscriminatorDatabaseName   TenantDiscriminator = "DatabaseName"
)

// DiscriminatorPrecedence selects what happens when two discriminators disagree about a
// connection's tenant. Strict rejects the connection rather than guessing, because a wrong
// guess routes one customer's queries into another customer's database.
// +kubebuilder:validation:Enum=Strict
type DiscriminatorPrecedence string

const (
	DiscriminatorPrecedenceStrict DiscriminatorPrecedence = "Strict"
)

// CancelKeyRouting selects how a CancelRequest arriving at an arbitrary proxy replica
// reaches the replica actually holding the client's backend. The wire format of the cancel
// key is fixed on day one: changing it later breaks every in-flight client.
// +kubebuilder:validation:Enum=EmbeddedPodIdentity
type CancelKeyRouting string

const (
	CancelKeyEmbeddedPodIdentity CancelKeyRouting = "EmbeddedPodIdentity"
)

// ProxyDrainMode selects what a terminating proxy replica waits for.
//
// waitForClients waits until every client disconnects, which preserves sessions but can
// take arbitrarily long. waitForServers waits only until backends are released.
// +kubebuilder:validation:Enum=waitForClients;waitForServers
type ProxyDrainMode string

const (
	ProxyDrainWaitForClients ProxyDrainMode = "waitForClients"
	ProxyDrainWaitForServers ProxyDrainMode = "waitForServers"
)

// ProxyReadinessMode selects how readiness is decided. A bare TCP probe is never an
// option: the listener accepts long before the pool can serve, so a TCP probe marks a
// replica ready while every client would be rejected.
// +kubebuilder:validation:Enum=adminState
type ProxyReadinessMode string

const (
	ProxyReadinessAdminState ProxyReadinessMode = "adminState"
)

// PoolPhase is a display-only summary shown in kubectl get. It is a pure function of the
// conditions and carries no information the conditions do not; never branch on it.
// +kubebuilder:validation:Enum=Pending;Provisioning;Ready;Degraded;Paused;Terminating
type PoolPhase string

const (
	PoolPhasePending      PoolPhase = "Pending"
	PoolPhaseProvisioning PoolPhase = "Provisioning"
	PoolPhaseReady        PoolPhase = "Ready"
	PoolPhaseDegraded     PoolPhase = "Degraded"
	PoolPhasePaused       PoolPhase = "Paused"
	PoolPhaseTerminating  PoolPhase = "Terminating"
)

// PoolCapacity is the pool's budget. Backend connections are the only capacity unit that
// backs a guarantee: under transaction pooling a held backend connection is exactly one
// unit of work in progress, so the elastic-pool budget and the pooler's pool size are the
// same number and must not be stacked as two independent limiters.
type PoolCapacity struct {
	// backendConnections is the pool-wide budget of tenant-usable backend connections.
	// It is the scale subresource's spec path. Admission rejects a value exceeding the sum
	// of the member instances' allocatable connections, which is itself derived from
	// max_connections minus superuser, replication and agent reserves.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	// +required
	BackendConnections int32 `json:"backendConnections"`

	// headroomPercent is withheld from the budget before guarantees are counted, so
	// allocatable = backendConnections * (1 - headroomPercent/100). Guarantees are admitted
	// against allocatable, never against the raw budget.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +kubebuilder:default=25
	// +optional
	HeadroomPercent *int32 `json:"headroomPercent,omitempty"`

	// maxClientConnections caps client-side sockets across the proxy fleet. This is a
	// second, independent currency bounded by file descriptors rather than by
	// max_connections. In Session pool mode the two currencies collapse and admission
	// clamps each tenant's client limit to its burstable value.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	// +optional
	MaxClientConnections *int32 `json:"maxClientConnections,omitempty"`

	// storage caps tenant data across the pool. It excludes WAL and temporary files, so a
	// pool can still exhaust its volumes while reporting storage headroom.
	// +optional
	Storage *resource.Quantity `json:"storage,omitempty"`

	// maxOversubscriptionRatio caps the sum of every tenant's burstable capacity divided by
	// allocatable. Oversubscription is the product, so this is a ceiling rather than a
	// prohibition; the observed ratio is published in status.
	// +kubebuilder:validation:MaxLength=16
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?$`
	// +kubebuilder:default="12"
	// +optional
	MaxOversubscriptionRatio string `json:"maxOversubscriptionRatio,omitempty"`
}

// PoolInstances declares how many PostgreSQL instances back the pool and what shape they
// take. pgelastic provisions these; they are not adopted from elsewhere.
type PoolInstances struct {
	// replicas is the number of PgInstance objects in the pool. Each one is itself an HA
	// group, so this is not a redundancy setting.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	// +kubebuilder:default=3
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// template is the shape every member instance is provisioned from.
	// +required
	Template PgInstanceTemplate `json:"template"`
}

// PgInstanceTemplate is the instance shape a pool provisions its members from. Its fields
// mirror PgInstanceSpec so a member can be diffed against the template without
// translation.
type PgInstanceTemplate struct {
	// class names the CPU/memory/storage sizing class. It is what max_connections, and
	// therefore the pool's capacity ceiling, is derived from.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:MinLength=1
	// +required
	Class string `json:"class"`

	// postgresVersion is the major the members provisioned from this template run.
	//
	// It accepts what PgInstanceSpec.PostgresVersion accepts, because that is the field it is
	// stamped onto and a pool that could not express a major its own instances can is a pool
	// that cannot be built on this template. Changing it later does not upgrade the members
	// that already exist - the version is immutable on an instance, and there is no in-place
	// major upgrade here - so a pool moves majors by provisioning new members and migrating
	// its tenants onto them, one tenant at a time.
	// +kubebuilder:validation:Enum="18";"19"
	// +kubebuilder:default="18"
	// +optional
	PostgresVersion *string `json:"postgresVersion,omitempty"`

	// highAvailability configures the replication topology of each member.
	// +optional
	HighAvailability *InstanceHighAvailability `json:"highAvailability,omitempty"`

	// storage configures the data and WAL volumes.
	// +required
	Storage InstanceStorage `json:"storage"`

	// resources are the PostgreSQL container's compute resources.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// parameters sets the user-settable subset of PostgreSQL GUCs. Operator-owned
	// parameters listed as fixed or blocked are rejected at admission and dropped again in
	// the config generator, so a stale object cannot poison a pod.
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(name, name.matches('^[a-zA-Z_][a-zA-Z0-9_]*([.][a-zA-Z_][a-zA-Z0-9_]*)?$'))",message="a parameter name must be a PostgreSQL identifier, optionally qualified by an extension prefix"
	// +optional
	Parameters map[string]GUCValue `json:"parameters,omitempty"`

	// backup configures physical backup and WAL archiving.
	// +optional
	Backup *InstanceBackup `json:"backup,omitempty"`

	// perTenantLogicalBackup configures nightly per-tenant dumps, which are the only path
	// that restores one tenant without touching the others.
	// +optional
	PerTenantLogicalBackup *PerTenantLogicalBackup `json:"perTenantLogicalBackup,omitempty"`
}

// PoolAdmission governs which tenants may bind to the pool and how contended capacity is
// handed out.
type PoolAdmission struct {
	// strategy orders waiting clients when the budget is exhausted.
	// +kubebuilder:default=WeightedDeficit
	// +optional
	Strategy AdmissionStrategy `json:"strategy,omitempty"`

	// queueDepthPerTenant caps queued clients per tenant. Bounding it per tenant rather than
	// per pool stops one tenant's retry storm from evicting every other tenant's waiters.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	// +kubebuilder:default=64
	// +optional
	QueueDepthPerTenant *int32 `json:"queueDepthPerTenant,omitempty"`

	// maxWaitSeconds bounds how long a client waits for a backend before it is denied.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=30
	// +optional
	MaxWaitSeconds *int32 `json:"maxWaitSeconds,omitempty"`

	// queryWaitNotifySeconds is when a still-waiting client receives a NoticeResponse, so a
	// slow connect is diagnosable from the client side rather than only from pool metrics.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=5
	// +optional
	QueryWaitNotifySeconds *int32 `json:"queryWaitNotifySeconds,omitempty"`

	// reservationMode selects whether idle guaranteed credits are lendable.
	// +kubebuilder:default=Strict
	// +optional
	ReservationMode ReservationMode `json:"reservationMode,omitempty"`

	// admittedNamespaces is the pool side of a bidirectional consent: the class must admit
	// the namespace and so must the pool. One-way selection, where a tenant simply names a
	// pool, is a tenancy escape.
	// +optional
	AdmittedNamespaces *NamespaceAdmission `json:"admittedNamespaces,omitempty"`

	// defaultWorkloadClassName is applied to tenants that name no class.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	DefaultWorkloadClassName string `json:"defaultWorkloadClassName,omitempty"`

	// allowedWorkloadClassNames restricts which classes tenants in this pool may claim. An
	// empty list allows every class the pool's PgElasticClass permits.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=253
	// +optional
	AllowedWorkloadClassNames []string `json:"allowedWorkloadClassNames,omitempty"`

	// breakGlassRole names a PostgreSQL role granted one reserved connection that bypasses
	// every admission gate, so an operator can still reach a pool that is refusing everyone.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	BreakGlassRole string `json:"breakGlassRole,omitempty"`

	// quarantine holds newly admitted tenants in a restricted class until their workload has
	// been observed, so an uncharacterised tenant cannot destabilise the pool on day one.
	// +optional
	Quarantine *PoolQuarantine `json:"quarantine,omitempty"`
}

// PoolQuarantine holds new tenants in a restricted workload class until enough of their
// behaviour has been observed to size them.
type PoolQuarantine struct {
	// enabled turns quarantine on for newly bound tenants.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// observationWindow is how long a tenant's utilization is sampled before promotion is
	// considered.
	// +kubebuilder:default="168h"
	// +optional
	ObservationWindow *metav1.Duration `json:"observationWindow,omitempty"`

	// promotionPercentile is the statistic of the observed window that the promoted class's
	// capacity must cover.
	// +kubebuilder:default=P95
	// +optional
	PromotionPercentile Percentile `json:"promotionPercentile,omitempty"`

	// promotionWorkloadClassName is the class a tenant graduates into.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	PromotionWorkloadClassName string `json:"promotionWorkloadClassName,omitempty"`

	// autoPromote graduates a tenant without human approval once the window closes.
	// +kubebuilder:default=false
	// +optional
	AutoPromote *bool `json:"autoPromote,omitempty"`
}

// PoolingConfig configures the connection multiplexer. These knobs govern correctness of
// backend reuse, not just throughput: the reset and pinning settings are what keep one
// tenant's session state from reaching another tenant.
type PoolingConfig struct {
	// mode selects when a backend connection is returned to the pool. Replication
	// connections are forced to Session regardless of this setting.
	// +kubebuilder:default=Transaction
	// +optional
	Mode PoolMode `json:"mode,omitempty"`

	// preparedStatements selects prepared-statement multiplexing. Disabling it makes
	// transaction mode unusable for pgjdbc, psycopg3, asyncpg, pgx and Npgsql, all of which
	// prepare statements by default.
	// +kubebuilder:default=Extended
	// +optional
	PreparedStatements PreparedStatementsMode `json:"preparedStatements,omitempty"`

	// preparedStatementsLimit caps cached statements per backend connection; the least
	// recently used entry is closed on overflow.
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

	// serverIdleTimeout closes idle backends. It is suppressed for a tenant at or below its
	// guaranteed count, since closing those only forces an immediate reconnect.
	// +kubebuilder:default="600s"
	// +optional
	ServerIdleTimeout *metav1.Duration `json:"serverIdleTimeout,omitempty"`

	// serverLifetime recycles a backend regardless of activity.
	// +kubebuilder:default="3600s"
	// +optional
	ServerLifetime *metav1.Duration `json:"serverLifetime,omitempty"`

	// serverLifetimeJitter spreads recycling. Without it a pool that started together
	// recycles together, which is a self-inflicted outage once an hour.
	// +kubebuilder:default="300s"
	// +optional
	ServerLifetimeJitter *metav1.Duration `json:"serverLifetimeJitter,omitempty"`

	// idleSelection picks which idle backend serves the next client.
	// +kubebuilder:default=lifo
	// +optional
	IdleSelection IdleSelection `json:"idleSelection,omitempty"`

	// resetMode selects how much session state is scrubbed before reuse.
	// +kubebuilder:default=DirtyTracked
	// +optional
	ResetMode ResetPolicy `json:"resetMode,omitempty"`

	// trackExtraParameters adds GUCs to the variable cache beyond the built-in set, so a
	// client assigning them does not silently inherit another client's value.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=63
	// +optional
	TrackExtraParameters []string `json:"trackExtraParameters,omitempty"`

	// ignoreStartupParameters lists startup packet keys excluded from the pool key. The
	// defaults are the two that every driver sends and that no backend behaviour depends on.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:default={"extra_float_digits","options"}
	// +optional
	IgnoreStartupParameters []string `json:"ignoreStartupParameters,omitempty"`

	// startupParameterPolicy handles startup parameters the proxy does not track, including
	// ones nested inside options.
	// +kubebuilder:default=PoolKey
	// +optional
	StartupParameterPolicy StartupParameterPolicy `json:"startupParameterPolicy,omitempty"`

	// pinOnSessionState keeps a client on its backend once it creates state that cannot be
	// scrubbed, such as LISTEN, a WITH HOLD cursor or a session advisory lock. Pinning costs
	// throughput; the alternative leaks one tenant's data to another.
	// +kubebuilder:default=true
	// +optional
	PinOnSessionState *bool `json:"pinOnSessionState,omitempty"`

	// maxPinnedFractionPercent caps how much of the budget may be held by pinned backends.
	// Pinned connections are excluded from the elastic budget and gauged separately,
	// otherwise the effective ceiling becomes unexplainable.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=20
	// +optional
	MaxPinnedFractionPercent *int32 `json:"maxPinnedFractionPercent,omitempty"`

	// maxPinDuration closes a pinned backend that has been held this long.
	// +kubebuilder:default="1h"
	// +optional
	MaxPinDuration *metav1.Duration `json:"maxPinDuration,omitempty"`
}

// PoolTimeouts bounds every wait in the data path. Each one exists because the
// corresponding unbounded wait holds a backend connection, and a held backend connection
// is capacity taken from every other tenant.
type PoolTimeouts struct {
	// connect bounds a backend TCP connect plus authentication.
	// +kubebuilder:default="5s"
	// +optional
	Connect *metav1.Duration `json:"connect,omitempty"`

	// checkout bounds how long a client waits to be granted a backend.
	// +kubebuilder:default="30s"
	// +optional
	Checkout *metav1.Duration `json:"checkout,omitempty"`

	// query bounds a single statement, enforced by the proxy with a CancelRequest and then a
	// close. This is the authoritative deadline; the equivalent GUCs are advisory because a
	// client can SET them back. It bounds a statement and not a transaction: a client sitting
	// idle between two statements is bounded by clientIdleInTransaction instead.
	//
	// Zero disables it, which is the only way to allow a statement to run unbounded. Every
	// other timeout here treats zero as "unset" and falls back to its default.
	// +kubebuilder:default="120s"
	// +optional
	Query *metav1.Duration `json:"query,omitempty"`

	// clientLogin bounds a client's startup and authentication exchange.
	// +kubebuilder:default="10s"
	// +optional
	ClientLogin *metav1.Duration `json:"clientLogin,omitempty"`

	// clientIdleInTransaction closes a client that holds an open transaction without
	// working, which also holds locks and the xmin horizon.
	// +kubebuilder:default="60s"
	// +optional
	ClientIdleInTransaction *metav1.Duration `json:"clientIdleInTransaction,omitempty"`

	// rollback bounds the cleanup rollback issued before a dirty backend is returned.
	// +kubebuilder:default="5s"
	// +optional
	Rollback *metav1.Duration `json:"rollback,omitempty"`

	// cancelWait bounds the dedicated connection opened to deliver a CancelRequest.
	// +kubebuilder:default="10s"
	// +optional
	CancelWait *metav1.Duration `json:"cancelWait,omitempty"`

	// shutdown bounds graceful drain on SIGTERM.
	// +kubebuilder:default="60s"
	// +optional
	Shutdown *metav1.Duration `json:"shutdown,omitempty"`

	// shutdownTermination bounds the forced close phase after drain gives up.
	// +kubebuilder:default="60s"
	// +optional
	ShutdownTermination *metav1.Duration `json:"shutdownTermination,omitempty"`
}

// PoolPlacement decides which instance a newly admitted tenant lands on.
type PoolPlacement struct {
	// strategy selects the bin-packing algorithm.
	// +kubebuilder:default=BestFitDecreasing
	// +optional
	Strategy PlacementStrategy `json:"strategy,omitempty"`

	// packOnPercentile selects the statistic packing decisions use.
	// +kubebuilder:default=P95
	// +optional
	PackOnPercentile Percentile `json:"packOnPercentile,omitempty"`

	// observationWindow is the trailing window the percentile is computed over.
	// +kubebuilder:default="168h"
	// +optional
	ObservationWindow *metav1.Duration `json:"observationWindow,omitempty"`

	// maxSkewTenants bounds the tenant-count difference between the fullest and emptiest
	// instance before placement starts preferring the emptiest.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	// +kubebuilder:default=15
	// +optional
	MaxSkewTenants *int32 `json:"maxSkewTenants,omitempty"`
}

// PoolRebalancing moves already-placed tenants between instances to correct drift.
type PoolRebalancing struct {
	// enabled turns automatic rebalancing on. It is off by default because every rebalance
	// is a live tenant migration.
	// +kubebuilder:default=false
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// mode bounds which tenants may be moved.
	// +kubebuilder:default=ColdTenantsOnly
	// +optional
	Mode RebalanceMode `json:"mode,omitempty"`

	// evaluationInterval is how often imbalance is assessed.
	// +kubebuilder:default="15m"
	// +optional
	EvaluationInterval *metav1.Duration `json:"evaluationInterval,omitempty"`

	// minImbalancePercent is the spread that must exist before any move is proposed. Below
	// it, the cost of moving exceeds the benefit.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=20
	// +optional
	MinImbalancePercent *int32 `json:"minImbalancePercent,omitempty"`

	// maxConcurrentMigrations caps simultaneous moves. Each move holds a replication slot
	// and adds logical decoding load to its source.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=16
	// +kubebuilder:default=1
	// +optional
	MaxConcurrentMigrations *int32 `json:"maxConcurrentMigrations,omitempty"`

	// hotTenantUtilizationThresholdPercent is the utilization at or above which a tenant
	// counts as hot and, under ColdTenantsOnly, becomes ineligible to move.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=15
	// +optional
	HotTenantUtilizationThresholdPercent *int32 `json:"hotTenantUtilizationThresholdPercent,omitempty"`

	// forbidMoveWhenSourceUtilizationAbovePercent refuses to move anything off an instance
	// that is already busy, since logical decoding would consume exactly the capacity the
	// move is meant to relieve.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=65
	// +optional
	ForbidMoveWhenSourceUtilizationAbovePercent *int32 `json:"forbidMoveWhenSourceUtilizationAbovePercent,omitempty"`

	// blackoutWindows suspend rebalancing during business hours or change freezes.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	// +optional
	BlackoutWindows []TimeWindow `json:"blackoutWindows,omitempty"`
}

// PoolMigration sets pool-wide defaults for tenant moves. A PgTenantMigration may narrow
// these but never widen them.
type PoolMigration struct {
	// defaultStrategy selects the move mechanism used when a migration does not name one.
	// +kubebuilder:default=Online
	// +optional
	DefaultStrategy MigrationStrategy `json:"defaultStrategy,omitempty"`

	// allowOnlineDuringBusinessHours permits Online moves at any time. Online cutover queues
	// clients rather than dropping them, which is what makes reactive rebalancing viable.
	// +kubebuilder:default=true
	// +optional
	AllowOnlineDuringBusinessHours *bool `json:"allowOnlineDuringBusinessHours,omitempty"`

	// offlineWindows confine Offline moves, whose pause is measured in tens of seconds.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	// +optional
	OfflineWindows []TimeWindow `json:"offlineWindows,omitempty"`

	// maxPause is the cutover pause budget. Exceeding it aborts the cutover back to the
	// source rather than extending the outage.
	// +kubebuilder:default="1s"
	// +optional
	MaxPause *metav1.Duration `json:"maxPause,omitempty"`

	// rollbackWindow is how long the source database is kept, connections refused, after a
	// successful cutover.
	// +kubebuilder:default="1h"
	// +optional
	RollbackWindow *metav1.Duration `json:"rollbackWindow,omitempty"`

	// verification is the evidence required before the source copy is dropped.
	// +kubebuilder:default=RowCounts
	// +optional
	Verification MigrationVerification `json:"verification,omitempty"`

	// requireApproval holds every migration at the preflight boundary until a human
	// approves it.
	// +kubebuilder:default=false
	// +optional
	RequireApproval *bool `json:"requireApproval,omitempty"`
}

// PoolAutoscaling configures the capacity planner.
// +kubebuilder:validation:XValidation:rule="!has(self.minInstances) || !has(self.maxInstances) || self.maxInstances >= self.minInstances",message="maxInstances must be greater than or equal to minInstances"
type PoolAutoscaling struct {
	// mode selects whether plans are executed or only published as Events and status.
	// +kubebuilder:default=Recommend
	// +optional
	Mode AutoscalingMode `json:"mode,omitempty"`

	// minInstances is the floor on pool member count.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	// +optional
	MinInstances *int32 `json:"minInstances,omitempty"`

	// maxInstances is the ceiling on pool member count.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	// +optional
	MaxInstances *int32 `json:"maxInstances,omitempty"`

	// targetUtilizationPercent is the allocatable-connection utilization the planner steers
	// towards.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=70
	// +optional
	TargetUtilizationPercent *int32 `json:"targetUtilizationPercent,omitempty"`

	// stabilizationWindow damps oscillation.
	// +optional
	StabilizationWindow *PoolStabilizationWindow `json:"stabilizationWindow,omitempty"`

	// autoActions is the set of change classes the planner may execute in Auto mode.
	// Everything not listed is planned and reported but never applied, so the list is opted
	// into one class at a time. Within one planning pass at most one class is executed, and
	// the classes are considered in the order they are declared on AutoAction: cheapest and
	// most reversible first, scale-in last.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=6
	// +optional
	AutoActions []AutoAction `json:"autoActions,omitempty"`

	// tolerancePercent is the HPA-shaped dead band. A utilization within this much of the
	// target proposes nothing at all, which is what stops a pool oscillating around a
	// boundary it is already close enough to.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +kubebuilder:default=10
	// +optional
	TolerancePercent *int32 `json:"tolerancePercent,omitempty"`

	// staleMetricThreshold is how old the newest metering sample may be before the planner
	// treats the pool as unmeasured.
	// +kubebuilder:default="5m"
	// +optional
	StaleMetricThreshold *metav1.Duration `json:"staleMetricThreshold,omitempty"`

	// staleMetricPolicy is what happens once that threshold is crossed.
	// +kubebuilder:default=DoNothing
	// +optional
	StaleMetricPolicy StaleMetricPolicy `json:"staleMetricPolicy,omitempty"`

	// consolidationDwellTime is how long an instance must have been consolidatable before
	// scale-in will act on it. It is separate from the scale-down stabilization window: the
	// window says the surplus has persisted, the dwell time says the decision to reclaim a
	// specific instance has persisted.
	// +kubebuilder:default="24h"
	// +optional
	ConsolidationDwellTime *metav1.Duration `json:"consolidationDwellTime,omitempty"`

	// scaleInEvidenceWindow is how much continuous per-tenant history must exist before an
	// instance may be removed. Scale-in is the one action that cannot be undone in seconds,
	// so it is gated on a week of evidence rather than on a stabilization window.
	// +kubebuilder:default="168h"
	// +optional
	ScaleInEvidenceWindow *metav1.Duration `json:"scaleInEvidenceWindow,omitempty"`

	// migrationBudget bounds how much tenant movement the planner may spend.
	// +optional
	MigrationBudget *MigrationBudget `json:"migrationBudget,omitempty"`

	// blackoutWindows suspend every executed action, not only rebalancing. A pool that must
	// not be touched during a change freeze must not be resized during it either.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	// +optional
	BlackoutWindows []TimeWindow `json:"blackoutWindows,omitempty"`

	// storage configures when a volume is grown.
	// +optional
	Storage *StorageAutoscaling `json:"storage,omitempty"`
}

// MigrationBudget bounds tenant movement, in the shape of a Karpenter disruption budget:
// a concurrency cap, a rate cap, and cron-scoped windows outside which the budget is zero.
type MigrationBudget struct {
	// maxConcurrent caps simultaneous moves. Each one holds a replication slot and adds
	// logical decoding load to its source.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=16
	// +kubebuilder:default=1
	// +optional
	MaxConcurrent *int32 `json:"maxConcurrent,omitempty"`

	// maxPerWindow caps how many moves may start inside one scheduling window, so a plan
	// that wants to move fifty tenants spends its budget over days rather than in an hour.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	// +kubebuilder:default=4
	// +optional
	MaxPerWindow *int32 `json:"maxPerWindow,omitempty"`

	// windows scope when moves may start. An empty list means any time, which is only a
	// sane default because an Online move queues clients rather than dropping them.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Windows []TimeWindow `json:"windows,omitempty"`
}

// StorageAutoscaling configures volume expansion, the one action the planner executes even
// in Recommend mode. It is online, it is the only remedy for a full volume, and it is the
// only scaling action a database survives without noticing. Shrinking is impossible, which
// is why the trigger is deliberately late and the target deliberately generous.
type StorageAutoscaling struct {
	// expandAtPercent is the used-to-allocated ratio that triggers an expansion.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=99
	// +kubebuilder:default=80
	// +optional
	ExpandAtPercent *int32 `json:"expandAtPercent,omitempty"`

	// expandToPercent is the used-to-allocated ratio the expansion aims to restore.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=99
	// +kubebuilder:default=60
	// +optional
	ExpandToPercent *int32 `json:"expandToPercent,omitempty"`

	// maxSize caps how large a volume the planner may ask for, so a runaway table cannot
	// spend an unbounded amount of somebody's money.
	// +optional
	MaxSize *resource.Quantity `json:"maxSize,omitempty"`
}

// PoolStabilizationWindow is the asymmetric damping applied to scaling decisions. Scale-up
// is cheap and reversible; scale-down consumes migrations and cannot be undone quickly,
// so the two windows are deliberately different orders of magnitude.
type PoolStabilizationWindow struct {
	// scaleUp is how long a shortage must persist before capacity is added.
	// +kubebuilder:default="3m"
	// +optional
	ScaleUp *metav1.Duration `json:"scaleUp,omitempty"`

	// scaleDown is how long a surplus must persist before capacity is removed.
	// +kubebuilder:default="30m"
	// +optional
	ScaleDown *metav1.Duration `json:"scaleDown,omitempty"`
}

// PoolAuth configures how clients authenticate to the proxy and how the proxy then
// authenticates to the backends.
type PoolAuth struct {
	// mode selects the authentication chain.
	// +kubebuilder:default=ScramPassthrough
	// +optional
	Mode AuthMode `json:"mode,omitempty"`

	// scramIterations is the PBKDF2 iteration count used when provisioning verifiers. Under
	// ScramPassthrough the proxy must reproduce the backend verifier byte for byte, so this
	// value is part of the credential, not a tunable.
	// +kubebuilder:validation:Minimum=4096
	// +kubebuilder:validation:Maximum=1000000
	// +kubebuilder:default=4096
	// +optional
	ScramIterations *int32 `json:"scramIterations,omitempty"`

	// rotation schedules credential rotation.
	// +optional
	Rotation *PoolAuthRotation `json:"rotation,omitempty"`
}

// PoolAuthRotation schedules credential rotation. Rotation keeps a current and a previous
// verifier valid at once so in-flight clients are not disconnected mid-rotation.
type PoolAuthRotation struct {
	// schedule is a five-field cron expression.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// overlapWindow is how long the previous verifier stays accepted after a rotation.
	// +kubebuilder:default="24h"
	// +optional
	OverlapWindow *metav1.Duration `json:"overlapWindow,omitempty"`
}

// ProxySpec configures the pool's proxy fleet. The fleet is inline on the pool rather than
// a separate kind because it holds the pool's reservation ledger and its tenants' SCRAM
// verifiers: sharing one fleet across pools would put a config blast radius and a CVE
// blast radius across a tenancy boundary.
type ProxySpec struct {
	// replicas is the proxy pod count, and it is a capacity multiplier. Every replica reads
	// one configuration document carrying the undivided backendConnections budget, so N
	// replicas can hold N times it against PostgreSQL. Admission accounts for that: a
	// configuration whose worst case exceeds the pool's declared oversubscription ceiling is
	// rejected, and status.proxy.leasedConnections publishes the grant actually in force.
	// Per-replica leases, which would make this a division rather than a multiplication, are
	// not implemented.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=3
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// resources are the proxy container's compute resources.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// workers is the async runtime's worker thread count. It is set explicitly rather than
	// derived from the visible CPU count, because a pod that spawns one worker per host core
	// under a CPU limit spends its quota on CFS throttling.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=256
	// +kubebuilder:default=2
	// +optional
	Workers *int32 `json:"workers,omitempty"`

	// tls configures both the client-facing listener and the backend connections.
	// +optional
	TLS *ProxyTLS `json:"tls,omitempty"`

	// routing configures how a connection is mapped to a tenant.
	// +optional
	Routing *ProxyRouting `json:"routing,omitempty"`

	// drain configures graceful shutdown.
	// +optional
	Drain *ProxyDrain `json:"drain,omitempty"`

	// readiness configures the readiness probe.
	// +optional
	Readiness *ProxyReadiness `json:"readiness,omitempty"`

	// service configures the client-facing Service.
	// +optional
	Service *ProxyService `json:"service,omitempty"`

	// podDisruptionBudget configures voluntary-disruption protection for the fleet.
	// +optional
	PodDisruptionBudget *ProxyPodDisruptionBudget `json:"podDisruptionBudget,omitempty"`

	// terminationGracePeriodSeconds must cover the pre-stop delay plus the shutdown and
	// shutdown-termination timeouts with margin, or the kubelet kills the proxy mid-drain
	// and every client on that replica is dropped. Admission enforces the relationship
	// against spec.timeouts.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=150
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// template is strategically merged over the generated proxy pod. It is the single escape
	// hatch that keeps node selectors, tolerations, sidecars and volumes from becoming thirty
	// dedicated fields.
	// +optional
	Template *ProxyPodTemplate `json:"template,omitempty"`

	// metrics configures the Prometheus endpoint.
	// +optional
	Metrics *ProxyMetrics `json:"metrics,omitempty"`
}

// ProxyTLS configures TLS on both sides of the proxy. Channel binding is offered against
// the proxy's own certificate; end-to-end binding to the backend is structurally
// impossible behind a TLS-terminating proxy.
type ProxyTLS struct {
	// TLSConfig applies to the client-facing listener.
	TLSConfig `json:",inline"`

	// wildcardSNI is the DNS suffix pattern tenant hostnames are matched against when SNI is
	// used as a tenant discriminator.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	WildcardSNI string `json:"wildcardSNI,omitempty"`

	// backendTLS configures the proxy's connections to PostgreSQL.
	// +optional
	BackendTLS *TLSConfig `json:"backendTLS,omitempty"`
}

// ProxyRouting configures how an incoming connection is attributed to a tenant.
type ProxyRouting struct {
	// tenantDiscriminators are the inputs consulted, in order.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:default={"SNI","StartupOptions","DatabaseName"}
	// +optional
	TenantDiscriminators []TenantDiscriminator `json:"tenantDiscriminators,omitempty"`

	// discriminatorPrecedence resolves disagreement between discriminators.
	// +kubebuilder:default=Strict
	// +optional
	DiscriminatorPrecedence DiscriminatorPrecedence `json:"discriminatorPrecedence,omitempty"`

	// startupOptionKey is the key read out of the startup packet's options string when
	// StartupOptions is a discriminator.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:default="pgelastic.tenant"
	// +optional
	StartupOptionKey string `json:"startupOptionKey,omitempty"`

	// reservedSNISubdomains cannot be claimed as tenant hostnames, so a tenant cannot
	// register a name that shadows an operator endpoint.
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:default={"admin","metrics","health"}
	// +optional
	ReservedSNISubdomains []string `json:"reservedSNISubdomains,omitempty"`

	// cancelKeyRouting selects how a CancelRequest reaches the replica holding the target
	// query. A Service load-balances the cancel connection to an arbitrary replica, so
	// without routing information in the key the cancel is silently dropped.
	// +kubebuilder:default=EmbeddedPodIdentity
	// +optional
	CancelKeyRouting CancelKeyRouting `json:"cancelKeyRouting,omitempty"`
}

// ProxyDrain configures graceful shutdown of a proxy replica.
type ProxyDrain struct {
	// mode selects what the replica waits for before exiting.
	// +kubebuilder:default=waitForClients
	// +optional
	Mode ProxyDrainMode `json:"mode,omitempty"`

	// preStopDelay is dead time before draining starts, sized to let EndpointSlice removal
	// propagate. Without it, clients keep arriving at a replica that is already draining.
	// +kubebuilder:default="20s"
	// +optional
	PreStopDelay *metav1.Duration `json:"preStopDelay,omitempty"`

	// maxSurge must stay at 0: a surging rollout runs old and new replicas at once, and each
	// one holds leased backend connections, so a surge transiently doubles backend usage.
	// +kubebuilder:default=0
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`

	// maxUnavailable bounds how much of the fleet a rollout may take down at once.
	// +kubebuilder:default=1
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// ProxyReadiness configures health reporting for a proxy replica.
type ProxyReadiness struct {
	// mode selects how readiness is decided.
	// +kubebuilder:default=adminState
	// +optional
	Mode ProxyReadinessMode `json:"mode,omitempty"`

	// enableLivenessProbe adds a liveness probe. It is off by default because a restart
	// drops every client on the replica, which is a worse outcome than almost anything a
	// liveness probe can detect.
	// +kubebuilder:default=false
	// +optional
	EnableLivenessProbe *bool `json:"enableLivenessProbe,omitempty"`
}

// ProxyService configures the client-facing Service.
type ProxyService struct {
	// type is the Service type.
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	// +optional
	Type corev1.ServiceType `json:"type,omitempty"`

	// port is the PostgreSQL wire port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=5432
	// +optional
	Port *int32 `json:"port,omitempty"`
}

// ProxyPodTemplate is the proxy pod escape hatch.
//
// It is not corev1.PodTemplateSpec: controller-gen renders an embedded metav1.ObjectMeta
// as a bare `type: object` with no properties, which puts labels and annotations outside
// the structural schema. The API server then drops them silently under the default field
// validation and rejects them under strict, so pod-template labels would be unusable.
// Declaring metadata explicitly keeps them schema-backed.
type ProxyPodTemplate struct {
	// metadata is merged into the generated proxy pod's labels and annotations. The
	// controller's own selector labels always win.
	// +optional
	Metadata *ProxyPodTemplateMetadata `json:"metadata,omitempty"`

	// spec is strategically merged over the generated proxy pod spec.
	// +optional
	Spec *corev1.PodSpec `json:"spec,omitempty"`
}

// ProxyPodTemplateMetadata carries the subset of ObjectMeta that is meaningful on a
// template the controller owns.
type ProxyPodTemplateMetadata struct {
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ProxyPodDisruptionBudget protects the fleet from voluntary disruption.
// +kubebuilder:validation:XValidation:rule="!(has(self.minAvailable) && has(self.maxUnavailable))",message="minAvailable and maxUnavailable are mutually exclusive"
type ProxyPodDisruptionBudget struct {
	// minAvailable is the count or percentage of replicas that must stay up.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`

	// maxUnavailable is the count or percentage of replicas that may be down.
	//
	// This deliberately carries no default. Defaulting runs before CEL, so a default here
	// would inject maxUnavailable into every object that set only minAvailable, and the
	// mutual-exclusion rule above would then reject it — making minAvailable unreachable.
	// The controller applies maxUnavailable=1 when neither field is set.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// ProxyMetrics configures the Prometheus endpoint.
type ProxyMetrics struct {
	// port serves /metrics.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=9127
	// +optional
	Port *int32 `json:"port,omitempty"`
}

// PoolMetering configures the utilization history that placement, rebalancing and
// autoscaling all read from.
type PoolMetering struct {
	// enabled turns sampling on. Placement and autoscaling degrade to their configured
	// stale-metric behaviour without it.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// sampleInterval is the utilization sampling period.
	// +kubebuilder:default="60s"
	// +optional
	SampleInterval *metav1.Duration `json:"sampleInterval,omitempty"`

	// retentionWindow is how much history is kept. It must cover the longest observation
	// window any policy references, or those policies never have enough evidence to act.
	// +kubebuilder:default="168h"
	// +optional
	RetentionWindow *metav1.Duration `json:"retentionWindow,omitempty"`

	// perTenantSeries records history per tenant rather than per pool. It is what makes
	// per-tenant placement and quarantine promotion possible, at a cardinality cost
	// proportional to tenant count.
	// +kubebuilder:default=true
	// +optional
	PerTenantSeries *bool `json:"perTenantSeries,omitempty"`
}

// PoolObservability configures proxy logging and metrics exposition.
type PoolObservability struct {
	// logLevel sets the proxy's log verbosity.
	// +kubebuilder:validation:Enum=Debug;Info;Warn;Error
	// +kubebuilder:default=Info
	// +optional
	LogLevel string `json:"logLevel,omitempty"`

	// logFormat selects structured or human-readable output.
	// +kubebuilder:validation:Enum=Json;Text
	// +kubebuilder:default=Json
	// +optional
	LogFormat string `json:"logFormat,omitempty"`

	// perTenantMetrics publishes the pg_stat_database counters broken down by tenant, as
	// pgelastic_metering_tenant_database_stats_total. Counters stay monotonic per tenant
	// independent of pool object lifetime, so freeing an idle pool does not read as a
	// counter reset downstream.
	//
	// It is the database counters and nothing else. The rest of the pool's exposition -
	// allocatable connections, the tenant population, the pool's staleness - are facts about
	// the pool with no per-tenant reading to give, so labelling them by tenant would multiply
	// the series count without adding a number anybody could read.
	//
	// Off by default, and the default is the whole of the decision. It costs 16 series per
	// tenant, so at the design point of ~200 tenants a pool goes from 32 series to 3,232, and
	// a Prometheus that falls over is worse than one that cannot break a number down. The
	// per-tenant figures are published where an operator looks for them anyway - on the
	// tenant's own CR - so this buys aggregation and alerting across tenants rather than
	// visibility of them, and it should be turned on deliberately by somebody who has counted
	// their tenants.
	//
	// Turning it off releases the series turning it on created, on the pool's next pass.
	// +kubebuilder:default=false
	// +optional
	PerTenantMetrics *bool `json:"perTenantMetrics,omitempty"`

	// pgBouncerCompatShow exposes a read-only PgBouncer-wire SHOW console so existing
	// exporters and dashboards work unmodified.
	// +kubebuilder:default=true
	// +optional
	PgBouncerCompatShow *bool `json:"pgBouncerCompatShow,omitempty"`

	// rewriteApplicationName stamps the tenant onto each backend checkout. Without it,
	// pg_stat_activity, pg_stat_statements and log_line_prefix attribute one tenant's queries
	// to whichever tenant held the backend before, which breaks audit trails even though no
	// data crosses the boundary.
	// +kubebuilder:default=true
	// +optional
	RewriteApplicationName *bool `json:"rewriteApplicationName,omitempty"`
}

// PgElasticPoolSpec defines the desired state of PgElasticPool.
type PgElasticPoolSpec struct {
	// classRef binds the pool to a cluster-scoped PgElasticClass. It is immutable because
	// the class fixes the capacity model, the extension allowlist and the collation
	// contract, all of which are baked into provisioned instances.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="classRef is immutable"
	// +kubebuilder:validation:XValidation:rule="self.apiGroup == 'pgelastic.io' && self.kind == 'PgElasticClass'",message="classRef must reference a pgelastic.io PgElasticClass"
	// +required
	ClassRef ClassReference `json:"classRef"`

	// capacity is the pool's budget and the denominator of every guarantee.
	// +required
	Capacity PoolCapacity `json:"capacity"`

	// instances declares the PostgreSQL instances pgelastic provisions for this pool.
	// +required
	Instances PoolInstances `json:"instances"`

	// admission governs which tenants bind and how contended capacity is handed out.
	// +optional
	Admission *PoolAdmission `json:"admission,omitempty"`

	// pooling configures backend multiplexing and reuse.
	// +optional
	Pooling *PoolingConfig `json:"pooling,omitempty"`

	// timeouts bounds every wait in the data path.
	// +optional
	Timeouts *PoolTimeouts `json:"timeouts,omitempty"`

	// placement decides where newly admitted tenants land.
	// +optional
	Placement *PoolPlacement `json:"placement,omitempty"`

	// rebalancing moves already-placed tenants to correct drift.
	// +optional
	Rebalancing *PoolRebalancing `json:"rebalancing,omitempty"`

	// migration sets pool-wide defaults for tenant moves.
	// +optional
	Migration *PoolMigration `json:"migration,omitempty"`

	// autoscaling configures the capacity planner.
	// +optional
	Autoscaling *PoolAutoscaling `json:"autoscaling,omitempty"`

	// auth configures client and backend authentication.
	// +optional
	Auth *PoolAuth `json:"auth,omitempty"`

	// proxy configures the pool's proxy fleet.
	// +optional
	Proxy *ProxySpec `json:"proxy,omitempty"`

	// metering configures the utilization history policies read from.
	// +optional
	Metering *PoolMetering `json:"metering,omitempty"`

	// observability configures proxy logging and metrics.
	// +optional
	Observability *PoolObservability `json:"observability,omitempty"`

	// paused stops all reconciliation of this pool and its members while leaving the data
	// plane serving. It is the supervised-maintenance switch, not a shutdown.
	// +kubebuilder:default=false
	// +optional
	Paused *bool `json:"paused,omitempty"`
}

// CapacityLedger is the pool's published accounting. It is the only place the derived
// pool-wide and per-instance numbers appear: users set a budget and per-tenant guarantees,
// and these figures are computed rather than configured. The per-replica arithmetic is not
// here — it is in status.proxy.leasedConnections, because it is a property of the fleet
// rather than of the budget.
type CapacityLedger struct {
	// backendConnections is the configured budget, echoed for the scale subresource.
	// +optional
	BackendConnections int32 `json:"backendConnections,omitempty"`

	// allocatable is the budget after headroom is withheld. Guarantees are admitted against
	// this number.
	// +optional
	Allocatable int32 `json:"allocatable,omitempty"`

	// reserved is the sum of guaranteed capacity over bound tenants.
	// +optional
	Reserved int32 `json:"reserved,omitempty"`

	// available is allocatable minus reserved: what remains admittable as new guarantees.
	// +optional
	Available int32 `json:"available,omitempty"`

	// committedBurst is the sum of burstable capacity over bound tenants. It is expected to
	// exceed allocatable; that excess is the product.
	// +optional
	CommittedBurst int32 `json:"committedBurst,omitempty"`

	// observedOversubscription is committedBurst divided by allocatable.
	// +kubebuilder:validation:MaxLength=16
	// +optional
	ObservedOversubscription string `json:"observedOversubscription,omitempty"`

	// inUse is the live backend connection count across every proxy replica. No single
	// replica can observe this number, which is why it is published here.
	// +optional
	InUse int32 `json:"inUse,omitempty"`

	// derivedFrom explains how the budget was computed from the member instances. Capacity
	// is derived, never invented, and the arithmetic has to be auditable from kubectl.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	DerivedFrom string `json:"derivedFrom,omitempty"`

	// maxTenantsAtCurrentReservations is how many more tenants of the pool's default
	// workload class could still be admitted. A pool where every tenant sets a safety floor
	// performs worse than one with all floors at zero, and this is the number that makes
	// that visible before it happens.
	// +optional
	MaxTenantsAtCurrentReservations int32 `json:"maxTenantsAtCurrentReservations,omitempty"`
}

// PoolInstanceStatus is one member instance's contribution to the ledger. This breakdown
// is mandatory rather than decorative: pool accounting is not additive across instances,
// because a tenant's guarantee is only satisfiable on the instance it is bound to.
type PoolInstanceStatus struct {
	// name of the PgInstance.
	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// allocatable connections on this instance.
	// +optional
	Allocatable int32 `json:"allocatable,omitempty"`

	// reserved is the sum of guarantees for tenants bound to this instance.
	// +optional
	Reserved int32 `json:"reserved,omitempty"`

	// inUse is the live backend connection count on this instance.
	// +optional
	InUse int32 `json:"inUse,omitempty"`

	// tenants bound to this instance.
	// +optional
	Tenants int32 `json:"tenants,omitempty"`

	// role of the instance's current primary member.
	// +optional
	Role InstanceRole `json:"role,omitempty"`

	// ready reports whether the instance can accept tenant traffic. An instance re-cloning a
	// replica reports not ready, and its headroom must not be counted as available.
	// +optional
	Ready bool `json:"ready,omitempty"`
}

// ProxyStatus reports the observed state of the proxy fleet.
type ProxyStatus struct {
	// replicas is the desired proxy pod count.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ready is the number of proxy pods serving.
	// +optional
	Ready int32 `json:"ready,omitempty"`

	// configVersion identifies the configuration the fleet has converged on, so a partial
	// rollout is distinguishable from a converged one.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	ConfigVersion string `json:"configVersion,omitempty"`

	// leasedConnections is the total backend-connection credit the fleet holds: replicas
	// multiplied by the per-replica budget. It is the failure mode no single-process pooler
	// can see, and until per-replica leases exist it is a static grant rather than a lease —
	// which is why it can exceed allocatable, and why admission bounds the worst case
	// instead.
	// +optional
	LeasedConnections int32 `json:"leasedConnections,omitempty"`
}

// QoSClassCounts breaks a tenant population down by derived QoS class.
type QoSClassCounts struct {
	// guaranteed counts tenants whose guaranteed equals their burstable.
	// +optional
	Guaranteed int32 `json:"guaranteed,omitempty"`

	// burstable counts tenants with a non-zero guarantee below their burstable.
	// +optional
	Burstable int32 `json:"burstable,omitempty"`

	// bestEffort counts tenants with no guarantee.
	// +optional
	BestEffort int32 `json:"bestEffort,omitempty"`
}

// PoolTenantCounts summarises the pool's tenant population.
type PoolTenantCounts struct {
	// total tenants referencing this pool.
	// +optional
	Total int32 `json:"total,omitempty"`

	// bound tenants, meaning placed on an instance with a database provisioned.
	// +optional
	Bound int32 `json:"bound,omitempty"`

	// pending tenants awaiting placement.
	// +optional
	Pending int32 `json:"pending,omitempty"`

	// quarantined tenants still inside their observation window.
	// +optional
	Quarantined int32 `json:"quarantined,omitempty"`

	// throttledLast24h counts tenants that hit any admission limit in the last day.
	// +optional
	ThrottledLast24h int32 `json:"throttledLast24h,omitempty"`

	// byQosClass breaks the population down by derived QoS class.
	// +optional
	ByQosClass *QoSClassCounts `json:"byQosClass,omitempty"`
}

// AutoscalingPlan is the whole capacity plan, computed every reconcile and published
// whether or not any of it will be executed.
//
// Publishing the plan in Recommend mode is the point of Recommend mode: an operator can read
// exactly what would have happened, argue with it, and opt one action class into Auto once
// the plan has been boring for a while. A plan that only existed when it was about to be
// executed would give nobody that evidence.
type AutoscalingPlan struct {
	// mode is the mode the plan was computed under.
	// +optional
	Mode AutoscalingMode `json:"mode,omitempty"`

	// computedAt is when the plan was last recomputed.
	// +optional
	ComputedAt *metav1.Time `json:"computedAt,omitempty"`

	// metricsStale reports that the readings the plan was computed from are older than the
	// staleness threshold. Every action is refused while it is true.
	// +optional
	MetricsStale bool `json:"metricsStale,omitempty"`

	// observedInstances is how many instances the pool has now.
	// +optional
	ObservedInstances int32 `json:"observedInstances,omitempty"`

	// recommendedInstances is how many it should have.
	// +optional
	RecommendedInstances int32 `json:"recommendedInstances,omitempty"`

	// measuredInstances is how many of them the utilization was read from. It is lower than
	// observedInstances whenever a member is cordoned, draining or recovering, and zero of it
	// means the pool's load could not be read at all rather than that there was none.
	// +optional
	MeasuredInstances int32 `json:"measuredInstances,omitempty"`

	// observedUtilizationPercent is connections in use over allocatable, across the instances
	// a tenant could actually be placed on. Capacity nothing may be scheduled onto is left out
	// of both halves of the ratio, so a cordoned member neither flatters nor deflates it.
	// +optional
	ObservedUtilizationPercent int32 `json:"observedUtilizationPercent,omitempty"`

	// targetUtilizationPercent is what the planner steers towards.
	// +optional
	TargetUtilizationPercent int32 `json:"targetUtilizationPercent,omitempty"`

	// summary is a one-line human-readable explanation of the plan.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Summary string `json:"summary,omitempty"`

	// perInstance is the per-instance target the plan would converge on.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	// +optional
	PerInstance []InstanceTarget `json:"perInstance,omitempty"`

	// moves is the tenant movement the plan implies. Eviction and destination appear here
	// together because they were decided together: a plan that said which tenant to evict
	// without saying where it lands is not a plan, it is half of one.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=128
	// +optional
	Moves []PlannedMove `json:"moves,omitempty"`

	// actions is every change class the plan proposes, each carrying whether it is permitted
	// to execute and, when it is not, which guardrail refused it.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=6
	// +optional
	Actions []PlannedAction `json:"actions,omitempty"`
}

// InstanceTarget is what one instance should look like once the plan is applied.
type InstanceTarget struct {
	// name of the PgInstance.
	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// utilizationPercent is the instance's connections in use over allocatable.
	// +optional
	UtilizationPercent int32 `json:"utilizationPercent,omitempty"`

	// packedConnections is the sum, over the tenants the plan puts here, of each tenant's
	// guarantee or trailing-window percentile, whichever is larger.
	// +optional
	PackedConnections int32 `json:"packedConnections,omitempty"`

	// allocatableConnections is what the instance can hold.
	// +optional
	AllocatableConnections int32 `json:"allocatableConnections,omitempty"`

	// tenants is how many tenants the plan puts here.
	// +optional
	Tenants int32 `json:"tenants,omitempty"`

	// storageUsedPercent is the data volume's used-to-allocated ratio.
	// +optional
	StorageUsedPercent int32 `json:"storageUsedPercent,omitempty"`

	// recommendedStorage is the data volume size the plan would expand to. It is absent
	// when no expansion is warranted, and never smaller than the current size: PVCs cannot
	// shrink.
	// +optional
	RecommendedStorage *resource.Quantity `json:"recommendedStorage,omitempty"`

	// consolidatable reports that every tenant here could be rehomed onto the rest of the
	// pool, which is the precondition for scale-in.
	// +optional
	Consolidatable bool `json:"consolidatable,omitempty"`

	// consolidatableSince is when this instance first became consolidatable, and the instant
	// the dwell time is measured from. It lives in status rather than in the controller's
	// memory so that an operator restart does not silently reset a day-long timer.
	// +optional
	ConsolidatableSince *metav1.Time `json:"consolidatableSince,omitempty"`
}

// PlannedMove is one tenant relocation the plan implies.
type PlannedMove struct {
	// name is the PgTenant being moved.
	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// from is the instance it leaves.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	From string `json:"from,omitempty"`

	// to is the instance it lands on.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	To string `json:"to,omitempty"`

	// expectedImprovementPercent is how many percentage points of the source instance's
	// utilization this move relieves. It is the number that justifies spending a live
	// migration, and a move that cannot state one should not be made.
	// +optional
	ExpectedImprovementPercent int32 `json:"expectedImprovementPercent,omitempty"`

	// reason names why the move is proposed.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Reason string `json:"reason,omitempty"`

	// eligible reports whether the tenant may actually be moved: cold enough, permitted by
	// its workload class, and not sitting on a source that is too busy to decode for it.
	// +optional
	Eligible bool `json:"eligible,omitempty"`

	// blockedBy names the check that makes an ineligible move ineligible.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	BlockedBy string `json:"blockedBy,omitempty"`
}

// PlannedAction is one change class the plan proposes.
type PlannedAction struct {
	// name is the action class. It is the list map key, so a plan carries at most one entry
	// per class and a reader can address the one it cares about.
	// +required
	Name AutoAction `json:"name"`

	// target names the object the action would change.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	Target string `json:"target,omitempty"`

	// detail spells out the change in the terms it would be applied in.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Detail string `json:"detail,omitempty"`

	// permitted reports whether every guardrail allows this action to execute now.
	// +optional
	Permitted bool `json:"permitted,omitempty"`

	// reason names the guardrail that refused, or records that none did.
	// +kubebuilder:validation:MaxLength=64
	// +optional
	Reason string `json:"reason,omitempty"`

	// message explains the reason in the numbers it was decided on.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Message string `json:"message,omitempty"`

	// executedAt is when this action class was last actually applied. An action that is
	// permitted but never executed and one that executes every reconcile look identical
	// without it.
	// +optional
	ExecutedAt *metav1.Time `json:"executedAt,omitempty"`
}

// PgElasticPoolStatus defines the observed state of PgElasticPool.
type PgElasticPoolStatus struct {
	// observedGeneration is the spec generation the rest of this status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase is a display-only summary for kubectl get. It is a pure function of the
	// conditions; automation must read the conditions instead.
	// +kubebuilder:default=Pending
	// +optional
	Phase PoolPhase `json:"phase,omitempty"`

	// selector is the label selector matching the proxy pods, in the form required by the
	// scale subresource so that HPA and KEDA can target this object with no new API.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Selector string `json:"selector,omitempty"`

	// capacity is the reservation ledger.
	// +optional
	Capacity *CapacityLedger `json:"capacity,omitempty"`

	// perInstance is the ledger broken down by member instance.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	// +optional
	PerInstance []PoolInstanceStatus `json:"perInstance,omitempty"`

	// proxy is the observed state of the proxy fleet.
	// +optional
	Proxy *ProxyStatus `json:"proxy,omitempty"`

	// tenants summarises the pool's tenant population.
	// +optional
	Tenants *PoolTenantCounts `json:"tenants,omitempty"`

	// autoscaling is the whole capacity plan, published in every mode and executed only in
	// the classes the pool has opted into.
	// +optional
	Autoscaling *AutoscalingPlan `json:"autoscaling,omitempty"`

	// conditions represent the current state of the PgElasticPool resource.
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.capacity.backendConnections,statuspath=.status.capacity.backendConnections,selectorpath=.status.selector
// +kubebuilder:resource:categories=pgelastic,shortName=pgpool
// +kubebuilder:printcolumn:name="Capacity",type=integer,JSONPath=`.spec.capacity.backendConnections`
// +kubebuilder:printcolumn:name="Allocatable",type=integer,JSONPath=`.status.capacity.allocatable`
// +kubebuilder:printcolumn:name="Reserved",type=integer,JSONPath=`.status.capacity.reserved`
// +kubebuilder:printcolumn:name="Available",type=integer,JSONPath=`.status.capacity.available`
// +kubebuilder:printcolumn:name="InUse",type=integer,JSONPath=`.status.capacity.inUse`
// +kubebuilder:printcolumn:name="Tenants",type=integer,JSONPath=`.status.tenants.total`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PgElasticPool is the resource boundary: one capacity budget, one reservation ledger and
// one proxy fleet, shared by many tenant databases across several PostgreSQL instances.
type PgElasticPool struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of PgElasticPool
	// +required
	Spec PgElasticPoolSpec `json:"spec"`

	// status defines the observed state of PgElasticPool
	// +optional
	Status PgElasticPoolStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// PgElasticPoolList contains a list of PgElasticPool
type PgElasticPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []PgElasticPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &PgElasticPool{}, &PgElasticPoolList{})
		return nil
	})
}
