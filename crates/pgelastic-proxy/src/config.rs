//! File-backed configuration.
//!
//! The field names deliberately track `PgElasticPool`'s `spec.proxy` rather than
//! inventing a second vocabulary, because the operator renders this document
//! from that spec.
//!
//! TOML rather than YAML: `serde_yaml` is archived and unmaintained.
//!
//! The document is split in two by [`Config::structural`]. The structural half —
//! listen address, TLS material, the instance list, worker counts — can only be
//! applied by a process restart, so a change to it rolls the fleet. The dynamic
//! half — the tenant routing table and the per-tenant capacity claims — is
//! re-read at run time by [`reload`](crate::reload) and applied at a checkout
//! boundary. Rendering both halves into one document and hashing only the
//! structural half is what keeps adding a tenant from restarting every proxy
//! replica and dropping every client on it.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{ProxyError, Result};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    pub listen: ListenConfig,
    pub backend: BackendConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub drain: DrainConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub routing: RoutingConfig,
    #[serde(default)]
    pub pool: PoolConfig,
    #[serde(default)]
    pub fence: FenceConfig,
    #[serde(default)]
    pub stall: StallConfig,
    #[serde(default)]
    pub control: ControlConfig,
    #[serde(default)]
    pub reload: ReloadConfig,
    /// Identifies this document.
    ///
    /// Rendered by the operator and reported back once applied, so a fleet
    /// half-way through picking up a change is distinguishable from one that has
    /// converged on it. Empty means nobody is tracking versions.
    #[serde(default)]
    pub config_version: String,
    /// The instances this proxy fronts.
    ///
    /// Empty means one implicit instance named [`DEFAULT_INSTANCE`] at
    /// `backend.address`, which is the whole of the single-instance
    /// configuration and keeps it meaning exactly what it did.
    #[serde(default)]
    pub instances: Vec<InstanceConfig>,
}

impl std::str::FromStr for Config {
    type Err = ProxyError;

    fn from_str(source: &str) -> Result<Self> {
        let config: Self = toml::from_str(source).map_err(|e| ProxyError::config(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path)
            .map_err(|e| ProxyError::config(format!("reading {}: {e}", path.display())))?;
        source.parse()
    }

    fn validate(&self) -> Result<()> {
        if let Some(tls) = &self.listen.tls
            && (!tls.certificate_file.exists() || !tls.key_file.exists())
        {
            return Err(ProxyError::config(
                "listen.tls certificateFile and keyFile must both exist",
            ));
        }
        if self.listen.tls.is_none() && self.listen.require_tls {
            return Err(ProxyError::config(
                "listen.requireTls is set but no listen.tls material is configured",
            ));
        }
        if self.backend.tls.mode == BackendTlsMode::VerifyFull && self.backend.tls.ca_file.is_none()
        {
            return Err(ProxyError::config(
                "backend.tls.mode = VerifyFull requires a caFile",
            ));
        }
        if self.limits.inline_frame_bytes > self.limits.max_frame_bytes {
            return Err(ProxyError::config(
                "limits.inlineFrameBytes must not exceed limits.maxFrameBytes",
            ));
        }
        for user in &self.auth.users {
            if user.verifier.is_none() && user.password.is_none() {
                return Err(ProxyError::config(format!(
                    "auth user {:?} has neither a verifier nor a password",
                    user.name
                )));
            }
        }
        self.fence
            .lease
            .validate()
            .map_err(|e| ProxyError::config(format!("fence.lease: {e}")))?;
        if self.fence.require_epoch && !self.fence.verify_at_checkout {
            return Err(ProxyError::config(
                "fence.requireEpoch needs fence.verifyAtCheckout: the epoch cannot be \
                 required from a connection nobody asks",
            ));
        }
        self.validate_fleet()?;
        if self.control.max_lease_ttl_ms < self.control.default_lease_ttl_ms {
            return Err(ProxyError::config(
                "control.maxLeaseTtlMs must not be below control.defaultLeaseTtlMs",
            ));
        }
        if self.control.default_lease_ttl_ms == 0 {
            return Err(ProxyError::config(
                "control.defaultLeaseTtlMs must be non-zero: a lease that has already \
                 expired would quiesce a tenant nobody can resume",
            ));
        }
        self.validate_control()?;
        if self.routing.tenant_discriminators.is_empty() {
            return Err(ProxyError::config(
                "routing.tenantDiscriminators must name at least one input: a connection \
                 whose tenant cannot be established is one that would be served from \
                 somebody else's budget",
            ));
        }
        if self.stall.interval_ms == 0 || self.stall.confirmations == 0 {
            return Err(ProxyError::config(
                "stall.intervalMs and stall.confirmations must both be non-zero",
            ));
        }
        Ok(())
    }

    /// Refuses a control listener nobody has to authenticate to.
    ///
    /// The check is here rather than at the listener because a proxy that binds
    /// the port and only then discovers it cannot verify anyone has already
    /// exposed every tenant's gate to whoever reaches the pod.
    fn validate_control(&self) -> Result<()> {
        let Some(address) = &self.control.address else {
            return Ok(());
        };
        let Some(tls) = &self.control.tls else {
            return Err(ProxyError::config(format!(
                "control.address {address:?} needs control.tls: an unauthenticated caller \
                 can quiesce any tenant, which holds its clients' sockets open with nothing \
                 behind them"
            )));
        };
        for (field, path) in [
            ("certificateFile", &tls.certificate_file),
            ("keyFile", &tls.key_file),
            ("clientCaFile", &tls.client_ca_file),
        ] {
            if !path.exists() {
                return Err(ProxyError::config(format!(
                    "control.tls.{field} {} does not exist",
                    path.display()
                )));
            }
        }
        if tls.client_name.is_empty() {
            return Err(ProxyError::config(
                "control.tls.clientName must name the caller the listener will accept",
            ));
        }
        Ok(())
    }

    fn validate_fleet(&self) -> Result<()> {
        let mut seen = std::collections::BTreeSet::new();
        for instance in &self.instances {
            if instance.name.is_empty() {
                return Err(ProxyError::config("an instance must have a name"));
            }
            if !seen.insert(instance.name.as_str()) {
                return Err(ProxyError::config(format!(
                    "instance {:?} is declared twice",
                    instance.name
                )));
            }
        }
        let known = |name: &str| {
            if self.instances.is_empty() {
                name == DEFAULT_INSTANCE
            } else {
                seen.contains(name)
            }
        };
        if let Some(default) = &self.routing.default_instance
            && !known(default)
        {
            return Err(ProxyError::config(format!(
                "routing.defaultInstance {default:?} is not a declared instance"
            )));
        }
        for (tenant, instance) in &self.routing.tenants {
            if !known(instance) {
                return Err(ProxyError::config(format!(
                    "tenant {tenant:?} is routed to {instance:?}, which is not a declared instance"
                )));
            }
        }
        Ok(())
    }

    /// This document with everything a running process can adopt stripped out.
    ///
    /// Two configurations with equal structural halves describe the same
    /// process, so the operator hashes this rather than the whole document into
    /// the pod template: a new tenant then changes the file every replica reads
    /// without changing the pod every replica runs in.
    #[must_use]
    pub fn structural(&self) -> Self {
        let mut structural = self.clone();
        structural.config_version = String::new();
        structural.routing.tenants.clear();
        structural.pool.tenants.clear();
        structural
    }

    /// Whether `other` can be adopted without a restart.
    pub fn is_dynamic_change(&self, other: &Self) -> bool {
        self.structural() == other.structural()
    }
}

/// The name the single-instance configuration's implicit instance carries.
pub const DEFAULT_INSTANCE: &str = "default";

/// One `PgInstance` behind this proxy.
///
/// An instance is a capacity boundary as well as an address: its
/// `backendConnections` is derived from *its own* `max_connections`, so two
/// instances never draw on one budget and a tenant that saturates one cannot
/// reach the other's.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstanceConfig {
    pub name: String,
    pub address: String,
    /// Overrides `pool.backendConnections` for this instance.
    #[serde(default)]
    pub backend_connections: Option<u32>,
    /// Overrides `backend.user` for this instance.
    ///
    /// Each `PgInstance` issues its own role passwords at bootstrap, so a fleet
    /// spanning two instances holds two different credentials for the same role
    /// name. One shared `backend.password` would authenticate against exactly
    /// one of them.
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// Overrides `backend.database` for this instance.
    #[serde(default)]
    pub database: Option<String>,
}

impl InstanceConfig {
    /// This instance's backend leg: the pool-wide `backend` settings with
    /// whatever this instance overrides applied.
    pub fn backend(&self, base: &BackendConfig) -> BackendConfig {
        let mut backend = base.clone();
        backend.address.clone_from(&self.address);
        if let Some(user) = &self.user {
            backend.user.clone_from(user);
        }
        if self.user.is_some() || self.password.is_some() {
            backend.password.clone_from(&self.password);
        }
        if self.database.is_some() {
            backend.database.clone_from(&self.database);
        }
        backend
    }
}

/// Where the run-time half of the configuration is re-read from.
///
/// A `get` on one named object rather than a list-watch on the namespace: RBAC
/// can restrict `get` to a single `resourceName` and cannot restrict a watch at
/// all, so polling one Secret is the form of this that does not hand the data
/// plane read access to every Secret its namespace holds.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReloadConfig {
    /// Omitted means the configuration is whatever was read at start-up and
    /// nothing re-reads it.
    #[serde(default)]
    pub secret: Option<SecretSource>,
    #[serde(default = "default_reload_interval_ms")]
    pub interval_ms: u64,
    /// Publish the applied `configVersion` as an annotation on this replica's
    /// own Pod, which is how the operator tells a converged fleet from one
    /// half-way through picking a change up. Needs `PGELASTIC_POD_NAME`.
    #[serde(default)]
    pub report_to_pod: bool,
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            secret: None,
            interval_ms: default_reload_interval_ms(),
            report_to_pod: false,
        }
    }
}

impl ReloadConfig {
    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms.max(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretSource {
    pub namespace: String,
    pub name: String,
    /// The Secret key holding this same TOML document.
    pub key: String,
}

/// Proactive write-stall detection.
///
/// `dataDurability: Required` means a commit *stalls* when quorum is lost
/// rather than degrading to asynchronous replication. That is the correct
/// behaviour and it is invisible: the backend parks in `IPC.SyncRep` and the
/// proxy sees a connection that is merely busy. Detecting it is what keeps one
/// instance's quorum loss from consuming every pooled backend and cascading
/// into tenants that are not on it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StallConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// How often each instance is sampled.
    #[serde(default = "default_stall_interval_ms")]
    pub interval_ms: u64,
    /// Consecutive samples agreeing before the verdict changes.
    ///
    /// One sample is enough to be *right*; two is what stops a standby
    /// reconnecting inside one interval from flapping every tenant on the
    /// instance through a refusal.
    #[serde(default = "default_stall_confirmations")]
    pub confirmations: u32,
    /// Refuse a checkout onto a stalled instance instead of queueing it.
    #[serde(default = "default_true")]
    pub fail_fast: bool,
}

impl Default for StallConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_ms: default_stall_interval_ms(),
            confirmations: default_stall_confirmations(),
            fail_fast: true,
        }
    }
}

impl StallConfig {
    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }
}

/// The lease-bound control API: quiesce, drain, route, resume, unquiesce.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlConfig {
    /// Omitted means no control listener at all, and therefore no way to
    /// quiesce a tenant.
    #[serde(default)]
    pub address: Option<String>,
    /// The mutual TLS a caller must pass, which is required whenever
    /// [`address`](Self::address) is set.
    #[serde(default)]
    pub tls: Option<ControlTlsConfig>,
    #[serde(default = "default_lease_ttl_ms")]
    pub default_lease_ttl_ms: u64,
    /// The longest lease the API will grant.
    ///
    /// A quiesced tenant is holding client sockets open with nothing behind
    /// them, so the ceiling on how long a dead operator can do that is a
    /// configuration decision rather than the caller's.
    #[serde(default = "default_max_lease_ttl_ms")]
    pub max_lease_ttl_ms: u64,
}

/// The client certificate the control API authenticates a caller by.
///
/// Mandatory whenever the listener is bound at all. Quiescing a tenant holds
/// its client sockets open with nothing behind them, so an unauthenticated
/// caller can stall any tenant at will — there is no posture in which serving
/// these endpoints to whoever connects is acceptable, and the configuration is
/// shaped so that one cannot be expressed.
///
/// A bearer token in this same document was rejected: the document is a Secret
/// mounted into the pod, so a token in it protects nothing the pod does not
/// already hold.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlTlsConfig {
    /// The listener's own certificate and key.
    pub certificate_file: PathBuf,
    pub key_file: PathBuf,
    /// The CA a caller's client certificate must chain to.
    pub client_ca_file: PathBuf,
    /// The DNS name that certificate must carry.
    ///
    /// Checked against the subject alternative names rather than the common
    /// name: CN-as-identity has been deprecated since RFC 2818 was replaced,
    /// and cert-manager issues a `Certificate`'s `dnsNames` as SANs.
    pub client_name: String,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            address: None,
            tls: None,
            default_lease_ttl_ms: default_lease_ttl_ms(),
            max_lease_ttl_ms: default_max_lease_ttl_ms(),
        }
    }
}

impl ControlConfig {
    pub fn default_lease_ttl(&self) -> Duration {
        Duration::from_millis(self.default_lease_ttl_ms)
    }

    pub fn max_lease_ttl(&self) -> Duration {
        Duration::from_millis(self.max_lease_ttl_ms)
    }
}

/// The primary-epoch fence.
///
/// The lease parameters and the fence's reaction deadline are one decision and
/// live in one struct — see [`FenceTiming`](crate::epoch::FenceTiming).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FenceConfig {
    #[serde(default)]
    pub lease: crate::epoch::FenceTiming,
    /// Read the epoch off every backend connection at checkout.
    ///
    /// This is the pull/verify path, and it is the only one that survives a
    /// partition. Turning it off leaves the fence depending on reachability,
    /// which is exactly the assumption the fence exists to remove.
    #[serde(default = "default_true")]
    pub verify_at_checkout: bool,
    /// Refuse a checkout whose backend carries no `pgelastic.primary_epoch`.
    ///
    /// Off by default so the proxy can front a `PostgreSQL` that pgelastic did
    /// not provision. On, a backend that cannot prove which epoch it is serving
    /// is not handed to a client — a stalled tenant is recoverable, a write to
    /// a demoted primary is not.
    #[serde(default)]
    pub require_epoch: bool,
    /// Where the durable in-doubt log is kept. Omitted keeps it in memory, so
    /// it does not survive a restart.
    #[serde(default)]
    pub in_doubt_log: Option<PathBuf>,
    /// Listen address for the push endpoint the promoting agent calls. Omitted
    /// means no push path.
    #[serde(default)]
    pub push_address: Option<String>,
    /// The `PgInstance` whose `status.primaryEpoch` is watched. Omitted means
    /// no watch path.
    #[serde(default)]
    pub watch: Option<FenceWatchConfig>,
}

impl Default for FenceConfig {
    fn default() -> Self {
        Self {
            lease: crate::epoch::FenceTiming::default(),
            verify_at_checkout: true,
            require_epoch: false,
            in_doubt_log: None,
            push_address: None,
            watch: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FenceWatchConfig {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListenConfig {
    pub address: String,
    #[serde(default = "default_max_client_connections")]
    pub max_client_connections: usize,
    /// Refuse any client that reaches the startup packet without having
    /// negotiated TLS first.
    #[serde(default)]
    pub require_tls: bool,
    #[serde(default)]
    pub tls: Option<ServerTlsConfig>,
    #[serde(default = "default_client_login_seconds")]
    pub client_login_seconds: u64,
}

impl ListenConfig {
    pub fn client_login_timeout(&self) -> Duration {
        Duration::from_secs(self.client_login_seconds)
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServerTlsConfig {
    pub certificate_file: PathBuf,
    pub key_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackendConfig {
    pub address: String,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default = "default_connect_seconds")]
    pub connect_seconds: u64,
    #[serde(default)]
    pub tls: BackendTlsConfig,
}

impl BackendConfig {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_seconds)
    }
}

/// The relevant rungs of libpq's `sslmode` ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
pub enum BackendTlsMode {
    /// No TLS at all.
    #[default]
    Disable,
    /// Encrypt, but accept any certificate. Protects against passive capture
    /// only; documented as such because it is not authentication.
    Require,
    /// Verify the chain against `caFile` and the name against `serverName`.
    VerifyFull,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackendTlsConfig {
    #[serde(default)]
    pub mode: BackendTlsMode,
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    /// Name to verify the backend certificate against. Defaults to the host
    /// part of `backend.address`.
    #[serde(default)]
    pub server_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default = "default_scram_iterations")]
    pub scram_iterations: u32,
    #[serde(default)]
    pub users: Vec<UserConfig>,
    /// Concurrent PBKDF2 jobs. Doubles as the authentication rate limiter.
    #[serde(default = "default_kdf_concurrency")]
    pub kdf_concurrency: usize,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            scram_iterations: default_scram_iterations(),
            users: Vec::new(),
            kdf_concurrency: default_kdf_concurrency(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UserConfig {
    pub name: String,
    /// The tenant this login belongs to, and the only one it may reach.
    ///
    /// Authenticating and choosing a tenant are two separate acts on two
    /// separate inputs: the password proves who the client is, the
    /// discriminators say which tenant it asked for. Without this field nothing
    /// relates them, and a client holding one tenant's password can name
    /// another tenant's database in the same startup packet and be routed
    /// there. Empty means the login is not bound to a tenant, which only the
    /// single-tenant development shape should ever be.
    #[serde(default)]
    pub tenant: String,
    /// A `PostgreSQL` `rolpassword` SCRAM secret. Preferred: the proxy then
    /// never holds a password it could replay.
    #[serde(default)]
    pub verifier: Option<String>,
    /// A cleartext password, hashed into a verifier at start-up. Development
    /// affordance only.
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DrainConfig {
    /// How long a graceful drain waits for sessions to reach an idle boundary
    /// before closing them anyway.
    #[serde(default = "default_shutdown_seconds")]
    pub shutdown_seconds: u64,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            shutdown_seconds: default_shutdown_seconds(),
        }
    }
}

impl DrainConfig {
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_seconds)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetricsConfig {
    /// Omitted means no metrics listener at all.
    #[serde(default)]
    pub address: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LimitsConfig {
    /// Largest frame buffered whole on the passthrough path. Anything bigger
    /// is streamed, so this is the per-direction memory bound.
    #[serde(default = "default_inline_frame_bytes")]
    pub inline_frame_bytes: usize,
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            inline_frame_bytes: default_inline_frame_bytes(),
            max_frame_bytes: default_max_frame_bytes(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RoutingConfig {
    /// Identity of this replica, embedded in every cancel key it mints so a
    /// `CancelRequest` that kube-proxy lands on the wrong pod can still be
    /// routed. Reserved now because widening a cancel key later is a wire
    /// break.
    #[serde(default)]
    pub cancel_routing_id: u16,
    /// Where a tenant with no entry in [`tenants`](Self::tenants) is sent.
    /// Omitted means the first declared instance.
    #[serde(default)]
    pub default_instance: Option<String>,
    /// The tenant routing table's starting state. `setRoute` rewrites it at
    /// run time; this is only where it begins.
    #[serde(default)]
    pub tenants: std::collections::BTreeMap<String, String>,
    /// The inputs a new connection's tenant is read out of, consulted in order.
    ///
    /// The default is the login role alone, which is the single-tenant
    /// development shape. A pool fronting many tenants behind one Service is
    /// rendered by the operator with `DatabaseName` in the list, because that is
    /// the only discriminator every `PostgreSQL` client already sends.
    #[serde(default = "default_discriminators")]
    pub tenant_discriminators: Vec<TenantDiscriminator>,
    /// The key read out of the startup packet's `options` when
    /// [`TenantDiscriminator::StartupOptions`] is consulted.
    #[serde(default = "default_startup_option_key")]
    pub startup_option_key: String,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            cancel_routing_id: 0,
            default_instance: None,
            tenants: std::collections::BTreeMap::new(),
            tenant_discriminators: default_discriminators(),
            startup_option_key: default_startup_option_key(),
        }
    }
}

/// One input a connection's tenant can be read out of.
///
/// The spelling matches `PgElasticPool`'s `spec.proxy.routing.tenantDiscriminators`
/// so the operator renders the list it was given rather than translating it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum TenantDiscriminator {
    /// Not implemented. Accepted so that a pool rendering the API's default list
    /// starts rather than failing to parse its own configuration; it contributes
    /// no candidate, and the discriminators after it decide.
    #[serde(rename = "SNI")]
    Sni,
    /// `-c pgelastic.tenant=<name>` in the startup packet's `options`.
    StartupOptions,
    /// The database the client asked for. Every client sends one.
    DatabaseName,
    /// The login role. The single-tenant default.
    Role,
}

/// Pooling, capacity and per-tenant claims.
///
/// `poolSize` is deliberately absent and always will be. A held backend
/// connection is one unit of work-in-progress, so the pool's capacity unit and
/// `PgBouncer`'s `pool_size` are the same number; stacking two limiters is how
/// a ceiling becomes unexplainable. Operators set `backendConnections` and
/// per-tenant guaranteed/burstable, and everything else is derived.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PoolConfig {
    #[serde(default)]
    pub mode: PoolModeConfig,
    /// The pool's whole capacity envelope, derived by the operator from
    /// `max_connections − superuser_reserved_connections − replication slots −
    /// overhead`. Never invented.
    #[serde(default = "default_backend_connections")]
    pub backend_connections: u32,
    #[serde(default)]
    pub headroom_percent: u8,
    /// Client sockets the pool will hold. A second, independent currency: it is
    /// bounded by file descriptors rather than by `max_connections`, which is
    /// why it is not derived from `backendConnections`.
    #[serde(default = "default_pool_max_client_connections")]
    pub max_client_connections: u32,
    #[serde(default)]
    pub reset_policy: ResetPolicyConfig,
    /// How long a client may wait in the admission queue before it is refused
    /// with `PGE1024`.
    #[serde(default = "default_query_wait_seconds")]
    pub query_wait_seconds: u64,
    /// When a queued client is sent a `NoticeResponse` telling it why it is
    /// still waiting.
    #[serde(default = "default_notify_after_seconds")]
    pub notify_after_seconds: u64,
    #[serde(default = "default_queue_depth_per_tenant")]
    pub queue_depth_per_tenant: u32,
    /// Prepared statements kept parsed on one backend link before the LRU
    /// evicts. `PostgreSQL` has no server-side limit, so this is the proxy's.
    #[serde(default = "default_max_server_statements")]
    pub max_server_statements: usize,
    /// How long a backend link may live before it is recycled, spread over a
    /// jittered window so a pool does not recycle every link at once.
    #[serde(default = "default_server_lifetime_seconds")]
    pub server_lifetime_seconds: u64,
    /// `serverLoginRetry`: how long a pool whose last backend connect failed
    /// fast-fails arriving clients against the cached error before it lets one
    /// through to try again.
    #[serde(default = "default_server_login_retry_seconds")]
    pub server_login_retry_seconds: u64,
    #[serde(default)]
    pub tenants: Vec<TenantConfig>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            mode: PoolModeConfig::default(),
            backend_connections: default_backend_connections(),
            headroom_percent: 0,
            max_client_connections: default_pool_max_client_connections(),
            reset_policy: ResetPolicyConfig::default(),
            query_wait_seconds: default_query_wait_seconds(),
            notify_after_seconds: default_notify_after_seconds(),
            queue_depth_per_tenant: default_queue_depth_per_tenant(),
            max_server_statements: default_max_server_statements(),
            server_lifetime_seconds: default_server_lifetime_seconds(),
            server_login_retry_seconds: default_server_login_retry_seconds(),
            tenants: Vec::new(),
        }
    }
}

impl PoolConfig {
    pub fn query_wait_timeout(&self) -> Duration {
        Duration::from_secs(self.query_wait_seconds)
    }

    pub fn notify_after(&self) -> Duration {
        Duration::from_secs(self.notify_after_seconds)
    }

    pub fn server_lifetime(&self) -> Duration {
        Duration::from_secs(self.server_lifetime_seconds)
    }

    pub fn server_login_retry(&self) -> Duration {
        Duration::from_secs(self.server_login_retry_seconds)
    }
}

/// A tenant's effective claim, as the controller would have merged it from the
/// workload class and the `PgTenant`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TenantConfig {
    pub name: String,
    #[serde(default)]
    pub guaranteed: u32,
    pub burstable: u32,
    #[serde(default = "default_weight")]
    pub weight: u32,
    #[serde(default = "default_priority")]
    pub priority: u32,
    #[serde(default = "default_tenant_max_client_connections")]
    pub max_client_connections: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PoolModeConfig {
    /// One backend per client for the client's whole life.
    #[default]
    Session,
    /// A backend is released at every `ReadyForQuery` carrying `'I'`.
    Transaction,
}

impl PoolModeConfig {
    pub fn is_transaction(self) -> bool {
        matches!(self, Self::Transaction)
    }
}

impl From<PoolModeConfig> for pgelastic_pool::PoolMode {
    fn from(mode: PoolModeConfig) -> Self {
        match mode {
            PoolModeConfig::Session => Self::Session,
            PoolModeConfig::Transaction => Self::Transaction,
        }
    }
}

impl From<PoolModeConfig> for pgelastic_capacity::PoolMode {
    fn from(mode: PoolModeConfig) -> Self {
        match mode {
            PoolModeConfig::Session => Self::Session,
            PoolModeConfig::Transaction => Self::Transaction,
        }
    }
}

/// How hard a link is scrubbed before it may serve a different client.
///
/// The default is `discardAll` rather than `dirtyTracked`, and the reason is
/// worth stating: taint is fed only by facts the *protocol* exposes — a
/// `ParameterStatus` for a `GUC_REPORT`ed setting, a named `Parse`. `SET ROLE`
/// reports nothing, `SELECT set_config(...)` reports nothing, and a
/// `CommandComplete` tag is deliberately never sniffed because that heuristic
/// misses all of them. So `dirtyTracked` is safe for exactly the state the
/// protocol announces and no more, whereas cross-tenant session-state isolation
/// has to hold unconditionally. `discardAll` costs one round trip per release
/// and buys that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResetPolicyConfig {
    None,
    DirtyTracked,
    SmartDiscard,
    #[default]
    DiscardAll,
    Verified,
}

impl From<ResetPolicyConfig> for pgelastic_pool::ResetPolicy {
    fn from(policy: ResetPolicyConfig) -> Self {
        match policy {
            ResetPolicyConfig::None => Self::None,
            ResetPolicyConfig::DirtyTracked => Self::DirtyTracked,
            ResetPolicyConfig::SmartDiscard => Self::SmartDiscard,
            ResetPolicyConfig::DiscardAll => Self::DiscardAll,
            ResetPolicyConfig::Verified => Self::Verified,
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_reload_interval_ms() -> u64 {
    1_000
}
fn default_discriminators() -> Vec<TenantDiscriminator> {
    vec![TenantDiscriminator::Role]
}
fn default_startup_option_key() -> String {
    "pgelastic.tenant".to_owned()
}
fn default_stall_interval_ms() -> u64 {
    250
}
fn default_stall_confirmations() -> u32 {
    2
}
fn default_lease_ttl_ms() -> u64 {
    15_000
}
fn default_max_lease_ttl_ms() -> u64 {
    120_000
}
fn default_backend_connections() -> u32 {
    20
}
fn default_query_wait_seconds() -> u64 {
    120
}
fn default_notify_after_seconds() -> u64 {
    5
}
fn default_queue_depth_per_tenant() -> u32 {
    64
}
fn default_max_server_statements() -> usize {
    64
}
fn default_server_lifetime_seconds() -> u64 {
    3600
}
fn default_server_login_retry_seconds() -> u64 {
    15
}
fn default_pool_max_client_connections() -> u32 {
    10_000
}
fn default_weight() -> u32 {
    100
}
fn default_priority() -> u32 {
    1_000
}
fn default_tenant_max_client_connections() -> u32 {
    200
}
fn default_max_client_connections() -> usize {
    1000
}
fn default_client_login_seconds() -> u64 {
    10
}
fn default_connect_seconds() -> u64 {
    5
}
fn default_scram_iterations() -> u32 {
    crate::scram::DEFAULT_ITERATIONS
}
fn default_kdf_concurrency() -> usize {
    4
}
fn default_shutdown_seconds() -> u64 {
    60
}
fn default_inline_frame_bytes() -> usize {
    crate::relay::DEFAULT_INLINE_FRAME_BYTES
}
fn default_max_frame_bytes() -> usize {
    crate::relay::DEFAULT_MAX_FRAME_BYTES
}

/// Resolves a `host:port` string, preferring the first IPv4 answer so a
/// container-mapped `localhost` port does not silently resolve to `::1`.
pub fn resolve(address: &str) -> Result<SocketAddr> {
    use std::net::ToSocketAddrs;
    let mut resolved = address
        .to_socket_addrs()
        .map_err(|e| ProxyError::config(format!("resolving {address}: {e}")))?
        .collect::<Vec<_>>();
    resolved.sort_by_key(|a| u8::from(a.is_ipv6()));
    resolved
        .into_iter()
        .next()
        .ok_or_else(|| ProxyError::config(format!("{address} resolved to nothing")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr as _;

    const MINIMAL: &str = r#"
        [listen]
        address = "127.0.0.1:0"

        [backend]
        address = "127.0.0.1:5432"
        user = "postgres"
    "#;

    #[test]
    fn a_minimal_config_defaults_the_rest() {
        let config = Config::from_str(MINIMAL).unwrap();
        assert_eq!(config.listen.max_client_connections, 1000);
        assert_eq!(config.auth.scram_iterations, 4096);
        assert_eq!(config.backend.tls.mode, BackendTlsMode::Disable);
        assert_eq!(config.drain.shutdown_seconds, 60);
        assert!(config.metrics.address.is_none());
    }

    #[test]
    fn an_unknown_key_is_refused_rather_than_ignored() {
        let source = format!(
            "{MINIMAL}\n[listen.tls]\ncertificateFile = \"x\"\nkeyFile = \"y\"\nnonsense = 1\n"
        );
        assert!(Config::from_str(&source).is_err());
    }

    #[test]
    fn verify_full_without_a_ca_is_refused() {
        let source = format!("{MINIMAL}\n[backend.tls]\nmode = \"VerifyFull\"\n");
        let err = Config::from_str(&source).unwrap_err();
        assert!(err.to_string().contains("caFile"));
    }

    #[test]
    fn require_tls_without_tls_material_is_refused() {
        let source = MINIMAL.replace(
            "address = \"127.0.0.1:0\"",
            "address = \"127.0.0.1:0\"\nrequireTls = true",
        );
        assert!(Config::from_str(&source).is_err());
    }

    #[test]
    fn a_user_with_neither_password_nor_verifier_is_refused() {
        let source = format!("{MINIMAL}\n[[auth.users]]\nname = \"alice\"\n");
        assert!(Config::from_str(&source).is_err());
    }

    #[test]
    fn users_parse_in_both_forms() {
        let source = format!(
            "{MINIMAL}\n[[auth.users]]\nname = \"alice\"\npassword = \"s3cret\"\n\
             \n[[auth.users]]\nname = \"bob\"\nverifier = \"SCRAM-SHA-256$4096:YQ==$YQ==:YQ==\"\n"
        );
        let config = Config::from_str(&source).unwrap();
        assert_eq!(config.auth.users.len(), 2);
        assert_eq!(config.auth.users[0].password.as_deref(), Some("s3cret"));
        assert!(config.auth.users[1].verifier.is_some());
    }

    #[test]
    fn the_fence_defaults_to_cnpgs_lease_with_the_partition_safe_path_on() {
        let config = Config::from_str(MINIMAL).unwrap();
        assert_eq!(config.fence.lease, crate::epoch::FenceTiming::default());
        assert!(config.fence.verify_at_checkout);
        assert!(!config.fence.require_epoch);
        assert!(config.fence.push_address.is_none());
        assert!(config.fence.watch.is_none());
    }

    #[test]
    fn a_lease_whose_relationship_does_not_hold_is_refused_before_the_proxy_binds() {
        let source = format!(
            "{MINIMAL}\n[fence.lease]\n\
             leaseDurationMs = 1000\nrenewDeadlineMs = 900\nretryPeriodMs = 800\n"
        );
        let error = Config::from_str(&source).unwrap_err();
        assert!(error.to_string().contains("fence.lease"), "{error}");
    }

    #[test]
    fn requiring_the_epoch_without_asking_any_backend_for_it_is_refused() {
        let source = format!("{MINIMAL}\n[fence]\nrequireEpoch = true\nverifyAtCheckout = false\n");
        let error = Config::from_str(&source).unwrap_err();
        assert!(error.to_string().contains("requireEpoch"), "{error}");
    }

    #[test]
    fn the_fence_reads_its_three_paths_out_of_one_table() {
        let source = format!(
            "{MINIMAL}\n[fence]\n\
             requireEpoch = true\n\
             inDoubtLog = \"/var/lib/pgelastic/in-doubt.jsonl\"\n\
             pushAddress = \"127.0.0.1:9099\"\n\
             \n[fence.watch]\nnamespace = \"tenants\"\nname = \"shard-a\"\n"
        );
        let config = Config::from_str(&source).unwrap();
        assert!(config.fence.require_epoch);
        assert_eq!(
            config.fence.in_doubt_log.as_deref(),
            Some(Path::new("/var/lib/pgelastic/in-doubt.jsonl"))
        );
        assert_eq!(config.fence.push_address.as_deref(), Some("127.0.0.1:9099"));
        let watch = config.fence.watch.unwrap();
        assert_eq!(watch.namespace, "tenants");
        assert_eq!(watch.name, "shard-a");
    }

    #[test]
    fn a_configuration_with_no_instances_still_names_the_implicit_one() {
        let config = Config::from_str(MINIMAL).unwrap();
        assert!(config.instances.is_empty());
        assert!(config.routing.default_instance.is_none());
        assert!(config.routing.tenants.is_empty());
    }

    #[test]
    fn the_fleet_reads_its_instances_and_its_routing_table() {
        let source = format!(
            "{MINIMAL}\n\
             [[instances]]\n\
             name = \"inst-a\"\n\
             address = \"10.0.0.1:5432\"\n\
             \n\
             [[instances]]\n\
             name = \"inst-b\"\n\
             address = \"10.0.0.2:5432\"\n\
             backendConnections = 8\n\
             \n\
             [routing]\n\
             defaultInstance = \"inst-a\"\n\
             tenants = {{ beta = \"inst-b\" }}\n"
        );
        let config = Config::from_str(&source).unwrap();
        assert_eq!(config.instances.len(), 2);
        assert_eq!(config.instances[1].backend_connections, Some(8));
        assert_eq!(config.routing.default_instance.as_deref(), Some("inst-a"));
        assert_eq!(config.routing.tenants["beta"], "inst-b");
    }

    #[test]
    fn an_instance_declared_twice_is_refused() {
        let source = format!(
            "{MINIMAL}\n\
             [[instances]]\nname = \"inst-a\"\naddress = \"10.0.0.1:5432\"\n\
             \n[[instances]]\nname = \"inst-a\"\naddress = \"10.0.0.2:5432\"\n"
        );
        let error = Config::from_str(&source).unwrap_err();
        assert!(error.to_string().contains("declared twice"), "{error}");
    }

    #[test]
    fn a_tenant_routed_to_an_instance_that_does_not_exist_is_refused_at_start_up() {
        let source = format!(
            "{MINIMAL}\n\
             [[instances]]\nname = \"inst-a\"\naddress = \"10.0.0.1:5432\"\n\
             \n[routing]\ntenants = {{ beta = \"inst-z\" }}\n"
        );
        let error = Config::from_str(&source).unwrap_err();
        assert!(error.to_string().contains("inst-z"), "{error}");
    }

    #[test]
    fn a_default_instance_that_does_not_exist_is_refused_at_start_up() {
        let source = format!(
            "{MINIMAL}\n\
             [[instances]]\nname = \"inst-a\"\naddress = \"10.0.0.1:5432\"\n\
             \n[routing]\ndefaultInstance = \"inst-z\"\n"
        );
        let error = Config::from_str(&source).unwrap_err();
        assert!(error.to_string().contains("defaultInstance"), "{error}");
    }

    #[test]
    fn the_single_instance_configuration_may_still_name_the_implicit_instance() {
        let source = format!("{MINIMAL}\n[routing]\ndefaultInstance = \"default\"\n");
        assert!(Config::from_str(&source).is_ok());
    }

    #[test]
    fn write_stall_detection_is_on_by_default_and_fails_fast() {
        let config = Config::from_str(MINIMAL).unwrap();
        assert!(config.stall.enabled);
        assert!(config.stall.fail_fast);
        assert_eq!(config.stall.interval(), Duration::from_millis(250));
        assert_eq!(config.stall.confirmations, 2);
    }

    #[test]
    fn a_stall_probe_that_would_never_sample_is_refused() {
        let source = format!("{MINIMAL}\n[stall]\nintervalMs = 0\n");
        assert!(Config::from_str(&source).is_err());
        let source = format!("{MINIMAL}\n[stall]\nconfirmations = 0\n");
        assert!(Config::from_str(&source).is_err());
    }

    #[test]
    fn there_is_no_control_listener_unless_one_is_configured() {
        let config = Config::from_str(MINIMAL).unwrap();
        assert!(config.control.address.is_none());
        assert_eq!(config.control.default_lease_ttl(), Duration::from_secs(15));
        assert_eq!(config.control.max_lease_ttl(), Duration::from_secs(120));
    }

    /// The gap the listener was never rendered over, closed in the schema so it
    /// cannot be reopened by a rendering mistake.
    #[test]
    fn a_control_listener_without_client_authentication_is_refused() {
        let source = format!("{MINIMAL}\n[control]\naddress = \"0.0.0.0:9128\"\n");
        let error = Config::from_str(&source).unwrap_err();
        assert!(error.to_string().contains("control.tls"), "{error}");
    }

    #[test]
    fn control_tls_material_that_is_not_on_disk_is_refused_before_the_port_is_bound() {
        let source = format!(
            "{MINIMAL}\n[control]\naddress = \"0.0.0.0:9128\"\n\
             [control.tls]\ncertificateFile = \"/nope/tls.crt\"\nkeyFile = \"/nope/tls.key\"\n\
             clientCaFile = \"/nope/ca.crt\"\nclientName = \"operator\"\n"
        );
        let error = Config::from_str(&source).unwrap_err();
        assert!(error.to_string().contains("certificateFile"), "{error}");
    }

    /// The control section is structural: a change to it has to roll the fleet,
    /// because a running process cannot rebind a port or reload a trust root.
    #[test]
    fn moving_the_control_listener_is_not_something_a_running_process_can_adopt() {
        let dir = tempfile::TempDir::new().unwrap();
        for name in ["tls.crt", "tls.key", "ca.crt"] {
            std::fs::write(dir.path().join(name), "unused").unwrap();
        }
        let control = |address: &str| {
            format!(
                "{MINIMAL}\n[control]\naddress = \"{address}\"\n[control.tls]\n\
                 certificateFile = {:?}\nkeyFile = {:?}\nclientCaFile = {:?}\n\
                 clientName = \"operator\"\n",
                dir.path().join("tls.crt").display().to_string(),
                dir.path().join("tls.key").display().to_string(),
                dir.path().join("ca.crt").display().to_string(),
            )
        };
        let first = Config::from_str(&control("0.0.0.0:9128")).unwrap();
        let second = Config::from_str(&control("0.0.0.0:9129")).unwrap();
        assert!(!first.is_dynamic_change(&second));
    }

    #[test]
    fn a_lease_ceiling_below_the_default_is_refused_rather_than_silently_clamped() {
        let source =
            format!("{MINIMAL}\n[control]\ndefaultLeaseTtlMs = 30000\nmaxLeaseTtlMs = 1000\n");
        let error = Config::from_str(&source).unwrap_err();
        assert!(error.to_string().contains("maxLeaseTtlMs"), "{error}");
    }

    #[test]
    fn a_lease_that_has_already_expired_is_refused() {
        let source = format!("{MINIMAL}\n[control]\ndefaultLeaseTtlMs = 0\n");
        assert!(Config::from_str(&source).is_err());
    }

    #[test]
    fn each_instance_carries_the_credentials_its_own_bootstrap_issued() {
        let source = format!(
            "{MINIMAL}\n\
             [[instances]]\nname = \"inst-a\"\naddress = \"10.0.0.1:5432\"\n\
             user = \"pgelastic_ops\"\npassword = \"a-secret\"\n\
             \n[[instances]]\nname = \"inst-b\"\naddress = \"10.0.0.2:5432\"\n\
             user = \"pgelastic_ops\"\npassword = \"b-secret\"\n"
        );
        let config = Config::from_str(&source).unwrap();
        let a = config.instances[0].backend(&config.backend);
        let b = config.instances[1].backend(&config.backend);
        assert_eq!(a.user, "pgelastic_ops");
        assert_eq!(a.password.as_deref(), Some("a-secret"));
        assert_eq!(b.password.as_deref(), Some("b-secret"));
        assert_ne!(a.password, b.password);
    }

    #[test]
    fn an_instance_that_overrides_nothing_keeps_the_pool_wide_backend_leg() {
        let source =
            format!("{MINIMAL}\n[[instances]]\nname = \"inst-a\"\naddress = \"10.0.0.1:5432\"\n");
        let config = Config::from_str(&source).unwrap();
        let backend = config.instances[0].backend(&config.backend);
        assert_eq!(backend.user, "postgres");
        assert_eq!(backend.address, "10.0.0.1:5432");
    }

    #[test]
    fn a_new_tenant_changes_the_document_without_changing_the_process() {
        let current = Config::from_str(MINIMAL).unwrap();
        let next = Config::from_str(&format!(
            "configVersion = \"2\"\n{MINIMAL}\n\
             [[pool.tenants]]\nname = \"orders\"\nburstable = 4\n\
             \n[routing]\ntenants = {{ orders = \"default\" }}\n"
        ))
        .unwrap();
        assert!(current.is_dynamic_change(&next));
    }

    #[test]
    fn moving_the_listener_is_not_something_a_running_process_can_adopt() {
        let current = Config::from_str(MINIMAL).unwrap();
        let next = Config::from_str(&MINIMAL.replace("127.0.0.1:0", "127.0.0.1:6543")).unwrap();
        assert!(!current.is_dynamic_change(&next));
    }

    #[test]
    fn the_discriminator_list_the_api_defaults_to_parses_rather_than_bricking_the_fleet() {
        let source = format!(
            "{MINIMAL}\n[routing]\n\
             tenantDiscriminators = [\"SNI\", \"StartupOptions\", \"DatabaseName\"]\n"
        );
        let config = Config::from_str(&source).unwrap();
        assert_eq!(
            config.routing.tenant_discriminators,
            vec![
                TenantDiscriminator::Sni,
                TenantDiscriminator::StartupOptions,
                TenantDiscriminator::DatabaseName,
            ]
        );
    }

    #[test]
    fn a_configuration_naming_no_discriminator_at_all_is_refused() {
        let source = format!("{MINIMAL}\n[routing]\ntenantDiscriminators = []\n");
        let error = Config::from_str(&source).unwrap_err();
        assert!(
            error.to_string().contains("tenantDiscriminators"),
            "{error}"
        );
    }

    #[test]
    fn the_single_tenant_default_is_the_login_role() {
        let config = Config::from_str(MINIMAL).unwrap();
        assert_eq!(
            config.routing.tenant_discriminators,
            vec![TenantDiscriminator::Role]
        );
        assert_eq!(config.routing.startup_option_key, "pgelastic.tenant");
    }

    #[test]
    fn nothing_re_reads_the_configuration_unless_a_source_is_named() {
        let config = Config::from_str(MINIMAL).unwrap();
        assert!(config.reload.secret.is_none());
        assert_eq!(config.reload.interval(), Duration::from_secs(1));
        assert!(!config.reload.report_to_pod);
    }

    #[test]
    fn the_reload_source_names_one_secret_and_one_key() {
        let source = format!(
            "{MINIMAL}\n[reload]\nintervalMs = 250\nreportToPod = true\n\
             \n[reload.secret]\nnamespace = \"ns\"\nname = \"pool-proxy-config\"\n\
             key = \"proxy.toml\"\n"
        );
        let config = Config::from_str(&source).unwrap();
        let secret = config.reload.secret.as_ref().unwrap();
        assert_eq!(secret.namespace, "ns");
        assert_eq!(secret.name, "pool-proxy-config");
        assert_eq!(secret.key, "proxy.toml");
        assert_eq!(config.reload.interval(), Duration::from_millis(250));
        assert!(config.reload.report_to_pod);
    }

    #[test]
    fn resolve_prefers_ipv4() {
        let addr = resolve("localhost:5432").unwrap();
        assert!(addr.is_ipv4() || addr.is_ipv6());
        assert_eq!(addr.port(), 5432);
    }
}
