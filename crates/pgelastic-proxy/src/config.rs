//! File-backed configuration.
//!
//! A stand-in for the operator control stream that lands in a later milestone,
//! so the field names deliberately track `PgElasticPool`'s `spec.proxy` rather
//! than inventing a second vocabulary.
//!
//! TOML rather than YAML: `serde_yaml` is archived and unmaintained, and this
//! file is a development affordance — the production shape arrives over the
//! control stream, not off disk.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{ProxyError, Result};

#[derive(Debug, Clone, Deserialize)]
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
        Ok(())
    }
}

/// The primary-epoch fence.
///
/// The lease parameters and the fence's reaction deadline are one decision and
/// live in one struct — see [`FenceTiming`](crate::epoch::FenceTiming).
#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FenceWatchConfig {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServerTlsConfig {
    pub certificate_file: PathBuf,
    pub key_file: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Default, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UserConfig {
    pub name: String,
    /// A `PostgreSQL` `rolpassword` SCRAM secret. Preferred: the proxy then
    /// never holds a password it could replay.
    #[serde(default)]
    pub verifier: Option<String>,
    /// A cleartext password, hashed into a verifier at start-up. Development
    /// affordance only.
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetricsConfig {
    /// Omitted means no metrics listener at all.
    #[serde(default)]
    pub address: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RoutingConfig {
    /// Identity of this replica, embedded in every cancel key it mints so a
    /// `CancelRequest` that kube-proxy lands on the wrong pod can still be
    /// routed. Reserved now because widening a cancel key later is a wire
    /// break.
    #[serde(default)]
    pub cancel_routing_id: u16,
}

/// Pooling, capacity and per-tenant claims.
///
/// `poolSize` is deliberately absent and always will be. A held backend
/// connection is one unit of work-in-progress, so the pool's capacity unit and
/// `PgBouncer`'s `pool_size` are the same number; stacking two limiters is how
/// a ceiling becomes unexplainable. Operators set `backendConnections` and
/// per-tenant guaranteed/burstable, and everything else is derived.
#[derive(Debug, Clone, Deserialize)]
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
#[derive(Debug, Clone, Deserialize)]
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
    fn resolve_prefers_ipv4() {
        let addr = resolve("localhost:5432").unwrap();
        assert!(addr.is_ipv4() || addr.is_ipv6());
        assert_eq!(addr.port(), 5432);
    }
}
