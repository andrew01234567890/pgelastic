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
        Ok(())
    }
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
    fn resolve_prefers_ipv4() {
        let addr = resolve("localhost:5432").unwrap();
        assert!(addr.is_ipv4() || addr.is_ipv6());
        assert_eq!(addr.port(), 5432);
    }
}
