//! The pool key: the identity a backend link is allowed to be reused under.
//!
//! A backend connection may only be handed to a client whose [`PoolKey`]
//! compares equal to the one the link was opened under. Every axis along which
//! two sessions can differ observably — tenant, physical target, database,
//! authenticated role, startup parameters, TLS posture, replication kind,
//! effective pool mode, credential generation — is a field of the key, so
//! "could this link leak state across a boundary?" reduces to "are the keys
//! equal?".
//!
//! `RESET ALL` restores GUCs to their *session-start* values, which are exactly
//! the values the startup packet asked for. No reset ladder can therefore undo
//! a startup parameter, and startup parameters are part of session identity
//! rather than session state.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

macro_rules! string_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Arc<str>);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> Self {
                Self(Arc::from(value.as_ref()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_newtype! {
    /// The tenant that owns the pool.
    ///
    /// First field of the key for a reason: it is the boundary whose violation
    /// is unrecoverable.
    TenantId
}

string_newtype! {
    /// The database name from the startup packet, byte-for-byte.
    ///
    /// Never case-folded: `PostgreSQL` database names are literal, and `"DB"`
    /// and `"db"` are different databases.
    DatabaseName
}

string_newtype! {
    /// The role the client actually authenticated as, not the one it asked for.
    RoleName
}

/// A concrete backend endpoint.
///
/// Distinct from the tenant: one tenant's pool can be re-pointed at a new
/// primary by a failover, and links to the old primary must not be reused.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BackendTarget {
    host: Arc<str>,
    port: u16,
}

impl BackendTarget {
    pub fn new(host: impl AsRef<str>, port: u16) -> Self {
        Self {
            host: Arc::from(host.as_ref()),
            port,
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for BackendTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// The TLS posture of the *backend* link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TlsPosture {
    Plaintext,
    /// Negotiated with an `SSLRequest` round trip.
    Tls,
    /// `PG17+` direct TLS with ALPN `postgresql`, no negotiation packet.
    DirectTls,
}

/// What the `replication` startup parameter asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum ReplicationKind {
    #[default]
    None,
    /// `replication=true` — the physical walsender protocol.
    Physical,
    /// `replication=database` — logical decoding.
    Logical,
}

impl ReplicationKind {
    /// Parses the `replication` startup value using `PostgreSQL`'s own
    /// bool-or-`database` grammar.
    pub fn from_startup_value(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "database" => Some(Self::Logical),
            "true" | "on" | "yes" | "1" => Some(Self::Physical),
            "false" | "off" | "no" | "0" | "" => Some(Self::None),
            _ => None,
        }
    }

    pub fn is_replication(self) -> bool {
        !matches!(self, Self::None)
    }
}

/// How aggressively a link is multiplexed between clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum PoolMode {
    /// One link per client for the life of the client.
    #[default]
    Session,
    /// Released at every `ReadyForQuery` with status `'I'`.
    Transaction,
    /// Released at every `ReadyForQuery`, which must always be `'I'`.
    Statement,
}

impl PoolMode {
    /// Applies the unconditional session override for replication connections.
    ///
    /// A walsender or logical-decoding session is a long-lived stream with no
    /// transaction boundaries to release on; multiplexing it silently corrupts
    /// the replication stream.
    #[must_use]
    pub fn effective(self, replication: ReplicationKind) -> Self {
        if replication.is_replication() {
            Self::Session
        } else {
            self
        }
    }
}

/// Monotonic counter bumped whenever a tenant's credentials are rotated.
///
/// Links opened under an older generation are no longer authorised and must be
/// closed rather than reused, so the generation is part of the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct CredentialGeneration(u64);

impl CredentialGeneration {
    pub fn new(generation: u64) -> Self {
        Self(generation)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for CredentialGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What to do with a startup parameter that is not one of the protocol-reserved
/// names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartupParamPolicy {
    /// Refuse the connection.
    Reject,
    /// Drop the parameter; the client silently gets the server default.
    Ignore,
    /// Carry the parameter into the pool key.
    #[default]
    PoolKey,
}

/// Per-parameter startup policy.
#[derive(Debug, Clone)]
pub struct FingerprintPolicy {
    default: StartupParamPolicy,
    overrides: HashMap<String, StartupParamPolicy>,
}

impl Default for FingerprintPolicy {
    /// `poolKey` for everything except `extra_float_digits`.
    ///
    /// `extra_float_digits` is set by every libpq-derived driver at startup with
    /// no semantic intent, and keying on it fragments a pool for nothing.
    /// `options` is deliberately *not* in this list: it is expanded into the
    /// settings it carries and each of those is judged individually, so the
    /// blanket ignore that a textual matcher needs does not apply here.
    fn default() -> Self {
        let mut overrides = HashMap::new();
        overrides.insert("extra_float_digits".to_owned(), StartupParamPolicy::Ignore);
        Self {
            default: StartupParamPolicy::PoolKey,
            overrides,
        }
    }
}

impl FingerprintPolicy {
    pub fn new(default: StartupParamPolicy) -> Self {
        Self {
            default,
            overrides: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_override(mut self, name: impl AsRef<str>, policy: StartupParamPolicy) -> Self {
        self.overrides
            .insert(name.as_ref().to_ascii_lowercase(), policy);
        self
    }

    pub fn policy_for(&self, name: &str) -> StartupParamPolicy {
        self.overrides.get(name).copied().unwrap_or(self.default)
    }
}

/// A startup parameter the policy refuses.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("startup parameter {name} is not permitted")]
pub struct RejectedStartupParam {
    pub name: String,
}

/// Startup parameters reduced to a canonical, order-independent form.
///
/// Names are lower-cased (GUC names are case-insensitive), values are kept
/// literally (GUC values are not), entries are sorted, and `options` is
/// expanded so that `options=-c search_path=x` and a bare `search_path=x`
/// parameter produce the same fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct StartupFingerprint {
    params: Arc<[(Box<str>, Box<str>)]>,
}

/// Startup parameter names that are key fields in their own right and must
/// never also appear in the fingerprint.
const RESERVED: [&str; 3] = ["user", "database", "replication"];

impl StartupFingerprint {
    /// Builds a fingerprint from raw startup parameters in wire order.
    ///
    /// Later occurrences of a name overwrite earlier ones, matching the way
    /// `PostgreSQL` applies the startup packet.
    pub fn build<I, K, V>(
        params: I,
        policy: &FingerprintPolicy,
    ) -> Result<Self, RejectedStartupParam>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut canonical: BTreeMap<String, String> = BTreeMap::new();
        for (name, value) in params {
            let name = name.as_ref().to_ascii_lowercase();
            if RESERVED.contains(&name.as_str()) {
                continue;
            }
            if name == "options" {
                for (inner, value) in expand_options(value.as_ref()) {
                    apply(&mut canonical, policy, inner, value)?;
                }
            } else {
                apply(&mut canonical, policy, name, value.as_ref().to_owned())?;
            }
        }

        let params = canonical
            .into_iter()
            .map(|(name, value)| (name.into_boxed_str(), value.into_boxed_str()))
            .collect::<Vec<_>>();
        Ok(Self {
            params: Arc::from(params),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.params.is_empty()
    }

    pub fn len(&self) -> usize {
        self.params.len()
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.params
            .iter()
            .find(|(key, _)| **key == *name)
            .map(|(_, value)| &**value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.params.iter().map(|(k, v)| (&**k, &**v))
    }
}

fn apply(
    canonical: &mut BTreeMap<String, String>,
    policy: &FingerprintPolicy,
    name: String,
    value: String,
) -> Result<(), RejectedStartupParam> {
    match policy.policy_for(&name) {
        StartupParamPolicy::Reject => Err(RejectedStartupParam { name }),
        StartupParamPolicy::Ignore => Ok(()),
        StartupParamPolicy::PoolKey => {
            canonical.insert(name, value);
            Ok(())
        }
    }
}

/// Splits an `options` string into the settings it carries.
///
/// Tokens that are not `-c name=value` or `--name=value` cannot be attributed
/// to a GUC, so they are preserved verbatim under the reserved `options` name
/// rather than dropped — dropping them would merge two sessions that the
/// backend will treat differently.
/// Splits an `options` startup value into the settings it carries.
///
/// Public because the fingerprint is not the only thing that has to see inside
/// `options`: a `TimeZone` a client asked for arrives here rather than as a
/// startup key of its own, and the proxy's variable cache has to know about it
/// or the client silently runs under whatever the pool's first client asked for.
/// Whatever cannot be attributed to a setting is returned under the literal name
/// `options`, so nothing is dropped.
pub fn expand_startup_options(raw: &str) -> Vec<(String, String)> {
    expand_options(raw)
}

fn expand_options(raw: &str) -> Vec<(String, String)> {
    let tokens = split_options(raw);
    let mut settings = Vec::new();
    let mut residual: Vec<String> = Vec::new();
    let mut iter = tokens.into_iter().peekable();

    while let Some(token) = iter.next() {
        let assignment = if token == "-c" {
            iter.next()
        } else if let Some(rest) = token.strip_prefix("-c") {
            Some(rest.to_owned())
        } else if let Some(rest) = token.strip_prefix("--") {
            Some(rest.to_owned())
        } else {
            residual.push(token);
            continue;
        };

        match assignment.as_deref().and_then(|a| a.split_once('=')) {
            Some((name, value)) => {
                settings.push((name.trim().to_ascii_lowercase(), value.to_owned()));
            }
            None => {
                if let Some(assignment) = assignment {
                    residual.push(assignment);
                }
            }
        }
    }

    if !residual.is_empty() {
        settings.push(("options".to_owned(), residual.join(" ")));
    }
    settings
}

/// Whitespace-splits an `options` string honouring libpq's backslash escape.
fn split_options(raw: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut started = false;
    let mut chars = raw.chars();

    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(escaped) = chars.next() {
                current.push(escaped);
                started = true;
            }
        } else if c.is_whitespace() {
            if started {
                tokens.push(std::mem::take(&mut current));
                started = false;
            }
        } else {
            current.push(c);
            started = true;
        }
    }
    if started {
        tokens.push(current);
    }
    tokens
}

/// The fields of a [`PoolKey`], before the replication override is applied.
#[derive(Debug, Clone)]
pub struct PoolKeySpec {
    pub tenant: TenantId,
    pub target: BackendTarget,
    pub database: DatabaseName,
    pub role: RoleName,
    pub fingerprint: StartupFingerprint,
    pub tls: TlsPosture,
    pub replication: ReplicationKind,
    /// The mode from configuration; [`PoolKey::mode`] may differ.
    pub configured_mode: PoolMode,
    pub credentials: CredentialGeneration,
}

/// The total identity of a poolable backend link.
///
/// Equality is field-wise and exact. There is no partial or "compatible enough"
/// comparison, and there is deliberately no `From<(&str, &str)>`: every caller
/// has to name all nine axes, so adding a tenth axis is a compile error at every
/// construction site rather than a silent cross-tenant reuse.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolKey {
    tenant: TenantId,
    target: BackendTarget,
    database: DatabaseName,
    role: RoleName,
    fingerprint: StartupFingerprint,
    tls: TlsPosture,
    replication: ReplicationKind,
    mode: PoolMode,
    credentials: CredentialGeneration,
}

impl PoolKey {
    pub fn new(spec: PoolKeySpec) -> Self {
        Self {
            mode: spec.configured_mode.effective(spec.replication),
            tenant: spec.tenant,
            target: spec.target,
            database: spec.database,
            role: spec.role,
            fingerprint: spec.fingerprint,
            tls: spec.tls,
            replication: spec.replication,
            credentials: spec.credentials,
        }
    }

    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn target(&self) -> &BackendTarget {
        &self.target
    }

    pub fn database(&self) -> &DatabaseName {
        &self.database
    }

    pub fn role(&self) -> &RoleName {
        &self.role
    }

    pub fn fingerprint(&self) -> &StartupFingerprint {
        &self.fingerprint
    }

    pub fn tls(&self) -> TlsPosture {
        self.tls
    }

    pub fn replication(&self) -> ReplicationKind {
        self.replication
    }

    /// The mode after the replication override.
    pub fn mode(&self) -> PoolMode {
        self.mode
    }

    pub fn credentials(&self) -> CredentialGeneration {
        self.credentials
    }

    /// A new key identical except for the credential generation.
    #[must_use]
    pub fn with_credentials(&self, credentials: CredentialGeneration) -> Self {
        let mut next = self.clone();
        next.credentials = credentials;
        next
    }
}

impl fmt::Display for PoolKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}@{}/{} tls={:?} repl={:?} mode={:?} gen={} params={}",
            self.tenant,
            self.role,
            self.target,
            self.database,
            self.tls,
            self.replication,
            self.mode,
            self.credentials,
            self.fingerprint.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::*;

    fn hash_of(key: &PoolKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn spec() -> PoolKeySpec {
        PoolKeySpec {
            tenant: TenantId::new("tenant-a"),
            target: BackendTarget::new("primary.example.com", 5432),
            database: DatabaseName::new("appdb"),
            role: RoleName::new("app"),
            fingerprint: StartupFingerprint::default(),
            tls: TlsPosture::Tls,
            replication: ReplicationKind::None,
            configured_mode: PoolMode::Transaction,
            credentials: CredentialGeneration::new(1),
        }
    }

    #[test]
    fn identical_specs_produce_equal_keys() {
        assert_eq!(PoolKey::new(spec()), PoolKey::new(spec()));
        assert_eq!(
            hash_of(&PoolKey::new(spec())),
            hash_of(&PoolKey::new(spec()))
        );
    }

    #[test]
    fn a_different_tenant_is_a_different_key() {
        let mut other = spec();
        other.tenant = TenantId::new("tenant-b");
        assert_ne!(PoolKey::new(spec()), PoolKey::new(other));
    }

    #[test]
    fn every_axis_discriminates() {
        let base = PoolKey::new(spec());

        let mut target = spec();
        target.target = BackendTarget::new("replica.example.com", 5432);
        assert_ne!(base, PoolKey::new(target));

        let mut port = spec();
        port.target = BackendTarget::new("primary.example.com", 5433);
        assert_ne!(base, PoolKey::new(port));

        let mut database = spec();
        database.database = DatabaseName::new("otherdb");
        assert_ne!(base, PoolKey::new(database));

        let mut role = spec();
        role.role = RoleName::new("reporting");
        assert_ne!(base, PoolKey::new(role));

        let mut tls = spec();
        tls.tls = TlsPosture::Plaintext;
        assert_ne!(base, PoolKey::new(tls));

        let mut mode = spec();
        mode.configured_mode = PoolMode::Session;
        assert_ne!(base, PoolKey::new(mode));

        let mut credentials = spec();
        credentials.credentials = CredentialGeneration::new(2);
        assert_ne!(base, PoolKey::new(credentials));

        let mut fingerprint = spec();
        fingerprint.fingerprint =
            StartupFingerprint::build([("search_path", "audit")], &FingerprintPolicy::default())
                .unwrap();
        assert_ne!(base, PoolKey::new(fingerprint));
    }

    #[test]
    fn database_names_are_case_sensitive() {
        let mut upper = spec();
        upper.database = DatabaseName::new("APPDB");
        assert_ne!(PoolKey::new(spec()), PoolKey::new(upper));
    }

    #[test]
    fn replication_forces_session_mode() {
        let mut physical = spec();
        physical.replication = ReplicationKind::Physical;
        physical.configured_mode = PoolMode::Transaction;
        assert_eq!(PoolKey::new(physical).mode(), PoolMode::Session);

        let mut logical = spec();
        logical.replication = ReplicationKind::Logical;
        logical.configured_mode = PoolMode::Statement;
        assert_eq!(PoolKey::new(logical).mode(), PoolMode::Session);
    }

    #[test]
    fn physical_and_logical_replication_do_not_share_a_pool() {
        let mut physical = spec();
        physical.replication = ReplicationKind::Physical;
        let mut logical = spec();
        logical.replication = ReplicationKind::Logical;
        assert_ne!(PoolKey::new(physical), PoolKey::new(logical));
    }

    #[test]
    fn startup_parameter_order_does_not_matter() {
        let policy = FingerprintPolicy::default();
        let forward =
            StartupFingerprint::build([("search_path", "a"), ("TimeZone", "UTC")], &policy)
                .unwrap();
        let reverse =
            StartupFingerprint::build([("timezone", "UTC"), ("SEARCH_PATH", "a")], &policy)
                .unwrap();
        assert_eq!(forward, reverse);
    }

    #[test]
    fn startup_parameter_values_are_case_sensitive() {
        let policy = FingerprintPolicy::default();
        let upper = StartupFingerprint::build([("search_path", "A")], &policy).unwrap();
        let lower = StartupFingerprint::build([("search_path", "a")], &policy).unwrap();
        assert_ne!(upper, lower);
    }

    #[test]
    fn options_expand_to_the_same_fingerprint_as_plain_parameters() {
        let policy = FingerprintPolicy::default();
        let nested =
            StartupFingerprint::build([("options", "-c search_path=audit")], &policy).unwrap();
        let plain = StartupFingerprint::build([("search_path", "audit")], &policy).unwrap();
        assert_eq!(nested, plain);
    }

    #[test]
    fn options_accept_every_spelling_postgres_does() {
        let policy = FingerprintPolicy::default();
        let expected = StartupFingerprint::build([("search_path", "audit")], &policy).unwrap();
        for spelling in [
            "-c search_path=audit",
            "-csearch_path=audit",
            "--search_path=audit",
            "  --search_path=audit  ",
        ] {
            assert_eq!(
                StartupFingerprint::build([("options", spelling)], &policy).unwrap(),
                expected,
                "spelling {spelling}",
            );
        }
    }

    #[test]
    fn escaped_spaces_inside_options_stay_in_one_value() {
        let policy = FingerprintPolicy::default();
        let fingerprint =
            StartupFingerprint::build([("options", r"-c application_name=my\ app")], &policy)
                .unwrap();
        assert_eq!(fingerprint.get("application_name"), Some("my app"));
    }

    #[test]
    fn unattributable_option_tokens_survive_into_the_fingerprint() {
        let policy = FingerprintPolicy::default();
        let fingerprint =
            StartupFingerprint::build([("options", "-o something")], &policy).unwrap();
        assert_eq!(fingerprint.get("options"), Some("-o something"));
    }

    #[test]
    fn reserved_parameters_never_enter_the_fingerprint() {
        let policy = FingerprintPolicy::default();
        let fingerprint = StartupFingerprint::build(
            [
                ("user", "app"),
                ("database", "appdb"),
                ("replication", "true"),
            ],
            &policy,
        )
        .unwrap();
        assert!(fingerprint.is_empty());
    }

    #[test]
    fn extra_float_digits_is_ignored_by_default() {
        let policy = FingerprintPolicy::default();
        let with = StartupFingerprint::build([("extra_float_digits", "3")], &policy).unwrap();
        assert!(with.is_empty());
    }

    #[test]
    fn a_rejected_parameter_fails_the_build() {
        let policy =
            FingerprintPolicy::default().with_override("search_path", StartupParamPolicy::Reject);
        let error = StartupFingerprint::build([("search_path", "audit")], &policy).unwrap_err();
        assert_eq!(error.name, "search_path");
    }

    #[test]
    fn a_rejected_parameter_nested_in_options_also_fails() {
        let policy =
            FingerprintPolicy::default().with_override("search_path", StartupParamPolicy::Reject);
        assert!(StartupFingerprint::build([("options", "-c search_path=audit")], &policy).is_err());
    }

    #[test]
    fn the_last_occurrence_of_a_parameter_wins() {
        let policy = FingerprintPolicy::default();
        let fingerprint =
            StartupFingerprint::build([("search_path", "a"), ("search_path", "b")], &policy)
                .unwrap();
        assert_eq!(fingerprint.get("search_path"), Some("b"));
    }

    #[test]
    fn replication_startup_values_parse_like_postgres() {
        assert_eq!(
            ReplicationKind::from_startup_value("database"),
            Some(ReplicationKind::Logical)
        );
        assert_eq!(
            ReplicationKind::from_startup_value("TRUE"),
            Some(ReplicationKind::Physical)
        );
        assert_eq!(
            ReplicationKind::from_startup_value("off"),
            Some(ReplicationKind::None)
        );
        assert_eq!(ReplicationKind::from_startup_value("maybe"), None);
    }
}
