//! Picking up a published configuration without dropping a client.
//!
//! The operator renders one document and the whole fleet reads it. Half of that
//! document describes the process — listen address, TLS material, which
//! instances exist — and can only change by starting a new one, so a change
//! there rolls the Deployment. The other half is the tenant routing table and
//! the per-tenant capacity claims, and both of those are read at a checkout
//! boundary rather than held by a session. This module applies that half in
//! place.
//!
//! The safety argument is not "the swap is fast". It is that neither structure
//! is consulted inside a transaction:
//!
//! - the routing table is read at every checkout, so a client between two
//!   transactions moves and a client inside one finishes on the backend it
//!   already holds;
//! - a capacity claim is read under the allocator's lock, which a checkout holds
//!   for the moment it takes to decide and never across a statement.
//!
//! A `get` on one named Secret every interval rather than a watch: RBAC can
//! restrict `get` to a single `resourceName` and cannot restrict a watch at all,
//! so this is the shape that does not hand the data plane read access to every
//! Secret in its namespace. The cost is one small request per replica per
//! interval and a propagation bound of exactly that interval.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::api::{Api, Patch, PatchParams};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::{Config, SecretSource};
use crate::metrics::Metrics;

/// The annotation a replica publishes its applied `configVersion` under.
///
/// The operator reads it back off every ready Pod, which is the only way it can
/// tell a fleet that has converged on a configuration from one where some
/// replicas are still serving the previous one.
pub const APPLIED_VERSION_ANNOTATION: &str = "pgelastic.io/proxyConfigVersion";

/// The environment variable naming this replica's own Pod, from the downward
/// API. Without it there is nothing to annotate.
pub const POD_NAME_ENV: &str = "PGELASTIC_POD_NAME";

/// How long a failed poll waits before trying again.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// Everything the reload loop touches.
pub struct Reloader {
    pub proxy: Arc<crate::server::Proxy>,
    pub metrics: Arc<Metrics>,
    pub source: SecretSource,
    pub interval: Duration,
    /// The Pod to publish the applied version onto, if the fleet is being
    /// tracked.
    pub pod: Option<String>,
    applied: String,
}

impl std::fmt::Debug for Reloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reloader")
            .field("source", &self.source)
            .field("applied", &self.applied)
            .finish_non_exhaustive()
    }
}

impl Reloader {
    pub fn new(
        config: &Config,
        proxy: Arc<crate::server::Proxy>,
        metrics: Arc<Metrics>,
    ) -> Option<Self> {
        let source = config.reload.secret.clone()?;
        let pod = config
            .reload
            .report_to_pod
            .then(|| std::env::var(POD_NAME_ENV).ok())
            .flatten();
        if config.reload.report_to_pod && pod.is_none() {
            warn!(
                "reload.reportToPod is set but {POD_NAME_ENV} is unset, so this replica \
                 cannot publish the configuration it has applied"
            );
        }
        Some(Self {
            proxy,
            metrics,
            source,
            interval: config.reload.interval(),
            pod,
            applied: config.config_version.clone(),
        })
    }

    /// Adopts the dynamic half of `next`, and reports what it could not adopt.
    ///
    /// The structural half is deliberately not applied: the process was built
    /// from it, and pretending otherwise would leave the running proxy
    /// disagreeing with the document it claims to be serving.
    pub fn apply(&mut self, current: &Config, next: &Config) -> Applied {
        let structural = !current.is_dynamic_change(next);
        let fleet = &self.proxy.fleet;
        let routes = fleet.apply_routes(&next.routing.tenants);
        let mut claims = 0;
        for instance in fleet.instances() {
            claims += instance.pools.apply_tenants(&next.pool.tenants);
        }
        fleet.publish_budget_now();
        // The logins and the per-tenant backend identities. A failure here leaves the previous
        // pair in place rather than half of each: the alternative is a replica authenticating
        // against one document and dialling with another.
        let identities = match self.proxy.adopt(next) {
            Ok(changed) => changed,
            Err(error) => {
                warn!(%error, version = next.config_version,
                      "the published logins could not be adopted; the running ones are kept");
                self.metrics.config_rejected();
                false
            }
        };
        self.applied.clone_from(&next.config_version);
        self.metrics.config_applied(&next.config_version);
        Applied {
            version: next.config_version.clone(),
            routes,
            claims,
            identities,
            structural,
        }
    }

    pub fn applied_version(&self) -> &str {
        &self.applied
    }
}

/// What one adoption changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    pub version: String,
    pub routes: usize,
    pub claims: usize,
    /// Whether the logins or the per-tenant backend identities moved. Reported separately
    /// from `claims` because a tenant can gain a role and a credential without its capacity
    /// claim changing at all, and that is the case onboarding one actually produces.
    pub identities: bool,
    /// The published document also changed something only a restart can apply.
    /// The operator rolls the fleet for that; this is the log line that says the
    /// running process is not the whole of what was published.
    pub structural: bool,
}

/// Polls the published configuration until `shutdown` goes true.
///
/// Never returns an error. A control plane that has gone away leaves the proxy
/// serving the configuration it already has, which is the correct behaviour: the
/// routing table it holds is the one that was true when the operator was last
/// heard from.
pub async fn run(mut reloader: Reloader, current: Config, mut shutdown: watch::Receiver<bool>) {
    let source = reloader.source.clone();
    info!(
        namespace = source.namespace,
        secret = source.name,
        key = source.key,
        interval_ms = reloader.interval.as_millis(),
        "polling the published proxy configuration"
    );

    // Published before the first poll so a fleet nobody has changed anything on
    // still reports the version it booted with.
    reloader.metrics.config_applied(&current.config_version);
    // One client for the life of the loop. Rebuilt only when a request fails,
    // because a fresh one per tick is a TLS handshake per tick.
    let mut client = None;
    publish_applied(&reloader, &mut client, &current.config_version).await;

    let mut interval = reloader.interval;
    loop {
        tokio::select! {
            biased;
            () = wait_true(&mut shutdown) => return,
            () = tokio::time::sleep(interval) => {}
        }

        match fetch(&source, &mut client).await {
            Ok(document) => {
                interval = reloader.interval;
                let published: Config = match document.parse() {
                    Ok(config) => config,
                    Err(error) => {
                        warn!(%error, "the published proxy configuration does not parse; \
                              the running one is kept");
                        reloader.metrics.config_rejected();
                        continue;
                    }
                };
                if published.config_version == reloader.applied {
                    continue;
                }
                let applied = reloader.apply(&current, &published);
                if applied.structural {
                    warn!(
                        version = applied.version,
                        "the published configuration also changes something only a restart \
                         can apply; this replica keeps serving the process it was started with"
                    );
                }
                info!(
                    version = applied.version,
                    routes = applied.routes,
                    claims = applied.claims,
                    identities = applied.identities,
                    "applied a published proxy configuration"
                );
                publish_applied(&reloader, &mut client, &applied.version).await;
            }
            Err(error) => {
                interval = RETRY_BACKOFF;
                client = None;
                warn!(%error, "reading the published proxy configuration failed; \
                      the running one is kept");
            }
        }
    }
}

/// Returns the cached client, building one if there is none.
async fn connected(client: &mut Option<kube::Client>) -> Result<kube::Client, kube::Error> {
    if let Some(existing) = client {
        return Ok(existing.clone());
    }
    let built = kube::Client::try_default().await?;
    *client = Some(built.clone());
    Ok(built)
}

async fn fetch(
    source: &SecretSource,
    client: &mut Option<kube::Client>,
) -> Result<String, kube::Error> {
    let api: Api<Secret> = Api::namespaced(connected(client).await?, &source.namespace);
    let secret = api.get(&source.name).await?;
    document_of(&secret, &source.key).ok_or_else(|| {
        kube::Error::Service(Box::new(std::io::Error::other(format!(
            "secret {} carries no key {}",
            source.name, source.key
        ))))
    })
}

/// Reads one key out of a Secret, from either half of the API's split.
fn document_of(secret: &Secret, key: &str) -> Option<String> {
    if let Some(data) = secret.data.as_ref().and_then(|data| data.get(key)) {
        return String::from_utf8(data.0.clone()).ok();
    }
    secret
        .string_data
        .as_ref()
        .and_then(|data| data.get(key))
        .cloned()
}

/// Annotates this replica's own Pod with the version it is serving.
///
/// Best effort by design: a proxy that cannot reach the API server is still
/// proxying, and a missing annotation makes the operator report an unconverged
/// fleet — which is the safe direction to be wrong in.
async fn publish_applied(reloader: &Reloader, client: &mut Option<kube::Client>, version: &str) {
    let Some(pod) = &reloader.pod else { return };
    if version.is_empty() {
        return;
    }
    // A merge patch rather than a server-side apply: the replica is adding one
    // annotation to an object somebody else owns, and an apply would make it a
    // field manager for everything it sent — which for an apply is the whole
    // object, including an empty spec.
    let patch = serde_json::json!({
        "metadata": { "annotations": { APPLIED_VERSION_ANNOTATION: version } }
    });
    let result = async {
        let api: Api<Pod> = Api::namespaced(connected(client).await?, &reloader.source.namespace);
        api.patch(pod, &PatchParams::default(), &Patch::Merge(&patch))
            .await
    }
    .await;
    match result {
        Ok(_) => debug!(pod, version, "published the applied configuration version"),
        Err(error) => {
            *client = None;
            warn!(%error, pod, "annotating this Pod with the applied configuration \
                  version failed; the operator will read the fleet as unconverged");
        }
    }
}

async fn wait_true(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr as _;

    const BASE: &str = r#"
        configVersion = "1-aaa"

        [listen]
        address = "127.0.0.1:0"

        [backend]
        address = "127.0.0.1:5432"
        user = "postgres"

        [[instances]]
        name = "inst-a"
        address = "127.0.0.1:5001"

        [[instances]]
        name = "inst-b"
        address = "127.0.0.1:5002"
    "#;

    fn reloader(config: &Config) -> Reloader {
        let metrics = Metrics::new();
        let proxy = crate::server::Proxy::new(config.clone(), Arc::clone(&metrics))
            .expect("the proxy builds");
        Reloader {
            proxy,
            metrics,
            source: SecretSource {
                namespace: "ns".to_owned(),
                name: "pool-proxy-config".to_owned(),
                key: "proxy.toml".to_owned(),
            },
            interval: Duration::from_millis(250),
            pod: None,
            applied: config.config_version.clone(),
        }
    }

    /// The identity a tenant's backend sessions run as has to reach a running replica.
    ///
    /// A fleet's last structural change is its instances appearing; tenants are onboarded
    /// afterwards. `Config::structural` clears `pool.tenants` and `auth.users` precisely so
    /// that onboarding one does not restart every replica and drop every other tenant's
    /// clients - but removing the restart is only half of it, and nothing here adopted the
    /// half that was left behind.
    ///
    /// Until it does, a tenant created after this process started is invisible to
    /// `backend_for`, which falls through to the instance's own identity: the control
    /// plane's role, running that tenant's SQL, with `session_user` naming the wrong
    /// principal in every audit trail. That is the fallback the tenant branch already
    /// refuses by name the moment it can see the tenant at all.
    #[test]
    fn a_tenants_published_identity_reaches_a_running_replica() {
        let current = Config::from_str(BASE).unwrap();
        let mut reloader = reloader(&current);
        let proxy = Arc::clone(&reloader.proxy);
        let instance = proxy.fleet.route("orders");
        assert_eq!(
            proxy
                .backend_for(&instance, "orders", "owner")
                .unwrap()
                .user,
            "postgres",
            "a tenant with no published identity dials as the instance"
        );

        let next = Config::from_str(&format!(
            "{}{ORDERS_IDENTITY}",
            BASE.replace("1-aaa", "2-bbb")
        ))
        .unwrap();
        let applied = reloader.apply(&current, &next);

        assert!(applied.identities, "the adoption reported nothing changed");
        assert_eq!(
            proxy
                .backend_for(&instance, "orders", "owner")
                .unwrap()
                .user,
            "pgt_orders_c0ffee",
            "the replica is still dialling as the identity it started with"
        );
        assert_eq!(
            proxy.credential_generation("orders", "owner"),
            7,
            "the pool key still carries the generation this process started with, so a \
             rotation would not evict the links opened under the superseded credential"
        );
    }

    /// An unknown login must be indistinguishable from a known one, and an adoption must not
    /// start telling them apart.
    ///
    /// Both halves are asserted because either alone is an oracle pointing one way or the
    /// other. The operator renders `password`, not `verifier`, so a rebuilt record gets a fresh
    /// random salt: carrying only the mock would freeze the unknown login and move every known
    /// one, which is just as much of a tell as the reverse.
    #[test]
    fn an_adoption_moves_neither_an_unknown_nor_an_unchanged_logins_challenge() {
        let alice = "\n[[auth.users]]\nname = \"alice\"\npassword = \"hunter2\"\n";
        let current = Config::from_str(&format!("{BASE}{alice}")).unwrap();
        let mut reloader = reloader(&current);
        let unknown_before = reloader.proxy.auth().challenge_salt_for(b"nobody");
        let alice_before = reloader.proxy.auth().challenge_salt_for(b"alice");

        // Onboarding bob is the publication an attacker can provoke: it changes the login
        // table without touching alice's own entry.
        let next = Config::from_str(&format!(
            "{}{alice}[[auth.users]]\nname = \"bob\"\npassword = \"correct-horse\"\n",
            BASE.replace("1-aaa", "2-bbb")
        ))
        .unwrap();
        reloader.apply(&current, &next);

        assert_eq!(
            unknown_before,
            reloader.proxy.auth().challenge_salt_for(b"nobody"),
            "adopting a login moved the salt an unknown user is challenged with"
        );
        assert_eq!(
            alice_before,
            reloader.proxy.auth().challenge_salt_for(b"alice"),
            "adopting an unrelated login moved an unchanged login's salt, which tells an \
             attacker polling both that this one is real"
        );
    }

    /// Trust mode admits every client with no challenge at all, and it is a decision the
    /// process makes once, at start-up, from the document it was built with.
    ///
    /// It must not become reachable by adoption. The operator drops any login whose credentials
    /// Secret it cannot read, so a document with no logins is something a transient control-plane
    /// failure produces rather than something anybody asks for - and `auth.users` is cleared by
    /// `Config::structural`, so such a document rolls nothing and every replica would take it
    /// inside one interval.
    #[test]
    fn a_document_that_has_lost_its_logins_does_not_open_the_fleet() {
        let current = Config::from_str(&format!(
            "{BASE}\n[[auth.users]]\nname = \"alice\"\npassword = \"hunter2\"\n"
        ))
        .unwrap();
        let mut reloader = reloader(&current);
        assert!(!reloader.proxy.auth().is_trust());

        let next = Config::from_str(&BASE.replace("1-aaa", "2-bbb")).unwrap();
        let applied = reloader.apply(&current, &next);

        assert!(
            !reloader.proxy.auth().is_trust(),
            "adopting a document with no logins turned an authenticating fleet into an open one"
        );
        assert!(
            !applied.identities,
            "the adoption reported the logins as taken up when it refused them"
        );
    }

    /// A login whose credential really did change must get a new salt, or a rotation would be
    /// indistinguishable from no rotation at all.
    #[test]
    fn a_login_whose_password_changed_is_rebuilt_rather_than_carried() {
        let current = Config::from_str(&format!(
            "{BASE}\n[[auth.users]]\nname = \"alice\"\npassword = \"hunter2\"\n"
        ))
        .unwrap();
        let mut reloader = reloader(&current);
        let before = reloader.proxy.auth().challenge_salt_for(b"alice");

        let next = Config::from_str(&format!(
            "{}\n[[auth.users]]\nname = \"alice\"\npassword = \"rotated\"\n",
            BASE.replace("1-aaa", "2-bbb")
        ))
        .unwrap();
        reloader.apply(&current, &next);

        assert_ne!(
            before,
            reloader.proxy.auth().challenge_salt_for(b"alice"),
            "a rotated credential kept the verifier derived from the old password"
        );
    }

    /// A login with an identity of its own dials as *that* role, not its tenant's owner.
    /// A contained user that dialled as the owner would hold the owner's privileges and be
    /// indistinguishable from it in `pg_stat_activity`, which is the whole of what it exists
    /// not to be.
    #[test]
    fn a_login_dials_the_backend_as_its_own_role_rather_than_its_tenants() {
        let current = Config::from_str(BASE).unwrap();
        let mut reloader = reloader(&current);
        let proxy = Arc::clone(&reloader.proxy);
        let instance = proxy.fleet.route("orders");

        let next = Config::from_str(&format!(
            "{}{ORDERS_IDENTITY}{APP_IDENTITY}",
            BASE.replace("1-aaa", "2-bbb")
        ))
        .unwrap();
        reloader.apply(&current, &next);

        assert_eq!(
            proxy.backend_for(&instance, "orders", "app").unwrap().user,
            "pgtu_orders_app_deadbeef",
            "the login dialled as somebody else"
        );
        assert_eq!(
            proxy.credential_generation("orders", "app"),
            3,
            "the login's links are keyed on its tenant's generation, so rotating its own \
             credential would not evict them"
        );
        // The tenant's own login is unaffected and still assumes the tenant role.
        assert_eq!(
            proxy
                .backend_for(&instance, "orders", "owner")
                .unwrap()
                .user,
            "pgt_orders_c0ffee",
        );
    }

    /// A login that names a role but carries no credential is refused, never quietly dialled
    /// as its tenant - that fallback is the defect the per-login identity exists to fix,
    /// applied silently during a config-propagation lag.
    #[test]
    fn a_login_whose_credential_is_missing_is_refused_rather_than_downgraded() {
        let current = Config::from_str(BASE).unwrap();
        let mut reloader = reloader(&current);
        let proxy = Arc::clone(&reloader.proxy);
        let instance = proxy.fleet.route("orders");

        let next = Config::from_str(&format!(
            "{}{ORDERS_IDENTITY}\n[[auth.users]]\nname = \"app\"\npassword = \"x\"\n\
             backendRole = \"pgtu_orders_app_deadbeef\"\n",
            BASE.replace("1-aaa", "2-bbb")
        ))
        .unwrap();
        reloader.apply(&current, &next);

        assert!(
            proxy.backend_for(&instance, "orders", "app").is_err(),
            "a login with no credential was dialled as its tenant"
        );
    }

    /// A login named after its own tenant's owner is still dialled as *itself*.
    ///
    /// `spec.userName` is a tenant operator's to choose, so a contained login can carry the
    /// same name as the owner it sits beside. Both render into `[[auth.users]]`: the owner's
    /// entry authenticates the tenant and carries no backend role, the login's carries one.
    /// Matching on the name alone stops at whichever comes first - the owner - finds no
    /// backend role on it, and falls through to the tenant's own identity. The client would
    /// then authenticate with the login's password and run as the tenant OWNER, holding the
    /// owner's privileges and indistinguishable from it in `pg_stat_activity`.
    #[test]
    fn a_login_named_after_its_tenants_owner_is_not_dialled_as_the_owner() {
        let current = Config::from_str(BASE).unwrap();
        let mut reloader = reloader(&current);
        let proxy = Arc::clone(&reloader.proxy);
        let instance = proxy.fleet.route("orders");

        // The owner's authentication entry first, exactly as proxyUsers renders it, then the
        // contained login under the same name and tenant.
        let next = Config::from_str(&format!(
            "{}{ORDERS_IDENTITY}\n[[auth.users]]\nname = \"owner\"\ntenant = \"orders\"\n\
             password = \"ownerpw\"\n\n[[auth.users]]\nname = \"owner\"\ntenant = \"orders\"\n\
             password = \"hunter2\"\nbackendRole = \"pgtu_orders_owner_deadbeef\"\n\
             backendSaltedPassword = \"c2FsdGVk\"\nbackendSalt = \"c2FsdA\"\n\
             backendIterations = 4096\nbackendCredentialGeneration = 3\n",
            BASE.replace("1-aaa", "2-bbb")
        ))
        .unwrap();
        reloader.apply(&current, &next);

        assert_eq!(
            proxy
                .backend_for(&instance, "orders", "owner")
                .unwrap()
                .user,
            "pgtu_orders_owner_deadbeef",
            "a login sharing its tenant owner's name was dialled as the owner"
        );
        assert_eq!(
            proxy.credential_generation("orders", "owner"),
            3,
            "the login's links were keyed on the tenant's generation, so the two identities \
             share a pool"
        );
    }

    const APP_IDENTITY: &str = r#"
        [[auth.users]]
        name = "app"
        tenant = "orders"
        password = "hunter2"
        backendRole = "pgtu_orders_app_deadbeef"
        backendSaltedPassword = "c2FsdGVk"
        backendSalt = "c2FsdA"
        backendIterations = 4096
        backendCredentialGeneration = 3
    "#;

    const ORDERS_IDENTITY: &str = r#"
        [[pool.tenants]]
        name = "orders"
        burstable = 10
        backendRole = "pgt_orders_c0ffee"
        backendSaltedPassword = "c2FsdGVk"
        backendSalt = "c2FsdA"
        backendIterations = 4096
        credentialGeneration = 7
    "#;

    #[test]
    fn a_new_route_moves_the_tenant_without_touching_the_process() {
        let current = Config::from_str(BASE).unwrap();
        let mut reloader = reloader(&current);
        assert_eq!(reloader.proxy.fleet.route("orders").id.as_str(), "inst-a");

        let next = Config::from_str(&format!(
            "{}\n[routing]\ntenants = {{ orders = \"inst-b\" }}\n",
            BASE.replace("1-aaa", "2-bbb")
        ))
        .unwrap();
        let applied = reloader.apply(&current, &next);

        assert_eq!(applied.routes, 1);
        assert!(!applied.structural);
        assert_eq!(applied.version, "2-bbb");
        assert_eq!(reloader.proxy.fleet.route("orders").id.as_str(), "inst-b");
        assert_eq!(reloader.applied_version(), "2-bbb");
    }

    #[test]
    fn a_tenant_that_has_left_the_pool_loses_its_route_rather_than_keeping_the_old_one() {
        let current = Config::from_str(&format!(
            "{BASE}\n[routing]\ntenants = {{ orders = \"inst-b\" }}\n"
        ))
        .unwrap();
        let mut reloader = reloader(&current);
        assert_eq!(reloader.proxy.fleet.route("orders").id.as_str(), "inst-b");

        let next = Config::from_str(&BASE.replace("1-aaa", "2-bbb")).unwrap();
        reloader.apply(&current, &next);
        assert_eq!(reloader.proxy.fleet.route("orders").id.as_str(), "inst-a");
    }

    #[test]
    fn a_published_claim_is_adopted_by_every_instance_in_the_fleet() {
        let current = Config::from_str(BASE).unwrap();
        let mut reloader = reloader(&current);

        let next = Config::from_str(&format!(
            "{}\n[[pool.tenants]]\nname = \"orders\"\nguaranteed = 2\nburstable = 8\n",
            BASE.replace("1-aaa", "2-bbb")
        ))
        .unwrap();
        let applied = reloader.apply(&current, &next);
        assert_eq!(applied.claims, 2, "one claim on each of two instances");
    }

    #[test]
    fn a_change_only_a_restart_can_apply_is_reported_rather_than_half_applied() {
        let current = Config::from_str(BASE).unwrap();
        let mut reloader = reloader(&current);

        let next = Config::from_str(
            &BASE
                .replace("1-aaa", "2-bbb")
                .replace("127.0.0.1:5001", "127.0.0.1:5999"),
        )
        .unwrap();
        let applied = reloader.apply(&current, &next);

        assert!(applied.structural);
        assert_eq!(
            reloader
                .proxy
                .fleet
                .get(&crate::route::InstanceId::new("inst-a"))
                .unwrap()
                .backend
                .address,
            "127.0.0.1:5001",
            "a running proxy must not claim to be serving an address it never dialled"
        );
    }

    #[test]
    fn a_route_to_an_instance_this_proxy_does_not_front_is_dropped_rather_than_applied() {
        let current = Config::from_str(BASE).unwrap();
        let mut reloader = reloader(&current);

        // Not through Config, which refuses such a route at parse time. This is
        // the shape a proxy mid-rollout sees: a document written for a fleet
        // that has an instance its own process was not built with.
        let mut routes = std::collections::BTreeMap::new();
        routes.insert("orders".to_owned(), "inst-z".to_owned());
        assert_eq!(reloader.proxy.fleet.apply_routes(&routes), 0);
        assert_eq!(reloader.proxy.fleet.route("orders").id.as_str(), "inst-a");
        let _ = &mut reloader;
    }

    #[test]
    fn the_document_is_read_from_either_half_of_a_secret() {
        let secret = Secret {
            data: Some(std::collections::BTreeMap::from([(
                "proxy.toml".to_owned(),
                k8s_openapi::ByteString(b"listen = 1".to_vec()),
            )])),
            ..Secret::default()
        };
        assert_eq!(
            document_of(&secret, "proxy.toml").as_deref(),
            Some("listen = 1")
        );

        let written = Secret {
            string_data: Some(std::collections::BTreeMap::from([(
                "proxy.toml".to_owned(),
                "listen = 2".to_owned(),
            )])),
            ..Secret::default()
        };
        assert_eq!(
            document_of(&written, "proxy.toml").as_deref(),
            Some("listen = 2")
        );
        assert_eq!(document_of(&written, "other.toml"), None);
    }
}
