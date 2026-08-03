//! The listener, the per-connection task, and graceful drain.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use arc_swap::ArcSwap;
use pgelastic_wire::{BackendMessage, TransactionStatus};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::backend;
use crate::cancel::{CancelRegistry, CancelRoute, CancelToken};
use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::handshake::{self, Accepted, ClientAuth};
use crate::metrics::{AuthOutcome, Metrics, RejectReason};
use crate::quiesce::QuiesceRegistry;
use crate::route::{Fleet, Instance};
use crate::scram::{KdfPool, ScramVerifier};
use crate::session::{self, Ending};
use crate::stream::ClientStream;
use crate::tenant::TenantResolver;

/// The half of a published document a running process can adopt.
///
/// Held behind an `ArcSwap` for the same reason `Fleet::routes` is: every connection reads it
/// and the reload loop writes it at most once an interval. Both fields are cleared by
/// `Config::structural`, which is the definition of belonging here - that function decides what
/// a change may not roll the fleet for, and this is what then has to pick the change up.
struct Dynamic {
    /// The per-tenant claims, which carry each tenant's backend identity.
    tenants: Vec<crate::config::TenantConfig>,
    /// What `auth` was built from, kept so that an adoption which does not touch the logins
    /// can reuse the table rather than deriving every password-configured verifier again.
    users: Vec<crate::config::UserConfig>,
    auth: Arc<ClientAuth>,
}

/// Everything a connection task needs, shared by `Arc`.
pub struct Proxy {
    pub config: Config,
    pub acceptor: Option<TlsAcceptor>,
    dynamic: ArcSwap<Dynamic>,
    pub kdf: KdfPool,
    pub cancels: Arc<CancelRegistry>,
    pub metrics: Arc<Metrics>,
    /// The instances this proxy fronts, and which tenant is on which.
    pub fleet: Arc<Fleet>,
    /// Every tenant's admission gate, for the migration cutover.
    pub quiesce: Arc<QuiesceRegistry>,
    /// Reads a new connection's tenant out of its startup packet.
    pub tenant: TenantResolver,
    permits: Arc<Semaphore>,
}

impl std::fmt::Debug for Proxy {
    /// Hand-written because `TlsAcceptor` has no `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proxy")
            .field("listen", &self.config.listen.address)
            .field("backend", &self.config.backend.address)
            .field("tls", &self.acceptor.is_some())
            .finish_non_exhaustive()
    }
}

impl Proxy {
    pub fn new(config: Config, metrics: Arc<Metrics>) -> Result<Arc<Self>> {
        let acceptor = config
            .listen
            .tls
            .as_ref()
            .map(crate::tls::server_acceptor)
            .transpose()?;
        let iterations = NonZeroU32::new(config.auth.scram_iterations)
            .ok_or_else(|| ProxyError::config("auth.scramIterations must be non-zero"))?;
        let users = login_table(&config.auth.users, iterations)?;

        let permits = Arc::new(Semaphore::new(config.listen.max_client_connections.max(1)));
        let kdf = KdfPool::new(config.auth.kdf_concurrency);
        let fleet = Fleet::build(&config, &metrics)?;
        metrics.in_doubt(fleet.default_instance().fence.fence.in_doubt().len());

        Ok(Arc::new(Self {
            dynamic: ArcSwap::from_pointee(Dynamic {
                tenants: config.pool.tenants.clone(),
                users: config.auth.users.clone(),
                auth: Arc::new(ClientAuth::new(users, iterations)?),
            }),
            tenant: TenantResolver::new(&config.routing),
            config,
            acceptor,
            kdf,
            cancels: CancelRegistry::new(),
            metrics,
            fleet,
            quiesce: QuiesceRegistry::new(),
            permits,
        }))
    }

    /// Everything the client leg needs to authenticate a peer, as last published.
    ///
    /// Load it once per connection and hold the result: the exchange and the `admits` check
    /// that follows it must run against the same generation, or a login could prove itself
    /// against one table and be bound against another.
    pub fn auth(&self) -> Arc<ClientAuth> {
        Arc::clone(&self.dynamic.load().auth)
    }

    /// Takes up the half of `next` this process can apply without being restarted, and reports
    /// whether anything actually moved.
    ///
    /// The structural half is deliberately not applied: the process was built from it, and a
    /// running proxy that claimed otherwise would disagree with the document it says it serves.
    pub fn adopt(&self, next: &Config) -> Result<bool> {
        let current = self.dynamic.load();
        let logins_changed = current.users != next.auth.users;
        if !logins_changed && current.tenants == next.pool.tenants {
            return Ok(false);
        }
        // Trust mode is a start-up decision and has to stay one. `ClientAuth::is_trust` admits
        // every client with no challenge at all and binds it to whatever tenant it names, so a
        // replica that adopted a document which had lost its logins would turn an authenticating
        // fleet into an open one - inside one interval, with nothing rolled, because
        // `Config::structural` clears `auth.users` precisely so that editing them does not
        // restart anybody.
        //
        // And an empty list is not a thing an operator asks for. The control plane drops any
        // login whose credentials Secret it cannot read, so one unreadable Secret per tenant is
        // enough to render this document; `proxyUsers` says an empty list "is reported rather
        // than quietly accepted", and no such report exists. Refusing here is the half of that
        // sentence which is true.
        if !current.users.is_empty() && next.auth.users.is_empty() {
            return Err(ProxyError::config(
                "the published configuration carries no logins while this replica is \
                 authenticating against some; refusing it rather than admitting every client \
                 without a challenge",
            ));
        }
        let auth = if logins_changed {
            let iterations = NonZeroU32::new(self.config.auth.scram_iterations)
                .ok_or_else(|| ProxyError::config("auth.scramIterations must be non-zero"))?;
            Arc::new(current.auth.rebuild(adopted_login_table(
                &current,
                &next.auth.users,
                iterations,
            )?))
        } else {
            Arc::clone(&current.auth)
        };
        self.dynamic.store(Arc::new(Dynamic {
            tenants: next.pool.tenants.clone(),
            users: next.auth.users.clone(),
            auth,
        }));
        Ok(true)
    }

    /// The control-plane facade over this proxy's fleet.
    pub fn control(&self) -> crate::control::Control {
        crate::control::Control {
            fleet: Arc::clone(&self.fleet),
            quiesce: Arc::clone(&self.quiesce),
            metrics: Arc::clone(&self.metrics),
            config: self.config.control.clone(),
        }
    }
}

/// Turns the published logins into the table the client leg authenticates against.
///
/// A malformed verifier is refused here rather than at login, so a document that cannot
/// authenticate anybody fails where somebody is looking. On the adoption path that means one
/// bad login costs the whole adoption and the replica keeps the table it had - which is the
/// right way round, because the alternative is a fleet half of which admits a login the rest
/// refuses.
fn login_table(
    users: &[crate::config::UserConfig],
    iterations: NonZeroU32,
) -> Result<HashMap<Vec<u8>, crate::handshake::UserRecord>> {
    let mut table = HashMap::with_capacity(users.len());
    for user in users {
        table.insert(
            user.name.as_bytes().to_vec(),
            login_record(user, iterations)?,
        );
    }
    Ok(table)
}

/// The next login table, keeping the record of every login whose published configuration is
/// unchanged.
///
/// The reuse is a security property. `ScramVerifier::from_password` mints a fresh random salt
/// on every build and the operator renders `password` rather than `verifier` for every login,
/// so rebuilding an unchanged record would move that login's challenge salt. Paired with a
/// carried mock secret - which holds an *unknown* login's salt still - that would make the two
/// distinguishable across any publication an attacker can provoke. See `ClientAuth::rebuild`.
///
/// It also keeps a PBKDF2 per login off the reload tick, but that is the lesser reason.
fn adopted_login_table(
    previous: &Dynamic,
    next: &[crate::config::UserConfig],
    iterations: NonZeroU32,
) -> Result<HashMap<Vec<u8>, crate::handshake::UserRecord>> {
    let mut table = HashMap::with_capacity(next.len());
    for user in next {
        let key = user.name.as_bytes().to_vec();
        let carried = previous
            .users
            .iter()
            .any(|previous| previous == user)
            .then(|| previous.auth.record(&key))
            .flatten();
        match carried {
            Some(record) => table.insert(key, record.clone()),
            None => table.insert(key, login_record(user, iterations)?),
        };
    }
    Ok(table)
}

/// One login's record: what proves it, and which tenant it may be.
fn login_record(
    user: &crate::config::UserConfig,
    iterations: NonZeroU32,
) -> Result<crate::handshake::UserRecord> {
    let verifier = match (&user.verifier, &user.password) {
        (Some(secret), _) => ScramVerifier::parse(secret)
            .map_err(|e| ProxyError::config(format!("auth user {:?}: {e}", user.name)))?,
        (None, Some(password)) => ScramVerifier::from_password(
            password,
            crate::scram::crypto::random_bytes::<{ crate::scram::verifier::SALT_LEN }>()?.to_vec(),
            iterations,
        ),
        (None, None) => {
            return Err(ProxyError::config(format!(
                "auth user {:?} has neither a verifier nor a password",
                user.name
            )));
        }
    };
    Ok(crate::handshake::UserRecord {
        verifier,
        tenant: user.tenant.clone(),
    })
}

/// How long the supervisor waits beyond a session's own forced-drain deadline.
const DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// A running listener.
#[derive(Debug)]
pub struct Running {
    pub address: SocketAddr,
    /// Where the control listener actually bound, when one is configured.
    ///
    /// Reported rather than assumed, because a configured port of 0 means "any
    /// free one" and only the kernel knows which. A caller that wants an
    /// ephemeral port and has to guess it instead has to bind a scratch socket,
    /// read its port and close it - and anything else on the host may take that
    /// port in the gap. That race is not hypothetical: it is what made this
    /// field necessary.
    pub control_address: Option<SocketAddr>,
    /// Where the epoch push endpoint bound, on the same reasoning.
    pub push_address: Option<SocketAddr>,
    shutdown: watch::Sender<bool>,
    idle: mpsc::Receiver<std::convert::Infallible>,
    accept: tokio::task::JoinHandle<()>,
    drain_timeout: std::time::Duration,
}

impl Running {
    /// Signals a drain and waits for every session to finish.
    ///
    /// Returns `true` if all of them reached an idle boundary within the
    /// configured window.
    pub async fn shutdown(mut self) -> bool {
        let _ = self.shutdown.send(true);
        self.accept.abort();
        let _ = self.accept.await;
        // Every session task holds a clone of the sender, so the channel closes
        // exactly when the last one has finished.
        //
        // The grace matters: a session that hits its own forced-drain deadline
        // closes at exactly `drain_timeout`, so waiting for precisely that long
        // would report a timeout for a drain that did in fact finish.
        tokio::time::timeout(self.drain_timeout + DRAIN_GRACE, self.idle.recv())
            .await
            .is_ok()
    }
}

/// Binds the listener and starts accepting.
pub async fn spawn(proxy: Arc<Proxy>, shutdown: watch::Sender<bool>) -> Result<Running> {
    let addr = crate::config::resolve(&proxy.config.listen.address).await?;
    let listener = TcpListener::bind(addr).await?;
    let address = listener.local_addr()?;
    let drain_timeout = proxy.config.drain.shutdown_timeout();

    let (alive, idle) = mpsc::channel::<std::convert::Infallible>(1);
    let receiver = shutdown.subscribe();
    let push_address = spawn_fence_paths(&proxy, &shutdown).await?;
    let control_address = spawn_availability_paths(&proxy, &shutdown).await?;
    let accept = tokio::spawn(accept_loop(proxy, listener, receiver, alive));

    info!(%address, "proxy listening");
    Ok(Running {
        address,
        control_address,
        push_address,
        shutdown,
        idle,
        accept,
        drain_timeout,
    })
}

/// Starts the sweeper and whichever of the two optional delivery paths are
/// configured.
///
/// The verify path needs nothing started: it runs inside every checkout, which
/// is what makes it the one that survives a partition.
async fn spawn_fence_paths(
    proxy: &Arc<Proxy>,
    shutdown: &watch::Sender<bool>,
) -> Result<Option<SocketAddr>> {
    for instance in proxy.fleet.instances() {
        tokio::spawn(sweep_loop(
            Arc::clone(instance),
            Arc::clone(&proxy.metrics),
            shutdown.subscribe(),
        ));

        if proxy.config.fence.verify_at_checkout {
            tokio::spawn(crate::epoch::verify::probe_loop(
                crate::epoch::verify::Prober {
                    backend: instance.backend.clone(),
                    tls: instance.tls.clone(),
                    kdf: proxy.kdf.clone(),
                    fence: Arc::clone(&instance.fence.fence),
                    metrics: Arc::clone(&proxy.metrics),
                },
                shutdown.subscribe(),
            ));
        }
    }

    // Push and watch address a single `PgInstance`, so they are wired to the
    // default one. A fleet learns the other instances' epochs over the pull
    // path, which is the one that has to work regardless.
    let default = proxy.fleet.default_instance();
    let mut push_address = None;
    if let Some(address) = &proxy.config.fence.push_address {
        let listener = TcpListener::bind(crate::config::resolve(address).await?).await?;
        let bound = listener.local_addr()?;
        info!(address = %bound, "epoch push endpoint listening");
        push_address = Some(bound);
        tokio::spawn(crate::epoch::admin::serve(
            listener,
            Arc::clone(&default.fence.fence),
            shutdown.subscribe(),
        ));
    }

    if let Some(watch) = &proxy.config.fence.watch {
        tokio::spawn(crate::epoch::watch::run(
            crate::epoch::watch::WatchTarget {
                namespace: watch.namespace.clone(),
                name: watch.name.clone(),
            },
            Arc::clone(&default.fence.fence),
            shutdown.subscribe(),
        ));
    }
    Ok(push_address)
}

/// Starts write-stall detection and the control API.
///
/// The stall probe runs per instance and the control listener is one for the
/// proxy: a stall is a property of a primary, and a cutover is a property of
/// the routing table that spans them.
async fn spawn_availability_paths(
    proxy: &Arc<Proxy>,
    shutdown: &watch::Sender<bool>,
) -> Result<Option<SocketAddr>> {
    if proxy.config.stall.enabled {
        for instance in proxy.fleet.instances() {
            tokio::spawn(crate::stall::probe_loop(
                crate::stall::StallProbe {
                    backend: instance.backend.clone(),
                    tls: instance.tls.clone(),
                    kdf: proxy.kdf.clone(),
                    monitor: Arc::clone(&instance.stall),
                    metrics: Arc::clone(&proxy.metrics),
                },
                proxy.config.stall.interval(),
                shutdown.subscribe(),
            ));
        }
    }

    let control = proxy.control();
    // Swept far more often than a lease lasts: the deadline a killed operator
    // is held to has to be the lease, not the lease plus however coarse the
    // sweep happens to be. Capped as well as floored, so a long default TTL
    // cannot stretch the sweep past the shortest lease a caller may ask for.
    let sweep = (proxy.config.control.default_lease_ttl() / 20).clamp(REAP_FLOOR, REAP_CEILING);
    tokio::spawn(crate::control::reap_loop(
        control.clone(),
        sweep,
        shutdown.subscribe(),
    ));

    let mut control_address = None;
    if let Some(address) = &proxy.config.control.address {
        // Unwrapped rather than defended against: the configuration refuses an
        // address without TLS material, so reaching here without it would mean
        // validation had been bypassed.
        let tls =
            proxy.config.control.tls.as_ref().ok_or_else(|| {
                ProxyError::config("control.address is set but control.tls is not")
            })?;
        let authority = crate::tls::ControlAuthority::new(tls)?;
        let listener = TcpListener::bind(crate::config::resolve(address).await?).await?;
        let bound = listener.local_addr()?;
        info!(
            address = %bound,
            client = %tls.client_name,
            "control endpoint listening, mutual TLS required"
        );
        control_address = Some(bound);
        tokio::spawn(crate::control::serve(
            listener,
            control,
            authority,
            shutdown.subscribe(),
        ));
    }
    Ok(control_address)
}

impl Proxy {
    /// The backend configuration a tenant's sessions dial with.
    ///
    /// The instance supplies the address and the TLS posture; the tenant supplies the identity
    /// and the credential. That split is the whole change: `session_user` on the far side
    /// becomes the tenant's own role rather than the control plane's, so `pg_stat_activity`,
    /// `log_line_prefix` and any audit extension attribute a statement to whoever ran it.
    /// `SET ROLE` could not have done this - it moves `current_user` and leaves `session_user`
    /// alone, and `session_user` is the one auditing follows.
    ///
    /// A tenant with no credential is refused rather than dialled on the instance identity. The
    /// fallback is the dangerous option: it would put tenant SQL back on `pgelastic_ops`
    /// silently, during a config-propagation lag, which is exactly when nobody is looking.
    pub fn backend_for(
        &self,
        instance: &crate::route::Instance,
        tenant: &str,
    ) -> Result<crate::config::BackendConfig> {
        let dynamic = self.dynamic.load();
        let Some(entry) = dynamic.tenants.iter().find(|t| t.name == tenant) else {
            // Not an error: a tenant with no [[pool.tenants]] entry at all is the
            // single-tenant shape, which has no per-tenant identity to assume.
            return Ok(instance.backend.clone());
        };
        if entry.backend_role.is_empty() {
            return Ok(instance.backend.clone());
        }
        if entry.backend_salted_password.is_empty() {
            return Err(ProxyError::config(format!(
                "tenant {tenant:?} names backend role {:?} but carries no credential for it; \
                 refusing rather than dialling as the control plane's own role",
                entry.backend_role
            )));
        }
        let mut backend = instance.backend.clone();
        backend.user.clone_from(&entry.backend_role);
        backend.salted_password = Some(crate::config::SaltedSecret {
            salted_password: entry.backend_salted_password.clone(),
            salt: entry.backend_salt.clone(),
            iterations: entry.backend_iterations,
        });
        Ok(backend)
    }

    /// The credential generation a tenant's pooled links are keyed on, so a rotation makes the
    /// old ones unreachable rather than reusable.
    pub fn credential_generation(&self, tenant: &str) -> u64 {
        self.dynamic
            .load()
            .tenants
            .iter()
            .find(|t| t.name == tenant)
            .map_or(0, |t| t.credential_generation)
    }
}

/// The shortest lease sweep interval, so a very short TTL cannot turn the
/// sweeper into a spin loop.
const REAP_FLOOR: std::time::Duration = std::time::Duration::from_millis(10);
/// The longest one, so how late an expiry is noticed is bounded by this rather
/// than by whatever the default TTL happens to be.
const REAP_CEILING: std::time::Duration = std::time::Duration::from_millis(250);

/// Severs superseded parked sockets the moment the epoch moves.
///
/// Every checkout sweeps too, so this is not what makes the fence correct — it
/// is what makes it prompt on an idle pool, where the next checkout might be
/// minutes away and the sockets would sit there ESTABLISHED in the meantime.
async fn sweep_loop(
    instance: Arc<Instance>,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) {
    let fence = instance.fence.clone();
    let pools = Arc::clone(&instance.pools);
    let mut epochs = fence.fence.subscribe();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            changed = epochs.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
        let epoch = *epochs.borrow_and_update();
        metrics.primary_epoch(epoch);
        let severed = pools.sever_superseded();
        if let Some(advanced_at) = fence.fence.advanced_at() {
            let elapsed = advanced_at.elapsed();
            metrics.fence_latency(elapsed);
            info!(
                %epoch,
                severed,
                elapsed_us = elapsed.as_micros(),
                deadline_ms = fence.fence.timing().fence_deadline().as_millis(),
                "the epoch fence completed"
            );
        }
    }
}

async fn accept_loop(
    proxy: Arc<Proxy>,
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
    alive: mpsc::Sender<std::convert::Infallible>,
) {
    loop {
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            result = listener.accept() => result,
        };
        let Ok((socket, peer)) = accepted else {
            // An accept error is per-connection (fd exhaustion, RST between
            // SYN and accept); tearing the listener down over one would turn a
            // transient fault into an outage.
            continue;
        };

        let Ok(permit) = Arc::clone(&proxy.permits).try_acquire_owned() else {
            proxy.metrics.client_rejected(RejectReason::ConnectionLimit);
            tokio::spawn(refuse(socket, ProxyError::ConnectionLimit));
            continue;
        };

        proxy.metrics.client_accepted();
        let proxy = Arc::clone(&proxy);
        let receiver = shutdown.clone();
        let alive = alive.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _alive = alive;
            if let Err(error) = serve(&proxy, socket, receiver).await {
                debug!(%peer, %error, "client session ended with an error");
            }
            proxy.metrics.client_closed();
        });
    }
}

/// Refuses a client that never got a permit, in a way libpq understands.
async fn refuse(socket: TcpStream, error: ProxyError) {
    let mut stream: ClientStream =
        crate::stream::MaybeTls::Plain(crate::stream::Prefixed::new(bytes::Bytes::new(), socket));
    handshake::report(&mut stream, None, &error).await;
}

async fn serve(
    proxy: &Arc<Proxy>,
    socket: TcpStream,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let login = proxy.config.listen.client_login_timeout();
    let accepted = tokio::time::timeout(
        login,
        handshake::negotiate(
            socket,
            proxy.acceptor.as_ref(),
            proxy.config.listen.require_tls,
        ),
    )
    .await
    .map_err(|_| ProxyError::Timeout(login))??;

    let mut session = match accepted {
        Accepted::Cancel(request) => return cancel(proxy, &request).await,
        Accepted::Session(session) => session,
    };

    if *shutdown.borrow_and_update() {
        proxy.metrics.client_rejected(RejectReason::ShuttingDown);
        handshake::report(&mut session.stream, None, &ProxyError::ShuttingDown).await;
        return Err(ProxyError::ShuttingDown);
    }

    handshake::negotiate_protocol_version(&mut session).await?;

    let user = session.user()?.clone();
    // Loaded once and held. Proving a password and binding the login to a tenant are two
    // reads of the same table, and an adoption landing between them would decide the second
    // against a table the first never saw.
    let auth = proxy.auth();
    if let Err(error) = handshake::authenticate_client(&mut session, &auth).await {
        proxy.metrics.client_auth(AuthOutcome::Failure);
        proxy.metrics.client_rejected(RejectReason::Handshake);
        handshake::report(&mut session.stream, Some(&user), &error).await;
        return Err(error);
    }
    proxy.metrics.client_auth(AuthOutcome::Success);

    let role = String::from_utf8_lossy(&user).into_owned();
    // Everything downstream is keyed on the tenant: which instance holds the
    // data, whose budget the checkout is charged to, which gate a cutover
    // closes. So it is established once, here, from the startup packet the
    // client actually sent.
    let tenant = match proxy.tenant.resolve(&session.startup, &role) {
        Ok(tenant) => tenant,
        Err(error) => {
            proxy.metrics.client_rejected(RejectReason::Handshake);
            handshake::report(&mut session.stream, Some(&user), &error).await;
            return Err(error);
        }
    };
    // Authenticating and choosing a tenant read different parts of the same
    // startup packet, so proving one says nothing about the other. Until they
    // are related here, a client holding one tenant's password reaches any
    // tenant it can name - which is the whole of what "the identity is confined
    // to the database" has to mean.
    //
    // Refused as an authentication failure, with the same error and the same
    // metrics as a wrong password, because the alternative tells the caller that
    // the credential was good and only the tenant was wrong. That is an oracle
    // for which tenants exist and which login belongs to which.
    if !auth.admits(&user, tenant.as_str()) {
        proxy.metrics.client_auth(AuthOutcome::Failure);
        proxy.metrics.client_rejected(RejectReason::Handshake);
        let error = ProxyError::AuthenticationFailed;
        handshake::report(&mut session.stream, Some(&user), &error).await;
        return Err(error);
    }
    // The route is read here only to decide the pool key's shape and to serve a
    // session-mode client. A transaction-mode client re-reads it at every
    // checkout, which is what lets a cutover move it without it reconnecting.
    let instance = proxy.fleet.route(&tenant);
    let key = match crate::pool::pool_key(
        &proxy.config,
        &proxy.backend_for(&instance, &tenant)?,
        &session.startup,
        &tenant,
        proxy.credential_generation(&tenant),
    )
    .await
    {
        Ok(key) => key,
        Err(error) => {
            proxy.metrics.client_rejected(RejectReason::Handshake);
            handshake::report(&mut session.stream, Some(&user), &error).await;
            return Err(error);
        }
    };

    // A replication connection is forced to session mode by the pool key
    // itself: a walsender stream has no transaction boundaries to release on,
    // and multiplexing one silently corrupts it.
    let result = if key.mode() == pgelastic_pool::PoolMode::Transaction {
        multiplexed(proxy, &mut session, &tenant, &mut shutdown).await
    } else {
        bound(proxy, &mut session, &instance, &tenant, &mut shutdown).await
    };

    if let Err(error) = &result
        && !matches!(error, ProxyError::PeerGone | ProxyError::Io(_))
    {
        handshake::report(&mut session.stream, Some(&user), error).await;
    }
    result
}

/// Session mode: one client, one backend, for the client's whole life.
async fn bound(
    proxy: &Arc<Proxy>,
    session: &mut handshake::ClientSession,
    instance: &Arc<Instance>,
    tenant: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    // A session-mode client holds one backend for its whole life, so refusing
    // it here is the only chance the stall detector gets: once it is connected
    // there is no boundary at which to take the connection back.
    //
    // A replication connection is exempt, and the exemption is not a courtesy:
    // a walsender is how a standby rejoins, and rejoining is what ends the
    // stall. Refusing it would make the proxy the reason the instance cannot
    // recover.
    let replication = session.startup.get(b"replication").is_some();
    if !replication && let Some(health) = instance.stall.must_refuse() {
        proxy.metrics.write_stall_refused();
        proxy.metrics.client_rejected(RejectReason::Backend);
        return Err(write_stalled(&instance.id, health));
    }
    // A session-mode client never reaches an idle boundary, so it counts as
    // permanently in flight: a cutover must not believe a tenant has drained
    // while one of its sessions is still writing to the source.
    let _holding = proxy.quiesce.gate(tenant).hold();

    let mut link = match backend::connect(
        &proxy.backend_for(instance, tenant)?,
        instance.tls.as_ref(),
        &proxy.kdf,
        &session.startup,
    )
    .await
    {
        Ok(link) => link,
        Err(error) => {
            proxy.metrics.backend_auth(AuthOutcome::Failure);
            proxy.metrics.client_rejected(RejectReason::Backend);
            return Err(error);
        }
    };
    proxy.metrics.backend_auth(AuthOutcome::Success);
    proxy.metrics.backend_opened();

    let fenced = match verify_bound(proxy, instance, session, &mut link, tenant).await {
        Ok(fenced) => fenced,
        Err(error) => {
            proxy.metrics.client_rejected(RejectReason::Backend);
            proxy
                .metrics
                .backend_severed(crate::epoch::FenceAction::Close);
            proxy.metrics.backend_closed();
            link.stream.sever();
            return Err(error);
        }
    };

    let result = relay(proxy, instance, session, &mut link, &fenced, shutdown).await;
    proxy.metrics.backend_closed();
    result
}

/// The refusal a client gets when its instance cannot complete a commit.
pub(crate) fn write_stalled(
    instance: &crate::route::InstanceId,
    health: crate::stall::WriteHealth,
) -> ProxyError {
    ProxyError::WriteStalled {
        message: format!(
            "instance {instance} is {health}, so a COMMIT there would block indefinitely.              No backend was taken and nothing was forwarded, so this transaction did not              happen. Retrying will not help until quorum returns or this tenant is moved"
        ),
    }
}

/// Tags a bound link with the epoch it proved it was serving.
///
/// A replication connection is deliberately not probed: a physical walsender
/// answers replication commands and nothing else, so a `SELECT` there is a
/// protocol error rather than a verification. It is tagged with what the proxy
/// knows instead, which still lets a later push or watch sever it — a
/// replication stream to a demoted primary is exactly as dangerous as a client
/// one.
async fn verify_bound<'a>(
    proxy: &Arc<Proxy>,
    instance: &'a Arc<Instance>,
    session: &handshake::ClientSession,
    link: &mut backend::BackendSession,
    tenant: &str,
) -> Result<session::Fenced<'a>> {
    let mut fenced = session::Fenced {
        runtime: &instance.fence,
        opened_under: instance.fence.current(),
        tenant: tenant.to_owned(),
        backend_pid: link.key_data.as_ref().map(|data| data.process_id),
        lsn: None,
    };

    for message in &link.parameters {
        if let BackendMessage::ParameterStatus(status) = message
            && let Some((epoch, observation)) =
                crate::epoch::verify::observe_parameter_status(&instance.fence.fence, status)
        {
            proxy
                .metrics
                .epoch_observed(crate::epoch::EpochSource::Verify, observation.into());
            fenced.opened_under = epoch;
        }
    }

    let replication = session.startup.get(b"replication").is_some();
    if proxy.config.fence.verify_at_checkout && !replication {
        let mut relay = crate::relay::FrameRelay::default();
        relay.extend_from_slice(link.buf.as_slice());
        let probe = crate::epoch::verify::probe(&mut link.stream, &mut relay).await?;
        // Whatever the probe read past its own answer belongs to the session.
        link.buf = pgelastic_wire::MessageBuffer::new();
        fenced.backend_pid = probe.backend_pid.or(fenced.backend_pid);
        fenced.lsn = probe.lsn;
        match probe.epoch {
            Some(epoch) => {
                let observation = instance
                    .fence
                    .fence
                    .observe(crate::epoch::EpochSource::Verify, epoch);
                proxy
                    .metrics
                    .epoch_observed(crate::epoch::EpochSource::Verify, observation.into());
                fenced.opened_under = epoch;
            }
            None if proxy.config.fence.require_epoch => {
                return Err(ProxyError::SupersededEpoch {
                    message: "this backend carries no pgelastic.primary_epoch, so the epoch it \
                              is serving cannot be established"
                        .to_owned(),
                });
            }
            None => {}
        }
    }
    proxy.metrics.primary_epoch(instance.fence.current());

    if fenced.opened_under < instance.fence.current() {
        return Err(ProxyError::SupersededEpoch {
            message: format!(
                "this backend is serving primary epoch {} and the cluster has reached {}",
                fenced.opened_under,
                instance.fence.current()
            ),
        });
    }
    Ok(fenced)
}

/// Greets the client with the backend's session state, then relays until the
/// session ends.
async fn relay(
    proxy: &Arc<Proxy>,
    instance: &Arc<Instance>,
    session: &mut handshake::ClientSession,
    link: &mut backend::BackendSession,
    fenced: &session::Fenced<'_>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let token = CancelToken::mint(proxy.config.routing.cancel_routing_id)?;
    let route = CancelRoute::new();
    route.set(Some(crate::cancel::CancelTarget {
        address: std::sync::Arc::from(instance.backend.address.as_str()),
        key_data: link.key_data.clone(),
        instance: instance.id.clone(),
        // Session mode: the client owns its one backend for life and takes
        // nothing from the allocator, so there is no credit to charge.
        client: None,
    }));
    let _registration = proxy.cancels.register(token.clone(), route);

    let mut greeting = vec![BackendMessage::Authentication(
        pgelastic_wire::Authentication::Ok,
    )];
    greeting.extend(link.parameters.iter().cloned());
    greeting.push(BackendMessage::BackendKeyData(token.key_data()?));
    greeting.push(BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    crate::wire_io::write_backend(&mut session.stream, &greeting).await?;
    // Dropped rather than left to the end of the scope. It has already been written to the
    // client, and a local that is merely unused still occupies the coroutine frame at every
    // await inside its scope -- so leaving it here costs its capacity on every connection for
    // as long as that connection lives, and widens the task allocation for everyone.
    drop(greeting);

    let pending_from_client = session.buf.as_slice().to_vec();
    let pending_from_backend = link.buf.as_slice().to_vec();
    // The handshake buffer has done its job, and it is 8 KiB that would otherwise stay resident
    // for as long as the client is connected -- `session` outlives it because it owns the
    // stream. Released the same way the backend link's is above, which is where this pattern
    // came from; the client side simply never did it.
    session.buf = pgelastic_wire::MessageBuffer::new();
    link.buf = pgelastic_wire::MessageBuffer::new();

    let ending = session::run(
        &mut session.stream,
        &mut link.stream,
        session::Context {
            pending: session::Pending {
                from_client: &pending_from_client,
                from_backend: &pending_from_backend,
            },
            limits: limits(proxy),
            metrics: &proxy.metrics,
            force_after: proxy.config.drain.shutdown_timeout(),
            fence: fenced,
        },
        shutdown,
    )
    .await;

    match ending {
        Ok(Ending::Drained | Ending::Forced) => {
            let forced = matches!(ending, Ok(Ending::Forced));
            proxy.metrics.drain_completed(forced);
            session::close_for_drain(&mut session.stream, forced).await;
            session::terminate_backend(&mut link.stream).await;
            Ok(())
        }
        Ok(Ending::PeerClosed) => {
            session::terminate_backend(&mut link.stream).await;
            Ok(())
        }
        // A fenced session's socket has already been armed for an RST, and a
        // `Terminate` on it would be one more message the demoted primary is
        // free to finish its current statement before honouring.
        Err(error @ (ProxyError::SupersededEpoch { .. } | ProxyError::OutcomeUnknown { .. })) => {
            Err(error)
        }
        Err(error) => {
            session::terminate_backend(&mut link.stream).await;
            Err(error)
        }
    }
}

/// Transaction mode: the client keeps its socket, the backend changes under it.
async fn multiplexed(
    proxy: &Arc<Proxy>,
    session: &mut handshake::ClientSession,
    tenant: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let gate = proxy.quiesce.gate(tenant);
    let binding = crate::txn::Binding::open(proxy, &session.startup, tenant).await?;

    // The greeting is the first link's `ParameterStatus` set, cached per pool
    // key: a client that arrives twentieth must not have to hold a backend just
    // to be told the server's `TimeZone`.
    let parameters = if let Some(cached) = binding.instance.pools.greeting(&binding.key) {
        cached
    } else {
        // Opening the first link of a pool is a checkout like any other, so it
        // waits at the tenant's gate: a client that connects during a cutover
        // must not open a backend on the source the cutover is trying to drain.
        let baton = gate.admit().await;
        let held = gate.hold();
        let parameters = crate::txn::bootstrap_greeting(
            &binding,
            &proxy.kdf,
            &session.startup,
            &mut session.stream,
            &proxy.metrics,
        )
        .await;
        drop(held);
        drop(baton);
        parameters?
    };

    let mut client_vars = crate::vars::VariableCache::new();
    for message in parameters.iter() {
        if let BackendMessage::ParameterStatus(status) = message {
            client_vars.observe(&status.name, &status.value);
        }
    }

    let token = CancelToken::mint(proxy.config.routing.cancel_routing_id)?;
    let route = CancelRoute::new();
    let _registration = proxy.cancels.register(token.clone(), route.clone());

    let mut greeting = vec![BackendMessage::Authentication(
        pgelastic_wire::Authentication::Ok,
    )];
    greeting.extend(parameters.iter().cloned());
    greeting.push(BackendMessage::BackendKeyData(token.key_data()?));
    greeting.push(BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    crate::wire_io::write_backend(&mut session.stream, &greeting).await?;
    // Dropped rather than left to the end of the scope. It has already been written to the
    // client, and a local that is merely unused still occupies the coroutine frame at every
    // await inside its scope -- so leaving it here costs its capacity on every connection for
    // as long as that connection lives, and widens the task allocation for everyone.
    drop(greeting);

    let pending = session.buf.as_slice().to_vec();
    // The handshake buffer has done its job, and it is 8 KiB that would otherwise stay resident
    // for as long as the client is connected -- `session` outlives it because it owns the
    // stream. Released the same way the backend link's is above, which is where this pattern
    // came from; the client side simply never did it.
    session.buf = pgelastic_wire::MessageBuffer::new();

    let ending = crate::txn::run(
        crate::txn::Session {
            client: &mut session.stream,
            proxy,
            startup: &session.startup,
            tenant: tenant.to_owned(),
            gate,
            binding,
            route,
            metrics: &proxy.metrics,
            limits: limits(proxy),
            client_vars,
            force_after: proxy.config.drain.shutdown_timeout(),
        },
        &pending,
        shutdown,
    )
    .await;

    match ending {
        Ok(Ending::Drained | Ending::Forced) => {
            let forced = matches!(ending, Ok(Ending::Forced));
            proxy.metrics.drain_completed(forced);
            session::close_for_drain(&mut session.stream, forced).await;
            Ok(())
        }
        Ok(Ending::PeerClosed) => Ok(()),
        Err(error) => Err(error),
    }
}

fn limits(proxy: &Arc<Proxy>) -> session::Limits {
    session::Limits {
        inline_frame_bytes: proxy.config.limits.inline_frame_bytes,
        max_frame_bytes: proxy.config.limits.max_frame_bytes,
    }
}

async fn cancel(proxy: &Arc<Proxy>, request: &pgelastic_wire::CancelRequest) -> Result<()> {
    let token = CancelToken::from(request);
    // Resolved here rather than at registration: under transaction pooling the
    // client's query is running on whichever backend it holds at this instant,
    // and the one it held a moment ago now belongs to somebody else.
    let Some(route) = proxy.cancels.lookup(&token) else {
        // Silently dropped, exactly as PostgreSQL does: answering would confirm
        // which keys exist and turn the cancel port into an oracle.
        proxy.metrics.cancel(false);
        return Ok(());
    };
    // Marked before the target is read and held until the request is on the
    // wire: the session that owns the route must not release its backend inside
    // that window, or the cancel arrives at somebody else's statement.
    let _in_flight = route.dispatching();
    let Some(target) = route.resolve() else {
        proxy.metrics.cancel(false);
        return Ok(());
    };
    // The instance comes off the target rather than off the session, because a
    // tenant migrated mid-session is holding a backend on the new one and its
    // credit has to be drawn from that instance's allocator.
    let Some(instance) = proxy.fleet.get(&target.instance).map(Arc::clone) else {
        proxy.metrics.cancel(false);
        return Ok(());
    };
    // Step 0 of the admission ladder. A cancel opens a real backend socket, so
    // it is admitted like one — but from its own bounded credit pool, which is
    // both why a cancel storm cannot eat the tenant's capacity and why a tenant
    // pinned at its burst ceiling can still cancel.
    let _credit = match target.client {
        Some(client) => match instance.pools.lease_cancel(client) {
            Ok(credit) => Some(credit),
            Err(denial) => {
                proxy.metrics.cancel_refused();
                debug!(%denial, "a cancel request was refused by the cancel credit pool");
                return Ok(());
            }
        },
        None => None,
    };
    proxy.metrics.cancel(true);
    if let Err(error) = crate::cancel::deliver(
        &target,
        instance.tls.as_ref(),
        instance.backend.connect_timeout(),
    )
    .await
    {
        warn!(%error, "delivering a cancel request to the backend failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::str::FromStr as _;

    fn config(source: &str) -> Config {
        Config::from_str(source).unwrap()
    }

    const MINIMAL: &str = r#"
        [listen]
        address = "127.0.0.1:0"

        [backend]
        address = "127.0.0.1:1"
        user = "postgres"
    "#;

    #[tokio::test]
    async fn a_listener_binds_and_reports_its_address() {
        let proxy = Proxy::new(config(MINIMAL), Metrics::new()).unwrap();
        let (tx, _rx) = watch::channel(false);
        let running = spawn(proxy, tx).await.unwrap();
        assert_ne!(running.address.port(), 0);
        assert!(running.shutdown().await);
    }

    #[tokio::test]
    async fn a_user_configured_with_a_password_gets_a_usable_verifier() {
        let source =
            format!("{MINIMAL}\n[[auth.users]]\nname = \"alice\"\npassword = \"hunter2\"\n");
        let proxy = Proxy::new(config(&source), Metrics::new()).unwrap();
        assert!(!proxy.auth().is_trust());
    }

    #[tokio::test]
    async fn a_malformed_verifier_is_refused_at_start_up_not_at_login() {
        let source =
            format!("{MINIMAL}\n[[auth.users]]\nname = \"alice\"\nverifier = \"md5deadbeef\"\n");
        let error = Proxy::new(config(&source), Metrics::new()).unwrap_err();
        assert!(matches!(error, ProxyError::Config(_)));
    }

    #[tokio::test]
    async fn a_client_over_the_limit_is_refused_rather_than_queued() {
        let source = MINIMAL.replace(
            "address = \"127.0.0.1:0\"",
            "address = \"127.0.0.1:0\"\nmaxClientConnections = 1",
        );
        let proxy = Proxy::new(config(&source), Metrics::new()).unwrap();
        let metrics = Arc::clone(&proxy.metrics);
        let (tx, _rx) = watch::channel(false);
        let running = spawn(proxy, tx).await.unwrap();

        let _held = TcpStream::connect(running.address).await.unwrap();
        // The permit is taken by the task the first connection spawned, so the
        // second has to wait for that task to exist before it is refused.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let refused = TcpStream::connect(running.address).await.unwrap();

        let mut buf = pgelastic_wire::MessageBuffer::new();
        let mut refused = refused;
        let message = crate::wire_io::read_backend_message(&mut refused, &mut buf)
            .await
            .unwrap();
        let BackendMessage::ErrorResponse(fields) = message else {
            panic!("expected an ErrorResponse");
        };
        assert_eq!(fields.sqlstate().unwrap(), "53300");
        assert!(metrics.render().contains("reason=\"connection_limit\"} 1"));
        running.shutdown().await;
    }
}
