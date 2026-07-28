//! The listener, the per-connection task, and graceful drain.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use pgelastic_wire::{BackendMessage, TransactionStatus};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use crate::backend;
use crate::cancel::{CancelRegistry, CancelRoute, CancelToken};
use crate::config::Config;
use crate::epoch::FenceRuntime;
use crate::error::{ProxyError, Result};
use crate::handshake::{self, Accepted, ClientAuth};
use crate::metrics::{AuthOutcome, Metrics, RejectReason};
use crate::pool::PoolManager;
use crate::scram::{KdfPool, ScramVerifier};
use crate::session::{self, Ending};
use crate::stream::ClientStream;
use crate::tls::BackendTls;

/// Everything a connection task needs, shared by `Arc`.
pub struct Proxy {
    pub config: Config,
    pub acceptor: Option<TlsAcceptor>,
    pub backend_tls: Option<BackendTls>,
    pub auth: Arc<ClientAuth>,
    pub kdf: KdfPool,
    pub cancels: Arc<CancelRegistry>,
    pub metrics: Arc<Metrics>,
    pub pools: Arc<PoolManager>,
    pub fence: FenceRuntime,
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
        let backend_host = config
            .backend
            .address
            .rsplit_once(':')
            .map_or(config.backend.address.as_str(), |(host, _)| host);
        let backend_tls = crate::tls::backend_connector(&config.backend.tls, backend_host)?;

        let iterations = NonZeroU32::new(config.auth.scram_iterations)
            .ok_or_else(|| ProxyError::config("auth.scramIterations must be non-zero"))?;
        let mut verifiers = HashMap::new();
        for user in &config.auth.users {
            let verifier = match (&user.verifier, &user.password) {
                (Some(secret), _) => ScramVerifier::parse(secret)
                    .map_err(|e| ProxyError::config(format!("auth user {:?}: {e}", user.name)))?,
                (None, Some(password)) => ScramVerifier::from_password(
                    password,
                    crate::scram::crypto::random_bytes::<{ crate::scram::verifier::SALT_LEN }>()?
                        .to_vec(),
                    iterations,
                ),
                (None, None) => {
                    return Err(ProxyError::config(format!(
                        "auth user {:?} has neither a verifier nor a password",
                        user.name
                    )));
                }
            };
            verifiers.insert(user.name.as_bytes().to_vec(), verifier);
        }

        let permits = Arc::new(Semaphore::new(config.listen.max_client_connections.max(1)));
        let kdf = KdfPool::new(config.auth.kdf_concurrency);
        let fence = FenceRuntime::from(&config.fence);
        let pools = PoolManager::new(config.pool.clone(), fence.clone(), Arc::clone(&metrics))?;
        pools.publish_budget();
        metrics.in_doubt(fence.fence.in_doubt().len());

        Ok(Arc::new(Self {
            auth: Arc::new(ClientAuth::new(verifiers, iterations)?),
            config,
            acceptor,
            backend_tls,
            kdf,
            cancels: CancelRegistry::new(),
            metrics,
            pools,
            fence,
            permits,
        }))
    }
}

/// How long the supervisor waits beyond a session's own forced-drain deadline.
const DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// A running listener.
#[derive(Debug)]
pub struct Running {
    pub address: SocketAddr,
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
    let addr = crate::config::resolve(&proxy.config.listen.address)?;
    let listener = TcpListener::bind(addr).await?;
    let address = listener.local_addr()?;
    let drain_timeout = proxy.config.drain.shutdown_timeout();

    let (alive, idle) = mpsc::channel::<std::convert::Infallible>(1);
    let receiver = shutdown.subscribe();
    spawn_fence_paths(&proxy, &shutdown).await?;
    let accept = tokio::spawn(accept_loop(proxy, listener, receiver, alive));

    info!(%address, "proxy listening");
    Ok(Running {
        address,
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
async fn spawn_fence_paths(proxy: &Arc<Proxy>, shutdown: &watch::Sender<bool>) -> Result<()> {
    tokio::spawn(sweep_loop(
        Arc::clone(&proxy.pools),
        proxy.fence.clone(),
        Arc::clone(&proxy.metrics),
        shutdown.subscribe(),
    ));

    if proxy.config.fence.verify_at_checkout {
        tokio::spawn(crate::epoch::verify::probe_loop(
            crate::epoch::verify::Prober {
                backend: proxy.config.backend.clone(),
                tls: proxy.backend_tls.clone(),
                kdf: proxy.kdf.clone(),
                fence: Arc::clone(&proxy.fence.fence),
                metrics: Arc::clone(&proxy.metrics),
            },
            shutdown.subscribe(),
        ));
    }

    if let Some(address) = &proxy.config.fence.push_address {
        let listener = TcpListener::bind(crate::config::resolve(address)?).await?;
        info!(address = %listener.local_addr()?, "epoch push endpoint listening");
        tokio::spawn(crate::epoch::admin::serve(
            listener,
            Arc::clone(&proxy.fence.fence),
            shutdown.subscribe(),
        ));
    }

    if let Some(watch) = &proxy.config.fence.watch {
        tokio::spawn(crate::epoch::watch::run(
            crate::epoch::watch::WatchTarget {
                namespace: watch.namespace.clone(),
                name: watch.name.clone(),
            },
            Arc::clone(&proxy.fence.fence),
            shutdown.subscribe(),
        ));
    }
    Ok(())
}

/// Severs superseded parked sockets the moment the epoch moves.
///
/// Every checkout sweeps too, so this is not what makes the fence correct — it
/// is what makes it prompt on an idle pool, where the next checkout might be
/// minutes away and the sockets would sit there ESTABLISHED in the meantime.
async fn sweep_loop(
    pools: Arc<PoolManager>,
    fence: FenceRuntime,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) {
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
    if let Err(error) = handshake::authenticate_client(&mut session, &proxy.auth).await {
        proxy.metrics.client_auth(AuthOutcome::Failure);
        proxy.metrics.client_rejected(RejectReason::Handshake);
        handshake::report(&mut session.stream, Some(&user), &error).await;
        return Err(error);
    }
    proxy.metrics.client_auth(AuthOutcome::Success);

    let role = String::from_utf8_lossy(&user).into_owned();
    let key = match crate::pool::pool_key(&proxy.config, &session.startup, &role) {
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
        multiplexed(proxy, &mut session, &key, &role, &mut shutdown).await
    } else {
        bound(proxy, &mut session, &role, &mut shutdown).await
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
    role: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let mut link = match backend::connect(
        &proxy.config.backend,
        proxy.backend_tls.as_ref(),
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

    let fenced = match verify_bound(proxy, session, &mut link, role).await {
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

    let result = relay(proxy, session, &mut link, &fenced, shutdown).await;
    proxy.metrics.backend_closed();
    result
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
    proxy: &'a Arc<Proxy>,
    session: &handshake::ClientSession,
    link: &mut backend::BackendSession,
    role: &str,
) -> Result<session::Fenced<'a>> {
    let mut fenced = session::Fenced {
        runtime: &proxy.fence,
        opened_under: proxy.fence.current(),
        tenant: role.to_owned(),
        backend_pid: link.key_data.as_ref().map(|data| data.process_id),
        lsn: None,
    };

    for message in &link.parameters {
        if let BackendMessage::ParameterStatus(status) = message
            && let Some((epoch, observation)) =
                crate::epoch::verify::observe_parameter_status(&proxy.fence.fence, status)
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
                let observation = proxy
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
    proxy.metrics.primary_epoch(proxy.fence.current());

    if fenced.opened_under < proxy.fence.current() {
        return Err(ProxyError::SupersededEpoch {
            message: format!(
                "this backend is serving primary epoch {} and the cluster has reached {}",
                fenced.opened_under,
                proxy.fence.current()
            ),
        });
    }
    Ok(fenced)
}

/// Greets the client with the backend's session state, then relays until the
/// session ends.
async fn relay(
    proxy: &Arc<Proxy>,
    session: &mut handshake::ClientSession,
    link: &mut backend::BackendSession,
    fenced: &session::Fenced<'_>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let token = CancelToken::mint(proxy.config.routing.cancel_routing_id)?;
    let route = CancelRoute::new();
    route.set(Some(crate::cancel::CancelTarget {
        address: proxy.config.backend.address.clone(),
        key_data: link.key_data.clone(),
    }));
    let _registration = proxy.cancels.register(token.clone(), route);

    let mut greeting = vec![BackendMessage::Authentication(
        pgelastic_wire::Authentication::Ok,
    )];
    greeting.extend(link.parameters.iter().cloned());
    greeting.push(BackendMessage::BackendKeyData(token.key_data()?));
    greeting.push(BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    crate::wire_io::write_backend(&mut session.stream, &greeting).await?;

    let pending_from_client = session.buf.as_slice().to_vec();
    let pending_from_backend = link.buf.as_slice().to_vec();

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
    key: &pgelastic_pool::PoolKey,
    role: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let tenant = proxy.pools.ensure_tenant(role).map_err(admission)?;
    let client_id = proxy.pools.connect_client(&tenant).map_err(admission)?;
    let _client = ClientRegistration {
        pools: Arc::clone(&proxy.pools),
        client: client_id,
    };

    let connector = crate::pool::Connector {
        backend: &proxy.config.backend,
        tls: proxy.backend_tls.as_ref(),
        kdf: &proxy.kdf,
        startup: &session.startup,
    };

    // The greeting is the first link's `ParameterStatus` set, cached per pool
    // key: a client that arrives twentieth must not have to hold a backend just
    // to be told the server's `TimeZone`.
    let parameters = if let Some(cached) = proxy.pools.greeting(key) {
        cached
    } else {
        let request = crate::pool::AcquireRequest {
            key,
            tenant: &tenant,
            client: client_id,
        };
        let checkout = proxy
            .pools
            .acquire(&request, &connector, &mut session.stream)
            .await
            .map_err(admission)?;
        proxy.pools.check_in(key, checkout);
        proxy
            .pools
            .greeting(key)
            .expect("opening a link caches the pool's greeting")
    };

    let mut client_vars = crate::vars::VariableCache::new();
    for message in parameters.iter() {
        if let BackendMessage::ParameterStatus(status) = message {
            client_vars.observe(&status.name, &status.value);
        }
    }

    let token = CancelToken::mint(proxy.config.routing.cancel_routing_id)?;
    let route = CancelRoute::for_client(client_id);
    let _registration = proxy.cancels.register(token.clone(), route.clone());

    let mut greeting = vec![BackendMessage::Authentication(
        pgelastic_wire::Authentication::Ok,
    )];
    greeting.extend(parameters.iter().cloned());
    greeting.push(BackendMessage::BackendKeyData(token.key_data()?));
    greeting.push(BackendMessage::ReadyForQuery(TransactionStatus::Idle));
    crate::wire_io::write_backend(&mut session.stream, &greeting).await?;

    let pending = session.buf.as_slice().to_vec();
    let ending = crate::txn::run(
        crate::txn::Session {
            client: &mut session.stream,
            manager: &proxy.pools,
            connector,
            key: key.clone(),
            tenant,
            client_id,
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

fn admission(denial: crate::pool::Denial) -> ProxyError {
    ProxyError::Admission {
        sqlstate: denial.sqlstate,
        message: denial.message,
    }
}

/// Releases the client's place in the second currency however the session ends.
struct ClientRegistration {
    pools: Arc<PoolManager>,
    client: pgelastic_capacity::ClientId,
}

impl Drop for ClientRegistration {
    fn drop(&mut self) {
        self.pools.disconnect_client(self.client);
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
    // Step 0 of the admission ladder. A cancel opens a real backend socket, so
    // it is admitted like one — but from its own bounded credit pool, which is
    // both why a cancel storm cannot eat the tenant's capacity and why a tenant
    // pinned at its burst ceiling can still cancel.
    let _credit = match route.client() {
        Some(client) => match proxy.pools.lease_cancel(client) {
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
        proxy.backend_tls.as_ref(),
        proxy.config.backend.connect_timeout(),
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
        assert!(!proxy.auth.is_trust());
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
