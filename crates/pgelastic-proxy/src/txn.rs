//! The transaction-mode relay: one client, many backends, one at a time.
//!
//! The client keeps its socket for its whole session; the backend it is talking
//! to is acquired at the first message that needs one and given back the moment
//! [`ServerLink::can_check_in`] says it may be. That predicate is the only
//! authority on release in this file. Nothing here reads the SQL text to find a
//! transaction boundary, and nothing here re-derives any part of the gate.
//!
//! Between those two points the relay is doing four things at once, and each of
//! them exists because a driver would otherwise break:
//!
//! - **Variable cache.** The backend a client lands on is not the one it was
//!   greeted by, so the tracked `GUC_REPORT` parameters are diffed and the
//!   `SET`s are issued before the client's message is forwarded.
//! - **Prepared statements.** Client statement names are rewritten to
//!   content-addressed per-server names, the `Parse` the pool injects has its
//!   `ParseComplete` swallowed, and `DEALLOCATE ALL`/`DISCARD ALL` passing
//!   through invalidate what the pool believes is parsed.
//! - **Pinning.** A tripwire for state no reset removes binds the client to its
//!   backend and takes that connection out of the elastic budget.
//! - **Cancellation.** The route a `CancelRequest` will follow is rewritten at
//!   every checkout and cleared at every release, so it is resolved against the
//!   backend that is running the query *now*.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use pgelastic_pool::{
    CacheInvalidation, CheckInBlock, ClientStatements, Origin, PinReason, PoolKey,
    PreparedStatement, Relay, ResetDisposition, ResetPolicy, ResetStep, ServerAction, ServerEvent,
    StatementKey, detect_cache_invalidation,
};
use pgelastic_wire::{
    BackendMessage, Close, FrontendMessage, Parse, RawFrame, Target, TransactionStatus,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::cancel::{CancelRoute, CancelTarget};
use crate::epoch::{Epoch, FenceAction, TransactionWitness};
use crate::error::{ProxyError, Result};
use crate::metrics::{Metrics, StatementDeadline};
use crate::pool::{AcquireRequest, Checkout, Connector, Denial, PoolManager};
use crate::quiesce::{InFlight, TenantGate};
use crate::relay::{FrameRelay, Relayed};
use crate::route::Instance;
use crate::server::Proxy;
use crate::session::{Ending, Limits};
use crate::stream::ClientStream;
use crate::vars::{self, VariableCache};

/// How long a release waits for a cancel that has been picked up but not yet
/// delivered. A cancel connection that cannot be opened must not strand a
/// backend for the life of the pool.
pub(crate) const CANCEL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// What a session is currently bound to.
///
/// Re-derived whenever the routing table moves the tenant, because every field
/// here belongs to one instance: the pool key names its address, and the tenant
/// and client ids are issued by *its* allocator and mean nothing to another's.
#[derive(Debug)]
pub struct Binding {
    pub instance: Arc<Instance>,
    /// The backend configuration this session actually dials with: the instance's address and
    /// TLS posture, but the *tenant's* identity and credential.
    ///
    /// Built once per binding rather than per checkout, and re-derived whenever the routing
    /// table moves the tenant - the credential is a property of the tenant, so it is the same
    /// on either instance during a migration.
    pub backend: Arc<crate::config::BackendConfig>,
    pub key: PoolKey,
    pub tenant: pgelastic_capacity::TenantId,
    pub client: pgelastic_capacity::ClientId,
}

impl Binding {
    /// Binds a session to whichever instance the routing table names now.
    pub async fn open(
        proxy: &Arc<Proxy>,
        startup: &pgelastic_wire::StartupMessage,
        tenant_name: &str,
        login: &str,
    ) -> Result<Self> {
        let instance = proxy.fleet.route(tenant_name);
        let backend = Arc::new(proxy.backend_for(&instance, tenant_name, login)?);
        let key = crate::pool::pool_key(
            &proxy.config,
            proxy.fingerprint_policy(),
            &backend,
            startup,
            tenant_name,
            proxy.credential_generation(tenant_name, login),
        )
        .await?;
        let tenant = instance
            .pools
            .ensure_tenant(tenant_name)
            .map_err(admission_error)?;
        let client = instance
            .pools
            .connect_client(&tenant)
            .map_err(admission_error)?;
        Ok(Self {
            instance,
            backend,
            key,
            tenant,
            client,
        })
    }

    pub fn manager(&self) -> &Arc<PoolManager> {
        &self.instance.pools
    }
}

impl Drop for Binding {
    /// Releases the client's place in the second currency, however the session
    /// ends and whichever instance it ended on.
    fn drop(&mut self) {
        self.instance.pools.disconnect_client(self.client);
    }
}

/// Opens the first link of a pool so its greeting can be cached.
pub async fn bootstrap_greeting(
    binding: &Binding,
    kdf: &crate::scram::KdfPool,
    startup: &pgelastic_wire::StartupMessage,
    client: &mut ClientStream,
    metrics: &Metrics,
) -> Result<Arc<Vec<pgelastic_wire::BackendMessage>>> {
    // A greeting costs a real backend on a real instance, so a stalled one
    // refuses here too. It is the first checkout of a pool, not an exception
    // to what a checkout is.
    if let Some(health) = binding.instance.stall.must_refuse() {
        metrics.write_stall_refused();
        return Err(crate::server::write_stalled(&binding.instance.id, health));
    }
    let connector = Connector {
        backend: &binding.backend,
        tls: binding.instance.tls.as_ref(),
        kdf,
        startup,
    };
    let request = AcquireRequest {
        key: &binding.key,
        tenant: &binding.tenant,
        client: binding.client,
    };
    let checkout = binding
        .manager()
        .acquire(&request, &connector, client)
        .await
        .map_err(admission_error)?;
    binding.manager().check_in(&binding.key, checkout);
    Ok(binding
        .manager()
        .greeting(&binding.key)
        .expect("opening a link caches the pool's greeting"))
}

/// Everything a transaction-mode session needs that outlives one message.
#[derive(Debug)]
pub struct Session<'a> {
    pub client: &'a mut ClientStream,
    pub proxy: &'a Arc<Proxy>,
    pub startup: &'a pgelastic_wire::StartupMessage,
    pub tenant: String,
    /// The login the client authenticated as, which decides the backend identity a checkout
    /// dials with. Held on the session because a re-bind after a cutover has to reach the
    /// same one - a login that changed identity mid-session would be a different principal
    /// on the far side of the same client socket.
    pub login: String,
    /// This tenant's admission gate, closed for the length of a cutover.
    pub gate: Arc<TenantGate>,
    pub binding: Binding,
    pub route: CancelRoute,
    pub metrics: &'a Arc<Metrics>,
    pub limits: Limits,
    pub client_vars: VariableCache,
    pub force_after: Duration,
}

/// The running relay.
#[derive(Debug)]
struct Running<'a> {
    session: Session<'a>,
    from_client: FrameRelay,
    to_backend: BytesMut,
    to_client: BytesMut,
    checkout: Option<Checkout>,
    statements: ClientStatements,
    reset_policy: ResetPolicy,
    /// Bytes still to come of an oversized client frame the relay is streaming.
    client_streaming: usize,
    /// What the held link has been asked to do, so the fence can classify it
    /// without reading the SQL a second time at the moment it fires.
    witness: TransactionWitness,
    /// Set once the fence has fired on a read-only transaction: the outstanding
    /// statement is allowed to finish and nothing new is admitted.
    draining_fence: bool,
    /// Set while the client owes the backend a `Sync`.
    ///
    /// An extended-query batch ended with `Flush` rather than `Sync` draws no
    /// `ReadyForQuery`, so `tx_status` still reports whatever the last completed
    /// batch left and the outstanding queue empties anyway. The link is inside an
    /// implicit transaction that nothing else here can see, and without this bit
    /// neither bound would ever fire on it.
    unsynced_batch: bool,
    /// How much of `to_backend` and `to_client` the far side has already accepted.
    ///
    /// Held here rather than inside the write future so that abandoning a write loses
    /// nothing: the buffer and the offset both outlive it, and the next flush resumes from
    /// exactly the byte the last one stopped at, mid-frame or not.
    backend_written: usize,
    client_written: usize,
    /// Set when a tripwire asked to pin a link the pool had no pinned budget for.
    /// The link cannot be pinned and must not be shared, so the session ends.
    pin_refused: Option<PinReason>,
    /// Held for exactly as long as `checkout` is, so `drainStatus` can say
    /// whether the tenant still has work on its source.
    holding: Option<InFlight>,
    /// The gate that holds every tenant on the bound instance, which a planned
    /// switchover closes and a migration does not.
    ///
    /// Re-resolved at every checkout alongside the route, because the instance
    /// a session is bound to is the thing that moves.
    instance_gate: Arc<TenantGate>,
    /// The bound instance's epoch fence.
    ///
    /// Resubscribed whenever the session follows its tenant elsewhere: two
    /// instances have two independent promotion histories, and a session left
    /// watching the one it has been moved off would sleep through the fence
    /// that concerns it.
    epochs: watch::Receiver<Epoch>,
}

/// What the client's last message asked the relay to do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continue,
    Terminate,
}

/// Runs a transaction-mode session to its end.
pub async fn run(
    session: Session<'_>,
    pending_from_client: &[u8],
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Ending> {
    let limits = session.limits;
    let force_after = session.force_after;
    let reset_policy = session.binding.manager().config().reset_policy.into();

    let mut running = Running {
        from_client: FrameRelay::new(limits.inline_frame_bytes, limits.max_frame_bytes),
        to_backend: BytesMut::new(),
        to_client: BytesMut::new(),
        checkout: None,
        statements: ClientStatements::new(),
        reset_policy,
        client_streaming: 0,
        witness: TransactionWitness::new(),
        draining_fence: false,
        unsynced_batch: false,
        backend_written: 0,
        client_written: 0,
        pin_refused: None,
        holding: None,
        instance_gate: instance_gate_of(&session),
        epochs: epochs_of(&session),
        session,
    };
    running.from_client.extend_from_slice(pending_from_client);

    let ending = running.drive(shutdown, force_after).await;
    running.finish().await;
    ending
}

/// How long a cancel's delivery may take before it is abandoned.
///
/// **Strictly less than [`CANCEL_WAIT_TIMEOUT`]**, and that ordering is the whole point. The
/// release waits `CANCEL_WAIT_TIMEOUT` for a cancel that has been picked up, and then hands the
/// link on regardless. A delivery bound longer than that wait meant a cancel could still be in
/// flight after the link had been given to somebody else - and in transaction mode the somebody
/// else is another tenant, whose statement it then cancels. It was five seconds against a
/// two-second wait, so the window was three seconds wide on every cancel that could not connect
/// promptly.
///
/// A cancel is best effort by construction - `PostgreSQL`'s own is - so abandoning a slow one is
/// the cheap side of this trade. Cancelling a stranger's query is not.
pub(crate) const CANCEL_DELIVERY_TIMEOUT: Duration = Duration::from_millis(1500);

const _: () = assert!(
    CANCEL_DELIVERY_TIMEOUT.as_millis() < CANCEL_WAIT_TIMEOUT.as_millis(),
    "a cancel must not outlive the release that stopped waiting for it, or it lands on \
     whichever tenant holds the link next"
);

impl Running<'_> {
    async fn drive(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
        force_after: Duration,
    ) -> Result<Ending> {
        let mut draining = *shutdown.borrow_and_update();
        let deadline = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(deadline);
        let mut holds = HoldDeadlines::new();
        if draining {
            deadline
                .as_mut()
                .reset(tokio::time::Instant::now() + force_after);
        }
        self.epochs.mark_unchanged();

        // Bytes the handshake already buffered can complete a whole message on
        // their own, so the first pump happens before the first read.
        if self.pump_client().await? == Flow::Terminate {
            return Ok(Ending::PeerClosed);
        }

        loop {
            // First, before the drain boundary and before any deadline is armed.
            // `pin()` records the refusal rather than ending the session where it
            // is noticed, because that is inside the client pump, mid-message.
            // The check lives here rather than beside a pump so that every pump -
            // including the one before this loop, over whatever the client
            // pipelined behind its startup packet - is covered by construction.
            if let Some(reason) = self.pin_refused {
                self.on_pin_refused(reason).await;
                return Ok(Ending::PinCeiling);
            }
            if draining && self.at_drain_boundary() {
                return Ok(Ending::Drained);
            }

            holds.arm(self);
            let HoldDeadlines {
                statement,
                statement_armed,
                idle,
                idle_armed,
                pinned_for,
                pinned_armed,
            } = &mut holds;

            let event = tokio::select! {
                biased;
                _ = shutdown.changed(), if !draining => Event::Drain,
                // Ahead of every read: an epoch that has already moved must
                // reach the policy before another client byte is forwarded.
                _ = self.epochs.changed() => Event::EpochChanged,
                () = &mut deadline, if draining => Event::Deadline,
                () = &mut *statement, if *statement_armed => Event::StatementDeadline,
                () = &mut *idle, if *idle_armed => Event::IdleInTransaction,
                () = &mut *pinned_for, if *pinned_armed => Event::PinExpired,
                read = self.session.client.read_buf(self.from_client.read_target()) => {
                    Event::FromClient(read)
                }
                read = read_backend(&mut self.checkout), if self.checkout.is_some() => {
                    Event::FromBackend(read)
                }
            };

            match event {
                Event::EpochChanged => self.on_epoch_change()?,
                Event::Drain => {
                    draining = true;
                    deadline
                        .as_mut()
                        .reset(tokio::time::Instant::now() + force_after);
                }
                Event::Deadline => return Ok(Ending::Forced),
                Event::StatementDeadline => {
                    self.on_statement_deadline().await;
                    return Ok(Ending::StatementTimeout);
                }
                Event::IdleInTransaction => {
                    self.on_idle_in_transaction().await;
                    return Ok(Ending::IdleInTransaction);
                }
                Event::PinExpired => {
                    self.on_pin_expired().await;
                    return Ok(Ending::PinExpired);
                }
                Event::FromClient(read) => {
                    if read? == 0 {
                        return Ok(Ending::PeerClosed);
                    }
                    if self.pump_client().await? == Flow::Terminate {
                        return Ok(Ending::PeerClosed);
                    }
                }
                Event::FromBackend(read) => {
                    if read? == 0 {
                        // The backend vanished mid-session. The link cannot be
                        // returned to the pool, and the client is told rather
                        // than left waiting for a response that will not come.
                        self.abandon();
                        return Err(ProxyError::backend("the backend closed the connection"));
                    }
                    self.pump_backend().await?;
                }
            }
        }
    }

    /// Whether this session is holding a pinned link.
    fn holds_a_pin(&self) -> bool {
        self.checkout
            .as_ref()
            .is_some_and(|checkout| checkout.conn.link.pin().is_some())
    }

    /// Ends a session that has held a pinned link for longer than the pool allows.
    ///
    /// The link is closed, not returned. The state it was pinned for is state no
    /// reset removes, which is the whole reason it was pinned, so it can no more
    /// be shared at the end of the bound than at the start of it. What the bound
    /// buys is the connection itself: a pool at its pinned ceiling otherwise stays
    /// there for as long as its longest-lived client.
    async fn on_pin_expired(&mut self) {
        self.session.metrics.pin_expired();
        // The pin timer is armed on holding a pin and nothing else, so unlike the
        // idle bound it can fire with a statement still running. Ending the
        // session would otherwise take the statement deadline with it, so a pin
        // that expires first silently voids the deadline that would have stopped
        // the statement.
        self.cancel_anything_outstanding().await;
        crate::wire_io::send_fatal(
            self.session.client,
            "53300",
            "terminating a connection that has held a pinned backend for longer than this \
             pool allows",
        )
        .await;
        self.abandon();
    }

    /// Ends a session whose link could not be pinned.
    ///
    /// The link is closed, not returned. It carries state no reset removes - that
    /// is what asked for the pin - so handing it to the next client would give one
    /// tenant's `LISTEN`, cursor or advisory lock to somebody else. Losing one
    /// client is the cheaper of the two.
    async fn on_pin_refused(&mut self, reason: PinReason) {
        self.cancel_anything_outstanding().await;
        crate::wire_io::send_fatal(
            self.session.client,
            "53300",
            &format!(
                "this pool is at its limit for connections pinned by session state ({reason})"
            ),
        )
        .await;
        self.abandon();
    }

    /// Whether the client is holding an open transaction and running nothing.
    ///
    /// Read off the last `ReadyForQuery` rather than off "a link is held",
    /// because a pinned session holds its backend between transactions too, and
    /// closing one of those would be closing an idle client for being idle.
    fn idle_in_transaction(&self) -> bool {
        self.checkout.as_ref().is_some_and(|checkout| {
            checkout.conn.link.outstanding().is_empty()
                && (self.unsynced_batch
                    || checkout
                        .conn
                        .link
                        .tx_status()
                        .is_some_and(|status| !status.is_releasable()))
        })
    }

    /// Closes a client that has held an open transaction without working.
    ///
    /// The link is closed rather than rolled back and returned. What the
    /// transaction was holding - its locks, and the xmin horizon that pins every
    /// dead tuple in the cluster behind it - is released by the backend going
    /// away, and a rollback issued to a session the proxy has already given up on
    /// is one more round trip that can itself hang.
    ///
    /// The client is told first. A bare socket close is indistinguishable from a
    /// network fault, and a driver that cannot tell them apart will retry.
    async fn on_idle_in_transaction(&mut self) {
        self.session.metrics.idle_in_transaction_closed();
        crate::wire_io::send_fatal(
            self.session.client,
            "25P03",
            "terminating connection due to idle-in-transaction timeout",
        )
        .await;
        self.abandon();
    }

    /// Cancels whatever the backend is still running before the link is closed.
    ///
    /// `abandon` decrements the ledger and hands the freed slot to a queued client
    /// *before* the `Terminate` reaches the backend - and a `Terminate` is only
    /// honoured once the backend finishes what it is running, which pool.rs says
    /// about the same message. Without this the instance runs one backend over
    /// budget for as long as the abandoned statement takes, and the statement
    /// deadline that would have stopped it died with the session.
    ///
    /// A no-op when nothing is outstanding, which is every idle-in-transaction
    /// expiry and most pin expiries.
    async fn cancel_anything_outstanding(&mut self) {
        if !self.statement_outstanding() {
            return;
        }
        let outcome = self.cancel_overrunning_statement().await;
        self.session.metrics.statement_deadline(outcome);
    }

    /// Whether the held backend owes a response to a request already sent.
    ///
    /// The same queue the release gate reads, so the deadline and the release
    /// cannot disagree about whether a statement is running.
    fn statement_outstanding(&self) -> bool {
        self.checkout
            .as_ref()
            .is_some_and(|checkout| !checkout.conn.link.outstanding().is_empty())
    }

    /// Cancels the statement that outran the pool's deadline.
    ///
    /// The cancel is sent on a fresh socket to the backend's own key, which is
    /// the only way `PostgreSQL` accepts one, and it is bounded: an unbounded
    /// cancel against a backend that completes the TCP handshake and then goes
    /// silent - what an overloaded or mid-migration instance looks like - would
    /// hold this session open on the deadline that was supposed to end it.
    ///
    /// The link is not returned to the pool afterwards, whether or not the cancel
    /// landed. A cancel that did not land leaves a backend still running the
    /// statement, and handing that to the next client is worse than losing a link.
    ///
    /// Every way this can fail to reach the backend is counted rather than
    /// swallowed. An enforcement path that reports success when it enforced
    /// nothing is indistinguishable from one that works, and this one has three
    /// separate ways to arrive there.
    ///
    /// The client is told, with the code `PostgreSQL` uses for its own
    /// `statement_timeout`. The backend's error never reaches it - the link is
    /// abandoned rather than pumped - so without this the client sees only its
    /// socket close, which is what a network fault looks like and what a driver
    /// will retry.
    async fn on_statement_deadline(&mut self) {
        let outcome = self.cancel_overrunning_statement().await;
        self.session.metrics.statement_deadline(outcome);
        if outcome != StatementDeadline::Cancelled {
            tracing::warn!(
                outcome = outcome.label(),
                "a statement deadline fired without reaching its backend"
            );
        }
        crate::wire_io::send_fatal(
            self.session.client,
            "57014",
            "canceling statement due to the pool's query deadline",
        )
        .await;
        self.abandon();
    }

    async fn cancel_overrunning_statement(&mut self) -> StatementDeadline {
        let Some(checkout) = self.checkout.as_ref() else {
            return StatementDeadline::NothingHeld;
        };
        if checkout.conn.key_data.is_none() {
            // `deliver` treats a missing key as nothing to do and returns Ok, so
            // without this the deadline would report a cancel it never sent.
            return StatementDeadline::NoCancelKey;
        }
        let target = crate::cancel::CancelTarget {
            address: checkout.conn.address.clone(),
            key_data: checkout.conn.key_data.clone(),
            instance: self.manager().instance().clone(),
            client: Some(self.session.binding.client),
        };
        // The instance's own TLS and connect timeout, exactly as the client-facing
        // cancel path uses. Passing None for the connector makes every cancel fail
        // on a TLS backend leg.
        let instance = &self.session.binding.instance;
        let limit = CANCEL_DELIVERY_TIMEOUT.min(instance.backend.connect_timeout());
        match tokio::time::timeout(
            CANCEL_DELIVERY_TIMEOUT,
            crate::cancel::deliver(&target, instance.tls.as_ref(), limit),
        )
        .await
        {
            Ok(Ok(())) => StatementDeadline::Cancelled,
            Ok(Err(error)) => {
                tracing::warn!(%error, "the statement deadline's cancel could not be delivered");
                StatementDeadline::Undeliverable
            }
            Err(_) => StatementDeadline::TimedOut,
        }
    }

    /// A drain may close only when nothing is owed in either direction: no
    /// backend held, no half-read client message, nothing queued to write.
    fn at_drain_boundary(&self) -> bool {
        self.checkout.is_none()
            && self.from_client.at_frame_boundary()
            && self.to_client.is_empty()
            && self.to_backend.is_empty()
    }

    // ---- the primary-epoch fence ----------------------------------------

    /// The epoch this session's backend was opened under, against the highest
    /// the proxy has ever seen.
    fn superseded(&self) -> bool {
        let current = self.manager().fence().current();
        self.checkout
            .as_ref()
            .is_some_and(|checkout| checkout.conn.epoch < current)
    }

    fn on_epoch_change(&mut self) -> Result<()> {
        if self.superseded() {
            self.apply_fence()?;
        }
        Ok(())
    }

    /// Whether this session's traffic is being kept still on purpose.
    ///
    /// Either gate counts. A migration holds the tenant, a planned switchover
    /// holds the instance, and the fact the fence needs is the same either way:
    /// somebody chose this moment, so the promotion that just happened is not
    /// one whose writes are about to be discarded out from under a client.
    fn held(&self) -> crate::epoch::Held {
        if self.session.gate.held() || self.instance_gate.held() {
            crate::epoch::Held::ByHolder
        } else {
            crate::epoch::Held::No
        }
    }

    /// Applies the in-flight transaction policy to the held link.
    ///
    /// The matrix lives in `epoch::policy` and is not re-derived here. What
    /// this does is carry out the verdict: `Ok(())` means the session may go
    /// on — either because the link was released without loss or because a
    /// read is being allowed to finish — and an error means the session is over
    /// and the client is being told which kind of over it is.
    fn apply_fence(&mut self) -> Result<()> {
        let Some(checkout) = self.checkout.as_ref() else {
            return Ok(());
        };
        let opened_under = checkout.conn.epoch;
        let current = self.manager().fence().current();
        let status = checkout
            .conn
            .link
            .tx_status()
            .unwrap_or(TransactionStatus::Idle);
        let state = self.witness.state(status);
        let held = self.held();
        let action = crate::epoch::action(state, held);
        debug!(
            %opened_under,
            %current,
            ?state,
            ?held,
            ?action,
            "the primary epoch moved under a held backend"
        );

        match action {
            // Nothing is outstanding. A pinned link carries session state that
            // died with the primary, so its client has to be told; an unpinned
            // one is simply given back and the client's next statement lands on
            // a backend that has been verified against the new epoch.
            FenceAction::Close => {
                let pinned = checkout.conn.link.pin().is_some();
                let checkout = self.take_checkout().expect("just observed");
                self.session.route.set(None);
                self.manager().sever(checkout, action);
                self.manager().publish_budget();
                if pinned {
                    return Err(superseded_error(opened_under, current));
                }
                Ok(())
            }
            // A read cannot cause split brain, so killing it would fail a query
            // that was going to be correct. Nothing new is admitted from here.
            FenceAction::DrainThenClose => {
                self.draining_fence = true;
                Ok(())
            }
            // Somebody is deliberately holding this tenant's clients, and this
            // session owes the superseded primary nothing. The backend is given
            // back with a `Terminate` rather than an RST — it is stopping
            // cleanly, so there is no commit to race — and the client keeps its
            // socket. Its next transaction waits at the gate and runs against
            // whichever member is the primary when that gate opens.
            //
            // A pinned link is the exception, and it is not one the hold can
            // paper over: its temp tables, its `LISTEN` registrations and its
            // session advisory locks lived in a postmaster that is going away,
            // so the client has to be told. Such a session also never releases
            // its backend, which is why a drain that waited for it would never
            // report drained and a switchover would refuse to proceed.
            FenceAction::HoldForResume => {
                let pinned = checkout.conn.link.pin().is_some();
                let checkout = self.take_checkout().expect("just observed");
                self.session.route.set(None);
                if pinned {
                    self.manager().sever(checkout, FenceAction::Close);
                    self.manager().publish_budget();
                    return Err(superseded_error(opened_under, current));
                }
                self.manager()
                    .discard(checkout, crate::metrics::BackendCloseReason::FenceHeld);
                self.session.metrics.backend_held();
                self.manager().publish_budget();
                Ok(())
            }
            FenceAction::ResetNow => {
                let checkout = self.take_checkout().expect("just observed");
                self.session.route.set(None);
                self.manager().sever(checkout, action);
                self.manager().publish_budget();
                Err(superseded_error(opened_under, current))
            }
            FenceAction::ReportUnknown => {
                let key = checkout
                    .conn
                    .in_doubt_key(self.session.binding.tenant.as_str(), opened_under);
                let fence = self.manager().fence().fence.clone();
                fence
                    .in_doubt()
                    .record(key.clone(), self.witness.pending_sql());
                self.session.metrics.in_doubt(fence.in_doubt().len());
                warn!(
                    %key,
                    "a commit was forwarded and its outcome was never observed; \
                     it is neither reported as committed nor as rolled back"
                );

                let checkout = self.take_checkout().expect("just observed");
                self.session.route.set(None);
                self.manager().sever(checkout, action);
                self.manager().publish_budget();
                Err(ProxyError::OutcomeUnknown {
                    message: format!(
                        "the outcome of this transaction is UNKNOWN: its commit was forwarded to \
                         a backend serving primary epoch {opened_under}, the cluster reached \
                         {current} before the commit was answered, and the proxy did not observe \
                         whether it took effect. Do not retry. Recorded as {key}"
                    ),
                })
            }
        }
    }

    // ---- the client leg ------------------------------------------------

    async fn pump_client(&mut self) -> Result<Flow> {
        loop {
            match self.from_client.next_output()? {
                Relayed::NeedMore => break,
                Relayed::Opaque(bytes) => {
                    self.on_client_opaque(&bytes).await?;
                }
                Relayed::Frame(frame) => {
                    if frame.tag == b'X' {
                        return Ok(Flow::Terminate);
                    }
                    let message =
                        FrontendMessage::decode(&frame, pgelastic_wire::AuthState::Password)?;
                    if matches!(
                        message,
                        FrontendMessage::PasswordMessage(_)
                            | FrontendMessage::SaslInitialResponse(_)
                            | FrontendMessage::SaslResponse(_)
                            | FrontendMessage::GssResponse(_)
                    ) {
                        return Err(ProxyError::client(
                            "an authentication message arrived after authentication finished",
                        ));
                    }
                    self.on_frontend(message).await?;
                }
            }
        }
        self.flush().await?;
        Ok(Flow::Continue)
    }

    /// Handles a chunk of a frame too large to buffer whole.
    ///
    /// The first chunk of such a frame is exactly its five-byte header, which is
    /// where the tag and the length come from. A `CopyData` frame is data and is
    /// relayed untouched; anything else is a message the relay would have had to
    /// rewrite and could not, so the link is pinned rather than forwarded blind.
    /// Forwarding an unrewritten `Bind` would put one client's statement name on
    /// a shared link.
    async fn on_client_opaque(&mut self, bytes: &[u8]) -> Result<()> {
        if self.superseded() {
            self.apply_fence()?;
        }
        self.ensure_backend().await?;
        if self.client_streaming == 0 {
            let tag = bytes[0];
            let declared = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
            self.client_streaming = usize::try_from(declared)
                .ok()
                .and_then(|len| len.checked_sub(4))
                .ok_or(pgelastic_wire::WireError::InvalidLength(declared))?;
            if tag != b'd' {
                debug!(
                    tag = char::from(tag).to_string(),
                    "an oversized frontend message cannot be rewritten; pinning the link"
                );
                self.pin(PinReason::DesyncedPipeline);
            }
            // The request still has to reach the ledger, pinned or not. The backend answers a
            // frame it could not decode exactly as it answers one it could, and the reply is
            // matched against the outstanding queue - so an unrecorded request means the
            // `ReadyForQuery` for a statement that has already committed underflows and kills
            // the session. `CopyData` records nothing: it is payload inside a request already
            // on the queue.
            if let Some(kind) = pgelastic_pool::RequestKind::from_tag(tag)
                && let Some(checkout) = self.checkout.as_mut()
            {
                checkout.conn.link.observe_frontend_opaque(kind);
            }
        } else {
            self.client_streaming = self.client_streaming.saturating_sub(bytes.len());
        }
        self.to_backend.put_slice(bytes);
        Ok(())
    }

    async fn on_frontend(&mut self, message: FrontendMessage) -> Result<()> {
        // The write admission gate. Nothing reaches the backend on a superseded
        // epoch — not a `Query`, not a `Parse`, not a `Bind`, not an `Execute`
        // — and the check happens before the message is looked at rather than
        // per message kind, so a message kind added later cannot slip past it.
        if self.superseded() {
            self.apply_fence()?;
        }
        if self.draining_fence
            && let Some(error) = self.finish_fence_drain()
        {
            return Err(error);
        }

        if matches!(message, FrontendMessage::Flush) {
            // Draws no response and retires no request, so it never needs a
            // backend of its own — but it does need one to be sent to.
            self.ensure_backend().await?;
            self.dispatch(&message, Relay::Forward);
            return Ok(());
        }
        self.ensure_backend().await?;

        match message {
            FrontendMessage::Query(sql) => {
                self.on_sql(&sql);
                if let Some(invalidation) = detect_cache_invalidation(&sql) {
                    self.invalidate(invalidation);
                }
                self.dispatch(&FrontendMessage::Query(sql), Relay::Forward);
            }
            FrontendMessage::Parse(parse) => {
                self.on_sql(&parse.query);
                // Before `on_parse`, so the `ensure` it performs records against a view that
                // has already forgotten what this statement is about to destroy.
                if let Some(invalidation) = detect_cache_invalidation(&parse.query) {
                    self.invalidate(invalidation);
                }
                self.on_parse(parse);
            }
            FrontendMessage::Bind(mut bind) => {
                if let Some(statement) = self.resolve(&bind.statement) {
                    self.ensure_parsed(&statement);
                    bind.statement = statement.name().as_bytes().clone();
                } else {
                    bind.statement = reserve_namespace(&bind.statement);
                }
                self.dispatch(&FrontendMessage::Bind(bind), Relay::Forward);
            }
            FrontendMessage::Describe(mut describe) => {
                if describe.target == Target::Statement {
                    if let Some(statement) = self.resolve(&describe.name) {
                        self.ensure_parsed(&statement);
                        describe.name = statement.name().as_bytes().clone();
                    } else {
                        describe.name = reserve_namespace(&describe.name);
                    }
                }
                self.dispatch(&FrontendMessage::Describe(describe), Relay::Forward);
            }
            FrontendMessage::Close(close) if close.target == Target::Statement => {
                self.on_close_statement(close);
            }
            other => self.dispatch(&other, Relay::Forward),
        }
        Ok(())
    }

    /// Everything decided from statement text, in one place.
    ///
    /// Two separable questions, and neither is about *when* a link is released — that is the
    /// backend's `ReadyForQuery` byte and nothing else. Pinning decides whether a link may be
    /// handed on at all. Tainting decides whether it must be scrubbed first, and exists
    /// because the server announces only its `GUC_REPORT` parameters: without reading the SQL,
    /// a `SET search_path` is invisible and would survive to whoever gets the link next.
    fn on_sql(&mut self, sql: &Bytes) {
        let scan = crate::tripwire::scan(sql);
        if let Some(reason) = scan.pin {
            self.pin(reason);
        }
        if !scan.taint.is_clean()
            && let Some(checkout) = self.checkout.as_mut()
        {
            checkout.conn.link.add_taints(scan.taint);
        }
    }

    fn on_parse(&mut self, parse: Parse) {
        if parse.name.is_empty() {
            // The unnamed statement is replaced on every Parse and destroyed at
            // the end of the transaction, so it is never shared: this client is
            // entitled to its own.
            self.dispatch(&FrontendMessage::Parse(parse), Relay::Forward);
            return;
        }

        let statement = self.manager().intern_statement(StatementKey::new(
            parse.query.clone(),
            parse.param_types.clone(),
        ));
        self.statements
            .insert(parse.name.clone(), Arc::clone(&statement));

        let Some(checkout) = self.checkout.as_mut() else {
            return;
        };
        match checkout.conn.statements.ensure(&statement) {
            // Already parsed on this link, so the Parse is answered by the pool
            // and never reaches the backend. The entry still occupies its place
            // in the outstanding queue, which is what keeps it correct under
            // pipelining.
            ServerAction::Ready => self.dispatch(
                &FrontendMessage::Parse(parse),
                Relay::fake(BackendMessage::ParseComplete),
            ),
            ServerAction::Parse(name) => {
                self.dispatch_pool(
                    &FrontendMessage::Parse(rename(parse, &name)),
                    Relay::Forward,
                );
            }
            ServerAction::EvictThenParse { evict, name } => {
                self.dispatch_pool(&close_statement(&evict), Relay::Skip);
                self.dispatch_pool(
                    &FrontendMessage::Parse(rename(parse, &name)),
                    Relay::Forward,
                );
            }
        }
    }

    /// Answers the client's `Close` itself.
    ///
    /// The server-side statement is shared with every other client that prepared
    /// the same text, so closing it here would answer their next `Bind` with
    /// `26000 invalid_sql_statement_name`. Only this client's name goes away.
    fn on_close_statement(&mut self, close: Close) {
        if !close.name.is_empty() {
            self.statements.remove(&close.name);
        }
        self.dispatch(
            &FrontendMessage::Close(close),
            Relay::fake(BackendMessage::CloseComplete),
        );
    }

    fn resolve(&self, name: &Bytes) -> Option<Arc<PreparedStatement>> {
        if name.is_empty() {
            return None;
        }
        self.statements.resolve(name).map(Arc::clone)
    }

    /// Puts `statement` on the current link if it is not already there, with
    /// every injected response swallowed.
    fn ensure_parsed(&mut self, statement: &Arc<PreparedStatement>) {
        let Some(checkout) = self.checkout.as_mut() else {
            return;
        };
        let action = checkout.conn.statements.ensure(statement);
        match action {
            ServerAction::Ready => {}
            ServerAction::Parse(name) => {
                self.dispatch_pool(&parse_for(statement, &name), Relay::Skip);
            }
            ServerAction::EvictThenParse { evict, name } => {
                self.dispatch_pool(&close_statement(&evict), Relay::Skip);
                self.dispatch_pool(&parse_for(statement, &name), Relay::Skip);
            }
        }
    }

    fn invalidate(&mut self, invalidation: CacheInvalidation) {
        debug!(
            ?invalidation,
            "a statement passing through invalidated the link's cache"
        );
        if let Some(checkout) = self.checkout.as_mut() {
            checkout.conn.statements.clear();
        }
    }

    /// Records a client's own message against the link and queues its bytes.
    ///
    /// A faked request is recorded but never sent: the pool answers it.
    fn dispatch(&mut self, message: &FrontendMessage, relay: Relay) {
        self.dispatch_from(message, relay, Origin::Client);
    }

    /// Records a message the pool authored: a `Parse` under a name it minted, or the `Close`
    /// that evicts one.
    ///
    /// Separate from [`dispatch`](Self::dispatch) so provenance is stated at the call site
    /// rather than guessed from the message. It cannot be guessed: `on_parse` rewrites a
    /// client's statement onto a `pgel_` name, so by the time a `Parse` reaches the wire the
    /// pool's own and the client's are indistinguishable.
    fn dispatch_pool(&mut self, message: &FrontendMessage, relay: Relay) {
        self.dispatch_from(message, relay, Origin::Pool);
    }

    fn dispatch_from(&mut self, message: &FrontendMessage, relay: Relay, origin: Origin) {
        // Before the early return: a message that never reached a backend has
        // opened no batch. Only the client's own messages count - a `Parse` the
        // pool injects is followed by the client's own `Sync` or by nothing.
        if origin == Origin::Client
            && let Some(kind) = pgelastic_pool::RequestKind::from_frontend(message)
        {
            self.unsynced_batch = !kind.terminates_batch();
        }
        let Some(checkout) = self.checkout.as_mut() else {
            return;
        };
        let faked = matches!(relay, Relay::Fake(_));
        checkout.conn.link.observe_frontend(message, relay, origin);
        if !faked {
            // Only what actually goes out: a request the pool answers from its
            // own cache executes nothing, so it can be neither a write nor an
            // undecidable commit.
            self.witness.observe_frontend(message);
            message.encode(&mut self.to_backend);
        }
        for response in checkout.conn.link.take_ready_fakes() {
            response.encode(&mut self.to_client);
        }
    }

    // ---- the backend leg -----------------------------------------------

    async fn pump_backend(&mut self) -> Result<()> {
        let mut saw_ready = false;
        loop {
            let Some(checkout) = self.checkout.as_mut() else {
                return Ok(());
            };
            match checkout.conn.relay.next_output()? {
                Relayed::NeedMore => break,
                // Only `DataRow` and `CopyData` reach this size, and both belong
                // to a client request: the pool's own injected statements draw
                // nothing large enough to stream.
                Relayed::Opaque(bytes) => self.to_client.put_slice(&bytes),
                Relayed::Frame(frame) => {
                    let message = BackendMessage::decode(&frame)?;
                    let reaction = checkout
                        .conn
                        .link
                        .observe_backend(&message)
                        .map_err(|e| ProxyError::backend(e.to_string()))?;

                    if let BackendMessage::ParameterStatus(status) = &message {
                        checkout.conn.vars.observe(&status.name, &status.value);
                        if reaction.disposition.forwards() {
                            self.session
                                .client_vars
                                .observe(&status.name, &status.value);
                        }
                    }
                    // `ensure` records a statement as parsed the moment it decides to send the
                    // `Parse`, so an error means the pool's view may name a statement the
                    // backend does not have. It cannot tell from here which error was whose,
                    // and the link outlives the transaction now, so a wrong belief would be a
                    // permanent `26000` for every client that later lands here rather than a
                    // one-transaction annoyance. Dropping the view costs a re-`Parse`.
                    if matches!(message, BackendMessage::ErrorResponse(_)) {
                        checkout.conn.statements.clear();
                    }
                    if reaction.disposition.forwards() {
                        self.witness.observe_backend(&message);
                        put_frame(&mut self.to_client, &frame);
                    }
                    for response in checkout.conn.link.take_ready_fakes() {
                        response.encode(&mut self.to_client);
                    }
                    if matches!(message, BackendMessage::ReadyForQuery(_)) {
                        saw_ready = true;
                        // The byte that ends a batch, which is what makes the
                        // implicit transaction behind an unsynced one visible
                        // again in `tx_status`.
                        //
                        // Only when it is answering the last thing the client sent. A client
                        // may pipeline a Sync and a Flush-terminated batch in one write: the
                        // batch sets the flag, and this ReadyForQuery - which belongs to the
                        // Sync, not to the batch - would clear it while the batch is still
                        // open. tx_status then reports the idle the Sync left, the flag says
                        // no batch is open, and the idle-in-transaction bound never arms. The
                        // statement deadline disarms too the moment the batch is answered, so
                        // the backend is held with no bound of any kind.
                        if checkout.conn.link.outstanding().is_empty() {
                            self.unsynced_batch = false;
                        }
                    }
                }
            }
        }

        self.flush().await?;
        // The read the fence was waiting on has been delivered. It could not
        // have caused split brain, the client has its answer, and now the
        // socket goes.
        if saw_ready
            && self.draining_fence
            && self.at_backend_frame_boundary()
            && let Some(error) = self.finish_fence_drain()
        {
            return Err(error);
        }
        // Only once the whole read has been drained: a release taken with bytes
        // still buffered would run the reset ladder over the tail of the
        // client's own answer.
        if saw_ready && self.at_backend_frame_boundary() {
            self.try_release().await?;
        }
        Ok(())
    }

    /// Ends a fence drain now that the read it was waiting on has been
    /// answered, and reports what the client is owed.
    ///
    /// The disposition is the matrix again, applied to the session the drain
    /// has just left idle. Consulting it twice at the drain's two instants is
    /// what keeps the decision in `epoch::policy` instead of half here: the
    /// session really is idle now, and the idle row is exactly the question.
    /// `None` means the client keeps its socket.
    fn finish_fence_drain(&mut self) -> Option<ProxyError> {
        self.draining_fence = false;
        let current = self.manager().fence().current();
        let opened_under = self
            .checkout
            .as_ref()
            .map_or(current, |checkout| checkout.conn.epoch);
        let pinned = self
            .checkout
            .as_ref()
            .is_some_and(|checkout| checkout.conn.link.pin().is_some());
        let hold = !pinned
            && crate::epoch::action(crate::epoch::InFlight::Idle, self.held())
                == FenceAction::HoldForResume;

        if let Some(checkout) = self.take_checkout() {
            self.session.route.set(None);
            if hold {
                self.manager()
                    .discard(checkout, crate::metrics::BackendCloseReason::Lifecycle);
                self.session.metrics.backend_held();
            } else {
                self.manager().sever(checkout, FenceAction::DrainThenClose);
            }
            self.manager().publish_budget();
        }
        (!hold).then(|| superseded_error(opened_under, current))
    }

    fn at_backend_frame_boundary(&self) -> bool {
        self.checkout
            .as_ref()
            .is_some_and(|checkout| checkout.conn.relay.at_frame_boundary())
    }

    // ---- checkout, check-in and the reset ladder ------------------------

    fn manager(&self) -> &Arc<PoolManager> {
        self.session.binding.manager()
    }

    /// Takes the held link back, releasing the tenant's in-flight count with it.
    ///
    /// The one place either of those two things happens, so a release path
    /// added later cannot leave `drainStatus` reporting work that finished.
    fn take_checkout(&mut self) -> Option<Checkout> {
        self.holding = None;
        self.checkout.take()
    }

    /// Rebinds this session if a cutover moved its tenant.
    ///
    /// Only ever called with no backend held, which is what makes it safe: the
    /// instance changes at a transaction boundary and never underneath one.
    async fn follow_route(&mut self) -> Result<()> {
        debug_assert!(self.checkout.is_none(), "a bound session must not be moved");
        let routed = self.session.proxy.fleet.route_id(&self.session.tenant);
        if routed == self.session.binding.instance.id {
            return Ok(());
        }
        let binding = Binding::open(
            self.session.proxy,
            self.session.startup,
            &self.session.tenant,
            &self.session.login,
        )
        .await?;
        debug!(
            tenant = %self.session.tenant,
            from = %self.session.binding.instance.id,
            to = %binding.instance.id,
            "this session follows its tenant to another instance"
        );
        self.session.binding = binding;
        // The statement cache is per instance, so nothing this client prepared
        // on the source is parsed on the target — but the client does not know
        // that, and its driver still holds the names. Re-interning against the
        // new instance keeps every name resolvable, and the per-link cache on
        // the target answers the next `Bind` with a `Parse` of its own. Clearing
        // the map instead is how a queued client survives the pause and is then
        // told `26000 prepared statement does not exist`, which is a dropped
        // transaction wearing the costume of a completed move.
        let manager = Arc::clone(self.session.binding.manager());
        self.statements
            .rebind(|key| manager.intern_statement(key.clone()));
        self.epochs = epochs_of(&self.session);
        self.epochs.mark_unchanged();
        Ok(())
    }

    async fn ensure_backend(&mut self) -> Result<()> {
        if self.checkout.is_some() {
            return Ok(());
        }
        // Anything already owed to the client goes out before the admission
        // path can write a NoticeResponse of its own, or the notice overtakes
        // the answer to the client's previous request.
        self.flush().await?;

        // The tenant gate, then the route, then the instance gate, then the
        // stall check, in that order. A queued client has to read the routing
        // table *after* it is released from the tenant gate, because being
        // released is precisely the moment a cutover has finished rewriting it
        // — and it can only ask which instance is holding it once it knows
        // which instance it is on. Batons are held until this client has its
        // backend, so each queue drains in the order it filled, and they are
        // always taken tenant-first so two gates never wait on each other.
        let baton = self.session.gate.admit().await;
        self.follow_route().await?;
        self.instance_gate = instance_gate_of(&self.session);
        let instance_gate = Arc::clone(&self.instance_gate);
        let instance_baton = instance_gate.admit().await;
        let instance = Arc::clone(&self.session.binding.instance);
        if let Some(health) = instance.stall.must_refuse() {
            self.session.metrics.write_stall_refused();
            return Err(crate::server::write_stalled(&instance.id, health));
        }

        let connector = Connector {
            backend: &self.session.binding.backend,
            tls: instance.tls.as_ref(),
            kdf: &self.session.proxy.kdf,
            startup: self.session.startup,
        };
        let request = AcquireRequest {
            key: &self.session.binding.key,
            tenant: &self.session.binding.tenant,
            client: self.session.binding.client,
        };
        // Boxed, and this is a memory fix rather than a style choice. A coroutine frame is
        // sized by its largest suspend point, so the whole dial-connect-SCRAM-TLS chain behind
        // `acquire` -- 4,848 bytes of it -- was reserved inside every client's task for the
        // life of the connection, including the thousands that are sitting idle having never
        // checked out a backend at all. Moving it behind a pointer pays one allocation when a
        // checkout actually happens, instead of the space on every connection that never does.
        let checkout = Box::pin(
            instance
                .pools
                .acquire(&request, &connector, self.session.client),
        )
        .await
        .map_err(admission_error)?;
        drop(instance_baton);
        drop(baton);

        self.session.route.set(Some(CancelTarget {
            address: checkout.conn.address.clone(),
            key_data: checkout.conn.key_data.clone(),
            instance: instance.id.clone(),
            client: Some(self.session.binding.client),
        }));
        self.holding = Some(self.session.gate.hold());
        self.checkout = Some(checkout);
        self.manager().publish_budget();

        self.sync_variables().await
    }

    /// Brings the link's reported parameters in line with what the client has
    /// been told, and holds the client until they have flushed.
    ///
    /// Nothing client-supplied is applied while the link is running this: the
    /// batch is issued, its `ReadyForQuery` is consumed, and only then does the
    /// client's own message go out. The parameter *names* come from a closed set
    /// and the values are quoted literals, which is what keeps a client-chosen
    /// `search_path` out of an internally generated statement (CVE-2025-12819).
    async fn sync_variables(&mut self) -> Result<()> {
        let Some(checkout) = self.checkout.as_ref() else {
            return Ok(());
        };
        let Some(sql) = vars::sync_statement(&self.session.client_vars, &checkout.conn.vars) else {
            return Ok(());
        };
        self.run_internal(&sql).await
    }

    /// Runs a statement of the pool's own and discards its whole answer.
    async fn run_internal(&mut self, sql: &str) -> Result<()> {
        let message = FrontendMessage::Query(Bytes::copy_from_slice(sql.as_bytes()));
        self.dispatch(&message, Relay::Skip);
        self.flush().await?;

        let mut failure = None;
        loop {
            let Some(checkout) = self.checkout.as_mut() else {
                return Ok(());
            };
            match checkout.conn.relay.next_output()? {
                Relayed::NeedMore => {
                    if checkout
                        .conn
                        .stream
                        .read_buf(checkout.conn.relay.read_target())
                        .await?
                        == 0
                    {
                        return Err(ProxyError::backend(
                            "the backend closed the connection during an internal statement",
                        ));
                    }
                }
                Relayed::Opaque(_) => {}
                Relayed::Frame(frame) => {
                    let response = BackendMessage::decode(&frame)?;
                    let reaction = checkout
                        .conn
                        .link
                        .observe_backend(&response)
                        .map_err(|e| ProxyError::backend(e.to_string()))?;
                    match &response {
                        BackendMessage::ParameterStatus(status) => {
                            checkout.conn.vars.observe(&status.name, &status.value);
                        }
                        BackendMessage::ErrorResponse(fields) => {
                            failure = Some(
                                String::from_utf8_lossy(
                                    fields.message().map_or(&b""[..], |m| m.as_ref()),
                                )
                                .into_owned(),
                            );
                        }
                        _ => {}
                    }
                    // An asynchronous notification is the client's, not the
                    // pool's, and arrives here only on a link that has been
                    // pinned for LISTEN.
                    if reaction.disposition.forwards() {
                        put_frame(&mut self.to_client, &frame);
                    }
                    if matches!(response, BackendMessage::ReadyForQuery(_)) {
                        break;
                    }
                }
            }
        }

        self.flush().await?;
        match failure {
            Some(message) => Err(ProxyError::backend(format!(
                "an internal statement failed: {message}"
            ))),
            None => Ok(()),
        }
    }

    /// The release decision, taken by the gate and by nothing else.
    async fn try_release(&mut self) -> Result<()> {
        self.flush().await?;
        self.settle_cancels().await;
        // A batch the client ended with Flush rather than Sync is answered without a
        // ReadyForQuery, so the gate below would read the transaction status the batch BEFORE
        // it left - which said idle - while the server holds whatever this one opened: an
        // implicit transaction, an unclosed portal, an unnamed statement. Nothing in the
        // protocol has said so yet, so the gate cannot see it.
        //
        // No caller reaches here in that state today: try_release runs only behind a
        // ReadyForQuery in the same pass, and an unsynced batch draws none. This is a guard
        // against that stopping being true, not a fix for a reachable defect - a review
        // claimed the path was live and it could not be reproduced. It costs a release
        // deferred to the next Sync, which the idle-in-transaction bound already bounds.
        if self.unsynced_batch {
            return Ok(());
        }
        let Some(checkout) = self.checkout.as_mut() else {
            return Ok(());
        };
        checkout.conn.link.check_lifetime(std::time::Instant::now());

        match checkout.conn.link.can_check_in() {
            // Clean, and clean is not the same as scrubbed: the configured
            // policy may still want a `DISCARD ALL` over state the protocol
            // never announced.
            Ok(()) | Err(CheckInBlock::ResetRequired) => self.reset_and_release().await,
            Err(CheckInBlock::Disqualified(flags)) => {
                debug!(?flags, "a link is disqualified from reuse and is closed");
                let checkout = self.take_checkout().expect("just observed");
                self.session.route.set(None);
                self.manager()
                    .discard(checkout, crate::metrics::BackendCloseReason::Disqualified);
                self.manager().publish_budget();
                Ok(())
            }
            // Either the client is entitled to the state it created — a pinned
            // link stays with it, having already left the elastic budget — or
            // the link is still owed something: an open transaction, an
            // unanswered request, a COPY that has not drained, a cancel in
            // flight. Both mean the same thing here: keep it.
            Err(_) => Ok(()),
        }
    }

    /// Holds the link until no cancel aimed at this client is still being
    /// delivered.
    ///
    /// The mark goes on the link, so the release gate is the thing that reports
    /// the block — [`CheckInBlock::CancelInFlight`] — rather than this function
    /// deciding on its own. Bounded, because a cancel connection that cannot be
    /// opened must not strand a backend.
    async fn settle_cancels(&mut self) {
        if self.session.route.cancels_in_flight() == 0 {
            return;
        }
        let Some(checkout) = self.checkout.as_mut() else {
            return;
        };
        checkout.conn.link.cancel_dispatched();
        debug_assert!(
            checkout.conn.link.can_check_in().is_err(),
            "a link with a cancel in flight must not pass the release gate"
        );

        let deadline = tokio::time::Instant::now() + CANCEL_WAIT_TIMEOUT;
        while self.session.route.cancels_in_flight() > 0 {
            if tokio::time::Instant::now() >= deadline {
                debug!("a cancel is still in flight at its deadline; releasing anyway");
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        if let Some(checkout) = self.checkout.as_mut() {
            checkout.conn.link.cancel_resolved();
        }
    }

    async fn reset_and_release(&mut self) -> Result<()> {
        let Some(checkout) = self.checkout.as_mut() else {
            return Ok(());
        };
        let status = checkout
            .conn
            .link
            .tx_status()
            .unwrap_or(TransactionStatus::Idle);
        let plan = pgelastic_pool::plan(
            self.reset_policy,
            checkout.conn.link.taint(),
            checkout.conn.link.pin(),
            pgelastic_pool::ReleaseContext {
                tx_status: status,
                client_gone: false,
            },
        );
        if let ResetDisposition::Close(reason) = plan.disposition() {
            let checkout = self.take_checkout().expect("just observed");
            // Logged rather than silently discarded, as its three siblings below already are.
            // A pool closing every link it takes and one reusing them look identical from
            // outside: the only tell was `checkouts_total{source="reused"}` going to nothing,
            // and nothing named the cause.
            debug!(
                %reason,
                policy = ?self.reset_policy,
                taint = ?checkout.conn.link.taint(),
                "the reset policy will not scrub this link, so it is closed rather than reused"
            );
            self.session.route.set(None);
            self.manager()
                .discard(checkout, crate::metrics::BackendCloseReason::ResetDisabled);
            self.manager().publish_budget();
            return Ok(());
        }
        if plan.is_empty() {
            let checkout = self.take_checkout().expect("just observed");
            self.hand_back(checkout);
            return Ok(());
        }

        checkout
            .conn
            .link
            .apply(ServerEvent::ResetStarted)
            .map_err(|e| ProxyError::backend(e.to_string()))?;
        let steps: Vec<ResetStep> = plan.steps().to_vec();
        for step in &steps {
            if let Err(error) = self.run_internal(step.sql()).await {
                debug!(%error, %step, "the reset ladder failed; the link is closed");
                if let Some(checkout) = self.take_checkout() {
                    self.session.route.set(None);
                    self.manager()
                        .discard(checkout, crate::metrics::BackendCloseReason::ResetFailed);
                    self.manager().publish_budget();
                }
                return Ok(());
            }
        }

        let Some(checkout) = self.checkout.as_mut() else {
            return Ok(());
        };
        if steps
            .iter()
            .any(|step| matches!(step, ResetStep::DiscardAll | ResetStep::DeallocateAll))
        {
            checkout.conn.statements.clear();
        }
        checkout.conn.link.reset_completed();

        match checkout.conn.link.can_check_in() {
            Ok(()) => {
                let checkout = self.take_checkout().expect("just observed");
                self.hand_back(checkout);
            }
            Err(block) => {
                debug!(%block, "a scrubbed link still cannot be checked in; closing it");
                let checkout = self.take_checkout().expect("just observed");
                self.session.route.set(None);
                self.manager()
                    .discard(checkout, crate::metrics::BackendCloseReason::StillBlocked);
                self.manager().publish_budget();
            }
        }
        Ok(())
    }

    fn hand_back(&mut self, checkout: Checkout) {
        self.session.route.set(None);
        self.manager().check_in(&self.session.binding.key, checkout);
        self.manager().publish_budget();
    }

    /// Records unscrubbable state and takes the link out of the elastic budget.
    fn pin(&mut self, reason: PinReason) {
        let Some(checkout) = self.checkout.as_ref() else {
            return;
        };
        if checkout.conn.link.pin().is_some() {
            return;
        }
        if let crate::pool::PinOutcome::Refused { pinned, ceiling } =
            self.manager().record_pin(reason)
        {
            // Recorded rather than acted on here: this runs inside the client
            // pump, mid-message, and the session has to end at the top of the
            // loop where nothing is half-written to either side.
            warn!(
                %reason, pinned, ceiling,
                "the pool is at its pinned ceiling, so this link is closed rather than pinned"
            );
            self.session.metrics.pin_refused();
            self.pin_refused = Some(reason);
            return;
        }
        if let Some(checkout) = self.checkout.as_mut() {
            checkout.conn.link.set_pin(reason);
        }
        self.session.metrics.pinned(reason);
        self.manager().publish_budget();
        debug!(%reason, "a tripwire pinned this client to its backend");
    }

    /// Gives up on a link that can no longer be trusted.
    fn abandon(&mut self) {
        if let Some(checkout) = self.take_checkout() {
            self.session.route.set(None);
            self.manager()
                .discard(checkout, crate::metrics::BackendCloseReason::Abandoned);
            self.manager().publish_budget();
        }
    }

    /// Ends the session, scrubbing a pinned link if the state it was pinned for
    /// is something `DISCARD ALL` can actually remove.
    async fn finish(&mut self) {
        let Some(checkout) = self.checkout.as_ref() else {
            return;
        };
        let Some(reason) = checkout.conn.link.pin() else {
            self.abandon();
            return;
        };

        let status = checkout
            .conn
            .link
            .tx_status()
            .unwrap_or(TransactionStatus::Idle);
        let plan = pgelastic_pool::plan(
            self.reset_policy,
            checkout.conn.link.taint(),
            Some(reason),
            pgelastic_pool::ReleaseContext {
                tx_status: status,
                client_gone: true,
            },
        );
        if plan.disposition() != ResetDisposition::Reuse {
            self.abandon();
            return;
        }

        let steps: Vec<ResetStep> = plan.steps().to_vec();
        if self
            .checkout
            .as_mut()
            .expect("just observed")
            .conn
            .link
            .apply(ServerEvent::ResetStarted)
            .is_err()
        {
            self.abandon();
            return;
        }
        for step in &steps {
            if self.run_internal(step.sql()).await.is_err() {
                self.abandon();
                return;
            }
        }

        let Some(checkout) = self.checkout.as_mut() else {
            return;
        };
        checkout.conn.statements.clear();
        checkout.conn.link.reset_completed();
        checkout.conn.link.clear_pin();
        let manager = Arc::clone(self.session.binding.manager());
        manager.release_pin(reason);

        match checkout.conn.link.can_check_in() {
            Ok(()) => {
                let checkout = self.take_checkout().expect("just observed");
                self.hand_back(checkout);
            }
            Err(_) => self.abandon(),
        }
        self.manager().publish_budget();
    }

    /// Writes both legs, resumably.
    ///
    /// Every byte is written through `write`, which tokio documents as cancel safe - dropped
    /// before it resolves, it is guaranteed to have written nothing - and the count of bytes
    /// already accepted lives in `self`, never in the future. That is the same shape
    /// `read_backend` and `session::run`'s `FrameRelay` use, and it is what makes this
    /// abandonable: dropping this future loses no bytes and truncates no frame, because the
    /// buffer and the offset both survive it and the next call resumes mid-frame exactly
    /// where the last one stopped.
    ///
    /// `write_all` could not offer that. Dropped mid-frame it leaves the wire with a partial
    /// message and no record of how much went out, which is why the comment above
    /// `HoldDeadlines` forbids putting a timeout around a loop that used it.
    async fn flush(&mut self) -> Result<()> {
        // The same bound the statement deadline applies, on the other half of holding a
        // backend. A client that stops reading parks this task where no deadline branch can
        // fire, and it keeps its backend for as long as it likes; PostgreSQL sees an active
        // session and its own idle_in_transaction_session_timeout never looks at it.
        //
        // Safe to bound only because the writes above are resumable: expiry abandons a future
        // that has written a known number of bytes, not one that has truncated a frame with no
        // record of how much went out.
        let deadline = self
            .manager()
            .query_deadline()
            .map(|limit| tokio::time::Instant::now() + limit);
        if !self.to_backend.is_empty() {
            if self.checkout.is_none() {
                self.to_backend.clear();
                self.backend_written = 0;
                return Err(ProxyError::backend(
                    "there is no backend to write the client's request to",
                ));
            }
            while self.backend_written < self.to_backend.len() {
                let checkout = self.checkout.as_mut().expect("just observed");
                let pending = checkout
                    .conn
                    .stream
                    .write(&self.to_backend[self.backend_written..]);
                let wrote = match deadline {
                    Some(at) => tokio::time::timeout_at(at, pending)
                        .await
                        .map_err(|_| ProxyError::backend("the backend stopped accepting bytes"))?,
                    None => pending.await,
                }?;
                if wrote == 0 {
                    return Err(ProxyError::backend("the backend accepted no bytes"));
                }
                self.backend_written += wrote;
            }
            let checkout = self.checkout.as_mut().expect("just observed");
            checkout.conn.stream.flush().await?;
            self.session
                .metrics
                .relayed_to_backend(self.to_backend.len());
            self.to_backend.clear();
            self.backend_written = 0;
        }
        if !self.to_client.is_empty() {
            while self.client_written < self.to_client.len() {
                let pending = self
                    .session
                    .client
                    .write(&self.to_client[self.client_written..]);
                let wrote = match deadline {
                    Some(at) => tokio::time::timeout_at(at, pending)
                        .await
                        .map_err(|_| ProxyError::client("the client stopped reading"))?,
                    None => pending.await,
                }?;
                if wrote == 0 {
                    return Err(ProxyError::client("the client accepted no bytes"));
                }
                self.client_written += wrote;
            }
            self.session.client.flush().await?;
            self.session.metrics.relayed_to_client(self.to_client.len());
            self.to_client.clear();
            self.client_written = 0;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Event {
    Drain,
    Deadline,
    StatementDeadline,
    IdleInTransaction,
    PinExpired,
    EpochChanged,
    FromClient(std::io::Result<usize>),
    FromBackend(std::io::Result<usize>),
}

/// Cancel-safe: the partially-read frame lives in the relay, never in the
/// future, so losing a `select!` race cannot lose bytes.
async fn read_backend(checkout: &mut Option<Checkout>) -> std::io::Result<usize> {
    let checkout = checkout
        .as_mut()
        .expect("the branch is disabled without a checkout");
    checkout
        .conn
        .stream
        .read_buf(checkout.conn.relay.read_target())
        .await
}

/// The bound instance's epoch channel.
fn epochs_of(session: &Session<'_>) -> watch::Receiver<Epoch> {
    session.binding.instance.fence.fence.subscribe()
}

/// The bound instance's admission gate.
fn instance_gate_of(session: &Session<'_>) -> Arc<TenantGate> {
    session
        .proxy
        .quiesce
        .instance(session.binding.instance.id.as_str())
}

/// The refusal a client gets when its connection was on a superseded epoch.
///
/// A definite failure: nothing further was forwarded, so the transaction is
/// aborted and a retry on a fresh connection is safe. That is precisely what
/// separates it from [`ProxyError::OutcomeUnknown`], which is not a failure at
/// all.
fn superseded_error(opened_under: Epoch, current: Epoch) -> ProxyError {
    ProxyError::SupersededEpoch {
        message: format!(
            "this connection was serving primary epoch {opened_under} and the cluster has \
             reached {current}; the transaction was aborted rather than committed to a \
             primary that is about to be rewound. Reconnect and retry"
        ),
    }
}

fn admission_error(denial: Denial) -> ProxyError {
    ProxyError::Admission {
        sqlstate: denial.sqlstate,
        message: denial.message,
    }
}

/// Keeps a name this session never prepared out of the pool's own namespace.
///
/// Minted names are the `pgel_` prefix and a fixed-width hex id, so a client could name one it
/// never prepared and reach a statement belonging to somebody else on the same pool key — the
/// same tenant, role and database, but not the same session, and a `Describe` of it would
/// return that session's parameter and row descriptions.
///
/// Only names of exactly the minted shape are moved, so a client that prepared a statement
/// through SQL rather than the protocol still finds its own. The escaped name goes to the
/// backend and is answered there with `26000`, which keeps the outstanding queue and the
/// error-recovery-to-`Sync` semantics precisely as `PostgreSQL` defines them.
fn reserve_namespace(name: &Bytes) -> Bytes {
    if pgelastic_pool::StatementName::is_generated(name) {
        pgelastic_pool::StatementName::escaped(name)
    } else {
        name.clone()
    }
}

fn rename(parse: Parse, name: &pgelastic_pool::StatementName) -> Parse {
    Parse {
        name: name.as_bytes().clone(),
        query: parse.query,
        param_types: parse.param_types,
    }
}

fn parse_for(
    statement: &PreparedStatement,
    name: &pgelastic_pool::StatementName,
) -> FrontendMessage {
    FrontendMessage::Parse(Parse {
        name: name.as_bytes().clone(),
        query: statement.key().query().clone(),
        param_types: statement.key().param_types().to_vec(),
    })
}

fn close_statement(name: &pgelastic_pool::StatementName) -> FrontendMessage {
    FrontendMessage::Close(Close {
        target: Target::Statement,
        name: name.as_bytes().clone(),
    })
}

/// Re-emits a frame byte-identically rather than re-encoding the message it
/// decoded to, so field order and unknown fields survive the relay.
/// The three deadlines that bound how long one session may hold a backend.
///
/// Grouped because they are armed together on every pass of the relay loop and
/// because each is a branch of the same `select!`. Boxed rather than pinned to the
/// stack of `drive` so that arming them can be one call instead of three.
///
/// Never an outer `timeout()` around the loop, for the reason `session.rs` gives
/// about the drain deadline: an outer timeout drops the future mid-`write_all` and
/// truncates a frame on the wire, and a truncated frame is not a bounded session,
/// it is a corrupt one.
struct HoldDeadlines {
    /// A request is outstanding on the backend. Disarmed by its `ReadyForQuery`.
    statement: std::pin::Pin<Box<tokio::time::Sleep>>,
    statement_armed: bool,
    /// The complement of the statement deadline, armed exactly when that one is
    /// not: a client holding an open transaction and running nothing. Between them
    /// they bound both ways a backend can be held, and neither fires on the state
    /// the other governs.
    idle: std::pin::Pin<Box<tokio::time::Sleep>>,
    idle_armed: bool,
    /// A link is pinned. Never disarmed while it is, because a pin is a state the
    /// client stays in rather than an operation it repeats.
    pinned_for: std::pin::Pin<Box<tokio::time::Sleep>>,
    pinned_armed: bool,
}

impl HoldDeadlines {
    fn new() -> Self {
        Self {
            statement: Box::pin(tokio::time::sleep(Duration::ZERO)),
            statement_armed: false,
            idle: Box::pin(tokio::time::sleep(Duration::ZERO)),
            idle_armed: false,
            pinned_for: Box::pin(tokio::time::sleep(Duration::ZERO)),
            pinned_armed: false,
        }
    }

    /// Arms each deadline the session has just entered the state for, and disarms
    /// each one it has just left.
    ///
    /// Called at the top of the loop rather than beside any one message, because
    /// each of these states is reached down several paths and one that armed
    /// nothing is exactly the hold these exist to bound.
    fn arm(&mut self, running: &Running<'_>) {
        let manager = running.manager();
        arm(
            &mut self.statement_armed,
            self.statement.as_mut(),
            running.statement_outstanding(),
            manager.query_deadline(),
        );
        arm(
            &mut self.idle_armed,
            self.idle.as_mut(),
            running.idle_in_transaction(),
            manager.client_idle_in_transaction(),
        );
        arm(
            &mut self.pinned_armed,
            self.pinned_for.as_mut(),
            running.holds_a_pin(),
            manager.max_pin_duration(),
        );
    }
}

/// Arms a deadline while a condition holds and disarms it when it stops.
///
/// A timer only ever starts on the transition into the state it bounds, so a
/// state that persists across many loop passes is measured from when it began
/// rather than restarted by each pass.
fn arm(
    armed: &mut bool,
    timer: std::pin::Pin<&mut tokio::time::Sleep>,
    condition: bool,
    limit: Option<Duration>,
) {
    match (*armed, condition) {
        (false, true) => {
            if let Some(limit) = limit {
                timer.reset(tokio::time::Instant::now() + limit);
                *armed = true;
            }
        }
        (true, false) => *armed = false,
        _ => {}
    }
}

fn put_frame(out: &mut BytesMut, frame: &RawFrame) {
    out.put_u8(frame.tag);
    out.put_i32(i32::try_from(frame.body.len() + 4).unwrap_or(i32::MAX));
    out.put_slice(&frame.body);
}
