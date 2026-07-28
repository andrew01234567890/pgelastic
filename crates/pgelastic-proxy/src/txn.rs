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
    CacheInvalidation, CheckInBlock, ClientStatements, PinReason, PoolKey, PreparedStatement,
    Relay, ResetDisposition, ResetPolicy, ResetStep, ServerAction, ServerEvent, StatementKey,
    detect_cache_invalidation,
};
use pgelastic_wire::{
    BackendMessage, Close, FrontendMessage, Parse, RawFrame, Target, TransactionStatus,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use tracing::debug;

use crate::cancel::{CancelRoute, CancelTarget};
use crate::error::{ProxyError, Result};
use crate::metrics::Metrics;
use crate::pool::{AcquireRequest, Checkout, Connector, Denial, PoolManager};
use crate::relay::{FrameRelay, Relayed};
use crate::session::{Ending, Limits};
use crate::stream::ClientStream;
use crate::vars::{self, VariableCache};

/// How long a release waits for a cancel that has been picked up but not yet
/// delivered. A cancel connection that cannot be opened must not strand a
/// backend for the life of the pool.
const CANCEL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// Everything a transaction-mode session needs that outlives one message.
#[derive(Debug)]
pub struct Session<'a> {
    pub client: &'a mut ClientStream,
    pub manager: &'a Arc<PoolManager>,
    pub connector: Connector<'a>,
    pub key: PoolKey,
    pub tenant: pgelastic_capacity::TenantId,
    pub client_id: pgelastic_capacity::ClientId,
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
    let reset_policy = session.manager.config().reset_policy.into();

    let mut running = Running {
        from_client: FrameRelay::new(limits.inline_frame_bytes, limits.max_frame_bytes),
        to_backend: BytesMut::new(),
        to_client: BytesMut::new(),
        checkout: None,
        statements: ClientStatements::new(),
        reset_policy,
        client_streaming: 0,
        session,
    };
    running.from_client.extend_from_slice(pending_from_client);

    let ending = running.drive(shutdown, force_after).await;
    running.finish().await;
    ending
}

impl Running<'_> {
    async fn drive(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
        force_after: Duration,
    ) -> Result<Ending> {
        let mut draining = *shutdown.borrow_and_update();
        let deadline = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(deadline);
        if draining {
            deadline
                .as_mut()
                .reset(tokio::time::Instant::now() + force_after);
        }

        // Bytes the handshake already buffered can complete a whole message on
        // their own, so the first pump happens before the first read.
        if self.pump_client().await? == Flow::Terminate {
            return Ok(Ending::PeerClosed);
        }

        loop {
            if draining && self.at_drain_boundary() {
                return Ok(Ending::Drained);
            }

            let event = tokio::select! {
                biased;
                _ = shutdown.changed(), if !draining => Event::Drain,
                () = &mut deadline, if draining => Event::Deadline,
                read = self.session.client.read_buf(self.from_client.read_target()) => {
                    Event::FromClient(read)
                }
                read = read_backend(&mut self.checkout), if self.checkout.is_some() => {
                    Event::FromBackend(read)
                }
            };

            match event {
                Event::Drain => {
                    draining = true;
                    deadline
                        .as_mut()
                        .reset(tokio::time::Instant::now() + force_after);
                }
                Event::Deadline => return Ok(Ending::Forced),
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

    /// A drain may close only when nothing is owed in either direction: no
    /// backend held, no half-read client message, nothing queued to write.
    fn at_drain_boundary(&self) -> bool {
        self.checkout.is_none()
            && self.from_client.at_frame_boundary()
            && self.to_client.is_empty()
            && self.to_backend.is_empty()
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
        } else {
            self.client_streaming = self.client_streaming.saturating_sub(bytes.len());
        }
        self.to_backend.put_slice(bytes);
        Ok(())
    }

    async fn on_frontend(&mut self, message: FrontendMessage) -> Result<()> {
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
                self.on_parse(parse);
            }
            FrontendMessage::Bind(mut bind) => {
                if let Some(statement) = self.resolve(&bind.statement) {
                    self.ensure_parsed(&statement);
                    bind.statement = statement.name().as_bytes().clone();
                }
                self.dispatch(&FrontendMessage::Bind(bind), Relay::Forward);
            }
            FrontendMessage::Describe(mut describe) => {
                if describe.target == Target::Statement
                    && let Some(statement) = self.resolve(&describe.name)
                {
                    self.ensure_parsed(&statement);
                    describe.name = statement.name().as_bytes().clone();
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
    /// Pinning is the only thing SQL text is ever consulted for: it decides
    /// whether a link may be handed on at all, never when it is handed on.
    fn on_sql(&mut self, sql: &Bytes) {
        if let Some(reason) = crate::tripwire::scan(sql) {
            self.pin(reason);
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

        let statement = self.session.manager.intern_statement(StatementKey::new(
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
                self.dispatch(
                    &FrontendMessage::Parse(rename(parse, &name)),
                    Relay::Forward,
                );
            }
            ServerAction::EvictThenParse { evict, name } => {
                self.dispatch(&close_statement(&evict), Relay::Skip);
                self.dispatch(
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
                self.dispatch(&parse_for(statement, &name), Relay::Skip);
            }
            ServerAction::EvictThenParse { evict, name } => {
                self.dispatch(&close_statement(&evict), Relay::Skip);
                self.dispatch(&parse_for(statement, &name), Relay::Skip);
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

    /// Records a frontend message against the link and queues its bytes.
    ///
    /// A faked request is recorded but never sent: the pool answers it.
    fn dispatch(&mut self, message: &FrontendMessage, relay: Relay) {
        let Some(checkout) = self.checkout.as_mut() else {
            return;
        };
        let faked = matches!(relay, Relay::Fake(_));
        checkout.conn.link.observe_frontend(message, relay);
        if !faked {
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
                    if reaction.disposition.forwards() {
                        put_frame(&mut self.to_client, &frame);
                    }
                    for response in checkout.conn.link.take_ready_fakes() {
                        response.encode(&mut self.to_client);
                    }
                    if matches!(message, BackendMessage::ReadyForQuery(_)) {
                        saw_ready = true;
                    }
                }
            }
        }

        self.flush().await?;
        // Only once the whole read has been drained: a release taken with bytes
        // still buffered would run the reset ladder over the tail of the
        // client's own answer.
        if saw_ready && self.at_backend_frame_boundary() {
            self.try_release().await?;
        }
        Ok(())
    }

    fn at_backend_frame_boundary(&self) -> bool {
        self.checkout
            .as_ref()
            .is_some_and(|checkout| checkout.conn.relay.at_frame_boundary())
    }

    // ---- checkout, check-in and the reset ladder ------------------------

    async fn ensure_backend(&mut self) -> Result<()> {
        if self.checkout.is_some() {
            return Ok(());
        }
        // Anything already owed to the client goes out before the admission
        // path can write a NoticeResponse of its own, or the notice overtakes
        // the answer to the client's previous request.
        self.flush().await?;

        let request = AcquireRequest {
            key: &self.session.key,
            tenant: &self.session.tenant,
            client: self.session.client_id,
        };
        let checkout = self
            .session
            .manager
            .acquire(&request, &self.session.connector, self.session.client)
            .await
            .map_err(admission_error)?;

        self.session.route.set(Some(CancelTarget {
            address: checkout.conn.address.clone(),
            key_data: checkout.conn.key_data.clone(),
        }));
        self.checkout = Some(checkout);
        self.session.manager.publish_budget();

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
                let checkout = self.checkout.take().expect("just observed");
                self.session.route.set(None);
                self.session.manager.discard(checkout);
                self.session.manager.publish_budget();
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
        if plan.disposition() != ResetDisposition::Reuse {
            let checkout = self.checkout.take().expect("just observed");
            self.session.route.set(None);
            self.session.manager.discard(checkout);
            self.session.manager.publish_budget();
            return Ok(());
        }
        if plan.is_empty() {
            let checkout = self.checkout.take().expect("just observed");
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
                if let Some(checkout) = self.checkout.take() {
                    self.session.route.set(None);
                    self.session.manager.discard(checkout);
                    self.session.manager.publish_budget();
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
                let checkout = self.checkout.take().expect("just observed");
                self.hand_back(checkout);
            }
            Err(block) => {
                debug!(%block, "a scrubbed link still cannot be checked in; closing it");
                let checkout = self.checkout.take().expect("just observed");
                self.session.route.set(None);
                self.session.manager.discard(checkout);
                self.session.manager.publish_budget();
            }
        }
        Ok(())
    }

    fn hand_back(&mut self, checkout: Checkout) {
        self.session.route.set(None);
        self.session.manager.check_in(&self.session.key, checkout);
        self.session.manager.publish_budget();
    }

    /// Records unscrubbable state and takes the link out of the elastic budget.
    fn pin(&mut self, reason: PinReason) {
        let Some(checkout) = self.checkout.as_mut() else {
            return;
        };
        if checkout.conn.link.pin().is_some() {
            return;
        }
        checkout.conn.link.set_pin(reason);
        self.session.manager.record_pin(reason);
        self.session.metrics.pinned(reason);
        self.session.manager.publish_budget();
        debug!(%reason, "a tripwire pinned this client to its backend");
    }

    /// Gives up on a link that can no longer be trusted.
    fn abandon(&mut self) {
        if let Some(checkout) = self.checkout.take() {
            self.session.route.set(None);
            self.session.manager.discard(checkout);
            self.session.manager.publish_budget();
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
        self.session.manager.release_pin(reason);

        match checkout.conn.link.can_check_in() {
            Ok(()) => {
                let checkout = self.checkout.take().expect("just observed");
                self.hand_back(checkout);
            }
            Err(_) => self.abandon(),
        }
        self.session.manager.publish_budget();
    }

    async fn flush(&mut self) -> Result<()> {
        if !self.to_backend.is_empty() {
            let Some(checkout) = self.checkout.as_mut() else {
                self.to_backend.clear();
                return Err(ProxyError::backend(
                    "there is no backend to write the client's request to",
                ));
            };
            checkout.conn.stream.write_all(&self.to_backend).await?;
            checkout.conn.stream.flush().await?;
            self.session
                .metrics
                .relayed_to_backend(self.to_backend.len());
            self.to_backend.clear();
        }
        if !self.to_client.is_empty() {
            self.session.client.write_all(&self.to_client).await?;
            self.session.client.flush().await?;
            self.session.metrics.relayed_to_client(self.to_client.len());
            self.to_client.clear();
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Event {
    Drain,
    Deadline,
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

fn admission_error(denial: Denial) -> ProxyError {
    ProxyError::Admission {
        sqlstate: denial.sqlstate,
        message: denial.message,
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
fn put_frame(out: &mut BytesMut, frame: &RawFrame) {
    out.put_u8(frame.tag);
    out.put_i32(i32::try_from(frame.body.len() + 4).unwrap_or(i32::MAX));
    out.put_slice(&frame.body);
}
