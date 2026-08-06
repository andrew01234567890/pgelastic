//! The server-side state machine and the link state the release gate reads.
//!
//! The states are `PgBouncer`'s, name for name: `login`, `idle`, `active`,
//! `used`, `tested` and the `being_canceled` state a link enters while a
//! `CancelRequest` aimed at it is in flight.
//!
//! `used` and `idle` are not the same thing and the difference is not cosmetic.
//! A link that has just finished serving a client is `used`: it works, but
//! nothing has scrubbed it. It only becomes `idle` — eligible to be handed to a
//! different client — once the reset ladder has run.

use std::time::{Duration, Instant};

use pgelastic_wire::{BackendMessage, FrontendMessage, TransactionStatus};
use thiserror::Error;

use crate::gate::{CheckInBlock, can_check_in};
use crate::key::PoolKey;
use crate::outstanding::{
    Disposition, OutstandingError, OutstandingQueue, Reaction, Relay, RequestKind,
};
use crate::pin::PinReason;
use crate::reset::{Taint, TaintSet};

/// Who wrote a frontend message on its way to the backend.
///
/// The pool does not only forward what a client sent. It injects `Parse` and `Close` of its
/// own, and it rewrites a client's named `Parse` onto a content-addressed name it minted
/// ([`crate::stmt`]). Neither dirties the link, and the distinction cannot be recovered from
/// the message: by the time it reaches the wire, a client's statement and one the pool
/// synthesised look identical, both carrying a `pgel_` name.
///
/// Only the caller knows, so only the caller can say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// The client authored this. A named `Parse` from a client is state the pool is not
    /// tracking, and taints the link.
    Client,
    /// The pool authored this. The statement it creates is identified by its full text and
    /// parameter types, is recorded in [`crate::ServerStatements`], and is therefore
    /// shareable with any client that asks for the same thing — so it is not dirt.
    Pool,
}

/// Identifies a backend link for the lifetime of the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServerId(u64);

impl ServerId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerState {
    /// Connecting or authenticating; not yet usable.
    Login,
    /// Scrubbed and available to any client with a matching pool key.
    Idle,
    /// Bound to a client.
    Active,
    /// Released by its client but not yet scrubbed.
    Used,
    /// Running the reset ladder or a check query.
    Tested,
    /// A `CancelRequest` aimed at this link is in flight.
    BeingCanceled,
    /// Terminal.
    Closed,
}

impl ServerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Login => "login",
            Self::Idle => "idle",
            Self::Active => "active",
            Self::Used => "used",
            Self::Tested => "tested",
            Self::BeingCanceled => "being_canceled",
            Self::Closed => "closed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerEvent {
    LoginSucceeded,
    LoginFailed,
    /// Handed to a client.
    Assigned,
    /// The client let go of it.
    Released,
    /// The reset ladder or check query started.
    ResetStarted,
    ResetSucceeded,
    ResetFailed,
    /// A cancel aimed at this link was dispatched.
    CancelStarted,
    /// Every cancel aimed at this link has been resolved.
    CancelFinished,
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("server cannot {event:?} while {}", state.as_str())]
pub struct IllegalServerTransition {
    pub state: ServerState,
    pub event: ServerEvent,
}

/// Conditions that permanently disqualify a link from being reused.
///
/// A bit set rather than a row of booleans so that a new condition is one
/// constant and cannot be forgotten at a construction site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ReleaseFlags(u8);

impl ReleaseFlags {
    /// Something went wrong that the link cannot be trusted after.
    pub const CLOSE_NEEDED: Self = Self(1 << 0);
    /// `serverLifetime` elapsed.
    pub const LIFETIME_EXPIRED: Self = Self(1 << 1);
    /// The tenant's credentials were rotated under this link.
    pub const CREDENTIALS_STALE: Self = Self(1 << 2);

    pub fn empty() -> Self {
        Self(0)
    }

    pub fn insert(&mut self, flag: Self) {
        self.0 |= flag.0;
    }

    pub fn remove(&mut self, flag: Self) {
        self.0 &= !flag.0;
    }

    pub fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// Which COPY subprotocol, if any, is open on the link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CopyState {
    #[default]
    None,
    /// The client is streaming rows to the backend.
    In,
    /// The backend is streaming rows to the client.
    Out,
    /// Both directions, as used by replication.
    Both,
}

impl CopyState {
    pub fn is_open(self) -> bool {
        !matches!(self, Self::None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LinkError {
    #[error(transparent)]
    Outstanding(#[from] OutstandingError),
    #[error(transparent)]
    Transition(#[from] IllegalServerTransition),
}

/// Everything about a backend link that the release gate reads.
#[derive(Debug)]
pub struct ServerLink {
    id: ServerId,
    key: PoolKey,
    state: ServerState,
    tx_status: Option<TransactionStatus>,
    /// A batch the client ended with something other than `Sync` is still open.
    ///
    /// `tx_status` cannot answer this. It is read off the last `ReadyForQuery`, and a batch
    /// ended with `Flush` draws none - so the status reports whatever the batch BEFORE it
    /// left, which is usually idle, while `PostgreSQL` is inside the implicit transaction this
    /// one opened. The outstanding queue empties as the rows arrive, so it cannot answer it
    /// either. Without this the check-in gate had no way to see the state at all, and its
    /// callers had to remember not to release on it.
    batch_open: bool,
    outstanding: OutstandingQueue,
    copy: CopyState,
    taint: TaintSet,
    pin: Option<PinReason>,
    flags: ReleaseFlags,
    cancels_in_flight: u32,
    deadline: Option<Instant>,
}

impl ServerLink {
    pub fn new(id: ServerId, key: PoolKey) -> Self {
        Self {
            id,
            key,
            state: ServerState::Login,
            tx_status: None,
            batch_open: false,
            outstanding: OutstandingQueue::new(),
            copy: CopyState::None,
            taint: TaintSet::new(),
            pin: None,
            flags: ReleaseFlags::empty(),
            cancels_in_flight: 0,
            deadline: None,
        }
    }

    pub fn id(&self) -> ServerId {
        self.id
    }

    pub fn key(&self) -> &PoolKey {
        &self.key
    }

    pub fn state(&self) -> ServerState {
        self.state
    }

    /// Whether a batch the client has not ended with `Sync` is still open.
    pub fn batch_open(&self) -> bool {
        self.batch_open
    }

    pub fn tx_status(&self) -> Option<TransactionStatus> {
        self.tx_status
    }

    pub fn outstanding(&self) -> &OutstandingQueue {
        &self.outstanding
    }

    /// Synthesised responses that have reached the head of the queue.
    ///
    /// See [`OutstandingQueue::take_ready_fakes`]; call after every
    /// [`observe_frontend`](Self::observe_frontend) and
    /// [`observe_backend`](Self::observe_backend).
    pub fn take_ready_fakes(&mut self) -> Vec<BackendMessage> {
        self.outstanding.take_ready_fakes()
    }

    pub fn copy(&self) -> CopyState {
        self.copy
    }

    pub fn taint(&self) -> TaintSet {
        self.taint
    }

    pub fn pin(&self) -> Option<PinReason> {
        self.pin
    }

    pub fn flags(&self) -> ReleaseFlags {
        self.flags
    }

    pub fn cancels_in_flight(&self) -> u32 {
        self.cancels_in_flight
    }

    pub fn add_taint(&mut self, taint: Taint) {
        self.taint.insert(taint);
    }

    /// Records state the server will not announce.
    ///
    /// `ParameterStatus` covers only `GUC_REPORT` parameters, so a `SET search_path`, a
    /// `SET ROLE` or any custom variable reaches the link without the link knowing. The
    /// caller reads that out of the statement text and folds it in here.
    pub fn add_taints(&mut self, taints: TaintSet) {
        self.taint.union(taints);
    }

    /// Records unscrubbable state. The first reason wins: it is the one that
    /// explains why the link left the elastic budget.
    pub fn set_pin(&mut self, reason: PinReason) {
        self.pin.get_or_insert(reason);
    }

    pub fn clear_pin(&mut self) {
        self.pin = None;
    }

    pub fn set_flag(&mut self, flag: ReleaseFlags) {
        self.flags.insert(flag);
    }

    /// Marks the reset ladder as having run to completion.
    pub fn reset_completed(&mut self) {
        self.taint.clear();
    }

    pub fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = Some(deadline);
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Raises [`ReleaseFlags::LIFETIME_EXPIRED`] once the deadline has passed.
    pub fn check_lifetime(&mut self, now: Instant) {
        if self.deadline.is_some_and(|deadline| now >= deadline) {
            self.flags.insert(ReleaseFlags::LIFETIME_EXPIRED);
        }
    }

    pub fn cancel_dispatched(&mut self) {
        self.cancels_in_flight += 1;
    }

    pub fn cancel_resolved(&mut self) {
        self.cancels_in_flight = self.cancels_in_flight.saturating_sub(1);
    }

    /// The single release predicate; see [`crate::gate::can_check_in`].
    pub fn can_check_in(&self) -> Result<(), CheckInBlock> {
        can_check_in(self)
    }

    pub fn apply(&mut self, event: ServerEvent) -> Result<ServerState, IllegalServerTransition> {
        let next = Self::next(self.state, event).ok_or(IllegalServerTransition {
            state: self.state,
            event,
        })?;
        self.state = next;
        Ok(next)
    }

    fn next(state: ServerState, event: ServerEvent) -> Option<ServerState> {
        use ServerEvent as E;
        use ServerState as S;

        match (state, event) {
            (S::Closed, _) => None,

            (_, E::Close) | (S::Login, E::LoginFailed) | (S::Tested, E::ResetFailed) => {
                Some(S::Closed)
            }
            (S::Login, E::LoginSucceeded) | (S::Tested, E::ResetSucceeded) => Some(S::Idle),
            (S::Idle | S::Used, E::Assigned) | (S::BeingCanceled, E::CancelFinished) => {
                Some(S::Active)
            }
            (S::Active, E::Released) => Some(S::Used),
            (S::Active | S::Used, E::ResetStarted) => Some(S::Tested),
            (S::Active, E::CancelStarted) => Some(S::BeingCanceled),

            _ => None,
        }
    }

    /// Records a frontend message on its way to the backend.
    ///
    /// See [`Origin`] for why the caller has to say who wrote the message.
    pub fn observe_frontend(&mut self, message: &FrontendMessage, relay: Relay, origin: Origin) {
        let disposition = relay.disposition();
        match message {
            FrontendMessage::CopyDone | FrontendMessage::CopyFail(_) => {
                // Only for a copy-in this link knows is open. The backend ignores a `CopyDone`
                // outside copy mode, so a stray one must move nothing here either.
                if self.copy == CopyState::In {
                    self.outstanding.discard_trailing_syncs();
                }
                self.copy = CopyState::None;
            }
            // Two conditions, and they are independent.
            //
            // `Origin::Pool` is the real rule: a statement the pool minted is content-addressed
            // and tracked in `ServerStatements`, so it is shareable with any client asking for
            // the same text and parameter types. That is not dirt on the link, it is pool-owned
            // state — and tainting it made the cache in `stmt.rs` unable to outlive a single
            // transaction, because the scrub it provoked deallocated exactly what it had cached.
            //
            // The `Fake` clause states the weaker invariant that a message never written to the
            // socket cannot dirty the socket. The caller already relies on it for the witness
            // and the encoder; saying it here too means a future caller that forgets one does
            // not silently start tainting.
            FrontendMessage::Parse(parse)
                if !parse.name.is_empty()
                    && origin == Origin::Client
                    && disposition != Disposition::Fake =>
            {
                self.taint.insert(Taint::PreparedStatement);
            }
            _ => {}
        }

        if let Some(kind) = RequestKind::from_frontend(message) {
            // Only the client's own messages open a batch. A `Parse` the pool injects is
            // followed by the client's own `Sync`, or by nothing at all.
            if origin == Origin::Client {
                self.batch_open = !kind.terminates_batch();
            }
            self.outstanding.record(kind, relay);
        }
    }

    /// Records a client request whose frame was too large to decode.
    ///
    /// The relay streams a frame over its inline limit straight through, so the request never
    /// becomes a `FrontendMessage` and `observe_frontend` never sees it. The backend answers
    /// it regardless: a >64KiB `Query` executes and commits, and its `ReadyForQuery` then
    /// arrives against an empty ledger and underflows, so the session dies after the write
    /// landed. A driver that retries what looks like a failure writes it twice.
    ///
    /// A `Parse` this size is always tainting: nothing here can show it was the unnamed
    /// statement, and the framed path taints on exactly that doubt.
    pub fn observe_frontend_opaque(&mut self, kind: RequestKind) {
        if kind == RequestKind::Parse {
            self.taint.insert(Taint::PreparedStatement);
        }
        self.batch_open = !kind.terminates_batch();
        self.outstanding.record(kind, Relay::Forward);
    }

    /// Records a backend message on its way to the client.
    pub fn observe_backend(&mut self, message: &BackendMessage) -> Result<Reaction, LinkError> {
        if self.state == ServerState::Login {
            if let BackendMessage::ReadyForQuery(status) = message {
                self.tx_status = Some(*status);
            }
            return Ok(Reaction {
                disposition: Disposition::Forward,
                popped: None,
                batch_ended: false,
            });
        }

        match message {
            // Deliberately *not* clearing the COPY state: the backend announces
            // readiness before the COPY subprotocol has drained, and treating
            // this byte as the end of the COPY is what hands a link to another
            // client mid-transfer.
            BackendMessage::ReadyForQuery(status) => {
                self.tx_status = Some(*status);
                // Only when it answers the last thing the client sent. A client may pipeline
                // a Sync and a Flush-terminated batch in one write, and this byte belongs to
                // the Sync - clearing it then would call the batch behind it closed.
                if self.outstanding.is_empty() {
                    self.batch_open = false;
                }
            }
            BackendMessage::CopyInResponse(_) => self.copy = CopyState::In,
            BackendMessage::CopyOutResponse(_) => self.copy = CopyState::Out,
            BackendMessage::CopyBothResponse(_) => self.copy = CopyState::Both,
            BackendMessage::CopyDone if self.copy != CopyState::In => self.copy = CopyState::None,
            BackendMessage::ErrorResponse(_) => self.copy = CopyState::None,
            // A reported GUC changed value, which is a protocol-level fact and
            // not a guess made from a command tag.
            BackendMessage::ParameterStatus(_) => self.taint.insert(Taint::SessionParameter),
            _ => {}
        }

        Ok(self.outstanding.apply(message)?)
    }
}

/// Spreads `serverLifetime` deterministically over `[base - jitter, base]`.
///
/// A pool whose links were all opened in the same second would otherwise
/// recycle every one of them in the same second an hour later, which is an
/// outage the pool inflicts on itself. The value is derived from the link's id
/// rather than from a random source so that a link's deadline is reproducible
/// in a test and in a log.
pub fn jittered_lifetime(base: Duration, jitter_percent: u32, seed: u64) -> Duration {
    let base_ms = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
    let spread = base_ms / 100 * u64::from(jitter_percent.min(100));
    if spread == 0 {
        return base;
    }
    Duration::from_millis(base_ms - mix(seed) % spread)
}

/// `SplitMix64`, so that neighbouring ids do not get neighbouring deadlines.
fn mix(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
pub(crate) mod tests {
    use bytes::Bytes;
    use pgelastic_wire::{CopyResponse, Format, ParameterStatus, Parse};

    use super::*;
    use crate::key::{
        BackendTarget, CredentialGeneration, DatabaseName, PoolKey, PoolKeySpec, PoolMode,
        ReplicationKind, RoleName, StartupFingerprint, TenantId, TlsPosture,
    };

    pub(crate) fn test_key() -> PoolKey {
        PoolKey::new(PoolKeySpec {
            tenant: TenantId::new("tenant-a"),
            target: BackendTarget::new("primary.example.com", 5432),
            database: DatabaseName::new("appdb"),
            role: RoleName::new("app"),
            fingerprint: StartupFingerprint::default(),
            tls: TlsPosture::Tls,
            replication: ReplicationKind::None,
            configured_mode: PoolMode::Transaction,
            credentials: CredentialGeneration::new(1),
        })
    }

    pub(crate) fn logged_in() -> ServerLink {
        let mut link = ServerLink::new(ServerId::new(1), test_key());
        link.apply(ServerEvent::LoginSucceeded).unwrap();
        link.apply(ServerEvent::Assigned).unwrap();
        link
    }

    fn copy_response() -> CopyResponse {
        CopyResponse {
            format: Format::Text,
            column_formats: Vec::new(),
        }
    }

    #[test]
    fn a_link_starts_in_login() {
        let link = ServerLink::new(ServerId::new(7), test_key());
        assert_eq!(link.state(), ServerState::Login);
        assert_eq!(link.id(), ServerId::new(7));
    }

    #[test]
    fn the_happy_path_is_login_idle_active_used_tested_idle() {
        let mut link = ServerLink::new(ServerId::new(1), test_key());
        assert_eq!(
            link.apply(ServerEvent::LoginSucceeded).unwrap(),
            ServerState::Idle
        );
        assert_eq!(
            link.apply(ServerEvent::Assigned).unwrap(),
            ServerState::Active
        );
        assert_eq!(
            link.apply(ServerEvent::Released).unwrap(),
            ServerState::Used
        );
        assert_eq!(
            link.apply(ServerEvent::ResetStarted).unwrap(),
            ServerState::Tested
        );
        assert_eq!(
            link.apply(ServerEvent::ResetSucceeded).unwrap(),
            ServerState::Idle
        );
    }

    #[test]
    fn a_used_link_can_be_handed_out_again_without_a_reset() {
        let mut link = ServerLink::new(ServerId::new(1), test_key());
        link.apply(ServerEvent::LoginSucceeded).unwrap();
        link.apply(ServerEvent::Assigned).unwrap();
        link.apply(ServerEvent::Released).unwrap();
        assert_eq!(
            link.apply(ServerEvent::Assigned).unwrap(),
            ServerState::Active
        );
    }

    #[test]
    fn a_failed_login_closes_the_link() {
        let mut link = ServerLink::new(ServerId::new(1), test_key());
        assert_eq!(
            link.apply(ServerEvent::LoginFailed).unwrap(),
            ServerState::Closed
        );
    }

    #[test]
    fn a_failed_reset_closes_the_link() {
        let mut link = ServerLink::new(ServerId::new(1), test_key());
        link.apply(ServerEvent::LoginSucceeded).unwrap();
        link.apply(ServerEvent::Assigned).unwrap();
        link.apply(ServerEvent::ResetStarted).unwrap();
        assert_eq!(
            link.apply(ServerEvent::ResetFailed).unwrap(),
            ServerState::Closed
        );
    }

    #[test]
    fn being_canceled_returns_to_active() {
        let mut link = logged_in();
        assert_eq!(
            link.apply(ServerEvent::CancelStarted).unwrap(),
            ServerState::BeingCanceled
        );
        assert_eq!(
            link.apply(ServerEvent::CancelFinished).unwrap(),
            ServerState::Active
        );
    }

    #[test]
    fn a_link_being_canceled_cannot_be_released_to_another_client() {
        let mut link = logged_in();
        link.apply(ServerEvent::CancelStarted).unwrap();
        assert!(link.apply(ServerEvent::Released).is_err());
        assert_eq!(link.state(), ServerState::BeingCanceled);
    }

    #[test]
    fn an_idle_link_cannot_be_released() {
        let mut link = ServerLink::new(ServerId::new(1), test_key());
        link.apply(ServerEvent::LoginSucceeded).unwrap();
        assert_eq!(
            link.apply(ServerEvent::Released).unwrap_err(),
            IllegalServerTransition {
                state: ServerState::Idle,
                event: ServerEvent::Released,
            }
        );
    }

    #[test]
    fn a_closed_link_accepts_nothing_further() {
        let mut link = logged_in();
        link.apply(ServerEvent::Close).unwrap();
        for event in [
            ServerEvent::LoginSucceeded,
            ServerEvent::Assigned,
            ServerEvent::Released,
            ServerEvent::ResetStarted,
            ServerEvent::CancelStarted,
            ServerEvent::Close,
        ] {
            assert!(link.apply(event).is_err(), "{event:?}");
        }
    }

    #[test]
    fn login_traffic_never_reaches_the_outstanding_queue() {
        let mut link = ServerLink::new(ServerId::new(1), test_key());
        link.observe_backend(&BackendMessage::ParameterStatus(ParameterStatus {
            name: Bytes::from_static(b"TimeZone"),
            value: Bytes::from_static(b"UTC"),
        }))
        .unwrap();
        link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
            .unwrap();

        assert!(link.outstanding().is_empty());
        assert!(link.taint().is_clean());
        assert_eq!(link.tx_status(), Some(TransactionStatus::Idle));
    }

    #[test]
    fn a_reported_parameter_change_taints_the_link() {
        let mut link = logged_in();
        link.observe_backend(&BackendMessage::ParameterStatus(ParameterStatus {
            name: Bytes::from_static(b"search_path"),
            value: Bytes::from_static(b"audit"),
        }))
        .unwrap();
        assert!(link.taint().contains(Taint::SessionParameter));
    }

    fn parse_named(name: &'static [u8]) -> FrontendMessage {
        FrontendMessage::Parse(Parse {
            name: Bytes::from_static(name),
            query: Bytes::from_static(b"SELECT 1"),
            param_types: Vec::new(),
        })
    }

    #[test]
    fn an_unnamed_parse_does_not_taint_the_link() {
        let mut link = logged_in();
        link.observe_frontend(
            &FrontendMessage::Parse(Parse {
                name: Bytes::new(),
                query: Bytes::from_static(b"SELECT 1"),
                param_types: Vec::new(),
            }),
            Relay::Forward,
            Origin::Client,
        );
        assert!(link.taint().is_clean());
    }

    #[test]
    fn a_client_authored_named_parse_still_taints_the_link() {
        let mut link = logged_in();
        link.observe_frontend(&parse_named(b"s1"), Relay::Forward, Origin::Client);
        assert!(
            link.taint().contains(Taint::PreparedStatement),
            "a statement the pool is not tracking is unaccounted state on the link"
        );
    }

    /// The statement cache exists to let one server-side statement serve every client that
    /// asks for the same text. Tainting the link when the pool parses one made the scrub at
    /// release deallocate exactly what had just been cached, so the cache could never outlive
    /// a transaction.
    #[test]
    fn a_parse_the_pool_minted_does_not_taint_the_link() {
        let mut link = logged_in();
        link.observe_frontend(
            &parse_named(b"pgel_0000000000000001"),
            Relay::Forward,
            Origin::Pool,
        );
        assert!(link.taint().is_clean());
    }

    /// A message that is answered from the pool's cache never reaches the socket, so it cannot
    /// have dirtied it. This is the client's own name, which is why `Origin` alone would not
    /// exempt it.
    #[test]
    fn a_parse_the_pool_answers_itself_does_not_taint_the_link() {
        let mut link = logged_in();
        link.observe_frontend(
            &parse_named(b"s1"),
            Relay::fake(BackendMessage::ParseComplete),
            Origin::Client,
        );
        assert!(link.taint().is_clean());
    }

    #[test]
    fn copy_in_is_opened_by_the_backend_and_closed_by_the_client() {
        let mut link = logged_in();
        link.observe_backend(&BackendMessage::CopyInResponse(copy_response()))
            .unwrap();
        assert_eq!(link.copy(), CopyState::In);

        link.observe_frontend(&FrontendMessage::CopyDone, Relay::Forward, Origin::Client);
        assert_eq!(link.copy(), CopyState::None);
    }

    /// The `Sync` every driver pipelines behind the `Execute` that opens a COPY reaches the
    /// backend in copy-in mode, where `PostgreSQL` discards it. Left on the queue it makes
    /// every response after the copy answer the request before the one it belongs to.
    #[test]
    fn a_sync_the_backend_ignored_in_copy_in_leaves_the_queue() {
        let mut link = logged_in();
        link.observe_frontend(&FrontendMessage::Sync, Relay::Forward, Origin::Client);
        link.observe_backend(&BackendMessage::CopyInResponse(copy_response()))
            .unwrap();
        assert_eq!(link.outstanding().len(), 1);

        link.observe_frontend(&FrontendMessage::CopyDone, Relay::Forward, Origin::Client);
        assert!(link.outstanding().is_empty());
    }

    /// The `Sync` after the `CopyDone` is answered, so the withdrawal has to stop at the end
    /// of the copy rather than take every `Sync` it can find.
    #[test]
    fn the_sync_that_ends_the_copy_batch_stays_on_the_queue() {
        let mut link = logged_in();
        link.observe_backend(&BackendMessage::CopyInResponse(copy_response()))
            .unwrap();
        link.observe_frontend(&FrontendMessage::CopyDone, Relay::Forward, Origin::Client);
        link.observe_frontend(&FrontendMessage::Sync, Relay::Forward, Origin::Client);
        assert_eq!(
            link.outstanding().kinds().collect::<Vec<_>>(),
            vec![RequestKind::Sync]
        );
    }

    /// A `CopyDone` outside copy-in mode is ignored by the backend, so it must move nothing
    /// here: a client that sends a stray one would otherwise unrecord a `Sync` that is coming.
    #[test]
    fn a_stray_copy_done_withdraws_nothing() {
        let mut link = logged_in();
        link.observe_frontend(&FrontendMessage::Sync, Relay::Forward, Origin::Client);
        link.observe_frontend(&FrontendMessage::CopyDone, Relay::Forward, Origin::Client);
        assert_eq!(
            link.outstanding().kinds().collect::<Vec<_>>(),
            vec![RequestKind::Sync]
        );
    }

    #[test]
    fn a_backend_copy_done_does_not_end_a_client_copy_in() {
        let mut link = logged_in();
        link.observe_backend(&BackendMessage::CopyInResponse(copy_response()))
            .unwrap();
        link.observe_backend(&BackendMessage::CopyDone).unwrap();
        assert_eq!(link.copy(), CopyState::In);
    }

    #[test]
    fn a_copy_out_is_closed_by_the_backend() {
        let mut link = logged_in();
        link.observe_backend(&BackendMessage::CopyOutResponse(copy_response()))
            .unwrap();
        assert_eq!(link.copy(), CopyState::Out);
        link.observe_backend(&BackendMessage::CopyDone).unwrap();
        assert_eq!(link.copy(), CopyState::None);
    }

    #[test]
    fn the_first_pin_reason_is_the_one_that_sticks() {
        let mut link = logged_in();
        link.set_pin(PinReason::Listen);
        link.set_pin(PinReason::TempTable);
        assert_eq!(link.pin(), Some(PinReason::Listen));
    }

    #[test]
    fn the_lifetime_flag_is_raised_only_after_the_deadline() {
        let mut link = logged_in();
        let now = Instant::now();
        link.set_deadline(now + Duration::from_secs(60));

        link.check_lifetime(now);
        assert!(!link.flags().contains(ReleaseFlags::LIFETIME_EXPIRED));

        link.check_lifetime(now + Duration::from_secs(61));
        assert!(link.flags().contains(ReleaseFlags::LIFETIME_EXPIRED));
    }

    #[test]
    fn cancel_accounting_never_underflows() {
        let mut link = logged_in();
        link.cancel_resolved();
        assert_eq!(link.cancels_in_flight(), 0);
        link.cancel_dispatched();
        link.cancel_dispatched();
        link.cancel_resolved();
        assert_eq!(link.cancels_in_flight(), 1);
    }

    #[test]
    fn lifetime_jitter_stays_inside_the_window_and_is_reproducible() {
        let base = Duration::from_secs(3600);
        let low = base * 9 / 10;
        for seed in 0..1000 {
            let lifetime = jittered_lifetime(base, 10, seed);
            assert!(
                lifetime <= base && lifetime >= low,
                "seed {seed}: {lifetime:?}"
            );
            assert_eq!(lifetime, jittered_lifetime(base, 10, seed));
        }
    }

    #[test]
    fn lifetime_jitter_actually_spreads_the_deadlines() {
        let base = Duration::from_secs(3600);
        let distinct = (0..64)
            .map(|seed| jittered_lifetime(base, 10, seed))
            .collect::<std::collections::HashSet<_>>();
        assert!(
            distinct.len() > 32,
            "only {} distinct deadlines",
            distinct.len()
        );
    }

    #[test]
    fn zero_jitter_leaves_the_lifetime_alone() {
        let base = Duration::from_secs(600);
        assert_eq!(jittered_lifetime(base, 0, 42), base);
    }

    #[test]
    fn release_flags_are_independent() {
        let mut flags = ReleaseFlags::empty();
        assert!(flags.is_empty());
        flags.insert(ReleaseFlags::CLOSE_NEEDED);
        flags.insert(ReleaseFlags::CREDENTIALS_STALE);
        assert!(flags.contains(ReleaseFlags::CLOSE_NEEDED));
        assert!(flags.contains(ReleaseFlags::CREDENTIALS_STALE));
        assert!(!flags.contains(ReleaseFlags::LIFETIME_EXPIRED));
        flags.remove(ReleaseFlags::CLOSE_NEEDED);
        assert!(!flags.contains(ReleaseFlags::CLOSE_NEEDED));
    }
}
