//! The passthrough loop: one client bound to one backend for its whole life.

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use pgelastic_wire::{RawFrame, TransactionStatus};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;

use crate::error::{Result, sqlstate};
use crate::metrics::Metrics;
use crate::relay::{FrameRelay, Relayed};
use crate::stream::{BackendStream, ClientStream};

/// How a session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ending {
    /// One side closed the connection.
    PeerClosed,
    /// A drain reached an idle boundary and closed the session cleanly.
    Drained,
    /// A drain ran out of time and closed the session mid-work.
    Forced,
    /// A tripwire asked to pin a link the pool had no pinned budget left for. The
    /// link is closed rather than pinned or shared: it carries state no reset
    /// removes, which is what asked for the pin in the first place.
    PinCeiling,
    /// A client held an open transaction without working for longer than the
    /// pool allows. The link is closed rather than rolled back and returned: what
    /// the transaction held is released by the backend going away, and a rollback
    /// issued to a session already given up on is a round trip that can hang.
    IdleInTransaction,
    /// A statement outran the pool's query deadline. The backend was cancelled
    /// and the link closed rather than returned: a cancel that did not land
    /// leaves a backend still running the statement, and handing that to the
    /// next client is worse than losing the link.
    StatementTimeout,
}

/// Tracks whether the session is at a boundary a drain may close on.
///
/// The backend's `ReadyForQuery` transaction-status byte is the only authority
/// on this: `'I'` means the last statement finished and no transaction is open.
/// `'T'` and `'E'` mean the client still holds transactional state that closing
/// would silently roll back. Never inferred from the SQL text.
#[derive(Debug)]
struct Boundary {
    ready_idle: bool,
    request_in_flight: bool,
}

impl Boundary {
    fn new() -> Self {
        Self {
            ready_idle: true,
            request_in_flight: false,
        }
    }

    fn saw_backend_frame(&mut self, frame: &RawFrame) {
        if frame.tag != b'Z' {
            return;
        }
        // A NotificationResponse carries no ready state, so only 'Z' moves it.
        self.ready_idle = frame.body.first() == Some(&TransactionStatus::Idle.as_byte());
        self.request_in_flight = false;
    }

    fn saw_client_bytes(&mut self) {
        self.request_in_flight = true;
    }

    fn is_closable(&self, to_backend: &FrameRelay, to_client: &FrameRelay) -> bool {
        self.ready_idle
            && !self.request_in_flight
            && to_backend.at_frame_boundary()
            && to_client.at_frame_boundary()
    }
}

/// Bytes the handshake read past the message it was after.
#[derive(Debug, Clone, Copy)]
pub struct Pending<'a> {
    pub from_client: &'a [u8],
    pub from_backend: &'a [u8],
}

/// The relay's memory bounds.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub inline_frame_bytes: usize,
    pub max_frame_bytes: usize,
}

/// The primary-epoch fence as a bound session sees it.
///
/// Session mode never goes through the pool manager, so the sweep that severs
/// parked links cannot reach this connection: without this it would be the one
/// place in the proxy where a demoted primary keeps serving writes. Replication
/// connections are forced into session mode, which makes it the *last* place
/// that may be left unfenced.
#[derive(Debug)]
pub struct Fenced<'a> {
    pub runtime: &'a crate::epoch::FenceRuntime,
    /// The epoch this backend proved it was serving when it was opened.
    pub opened_under: crate::epoch::Epoch,
    pub tenant: String,
    pub backend_pid: Option<i32>,
    pub lsn: Option<String>,
}

impl Fenced<'_> {
    fn superseded(&self) -> bool {
        self.opened_under < self.runtime.current()
    }

    fn key(&self) -> crate::epoch::InDoubtKey {
        crate::epoch::InDoubtKey::new(
            self.tenant.clone(),
            self.opened_under,
            self.backend_pid,
            self.lsn.clone(),
        )
    }
}

/// Everything a bound session needs that is not one of its two sockets.
#[derive(Debug)]
pub struct Context<'a> {
    pub pending: Pending<'a>,
    pub limits: Limits,
    pub metrics: &'a Arc<Metrics>,
    pub force_after: Duration,
    pub fence: &'a Fenced<'a>,
}

/// Relays until one side closes, a drain completes, or the primary epoch moves.
///
/// # Cancel safety
///
/// Every partially-read frame lives in a [`FrameRelay`] owned by this function,
/// never inside the read future, so a `select!` branch losing the race cannot
/// lose bytes. The forced-drain deadline is a branch of the same `select!`
/// rather than an outer `timeout`, because an outer timeout would drop the
/// future mid-`write_all` and truncate a frame on the wire — the one place
/// cancellation genuinely is not safe.
pub async fn run(
    client: &mut ClientStream,
    backend: &mut BackendStream,
    context: Context<'_>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Ending> {
    let Context {
        pending,
        limits,
        metrics,
        force_after,
        fence,
    } = context;

    let mut state = State::new(limits, pending);
    let mut draining = *shutdown.borrow_and_update();
    let mut epochs = fence.runtime.fence.subscribe();
    let mut draining_fence = false;

    let deadline = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(deadline);
    if draining {
        deadline
            .as_mut()
            .reset(tokio::time::Instant::now() + force_after);
    }

    // Bytes the handshake already buffered can complete a frame on their own.
    state.flush_to_backend(backend, metrics).await?;
    state.flush_to_client(client, metrics).await?;

    loop {
        if state.is_closable() {
            if draining {
                return Ok(Ending::Drained);
            }
            // The read the fence was waiting on has been delivered; now the
            // socket goes.
            if draining_fence {
                return Err(drained_fence(backend, fence, metrics, &state.witness));
            }
        }

        // Nothing in a branch handler touches a relay: the read futures hold
        // mutable borrows of them for as long as the `select!` block lives, so
        // the work happens after it, on the `Event` it produced.
        let event = tokio::select! {
            biased;
            _ = shutdown.changed(), if !draining => Event::Drain,
            _ = epochs.changed() => Event::EpochChanged,
            () = &mut deadline, if draining => Event::Deadline,
            read = client.read_buf(state.to_backend.read_target()) => Event::FromClient(read),
            read = backend.read_buf(state.to_client.read_target()) => Event::FromBackend(read),
        };

        match event {
            Event::EpochChanged => {
                if let Some(error) =
                    apply_fence(backend, fence, metrics, &state.witness, &mut draining_fence)
                {
                    return Err(error);
                }
            }
            // The watch only ever moves false -> true, and a dropped sender
            // means the supervisor is gone, which is also a reason to drain.
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
                // The write admission gate: nothing the client sent may reach a
                // backend on a superseded epoch.
                if let Some(error) =
                    apply_fence(backend, fence, metrics, &state.witness, &mut draining_fence)
                {
                    return Err(error);
                }
                if draining_fence {
                    return Err(drained_fence(backend, fence, metrics, &state.witness));
                }
                state.boundary.saw_client_bytes();
                state.flush_to_backend(backend, metrics).await?;
            }
            Event::FromBackend(read) => {
                if read? == 0 {
                    return Ok(Ending::PeerClosed);
                }
                state.flush_to_client(client, metrics).await?;
            }
        }
    }
}

/// The two relays, the drain boundary and the fence's witness.
///
/// One struct because all four are advanced by the same two operations, and a
/// relay pumped without its witness being fed is a session the fence would
/// mis-classify.
#[derive(Debug)]
struct State {
    to_backend: FrameRelay,
    to_client: FrameRelay,
    boundary: Boundary,
    witness: crate::epoch::TransactionWitness,
}

impl State {
    fn new(limits: Limits, pending: Pending<'_>) -> Self {
        let mut to_backend = FrameRelay::new(limits.inline_frame_bytes, limits.max_frame_bytes);
        let mut to_client = FrameRelay::new(limits.inline_frame_bytes, limits.max_frame_bytes);
        to_backend.extend_from_slice(pending.from_client);
        to_client.extend_from_slice(pending.from_backend);
        let mut boundary = Boundary::new();
        if !pending.from_client.is_empty() {
            boundary.saw_client_bytes();
        }
        Self {
            to_backend,
            to_client,
            boundary,
            witness: crate::epoch::TransactionWitness::new(),
        }
    }

    fn is_closable(&self) -> bool {
        self.boundary.is_closable(&self.to_backend, &self.to_client)
    }

    async fn flush_to_backend(
        &mut self,
        backend: &mut BackendStream,
        metrics: &Arc<Metrics>,
    ) -> Result<()> {
        let witness = &mut self.witness;
        let bytes = pump(&mut self.to_backend, backend, |frame| {
            observe_frontend(witness, frame);
        })
        .await?;
        metrics.relayed_to_backend(bytes);
        Ok(())
    }

    async fn flush_to_client(
        &mut self,
        client: &mut ClientStream,
        metrics: &Arc<Metrics>,
    ) -> Result<()> {
        let witness = &mut self.witness;
        let boundary = &mut self.boundary;
        let bytes = pump(&mut self.to_client, client, |frame| {
            boundary.saw_backend_frame(frame);
            observe_backend(witness, frame);
        })
        .await?;
        metrics.relayed_to_client(bytes);
        Ok(())
    }
}

fn drained_fence(
    backend: &BackendStream,
    fence: &Fenced<'_>,
    metrics: &Arc<Metrics>,
    witness: &crate::epoch::TransactionWitness,
) -> crate::error::ProxyError {
    sever(
        backend,
        fence,
        metrics,
        crate::epoch::FenceAction::DrainThenClose,
        witness,
    )
}

/// Feeds the witness a frame on its way to the backend.
///
/// A frame that will not decode is left to the witness's own conservative
/// default rather than being skipped: the alternative is to conclude a session
/// is idle because the proxy could not read what it just forwarded.
fn observe_frontend(witness: &mut crate::epoch::TransactionWitness, frame: &RawFrame) {
    if let Ok(message) =
        pgelastic_wire::FrontendMessage::decode(frame, pgelastic_wire::AuthState::Password)
    {
        witness.observe_frontend(&message);
    }
}

fn observe_backend(witness: &mut crate::epoch::TransactionWitness, frame: &RawFrame) {
    if let Ok(message) = pgelastic_wire::BackendMessage::decode(frame) {
        witness.observe_backend(&message);
    }
}

/// Carries out the in-flight policy on a bound session.
///
/// Returns `None` only for the read-only row, which is allowed to finish; every
/// other row ends the session, and `draining_fence` records which case is
/// still owed an answer.
fn apply_fence(
    backend: &BackendStream,
    fence: &Fenced<'_>,
    metrics: &Arc<Metrics>,
    witness: &crate::epoch::TransactionWitness,
    draining_fence: &mut bool,
) -> Option<crate::error::ProxyError> {
    if !fence.superseded() {
        return None;
    }
    let state = witness.state(TransactionStatus::Idle);
    // Never held. A session-mode client owns its backend for its whole life, so
    // there is no boundary at which to hand it back and let it queue - which is
    // the same reason quiesce excludes session pooling outright. Offering it the
    // held row would return a link the client still believes it owns.
    let action = crate::epoch::action(state, crate::epoch::Held::No);
    if action == crate::epoch::FenceAction::DrainThenClose {
        *draining_fence = true;
        return None;
    }
    Some(sever(backend, fence, metrics, action, witness))
}

/// Arms the backend socket for an RST and builds the error the client is given.
fn sever(
    backend: &BackendStream,
    fence: &Fenced<'_>,
    metrics: &Arc<Metrics>,
    action: crate::epoch::FenceAction,
    witness: &crate::epoch::TransactionWitness,
) -> crate::error::ProxyError {
    backend.arm_reset();
    metrics.backend_severed(action);

    let opened_under = fence.opened_under;
    let current = fence.runtime.current();
    if action != crate::epoch::FenceAction::ReportUnknown {
        return crate::error::ProxyError::SupersededEpoch {
            message: format!(
                "this connection was serving primary epoch {opened_under} and the cluster has \
                 reached {current}; it was severed rather than left writing to a primary that \
                 is about to be rewound. Reconnect and retry"
            ),
        };
    }

    let key = fence.key();
    let log = fence.runtime.fence.in_doubt();
    log.record(key.clone(), witness.pending_sql());
    metrics.in_doubt(log.len());
    tracing::warn!(
        %key,
        "a commit was forwarded on a bound session and its outcome was never observed"
    );
    crate::error::ProxyError::OutcomeUnknown {
        message: format!(
            "the outcome of this transaction is UNKNOWN: its commit was forwarded to a backend \
             serving primary epoch {opened_under}, the cluster reached {current} before the \
             commit was answered, and the proxy did not observe whether it took effect. \
             Do not retry. Recorded as {key}"
        ),
    }
}

#[derive(Debug)]
enum Event {
    Drain,
    Deadline,
    EpochChanged,
    FromClient(std::io::Result<usize>),
    FromBackend(std::io::Result<usize>),
}

/// Moves every frame the relay can produce to the far side in one write.
///
/// Coalescing matters: a 50k-row result set is 50k frames, and one `write_all`
/// each turns a single read into 50k syscalls.
async fn pump<W: AsyncWrite + Unpin>(
    relay: &mut FrameRelay,
    out: &mut W,
    mut observe: impl FnMut(&RawFrame),
) -> Result<usize> {
    let mut wire = BytesMut::new();
    loop {
        match relay.next_output()? {
            Relayed::Frame(frame) => {
                observe(&frame);
                wire.put_u8(frame.tag);
                wire.put_i32(i32::try_from(frame.body.len() + 4).unwrap_or(i32::MAX));
                wire.put_slice(&frame.body);
            }
            Relayed::Opaque(bytes) => wire.put_slice(&bytes),
            Relayed::NeedMore => break,
        }
    }
    if wire.is_empty() {
        return Ok(0);
    }
    let len = wire.len();
    out.write_all(&wire).await?;
    out.flush().await?;
    Ok(len)
}

/// Tells a client its session is being closed by a drain, then closes it.
///
/// The same `57P01` `PostgreSQL` itself sends for `pg_terminate_backend`, so
/// existing client retry logic recognises it.
pub async fn close_for_drain<S: AsyncWrite + Unpin>(client: &mut S, forced: bool) {
    let message = if forced {
        "terminating connection due to administrator command"
    } else {
        "the proxy is shutting down; reconnect to continue"
    };
    crate::wire_io::send_fatal(client, sqlstate::ADMIN_SHUTDOWN, message).await;
}

/// Best-effort `Terminate` so the backend sees a clean logout rather than a
/// reset socket.
pub async fn terminate_backend<S: AsyncRead + AsyncWrite + Unpin>(backend: &mut S) {
    let _ = crate::wire_io::write_frontend(backend, &[pgelastic_wire::FrontendMessage::Terminate])
        .await;
    let _ = backend.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[derive(Default)]
    struct Counting {
        writes: usize,
        bytes: Vec<u8>,
    }
    impl AsyncWrite for Counting {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.writes += 1;
            self.bytes.extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    fn relay_with(bytes: &[u8]) -> FrameRelay {
        let mut relay = FrameRelay::default();
        relay.extend_from_slice(bytes);
        relay
    }

    #[tokio::test]
    async fn a_pumped_frame_comes_out_byte_identical() {
        let input = wire(b'Q', b"select 1\0");
        let mut relay = relay_with(&input);
        let mut out = Vec::new();
        let bytes = pump(&mut relay, &mut out, |_| {}).await.unwrap();
        assert_eq!(out, input);
        assert_eq!(bytes, input.len());
    }

    #[tokio::test]
    async fn a_streamed_oversized_frame_comes_out_byte_identical() {
        let input = wire(b'd', &vec![0xa5; 200_000]);
        let mut relay = FrameRelay::new(1024, 1 << 30);
        relay.extend_from_slice(&input);
        let mut out = Vec::new();
        pump(&mut relay, &mut out, |_| {}).await.unwrap();
        assert_eq!(out, input);
    }

    #[tokio::test]
    async fn many_frames_are_written_once_not_once_each() {
        let mut input = Vec::new();
        for _ in 0..1000 {
            input.extend_from_slice(&wire(b'D', b"row"));
        }
        let mut relay = relay_with(&input);
        let mut out = Counting::default();
        pump(&mut relay, &mut out, |_| {}).await.unwrap();
        assert_eq!(out.writes, 1);
        assert_eq!(out.bytes.len(), input.len());
    }

    /// The property the whole relay design rests on: a read future that loses
    /// a `select!` race and is dropped must not take buffered bytes with it.
    #[tokio::test]
    async fn a_dropped_read_future_never_loses_a_partial_frame() {
        let framed = wire(b'd', &vec![0x5a; 300_000]);
        let (mut writer, mut reader) = tokio::io::duplex(8192);

        let mut relay = FrameRelay::new(1024, 1 << 30);
        let mut out = Vec::new();
        let mut cancellations = 0usize;

        for chunk in framed.chunks(97) {
            // A read against an empty pipe, polled once and then dropped: this
            // is precisely what a lost `select!` race does to the relay.
            {
                let read = reader.read_buf(relay.read_target());
                futures_util::pin_mut!(read);
                assert!(futures_util::poll!(read.as_mut()).is_pending());
                cancellations += 1;
            }

            writer.write_all(chunk).await.unwrap();
            reader.read_buf(relay.read_target()).await.unwrap();
            out.extend_from_slice(&pump_to_vec(&mut relay));
        }

        assert_eq!(cancellations, framed.len().div_ceil(97));
        assert_eq!(out, framed, "a cancelled read corrupted the stream");
    }

    fn pump_to_vec(relay: &mut FrameRelay) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            match relay.next_output().unwrap() {
                Relayed::Frame(frame) => {
                    out.push(frame.tag);
                    out.extend_from_slice(
                        &i32::try_from(frame.body.len() + 4).unwrap().to_be_bytes(),
                    );
                    out.extend_from_slice(&frame.body);
                }
                Relayed::Opaque(bytes) => out.extend_from_slice(&bytes),
                Relayed::NeedMore => return out,
            }
        }
    }

    #[test]
    fn a_copy_in_that_is_still_streaming_is_not_a_closable_boundary() {
        let mut boundary = Boundary::new();
        // COPY FROM STDIN: the client sends the command, the server answers
        // CopyInResponse, and no ReadyForQuery arrives until the copy ends.
        boundary.saw_client_bytes();
        boundary.saw_backend_frame(&RawFrame {
            tag: b'G',
            body: Bytes::from_static(b"\0\0\0"),
        });
        assert!(!boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));

        boundary.saw_client_bytes();
        assert!(!boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));

        boundary.saw_backend_frame(&RawFrame {
            tag: b'Z',
            body: Bytes::from_static(b"I"),
        });
        assert!(boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));
    }

    #[test]
    fn a_session_starts_at_a_closable_boundary() {
        let boundary = Boundary::new();
        assert!(boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));
    }

    #[test]
    fn a_client_request_leaves_the_boundary() {
        let mut boundary = Boundary::new();
        boundary.saw_client_bytes();
        assert!(!boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));
    }

    #[test]
    fn only_an_idle_ready_for_query_reopens_the_boundary() {
        let mut boundary = Boundary::new();
        boundary.saw_client_bytes();
        boundary.saw_backend_frame(&RawFrame {
            tag: b'Z',
            body: Bytes::from_static(b"T"),
        });
        assert!(!boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));

        boundary.saw_backend_frame(&RawFrame {
            tag: b'Z',
            body: Bytes::from_static(b"I"),
        });
        assert!(boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));
    }

    #[test]
    fn a_failed_transaction_is_not_a_closable_boundary() {
        let mut boundary = Boundary::new();
        boundary.saw_backend_frame(&RawFrame {
            tag: b'Z',
            body: Bytes::from_static(b"E"),
        });
        assert!(!boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));
    }

    #[test]
    fn a_notification_does_not_disturb_the_boundary() {
        let mut boundary = Boundary::new();
        boundary.saw_backend_frame(&RawFrame {
            tag: b'A',
            body: Bytes::from_static(b"\0\0\0\x01chan\0\0"),
        });
        assert!(boundary.is_closable(&FrameRelay::default(), &FrameRelay::default()));
    }

    #[test]
    fn a_half_read_frame_is_never_a_closable_boundary() {
        let boundary = Boundary::new();
        let partial = relay_with(&wire(b'Q', b"select 1\0")[..4]);
        assert!(!boundary.is_closable(&partial, &FrameRelay::default()));
    }
}
