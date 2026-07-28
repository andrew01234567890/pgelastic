//! Proactive write-stall detection.
//!
//! `dataDurability: Required` means a commit **stalls** when quorum is lost: a
//! three-node instance that loses both standbys blocks rather than silently
//! degrading to asynchronous replication. That is the correct behaviour, and on
//! its own it is indistinguishable from a slow query. The backend parks in
//! `IPC.SyncRep` holding a pooled connection, the next client takes another
//! one, and within seconds a single instance's quorum loss has consumed the
//! pool's whole budget — including the burst headroom that belongs to tenants
//! on instances that are perfectly healthy.
//!
//! So the proxy has to see it coming rather than discover it by running out of
//! backends. [`StallProbe`] samples the primary on a timer and compares the
//! number of standbys actually streaming against the `num_sync` the postmaster
//! has **loaded** — read out of the live server with `current_setting`, never
//! from a CR spec, so a partially applied reload cannot fool it. Fewer
//! streaming sync candidates than `num_sync` means the next commit will block,
//! and [`StallMonitor`] publishes that verdict to the checkout path, which
//! refuses rather than queues.
//!
//! # What this cannot see
//!
//! **A standby that is streaming but not flushing.** `synchronous_commit = on`
//! waits for `flush_lsn`, and a standby whose disk has stopped acknowledging
//! writes keeps its `pg_stat_replication` row, keeps `state = 'streaming'` and
//! keeps its `sync_state`, while every commit waits on it forever. Its
//! `flush_lsn` falls behind, but "behind" is also what a healthy standby under
//! load looks like, and there is no threshold that separates the two without
//! inventing one. The counting detector therefore does **not** cover it, and
//! this module does not pretend otherwise: such an instance is reported
//! [`WriteHealth::Writable`] and the stall reaches clients as latency. The
//! operator-side `WriteStalled` condition, which watches commit latency
//! directly, is the half of the system that sees it.
//!
//! Two other honest gaps, both reported as [`WriteHealth::Unknown`] rather than
//! guessed at:
//!
//! - A role that cannot read `pg_stat_replication` sees **zero rows** whatever
//!   the truth is, which would make every instance look stalled. The probe asks
//!   whether it is entitled to the view and refuses to draw a conclusion when
//!   it is not.
//! - A backend that cannot be reached is not evidence of a stall. The probe
//!   keeps the previous verdict and says so.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use pgelastic_wire::{BackendMessage, FrontendMessage};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::error::{ProxyError, Result};
use crate::relay::{FrameRelay, Relayed};
use crate::route::InstanceId;

/// Everything the verdict needs, in one round trip.
///
/// `current_setting('synchronous_standby_names')` is the value the postmaster
/// has **loaded**, which is the only value that decides whether a commit
/// blocks. The `sync_state` filter counts the standbys `PostgreSQL` itself
/// considers able to satisfy the clause, so the proxy never has to re-implement
/// the matching rules for `*`, quoted names or priority promotion.
const STALL_SQL: &str = "SELECT current_setting('synchronous_standby_names'), \
                         pg_is_in_recovery()::text, \
                         (SELECT count(*) FROM pg_stat_replication \
                          WHERE state = 'streaming')::text, \
                         (SELECT count(*) FROM pg_stat_replication \
                          WHERE state = 'streaming' \
                            AND sync_state IN ('sync', 'quorum'))::text, \
                         (current_setting('is_superuser')::bool \
                          OR pg_has_role(current_user, 'pg_read_all_stats', 'usage'))::text";

/// How `synchronous_standby_names` counts its acknowledgements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMethod {
    /// `FIRST k (...)`, and the bare list, which is `FIRST 1` spelled shorter.
    Priority,
    /// `ANY k (...)`.
    Quorum,
}

/// The parsed `synchronous_standby_names` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRepClause {
    pub method: SyncMethod,
    pub num_sync: u32,
    pub members: Vec<String>,
}

impl SyncRepClause {
    /// Parses the loaded clause, or `None` when synchronous replication is off.
    ///
    /// `PostgreSQL`'s three accepted forms, and nothing else:
    ///
    /// ```text
    /// [FIRST] num_sync ( standby_name [, ...] )
    /// ANY num_sync ( standby_name [, ...] )
    /// standby_name [, ...]
    /// ```
    ///
    /// A clause that does not parse returns `None` — treated as "no evidence"
    /// and never as "no synchronous replication", because the caller turns
    /// `None` into [`WriteHealth::Writable`] only after checking that the raw
    /// text was actually empty.
    pub fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        let (method, rest) = if let Some(rest) = strip_keyword(text, "ANY") {
            (SyncMethod::Quorum, rest)
        } else if let Some(rest) = strip_keyword(text, "FIRST") {
            (SyncMethod::Priority, rest)
        } else {
            (SyncMethod::Priority, text)
        };

        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() {
            // The bare list form. Every listed standby is a candidate and one
            // acknowledgement is required.
            if method == SyncMethod::Quorum || rest.starts_with('(') {
                return None;
            }
            let members = split_names(rest);
            return (!members.is_empty()).then_some(Self {
                method: SyncMethod::Priority,
                num_sync: 1,
                members,
            });
        }

        let num_sync: u32 = digits.parse().ok()?;
        let list = rest[digits.len()..].trim_start();
        let list = list.strip_prefix('(')?.strip_suffix(')')?;
        let members = split_names(list);
        (!members.is_empty()).then_some(Self {
            method,
            num_sync,
            members,
        })
    }
}

/// One sample of an instance's ability to commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncEvidence {
    /// The raw `synchronous_standby_names` the postmaster has loaded.
    pub standby_names: String,
    pub in_recovery: bool,
    pub streaming: u32,
    /// Streaming standbys `PostgreSQL` counts towards the clause.
    pub streaming_sync: u32,
    /// Whether this role can see other roles' rows in `pg_stat_replication`.
    pub may_read_stats: bool,
}

impl SyncEvidence {
    /// The verdict this sample supports, and nothing beyond it.
    pub fn verdict(&self) -> WriteHealth {
        if self.in_recovery {
            // A standby never waits for synchronous replication: it is not the
            // one committing.
            return WriteHealth::Writable;
        }
        let Some(clause) = SyncRepClause::parse(&self.standby_names) else {
            return if self.standby_names.trim().is_empty() {
                WriteHealth::Writable
            } else {
                WriteHealth::Unknown(UnknownReason::UnparsableClause)
            };
        };
        if !self.may_read_stats {
            return WriteHealth::Unknown(UnknownReason::StatsHidden);
        }
        if self.streaming_sync < clause.num_sync {
            WriteHealth::Stalled {
                required: clause.num_sync,
                streaming: self.streaming_sync,
            }
        } else {
            WriteHealth::Writable
        }
    }
}

/// Why the probe declined to reach a verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownReason {
    /// The probe's role cannot see `pg_stat_replication`, so a count of zero
    /// carries no information.
    StatsHidden,
    /// `synchronous_standby_names` is set to something this parser does not
    /// recognise.
    UnparsableClause,
    /// The instance could not be sampled at all.
    Unreachable,
}

impl UnknownReason {
    pub fn label(self) -> &'static str {
        match self {
            Self::StatsHidden => "the probe's role cannot read pg_stat_replication",
            Self::UnparsableClause => "synchronous_standby_names could not be parsed",
            Self::Unreachable => "the instance could not be sampled",
        }
    }
}

/// What the proxy believes about an instance's ability to complete a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteHealth {
    /// Nothing observed says a commit would block. **Not** a guarantee: see the
    /// streaming-but-not-flushing gap in the module documentation.
    Writable,
    /// Fewer standbys are streaming than the loaded `num_sync` requires, so the
    /// next commit will park in `IPC.SyncRep` and never return.
    Stalled { required: u32, streaming: u32 },
    /// No conclusion was reachable. Treated exactly as [`Self::Writable`] by
    /// the checkout path — refusing a tenant on the strength of a question the
    /// proxy could not answer would be inventing an outcome.
    Unknown(UnknownReason),
}

impl WriteHealth {
    pub fn is_stalled(self) -> bool {
        matches!(self, Self::Stalled { .. })
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Writable => "writable",
            Self::Stalled { .. } => "stalled",
            Self::Unknown(_) => "unknown",
        }
    }
}

impl std::fmt::Display for WriteHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Writable => f.write_str("writable"),
            Self::Stalled {
                required,
                streaming,
            } => write!(
                f,
                "write-stalled: {streaming} of the {required} synchronous standbys the loaded \
                 synchronous_standby_names requires are streaming"
            ),
            Self::Unknown(reason) => write!(f, "unknown ({})", reason.label()),
        }
    }
}

/// One instance's published write health.
///
/// Per instance and never shared, which is what bounds the blast radius: the
/// checkout path consults the monitor belonging to the instance the tenant is
/// routed to, so a stalled instance can refuse its own tenants without any
/// tenant on another instance being able to observe that it happened.
#[derive(Debug)]
pub struct StallMonitor {
    instance: InstanceId,
    health: watch::Sender<WriteHealth>,
    /// Samples in a row agreeing with `pending`.
    pending: Mutex<Pending>,
    confirmations: u32,
    stalled_since: Mutex<Option<Instant>>,
    detections: AtomicU64,
    refusals: AtomicU64,
    fail_fast: bool,
}

#[derive(Debug)]
struct Pending {
    verdict: WriteHealth,
    agreeing: u32,
}

impl StallMonitor {
    pub fn new(instance: InstanceId, confirmations: u32, fail_fast: bool) -> Arc<Self> {
        Arc::new(Self {
            instance,
            health: watch::Sender::new(WriteHealth::Writable),
            pending: Mutex::new(Pending {
                verdict: WriteHealth::Writable,
                agreeing: 0,
            }),
            confirmations: confirmations.max(1),
            stalled_since: Mutex::new(None),
            detections: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
            fail_fast,
        })
    }

    pub fn instance(&self) -> &InstanceId {
        &self.instance
    }

    pub fn health(&self) -> WriteHealth {
        *self.health.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<WriteHealth> {
        self.health.subscribe()
    }

    pub fn detections(&self) -> u64 {
        self.detections.load(Ordering::Relaxed)
    }

    pub fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::Relaxed)
    }

    pub fn stalled_for(&self) -> Option<Duration> {
        self.stalled_since
            .lock()
            .expect("the stall monitor is never poisoned")
            .map(|since| since.elapsed())
    }

    /// Whether a checkout onto this instance must be refused now, counting the
    /// refusal if it is.
    ///
    /// Only a confirmed [`WriteHealth::Stalled`] refuses. An `Unknown` verdict
    /// does not, because a proxy that cannot answer the question has not earned
    /// the right to fail a tenant's transaction.
    pub fn must_refuse(&self) -> Option<WriteHealth> {
        if !self.fail_fast {
            return None;
        }
        let health = self.health();
        health.is_stalled().then(|| {
            self.refusals.fetch_add(1, Ordering::Relaxed);
            health
        })
    }

    /// Folds one sample in, publishing only once `confirmations` samples agree.
    pub fn observe(&self, verdict: WriteHealth) -> WriteHealth {
        let publish = {
            let mut pending = self
                .pending
                .lock()
                .expect("the stall monitor is never poisoned");
            if pending.verdict == verdict {
                pending.agreeing = pending.agreeing.saturating_add(1);
            } else {
                pending.verdict = verdict;
                pending.agreeing = 1;
            }
            pending.agreeing >= self.confirmations
        };
        if !publish {
            return self.health();
        }

        let mut changed = false;
        self.health.send_if_modified(|current| {
            if *current == verdict {
                false
            } else {
                *current = verdict;
                changed = true;
                true
            }
        });
        if changed {
            let mut since = self
                .stalled_since
                .lock()
                .expect("the stall monitor is never poisoned");
            if verdict.is_stalled() {
                *since = Some(Instant::now());
                self.detections.fetch_add(1, Ordering::Relaxed);
                warn!(
                    instance = %self.instance,
                    %verdict,
                    "this instance cannot complete a commit; new checkouts onto it are refused \
                     rather than queued so its tenants do not consume the pool other tenants need"
                );
            } else {
                let held = since.take().map(|start| start.elapsed());
                *since = None;
                info!(
                    instance = %self.instance,
                    %verdict,
                    stalled_for_ms = held.map(|d| d.as_millis()),
                    "this instance can commit again"
                );
            }
        }
        verdict
    }
}

/// A persistent sampling connection to one instance.
///
/// Persistent rather than dialled per sample: the round trip is what the
/// detection lag is made of, and re-authenticating four times a second to
/// measure a condition that lasts minutes is pure cost. The probe only ever
/// runs `SELECT`s, so it cannot itself be caught by the stall it is looking
/// for — synchronous replication is waited on at commit, and a read-only
/// transaction writes no WAL to wait for.
///
/// One connection per instance, outside the pool's budget for the same reason
/// the epoch prober is: a probe that had to queue for admission would be
/// blocked by exactly the saturation it exists to prevent. It has to be priced
/// into `backendConnections` alongside the epoch prober's, as one more
/// connection the operator does not get to hand out.
pub struct StallProbe {
    pub backend: crate::config::BackendConfig,
    pub tls: Option<crate::tls::BackendTls>,
    pub kdf: crate::scram::KdfPool,
    pub monitor: Arc<StallMonitor>,
    pub metrics: Arc<crate::metrics::Metrics>,
}

impl std::fmt::Debug for StallProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StallProbe")
            .field("instance", self.monitor.instance())
            .field("backend", &self.backend.address)
            .finish_non_exhaustive()
    }
}

/// Longest the redial backoff stretches the sampling interval.
///
/// An instance that will not answer is a problem the connect gate and the
/// operator already own; the probe's job is to notice when it comes back, not
/// to keep knocking at full rate while it is down.
const MAX_BACKOFF_MULTIPLIER: u32 = 8;

/// Samples one instance every `interval` until `shutdown` goes true.
pub async fn probe_loop(
    probe: StallProbe,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut session: Option<crate::backend::BackendSession> = None;
    let mut relay = FrameRelay::default();
    let mut failures: u32 = 0;
    loop {
        let wait = interval * failures.clamp(1, MAX_BACKOFF_MULTIPLIER);
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = wait_true(&mut shutdown) => break,
        }

        if session.is_none() {
            match dial(&probe).await {
                Ok((opened, buffered)) => {
                    relay = FrameRelay::default();
                    relay.extend_from_slice(&buffered);
                    session = Some(opened);
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    debug!(
                        instance = %probe.monitor.instance(),
                        %error,
                        failures,
                        "the write-stall probe could not reach the instance"
                    );
                    probe
                        .monitor
                        .observe(WriteHealth::Unknown(UnknownReason::Unreachable));
                    continue;
                }
            }
        }
        failures = 0;

        let Some(open) = session.as_mut() else {
            continue;
        };
        match sample(&mut open.stream, &mut relay).await {
            Ok(evidence) => {
                let verdict = evidence.verdict();
                probe.monitor.observe(verdict);
                probe
                    .metrics
                    .write_health(probe.monitor.instance().as_str(), verdict);
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                debug!(
                    instance = %probe.monitor.instance(),
                    %error,
                    "the write-stall probe's connection failed; it will be reopened"
                );
                session = None;
                probe
                    .monitor
                    .observe(WriteHealth::Unknown(UnknownReason::Unreachable));
            }
        }
    }

    if let Some(mut open) = session {
        crate::session::terminate_backend(&mut open.stream).await;
    }
}

async fn dial(probe: &StallProbe) -> Result<(crate::backend::BackendSession, Vec<u8>)> {
    let startup = pgelastic_wire::StartupMessage::new(
        pgelastic_wire::ProtocolVersion::V3_0,
        vec![(
            Bytes::from_static(b"application_name"),
            Bytes::from_static(b"pgelastic_stall_probe"),
        )],
    );
    let session =
        crate::backend::connect(&probe.backend, probe.tls.as_ref(), &probe.kdf, &startup).await?;
    let buffered = session.buf.as_slice().to_vec();
    Ok((session, buffered))
}

/// Runs [`STALL_SQL`] and reads its single row.
pub async fn sample(
    stream: &mut crate::stream::BackendStream,
    relay: &mut FrameRelay,
) -> Result<SyncEvidence> {
    let mut wire = BytesMut::new();
    FrontendMessage::Query(Bytes::from_static(STALL_SQL.as_bytes())).encode(&mut wire);
    stream.write_all(&wire).await?;
    stream.flush().await?;

    let mut row: Option<Vec<Option<Bytes>>> = None;
    let mut failure = None;
    loop {
        match relay.next_output()? {
            Relayed::NeedMore => {
                if stream.read_buf(relay.read_target()).await? == 0 {
                    return Err(ProxyError::backend(
                        "the backend closed the connection while its write health was sampled",
                    ));
                }
            }
            Relayed::Opaque(_) => {}
            Relayed::Frame(frame) => match BackendMessage::decode(&frame)? {
                BackendMessage::DataRow(data) => {
                    row = Some(crate::epoch::verify::columns(data.as_bytes())?);
                }
                BackendMessage::ErrorResponse(fields) => {
                    failure = Some(
                        String::from_utf8_lossy(fields.message().map_or(&b""[..], |m| m.as_ref()))
                            .into_owned(),
                    );
                }
                BackendMessage::ReadyForQuery(_) => break,
                _ => {}
            },
        }
    }

    if let Some(message) = failure {
        return Err(ProxyError::backend(format!(
            "sampling an instance's write health failed: {message}"
        )));
    }
    let row = row.ok_or_else(|| ProxyError::backend("the write-health sample returned no row"))?;
    Ok(evidence_from(&row))
}

fn evidence_from(row: &[Option<Bytes>]) -> SyncEvidence {
    let text = |index: usize| -> Option<String> {
        row.get(index)
            .and_then(Option::as_ref)
            .map(|value| String::from_utf8_lossy(value).into_owned())
    };
    let flag = |index: usize| text(index).is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let count = |index: usize| text(index).and_then(|v| v.parse().ok()).unwrap_or(0);
    SyncEvidence {
        standby_names: text(0).unwrap_or_default(),
        in_recovery: flag(1),
        streaming: count(2),
        streaming_sync: count(3),
        may_read_stats: flag(4),
    }
}

/// Strips a leading keyword and the whitespace after it, case-insensitively.
fn strip_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let head = text.get(..keyword.len())?;
    if !head.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &text[keyword.len()..];
    rest.starts_with(char::is_whitespace)
        .then(|| rest.trim_start())
}

/// Splits a standby list on commas, honouring double-quoted names.
///
/// A quoted name may contain a comma, and `""` inside one is a literal quote —
/// which is why this is a scanner rather than a `split(',')`.
fn split_names(list: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = list.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                current.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => {
                push_name(&mut names, &mut current);
            }
            _ => current.push(c),
        }
    }
    push_name(&mut names, &mut current);
    names
}

fn push_name(names: &mut Vec<String>, current: &mut String) {
    let name = current.trim().to_owned();
    current.clear();
    if !name.is_empty() {
        names.push(name);
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

    fn evidence(names: &str, streaming_sync: u32) -> SyncEvidence {
        SyncEvidence {
            standby_names: names.to_owned(),
            in_recovery: false,
            streaming: streaming_sync,
            streaming_sync,
            may_read_stats: true,
        }
    }

    #[test]
    fn an_empty_clause_is_synchronous_replication_switched_off() {
        assert_eq!(SyncRepClause::parse(""), None);
        assert_eq!(SyncRepClause::parse("   "), None);
    }

    #[test]
    fn a_bare_list_requires_one_acknowledgement() {
        let clause = SyncRepClause::parse("standby1, standby2").unwrap();
        assert_eq!(clause.method, SyncMethod::Priority);
        assert_eq!(clause.num_sync, 1);
        assert_eq!(clause.members, ["standby1", "standby2"]);
    }

    #[test]
    fn any_and_first_are_read_as_the_quorum_and_priority_forms() {
        let any = SyncRepClause::parse("ANY 2 (a, b, c)").unwrap();
        assert_eq!(any.method, SyncMethod::Quorum);
        assert_eq!(any.num_sync, 2);
        let first = SyncRepClause::parse("FIRST 1 (a, b)").unwrap();
        assert_eq!(first.method, SyncMethod::Priority);
        assert_eq!(first.num_sync, 1);
    }

    #[test]
    fn the_keyword_is_case_insensitive_as_postgresql_reads_it() {
        assert_eq!(
            SyncRepClause::parse("any 1 (a)").unwrap().method,
            SyncMethod::Quorum
        );
        assert_eq!(SyncRepClause::parse("First 2 (a, b)").unwrap().num_sync, 2);
    }

    #[test]
    fn the_legacy_numeric_form_without_first_is_the_priority_form() {
        let clause = SyncRepClause::parse("2 (a, b, c)").unwrap();
        assert_eq!(clause.method, SyncMethod::Priority);
        assert_eq!(clause.num_sync, 2);
        assert_eq!(clause.members.len(), 3);
    }

    #[test]
    fn a_quoted_name_may_contain_a_comma_and_an_escaped_quote() {
        let clause = SyncRepClause::parse(r#"ANY 1 ("node,one", "say""hi")"#).unwrap();
        assert_eq!(clause.members, [r"node,one", r#"say"hi"#]);
    }

    #[test]
    fn a_wildcard_is_carried_through_as_a_member() {
        assert_eq!(SyncRepClause::parse("ANY 1 (*)").unwrap().members, ["*"]);
    }

    #[test]
    fn a_clause_that_does_not_parse_is_not_read_as_replication_being_off() {
        assert_eq!(SyncRepClause::parse("ANY (a, b)"), None);
        assert_eq!(SyncRepClause::parse("ANY 1 a, b"), None);
        assert_eq!(
            evidence("ANY (a, b)", 0).verdict(),
            WriteHealth::Unknown(UnknownReason::UnparsableClause)
        );
    }

    #[test]
    fn fewer_streaming_standbys_than_num_sync_is_a_stall() {
        assert_eq!(
            evidence("ANY 1 (a, b)", 0).verdict(),
            WriteHealth::Stalled {
                required: 1,
                streaming: 0
            }
        );
        assert_eq!(
            evidence("FIRST 2 (a, b, c)", 1).verdict(),
            WriteHealth::Stalled {
                required: 2,
                streaming: 1
            }
        );
    }

    #[test]
    fn enough_streaming_standbys_is_writable() {
        assert_eq!(evidence("ANY 1 (a, b)", 1).verdict(), WriteHealth::Writable);
        assert_eq!(
            evidence("FIRST 2 (a, b, c)", 2).verdict(),
            WriteHealth::Writable
        );
    }

    #[test]
    fn an_instance_with_no_synchronous_replication_never_stalls() {
        assert_eq!(evidence("", 0).verdict(), WriteHealth::Writable);
    }

    #[test]
    fn a_standby_is_writable_because_it_is_not_the_one_committing() {
        let mut sample = evidence("ANY 1 (a)", 0);
        sample.in_recovery = true;
        assert_eq!(sample.verdict(), WriteHealth::Writable);
    }

    #[test]
    fn a_role_that_cannot_see_the_replication_view_reaches_no_verdict() {
        let mut sample = evidence("ANY 1 (a)", 0);
        sample.may_read_stats = false;
        assert_eq!(
            sample.verdict(),
            WriteHealth::Unknown(UnknownReason::StatsHidden)
        );
        assert!(!sample.verdict().is_stalled());
    }

    fn monitor(confirmations: u32) -> Arc<StallMonitor> {
        StallMonitor::new(InstanceId::new("inst"), confirmations, true)
    }

    #[test]
    fn one_disagreeing_sample_does_not_flap_the_verdict() {
        let monitor = monitor(2);
        let stalled = WriteHealth::Stalled {
            required: 1,
            streaming: 0,
        };
        monitor.observe(stalled);
        assert_eq!(monitor.health(), WriteHealth::Writable);
        monitor.observe(stalled);
        assert_eq!(monitor.health(), stalled);
    }

    #[test]
    fn the_verdict_returns_to_writable_once_the_standbys_are_back() {
        let monitor = monitor(1);
        monitor.observe(WriteHealth::Stalled {
            required: 1,
            streaming: 0,
        });
        assert!(monitor.health().is_stalled());
        assert!(monitor.stalled_for().is_some());
        monitor.observe(WriteHealth::Writable);
        assert_eq!(monitor.health(), WriteHealth::Writable);
        assert!(monitor.stalled_for().is_none());
        assert_eq!(monitor.detections(), 1);
    }

    #[test]
    fn an_unknown_verdict_never_refuses_a_checkout() {
        let monitor = monitor(1);
        monitor.observe(WriteHealth::Unknown(UnknownReason::Unreachable));
        assert!(monitor.must_refuse().is_none());
        assert_eq!(monitor.refusals(), 0);
    }

    #[test]
    fn a_confirmed_stall_refuses_and_counts_the_refusal() {
        let monitor = monitor(1);
        monitor.observe(WriteHealth::Stalled {
            required: 1,
            streaming: 0,
        });
        assert!(monitor.must_refuse().is_some());
        assert_eq!(monitor.refusals(), 1);
    }

    #[test]
    fn fail_fast_off_leaves_the_verdict_visible_but_refuses_nothing() {
        let monitor = StallMonitor::new(InstanceId::new("inst"), 1, false);
        monitor.observe(WriteHealth::Stalled {
            required: 1,
            streaming: 0,
        });
        assert!(monitor.health().is_stalled());
        assert!(monitor.must_refuse().is_none());
    }

    #[test]
    fn a_sample_row_is_read_positionally() {
        let row: Vec<Option<Bytes>> = ["ANY 1 (a)", "false", "1", "0", "true"]
            .iter()
            .map(|value| Some(Bytes::from_static(value.as_bytes())))
            .collect();
        let evidence = evidence_from(&row);
        assert_eq!(evidence.standby_names, "ANY 1 (a)");
        assert!(!evidence.in_recovery);
        assert_eq!(evidence.streaming, 1);
        assert_eq!(evidence.streaming_sync, 0);
        assert!(evidence.may_read_stats);
    }

    #[test]
    fn the_probe_reads_the_loaded_setting_rather_than_any_configuration() {
        assert!(STALL_SQL.contains("current_setting('synchronous_standby_names')"));
        assert!(STALL_SQL.contains("pg_stat_replication"));
    }
}
