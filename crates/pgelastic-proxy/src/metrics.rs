//! Prometheus metrics and the `/metrics` listener.
//!
//! Label cardinality is fixed at compile time. Every label value below comes
//! from a closed enum, never from client input: a per-user or per-database
//! label is a memory leak with a hostile peer choosing its size.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

/// Outcome of an authentication attempt.
///
/// Deliberately two-valued. An `UnknownUser` variant would put the enumeration
/// oracle the SCRAM path is built to avoid straight into the metrics endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthOutcome {
    Success,
    Failure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    ConnectionLimit,
    ShuttingDown,
    Handshake,
    Backend,
}

/// Every variant, so the exposition carries all four series even at zero.
const REJECT_REASONS: [RejectReason; 4] = [
    RejectReason::ConnectionLimit,
    RejectReason::ShuttingDown,
    RejectReason::Handshake,
    RejectReason::Backend,
];

impl RejectReason {
    fn label(self) -> &'static str {
        match self {
            Self::ConnectionLimit => "connection_limit",
            Self::ShuttingDown => "shutting_down",
            Self::Handshake => "handshake",
            Self::Backend => "backend",
        }
    }
}

/// What the pool's connect gate did with a request for a new backend link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectGateOutcome {
    /// This request held the pool's single connect slot and dialled.
    Attempted,
    /// Another request held the slot, so this one waited for it to settle.
    Deferred,
    /// The last attempt failed inside `serverLoginRetry`, so this request was
    /// refused with the cached error without dialling.
    FastFailed,
}

const CONNECT_GATE_OUTCOMES: [ConnectGateOutcome; 3] = [
    ConnectGateOutcome::Attempted,
    ConnectGateOutcome::Deferred,
    ConnectGateOutcome::FastFailed,
];

impl ConnectGateOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Attempted => "attempted",
            Self::Deferred => "deferred",
            Self::FastFailed => "fast_failed",
        }
    }
}

/// Every code in the capacity taxonomy, so the exposition carries all six even
/// when nothing has been refused.
const ERROR_CODES: [pgelastic_capacity::ErrorCode; 6] = pgelastic_capacity::ErrorCode::ALL;

/// What one epoch observation meant, as a metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochOutcome {
    Unchanged,
    Advanced,
    Regressed,
}

const EPOCH_OUTCOMES: [EpochOutcome; 3] = [
    EpochOutcome::Unchanged,
    EpochOutcome::Advanced,
    EpochOutcome::Regressed,
];

impl EpochOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::Advanced => "advanced",
            Self::Regressed => "regressed",
        }
    }
}

impl From<crate::epoch::Observation> for EpochOutcome {
    fn from(observation: crate::epoch::Observation) -> Self {
        match observation {
            crate::epoch::Observation::Unchanged => Self::Unchanged,
            crate::epoch::Observation::Advanced { .. } => Self::Advanced,
            crate::epoch::Observation::Regressed { .. } => Self::Regressed,
        }
    }
}

const FENCE_ACTIONS: [crate::epoch::FenceAction; 4] = [
    crate::epoch::FenceAction::Close,
    crate::epoch::FenceAction::DrainThenClose,
    crate::epoch::FenceAction::ResetNow,
    crate::epoch::FenceAction::ReportUnknown,
];

fn fence_action_label(action: crate::epoch::FenceAction) -> &'static str {
    match action {
        crate::epoch::FenceAction::Close => "close",
        crate::epoch::FenceAction::DrainThenClose => "drain_then_close",
        crate::epoch::FenceAction::ResetNow => "reset_now",
        crate::epoch::FenceAction::ReportUnknown => "report_unknown",
    }
}

#[derive(Debug, Default)]
pub struct Metrics {
    clients_accepted: AtomicU64,
    clients_rejected: [AtomicU64; 4],
    clients_active: AtomicI64,
    backends_active: AtomicI64,
    client_auth: [AtomicU64; 2],
    backend_auth: [AtomicU64; 2],
    cancels_matched: AtomicU64,
    cancels_unmatched: AtomicU64,
    cancels_refused: AtomicU64,
    connect_gate: [AtomicU64; 3],
    bytes_to_backend: AtomicU64,
    bytes_to_client: AtomicU64,
    drains_completed: AtomicU64,
    drains_forced: AtomicU64,
    checkouts_reused: AtomicU64,
    checkouts_opened: AtomicU64,
    check_ins: AtomicU64,
    admission_queued: AtomicU64,
    admission_dequeued: AtomicU64,
    admission_denied: [AtomicU64; ERROR_CODES.len()],
    pins: [AtomicU64; pgelastic_pool::PinReason::ALL.len()],
    /// The elastic/pinned split, refreshed from the pool manager's ledger.
    budget: [AtomicI64; 3],
    primary_epoch: AtomicI64,
    /// `source x outcome`, flattened row-major over the three of each.
    epoch_observations: [AtomicU64; 9],
    backends_severed: [AtomicU64; FENCE_ACTIONS.len()],
    in_doubt: AtomicI64,
    /// Microseconds from the epoch advancing to the last sweep completing.
    /// Microseconds rather than milliseconds because a healthy fence finishes
    /// inside one, and a gauge that always reads zero proves nothing.
    fence_latency_us: AtomicI64,
    /// The budget is kept per instance and summed at render time. Per instance
    /// because each has its own allocator; summed rather than labelled because
    /// instance names come from operator configuration and a label whose value
    /// set is not fixed at compile time is a series set that grows.
    budgets: Mutex<BTreeMap<String, [u32; 3]>>,
    /// Instances currently believed unable to complete a commit.
    stalled: Mutex<BTreeSet<String>>,
    write_stalls_detected: AtomicU64,
    write_stall_refusals: AtomicU64,
    tenants_rerouted: AtomicU64,
    quiesces_started: AtomicU64,
    quiesces_resumed: AtomicU64,
    quiesce_leases_expired: AtomicU64,
    quiesce_transactions_queued: AtomicU64,
    /// Microseconds of the slowest quiesce/resume round trip seen so far.
    quiesce_round_trip_max_us: AtomicI64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn client_accepted(&self) {
        self.clients_accepted.fetch_add(1, Ordering::Relaxed);
        self.clients_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn client_closed(&self) {
        self.clients_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn client_rejected(&self, reason: RejectReason) {
        self.clients_rejected[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn backend_opened(&self) {
        self.backends_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn backend_closed(&self) {
        self.backends_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn client_auth(&self, outcome: AuthOutcome) {
        self.client_auth[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn backend_auth(&self, outcome: AuthOutcome) {
        self.backend_auth[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn cancel(&self, matched: bool) {
        if matched {
            &self.cancels_matched
        } else {
            &self.cancels_unmatched
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    /// A cancel that resolved to a live session but could not draw credit.
    pub fn cancel_refused(&self) {
        self.cancels_refused.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connect_gated(&self, outcome: ConnectGateOutcome) {
        self.connect_gate[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn relayed_to_backend(&self, bytes: usize) {
        self.bytes_to_backend
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn relayed_to_client(&self, bytes: usize) {
        self.bytes_to_client
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn drain_completed(&self, forced: bool) {
        if forced {
            &self.drains_forced
        } else {
            &self.drains_completed
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    pub fn active_clients(&self) -> i64 {
        self.clients_active.load(Ordering::Relaxed)
    }

    pub fn checkout(&self, reused: bool) {
        if reused {
            &self.checkouts_reused
        } else {
            &self.checkouts_opened
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    pub fn check_in(&self) {
        self.check_ins.fetch_add(1, Ordering::Relaxed);
    }

    pub fn admission_queued(&self) {
        self.admission_queued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn admission_dequeued(&self) {
        self.admission_dequeued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn admission_denied(&self, code: pgelastic_capacity::ErrorCode) {
        let index = ERROR_CODES
            .iter()
            .position(|known| *known == code)
            .unwrap_or(0);
        self.admission_denied[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn pinned(&self, reason: pgelastic_pool::PinReason) {
        let index = pgelastic_pool::PinReason::ALL
            .iter()
            .position(|known| *known == reason)
            .unwrap_or(0);
        self.pins[index].fetch_add(1, Ordering::Relaxed);
    }

    /// Publishes the pool manager's elastic/pinned split.
    ///
    /// Pinned connections are counted apart from the elastic ones on purpose:
    /// they still occupy a backend connection, so the reusable pool's ceiling
    /// drops by exactly this many, and a ceiling that drops without an
    /// attributable cause is the thing nobody can explain.
    pub fn budget(
        &self,
        instance: &crate::route::InstanceId,
        limit: u32,
        elastic: u32,
        elastic_limit: u32,
    ) {
        let mut totals = [0u64; 3];
        {
            let mut budgets = self.budgets.lock().expect("the metrics are never poisoned");
            budgets.insert(
                instance.as_str().to_owned(),
                [limit, elastic, elastic_limit],
            );
            for entry in budgets.values() {
                for (total, value) in totals.iter_mut().zip(entry) {
                    *total += u64::from(*value);
                }
            }
        }
        for (slot, total) in self.budget.iter().zip(totals) {
            slot.store(i64::try_from(total).unwrap_or(i64::MAX), Ordering::Relaxed);
        }
    }

    /// Publishes one instance's write health.
    ///
    /// The gauge is a *count* of stalled instances rather than a per-instance
    /// series: an instance name is operator-supplied, and a label whose value
    /// set is not fixed at compile time is how a metrics endpoint becomes a
    /// memory leak. Which instance is stalled is answered by the control API.
    pub fn write_health(&self, instance: &str, health: crate::stall::WriteHealth) {
        let mut stalled = self.stalled.lock().expect("the metrics are never poisoned");
        let changed = if health.is_stalled() {
            stalled.insert(instance.to_owned())
        } else {
            stalled.remove(instance)
        };
        if changed && health.is_stalled() {
            self.write_stalls_detected.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A checkout refused because its instance cannot commit.
    pub fn write_stall_refused(&self) {
        self.write_stall_refusals.fetch_add(1, Ordering::Relaxed);
    }

    pub fn tenant_rerouted(&self) {
        self.tenants_rerouted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn quiesce_started(&self) {
        self.quiesces_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn quiesce_resumed(&self, released: u64) {
        self.quiesces_resumed.fetch_add(1, Ordering::Relaxed);
        self.quiesce_transactions_queued
            .fetch_add(released, Ordering::Relaxed);
    }

    pub fn quiesce_lease_expired(&self) {
        self.quiesce_leases_expired.fetch_add(1, Ordering::Relaxed);
    }

    /// Records how long one tenant stayed quiesced, as a high-water mark.
    ///
    /// The cutover pause is the number the migration design commits to, so the
    /// interesting statistic is the worst one rather than the average.
    pub fn quiesce_round_trip(&self, elapsed: std::time::Duration) {
        let micros = i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX);
        self.quiesce_round_trip_max_us
            .fetch_max(micros, Ordering::Relaxed);
    }

    /// Records one epoch observation and the epoch the fence now holds.
    pub fn epoch_observed(&self, source: crate::epoch::EpochSource, outcome: EpochOutcome) {
        let index = source as usize * EPOCH_OUTCOMES.len() + outcome as usize;
        self.epoch_observations[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn primary_epoch(&self, epoch: crate::epoch::Epoch) {
        self.primary_epoch.store(
            i64::try_from(epoch.get()).unwrap_or(i64::MAX),
            Ordering::Relaxed,
        );
    }

    /// One backend socket severed by the fence, labelled with what the policy
    /// said to do about whatever it was carrying.
    pub fn backend_severed(&self, action: crate::epoch::FenceAction) {
        self.backends_severed[action as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// How many transactions the proxy has recorded as undecidable. A gauge
    /// rather than a counter because a restart replays the durable log, and an
    /// operator reconciling an instance wants the whole set, not this process's
    /// share of it.
    pub fn in_doubt(&self, total: usize) {
        self.in_doubt
            .store(i64::try_from(total).unwrap_or(i64::MAX), Ordering::Relaxed);
    }

    /// How long the last fence took, from the epoch advancing to the sweep
    /// finishing. Asserted against `fence.lease.retryPeriod`.
    pub fn fence_latency(&self, elapsed: std::time::Duration) {
        self.fence_latency_us.store(
            i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn render(&self) -> String {
        let mut out = String::with_capacity(2048);
        let load = |v: &AtomicU64| v.load(Ordering::Relaxed);

        counter(
            &mut out,
            "pgelastic_proxy_client_connections_accepted_total",
            "Client connections accepted by the listener.",
            &[("", load(&self.clients_accepted))],
        );
        counter(
            &mut out,
            "pgelastic_proxy_client_connections_rejected_total",
            "Client connections refused before reaching a backend.",
            &REJECT_REASONS
                .map(|reason| {
                    (
                        format!("reason=\"{}\"", reason.label()),
                        load(&self.clients_rejected[reason as usize]),
                    )
                })
                .iter()
                .map(|(labels, value)| (labels.as_str(), *value))
                .collect::<Vec<_>>(),
        );
        gauge(
            &mut out,
            "pgelastic_proxy_client_connections",
            "Client connections currently established.",
            self.clients_active.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "pgelastic_proxy_backend_connections",
            "Backend connections currently established.",
            self.backends_active.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "pgelastic_proxy_auth_total",
            "Authentication outcomes, by leg.",
            &[
                (
                    "side=\"client\",outcome=\"success\"",
                    load(&self.client_auth[AuthOutcome::Success as usize]),
                ),
                (
                    "side=\"client\",outcome=\"failure\"",
                    load(&self.client_auth[AuthOutcome::Failure as usize]),
                ),
                (
                    "side=\"backend\",outcome=\"success\"",
                    load(&self.backend_auth[AuthOutcome::Success as usize]),
                ),
                (
                    "side=\"backend\",outcome=\"failure\"",
                    load(&self.backend_auth[AuthOutcome::Failure as usize]),
                ),
            ],
        );
        counter(
            &mut out,
            "pgelastic_proxy_cancel_requests_total",
            "CancelRequests received, by whether the key resolved and drew credit.",
            &[
                ("outcome=\"matched\"", load(&self.cancels_matched)),
                ("outcome=\"unmatched\"", load(&self.cancels_unmatched)),
                ("outcome=\"refused\"", load(&self.cancels_refused)),
            ],
        );
        counter(
            &mut out,
            "pgelastic_proxy_relayed_bytes_total",
            "Bytes relayed on the passthrough path.",
            &[
                ("direction=\"to_backend\"", load(&self.bytes_to_backend)),
                ("direction=\"to_client\"", load(&self.bytes_to_client)),
            ],
        );
        counter(
            &mut out,
            "pgelastic_proxy_session_drains_total",
            "Sessions closed by a drain, by whether they reached an idle boundary first.",
            &[
                ("outcome=\"graceful\"", load(&self.drains_completed)),
                ("outcome=\"forced\"", load(&self.drains_forced)),
            ],
        );
        self.render_pooling(&mut out);
        self.render_fence(&mut out);
        self.render_availability(&mut out);
        out
    }

    /// Write-stall detection and the quiesce API.
    fn render_availability(&self, out: &mut String) {
        let load = |v: &AtomicU64| v.load(Ordering::Relaxed);
        let stalled = self
            .stalled
            .lock()
            .expect("the metrics are never poisoned")
            .len();
        gauge(
            out,
            "pgelastic_proxy_write_stalled_instances",
            "Instances whose loaded synchronous_standby_names cannot currently be \
             satisfied, so every commit on them would block.",
            i64::try_from(stalled).unwrap_or(i64::MAX),
        );
        counter(
            out,
            "pgelastic_proxy_write_stalls_detected_total",
            "Times an instance was found unable to complete a commit.",
            &[("", load(&self.write_stalls_detected))],
        );
        counter(
            out,
            "pgelastic_proxy_write_stall_refusals_total",
            "Checkouts refused rather than queued because the tenant's instance is \
             write-stalled.",
            &[("", load(&self.write_stall_refusals))],
        );
        counter(
            out,
            "pgelastic_proxy_tenant_reroutes_total",
            "Tenants moved to another instance by the control API.",
            &[("", load(&self.tenants_rerouted))],
        );
        counter(
            out,
            "pgelastic_proxy_quiesce_total",
            "Quiesce lifecycle events, by which one.",
            &[
                ("event=\"started\"", load(&self.quiesces_started)),
                ("event=\"resumed\"", load(&self.quiesces_resumed)),
                ("event=\"expired\"", load(&self.quiesce_leases_expired)),
            ],
        );
        counter(
            out,
            "pgelastic_proxy_quiesce_transactions_queued_total",
            "Transactions held at a closed gate and later released, rather than dropped.",
            &[("", load(&self.quiesce_transactions_queued))],
        );
        gauge(
            out,
            "pgelastic_proxy_quiesce_round_trip_max_us",
            "Microseconds of the longest quiesce-to-resume round trip observed.",
            self.quiesce_round_trip_max_us.load(Ordering::Relaxed),
        );
    }

    /// The primary-epoch fence's half of the exposition.
    fn render_fence(&self, out: &mut String) {
        let load = |v: &AtomicU64| v.load(Ordering::Relaxed);
        gauge(
            out,
            "pgelastic_proxy_primary_epoch",
            "The highest primary epoch this proxy has ever observed. Never decreases.",
            self.primary_epoch.load(Ordering::Relaxed),
        );
        let mut observations = Vec::new();
        for source in crate::epoch::EpochSource::ALL {
            for outcome in EPOCH_OUTCOMES {
                observations.push((
                    format!(
                        "source=\"{}\",outcome=\"{}\"",
                        source.label(),
                        outcome.label()
                    ),
                    load(
                        &self.epoch_observations
                            [source as usize * EPOCH_OUTCOMES.len() + outcome as usize],
                    ),
                ));
            }
        }
        counter(
            out,
            "pgelastic_proxy_epoch_observations_total",
            "Primary-epoch readings, by which of the three delivery paths carried them \
             and what they meant.",
            &observations
                .iter()
                .map(|(labels, value)| (labels.as_str(), *value))
                .collect::<Vec<_>>(),
        );
        counter(
            out,
            "pgelastic_proxy_backends_severed_total",
            "Backend sockets severed by the epoch fence, by what the in-flight policy \
             said to do about what they were carrying.",
            &labelled(&FENCE_ACTIONS.map(|action| {
                (
                    format!("action=\"{}\"", fence_action_label(action)),
                    load(&self.backends_severed[action as usize]),
                )
            })),
        );
        gauge(
            out,
            "pgelastic_proxy_in_doubt_transactions",
            "Transactions whose commit was forwarded and whose outcome was never observed. \
             Neither succeeded nor failed; each needs reconciling by hand.",
            self.in_doubt.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "pgelastic_proxy_epoch_fence_latency_us",
            "Microseconds from the epoch advancing to every older backend socket being \
             severed. Must stay under the lease's retryPeriod.",
            self.fence_latency_us.load(Ordering::Relaxed),
        );
    }

    /// The pooling half of the exposition.
    fn render_pooling(&self, out: &mut String) {
        let load = |v: &AtomicU64| v.load(Ordering::Relaxed);
        counter(
            out,
            "pgelastic_proxy_checkouts_total",
            "Backend checkouts, by whether a parked link was reused.",
            &[
                ("source=\"reused\"", load(&self.checkouts_reused)),
                ("source=\"opened\"", load(&self.checkouts_opened)),
            ],
        );
        counter(
            out,
            "pgelastic_proxy_backend_connect_gate_total",
            "Trips through a pool's connect gate, by what it decided. A deferred request comes back.",
            &labelled(&CONNECT_GATE_OUTCOMES.map(|outcome| {
                (
                    format!("outcome=\"{}\"", outcome.label()),
                    load(&self.connect_gate[outcome as usize]),
                )
            })),
        );
        counter(
            out,
            "pgelastic_proxy_check_ins_total",
            "Backend links returned to the pool through the release gate.",
            &[("", load(&self.check_ins))],
        );
        counter(
            out,
            "pgelastic_proxy_admission_queued_total",
            "Checkouts that had to wait, and those that were then served.",
            &[
                ("outcome=\"enqueued\"", load(&self.admission_queued)),
                ("outcome=\"granted\"", load(&self.admission_dequeued)),
            ],
        );
        counter(
            out,
            "pgelastic_proxy_admission_denied_total",
            "Checkouts refused, by the code the client was given.",
            &labelled(&ERROR_CODES.map(|code| {
                (
                    format!("code=\"{code}\",sqlstate=\"{}\"", code.sqlstate()),
                    load(
                        &self.admission_denied[ERROR_CODES
                            .iter()
                            .position(|known| *known == code)
                            .unwrap_or(0)],
                    ),
                )
            })),
        );
        counter(
            out,
            "pgelastic_proxy_pins_total",
            "Links pinned to one client because a tripwire found unscrubbable state.",
            &labelled(&pgelastic_pool::PinReason::ALL.map(|reason| {
                (
                    format!("reason=\"{}\"", reason.as_str()),
                    load(
                        &self.pins[pgelastic_pool::PinReason::ALL
                            .iter()
                            .position(|known| *known == reason)
                            .unwrap_or(0)],
                    ),
                )
            })),
        );
        gauge(
            out,
            "pgelastic_proxy_backend_budget",
            "The pool's total backend connection budget.",
            self.budget[0].load(Ordering::Relaxed),
        );
        gauge(
            out,
            "pgelastic_proxy_backend_elastic_connections",
            "Reusable backend connections currently open.",
            self.budget[1].load(Ordering::Relaxed),
        );
        gauge(
            out,
            "pgelastic_proxy_backend_elastic_limit",
            "The ceiling the reusable pool can reach, which is the budget less \
             every pinned connection.",
            self.budget[2].load(Ordering::Relaxed),
        );
    }
}

fn labelled<const N: usize>(series: &[(String, u64); N]) -> Vec<(&str, u64)> {
    series
        .iter()
        .map(|(labels, value)| (labels.as_str(), *value))
        .collect()
}

fn counter(out: &mut String, name: &str, help: &str, series: &[(&str, u64)]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    for (labels, value) in series {
        if labels.is_empty() {
            let _ = writeln!(out, "{name} {value}");
        } else {
            let _ = writeln!(out, "{name}{{{labels}}} {value}");
        }
    }
}

fn gauge(out: &mut String, name: &str, help: &str, value: i64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

/// Serves `/metrics` until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            result = listener.accept() => result,
            () = wait_true(&mut shutdown) => return,
        };
        let Ok((socket, _)) = accepted else { continue };
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                let metrics = Arc::clone(&metrics);
                async move { Ok::<_, std::convert::Infallible>(respond(&request, &metrics)) }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(socket), service)
                .await;
        });
    }
}

fn respond(request: &Request<hyper::body::Incoming>, metrics: &Metrics) -> Response<Full<Bytes>> {
    match request.uri().path() {
        "/metrics" => Response::builder()
            .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
            .body(Full::new(Bytes::from(metrics.render())))
            .expect("static response builds"),
        "/healthz" => Response::new(Full::new(Bytes::from_static(b"ok\n"))),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"not found\n")))
            .expect("static response builds"),
    }
}

async fn wait_true(rx: &mut tokio::sync::watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_exposition_carries_every_series_even_at_zero() {
        let metrics = Metrics::new();
        let rendered = metrics.render();
        for name in [
            "pgelastic_proxy_client_connections_accepted_total",
            "pgelastic_proxy_client_connections_rejected_total",
            "pgelastic_proxy_client_connections",
            "pgelastic_proxy_backend_connections",
            "pgelastic_proxy_auth_total",
            "pgelastic_proxy_cancel_requests_total",
            "pgelastic_proxy_relayed_bytes_total",
            "pgelastic_proxy_session_drains_total",
            "pgelastic_proxy_primary_epoch",
            "pgelastic_proxy_epoch_observations_total",
            "pgelastic_proxy_backends_severed_total",
            "pgelastic_proxy_in_doubt_transactions",
            "pgelastic_proxy_epoch_fence_latency_us",
        ] {
            assert!(
                rendered.contains(&format!("# TYPE {name}")),
                "missing {name}"
            );
        }
    }

    #[test]
    fn the_fence_reports_every_path_and_every_verdict_even_at_zero() {
        let rendered = Metrics::new().render();
        for source in crate::epoch::EpochSource::ALL {
            for outcome in EPOCH_OUTCOMES {
                assert!(
                    rendered.contains(&format!(
                        "source=\"{}\",outcome=\"{}\"}} 0",
                        source.label(),
                        outcome.label()
                    )),
                    "missing the {source}/{} series",
                    outcome.label()
                );
            }
        }
        for action in FENCE_ACTIONS {
            assert!(
                rendered.contains(&format!("action=\"{}\"}} 0", fence_action_label(action))),
                "missing the {action:?} series"
            );
        }
    }

    #[test]
    fn an_in_doubt_transaction_is_visible_as_a_gauge_rather_than_only_in_a_log() {
        let metrics = Metrics::new();
        metrics.epoch_observed(
            crate::epoch::EpochSource::Push,
            crate::epoch::Observation::Advanced {
                from: crate::epoch::Epoch::UNKNOWN,
                to: crate::epoch::Epoch::new(7),
            }
            .into(),
        );
        metrics.primary_epoch(crate::epoch::Epoch::new(7));
        metrics.backend_severed(crate::epoch::FenceAction::ReportUnknown);
        metrics.in_doubt(1);
        metrics.fence_latency(std::time::Duration::from_micros(850));

        let rendered = metrics.render();
        assert!(rendered.contains("pgelastic_proxy_primary_epoch 7"));
        assert!(rendered.contains("source=\"push\",outcome=\"advanced\"} 1"));
        assert!(rendered.contains("action=\"report_unknown\"} 1"));
        assert!(rendered.contains("pgelastic_proxy_in_doubt_transactions 1"));
        assert!(rendered.contains("pgelastic_proxy_epoch_fence_latency_us 850"));
    }

    #[test]
    fn counters_and_gauges_move() {
        let metrics = Metrics::new();
        metrics.client_accepted();
        metrics.client_auth(AuthOutcome::Success);
        metrics.backend_auth(AuthOutcome::Failure);
        metrics.cancel(true);
        metrics.relayed_to_client(4096);
        let rendered = metrics.render();
        assert!(rendered.contains("pgelastic_proxy_client_connections 1"));
        assert!(rendered.contains("side=\"client\",outcome=\"success\"} 1"));
        assert!(rendered.contains("side=\"backend\",outcome=\"failure\"} 1"));
        assert!(rendered.contains("outcome=\"matched\"} 1"));
        assert!(rendered.contains("direction=\"to_client\"} 4096"));
        metrics.client_closed();
        assert!(
            metrics
                .render()
                .contains("pgelastic_proxy_client_connections 0")
        );
    }

    #[test]
    fn the_series_set_does_not_grow_with_traffic() {
        let series = |metrics: &Metrics| {
            metrics
                .render()
                .lines()
                .filter(|line| !line.starts_with('#'))
                .map(|line| line.split(' ').next().unwrap_or_default().to_owned())
                .collect::<Vec<_>>()
        };

        let metrics = Metrics::new();
        let idle = series(&metrics);

        for reason in [
            RejectReason::ConnectionLimit,
            RejectReason::ShuttingDown,
            RejectReason::Handshake,
            RejectReason::Backend,
        ] {
            metrics.client_rejected(reason);
        }
        for _ in 0..1000 {
            metrics.client_accepted();
            metrics.client_auth(AuthOutcome::Failure);
            metrics.cancel(false);
            metrics.drain_completed(true);
        }
        assert_eq!(series(&metrics), idle);
    }
}
