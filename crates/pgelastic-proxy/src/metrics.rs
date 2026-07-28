//! Prometheus metrics and the `/metrics` listener.
//!
//! Label cardinality is fixed at compile time. Every label value below comes
//! from a closed enum, never from client input: a per-user or per-database
//! label is a memory leak with a hostile peer choosing its size.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

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
    bytes_to_backend: AtomicU64,
    bytes_to_client: AtomicU64,
    drains_completed: AtomicU64,
    drains_forced: AtomicU64,
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
            "CancelRequests received, by whether the key resolved to a session.",
            &[
                ("outcome=\"matched\"", load(&self.cancels_matched)),
                ("outcome=\"unmatched\"", load(&self.cancels_unmatched)),
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
        out
    }
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
        ] {
            assert!(
                rendered.contains(&format!("# TYPE {name}")),
                "missing {name}"
            );
        }
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
