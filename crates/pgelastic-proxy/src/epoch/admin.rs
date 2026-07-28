//! The push path: the admin endpoint the promoting agent calls.
//!
//! The agent calls this **before** it writes `currentPrimary`, so in the happy
//! path the proxy has already severed every old-epoch socket by the time
//! anything else in the cluster can observe that a promotion happened. It is
//! the fastest of the three paths and the least trustworthy: it needs the agent
//! to be able to reach the proxy, which is exactly what a partition removes.
//!
//! Its own listener rather than a route on `/metrics`: this endpoint changes
//! behaviour, the metrics endpoint does not, and the two belong on different
//! ports so they can be exposed differently.

use std::sync::Arc;

use http_body_util::{BodyExt as _, Full};
use hyper::body::Body as _;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::debug;

use super::{Epoch, EpochFence, EpochSource, Observation};

/// Largest body the endpoint will read. The payload is a decimal integer.
const MAX_BODY_BYTES: u64 = 64;

/// What the caller is told about its own push.
///
/// A `Regressed` answer is not an error: the agent's report was truthful and
/// the proxy is simply further ahead. Reporting it as a failure would invite a
/// retry loop against a proxy that is already correct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Advanced,
    Unchanged,
    Regressed,
}

impl PushOutcome {
    pub fn label(self) -> &'static str {
        match self {
            Self::Advanced => "advanced",
            Self::Unchanged => "unchanged",
            Self::Regressed => "regressed",
        }
    }
}

impl From<Observation> for PushOutcome {
    fn from(observation: Observation) -> Self {
        match observation {
            Observation::Advanced { .. } => Self::Advanced,
            Observation::Unchanged => Self::Unchanged,
            Observation::Regressed { .. } => Self::Regressed,
        }
    }
}

/// Applies a pushed epoch. Separated from the HTTP plumbing so the fence's
/// behaviour can be tested without a socket.
pub fn push(fence: &EpochFence, epoch: Epoch) -> (PushOutcome, Epoch) {
    let observation = fence.observe(EpochSource::Push, epoch);
    (observation.into(), fence.current())
}

/// Serves the admin endpoint until `shutdown` goes true.
pub async fn serve(
    listener: TcpListener,
    fence: Arc<EpochFence>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            result = listener.accept() => result,
            () = wait_true(&mut shutdown) => return,
        };
        let Ok((socket, _)) = accepted else { continue };
        let fence = Arc::clone(&fence);
        tokio::spawn(async move {
            let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                let fence = Arc::clone(&fence);
                async move { Ok::<_, std::convert::Infallible>(respond(request, &fence).await) }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(socket), service)
                .await;
        });
    }
}

async fn respond(
    request: Request<hyper::body::Incoming>,
    fence: &EpochFence,
) -> Response<Full<Bytes>> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/epoch") => json(StatusCode::OK, report(fence, None)),
        (&Method::POST, "/epoch") => match read_epoch(request).await {
            Ok(epoch) => {
                let (outcome, current) = push(fence, epoch);
                debug!(%epoch, %current, outcome = outcome.label(), "an epoch was pushed");
                json(StatusCode::OK, report(fence, Some(outcome)))
            }
            Err(message) => json(
                StatusCode::BAD_REQUEST,
                format!("{{\"error\":{}}}", quote(&message)),
            ),
        },
        _ => json(
            StatusCode::NOT_FOUND,
            "{\"error\":\"not found\"}".to_owned(),
        ),
    }
}

/// Accepts either a bare decimal or `{"epoch": N}`, so a promoting agent can
/// push with `curl -d 12` and a client library can send JSON.
async fn read_epoch(request: Request<hyper::body::Incoming>) -> Result<Epoch, String> {
    let upper = request.body().size_hint().upper().unwrap_or(u64::MAX);
    if upper > MAX_BODY_BYTES {
        return Err("the request body is too large".to_owned());
    }
    let body = request
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("reading the request body failed: {error}"))?
        .to_bytes();
    parse_epoch_body(&body)
}

/// Accepts either a bare decimal or `{"epoch": N}`.
fn parse_epoch_body(body: &[u8]) -> Result<Epoch, String> {
    let text = std::str::from_utf8(body)
        .map_err(|_| "the request body is not UTF-8".to_owned())?
        .trim();
    let digits = text
        .strip_prefix('{')
        .and_then(|rest| rest.split_once(':'))
        .map_or(text, |(_, value)| {
            value.trim_end_matches('}').trim().trim_matches('"')
        });
    digits
        .trim()
        .parse()
        .map_err(|_| format!("{text:?} is not a primary epoch"))
}

fn report(fence: &EpochFence, outcome: Option<PushOutcome>) -> String {
    let outcome = outcome.map_or_else(
        || "null".to_owned(),
        |outcome| format!("\"{}\"", outcome.label()),
    );
    format!(
        "{{\"epoch\":{},\"generation\":{},\"outcome\":{outcome}}}",
        fence.current().get(),
        fence.generation(),
    )
}

fn json(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("a static response builds")
}

fn quote(value: &str) -> String {
    let escaped: String = value
        .chars()
        .flat_map(|c| match c {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect(),
            '\n' | '\r' | '\t' => vec![' '],
            other => vec![other],
        })
        .collect();
    format!("\"{escaped}\"")
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
    fn a_push_that_advances_the_fence_reports_that_it_did() {
        let fence = EpochFence::in_memory();
        assert_eq!(
            push(&fence, Epoch::new(3)),
            (PushOutcome::Advanced, Epoch::new(3))
        );
    }

    #[test]
    fn a_repeated_push_is_unchanged_rather_than_an_error() {
        let fence = EpochFence::in_memory();
        push(&fence, Epoch::new(3));
        assert_eq!(
            push(&fence, Epoch::new(3)),
            (PushOutcome::Unchanged, Epoch::new(3))
        );
    }

    #[test]
    fn a_push_below_the_current_epoch_is_reported_without_lowering_it() {
        let fence = EpochFence::in_memory();
        push(&fence, Epoch::new(9));
        assert_eq!(
            push(&fence, Epoch::new(2)),
            (PushOutcome::Regressed, Epoch::new(9))
        );
        assert_eq!(fence.current(), Epoch::new(9));
    }

    #[test]
    fn the_report_carries_the_epoch_and_the_generation() {
        let fence = EpochFence::in_memory();
        push(&fence, Epoch::new(4));
        let body = report(&fence, Some(PushOutcome::Advanced));
        assert!(body.contains("\"epoch\":4"));
        assert!(body.contains("\"generation\":1"));
        assert!(body.contains("\"outcome\":\"advanced\""));
    }

    #[test]
    fn an_error_message_is_escaped_into_its_json_string() {
        assert_eq!(quote("a \"b\"\nc"), "\"a \\\"b\\\" c\"");
    }

    #[test]
    fn a_bare_decimal_and_a_json_object_both_carry_an_epoch() {
        assert_eq!(parse_epoch_body(b"12"), Ok(Epoch::new(12)));
        assert_eq!(parse_epoch_body(b" 12\n"), Ok(Epoch::new(12)));
        assert_eq!(parse_epoch_body(br#"{"epoch": 12}"#), Ok(Epoch::new(12)));
        assert_eq!(parse_epoch_body(br#"{"epoch":"12"}"#), Ok(Epoch::new(12)));
    }

    #[test]
    fn a_body_that_is_not_an_epoch_is_refused_rather_than_defaulted() {
        assert!(parse_epoch_body(b"").is_err());
        assert!(parse_epoch_body(b"latest").is_err());
        assert!(parse_epoch_body(b"-1").is_err());
        assert!(parse_epoch_body(&[0xff, 0xfe]).is_err());
    }
}
