//! The pull/verify path: reading the epoch off the backend connection itself.
//!
//! This is the only one of the three delivery paths that is safe under
//! partition, which is why it is mandatory rather than an optimisation. A proxy
//! that has lost the API server and cannot be reached by the promoting agent
//! still learns, from the connection it is about to hand to a client, which
//! postmaster is on the other end of it.
//!
//! Two forms, preferred in this order:
//!
//! 1. **`ParameterStatus`** — if `pgelastic.primary_epoch` is `GUC_REPORT` the
//!    postmaster volunteers it, and a change arrives asynchronously on the
//!    connection at zero extra round trips. [`observe_parameter_status`] folds
//!    those into the fence.
//! 2. **`SHOW`** — otherwise the epoch is read at checkout with one round trip.
//!    A custom GUC set from `postgresql.conf` is a *placeholder* GUC and
//!    placeholders cannot carry `GUC_REPORT`, so this is the form that actually
//!    runs today.
//!
//! The probe reads the backend PID and the current WAL LSN in the same round
//! trip, because those are two of the four fields the in-doubt log is keyed by
//! and there is no second chance to ask for them once the socket has been
//! severed.
//!
//! [`probe_loop`] is what makes this path *sufficient* rather than merely
//! present. A checkout-time probe only ever asks connections the pool already
//! holds, and those all reach the demoted primary, which is still truthfully
//! reporting the old epoch. Learning that a promotion happened means opening a
//! *fresh* socket, because that is the one the Service routes to whoever the
//! primary now is. So the loop dials one new connection every `retryPeriod` —
//! the fence's reaction deadline, and therefore the interval at which the pull
//! path has to be able to notice.

use bytes::{Buf as _, Bytes, BytesMut};
use pgelastic_wire::{BackendMessage, FrontendMessage, ParameterStatus};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{Epoch, EpochFence, EpochSource, Observation};
use crate::error::{ProxyError, Result};
use crate::relay::{FrameRelay, Relayed};
use crate::stream::BackendStream;

/// The custom GUC the fence token is bound into.
///
/// Spelled once. The Go side spells it once too, in
/// `internal/instance/pgconf`; the two must not drift, and a rename that
/// touches only one of them shows up as this module reading `NULL`.
pub const EPOCH_GUC: &str = "pgelastic.primary_epoch";

/// One round trip that answers every question the fence has about a connection.
///
/// `current_setting(..., true)` rather than `SHOW`: a `SHOW` of an unset custom
/// GUC is an error that would have to be told apart from a real failure, while
/// this returns `NULL` and lets an absent epoch stay absent.
const PROBE_SQL: &str = "SELECT current_setting('pgelastic.primary_epoch', true), \
                         pg_backend_pid()::text, \
                         CASE WHEN pg_is_in_recovery() \
                              THEN pg_last_wal_replay_lsn()::text \
                              ELSE pg_current_wal_lsn()::text END";

/// What a connection said about itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Probe {
    /// `None` when the postmaster carries no `pgelastic.primary_epoch` at all.
    /// Absent evidence, not evidence of epoch zero.
    pub epoch: Option<Epoch>,
    pub backend_pid: Option<i32>,
    pub lsn: Option<String>,
}

/// Folds a `ParameterStatus` into the fence if it is the epoch's.
///
/// Returns `None` for every other parameter, so the caller can pass the whole
/// stream through without knowing which name matters.
pub fn observe_parameter_status(
    fence: &EpochFence,
    status: &ParameterStatus,
) -> Option<(Epoch, Observation)> {
    if !status.name.eq_ignore_ascii_case(EPOCH_GUC.as_bytes()) {
        return None;
    }
    let epoch = std::str::from_utf8(&status.value)
        .ok()
        .and_then(|value| value.parse::<Epoch>().ok())?;
    Some((epoch, fence.observe(EpochSource::Verify, epoch)))
}

/// Runs the probe over an idle backend connection.
///
/// The link's own bookkeeping is deliberately not fed: this statement belongs
/// to the pool, draws a complete response, and is drained to its
/// `ReadyForQuery` before the caller does anything else, so the outstanding
/// queue never has to know it happened.
pub async fn probe(stream: &mut BackendStream, relay: &mut FrameRelay) -> Result<Probe> {
    let mut wire = BytesMut::new();
    FrontendMessage::Query(Bytes::from_static(PROBE_SQL.as_bytes())).encode(&mut wire);
    stream.write_all(&wire).await?;
    stream.flush().await?;

    let mut row: Option<Vec<Option<Bytes>>> = None;
    let mut failure = None;
    loop {
        match relay.next_output()? {
            Relayed::NeedMore => {
                if stream.read_buf(relay.read_target()).await? == 0 {
                    return Err(ProxyError::backend(
                        "the backend closed the connection while its epoch was being verified",
                    ));
                }
            }
            Relayed::Opaque(_) => {}
            Relayed::Frame(frame) => match BackendMessage::decode(&frame)? {
                BackendMessage::DataRow(data) => row = Some(columns(data.as_bytes())?),
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
            "verifying a backend's primary epoch failed: {message}"
        )));
    }

    let row = row.unwrap_or_default();
    let text = |index: usize| -> Option<String> {
        row.get(index)
            .and_then(Option::as_ref)
            .map(|value| String::from_utf8_lossy(value).into_owned())
    };
    Ok(Probe {
        epoch: text(0).and_then(|value| value.parse().ok()),
        backend_pid: text(1).and_then(|value| value.parse().ok()),
        lsn: text(2),
    })
}

/// Everything the periodic prober needs to open a socket of its own.
pub struct Prober {
    pub backend: crate::config::BackendConfig,
    pub tls: Option<crate::tls::BackendTls>,
    pub kdf: crate::scram::KdfPool,
    pub fence: std::sync::Arc<EpochFence>,
    pub metrics: std::sync::Arc<crate::metrics::Metrics>,
}

impl std::fmt::Debug for Prober {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prober")
            .field("backend", &self.backend.address)
            .finish_non_exhaustive()
    }
}

/// Dials one fresh backend connection every `retryPeriod` and folds the epoch
/// it reports into the fence.
///
/// One connection every two seconds is the price of a fence that keeps working
/// when the API server is unreachable and the promoting agent cannot call in.
/// It is deliberately outside the pool's budget: a probe that had to queue for
/// admission would be blocked by exactly the saturation a failover causes.
///
/// A failed dial is not evidence of anything, so it is logged and the loop
/// carries on. The proxy does not lower its epoch, refuse traffic, or fence on
/// its own inability to connect.
pub async fn probe_loop(prober: Prober, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    let interval = prober.fence.timing().retry_period();
    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = wait_true(&mut shutdown) => return,
        }
        match probe_once(&prober).await {
            Ok(Some(epoch)) => {
                let observation = prober.fence.observe(EpochSource::Verify, epoch);
                prober
                    .metrics
                    .epoch_observed(EpochSource::Verify, observation.into());
                prober.metrics.primary_epoch(prober.fence.current());
            }
            Ok(None) => tracing::debug!(
                "the backend publishes no pgelastic.primary_epoch; the pull path has \
                 nothing to report"
            ),
            Err(error) => {
                tracing::debug!(%error, "the periodic epoch probe could not reach the backend");
            }
        }
    }
}

async fn probe_once(prober: &Prober) -> Result<Option<Epoch>> {
    let startup = pgelastic_wire::StartupMessage::new(
        pgelastic_wire::ProtocolVersion::V3_0,
        vec![(
            Bytes::from_static(b"application_name"),
            Bytes::from_static(b"pgelastic_epoch_probe"),
        )],
    );
    let mut session =
        crate::backend::connect(&prober.backend, prober.tls.as_ref(), &prober.kdf, &startup)
            .await?;

    // The zero-round-trip form first: if the GUC is GUC_REPORT the answer was
    // already in the start-up parameter set and there is nothing to ask.
    for message in &session.parameters {
        if let BackendMessage::ParameterStatus(status) = message
            && let Some((epoch, _)) = observe_parameter_status(&prober.fence, status)
        {
            crate::session::terminate_backend(&mut session.stream).await;
            return Ok(Some(epoch));
        }
    }

    let mut relay = FrameRelay::default();
    relay.extend_from_slice(session.buf.as_slice());
    let probe = probe(&mut session.stream, &mut relay).await;
    crate::session::terminate_backend(&mut session.stream).await;
    probe.map(|probe| probe.epoch)
}

async fn wait_true(rx: &mut tokio::sync::watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Splits a `DataRow` body into its columns. `None` is SQL `NULL`, which is not
/// the same as an empty string — an unset GUC has to be distinguishable from
/// one set to `""`.
fn columns(body: &Bytes) -> Result<Vec<Option<Bytes>>> {
    let mut cursor = body.clone();
    if cursor.remaining() < 2 {
        return Err(ProxyError::backend("a DataRow carried no column count"));
    }
    let count = cursor.get_i16();
    let mut columns = Vec::with_capacity(usize::try_from(count.max(0)).unwrap_or(0));
    for _ in 0..count.max(0) {
        if cursor.remaining() < 4 {
            return Err(ProxyError::backend("a DataRow column ran off the end"));
        }
        let len = cursor.get_i32();
        if len < 0 {
            columns.push(None);
            continue;
        }
        let len = usize::try_from(len).unwrap_or(0);
        if cursor.remaining() < len {
            return Err(ProxyError::backend("a DataRow column ran off the end"));
        }
        columns.push(Some(cursor.split_to(len)));
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(name: &str, value: &str) -> ParameterStatus {
        ParameterStatus {
            name: Bytes::copy_from_slice(name.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
        }
    }

    fn data_row(values: &[Option<&str>]) -> Bytes {
        let mut out = BytesMut::new();
        out.extend_from_slice(&i16::try_from(values.len()).unwrap().to_be_bytes());
        for value in values {
            match value {
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(text) => {
                    out.extend_from_slice(&i32::try_from(text.len()).unwrap().to_be_bytes());
                    out.extend_from_slice(text.as_bytes());
                }
            }
        }
        out.freeze()
    }

    #[test]
    fn a_parameter_status_for_the_epoch_guc_advances_the_fence() {
        let fence = EpochFence::in_memory();
        let (epoch, observation) =
            observe_parameter_status(&fence, &status(EPOCH_GUC, "9")).unwrap();
        assert_eq!(epoch, Epoch::new(9));
        assert!(observation.advanced());
        assert_eq!(fence.current(), Epoch::new(9));
    }

    #[test]
    fn a_parameter_status_for_anything_else_is_ignored() {
        let fence = EpochFence::in_memory();
        assert!(observe_parameter_status(&fence, &status("TimeZone", "UTC")).is_none());
        assert_eq!(fence.current(), Epoch::UNKNOWN);
    }

    #[test]
    fn a_parameter_status_carrying_a_lower_epoch_fences_without_lowering_it() {
        let fence = EpochFence::in_memory();
        fence.observe(EpochSource::Push, Epoch::new(5));
        let (_, observation) = observe_parameter_status(&fence, &status(EPOCH_GUC, "4")).unwrap();
        assert!(observation.fences());
        assert!(!observation.advanced());
        assert_eq!(fence.current(), Epoch::new(5));
    }

    #[test]
    fn a_parameter_status_whose_value_is_not_a_number_is_not_evidence() {
        let fence = EpochFence::in_memory();
        assert!(observe_parameter_status(&fence, &status(EPOCH_GUC, "epoch-3")).is_none());
        assert_eq!(fence.current(), Epoch::UNKNOWN);
    }

    #[test]
    fn a_row_of_nulls_reports_absent_evidence_rather_than_epoch_zero() {
        let columns = columns(&data_row(&[None, None, None])).unwrap();
        assert_eq!(columns, vec![None, None, None]);
    }

    #[test]
    fn a_row_splits_into_its_columns() {
        let columns = columns(&data_row(&[Some("12"), Some("4242"), Some("0/16B3748")])).unwrap();
        assert_eq!(columns[0].as_deref(), Some(&b"12"[..]));
        assert_eq!(columns[2].as_deref(), Some(&b"0/16B3748"[..]));
    }

    #[test]
    fn a_truncated_row_is_an_error_rather_than_a_partial_reading() {
        let mut truncated = BytesMut::from(&data_row(&[Some("12")])[..]);
        truncated.truncate(4);
        assert!(columns(&truncated.freeze()).is_err());
    }

    #[test]
    fn the_probe_asks_for_the_guc_the_instance_manager_writes() {
        assert!(PROBE_SQL.contains(EPOCH_GUC));
        assert_eq!(EPOCH_GUC, "pgelastic.primary_epoch");
    }
}
