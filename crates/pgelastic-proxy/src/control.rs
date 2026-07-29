//! The lease-bound control API a migration cutover drives.
//!
//! Five operations, in the order §5's choreography uses them:
//!
//! | Route | What it does |
//! |---|---|
//! | `POST /quiesce` | Takes the lease and closes the tenant's gate. New transactions queue, holding their client sockets. |
//! | `GET /drainStatus` | Reports in-flight and queued counts. `drained` is the instant the cutover may proceed. |
//! | `POST /setRoute` | Points the tenant at another instance. Refused unless the tenant is held. |
//! | `POST /resume` | Opens the gate. Queued clients run against the new route, in the order they arrived. |
//! | `POST /unquiesce` | Releases the lease. If no `resume` committed the flip, the route goes back. |
//!
//! Plus `GET /instances`, which answers the two questions a stall makes urgent:
//! which instance a tenant is on, and whether that instance can commit.
//!
//! **Its own listener, not a route on `/metrics`.** These endpoints change
//! behaviour and `/metrics` does not, so they belong on a port that can be
//! exposed differently. It is also deliberately separate from the epoch push
//! endpoint: that one carries a single monotonic scalar that is safe for
//! anyone to report, while everything here is mutually exclusive under a lease.
//!
//! Every mutating call carries a `holder`. A second caller is refused with
//! `409` rather than taking the lease over, because two operators each
//! believing they own a cutover is how a tenant ends up half on each instance.

use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt as _, Full};
use hyper::body::Body as _;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::ControlConfig;
use crate::metrics::Metrics;
use crate::quiesce::{DrainStatus, QuiesceError, QuiesceRegistry};
use crate::route::{Fleet, InstanceId};
use crate::tls::ControlAuthority;

/// Largest request body the API will read. Every payload is a handful of short
/// identifiers.
const MAX_BODY_BYTES: u64 = 4096;

/// Everything the control API mutates.
#[derive(Debug, Clone)]
pub struct Control {
    pub fleet: Arc<Fleet>,
    pub quiesce: Arc<QuiesceRegistry>,
    pub metrics: Arc<Metrics>,
    pub config: ControlConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QuiesceRequest {
    tenant: String,
    holder: String,
    #[serde(default)]
    ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HolderRequest {
    tenant: String,
    holder: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RouteRequest {
    tenant: String,
    holder: String,
    instance: String,
}

impl Control {
    /// Closes a tenant's gate under a lease.
    pub fn quiesce(
        &self,
        tenant: &str,
        holder: &str,
        ttl: Option<Duration>,
    ) -> Result<DrainStatus, QuiesceError> {
        let ttl = ttl.unwrap_or_else(|| self.config.default_lease_ttl());
        let gate = self.quiesce.gate(tenant);
        gate.quiesce(
            holder,
            ttl,
            self.config.max_lease_ttl(),
            self.fleet.route_id(tenant),
        )?;
        self.metrics.quiesce_started();
        Ok(self.drain_status(tenant))
    }

    pub fn drain_status(&self, tenant: &str) -> DrainStatus {
        let instance = self.fleet.route_id(tenant);
        let Some(gate) = self.quiesce.existing(tenant) else {
            return DrainStatus {
                tenant: tenant.to_owned(),
                instance,
                quiesced: false,
                in_flight: 0,
                queued: 0,
                drained: false,
                holder: None,
                lease_expires_in: None,
            };
        };
        let lease = gate.lease();
        let quiesced = !gate.is_open();
        let in_flight = gate.in_flight();
        DrainStatus {
            tenant: tenant.to_owned(),
            instance,
            quiesced,
            in_flight,
            queued: gate.queued(),
            drained: quiesced && in_flight == 0,
            holder: lease.as_ref().map(|lease| lease.holder.clone()),
            lease_expires_in: lease.as_ref().map(super::quiesce::Lease::expires_in),
        }
    }

    /// Moves a held tenant to another instance.
    pub fn set_route(
        &self,
        tenant: &str,
        holder: &str,
        instance: &str,
    ) -> Result<InstanceId, QuiesceError> {
        let gate = self.quiesce.gate(tenant);
        gate.assert_held(holder)?;
        let target = InstanceId::new(instance);
        let previous =
            self.fleet
                .set_route(tenant, &target)
                .ok_or_else(|| QuiesceError::NoSuchInstance {
                    instance: instance.to_owned(),
                })?;
        info!(
            tenant,
            from = %previous,
            to = %target,
            "a tenant's route was flipped while its traffic was held"
        );
        Ok(target)
    }

    /// Opens the gate and commits the current route.
    pub fn resume(&self, tenant: &str, holder: &str) -> Result<u64, QuiesceError> {
        let gate = self.quiesce.gate(tenant);
        // Read before the release, which is what clears the clock, and recorded
        // only once the release succeeded: a refused call held nobody.
        let held = gate.held_for();
        let released = gate.resume(holder)?;
        if let Some(held) = held {
            self.metrics.quiesce_round_trip(held);
        }
        self.metrics.quiesce_resumed(released);
        info!(
            tenant,
            released,
            instance = %self.fleet.route_id(tenant),
            "a quiesced tenant resumed; every queued transaction was released, none dropped"
        );
        Ok(released)
    }

    /// Releases the lease, rolling the route back if nothing ran on the target.
    pub fn unquiesce(&self, tenant: &str, holder: &str) -> Result<InstanceId, QuiesceError> {
        let gate = self.quiesce.gate(tenant);
        let held = gate.held_for();
        let rollback = gate.unquiesce(holder)?;
        if let Some(held) = held {
            self.metrics.quiesce_round_trip(held);
        }
        if let Some(source) = rollback {
            self.fleet.set_route(tenant, &source);
        }
        Ok(self.fleet.route_id(tenant))
    }

    /// Applies whatever lease expiry implies, for every tenant it applies to.
    ///
    /// Called on a timer. Expiry is defined to be an `unquiesce`, so a killed
    /// operator leaves a tenant exactly where a clean abort would have.
    pub fn reap_expired_leases(&self) {
        for (tenant, expiry) in self.quiesce.reap_expired() {
            self.metrics.quiesce_lease_expired();
            if let Some(held) = expiry.held {
                self.metrics.quiesce_round_trip(held);
            }
            if let Some(source) = expiry.rollback {
                self.fleet.set_route(&tenant, &source);
                info!(
                    tenant,
                    instance = %source,
                    "a quiesce lease expired before the cutover committed; the tenant is \
                     serving from its source again"
                );
            }
        }
    }
}

/// Serves the control API over mutual TLS until `shutdown` goes true.
///
/// There is no plaintext variant. Every endpoint below either holds a tenant's
/// clients still or decides which instance they run against, so a caller that
/// has not proved who it is gets `401` and nothing else.
pub async fn serve(
    listener: TcpListener,
    control: Control,
    authority: Arc<ControlAuthority>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            result = listener.accept() => result,
            () = wait_true(&mut shutdown) => return,
        };
        let Ok((socket, peer)) = accepted else {
            continue;
        };
        let control = control.clone();
        let authority = Arc::clone(&authority);
        tokio::spawn(async move {
            let stream = match authority.acceptor().accept(socket).await {
                Ok(stream) => stream,
                Err(error) => {
                    debug!(%peer, %error, "a control-plane connection failed to negotiate TLS");
                    return;
                }
            };
            let identity = authority.authorize(stream.get_ref().1.peer_certificates());
            if let Some(reason) = identity.refusal() {
                warn!(%peer, %reason, "a control-plane caller was refused");
            }
            let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                let control = control.clone();
                let identity = identity.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(match identity.refusal() {
                        Some(reason) => unauthorized(&reason),
                        None => respond(request, &control).await,
                    })
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

/// Drops expired leases every `interval`.
///
/// The sweep is what makes the lease a real deadline rather than a value nobody
/// reads: without it a killed operator's quiesce would only be noticed by the
/// next caller, and there is no next caller.
pub async fn reap_loop(control: Control, interval: Duration, mut shutdown: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => control.reap_expired_leases(),
            () = wait_true(&mut shutdown) => return,
        }
    }
}

async fn respond(
    request: Request<hyper::body::Incoming>,
    control: &Control,
) -> Response<Full<Bytes>> {
    let path = request.uri().path().to_owned();
    let query = request.uri().query().unwrap_or_default().to_owned();
    match (request.method(), path.as_str()) {
        (&Method::GET, "/instances") => json(StatusCode::OK, instances_report(control)),
        (&Method::GET, "/drainStatus") => match param(&query, "tenant") {
            Some(tenant) => json(StatusCode::OK, drain_report(&control.drain_status(&tenant))),
            None => bad_request("drainStatus needs a tenant"),
        },
        (&Method::POST, "/quiesce") => match body::<QuiesceRequest>(request).await {
            Ok(ask) => {
                let ttl = ask.ttl_ms.map(Duration::from_millis);
                answer(control.quiesce(&ask.tenant, &ask.holder, ttl), |status| {
                    drain_report(&status)
                })
            }
            Err(message) => bad_request(&message),
        },
        (&Method::POST, "/setRoute") => match body::<RouteRequest>(request).await {
            Ok(ask) => answer(
                control.set_route(&ask.tenant, &ask.holder, &ask.instance),
                |instance| format!("{{\"instance\":{}}}", quote(instance.as_str())),
            ),
            Err(message) => bad_request(&message),
        },
        (&Method::POST, "/resume") => match body::<HolderRequest>(request).await {
            Ok(ask) => answer(control.resume(&ask.tenant, &ask.holder), |released| {
                format!("{{\"released\":{released}}}")
            }),
            Err(message) => bad_request(&message),
        },
        (&Method::POST, "/unquiesce") => match body::<HolderRequest>(request).await {
            Ok(ask) => answer(control.unquiesce(&ask.tenant, &ask.holder), |instance| {
                format!("{{\"instance\":{}}}", quote(instance.as_str()))
            }),
            Err(message) => bad_request(&message),
        },
        _ => json(
            StatusCode::NOT_FOUND,
            "{\"error\":\"not found\"}".to_owned(),
        ),
    }
}

fn answer<T>(
    outcome: Result<T, QuiesceError>,
    render: impl FnOnce(T) -> String,
) -> Response<Full<Bytes>> {
    match outcome {
        Ok(value) => json(StatusCode::OK, render(value)),
        Err(error) => {
            debug!(%error, "a control-plane call was refused");
            json(
                status_for(&error),
                format!("{{\"error\":{}}}", quote(&error.to_string())),
            )
        }
    }
}

/// A refusal's HTTP status, chosen so a caller can retry on the right ones.
///
/// `409` is "somebody else holds this", which is worth retrying after their
/// lease expires. `422` is "this call does not apply", which is not.
fn status_for(error: &QuiesceError) -> StatusCode {
    match error {
        QuiesceError::LeaseHeld { .. } => StatusCode::CONFLICT,
        QuiesceError::NotQuiesced | QuiesceError::NotHeld => StatusCode::UNPROCESSABLE_ENTITY,
        QuiesceError::TtlTooLong { .. } | QuiesceError::NoSuchInstance { .. } => {
            StatusCode::BAD_REQUEST
        }
    }
}

fn drain_report(status: &DrainStatus) -> String {
    format!(
        "{{\"tenant\":{},\"instance\":{},\"quiesced\":{},\"inFlight\":{},\"queued\":{},\
          \"drained\":{},\"holder\":{},\"leaseExpiresInMs\":{}}}",
        quote(&status.tenant),
        quote(status.instance.as_str()),
        status.quiesced,
        status.in_flight,
        status.queued,
        status.drained,
        status
            .holder
            .as_ref()
            .map_or_else(|| "null".to_owned(), |holder| quote(holder)),
        status
            .lease_expires_in
            .map_or_else(|| "null".to_owned(), |left| left.as_millis().to_string()),
    )
}

fn instances_report(control: &Control) -> String {
    let mut entries = Vec::new();
    for instance in control.fleet.instances() {
        let health = instance.stall.health();
        entries.push(format!(
            "{{\"name\":{},\"address\":{},\"writeHealth\":{},\"detail\":{},\
              \"backendConnections\":{},\"stalledForMs\":{},\"detections\":{},\"refusals\":{}}}",
            quote(instance.id.as_str()),
            quote(&instance.backend.address),
            quote(health.label()),
            quote(&health.to_string()),
            instance.pools.config().backend_connections,
            instance
                .stall
                .stalled_for()
                .map_or_else(|| "null".to_owned(), |held| held.as_millis().to_string()),
            instance.stall.detections(),
            instance.stall.refusals(),
        ));
    }
    format!("{{\"instances\":[{}]}}", entries.join(","))
}

async fn body<T: serde::de::DeserializeOwned>(
    request: Request<hyper::body::Incoming>,
) -> Result<T, String> {
    let upper = request.body().size_hint().upper().unwrap_or(u64::MAX);
    if upper > MAX_BODY_BYTES {
        return Err("the request body is too large".to_owned());
    }
    let bytes = request
        .into_body()
        .collect()
        .await
        .map_err(|error| format!("reading the request body failed: {error}"))?
        .to_bytes();
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("the request body is not valid: {error}"))
}

fn param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| percent_decode(value))
    })
}

/// Enough percent-decoding for a tenant name in a query string.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or_default();
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    out.push(byte);
                    index += 3;
                } else {
                    out.push(bytes[index]);
                    index += 1;
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The refusal an unauthenticated or untrusted caller gets.
///
/// A body rather than a bare close, because the three ways to fail — no
/// certificate, one this listener does not trust, one belonging to somebody
/// else — are three different misconfigurations and look identical from the
/// outside otherwise.
fn unauthorized(reason: &str) -> Response<Full<Bytes>> {
    json(
        StatusCode::UNAUTHORIZED,
        format!("{{\"error\":{}}}", quote(reason)),
    )
}

fn bad_request(message: &str) -> Response<Full<Bytes>> {
    json(
        StatusCode::BAD_REQUEST,
        format!("{{\"error\":{}}}", quote(message)),
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
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
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
    use crate::config::Config;
    use std::str::FromStr as _;

    const TWO: &str = r#"
        [listen]
        address = "127.0.0.1:0"

        [backend]
        address = "127.0.0.1:5432"
        user = "postgres"

        [[instances]]
        name = "inst-a"
        address = "127.0.0.1:5001"

        [[instances]]
        name = "inst-b"
        address = "127.0.0.1:5002"

        [routing]
        defaultInstance = "inst-a"
    "#;

    fn control() -> Control {
        let config = Config::from_str(TWO).expect("the configuration parses");
        let metrics = Metrics::new();
        Control {
            fleet: Fleet::build(&config, &metrics).expect("the fleet builds"),
            quiesce: QuiesceRegistry::new(),
            metrics,
            config: config.control,
        }
    }

    #[test]
    fn a_tenant_nobody_has_quiesced_reports_that_it_is_not() {
        let status = control().drain_status("alpha");
        assert!(!status.quiesced);
        assert!(!status.drained);
        assert_eq!(status.instance.as_str(), "inst-a");
    }

    #[test]
    fn a_quiesced_tenant_with_nothing_in_flight_is_drained() {
        let control = control();
        let status = control.quiesce("alpha", "migration", None).unwrap();
        assert!(status.quiesced);
        assert!(status.drained);
        assert_eq!(status.holder.as_deref(), Some("migration"));
    }

    #[test]
    fn a_tenant_holding_a_backend_is_quiesced_but_not_drained() {
        let control = control();
        let held = control.quiesce.gate("alpha").hold();
        let status = control.quiesce("alpha", "migration", None).unwrap();
        assert!(status.quiesced);
        assert!(!status.drained);
        assert_eq!(status.in_flight, 1);
        drop(held);
        assert!(control.drain_status("alpha").drained);
    }

    #[test]
    fn the_route_cannot_be_flipped_without_the_lease() {
        let control = control();
        let error = control
            .set_route("alpha", "migration", "inst-b")
            .unwrap_err();
        assert_eq!(error, QuiesceError::NotQuiesced);
        assert_eq!(control.fleet.route_id("alpha").as_str(), "inst-a");
    }

    #[test]
    fn the_cutover_sequence_moves_the_tenant_and_keeps_it_there() {
        let control = control();
        control.quiesce("alpha", "migration", None).unwrap();
        assert_eq!(
            control
                .set_route("alpha", "migration", "inst-b")
                .unwrap()
                .as_str(),
            "inst-b"
        );
        control.resume("alpha", "migration").unwrap();
        assert_eq!(
            control.unquiesce("alpha", "migration").unwrap().as_str(),
            "inst-b"
        );
    }

    #[test]
    fn an_abort_before_the_resume_puts_the_tenant_back_on_its_source() {
        let control = control();
        control.quiesce("alpha", "migration", None).unwrap();
        control.set_route("alpha", "migration", "inst-b").unwrap();
        assert_eq!(
            control.unquiesce("alpha", "migration").unwrap().as_str(),
            "inst-a"
        );
        assert!(control.quiesce.gate("alpha").is_open());
    }

    #[test]
    fn an_expired_lease_puts_the_tenant_back_without_anyone_calling() {
        let control = control();
        control
            .quiesce("alpha", "migration", Some(Duration::from_millis(1)))
            .unwrap();
        control.set_route("alpha", "migration", "inst-b").unwrap();
        std::thread::sleep(Duration::from_millis(5));
        control.reap_expired_leases();
        assert_eq!(control.fleet.route_id("alpha").as_str(), "inst-a");
        assert!(control.quiesce.gate("alpha").is_open());
    }

    #[test]
    fn an_expired_lease_after_the_resume_leaves_the_tenant_on_the_target() {
        let control = control();
        control
            .quiesce("alpha", "migration", Some(Duration::from_millis(1)))
            .unwrap();
        control.set_route("alpha", "migration", "inst-b").unwrap();
        control.resume("alpha", "migration").unwrap();
        std::thread::sleep(Duration::from_millis(5));
        control.reap_expired_leases();
        assert_eq!(control.fleet.route_id("alpha").as_str(), "inst-b");
    }

    #[test]
    fn a_route_to_an_instance_this_proxy_does_not_front_is_refused() {
        let control = control();
        control.quiesce("alpha", "migration", None).unwrap();
        assert_eq!(
            control.set_route("alpha", "migration", "elsewhere"),
            Err(QuiesceError::NoSuchInstance {
                instance: "elsewhere".to_owned()
            })
        );
    }

    #[test]
    fn a_second_holder_is_a_conflict_rather_than_a_takeover() {
        let control = control();
        control.quiesce("alpha", "migration-a", None).unwrap();
        let error = control.quiesce("alpha", "migration-b", None).unwrap_err();
        assert!(matches!(error, QuiesceError::LeaseHeld { .. }));
        assert_eq!(status_for(&error), StatusCode::CONFLICT);
    }

    #[test]
    fn a_lease_over_the_ceiling_is_refused_before_it_takes_effect() {
        let control = control();
        let error = control
            .quiesce("alpha", "migration", Some(Duration::from_secs(3600)))
            .unwrap_err();
        assert!(matches!(error, QuiesceError::TtlTooLong { .. }));
        assert!(control.quiesce.gate("alpha").is_open());
    }

    #[test]
    fn the_instances_report_names_every_instance_and_its_write_health() {
        let report = instances_report(&control());
        assert!(report.contains("\"name\":\"inst-a\""));
        assert!(report.contains("\"name\":\"inst-b\""));
        assert!(report.contains("\"writeHealth\":\"writable\""));
    }

    #[test]
    fn a_drain_report_round_trips_the_fields_the_cutover_reads() {
        let control = control();
        control.quiesce("alpha", "migration", None).unwrap();
        let report = drain_report(&control.drain_status("alpha"));
        assert!(report.contains("\"tenant\":\"alpha\""));
        assert!(report.contains("\"quiesced\":true"));
        assert!(report.contains("\"drained\":true"));
        assert!(report.contains("\"holder\":\"migration\""));
    }

    #[test]
    fn a_tenant_name_survives_the_query_string() {
        assert_eq!(param("tenant=alpha", "tenant").as_deref(), Some("alpha"));
        assert_eq!(param("x=1&tenant=a%2Fb", "tenant").as_deref(), Some("a/b"));
        assert_eq!(param("tenant=a+b", "tenant").as_deref(), Some("a b"));
        assert_eq!(param("holder=x", "tenant"), None);
    }

    #[test]
    fn a_resume_records_how_long_the_tenant_was_held() {
        let control = control();
        control.quiesce("alpha", "migration", None).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        control.resume("alpha", "migration").unwrap();
        assert!(
            round_trip_us(&control.metrics) >= 1000,
            "the pause a queued client saw was never recorded: {}",
            control.metrics.render()
        );
    }

    #[test]
    fn an_aborted_cutover_records_the_pause_it_still_cost() {
        let control = control();
        control.quiesce("alpha", "migration", None).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        control.unquiesce("alpha", "migration").unwrap();
        assert!(round_trip_us(&control.metrics) >= 1000);
    }

    #[test]
    fn an_expired_lease_records_the_pause_nobody_ended_deliberately() {
        let control = control();
        control
            .quiesce("alpha", "migration", Some(Duration::from_millis(2)))
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        control.reap_expired_leases();
        assert!(round_trip_us(&control.metrics) >= 2000);
    }

    /// A renewal must not restart the clock, or the reported pause is however
    /// often the operator happened to renew rather than how long clients waited.
    #[test]
    fn renewing_the_lease_does_not_shorten_the_pause_that_gets_reported() {
        let control = control();
        control.quiesce("alpha", "migration", None).unwrap();
        std::thread::sleep(Duration::from_millis(4));
        control.quiesce("alpha", "migration", None).unwrap();
        control.resume("alpha", "migration").unwrap();
        assert!(round_trip_us(&control.metrics) >= 4000);
    }

    fn round_trip_us(metrics: &Metrics) -> i64 {
        let rendered = metrics.render();
        for line in rendered.lines() {
            if let Some(value) = line.strip_prefix("pgelastic_proxy_quiesce_round_trip_max_us ") {
                return value.trim().parse().expect("a gauge is a number");
            }
        }
        panic!("no quiesce round trip gauge in:\n{rendered}");
    }
}

/// The authentication the listener is useless without.
///
/// Three callers, one endpoint: the one holding the operator's certificate is
/// served, and the two that are not are told `401` with the reason rather than
/// being dropped. The refusals are asserted on the status line a real client
/// would read, over a real TLS handshake, because the whole point of deferring
/// the trust decision to the HTTP layer is that it is visible there.
#[cfg(test)]
mod auth_tests {
    use super::*;
    use crate::config::{Config, ControlTlsConfig};
    use crate::tls::ControlAuthority;
    use std::str::FromStr as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    /// The name the listener will accept, and nothing else.
    const OPERATOR: &str = "pgelastic-operator.pgelastic-system.svc";

    struct Pki {
        _dir: tempfile::TempDir,
        server_cert: std::path::PathBuf,
        server_key: std::path::PathBuf,
        ca: std::path::PathBuf,
        operator: Identity,
        impostor: Identity,
        stranger: Identity,
        roots: rustls::RootCertStore,
    }

    #[derive(Clone)]
    struct Identity {
        chain: Vec<rustls_pki_types::CertificateDer<'static>>,
        key_der: Vec<u8>,
    }

    impl Identity {
        fn key(&self) -> rustls_pki_types::PrivateKeyDer<'static> {
            rustls_pki_types::PrivateKeyDer::try_from(self.key_der.clone())
                .expect("a generated key is well formed")
        }
    }

    fn pki() -> Pki {
        use rcgen::{
            BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
            KeyPair, KeyUsagePurpose,
        };
        crate::tls::install_crypto_provider();

        let authority = |common_name: &str| {
            let mut params = CertificateParams::new(Vec::new()).expect("CA parameters");
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            params
                .distinguished_name
                .push(DnType::CommonName, common_name);
            let key = KeyPair::generate().expect("CA key");
            let cert = params.self_signed(&key).expect("self-signed CA");
            (params, key, cert)
        };
        let (ca_params, ca_key, ca_cert) = authority("pgelastic control CA");
        let (other_params, other_key, _other_cert) = authority("somebody else's CA");

        let leaf = |name: &str, usage: ExtendedKeyUsagePurpose, issuer: &Issuer<'_, &KeyPair>| {
            let mut params = CertificateParams::new(vec![name.to_owned()]).expect("parameters");
            params.distinguished_name.push(DnType::CommonName, name);
            params.extended_key_usages = vec![usage];
            let key = KeyPair::generate().expect("leaf key");
            let cert = params.signed_by(&key, issuer).expect("signed leaf");
            (cert, key)
        };

        let issuer = Issuer::from_params(&ca_params, &ca_key);
        let (server, server_key) = leaf("localhost", ExtendedKeyUsagePurpose::ServerAuth, &issuer);
        let (operator, operator_key) = leaf(OPERATOR, ExtendedKeyUsagePurpose::ClientAuth, &issuer);
        // Issued by the same CA, so it is trusted — and still refused, because
        // trust is not identity.
        let (impostor, impostor_key) = leaf(
            "someone-else.pgelastic-system.svc",
            ExtendedKeyUsagePurpose::ClientAuth,
            &issuer,
        );
        let other_issuer = Issuer::from_params(&other_params, &other_key);
        let (stranger, stranger_key) =
            leaf(OPERATOR, ExtendedKeyUsagePurpose::ClientAuth, &other_issuer);

        let dir = tempfile::TempDir::new().expect("temp dir");
        let write = |name: &str, contents: String| {
            let path = dir.path().join(name);
            std::fs::write(&path, contents).expect("write");
            path
        };
        let ca = write("ca.pem", ca_cert.pem());
        let server_cert = write("server.pem", server.pem());
        let server_key_path = write("server.key", server_key.serialize_pem());

        let identity = |cert: &rcgen::Certificate, key: &KeyPair| Identity {
            chain: vec![rustls_pki_types::CertificateDer::from(cert.der().to_vec())],
            key_der: key.serialize_der(),
        };

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls_pki_types::CertificateDer::from(
                ca_cert.der().to_vec(),
            ))
            .expect("the CA is a root");

        Pki {
            server_cert,
            server_key: server_key_path,
            ca,
            operator: identity(&operator, &operator_key),
            impostor: identity(&impostor, &impostor_key),
            stranger: identity(&stranger, &stranger_key),
            roots,
            _dir: dir,
        }
    }

    const SINGLE: &str = r#"
        [listen]
        address = "127.0.0.1:0"

        [backend]
        address = "127.0.0.1:5432"
        user = "postgres"
    "#;

    fn control() -> Control {
        let config = Config::from_str(SINGLE).expect("the configuration parses");
        let metrics = Metrics::new();
        Control {
            fleet: crate::route::Fleet::build(&config, &metrics).expect("the fleet builds"),
            quiesce: QuiesceRegistry::new(),
            metrics,
            config: config.control,
        }
    }

    /// Starts the listener and answers with the status line one request got.
    async fn status_for(pki: &Pki, client: Option<&Identity>) -> String {
        let authority = ControlAuthority::new(&ControlTlsConfig {
            certificate_file: pki.server_cert.clone(),
            key_file: pki.server_key.clone(),
            client_ca_file: pki.ca.clone(),
            client_name: OPERATOR.to_owned(),
        })
        .expect("the control authority builds");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        tokio::spawn(serve(listener, control(), authority, receiver));

        let builder = rustls::ClientConfig::builder().with_root_certificates(pki.roots.clone());
        let config = match client {
            Some(identity) => builder
                .with_client_auth_cert(identity.chain.clone(), identity.key())
                .expect("the client identity is usable"),
            None => builder.with_no_client_auth(),
        };
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let socket = tokio::net::TcpStream::connect(address).await.unwrap();
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut stream = connector.connect(name, socket).await.expect("handshake");

        stream
            .write_all(b"GET /instances HTTP/1.1\r\nHost: control\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut answer = Vec::new();
        let _ = stream.read_to_end(&mut answer).await;
        let _ = shutdown.send(true);

        let text = String::from_utf8_lossy(&answer).into_owned();
        text.lines().next().unwrap_or_default().to_owned()
    }

    #[tokio::test]
    async fn a_caller_with_no_certificate_is_told_why_rather_than_dropped() {
        let pki = pki();
        assert_eq!(status_for(&pki, None).await, "HTTP/1.1 401 Unauthorized");
    }

    #[tokio::test]
    async fn a_certificate_from_another_authority_is_refused() {
        let pki = pki();
        let stranger = pki.stranger.clone();
        assert_eq!(
            status_for(&pki, Some(&stranger)).await,
            "HTTP/1.1 401 Unauthorized"
        );
    }

    #[tokio::test]
    async fn a_trusted_certificate_belonging_to_somebody_else_is_refused() {
        let pki = pki();
        let impostor = pki.impostor.clone();
        assert_eq!(
            status_for(&pki, Some(&impostor)).await,
            "HTTP/1.1 401 Unauthorized"
        );
    }

    #[tokio::test]
    async fn the_operators_own_certificate_is_served() {
        let pki = pki();
        let operator = pki.operator.clone();
        assert_eq!(status_for(&pki, Some(&operator)).await, "HTTP/1.1 200 OK");
    }

    #[test]
    fn every_refusal_names_a_different_cause() {
        use crate::tls::ControlIdentity;
        let reasons = [
            ControlIdentity::Anonymous.refusal().unwrap(),
            ControlIdentity::Untrusted("bad signature".to_owned())
                .refusal()
                .unwrap(),
            ControlIdentity::WrongName(OPERATOR.to_owned())
                .refusal()
                .unwrap(),
        ];
        assert!(ControlIdentity::Authorized.refusal().is_none());
        for (index, reason) in reasons.iter().enumerate() {
            for other in &reasons[index + 1..] {
                assert_ne!(reason, other);
            }
        }
    }
}
