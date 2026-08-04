//! The `pgelastic-proxy` binary.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use pgelastic_proxy::config::{self, Config};
use pgelastic_proxy::metrics::{self, Metrics};
use pgelastic_proxy::reload::{self, Reloader};
use pgelastic_proxy::server::{self, Proxy};
use pgelastic_proxy::{Result, tls};
use tokio::sync::watch;
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "pgelastic-proxy", about = "PostgreSQL proxy for pgelastic")]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(long, short)]
    config: PathBuf,
}

/// The environment variable that selects human-readable output.
///
/// JSON is the default because a proxy's logs are read by a collector far more often than by a
/// person, and because `spec.observability.logFormat` has defaulted to `Json` since the field
/// existed. This is the escape hatch for the times it is a person: `kubectl exec`, a local run,
/// a `kind` cluster somebody is staring at.
const LOG_FORMAT_ENV: &str = "PGELASTIC_LOG_FORMAT";

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    tls::install_crypto_provider();

    let args = Args::parse();
    let config = Config::from_file(&args.config)?;
    let metrics = Metrics::new();

    // Bound before the proxy is announced ready, so a replica whose metrics port
    // is already taken fails at start-up rather than passing a readiness probe
    // nobody can reach.
    let metrics_listener = match &config.metrics.address {
        Some(address) => {
            Some(tokio::net::TcpListener::bind(config::resolve(address).await?).await?)
        }
        None => None,
    };

    let (shutdown, receiver) = watch::channel(false);
    let proxy = Proxy::new(config.clone(), Arc::clone(&metrics))?;
    let reloader = Reloader::new(&config, Arc::clone(&proxy), Arc::clone(&metrics));
    let running = server::spawn(proxy, shutdown.clone()).await?;

    if let Some(listener) = metrics_listener {
        tokio::spawn(metrics::serve(listener, Arc::clone(&metrics), receiver));
    }
    if let Some(reloader) = reloader {
        tokio::spawn(reload::run(reloader, config, shutdown.subscribe()));
    }

    // Readiness is admin state and is set here, once the listener is bound and
    // the fleet is built. A probe that answers before this point would report a
    // replica ready while every client on it would be refused.
    metrics.set_ready(true);

    wait_for_signal().await;
    // Flipped before the drain starts rather than after it, so the endpoint is
    // withdrawn while there are still sessions to finish rather than once there
    // are none left to lose.
    metrics.set_ready(false);
    info!("draining");
    if running.shutdown().await {
        info!("drain complete");
    } else {
        warn!("drain timed out; remaining sessions were closed");
    }
    Ok(())
}

/// Waits for `SIGTERM` or `SIGINT`.
///
/// `SIGTERM` is the one that matters: it is what a Kubernetes pod deletion
/// sends, and treating it as an immediate exit resets every in-flight
/// transaction instead of letting it finish.
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(error) => {
                warn!(%error, "cannot listen for SIGTERM");
                return;
            }
        };
        tokio::select! {
            _ = term.recv() => {}
            result = tokio::signal::ctrl_c() => {
                let _ = result;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Installs the subscriber, in JSON unless a human has asked otherwise.
///
/// The filter is read the same way either way, so `RUST_LOG` keeps working exactly as it did.
///
/// Span close records are emitted in both shapes. The default is `FmtSpan::NONE`, which drops
/// every field a span records - so the admission wait, whose whole content is `outcome` and
/// `waited_ms` set at the moment it ends, would produce no output at all. Both branches, and
/// not only the JSON one: a fleet running with `logFormat: Text` would otherwise lose exactly
/// the record somebody set it to read.
fn init_logging() {
    use tracing_subscriber::fmt::format::FmtSpan;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let human = std::env::var(LOG_FORMAT_ENV).is_ok_and(|value| {
        value.eq_ignore_ascii_case("text") || value.eq_ignore_ascii_case("console")
    });
    if human {
        tracing_subscriber::fmt()
            .with_span_events(FmtSpan::CLOSE)
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_events(FmtSpan::CLOSE)
            .with_env_filter(filter)
            .init();
    }
}
