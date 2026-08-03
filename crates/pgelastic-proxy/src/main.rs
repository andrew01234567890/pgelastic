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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
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
