//! The thundering-herd defense, proved through the running proxy.
//!
//! No `PostgreSQL` here on purpose: the property is about what the proxy does
//! when a backend will *not* talk to it, so the backend is a listener that
//! accepts and hangs up. That listener is also the only honest way to count
//! connection attempts — a proxy-side counter would be asserting on the very
//! bookkeeping under test.

mod harness;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;

use harness::{BACKEND_DATABASE, ProxyUnderTest, start_proxy};

/// A backend that answers every connection by closing it, and counts them.
struct RefusingBackend {
    address: SocketAddr,
    attempts: Arc<AtomicUsize>,
    accepting: tokio::task::JoinHandle<()>,
}

impl RefusingBackend {
    async fn start() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("binding the refusing backend");
        let address = listener.local_addr().expect("the backend's address");
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let accepting = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                drop(socket);
            }
        });
        Self {
            address,
            attempts,
            accepting,
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

impl Drop for RefusingBackend {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

async fn proxy_for(backend: &RefusingBackend, login_retry_seconds: u64) -> ProxyUnderTest {
    start_proxy(&format!(
        "[listen]\n\
         address = \"127.0.0.1:0\"\n\
         \n\
         [backend]\n\
         address = \"{address}\"\n\
         user = \"pgelastic\"\n\
         database = \"{BACKEND_DATABASE}\"\n\
         connectSeconds = 2\n\
         \n\
         [pool]\n\
         mode = \"transaction\"\n\
         backendConnections = 32\n\
         serverLoginRetrySeconds = {login_retry_seconds}\n",
        address = backend.address,
    ))
    .await
}

/// Connects `count` clients at once, returning the SQLSTATE each was refused
/// with.
async fn burst(proxy: &ProxyUnderTest, count: usize) -> Vec<String> {
    let url = format!(
        "host=127.0.0.1 port={} user=tenant dbname={BACKEND_DATABASE}",
        proxy.port()
    );
    let mut clients = Vec::with_capacity(count);
    for _ in 0..count {
        let url = url.clone();
        clients.push(tokio::spawn(async move {
            tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .err()
                .and_then(|error| error.as_db_error().map(|db| db.code().code().to_owned()))
        }));
    }

    let mut codes = Vec::with_capacity(count);
    for client in clients {
        codes.push(
            client
                .await
                .expect("the client task must not panic")
                .expect("every client must be refused with a real ErrorResponse"),
        );
    }
    codes
}

#[tokio::test(flavor = "multi_thread")]
async fn a_burst_of_clients_against_a_refusing_backend_makes_exactly_one_connect_attempt() {
    let backend = RefusingBackend::start().await;
    let proxy = proxy_for(&backend, 15).await;

    let codes = burst(&proxy, 16).await;

    assert_eq!(codes.len(), 16);
    assert!(
        codes.iter().all(|code| code == "08006"),
        "every refusal must carry the cached connection failure, got {codes:?}"
    );
    assert_eq!(
        backend.attempts(),
        1,
        "the connect gate must let exactly one attempt through a burst"
    );

    let rendered = proxy.metrics.render();
    assert!(
        rendered.contains("outcome=\"attempted\"} 1"),
        "expected one attempt in {rendered}"
    );
    assert!(
        rendered.contains("outcome=\"fast_failed\"} 15"),
        "expected fifteen fast-failures in {rendered}"
    );

    proxy.running.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn exactly_one_new_attempt_is_made_once_the_login_retry_interval_elapses() {
    let backend = RefusingBackend::start().await;
    let proxy = proxy_for(&backend, 1).await;

    burst(&proxy, 8).await;
    assert_eq!(backend.attempts(), 1);

    // Still inside the window: nothing new may reach the backend.
    tokio::time::sleep(Duration::from_millis(300)).await;
    burst(&proxy, 8).await;
    assert_eq!(
        backend.attempts(),
        1,
        "a client arriving inside serverLoginRetry must not dial"
    );

    tokio::time::sleep(Duration::from_millis(900)).await;
    burst(&proxy, 8).await;
    assert_eq!(
        backend.attempts(),
        2,
        "exactly one attempt may be made once serverLoginRetry has elapsed"
    );

    proxy.running.shutdown().await;
}
