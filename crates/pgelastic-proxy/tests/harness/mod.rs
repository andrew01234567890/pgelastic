//! (Shared by several test binaries; each uses a subset.)
#![allow(dead_code)]

//! A real `PostgreSQL` 18 container and a real proxy in front of it.
//!
//! Nothing here skips when Docker is missing: a container that will not start
//! is a failed test, because a green suite that quietly proved nothing is worse
//! than a red one.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use pgelastic_proxy::config::Config;
use pgelastic_proxy::metrics::Metrics;
use pgelastic_proxy::server::{self, Proxy, Running};
use std::str::FromStr as _;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres;
use tokio::sync::watch;

/// Containers allowed to be starting at once within one test binary.
///
/// Deliberately small. `cargo test` runs the test binaries in parallel too, so
/// the real ceiling is this times the number of binaries, and `initdb` is disk
/// bound: past a dozen at once they only make each other slower until one
/// misses its readiness window and fails a test that has nothing to do with
/// start-up.
static STARTING: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

pub const BACKEND_USER: &str = "pgelastic";
pub const BACKEND_PASSWORD: &str = "backend-secret-not-real";
pub const BACKEND_DATABASE: &str = "appdb";

/// A CA plus one certificate signed by it, on disk.
pub struct Certificates {
    pub dir: tempfile::TempDir,
    pub ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
}

impl Certificates {
    pub fn generate() -> Self {
        use rcgen::{
            BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose,
        };

        let mut ca_params = CertificateParams::new(Vec::new()).expect("CA parameters");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "pgelastic test CA");
        let ca_key = KeyPair::generate().expect("CA key");
        let ca_cert = ca_params.self_signed(&ca_key).expect("self-signed CA");

        let mut leaf_params =
            CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
                .expect("leaf parameters");
        leaf_params
            .distinguished_name
            .push(DnType::CommonName, "localhost");
        let leaf_key = KeyPair::generate().expect("leaf key");
        let issuer = Issuer::from_params(&ca_params, &ca_key);
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &issuer)
            .expect("signed leaf");

        let dir = tempfile::TempDir::new().expect("temp dir");
        let this = Self {
            ca_pem: ca_cert.pem(),
            cert_pem: leaf_cert.pem(),
            key_pem: leaf_key.serialize_pem(),
            dir,
        };
        std::fs::write(this.ca_path(), &this.ca_pem).expect("write ca");
        std::fs::write(this.cert_path(), &this.cert_pem).expect("write cert");
        std::fs::write(this.key_path(), &this.key_pem).expect("write key");
        this
    }

    pub fn ca_path(&self) -> PathBuf {
        self.dir.path().join("ca.pem")
    }

    pub fn cert_path(&self) -> PathBuf {
        self.dir.path().join("server.pem")
    }

    pub fn key_path(&self) -> PathBuf {
        self.dir.path().join("server.key")
    }

    pub fn root_store(&self) -> rustls::RootCertStore {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut self.ca_pem.as_bytes()) {
            roots.add(cert.expect("parse ca")).expect("add ca");
        }
        roots
    }
}

/// The name the control API's client certificate carries. A caller without a
/// certificate, or with one from another authority, is refused with 401 — so the
/// harness has to be an authenticated caller before it can drive a cutover.
pub const CONTROL_CLIENT_NAME: &str = "pgelastic-test-operator";

/// The control listener's mutual TLS: one CA, the listener's certificate, and
/// the caller's.
pub struct ControlPki {
    dir: tempfile::TempDir,
    roots: rustls::RootCertStore,
    client_chain: Vec<rustls_pki_types::CertificateDer<'static>>,
    client_key_der: Vec<u8>,
}

impl ControlPki {
    pub fn generate() -> Self {
        use rcgen::{
            BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
            KeyPair, KeyUsagePurpose,
        };

        let mut ca_params = CertificateParams::new(Vec::new()).expect("CA parameters");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "pgelastic control CA");
        let ca_key = KeyPair::generate().expect("CA key");
        let ca_cert = ca_params.self_signed(&ca_key).expect("self-signed CA");
        let issuer = Issuer::from_params(&ca_params, &ca_key);

        let leaf = |name: &str, usage: ExtendedKeyUsagePurpose| {
            let mut params = CertificateParams::new(vec![name.to_owned()]).expect("parameters");
            params.distinguished_name.push(DnType::CommonName, name);
            params.extended_key_usages = vec![usage];
            let key = KeyPair::generate().expect("leaf key");
            let cert = params.signed_by(&key, &issuer).expect("signed leaf");
            (cert, key)
        };
        let (server, server_key) = leaf("localhost", ExtendedKeyUsagePurpose::ServerAuth);
        let (client, client_key) = leaf(CONTROL_CLIENT_NAME, ExtendedKeyUsagePurpose::ClientAuth);

        let dir = tempfile::TempDir::new().expect("temp dir");
        std::fs::write(dir.path().join("ca.pem"), ca_cert.pem()).expect("write ca");
        std::fs::write(dir.path().join("server.pem"), server.pem()).expect("write cert");
        std::fs::write(dir.path().join("server.key"), server_key.serialize_pem())
            .expect("write key");

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls_pki_types::CertificateDer::from(
                ca_cert.der().to_vec(),
            ))
            .expect("add ca");

        Self {
            dir,
            roots,
            client_chain: vec![rustls_pki_types::CertificateDer::from(
                client.der().to_vec(),
            )],
            client_key_der: client_key.serialize_der(),
        }
    }

    /// The `[control.tls]` section the proxy is configured with.
    pub fn section(&self) -> String {
        format!(
            "[control.tls]\n\
             certificateFile = \"{cert}\"\n\
             keyFile = \"{key}\"\n\
             clientCaFile = \"{ca}\"\n\
             clientName = \"{CONTROL_CLIENT_NAME}\"\n",
            cert = self.dir.path().join("server.pem").display(),
            key = self.dir.path().join("server.key").display(),
            ca = self.dir.path().join("ca.pem").display(),
        )
    }

    fn connector(&self) -> tokio_rustls::TlsConnector {
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(self.roots.clone())
            .with_client_auth_cert(
                self.client_chain.clone(),
                rustls_pki_types::PrivateKeyDer::try_from(self.client_key_der.clone())
                    .expect("a generated key is well formed"),
            )
            .expect("the client identity is usable");
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    /// A connector that trusts the listener but proves nothing about itself,
    /// for asserting that such a caller is refused rather than served.
    pub fn anonymous_connector(&self) -> tokio_rustls::TlsConnector {
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(self.roots.clone())
            .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }
}

/// The `PostgreSQL` major these tests run against.
///
/// An environment variable rather than a literal, and the same one the Go suites read, so a
/// second major is a variable rather than four edits in this file. It defaults to the major
/// the merge gate runs, because a test run that silently chose a different one would be
/// answering a question nobody asked.
fn postgres_tag() -> String {
    std::env::var("PGELASTIC_PG_MAJOR").unwrap_or_else(|_| "18".to_owned())
}

/// A running `PostgreSQL` container.
pub struct Postgres {
    _container: ContainerAsync<postgres::Postgres>,
    pub port: u16,
    pub certificates: Certificates,
}

impl Postgres {
    pub fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// A connection string that bypasses the proxy entirely.
    ///
    /// Used to observe the server's own view of how many backends the proxy is
    /// holding, which is the only account that cannot be fooled by a bug in the
    /// proxy's own bookkeeping. The `application_name` is what distinguishes the
    /// observer from the connections it is counting.
    pub fn direct_url(&self, application_name: &str) -> String {
        format!(
            "host=127.0.0.1 port={} user={BACKEND_USER} password={BACKEND_PASSWORD} \
             dbname={BACKEND_DATABASE} application_name={application_name}",
            self.port
        )
    }
}

/// Starts `PostgreSQL` with TLS enabled and SCRAM required on host connections.
///
/// The certificate is written by an `initdb.d` script rather than copied in:
/// the script runs as the `postgres` user, so the key ends up owned by the user
/// the server runs as, which is the only ownership `PostgreSQL` accepts.
pub async fn start_postgres() -> Postgres {
    start_postgres_with("").await
}

/// As [`start_postgres`], with `extra_conf` appended to `postgresql.conf`.
///
/// How the epoch tests bind `pgelastic.primary_epoch` into a postmaster: a
/// dotted parameter name is a *placeholder* GUC, so `PostgreSQL` accepts it
/// from the configuration file without an extension having defined it, and
/// `current_setting()` reads it back off any backend connection — which is
/// exactly the property the fence's pull path depends on.
pub async fn start_postgres_with(extra_conf: &str) -> Postgres {
    // Container start-up is disk- and CPU-bound, and a binary whose tests each
    // want two of them will start eight at once. Past a handful they simply
    // make each other slower, and a server that has not finished initdb inside
    // the readiness window fails a test that has nothing to do with start-up.
    let _slot = STARTING
        .acquire()
        .await
        .expect("the start-up limiter is never closed");
    let certificates = Certificates::generate();
    let script = format!(
        "set -e\n\
         cat > \"$PGDATA/server.crt\" <<'PEMEOF'\n{cert}PEMEOF\n\
         cat > \"$PGDATA/server.key\" <<'PEMEOF'\n{key}PEMEOF\n\
         chmod 600 \"$PGDATA/server.key\"\n\
         cat >> \"$PGDATA/postgresql.conf\" <<'CONFEOF'\n\
         ssl = on\n\
         ssl_cert_file = 'server.crt'\n\
         ssl_key_file = 'server.key'\n\
         {extra_conf}\n\
         CONFEOF\n",
        cert = certificates.cert_pem,
        key = certificates.key_pem,
    );

    let container = postgres::Postgres::default()
        .with_user(BACKEND_USER)
        .with_password(BACKEND_PASSWORD)
        .with_db_name(BACKEND_DATABASE)
        .with_tag(postgres_tag())
        .with_copy_to(
            "/docker-entrypoint-initdb.d/00-enable-ssl.sh",
            script.into_bytes(),
        )
        .start()
        .await
        .expect("PostgreSQL must start - these tests require a working Docker daemon");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port must be published");

    let postgres = Postgres {
        _container: container,
        port,
        certificates,
    };
    await_ready(&postgres).await;
    postgres
}

/// Waits until the server is really accepting connections on the published
/// port.
///
/// The container's "ready" log line is emitted before the mapped host port is
/// reliably reachable under load, and a proxy that meets a refused connect
/// remembers it for `serverLoginRetry` — so handing the proxy a backend that is
/// not up yet poisons its pool for the rest of the test.
async fn await_ready(postgres: &Postgres) {
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(120);
    let url = postgres.direct_url("pgelastic_readiness");
    let mut attempts = 0_u32;
    loop {
        attempts += 1;
        match tokio_postgres::connect(&url, tokio_postgres::NoTls).await {
            Ok((_client, connection)) => {
                drop(connection);
                return;
            }
            // Reports the elapsed time and attempt count because the failure this
            // produces under a loaded Docker daemon is indistinguishable from a real
            // startup fault without them.
            Err(error) => assert!(
                std::time::Instant::now() < deadline,
                "PostgreSQL never became reachable after {attempts} attempts over {:?}: {error}",
                started.elapsed()
            ),
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A proxy listening on an ephemeral port.
pub struct ProxyUnderTest {
    pub running: Running,
    pub metrics: Arc<Metrics>,
    pub shutdown: watch::Sender<bool>,
}

impl ProxyUnderTest {
    pub fn address(&self) -> SocketAddr {
        self.running.address
    }

    /// Where the epoch push endpoint bound.
    ///
    /// Asked of the listener rather than chosen in advance, so a test never
    /// reserves a port, releases it, and races whatever takes it next.
    pub fn push_port(&self) -> u16 {
        self.running
            .push_address
            .expect("the push endpoint must report where it bound")
            .port()
    }

    pub fn port(&self) -> u16 {
        self.running.address.port()
    }
}

/// Builds a proxy configuration for a backend, with `extra` appended verbatim.
pub fn config_for(pg: &Postgres, extra: &str) -> String {
    config_for_listener(pg, "", extra)
}

/// As [`config_for`], with `listen_extra` folded into the `[listen]` table.
pub fn config_for_listener(pg: &Postgres, listen_extra: &str, extra: &str) -> String {
    config_for_address(&pg.address(), listen_extra, extra)
}

/// As [`config_for_listener`], against an arbitrary backend address — a
/// [`Switch`], for the tests that move the backend under the proxy.
pub fn config_for_address(address: &str, listen_extra: &str, extra: &str) -> String {
    format!(
        "[listen]\n\
         address = \"127.0.0.1:0\"\n\
         {listen_extra}\n\
         \n\
         [backend]\n\
         address = \"{address}\"\n\
         user = \"{BACKEND_USER}\"\n\
         password = \"{BACKEND_PASSWORD}\"\n\
         database = \"{BACKEND_DATABASE}\"\n\
         \n\
         [drain]\n\
         shutdownSeconds = 30\n\
         {extra}\n",
    )
}

/// A TCP forwarder whose destination can be changed while it is running.
///
/// It stands in for the Kubernetes Service in front of an instance, including
/// the property the whole epoch fence exists because of: repointing it does
/// **nothing** to the connections already established through it. Only new
/// connections follow the new target, exactly as kube-proxy behaves — it never
/// touches `ESTABLISHED` conntrack entries.
pub struct Switch {
    pub address: SocketAddr,
    target: Arc<std::sync::Mutex<SocketAddr>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Switch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Switch {
    pub async fn to(pg: &Postgres) -> Self {
        let target = Arc::new(std::sync::Mutex::new(
            pg.address().parse().expect("a container address parses"),
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding the switch");
        let address = listener.local_addr().expect("the switch's address");

        let forward = Arc::clone(&target);
        let task = tokio::spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                let to = *forward.lock().expect("the switch is never poisoned");
                tokio::spawn(async move {
                    let Ok(mut server) = tokio::net::TcpStream::connect(to).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                });
            }
        });

        Self {
            address,
            target,
            task,
        }
    }

    /// Points new connections at another container. Established ones are left
    /// exactly where they are.
    pub fn point_at(&self, pg: &Postgres) {
        *self.target.lock().expect("the switch is never poisoned") =
            pg.address().parse().expect("a container address parses");
    }

    pub fn address(&self) -> String {
        self.address.to_string()
    }
}

pub async fn start_proxy(source: &str) -> ProxyUnderTest {
    pgelastic_proxy::tls::install_crypto_provider();
    let config = Config::from_str(source).expect("the test configuration must parse");
    let metrics = Metrics::new();
    let proxy = Proxy::new(config, Arc::clone(&metrics)).expect("the proxy must build");
    let (shutdown, _) = watch::channel(false);
    let running = server::spawn(proxy, shutdown.clone())
        .await
        .expect("the proxy must bind");
    ProxyUnderTest {
        running,
        metrics,
        shutdown,
    }
}

/// The whole stack: a container and a proxy in front of it.
pub struct Stack {
    pub pg: Postgres,
    pub proxy: ProxyUnderTest,
}

pub async fn stack_with(extra: &str) -> Stack {
    let pg = start_postgres().await;
    let proxy = start_proxy(&config_for(&pg, extra)).await;
    Stack { pg, proxy }
}

pub async fn stack() -> Stack {
    stack_with("").await
}

impl Stack {
    pub fn url(&self) -> String {
        self.url_for("tenant")
    }

    /// The proxy sees the startup `user` as the tenant, so two different values
    /// here are two different tenants with two different pool keys.
    pub fn url_for(&self, tenant: &str) -> String {
        format!(
            "host=127.0.0.1 port={} user={tenant} dbname={BACKEND_DATABASE}",
            self.proxy.port()
        )
    }

    pub async fn connect_as(&self, tenant: &str) -> tokio_postgres::Client {
        self.connect_with(&self.url_for(tenant)).await
    }

    /// Connects, returning the driver error rather than panicking, so a refusal
    /// can be asserted on.
    pub async fn try_connect_as(
        &self,
        tenant: &str,
    ) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
        let (client, connection) =
            tokio_postgres::connect(&self.url_for(tenant), tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// Opens a connection straight to `PostgreSQL`, past the proxy.
    pub async fn observer(&self, application_name: &str) -> tokio_postgres::Client {
        self.connect_with(&self.pg.direct_url(application_name))
            .await
    }

    /// Connects through the proxy without TLS, driving the connection task in
    /// the background.
    pub async fn connect(&self) -> tokio_postgres::Client {
        self.connect_with(&self.url()).await
    }

    pub async fn connect_with(&self, url: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .expect("connecting through the proxy must succeed");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    pub fn localhost(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.proxy.port())
    }
}

/// A protocol-level client, for the assertions `tokio-postgres` hides.
///
/// `ReadyForQuery`'s transaction-status byte and the shape of a pipelined batch
/// are both invisible through a driver, and both are exactly what the proxy has
/// to get right.
pub struct RawClient {
    socket: tokio::net::TcpStream,
    buf: pgelastic_wire::MessageBuffer,
}

impl RawClient {
    pub async fn connect(address: SocketAddr, user: &str, database: &str) -> Self {
        use bytes::{Bytes, BytesMut};
        use pgelastic_wire::{ProtocolVersion, StartupMessage};

        let mut socket = tokio::net::TcpStream::connect(address)
            .await
            .expect("connecting to the proxy");
        let mut wire = BytesMut::new();
        StartupMessage::new(
            ProtocolVersion::V3_0,
            vec![
                (
                    Bytes::from_static(b"user"),
                    Bytes::copy_from_slice(user.as_bytes()),
                ),
                (
                    Bytes::from_static(b"database"),
                    Bytes::copy_from_slice(database.as_bytes()),
                ),
                // Startup parameters are part of the pool key, so a raw client
                // that omits what `tokio-postgres` sends would land in a pool of
                // its own and never share a backend with one.
                (
                    Bytes::from_static(b"client_encoding"),
                    Bytes::from_static(b"UTF8"),
                ),
            ],
        )
        .encode(&mut wire);
        tokio::io::AsyncWriteExt::write_all(&mut socket, &wire)
            .await
            .expect("sending the startup packet");

        let mut client = Self {
            socket,
            buf: pgelastic_wire::MessageBuffer::new(),
        };
        client.read_until_ready().await;
        client
    }

    pub async fn send(&mut self, messages: &[pgelastic_wire::FrontendMessage]) {
        pgelastic_proxy::wire_io::write_frontend(&mut self.socket, messages)
            .await
            .expect("writing to the proxy");
    }

    pub async fn read(&mut self) -> pgelastic_wire::BackendMessage {
        pgelastic_proxy::wire_io::read_backend_message(&mut self.socket, &mut self.buf)
            .await
            .expect("reading from the proxy")
    }

    /// Reads until the proxy has nothing more to say, tolerating the close that
    /// follows a `FATAL`.
    pub async fn read_until_closed(&mut self) -> Vec<pgelastic_wire::BackendMessage> {
        let mut messages = Vec::new();
        while let Ok(message) =
            pgelastic_proxy::wire_io::read_backend_message(&mut self.socket, &mut self.buf).await
        {
            let ready = matches!(message, pgelastic_wire::BackendMessage::ReadyForQuery(_));
            messages.push(message);
            if ready {
                break;
            }
        }
        messages
    }

    pub async fn query_until_closed(&mut self, sql: &str) -> Vec<pgelastic_wire::BackendMessage> {
        self.send(&[pgelastic_wire::FrontendMessage::Query(
            bytes::Bytes::copy_from_slice(sql.as_bytes()),
        )])
        .await;
        self.read_until_closed().await
    }

    /// Reads to the next `ReadyForQuery`, returning what came before it and the
    /// transaction status it reported.
    pub async fn read_until_ready(
        &mut self,
    ) -> (
        Vec<pgelastic_wire::BackendMessage>,
        pgelastic_wire::TransactionStatus,
    ) {
        let mut messages = Vec::new();
        loop {
            match self.read().await {
                pgelastic_wire::BackendMessage::ReadyForQuery(status) => {
                    return (messages, status);
                }
                other => messages.push(other),
            }
        }
    }

    /// Reads until the message the predicate accepts, panicking if the stream
    /// ends first.
    pub async fn read_until(
        &mut self,
        accept: impl Fn(&pgelastic_wire::BackendMessage) -> bool,
    ) -> pgelastic_wire::BackendMessage {
        loop {
            let message = self.read().await;
            if accept(&message) {
                return message;
            }
        }
    }

    /// Whether the proxy closes this connection within `limit`, whatever it says
    /// on the way out.
    ///
    /// Reads rather than sleeps, because a bound that ends a session is only
    /// observable as the socket going away: a client with nothing to send would
    /// otherwise never learn it had happened.
    pub async fn closed_within(&mut self, limit: std::time::Duration) -> bool {
        tokio::time::timeout(limit, async {
            while pgelastic_proxy::wire_io::read_backend_message(&mut self.socket, &mut self.buf)
                .await
                .is_ok()
            {}
        })
        .await
        .is_ok()
    }

    pub async fn simple_query(
        &mut self,
        sql: &str,
    ) -> (
        Vec<pgelastic_wire::BackendMessage>,
        pgelastic_wire::TransactionStatus,
    ) {
        // The encoder appends the terminating NUL; adding one here would make
        // the frame one byte too long and draw `invalid message format`.
        self.send(&[pgelastic_wire::FrontendMessage::Query(
            bytes::Bytes::copy_from_slice(sql.as_bytes()),
        )])
        .await;
        self.read_until_ready().await
    }
}

/// Two `PostgreSQL` containers behind one proxy, plus its control API.
///
/// The pair is what makes "bounded to the affected instance" testable at all: a
/// claim about blast radius needs a bystander, and a bystander that shares a
/// process with the casualty is the only kind that can prove anything.
pub struct Fleet {
    pub a: Postgres,
    pub b: Postgres,
    pub proxy: ProxyUnderTest,
    pub control: Control,
}

impl Fleet {
    /// Starts both containers and a proxy fronting them, with `extra` appended
    /// to the configuration.
    pub async fn start(extra: &str) -> Self {
        Self::start_sized(20, 20, extra).await
    }

    /// As [`start`](Self::start), with an explicit backend budget per instance.
    ///
    /// A budget of one makes the order a queue drains in observable: only one
    /// transaction can be running, so the order queued transactions leave their
    /// mark in *is* the order the gate released them. It says nothing about the
    /// order of transactions the gate never held - a caller watching for an
    /// effect has to make sure the transaction that produces it is the one that
    /// was queued.
    pub async fn start_sized(a_budget: u32, b_budget: u32, extra: &str) -> Self {
        Self::start_leased(a_budget, b_budget, 15_000, extra).await
    }

    /// As [`start_sized`](Self::start_sized), with the control API's default
    /// lease. The lease-expiry sweep is derived from it, so a test about what a
    /// killed operator costs has to set it.
    pub async fn start_leased(
        a_budget: u32,
        b_budget: u32,
        default_lease_ms: u64,
        extra: &str,
    ) -> Self {
        Self::start_leased_with_conf(a_budget, b_budget, default_lease_ms, "", extra).await
    }

    /// As [`start_leased`](Self::start_leased), with `a_conf` appended to the
    /// first member's `postgresql.conf`.
    ///
    /// A postmaster setting a test needs from birth rather than by reload:
    /// `synchronous_standby_names`, whose effect on commits is published by the
    /// checkpointer and so cannot be waited for from a client connection.
    pub async fn start_leased_with_conf(
        a_budget: u32,
        b_budget: u32,
        default_lease_ms: u64,
        a_conf: &str,
        extra: &str,
    ) -> Self {
        let (a, b) = tokio::join!(start_postgres_with(a_conf), start_postgres());
        let pki = ControlPki::generate();
        let source = format!(
            "[listen]\n\
             address = \"127.0.0.1:0\"\n\
             \n\
             [backend]\n\
             address = \"{a_addr}\"\n\
             user = \"{BACKEND_USER}\"\n\
             password = \"{BACKEND_PASSWORD}\"\n\
             database = \"{BACKEND_DATABASE}\"\n\
             \n\
             [drain]\n\
             shutdownSeconds = 30\n\
             \n\
             [pool]\n\
             mode = \"transaction\"\n\
             \n\
             [control]\n\
             address = \"127.0.0.1:0\"\n\
             defaultLeaseTtlMs = {default_lease_ms}\n\
             \n\
             {control_tls}\
             \n\
             [[instances]]\n\
             name = \"inst-a\"\n\
             address = \"{a_addr}\"\n\
             backendConnections = {a_budget}\n\
             \n\
             [[instances]]\n\
             name = \"inst-b\"\n\
             address = \"{b_addr}\"\n\
             backendConnections = {b_budget}\n\
             \n\
             [routing]\n\
             defaultInstance = \"inst-a\"\n\
             tenants = {{ beta = \"inst-b\" }}\n\
             {extra}\n",
            a_addr = a.address(),
            b_addr = b.address(),
            control_tls = pki.section(),
        );
        let proxy = start_proxy(&source).await;
        // Read back rather than chosen: the listener binds port 0 and reports
        // what it got, so no port is ever reserved and released for something
        // else on the host to take in between.
        let control = Control {
            address: proxy
                .running
                .control_address
                .expect("the control listener must report where it bound")
                .to_string(),
            connector: pki.connector(),
            pki,
        };
        control.await_ready().await;
        Self {
            a,
            b,
            proxy,
            control,
        }
    }

    pub fn url_for(&self, tenant: &str) -> String {
        format!(
            "host=127.0.0.1 port={} user={tenant} dbname={BACKEND_DATABASE}",
            self.proxy.port()
        )
    }

    pub async fn connect_as(&self, tenant: &str) -> tokio_postgres::Client {
        let (client, connection) =
            tokio_postgres::connect(&self.url_for(tenant), tokio_postgres::NoTls)
                .await
                .expect("connecting through the proxy must succeed");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    pub async fn try_connect_as(
        &self,
        tenant: &str,
    ) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
        let (client, connection) =
            tokio_postgres::connect(&self.url_for(tenant), tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// A connection straight to one container, past the proxy.
    pub async fn observer(&self, pg: &Postgres, application_name: &str) -> tokio_postgres::Client {
        let (client, connection) =
            tokio_postgres::connect(&pg.direct_url(application_name), tokio_postgres::NoTls)
                .await
                .expect("connecting to the container must succeed");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
}

// There is deliberately no free_port helper. Reserving a port, closing the
// socket and then asking the proxy to bind it is a race against everything else
// on the host, and it is the race that turned a green suite red under CI load
// with `Address already in use`. Every listener binds port 0 and reports what it
// got instead.

/// A minimal HTTP/1.1-over-mutual-TLS client for the proxy's control API.
///
/// Hand-rolled rather than pulled in: the proxy's own `hyper` carries only the
/// server half, and a cutover's five calls are a status line and a JSON body
/// each. The TLS is not optional — the listener refuses an unauthenticated
/// caller with 401, because quiescing a tenant holds its clients' sockets open
/// with nothing behind them.
pub struct Control {
    pub address: String,
    pub pki: ControlPki,
    connector: tokio_rustls::TlsConnector,
}

/// One control-API answer.
#[derive(Debug)]
pub struct ControlResponse {
    pub status: u16,
    pub body: serde_json::Value,
}

impl ControlResponse {
    pub fn ok(self) -> serde_json::Value {
        assert_eq!(self.status, 200, "the call was refused: {}", self.body);
        self.body
    }

    pub fn str_field(&self, name: &str) -> String {
        self.body[name]
            .as_str()
            .unwrap_or_else(|| panic!("{name} missing from {}", self.body))
            .to_owned()
    }
}

impl Control {
    async fn await_ready(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while tokio::net::TcpStream::connect(&self.address).await.is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "the control endpoint never bound"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    pub async fn get(&self, path: &str) -> ControlResponse {
        self.request("GET", path, None).await
    }

    pub async fn post(&self, path: &str, body: serde_json::Value) -> ControlResponse {
        self.request("POST", path, Some(body.to_string())).await
    }

    pub async fn quiesce(&self, tenant: &str, holder: &str, ttl_ms: u64) -> ControlResponse {
        self.post(
            "/quiesce",
            serde_json::json!({ "tenant": tenant, "holder": holder, "ttlMs": ttl_ms }),
        )
        .await
    }

    pub async fn drain_status(&self, tenant: &str) -> serde_json::Value {
        self.get(&format!("/drainStatus?tenant={tenant}"))
            .await
            .ok()
    }

    pub async fn set_route(&self, tenant: &str, holder: &str, instance: &str) -> ControlResponse {
        self.post(
            "/setRoute",
            serde_json::json!({ "tenant": tenant, "holder": holder, "instance": instance }),
        )
        .await
    }

    pub async fn resume(&self, tenant: &str, holder: &str) -> ControlResponse {
        self.post(
            "/resume",
            serde_json::json!({ "tenant": tenant, "holder": holder }),
        )
        .await
    }

    pub async fn unquiesce(&self, tenant: &str, holder: &str) -> ControlResponse {
        self.post(
            "/unquiesce",
            serde_json::json!({ "tenant": tenant, "holder": holder }),
        )
        .await
    }

    pub async fn quiesce_instance(
        &self,
        instance: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> ControlResponse {
        self.post(
            "/quiesceInstance",
            serde_json::json!({ "instance": instance, "holder": holder, "ttlMs": ttl_ms }),
        )
        .await
    }

    pub async fn instance_drain_status(&self, instance: &str) -> serde_json::Value {
        self.get(&format!("/instanceDrainStatus?instance={instance}"))
            .await
            .ok()
    }

    pub async fn resume_instance(&self, instance: &str, holder: &str) -> ControlResponse {
        self.post(
            "/resumeInstance",
            serde_json::json!({ "instance": instance, "holder": holder }),
        )
        .await
    }

    pub async fn unquiesce_instance(&self, instance: &str, holder: &str) -> ControlResponse {
        self.post(
            "/unquiesceInstance",
            serde_json::json!({ "instance": instance, "holder": holder }),
        )
        .await
    }

    /// The write health the proxy currently believes of one instance.
    pub async fn write_health(&self, instance: &str) -> String {
        let report = self.get("/instances").await.ok();
        report["instances"]
            .as_array()
            .expect("instances is an array")
            .iter()
            .find(|entry| entry["name"] == instance)
            .unwrap_or_else(|| panic!("{instance} is not in {report}"))["writeHealth"]
            .as_str()
            .expect("writeHealth is a string")
            .to_owned()
    }

    async fn request(&self, method: &str, path: &str, body: Option<String>) -> ControlResponse {
        self.request_as(&self.connector, method, path, body).await
    }

    /// The same call made by a caller of the test's choosing, so a spec can ask
    /// what an unauthenticated one gets.
    pub async fn request_as(
        &self,
        connector: &tokio_rustls::TlsConnector,
        method: &str,
        path: &str,
        body: Option<String>,
    ) -> ControlResponse {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let tcp = tokio::net::TcpStream::connect(&self.address)
            .await
            .expect("the control endpoint must be reachable");
        let name = rustls_pki_types::ServerName::try_from("localhost").expect("a valid name");
        let mut socket = connector
            .connect(name, tcp)
            .await
            .expect("the control endpoint must complete a TLS handshake");
        let body = body.unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: control\r\nConnection: close\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        socket
            .write_all(request.as_bytes())
            .await
            .expect("writing the control request");
        socket.flush().await.expect("flushing the control request");

        let mut raw = Vec::new();
        socket
            .read_to_end(&mut raw)
            .await
            .expect("reading the control response");
        let text = String::from_utf8_lossy(&raw).into_owned();
        let (head, payload) = text
            .split_once("\r\n\r\n")
            .unwrap_or_else(|| panic!("a control response must have a body: {text:?}"));
        let status: u16 = head
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("a control response must have a status: {head:?}"));
        let body = serde_json::from_str(payload)
            .unwrap_or_else(|error| panic!("the control body {payload:?} is not JSON: {error}"));
        ControlResponse { status, body }
    }
}

/// Polls `condition` until it holds, failing loudly at `deadline`.
pub async fn until(
    what: &str,
    limit: std::time::Duration,
    mut condition: impl AsyncFnMut() -> bool,
) {
    let deadline = std::time::Instant::now() + limit;
    loop {
        if condition().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{what} did not happen within {limit:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// Waits until backend `pid` is actually parked in the synchronous-replication wait.
///
/// Ordering only. This orders whatever comes next - an epoch push, a quiesce - after the
/// commit has reached the wait, and it **cannot cause the wait**: a commit that took the fast
/// exit has already completed, so no amount of waiting will make it park. The caller owes the
/// precondition, and it is not `SHOW synchronous_standby_names`. `SyncRepWaitForLSN` consults
/// `WalSndCtl->sync_standbys_status`, a shared-memory word the **checkpointer** publishes;
/// arming with `ALTER SYSTEM` + `pg_reload_conf` races a process no query can observe. Name
/// the absent standby in `postgresql.conf` before the postmaster starts instead, and the
/// question does not arise: either the checkpointer has published the word, or it has not yet
/// initialised it and the backend falls back to the clause it loaded itself.
///
/// Keyed by pid, because once such a clause is live any commit that opts in parks, and the
/// only evidence worth having is that *this* backend did.
pub async fn await_stalled_commit(observer: &tokio_postgres::Client, pid: i32) {
    until(
        "the commit to park in the synchronous-replication wait",
        std::time::Duration::from_secs(30),
        async || {
            observer
                .query_one(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity \
                     WHERE pid = $1 AND wait_event_type = 'IPC' AND wait_event = 'SyncRep'",
                    &[&pid],
                )
                .await
                .expect("looking for a stalled commit")
                .get::<_, i64>(0)
                > 0
        },
    )
    .await;
}
