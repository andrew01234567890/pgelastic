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

/// A running `postgres:18`.
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

/// Starts `postgres:18` with TLS enabled and SCRAM required on host connections.
///
/// The certificate is written by an `initdb.d` script rather than copied in:
/// the script runs as the `postgres` user, so the key ends up owned by the user
/// the server runs as, which is the only ownership `PostgreSQL` accepts.
pub async fn start_postgres() -> Postgres {
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
         CONFEOF\n",
        cert = certificates.cert_pem,
        key = certificates.key_pem,
    );

    let container = postgres::Postgres::default()
        .with_user(BACKEND_USER)
        .with_password(BACKEND_PASSWORD)
        .with_db_name(BACKEND_DATABASE)
        .with_tag("18")
        .with_copy_to(
            "/docker-entrypoint-initdb.d/00-enable-ssl.sh",
            script.into_bytes(),
        )
        .start()
        .await
        .expect("postgres:18 must start — these tests require a working Docker daemon");

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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let url = postgres.direct_url("pgelastic_readiness");
    loop {
        match tokio_postgres::connect(&url, tokio_postgres::NoTls).await {
            Ok((_client, connection)) => {
                drop(connection);
                return;
            }
            Err(error) => assert!(
                std::time::Instant::now() < deadline,
                "postgres:18 never became reachable: {error}"
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
    format!(
        "[listen]\n\
         address = \"127.0.0.1:0\"\n\
         {listen_extra}\n\
         \n\
         [backend]\n\
         address = \"{address}\"\n\
         user = \"{user}\"\n\
         password = \"{password}\"\n\
         database = \"{database}\"\n\
         \n\
         [drain]\n\
         shutdownSeconds = 30\n\
         {extra}\n",
        address = pg.address(),
        user = BACKEND_USER,
        password = BACKEND_PASSWORD,
        database = BACKEND_DATABASE,
    )
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
