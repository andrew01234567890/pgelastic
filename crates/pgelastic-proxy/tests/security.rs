//! TLS on both legs, SCRAM in both directions, and the pre-startup packet
//! handling that has a CVE attached to getting it wrong.

mod harness;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use harness::{BACKEND_DATABASE, config_for, stack, start_postgres, start_proxy};
use pgelastic_wire::startup::{encode_gssenc_request, encode_ssl_request};
use pgelastic_wire::{
    BackendMessage, MessageBuffer, ProtocolVersion, StartupMessage, TransactionStatus,
};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TENANT_PASSWORD: &str = "tenant-password-not-real";

fn scram_users() -> String {
    format!(
        "\n[auth]\nscramIterations = 4096\n\n\
         [[auth.users]]\nname = \"tenant\"\npassword = \"{TENANT_PASSWORD}\"\n"
    )
}

fn startup_packet(user: &str) -> BytesMut {
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
                Bytes::copy_from_slice(BACKEND_DATABASE.as_bytes()),
            ),
        ],
    )
    .encode(&mut wire);
    wire
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gssenc_request_is_answered_with_a_single_n_byte_and_the_session_continues() {
    let stack = stack().await;
    let mut socket = TcpStream::connect(stack.localhost()).await.unwrap();

    let mut wire = BytesMut::new();
    encode_gssenc_request(&mut wire);
    socket.write_all(&wire).await.unwrap();

    let mut answer = [0u8; 1];
    socket.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer, b"N", "GSSENCRequest must draw exactly one 'N'");

    socket.write_all(&startup_packet("tenant")).await.unwrap();
    let mut buf = MessageBuffer::new();
    loop {
        match pgelastic_proxy::wire_io::read_backend_message(&mut socket, &mut buf)
            .await
            .unwrap()
        {
            BackendMessage::ReadyForQuery(status) => {
                assert_eq!(status, TransactionStatus::Idle);
                break;
            }
            BackendMessage::ErrorResponse(fields) => {
                panic!("session refused: {:?}", fields.message());
            }
            _ => {}
        }
    }
}

/// CVE-2021-23214 / CVE-2021-23222.
///
/// Everything the client sent in the same segment as the `SSLRequest` arrived
/// in the clear. Treating any of it as if it came from inside the tunnel lets
/// an active attacker inject a startup packet — or a whole query — that the
/// session then attributes to the peer that authenticated inside it.
///
/// `PostgreSQL` refuses the connection outright here rather than silently
/// dropping the bytes, because a conforming client cannot produce them: it has
/// to wait for the `'S'` before it knows whether to start a handshake.
#[tokio::test(flavor = "multi_thread")]
async fn bytes_pipelined_behind_an_ssl_request_are_refused_not_replayed() {
    let pg = start_postgres().await;
    let extra = format!(
        "\n[listen.tls]\ncertificateFile = \"{cert}\"\nkeyFile = \"{key}\"\n",
        cert = pg.certificates.cert_path().display(),
        key = pg.certificates.key_path().display(),
    );
    let proxy = start_proxy(&config_for(&pg, &extra)).await;

    let mut socket = TcpStream::connect(proxy.address()).await.unwrap();

    // One write, so the SSLRequest and the smuggled startup packet land in the
    // same segment and are read into the same buffer.
    let mut wire = BytesMut::new();
    encode_ssl_request(&mut wire);
    wire.extend_from_slice(&startup_packet("smuggled-identity"));
    socket.write_all(&wire).await.unwrap();

    let mut answer = [0u8; 1];
    socket.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer, b"S");

    let connector = tokio_rustls::TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(pg.certificates.root_store())
            .with_no_client_auth(),
    ));
    let smuggled = connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await;
    assert!(
        smuggled.is_err(),
        "a session must not be established over plaintext that was pipelined \
         behind the SSLRequest"
    );

    // The same listener still serves a client that waits for the answer.
    let mut clean = TcpStream::connect(proxy.address()).await.unwrap();
    let mut request = BytesMut::new();
    encode_ssl_request(&mut request);
    clean.write_all(&request).await.unwrap();
    let mut answer = [0u8; 1];
    clean.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer, b"S");

    let mut tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), clean)
        .await
        .expect("a well-behaved client must still get a tunnel");
    tls.write_all(&startup_packet("tenant")).await.unwrap();

    let mut buf = MessageBuffer::new();
    loop {
        match pgelastic_proxy::wire_io::read_backend_message(&mut tls, &mut buf)
            .await
            .unwrap()
        {
            BackendMessage::ReadyForQuery(status) => {
                assert_eq!(status, TransactionStatus::Idle);
                break;
            }
            BackendMessage::ErrorResponse(fields) => {
                panic!("session refused: {:?}", fields.message());
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_demanding_verify_full_succeeds_against_a_certificate_it_trusts() {
    let pg = start_postgres().await;
    let extra = format!(
        "\n[listen.tls]\ncertificateFile = \"{cert}\"\nkeyFile = \"{key}\"\n",
        cert = pg.certificates.cert_path().display(),
        key = pg.certificates.key_path().display(),
    );
    let proxy = start_proxy(&config_for(&pg, &extra)).await;

    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(pg.certificates.root_store())
            .with_no_client_auth(),
    );
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=localhost port={} user=tenant dbname={BACKEND_DATABASE} sslmode=require",
            proxy.port()
        ),
        tls,
    )
    .await
    .expect("a verify-full client must accept a certificate chaining to its CA");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    assert_eq!(
        client
            .query_one("SELECT 11 AS over_tls", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        11
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_does_not_trust_the_certificate_is_refused() {
    let pg = start_postgres().await;
    let extra = format!(
        "\n[listen.tls]\ncertificateFile = \"{cert}\"\nkeyFile = \"{key}\"\n",
        cert = pg.certificates.cert_path().display(),
        key = pg.certificates.key_path().display(),
    );
    let proxy = start_proxy(&config_for(&pg, &extra)).await;

    let unrelated = harness::Certificates::generate();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(unrelated.root_store())
            .with_no_client_auth(),
    );
    let error = tokio_postgres::connect(
        &format!(
            "host=localhost port={} user=tenant dbname={BACKEND_DATABASE} sslmode=require",
            proxy.port()
        ),
        tls,
    )
    .await
    .err()
    .expect("a certificate from an untrusted CA must not be accepted");
    assert!(error.to_string().contains("tls") || error.to_string().contains("error"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_direct_tls_client_that_negotiates_the_alpn_protocol_is_accepted() {
    let pg = start_postgres().await;
    let extra = format!(
        "\n[listen.tls]\ncertificateFile = \"{cert}\"\nkeyFile = \"{key}\"\n",
        cert = pg.certificates.cert_path().display(),
        key = pg.certificates.key_path().display(),
    );
    let proxy = start_proxy(&harness::config_for_listener(
        &pg,
        "requireTls = true",
        &extra,
    ))
    .await;

    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(pg.certificates.root_store())
        .with_no_client_auth();
    config.alpn_protocols = vec![pgelastic_wire::DIRECT_TLS_ALPN.to_vec()];

    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(config);
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=localhost port={} user=tenant dbname={BACKEND_DATABASE} \
             sslmode=require sslnegotiation=direct",
            proxy.port()
        ),
        tls,
    )
    .await
    .expect("direct TLS with the postgresql ALPN protocol must be accepted");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    assert_eq!(
        client
            .query_one("SELECT 13 AS direct", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        13
    );
}

/// Without mandatory ALPN a direct-TLS listener is an ALPACA-style
/// cross-protocol confusion target: nothing else distinguishes it from any
/// other TLS service on the same host.
#[tokio::test(flavor = "multi_thread")]
async fn a_direct_tls_client_without_the_alpn_protocol_is_refused() {
    let pg = start_postgres().await;
    let extra = format!(
        "\n[listen.tls]\ncertificateFile = \"{cert}\"\nkeyFile = \"{key}\"\n",
        cert = pg.certificates.cert_path().display(),
        key = pg.certificates.key_path().display(),
    );
    let proxy = start_proxy(&config_for(&pg, &extra)).await;

    let connector = tokio_rustls::TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(pg.certificates.root_store())
            .with_no_client_auth(),
    ));
    let socket = TcpStream::connect(proxy.address()).await.unwrap();
    let handshake = connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await;

    let refused = match handshake {
        Err(_) => true,
        Ok(mut tls) => {
            tls.write_all(&startup_packet("tenant")).await.unwrap();
            let mut sink = Vec::new();
            // A refused session is closed without a startup reply. Bounded,
            // because a build that accepted the connection would hold it open
            // and a hung test reports nothing.
            match tokio::time::timeout(Duration::from_secs(5), tls.read_to_end(&mut sink)).await {
                Ok(Err(_)) => true,
                Ok(Ok(_)) => sink.is_empty(),
                Err(_) => false,
            }
        }
    };
    assert!(
        refused,
        "a direct TLS connection with no ALPN protocol must not become a session"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn scram_succeeds_with_the_right_password() {
    let pg = start_postgres().await;
    let proxy = start_proxy(&config_for(&pg, &scram_users())).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=tenant password={TENANT_PASSWORD} \
             dbname={BACKEND_DATABASE}",
            proxy.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("the right password must authenticate");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    assert_eq!(
        client
            .query_one("SELECT 17 AS authenticated", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        17
    );
    assert!(
        proxy
            .metrics
            .render()
            .contains("side=\"client\",outcome=\"success\"} 1")
    );
}

/// One authentication attempt: its SQLSTATE, its message, and how long the
/// proxy took to reject it.
async fn attempt(port: u16, user: &str) -> (String, String, Duration) {
    let started = Instant::now();
    let error = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user={user} password=definitely-wrong \
             dbname={BACKEND_DATABASE}"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .err()
    .expect("authentication must fail");
    let elapsed = started.elapsed();
    let db = error.as_db_error().expect("a SQLSTATE-carrying error");
    (
        db.code().code().to_owned(),
        db.message().to_owned(),
        elapsed,
    )
}

/// The tenant-enumeration test.
///
/// An unknown user and a wrong password must be indistinguishable: same
/// SQLSTATE, same message text, and no timing difference an attacker could
/// separate. A proxy that fails fast for unknown users is a directory of every
/// tenant on the platform.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_user_fails_identically_to_a_wrong_password() {
    let pg = start_postgres().await;
    let proxy = start_proxy(&config_for(&pg, &scram_users())).await;

    let (wrong_code, wrong_message, _) = attempt(proxy.port(), "tenant").await;
    let (unknown_code, unknown_message, _) = attempt(proxy.port(), "no-such-tenant").await;

    assert_eq!(wrong_code, "28P01");
    assert_eq!(unknown_code, "28P01");
    assert_eq!(
        wrong_message.replace("tenant", "USER"),
        unknown_message.replace("no-such-tenant", "USER"),
        "the two failures must differ only in the username the client already knows"
    );

    // Timing: the medians must be within an order of magnitude of each other.
    // A verifier lookup that short-circuits for unknown users shows up here as
    // a difference of milliseconds against microseconds.
    let mut wrong = Vec::new();
    let mut unknown = Vec::new();
    for _ in 0..9 {
        wrong.push(attempt(proxy.port(), "tenant").await.2);
        unknown.push(attempt(proxy.port(), "no-such-tenant").await.2);
    }
    wrong.sort_unstable();
    unknown.sort_unstable();
    let (fast, slow) = if wrong[4] < unknown[4] {
        (wrong[4], unknown[4])
    } else {
        (unknown[4], wrong[4])
    };
    assert!(
        slow.as_secs_f64() < fast.as_secs_f64() * 3.0 + 0.002,
        "median timings differ too much to be indistinguishable: \
         wrong password {:?}, unknown user {:?}",
        wrong[4],
        unknown[4]
    );

    assert!(
        proxy
            .metrics
            .render()
            .contains("side=\"client\",outcome=\"success\"} 0"),
        "no attempt should have succeeded"
    );
}

/// Two logins, each bound to its own tenant, discriminated by database name -
/// the shape the operator renders for a pool fronting many tenants.
fn two_tenants() -> String {
    format!(
        "\n[routing]\ntenantDiscriminators = [\"DatabaseName\"]\n\
         \n[auth]\nscramIterations = 4096\n\n\
         [[auth.users]]\nname = \"alpha\"\ntenant = \"alpha\"\npassword = \"{TENANT_PASSWORD}\"\n\n\
         [[auth.users]]\nname = \"beta\"\ntenant = \"beta\"\npassword = \"{TENANT_PASSWORD}\"\n"
    )
}

/// One attempt with a password that is actually correct, so the only thing
/// under test is which tenant the client named.
async fn attempt_as(
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> Option<(String, String)> {
    let outcome = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user={user} password={password} dbname={database}"),
        tokio_postgres::NoTls,
    )
    .await;
    match outcome {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            assert_eq!(
                client
                    .query_one("SELECT 1 AS reached", &[])
                    .await
                    .unwrap()
                    .get::<_, i32>(0),
                1
            );
            None
        }
        Err(error) => {
            let db = error.as_db_error().expect("a SQLSTATE-carrying error");
            Some((db.code().code().to_owned(), db.message().to_owned()))
        }
    }
}

/// Holding a tenant's password must not be enough to reach another tenant.
///
/// Authenticating and choosing a tenant read different fields of the same
/// startup packet, so proving one says nothing about the other. Before these
/// were related, a client could authenticate as `alpha` and name `beta`'s
/// database in the same packet and be routed into it - the whole of the claim
/// that a tenant's identity is confined to its own database, false.
///
/// The refusal is asserted to be *indistinguishable from a wrong password*,
/// because a distinct error would answer "does this tenant exist, and is this
/// login one of its own" for anyone who asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_login_cannot_reach_a_tenant_it_is_not_bound_to() {
    let pg = start_postgres().await;
    let proxy = start_proxy(&config_for(&pg, &two_tenants())).await;

    assert!(
        attempt_as(proxy.port(), "alpha", TENANT_PASSWORD, "alpha")
            .await
            .is_none(),
        "a login must still reach its own tenant"
    );

    let crossed = attempt_as(proxy.port(), "alpha", TENANT_PASSWORD, "beta")
        .await
        .expect("alpha must not reach beta merely by naming it");
    let wrong = attempt_as(proxy.port(), "alpha", "definitely-wrong", "alpha")
        .await
        .expect("a wrong password must fail");

    assert_eq!(crossed.0, "28P01");
    assert_eq!(
        crossed, wrong,
        "reaching for another tenant must be indistinguishable from getting the password \
         wrong, or the error is an oracle for which tenants exist and who belongs to them"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_backend_leg_runs_over_tls_when_configured() {
    let pg = start_postgres().await;
    let extra = format!(
        "\n[backend.tls]\nmode = \"VerifyFull\"\ncaFile = \"{ca}\"\nserverName = \"localhost\"\n",
        ca = pg.certificates.ca_path().display(),
    );
    let proxy = start_proxy(&config_for(&pg, &extra)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=tenant dbname={BACKEND_DATABASE}",
            proxy.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("a TLS backend leg must still serve a plaintext client");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // pg_stat_ssl reports the backend's own view of its connection, which is
    // the proxy's socket, so this asserts the far leg and not the near one.
    let row = client
        .query_one(
            "SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.get::<_, bool>("ssl"),
        "the backend leg must be encrypted"
    );
    assert!(row.get::<_, String>("version").starts_with("TLSv1."));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backend_leg_verifying_the_wrong_name_refuses_to_connect() {
    let pg = start_postgres().await;
    let extra = format!(
        "\n[backend.tls]\nmode = \"VerifyFull\"\ncaFile = \"{ca}\"\n\
         serverName = \"not-the-certificate-name.example.com\"\n",
        ca = pg.certificates.ca_path().display(),
    );
    let proxy = start_proxy(&config_for(&pg, &extra)).await;

    let error = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=tenant dbname={BACKEND_DATABASE}",
            proxy.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .err()
    .expect("a name that does not match the certificate must not connect");
    assert!(error.as_db_error().is_some() || error.to_string().contains("closed"));
}

#[tokio::test(flavor = "multi_thread")]
async fn both_legs_can_be_encrypted_at_once() {
    let pg = start_postgres().await;
    let extra = format!(
        "\n[listen.tls]\ncertificateFile = \"{cert}\"\nkeyFile = \"{key}\"\n\
         \n[backend.tls]\nmode = \"VerifyFull\"\ncaFile = \"{ca}\"\nserverName = \"localhost\"\n",
        cert = pg.certificates.cert_path().display(),
        key = pg.certificates.key_path().display(),
        ca = pg.certificates.ca_path().display(),
    );
    let proxy = start_proxy(&config_for(&pg, &extra)).await;

    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(pg.certificates.root_store())
            .with_no_client_auth(),
    );
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=localhost port={} user=tenant dbname={BACKEND_DATABASE} sslmode=require",
            proxy.port()
        ),
        tls,
    )
    .await
    .expect("both legs encrypted must still connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    assert!(
        client
            .query_one(
                "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
                &[]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
}
