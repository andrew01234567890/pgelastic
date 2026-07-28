//! The client leg: pre-startup negotiation and client authentication.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use bytes::Bytes;
use pgelastic_wire::{
    Authentication, BackendMessage, CancelRequest, DIRECT_TLS_ALPN, FrontendMessage, MessageBuffer,
    NegotiateProtocolVersion, PreStartup, PreStartupMachine, ProtocolVersion, StartupMessage,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use crate::error::{ProxyError, Result, sqlstate};
use crate::scram::{MECHANISM, MockSecret, ScramOutcome, ScramServer, ScramVerifier};
use crate::stream::{ClientStream, MaybeTls, Prefixed};
use crate::wire_io::{read_frontend_message, read_pre_startup, write_backend};

/// What a client turned out to be.
#[derive(Debug)]
pub enum Accepted {
    Session(Box<ClientSession>),
    /// A `CancelRequest` arrives on its own connection and carries no startup
    /// packet, so it never becomes a session.
    Cancel(CancelRequest),
}

/// A client that has sent a startup packet and settled its encryption.
#[derive(Debug)]
pub struct ClientSession {
    pub stream: ClientStream,
    pub buf: MessageBuffer,
    pub startup: StartupMessage,
}

impl ClientSession {
    /// The role the client asked for. Required: `PostgreSQL` itself refuses a
    /// startup packet without one.
    pub fn user(&self) -> Result<&Bytes> {
        self.startup
            .get(b"user")
            .ok_or_else(|| ProxyError::client("startup packet carries no user parameter"))
    }
}

/// Everything the client leg needs to authenticate a peer.
#[derive(Debug)]
pub struct ClientAuth {
    verifiers: HashMap<Vec<u8>, ScramVerifier>,
    mock: MockSecret,
    iterations: NonZeroU32,
}

impl ClientAuth {
    pub fn new(verifiers: HashMap<Vec<u8>, ScramVerifier>, iterations: NonZeroU32) -> Result<Self> {
        Ok(Self {
            verifiers,
            mock: MockSecret::generate()?,
            iterations,
        })
    }

    /// True when no verifiers are configured at all, in which case every client
    /// is admitted without a challenge.
    ///
    /// A development affordance, and the reason `listen.requireTls` exists: it
    /// is the only thing standing between an open listener and the backend.
    pub fn is_trust(&self) -> bool {
        self.verifiers.is_empty()
    }

    /// The verifier to run the exchange against.
    ///
    /// An unknown user gets a mock verifier rather than an early return. The
    /// exchange then fails at exactly the same point, after exactly the same
    /// work, with exactly the same message as a wrong password, so the proxy
    /// cannot be used to enumerate tenants.
    fn verifier_for(&self, user: &[u8]) -> ScramVerifier {
        self.verifiers
            .get(user)
            .cloned()
            .unwrap_or_else(|| self.mock.verifier_for(user, self.iterations))
    }
}

/// Settles encryption and reads the startup packet.
pub async fn negotiate(
    socket: TcpStream,
    acceptor: Option<&TlsAcceptor>,
    require_tls: bool,
) -> Result<Accepted> {
    socket.set_nodelay(true)?;
    let mut stream: ClientStream = MaybeTls::Plain(Prefixed::new(Bytes::new(), socket));
    let mut buf = MessageBuffer::new();
    let mut machine = PreStartupMachine::new();

    loop {
        match read_pre_startup(&mut stream, &mut buf, &mut machine).await? {
            PreStartup::GssEncRequest => {
                // A single 'N'. The proxy has no GSSAPI credential, and the
                // client is expected to fall through to SSLRequest.
                stream.write_all(b"N").await?;
                stream.flush().await?;
            }
            PreStartup::SslRequest => {
                let Some(acceptor) = acceptor else {
                    stream.write_all(b"N").await?;
                    stream.flush().await?;
                    continue;
                };
                stream.write_all(b"S").await?;
                stream.flush().await?;

                // CVE-2021-23214 / CVE-2021-23222. Anything the client
                // pipelined behind the SSLRequest arrived in the clear, before
                // the tunnel existed, and must never be attributed to the peer
                // that later authenticates inside it.
                //
                // Discarding alone is not enough, which is why this rejects as
                // well: bytes the read did not reach are still queued in the
                // socket and would be consumed by rustls as a ClientHello
                // prefix. A conforming client cannot produce this — it has to
                // wait for the 'S' before it knows whether to start a
                // handshake — so anything buffered here is either a broken
                // client or an injection attempt. `PostgreSQL` itself refuses
                // the connection at exactly this point
                // (`pq_buffer_remaining_data`, "received unencrypted data
                // after SSL request").
                let smuggled = !buf.is_empty();
                buf.discard_all();
                if smuggled {
                    return Err(ProxyError::client(
                        "received unencrypted data after SSLRequest",
                    ));
                }

                let MaybeTls::Plain(plain) = stream else {
                    return Err(ProxyError::client(
                        "SSLRequest inside an established tunnel",
                    ));
                };
                let accepted = acceptor.accept(plain).await?;
                stream = MaybeTls::Tls(Box::new(accepted));
                machine = PreStartupMachine::after_tls();
            }
            PreStartup::DirectTls => {
                let Some(acceptor) = acceptor else {
                    return Err(ProxyError::client(
                        "a direct TLS connection arrived but no server certificate is configured",
                    ));
                };
                let MaybeTls::Plain(plain) = stream else {
                    return Err(ProxyError::client("a second TLS handshake inside a tunnel"));
                };
                // The ClientHello belongs to rustls, so it is replayed rather
                // than consumed; the buffer is then emptied so nothing read
                // before the tunnel can leak into it.
                let pending = Bytes::copy_from_slice(buf.as_slice());
                buf.discard_all();
                let accepted = acceptor
                    .accept(Prefixed::new(pending, plain.into_inner()))
                    .await?;

                // Mandatory, not advisory: without ALPN a direct-TLS listener
                // is an ALPACA-style cross-protocol confusion target, because
                // nothing else distinguishes it from any other TLS service on
                // the host.
                if accepted.get_ref().1.alpn_protocol() != Some(DIRECT_TLS_ALPN) {
                    return Err(ProxyError::client(
                        "a direct TLS connection must negotiate the postgresql ALPN protocol",
                    ));
                }
                stream = MaybeTls::Tls(Box::new(accepted));
                machine = PreStartupMachine::after_tls();
            }
            PreStartup::CancelRequest(request) => return Ok(Accepted::Cancel(request)),
            PreStartup::Startup(startup) => {
                if require_tls && !stream.is_tls() {
                    return Err(ProxyError::client("this listener requires TLS"));
                }
                return Ok(Accepted::Session(Box::new(ClientSession {
                    stream,
                    buf,
                    startup,
                })));
            }
        }
    }
}

/// Answers a client that asked for a protocol version or extensions the proxy
/// does not implement.
///
/// Erroring on an unrecognized `_pq_.` option instead of echoing it back is the
/// bug that permanently burned protocol 3.1, so every one the client named is
/// listed verbatim.
pub async fn negotiate_protocol_version(session: &mut ClientSession) -> Result<()> {
    let negotiate = NegotiateProtocolVersion::for_startup(
        &session.startup,
        u32::from(ProtocolVersion::V3_0.minor()),
        &[],
    );
    if session.startup.version == ProtocolVersion::V3_0 && negotiate.unrecognized_options.is_empty()
    {
        return Ok(());
    }
    write_backend(
        &mut session.stream,
        &[BackendMessage::NegotiateProtocolVersion(negotiate)],
    )
    .await
}

/// Runs SCRAM against the client, or admits it when no verifiers are set.
///
/// Returns without sending `AuthenticationOk`: the caller sends that only once
/// the backend connection is established, so a backend failure reaches the
/// client as an error rather than as a session that dies immediately after
/// being told it succeeded.
pub async fn authenticate_client(
    session: &mut ClientSession,
    auth: &Arc<ClientAuth>,
) -> Result<()> {
    if auth.is_trust() {
        return Ok(());
    }
    let user = session.user()?.clone();
    let mut server = ScramServer::new(
        auth.verifier_for(&user),
        crate::scram::crypto::random_nonce()?,
    );

    write_backend(
        &mut session.stream,
        &[BackendMessage::Authentication(Authentication::Sasl {
            mechanisms: vec![Bytes::from_static(MECHANISM.as_bytes())],
        })],
    )
    .await?;

    let initial = match read_frontend_message(
        &mut session.stream,
        &mut session.buf,
        pgelastic_wire::AuthState::SaslInitial,
    )
    .await?
    {
        FrontendMessage::SaslInitialResponse(response) => response,
        other => {
            return Err(ProxyError::client(format!(
                "expected a SASLInitialResponse, got tag {}",
                other.tag() as char
            )));
        }
    };
    if initial.mechanism.as_ref() != MECHANISM.as_bytes() {
        // Includes a client that insisted on SCRAM-SHA-256-PLUS: refused
        // outright rather than silently downgraded.
        return Err(ProxyError::AuthenticationFailed);
    }
    let client_first = initial
        .initial_response
        .ok_or(ProxyError::AuthenticationFailed)?;

    let server_first = server.server_first(&client_first)?;
    write_backend(
        &mut session.stream,
        &[BackendMessage::Authentication(
            Authentication::SaslContinue(Bytes::from(server_first.into_bytes())),
        )],
    )
    .await?;

    let client_final = match read_frontend_message(
        &mut session.stream,
        &mut session.buf,
        pgelastic_wire::AuthState::SaslContinue,
    )
    .await?
    {
        FrontendMessage::SaslResponse(data) => data,
        other => {
            return Err(ProxyError::client(format!(
                "expected a SASLResponse, got tag {}",
                other.tag() as char
            )));
        }
    };

    match server.finish(&client_final)? {
        ScramOutcome::Verified(server_final) => {
            write_backend(
                &mut session.stream,
                &[BackendMessage::Authentication(Authentication::SaslFinal(
                    Bytes::from(server_final.into_bytes()),
                ))],
            )
            .await?;
            Ok(())
        }
        ScramOutcome::Rejected => Err(ProxyError::AuthenticationFailed),
    }
}

/// The message a failed authentication produces.
///
/// Byte-for-byte identical whether the user exists or not, and identical to
/// what `PostgreSQL` itself sends, so neither the wire nor a diffing client can
/// tell the two apart.
pub fn authentication_failed_message(user: &[u8]) -> String {
    format!(
        "password authentication failed for user \"{}\"",
        String::from_utf8_lossy(user)
    )
}

/// Reports a handshake failure to a client that can still receive one.
pub async fn report(stream: &mut ClientStream, user: Option<&Bytes>, error: &ProxyError) {
    let message = match error {
        ProxyError::AuthenticationFailed => {
            authentication_failed_message(user.map_or(&b""[..], |u| u.as_ref()))
        }
        ProxyError::ConnectionLimit => "too many clients already".to_owned(),
        ProxyError::ShuttingDown => "the proxy is shutting down".to_owned(),
        other => other.to_string(),
    };
    let code = if matches!(error, ProxyError::BackendRejected(_)) {
        sqlstate::INVALID_AUTHORIZATION
    } else {
        error.sqlstate()
    };
    crate::wire_io::send_fatal(stream, code, &message).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scram::client::ScramClient;
    use crate::scram::crypto::salted_password_blocking;
    use bytes::BytesMut;
    use pgelastic_wire::startup::{encode_gssenc_request, encode_ssl_request};
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn listener() -> (TcpListener, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    fn startup_bytes(user: &str) -> BytesMut {
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
                    Bytes::from_static(b"postgres"),
                ),
            ],
        )
        .encode(&mut wire);
        wire
    }

    #[tokio::test]
    async fn a_gssenc_request_is_answered_with_a_single_n_byte() {
        let (listener, addr) = listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            negotiate(socket, None, false).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut wire = BytesMut::new();
        encode_gssenc_request(&mut wire);
        client.write_all(&wire).await.unwrap();

        let mut answer = [0u8; 1];
        client.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"N");

        client.write_all(&startup_bytes("alice")).await.unwrap();
        let Accepted::Session(session) = server.await.unwrap().unwrap() else {
            panic!("expected a session");
        };
        assert_eq!(session.user().unwrap(), "alice");
    }

    #[tokio::test]
    async fn an_ssl_request_without_a_certificate_is_answered_with_n() {
        let (listener, addr) = listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            negotiate(socket, None, false).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut wire = BytesMut::new();
        encode_ssl_request(&mut wire);
        client.write_all(&wire).await.unwrap();

        let mut answer = [0u8; 1];
        client.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"N");

        client.write_all(&startup_bytes("bob")).await.unwrap();
        assert!(matches!(
            server.await.unwrap().unwrap(),
            Accepted::Session(_)
        ));
    }

    /// A `TlsAcceptor` over a throwaway certificate, so the `SSLRequest` path
    /// can be exercised without a fixture on disk.
    fn test_acceptor() -> (tempfile::TempDir, tokio_rustls::TlsAcceptor) {
        crate::tls::install_crypto_provider();
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        let certificate = params.self_signed(&key).unwrap();

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("cert.pem"), certificate.pem()).unwrap();
        std::fs::write(dir.path().join("key.pem"), key.serialize_pem()).unwrap();
        let acceptor = crate::tls::server_acceptor(&crate::config::ServerTlsConfig {
            certificate_file: dir.path().join("cert.pem"),
            key_file: dir.path().join("key.pem"),
        })
        .unwrap();
        (dir, acceptor)
    }

    /// CVE-2021-23214 / CVE-2021-23222, pinned at the point of decision.
    ///
    /// The bytes are delivered in the same write as the `SSLRequest`, so they
    /// land in the pre-startup buffer. Neither replaying them nor silently
    /// dropping them is acceptable: the connection is refused, and the refusal
    /// happens before rustls is ever handed the socket.
    #[tokio::test]
    async fn plaintext_pipelined_behind_an_ssl_request_refuses_the_connection() {
        let (_dir, acceptor) = test_acceptor();
        let (listener, addr) = listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            negotiate(socket, Some(&acceptor), false).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut wire = BytesMut::new();
        encode_ssl_request(&mut wire);
        wire.extend_from_slice(&startup_bytes("smuggled-identity"));
        client.write_all(&wire).await.unwrap();

        let mut answer = [0u8; 1];
        client.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"S");

        // Bounded: a build that does not refuse would sit waiting for a
        // `ClientHello` that this client never sends, and a hung test reports
        // nothing.
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("the connection must be refused rather than left waiting")
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(&error, ProxyError::ClientProtocol(message)
                if message.contains("unencrypted data after SSLRequest")),
            "expected a refusal naming the smuggled plaintext, got {error}"
        );
    }

    /// The same listener must still serve a client that waits for the answer,
    /// which is the only sequence a conforming client can produce.
    #[tokio::test]
    async fn an_ssl_request_on_its_own_upgrades_the_connection() {
        let (_dir, acceptor) = test_acceptor();
        let (listener, addr) = listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            negotiate(socket, Some(&acceptor), true).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut wire = BytesMut::new();
        encode_ssl_request(&mut wire);
        client.write_all(&wire).await.unwrap();
        let mut answer = [0u8; 1];
        client.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"S");

        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(AcceptAnything))
                .with_no_client_auth(),
        ));
        let mut tls = connector
            .connect(
                rustls_pki_types::ServerName::try_from("localhost").unwrap(),
                client,
            )
            .await
            .unwrap();
        tls.write_all(&startup_bytes("tenant")).await.unwrap();

        let Accepted::Session(session) = server.await.unwrap().unwrap() else {
            panic!("expected a session");
        };
        assert_eq!(session.user().unwrap(), "tenant");
        assert!(session.stream.is_tls());
    }

    #[derive(Debug)]
    struct AcceptAnything;

    impl rustls::client::danger::ServerCertVerifier for AcceptAnything {
        fn verify_server_cert(
            &self,
            _: &rustls_pki_types::CertificateDer<'_>,
            _: &[rustls_pki_types::CertificateDer<'_>],
            _: &rustls_pki_types::ServerName<'_>,
            _: &[u8],
            _: rustls_pki_types::UnixTime,
        ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error>
        {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &rustls_pki_types::CertificateDer<'_>,
            _: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
        {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &rustls_pki_types::CertificateDer<'_>,
            _: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
        {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::aws_lc_rs::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    #[tokio::test]
    async fn a_cancel_request_never_becomes_a_session() {
        let (listener, addr) = listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            negotiate(socket, None, false).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut wire = BytesMut::new();
        CancelRequest {
            process_id: 99,
            key: pgelastic_wire::CancelKey::new(Bytes::from_static(b"abcd")).unwrap(),
        }
        .encode(&mut wire);
        client.write_all(&wire).await.unwrap();

        let Accepted::Cancel(request) = server.await.unwrap().unwrap() else {
            panic!("expected a cancel request");
        };
        assert_eq!(request.process_id, 99);
    }

    #[tokio::test]
    async fn a_plaintext_client_is_refused_when_the_listener_requires_tls() {
        let (listener, addr) = listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            negotiate(socket, None, true).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("carol")).await.unwrap();
        assert!(matches!(
            server.await.unwrap(),
            Err(ProxyError::ClientProtocol(_))
        ));
    }

    fn auth_with(users: &[(&str, &str)]) -> Arc<ClientAuth> {
        let verifiers = users
            .iter()
            .map(|(name, password)| {
                (
                    name.as_bytes().to_vec(),
                    ScramVerifier::generate(password).unwrap(),
                )
            })
            .collect();
        Arc::new(ClientAuth::new(verifiers, NonZeroU32::new(4096).unwrap()).unwrap())
    }

    /// Everything a client can observe about one authentication attempt.
    ///
    /// The verdict alone proves nothing about enumeration: what an attacker
    /// reads is the sequence of messages on the wire and the fields of the
    /// error that ends it.
    #[derive(Debug, PartialEq, Eq)]
    struct Observed {
        tags: Vec<u8>,
        error: Option<Vec<(u8, Vec<u8>)>>,
    }

    /// Drives a whole SCRAM exchange over a real socket, recording what the
    /// client saw and how long it took.
    async fn scram_attempt(
        auth: &Arc<ClientAuth>,
        user: &str,
        password: &str,
    ) -> (Result<()>, Observed, std::time::Duration) {
        let (listener, addr) = listener().await;
        let auth = Arc::clone(auth);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let Accepted::Session(mut session) = negotiate(socket, None, false).await.unwrap()
            else {
                panic!("expected a session");
            };
            let user = session.user().unwrap().clone();
            let verdict = authenticate_client(&mut session, &auth).await;
            match &verdict {
                Err(error) => report(&mut session.stream, Some(&user), error).await,
                Ok(()) => {
                    let _ = write_backend(
                        &mut session.stream,
                        &[BackendMessage::Authentication(Authentication::Ok)],
                    )
                    .await;
                }
            }
            let _ = tokio::io::AsyncWriteExt::shutdown(&mut session.stream).await;
            verdict
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes(user)).await.unwrap();

        let started = Instant::now();
        let observed = run_client_scram(&mut client, password).await;
        let elapsed = started.elapsed();
        let verdict = server.await.unwrap();
        (verdict, observed, elapsed)
    }

    /// The client half: answers whatever the proxy asks for and records the
    /// whole trace, including a refusal that arrives before any challenge.
    async fn run_client_scram(socket: &mut TcpStream, password: &str) -> Observed {
        use crate::wire_io::{read_backend_message, write_frontend};

        let mut buf = MessageBuffer::new();
        let mut scram = ScramClient::new("client-nonce-for-the-test".to_owned());
        let mut tags = Vec::new();
        let mut error = None;

        while let Ok(message) = read_backend_message(socket, &mut buf).await {
            tags.push(message.tag());
            match message {
                BackendMessage::Authentication(Authentication::Sasl { .. }) => {
                    let first = scram.client_first();
                    write_frontend(
                        socket,
                        &[FrontendMessage::SaslInitialResponse(
                            pgelastic_wire::SaslInitialResponse {
                                mechanism: Bytes::from_static(MECHANISM.as_bytes()),
                                initial_response: Some(Bytes::from(first.into_bytes())),
                            },
                        )],
                    )
                    .await
                    .unwrap();
                }
                BackendMessage::Authentication(Authentication::SaslContinue(server_first)) => {
                    let parsed = ScramClient::parse_server_first(&server_first).unwrap();
                    let salted =
                        salted_password_blocking(password, &parsed.salt, parsed.iterations);
                    let final_message = scram.client_final(&server_first, &salted).unwrap();
                    write_frontend(
                        socket,
                        &[FrontendMessage::SaslResponse(Bytes::from(
                            final_message.into_bytes(),
                        ))],
                    )
                    .await
                    .unwrap();
                }
                BackendMessage::ErrorResponse(fields) => {
                    error = Some(
                        fields
                            .as_slice()
                            .iter()
                            .map(|(kind, value)| (*kind, value.to_vec()))
                            .collect(),
                    );
                }
                _ => {}
            }
        }
        Observed { tags, error }
    }

    #[tokio::test]
    async fn the_right_password_authenticates() {
        let auth = auth_with(&[("alice", "hunter2")]);
        let (verdict, observed, _) = scram_attempt(&auth, "alice", "hunter2").await;
        verdict.unwrap();
        assert_eq!(observed.tags, vec![b'R', b'R', b'R', b'R']);
        assert!(observed.error.is_none());
    }

    #[tokio::test]
    async fn the_wrong_password_is_refused() {
        let auth = auth_with(&[("alice", "hunter2")]);
        let (verdict, _, _) = scram_attempt(&auth, "alice", "hunter3").await;
        assert!(matches!(verdict, Err(ProxyError::AuthenticationFailed)));
    }

    /// The tenant-enumeration property, asserted on what the wire carries.
    ///
    /// A proxy that answers an unknown user early — even with the same error —
    /// is a directory of every tenant on the platform, because the *shape* of
    /// the exchange differs. So the whole trace has to match: the same
    /// challenges, in the same order, ending in the same error fields.
    #[tokio::test]
    async fn an_unknown_user_is_indistinguishable_from_a_wrong_password_on_the_wire() {
        let auth = auth_with(&[("alice", "hunter2")]);
        let (known, known_trace, _) = scram_attempt(&auth, "alice", "hunter3").await;
        let (unknown, unknown_trace, _) = scram_attempt(&auth, "mallory", "hunter3").await;

        assert!(matches!(known, Err(ProxyError::AuthenticationFailed)));
        assert!(matches!(unknown, Err(ProxyError::AuthenticationFailed)));

        assert_eq!(
            known_trace.tags, unknown_trace.tags,
            "the two exchanges must have the same shape"
        );
        assert_eq!(
            known_trace.tags,
            vec![b'R', b'R', b'E'],
            "both must be challenged before being refused"
        );

        // The only field allowed to differ is the username the client already
        // knows, because that is what PostgreSQL itself echoes.
        let redact = |trace: &Observed, user: &str| {
            trace
                .error
                .clone()
                .expect("a refusal must carry an ErrorResponse")
                .into_iter()
                .map(|(kind, value)| {
                    (
                        kind,
                        String::from_utf8_lossy(&value).replace(user, "<user>"),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            redact(&known_trace, "alice"),
            redact(&unknown_trace, "mallory")
        );
    }

    #[test]
    fn the_failure_message_does_not_reveal_whether_the_user_exists() {
        assert_eq!(
            authentication_failed_message(b"alice"),
            authentication_failed_message(b"alice")
        );
        assert!(!authentication_failed_message(b"alice").contains("exist"));
    }

    #[tokio::test]
    async fn an_unknown_user_gets_a_verifier_shaped_like_a_real_one() {
        let auth = auth_with(&[("alice", "hunter2")]);
        let real = auth.verifier_for(b"alice");
        let mock = auth.verifier_for(b"mallory");
        assert_eq!(real.iterations, mock.iterations);
        assert_eq!(real.salt.len(), mock.salt.len());
        assert_eq!(auth.verifier_for(b"mallory").salt, mock.salt);
    }

    #[tokio::test]
    async fn a_client_demanding_channel_binding_is_refused_not_downgraded() {
        let auth = auth_with(&[("alice", "hunter2")]);
        let (listener, addr) = listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let Accepted::Session(mut session) = negotiate(socket, None, false).await.unwrap()
            else {
                panic!("expected a session");
            };
            authenticate_client(&mut session, &auth).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&startup_bytes("alice")).await.unwrap();
        let mut buf = MessageBuffer::new();
        let _ = crate::wire_io::read_backend_message(&mut client, &mut buf).await;
        crate::wire_io::write_frontend(
            &mut client,
            &[FrontendMessage::SaslInitialResponse(
                pgelastic_wire::SaslInitialResponse {
                    mechanism: Bytes::from_static(b"SCRAM-SHA-256-PLUS"),
                    initial_response: Some(Bytes::from_static(b"p=tls-server-end-point,,n=,r=x")),
                },
            )],
        )
        .await
        .unwrap();

        assert!(matches!(
            server.await.unwrap(),
            Err(ProxyError::AuthenticationFailed)
        ));
    }

    #[test]
    fn a_trust_configuration_is_recognised_as_such() {
        let auth = ClientAuth::new(HashMap::new(), NonZeroU32::new(4096).unwrap()).unwrap();
        assert!(auth.is_trust());
        assert!(!auth_with(&[("alice", "x")]).is_trust());
    }
}
