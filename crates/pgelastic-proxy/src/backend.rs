//! The backend leg: opening a socket to `PostgreSQL` and logging in.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use pgelastic_wire::startup::encode_ssl_request;
use pgelastic_wire::{
    Authentication, BackendKeyData, BackendMessage, FrontendMessage, MessageBuffer,
    ProtocolVersion, SaslInitialResponse, StartupMessage, TransactionStatus,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use zeroize::Zeroizing;

use crate::config::BackendConfig;
use crate::error::{ProxyError, Result};
use crate::scram::{KdfPool, MECHANISM, ScramClient};
use crate::stream::{BackendStream, MaybeTls};
use crate::tls::BackendTls;
use crate::wire_io::{read_backend_message, write_frontend};

/// Opens a TCP connection, upgrading it to TLS when the backend is configured
/// for it.
///
/// Shared with the cancel path, which needs the same socket but sends a
/// `CancelRequest` rather than a startup packet.
pub async fn connect_socket(
    address: &str,
    tls: Option<&BackendTls>,
    connect_timeout: Duration,
) -> Result<BackendStream> {
    let addr = crate::config::resolve(address)?;
    let socket = tokio::time::timeout(connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| ProxyError::Timeout(connect_timeout))??;
    socket.set_nodelay(true)?;

    let Some(tls) = tls else {
        return Ok(MaybeTls::Plain(socket));
    };

    let mut socket = socket;
    let mut wire = BytesMut::new();
    encode_ssl_request(&mut wire);
    socket.write_all(&wire).await?;
    socket.flush().await?;

    let mut answer = [0u8; 1];
    socket.read_exact(&mut answer).await?;
    match answer[0] {
        b'S' => {}
        // Downgrading here would silently turn `require` into `disable`, which
        // is the whole failure mode TLS on this leg exists to prevent.
        _ => {
            return Err(ProxyError::BackendRejected(
                "backend refused the TLS upgrade".to_owned(),
            ));
        }
    }

    let stream = tls
        .connector
        .connect(tls.server_name.clone(), socket)
        .await?;
    Ok(MaybeTls::Tls(Box::new(stream)))
}

/// A backend connection that has reached its first `ReadyForQuery`.
#[derive(Debug)]
pub struct BackendSession {
    pub stream: BackendStream,
    pub buf: MessageBuffer,
    /// The backend's real cancel key. Never handed to a client.
    pub key_data: Option<BackendKeyData>,
    /// `ParameterStatus` messages the client still has to see.
    pub parameters: Vec<BackendMessage>,
}

/// Builds the startup parameter list sent to the backend.
///
/// The client's list is forwarded verbatim except for the identity fields: the
/// proxy logs in under its own role, and `_pq_.` protocol extensions are
/// dropped because the proxy speaks 3.0 to the backend and answers the client's
/// extension request itself with `NegotiateProtocolVersion`.
fn backend_parameters(config: &BackendConfig, client: &StartupMessage) -> Vec<(Bytes, Bytes)> {
    let database = config
        .database
        .as_deref()
        .map(|db| Bytes::copy_from_slice(db.as_bytes()))
        .or_else(|| client.get(b"database").cloned())
        .unwrap_or_else(|| Bytes::copy_from_slice(config.user.as_bytes()));

    let mut parameters = vec![
        (
            Bytes::from_static(b"user"),
            Bytes::copy_from_slice(config.user.as_bytes()),
        ),
        (Bytes::from_static(b"database"), database),
    ];
    for (key, value) in &client.parameters {
        if key.as_ref() == b"user"
            || key.as_ref() == b"database"
            || key.starts_with(StartupMessage::EXTENSION_PREFIX)
        {
            continue;
        }
        parameters.push((key.clone(), value.clone()));
    }
    parameters
}

/// Connects and authenticates, returning once the backend is idle and ready.
pub async fn connect(
    config: &BackendConfig,
    tls: Option<&BackendTls>,
    kdf: &KdfPool,
    client: &StartupMessage,
) -> Result<BackendSession> {
    let mut stream = connect_socket(&config.address, tls, config.connect_timeout()).await?;
    let mut buf = MessageBuffer::new();

    let mut wire = BytesMut::new();
    StartupMessage::new(ProtocolVersion::V3_0, backend_parameters(config, client))
        .encode(&mut wire);
    stream.write_all(&wire).await?;
    stream.flush().await?;

    authenticate(&mut stream, &mut buf, config, kdf).await?;

    let mut key_data = None;
    let mut parameters = Vec::new();
    loop {
        match read_backend_message(&mut stream, &mut buf).await? {
            BackendMessage::ReadyForQuery(TransactionStatus::Idle) => break,
            BackendMessage::ReadyForQuery(status) => {
                return Err(ProxyError::backend(format!(
                    "a fresh backend reported transaction status {status:?}"
                )));
            }
            BackendMessage::BackendKeyData(data) => key_data = Some(data),
            message @ BackendMessage::ParameterStatus(_) => parameters.push(message),
            BackendMessage::NoticeResponse(_) => {}
            BackendMessage::ErrorResponse(fields) => {
                return Err(ProxyError::BackendRejected(describe(&fields)));
            }
            other => {
                return Err(ProxyError::backend(format!(
                    "unexpected message with tag {} during start-up",
                    other.tag() as char
                )));
            }
        }
    }

    Ok(BackendSession {
        stream,
        buf,
        key_data,
        parameters,
    })
}

async fn authenticate(
    stream: &mut BackendStream,
    buf: &mut MessageBuffer,
    config: &BackendConfig,
    kdf: &KdfPool,
) -> Result<()> {
    loop {
        match read_backend_message(stream, buf).await? {
            BackendMessage::Authentication(Authentication::Ok) => return Ok(()),
            BackendMessage::Authentication(Authentication::Sasl { mechanisms }) => {
                if !mechanisms
                    .iter()
                    .any(|m| m.as_ref() == MECHANISM.as_bytes())
                {
                    return Err(ProxyError::BackendRejected(
                        "backend offered no SCRAM-SHA-256 mechanism".to_owned(),
                    ));
                }
                return sasl(stream, buf, config, kdf).await;
            }
            BackendMessage::Authentication(Authentication::CleartextPassword) => {
                let password = password(config)?;
                let mut secret = password.as_bytes().to_vec();
                secret.push(0);
                write_frontend(
                    stream,
                    &[FrontendMessage::PasswordMessage(Bytes::from(secret))],
                )
                .await?;
            }
            BackendMessage::Authentication(other) => {
                // md5 is broken and gone from PG18 defaults; GSSAPI and SSPI
                // would need a credential the proxy has no way to hold.
                return Err(ProxyError::BackendRejected(format!(
                    "backend requested an unsupported authentication method: {other:?}"
                )));
            }
            BackendMessage::ErrorResponse(fields) => {
                return Err(ProxyError::BackendRejected(describe(&fields)));
            }
            BackendMessage::NoticeResponse(_) | BackendMessage::ParameterStatus(_) => {}
            other => {
                return Err(ProxyError::backend(format!(
                    "unexpected message with tag {} during authentication",
                    other.tag() as char
                )));
            }
        }
    }
}

async fn sasl(
    stream: &mut BackendStream,
    buf: &mut MessageBuffer,
    config: &BackendConfig,
    kdf: &KdfPool,
) -> Result<()> {
    let password = password(config)?;
    let mut client = ScramClient::new(crate::scram::crypto::random_nonce()?);

    let first = client.client_first();
    write_frontend(
        stream,
        &[FrontendMessage::SaslInitialResponse(SaslInitialResponse {
            mechanism: Bytes::from_static(MECHANISM.as_bytes()),
            initial_response: Some(Bytes::from(first.into_bytes())),
        })],
    )
    .await?;

    let server_first = match read_backend_message(stream, buf).await? {
        BackendMessage::Authentication(Authentication::SaslContinue(data)) => data,
        BackendMessage::ErrorResponse(fields) => {
            return Err(ProxyError::BackendRejected(describe(&fields)));
        }
        other => {
            return Err(ProxyError::backend(format!(
                "expected AuthenticationSASLContinue, got tag {}",
                other.tag() as char
            )));
        }
    };

    let parsed = ScramClient::parse_server_first(&server_first)?;
    let salted = kdf
        .salted_password(password, parsed.salt.clone(), parsed.iterations)
        .await?;
    let final_message = client.client_final(&server_first, &salted)?;
    write_frontend(
        stream,
        &[FrontendMessage::SaslResponse(Bytes::from(
            final_message.into_bytes(),
        ))],
    )
    .await?;

    let server_final = match read_backend_message(stream, buf).await? {
        BackendMessage::Authentication(Authentication::SaslFinal(data)) => data,
        BackendMessage::ErrorResponse(fields) => {
            return Err(ProxyError::BackendRejected(describe(&fields)));
        }
        other => {
            return Err(ProxyError::backend(format!(
                "expected AuthenticationSASLFinal, got tag {}",
                other.tag() as char
            )));
        }
    };
    client.verify_server_final(&server_final)?;

    match read_backend_message(stream, buf).await? {
        BackendMessage::Authentication(Authentication::Ok) => Ok(()),
        BackendMessage::ErrorResponse(fields) => {
            Err(ProxyError::BackendRejected(describe(&fields)))
        }
        other => Err(ProxyError::backend(format!(
            "expected AuthenticationOk, got tag {}",
            other.tag() as char
        ))),
    }
}

fn password(config: &BackendConfig) -> Result<Zeroizing<String>> {
    config.password.clone().map(Zeroizing::new).ok_or_else(|| {
        ProxyError::config("the backend asked for a password but none is configured")
    })
}

fn describe(fields: &pgelastic_wire::Fields) -> String {
    let text = |value: Option<&Bytes>| {
        value.map_or_else(String::new, |v| String::from_utf8_lossy(v).into_owned())
    };
    format!("{} {}", text(fields.sqlstate()), text(fields.message()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> BackendConfig {
        BackendConfig {
            address: "127.0.0.1:5432".to_owned(),
            user: "proxy".to_owned(),
            password: None,
            database: None,
            connect_seconds: 5,
            tls: crate::config::BackendTlsConfig::default(),
        }
    }

    fn startup(parameters: &[(&str, &str)]) -> StartupMessage {
        StartupMessage::new(
            ProtocolVersion::V3_0,
            parameters
                .iter()
                .map(|(k, v)| {
                    (
                        Bytes::copy_from_slice(k.as_bytes()),
                        Bytes::copy_from_slice(v.as_bytes()),
                    )
                })
                .collect(),
        )
    }

    #[test]
    fn the_backend_role_replaces_the_one_the_client_asked_for() {
        let parameters = backend_parameters(&config(), &startup(&[("user", "tenant")]));
        assert_eq!(parameters[0].1, "proxy");
    }

    #[test]
    fn the_clients_database_is_kept_when_none_is_configured() {
        let parameters = backend_parameters(
            &config(),
            &startup(&[("user", "tenant"), ("database", "shop")]),
        );
        assert_eq!(parameters[1].1, "shop");
    }

    #[test]
    fn a_configured_database_overrides_the_clients() {
        let mut config = config();
        config.database = Some("pinned".to_owned());
        let parameters = backend_parameters(
            &config,
            &startup(&[("user", "tenant"), ("database", "shop")]),
        );
        assert_eq!(parameters[1].1, "pinned");
    }

    #[test]
    fn other_startup_parameters_survive_in_order() {
        let parameters = backend_parameters(
            &config(),
            &startup(&[
                ("user", "tenant"),
                ("application_name", "psql"),
                ("client_encoding", "UTF8"),
            ]),
        );
        assert_eq!(parameters[2].0, "application_name");
        assert_eq!(parameters[3].0, "client_encoding");
    }

    #[test]
    fn protocol_extension_parameters_are_not_forwarded() {
        let parameters = backend_parameters(
            &config(),
            &startup(&[("user", "tenant"), ("_pq_.something", "1")]),
        );
        assert!(!parameters.iter().any(|(k, _)| k.starts_with(b"_pq_.")));
    }

    #[tokio::test]
    async fn a_backend_that_refuses_tls_is_an_error_rather_than_a_downgrade() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8];
            socket.read_exact(&mut request).await.unwrap();
            socket.write_all(b"N").await.unwrap();
        });

        let tls = crate::tls::backend_connector(
            &crate::config::BackendTlsConfig {
                mode: crate::config::BackendTlsMode::Require,
                ca_file: None,
                server_name: None,
            },
            "localhost",
        )
        .unwrap()
        .unwrap();

        let error = connect_socket(&addr.to_string(), Some(&tls), Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(error, ProxyError::BackendRejected(_)));
    }

    #[tokio::test]
    async fn connecting_to_a_dead_address_times_out_rather_than_hanging() {
        // 203.0.113.0/24 is TEST-NET-3: reserved for documentation, so it
        // never routes anywhere.
        let error = connect_socket("203.0.113.1:5432", None, Duration::from_millis(150))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ProxyError::Timeout(_) | ProxyError::Io(_) | ProxyError::Config(_)
        ));
    }
}
