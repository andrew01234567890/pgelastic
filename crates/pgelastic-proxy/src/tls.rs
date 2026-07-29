//! rustls configuration for both legs.

use std::io::BufReader;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::config::{BackendTlsConfig, BackendTlsMode, ControlTlsConfig, ServerTlsConfig};
use crate::error::{ProxyError, Result};

/// The ALPN identifier a direct-TLS `PostgreSQL` connection must negotiate.
pub const ALPN: &[u8] = pgelastic_wire::DIRECT_TLS_ALPN;

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Installs the process-wide crypto provider.
///
/// Idempotent: rustls only accepts the first installation, and a second call
/// losing the race is not an error because the winner installed the same
/// provider.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

pub fn server_acceptor(config: &ServerTlsConfig) -> Result<TlsAcceptor> {
    let certs = load_certs(&config.certificate_file)?;
    let key = load_key(&config.key_file)?;

    let mut server_config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    // Advertised for the direct-TLS path. A client that offers ALPN and does
    // not include this gets a no_application_protocol alert from rustls; a
    // client that offers none (the SSLRequest path) is unaffected.
    server_config.alpn_protocols = vec![ALPN.to_vec()];
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// The ALPN identifier the control listener advertises.
const CONTROL_ALPN: &[u8] = b"http/1.1";

/// What a control-plane caller proved about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlIdentity {
    /// A certificate that chains to the configured CA and carries the expected
    /// name.
    Authorized,
    /// No client certificate was presented at all.
    Anonymous,
    /// A certificate that does not chain to the configured CA, or that is not
    /// usable for client authentication.
    Untrusted(String),
    /// A trusted certificate belonging to somebody else.
    WrongName(String),
}

impl ControlIdentity {
    /// The refusal to send back, or `None` when the caller may proceed.
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        match self {
            Self::Authorized => None,
            Self::Anonymous => Some(
                "this endpoint requires a client certificate and none was presented".to_owned(),
            ),
            Self::Untrusted(detail) => Some(format!(
                "the client certificate does not chain to control.tls.clientCaFile: {detail}"
            )),
            Self::WrongName(expected) => Some(format!(
                "the client certificate is trusted but does not carry {expected}"
            )),
        }
    }
}

/// Terminates TLS on the control listener and decides who the caller is.
///
/// Mutual TLS rather than a token, and the identity is checked *after* the
/// handshake rather than during it. Rejecting an unknown certificate inside the
/// handshake ends the connection with a TLS alert, which reaches an operator as
/// an unexplained reset; the whole point of authenticating this listener is
/// that a misconfigured cutover has to be diagnosable, so the trust decision is
/// taken here and reported as a `401` with the reason named.
///
/// The deferral does not weaken the check: nothing but a caller this type
/// returns [`ControlIdentity::Authorized`] for is ever served, and that verdict
/// comes from the same `webpki` verifier the handshake would have used.
pub struct ControlAuthority {
    acceptor: TlsAcceptor,
    trust: Arc<dyn ClientCertVerifier>,
    expected: ServerName<'static>,
    expected_name: String,
}

impl std::fmt::Debug for ControlAuthority {
    /// Hand-written because `TlsAcceptor` has no `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlAuthority")
            .field("expected_name", &self.expected_name)
            .finish_non_exhaustive()
    }
}

impl ControlAuthority {
    pub fn new(config: &ControlTlsConfig) -> Result<Arc<Self>> {
        let certs = load_certs(&config.certificate_file)?;
        let key = load_key(&config.key_file)?;

        let mut roots = RootCertStore::empty();
        for cert in load_certs(&config.client_ca_file)? {
            roots.add(cert).map_err(|e| {
                ProxyError::config(format!("{}: {e}", config.client_ca_file.display()))
            })?;
        }
        let trust = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
            .build()
            .map_err(|e| ProxyError::config(format!("control.tls.clientCaFile: {e}")))?;

        let expected = ServerName::try_from(config.client_name.clone()).map_err(|_| {
            ProxyError::config(format!(
                "control.tls.clientName {:?} is not a DNS name",
                config.client_name
            ))
        })?;

        let mut server_config = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(Arc::new(DeferredClientAuth {
                trust: Arc::clone(&trust),
            }))
            .with_single_cert(certs, key)?;
        server_config.alpn_protocols = vec![CONTROL_ALPN.to_vec()];

        Ok(Arc::new(Self {
            acceptor: TlsAcceptor::from(Arc::new(server_config)),
            trust,
            expected,
            expected_name: config.client_name.clone(),
        }))
    }

    #[must_use]
    pub fn acceptor(&self) -> TlsAcceptor {
        self.acceptor.clone()
    }

    /// What the chain the caller presented entitles it to.
    #[must_use]
    pub fn authorize(&self, presented: Option<&[CertificateDer<'static>]>) -> ControlIdentity {
        let Some((end_entity, intermediates)) = presented.and_then(<[_]>::split_first) else {
            return ControlIdentity::Anonymous;
        };
        if let Err(error) =
            self.trust
                .verify_client_cert(end_entity, intermediates, UnixTime::now())
        {
            return ControlIdentity::Untrusted(error.to_string());
        }
        let Ok(cert) = webpki::EndEntityCert::try_from(end_entity) else {
            return ControlIdentity::Untrusted(
                "the client certificate could not be parsed".to_owned(),
            );
        };
        if cert
            .verify_is_valid_for_subject_name(&self.expected)
            .is_err()
        {
            return ControlIdentity::WrongName(self.expected_name.clone());
        }
        ControlIdentity::Authorized
    }
}

/// Collects the client's certificate without ruling on it.
///
/// Signature verification is still delegated to the real verifier, so a caller
/// that does not hold the private key for the certificate it presented never
/// completes the handshake. Only the *trust* decision is deferred, to
/// [`ControlAuthority::authorize`].
#[derive(Debug)]
struct DeferredClientAuth {
    trust: Arc<dyn ClientCertVerifier>,
}

impl ClientCertVerifier for DeferredClientAuth {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.trust.root_hint_subjects()
    }

    /// False so that a caller with no certificate reaches the HTTP layer and is
    /// told why it was refused, rather than being dropped mid-handshake.
    fn client_auth_mandatory(&self) -> bool {
        false
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.trust.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.trust.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.trust.supported_verify_schemes()
    }
}

#[derive(Clone)]
pub struct BackendTls {
    pub connector: TlsConnector,
    pub server_name: ServerName<'static>,
}

impl std::fmt::Debug for BackendTls {
    /// Hand-written because `TlsConnector` has no `Debug`, and the only part
    /// worth printing is the name the certificate is checked against.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendTls")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

pub fn backend_connector(
    config: &BackendTlsConfig,
    default_server_name: &str,
) -> Result<Option<BackendTls>> {
    if config.mode == BackendTlsMode::Disable {
        return Ok(None);
    }

    let builder =
        ClientConfig::builder_with_provider(provider()).with_safe_default_protocol_versions()?;

    let client_config = match config.mode {
        BackendTlsMode::Disable => unreachable!("returned above"),
        BackendTlsMode::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(EncryptOnlyVerifier::new()))
            .with_no_client_auth(),
        BackendTlsMode::VerifyFull => {
            let ca_file = config
                .ca_file
                .as_ref()
                .ok_or_else(|| ProxyError::config("VerifyFull requires backend.tls.caFile"))?;
            let mut roots = RootCertStore::empty();
            for cert in load_certs(ca_file)? {
                roots
                    .add(cert)
                    .map_err(|e| ProxyError::config(format!("{}: {e}", ca_file.display())))?;
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
    };

    let name = config.server_name.as_deref().unwrap_or(default_server_name);
    let server_name = ServerName::try_from(name.to_owned())
        .map_err(|_| ProxyError::config(format!("{name} is not a valid TLS server name")))?;

    Ok(Some(BackendTls {
        connector: TlsConnector::from(Arc::new(client_config)),
        server_name,
    }))
}

fn load_certs(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .map_err(|e| ProxyError::config(format!("opening {}: {e}", path.display())))?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| ProxyError::config(format!("parsing {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(ProxyError::config(format!(
            "{} contains no certificates",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_key(path: &std::path::Path) -> Result<rustls_pki_types::PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)
        .map_err(|e| ProxyError::config(format!("opening {}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|e| ProxyError::config(format!("parsing {}: {e}", path.display())))?
        .ok_or_else(|| ProxyError::config(format!("{} contains no private key", path.display())))
}

/// The verifier behind `sslmode=require`: encryption without authentication.
///
/// Named for what it actually provides. It stops passive capture and nothing
/// else — an active attacker can still present any certificate — which is
/// precisely libpq's own `require` semantics, and why `VerifyFull` is the
/// documented setting for anything that matters.
#[derive(Debug)]
struct EncryptOnlyVerifier {
    provider: Arc<CryptoProvider>,
}

impl EncryptOnlyVerifier {
    fn new() -> Self {
        Self {
            provider: provider(),
        }
    }
}

impl ServerCertVerifier for EncryptOnlyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackendTlsMode;

    #[test]
    fn a_disabled_backend_needs_no_connector() {
        let config = BackendTlsConfig {
            mode: BackendTlsMode::Disable,
            ca_file: None,
            server_name: None,
        };
        assert!(backend_connector(&config, "localhost").unwrap().is_none());
    }

    #[test]
    fn require_mode_builds_without_any_ca() {
        let config = BackendTlsConfig {
            mode: BackendTlsMode::Require,
            ca_file: None,
            server_name: None,
        };
        let tls = backend_connector(&config, "localhost").unwrap().unwrap();
        assert_eq!(tls.server_name, ServerName::try_from("localhost").unwrap());
    }

    #[test]
    fn verify_full_without_a_ca_is_refused() {
        let config = BackendTlsConfig {
            mode: BackendTlsMode::VerifyFull,
            ca_file: None,
            server_name: None,
        };
        assert!(backend_connector(&config, "localhost").is_err());
    }

    #[test]
    fn an_unparsable_server_name_is_refused() {
        let config = BackendTlsConfig {
            mode: BackendTlsMode::Require,
            ca_file: None,
            server_name: Some("not a hostname".to_owned()),
        };
        assert!(backend_connector(&config, "localhost").is_err());
    }
}
