//! rustls configuration for both legs.

use std::io::BufReader;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::config::{BackendTlsConfig, BackendTlsMode, ServerTlsConfig};
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
