use std::io;

use pgelastic_wire::WireError;

pub type Result<T, E = ProxyError> = std::result::Result<T, E>;

/// SQLSTATE codes the proxy raises on its own behalf.
pub mod sqlstate {
    /// `28P01 invalid_password`
    pub const INVALID_PASSWORD: &str = "28P01";
    /// `28000 invalid_authorization_specification`
    pub const INVALID_AUTHORIZATION: &str = "28000";
    /// `08P01 protocol_violation`
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    /// `08006 connection_failure`
    pub const CONNECTION_FAILURE: &str = "08006";
    /// `53300 too_many_connections`
    pub const TOO_MANY_CONNECTIONS: &str = "53300";
    /// `57P01 admin_shutdown`
    pub const ADMIN_SHUTDOWN: &str = "57P01";

    /// Puts a SQLSTATE that has been through an owned `String` back on the
    /// static set.
    ///
    /// The connect gate remembers a failed login as text so that the pooling
    /// crate need not know the proxy's error type, and every client that
    /// fast-fails against that cached failure still has to be handed a code from
    /// the same closed set the rest of the proxy reports.
    pub fn intern(code: &str) -> &'static str {
        match code {
            INVALID_PASSWORD => INVALID_PASSWORD,
            INVALID_AUTHORIZATION => INVALID_AUTHORIZATION,
            PROTOCOL_VIOLATION => PROTOCOL_VIOLATION,
            TOO_MANY_CONNECTIONS => TOO_MANY_CONNECTIONS,
            ADMIN_SHUTDOWN => ADMIN_SHUTDOWN,
            _ => CONNECTION_FAILURE,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProxyError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("protocol error: {0}")]
    Wire(#[from] WireError),

    /// A malformed or out-of-order SCRAM message on either leg.
    ///
    /// Distinct from [`AuthenticationFailed`](Self::AuthenticationFailed),
    /// which is the credential verdict. Nothing here depends on whether the
    /// user exists, so keeping the detail costs no enumeration surface.
    #[error("SCRAM error: {0}")]
    Scram(#[from] crate::scram::ScramError),

    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("peer closed the connection")]
    PeerGone,

    #[error("client violated the protocol: {0}")]
    ClientProtocol(String),

    #[error("backend violated the protocol: {0}")]
    BackendProtocol(String),

    /// Deliberately opaque: an unknown user and a wrong password must be
    /// indistinguishable to the client and in the logs, or the proxy becomes a
    /// tenant-enumeration oracle.
    #[error("authentication failed")]
    AuthenticationFailed,

    #[error("backend rejected the connection: {0}")]
    BackendRejected(String),

    #[error("the proxy is shutting down")]
    ShuttingDown,

    #[error("connection limit reached")]
    ConnectionLimit,

    #[error("timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// A capacity refusal. The SQLSTATE comes from the error taxonomy rather
    /// than from this enum, because the taxonomy is API surface a client writes
    /// retry logic against.
    #[error("{message}")]
    Admission {
        sqlstate: &'static str,
        message: String,
    },
}

impl ProxyError {
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    pub fn client(message: impl Into<String>) -> Self {
        Self::ClientProtocol(message.into())
    }

    pub fn backend(message: impl Into<String>) -> Self {
        Self::BackendProtocol(message.into())
    }

    /// The SQLSTATE to report to a client that is still able to receive one.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Admission { sqlstate, .. } => sqlstate,
            Self::AuthenticationFailed => sqlstate::INVALID_PASSWORD,
            Self::ConnectionLimit => sqlstate::TOO_MANY_CONNECTIONS,
            Self::ShuttingDown => sqlstate::ADMIN_SHUTDOWN,
            Self::ClientProtocol(_) | Self::Wire(_) | Self::Scram(_) => {
                sqlstate::PROTOCOL_VIOLATION
            }
            _ => sqlstate::CONNECTION_FAILURE,
        }
    }
}
