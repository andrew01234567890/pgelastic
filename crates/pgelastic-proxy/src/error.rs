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
    /// `40003 statement_completion_unknown`
    ///
    /// The commit was forwarded and its completion was never observed. The
    /// standard says what pgelastic needs it to say: the outcome is unknown. A
    /// client SDK must treat it as `UNKNOWN` and must not retry.
    pub const STATEMENT_COMPLETION_UNKNOWN: &str = "40003";
    /// `57P03 cannot_connect_now`
    ///
    /// Raised when a checkout is refused because the tenant's instance cannot
    /// complete a commit: its loaded `synchronous_standby_names` names more
    /// synchronous standbys than are streaming, so the next `COMMIT` would park
    /// in `IPC.SyncRep` and never return. Nothing was forwarded, so this is a
    /// definite refusal and a retry — ideally after the instance recovers or
    /// the tenant is moved — is safe.
    pub const CANNOT_CONNECT_NOW: &str = "57P03";
    /// `25006 read_only_sql_transaction`
    ///
    /// Raised when a write is refused *before* being forwarded because the
    /// backend it would have gone to is on a superseded primary epoch. Unlike
    /// [`STATEMENT_COMPLETION_UNKNOWN`] this is a definite failure: nothing
    /// reached the server, so retrying on a fresh connection is safe.
    pub const READ_ONLY_SQL_TRANSACTION: &str = "25006";

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
            CANNOT_CONNECT_NOW => CANNOT_CONNECT_NOW,
            ADMIN_SHUTDOWN => ADMIN_SHUTDOWN,
            STATEMENT_COMPLETION_UNKNOWN => STATEMENT_COMPLETION_UNKNOWN,
            READ_ONLY_SQL_TRANSACTION => READ_ONLY_SQL_TRANSACTION,
            _ => CONNECTION_FAILURE,
        }
    }
}

/// The two codes the primary-epoch fence raises on its own behalf.
///
/// They lead the message text, exactly as the capacity taxonomy's do, so a
/// client that cannot read SQLSTATE still has a stable token to match on. The
/// difference between them is the whole point of the fence's asymmetry: one is
/// a definite failure, the other is not an outcome at all.
pub mod fence_code {
    /// The outcome of a forwarded commit was never observed.
    ///
    /// **Never a failure and never a success.** A client SDK must surface it as
    /// `UNKNOWN` and must not retry: the transaction may have committed on a
    /// primary that is about to be rewound, or it may not have.
    pub const OUTCOME_UNKNOWN: &str = "PGE4003";
    /// A write was refused before it was forwarded, because the connection it
    /// would have used is on a superseded primary epoch. Definitely not
    /// applied, so it is safe to retry.
    pub const SUPERSEDED_EPOCH: &str = "PGE2506";
    /// The tenant's instance cannot complete a commit, so no backend was taken.
    ///
    /// Distinguished from every capacity refusal on purpose: `PGE1024` means
    /// *wait longer and you will be served*, and this means the opposite. A
    /// client that retries into a write-stalled instance is doing the one thing
    /// that makes the incident spread.
    pub const WRITE_STALLED: &str = "PGE5703";
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

    /// The outcome of a commit the proxy forwarded was never observed, because
    /// the primary epoch changed underneath it.
    ///
    /// Deliberately its own variant rather than an `Admission` with a different
    /// code: everything that handles a `ProxyError` has to be unable to
    /// accidentally treat this as a refusal. The transaction is recorded in the
    /// durable in-doubt log before this is constructed.
    #[error("{}: {message}", fence_code::OUTCOME_UNKNOWN)]
    OutcomeUnknown { message: String },

    /// A write was refused before being forwarded because its backend is on a
    /// superseded primary epoch.
    #[error("{}: {message}", fence_code::SUPERSEDED_EPOCH)]
    SupersededEpoch { message: String },

    /// A checkout was refused because the tenant's instance is write-stalled.
    ///
    /// Its own variant rather than an `Admission` refusal, because the two mean
    /// opposite things to a retrying client: an admission refusal clears when
    /// the pool has room, and this one does not clear until quorum comes back
    /// or the tenant is moved. Nothing was forwarded and no backend was taken,
    /// so the transaction definitely did not happen.
    #[error("{}: {message}", fence_code::WRITE_STALLED)]
    WriteStalled { message: String },
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
            Self::OutcomeUnknown { .. } => sqlstate::STATEMENT_COMPLETION_UNKNOWN,
            Self::SupersededEpoch { .. } => sqlstate::READ_ONLY_SQL_TRANSACTION,
            Self::WriteStalled { .. } => sqlstate::CANNOT_CONNECT_NOW,
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
