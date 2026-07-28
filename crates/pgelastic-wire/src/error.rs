use std::fmt;

use crate::startup::MAX_STARTUP_PACKET_LENGTH;

/// Which side of the connection produced the message being decoded.
///
/// Byte tags are ambiguous without it: `'D'`, `'C'`, `'E'` and `'S'` all mean
/// different things in each direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Frontend,
    Backend,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frontend => f.write_str("frontend"),
            Self::Backend => f.write_str("backend"),
        }
    }
}

/// Every way a `PostgreSQL` v3 message can fail to decode.
///
/// No variant is reachable by panic: a hostile peer can only ever produce one
/// of these.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WireError {
    #[error("message body ended before a complete field could be read")]
    Truncated,

    #[error("{0} trailing byte(s) after the end of the message body")]
    TrailingBytes(usize),

    #[error("string field is not NUL-terminated")]
    UnterminatedString,

    #[error("declared message length {0} is too small to be valid")]
    InvalidLength(i32),

    #[error("declared message length {len} exceeds the configured limit of {max} bytes")]
    FrameTooLarge { len: usize, max: usize },

    #[error(
        "startup packet of {len} bytes exceeds MAX_STARTUP_PACKET_LENGTH ({})",
        MAX_STARTUP_PACKET_LENGTH
    )]
    StartupPacketTooLarge { len: usize },

    #[error("unknown {direction} message tag {tag:?}")]
    UnknownTag { direction: Direction, tag: u8 },

    #[error("unknown authentication request code {0}")]
    UnknownAuthentication(i32),

    #[error("invalid ReadyForQuery transaction-status byte {0:?}")]
    InvalidTransactionStatus(u8),

    #[error("invalid Describe/Close target byte {0:?}, expected 'S' or 'P'")]
    InvalidTarget(u8),

    #[error("invalid format code {0}, expected 0 or 1")]
    InvalidFormat(i16),

    #[error("cancel key of {0} bytes is outside the permitted range 4..=256")]
    InvalidCancelKeyLength(usize),

    #[error("unsupported protocol major version {major} (requested {major}.{minor})")]
    UnsupportedProtocolVersion { major: u16, minor: u16 },

    #[error("unrecognised pre-startup request code {0}")]
    UnknownStartupCode(i32),

    #[error("peer sent a second {0} after the first was already answered")]
    RepeatedNegotiation(&'static str),

    #[error("pre-startup negotiation is already complete")]
    PreStartupComplete,

    #[error("declared element count {count} cannot fit in the {remaining} remaining bytes")]
    ImplausibleCount { count: usize, remaining: usize },
}
