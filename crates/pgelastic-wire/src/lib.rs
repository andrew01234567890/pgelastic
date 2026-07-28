//! `PostgreSQL` v3 frontend/backend protocol codec.
//!
//! The codec is deliberately narrow: it turns bytes into frames and frames into
//! messages, and does nothing else. There are no sockets, no TLS, no pooling
//! and no I/O of any kind here.
//!
//! Three properties are load-bearing enough to be worth stating up front.
//!
//! **Frontend and backend messages are disjoint types.** Tags collide by
//! direction — `'D'` is `Describe` one way and `DataRow` the other, `'S'` is
//! `Sync` one way and `ParameterStatus` the other — and the frontend `'p'` tag
//! covers four unrelated messages that only the authentication state can tell
//! apart. Decoding therefore always takes a direction, and `'p'` additionally
//! takes an [`AuthState`].
//!
//! **Row and COPY payloads are never decoded.** [`DataRow`] and `CopyData`
//! carry an opaque [`Bytes`](bytes::Bytes) slice of the read buffer straight
//! through to the write side.
//!
//! **[`MessageBuffer`] is cancel-safe and hand-rolled.** Nothing is buffered
//! inside a future, so dropping a read mid-poll cannot lose a partial frame.
//!
//! # Example
//!
//! ```
//! use bytes::BytesMut;
//! use pgelastic_wire::{BackendMessage, MessageBuffer, TransactionStatus};
//!
//! let mut wire = BytesMut::new();
//! BackendMessage::ReadyForQuery(TransactionStatus::Idle).encode(&mut wire);
//!
//! let mut buf = MessageBuffer::new();
//! buf.extend_from_slice(&wire);
//!
//! let frame = buf.next_frame().unwrap().expect("a whole frame is buffered");
//! let message = BackendMessage::decode(&frame).unwrap();
//! assert_eq!(message, BackendMessage::ReadyForQuery(TransactionStatus::Idle));
//! ```

#![allow(clippy::must_use_candidate)]

pub mod backend;
pub mod buffer;
mod codec;
pub mod error;
pub mod frontend;
pub mod startup;
pub mod types;

pub use backend::{
    Authentication, BackendKeyData, BackendMessage, CopyResponse, DataRow,
    NegotiateProtocolVersion, NotificationResponse, ParameterStatus, RowDescription,
};
pub use buffer::{DEFAULT_MAX_FRAME_LEN, DEFAULT_READ_CHUNK, MessageBuffer};
pub use codec::RawFrame;
pub use error::{Direction, WireError};
pub use frontend::{
    AuthState, Bind, Close, Describe, Execute, FrontendMessage, FunctionCall, Parse,
    SaslInitialResponse,
};
pub use startup::{
    CANCEL_REQUEST_CODE, CancelRequest, DIRECT_TLS_ALPN, GSSENC_REQUEST_CODE,
    MAX_STARTUP_PACKET_LENGTH, PreStartup, PreStartupMachine, PreStartupState, ProtocolVersion,
    SSL_REQUEST_CODE, StartupMessage, TLS_HANDSHAKE_FIRST_BYTE,
};
pub use types::{CancelKey, FieldDescription, Fields, Format, Target, TransactionStatus};
