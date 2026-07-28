//! Session-mode `PostgreSQL` proxy.
//!
//! One client connection is bound to one backend connection for its whole life.
//! Pooling, capacity leases and transaction-mode multiplexing land in a later
//! milestone; the crates that implement those state machines
//! (`pgelastic-pool`, `pgelastic-capacity`) are deliberately not wired in here
//! yet, so nothing in this crate has to reason about a connection changing
//! hands mid-session.
//!
//! What this crate owns is the part that must be right before any of that is
//! safe: the pre-startup negotiation, TLS on both legs, SCRAM in both
//! directions, cancellation, and a relay that never buffers a whole `DataRow`
//! or `CopyData` frame.

#![allow(clippy::must_use_candidate)]

pub mod backend;
pub mod cancel;
pub mod config;
pub mod error;
pub mod handshake;
pub mod metrics;
pub mod relay;
pub mod scram;
pub mod server;
pub mod session;
pub mod stream;
pub mod tls;
pub mod wire_io;

pub use error::{ProxyError, Result};
