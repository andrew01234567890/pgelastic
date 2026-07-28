//! A multiplexing `PostgreSQL` proxy.
//!
//! Two pooling modes share one data plane. In **session** mode a client is bound
//! to one backend for its whole life and the relay never looks inside a frame.
//! In **transaction** mode a backend is acquired at the first message that needs
//! one and released the moment the release gate says it may be, so *N* clients
//! run over a much smaller number of backend connections.
//!
//! The pieces are deliberately split by what has to be provable:
//!
//! - [`relay`], [`session`] and [`handshake`] own the bytes: pre-startup
//!   negotiation, TLS on both legs, SCRAM in both directions, and a relay that
//!   never buffers a whole `DataRow` or `CopyData` frame.
//! - `pgelastic-pool` owns the release predicate, the pool key, the reset
//!   ladder, the statement cache and the wait queue — all as pure state
//!   machines, so the properties that must not fail are provable without a
//!   `PostgreSQL` to talk to.
//! - `pgelastic-capacity` owns admission. Every checkout goes through
//!   `try_lease`, and its refusals reach the client as real `ErrorResponse`s
//!   carrying the taxonomy's SQLSTATE.
//! - [`pool`] and [`txn`] are the wiring: the map of pools, and the loop that
//!   drives checkout and check-in from the gate rather than from a second copy
//!   of it.

#![allow(clippy::must_use_candidate)]

pub mod backend;
pub mod cancel;
pub mod config;
pub mod error;
pub mod handshake;
pub mod metrics;
pub mod pool;
pub mod relay;
pub mod scram;
pub mod server;
pub mod session;
pub mod stream;
pub mod tls;
pub mod tripwire;
pub mod txn;
pub mod vars;
pub mod wire_io;

pub use error::{ProxyError, Result};
