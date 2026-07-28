//! SCRAM-SHA-256, in both directions.
//!
//! The proxy is a server to its clients and a client to `PostgreSQL`, and both
//! halves live here so the primitives, the message syntax and the constant-time
//! comparison are shared rather than reimplemented per direction.
//!
//! `SCRAM-SHA-256-PLUS` is not offered. End-to-end channel binding is
//! structurally impossible behind a TLS-terminating proxy; binding to the
//! *proxy's* certificate is the correct answer and is a later milestone, so
//! until then only the unbound mechanism is advertised and a client that
//! requires binding is refused rather than silently downgraded.

pub mod client;
pub mod crypto;
pub mod message;
pub mod server;
pub mod verifier;

pub use client::ScramClient;
pub use crypto::KdfPool;
pub use message::ScramError;
pub use server::{ScramOutcome, ScramServer};
pub use verifier::{DEFAULT_ITERATIONS, MockSecret, ScramVerifier, VerifierError};

/// The only mechanism the proxy advertises or accepts.
pub const MECHANISM: &str = "SCRAM-SHA-256";
