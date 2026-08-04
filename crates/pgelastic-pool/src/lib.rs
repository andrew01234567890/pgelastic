//! Connection pooling state machines.
//!
//! Everything here is pure logic. There are no sockets, no timers that fire on
//! their own and no I/O, because the properties this crate is responsible for —
//! that a backend link is never handed to a second client while the first is
//! still using it, never handed to a different tenant at all, and never handed
//! on carrying the previous client's session state — are the ones that must be
//! provable without a `PostgreSQL` to talk to. Time enters as an explicit
//! [`Instant`](std::time::Instant) argument; wire bytes enter as decoded
//! `pgelastic-wire` messages.
//!
//! The pieces, in the order a connection meets them:
//!
//! - [`key`] — the total identity a link may be reused under.
//! - [`client`] and [`server`] — the two state machines, `PgBouncer`'s states
//!   name for name.
//! - [`outstanding`] — the per-link request queue that makes pipelining and
//!   injected messages safe.
//! - [`gate`] — the single predicate that decides release.
//! - [`wait`] — the FIFO queue, whose waiters deregister on `Drop`.
//! - [`connect`] — one backend connect at a time, with a cached login failure.
//! - [`reset`] — taint tracking and the reset ladder.
//! - [`pin`] — unscrubbable state and the budget it is taken out of.
//! - [`stmt`] — the content-addressed prepared statement cache.

#![allow(clippy::must_use_candidate)]

pub mod client;
pub mod connect;
pub mod gate;
pub mod key;
pub mod outstanding;
pub mod pin;
pub mod reset;
pub mod server;
pub mod stmt;
pub mod wait;

pub use client::{ClientEvent, ClientMachine, ClientState, IllegalClientTransition};
pub use connect::{ConnectDecision, ConnectGate, ConnectPermit, LoginFailure};
pub use gate::{CheckInBlock, can_check_in};
pub use key::{
    BackendTarget, CredentialGeneration, DatabaseName, FingerprintPolicy, PoolKey, PoolKeySpec,
    PoolMode, ReplicationKind, RoleName, StartupFingerprint, StartupParamPolicy, TenantId,
    TlsPosture, expand_startup_options,
};
pub use outstanding::{
    Disposition, OutstandingError, OutstandingQueue, Reaction, Relay, RequestKind,
};
pub use pin::{BudgetError, BudgetLedger, PinReason};
pub use reset::{
    CloseReason, ReleaseContext, ResetDisposition, ResetPlan, ResetPolicy, ResetStep, Taint,
    TaintSet, plan,
};
pub use server::{
    CopyState, IllegalServerTransition, LinkError, Origin, ReleaseFlags, ServerEvent, ServerId,
    ServerLink, ServerState, jittered_lifetime,
};
pub use stmt::{
    CacheInvalidation, ClientStatements, DEFAULT_GLOBAL_STATEMENTS, GlobalStatementCache,
    PreparedStatement, ServerAction, ServerStatements, StatementKey, StatementName,
    detect_cache_invalidation,
};
pub use wait::{Priority, WaitError, WaitQueue, Waiter};
