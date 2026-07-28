//! The elastic capacity allocator, as a pure state machine.
//!
//! In transaction pooling a held backend connection is exactly one unit of
//! work-in-progress: a tenant holding *K* backends runs at most *K* statements
//! concurrently. So the elastic pool's capacity unit and `PgBouncer`'s
//! `pool_size` are literally the same number, and stacking two limiters is a
//! mistake. This crate replaces the static per-pool `pool_size` with a lease
//! drawn from a single [`CapacityBudget`].
//!
//! # I/O-free by construction
//!
//! There is no tokio here, no socket, no lock, no atomic, and no clock read
//! except through the injected [`Clock`]. Every method on [`Allocator`] takes
//! `&mut self`. That is what lets the whole capacity model be property-tested
//! at microsecond speed before it ever touches a socket.
//!
//! # The two numbers that matter
//!
//! [`CapacityBudget::free`] is `total − Σ_i max(guaranteed_i, live_i)`. The
//! `max()` is load-bearing: an idle tenant's unused guarantee is deliberately
//! not lendable. [`check_reservations`] enforces
//! `Σ guaranteed ≤ total × (1 − headroom)` at admission — a tenant whose
//! guarantee cannot be honoured is rejected, not degraded.
//!
//! # Example
//!
//! ```
//! use pgelastic_capacity::{
//!     Admission, Allocator, AdmissionSpec, CreditKind, PoolSpec, RequestKind, TenantSpec,
//! };
//!
//! let pool = PoolSpec { backend_connections: 10, headroom_percent: 0, ..PoolSpec::default() };
//! let mut allocator = Allocator::new(pool, AdmissionSpec::default())?;
//!
//! let acme = "acme".into();
//! allocator.add_tenant(acme, TenantSpec { guaranteed: 2, burstable: 8, ..TenantSpec::default() })?;
//!
//! let client = allocator.connect_client(&"acme".into())?;
//! let Admission::Granted(lease) = allocator.try_lease(client, RequestKind::Normal) else {
//!     panic!("the first connection is inside the guarantee");
//! };
//! assert_eq!(lease.credit, CreditKind::Reserved);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![allow(clippy::must_use_candidate)]

pub mod allocator;
pub mod budget;
pub mod config;
pub mod error;
pub mod time;
pub mod types;

pub use allocator::{
    Accounting, Admission, Allocator, CreditKind, Disposition, Expired, Grant, Lease,
    ReleaseOutcome, Revocation, ServerConn, ServerState, Ticket,
};
pub use budget::{CapacityBudget, TenantEntry};
pub use config::{
    AdmissionSpec, AdmissionStrategy, CANCEL_CREDIT_CAP, MAX_HEADROOM_PERCENT, MAX_PRIORITY,
    MAX_WEIGHT, PoolMode, PoolSpec, QosClass, Ratio, Reservations, TenantSpec, check_reservations,
};
pub use error::{ConfigError, ConnectRejection, DenialReason, ErrorCode, ShrinkScope};
pub use time::{Clock, ManualClock, SystemClock, Timestamp};
pub use types::{ClientId, RequestKind, ServerId, TenantId, TicketId};
