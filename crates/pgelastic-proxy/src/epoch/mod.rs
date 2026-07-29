//! The primary-epoch fence.
//!
//! Kubernetes Services do not tear down established TCP connections —
//! kube-proxy's `CleanStaleEntries` never touches `ESTABLISHED` conntrack. A
//! demoted primary therefore keeps serving writes to every client that was
//! already connected to it, and `pg_rewind` is about to discard exactly those
//! writes. The proxy can solve this and a Service cannot, because every client
//! socket terminates here and every backend socket originates here: the answer
//! is to **sever, not to deregister**.
//!
//! The epoch is a monotonic `u64` published by the operator into
//! `PgInstance.status.primaryEpoch` and bound into the postmaster as the
//! `pgelastic.primary_epoch` custom GUC. It reaches the proxy by three
//! independent paths, and the proxy acts on whichever fires first:
//!
//! - [`EpochSource::Watch`] — a kube-rs watch on `PgInstance` ([`watch`]).
//!   Control plane only; it is never on the data path.
//! - [`EpochSource::Push`] — the promoting agent calls the proxy's admin
//!   endpoint ([`admin`]) before it writes `currentPrimary`. Fastest, but it
//!   needs reachability.
//! - [`EpochSource::Verify`] — the epoch read back off the backend connection
//!   itself ([`verify`]). **Mandatory, not an optimisation**: it is the only
//!   path that is safe under partition, so it has to work with the other two
//!   disabled.
//!
//! [`EpochFence`] holds the one number all three feed, and that number **never
//! goes backwards**. A lower observed epoch is a fence trigger, not new
//! information: it means the connection that reported it is talking to a
//! postmaster that has been superseded.

pub mod admin;
pub mod config;
pub mod indoubt;
pub mod policy;
pub mod verify;
pub mod watch;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::sync::watch as tokio_watch;
use tracing::{info, warn};

pub use config::{FenceTiming, FenceTimingError};
pub use indoubt::{InDoubtKey, InDoubtLog, InDoubtRecord};
pub use policy::{FenceAction, Held, InFlight, TransactionWitness, action};

/// The fence token: a monotonic counter the operator derives from the promotion
/// Lease's `LeaderTransitions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(u64);

impl Epoch {
    /// The epoch of a proxy that has not yet learned one.
    ///
    /// Zero rather than one: the seeded value in `status.primaryEpoch` is 1, so
    /// the very first observation from any path advances the fence and is
    /// recorded as such rather than being silently absorbed.
    pub const UNKNOWN: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Epoch {
    type Err = std::num::ParseIntError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.trim().parse().map(Self)
    }
}

/// Which of the three delivery paths an observation arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochSource {
    Watch,
    Push,
    Verify,
}

impl EpochSource {
    pub const ALL: [Self; 3] = [Self::Watch, Self::Push, Self::Verify];

    pub fn label(self) -> &'static str {
        match self {
            Self::Watch => "watch",
            Self::Push => "push",
            Self::Verify => "verify",
        }
    }
}

impl std::fmt::Display for EpochSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// What one observation meant for the fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observation {
    /// The epoch the proxy already holds. Nothing to sever.
    Unchanged,
    /// A higher epoch. Every socket opened under a lower one must be severed
    /// before any further checkout.
    Advanced { from: Epoch, to: Epoch },
    /// A lower epoch than the proxy has already seen. The in-memory epoch does
    /// **not** move — this is a fence trigger, not new information — and the
    /// connection that reported it is severed by the same sweep.
    Regressed { observed: Epoch, current: Epoch },
}

impl Observation {
    /// Whether this observation obliges the caller to sever old-epoch sockets.
    pub fn fences(self) -> bool {
        !matches!(self, Self::Unchanged)
    }

    /// Whether this observation moved the proxy's in-memory epoch.
    pub fn advanced(self) -> bool {
        matches!(self, Self::Advanced { .. })
    }
}

/// The proxy's single view of the primary epoch.
///
/// Every path writes here and nothing else holds an epoch of its own, so
/// monotonicity is a property of one function rather than a convention three
/// call sites have to keep.
#[derive(Debug)]
pub struct EpochFence {
    current: tokio_watch::Sender<Epoch>,
    timing: FenceTiming,
    in_doubt: Arc<InDoubtLog>,
    /// Monotonic count of advances, so a subscriber can tell "the epoch moved
    /// and moved back" from "nothing happened" without polling.
    generation: AtomicU64,
    advanced_at: std::sync::Mutex<Option<Instant>>,
}

impl EpochFence {
    pub fn new(timing: FenceTiming, in_doubt: Arc<InDoubtLog>) -> Arc<Self> {
        Arc::new(Self {
            current: tokio_watch::Sender::new(Epoch::UNKNOWN),
            timing,
            in_doubt,
            generation: AtomicU64::new(0),
            advanced_at: std::sync::Mutex::new(None),
        })
    }

    /// A fence with the default lease timing and an in-doubt log that is kept
    /// only in memory. For tests and for a proxy started without a data
    /// directory; production passes a file-backed log.
    pub fn in_memory() -> Arc<Self> {
        Self::new(FenceTiming::default(), InDoubtLog::in_memory())
    }

    pub fn current(&self) -> Epoch {
        *self.current.borrow()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn subscribe(&self) -> tokio_watch::Receiver<Epoch> {
        self.current.subscribe()
    }

    pub fn timing(&self) -> &FenceTiming {
        &self.timing
    }

    pub fn in_doubt(&self) -> &Arc<InDoubtLog> {
        &self.in_doubt
    }

    /// When the epoch last moved, for the reaction-deadline assertion.
    pub fn advanced_at(&self) -> Option<Instant> {
        *self
            .advanced_at
            .lock()
            .expect("the fence is never poisoned")
    }

    /// Folds one observation into the fence.
    ///
    /// The in-memory epoch is raised and never lowered. Both an advance and a
    /// regression return an observation that [`Observation::fences`], because
    /// both mean at least one live socket is talking to a postmaster that is no
    /// longer the primary this proxy will serve.
    pub fn observe(&self, source: EpochSource, observed: Epoch) -> Observation {
        let mut outcome = Observation::Unchanged;
        self.current.send_if_modified(|current| {
            if observed > *current {
                outcome = Observation::Advanced {
                    from: *current,
                    to: observed,
                };
                *current = observed;
                true
            } else {
                if observed < *current {
                    outcome = Observation::Regressed {
                        observed,
                        current: *current,
                    };
                }
                false
            }
        });

        match outcome {
            Observation::Advanced { from, to } => {
                self.generation.fetch_add(1, Ordering::AcqRel);
                *self
                    .advanced_at
                    .lock()
                    .expect("the fence is never poisoned") = Some(Instant::now());
                info!(%source, %from, %to, "the primary epoch advanced; severing every older backend socket");
            }
            Observation::Regressed { observed, current } => {
                warn!(
                    %source, %observed, %current,
                    "a backend reported an epoch below the highest ever seen; \
                     it is talking to a superseded primary and is fenced"
                );
            }
            Observation::Unchanged => {}
        }
        outcome
    }
}

/// The fence plus the two decisions the data path has to make about it.
///
/// Bundled because every checkout needs all three and threading them
/// separately through the pool manager is how one of them ends up not being
/// consulted.
#[derive(Debug, Clone)]
pub struct FenceRuntime {
    pub fence: Arc<EpochFence>,
    /// Run the pull/verify probe at every checkout. The only path that is safe
    /// under partition; turning it off is a deliberate downgrade.
    pub verify_at_checkout: bool,
    /// Refuse a checkout whose backend carries no `pgelastic.primary_epoch`.
    pub require_epoch: bool,
}

impl FenceRuntime {
    /// A fence with no durable log and both optional paths on. For tests, and
    /// for a proxy fronting a `PostgreSQL` pgelastic did not provision.
    pub fn in_memory() -> Self {
        Self {
            fence: EpochFence::in_memory(),
            verify_at_checkout: true,
            require_epoch: false,
        }
    }

    pub fn current(&self) -> Epoch {
        self.fence.current()
    }
}

impl From<&crate::config::FenceConfig> for FenceRuntime {
    /// Falls back to an in-memory log when the configured path cannot be
    /// opened. Refusing to start would take the whole pool down over a log
    /// nothing has yet needed to write; a proxy that fences and cannot persist
    /// is strictly better than one that does not fence.
    fn from(config: &crate::config::FenceConfig) -> Self {
        let in_doubt = config
            .in_doubt_log
            .as_ref()
            .map_or_else(InDoubtLog::in_memory, |path| match InDoubtLog::open(path) {
                Ok(log) => log,
                Err(error) => {
                    warn!(
                        %error,
                        path = %path.display(),
                        "the in-doubt log could not be opened; records will not survive a restart"
                    );
                    InDoubtLog::in_memory()
                }
            });
        Self {
            fence: EpochFence::new(config.lease, in_doubt),
            verify_at_checkout: config.verify_at_checkout,
            require_epoch: config.require_epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence() -> Arc<EpochFence> {
        EpochFence::in_memory()
    }

    #[test]
    fn a_fresh_fence_holds_the_unknown_epoch() {
        assert_eq!(fence().current(), Epoch::UNKNOWN);
    }

    #[test]
    fn the_first_observation_from_any_path_advances_the_fence() {
        for source in EpochSource::ALL {
            let fence = fence();
            assert_eq!(
                fence.observe(source, Epoch::new(1)),
                Observation::Advanced {
                    from: Epoch::UNKNOWN,
                    to: Epoch::new(1),
                }
            );
            assert_eq!(fence.current(), Epoch::new(1));
        }
    }

    #[test]
    fn the_epoch_never_goes_backwards() {
        let fence = fence();
        fence.observe(EpochSource::Push, Epoch::new(7));
        let observation = fence.observe(EpochSource::Verify, Epoch::new(3));
        assert_eq!(
            observation,
            Observation::Regressed {
                observed: Epoch::new(3),
                current: Epoch::new(7),
            }
        );
        assert_eq!(fence.current(), Epoch::new(7));
    }

    #[test]
    fn a_lower_epoch_is_a_fence_trigger_rather_than_new_information() {
        let fence = fence();
        fence.observe(EpochSource::Watch, Epoch::new(9));
        assert!(fence.observe(EpochSource::Verify, Epoch::new(8)).fences());
        assert!(!fence.observe(EpochSource::Verify, Epoch::new(8)).advanced());
    }

    #[test]
    fn re_observing_the_same_epoch_fences_nothing() {
        let fence = fence();
        fence.observe(EpochSource::Push, Epoch::new(4));
        assert_eq!(
            fence.observe(EpochSource::Watch, Epoch::new(4)),
            Observation::Unchanged
        );
        assert!(!fence.observe(EpochSource::Verify, Epoch::new(4)).fences());
    }

    #[test]
    fn whichever_path_fires_first_wins_and_the_others_are_absorbed() {
        let fence = fence();
        assert!(
            fence
                .observe(EpochSource::Verify, Epoch::new(12))
                .advanced()
        );
        assert!(!fence.observe(EpochSource::Push, Epoch::new(12)).advanced());
        assert!(!fence.observe(EpochSource::Watch, Epoch::new(12)).advanced());
        assert_eq!(fence.generation(), 1);
    }

    #[test]
    fn every_advance_bumps_the_generation_and_a_regression_does_not() {
        let fence = fence();
        fence.observe(EpochSource::Watch, Epoch::new(1));
        fence.observe(EpochSource::Watch, Epoch::new(2));
        fence.observe(EpochSource::Verify, Epoch::new(1));
        assert_eq!(fence.generation(), 2);
    }

    #[tokio::test]
    async fn a_subscriber_is_woken_by_an_advance() {
        let fence = fence();
        let mut rx = fence.subscribe();
        fence.observe(EpochSource::Push, Epoch::new(5));
        rx.changed()
            .await
            .expect("the sender outlives the receiver");
        assert_eq!(*rx.borrow_and_update(), Epoch::new(5));
    }

    #[test]
    fn an_epoch_parses_from_the_text_a_show_returns() {
        assert_eq!(" 42\n".parse::<Epoch>().unwrap(), Epoch::new(42));
        assert!("not a number".parse::<Epoch>().is_err());
    }
}
