//! Backend connect admission: one attempt at a time, and a cached failure.
//!
//! Only one backend connect per pool may be in flight. Without that, a burst of
//! clients arriving at an empty pool opens one backend connection each, which is
//! both the thundering herd that overloads a recovering `PostgreSQL` and the
//! reason the cancel path cannot reason about which link a client is on.
//!
//! When a connect fails for a reason that will still be true a millisecond later
//! — wrong password, database does not exist, server still starting — every
//! client that arrives during the backoff window is failed immediately with the
//! cached error rather than queued behind an attempt that is certain to fail.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// `serverLoginRetry`.
pub const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(15);

/// The error a failed login is remembered by, and reported with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginFailure {
    /// The SQLSTATE to report to clients that fast-fail against this.
    pub sqlstate: String,
    pub message: String,
}

impl LoginFailure {
    pub fn new(sqlstate: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            sqlstate: sqlstate.into(),
            message: message.into(),
        }
    }
}

/// What a client asking for a new backend connection should do.
#[derive(Debug)]
pub enum ConnectDecision {
    /// Open the connection; hold the permit until it settles.
    Start(ConnectPermit),
    /// Somebody else is already opening one; wait for it.
    AlreadyInFlight,
    /// The last attempt failed and the retry interval has not elapsed.
    BackingOff(Arc<LoginFailure>),
}

#[derive(Debug)]
struct Inner {
    in_flight: bool,
    retry_interval: Duration,
    blocked_until: Option<Instant>,
    last_failure: Option<Arc<LoginFailure>>,
}

/// Serialises backend connects for one pool.
#[derive(Debug, Clone)]
pub struct ConnectGate {
    inner: Arc<Mutex<Inner>>,
}

impl Default for ConnectGate {
    fn default() -> Self {
        Self::new(DEFAULT_RETRY_INTERVAL)
    }
}

impl ConnectGate {
    pub fn new(retry_interval: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                in_flight: false,
                retry_interval,
                blocked_until: None,
                last_failure: None,
            })),
        }
    }

    pub fn retry_interval(&self) -> Duration {
        self.inner.lock().retry_interval
    }

    /// The cached error a newly arrived client should be failed with, if any.
    pub fn cached_failure(&self, now: Instant) -> Option<Arc<LoginFailure>> {
        let inner = self.inner.lock();
        if inner.blocked_until.is_some_and(|until| now < until) {
            inner.last_failure.clone()
        } else {
            None
        }
    }

    pub fn is_connecting(&self) -> bool {
        self.inner.lock().in_flight
    }

    /// Asks permission to open a backend connection.
    pub fn try_start(&self, now: Instant) -> ConnectDecision {
        let mut inner = self.inner.lock();

        if inner.blocked_until.is_some_and(|until| now < until)
            && let Some(failure) = inner.last_failure.clone()
        {
            return ConnectDecision::BackingOff(failure);
        }

        if inner.in_flight {
            return ConnectDecision::AlreadyInFlight;
        }

        inner.in_flight = true;
        ConnectDecision::Start(ConnectPermit {
            inner: Arc::clone(&self.inner),
            settled: false,
        })
    }
}

/// The right to have exactly one connect in flight.
///
/// Dropping it without calling [`succeeded`](Self::succeeded) or
/// [`failed`](Self::failed) frees the slot without recording a failure, so a
/// cancelled connect future cannot wedge the pool the way a cancelled waiter
/// could.
#[derive(Debug)]
pub struct ConnectPermit {
    inner: Arc<Mutex<Inner>>,
    settled: bool,
}

impl ConnectPermit {
    /// The connection is up; clears any backoff.
    pub fn succeeded(mut self) {
        self.settled = true;
        let mut inner = self.inner.lock();
        inner.in_flight = false;
        inner.blocked_until = None;
        inner.last_failure = None;
    }

    /// The connection failed; starts the backoff window.
    pub fn failed(mut self, failure: LoginFailure, now: Instant) -> Arc<LoginFailure> {
        self.settled = true;
        let failure = Arc::new(failure);
        let mut inner = self.inner.lock();
        inner.in_flight = false;
        inner.blocked_until = Some(now + inner.retry_interval);
        inner.last_failure = Some(Arc::clone(&failure));
        failure
    }
}

impl Drop for ConnectPermit {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        self.inner.lock().in_flight = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure() -> LoginFailure {
        LoginFailure::new("28P01", "password authentication failed")
    }

    #[test]
    fn only_one_connect_is_permitted_at_a_time() {
        let gate = ConnectGate::default();
        let now = Instant::now();

        let permit = match gate.try_start(now) {
            ConnectDecision::Start(permit) => permit,
            other => panic!("expected a permit, got {other:?}"),
        };
        assert!(matches!(
            gate.try_start(now),
            ConnectDecision::AlreadyInFlight
        ));
        assert!(gate.is_connecting());

        permit.succeeded();
        assert!(!gate.is_connecting());
        assert!(matches!(gate.try_start(now), ConnectDecision::Start(_)));
    }

    #[test]
    fn an_abandoned_attempt_releases_the_slot() {
        let gate = ConnectGate::default();
        let now = Instant::now();
        let permit = gate.try_start(now);
        drop(permit);
        assert!(!gate.is_connecting());
        assert!(matches!(gate.try_start(now), ConnectDecision::Start(_)));
    }

    #[test]
    fn an_abandoned_attempt_records_no_failure() {
        let gate = ConnectGate::default();
        let now = Instant::now();
        drop(gate.try_start(now));
        assert_eq!(gate.cached_failure(now), None);
    }

    #[test]
    fn new_clients_fast_fail_against_the_cached_error() {
        let gate = ConnectGate::default();
        let now = Instant::now();

        let ConnectDecision::Start(permit) = gate.try_start(now) else {
            panic!("expected a permit");
        };
        permit.failed(failure(), now);

        match gate.try_start(now + Duration::from_secs(1)) {
            ConnectDecision::BackingOff(cached) => assert_eq!(cached.sqlstate, "28P01"),
            other => panic!("expected a cached failure, got {other:?}"),
        }
        assert!(gate.cached_failure(now + Duration::from_secs(1)).is_some());
    }

    #[test]
    fn the_backoff_expires_after_the_retry_interval() {
        let gate = ConnectGate::new(Duration::from_secs(15));
        let now = Instant::now();

        let ConnectDecision::Start(permit) = gate.try_start(now) else {
            panic!("expected a permit");
        };
        permit.failed(failure(), now);

        assert!(gate.cached_failure(now + Duration::from_secs(14)).is_some());
        assert!(gate.cached_failure(now + Duration::from_secs(15)).is_none());
        assert!(matches!(
            gate.try_start(now + Duration::from_secs(15)),
            ConnectDecision::Start(_)
        ));
    }

    #[test]
    fn a_success_clears_a_previous_failure() {
        let gate = ConnectGate::default();
        let now = Instant::now();

        let ConnectDecision::Start(permit) = gate.try_start(now) else {
            panic!("expected a permit");
        };
        permit.failed(failure(), now);

        let later = now + DEFAULT_RETRY_INTERVAL;
        let ConnectDecision::Start(permit) = gate.try_start(later) else {
            panic!("expected a permit once the backoff expired");
        };
        permit.succeeded();

        assert_eq!(gate.cached_failure(later), None);
    }

    #[test]
    fn a_failure_during_backoff_beats_the_in_flight_check() {
        let gate = ConnectGate::default();
        let now = Instant::now();

        let ConnectDecision::Start(first) = gate.try_start(now) else {
            panic!("expected a permit");
        };
        first.failed(failure(), now);

        let ConnectDecision::Start(second) = gate.try_start(now + DEFAULT_RETRY_INTERVAL) else {
            panic!("expected a permit");
        };
        assert!(matches!(
            gate.try_start(now + DEFAULT_RETRY_INTERVAL),
            ConnectDecision::AlreadyInFlight
        ));
        drop(second);
    }
}
