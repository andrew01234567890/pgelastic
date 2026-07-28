//! The injected time source.
//!
//! Nothing in this crate reads a clock directly. Every deadline is evaluated
//! against a [`Clock`] handed to the allocator at construction, so a test can
//! drive an admission timeout in a nanosecond of wall time.

use std::cell::Cell;
use std::fmt;
use std::time::{Duration, Instant};

/// A point on the clock's own monotonic scale, in nanoseconds since its origin.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const ZERO: Self = Self(0);

    pub fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(1_000_000))
    }

    pub fn saturating_since(self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    #[must_use]
    pub fn saturating_add(self, delta: Duration) -> Self {
        Self(self.0.saturating_add(nanos_of(delta)))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

fn nanos_of(delta: Duration) -> u64 {
    u64::try_from(delta.as_nanos()).unwrap_or(u64::MAX)
}

/// A monotonic time source.
pub trait Clock: fmt::Debug {
    fn now(&self) -> Timestamp;
}

/// The production clock: [`Instant`] measured from the allocator's construction.
#[derive(Debug, Clone, Copy)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp(nanos_of(self.origin.elapsed()))
    }
}

/// A clock that only moves when a test moves it.
///
/// The [`Cell`] is the only interior mutability in the crate. It makes
/// `ManualClock` — and any allocator holding one — `!Sync`, so it cannot be
/// shared across threads even by accident.
#[derive(Debug, Default)]
pub struct ManualClock {
    now: Cell<u64>,
}

impl ManualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn advance(&self, delta: Duration) {
        self.now.set(self.now.get().saturating_add(nanos_of(delta)));
    }

    pub fn set(&self, at: Timestamp) {
        self.now.set(at.0);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.now.get())
    }
}

impl<C: Clock + ?Sized> Clock for &C {
    fn now(&self) -> Timestamp {
        (**self).now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_starts_at_zero_and_only_moves_when_advanced() {
        let clock = ManualClock::new();
        assert_eq!(clock.now(), Timestamp::ZERO);

        clock.advance(Duration::from_millis(30));
        assert_eq!(clock.now(), Timestamp::from_millis(30));
        assert_eq!(clock.now(), Timestamp::from_millis(30));
    }

    #[test]
    fn elapsed_saturates_rather_than_wrapping() {
        assert_eq!(Timestamp(5).saturating_since(Timestamp(9)), Duration::ZERO);
    }
}
