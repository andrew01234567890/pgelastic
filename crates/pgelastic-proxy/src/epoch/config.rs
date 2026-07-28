//! The fence's reaction deadline, derived from the lease rather than invented.
//!
//! A candidate cannot take over a held Lease until `leaseDuration` has elapsed
//! without renewal, so promotion is impossible before T+`leaseDuration`. The
//! fence must have stopped admitting on the old epoch by T+`retryPeriod`. With
//! CNPG's parameters that is *fence at T+2 s, promotion impossible before
//! T+15 s, 13 s of margin*.
//!
//! Both halves live in [`FenceTiming`] because they are one decision.
//! Shortening `leaseDuration` **must** shorten the fence deadline in lockstep,
//! and the way to make that impossible to get wrong is to derive the deadline
//! from `retryPeriod`, derive the floor from `leaseDuration`, and validate the
//! relationship between them here rather than trusting two numbers in two files
//! to be edited together.

use std::time::Duration;

use serde::Deserialize;

/// CNPG's lease parameters, which pgelastic adopts unchanged.
const DEFAULT_LEASE_DURATION_MS: u64 = 15_000;
const DEFAULT_RENEW_DEADLINE_MS: u64 = 10_000;
const DEFAULT_RETRY_PERIOD_MS: u64 = 2_000;

/// The lease parameters the fence's deadlines are derived from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FenceTiming {
    /// How long a held Lease survives without renewal. Promotion is impossible
    /// before this has elapsed, which is what buys the fence its margin.
    #[serde(default = "default_lease_duration_ms")]
    pub lease_duration_ms: u64,
    /// When the holder gives up renewing and stops the postmaster.
    #[serde(default = "default_renew_deadline_ms")]
    pub renew_deadline_ms: u64,
    /// The renewal interval, and therefore the fence's reaction deadline.
    #[serde(default = "default_retry_period_ms")]
    pub retry_period_ms: u64,
}

impl Default for FenceTiming {
    fn default() -> Self {
        Self {
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
            renew_deadline_ms: DEFAULT_RENEW_DEADLINE_MS,
            retry_period_ms: DEFAULT_RETRY_PERIOD_MS,
        }
    }
}

impl FenceTiming {
    pub const fn lease_duration(self) -> Duration {
        Duration::from_millis(self.lease_duration_ms)
    }

    pub const fn renew_deadline(self) -> Duration {
        Duration::from_millis(self.renew_deadline_ms)
    }

    pub const fn retry_period(self) -> Duration {
        Duration::from_millis(self.retry_period_ms)
    }

    /// By when the proxy must have stopped admitting on the superseded epoch.
    ///
    /// One `retryPeriod`: that is the interval at which the holder proves it
    /// still holds the Lease, so it is the shortest interval after which a
    /// missed renewal is observable at all.
    pub const fn fence_deadline(self) -> Duration {
        self.retry_period()
    }

    /// The earliest instant a promotion can possibly happen.
    pub const fn promotion_floor(self) -> Duration {
        self.lease_duration()
    }

    /// How long the fence has to spare. Positive by construction — see
    /// [`FenceTiming::validate`].
    pub fn margin(self) -> Duration {
        self.promotion_floor().saturating_sub(self.fence_deadline())
    }

    /// CNPG's admission rules, plus the one the fence itself depends on.
    ///
    /// The last check is not redundant with the others in spirit even though it
    /// follows from them arithmetically: it is the property the fence's whole
    /// design point rests on, so it is asserted where it is relied upon rather
    /// than left as a corollary a future edit could quietly break.
    pub fn validate(self) -> Result<(), FenceTimingError> {
        if self.lease_duration_ms == 0 || self.renew_deadline_ms == 0 || self.retry_period_ms == 0 {
            return Err(FenceTimingError::Zero);
        }
        if self.lease_duration_ms <= self.renew_deadline_ms {
            return Err(FenceTimingError::LeaseNotLongerThanRenew {
                lease_duration: self.lease_duration(),
                renew_deadline: self.renew_deadline(),
            });
        }
        if self.renew_deadline_ms <= self.retry_period_ms {
            return Err(FenceTimingError::RenewNotLongerThanRetry {
                renew_deadline: self.renew_deadline(),
                retry_period: self.retry_period(),
            });
        }
        if 5 * self.renew_deadline_ms <= 6 * self.retry_period_ms {
            return Err(FenceTimingError::TooFewRenewalAttempts {
                renew_deadline: self.renew_deadline(),
                retry_period: self.retry_period(),
            });
        }
        if self.fence_deadline() >= self.promotion_floor() {
            return Err(FenceTimingError::NoMargin {
                fence_deadline: self.fence_deadline(),
                promotion_floor: self.promotion_floor(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FenceTimingError {
    #[error("every lease parameter must be greater than zero")]
    Zero,

    #[error(
        "leaseDuration ({lease_duration:?}) must exceed renewDeadline ({renew_deadline:?}), \
         or the holder never gets a chance to stand down before it is preempted"
    )]
    LeaseNotLongerThanRenew {
        lease_duration: Duration,
        renew_deadline: Duration,
    },

    #[error(
        "renewDeadline ({renew_deadline:?}) must exceed retryPeriod ({retry_period:?}), \
         or the holder gets no renewal attempt at all"
    )]
    RenewNotLongerThanRetry {
        renew_deadline: Duration,
        retry_period: Duration,
    },

    #[error(
        "5 x renewDeadline ({renew_deadline:?}) must exceed 6 x retryPeriod ({retry_period:?}), \
         or a single slow API call loses the lease"
    )]
    TooFewRenewalAttempts {
        renew_deadline: Duration,
        retry_period: Duration,
    },

    #[error(
        "the fence deadline ({fence_deadline:?}) must fall before the earliest possible \
         promotion ({promotion_floor:?}); shortening leaseDuration must shorten retryPeriod \
         with it"
    )]
    NoMargin {
        fence_deadline: Duration,
        promotion_floor: Duration,
    },
}

fn default_lease_duration_ms() -> u64 {
    DEFAULT_LEASE_DURATION_MS
}
fn default_renew_deadline_ms() -> u64 {
    DEFAULT_RENEW_DEADLINE_MS
}
fn default_retry_period_ms() -> u64 {
    DEFAULT_RETRY_PERIOD_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_cnpgs_lease_and_leaves_thirteen_seconds_of_margin() {
        let timing = FenceTiming::default();
        timing.validate().unwrap();
        assert_eq!(timing.lease_duration(), Duration::from_secs(15));
        assert_eq!(timing.renew_deadline(), Duration::from_secs(10));
        assert_eq!(timing.retry_period(), Duration::from_secs(2));
        assert_eq!(timing.fence_deadline(), Duration::from_secs(2));
        assert_eq!(timing.promotion_floor(), Duration::from_secs(15));
        assert_eq!(timing.margin(), Duration::from_secs(13));
    }

    #[test]
    fn shortening_the_lease_shortens_the_fence_deadline_in_lockstep() {
        let fast = FenceTiming {
            lease_duration_ms: 3_000,
            renew_deadline_ms: 2_000,
            retry_period_ms: 400,
        };
        fast.validate().unwrap();
        assert_eq!(fast.fence_deadline(), Duration::from_millis(400));
        assert!(fast.fence_deadline() < FenceTiming::default().fence_deadline());
        assert!(fast.margin() < FenceTiming::default().margin());
    }

    #[test]
    fn a_lease_no_longer_than_the_renew_deadline_is_refused() {
        let timing = FenceTiming {
            lease_duration_ms: 10_000,
            renew_deadline_ms: 10_000,
            retry_period_ms: 2_000,
        };
        assert!(matches!(
            timing.validate(),
            Err(FenceTimingError::LeaseNotLongerThanRenew { .. })
        ));
    }

    #[test]
    fn a_renew_deadline_no_longer_than_the_retry_period_is_refused() {
        let timing = FenceTiming {
            lease_duration_ms: 15_000,
            renew_deadline_ms: 2_000,
            retry_period_ms: 2_000,
        };
        assert!(matches!(
            timing.validate(),
            Err(FenceTimingError::RenewNotLongerThanRetry { .. })
        ));
    }

    #[test]
    fn a_retry_period_that_allows_too_few_attempts_is_refused() {
        let timing = FenceTiming {
            lease_duration_ms: 15_000,
            renew_deadline_ms: 10_000,
            retry_period_ms: 9_000,
        };
        assert!(matches!(
            timing.validate(),
            Err(FenceTimingError::TooFewRenewalAttempts { .. })
        ));
    }

    #[test]
    fn a_zero_parameter_is_refused_rather_than_treated_as_no_deadline() {
        for timing in [
            FenceTiming {
                lease_duration_ms: 0,
                ..FenceTiming::default()
            },
            FenceTiming {
                renew_deadline_ms: 0,
                ..FenceTiming::default()
            },
            FenceTiming {
                retry_period_ms: 0,
                ..FenceTiming::default()
            },
        ] {
            assert_eq!(timing.validate(), Err(FenceTimingError::Zero));
        }
    }

    /// The margin is what the whole design point rests on: if the fence
    /// deadline ever reached the promotion floor there would be an interval in
    /// which two postmasters could both accept writes through this proxy.
    #[test]
    fn every_configuration_that_validates_leaves_the_fence_ahead_of_promotion() {
        for lease in [1_000u64, 3_000, 15_000, 60_000] {
            for renew in [500u64, 2_000, 10_000] {
                for retry in [100u64, 400, 2_000, 9_000] {
                    let timing = FenceTiming {
                        lease_duration_ms: lease,
                        renew_deadline_ms: renew,
                        retry_period_ms: retry,
                    };
                    if timing.validate().is_ok() {
                        assert!(
                            timing.fence_deadline() < timing.promotion_floor(),
                            "{timing:?} validated with no margin"
                        );
                        assert!(timing.margin() > Duration::ZERO);
                    }
                }
            }
        }
    }
}
