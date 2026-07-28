//! Pool and tenant configuration, and the admission-time invariant check.

use std::cmp::Ordering;
use std::fmt;
use std::time::Duration;

use crate::error::ConfigError;
use crate::types::TenantId;

/// Upper bound on the workload class `weight`.
pub const MAX_WEIGHT: u32 = 10_000;
/// Upper bound on the workload class `priority`.
pub const MAX_PRIORITY: u32 = 1_000_000;
/// Upper bound on `headroomPercent`.
pub const MAX_HEADROOM_PERCENT: u8 = 50;
/// Ceiling on the per-tenant cancel credit pool, before the `burstable` clamp.
pub const CANCEL_CREDIT_CAP: u32 = 8;

/// How a pooled backend is shared between clients.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum PoolMode {
    /// One backend per client connection, for the client's whole session.
    Session,
    #[default]
    /// One backend per transaction. A held backend is one unit of
    /// work-in-progress, which is what makes the capacity unit meaningful.
    Transaction,
    /// One backend per statement.
    Statement,
}

/// Derived from the guaranteed-vs-burstable relationship, never declared.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum QosClass {
    Guaranteed,
    Burstable,
    BestEffort,
}

/// Cross-tenant queue ordering.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum AdmissionStrategy {
    #[default]
    WeightedDeficit,
    Fifo,
}

/// An exact non-negative rational, so published ratios never drift.
#[derive(Clone, Copy, Debug)]
pub struct Ratio {
    numerator: u64,
    denominator: u64,
}

impl Ratio {
    /// A zero denominator is clamped to one: infinite oversubscription is
    /// reported as "all of it", which fails every ceiling just the same.
    pub const fn new(numerator: u64, denominator: u64) -> Self {
        Self {
            numerator,
            denominator: if denominator == 0 { 1 } else { denominator },
        }
    }

    pub const fn from_integer(value: u64) -> Self {
        Self::new(value, 1)
    }

    pub const fn numerator(self) -> u64 {
        self.numerator
    }

    pub const fn denominator(self) -> u64 {
        self.denominator
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn as_f64(self) -> f64 {
        self.numerator as f64 / self.denominator as f64
    }
}

impl PartialEq for Ratio {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Ratio {}

impl PartialOrd for Ratio {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ratio {
    fn cmp(&self, other: &Self) -> Ordering {
        let left = u128::from(self.numerator) * u128::from(other.denominator);
        let right = u128::from(other.numerator) * u128::from(self.denominator);
        left.cmp(&right)
    }
}

impl fmt::Display for Ratio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.2}", self.as_f64())
    }
}

/// A tenant's effective capacity claim, after workload-class defaults and
/// per-tenant overrides have been merged by the controller.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TenantSpec {
    pub guaranteed: u32,
    pub burstable: u32,
    pub weight: u32,
    pub priority: u32,
    pub max_client_connections: u32,
    pub storage_bytes: u64,
}

impl Default for TenantSpec {
    fn default() -> Self {
        Self {
            guaranteed: 0,
            burstable: 8,
            weight: 100,
            priority: 1_000,
            max_client_connections: 200,
            storage_bytes: 20 << 30,
        }
    }
}

impl TenantSpec {
    pub const fn qos_class(self) -> QosClass {
        if self.guaranteed == 0 {
            QosClass::BestEffort
        } else if self.guaranteed == self.burstable {
            QosClass::Guaranteed
        } else {
            QosClass::Burstable
        }
    }

    /// Client connections are a separate currency, but in session mode a client
    /// pins a backend for its whole session, so the two currencies collapse and
    /// the client limit cannot exceed the burst ceiling.
    pub const fn effective_max_client_connections(self, mode: PoolMode) -> u32 {
        match mode {
            PoolMode::Session if self.max_client_connections > self.burstable => self.burstable,
            _ => self.max_client_connections,
        }
    }

    pub fn validate(self, tenant: &TenantId) -> Result<(), ConfigError> {
        if self.burstable < self.guaranteed {
            return Err(ConfigError::BurstBelowGuarantee {
                tenant: tenant.clone(),
                guaranteed: self.guaranteed,
                burstable: self.burstable,
            });
        }
        if self.weight == 0 || self.weight > MAX_WEIGHT {
            return Err(ConfigError::WeightOutOfRange {
                tenant: tenant.clone(),
                weight: self.weight,
            });
        }
        if self.priority > MAX_PRIORITY {
            return Err(ConfigError::PriorityOutOfRange {
                tenant: tenant.clone(),
                priority: self.priority,
            });
        }
        Ok(())
    }
}

/// The pool's capacity envelope.
///
/// `backend_connections` is derived by the operator from
/// `max_connections − superuser_reserved_connections − replication slots −
/// operator overhead`, summed over the pool's instances. It is never invented.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PoolSpec {
    pub backend_connections: u32,
    pub headroom_percent: u8,
    pub max_client_connections: u32,
    pub max_oversubscription: Option<Ratio>,
    pub mode: PoolMode,
    /// File descriptors available to the proxy for client sockets. Client
    /// connections are bounded by this, not by `max_connections`.
    pub fd_budget: Option<u32>,
}

impl Default for PoolSpec {
    fn default() -> Self {
        Self {
            backend_connections: 900,
            headroom_percent: 25,
            max_client_connections: 12_000,
            max_oversubscription: Some(Ratio::from_integer(12)),
            mode: PoolMode::Transaction,
            fd_budget: None,
        }
    }
}

impl PoolSpec {
    /// `total × (1 − headroomPercent/100)`, floored.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "allocatable <= backend_connections"
    )]
    pub const fn allocatable(&self) -> u32 {
        let total = self.backend_connections as u64;
        let allocatable = total * (100 - self.headroom_percent as u64) / 100;
        allocatable as u32
    }

    pub fn effective_max_client_connections(&self) -> u32 {
        match self.fd_budget {
            Some(fds) => self.max_client_connections.min(fds),
            None => self.max_client_connections,
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.backend_connections == 0 {
            return Err(ConfigError::ZeroCapacity);
        }
        if self.headroom_percent > MAX_HEADROOM_PERCENT {
            return Err(ConfigError::HeadroomOutOfRange {
                headroom_percent: self.headroom_percent,
            });
        }
        Ok(())
    }
}

/// Queue shape and admission deadlines.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct AdmissionSpec {
    pub strategy: AdmissionStrategy,
    pub queue_depth_per_tenant: u32,
    pub max_wait: Duration,
}

impl Default for AdmissionSpec {
    fn default() -> Self {
        Self {
            strategy: AdmissionStrategy::WeightedDeficit,
            queue_depth_per_tenant: 64,
            max_wait: Duration::from_secs(30),
        }
    }
}

/// The pool ledger, mirroring `PgElasticPool.status.capacity`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Reservations {
    pub total: u32,
    pub allocatable: u32,
    pub reserved: u32,
    pub available: u32,
    pub committed_burst: u64,
    pub oversubscription: Ratio,
}

/// The admission-time invariant: `Σ guaranteed ≤ total × (1 − headroom)`.
///
/// This is the function the validating webhook's logic mirrors. A configuration
/// that fails it is rejected outright — a guarantee that cannot be honoured is
/// not a guarantee, and degrading it silently is the failure mode the whole
/// capacity model exists to avoid.
pub fn check_reservations<'a, I>(pool: &PoolSpec, tenants: I) -> Result<Reservations, ConfigError>
where
    I: IntoIterator<Item = (&'a TenantId, TenantSpec)>,
{
    pool.validate()?;

    let mut reserved: u64 = 0;
    let mut committed_burst: u64 = 0;
    for (id, spec) in tenants {
        spec.validate(id)?;
        reserved += u64::from(spec.guaranteed);
        committed_burst += u64::from(spec.burstable);
    }

    let allocatable = pool.allocatable();
    if reserved > u64::from(allocatable) {
        return Err(ConfigError::OverCommitted {
            reserved,
            allocatable,
        });
    }

    let oversubscription = Ratio::new(committed_burst, u64::from(allocatable));
    if let Some(max) = pool.max_oversubscription
        && oversubscription > max
    {
        return Err(ConfigError::Oversubscribed {
            observed: oversubscription,
            max,
        });
    }

    let reserved_u32 = u32::try_from(reserved).unwrap_or(u32::MAX);
    Ok(Reservations {
        total: pool.backend_connections,
        allocatable,
        reserved: reserved_u32,
        available: allocatable - reserved_u32,
        committed_burst,
        oversubscription,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(guaranteed: u32, burstable: u32) -> TenantSpec {
        TenantSpec {
            guaranteed,
            burstable,
            ..TenantSpec::default()
        }
    }

    #[test]
    fn allocatable_is_total_less_headroom() {
        let pool = PoolSpec {
            backend_connections: 900,
            headroom_percent: 25,
            ..PoolSpec::default()
        };
        assert_eq!(pool.allocatable(), 675);
    }

    #[test]
    fn qos_class_is_derived_from_the_guarantee_relationship() {
        assert_eq!(tenant(0, 8).qos_class(), QosClass::BestEffort);
        assert_eq!(tenant(4, 40).qos_class(), QosClass::Burstable);
        assert_eq!(tenant(4, 4).qos_class(), QosClass::Guaranteed);
    }

    #[test]
    fn session_mode_clamps_the_client_limit_to_the_burst_ceiling() {
        let spec = TenantSpec {
            burstable: 40,
            max_client_connections: 500,
            ..TenantSpec::default()
        };
        assert_eq!(spec.effective_max_client_connections(PoolMode::Session), 40);
        assert_eq!(
            spec.effective_max_client_connections(PoolMode::Transaction),
            500
        );
    }

    #[test]
    fn the_ledger_matches_the_published_status_fields() {
        let pool = PoolSpec {
            backend_connections: 900,
            headroom_percent: 25,
            max_oversubscription: None,
            ..PoolSpec::default()
        };
        let a = TenantId::from("a");
        let b = TenantId::from("b");
        let ledger =
            check_reservations(&pool, [(&a, tenant(200, 2000)), (&b, tenant(10, 140))]).unwrap();

        assert_eq!(ledger.allocatable, 675);
        assert_eq!(ledger.reserved, 210);
        assert_eq!(ledger.available, 465);
        assert_eq!(ledger.committed_burst, 2140);
        assert_eq!(ledger.oversubscription.to_string(), "3.17");
    }

    #[test]
    fn an_over_committed_config_is_rejected() {
        let pool = PoolSpec {
            backend_connections: 100,
            headroom_percent: 25,
            max_oversubscription: None,
            ..PoolSpec::default()
        };
        let a = TenantId::from("a");
        let b = TenantId::from("b");
        let err =
            check_reservations(&pool, [(&a, tenant(70, 70)), (&b, tenant(10, 10))]).unwrap_err();
        assert_eq!(
            err,
            ConfigError::OverCommitted {
                reserved: 80,
                allocatable: 75
            }
        );
    }

    #[test]
    fn an_oversubscription_ceiling_is_enforced() {
        let pool = PoolSpec {
            backend_connections: 100,
            headroom_percent: 0,
            max_oversubscription: Some(Ratio::from_integer(4)),
            ..PoolSpec::default()
        };
        let a = TenantId::from("a");
        assert!(check_reservations(&pool, [(&a, tenant(0, 400))]).is_ok());
        assert!(matches!(
            check_reservations(&pool, [(&a, tenant(0, 401))]),
            Err(ConfigError::Oversubscribed { .. })
        ));
    }
}
