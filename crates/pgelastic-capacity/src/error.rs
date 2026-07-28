//! The error taxonomy. It is API surface, not diagnostics.
//!
//! Every refusal the allocator can produce carries an [`ErrorCode`], and the
//! codes split three ways — *your ceiling*, *the pool's ceiling*, *transient* —
//! so a client library can write retry logic without parsing message text.

use std::fmt;
use std::time::Duration;

use crate::config::Ratio;
use crate::types::TenantId;

/// The wire-visible code attached to every refusal.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ErrorCode {
    /// Tenant hit its own ceiling — raise `burstable`.
    Pge1928,
    /// Pool is full — scale the pool.
    Pge1936,
    /// Within limits, but the pool is busy — retry.
    Pge1929,
    /// Storage quota exhausted — writes fail, `SELECT`/`DELETE` are unaffected.
    Pge0544,
    /// Admission queue timeout.
    Pge1024,
    /// Migration cutover.
    Pge1613,
}

impl ErrorCode {
    pub const ALL: [Self; 6] = [
        Self::Pge1928,
        Self::Pge1936,
        Self::Pge1929,
        Self::Pge0544,
        Self::Pge1024,
        Self::Pge1613,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pge1928 => "PGE1928",
            Self::Pge1936 => "PGE1936",
            Self::Pge1929 => "PGE1929",
            Self::Pge0544 => "PGE0544",
            Self::Pge1024 => "PGE1024",
            Self::Pge1613 => "PGE1613",
        }
    }

    pub const fn sqlstate(self) -> &'static str {
        match self {
            Self::Pge1928 => "53300",
            Self::Pge1936 | Self::Pge1929 | Self::Pge1024 => "53400",
            Self::Pge0544 => "53100",
            Self::Pge1613 => "57P01",
        }
    }

    /// The Azure SQL error this code is modelled on, where one exists.
    pub const fn azure_equivalent(self) -> Option<u32> {
        match self {
            Self::Pge1928 => Some(10928),
            Self::Pge1936 => Some(10936),
            Self::Pge1929 => Some(10929),
            Self::Pge0544 => Some(40544),
            Self::Pge1613 => Some(40613),
            Self::Pge1024 => None,
        }
    }

    /// Whether an unmodified retry of the same request can succeed.
    ///
    /// `Pge1928` and `Pge1936` are false because both need an operator action
    /// first — raise `burstable`, or scale the pool. Retrying either just burns
    /// the client's budget against a ceiling that has not moved.
    pub const fn retryable(self) -> bool {
        match self {
            Self::Pge1929 | Self::Pge1024 | Self::Pge1613 => true,
            Self::Pge1928 | Self::Pge1936 | Self::Pge0544 => false,
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why admission refused, naming the limit that blocked it.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum DenialReason {
    #[error("tenant {tenant} is at its burst ceiling ({live}/{burstable} backend connections)")]
    TenantCap {
        tenant: TenantId,
        live: u32,
        burstable: u32,
    },

    #[error("tenant {tenant} is at its client connection limit ({live}/{max})")]
    TenantClientCap {
        tenant: TenantId,
        live: u32,
        max: u32,
    },

    #[error("pool is full ({live}/{total} backend connections)")]
    PoolCapacity { live: u32, total: u32 },

    #[error("pool is at its client connection limit ({live}/{max})")]
    PoolClientCap { live: u32, max: u32 },

    #[error("tenant {tenant} admission queue is full ({queued}/{limit})")]
    QueueFull {
        tenant: TenantId,
        queued: u32,
        limit: u32,
    },

    #[error("tenant {tenant} has no cancel credit left ({in_flight}/{limit})")]
    CancelCredit {
        tenant: TenantId,
        in_flight: u32,
        limit: u32,
    },

    #[error("tenant {tenant} storage quota exhausted ({used}/{quota} bytes)")]
    StorageQuota {
        tenant: TenantId,
        used: u64,
        quota: u64,
    },

    #[error("tenant {tenant} waited {waited:?} for admission")]
    AdmissionTimeout { tenant: TenantId, waited: Duration },

    #[error("tenant {tenant} is in migration cutover")]
    MigrationCutover { tenant: TenantId },
}

impl DenialReason {
    pub const fn code(&self) -> ErrorCode {
        match self {
            Self::TenantCap { .. } | Self::TenantClientCap { .. } => ErrorCode::Pge1928,
            Self::PoolCapacity { .. } | Self::PoolClientCap { .. } => ErrorCode::Pge1936,
            Self::QueueFull { .. } | Self::CancelCredit { .. } => ErrorCode::Pge1929,
            Self::StorageQuota { .. } => ErrorCode::Pge0544,
            Self::AdmissionTimeout { .. } => ErrorCode::Pge1024,
            Self::MigrationCutover { .. } => ErrorCode::Pge1613,
        }
    }

    pub const fn sqlstate(&self) -> &'static str {
        self.code().sqlstate()
    }
}

/// Why a client connection was refused before it ever reached the ladder.
///
/// An unknown tenant is a routing failure, not a capacity decision, so it is
/// deliberately outside [`DenialReason`] and outside the code table.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum ConnectRejection {
    #[error("no tenant {0} is bound to this pool")]
    UnknownTenant(TenantId),
    #[error(transparent)]
    Denied(#[from] DenialReason),
}

/// Why a configuration was rejected at admission.
///
/// These are the checks the validating webhook mirrors: a tenant whose
/// guarantee cannot be honoured is *rejected*, never silently degraded.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("guarantees over-commit the pool: {reserved} reserved > {allocatable} allocatable")]
    OverCommitted { reserved: u64, allocatable: u32 },

    #[error("tenant {tenant}: burstable {burstable} is below guaranteed {guaranteed}")]
    BurstBelowGuarantee {
        tenant: TenantId,
        guaranteed: u32,
        burstable: u32,
    },

    #[error("oversubscription ratio {observed} exceeds the pool ceiling {max}")]
    Oversubscribed { observed: Ratio, max: Ratio },

    #[error("headroomPercent {headroom_percent} is outside 0..=50")]
    HeadroomOutOfRange { headroom_percent: u8 },

    #[error("tenant {tenant}: weight {weight} is outside 1..=10000")]
    WeightOutOfRange { tenant: TenantId, weight: u32 },

    #[error("tenant {tenant}: priority {priority} is outside 0..=1000000")]
    PriorityOutOfRange { tenant: TenantId, priority: u32 },

    #[error("backendConnections must be greater than zero")]
    ZeroCapacity,

    #[error("tenant {0} is already bound to this pool")]
    DuplicateTenant(TenantId),

    #[error("no tenant {0} is bound to this pool")]
    UnknownTenant(TenantId),

    #[error("cannot shrink {scope} to {target} while {live} connections are live; drain first")]
    ShrinkBelowLive {
        scope: ShrinkScope,
        target: u32,
        live: u32,
    },
}

/// Which ceiling a rejected shrink was trying to lower.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ShrinkScope {
    PoolCapacity,
    TenantBurstable,
}

impl fmt::Display for ShrinkScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PoolCapacity => f.write_str("backendConnections"),
            Self::TenantBurstable => f.write_str("burstable"),
        }
    }
}
