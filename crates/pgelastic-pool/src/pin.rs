//! Pinning tripwires and the budget accounting that makes pinning explainable.
//!
//! Some session state cannot be scrubbed by any reset. The converged industry
//! answer — RDS Proxy's — is to *pin* rather than to scrub: keep the link bound
//! to the client that dirtied it until that client goes away. Pinning costs
//! throughput; leaking loses tenant data.
//!
//! A pinned link still occupies a real backend connection but is no longer
//! available to anyone else, so it is removed from the elastic budget and
//! gauged separately. Without that, the pool's effective ceiling silently drops
//! and nobody can explain why.

use std::fmt;

/// Session state that no reset can remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PinReason {
    /// `LISTEN` — a leaked registration delivers another tenant's payloads.
    Listen,
    /// `DECLARE ... WITH HOLD` — the cursor outlives its transaction.
    HoldCursor,
    /// `CREATE TEMP TABLE ... ON COMMIT PRESERVE ROWS` / `DELETE ROWS`.
    TempTable,
    /// `pg_advisory_lock` at session scope.
    SessionAdvisoryLock,
    /// `LOAD` — a loaded module cannot be unloaded.
    Load,
    /// `setseed()`. `seed` is `GUC_NO_RESET | GUC_NO_RESET_ALL`, so it is
    /// literally unresettable and pinning is not enough.
    SetSeed,
    /// `PREPARE TRANSACTION` left a two-phase transaction behind.
    PreparedTransaction,
    /// `dblink_connect` opened an outbound connection owned by the session.
    Dblink,
    /// The extended-query pipeline desynchronised; the link's state is unknown.
    DesyncedPipeline,
    /// A COPY subprotocol was still open when it should not have been.
    OpenCopy,
    /// An `ErrorResponse` that could not be attributed to any request.
    UnattributableError,
}

impl PinReason {
    /// Whether the link is still unclean once the pinning client has gone.
    ///
    /// `DISCARD ALL` does remove listens, held cursors, temp tables and session
    /// advisory locks, so those reasons pin only for as long as the client that
    /// created them expects them to survive. The rest outlive `DISCARD ALL`:
    /// `seed` is `GUC_NO_RESET`, a loaded module cannot be unloaded, an outbound
    /// `dblink` connection belongs to the session, and a desynchronised pipeline
    /// or an unattributable error means the link's state is simply unknown.
    pub fn forces_close(self) -> bool {
        !matches!(
            self,
            Self::Listen | Self::HoldCursor | Self::TempTable | Self::SessionAdvisoryLock
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Listen => "listen",
            Self::HoldCursor => "hold_cursor",
            Self::TempTable => "temp_table",
            Self::SessionAdvisoryLock => "session_advisory_lock",
            Self::Load => "load",
            Self::SetSeed => "set_seed",
            Self::PreparedTransaction => "prepared_transaction",
            Self::Dblink => "dblink",
            Self::DesyncedPipeline => "desynced_pipeline",
            Self::OpenCopy => "open_copy",
            Self::UnattributableError => "unattributable_error",
        }
    }

    /// Every reason, in declaration order, for gauge labelling.
    pub const ALL: [Self; 11] = [
        Self::Listen,
        Self::HoldCursor,
        Self::TempTable,
        Self::SessionAdvisoryLock,
        Self::Load,
        Self::SetSeed,
        Self::PreparedTransaction,
        Self::Dblink,
        Self::DesyncedPipeline,
        Self::OpenCopy,
        Self::UnattributableError,
    ];
}

impl fmt::Display for PinReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The split between reusable and pinned backend connections under one limit.
///
/// `limit` is the total number of backend connections the pool may hold. Pinned
/// links are subtracted from it rather than counted inside it, so
/// [`elastic_limit`](Self::elastic_limit) is what the reusable pool can actually
/// grow to and the shortfall is attributable to a named [`PinReason`].
#[derive(Debug, Clone)]
pub struct BudgetLedger {
    limit: u32,
    elastic: u32,
    pinned: [u32; PinReason::ALL.len()],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetError {
    /// The limit is fully committed.
    Exhausted,
    /// Nothing was checked out to release or pin.
    NothingCheckedOut,
    /// No pinned link with that reason exists.
    NotPinned(PinReason),
}

impl fmt::Display for BudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted => f.write_str("backend budget exhausted"),
            Self::NothingCheckedOut => f.write_str("no elastic connection to account for"),
            Self::NotPinned(reason) => write!(f, "no connection pinned for {reason}"),
        }
    }
}

impl std::error::Error for BudgetError {}

impl BudgetLedger {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            elastic: 0,
            pinned: [0; PinReason::ALL.len()],
        }
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    pub fn set_limit(&mut self, limit: u32) {
        self.limit = limit;
    }

    /// Reusable connections currently open.
    pub fn elastic(&self) -> u32 {
        self.elastic
    }

    pub fn pinned(&self) -> u32 {
        self.pinned.iter().sum()
    }

    pub fn pinned_for(&self, reason: PinReason) -> u32 {
        self.pinned[index_of(reason)]
    }

    pub fn total(&self) -> u32 {
        self.elastic + self.pinned()
    }

    /// The ceiling the reusable pool can actually reach.
    pub fn elastic_limit(&self) -> u32 {
        self.limit.saturating_sub(self.pinned())
    }

    pub fn available(&self) -> u32 {
        self.elastic_limit().saturating_sub(self.elastic)
    }

    pub fn open(&mut self) -> Result<(), BudgetError> {
        if self.available() == 0 {
            return Err(BudgetError::Exhausted);
        }
        self.elastic += 1;
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), BudgetError> {
        self.elastic = self
            .elastic
            .checked_sub(1)
            .ok_or(BudgetError::NothingCheckedOut)?;
        Ok(())
    }

    /// Moves an open connection out of the elastic pool.
    pub fn pin(&mut self, reason: PinReason) -> Result<(), BudgetError> {
        self.elastic = self
            .elastic
            .checked_sub(1)
            .ok_or(BudgetError::NothingCheckedOut)?;
        self.pinned[index_of(reason)] += 1;
        Ok(())
    }

    /// Returns a pinned connection to the elastic pool.
    pub fn unpin(&mut self, reason: PinReason) -> Result<(), BudgetError> {
        let slot = &mut self.pinned[index_of(reason)];
        *slot = slot.checked_sub(1).ok_or(BudgetError::NotPinned(reason))?;
        self.elastic += 1;
        Ok(())
    }

    /// Drops a pinned connection entirely.
    pub fn close_pinned(&mut self, reason: PinReason) -> Result<(), BudgetError> {
        let slot = &mut self.pinned[index_of(reason)];
        *slot = slot.checked_sub(1).ok_or(BudgetError::NotPinned(reason))?;
        Ok(())
    }
}

fn index_of(reason: PinReason) -> usize {
    PinReason::ALL
        .iter()
        .position(|candidate| *candidate == reason)
        .expect("PinReason::ALL is exhaustive")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_forces_a_close_rather_than_a_pin() {
        assert!(PinReason::SetSeed.forces_close());
    }

    #[test]
    fn listen_pins_but_does_not_close() {
        assert!(!PinReason::Listen.forces_close());
    }

    #[test]
    fn pinning_shrinks_the_elastic_ceiling() {
        let mut ledger = BudgetLedger::new(10);
        for _ in 0..4 {
            ledger.open().unwrap();
        }
        ledger.pin(PinReason::Listen).unwrap();

        assert_eq!(ledger.elastic(), 3);
        assert_eq!(ledger.pinned(), 1);
        assert_eq!(ledger.total(), 4);
        assert_eq!(ledger.elastic_limit(), 9);
        assert_eq!(ledger.available(), 6);
    }

    #[test]
    fn pinned_connections_are_counted_per_reason() {
        let mut ledger = BudgetLedger::new(10);
        ledger.open().unwrap();
        ledger.open().unwrap();
        ledger.pin(PinReason::Listen).unwrap();
        ledger.pin(PinReason::TempTable).unwrap();

        assert_eq!(ledger.pinned_for(PinReason::Listen), 1);
        assert_eq!(ledger.pinned_for(PinReason::TempTable), 1);
        assert_eq!(ledger.pinned_for(PinReason::Load), 0);
    }

    #[test]
    fn the_total_never_exceeds_the_limit_however_it_is_split() {
        let mut ledger = BudgetLedger::new(3);
        ledger.open().unwrap();
        ledger.pin(PinReason::Listen).unwrap();
        ledger.open().unwrap();
        ledger.open().unwrap();
        assert_eq!(ledger.total(), 3);
        assert_eq!(ledger.open(), Err(BudgetError::Exhausted));
    }

    #[test]
    fn unpinning_returns_the_connection_to_the_elastic_pool() {
        let mut ledger = BudgetLedger::new(4);
        ledger.open().unwrap();
        ledger.pin(PinReason::HoldCursor).unwrap();
        ledger.unpin(PinReason::HoldCursor).unwrap();
        assert_eq!(ledger.elastic(), 1);
        assert_eq!(ledger.pinned(), 0);
        assert_eq!(ledger.elastic_limit(), 4);
    }

    #[test]
    fn releasing_what_was_never_taken_is_an_error_not_an_underflow() {
        let mut ledger = BudgetLedger::new(1);
        assert_eq!(ledger.close(), Err(BudgetError::NothingCheckedOut));
        assert_eq!(
            ledger.pin(PinReason::Load),
            Err(BudgetError::NothingCheckedOut)
        );
        assert_eq!(
            ledger.unpin(PinReason::Load),
            Err(BudgetError::NotPinned(PinReason::Load))
        );
    }

    #[test]
    fn shrinking_the_limit_below_the_pinned_count_reports_no_headroom() {
        let mut ledger = BudgetLedger::new(4);
        ledger.open().unwrap();
        ledger.open().unwrap();
        ledger.pin(PinReason::Listen).unwrap();
        ledger.pin(PinReason::Listen).unwrap();
        ledger.set_limit(1);
        assert_eq!(ledger.elastic_limit(), 0);
        assert_eq!(ledger.available(), 0);
    }
}
