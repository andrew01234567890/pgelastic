//! Dirtiness tracking and the reset-on-release ladder.
//!
//! Dirtiness is tracked by **tainting**: the data plane marks the link when it
//! relays something that changes session state. The tempting alternative — read
//! the `CommandComplete` tag and reset when it says `SET` — is wrong three ways
//! over. It misses `set_config()`, whose tag is `SELECT`; it misses everything
//! inside a transaction, where the tags belong to the transaction; and it misses
//! `LISTEN`, `DECLARE ... WITH HOLD`, `CREATE TEMP TABLE` and
//! `pg_advisory_lock` entirely.
//!
//! `RESET ALL` is never used. `role`, `session_authorization` and `seed` carry
//! `GUC_NO_RESET_ALL`, so it silently fails to restore exactly the three
//! settings whose leakage matters most. `DISCARD ALL` is the scrub, and because
//! it is `PreventInTransactionBlock` (SQLSTATE 25001) it may only run after a
//! `ReadyForQuery` of `'I'` — hence the `ROLLBACK` that precedes it whenever the
//! link is left in `'T'` or `'E'`.

use std::fmt;

use pgelastic_wire::TransactionStatus;

use crate::pin::PinReason;

/// A kind of session state a reset can remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Taint {
    /// A GUC was assigned, by `SET`, by `set_config()` or by anything else.
    SessionParameter,
    /// A named prepared statement exists.
    PreparedStatement,
    /// A cursor is open.
    Cursor,
    /// Sequence state (`currval`) is visible.
    Sequence,
    /// Cached plans may be stale for the next client.
    PlanCache,
}

impl Taint {
    pub const ALL: [Self; 5] = [
        Self::SessionParameter,
        Self::PreparedStatement,
        Self::Cursor,
        Self::Sequence,
        Self::PlanCache,
    ];

    const fn bit(self) -> u8 {
        match self {
            Self::SessionParameter => 1 << 0,
            Self::PreparedStatement => 1 << 1,
            Self::Cursor => 1 << 2,
            Self::Sequence => 1 << 3,
            Self::PlanCache => 1 << 4,
        }
    }
}

/// The set of taints currently on a link.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TaintSet(u8);

impl TaintSet {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn insert(&mut self, taint: Taint) {
        self.0 |= taint.bit();
    }

    /// Folds another set in. Taints only ever accumulate until a scrub clears them.
    pub fn union(&mut self, other: Self) {
        self.0 |= other.0;
    }

    pub fn contains(self, taint: Taint) -> bool {
        self.0 & taint.bit() != 0
    }

    pub fn is_clean(self) -> bool {
        self.0 == 0
    }

    pub fn clear(&mut self) {
        self.0 = 0;
    }

    pub fn iter(self) -> impl Iterator<Item = Taint> {
        Taint::ALL.into_iter().filter(move |t| self.contains(*t))
    }
}

impl FromIterator<Taint> for TaintSet {
    fn from_iter<I: IntoIterator<Item = Taint>>(iter: I) -> Self {
        let mut set = Self::new();
        for taint in iter {
            set.insert(taint);
        }
        set
    }
}

/// How hard to scrub a link before it goes back to the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum ResetPolicy {
    /// Never scrub. A tainted link is closed rather than handed on, because the
    /// operator has opted out of the only mechanism that could clean it.
    None,
    /// Scrub only when the link is tainted.
    #[default]
    DirtyTracked,
    /// Scrub with the narrowest `DISCARD` subcommands the taints justify.
    SmartDiscard,
    /// `DISCARD ALL` on every release.
    DiscardAll,
    /// `DISCARD ALL` plus a round trip that proves the link still answers.
    Verified,
}

/// One statement in a reset plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResetStep {
    Rollback,
    CloseAll,
    DeallocateAll,
    DiscardPlans,
    DiscardSequences,
    DiscardAll,
    Verify,
}

impl ResetStep {
    /// The statement text.
    ///
    /// Every identifier is schema-qualified: an internally generated statement
    /// must not be resolvable through a client-influenced `search_path`
    /// (CVE-2025-12819).
    pub fn sql(self) -> &'static str {
        match self {
            Self::Rollback => "ROLLBACK",
            Self::CloseAll => "CLOSE ALL",
            Self::DeallocateAll => "DEALLOCATE ALL",
            Self::DiscardPlans => "DISCARD PLANS",
            Self::DiscardSequences => "DISCARD SEQUENCES",
            Self::DiscardAll => "DISCARD ALL",
            Self::Verify => "SELECT pg_catalog.pg_backend_pid()",
        }
    }

    /// Whether the backend refuses this statement inside a transaction block
    /// with SQLSTATE 25001.
    pub fn prevented_in_transaction_block(self) -> bool {
        matches!(
            self,
            Self::DiscardAll | Self::DiscardPlans | Self::DiscardSequences
        )
    }
}

impl fmt::Display for ResetStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.sql())
    }
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unscrubbable(reason) => write!(f, "unscrubbable state: {}", reason.as_str()),
            Self::ResetDisabled => f.write_str("the reset policy forbids scrubbing this link"),
        }
    }
}

/// Why a link is being closed instead of returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CloseReason {
    /// State that outlives `DISCARD ALL`.
    Unscrubbable(PinReason),
    /// The link is tainted and the policy forbids scrubbing it.
    ResetDisabled,
}

/// What happens to the link once the plan has run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResetDisposition {
    /// Return it to the pool.
    Reuse,
    /// Keep it bound to the client that dirtied it.
    Pin(PinReason),
    /// Close it.
    Close(CloseReason),
}

/// The state a link is being released from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseContext {
    pub tx_status: TransactionStatus,
    /// The pinning client has disconnected, so nothing is left to preserve
    /// session state for.
    pub client_gone: bool,
}

/// An ordered scrub, and what to do with the link afterwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetPlan {
    steps: Vec<ResetStep>,
    disposition: ResetDisposition,
}

impl ResetPlan {
    pub fn steps(&self) -> &[ResetStep] {
        &self.steps
    }

    pub fn disposition(&self) -> ResetDisposition {
        self.disposition
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Whether every 25001 statement is reached with the transaction closed.
    ///
    /// The ladder maintains this by construction; the check exists so the
    /// property can be asserted directly rather than inferred from step order.
    pub fn respects_transaction_block_rules(&self, start: TransactionStatus) -> bool {
        let mut in_transaction = start != TransactionStatus::Idle;
        for step in &self.steps {
            if step.prevented_in_transaction_block() && in_transaction {
                return false;
            }
            if *step == ResetStep::Rollback {
                in_transaction = false;
            }
        }
        true
    }
}

/// Builds the reset plan for a release.
///
/// A pin outranks everything: while the pinning client is still connected the
/// link is not scrubbed at all, because the client is entitled to the state it
/// created. Once that client is gone the pin either survives `DISCARD ALL`, in
/// which case the link is closed, or it does not, in which case the scrub is
/// forced to the full `DISCARD ALL` regardless of policy.
pub fn plan(
    policy: ResetPolicy,
    taint: TaintSet,
    pin: Option<PinReason>,
    context: ReleaseContext,
) -> ResetPlan {
    if let Some(reason) = pin {
        if !context.client_gone {
            return ResetPlan {
                steps: Vec::new(),
                disposition: ResetDisposition::Pin(reason),
            };
        }
        if reason.forces_close() {
            return ResetPlan {
                steps: Vec::new(),
                disposition: ResetDisposition::Close(CloseReason::Unscrubbable(reason)),
            };
        }
        return ResetPlan {
            steps: with_rollback(context.tx_status, vec![ResetStep::DiscardAll]),
            disposition: ResetDisposition::Reuse,
        };
    }

    let steps = match policy {
        ResetPolicy::None => {
            if taint.is_clean() {
                with_rollback(context.tx_status, Vec::new())
            } else {
                return ResetPlan {
                    steps: Vec::new(),
                    disposition: ResetDisposition::Close(CloseReason::ResetDisabled),
                };
            }
        }
        // No scrub-needed test, because there was never a case it decided. It read
        // `!taint.is_clean() || tx_status != Idle`, and when it was false the session was
        // both clean and idle - which is exactly when with_rollback(Idle, vec![]) returns the
        // empty vec the other arm returned by hand.
        ResetPolicy::DirtyTracked => with_rollback(
            context.tx_status,
            if taint.is_clean() {
                Vec::new()
            } else {
                vec![ResetStep::DiscardAll]
            },
        ),
        ResetPolicy::SmartDiscard => with_rollback(context.tx_status, smart_steps(taint)),
        ResetPolicy::DiscardAll => with_rollback(context.tx_status, vec![ResetStep::DiscardAll]),
        ResetPolicy::Verified => with_rollback(
            context.tx_status,
            vec![ResetStep::DiscardAll, ResetStep::Verify],
        ),
    };

    ResetPlan {
        steps,
        disposition: ResetDisposition::Reuse,
    }
}

/// A `SET` cannot be undone piecemeal without `RESET ALL`, which is banned, so a
/// session-parameter taint escalates the narrow ladder to the full scrub.
fn smart_steps(taint: TaintSet) -> Vec<ResetStep> {
    if taint.contains(Taint::SessionParameter) {
        return vec![ResetStep::DiscardAll];
    }
    let mut steps = Vec::new();
    if taint.contains(Taint::Cursor) {
        steps.push(ResetStep::CloseAll);
    }
    if taint.contains(Taint::PreparedStatement) {
        steps.push(ResetStep::DeallocateAll);
    }
    if taint.contains(Taint::PlanCache) {
        steps.push(ResetStep::DiscardPlans);
    }
    if taint.contains(Taint::Sequence) {
        steps.push(ResetStep::DiscardSequences);
    }
    steps
}

fn with_rollback(status: TransactionStatus, rest: Vec<ResetStep>) -> Vec<ResetStep> {
    if status == TransactionStatus::Idle {
        return rest;
    }
    let mut steps = Vec::with_capacity(rest.len() + 1);
    steps.push(ResetStep::Rollback);
    steps.extend(rest);
    steps
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idle() -> ReleaseContext {
        ReleaseContext {
            tx_status: TransactionStatus::Idle,
            client_gone: false,
        }
    }

    fn in_transaction() -> ReleaseContext {
        ReleaseContext {
            tx_status: TransactionStatus::Transaction,
            client_gone: false,
        }
    }

    #[test]
    fn a_clean_idle_link_is_reused_without_a_round_trip() {
        let plan = plan(ResetPolicy::DirtyTracked, TaintSet::new(), None, idle());
        assert!(plan.is_empty());
        assert_eq!(plan.disposition(), ResetDisposition::Reuse);
    }

    #[test]
    fn dirty_tracked_scrubs_only_when_tainted() {
        let taint = TaintSet::from_iter([Taint::SessionParameter]);
        let plan = plan(ResetPolicy::DirtyTracked, taint, None, idle());
        assert_eq!(plan.steps(), [ResetStep::DiscardAll]);
    }

    #[test]
    fn discard_all_scrubs_a_clean_link_too() {
        let plan = plan(ResetPolicy::DiscardAll, TaintSet::new(), None, idle());
        assert_eq!(plan.steps(), [ResetStep::DiscardAll]);
    }

    #[test]
    fn verified_adds_a_schema_qualified_round_trip() {
        let plan = plan(ResetPolicy::Verified, TaintSet::new(), None, idle());
        assert_eq!(plan.steps(), [ResetStep::DiscardAll, ResetStep::Verify]);
        assert_eq!(
            ResetStep::Verify.sql(),
            "SELECT pg_catalog.pg_backend_pid()"
        );
    }

    #[test]
    fn smart_discard_uses_the_narrowest_subcommands() {
        let taint = TaintSet::from_iter([Taint::Cursor, Taint::PreparedStatement]);
        let plan = plan(ResetPolicy::SmartDiscard, taint, None, idle());
        assert_eq!(
            plan.steps(),
            [ResetStep::CloseAll, ResetStep::DeallocateAll]
        );
    }

    #[test]
    fn smart_discard_escalates_when_a_guc_was_assigned() {
        let taint = TaintSet::from_iter([Taint::SessionParameter, Taint::Cursor]);
        let plan = plan(ResetPolicy::SmartDiscard, taint, None, idle());
        assert_eq!(plan.steps(), [ResetStep::DiscardAll]);
    }

    #[test]
    fn no_plan_ever_contains_reset_all() {
        for policy in [
            ResetPolicy::None,
            ResetPolicy::DirtyTracked,
            ResetPolicy::SmartDiscard,
            ResetPolicy::DiscardAll,
            ResetPolicy::Verified,
        ] {
            for taint in all_taint_sets() {
                let plan = plan(policy, taint, None, idle());
                for step in plan.steps() {
                    assert!(
                        !step.sql().contains("RESET ALL"),
                        "{policy:?} emitted {step}"
                    );
                }
            }
        }
    }

    #[test]
    fn an_open_transaction_is_rolled_back_before_any_discard() {
        let taint = TaintSet::from_iter([Taint::SessionParameter]);
        let plan = plan(ResetPolicy::DirtyTracked, taint, None, in_transaction());
        assert_eq!(plan.steps(), [ResetStep::Rollback, ResetStep::DiscardAll]);
    }

    #[test]
    fn a_failed_transaction_is_rolled_back_even_when_clean() {
        let context = ReleaseContext {
            tx_status: TransactionStatus::Failed,
            client_gone: false,
        };
        let plan = plan(ResetPolicy::DirtyTracked, TaintSet::new(), None, context);
        assert_eq!(plan.steps(), [ResetStep::Rollback]);
    }

    #[test]
    fn discard_is_never_issued_inside_a_transaction_block() {
        for policy in [
            ResetPolicy::None,
            ResetPolicy::DirtyTracked,
            ResetPolicy::SmartDiscard,
            ResetPolicy::DiscardAll,
            ResetPolicy::Verified,
        ] {
            for taint in all_taint_sets() {
                for status in [
                    TransactionStatus::Idle,
                    TransactionStatus::Transaction,
                    TransactionStatus::Failed,
                ] {
                    for client_gone in [false, true] {
                        for pin in pin_options() {
                            let context = ReleaseContext {
                                tx_status: status,
                                client_gone,
                            };
                            let plan = plan(policy, taint, pin, context);
                            assert!(
                                plan.respects_transaction_block_rules(status),
                                "{policy:?} {taint:?} {status:?} {pin:?} produced {:?}",
                                plan.steps()
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_pinned_link_is_not_scrubbed_while_its_client_is_connected() {
        let taint = TaintSet::from_iter([Taint::SessionParameter]);
        let plan = plan(
            ResetPolicy::DiscardAll,
            taint,
            Some(PinReason::Listen),
            idle(),
        );
        assert!(plan.is_empty());
        assert_eq!(plan.disposition(), ResetDisposition::Pin(PinReason::Listen));
    }

    #[test]
    fn a_scrubbable_pin_is_cleaned_up_once_the_client_goes() {
        let context = ReleaseContext {
            tx_status: TransactionStatus::Idle,
            client_gone: true,
        };
        let plan = plan(
            ResetPolicy::None,
            TaintSet::new(),
            Some(PinReason::Listen),
            context,
        );
        assert_eq!(plan.steps(), [ResetStep::DiscardAll]);
        assert_eq!(plan.disposition(), ResetDisposition::Reuse);
    }

    #[test]
    fn setseed_closes_the_link_because_seed_cannot_be_reset() {
        let context = ReleaseContext {
            tx_status: TransactionStatus::Idle,
            client_gone: true,
        };
        let plan = plan(
            ResetPolicy::Verified,
            TaintSet::new(),
            Some(PinReason::SetSeed),
            context,
        );
        assert!(plan.is_empty());
        assert_eq!(
            plan.disposition(),
            ResetDisposition::Close(CloseReason::Unscrubbable(PinReason::SetSeed))
        );
    }

    #[test]
    fn opting_out_of_resets_closes_tainted_links_rather_than_leaking_them() {
        let taint = TaintSet::from_iter([Taint::SessionParameter]);
        let plan = plan(ResetPolicy::None, taint, None, idle());
        assert_eq!(
            plan.disposition(),
            ResetDisposition::Close(CloseReason::ResetDisabled)
        );
    }

    #[test]
    fn taints_round_trip_through_the_set() {
        let mut taint = TaintSet::new();
        assert!(taint.is_clean());
        taint.insert(Taint::Cursor);
        taint.insert(Taint::Cursor);
        assert!(taint.contains(Taint::Cursor));
        assert!(!taint.contains(Taint::Sequence));
        assert_eq!(taint.iter().collect::<Vec<_>>(), vec![Taint::Cursor]);
        taint.clear();
        assert!(taint.is_clean());
    }

    fn all_taint_sets() -> Vec<TaintSet> {
        (0u8..(1 << Taint::ALL.len()))
            .map(|bits| {
                Taint::ALL
                    .into_iter()
                    .enumerate()
                    .filter(|(index, _)| bits & (1 << index) != 0)
                    .map(|(_, taint)| taint)
                    .collect()
            })
            .collect()
    }

    fn pin_options() -> Vec<Option<PinReason>> {
        let mut options = vec![None];
        options.extend(PinReason::ALL.into_iter().map(Some));
        options
    }
}
