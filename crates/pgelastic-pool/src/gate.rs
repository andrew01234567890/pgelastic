//! The release gate.
//!
//! There is exactly one predicate that decides whether a backend link may be
//! handed to a different client, and it lives here. Every condition is named,
//! every rejection says which condition fired, and no caller is allowed to
//! reimplement any part of it — a second copy of this logic is how a link ends
//! up serving two clients at once.
//!
//! The condition that is easy to get wrong is COPY. `PostgreSQL` sends
//! `ReadyForQuery` *before* the COPY subprotocol has drained, so a gate that
//! trusts the transaction-status byte alone releases the link in the middle of
//! a transfer and the next client's `COPY` data is appended to the previous
//! client's table. That is the bug `PgBouncer` fixed in 1.22.1, and it is why
//! the COPY state is a condition of its own rather than something inferred.

use std::fmt;

use pgelastic_wire::TransactionStatus;

use crate::pin::PinReason;
use crate::server::{CopyState, ReleaseFlags, ServerLink, ServerState};

/// Why a link may not be checked in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckInBlock {
    /// The link is not in a state a client could be releasing it from.
    State(ServerState),
    /// No `ReadyForQuery` has been seen, so the transaction state is unknown.
    NoReadyForQuery,
    /// The backend is inside a transaction block.
    NotIdle(TransactionStatus),
    /// Requests are still awaiting responses.
    OutstandingRequests(usize),
    /// A COPY subprotocol is still open.
    CopyOpen(CopyState),
    /// A `CancelRequest` aimed at this link has not resolved.
    CancelInFlight(u32),
    /// A flag that permanently disqualifies the link is set.
    Disqualified(ReleaseFlags),
    /// State the reset ladder cannot remove.
    Pinned(PinReason),
    /// The link is dirty and has not been through the reset ladder.
    ResetRequired,
}

impl fmt::Display for CheckInBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::State(state) => write!(f, "link is {}", state.as_str()),
            Self::NoReadyForQuery => f.write_str("no ReadyForQuery seen"),
            Self::NotIdle(status) => {
                write!(f, "transaction status is {}", status.as_byte() as char)
            }
            Self::OutstandingRequests(count) => write!(f, "{count} outstanding requests"),
            Self::CopyOpen(state) => write!(f, "COPY {state:?} is open"),
            Self::CancelInFlight(count) => write!(f, "{count} cancels in flight"),
            Self::Disqualified(flags) => write!(f, "disqualified: {flags:?}"),
            Self::Pinned(reason) => write!(f, "pinned by {reason}"),
            Self::ResetRequired => f.write_str("tainted and not yet reset"),
        }
    }
}

/// Whether a link may go back to the pool.
///
/// The conditions, in the order they are checked:
///
/// 1. the link is `active`, `used` or `tested` — the three states a release can
///    be reached from, `tested` being the link that has just finished its reset
///    ladder,
/// 2. the last `ReadyForQuery` carried transaction status `'I'`,
/// 3. the outstanding-request queue is empty,
/// 4. no COPY subprotocol is open,
/// 5. no cancel aimed at this link is in flight,
/// 6. no close-needed, lifetime or credential-generation flag is set,
/// 7. the link is not pinned,
/// 8. the link is not tainted.
pub fn can_check_in(link: &ServerLink) -> Result<(), CheckInBlock> {
    if !matches!(
        link.state(),
        ServerState::Active | ServerState::Used | ServerState::Tested
    ) {
        return Err(CheckInBlock::State(link.state()));
    }

    match link.tx_status() {
        None => return Err(CheckInBlock::NoReadyForQuery),
        Some(status) if !status.is_releasable() => return Err(CheckInBlock::NotIdle(status)),
        Some(_) => {}
    }

    if !link.outstanding().is_empty() {
        return Err(CheckInBlock::OutstandingRequests(link.outstanding().len()));
    }

    if link.copy().is_open() {
        return Err(CheckInBlock::CopyOpen(link.copy()));
    }

    if link.cancels_in_flight() > 0 {
        return Err(CheckInBlock::CancelInFlight(link.cancels_in_flight()));
    }

    if !link.flags().is_empty() {
        return Err(CheckInBlock::Disqualified(link.flags()));
    }

    if let Some(reason) = link.pin() {
        return Err(CheckInBlock::Pinned(reason));
    }

    if !link.taint().is_clean() {
        return Err(CheckInBlock::ResetRequired);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use pgelastic_wire::{BackendMessage, CopyResponse, Format, FrontendMessage};

    use super::*;
    use crate::outstanding::{Relay, RequestKind};
    use crate::reset::Taint;
    use crate::server::Origin;
    use crate::server::tests::logged_in;
    use crate::server::{ServerEvent, ServerLink};

    fn ready(link: &mut ServerLink, status: TransactionStatus) {
        link.observe_backend(&BackendMessage::ReadyForQuery(status))
            .unwrap();
    }

    fn idle_after_a_query() -> ServerLink {
        let mut link = logged_in();
        link.observe_frontend(
            &FrontendMessage::Query(Bytes::from_static(b"SELECT 1")),
            Relay::Forward,
            Origin::Client,
        );
        ready(&mut link, TransactionStatus::Idle);
        link
    }

    #[test]
    fn a_finished_idle_query_releases_the_link() {
        assert_eq!(idle_after_a_query().can_check_in(), Ok(()));
    }

    #[test]
    fn a_link_that_never_reported_ready_is_held() {
        let link = logged_in();
        assert_eq!(link.can_check_in(), Err(CheckInBlock::NoReadyForQuery));
    }

    #[test]
    fn an_open_transaction_holds_the_link() {
        let mut link = logged_in();
        link.observe_frontend(
            &FrontendMessage::Query(Bytes::from_static(b"BEGIN")),
            Relay::Forward,
            Origin::Client,
        );
        ready(&mut link, TransactionStatus::Transaction);
        assert_eq!(
            link.can_check_in(),
            Err(CheckInBlock::NotIdle(TransactionStatus::Transaction))
        );
    }

    #[test]
    fn a_failed_transaction_holds_the_link() {
        let mut link = logged_in();
        link.observe_frontend(
            &FrontendMessage::Query(Bytes::from_static(b"SELECT bad")),
            Relay::Forward,
            Origin::Client,
        );
        ready(&mut link, TransactionStatus::Failed);
        assert_eq!(
            link.can_check_in(),
            Err(CheckInBlock::NotIdle(TransactionStatus::Failed))
        );
    }

    #[test]
    fn an_unanswered_request_holds_the_link() {
        let mut link = idle_after_a_query();
        link.observe_frontend(&FrontendMessage::Sync, Relay::Forward, Origin::Client);
        assert_eq!(
            link.can_check_in(),
            Err(CheckInBlock::OutstandingRequests(1))
        );
    }

    #[test]
    fn ready_for_query_during_copy_in_does_not_release_the_link() {
        let mut link = logged_in();
        link.observe_frontend(
            &FrontendMessage::Query(Bytes::from_static(b"COPY t FROM STDIN")),
            Relay::Forward,
            Origin::Client,
        );
        link.observe_backend(&BackendMessage::CopyInResponse(CopyResponse {
            format: Format::Text,
            column_formats: Vec::new(),
        }))
        .unwrap();
        ready(&mut link, TransactionStatus::Idle);

        assert!(link.outstanding().is_empty());
        assert_eq!(link.tx_status(), Some(TransactionStatus::Idle));
        assert_eq!(
            link.can_check_in(),
            Err(CheckInBlock::CopyOpen(CopyState::In))
        );

        link.observe_frontend(&FrontendMessage::CopyDone, Relay::Forward, Origin::Client);
        assert_eq!(link.can_check_in(), Ok(()));
    }

    #[test]
    fn an_in_flight_cancel_holds_the_link() {
        let mut link = idle_after_a_query();
        link.cancel_dispatched();
        assert_eq!(link.can_check_in(), Err(CheckInBlock::CancelInFlight(1)));
        link.cancel_resolved();
        assert_eq!(link.can_check_in(), Ok(()));
    }

    #[test]
    fn each_disqualifying_flag_holds_the_link() {
        for flag in [
            ReleaseFlags::CLOSE_NEEDED,
            ReleaseFlags::LIFETIME_EXPIRED,
            ReleaseFlags::CREDENTIALS_STALE,
        ] {
            let mut link = idle_after_a_query();
            link.set_flag(flag);
            assert_eq!(link.can_check_in(), Err(CheckInBlock::Disqualified(flag)));
        }
    }

    #[test]
    fn a_pinned_link_holds() {
        let mut link = idle_after_a_query();
        link.set_pin(PinReason::Listen);
        assert_eq!(
            link.can_check_in(),
            Err(CheckInBlock::Pinned(PinReason::Listen))
        );
    }

    #[test]
    fn a_tainted_link_is_held_until_the_reset_ladder_has_run() {
        let mut link = idle_after_a_query();
        link.add_taint(Taint::SessionParameter);
        assert_eq!(link.can_check_in(), Err(CheckInBlock::ResetRequired));
        link.reset_completed();
        assert_eq!(link.can_check_in(), Ok(()));
    }

    #[test]
    fn a_link_still_logging_in_is_not_releasable() {
        let link = ServerLink::new(
            crate::server::ServerId::new(1),
            crate::server::tests::test_key(),
        );
        assert_eq!(
            link.can_check_in(),
            Err(CheckInBlock::State(ServerState::Login))
        );
    }

    #[test]
    fn a_link_midway_through_its_reset_ladder_is_not_releasable() {
        let mut link = idle_after_a_query();
        link.add_taint(Taint::SessionParameter);
        link.apply(ServerEvent::ResetStarted).unwrap();
        link.observe_frontend(
            &FrontendMessage::Query(Bytes::from_static(b"DISCARD ALL")),
            Relay::Skip,
            Origin::Client,
        );
        assert_eq!(
            link.can_check_in(),
            Err(CheckInBlock::OutstandingRequests(1))
        );
    }

    #[test]
    fn a_link_being_canceled_is_not_releasable() {
        let mut link = idle_after_a_query();
        link.apply(ServerEvent::CancelStarted).unwrap();
        assert_eq!(
            link.can_check_in(),
            Err(CheckInBlock::State(ServerState::BeingCanceled))
        );
    }

    #[test]
    fn an_extended_batch_releases_only_after_its_sync() {
        let mut link = logged_in();
        for kind in [
            RequestKind::Parse,
            RequestKind::Bind,
            RequestKind::Execute,
            RequestKind::Sync,
        ] {
            let message = match kind {
                RequestKind::Parse => FrontendMessage::Parse(pgelastic_wire::Parse {
                    name: Bytes::new(),
                    query: Bytes::from_static(b"SELECT 1"),
                    param_types: Vec::new(),
                }),
                RequestKind::Bind => FrontendMessage::Bind(pgelastic_wire::Bind {
                    portal: Bytes::new(),
                    statement: Bytes::new(),
                    param_formats: Vec::new(),
                    params: Vec::new(),
                    result_formats: Vec::new(),
                }),
                RequestKind::Execute => FrontendMessage::Execute(pgelastic_wire::Execute {
                    portal: Bytes::new(),
                    max_rows: 0,
                }),
                _ => FrontendMessage::Sync,
            };
            link.observe_frontend(&message, Relay::Forward, Origin::Client);
        }

        link.observe_backend(&BackendMessage::ParseComplete)
            .unwrap();
        link.observe_backend(&BackendMessage::BindComplete).unwrap();
        link.observe_backend(&BackendMessage::CommandComplete(Bytes::from_static(
            b"SELECT 1",
        )))
        .unwrap();
        assert!(link.can_check_in().is_err());

        ready(&mut link, TransactionStatus::Idle);
        assert_eq!(link.can_check_in(), Ok(()));
    }
}
