//! Properties that must hold for every interleaving, not just the ones a
//! hand-written test happens to pick.

use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use bytes::Bytes;
use pgelastic_pool::key::{
    BackendTarget, CredentialGeneration, DatabaseName, PoolKey, PoolKeySpec, PoolMode,
    ReplicationKind, RoleName, StartupFingerprint, TenantId, TlsPosture,
};
use pgelastic_pool::outstanding::{OutstandingQueue, Relay, RequestKind};
use pgelastic_pool::reset::{ReleaseContext, ResetDisposition, ResetPolicy, Taint, TaintSet};
use pgelastic_pool::server::{Origin, ServerEvent, ServerId, ServerLink};
use pgelastic_pool::wait::{Priority, WaitQueue, Waiter};
use pgelastic_wire::{BackendMessage, Fields, FrontendMessage, RowDescription, TransactionStatus};
use proptest::prelude::*;

fn pool_key() -> PoolKey {
    PoolKey::new(PoolKeySpec {
        tenant: TenantId::new("tenant-a"),
        target: BackendTarget::new("primary.example.com", 5432),
        database: DatabaseName::new("appdb"),
        role: RoleName::new("app"),
        fingerprint: StartupFingerprint::default(),
        tls: TlsPosture::Tls,
        replication: ReplicationKind::None,
        configured_mode: PoolMode::Transaction,
        credentials: CredentialGeneration::new(1),
    })
}

fn idle_link() -> ServerLink {
    let mut link = ServerLink::new(ServerId::new(1), pool_key());
    link.apply(ServerEvent::LoginSucceeded).unwrap();
    link.apply(ServerEvent::Assigned).unwrap();
    link.observe_frontend(
        &FrontendMessage::Query(Bytes::from_static(b"SELECT 1")),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .unwrap();
    link
}

fn try_receive(waiter: &mut Waiter<u64>) -> Option<u64> {
    let mut context = Context::from_waker(Waker::noop());
    match Pin::new(waiter).poll(&mut context) {
        Poll::Ready(Ok(value)) => Some(value),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
enum QueueOp {
    Enqueue(bool),
    Abandon(usize),
    Collect(usize),
    Release,
}

fn queue_op() -> impl Strategy<Value = QueueOp> {
    prop_oneof![
        2 => any::<bool>().prop_map(QueueOp::Enqueue),
        1 => (0usize..8).prop_map(QueueOp::Abandon),
        1 => (0usize..8).prop_map(QueueOp::Collect),
        2 => Just(QueueOp::Release),
    ]
}

fn request_kind() -> impl Strategy<Value = RequestKind> {
    prop_oneof![
        Just(RequestKind::Parse),
        Just(RequestKind::Bind),
        Just(RequestKind::Describe),
        Just(RequestKind::Execute),
        Just(RequestKind::Close),
    ]
}

fn response_for(kind: RequestKind) -> BackendMessage {
    match kind {
        RequestKind::Parse => BackendMessage::ParseComplete,
        RequestKind::Bind => BackendMessage::BindComplete,
        RequestKind::Describe => {
            BackendMessage::RowDescription(RowDescription { fields: Vec::new() })
        }
        RequestKind::Execute => BackendMessage::CommandComplete(Bytes::from_static(b"SELECT 0")),
        RequestKind::Close => BackendMessage::CloseComplete,
        _ => BackendMessage::ReadyForQuery(TransactionStatus::Idle),
    }
}

fn taint_set() -> impl Strategy<Value = TaintSet> {
    proptest::collection::vec(
        prop_oneof![
            Just(Taint::SessionParameter),
            Just(Taint::PreparedStatement),
            Just(Taint::Cursor),
            Just(Taint::Sequence),
            Just(Taint::PlanCache),
        ],
        0..5,
    )
    .prop_map(TaintSet::from_iter)
}

fn reset_policy() -> impl Strategy<Value = ResetPolicy> {
    prop_oneof![
        Just(ResetPolicy::None),
        Just(ResetPolicy::DirtyTracked),
        Just(ResetPolicy::SmartDiscard),
        Just(ResetPolicy::DiscardAll),
        Just(ResetPolicy::Verified),
    ]
}

proptest! {
    #[test]
    fn a_link_is_never_handed_to_two_waiters(ops in proptest::collection::vec(queue_op(), 0..40)) {
        let queue: WaitQueue<u64> = WaitQueue::new();
        let now = Instant::now();
        let mut waiters: Vec<Waiter<u64>> = Vec::new();
        let mut delivered: Vec<u64> = Vec::new();
        let mut handed_off: Vec<u64> = Vec::new();
        let mut next_link = 0u64;

        for op in ops {
            match op {
                QueueOp::Enqueue(cancel) => {
                    let priority = if cancel { Priority::Cancel } else { Priority::Normal };
                    waiters.push(queue.enqueue(priority, now).unwrap());
                }
                QueueOp::Abandon(index) => {
                    if !waiters.is_empty() {
                        waiters.remove(index % waiters.len());
                    }
                }
                QueueOp::Collect(index) => {
                    if !waiters.is_empty() {
                        let position = index % waiters.len();
                        if let Some(link) = try_receive(&mut waiters[position]) {
                            delivered.push(link);
                            waiters.remove(position);
                        }
                    }
                }
                QueueOp::Release => {
                    let link = next_link;
                    next_link += 1;
                    if queue.hand_off(link).is_ok() {
                        handed_off.push(link);
                    }
                }
            }
        }

        for waiter in &mut waiters {
            if let Some(link) = try_receive(waiter) {
                delivered.push(link);
            }
        }

        let mut seen = delivered.clone();
        seen.sort_unstable();
        seen.dedup();
        prop_assert_eq!(seen.len(), delivered.len(), "a link reached two waiters");
        for link in &delivered {
            prop_assert!(handed_off.contains(link), "a link was received but never handed off");
        }
    }

    #[test]
    fn a_waiter_dropped_mid_wait_leaves_no_trace(
        ops in proptest::collection::vec((any::<bool>(), any::<bool>(), 0usize..8), 0..40),
    ) {
        let queue: WaitQueue<u64> = WaitQueue::new();
        let now = Instant::now();
        let mut waiters: Vec<Waiter<u64>> = Vec::new();

        for (enqueue, poll_first, index) in ops {
            if enqueue {
                let mut waiter = queue.enqueue(Priority::Normal, now).unwrap();
                if poll_first {
                    prop_assert_eq!(try_receive(&mut waiter), None);
                }
                waiters.push(waiter);
            } else if !waiters.is_empty() {
                waiters.remove(index % waiters.len());
            }
            prop_assert_eq!(queue.len(), waiters.len());
        }

        drop(waiters);
        prop_assert_eq!(queue.len(), 0);
    }

    #[test]
    fn an_extended_batch_always_drains_on_sync(kinds in proptest::collection::vec(request_kind(), 0..12)) {
        let mut queue = OutstandingQueue::new();
        for kind in &kinds {
            queue.forward(*kind);
        }
        queue.forward(RequestKind::Sync);

        for kind in &kinds {
            queue.apply(&response_for(*kind)).unwrap();
        }
        queue
            .apply(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
            .unwrap();

        prop_assert!(queue.is_empty());
        prop_assert!(!queue.query_failed());
    }

    #[test]
    fn a_failed_batch_also_drains_on_sync(
        kinds in proptest::collection::vec(request_kind(), 0..12),
        failure_after in 0usize..12,
    ) {
        let mut queue = OutstandingQueue::new();
        for kind in &kinds {
            queue.forward(*kind);
        }
        queue.forward(RequestKind::Sync);

        for kind in kinds.iter().take(failure_after) {
            queue.apply(&response_for(*kind)).unwrap();
        }
        queue.apply(&BackendMessage::ErrorResponse(Fields::default())).unwrap();
        prop_assert!(queue.query_failed());

        queue
            .apply(&BackendMessage::ReadyForQuery(TransactionStatus::Failed))
            .unwrap();
        prop_assert!(queue.is_empty());
        prop_assert!(!queue.query_failed());
    }

    #[test]
    fn a_tainted_link_is_never_checked_in_without_a_reset(
        taint in taint_set(),
        policy in reset_policy(),
    ) {
        let mut link = idle_link();
        for one in taint.iter() {
            link.add_taint(one);
        }

        if !taint.is_clean() {
            prop_assert!(link.can_check_in().is_err(), "a dirty link was released");
        }

        let context = ReleaseContext {
            tx_status: link.tx_status().unwrap(),
            client_gone: false,
        };
        let plan = pgelastic_pool::reset::plan(policy, link.taint(), link.pin(), context);

        if plan.disposition() == ResetDisposition::Reuse {
            link.reset_completed();
            prop_assert_eq!(link.can_check_in(), Ok(()));
        } else {
            prop_assert!(link.can_check_in().is_err());
        }
    }

    #[test]
    fn a_checked_in_link_is_always_clean(taint in taint_set()) {
        let mut link = idle_link();
        for one in taint.iter() {
            link.add_taint(one);
        }
        if link.can_check_in().is_ok() {
            prop_assert!(link.taint().is_clean());
            prop_assert!(link.pin().is_none());
            prop_assert!(link.outstanding().is_empty());
            prop_assert!(!link.copy().is_open());
            prop_assert_eq!(link.tx_status(), Some(TransactionStatus::Idle));
        }
    }
}
