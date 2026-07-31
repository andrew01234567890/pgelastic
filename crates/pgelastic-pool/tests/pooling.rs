//! End-to-end behaviour of the pieces together: a link going through a full
//! release cycle, the statement cache driving the outstanding queue, and the
//! failure modes that motivated each of them.

use std::time::Instant;

use bytes::Bytes;
use pgelastic_pool::gate::CheckInBlock;
use pgelastic_pool::key::{
    BackendTarget, CredentialGeneration, DatabaseName, PoolKey, PoolKeySpec, PoolMode,
    ReplicationKind, RoleName, StartupFingerprint, TenantId, TlsPosture,
};
use pgelastic_pool::outstanding::{Disposition, OutstandingQueue, Relay, RequestKind};
use pgelastic_pool::pin::{BudgetLedger, PinReason};
use pgelastic_pool::reset::{
    ReleaseContext, ResetDisposition, ResetPolicy, ResetStep, Taint, plan,
};
use pgelastic_pool::server::{CopyState, Origin, ServerEvent, ServerId, ServerLink, ServerState};
use pgelastic_pool::stmt::{
    CacheInvalidation, ClientStatements, GlobalStatementCache, ServerAction, ServerStatements,
    StatementKey, detect_cache_invalidation,
};
use pgelastic_pool::wait::{Priority, WaitQueue};
use pgelastic_wire::{BackendMessage, RowDescription};
use pgelastic_wire::{
    Bind, CopyResponse, Execute, Format, FrontendMessage, Parse, TransactionStatus,
};

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

fn active_link() -> ServerLink {
    let mut link = ServerLink::new(ServerId::new(1), pool_key());
    link.apply(ServerEvent::LoginSucceeded).unwrap();
    link.apply(ServerEvent::Assigned).unwrap();
    link
}

fn parse(name: &'static str, query: &'static str) -> FrontendMessage {
    FrontendMessage::Parse(Parse {
        name: Bytes::from_static(name.as_bytes()),
        query: Bytes::from_static(query.as_bytes()),
        param_types: Vec::new(),
    })
}

fn bind(statement: &'static str) -> FrontendMessage {
    FrontendMessage::Bind(Bind {
        portal: Bytes::new(),
        statement: Bytes::from_static(statement.as_bytes()),
        param_formats: Vec::new(),
        params: Vec::new(),
        result_formats: Vec::new(),
    })
}

fn execute() -> FrontendMessage {
    FrontendMessage::Execute(Execute {
        portal: Bytes::new(),
        max_rows: 0,
    })
}

fn copy_in() -> BackendMessage {
    BackendMessage::CopyInResponse(CopyResponse {
        format: Format::Text,
        column_formats: Vec::new(),
    })
}

fn run_reset(link: &mut ServerLink, steps: &[ResetStep]) {
    link.apply(ServerEvent::ResetStarted).unwrap();
    for step in steps {
        link.observe_frontend(
            &FrontendMessage::Query(Bytes::copy_from_slice(step.sql().as_bytes())),
            Relay::Skip,
            Origin::Client,
        );
        link.observe_backend(&BackendMessage::CommandComplete(Bytes::from_static(b"OK")))
            .unwrap();
        link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
            .unwrap();
    }
    link.reset_completed();
}

#[test]
fn a_backend_ready_for_query_mid_copy_does_not_release_the_link() {
    let mut link = active_link();

    link.observe_frontend(
        &FrontendMessage::Query(Bytes::from_static(b"COPY orders FROM STDIN")),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_backend(&copy_in()).unwrap();
    link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .unwrap();

    assert!(
        link.outstanding().is_empty(),
        "the outstanding queue really is drained, which is what makes this dangerous"
    );
    assert_eq!(link.tx_status(), Some(TransactionStatus::Idle));
    assert_eq!(
        link.can_check_in(),
        Err(CheckInBlock::CopyOpen(CopyState::In)),
        "released mid-COPY: the next client's rows would land in this client's table"
    );

    link.observe_frontend(
        &FrontendMessage::CopyData(Bytes::from_static(b"1\n")),
        Relay::Forward,
        Origin::Client,
    );
    assert_eq!(
        link.can_check_in(),
        Err(CheckInBlock::CopyOpen(CopyState::In))
    );

    link.observe_frontend(&FrontendMessage::CopyDone, Relay::Forward, Origin::Client);
    assert_eq!(link.can_check_in(), Ok(()));
}

#[test]
fn an_extended_protocol_copy_is_held_by_the_outstanding_queue_as_well() {
    let mut link = active_link();
    link.observe_frontend(
        &FrontendMessage::Query(Bytes::from_static(b"SELECT 1")),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .unwrap();

    link.observe_frontend(
        &parse("", "COPY orders FROM STDIN"),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_frontend(&bind(""), Relay::Forward, Origin::Client);
    link.observe_frontend(&execute(), Relay::Forward, Origin::Client);
    link.observe_frontend(&FrontendMessage::Sync, Relay::Forward, Origin::Client);

    link.observe_backend(&BackendMessage::ParseComplete)
        .unwrap();
    link.observe_backend(&BackendMessage::BindComplete).unwrap();
    link.observe_backend(&copy_in()).unwrap();

    assert_eq!(
        link.can_check_in(),
        Err(CheckInBlock::OutstandingRequests(2)),
        "the Execute has not been answered, so the batch is not over"
    );

    link.observe_frontend(&FrontendMessage::CopyDone, Relay::Forward, Origin::Client);
    link.observe_backend(&BackendMessage::CommandComplete(Bytes::from_static(
        b"COPY 1",
    )))
    .unwrap();
    link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .unwrap();
    assert_eq!(link.can_check_in(), Ok(()));
}

#[test]
fn a_failed_copy_releases_the_link_once_the_backend_reports_the_error() {
    let mut link = active_link();
    link.observe_frontend(
        &FrontendMessage::Query(Bytes::from_static(b"COPY orders FROM STDIN")),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_backend(&copy_in()).unwrap();
    link.observe_backend(&BackendMessage::ErrorResponse(
        pgelastic_wire::Fields::default(),
    ))
    .unwrap();
    link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .unwrap();

    assert_eq!(link.copy(), CopyState::None);
    assert_eq!(link.can_check_in(), Ok(()));
}

#[test]
fn a_tainted_link_reaches_idle_only_through_the_reset_ladder() {
    let mut link = active_link();
    link.observe_frontend(
        &FrontendMessage::Query(Bytes::from_static(b"SET search_path = audit")),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_backend(&BackendMessage::ParameterStatus(
        pgelastic_wire::ParameterStatus {
            name: Bytes::from_static(b"search_path"),
            value: Bytes::from_static(b"audit"),
        },
    ))
    .unwrap();
    link.observe_backend(&BackendMessage::CommandComplete(Bytes::from_static(b"SET")))
        .unwrap();
    link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .unwrap();

    assert!(link.taint().contains(Taint::SessionParameter));
    assert_eq!(link.can_check_in(), Err(CheckInBlock::ResetRequired));

    link.apply(ServerEvent::Released).unwrap();
    let plan = plan(
        ResetPolicy::DirtyTracked,
        link.taint(),
        link.pin(),
        ReleaseContext {
            tx_status: link.tx_status().unwrap(),
            client_gone: false,
        },
    );
    assert_eq!(plan.disposition(), ResetDisposition::Reuse);
    assert_eq!(plan.steps(), [ResetStep::DiscardAll]);

    run_reset(&mut link, plan.steps());
    assert_eq!(link.can_check_in(), Ok(()));
    assert_eq!(
        link.apply(ServerEvent::ResetSucceeded).unwrap(),
        ServerState::Idle
    );
}

#[test]
fn an_open_transaction_is_rolled_back_before_the_scrub() {
    let mut link = active_link();
    link.observe_frontend(
        &FrontendMessage::Query(Bytes::from_static(b"BEGIN")),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_backend(&BackendMessage::ReadyForQuery(
        TransactionStatus::Transaction,
    ))
    .unwrap();
    link.add_taint(Taint::Cursor);

    let plan = plan(
        ResetPolicy::SmartDiscard,
        link.taint(),
        link.pin(),
        ReleaseContext {
            tx_status: link.tx_status().unwrap(),
            client_gone: false,
        },
    );
    assert_eq!(plan.steps(), [ResetStep::Rollback, ResetStep::CloseAll]);
    assert!(plan.respects_transaction_block_rules(TransactionStatus::Transaction));
}

#[test]
fn a_pinned_link_leaves_the_elastic_budget_and_is_counted_separately() {
    let mut ledger = BudgetLedger::new(10);
    ledger.open().unwrap();

    let mut link = active_link();
    link.observe_frontend(
        &FrontendMessage::Query(Bytes::from_static(b"LISTEN orders")),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .unwrap();
    link.set_pin(PinReason::Listen);
    ledger.pin(PinReason::Listen).unwrap();

    assert_eq!(
        link.can_check_in(),
        Err(CheckInBlock::Pinned(PinReason::Listen))
    );
    assert_eq!(ledger.elastic(), 0);
    assert_eq!(ledger.pinned_for(PinReason::Listen), 1);
    assert_eq!(ledger.elastic_limit(), 9);
    assert_eq!(ledger.total(), 1);
}

#[test]
fn a_released_link_reaches_the_head_of_the_queue_without_touching_the_idle_list() {
    let queue: WaitQueue<ServerId> = WaitQueue::new();
    let now = Instant::now();
    let mut idle: Vec<ServerId> = Vec::new();

    let mut waiter = queue.enqueue(Priority::Normal, now).unwrap();

    let link = active_link();
    assert!(link.can_check_in().is_err());

    let released = ServerId::new(1);
    if let Err(returned) = queue.hand_off(released) {
        idle.push(returned);
    }
    assert!(idle.is_empty(), "the link went via the idle list");

    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let polled = std::pin::Pin::new(&mut waiter).poll(&mut context);
    assert_eq!(polled, std::task::Poll::Ready(Ok(released)));
}

#[test]
fn an_evicted_prepared_statement_close_is_swallowed_not_forwarded() {
    let mut global = GlobalStatementCache::new();
    let mut client = ClientStatements::new();
    let mut server = ServerStatements::new(1);
    let mut queue = OutstandingQueue::new();

    let first = global.intern(StatementKey::new(
        Bytes::from_static(b"SELECT 1"),
        Vec::new(),
    ));
    let second = global.intern(StatementKey::new(
        Bytes::from_static(b"SELECT 2"),
        Vec::new(),
    ));
    client.insert(Bytes::from_static(b"S_1"), first.clone());
    client.insert(Bytes::from_static(b"S_2"), second.clone());

    assert_eq!(
        server.ensure(&first),
        ServerAction::Parse(first.name().clone())
    );
    queue.skip(RequestKind::Parse);
    assert_eq!(
        queue
            .apply(&BackendMessage::ParseComplete)
            .unwrap()
            .disposition,
        Disposition::Skip
    );

    let ServerAction::EvictThenParse { evict, name } = server.ensure(&second) else {
        panic!("the second statement should have evicted the first");
    };
    assert_eq!(&evict, first.name());
    assert_eq!(&name, second.name());

    queue.skip(RequestKind::Close);
    queue.skip(RequestKind::Parse);
    queue.forward(RequestKind::Bind);

    assert_eq!(
        queue
            .apply(&BackendMessage::CloseComplete)
            .unwrap()
            .disposition,
        Disposition::Skip,
        "the client never sent this Close and must not see its response"
    );
    assert_eq!(
        queue
            .apply(&BackendMessage::ParseComplete)
            .unwrap()
            .disposition,
        Disposition::Skip
    );
    assert_eq!(
        queue
            .apply(&BackendMessage::BindComplete)
            .unwrap()
            .disposition,
        Disposition::Forward
    );
}

#[test]
fn a_client_close_is_answered_by_the_pool_in_stream_order() {
    let mut global = GlobalStatementCache::new();
    let mut client = ClientStatements::new();
    let mut queue = OutstandingQueue::new();

    let statement = global.intern(StatementKey::new(
        Bytes::from_static(b"SELECT 1"),
        Vec::new(),
    ));
    client.insert(Bytes::from_static(b"S_1"), statement);

    queue.forward(RequestKind::Describe);
    assert!(client.remove(b"S_1").is_some());
    queue.fake(RequestKind::Close, BackendMessage::CloseComplete);
    queue.forward(RequestKind::Sync);

    assert!(
        queue.take_ready_fakes().is_empty(),
        "the fake overtook a response the client is still owed"
    );
    queue
        .apply(&BackendMessage::RowDescription(RowDescription {
            fields: Vec::new(),
        }))
        .unwrap();
    assert_eq!(
        queue.take_ready_fakes(),
        vec![BackendMessage::CloseComplete]
    );
    assert_eq!(queue.len(), 1);
}

#[test]
fn deallocate_all_invalidates_the_server_view_and_leaves_client_names_working() {
    let mut global = GlobalStatementCache::new();
    let mut client = ClientStatements::new();
    let mut server = ServerStatements::new(8);

    let statement = global.intern(StatementKey::new(
        Bytes::from_static(b"SELECT $1::int"),
        vec![23],
    ));
    client.insert(Bytes::from_static(b"S_1"), statement.clone());
    server.ensure(&statement);

    assert_eq!(
        detect_cache_invalidation(b"DEALLOCATE ALL"),
        Some(CacheInvalidation::Deallocate)
    );
    server.clear();

    assert!(
        client.resolve(b"S_1").is_some(),
        "the client's name must keep resolving or its next Bind gets 26000"
    );
    assert_eq!(
        server.ensure(&statement),
        ServerAction::Parse(statement.name().clone()),
        "the statement must be re-parsed rather than assumed present"
    );
}

#[test]
fn a_link_whose_credentials_were_rotated_is_never_reused() {
    let key = pool_key();
    let rotated = key.with_credentials(CredentialGeneration::new(2));
    assert_ne!(key, rotated);

    let mut link = active_link();
    link.observe_frontend(
        &FrontendMessage::Query(Bytes::from_static(b"SELECT 1")),
        Relay::Forward,
        Origin::Client,
    );
    link.observe_backend(&BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .unwrap();
    assert_eq!(link.can_check_in(), Ok(()));

    link.set_flag(pgelastic_pool::server::ReleaseFlags::CREDENTIALS_STALE);
    assert!(link.can_check_in().is_err());
}
