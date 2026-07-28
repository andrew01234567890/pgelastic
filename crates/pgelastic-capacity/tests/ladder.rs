//! One test per rung of the admission ladder, plus revocation victim choice.

use std::time::Duration;

use pgelastic_capacity::{
    Admission, AdmissionSpec, AdmissionStrategy, Allocator, CapacityBudget, ConfigError,
    ConnectRejection, CreditKind, DenialReason, Disposition, ErrorCode, Lease, ManualClock,
    PoolMode, PoolSpec, RequestKind, Revocation, ServerState, ShrinkScope, TenantId, TenantSpec,
};

fn pool(total: u32) -> PoolSpec {
    PoolSpec {
        backend_connections: total,
        headroom_percent: 0,
        max_oversubscription: None,
        ..PoolSpec::default()
    }
}

fn spec(guaranteed: u32, burstable: u32) -> TenantSpec {
    TenantSpec {
        guaranteed,
        burstable,
        ..TenantSpec::default()
    }
}

fn allocator(total: u32) -> Allocator<ManualClock> {
    Allocator::with_clock(pool(total), AdmissionSpec::default(), ManualClock::new()).unwrap()
}

fn tenant(allocator: &mut Allocator<ManualClock>, name: &str, spec: TenantSpec) {
    allocator.add_tenant(name.into(), spec).unwrap();
}

fn request(allocator: &mut Allocator<ManualClock>, name: &str, kind: RequestKind) -> Admission {
    let client = allocator.connect_client(&name.into()).unwrap();
    allocator.try_lease(client, kind)
}

fn checkout(allocator: &mut Allocator<ManualClock>, name: &str) -> Admission {
    request(allocator, name, RequestKind::Normal)
}

fn granted(allocator: &mut Allocator<ManualClock>, name: &str) -> Lease {
    match checkout(allocator, name) {
        Admission::Granted(lease) => lease,
        other => panic!("expected a grant for {name}, got {other:?}"),
    }
}

fn hold(allocator: &mut Allocator<ManualClock>, name: &str, count: u32) -> Vec<Lease> {
    (0..count).map(|_| granted(allocator, name)).collect()
}

fn live(allocator: &Allocator<ManualClock>, name: &str) -> u32 {
    allocator
        .budget()
        .tenant(&name.into())
        .expect("tenant is bound")
        .live()
}

#[test]
fn step_0_a_cancel_bypasses_a_completely_full_pool() {
    let mut allocator = allocator(4);
    tenant(&mut allocator, "acme", spec(0, 4));
    let _held = hold(&mut allocator, "acme", 4);

    assert_eq!(allocator.budget().free(), 0);
    assert!(matches!(
        checkout(&mut allocator, "acme"),
        Admission::Denied(DenialReason::TenantCap { .. })
    ));

    let Admission::Granted(lease) = request(&mut allocator, "acme", RequestKind::Cancel) else {
        panic!("a cancel must bypass every other rung");
    };
    assert_eq!(lease.credit, CreditKind::Cancel);
}

#[test]
fn step_0_cancel_credit_is_bounded_at_min_8_and_burstable() {
    let mut allocator = allocator(100);
    tenant(&mut allocator, "small", spec(0, 3));
    tenant(&mut allocator, "large", spec(0, 50));

    assert_eq!(allocator.cancel_credit(&"small".into()), 3);
    assert_eq!(allocator.cancel_credit(&"large".into()), 8);

    for _ in 0..3 {
        assert!(matches!(
            request(&mut allocator, "small", RequestKind::Cancel),
            Admission::Granted(_)
        ));
    }
    let denied = request(&mut allocator, "small", RequestKind::Cancel);
    assert!(matches!(
        denied,
        Admission::Denied(DenialReason::CancelCredit {
            in_flight: 3,
            limit: 3,
            ..
        })
    ));
}

#[test]
fn step_0_cancel_credit_is_outside_the_backend_budget() {
    let mut allocator = allocator(4);
    tenant(&mut allocator, "acme", spec(0, 4));
    let _held = hold(&mut allocator, "acme", 4);

    let _cancel = request(&mut allocator, "acme", RequestKind::Cancel);
    assert_eq!(allocator.budget().live_total(), 4);
    assert_eq!(live(&allocator, "acme"), 4);
    assert_eq!(allocator.cancel_in_flight(&"acme".into()), 1);
}

#[test]
fn step_1_a_guarantee_is_granted_from_reserved_credit_and_never_queues() {
    let mut allocator = allocator(10);
    tenant(&mut allocator, "vip", spec(3, 6));
    tenant(&mut allocator, "hog", spec(0, 10));

    // The hog bursts into everything that is lendable, which is total minus the
    // vip's untouched reservation.
    let _held = hold(&mut allocator, "hog", 7);
    assert_eq!(allocator.budget().free(), 0);

    for expected in 1..=3 {
        let Admission::Granted(lease) = checkout(&mut allocator, "vip") else {
            panic!("a request inside the guarantee never queues");
        };
        assert_eq!(lease.credit, CreditKind::Reserved);
        assert_eq!(live(&allocator, "vip"), expected);
    }
    assert_eq!(allocator.budget().live_total(), 10);
}

#[test]
fn step_2_the_burst_ceiling_denies_with_tenant_cap() {
    let mut allocator = allocator(100);
    tenant(&mut allocator, "acme", spec(0, 2));
    let _held = hold(&mut allocator, "acme", 2);

    let Admission::Denied(reason) = checkout(&mut allocator, "acme") else {
        panic!("the tenant is at its own ceiling");
    };
    assert_eq!(
        reason,
        DenialReason::TenantCap {
            tenant: "acme".into(),
            live: 2,
            burstable: 2
        }
    );
    assert_eq!(reason.code(), ErrorCode::Pge1928);
    assert_eq!(reason.sqlstate(), "53300");
    assert!(allocator.budget().free() > 0, "the pool itself is not full");
}

#[test]
fn step_3_burst_credit_is_granted_while_the_pool_has_free_capacity() {
    let mut allocator = allocator(10);
    tenant(&mut allocator, "acme", spec(2, 10));

    let first = granted(&mut allocator, "acme");
    let second = granted(&mut allocator, "acme");
    let third = granted(&mut allocator, "acme");

    assert_eq!(first.credit, CreditKind::Reserved);
    assert_eq!(second.credit, CreditKind::Reserved);
    assert_eq!(third.credit, CreditKind::Burst);
}

#[test]
fn step_5_a_burst_request_queues_when_the_pool_is_full() {
    let mut allocator = allocator(6);
    tenant(&mut allocator, "acme", spec(0, 6));
    tenant(&mut allocator, "vip", spec(2, 6));
    let _held = hold(&mut allocator, "acme", 4);
    assert_eq!(allocator.budget().free(), 0);

    let Admission::Queued { blocked_by, .. } = checkout(&mut allocator, "acme") else {
        panic!("a burst request queues rather than failing fast");
    };
    assert_eq!(blocked_by, DenialReason::PoolCapacity { live: 4, total: 6 });
    assert_eq!(blocked_by.code(), ErrorCode::Pge1936);
}

#[test]
fn step_4_revocation_takes_from_the_largest_surplus() {
    let mut allocator = allocator(12);
    tenant(&mut allocator, "hog", spec(0, 12));
    tenant(&mut allocator, "middling", spec(0, 12));
    tenant(&mut allocator, "vip", spec(0, 4));

    let _hog = hold(&mut allocator, "hog", 8);
    let _middling = hold(&mut allocator, "middling", 4);
    assert_eq!(allocator.budget().live_total(), 12);

    // The tier change that makes the guarantee unhonourable without revocation.
    allocator
        .set_tenant_spec(&"vip".into(), spec(2, 4))
        .unwrap();
    assert_eq!(allocator.budget().free(), 0);

    let Admission::Queued {
        revoked: Some(revocation),
        ..
    } = checkout(&mut allocator, "vip")
    else {
        panic!("a guarantee against a physically full pool must revoke");
    };
    assert_eq!(revocation.tenant(), &TenantId::from("hog"));
    assert!(matches!(revocation, Revocation::MarkedActive { .. }));
}

#[test]
fn step_4_revocation_closes_an_idle_server_and_grants_immediately() {
    let mut allocator = allocator(6);
    tenant(&mut allocator, "hog", spec(0, 6));
    tenant(&mut allocator, "vip", spec(0, 2));

    let mut hog = hold(&mut allocator, "hog", 6);
    let outcome = allocator.release(hog.pop().unwrap());
    assert!(matches!(outcome.disposition, Disposition::Idle(_)));
    assert_eq!(live(&allocator, "hog"), 6, "an idle server is still live");

    allocator
        .set_tenant_spec(&"vip".into(), spec(1, 2))
        .unwrap();

    let Admission::Granted(lease) = checkout(&mut allocator, "vip") else {
        panic!("closing the victim's idle server frees the slot outright");
    };
    assert_eq!(lease.credit, CreditKind::Reserved);
    assert_eq!(live(&allocator, "hog"), 5);
    assert_eq!(allocator.budget().live_total(), 6);
}

#[test]
fn step_4_revocation_marks_the_victims_lru_active_server() {
    let mut allocator = allocator(4);
    tenant(&mut allocator, "hog", spec(0, 4));
    tenant(&mut allocator, "vip", spec(0, 2));

    let hog = hold(&mut allocator, "hog", 4);
    let lru = hog[0].server;

    allocator
        .set_tenant_spec(&"vip".into(), spec(1, 2))
        .unwrap();
    let Admission::Queued {
        revoked: Some(Revocation::MarkedActive { server, .. }),
        ..
    } = checkout(&mut allocator, "vip")
    else {
        panic!("no idle server exists, so the LRU active one is marked");
    };
    assert_eq!(server, lru);
    assert_eq!(allocator.close_needed_active(), vec![lru]);
}

#[test]
fn revocation_never_pushes_a_tenant_below_its_own_guarantee() {
    let mut allocator = allocator(6);
    tenant(&mut allocator, "steady", spec(3, 3));
    tenant(&mut allocator, "hog", spec(0, 6));
    tenant(&mut allocator, "vip", spec(0, 3));

    let _steady = hold(&mut allocator, "steady", 3);
    let _hog = hold(&mut allocator, "hog", 3);
    assert_eq!(allocator.budget().live_total(), 6);

    allocator
        .set_tenant_spec(&"vip".into(), spec(2, 3))
        .unwrap();
    let Admission::Queued {
        revoked: Some(revocation),
        ..
    } = checkout(&mut allocator, "vip")
    else {
        panic!("expected revocation");
    };

    assert_eq!(
        revocation.tenant(),
        &TenantId::from("hog"),
        "the tenant sitting exactly on its guarantee is untouchable"
    );
}

#[test]
fn a_repeated_revocation_marks_a_different_server_each_time() {
    let mut allocator = allocator(4);
    tenant(&mut allocator, "hog", spec(0, 4));
    tenant(&mut allocator, "vip", spec(0, 2));
    let _hog = hold(&mut allocator, "hog", 4);
    allocator
        .set_tenant_spec(&"vip".into(), spec(2, 2))
        .unwrap();

    checkout(&mut allocator, "vip");
    checkout(&mut allocator, "vip");
    assert_eq!(allocator.close_needed_active().len(), 2);
}

#[test]
fn an_idle_server_is_reused_without_consuming_new_capacity() {
    let mut allocator = allocator(2);
    tenant(&mut allocator, "acme", spec(0, 2));

    let lease = granted(&mut allocator, "acme");
    let server = lease.server;
    allocator.release(lease);

    let created_before = allocator.accounting().created;
    let reused = granted(&mut allocator, "acme");

    assert_eq!(reused.server, server);
    assert_eq!(allocator.accounting().created, created_before);
    assert_eq!(live(&allocator, "acme"), 1);
}

#[test]
fn reuse_is_lifo_so_the_lru_end_stays_available_to_revocation() {
    let mut allocator = allocator(4);
    tenant(&mut allocator, "acme", spec(0, 4));

    let first = granted(&mut allocator, "acme");
    let second = granted(&mut allocator, "acme");
    let (older, newer) = (first.server, second.server);
    allocator.release(first);
    allocator.release(second);

    assert_eq!(granted(&mut allocator, "acme").server, newer);
    assert_eq!(granted(&mut allocator, "acme").server, older);
}

#[test]
fn a_full_admission_queue_is_pool_busy() {
    let admission = AdmissionSpec {
        queue_depth_per_tenant: 2,
        ..AdmissionSpec::default()
    };
    let mut allocator = Allocator::with_clock(pool(2), admission, ManualClock::new()).unwrap();
    tenant(&mut allocator, "acme", spec(0, 8));
    let _held = hold(&mut allocator, "acme", 2);

    assert!(matches!(
        checkout(&mut allocator, "acme"),
        Admission::Queued { .. }
    ));
    assert!(matches!(
        checkout(&mut allocator, "acme"),
        Admission::Queued { .. }
    ));

    let Admission::Denied(reason) = checkout(&mut allocator, "acme") else {
        panic!("the third client has nowhere to wait");
    };
    assert_eq!(reason.code(), ErrorCode::Pge1929);
    assert_eq!(reason.sqlstate(), "53400");
}

#[test]
fn a_queued_client_times_out_with_the_limit_that_blocked_it() {
    let admission = AdmissionSpec {
        max_wait: Duration::from_secs(30),
        ..AdmissionSpec::default()
    };
    let mut allocator = Allocator::with_clock(pool(1), admission, ManualClock::new()).unwrap();
    tenant(&mut allocator, "acme", spec(0, 4));
    let _held = hold(&mut allocator, "acme", 1);

    let Admission::Queued { ticket, .. } = checkout(&mut allocator, "acme") else {
        panic!("expected a queue");
    };
    assert!(allocator.expire_queued().is_empty());

    allocator.clock().advance(Duration::from_secs(30));
    let expired = allocator.expire_queued();

    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].ticket, ticket);
    assert_eq!(expired[0].reason.code(), ErrorCode::Pge1024);
    assert_eq!(expired[0].blocked_by.code(), ErrorCode::Pge1936);
    assert_eq!(allocator.queued_total(), 0);
}

#[test]
fn a_migrating_tenant_is_refused_with_the_cutover_code() {
    let mut allocator = allocator(10);
    tenant(&mut allocator, "acme", spec(0, 4));
    let lease = granted(&mut allocator, "acme");

    allocator.begin_migration(&"acme".into()).unwrap();
    let Admission::Denied(reason) = checkout(&mut allocator, "acme") else {
        panic!("cutover refuses new work");
    };
    assert_eq!(reason.code(), ErrorCode::Pge1613);
    assert_eq!(reason.sqlstate(), "57P01");

    let outcome = allocator.release(lease);
    assert!(
        matches!(outcome.disposition, Disposition::Closed(_)),
        "cutover marks the tenant's existing servers for close"
    );

    allocator.finish_migration(&"acme".into()).unwrap();
    assert!(matches!(
        checkout(&mut allocator, "acme"),
        Admission::Granted(_)
    ));
}

#[test]
fn the_storage_quota_is_a_separate_per_tenant_gate() {
    let mut allocator = allocator(10);
    tenant(
        &mut allocator,
        "acme",
        TenantSpec {
            storage_bytes: 1_000,
            ..spec(0, 4)
        },
    );
    allocator.set_storage_used(&"acme".into(), 900).unwrap();

    assert!(allocator.check_storage(&"acme".into(), 100).is_ok());
    let reason = allocator
        .check_storage(&"acme".into(), 101)
        .expect_err("the write crosses the quota");
    assert_eq!(reason.code(), ErrorCode::Pge0544);
    assert_eq!(reason.sqlstate(), "53100");

    assert!(
        matches!(checkout(&mut allocator, "acme"), Admission::Granted(_)),
        "storage exhaustion must not block SELECT or DELETE"
    );
}

#[test]
fn the_error_table_matches_the_design() {
    let table = [
        (ErrorCode::Pge1928, "PGE1928", "53300", Some(10928), false),
        (ErrorCode::Pge1936, "PGE1936", "53400", Some(10936), false),
        (ErrorCode::Pge1929, "PGE1929", "53400", Some(10929), true),
        (ErrorCode::Pge0544, "PGE0544", "53100", Some(40544), false),
        (ErrorCode::Pge1024, "PGE1024", "53400", None, true),
        (ErrorCode::Pge1613, "PGE1613", "57P01", Some(40613), true),
    ];
    assert_eq!(table.len(), ErrorCode::ALL.len());
    for (code, name, sqlstate, azure, retryable) in table {
        assert_eq!(code.as_str(), name);
        assert_eq!(code.sqlstate(), sqlstate);
        assert_eq!(code.azure_equivalent(), azure);
        assert_eq!(code.retryable(), retryable);
    }
}

#[test]
fn client_connections_are_a_separate_currency_from_backends() {
    let mut allocator = allocator(4);
    tenant(
        &mut allocator,
        "acme",
        TenantSpec {
            max_client_connections: 50,
            ..spec(0, 2)
        },
    );

    let clients: Vec<_> = (0..50)
        .map(|_| allocator.connect_client(&"acme".into()).unwrap())
        .collect();
    assert_eq!(allocator.tenant_client_count(&"acme".into()), 50);
    assert_eq!(allocator.budget().live_total(), 0);

    let rejected = allocator
        .connect_client(&"acme".into())
        .expect_err("the 51st client exceeds the tenant's client limit");
    assert_eq!(
        rejected,
        ConnectRejection::Denied(DenialReason::TenantClientCap {
            tenant: "acme".into(),
            live: 50,
            max: 50
        })
    );

    allocator.disconnect_client(clients[0]);
    assert!(allocator.connect_client(&"acme".into()).is_ok());
}

#[test]
fn client_connections_are_bounded_by_the_file_descriptor_budget() {
    let spec_with_fds = PoolSpec {
        max_client_connections: 12_000,
        fd_budget: Some(3),
        ..pool(100)
    };
    let mut allocator =
        Allocator::with_clock(spec_with_fds, AdmissionSpec::default(), ManualClock::new()).unwrap();
    tenant(&mut allocator, "acme", spec(0, 100));

    for _ in 0..3 {
        allocator.connect_client(&"acme".into()).unwrap();
    }
    let rejected = allocator.connect_client(&"acme".into()).unwrap_err();
    assert_eq!(
        rejected,
        ConnectRejection::Denied(DenialReason::PoolClientCap { live: 3, max: 3 })
    );
}

#[test]
fn session_mode_collapses_the_two_currencies_into_one() {
    let session = PoolSpec {
        mode: PoolMode::Session,
        ..pool(100)
    };
    let mut allocator =
        Allocator::with_clock(session, AdmissionSpec::default(), ManualClock::new()).unwrap();
    tenant(
        &mut allocator,
        "acme",
        TenantSpec {
            max_client_connections: 500,
            ..spec(0, 4)
        },
    );

    for _ in 0..4 {
        allocator.connect_client(&"acme".into()).unwrap();
    }
    let rejected = allocator.connect_client(&"acme".into()).unwrap_err();
    assert_eq!(
        rejected,
        ConnectRejection::Denied(DenialReason::TenantClientCap {
            tenant: "acme".into(),
            live: 4,
            max: 4
        }),
        "maxClientConnections is clamped to burstable in session mode"
    );
}

#[test]
fn an_unknown_tenant_is_a_routing_failure_not_a_capacity_decision() {
    let mut allocator = allocator(10);
    assert_eq!(
        allocator.connect_client(&"ghost".into()).unwrap_err(),
        ConnectRejection::UnknownTenant("ghost".into())
    );
}

#[test]
fn the_cross_tenant_scheduler_runs_only_when_free_capacity_is_gone() {
    let mut allocator = allocator(4);
    let heavy = TenantSpec {
        weight: 400,
        ..spec(0, 8)
    };
    let light = TenantSpec {
        weight: 100,
        ..spec(0, 8)
    };
    allocator.add_tenant("heavy".into(), heavy).unwrap();
    allocator.add_tenant("light".into(), light).unwrap();

    let heavy_held = hold(&mut allocator, "heavy", 2);
    let light_held = hold(&mut allocator, "light", 2);
    assert_eq!(allocator.budget().free(), 0);

    // `light` arrives first; the weighted-deficit order would prefer `heavy`.
    let Admission::Queued { ticket: first, .. } = checkout(&mut allocator, "light") else {
        panic!("expected a queue");
    };
    assert!(matches!(
        checkout(&mut allocator, "heavy"),
        Admission::Queued { .. }
    ));
    assert_eq!(
        allocator.scheduler_order(),
        vec![TenantId::from("heavy"), TenantId::from("light")]
    );

    // Freeing a slot makes free() > 0, and arrival order takes over.
    let grants = allocator.backend_died(heavy_held[0].server);
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].ticket, first);
    assert_eq!(grants[0].tenant, TenantId::from("light"));

    drop((heavy_held, light_held));
}

#[test]
fn the_guarantee_deficit_outranks_the_burst_fraction() {
    let mut allocator = allocator(8);
    tenant(&mut allocator, "hog", spec(0, 8));
    tenant(&mut allocator, "vip", spec(3, 8));

    let _hog = hold(&mut allocator, "hog", 5);
    assert_eq!(allocator.budget().free(), 0);
    assert!(matches!(
        checkout(&mut allocator, "hog"),
        Admission::Queued { .. }
    ));

    let _vip = hold(&mut allocator, "vip", 3);
    assert_eq!(allocator.budget().live_total(), 8);
    allocator
        .set_tenant_spec(&"vip".into(), spec(5, 8))
        .unwrap();
    assert!(matches!(
        checkout(&mut allocator, "vip"),
        Admission::Queued { .. }
    ));

    assert_eq!(
        allocator.scheduler_order(),
        vec![TenantId::from("vip"), TenantId::from("hog")],
        "a tenant below its guarantee is served before any burst"
    );
}

#[test]
fn a_heavier_workload_class_takes_a_larger_share_of_the_contended_surplus() {
    let mut allocator = allocator(10);
    let heavy = TenantSpec {
        weight: 400,
        ..spec(0, 10)
    };
    let light = TenantSpec {
        weight: 100,
        ..spec(0, 10)
    };
    allocator.add_tenant("heavy".into(), heavy).unwrap();
    allocator.add_tenant("light".into(), light).unwrap();

    let _heavy = hold(&mut allocator, "heavy", 6);
    let _light = hold(&mut allocator, "light", 4);
    assert_eq!(allocator.budget().free(), 0);
    assert!(matches!(
        checkout(&mut allocator, "light"),
        Admission::Queued { .. }
    ));
    assert!(matches!(
        checkout(&mut allocator, "heavy"),
        Admission::Queued { .. }
    ));

    // Raw fractions are 0.6 and 0.4, but weighting divides by 400 and 100.
    assert_eq!(
        allocator.scheduler_order(),
        vec![TenantId::from("heavy"), TenantId::from("light")]
    );
}

#[test]
fn the_fifo_strategy_ignores_deficit_entirely() {
    let admission = AdmissionSpec {
        strategy: AdmissionStrategy::Fifo,
        ..AdmissionSpec::default()
    };
    let mut allocator = Allocator::with_clock(pool(4), admission, ManualClock::new()).unwrap();
    tenant(&mut allocator, "hog", spec(0, 8));
    tenant(&mut allocator, "vip", spec(0, 4));

    let _hog = hold(&mut allocator, "hog", 4);
    checkout(&mut allocator, "hog");
    allocator
        .set_tenant_spec(&"vip".into(), spec(2, 4))
        .unwrap();
    checkout(&mut allocator, "vip");

    assert_eq!(
        allocator.scheduler_order(),
        vec![TenantId::from("hog"), TenantId::from("vip")]
    );
}

#[test]
fn within_a_tenant_the_queue_is_strict_fifo() {
    let mut allocator = allocator(1);
    tenant(&mut allocator, "acme", spec(0, 4));
    let held = granted(&mut allocator, "acme");

    let mut tickets = Vec::new();
    for _ in 0..3 {
        let Admission::Queued { ticket, .. } = checkout(&mut allocator, "acme") else {
            panic!("expected a queue");
        };
        tickets.push(ticket);
    }

    let outcome = allocator.release(held);
    assert_eq!(outcome.grants.len(), 1);
    assert_eq!(outcome.grants[0].ticket, tickets[0]);
}

#[test]
fn a_released_server_wakes_the_queue() {
    let mut allocator = allocator(2);
    tenant(&mut allocator, "acme", spec(0, 4));
    let mut held = hold(&mut allocator, "acme", 2);
    let Admission::Queued { ticket, .. } = checkout(&mut allocator, "acme") else {
        panic!("expected a queue");
    };

    let outcome = allocator.release(held.pop().unwrap());
    assert_eq!(outcome.grants.len(), 1);
    assert_eq!(outcome.grants[0].ticket, ticket);
    assert_eq!(allocator.queued_total(), 0);
}

#[test]
fn quorum_loss_pins_backends_without_breaching_the_budget() {
    let mut allocator = allocator(4);
    tenant(&mut allocator, "acme", spec(0, 8));
    let mut held = hold(&mut allocator, "acme", 4);
    let Admission::Queued { .. } = checkout(&mut allocator, "acme") else {
        panic!("expected a queue");
    };

    allocator.set_quorum_lost(true);
    let outcome = allocator.release(held.pop().unwrap());
    let Disposition::Pinned(lease) = outcome.disposition else {
        panic!("a blocked COMMIT has not returned, so its backend is pinned");
    };
    assert!(outcome.grants.is_empty());
    assert_eq!(allocator.budget().live_total(), 4);
    assert_eq!(allocator.queued_total(), 1);

    allocator.set_quorum_lost(false);
    let outcome = allocator.release(lease);
    assert_eq!(outcome.grants.len(), 1);
}

#[test]
fn a_dead_backend_frees_its_slot_and_is_counted_closed() {
    let mut allocator = allocator(2);
    tenant(&mut allocator, "acme", spec(0, 4));
    let held = hold(&mut allocator, "acme", 2);
    let Admission::Queued { ticket, .. } = checkout(&mut allocator, "acme") else {
        panic!("expected a queue");
    };

    let grants = allocator.backend_died(held[0].server);
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].ticket, ticket);

    let accounting = allocator.accounting();
    assert_eq!(accounting.closed, 1);
    assert!(accounting.conserved());
}

#[test]
fn releasing_a_stale_lease_is_a_no_op() {
    let mut allocator = allocator(2);
    tenant(&mut allocator, "acme", spec(0, 4));
    let lease = granted(&mut allocator, "acme");
    let server = lease.server;

    allocator.disconnect_client(lease.client);
    assert_eq!(allocator.server(server).unwrap().state, ServerState::Idle);

    let outcome = allocator.release(lease);
    assert_eq!(outcome.disposition, Disposition::Stale);
    assert_eq!(allocator.server(server).unwrap().state, ServerState::Idle);
}

#[test]
fn an_over_committed_tenant_is_rejected_at_admission() {
    let mut allocator = Allocator::with_clock(
        PoolSpec {
            backend_connections: 100,
            headroom_percent: 25,
            max_oversubscription: None,
            ..PoolSpec::default()
        },
        AdmissionSpec::default(),
        ManualClock::new(),
    )
    .unwrap();

    allocator.add_tenant("a".into(), spec(70, 70)).unwrap();
    let err = allocator.add_tenant("b".into(), spec(10, 10)).unwrap_err();
    assert_eq!(
        err,
        ConfigError::OverCommitted {
            reserved: 80,
            allocatable: 75
        }
    );
    assert!(allocator.budget().tenant(&"b".into()).is_none());
}

#[test]
fn the_published_ledger_matches_the_budget() {
    let mut allocator = allocator(100);
    tenant(&mut allocator, "a", spec(10, 60));
    tenant(&mut allocator, "b", spec(20, 80));
    let _held = hold(&mut allocator, "a", 30);

    let ledger = allocator.budget().reservations();
    assert_eq!(ledger.total, 100);
    assert_eq!(ledger.allocatable, 100);
    assert_eq!(ledger.reserved, 30);
    assert_eq!(ledger.available, 70);
    assert_eq!(ledger.committed_burst, 140);
    assert_eq!(ledger.oversubscription.to_string(), "1.40");
    assert_eq!(allocator.budget().free(), 100 - 30 - 20);
}

#[test]
fn shrinking_below_the_live_count_is_rejected_until_the_pool_has_drained() {
    let mut allocator = allocator(8);
    tenant(&mut allocator, "acme", spec(0, 8));
    let mut held = hold(&mut allocator, "acme", 8);

    let err = allocator.set_pool_spec(pool(4)).unwrap_err();
    assert_eq!(
        err,
        ConfigError::ShrinkBelowLive {
            scope: ShrinkScope::PoolCapacity,
            target: 4,
            live: 8
        }
    );

    assert_eq!(allocator.drain_to(4), 4);
    let marked = allocator.close_needed_active();
    assert_eq!(marked.len(), 4);
    for lease in held
        .drain(..)
        .filter(|lease| marked.contains(&lease.server))
    {
        allocator.release(lease);
    }
    assert_eq!(allocator.budget().live_total(), 4);
    assert!(allocator.set_pool_spec(pool(4)).is_ok());
}

#[test]
fn lowering_a_burst_ceiling_below_the_live_count_is_rejected() {
    let mut allocator = allocator(8);
    tenant(&mut allocator, "acme", spec(0, 8));
    let _held = hold(&mut allocator, "acme", 6);

    let err = allocator
        .set_tenant_spec(&"acme".into(), spec(0, 4))
        .unwrap_err();
    assert_eq!(
        err,
        ConfigError::ShrinkBelowLive {
            scope: ShrinkScope::TenantBurstable,
            target: 4,
            live: 6
        }
    );
}

#[test]
fn a_departing_tenant_returns_its_whole_reservation() {
    let mut allocator = allocator(10);
    tenant(&mut allocator, "leaver", spec(4, 8));
    tenant(&mut allocator, "stayer", spec(0, 10));
    let _held = hold(&mut allocator, "leaver", 2);
    assert_eq!(allocator.budget().free(), 6);

    allocator.remove_tenant(&"leaver".into()).unwrap();
    assert_eq!(allocator.budget().free(), 10);
    assert_eq!(allocator.budget().reserved(), 0);
    assert_eq!(allocator.budget().live_total(), 0);
    assert!(allocator.accounting().conserved());
}

#[test]
fn the_budget_is_the_same_arithmetic_the_webhook_runs() {
    let mut budget = CapacityBudget::new(PoolSpec {
        backend_connections: 900,
        headroom_percent: 25,
        max_oversubscription: None,
        ..PoolSpec::default()
    })
    .unwrap();
    budget.insert_tenant("a".into(), spec(200, 2_000)).unwrap();
    budget.insert_tenant("b".into(), spec(10, 140)).unwrap();

    let from_budget = budget.reservations();
    let from_check = pgelastic_capacity::check_reservations(
        budget.spec(),
        budget.iter().map(|(id, entry)| (id, entry.spec())),
    )
    .unwrap();
    assert_eq!(from_budget, from_check);
}

#[test]
fn revocation_never_marks_more_servers_than_a_victims_surplus() {
    let mut allocator = allocator(6);
    tenant(&mut allocator, "hog", spec(3, 6));
    tenant(&mut allocator, "vip", spec(0, 4));

    let hog = hold(&mut allocator, "hog", 6);
    allocator
        .set_tenant_spec(&"vip".into(), spec(3, 4))
        .unwrap();

    for _ in 0..4 {
        checkout(&mut allocator, "vip");
    }
    assert_eq!(
        allocator.close_needed_active().len(),
        3,
        "the hog's surplus over its own guarantee is 3, and no more"
    );

    let marked = allocator.close_needed_active();
    for lease in hog.into_iter().filter(|l| marked.contains(&l.server)) {
        allocator.release(lease);
    }
    assert_eq!(live(&allocator, "hog"), 3);
    assert_eq!(live(&allocator, "vip"), 3);
}

#[test]
fn lowering_a_burst_ceiling_withholds_new_cancel_credit_without_tearing_down_cancels() {
    let mut allocator = allocator(100);
    tenant(&mut allocator, "acme", spec(0, 20));
    for _ in 0..3 {
        assert!(matches!(
            request(&mut allocator, "acme", RequestKind::Cancel),
            Admission::Granted(_)
        ));
    }

    allocator
        .set_tenant_spec(&"acme".into(), spec(0, 2))
        .unwrap();
    assert_eq!(allocator.cancel_in_flight(&"acme".into()), 3);
    assert!(matches!(
        request(&mut allocator, "acme", RequestKind::Cancel),
        Admission::Denied(DenialReason::CancelCredit { limit: 2, .. })
    ));
}

/// The allocator carries no interior mutability, no atomic and no lock: every
/// mutating method takes `&mut self`, so concurrent checkout is impossible
/// without an external lock and there is nothing for a race detector to find.
/// This is why there is no `shuttle` harness in this crate.
#[test]
fn the_allocator_is_sendable_and_requires_exclusive_access_to_mutate() {
    fn assert_send<T: Send>() {}
    assert_send::<Allocator>();
    assert_send::<Lease>();

    let mut allocator = allocator(4);
    tenant(&mut allocator, "acme", spec(0, 4));
    let borrowed: &Allocator<ManualClock> = &allocator;
    assert_eq!(borrowed.budget().free(), 4);
}
