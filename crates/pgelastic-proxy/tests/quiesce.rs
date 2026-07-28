//! The cutover API, against two real `postgres:18` instances.
//!
//! The property under test is the one that distinguishes pgelastic's live
//! migration from an elastic-pool move: during the pause the clients are
//! **queued, not dropped**. So every assertion here is made through connections
//! that were opened before the quiesce and are still the same connections
//! afterwards — a test that reconnected would be proving the thing the design
//! explicitly refuses to rely on.

mod harness;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use harness::{Fleet, Postgres, until};

const HOLDER: &str = "pgelastic-migration";

async fn create_ledger(fleet: &Fleet, pg: &Postgres) {
    let admin = fleet.observer(pg, "pgelastic_cutover_setup").await;
    admin
        .simple_query("CREATE TABLE cutover (seq serial PRIMARY KEY, client int)")
        .await
        .expect("creating the table");
}

async fn rows_on(fleet: &Fleet, pg: &Postgres) -> Vec<i32> {
    let observer = fleet.observer(pg, "pgelastic_cutover_observer").await;
    observer
        .query("SELECT client FROM cutover ORDER BY seq", &[])
        .await
        .expect("reading the ledger")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn quiesce_holds_client_sockets_and_resume_completes_the_queue_in_order() {
    // One backend per instance: with a single link in flight at a time the
    // order the rows land in is the order the gate released the clients, so
    // "in order" is observed rather than inferred.
    let fleet = Fleet::start_sized(1, 1, "").await;
    create_ledger(&fleet, &fleet.a).await;

    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(fleet.connect_as("alpha").await);
    }

    fleet.control.quiesce("alpha", HOLDER, 30_000).await.ok();
    let status = fleet.control.drain_status("alpha").await;
    assert_eq!(status["quiesced"], true);
    assert_eq!(status["drained"], true, "nothing was in flight: {status}");

    let mut inflight = Vec::new();
    for (index, client) in clients.into_iter().enumerate() {
        inflight.push(tokio::spawn(async move {
            client
                .execute(
                    "INSERT INTO cutover (client) VALUES ($1)",
                    &[&i32::try_from(index).unwrap()],
                )
                .await
        }));
        until(
            "the transaction to be queued rather than refused",
            Duration::from_secs(5),
            async || fleet.control.drain_status("alpha").await["queued"] == index + 1,
        )
        .await;
    }

    // Held, not dropped: eight sockets are still open with eight transactions
    // parked behind the gate and nothing running.
    let status = fleet.control.drain_status("alpha").await;
    assert_eq!(status["queued"], 8);
    assert_eq!(status["inFlight"], 0);
    assert_eq!(rows_on(&fleet, &fleet.a).await, Vec::<i32>::new());

    let released = fleet.control.resume("alpha", HOLDER).await.ok();
    assert_eq!(released["released"], 8);

    for (index, handle) in inflight.into_iter().enumerate() {
        tokio::time::timeout(Duration::from_secs(30), handle)
            .await
            .unwrap_or_else(|_| panic!("queued client {index} never completed"))
            .expect("the client task must not panic")
            .unwrap_or_else(|error| panic!("queued client {index} was failed: {error}"));
    }

    assert_eq!(
        rows_on(&fleet, &fleet.a).await,
        (0..8).collect::<Vec<i32>>(),
        "the queue must drain in the order it filled"
    );

    fleet.control.unquiesce("alpha", HOLDER).await.ok();
    fleet.proxy.running.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn set_route_flips_the_backend_and_queued_clients_resume_against_the_new_one() {
    let fleet = Fleet::start("").await;
    create_ledger(&fleet, &fleet.a).await;
    create_ledger(&fleet, &fleet.b).await;

    let alpha = fleet.connect_as("alpha").await;
    alpha
        .execute("INSERT INTO cutover (client) VALUES (1)", &[])
        .await
        .expect("alpha writes to its source");
    assert_eq!(rows_on(&fleet, &fleet.a).await, vec![1]);

    fleet.control.quiesce("alpha", HOLDER, 30_000).await.ok();
    let queued = tokio::spawn(async move {
        let outcome = alpha
            .execute("INSERT INTO cutover (client) VALUES (2)", &[])
            .await;
        (alpha, outcome)
    });
    until(
        "the transaction to be queued",
        Duration::from_secs(5),
        async || fleet.control.drain_status("alpha").await["queued"] == 1,
    )
    .await;

    let flipped = fleet
        .control
        .set_route("alpha", HOLDER, "inst-b")
        .await
        .ok();
    assert_eq!(flipped["instance"], "inst-b");
    fleet.control.resume("alpha", HOLDER).await.ok();

    let (alpha, outcome) = tokio::time::timeout(Duration::from_secs(30), queued)
        .await
        .expect("the queued client must be released")
        .expect("the client task must not panic");
    outcome.expect("the queued transaction must succeed against the target");

    assert_eq!(
        rows_on(&fleet, &fleet.b).await,
        vec![2],
        "the queued transaction must have run on the target"
    );
    assert_eq!(
        rows_on(&fleet, &fleet.a).await,
        vec![1],
        "nothing more may reach the source after the flip"
    );

    // The same connection, never reopened, now serves from the target.
    alpha
        .execute("INSERT INTO cutover (client) VALUES (3)", &[])
        .await
        .expect("the client keeps its socket across the cutover");
    assert_eq!(rows_on(&fleet, &fleet.b).await, vec![2, 3]);
    assert_eq!(rows_on(&fleet, &fleet.a).await, vec![1]);

    fleet.control.unquiesce("alpha", HOLDER).await.ok();
    fleet.proxy.running.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_operator_auto_unquiesces_back_to_the_source_within_the_lease() {
    // The sweep interval is derived from the default lease, so a short default
    // is what makes "within the TTL" mean the TTL rather than the TTL plus a
    // coarse timer.
    let fleet = Fleet::start_leased(20, 20, 500, "").await;
    create_ledger(&fleet, &fleet.a).await;
    create_ledger(&fleet, &fleet.b).await;

    let alpha = fleet.connect_as("alpha").await;
    alpha
        .execute("INSERT INTO cutover (client) VALUES (1)", &[])
        .await
        .expect("alpha writes to its source");

    let ttl = Duration::from_millis(500);
    fleet.control.quiesce("alpha", HOLDER, 500).await.ok();
    fleet
        .control
        .set_route("alpha", HOLDER, "inst-b")
        .await
        .ok();

    let queued = tokio::spawn(async move {
        alpha
            .execute("INSERT INTO cutover (client) VALUES (2)", &[])
            .await
    });
    until(
        "the transaction to be queued",
        Duration::from_secs(5),
        async || fleet.control.drain_status("alpha").await["queued"] == 1,
    )
    .await;

    // The operator is now dead: no resume, no unquiesce, no renewal.
    let expired = Instant::now();
    until(
        "the lease to expire and unquiesce the tenant",
        Duration::from_secs(5),
        async || {
            let status = fleet.control.drain_status("alpha").await;
            status["quiesced"] == false && status["instance"] == "inst-a"
        },
    )
    .await;
    let recovery = expired.elapsed();
    println!("auto-unquiesce after a killed operator: {recovery:?}");
    assert!(
        recovery < ttl + Duration::from_millis(400),
        "the tenant stayed quiesced for {recovery:?}, well past its {ttl:?} lease"
    );

    tokio::time::timeout(Duration::from_secs(30), queued)
        .await
        .expect("the queued client must be released by the expiry")
        .expect("the client task must not panic")
        .expect("the queued transaction must succeed");

    assert_eq!(
        rows_on(&fleet, &fleet.a).await,
        vec![1, 2],
        "an abandoned cutover must leave the tenant serving from its source"
    );
    assert_eq!(
        rows_on(&fleet, &fleet.b).await,
        Vec::<i32>::new(),
        "nothing may have reached the target of a cutover that never committed"
    );

    fleet.proxy.running.shutdown().await;
}

/// The pause a live migration commits to, measured under load.
///
/// A no-op cutover — quiesce, drain, flip the route to where it already is,
/// resume — is the floor of what any real one costs, so it is the honest thing
/// to hold to a target.
#[tokio::test(flavor = "multi_thread")]
async fn a_quiesce_resume_round_trip_stays_under_a_hundred_milliseconds_under_load() {
    const ROUNDS: usize = 200;
    const WORKERS: usize = 8;

    let fleet = Fleet::start_sized(u32::try_from(WORKERS).unwrap(), 8, "").await;
    create_ledger(&fleet, &fleet.a).await;

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let committed = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let mut load = Vec::new();
    for worker in 0..WORKERS {
        let client = fleet.connect_as("alpha").await;
        let halt = Arc::clone(&stop);
        let ok = Arc::clone(&committed);
        let bad = Arc::clone(&failed);
        load.push(tokio::spawn(async move {
            while !halt.load(Ordering::Relaxed) {
                if client
                    .execute(
                        "INSERT INTO cutover (client) VALUES ($1)",
                        &[&i32::try_from(worker).unwrap()],
                    )
                    .await
                    .is_err()
                {
                    bad.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                ok.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Let the load actually start, so the round trips are measured against a
    // tenant with transactions in flight rather than an idle one.
    until(
        "the tenant to be under load",
        Duration::from_secs(10),
        async || committed.load(Ordering::Relaxed) > WORKERS * 4,
    )
    .await;

    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let started = Instant::now();
        fleet.control.quiesce("alpha", HOLDER, 30_000).await.ok();
        loop {
            if fleet.control.drain_status("alpha").await["drained"] == true {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "the tenant never drained"
            );
        }
        assert_eq!(
            fleet
                .control
                .set_route("alpha", HOLDER, "inst-a")
                .await
                .ok()["instance"],
            "inst-a"
        );
        fleet.control.resume("alpha", HOLDER).await.ok();
        samples.push(started.elapsed());
        fleet.control.unquiesce("alpha", HOLDER).await.ok();
    }

    stop.store(true, Ordering::Relaxed);
    for worker in load {
        worker.await.expect("a load worker must not panic");
    }

    assert_eq!(
        failed.load(Ordering::Relaxed),
        0,
        "no transaction may be failed by a cutover; {} committed",
        committed.load(Ordering::Relaxed)
    );

    samples.sort_unstable();
    // Nearest-rank: the p99 of 200 samples is the 198th, and rounding it any
    // other way would quietly report a smaller number than was measured.
    let at = |percent: usize| samples[(samples.len() * percent / 100).min(samples.len() - 1)];
    println!(
        "quiesce/resume round trip over {ROUNDS} no-op cutovers under {WORKERS} writers: \
         p50 {:?}, p99 {:?}, max {:?} ({} transactions committed throughout)",
        at(50),
        at(99),
        samples.last().expect("there is at least one sample"),
        committed.load(Ordering::Relaxed),
    );
    assert!(
        at(99) < Duration::from_millis(100),
        "p99 of the quiesce/resume round trip was {:?}",
        at(99)
    );

    fleet.proxy.running.shutdown().await;
}
