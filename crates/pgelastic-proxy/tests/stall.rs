//! Proactive write-stall detection, against two real `postgres:18` primaries.
//!
//! A stall is produced the way `dataDurability: Required` produces one: the
//! instance is told to require a synchronous standby that does not exist, and
//! reloaded. From that instant every commit on it parks in `IPC.SyncRep` and
//! never returns — which is exactly what these tests assert first, because a
//! test for a detector is worthless without evidence the condition it detects
//! is real.
//!
//! The second container is the point. "Bounded to the affected instance" is a
//! claim about a bystander, and the only bystander worth anything is one
//! sharing the same proxy process, the same listener and the same runtime as
//! the casualty.

mod harness;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use harness::{Fleet, Postgres, until};

/// Makes an instance unable to commit, exactly as losing quorum does.
async fn require_a_standby_that_does_not_exist(fleet: &Fleet, pg: &Postgres) {
    let admin = fleet.observer(pg, "pgelastic_stall_admin").await;
    // The session that arms the stall must not be caught by it. `ALTER SYSTEM`
    // writes postgresql.auto.conf rather than WAL, but a later statement on
    // this connection would wait, and a test that hangs in its own setup
    // reports nothing.
    admin
        .simple_query("SET synchronous_commit = local")
        .await
        .expect("the admin session opts out of synchronous replication");
    admin
        .simple_query("ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (\"pgelastic-ghost\")'")
        .await
        .expect("arming the stall");
    admin
        .simple_query("SELECT pg_reload_conf()")
        .await
        .expect("reloading the configuration");
}

async fn allow_commits_again(fleet: &Fleet, pg: &Postgres) {
    let admin = fleet.observer(pg, "pgelastic_stall_admin").await;
    admin
        .simple_query("SET synchronous_commit = local")
        .await
        .expect("the admin session opts out of synchronous replication");
    admin
        .simple_query("ALTER SYSTEM RESET synchronous_standby_names")
        .await
        .expect("clearing the stall");
    admin
        .simple_query("SELECT pg_reload_conf()")
        .await
        .expect("reloading the configuration");
}

async fn create_ledger(fleet: &Fleet, pg: &Postgres) {
    let admin = fleet.observer(pg, "pgelastic_stall_setup").await;
    admin
        .simple_query("CREATE TABLE ledger (id serial PRIMARY KEY, note text)")
        .await
        .expect("creating the table");
}

/// Backends parked waiting for a synchronous standby to acknowledge a commit.
async fn backends_in_sync_rep(fleet: &Fleet, pg: &Postgres) -> i64 {
    let observer = fleet.observer(pg, "pgelastic_syncrep_observer").await;
    observer
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
            &[],
        )
        .await
        .expect("counting parked backends")
        .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_stalled_instance_is_detected_and_refuses_instead_of_pinning_the_pool() {
    let fleet = Fleet::start("[stall]\nintervalMs = 100\nconfirmations = 2\n").await;
    create_ledger(&fleet, &fleet.a).await;
    create_ledger(&fleet, &fleet.b).await;

    let alpha = fleet.connect_as("alpha").await;
    alpha
        .execute("INSERT INTO ledger (note) VALUES ('before')", &[])
        .await
        .expect("alpha writes to a healthy instance");
    assert_eq!(fleet.control.write_health("inst-a").await, "writable");

    let armed = Instant::now();
    require_a_standby_that_does_not_exist(&fleet, &fleet.a).await;
    until(
        "inst-a to be reported write-stalled",
        Duration::from_secs(10),
        async || fleet.control.write_health("inst-a").await == "stalled",
    )
    .await;
    let detection_lag = armed.elapsed();
    println!("stall detection lag: {detection_lag:?}");
    assert!(
        detection_lag < Duration::from_secs(5),
        "detection took {detection_lag:?}"
    );
    assert_eq!(
        fleet.control.write_health("inst-b").await,
        "writable",
        "the healthy instance's verdict must not move because another one stalled"
    );

    // Fail fast, not queue: the pool's admission timeout is two minutes, so a
    // refusal that arrives in under a second is the whole difference.
    let refused = Instant::now();
    let error = fleet
        .connect_as("alpha")
        .await
        .execute("INSERT INTO ledger (note) VALUES ('during')", &[])
        .await
        .expect_err("a write onto a stalled instance must be refused");
    let refusal_latency = refused.elapsed();
    println!("fail-fast latency: {refusal_latency:?}");
    assert_eq!(
        error.as_db_error().map(|db| db.code().code()),
        Some("57P03"),
        "expected the distinguished write-stall refusal, got {error}"
    );
    let message = error
        .as_db_error()
        .map(|db| db.message().to_owned())
        .unwrap_or_default();
    assert!(
        message.contains("PGE5703"),
        "the refusal must carry its own code, got {message:?}"
    );
    assert!(
        refusal_latency < Duration::from_secs(2),
        "the refusal took {refusal_latency:?}, which is a queue rather than a fail-fast"
    );

    assert_eq!(
        backends_in_sync_rep(&fleet, &fleet.a).await,
        0,
        "no backend may be parked in IPC.SyncRep once the instance is known to be stalled"
    );

    allow_commits_again(&fleet, &fleet.a).await;
    until(
        "inst-a to be writable again",
        Duration::from_secs(10),
        async || fleet.control.write_health("inst-a").await == "writable",
    )
    .await;
    fleet
        .connect_as("alpha")
        .await
        .execute("INSERT INTO ledger (note) VALUES ('after')", &[])
        .await
        .expect("alpha writes again once quorum is back");

    fleet.proxy.running.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_on_another_instance_is_provably_unaffected_by_the_stall() {
    let fleet = Fleet::start("[stall]\nintervalMs = 100\nconfirmations = 2\n").await;
    create_ledger(&fleet, &fleet.a).await;
    create_ledger(&fleet, &fleet.b).await;

    // Beta writes continuously throughout, and every failure it sees is
    // recorded rather than swallowed.
    let beta_writes = Arc::new(AtomicUsize::new(0));
    let beta_failures = Arc::new(AtomicUsize::new(0));
    let beta_max_latency = Arc::new(std::sync::Mutex::new(Duration::ZERO));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let beta = fleet.connect_as("beta").await;
    let writes = Arc::clone(&beta_writes);
    let failures = Arc::clone(&beta_failures);
    let worst = Arc::clone(&beta_max_latency);
    let halt = Arc::clone(&stop);
    let bystander = tokio::spawn(async move {
        while !halt.load(Ordering::Relaxed) {
            let started = Instant::now();
            if beta
                .execute("INSERT INTO ledger (note) VALUES ('beta')", &[])
                .await
                .is_err()
            {
                failures.fetch_add(1, Ordering::Relaxed);
                return;
            }
            writes.fetch_add(1, Ordering::Relaxed);
            {
                let mut worst = worst.lock().expect("not poisoned");
                *worst = (*worst).max(started.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    require_a_standby_that_does_not_exist(&fleet, &fleet.a).await;
    until(
        "inst-a to be reported write-stalled",
        Duration::from_secs(10),
        async || fleet.control.write_health("inst-a").await == "stalled",
    )
    .await;

    // Twenty alpha clients pile onto the stalled instance. Without the
    // detector each would take a backend and park it in IPC.SyncRep; with it,
    // each is refused without one.
    let mut refusals = Vec::new();
    for _ in 0..20 {
        let url = fleet.url_for("alpha");
        // A tenant with no cached greeting is refused at connect and one with
        // a cached greeting at its first statement; both are the same refusal
        // and neither may take a backend, so the code is read off whichever
        // step produced it.
        refusals.push(tokio::spawn(async move {
            let code = |error: tokio_postgres::Error| {
                error.as_db_error().map_or_else(
                    || format!("no SQLSTATE: {error}"),
                    |db| db.code().code().to_owned(),
                )
            };
            let (client, connection) =
                match tokio_postgres::connect(&url, tokio_postgres::NoTls).await {
                    Ok(pair) => pair,
                    Err(error) => return code(error),
                };
            tokio::spawn(async move {
                let _ = connection.await;
            });
            match client
                .execute("INSERT INTO ledger (note) VALUES ('doomed')", &[])
                .await
            {
                Ok(_) => "the write was not refused at all".to_owned(),
                Err(error) => code(error),
            }
        }));
    }
    for refusal in refusals {
        let code = tokio::time::timeout(Duration::from_secs(10), refusal)
            .await
            .expect("no alpha client may hang against a stalled instance")
            .expect("the client task must not panic");
        assert_eq!(code, "57P03", "every alpha write is refused");
    }

    assert_eq!(
        backends_in_sync_rep(&fleet, &fleet.a).await,
        0,
        "twenty refused writes must have parked nothing"
    );

    let before = beta_writes.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let during = beta_writes.load(Ordering::Relaxed) - before;
    stop.store(true, Ordering::Relaxed);
    bystander.await.expect("the bystander must not panic");

    assert_eq!(
        beta_failures.load(Ordering::Relaxed),
        0,
        "a tenant on another instance must not see a single failure"
    );
    assert!(
        during > 10,
        "beta committed only {during} writes while alpha's instance was stalled"
    );
    let worst = *beta_max_latency.lock().expect("not poisoned");
    println!("bystander tenant's worst commit latency during the stall: {worst:?}");
    assert!(
        worst < Duration::from_secs(2),
        "the bystander's worst commit took {worst:?}, so the stall did reach it"
    );

    allow_commits_again(&fleet, &fleet.a).await;
    fleet.proxy.running.shutdown().await;
}

/// The counterfactual: with the refusal switched off, the stall really does
/// pin a pooled backend.
///
/// Without this the fail-fast test proves only that a refusal happens, not that
/// it is preventing anything.
#[tokio::test(flavor = "multi_thread")]
async fn without_the_refusal_a_stalled_commit_pins_the_backend_it_is_holding() {
    let fleet =
        Fleet::start("[stall]\nintervalMs = 100\nconfirmations = 2\nfailFast = false\n").await;
    create_ledger(&fleet, &fleet.a).await;

    let alpha = fleet.connect_as("alpha").await;
    alpha
        .execute("INSERT INTO ledger (note) VALUES ('before')", &[])
        .await
        .expect("alpha writes to a healthy instance");

    require_a_standby_that_does_not_exist(&fleet, &fleet.a).await;
    until(
        "inst-a to be reported write-stalled",
        Duration::from_secs(10),
        async || fleet.control.write_health("inst-a").await == "stalled",
    )
    .await;

    let hanging = tokio::spawn(async move {
        alpha
            .execute("INSERT INTO ledger (note) VALUES ('stuck')", &[])
            .await
    });
    until(
        "a backend to park in IPC.SyncRep",
        Duration::from_secs(15),
        async || backends_in_sync_rep(&fleet, &fleet.a).await > 0,
    )
    .await;
    assert!(
        !hanging.is_finished(),
        "the commit must still be waiting for an acknowledgement that will never come"
    );

    // Releasing the stall lets the parked commit complete, which is also the
    // proof it was parked rather than failed.
    allow_commits_again(&fleet, &fleet.a).await;
    tokio::time::timeout(Duration::from_secs(30), hanging)
        .await
        .expect("the parked commit must complete once quorum returns")
        .expect("the client task must not panic")
        .expect("the commit itself must succeed");

    fleet.proxy.running.shutdown().await;
}
