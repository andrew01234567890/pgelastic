//! A planned role change: every tenant on one instance held, the epoch bumped
//! underneath them, and nobody dropped.
//!
//! The pair of claims this file exists for are the two halves of the same
//! sentence. **A tenant somebody is deliberately holding is queued, not
//! severed, when the epoch advances** — because a quiesce is the only thing in
//! the system that can say a promotion was chosen rather than forced, the epoch
//! itself being identical either way. And **a tenant nobody is holding is still
//! severed**, because softening that would be a silent data-loss change dressed
//! up as an improvement.
//!
//! Both are asserted over sockets opened *before* the epoch moved and still the
//! same sockets afterwards. A test that reconnected would prove nothing: every
//! reconnection succeeds, which is exactly the outcome the design refuses to
//! rely on.

mod harness;

use std::time::Duration;

use harness::{Fleet, Postgres, until};

const SWITCHOVER: &str = "pgelastic-switchover";
const MIGRATION: &str = "pgelastic-migration";

/// The push endpoint stands in for the promoting agent, which calls it before
/// it writes `currentPrimary`. It addresses the default instance, which is
/// `inst-a` — so `inst-b` is a bystander whose epoch never moves.
async fn fleet_with_push() -> (Fleet, u16) {
    let admin = harness::free_port().await;
    let fleet = Fleet::start_sized(
        8,
        8,
        &format!("[fence]\npushAddress = \"127.0.0.1:{admin}\"\n"),
    )
    .await;
    (fleet, admin)
}

/// Pushes an epoch as the promoting agent does.
async fn promote_to(port: u16, epoch: u64) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let body = epoch.to_string();
    let request = format!(
        "POST /epoch HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("reaching the push endpoint");
    socket
        .write_all(request.as_bytes())
        .await
        .expect("sending the push");
    let mut answer = String::new();
    socket
        .read_to_string(&mut answer)
        .await
        .expect("reading the push answer");
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
}

/// The SQLSTATE and message a client actually received. `tokio_postgres::Error`
/// renders as the bare words "db error", so asserting on its Display would
/// assert that something went wrong rather than that the fence said so.
fn reported(error: &tokio_postgres::Error) -> String {
    error.as_db_error().map_or_else(
        || error.to_string(),
        |db| format!("{} {}", db.code().code(), db.message()),
    )
}

async fn create_ledger(fleet: &Fleet, pg: &Postgres) {
    let admin = fleet.observer(pg, "pgelastic_switchover_setup").await;
    admin
        .simple_query("CREATE TABLE ledger (seq serial PRIMARY KEY, tenant text)")
        .await
        .expect("creating the table");
}

async fn rows_on(fleet: &Fleet, pg: &Postgres) -> Vec<String> {
    let observer = fleet.observer(pg, "pgelastic_switchover_observer").await;
    observer
        .query("SELECT tenant FROM ledger ORDER BY seq", &[])
        .await
        .expect("reading the ledger")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// The headline. A read is in flight on the old epoch when the promotion lands;
/// it is allowed to finish, and because the instance is held the client keeps
/// its socket and its next transaction queues instead of failing.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_tenant_survives_the_promotion_on_the_same_socket() {
    let (fleet, admin) = fleet_with_push().await;
    create_ledger(&fleet, &fleet.a).await;
    let alpha = fleet.connect_as("alpha").await;
    alpha.simple_query("SELECT 1").await.expect("a first query");

    // The real sequence, in the real order: a transaction is already running
    // when the hold is taken, and the drain is what waits for it. Holding first
    // and starting the read afterwards would prove nothing, because the read
    // would never have reached a backend at all.
    let reader = tokio::spawn(async move {
        let outcome = alpha
            .simple_query("SELECT pg_sleep(1), 42 AS answer")
            .await
            .map(|_| ());
        (alpha, outcome)
    });
    until(
        "the read to be counted against the instance",
        Duration::from_secs(5),
        async || fleet.control.instance_drain_status("inst-a").await["inFlight"] == 1,
    )
    .await;

    let report = fleet
        .control
        .quiesce_instance("inst-a", SWITCHOVER, 30_000)
        .await
        .ok();
    assert_eq!(
        report["drained"], false,
        "a switchover must not believe an instance is drained while a read is running on it: \
         {report}"
    );

    promote_to(admin, 9).await;

    let (alpha, outcome) = reader.await.expect("the reader task");
    assert!(
        outcome.is_ok(),
        "a read on the old epoch cannot cause split brain and must be allowed to finish: \
         {outcome:?}"
    );
    until(
        "the instance to drain once the read has been answered",
        Duration::from_secs(10),
        async || fleet.control.instance_drain_status("inst-a").await["drained"] == true,
    )
    .await;

    // The same socket, after the promotion. It queues rather than failing.
    let queued = tokio::spawn(async move {
        alpha
            .execute("INSERT INTO ledger (tenant) VALUES ('alpha')", &[])
            .await
    });
    until(
        "the next transaction to queue behind the hold",
        Duration::from_secs(5),
        async || {
            assert!(
                !queued.is_finished(),
                "the client's next transaction did not queue, which means its socket did not \
                 survive the promotion"
            );
            fleet.control.instance_drain_status("inst-a").await["queued"] == 1
        },
    )
    .await;

    fleet
        .control
        .resume_instance("inst-a", SWITCHOVER)
        .await
        .ok();
    queued
        .await
        .expect("the queued task")
        .expect("a held client must be queued, never dropped");

    assert_eq!(rows_on(&fleet, &fleet.a).await, vec!["alpha".to_owned()]);
    fleet
        .control
        .unquiesce_instance("inst-a", SWITCHOVER)
        .await
        .ok();
    fleet.proxy.running.shutdown().await;
}

/// The regression guard, and the reason the one above is evidence rather than a
/// coincidence: the same script with nobody holding the instance ends the
/// client's session. If this ever passes, the fence has been softened.
#[tokio::test(flavor = "multi_thread")]
async fn an_unheld_tenant_is_still_severed_by_the_same_promotion() {
    let (fleet, admin) = fleet_with_push().await;
    create_ledger(&fleet, &fleet.a).await;
    let alpha = fleet.connect_as("alpha").await;
    alpha.simple_query("SELECT 1").await.expect("a first query");

    let reader = tokio::spawn(async move {
        let outcome = alpha
            .simple_query("SELECT pg_sleep(1), 42 AS answer")
            .await
            .map(|_| ());
        (alpha, outcome)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    promote_to(admin, 9).await;

    let (alpha, outcome) = reader.await.expect("the reader task");
    assert!(outcome.is_ok(), "the read still finishes: {outcome:?}");

    let error = alpha
        .execute("INSERT INTO ledger (tenant) VALUES ('alpha')", &[])
        .await
        .expect_err("an unheld session must not survive an epoch advance");
    assert!(
        reported(&error).contains("PGE2506") || reported(&error).contains("closed"),
        "{}",
        reported(&error)
    );
    assert!(rows_on(&fleet, &fleet.a).await.is_empty());
    fleet.proxy.running.shutdown().await;
}

/// One call holds every tenant the instance serves, including the ones the
/// routing table never names because they are on the default instance — and the
/// tenant on the other instance carries on.
#[tokio::test(flavor = "multi_thread")]
async fn one_instance_hold_covers_every_tenant_on_it_and_no_others() {
    let fleet = Fleet::start_sized(8, 8, "").await;
    create_ledger(&fleet, &fleet.a).await;
    create_ledger(&fleet, &fleet.b).await;

    // alpha and gamma have no route of their own, so they are on the default
    // instance; beta is routed to inst-b.
    let alpha = fleet.connect_as("alpha").await;
    let gamma = fleet.connect_as("gamma").await;
    let beta = fleet.connect_as("beta").await;
    for (client, name) in [(&alpha, "alpha"), (&gamma, "gamma"), (&beta, "beta")] {
        client
            .simple_query(&format!("SELECT '{name}'"))
            .await
            .expect("a first query");
    }

    let report = fleet
        .control
        .quiesce_instance("inst-a", SWITCHOVER, 30_000)
        .await
        .ok();
    assert_eq!(report["quiesced"], true);
    assert_eq!(report["drained"], true, "nothing was in flight: {report}");
    let tenants: Vec<String> = report["tenants"]
        .as_array()
        .expect("tenants is an array")
        .iter()
        .map(|value| value.as_str().expect("a tenant name").to_owned())
        .collect();
    assert_eq!(
        tenants,
        vec!["alpha".to_owned(), "gamma".to_owned()],
        "a tenant with no route of its own is on the default instance and must be held too"
    );

    let held: Vec<_> = [("alpha", alpha), ("gamma", gamma)]
        .into_iter()
        .map(|(name, client)| {
            tokio::spawn(async move {
                client
                    .execute("INSERT INTO ledger (tenant) VALUES ($1)", &[&name])
                    .await
            })
        })
        .collect();
    until(
        "both tenants on the instance to be queued",
        Duration::from_secs(5),
        async || fleet.control.instance_drain_status("inst-a").await["queued"] == 2,
    )
    .await;

    // The bystander. Its instance was never held, so it is not merely
    // undropped, it is unblocked.
    beta.execute("INSERT INTO ledger (tenant) VALUES ('beta')", &[])
        .await
        .expect("a tenant on another instance is untouched by this hold");
    assert_eq!(rows_on(&fleet, &fleet.b).await, vec!["beta".to_owned()]);
    assert!(
        rows_on(&fleet, &fleet.a).await.is_empty(),
        "nothing on the held instance may run while it is held"
    );

    fleet
        .control
        .resume_instance("inst-a", SWITCHOVER)
        .await
        .ok();
    for handle in held {
        handle
            .await
            .expect("the queued task")
            .expect("a held client must be queued, never dropped");
    }
    let mut landed = rows_on(&fleet, &fleet.a).await;
    landed.sort();
    assert_eq!(landed, vec!["alpha".to_owned(), "gamma".to_owned()]);

    fleet
        .control
        .unquiesce_instance("inst-a", SWITCHOVER)
        .await
        .ok();
    fleet.proxy.running.shutdown().await;
}

/// The reason the instance hold is its own lease rather than the tenant leases
/// taken in bulk: a live migration of one tenant on the instance neither blocks
/// the switchover nor is blocked by it.
#[tokio::test(flavor = "multi_thread")]
async fn a_migration_and_a_switchover_hold_the_same_traffic_at_once() {
    let fleet = Fleet::start_sized(8, 8, "").await;
    create_ledger(&fleet, &fleet.a).await;
    let alpha = fleet.connect_as("alpha").await;
    alpha.simple_query("SELECT 1").await.expect("a first query");

    assert_eq!(
        fleet
            .control
            .quiesce("alpha", MIGRATION, 30_000)
            .await
            .status,
        200
    );
    assert_eq!(
        fleet
            .control
            .quiesce_instance("inst-a", SWITCHOVER, 30_000)
            .await
            .status,
        200,
        "a tenant already held by a migration must not refuse the instance hold"
    );

    let queued = tokio::spawn(async move {
        let outcome = alpha
            .execute("INSERT INTO ledger (tenant) VALUES ('alpha')", &[])
            .await;
        (alpha, outcome)
    });
    until(
        "the tenant to be queued",
        Duration::from_secs(5),
        async || fleet.control.drain_status("alpha").await["queued"] == 1,
    )
    .await;

    // Releasing one hold is not releasing the other: the client is still held
    // by the switchover, which is exactly what composing means.
    fleet.control.resume("alpha", MIGRATION).await.ok();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !queued.is_finished(),
        "the instance hold must keep holding after the tenant hold is released"
    );

    fleet
        .control
        .resume_instance("inst-a", SWITCHOVER)
        .await
        .ok();
    let (_alpha, outcome) = queued.await.expect("the queued task");
    outcome.expect("a client held by two leases must be released by the second");
    assert_eq!(rows_on(&fleet, &fleet.a).await, vec!["alpha".to_owned()]);

    fleet.proxy.running.shutdown().await;
}

/// A hold is a statement about the promotion, never about the transaction. A
/// write the old primary still owes an answer to is reset whoever is holding
/// the gate, and nothing it wrote is there afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn a_hold_does_not_save_a_write_the_old_primary_still_owes_an_answer() {
    let (fleet, admin) = fleet_with_push().await;
    create_ledger(&fleet, &fleet.a).await;
    let alpha = fleet.connect_as("alpha").await;
    alpha.simple_query("BEGIN").await.expect("begin");
    alpha
        .simple_query("INSERT INTO ledger (tenant) VALUES ('alpha')")
        .await
        .expect("the write");

    fleet
        .control
        .quiesce_instance("inst-a", SWITCHOVER, 30_000)
        .await
        .ok();
    promote_to(admin, 9).await;

    let error = alpha
        .simple_query("COMMIT")
        .await
        .expect_err("an open transaction cannot be carried across a promotion");
    assert!(
        reported(&error).contains("PGE2506") || reported(&error).contains("closed"),
        "{}",
        reported(&error)
    );
    fleet
        .control
        .resume_instance("inst-a", SWITCHOVER)
        .await
        .ok();
    assert!(
        rows_on(&fleet, &fleet.a).await.is_empty(),
        "the aborted transaction must have left nothing behind"
    );
    fleet.proxy.running.shutdown().await;
}

/// The in-doubt path, checked rather than assumed.
///
/// The claim under test is *not* that a graceful stop makes an unacknowledged
/// commit impossible — it does not, and this is the evidence. A commit that was
/// forwarded and never answered is undecidable whoever holds the gate, so the
/// hold leaves that row exactly where it was: `UNKNOWN`, recorded, never
/// retried, never claimed either way.
///
/// What actually keeps the row empty during a planned switchover is the
/// precondition, and it is asserted here too: such a commit holds a backend, so
/// `inFlight` counts it and `drained` is false for as long as it is outstanding.
/// A switchover that waits for `drained` therefore never promotes while one is
/// in the air. This test is what a switchover that jumped that gun would look
/// like.
#[tokio::test(flavor = "multi_thread")]
async fn a_hold_never_decides_a_commit_that_was_forwarded_and_never_answered() {
    let (fleet, admin) = fleet_with_push().await;
    create_ledger(&fleet, &fleet.a).await;

    // Quorum loss, which is how the design says a commit hangs in production:
    // dataDurability Required waiting on a synchronous standby that is not there.
    let dba = fleet
        .observer(&fleet.a, "pgelastic_switchover_quorum")
        .await;
    dba.simple_query("ALTER SYSTEM SET synchronous_standby_names = 'ghost'")
        .await
        .expect("naming an absent standby");
    dba.simple_query("SELECT pg_reload_conf()")
        .await
        .expect("reloading");

    let alpha = fleet.connect_as("alpha").await;
    until(
        "the backend to load the clause that will stall its commit",
        Duration::from_secs(10),
        async || {
            alpha
                .simple_query("SHOW synchronous_standby_names")
                .await
                .ok()
                .is_some_and(|messages| {
                    messages.iter().any(|message| {
                        matches!(message, tokio_postgres::SimpleQueryMessage::Row(row)
                            if row.get(0) == Some("ghost"))
                    })
                })
        },
    )
    .await;

    alpha.simple_query("BEGIN").await.expect("begin");
    alpha
        .simple_query("INSERT INTO ledger (tenant) VALUES ('alpha')")
        .await
        .expect("the write");
    let committer = tokio::spawn(async move { alpha.simple_query("COMMIT").await });

    let report = fleet
        .control
        .quiesce_instance("inst-a", SWITCHOVER, 30_000)
        .await
        .ok();
    assert_eq!(
        report["inFlight"], 1,
        "a commit in the air must be counted, or a switchover would think it had drained: \
         {report}"
    );
    assert_eq!(
        report["drained"], false,
        "this is the precondition that keeps the undecidable row empty: {report}"
    );

    // The switchover proceeding anyway, which the drain exists to prevent.
    promote_to(admin, 9).await;

    let error = committer
        .await
        .expect("the committer task")
        .expect_err("an unobserved commit is not a success");
    let text = reported(&error);
    assert!(
        text.contains("PGE4003") && text.contains("UNKNOWN"),
        "a held gate must not decide a commit nobody observed, got {text}"
    );
    assert!(
        !text.to_ascii_lowercase().contains("rolled back"),
        "the proxy must not claim an outcome it did not observe, got {text}"
    );

    fleet
        .control
        .resume_instance("inst-a", SWITCHOVER)
        .await
        .ok();
    fleet.proxy.running.shutdown().await;
}
