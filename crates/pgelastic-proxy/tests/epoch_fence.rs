//! The primary-epoch fence, end to end, against real `postgres:18` containers.
//!
//! The property under test cannot be reached from a unit test, because it is a
//! fact about TCP: a Kubernetes Service repointed at a new primary leaves every
//! established connection to the old one exactly where it was. [`Switch`] is
//! that Service — it forwards new connections to whichever container it
//! currently points at and does nothing whatever to the ones already open — so
//! "the old sockets are gone" is a statement about real file descriptors on
//! real postmasters, counted by the servers themselves rather than by the
//! proxy's own bookkeeping.

mod harness;

use std::time::{Duration, Instant};

use harness::{BACKEND_DATABASE, Postgres, ProxyUnderTest, Switch};

/// A short lease, so the tests exercise the same derivation the production
/// 15s/10s/2s does without spending fifteen seconds proving it. The fence
/// deadline is `retryPeriod` whatever the numbers are, which is the point.
const LEASE: &str = "[fence.lease]\n\
                     leaseDurationMs = 3000\n\
                     renewDeadlineMs = 2000\n\
                     retryPeriodMs = 400\n";

const FENCE_DEADLINE: Duration = Duration::from_millis(400);

/// Long enough for the periodic prober to have run several times.
const SETTLE: Duration = Duration::from_secs(4);

async fn postgres_at_epoch(epoch: u64) -> Postgres {
    harness::start_postgres_with(&format!("pgelastic.primary_epoch = '{epoch}'")).await
}

/// Client backends the proxy is holding on this container, counted by the
/// server itself. The prober and the observers name themselves, so what is left
/// is the pool's own sockets.
async fn held_backends(pg: &Postgres) -> i64 {
    let (client, connection) =
        tokio_postgres::connect(&pg.direct_url("pgelastic_counter"), tokio_postgres::NoTls)
            .await
            .expect("connecting past the proxy");
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let count = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_stat_activity \
             WHERE datname = $1 AND backend_type = 'client backend' \
             AND application_name = ''",
            &[&BACKEND_DATABASE],
        )
        .await
        .expect("counting backends")
        .get::<_, i64>(0);
    drop(client);
    task.abort();
    count
}

async fn await_backends(pg: &Postgres, wanted: i64, within: Duration) -> Duration {
    let started = Instant::now();
    loop {
        if held_backends(pg).await == wanted {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < within,
            "still holding {} backends after {:?}, wanted {wanted}",
            held_backends(pg).await,
            started.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn await_epoch(proxy: &ProxyUnderTest, wanted: u64, within: Duration) -> Duration {
    let started = Instant::now();
    loop {
        if metric(proxy, "pgelastic_proxy_primary_epoch") == i64::try_from(wanted).ok() {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < within,
            "the proxy is still at epoch {:?} after {:?}, wanted {wanted}",
            metric(proxy, "pgelastic_proxy_primary_epoch"),
            started.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn metric(proxy: &ProxyUnderTest, name: &str) -> Option<i64> {
    proxy
        .metrics
        .render()
        .lines()
        .find(|line| line.starts_with(name) && !line.starts_with('#'))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
}

fn series(proxy: &ProxyUnderTest, prefix: &str) -> u64 {
    proxy
        .metrics
        .render()
        .lines()
        .filter(|line| line.starts_with(prefix) && !line.starts_with('#'))
        .filter_map(|line| line.split_whitespace().nth(1))
        .filter_map(|value| value.parse::<u64>().ok())
        .sum()
}

/// Pushes an epoch through the admin endpoint, as the promoting agent does.
async fn push(port: u16, epoch: u64) -> String {
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
    answer
}

// ---- the partition case: the pull path with nothing else -----------------

/// The one that matters. Watch and push are both switched off, so the only way
/// the proxy can learn that the primary moved is by reading the epoch off a
/// backend connection — and it still severs every socket to the old primary
/// before it hands anybody a new one.
#[tokio::test]
async fn the_pull_path_alone_fences_when_the_service_moves_to_a_new_primary() {
    let old = postgres_at_epoch(1).await;
    let new = postgres_at_epoch(2).await;
    let switch = Switch::to(&old).await;

    let proxy = harness::start_proxy(&harness::config_for_address(
        &switch.address(),
        "",
        &format!(
            "{LEASE}\n[fence]\nverifyAtCheckout = true\nrequireEpoch = true\n\n\
                  [pool]\nmode = \"transaction\"\nbackendConnections = 4\n"
        ),
    ))
    .await;

    let client = connect(&proxy, "acme").await;
    client.simple_query("SELECT 1").await.expect("a query");
    assert_eq!(metric(&proxy, "pgelastic_proxy_primary_epoch"), Some(1));
    assert!(
        held_backends(&old).await >= 1,
        "the pool must be on the old primary"
    );

    // The Service moves. Nothing severs the established connections — that is
    // precisely the problem this fence exists to solve.
    switch.point_at(&new);

    await_epoch(&proxy, 2, SETTLE).await;
    let severed_in = await_backends(&old, 0, SETTLE).await;
    assert_eq!(
        held_backends(&old).await,
        0,
        "every socket to the demoted primary must be gone"
    );
    assert!(
        series(&proxy, "pgelastic_proxy_backends_severed_total") >= 1,
        "the fence must report what it severed"
    );

    let fresh = connect(&proxy, "acme").await;
    fresh
        .simple_query("SELECT 1")
        .await
        .expect("a checkout after the fence lands on the new primary");
    assert!(
        held_backends(&new).await >= 1,
        "the new checkout must be on the promoted primary"
    );
    assert_eq!(
        held_backends(&old).await,
        0,
        "no socket to the demoted primary may be reopened"
    );
    assert!(
        severed_in < SETTLE,
        "the sweep took {severed_in:?}, which is not within the settling window"
    );
    proxy.shutdown.send_replace(true);
}

/// A demoted primary that comes back is not a lower epoch to adopt. The proxy
/// records the regression, refuses to serve that backend, and does not move
/// its own epoch backwards.
#[tokio::test]
async fn a_backend_reporting_a_lower_epoch_is_refused_and_the_fence_does_not_move_back() {
    let old = postgres_at_epoch(1).await;
    let new = postgres_at_epoch(2).await;
    let switch = Switch::to(&old).await;

    let proxy = harness::start_proxy(&harness::config_for_address(
        &switch.address(),
        "",
        &format!(
            "{LEASE}\n[fence]\nrequireEpoch = true\n\n\
                  [pool]\nmode = \"transaction\"\nbackendConnections = 4\n"
        ),
    ))
    .await;

    let acme = connect(&proxy, "acme").await;
    acme.simple_query("SELECT 1").await.expect("a query");
    await_epoch(&proxy, 1, SETTLE).await;

    switch.point_at(&new);
    await_epoch(&proxy, 2, SETTLE).await;
    await_backends(&old, 0, SETTLE).await;

    // The old primary comes back. Every fresh dial now reaches a postmaster
    // still truthfully reporting epoch 1.
    switch.point_at(&old);
    let started = Instant::now();
    while regressions(&proxy) == 0 {
        assert!(
            started.elapsed() < SETTLE,
            "the pull path never noticed the backend had gone backwards"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        metric(&proxy, "pgelastic_proxy_primary_epoch"),
        Some(2),
        "the proxy's epoch must never go backwards"
    );

    // A tenant with no parked link of its own has to dial, and what it reaches
    // is the demoted primary. The refusal lands at connect time, because the
    // greeting a new pool key is served from is itself read off a backend.
    let error = try_connect(&proxy, "bravo")
        .await
        .expect_err("a backend on the superseded epoch must not be handed to a client");
    assert!(
        reported(&error).contains("PGE2506") || reported(&error).starts_with("25006"),
        "expected the superseded-epoch refusal, got {}",
        reported(&error)
    );
    assert_eq!(metric(&proxy, "pgelastic_proxy_primary_epoch"), Some(2));
    proxy.shutdown.send_replace(true);
}

/// Verify-path readings that came back lower than the highest ever seen.
fn regressions(proxy: &ProxyUnderTest) -> u64 {
    proxy
        .metrics
        .render()
        .lines()
        .find(|line| {
            line.starts_with("pgelastic_proxy_epoch_observations_total")
                && line.contains("source=\"verify\"")
                && line.contains("outcome=\"regressed\"")
        })
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

// ---- the push path -------------------------------------------------------

/// The ordering the design requires, stated as an assertion: by the time a
/// checkout after the push can succeed, nothing on the old epoch is still open.
#[tokio::test]
async fn every_old_epoch_socket_is_severed_before_any_new_checkout_succeeds() {
    let pg = postgres_at_epoch(1).await;

    let proxy = harness::start_proxy(&harness::config_for(
        &pg,
        &format!(
            "{LEASE}\n[fence]\npushAddress = \"127.0.0.1:0\"\n\n\
             [pool]\nmode = \"transaction\"\nbackendConnections = 4\n"
        ),
    ))
    .await;

    let clients = futures_util::future::join_all((0..4).map(|_| connect(&proxy, "acme"))).await;
    for client in &clients {
        client.simple_query("SELECT 1").await.expect("a query");
    }
    assert!(held_backends(&pg).await >= 1);

    let answer = push(proxy.push_port(), 9).await;
    assert!(answer.contains("\"outcome\":\"advanced\""), "{answer}");

    let elapsed = await_backends(&pg, 0, SETTLE).await;
    assert_eq!(
        held_backends(&pg).await,
        0,
        "the fence must sever every socket opened under the superseded epoch"
    );

    // The backend is still reporting epoch 1, so it is a demoted primary as far
    // as this proxy is concerned and no further checkout may reach it.
    let after = connect(&proxy, "acme").await;
    let error = after
        .simple_query("SELECT 1")
        .await
        .expect_err("a checkout after the fence must not reach the superseded backend");
    assert!(
        reported(&error).contains("PGE2506"),
        "expected the superseded-epoch refusal, got {}",
        reported(&error)
    );
    assert_eq!(held_backends(&pg).await, 0);
    assert!(elapsed < SETTLE);
    proxy.shutdown.send_replace(true);
}

/// The endpoint's contract, on the wire: an epoch below the current one is
/// reported truthfully and changes nothing.
#[tokio::test]
async fn a_push_below_the_current_epoch_is_answered_without_lowering_it() {
    let pg = postgres_at_epoch(1).await;
    let proxy = harness::start_proxy(&harness::config_for(
        &pg,
        &format!("{LEASE}\n[fence]\npushAddress = \"127.0.0.1:0\"\n"),
    ))
    .await;

    assert!(
        push(proxy.push_port(), 7)
            .await
            .contains("\"outcome\":\"advanced\"")
    );
    assert!(
        push(proxy.push_port(), 7)
            .await
            .contains("\"outcome\":\"unchanged\"")
    );
    let regressed = push(proxy.push_port(), 3).await;
    assert!(
        regressed.contains("\"outcome\":\"regressed\""),
        "{regressed}"
    );
    assert!(regressed.contains("\"epoch\":7"), "{regressed}");
    assert_eq!(metric(&proxy, "pgelastic_proxy_primary_epoch"), Some(7));
    proxy.shutdown.send_replace(true);
}

// ---- the in-flight transaction policy, one test per row ------------------

/// Read-only on the old epoch: let it finish, then close.
#[tokio::test]
async fn a_read_only_transaction_on_the_old_epoch_is_allowed_to_finish() {
    let pg = postgres_at_epoch(1).await;
    let proxy = harness::start_proxy(&harness::config_for(
        &pg,
        &format!(
            "{LEASE}\n[fence]\npushAddress = \"127.0.0.1:0\"\n\n\
             [pool]\nmode = \"transaction\"\nbackendConnections = 4\n"
        ),
    ))
    .await;

    let client = connect(&proxy, "acme").await;
    client
        .simple_query("SELECT 1")
        .await
        .expect("a first query");

    let reader = tokio::spawn(async move {
        client
            .simple_query("SELECT pg_sleep(1), 42 AS answer")
            .await
            .map(|_| ())
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    push(proxy.push_port(), 5).await;

    let outcome = reader.await.expect("the reader task");
    assert!(
        outcome.is_ok(),
        "a read on the old epoch cannot cause split brain and must be allowed to finish: \
         {outcome:?}"
    );
    await_backends(&pg, 0, SETTLE).await;
    proxy.shutdown.send_replace(true);
}

/// Idle in transaction: RST immediately.
#[tokio::test]
async fn a_session_idle_in_transaction_is_reset_immediately() {
    let pg = postgres_at_epoch(1).await;
    let proxy = harness::start_proxy(&harness::config_for(
        &pg,
        &format!(
            "{LEASE}\n[fence]\npushAddress = \"127.0.0.1:0\"\n\n\
             [pool]\nmode = \"transaction\"\nbackendConnections = 4\n"
        ),
    ))
    .await;

    let client = connect(&proxy, "acme").await;
    client.simple_query("BEGIN").await.expect("begin");
    client.simple_query("SELECT 1").await.expect("a read");
    assert_eq!(held_backends(&pg).await, 1);

    push(proxy.push_port(), 5).await;
    let severed_in = await_backends(&pg, 0, SETTLE).await;
    assert!(
        severed_in < FENCE_DEADLINE * 4,
        "an idle-in-transaction backend was still open after {severed_in:?}"
    );

    let error = client
        .simple_query("SELECT 2")
        .await
        .expect_err("the session is over");
    assert!(
        reported(&error).contains("PGE2506") || reported(&error).contains("closed"),
        "{}",
        reported(&error)
    );
    proxy.shutdown.send_replace(true);
}

/// Write transaction, `Commit` not sent: RST immediately, and the write is not
/// there afterwards.
#[tokio::test]
async fn an_uncommitted_write_transaction_is_reset_and_leaves_nothing_behind() {
    let pg = postgres_at_epoch(1).await;
    let proxy = harness::start_proxy(&harness::config_for(
        &pg,
        &format!(
            "{LEASE}\n[fence]\npushAddress = \"127.0.0.1:0\"\n\n\
             [pool]\nmode = \"transaction\"\nbackendConnections = 4\n"
        ),
    ))
    .await;

    let setup = connect(&proxy, "acme").await;
    setup
        .simple_query("CREATE TABLE fence_writes (n int)")
        .await
        .expect("creating the table");

    let client = connect(&proxy, "acme").await;
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO fence_writes VALUES (1)")
        .await
        .expect("the insert");

    push(proxy.push_port(), 5).await;
    await_backends(&pg, 0, SETTLE).await;

    let error = client
        .simple_query("COMMIT")
        .await
        .expect_err("a commit on the superseded epoch must not be forwarded");
    assert!(
        reported(&error).contains("PGE2506") || reported(&error).contains("closed"),
        "{}",
        reported(&error)
    );

    // The write is gone, which is the correct answer: an aborted transaction is
    // an outcome, and this one was never acknowledged.
    let observer = connect_direct(&pg, "pgelastic_verifier").await;
    let rows: i64 = observer
        .query_one("SELECT count(*) FROM fence_writes", &[])
        .await
        .expect("counting")
        .get(0);
    assert_eq!(rows, 0, "an aborted transaction must leave nothing behind");
    proxy.shutdown.send_replace(true);
}

/// `Commit` forwarded, `CommandComplete` not received: genuinely undecidable.
///
/// The commit is made to hang the way the design says it will in production —
/// `dataDurability: Required` with quorum lost, which is a `COMMIT` waiting on
/// a synchronous standby that is not there.
#[tokio::test]
async fn a_commit_whose_outcome_was_never_observed_is_reported_unknown_and_logged() {
    let pg = postgres_at_epoch(1).await;
    let dir = tempfile::TempDir::new().expect("a temp dir");
    let log_path = dir.path().join("in-doubt.jsonl");

    let proxy = harness::start_proxy(&harness::config_for(
        &pg,
        &format!(
            "{LEASE}\n[fence]\npushAddress = \"127.0.0.1:0\"\n\
             inDoubtLog = \"{log}\"\n\n\
             [pool]\nmode = \"transaction\"\nbackendConnections = 4\n",
            log = log_path.display()
        ),
    ))
    .await;

    let setup = connect(&proxy, "acme").await;
    setup
        .simple_query("CREATE TABLE fence_commits (n int)")
        .await
        .expect("creating the table");

    // Quorum loss, exactly as the design describes it: commits stall rather
    // than silently degrading to asynchronous.
    let admin_client = connect_direct(&pg, "pgelastic_quorum").await;
    admin_client
        .simple_query("ALTER SYSTEM SET synchronous_standby_names = 'ghost'")
        .await
        .expect("naming an absent standby");
    admin_client
        .simple_query("SELECT pg_reload_conf()")
        .await
        .expect("reloading");

    let client = connect(&proxy, "acme").await;

    await_quorum_clause(&client, "ghost").await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO fence_commits VALUES (1)")
        .await
        .expect("the insert");

    let committer = tokio::spawn(async move { client.simple_query("COMMIT").await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    push(proxy.push_port(), 5).await;

    let error = committer
        .await
        .expect("the committer task")
        .expect_err("an unobserved commit is not a success");
    let text = reported(&error);
    assert!(
        text.contains("PGE4003"),
        "the client must be given the distinguished UNKNOWN code, got {text}"
    );
    assert!(
        text.contains("UNKNOWN"),
        "the message must say the outcome is unknown, got {text}"
    );
    assert!(
        !text.to_ascii_lowercase().contains("rolled back")
            && !text.to_ascii_lowercase().contains("committed successfully"),
        "the proxy must not claim an outcome it did not observe, got {text}"
    );
    assert_eq!(
        metric(&proxy, "pgelastic_proxy_in_doubt_transactions"),
        Some(1)
    );

    // Durable: a new process reading the same file finds the record.
    proxy.shutdown.send_replace(true);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let reopened =
        pgelastic_proxy::epoch::InDoubtLog::open(&log_path).expect("the in-doubt log reopens");
    assert_eq!(
        reopened.len(),
        1,
        "the record must survive the proxy that wrote it"
    );
    let record = &reopened.records()[0];
    assert_eq!(record.key.tenant, "acme");
    assert_eq!(
        record.key.epoch, 1,
        "keyed by the epoch the commit went out on"
    );
    assert!(
        record.key.backend_pid.is_some(),
        "keyed by the backend pid: {:?}",
        record.key
    );
    assert!(
        record.key.lsn.is_some(),
        "keyed by an lsn: {:?}",
        record.key
    );
    assert!(record.statement.to_ascii_uppercase().contains("COMMIT"));
}

/// Session mode never goes through the pool, so the sweep that severs parked
/// links cannot reach it. Without its own fence it would be the one place in
/// the proxy where a demoted primary keeps a client — and replication
/// connections are forced into session mode.
#[tokio::test]
async fn a_bound_session_is_fenced_even_though_it_never_touches_the_pool() {
    let pg = postgres_at_epoch(1).await;
    let proxy = harness::start_proxy(&harness::config_for(
        &pg,
        &format!(
            "{LEASE}\n[fence]\npushAddress = \"127.0.0.1:0\"\nrequireEpoch = true\n\n\
             [pool]\nmode = \"session\"\n"
        ),
    ))
    .await;

    let client = connect(&proxy, "acme").await;
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("SELECT 1")
        .await
        .expect("a read inside the transaction");
    assert_eq!(held_backends(&pg).await, 1);

    push(proxy.push_port(), 5).await;
    let severed_in = await_backends(&pg, 0, SETTLE).await;
    assert!(
        severed_in < FENCE_DEADLINE * 4,
        "a bound session's backend was still open after {severed_in:?}"
    );

    let error = client
        .simple_query("SELECT 2")
        .await
        .expect_err("a bound session on the superseded epoch is over");
    assert!(
        reported(&error).contains("PGE2506") || reported(&error).contains("closed"),
        "{}",
        reported(&error)
    );

    // A fresh session-mode client cannot be handed the superseded backend
    // either, and the refusal names the epoch rather than a generic failure.
    let refused = try_connect(&proxy, "acme")
        .await
        .expect_err("a session-mode connect must be refused on a superseded backend");
    assert!(
        reported(&refused).contains("PGE2506"),
        "{}",
        reported(&refused)
    );
    proxy.shutdown.send_replace(true);
}

// ---- the reaction deadline ----------------------------------------------

/// The fence has to complete inside one `retryPeriod`, and it has to do it with
/// the pool busy rather than only when it is idle.
#[tokio::test]
async fn the_fence_completes_within_the_reaction_deadline_under_load() {
    let pg = postgres_at_epoch(1).await;
    let proxy = harness::start_proxy(&harness::config_for(
        &pg,
        &format!(
            "{LEASE}\n[fence]\npushAddress = \"127.0.0.1:0\"\n\n\
             [pool]\nmode = \"transaction\"\nbackendConnections = 16\n"
        ),
    ))
    .await;

    let clients = futures_util::future::join_all((0..32).map(|_| connect(&proxy, "acme"))).await;
    futures_util::future::join_all(clients.iter().map(|client| async move {
        let _ = client.simple_query("SELECT 1").await;
    }))
    .await;
    let before = held_backends(&pg).await;
    assert!(
        before >= 4,
        "the pool should be holding several backends, saw {before}"
    );

    let started = Instant::now();
    push(proxy.push_port(), 5).await;
    await_backends(&pg, 0, SETTLE).await;
    let wall = started.elapsed();

    let reported = metric(&proxy, "pgelastic_proxy_epoch_fence_latency_us")
        .expect("the fence must report how long it took");
    let deadline_us = i64::try_from(FENCE_DEADLINE.as_micros()).expect("the deadline fits");
    assert!(
        reported > 0 && reported < deadline_us,
        "the fence reported {reported}us against a {deadline_us}us deadline"
    );
    println!(
        "fence latency: {reported}us reported for {before} sockets, {wall:?} wall including \
         the observer's own round trips (deadline {}us)",
        FENCE_DEADLINE.as_micros()
    );
    proxy.shutdown.send_replace(true);
}

/// The relationship the design insists on: shortening the lease shortens the
/// fence deadline with it, and a configuration where it did not would be
/// refused before the proxy ever binds a socket.
#[tokio::test]
async fn a_lease_the_fence_cannot_beat_is_refused_at_start_up() {
    let pg = postgres_at_epoch(1).await;
    let source = harness::config_for(
        &pg,
        "[fence.lease]\nleaseDurationMs = 1000\nrenewDeadlineMs = 900\nretryPeriodMs = 800\n",
    );
    let error = source
        .parse::<pgelastic_proxy::config::Config>()
        .expect_err("a lease with too few renewal attempts must not start");
    assert!(error.to_string().contains("fence.lease"), "{error}");
}

// ---- helpers -------------------------------------------------------------

async fn connect(proxy: &ProxyUnderTest, tenant: &str) -> tokio_postgres::Client {
    let url = format!(
        "host=127.0.0.1 port={} user={tenant} dbname={BACKEND_DATABASE}",
        proxy.port()
    );
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connecting through the proxy");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Connects without panicking, so a refusal can be asserted on.
async fn try_connect(
    proxy: &ProxyUnderTest,
    tenant: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let url = format!(
        "host=127.0.0.1 port={} user={tenant} dbname={BACKEND_DATABASE}",
        proxy.port()
    );
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn connect_direct(pg: &Postgres, application_name: &str) -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect(&pg.direct_url(application_name), tokio_postgres::NoTls)
            .await
            .expect("connecting past the proxy");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The server-side text of a driver error.
///
/// `tokio_postgres::Error` renders as the bare string "db error", so asserting
/// on a SQLSTATE or a `PGE` token means reaching through to the `ErrorResponse`
/// the proxy actually sent.
fn reported(error: &tokio_postgres::Error) -> String {
    error.as_db_error().map_or_else(
        || error.to_string(),
        |db| format!("{} {}", db.code().code(), db.message()),
    )
}

/// Waits until the backend serving `client` reports `expected` as its loaded quorum clause.
///
/// `synchronous_standby_names` is `PGC_SIGHUP`, so `pg_reload_conf` returns long before a backend
/// has picked the new value up. A test that assumes otherwise passes on a fast machine and
/// fails on a loaded one, because the commit it wanted to stall simply completes first.
async fn await_quorum_clause(client: &tokio_postgres::Client, expected: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let rows = client
            .simple_query("SHOW synchronous_standby_names")
            .await
            .expect("reading the loaded quorum clause");
        let loaded = rows.iter().any(|message| {
            matches!(message, tokio_postgres::SimpleQueryMessage::Row(row)
                if row.get(0) == Some(expected))
        });
        if loaded {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the backend never loaded {expected}, so a commit could not stall"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
