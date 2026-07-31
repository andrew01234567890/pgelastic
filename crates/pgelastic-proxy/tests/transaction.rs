//! Transaction pooling, end to end, against a real `postgres:18`.
//!
//! The properties asserted here are the ones a unit test cannot reach, because
//! the thing that must not happen — one tenant seeing another's session state —
//! is a fact about a real backend rather than about the proxy's bookkeeping.
//! Every count of "how many backend connections is the proxy holding" is
//! therefore taken from `pg_stat_activity` over a connection that bypasses the
//! proxy entirely.

mod harness;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use harness::{BACKEND_DATABASE, RawClient, Stack, stack_with};
use pgelastic_wire::BackendMessage;

/// A pool of four backends, whatever a test throws at it.
const FOUR: &str = "\
[pool]
mode = \"transaction\"
backendConnections = 4
";

/// One backend, so a handoff is not merely likely but forced: any two clients
/// that both do work must have used the same socket.
const ONE: &str = "\
[pool]
mode = \"transaction\"
backendConnections = 1
";

async fn pid(client: &tokio_postgres::Client) -> i32 {
    client
        .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
        .await
        .expect("reading the backend pid")
        .get(0)
}

/// Backend connections the proxy is holding, counted by the server itself.
///
/// The observer excludes itself by `application_name`: it connects directly, so
/// it is a `client backend` on the same database as the ones it is counting.
async fn held_backends(observer: &tokio_postgres::Client) -> i64 {
    observer
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_stat_activity \
             WHERE datname = $1 AND backend_type = 'client backend' \
             AND application_name = ''",
            &[&BACKEND_DATABASE],
        )
        .await
        .expect("counting backends")
        .get(0)
}

/// Samples the server's backend count until told to stop, returning the peak.
struct Sampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicI64>,
    task: tokio::task::JoinHandle<()>,
}

impl Sampler {
    async fn start(stack: &Stack) -> Self {
        let observer = stack.observer("pgelastic_probe").await;
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicI64::new(0));
        let task = tokio::spawn({
            let (stop, peak) = (Arc::clone(&stop), Arc::clone(&peak));
            async move {
                while !stop.load(Ordering::Relaxed) {
                    let held = held_backends(&observer).await;
                    peak.fetch_max(held, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(15)).await;
                }
            }
        });
        Self { stop, peak, task }
    }

    async fn finish(self) -> i64 {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.task.await;
        self.peak.load(Ordering::Relaxed)
    }
}

fn field(message: &BackendMessage, kind: u8) -> Option<String> {
    let (BackendMessage::ErrorResponse(fields) | BackendMessage::NoticeResponse(fields)) = message
    else {
        return None;
    };
    fields
        .get(kind)
        .map(|value| String::from_utf8_lossy(value).into_owned())
}

fn sqlstate(message: &BackendMessage) -> Option<String> {
    field(message, pgelastic_wire::types::field::CODE)
}

fn text(message: &BackendMessage) -> String {
    field(message, pgelastic_wire::types::field::MESSAGE).unwrap_or_default()
}

// ---- multiplexing ------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn twenty_clients_share_four_backends_and_the_server_never_sees_a_fifth() {
    let stack = stack_with(FOUR).await;
    let sampler = Sampler::start(&stack).await;

    let mut tasks = Vec::new();
    for id in 0..20i32 {
        let url = stack.url();
        tasks.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .expect("connecting through the proxy");
            tokio::spawn(async move {
                let _ = connection.await;
            });

            let mut seen = BTreeSet::new();
            for round in 0..10i32 {
                let row = client
                    .query_one(
                        "SELECT $1::int * 100 + $2::int, pg_catalog.pg_backend_pid()",
                        &[&id, &round],
                    )
                    .await
                    .expect("every multiplexed query must complete");
                assert_eq!(row.get::<_, i32>(0), id * 100 + round);
                seen.insert(row.get::<_, i32>(1));
            }
            seen
        }));
    }

    let mut backends = BTreeSet::new();
    for task in tasks {
        backends.extend(task.await.expect("no client task may panic"));
    }
    let peak = sampler.finish().await;

    assert!(
        backends.len() <= 4,
        "twenty clients touched {} distinct backends, which is over the cap of four: {backends:?}",
        backends.len()
    );
    assert!(
        backends.len() >= 2,
        "every client landed on one backend, so nothing was actually multiplexed"
    );
    assert!(
        peak <= 4,
        "the server saw {peak} backend connections at once, over the cap of four"
    );

    let rendered = stack.proxy.metrics.render();
    assert!(
        rendered.contains("pgelastic_proxy_backend_budget 4"),
        "the exposition must publish the budget: {rendered}"
    );
    assert!(
        !rendered.contains("pgelastic_proxy_checkouts_total{source=\"reused\"} 0"),
        "no link was ever reused, so the pool did not pool: {rendered}"
    );
}

/// The other half of the connect gate: serialising attempts must not stall the
/// pool. Every client that waited behind an attempt has to get its own link the
/// moment that attempt succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn a_successful_connect_releases_the_gate_and_the_clients_behind_it_proceed() {
    let stack = stack_with(
        "\
[pool]
mode = \"transaction\"
backendConnections = 8
",
    )
    .await;

    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(stack.connect().await);
    }

    let mut tasks = Vec::new();
    for client in clients {
        tasks.push(tokio::spawn(async move {
            let row = client
                .query_one(
                    "SELECT pg_catalog.pg_sleep(1), pg_catalog.pg_backend_pid()",
                    &[],
                )
                .await
                .expect("every overlapping query must get a backend of its own");
            row.get::<_, i32>(1)
        }));
    }

    let mut backends = BTreeSet::new();
    for task in tasks {
        backends.insert(task.await.expect("no client task may panic"));
    }

    assert_eq!(
        backends.len(),
        8,
        "eight overlapping queries shared backends, so somebody never got past the gate"
    );

    // One attempt for the link that cached the greeting, seven for the clients
    // that had to open one of their own; the eighth reused the parked link.
    let rendered = stack.proxy.metrics.render();
    assert!(
        rendered.contains("outcome=\"attempted\"} 8"),
        "expected eight gated attempts in {rendered}"
    );
    assert!(
        rendered.contains("outcome=\"fast_failed\"} 0"),
        "no attempt failed, so nothing may have fast-failed: {rendered}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_keeps_its_backend_from_begin_to_commit() {
    let stack = stack_with(FOUR).await;
    let mut client = stack.connect().await;

    let transaction = client.transaction().await.unwrap();
    let first: i32 = transaction
        .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    transaction
        .execute("SELECT pg_catalog.pg_sleep(0.05)", &[])
        .await
        .unwrap();
    let second: i32 = transaction
        .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        first, second,
        "a backend was handed away in the middle of a transaction"
    );
    transaction.commit().await.unwrap();

    assert_eq!(
        client
            .query_one("SELECT 1::int AS after_commit", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
}

// ---- session-state isolation -------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn scrubbable_session_state_never_survives_a_handoff() {
    let stack = stack_with(ONE).await;
    let observer = stack.observer("pgelastic_probe").await;
    observer
        .batch_execute(
            "CREATE ROLE snoop NOLOGIN; \
             GRANT snoop TO pgelastic; \
             CREATE SCHEMA audit;",
        )
        .await
        .unwrap();

    let alice = stack.connect().await;
    let poisoned = pid(&alice).await;
    let untouched = alice
        .query_one(
            "SELECT current_setting('search_path'), current_setting('TimeZone')",
            &[],
        )
        .await
        .unwrap();
    let (default_path, default_zone) =
        (untouched.get::<_, String>(0), untouched.get::<_, String>(1));

    alice
        .batch_execute(
            "SET search_path TO audit; \
             SET ROLE snoop; \
             SET TimeZone TO 'Pacific/Auckland'; \
             PREPARE alice_stmt AS SELECT 1;",
        )
        .await
        .unwrap();

    // A single backend, so Bob is provably handed the very socket Alice dirtied.
    let bob = stack.connect().await;
    assert_eq!(
        pid(&bob).await,
        poisoned,
        "the pool did not reuse the one backend it has, so this proves nothing"
    );

    let row = bob
        .query_one(
            "SELECT current_user::text, \
                    current_setting('search_path'), \
                    current_setting('TimeZone'), \
                    (SELECT count(*)::int FROM pg_catalog.pg_prepared_statements \
                     WHERE name = 'alice_stmt'), \
                    pg_catalog.to_regclass('pg_temp.alice_temp') IS NULL, \
                    (SELECT count(*)::int FROM pg_catalog.pg_listening_channels())",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(
        row.get::<_, String>(0),
        "pgelastic",
        "SET ROLE leaked across the handoff — RESET ALL would not have caught it either"
    );
    assert_eq!(
        row.get::<_, String>(1),
        default_path,
        "a search_path leaked across the handoff"
    );
    assert_eq!(
        row.get::<_, String>(2),
        default_zone,
        "a reported parameter leaked across the handoff"
    );
    // Named specifically: the pool has by now prepared Bob's *own* statement on
    // this link, and counting every entry would make that look like a leak.
    assert_eq!(
        row.get::<_, i32>(3),
        0,
        "Alice's prepared statement leaked across the handoff"
    );
    assert!(row.get::<_, bool>(4), "a temp table leaked");
    assert_eq!(row.get::<_, i32>(5), 0, "a LISTEN registration leaked");
}

#[tokio::test(flavor = "multi_thread")]
async fn unscrubbable_state_pins_its_client_and_is_invisible_to_everybody_else() {
    let stack = stack_with(FOUR).await;

    let alice = stack.connect().await;
    let alice_pid = pid(&alice).await;
    alice
        .batch_execute(
            "CREATE TEMP TABLE alice_only (n int); \
             INSERT INTO alice_only VALUES (1); \
             LISTEN alice_channel; \
             SELECT pg_catalog.pg_advisory_lock(4242);",
        )
        .await
        .unwrap();

    // Pinned: every later statement is the same socket, because the state Alice
    // created cannot be scrubbed while she is still entitled to it.
    assert_eq!(pid(&alice).await, alice_pid);
    assert_eq!(
        alice
            .query_one("SELECT count(*)::int FROM alice_only", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );

    let bob = stack.connect().await;
    assert_ne!(
        pid(&bob).await,
        alice_pid,
        "a pinned backend was handed to another client"
    );
    let row = bob
        .query_one(
            "SELECT pg_catalog.to_regclass('pg_temp.alice_only') IS NULL, \
                    (SELECT count(*)::int FROM pg_catalog.pg_listening_channels()), \
                    (SELECT count(*)::int FROM pg_catalog.pg_locks \
                     WHERE locktype = 'advisory' AND pid = pg_catalog.pg_backend_pid())",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.get::<_, bool>(0),
        "a temp table crossed to another client"
    );
    assert_eq!(
        row.get::<_, i32>(1),
        0,
        "a LISTEN crossed to another client"
    );
    assert_eq!(
        row.get::<_, i32>(2),
        0,
        "a session advisory lock crossed to another client"
    );

    drop(alice);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Alice is gone, so the state she was entitled to is scrubbed and the link
    // rejoins the elastic pool. Whoever gets it next must still see nothing.
    let carol = stack.connect().await;
    let row = carol
        .query_one(
            "SELECT pg_catalog.to_regclass('pg_temp.alice_only') IS NULL, \
                    (SELECT count(*)::int FROM pg_catalog.pg_listening_channels())",
            &[],
        )
        .await
        .unwrap();
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, i32>(1), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn two_tenants_never_share_a_backend() {
    let stack = stack_with(FOUR).await;

    let mut acme = BTreeSet::new();
    let mut globex = BTreeSet::new();
    for _ in 0..6 {
        let a = stack.connect_as("acme").await;
        let g = stack.connect_as("globex").await;
        acme.insert(pid(&a).await);
        globex.insert(pid(&g).await);
    }

    assert!(
        acme.is_disjoint(&globex),
        "a backend was shared between tenants: {acme:?} and {globex:?}"
    );
}

// ---- guarantees under contention ---------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_guaranteed_tenant_keeps_its_connections_while_another_floods_the_pool() {
    let stack = stack_with(
        "\
[pool]
mode = \"transaction\"
backendConnections = 4
headroomPercent = 0
queryWaitSeconds = 60

[[pool.tenants]]
name = \"vip\"
guaranteed = 2
burstable = 4

[[pool.tenants]]
name = \"flood\"
guaranteed = 0
burstable = 4
",
    )
    .await;

    let stop = Arc::new(AtomicBool::new(false));
    let mut flooders = Vec::new();
    for _ in 0..8 {
        let url = stack.url_for("flood");
        let stop = Arc::clone(&stop);
        flooders.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .expect("a flooding client must connect");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            while !stop.load(Ordering::Relaxed) {
                let _ = client.simple_query("SELECT pg_catalog.pg_sleep(0.2)").await;
            }
        }));
    }

    // Let the flood take every slot it can before the guaranteed tenant asks.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let vip = stack.connect_as("vip").await;
    let started = Instant::now();
    for round in 0..5i32 {
        let row = vip
            .query_one("SELECT $1::int", &[&round])
            .await
            .expect("a guaranteed tenant must not be refused while another floods the pool");
        assert_eq!(row.get::<_, i32>(0), round);
    }
    let elapsed = started.elapsed();

    stop.store(true, Ordering::Relaxed);
    for flooder in flooders {
        let _ = flooder.await;
    }

    assert!(
        elapsed < Duration::from_secs(5),
        "the guaranteed tenant waited {elapsed:?} for five trivial statements, \
         so its reservation bought it nothing"
    );
}

// ---- the denial paths --------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_at_its_own_ceiling_is_refused_with_pge1928_and_53300() {
    let stack = stack_with(
        "\
[pool]
mode = \"transaction\"
backendConnections = 4

[[pool.tenants]]
name = \"capped\"
guaranteed = 0
burstable = 1
",
    )
    .await;

    let mut holder = stack.connect_as("capped").await;
    let held = holder.transaction().await.unwrap();
    held.execute("SELECT 1", &[]).await.unwrap();

    let mut blocked = RawClient::connect(stack.localhost(), "capped", BACKEND_DATABASE).await;
    let messages = blocked.query_until_closed("SELECT 1").await;

    let error = messages
        .iter()
        .find(|message| matches!(message, BackendMessage::ErrorResponse(_)))
        .expect("the second client must be refused");
    assert_eq!(sqlstate(error).as_deref(), Some("53300"));
    assert!(
        text(error).starts_with("PGE1928"),
        "expected the tenant-ceiling code, got {:?}",
        text(error)
    );

    assert!(
        stack
            .proxy
            .metrics
            .render()
            .contains("code=\"PGE1928\",sqlstate=\"53300\"} 1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pool_at_its_client_limit_is_refused_with_pge1936_and_53400() {
    let stack = stack_with(
        "\
[pool]
mode = \"transaction\"
backendConnections = 4
maxClientConnections = 1
",
    )
    .await;

    let _first = stack.connect().await;
    let refused = stack
        .try_connect_as("tenant")
        .await
        .expect_err("the second client must be refused");

    let db = refused
        .as_db_error()
        .expect("the refusal must be a real ErrorResponse, not a transport failure");
    assert_eq!(db.code().code(), "53400");
    assert!(
        db.message().starts_with("PGE1936"),
        "expected the pool-full code, got {:?}",
        db.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_waits_too_long_is_noticed_then_refused_with_pge1024() {
    let stack = stack_with(
        "\
[pool]
mode = \"transaction\"
backendConnections = 1
notifyAfterSeconds = 1
queryWaitSeconds = 3
",
    )
    .await;

    // The pool's one backend is held open by a transaction that never commits.
    let mut holder = stack.connect().await;
    let held = holder.transaction().await.unwrap();
    held.execute("SELECT 1", &[]).await.unwrap();

    let mut blocked = RawClient::connect(stack.localhost(), "tenant", BACKEND_DATABASE).await;
    let started = Instant::now();
    let messages = blocked.query_until_closed("SELECT 1").await;
    let waited = started.elapsed();

    let notice = messages
        .iter()
        .find(|message| matches!(message, BackendMessage::NoticeResponse(_)))
        .expect("a queued client must be told why it is waiting before it is refused");
    assert_eq!(sqlstate(notice).as_deref(), Some("53400"));
    assert!(
        text(notice).starts_with("PGE1936"),
        "the notice must name the limit that blocked the grant, got {:?}",
        text(notice)
    );

    let error = messages
        .iter()
        .find(|message| matches!(message, BackendMessage::ErrorResponse(_)))
        .expect("the queued client must eventually be refused");
    assert_eq!(sqlstate(error).as_deref(), Some("53400"));
    assert!(
        text(error).starts_with("PGE1024"),
        "expected the admission-timeout code, got {:?}",
        text(error)
    );

    assert!(
        waited >= Duration::from_secs(3),
        "the client was refused after {waited:?}, before its wait had run out"
    );
    assert!(
        stack
            .proxy
            .metrics
            .render()
            .contains("code=\"PGE1024\",sqlstate=\"53400\"} 1")
    );
}

// ---- the pinned budget --------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_connection_leaves_the_elastic_budget_and_is_reported_separately() {
    let stack = stack_with(FOUR).await;

    let before = stack.proxy.metrics.render();
    assert!(before.contains("pgelastic_proxy_backend_elastic_limit 4"));

    let alice = stack.connect().await;
    alice.batch_execute("LISTEN alice_channel").await.unwrap();
    // The pin is recorded on the release attempt that follows the statement.
    alice.query_one("SELECT 1", &[]).await.unwrap();

    let after = stack.proxy.metrics.render();
    assert!(
        after.contains("pgelastic_proxy_pins_total{reason=\"listen\"} 1"),
        "the pin must be attributable to the tripwire that fired: {after}"
    );
    assert!(
        after.contains("pgelastic_proxy_backend_elastic_limit 3"),
        "a pinned connection must lower the ceiling the reusable pool can reach: {after}"
    );
    assert!(
        after.contains("pgelastic_proxy_backend_elastic_connections 0"),
        "a pinned connection must not still be counted as elastic: {after}"
    );

    // Three elastic connections are still available, and no more.
    let mut others = Vec::new();
    for _ in 0..3 {
        let client = stack.connect().await;
        client.query_one("SELECT 1", &[]).await.unwrap();
        others.push(client);
    }
    let observer = stack.observer("pgelastic_probe").await;
    assert!(
        held_backends(&observer).await <= 4,
        "the pinned connection was not counted against the total"
    );

    drop(alice);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let released = stack.proxy.metrics.render();
    assert!(
        released.contains("pgelastic_proxy_backend_elastic_limit 4"),
        "the ceiling must come back once the pinning client has gone: {released}"
    );
}

// ---- prepared statements across handoffs --------------------------------

/// One backend, and a policy that only scrubs a link it has reason to think is dirty.
const ONE_DIRTY_TRACKED: &str = "\
[pool]
mode = \"transaction\"
backendConnections = 1
resetPolicy = \"dirtyTracked\"
";

/// The statement cache exists so that one server-side statement serves every client that asks
/// for the same text. It could not: the pool tainted the link with its own injected `Parse`,
/// the taint made `dirtyTracked` scrub at every release, and the scrub deallocated exactly
/// what had just been cached. `maxServerStatements` was dead configuration.
///
/// Counted on the backend rather than inferred from latency, because "it got faster" would not
/// say which of the two mechanisms moved. `pg_prepared_statements` is session-local, so the
/// count has to run *through* the proxy to land on the pooled link — and over the simple
/// protocol, or the counting query would prepare a statement of its own and be included in it.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepared_statement_survives_the_transaction_that_created_it() {
    let stack = stack_with(ONE_DIRTY_TRACKED).await;
    let client = stack.connect().await;
    let statement = client.prepare("SELECT $1::int * 2").await.unwrap();

    // Twenty separate transactions. Each is its own checkout, so before the fix each one
    // re-parsed the same text onto a link that had just been scrubbed of it.
    for value in 0..20i32 {
        let row = client.query_one(&statement, &[&value]).await.unwrap();
        let doubled: i32 = row.get(0);
        assert_eq!(doubled, value * 2);
    }

    let rows = client
        .simple_query(
            "SELECT count(*)::text FROM pg_catalog.pg_prepared_statements \
             WHERE name LIKE 'pgel!_%' ESCAPE '!'",
        )
        .await
        .expect("counting the pool's statements on the backend");
    let parsed = rows
        .iter()
        .find_map(|row| match row {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .expect("a count row");
    assert_eq!(
        parsed, "1",
        "twenty transactions over one backend must leave one prepared statement, not a \
         re-parse of the same text at every checkout"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn named_prepared_statements_keep_working_across_handoffs() {
    let stack = stack_with(FOUR).await;

    let mut tasks = Vec::new();
    for id in 0..6i32 {
        let url = stack.url();
        tasks.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .expect("connecting through the proxy");
            tokio::spawn(async move {
                let _ = connection.await;
            });

            // `prepare` is Parse/Describe/Sync; every later use is
            // Bind/Execute/Sync in a batch of its own, so the statement has to
            // survive a checkout it was never parsed on.
            let doubled = client.prepare("SELECT $1::int * 2").await.unwrap();
            let named = client
                .prepare("SELECT $1::text || '-' || $2::int::text")
                .await
                .unwrap();
            let where_am_i = client
                .prepare("SELECT pg_catalog.pg_backend_pid()")
                .await
                .unwrap();

            let mut seen = BTreeSet::new();
            for round in 0..12i32 {
                let value = id * 1000 + round;
                assert_eq!(
                    client
                        .query_one(&doubled, &[&value])
                        .await
                        .unwrap()
                        .get::<_, i32>(0),
                    value * 2
                );
                assert_eq!(
                    client
                        .query_one(&named, &[&"row", &value])
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    format!("row-{value}")
                );
                seen.insert(
                    client
                        .query_one(&where_am_i, &[])
                        .await
                        .unwrap()
                        .get::<_, i32>(0),
                );
            }
            seen
        }));
    }

    let mut backends = BTreeSet::new();
    for task in tasks {
        backends.extend(task.await.expect("no prepared-statement client may panic"));
    }
    assert!(
        backends.len() >= 2,
        "the prepared statements never crossed a backend, so nothing was proved"
    );
    assert!(backends.len() <= 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn deallocate_all_leaves_no_ghost_statement_behind() {
    let stack = stack_with(ONE).await;
    let client = stack.connect().await;

    let statement = client.prepare("SELECT $1::int + 7").await.unwrap();
    assert_eq!(
        client
            .query_one(&statement, &[&1i32])
            .await
            .unwrap()
            .get::<_, i32>(0),
        8
    );

    // The pool believes this statement is parsed on the link. Telling the
    // backend to forget it without telling the pool is what produces
    // `26000 invalid_sql_statement_name` for whoever binds it next.
    client.batch_execute("DEALLOCATE ALL").await.unwrap();

    assert_eq!(
        client
            .query_one(&statement, &[&2i32])
            .await
            .unwrap()
            .get::<_, i32>(0),
        9
    );
    client.batch_execute("DISCARD ALL").await.unwrap();
    assert_eq!(
        client
            .query_one(&statement, &[&3i32])
            .await
            .unwrap()
            .get::<_, i32>(0),
        10
    );
}

// ---- cancellation across the pool ---------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_reaches_whichever_backend_is_running_that_clients_query() {
    let stack = stack_with(FOUR).await;

    let idle = stack.connect().await;
    idle.query_one("SELECT 1", &[]).await.unwrap();
    let idle_token = idle.cancel_token();

    let busy = stack.connect().await;
    busy.query_one("SELECT 1", &[]).await.unwrap();
    let busy_token = busy.cancel_token();

    let running =
        tokio::spawn(async move { busy.simple_query("SELECT pg_catalog.pg_sleep(30)").await });
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The idle client holds no backend at all, so its cancel must find nothing
    // to cancel rather than landing on the backend somebody else is using.
    idle_token
        .cancel_query(tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !running.is_finished(),
        "an idle client's cancel key cancelled another client's query"
    );

    busy_token
        .cancel_query(tokio_postgres::NoTls)
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the cancelled query must not run to completion")
        .unwrap()
        .expect_err("a cancelled query must fail");
    assert_eq!(
        error.code().map(tokio_postgres::error::SqlState::code),
        Some("57014"),
        "expected query_canceled, got {error}"
    );

    // The pool is still usable afterwards.
    assert_eq!(
        idle.query_one("SELECT 9::int", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        9
    );
}

/// Step 0 of the ladder, from the client's side: at the instant a normal
/// checkout is refused at the tenant's burst ceiling, a cancel for that same
/// tenant still gets through.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_is_admitted_while_its_tenant_sits_at_its_burst_ceiling() {
    let stack = stack_with(
        "\
[pool]
mode = \"transaction\"
backendConnections = 4

[[pool.tenants]]
name = \"capped\"
guaranteed = 0
burstable = 1
",
    )
    .await;

    let busy = stack.connect_as("capped").await;
    busy.query_one("SELECT 1", &[]).await.unwrap();
    let token = busy.cancel_token();

    let running =
        tokio::spawn(async move { busy.simple_query("SELECT pg_catalog.pg_sleep(30)").await });
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Rung 2 is shut: the tenant's one burstable connection is held by the
    // query that is about to be cancelled.
    let mut blocked = RawClient::connect(stack.localhost(), "capped", BACKEND_DATABASE).await;
    let refusal = blocked.query_until_closed("SELECT 1").await;
    let denied = refusal
        .iter()
        .find(|message| matches!(message, BackendMessage::ErrorResponse(_)))
        .expect("a normal checkout must be refused at the ceiling");
    assert_eq!(sqlstate(denied).as_deref(), Some("53300"));
    assert!(text(denied).starts_with("PGE1928"));

    token.cancel_query(tokio_postgres::NoTls).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the cancel must not have been queued behind the query it cancels")
        .unwrap()
        .expect_err("a cancelled query must fail");
    assert_eq!(
        error.code().map(tokio_postgres::error::SqlState::code),
        Some("57014"),
        "expected query_canceled, got {error}"
    );

    let rendered = stack.proxy.metrics.render();
    assert!(
        rendered.contains("pgelastic_proxy_cancel_requests_total{outcome=\"matched\"} 1"),
        "the cancel must have drawn credit and been delivered: {rendered}"
    );
    assert!(
        rendered.contains("pgelastic_proxy_cancel_requests_total{outcome=\"refused\"} 0"),
        "no cancel may have been refused: {rendered}"
    );
}

// ---- the variable cache -------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_reported_parameter_a_client_set_follows_it_onto_every_later_backend() {
    let stack = stack_with(FOUR).await;

    let client = stack.connect().await;
    let default_zone: String = client
        .query_one("SELECT current_setting('TimeZone')", &[])
        .await
        .unwrap()
        .get(0);
    assert_ne!(default_zone, "Pacific/Auckland");
    client
        .batch_execute("SET TimeZone TO 'Pacific/Auckland'")
        .await
        .unwrap();

    // Enough rounds that the checkout lands on a backend that has never seen
    // this client before, with other clients competing for the same links.
    let mut noise = Vec::new();
    for _ in 0..4 {
        noise.push(stack.connect().await);
    }
    for other in &noise {
        other.query_one("SELECT 1", &[]).await.unwrap();
    }

    for _ in 0..12 {
        let row = client
            .query_one(
                "SELECT current_setting('TimeZone'), pg_catalog.pg_backend_pid()",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            row.get::<_, String>(0),
            "Pacific/Auckland",
            "the variable cache did not follow the client onto backend {}",
            row.get::<_, i32>(1)
        );
        for other in &noise {
            assert_eq!(
                other
                    .query_one("SELECT current_setting('TimeZone')", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                default_zone,
                "one client's parameter reached another"
            );
        }
    }
}

// ---- session mode is untouched ------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn session_mode_still_binds_one_client_to_one_backend() {
    let stack = stack_with("[pool]\nmode = \"session\"\n").await;
    let client = stack.connect().await;

    let first = pid(&client).await;
    client
        .batch_execute("SET TimeZone TO 'Pacific/Auckland'; CREATE TEMP TABLE mine (n int)")
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT pg_catalog.pg_backend_pid(), current_setting('TimeZone'), \
                    pg_catalog.to_regclass('pg_temp.mine') IS NOT NULL",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(row.get::<_, i32>(0), first);
    assert_eq!(row.get::<_, String>(1), "Pacific/Auckland");
    assert!(
        row.get::<_, bool>(2),
        "session mode must keep the session's own state"
    );
}
