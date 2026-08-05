//! Every case here drives real traffic through the proxy to a real
//! `postgres:18`. Nothing is stubbed and nothing is skipped.

mod harness;

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use harness::{BACKEND_DATABASE, RawClient, stack, stack_with};
use pgelastic_wire::{
    Bind, Describe, Execute, Format, FrontendMessage, Parse, Target, TransactionStatus,
};
use std::fmt::Write as _;

#[tokio::test(flavor = "multi_thread")]
async fn a_client_connects_and_runs_a_simple_query() {
    let stack = stack().await;
    let client = stack.connect().await;

    let row = client.query_one("SELECT 1 AS one", &[]).await.unwrap();
    assert_eq!(row.get::<_, i32>("one"), 1);

    let version: String = client
        .query_one("SHOW server_version", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        version.starts_with("18"),
        "these tests must run against PostgreSQL 18, got {version}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_extended_query_with_parameters_round_trips() {
    let stack = stack().await;
    let client = stack.connect().await;

    client
        .execute(
            "CREATE TABLE widget (id int primary key, name text, weight double precision)",
            &[],
        )
        .await
        .unwrap();

    let statement = client
        .prepare("INSERT INTO widget (id, name, weight) VALUES ($1, $2, $3)")
        .await
        .unwrap();
    for id in 0..100i32 {
        client
            .execute(
                &statement,
                &[&id, &format!("widget-{id}"), &(f64::from(id) / 4.0)],
            )
            .await
            .unwrap();
    }

    let row = client
        .query_one(
            "SELECT count(*)::int, max(id) FROM widget WHERE name LIKE $1",
            &[&"widget-%"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 100);
    assert_eq!(row.get::<_, i32>(1), 99);

    let named = client
        .query_one("SELECT name FROM widget WHERE id = $1", &[&42i32])
        .await
        .unwrap();
    assert_eq!(named.get::<_, String>(0), "widget-42");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multi_statement_transaction_walks_the_ready_for_query_status_from_i_to_t_to_i() {
    let stack = stack().await;
    let mut raw = RawClient::connect(stack.localhost(), "tenant", BACKEND_DATABASE).await;

    let (_, idle) = raw.simple_query("SELECT 1").await;
    assert_eq!(idle, TransactionStatus::Idle);

    let (_, begun) = raw.simple_query("BEGIN").await;
    assert_eq!(begun, TransactionStatus::Transaction);

    let (_, still_open) = raw
        .simple_query("CREATE TEMP TABLE t (n int); INSERT INTO t VALUES (1)")
        .await;
    assert_eq!(still_open, TransactionStatus::Transaction);

    let (_, committed) = raw.simple_query("COMMIT").await;
    assert_eq!(committed, TransactionStatus::Idle);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_statement_inside_a_transaction_reports_e_until_it_is_rolled_back() {
    let stack = stack().await;
    let mut raw = RawClient::connect(stack.localhost(), "tenant", BACKEND_DATABASE).await;

    raw.simple_query("BEGIN").await;
    let (_, failed) = raw.simple_query("SELECT * FROM no_such_table").await;
    assert_eq!(failed, TransactionStatus::Failed);

    let (_, rolled_back) = raw.simple_query("ROLLBACK").await;
    assert_eq!(rolled_back, TransactionStatus::Idle);
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_in_carries_a_payload_that_spans_many_frames() {
    // A small inline limit forces the relay's streaming path, so a COPY chunk
    // larger than it is never buffered whole.
    let stack = stack_with("[limits]\ninlineFrameBytes = 8192\n").await;
    let client = stack.connect().await;

    client
        .execute("CREATE TABLE bulk (id int, payload text)", &[])
        .await
        .unwrap();

    let sink = client
        .copy_in("COPY bulk FROM STDIN WITH (FORMAT text)")
        .await
        .unwrap();
    futures_util::pin_mut!(sink);

    let mut expected_bytes = 0usize;
    for chunk in 0..64 {
        let mut buffer = String::with_capacity(256 * 1024);
        let padding = "x".repeat(100);
        for row in 0..2_000 {
            let id = chunk * 2_000 + row;
            writeln!(buffer, "{id}\tpayload-{id}-{padding}").unwrap();
        }
        expected_bytes += buffer.len();
        sink.send(Bytes::from(buffer)).await.unwrap();
    }
    let copied = sink.finish().await.unwrap();

    assert_eq!(copied, 128_000);
    assert!(
        expected_bytes > 10 * 1024 * 1024,
        "the payload must be large enough to span many frames, was {expected_bytes}"
    );

    let row = client
        .query_one("SELECT count(*)::int, min(id), max(id) FROM bulk", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 128_000);
    assert_eq!(row.get::<_, i32>(1), 0);
    assert_eq!(row.get::<_, i32>(2), 127_999);
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_out_carries_a_payload_that_spans_many_frames() {
    let stack = stack_with("[limits]\ninlineFrameBytes = 8192\n").await;
    let client = stack.connect().await;

    let stream = client
        .copy_out(
            "COPY (SELECT g, repeat('y', 200) FROM generate_series(1, 100000) g) \
             TO STDOUT WITH (FORMAT text)",
        )
        .await
        .unwrap();
    futures_util::pin_mut!(stream);

    let mut total = 0usize;
    let mut lines = 0usize;
    while let Some(chunk) = stream.try_next().await.unwrap() {
        total += chunk.len();
        lines += chunk
            .iter()
            .fold(0usize, |seen, byte| seen + usize::from(*byte == b'\n'));
    }

    assert_eq!(lines, 100_000);
    assert!(total > 20_000_000, "copied only {total} bytes");

    // The session must still be usable: COPY OUT ends with CopyDone then
    // CommandComplete then ReadyForQuery, and losing track of any of them
    // desynchronises everything after it.
    assert_eq!(
        client
            .query_one("SELECT 7 AS after_copy", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        7
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fifty_thousand_row_result_set_crosses_every_buffer_boundary() {
    let stack = stack().await;
    let client = stack.connect().await;

    let rows = client
        .query(
            "SELECT g, md5(g::text) FROM generate_series(1, 50000) g ORDER BY g",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 50_000);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[49_999].get::<_, i32>(0), 50_000);

    let expected: String = client
        .query_one("SELECT md5('50000')", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows[49_999].get::<_, String>(1), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_single_row_larger_than_the_inline_limit_is_streamed_not_buffered() {
    let stack = stack_with("[limits]\ninlineFrameBytes = 16384\n").await;
    let client = stack.connect().await;

    let row = client
        .query_one("SELECT repeat('z', 4000000) AS big", &[])
        .await
        .unwrap();
    let big: String = row.get(0);
    assert_eq!(big.len(), 4_000_000);
    assert!(big.bytes().all(|b| b == b'z'));
}

#[tokio::test(flavor = "multi_thread")]
async fn several_statements_are_pipelined_before_the_first_sync() {
    let stack = stack().await;
    let mut raw = RawClient::connect(stack.localhost(), "tenant", BACKEND_DATABASE).await;

    let mut batch = Vec::new();
    for index in 0..5i32 {
        batch.push(FrontendMessage::Parse(Parse {
            name: Bytes::from(format!("stmt{index}")),
            query: Bytes::from(format!("SELECT {index} + $1")),
            param_types: vec![23],
        }));
        batch.push(FrontendMessage::Bind(Bind {
            portal: Bytes::new(),
            statement: Bytes::from(format!("stmt{index}")),
            param_formats: vec![Format::Text],
            params: vec![Some(Bytes::from_static(b"10"))],
            result_formats: vec![Format::Text],
        }));
        batch.push(FrontendMessage::Describe(Describe {
            target: Target::Portal,
            name: Bytes::new(),
        }));
        batch.push(FrontendMessage::Execute(Execute {
            portal: Bytes::new(),
            max_rows: 0,
        }));
    }
    // One Sync for the whole batch: everything above is in flight at once.
    batch.push(FrontendMessage::Sync);
    raw.send(&batch).await;

    let (messages, status) = raw.read_until_ready().await;
    assert_eq!(status, TransactionStatus::Idle);

    let count = |tags: &[u8], wanted: u8| {
        tags.iter()
            .fold(0usize, |seen, tag| seen + usize::from(*tag == wanted))
    };
    let tags: Vec<u8> = messages
        .iter()
        .map(pgelastic_wire::BackendMessage::tag)
        .collect();
    assert_eq!(count(&tags, b'1'), 5, "expected five ParseComplete");
    assert_eq!(count(&tags, b'2'), 5, "expected five BindComplete");
    assert_eq!(count(&tags, b'C'), 5, "expected five CommandComplete");

    let values: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            pgelastic_wire::BackendMessage::DataRow(row) => Some(first_column(row)),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec!["10", "11", "12", "13", "14"]);
}

/// `DataRow` is opaque by design, so the test decodes it the same way a client
/// would: Int16 column count, then Int32 length and bytes per column.
fn first_column(row: &pgelastic_wire::DataRow) -> String {
    let body = row.as_bytes();
    assert!(i16::from_be_bytes([body[0], body[1]]) >= 1);
    let len = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
    let len = usize::try_from(len).expect("the column is not null");
    String::from_utf8(body[6..6 + len].to_vec()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_notice_and_an_error_response_relay_with_their_fields_intact() {
    let stack = stack().await;
    let mut raw = RawClient::connect(stack.localhost(), "tenant", BACKEND_DATABASE).await;

    let (notices, status) = raw
        .simple_query(
            "DO $$ BEGIN RAISE NOTICE 'proxied notice %', 42 USING HINT = 'a hint survives'; END $$",
        )
        .await;
    assert_eq!(status, TransactionStatus::Idle);

    let notice = notices
        .iter()
        .find_map(|message| match message {
            pgelastic_wire::BackendMessage::NoticeResponse(fields) => Some(fields),
            _ => None,
        })
        .expect("the NOTICE must reach the client");
    assert_eq!(notice.severity().unwrap(), "NOTICE");
    assert_eq!(notice.message().unwrap(), "proxied notice 42");
    assert_eq!(
        notice.get(pgelastic_wire::types::field::HINT).unwrap(),
        "a hint survives"
    );

    let (errors, failed) = raw
        .simple_query("SELECT * FROM a_table_that_is_not_there")
        .await;
    assert_eq!(failed, TransactionStatus::Idle);

    let error = errors
        .iter()
        .find_map(|message| match message {
            pgelastic_wire::BackendMessage::ErrorResponse(fields) => Some(fields),
            _ => None,
        })
        .expect("the ErrorResponse must reach the client");
    assert_eq!(error.sqlstate().unwrap(), "42P01");
    assert_eq!(error.severity().unwrap(), "ERROR");
    assert!(
        error
            .message()
            .unwrap()
            .starts_with(b"relation \"a_table_that_is_not_there\" does not exist".as_slice())
    );
    assert!(
        error.get(pgelastic_wire::types::field::POSITION).is_some(),
        "the position field must survive the relay"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_statement_that_outruns_the_pool_deadline_is_cancelled_and_its_link_closed() {
    // A backend is the scarce thing in this design and it is handed round between
    // transactions. Before this, one client could hold one for as long as PostgreSQL would
    // run its statement, and `spec.timeouts.query` - documented as "the authoritative
    // deadline" - was read by nothing.
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nqueryDeadlineSeconds = 2\n").await;
    let client = stack.connect().await;

    let started = Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(20),
        client.simple_query("SELECT pg_sleep(30)"),
    )
    .await
    .expect("a statement past the deadline must not run to completion")
    .expect_err("a statement past the deadline must fail");

    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the deadline did not end the statement: {error}"
    );
    // Told why, rather than left to infer it from a closed socket. A bare close is what a
    // network fault looks like, and a driver that cannot tell the two apart will retry the
    // statement the deadline just stopped.
    assert_eq!(
        error.code().map(tokio_postgres::error::SqlState::code),
        Some("57014"),
        "the client was not told why its statement ended: {error}"
    );

    // The backend is not left running the statement on a link somebody else might get.
    let (observer, conn) = tokio_postgres::connect(
        &stack.pg.direct_url("deadline_observer"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("an observer connection straight to PostgreSQL");
    tokio::spawn(conn);
    // The cancel is delivered on its own socket and PostgreSQL acts on it when the backend
    // next checks for interrupts, so this is a race the observer has to wait out rather than
    // an instant the deadline can guarantee.
    let cleared = Instant::now();
    loop {
        let sleeping: i64 = observer
            .query_one(
                // Excluding this backend is load-bearing, not defensive: the
                // observer's own row carries this very statement as its `query`,
                // and the pattern it searches for is a substring of itself. Without
                // the exclusion the count never reaches zero and the assertion below
                // fires whatever the deadline did.
                "SELECT count(*) FROM pg_catalog.pg_stat_activity \
                 WHERE pid <> pg_backend_pid() AND state = 'active' \
                 AND query LIKE '%pg_sleep(30)%'",
                &[],
            )
            .await
            .expect("counting backends still running the statement")
            .get(0);
        if sleeping == 0 {
            break;
        }
        assert!(
            cleared.elapsed() < Duration::from_secs(10),
            "a backend is still running the statement the deadline cancelled"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pool_deadline_is_measured_per_statement_and_not_per_session() {
    // Four seconds of work in three statements against a three-second deadline. A
    // deadline armed once and never disarmed kills the second one, and a session
    // that survives all three is the only evidence the timer resets at each
    // ReadyForQuery rather than merely at the first.
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nqueryDeadlineSeconds = 3\n").await;
    let client = stack.connect().await;

    let started = Instant::now();
    for statement in 1..=3 {
        client
            .simple_query("SELECT pg_sleep(1.4)")
            .await
            .unwrap_or_else(|error| {
                panic!("statement {statement} is inside the deadline and must survive: {error}")
            });
    }
    assert!(
        started.elapsed() > Duration::from_secs(3),
        "the session must have outlived the deadline for this to prove anything"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_thinking_between_statements_is_not_charged_the_query_deadline() {
    // `spec.timeouts.query` bounds how long one statement may run, not how long a
    // transaction may stay open. A client holds its backend for the whole of an
    // explicit transaction, so a deadline armed on "a link is held" rather than on
    // "a statement is outstanding" ends a session that is running nothing - and
    // reports it as a query timeout. Bounding the idle transaction itself is a
    // separate limit with a separate name.
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nqueryDeadlineSeconds = 2\n").await;
    let client = stack.connect().await;

    client
        .simple_query("BEGIN")
        .await
        .expect("opening a transaction");
    tokio::time::sleep(Duration::from_secs(4)).await;
    client
        .simple_query("SELECT 1")
        .await
        .expect("a client that spent longer thinking than one statement may run must survive");
    client.simple_query("COMMIT").await.expect("committing");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_idling_inside_a_transaction_is_closed_and_its_backend_released() {
    // What such a client costs is not CPU. It is the backend it holds, the locks its
    // transaction took, and the xmin horizon that pins every dead tuple in the cluster
    // behind it - on a pool whose capacity unit is the backend.
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nclientIdleInTransactionSeconds = 2\n")
            .await;
    let client = stack.connect().await;

    client
        .simple_query("BEGIN")
        .await
        .expect("opening a transaction");
    // Idle for longer than the bound in one stretch. Polling the session instead would
    // reset the timer at every poll and measure nothing.
    tokio::time::sleep(Duration::from_secs(5)).await;
    client
        .simple_query("SELECT 1")
        .await
        .expect_err("a transaction left open past the bound must have been closed");

    // And the backend it was holding is gone rather than parked in the transaction.
    let (observer, conn) =
        tokio_postgres::connect(&stack.pg.direct_url("idle_observer"), tokio_postgres::NoTls)
            .await
            .expect("an observer connection straight to PostgreSQL");
    tokio::spawn(conn);
    let cleared = Instant::now();
    loop {
        // Excluding this backend is load-bearing: the observer's own row is active while it
        // runs this, and counting it would make the loop below never reach zero.
        let held: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity \
                 WHERE pid <> pg_backend_pid() AND state = 'idle in transaction'",
                &[],
            )
            .await
            .expect("counting backends left inside a transaction")
            .get(0);
        if held == 0 {
            break;
        }
        assert!(
            cleared.elapsed() < Duration::from_secs(10),
            "a backend is still parked inside the transaction the bound closed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// The bound must not fire on a client that is merely between transactions: outside one it
// holds no backend, and closing it would be closing an idle client for being idle.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_idling_outside_a_transaction_is_left_alone() {
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nclientIdleInTransactionSeconds = 2\n")
            .await;
    let client = stack.connect().await;

    client
        .simple_query("SELECT 1")
        .await
        .expect("a first statement");
    tokio::time::sleep(Duration::from_secs(5)).await;
    client
        .simple_query("SELECT 1")
        .await
        .expect("a client between transactions holds nothing and must survive");
}

// The case the predicate is written for. A pinned session holds its backend between
// transactions as well as inside one, so a bound armed on "a link is held" rather than on
// "the last ReadyForQuery said T" closes it for being idle - which is what every pinned
// client is, most of the time.
#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_client_idling_outside_a_transaction_is_left_alone() {
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nclientIdleInTransactionSeconds = 2\n")
            .await;
    let client = stack.connect().await;

    client
        .simple_query("LISTEN pinned_channel")
        .await
        .expect("LISTEN is session state no reset removes, so it pins the link");
    tokio::time::sleep(Duration::from_secs(5)).await;
    client
        .simple_query("SELECT 1")
        .await
        .expect("a pinned client holds its backend by design and must not be closed for it");
}

// The hole a review found in the first version of these bounds. A batch ended with Flush
// rather than Sync draws no ReadyForQuery, so the link's transaction status still reports
// whatever the last completed batch left - and the outstanding queue empties anyway when the
// rows arrive. PostgreSQL is inside an implicit transaction with a pinned backend_xmin; the
// statement deadline has disarmed and the idle bound would never arm, so N such clients
// permanently remove N backends from a fixed budget with no bound and no signal. PostgreSQL's
// own idle_in_transaction_session_timeout cannot catch it either: it reports the backend as
// active, not idle in transaction.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_ended_with_flush_instead_of_sync_is_still_bounded() {
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nclientIdleInTransactionSeconds = 2\n")
            .await;
    let mut raw = RawClient::connect(stack.localhost(), "tenant", BACKEND_DATABASE).await;

    raw.send(&[
        FrontendMessage::Parse(Parse {
            name: Bytes::new(),
            query: Bytes::from_static(b"SELECT 1"),
            param_types: vec![],
        }),
        FrontendMessage::Bind(Bind {
            portal: Bytes::new(),
            statement: Bytes::new(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![Format::Text],
        }),
        FrontendMessage::Execute(Execute {
            portal: Bytes::new(),
            max_rows: 0,
        }),
        // Flush, not Sync: the rows come back and the batch stays open.
        FrontendMessage::Flush,
    ])
    .await;

    // Drain the CommandComplete, then go silent the way a client that forgot its Sync does.
    let started = Instant::now();
    raw.read_until(|message| matches!(message, pgelastic_wire::BackendMessage::CommandComplete(_)))
        .await;
    assert!(
        raw.closed_within(Duration::from_secs(15)).await,
        "a batch left unsynced held its backend past the bound: {:?}",
        started.elapsed()
    );
}

// arm() claims a persisting state is measured from when it began rather than restarted by
// each pass of the relay loop, and its (true, true) no-op is what implements that. Every
// other bound test holds its state with a client that generates no traffic at all, so the
// loop never iterates during the window and the claim goes untested. Each row here is large
// enough to overflow PostgreSQL's output buffer on its own, which is what makes it a separate
// flush - a hundred small rows would be buffered and arrive as one wake-up, testing nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_deadline_is_measured_from_when_its_state_began_not_from_the_last_byte() {
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nqueryDeadlineSeconds = 3\n").await;
    let client = stack.connect().await;

    let started = Instant::now();
    client
        .simple_query("SELECT pg_sleep(0.2), repeat('x', 30000) FROM generate_series(1, 100)")
        .await
        .expect_err("twenty seconds of streaming rows must not outlive a three-second deadline");
    assert!(
        started.elapsed() < Duration::from_secs(12),
        "the deadline was restarted by the rows arriving under it: {:?}",
        started.elapsed()
    );
}

// A pinned link is out of the elastic pool for as long as its client lives, so without a
// ceiling one application that opens a LISTEN per connection reduces the reusable pool to
// nothing and every other client of the tenant sees PGE1024 against a budget that looks
// unspent. The refused client is closed rather than left on a shared link: the state that
// asked for the pin is state no reset removes.
#[tokio::test(flavor = "multi_thread")]
async fn a_pin_past_the_pool_ceiling_closes_its_client_instead_of_sharing_the_link() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\nbackendConnections = 4\nmaxPinnedPercent = 20\n",
    )
    .await;

    // A ceiling of 20% over four backends is one link, rounded up.
    let pinned = stack.connect().await;
    pinned
        .simple_query("LISTEN first_channel")
        .await
        .expect("the first pin is inside the ceiling");
    pinned
        .simple_query("SELECT 1")
        .await
        .expect("and the client that took it keeps its session");

    let refused = stack.connect().await;
    let error = refused
        .simple_query("LISTEN second_channel")
        .await
        .expect_err("a second pin is past the ceiling and must not be granted");
    assert_eq!(
        error.code().map(tokio_postgres::error::SqlState::code),
        Some("53300"),
        "the refused client was not told why its session ended: {error}"
    );

    // The client that holds the one pinned link is unharmed by the refusal.
    pinned
        .simple_query("SELECT 2")
        .await
        .expect("refusing somebody else's pin must not disturb the pinned client");
}

// The hole a review found in the first version of the pin ceiling, and the worst kind: a
// refusal was recorded but read in exactly one place, so a pin refused by the pump that runs
// BEFORE the relay loop - over whatever the client pipelined behind its startup packet - was
// dropped. The link was then not marked pinned, LISTEN leaves no taint, and the release gate
// parked it in the shared pool with the registration live. The next client on that pool key
// would have received this one's NOTIFY payloads.
#[tokio::test(flavor = "multi_thread")]
async fn a_pin_refused_before_the_relay_loop_still_closes_its_client() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\nbackendConnections = 4\nmaxPinnedPercent = 20\n",
    )
    .await;

    let holder = stack.connect().await;
    holder
        .simple_query("LISTEN taken_channel")
        .await
        .expect("the first pin takes the only slot the ceiling allows");

    // The Query travels in the same write as the startup packet, so it is pumped once before
    // the relay loop begins. A connect-then-send client cannot reach that path.
    let mut pipelined = RawClient::connect_pipelining(
        stack.localhost(),
        "tenant",
        BACKEND_DATABASE,
        "LISTEN leaked_channel",
    )
    .await;
    assert!(
        pipelined.closed_within(Duration::from_secs(15)).await,
        "a client whose pin was refused before the loop was left running on a link that \
         cannot be pinned"
    );

    // And the link it used is not in the pool carrying its registration. A fresh client on the
    // same pool key must see no notifications for the channel it never listened to.
    let observer = stack.connect().await;
    observer
        .simple_query("NOTIFY leaked_channel, 'payload'")
        .await
        .expect("notifying a channel nobody in this pool should be listening on");
    let listeners: i64 = observer
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_listening_channels() AS c(name)",
            &[],
        )
        .await
        .expect("counting this session's own listening channels")
        .get(0);
    assert_eq!(
        listeners, 0,
        "a link carrying the refused client's LISTEN was handed to the next client"
    );
}

// The count ceiling alone leaves a pool stuck at it for as long as its longest-lived client.
// This bounds how long any one link stays pinned, so the reusable pool recovers.
#[tokio::test(flavor = "multi_thread")]
async fn a_pin_held_longer_than_the_pool_allows_is_closed_and_the_budget_recovers() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\nbackendConnections = 4\nmaxPinnedPercent = 20\n\
         maxPinDurationSeconds = 2\n",
    )
    .await;

    let holder = stack.connect().await;
    holder
        .simple_query("LISTEN held_channel")
        .await
        .expect("the pin is inside the count ceiling");
    tokio::time::sleep(Duration::from_secs(5)).await;
    holder
        .simple_query("SELECT 1")
        .await
        .expect_err("a pin held past the bound must have closed its client");

    // And the budget it was holding is free again: the next client can take the pin the
    // ceiling would otherwise still be refusing.
    let next = stack.connect().await;
    next.simple_query("LISTEN next_channel")
        .await
        .expect("the pinned budget the expiry released must be available again");
}

// A pin expiring mid-statement must cancel the backend, not merely close the session. The
// budget is handed to a queued client before the Terminate is even sent, and a Terminate is
// only honoured once the backend finishes what it is running - so without the cancel the
// instance runs one backend over budget for the length of the abandoned statement, and the
// query deadline that would have stopped it died with the session.
#[tokio::test(flavor = "multi_thread")]
async fn a_pin_that_expires_mid_statement_cancels_the_backend_it_gives_up() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\nbackendConnections = 4\nmaxPinnedPercent = 20\n\
         maxPinDurationSeconds = 3\n",
    )
    .await;

    let holder = stack.connect().await;
    holder
        .simple_query("LISTEN expiring_channel")
        .await
        .expect("the pin is inside the count ceiling");
    holder
        .simple_query("SELECT pg_sleep(60)")
        .await
        .expect_err("the pin expires under the statement and ends the session");

    let (observer, conn) = tokio_postgres::connect(
        &stack.pg.direct_url("pin_expiry_observer"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("an observer connection straight to PostgreSQL");
    tokio::spawn(conn);
    let cleared = Instant::now();
    loop {
        // Excluding this backend is load-bearing: the observer's own row carries this very
        // statement and would satisfy the pattern it searches for.
        let running: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity \
                 WHERE pid <> pg_backend_pid() AND state = 'active' \
                 AND query LIKE '%pg_sleep(60)%'",
                &[],
            )
            .await
            .expect("counting backends still running the abandoned statement")
            .get(0);
        if running == 0 {
            break;
        }
        assert!(
            cleared.elapsed() < Duration::from_secs(15),
            "the pin expiry gave the budget away and left the backend running the statement"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// application_name is in the variable cache's TRACKED set, so a client landing on a link that
// carries somebody else's has its own re-SET before its first message. Keying on it therefore
// buys nothing and costs everything: every distinct application name mints its own pool, and
// the key map grows without bound. Asserted on churn rather than on concurrency - a test that
// counted peak concurrent backends would pass against the bug, because the fragmentation shows
// up as links opened, not as links held at once.
#[tokio::test(flavor = "multi_thread")]
async fn two_application_names_share_one_backend_when_the_policy_ignores_them() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\nignoreStartupParameters = [\"application_name\"]\n",
    )
    .await;

    // Sequential, not concurrent: two clients at once need two backends whatever the key says.
    let first = stack
        .connect_with(&format!("{} application_name=alpha", stack.url()))
        .await;
    let alpha: i32 = first
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("the first client's backend")
        .get(0);
    drop(first);
    // The link is released at ReadyForQuery, but the session task that parks it is a separate
    // task, so the second connection has to lose that race rather than assume it.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let second = stack
        .connect_with(&format!("{} application_name=beta", stack.url()))
        .await;
    let beta: i32 = second
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("the second client's backend")
        .get(0);

    assert_eq!(
        alpha, beta,
        "two application names minted two pools, so every distinct one costs a backend"
    );

    // The half an adversarial review of this layer found missing, and the reason it was
    // missing is that the assertion above passes in exactly the run where this one fails.
    // Sharing a link is the *cost* side of ignoring a parameter; this is the safety side.
    let seen: String = second
        .query_one("SELECT current_setting('application_name')", &[])
        .await
        .expect("what the second client is actually running as")
        .get(0);
    assert_eq!(
        seen, "beta",
        "the second client inherited the first one's application_name from the pool's cached \
         greeting and nothing ever corrected it"
    );
}

// The sharper instance of the same defect. A client that inherits another's TimeZone gets a
// different answer from every now(), current_date and date_trunc it runs, silently, for its
// whole session - and TimeZone is in TRACKED, so it is a parameter an operator is allowed to
// keep out of the pool key.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_sharing_a_link_runs_under_its_own_timezone_not_the_pools() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\nignoreStartupParameters = [\"TimeZone\"]\n",
    )
    .await;

    let first = stack
        .connect_with(&format!("{} options='-c timezone=Asia/Tokyo'", stack.url()))
        .await;
    let tokyo: String = first
        .query_one("SHOW TimeZone", &[])
        .await
        .expect("the first client's zone")
        .get(0);
    assert_eq!(tokyo, "Asia/Tokyo");
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let second = stack
        .connect_with(&format!("{} options='-c timezone=UTC'", stack.url()))
        .await;
    let utc: String = second
        .query_one("SHOW TimeZone", &[])
        .await
        .expect("the second client's zone")
        .get(0);
    assert_eq!(
        utc, "UTC",
        "the second client asked for UTC and is running in the first client's timezone, so \
         every timestamp it computes is wrong"
    );
}

// The other half of the same policy: a parameter the cache does not track stays in the key,
// so a client never inherits another's session-start value for it.
#[tokio::test(flavor = "multi_thread")]
async fn an_untracked_parameter_still_separates_two_clients() {
    let stack = harness::stack_with("[pool]\nmode = \"transaction\"\n").await;

    let first = stack
        .connect_with(&format!("{} options='-c search_path=alpha'", stack.url()))
        .await;
    let alpha: i32 = first
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("the first client's backend")
        .get(0);
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let second = stack
        .connect_with(&format!("{} options='-c search_path=beta'", stack.url()))
        .await;
    let beta: i32 = second
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("the second client's backend")
        .get(0);

    assert_ne!(
        alpha, beta,
        "a client was handed a link opened with another client's search_path, which no reset \
         restores"
    );
}

// The shape an adversarial review of this layer reproduced against a real backend, and the
// reason tracking a parameter does not license ignoring it. A client that names search_path
// puts it on the link through a SET the proxy issues itself - one that never reaches the
// tripwire, taints nothing and is answered by no ParameterStatus, because PostgreSQL does not
// report search_path. The next client, if it named none of its own, has nothing to diff and
// would read the previous tenant's schema for its whole session. Keeping the parameter in the
// pool key is what stops that, so the two clients here must NOT share a link.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_names_no_search_path_never_inherits_one() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\ntrackExtraParameters = [\"search_path\"]\n",
    )
    .await;

    let first = stack
        .connect_with(&format!("{} options='-c search_path=alpha'", stack.url()))
        .await;
    first
        .simple_query("SELECT 1")
        .await
        .expect("the first client runs under its own schema");
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // No options at all: the ordinary client, and the one the leak was invisible to.
    let second = stack.connect().await;
    let seen: String = second
        .query_one("SELECT current_setting('search_path')", &[])
        .await
        .expect("what the second client is actually reading")
        .get(0);
    assert_ne!(
        seen, "alpha",
        "a client that asked for no schema is reading the previous client's, which no reset \
         restores and no ParameterStatus reports"
    );
}

// A parked link is a backend PostgreSQL holds open for nobody: a process, its work_mem and one
// of the instance's max_connections. The pool opened them on demand and gave none of them back,
// so an estate's connection count only ever ratcheted to its busiest minute and stayed there.
#[tokio::test(flavor = "multi_thread")]
async fn a_link_left_parked_past_the_pool_idle_timeout_is_closed() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\nserverIdleTimeoutSeconds = 2\n\n\
         [[pool.tenants]]\nname = \"tenant\"\nburstable = 8\n",
    )
    .await;

    let client = stack.connect().await;
    let pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("opening a link")
        .get(0);
    drop(client);

    let (observer, conn) =
        tokio_postgres::connect(&stack.pg.direct_url("reap_observer"), tokio_postgres::NoTls)
            .await
            .expect("an observer connection straight to PostgreSQL");
    tokio::spawn(conn);

    // The reaper looks on its own interval, so this waits it out rather than assuming an
    // instant the pool can guarantee.
    let waited = Instant::now();
    loop {
        // Excluding this backend is load-bearing: the observer's own row is in the view.
        let alive: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity \
                 WHERE pid <> pg_backend_pid() AND pid = $1",
                &[&pid],
            )
            .await
            .expect("counting the parked backend")
            .get(0);
        if alive == 0 {
            break;
        }
        assert!(
            waited.elapsed() < Duration::from_secs(40),
            "a link parked past the idle timeout is still open"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// The guarantee is the one promise this allocator makes that nothing else may take back:
// acquire admits a tenant under its floor without queueing, so closing a link that puts it
// there turns the next arrival from an immediate grant into a connect. Idle is not unpromised.
#[tokio::test(flavor = "multi_thread")]
async fn a_guaranteed_link_is_not_reaped_however_long_it_sits() {
    let stack = harness::stack_with(
        "[pool]\nmode = \"transaction\"\nserverIdleTimeoutSeconds = 2\n\n\
         [[pool.tenants]]\nname = \"tenant\"\nguaranteed = 1\nburstable = 8\n",
    )
    .await;

    let client = stack.connect().await;
    let pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("opening a link")
        .get(0);
    drop(client);
    tokio::time::sleep(Duration::from_secs(25)).await;

    let (observer, conn) = tokio_postgres::connect(
        &stack.pg.direct_url("floor_observer"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("an observer connection straight to PostgreSQL");
    tokio::spawn(conn);
    let alive: i64 = observer
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_stat_activity \
             WHERE pid <> pg_backend_pid() AND pid = $1",
            &[&pid],
        )
        .await
        .expect("counting the guaranteed backend")
        .get(0);
    assert_eq!(
        alive, 1,
        "the tenant's guaranteed link was reaped, so its next client connects instead of \
         being granted one"
    );
}

#[tokio::test]
async fn a_cancel_request_cancels_a_long_running_query() {
    let stack = stack().await;
    let client = stack.connect().await;
    let token = client.cancel_token();

    let query = tokio::spawn(async move { client.simple_query("SELECT pg_sleep(30)").await });
    tokio::time::sleep(Duration::from_millis(500)).await;

    token.cancel_query(tokio_postgres::NoTls).await.unwrap();

    let started = Instant::now();
    let error = tokio::time::timeout(Duration::from_secs(10), query)
        .await
        .expect("the cancelled query must not run to completion")
        .unwrap()
        .expect_err("a cancelled query must fail");

    assert_eq!(
        error.code().map(tokio_postgres::error::SqlState::code),
        Some("57014"),
        "expected query_canceled, got {error}"
    );
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(
        stack
            .proxy
            .metrics
            .render()
            .contains("outcome=\"matched\"} 1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_cancel_key_is_dropped_rather_than_forwarded() {
    let stack = stack().await;
    let client = stack.connect().await;

    let mut socket = tokio::net::TcpStream::connect(stack.localhost())
        .await
        .unwrap();
    let mut wire = bytes::BytesMut::new();
    pgelastic_wire::CancelRequest {
        process_id: 1,
        key: pgelastic_wire::CancelKey::new(Bytes::from_static(b"\0\0\0\0")).unwrap(),
    }
    .encode(&mut wire);
    tokio::io::AsyncWriteExt::write_all(&mut socket, &wire)
        .await
        .unwrap();
    drop(socket);

    // The live session is untouched.
    assert_eq!(
        client
            .query_one("SELECT 5 AS still_here", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        5
    );
    assert!(
        stack
            .proxy
            .metrics
            .render()
            .contains("outcome=\"unmatched\"} 1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_graceful_shutdown_drains_in_flight_work_instead_of_resetting_it() {
    let stack = stack().await;
    let client = stack.connect().await;

    let query = tokio::spawn(async move {
        let rows = client.query("SELECT pg_sleep(3), 99 AS answer", &[]).await;
        (client, rows)
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let started = Instant::now();
    let drained = stack.proxy.running.shutdown().await;
    let elapsed = started.elapsed();

    let (client, rows) = query.await.unwrap();
    let rows = rows.expect("an in-flight query must finish, not be reset");
    assert_eq!(rows[0].get::<_, i32>("answer"), 99);

    assert!(drained, "the drain must complete inside its window");
    assert!(
        elapsed >= Duration::from_secs(2),
        "the drain returned in {elapsed:?}, so it cannot have waited for the query"
    );

    // Once drained the session is gone, so the next statement fails.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(client.query_one("SELECT 1", &[]).await.is_err());
    assert!(
        stack
            .proxy
            .metrics
            .render()
            .contains("outcome=\"graceful\"} 1")
    );
}

// The pipelined half of the same hole. A client may send a Sync and a Flush-terminated batch
// in one write. The batch sets unsynced_batch; the ReadyForQuery that answers the SYNC then
// cleared it, while the batch it does not belong to is still open. tx_status reports the idle
// the Sync left, the flag says no batch is open, and the idle bound never arms - and the
// statement deadline disarms the moment the batch's rows arrive. No bound of any kind.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_pipelined_behind_a_sync_is_still_bounded() {
    let stack =
        harness::stack_with("[pool]\nmode = \"transaction\"\nclientIdleInTransactionSeconds = 2\n")
            .await;
    let mut raw = RawClient::connect(stack.localhost(), "tenant", BACKEND_DATABASE).await;

    let batch = |query: &'static str| {
        vec![
            FrontendMessage::Parse(Parse {
                name: Bytes::new(),
                query: Bytes::from_static(query.as_bytes()),
                param_types: vec![],
            }),
            FrontendMessage::Bind(Bind {
                portal: Bytes::new(),
                statement: Bytes::new(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![Format::Text],
            }),
            FrontendMessage::Execute(Execute {
                portal: Bytes::new(),
                max_rows: 0,
            }),
        ]
    };

    // One write: a batch the client ends properly, then a second it leaves open.
    let mut pipelined = batch("SELECT 1");
    pipelined.push(FrontendMessage::Sync);
    pipelined.extend(batch("SELECT 2"));
    pipelined.push(FrontendMessage::Flush);
    raw.send(&pipelined).await;

    let started = Instant::now();
    raw.read_until(|message| matches!(message, pgelastic_wire::BackendMessage::ReadyForQuery(_)))
        .await;
    assert!(
        raw.closed_within(Duration::from_secs(15)).await,
        "a batch pipelined behind a Sync held its backend past the bound: {:?}",
        started.elapsed()
    );
}
