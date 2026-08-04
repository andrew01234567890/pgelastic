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
