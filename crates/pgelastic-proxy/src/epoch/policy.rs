//! The in-flight transaction policy, which is explicit and asymmetric.
//!
//! | State on the old epoch | Action |
//! |---|---|
//! | Read-only transaction | Let it finish, then close. It cannot cause split brain. |
//! | Idle in transaction | RST immediately. |
//! | Write transaction, `Commit` not sent | RST immediately; an aborted transaction is a correct answer. |
//! | `Commit` forwarded, `CommandComplete` not received | **Genuinely undecidable.** Report `UNKNOWN`, record it, never retry, never claim either outcome. |
//!
//! Every classification here errs towards *write*. Mistaking a read for a write
//! costs one client an aborted transaction, which is recoverable. Mistaking a
//! write for a read is a lost acknowledged commit, which is not. So
//! [`may_write`] is a whitelist of statements that provably cannot write and
//! everything else is a write, and a statement whose text was never seen — an
//! `Execute` naming a portal this session did not `Bind` — is a write too.
//!
//! **The one gap this scan cannot close, stated rather than hidden:** a
//! `SELECT` that calls a `VOLATILE` user-defined function which writes is
//! indistinguishable from a read by text alone. [`WRITE_FUNCTIONS`] catches the
//! built-ins that do it, and nothing catches a user's own. The exposure is
//! bounded and it is not the unbounded one: such a statement is allowed to
//! finish, so its outcome *is* observed and never invented — what is lost is
//! that its write may be rewound after the client was told it succeeded. A
//! transaction the client declares with `BEGIN READ ONLY` closes the gap
//! completely, because `PostgreSQL` itself then enforces the property.

use std::collections::HashMap;

use bytes::Bytes;
use pgelastic_wire::{BackendMessage, FrontendMessage, TransactionStatus};

/// What a session was doing on the superseded epoch when the fence fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InFlight {
    /// Between transactions with nothing outstanding. Nothing is owed either
    /// way, so the socket simply goes.
    Idle,
    /// A transaction — explicit or implicit — that has issued nothing able to
    /// write and is still waiting for its answer.
    ReadOnlyTransaction,
    /// Inside a transaction block with no request outstanding: the client is
    /// thinking, and it is thinking on a postmaster that is about to be
    /// rewound.
    IdleInTransaction,
    /// A transaction that has issued a write whose `Commit` has not been sent.
    WriteUncommitted,
    /// The commit — an explicit `COMMIT`, or the implicit one that ends a
    /// single-statement write — was forwarded and its completion was never
    /// seen.
    CommitInDoubt,
}

/// What the fence does about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceAction {
    /// Close the backend socket. Nothing is owed to the client.
    Close,
    /// Let the outstanding statement finish, then close. A read cannot cause
    /// split brain, and killing it would fail a query that was going to be
    /// correct.
    DrainThenClose,
    /// RST now, without waiting and without a graceful `Terminate`. The
    /// transaction aborts, and an aborted transaction is a correct answer.
    ResetNow,
    /// Return the distinguished `UNKNOWN` code, record the transaction in the
    /// durable in-doubt log, and then RST. Never reported as a success, never
    /// as a failure, never retried.
    ReportUnknown,
}

/// The policy matrix, and the only place it is written down.
pub const fn action(state: InFlight) -> FenceAction {
    match state {
        InFlight::Idle => FenceAction::Close,
        InFlight::ReadOnlyTransaction => FenceAction::DrainThenClose,
        InFlight::IdleInTransaction | InFlight::WriteUncommitted => FenceAction::ResetNow,
        InFlight::CommitInDoubt => FenceAction::ReportUnknown,
    }
}

/// What the outstanding request would do if it completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Provably unable to write.
    Read,
    /// Able to write, or not provably unable to.
    Write,
    /// Ends a transaction by making its writes durable.
    Commit,
}

/// Everything the fence needs to classify a session, fed by the frames the
/// relay is already decoding.
///
/// It follows the extended protocol as well as the simple one: the text arrives
/// at `Parse`, the statement name is bound to a portal at `Bind`, and `Execute`
/// names the portal. A name this witness never saw resolves to
/// [`Kind::Write`], because the alternative is to guess in the unsafe
/// direction.
#[derive(Debug, Default)]
pub struct TransactionWitness {
    /// A write has been issued in the transaction currently open.
    wrote: bool,
    /// The nature of the request forwarded and not yet answered.
    pending: Option<Kind>,
    /// The text of that request, so the in-doubt log records what it was rather
    /// than only that there was one.
    pending_sql: String,
    statements: HashMap<Bytes, Kind>,
    portals: HashMap<Bytes, Kind>,
    texts: HashMap<Bytes, String>,
}

impl TransactionWitness {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a message on its way to the backend.
    ///
    /// Called with the message the relay is about to forward, never with one it
    /// fakes an answer to: a `Parse` the pool answers from its own cache
    /// executes nothing.
    pub fn observe_frontend(&mut self, message: &FrontendMessage) {
        match message {
            FrontendMessage::Query(sql) => {
                let kind = classify(sql);
                self.begin_request(kind, text(sql));
            }
            FrontendMessage::Parse(parse) => {
                self.statements
                    .insert(parse.name.clone(), classify(&parse.query));
                self.texts.insert(parse.name.clone(), text(&parse.query));
            }
            FrontendMessage::Bind(bind) => {
                let kind = self
                    .statements
                    .get(&bind.statement)
                    .copied()
                    .unwrap_or(Kind::Write);
                self.portals.insert(bind.portal.clone(), kind);
                let sql = self
                    .texts
                    .get(&bind.statement)
                    .cloned()
                    .unwrap_or_else(|| UNKNOWN_STATEMENT.to_owned());
                self.texts.insert(bind.portal.clone(), sql);
            }
            FrontendMessage::Execute(execute) => {
                let kind = self
                    .portals
                    .get(&execute.portal)
                    .copied()
                    .unwrap_or(Kind::Write);
                let sql = self
                    .texts
                    .get(&execute.portal)
                    .cloned()
                    .unwrap_or_else(|| UNKNOWN_STATEMENT.to_owned());
                self.begin_request(kind, sql);
            }
            FrontendMessage::FunctionCall(_) => {
                self.begin_request(Kind::Write, "FunctionCall".to_owned());
            }
            // Everything else executes nothing of its own. `CopyData` and
            // `CopyDone` are payload on a link the enclosing statement has
            // already classified; `Describe`, `Close`, `Flush` and `Sync` draw
            // answers but retire no request this policy cares about.
            _ => {}
        }
    }

    /// The text of the request whose answer has not arrived, or a stated
    /// placeholder. Never a guess: an `Execute` for a portal this session did
    /// not `Bind` reports that it does not know the statement.
    pub fn pending_sql(&self) -> &str {
        if self.pending.is_none() {
            return "";
        }
        &self.pending_sql
    }

    /// Records a message on its way back to the client.
    pub fn observe_backend(&mut self, message: &BackendMessage) {
        match message {
            // The outcome of the outstanding request has been observed. That is
            // the whole point: after this the commit is no longer in doubt.
            BackendMessage::CommandComplete(_)
            | BackendMessage::EmptyQueryResponse
            | BackendMessage::ErrorResponse(_)
            | BackendMessage::PortalSuspended => self.pending = None,
            BackendMessage::ReadyForQuery(status) => {
                self.pending = None;
                if *status == TransactionStatus::Idle {
                    self.wrote = false;
                    self.portals.clear();
                }
            }
            _ => {}
        }
    }

    /// The session's state, given the transaction status the link last
    /// reported.
    pub fn state(&self, tx_status: TransactionStatus) -> InFlight {
        match (self.pending, tx_status) {
            // A single-statement write outside a transaction block carries its
            // own commit, so its completion is exactly as undecidable as an
            // explicit `COMMIT`'s.
            (Some(Kind::Commit), _) | (Some(Kind::Write), TransactionStatus::Idle) => {
                InFlight::CommitInDoubt
            }
            (Some(Kind::Write), _) => InFlight::WriteUncommitted,
            (Some(Kind::Read), _) => InFlight::ReadOnlyTransaction,
            (None, TransactionStatus::Idle) => InFlight::Idle,
            (None, TransactionStatus::Transaction | TransactionStatus::Failed) => {
                InFlight::IdleInTransaction
            }
        }
    }

    fn begin_request(&mut self, kind: Kind, sql: String) {
        if kind == Kind::Write {
            self.wrote = true;
        }
        self.pending_sql = sql;
        // A `COMMIT` that ends a transaction which has written is undecidable;
        // one that ends a read-only transaction has nothing to make durable, so
        // it is treated as the read it is.
        self.pending = Some(match kind {
            Kind::Commit if self.wrote => Kind::Commit,
            Kind::Commit => Kind::Read,
            other => other,
        });
    }
}

/// Whether `sql` ends a transaction by making its writes durable.
///
/// `PREPARE TRANSACTION` counts: it is the point at which the writes become
/// recoverable independently of the session, so its completion is exactly as
/// undecidable as a `COMMIT`'s.
pub fn is_commit(sql: &[u8]) -> bool {
    let tokens = tokenize(sql);
    let first = tokens.first().copied().unwrap_or_default();
    if eq(first, "commit") || eq(first, "end") {
        return true;
    }
    eq(first, "prepare") && tokens.get(1).copied().is_some_and(|t| eq(t, "transaction"))
}

/// Built-in functions that write even inside a statement that reads.
///
/// Not exhaustive and cannot be: a user-defined `VOLATILE` function is opaque
/// to a token scan. See this module's own documentation for what that costs and
/// what it does not cost.
pub const WRITE_FUNCTIONS: [&str; 6] = [
    "nextval",
    "setval",
    "dblink_exec",
    "pg_logical_emit_message",
    "pg_create_restore_point",
    "pg_replication_origin_advance",
];

/// Whether `sql` might write.
///
/// A whitelist, deliberately: `false` is only ever returned for a statement
/// that provably cannot modify data, and every other statement — including one
/// this function does not recognise — is a write.
pub fn may_write(sql: &[u8]) -> bool {
    let tokens = tokenize(sql);
    let Some(first) = tokens.first().copied() else {
        return false;
    };
    let has = |word: &str| tokens.iter().any(|token| eq(token, word));

    let calls_a_writing_function = tokens
        .iter()
        .any(|token| WRITE_FUNCTIONS.iter().any(|name| eq(token, name)));

    if eq(first, "select") || eq(first, "table") || eq(first, "values") {
        // A locking clause takes row locks and blocks other writers, and
        // `SELECT INTO` creates a table.
        return has("update") || has("share") || has("into") || calls_a_writing_function;
    }
    if eq(first, "explain") {
        return has("analyze") || calls_a_writing_function;
    }
    // Transaction and session control write no data of their own. `COMMIT` is
    // in this set because it is classified by `is_commit` before it gets here,
    // and a `COMMIT` of a transaction that wrote nothing has nothing to lose.
    for control in [
        "show",
        "fetch",
        "begin",
        "start",
        "commit",
        "end",
        "rollback",
        "abort",
        "savepoint",
        "release",
        "set",
        "reset",
        "discard",
        "close",
        "deallocate",
        "unlisten",
    ] {
        if eq(first, control) {
            return false;
        }
    }
    true
}

fn classify(sql: &[u8]) -> Kind {
    if is_commit(sql) {
        Kind::Commit
    } else if may_write(sql) {
        Kind::Write
    } else {
        Kind::Read
    }
}

/// What the in-doubt log records when the client executed a portal this
/// session never saw a `Parse` for. Stated, not guessed.
const UNKNOWN_STATEMENT: &str = "<statement text not seen by this proxy>";

fn text(sql: &Bytes) -> String {
    String::from_utf8_lossy(sql).trim().to_owned()
}

fn eq(token: &[u8], word: &str) -> bool {
    token.eq_ignore_ascii_case(word.as_bytes())
}

/// Splits on everything that cannot appear inside an identifier, so
/// `committed_at` is not `COMMIT`.
fn tokenize(sql: &[u8]) -> Vec<&[u8]> {
    sql.split(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
        .filter(|token| !token.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(sql: &str) -> FrontendMessage {
        FrontendMessage::Query(Bytes::copy_from_slice(sql.as_bytes()))
    }

    fn ready(status: TransactionStatus) -> BackendMessage {
        BackendMessage::ReadyForQuery(status)
    }

    fn complete(tag: &str) -> BackendMessage {
        BackendMessage::CommandComplete(Bytes::copy_from_slice(tag.as_bytes()))
    }

    // ---- one test per row of the matrix ---------------------------------

    #[test]
    fn a_read_only_transaction_is_allowed_to_finish_and_then_closed() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("BEGIN"));
        witness.observe_backend(&complete("BEGIN"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("SELECT count(*) FROM orders"));

        let state = witness.state(TransactionStatus::Transaction);
        assert_eq!(state, InFlight::ReadOnlyTransaction);
        assert_eq!(action(state), FenceAction::DrainThenClose);
    }

    #[test]
    fn a_session_idle_in_transaction_is_reset_immediately() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("BEGIN"));
        witness.observe_backend(&complete("BEGIN"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));

        let state = witness.state(TransactionStatus::Transaction);
        assert_eq!(state, InFlight::IdleInTransaction);
        assert_eq!(action(state), FenceAction::ResetNow);
    }

    #[test]
    fn a_write_transaction_whose_commit_was_not_sent_is_reset_immediately() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("BEGIN"));
        witness.observe_backend(&complete("BEGIN"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("INSERT INTO orders (id) VALUES (1)"));

        let state = witness.state(TransactionStatus::Transaction);
        assert_eq!(state, InFlight::WriteUncommitted);
        assert_eq!(action(state), FenceAction::ResetNow);
    }

    #[test]
    fn a_commit_forwarded_without_a_command_complete_is_undecidable() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("BEGIN"));
        witness.observe_backend(&complete("BEGIN"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("UPDATE accounts SET balance = 0"));
        witness.observe_backend(&complete("UPDATE 1"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("COMMIT"));

        let state = witness.state(TransactionStatus::Transaction);
        assert_eq!(state, InFlight::CommitInDoubt);
        assert_eq!(action(state), FenceAction::ReportUnknown);
    }

    // ---- the rows' boundaries -------------------------------------------

    #[test]
    fn a_commit_whose_command_complete_arrived_is_no_longer_in_doubt() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("BEGIN"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("INSERT INTO orders (id) VALUES (1)"));
        witness.observe_backend(&complete("INSERT 0 1"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("COMMIT"));
        witness.observe_backend(&complete("COMMIT"));
        witness.observe_backend(&ready(TransactionStatus::Idle));

        assert_eq!(witness.state(TransactionStatus::Idle), InFlight::Idle);
        assert_eq!(action(InFlight::Idle), FenceAction::Close);
    }

    /// An autocommit write carries its own commit, so losing its answer is the
    /// same undecidable case as losing an explicit `COMMIT`'s.
    #[test]
    fn an_unanswered_autocommit_write_is_undecidable_too() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("INSERT INTO orders (id) VALUES (1)"));
        assert_eq!(
            witness.state(TransactionStatus::Idle),
            InFlight::CommitInDoubt
        );
    }

    #[test]
    fn an_unanswered_autocommit_read_is_allowed_to_finish() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("SELECT 1"));
        assert_eq!(
            witness.state(TransactionStatus::Idle),
            InFlight::ReadOnlyTransaction
        );
    }

    #[test]
    fn a_commit_of_a_transaction_that_wrote_nothing_has_nothing_to_lose() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("BEGIN"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("SELECT 1"));
        witness.observe_backend(&complete("SELECT 1"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("COMMIT"));

        assert_eq!(
            witness.state(TransactionStatus::Transaction),
            InFlight::ReadOnlyTransaction
        );
    }

    #[test]
    fn a_failed_transaction_block_is_still_idle_in_transaction() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("BEGIN"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("SELECT nonsense"));
        witness.observe_backend(&BackendMessage::ErrorResponse(pgelastic_wire::Fields::new(
            Vec::new(),
        )));
        witness.observe_backend(&ready(TransactionStatus::Failed));

        assert_eq!(
            witness.state(TransactionStatus::Failed),
            InFlight::IdleInTransaction
        );
    }

    #[test]
    fn a_write_earlier_in_the_transaction_outranks_a_read_outstanding_now() {
        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&query("BEGIN"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("DELETE FROM orders WHERE id = 1"));
        witness.observe_backend(&complete("DELETE 1"));
        witness.observe_backend(&ready(TransactionStatus::Transaction));
        witness.observe_frontend(&query("COMMIT"));

        assert_eq!(
            witness.state(TransactionStatus::Transaction),
            InFlight::CommitInDoubt
        );
    }

    // ---- the extended protocol ------------------------------------------

    #[test]
    fn an_execute_is_classified_by_the_text_its_parse_carried() {
        use pgelastic_wire::{Bind, Execute, Parse};

        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&FrontendMessage::Parse(Parse {
            name: Bytes::from_static(b"s1"),
            query: Bytes::from_static(b"INSERT INTO orders (id) VALUES ($1)"),
            param_types: Vec::new(),
        }));
        witness.observe_frontend(&FrontendMessage::Bind(Bind {
            portal: Bytes::from_static(b"p1"),
            statement: Bytes::from_static(b"s1"),
            param_formats: Vec::new(),
            params: Vec::new(),
            result_formats: Vec::new(),
        }));
        witness.observe_frontend(&FrontendMessage::Execute(Execute {
            portal: Bytes::from_static(b"p1"),
            max_rows: 0,
        }));

        assert_eq!(
            witness.state(TransactionStatus::Idle),
            InFlight::CommitInDoubt
        );
    }

    #[test]
    fn an_execute_naming_a_portal_this_session_never_bound_is_a_write() {
        use pgelastic_wire::Execute;

        let mut witness = TransactionWitness::new();
        witness.observe_frontend(&FrontendMessage::Execute(Execute {
            portal: Bytes::from_static(b"stranger"),
            max_rows: 0,
        }));
        assert_eq!(
            witness.state(TransactionStatus::Transaction),
            InFlight::WriteUncommitted
        );
    }

    // ---- classification --------------------------------------------------

    #[test]
    fn a_plain_select_cannot_write_but_a_locking_one_can() {
        assert!(!may_write(b"SELECT id FROM orders"));
        assert!(!may_write(b"  select 1  "));
        assert!(may_write(b"SELECT id FROM orders FOR UPDATE"));
        assert!(may_write(b"SELECT id FROM orders FOR SHARE"));
        assert!(may_write(b"SELECT id INTO snapshot FROM orders"));
    }

    #[test]
    fn every_statement_this_scan_does_not_recognise_is_a_write() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET n = 1",
            "DELETE FROM t",
            "MERGE INTO t USING s ON true WHEN MATCHED THEN DO NOTHING",
            "WITH moved AS (DELETE FROM t RETURNING *) SELECT * FROM moved",
            "CREATE TABLE t (n int)",
            "TRUNCATE t",
            "REFRESH MATERIALIZED VIEW v",
            "CALL do_something()",
            "COPY t FROM STDIN",
            "VACUUM",
            "GRANT ALL ON t TO alice",
            "some_future_statement_nobody_has_written_yet",
        ] {
            assert!(may_write(sql.as_bytes()), "{sql} must count as a write");
        }
    }

    #[test]
    fn a_select_that_calls_a_writing_builtin_is_a_write() {
        assert!(may_write(b"SELECT nextval('orders_id_seq')"));
        assert!(may_write(b"SELECT setval('s', 1)"));
        assert!(may_write(b"SELECT dblink_exec('remote', 'DELETE FROM t')"));
        assert!(may_write(b"EXPLAIN SELECT nextval('s')"));
    }

    #[test]
    fn explain_is_a_read_unless_it_analyses() {
        assert!(!may_write(b"EXPLAIN SELECT 1"));
        assert!(may_write(b"EXPLAIN ANALYZE DELETE FROM t"));
    }

    #[test]
    fn transaction_and_session_control_write_nothing_of_their_own() {
        for sql in [
            "BEGIN",
            "START TRANSACTION",
            "ROLLBACK",
            "ABORT",
            "SAVEPOINT s",
            "RELEASE SAVEPOINT s",
            "SET application_name = 'x'",
            "RESET ALL",
            "DISCARD ALL",
            "SHOW pgelastic.primary_epoch",
            "DEALLOCATE ALL",
        ] {
            assert!(
                !may_write(sql.as_bytes()),
                "{sql} must not count as a write"
            );
        }
    }

    #[test]
    fn commit_end_and_prepare_transaction_are_commits_and_rollback_is_not() {
        assert!(is_commit(b"COMMIT"));
        assert!(is_commit(b"commit;"));
        assert!(is_commit(b"END"));
        assert!(is_commit(b"PREPARE TRANSACTION 'txn-1'"));
        assert!(!is_commit(b"ROLLBACK"));
        assert!(!is_commit(b"PREPARE p AS SELECT $1::int"));
        assert!(!is_commit(b"SELECT committed_at FROM orders"));
    }

    #[test]
    fn the_matrix_maps_every_state_to_exactly_one_action() {
        assert_eq!(action(InFlight::Idle), FenceAction::Close);
        assert_eq!(
            action(InFlight::ReadOnlyTransaction),
            FenceAction::DrainThenClose
        );
        assert_eq!(action(InFlight::IdleInTransaction), FenceAction::ResetNow);
        assert_eq!(action(InFlight::WriteUncommitted), FenceAction::ResetNow);
        assert_eq!(action(InFlight::CommitInDoubt), FenceAction::ReportUnknown);
    }
}
