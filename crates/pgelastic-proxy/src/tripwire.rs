//! Detection of session state no reset can remove.
//!
//! The converged industry answer to unscrubbable state — RDS Proxy's — is to
//! **pin rather than scrub**, and it is right: pinning costs throughput,
//! leaking loses tenant data. So this scan is deliberately over-eager. A token
//! that merely looks like `LISTEN` costs one client its multiplexing; a `LISTEN`
//! that slips through delivers one tenant's notification payloads to another.
//!
//! It is a token scan and emphatically not a parser. Nothing in the proxy uses
//! SQL text to decide *pooling* behaviour — the release boundary is the backend's
//! `ReadyForQuery` byte and nothing else. This decides only whether a link may
//! ever be handed on, which is a one-way door that can be taken too often
//! without harm.

use pgelastic_pool::{PinReason, Taint, TaintSet};

/// What one statement's text implies about the link that ran it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scan {
    /// State no reset removes. The link may never be handed on.
    pub pin: Option<PinReason>,
    /// State a reset *does* remove, but which `PostgreSQL` will not report.
    ///
    /// `Taint::SessionParameter` is otherwise fed only by `ParameterStatus`, and the server
    /// sends that for its `GUC_REPORT` set alone — so `search_path`, `role`, `statement_timeout`
    /// and every custom variable are invisible to it. Under a policy that scrubs only what it
    /// believes is dirty, invisible means it survives to the next client on the same pool key.
    pub taint: TaintSet,
}

/// Everything `sql` implies about its link, in one pass.
pub fn scan(sql: &[u8]) -> Scan {
    let tokens = tokenize(sql);
    Scan {
        pin: pin_reason(&tokens),
        taint: taint_of(sql),
    }
}

fn pin_reason(tokens: &[&[u8]]) -> Option<PinReason> {
    let has = |word: &str| {
        tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case(word.as_bytes()))
    };

    // `setseed` first: `seed` is GUC_NO_RESET | GUC_NO_RESET_ALL, so it is the
    // one reason that forces the connection closed rather than merely pinned,
    // and reporting a weaker reason alongside it would understate that.
    if has("setseed") {
        return Some(PinReason::SetSeed);
    }
    if has("dblink_connect") || has("dblink_connect_u") {
        return Some(PinReason::Dblink);
    }
    if has("prepare") && has("transaction") {
        return Some(PinReason::PreparedTransaction);
    }
    if has("load") {
        return Some(PinReason::Load);
    }
    if tokens.iter().any(|token| is_session_advisory_lock(token)) {
        return Some(PinReason::SessionAdvisoryLock);
    }
    if has("listen") {
        return Some(PinReason::Listen);
    }
    if has("declare") && has("hold") {
        return Some(PinReason::HoldCursor);
    }
    if has("create") && (has("temp") || has("temporary")) {
        return Some(PinReason::TempTable);
    }
    None
}

/// Session state a scrub would remove but the server never announces.
///
/// Position matters, and it is the whole difference between this being useful and being a
/// permanent `DISCARD ALL`: `SET` is a session assignment only when it opens a statement.
/// Matching it anywhere would fire on every `UPDATE ... SET ...`, i.e. on every write.
///
/// Over-eager in every other respect, deliberately. A statement separator inside a string
/// literal splits a statement that did not need splitting, and the cost of a false positive is
/// one `DISCARD ALL` — exactly what the previous default did unconditionally. The cost of a
/// false negative is another client reading this one's `search_path`.
fn taint_of(sql: &[u8]) -> TaintSet {
    let mut taint = TaintSet::new();

    for statement in sql.split(|byte| *byte == b';') {
        let tokens = tokenize(statement);
        let Some(first) = tokens.first() else {
            continue;
        };
        let is = |token: &[u8], word: &str| token.eq_ignore_ascii_case(word.as_bytes());
        let second = tokens.get(1);

        // `SET LOCAL` reverts when the transaction ends, and the link is released at exactly
        // that boundary, so it is gone before anybody else could see it. Excluding it matters:
        // it is the recommended way to carry a tenant id for row-level security, and tainting
        // it would scrub every transaction of the users who most need this to be cheap.
        let set_local = is(first, "set") && second.is_some_and(|token| is(token, "local"));

        if (is(first, "set") && !set_local) || is(first, "reset") {
            taint.insert(Taint::SessionParameter);
        }
        // `PREPARE TRANSACTION` is a two-phase commit and already pins.
        if is(first, "prepare") && !second.is_some_and(|token| is(token, "transaction")) {
            taint.insert(Taint::PreparedStatement);
        }
        if is(first, "declare") {
            taint.insert(Taint::Cursor);
        }
        for token in &tokens {
            if is(token, "set_config") {
                taint.insert(Taint::SessionParameter);
            }
            if is(token, "nextval") || is(token, "setval") {
                taint.insert(Taint::Sequence);
            }
        }
    }
    taint
}

/// Whether a token names an advisory lock held for the session.
///
/// `pg_advisory_xact_lock` and friends release at commit, so they are ordinary
/// transactional state. `pg_advisory_unlock` releases rather than takes.
fn is_session_advisory_lock(token: &[u8]) -> bool {
    let Ok(name) = std::str::from_utf8(token) else {
        return false;
    };
    let name = name.to_ascii_lowercase();
    name.starts_with("pg_advisory_lock") || name.starts_with("pg_try_advisory_lock")
}

/// Splits on everything that cannot appear inside an identifier, so `UNLISTEN`
/// and `LISTEN` are different tokens and `discarded_at` is not `DISCARD`.
fn tokenize(sql: &[u8]) -> Vec<&[u8]> {
    sql.split(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
        .filter(|token| !token.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scanned(sql: &str) -> Option<PinReason> {
        scan(sql.as_bytes()).pin
    }

    fn tainted(sql: &str) -> TaintSet {
        scan(sql.as_bytes()).taint
    }

    /// The single most likely way to get this scan wrong: `SET` is a session assignment only
    /// when it opens a statement, and matching it anywhere would taint every write.
    #[test]
    fn an_update_with_a_set_clause_is_not_a_session_parameter_assignment() {
        assert!(tainted("UPDATE orders SET total = 10 WHERE id = 1").is_clean());
        assert!(
            tainted("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2").is_clean()
        );
        assert!(tainted("SELECT * FROM settings").is_clean());
    }

    #[test]
    fn a_session_assignment_the_server_never_reports_is_still_seen() {
        for sql in [
            "SET search_path TO audit",
            "set ROLE snoop",
            "RESET search_path",
            "SELECT set_config('app.tenant', '42', false)",
        ] {
            assert!(
                tainted(sql).contains(Taint::SessionParameter),
                "{sql} leaves state PostgreSQL will not announce"
            );
        }
    }

    /// `SET LOCAL` reverts when the transaction ends, and the link is released at exactly that
    /// boundary. Tainting it would scrub every transaction of the row-level-security users who
    /// most need this to be cheap.
    #[test]
    fn a_transaction_scoped_assignment_is_not_a_taint() {
        assert!(tainted("SET LOCAL app.current_tenant = '42'").is_clean());
        assert!(tainted("BEGIN; SET LOCAL app.tenant = '7'; SELECT 1; COMMIT").is_clean());
    }

    #[test]
    fn sql_level_prepare_and_declare_taint_without_pinning() {
        let prepared = scan(b"PREPARE p AS SELECT 1");
        assert!(prepared.taint.contains(Taint::PreparedStatement));
        assert_eq!(prepared.pin, None, "an ordinary PREPARE is scrubbable");

        let cursor = scan(b"DECLARE c CURSOR FOR SELECT 1");
        assert!(cursor.taint.contains(Taint::Cursor));
        assert_eq!(cursor.pin, None);

        // Two-phase commit is a different thing and keeps its pin.
        assert_eq!(
            scan(b"PREPARE TRANSACTION 'tx1'").pin,
            Some(PinReason::PreparedTransaction)
        );
    }

    #[test]
    fn a_sequence_call_taints_the_link() {
        assert!(tainted("SELECT nextval('s')").contains(Taint::Sequence));
        assert!(tainted("SELECT setval('s', 1)").contains(Taint::Sequence));
    }

    #[test]
    fn listen_pins_the_connection() {
        assert_eq!(scanned("LISTEN channel_a"), Some(PinReason::Listen));
        assert_eq!(scanned("listen \"MixedCase\""), Some(PinReason::Listen));
    }

    #[test]
    fn unlisten_is_not_listen() {
        assert_eq!(scanned("UNLISTEN *"), None);
    }

    #[test]
    fn a_held_cursor_pins_the_connection() {
        assert_eq!(
            scanned("DECLARE c CURSOR WITH HOLD FOR SELECT 1"),
            Some(PinReason::HoldCursor)
        );
        assert_eq!(scanned("DECLARE c CURSOR FOR SELECT 1"), None);
    }

    #[test]
    fn a_temp_table_pins_the_connection() {
        assert_eq!(
            scanned("CREATE TEMP TABLE scratch (n int)"),
            Some(PinReason::TempTable)
        );
        assert_eq!(
            scanned("create temporary table t on commit preserve rows as select 1"),
            Some(PinReason::TempTable)
        );
        assert_eq!(scanned("CREATE TABLE permanent (n int)"), None);
    }

    #[test]
    fn a_session_advisory_lock_pins_but_a_transactional_one_does_not() {
        assert_eq!(
            scanned("SELECT pg_advisory_lock(42)"),
            Some(PinReason::SessionAdvisoryLock)
        );
        assert_eq!(
            scanned("SELECT pg_try_advisory_lock(1, 2)"),
            Some(PinReason::SessionAdvisoryLock)
        );
        assert_eq!(
            scanned("SELECT pg_advisory_lock_shared(42)"),
            Some(PinReason::SessionAdvisoryLock)
        );
        assert_eq!(scanned("SELECT pg_advisory_xact_lock(42)"), None);
        assert_eq!(scanned("SELECT pg_advisory_unlock(42)"), None);
    }

    #[test]
    fn setseed_outranks_every_other_reason_because_it_forces_a_close() {
        assert_eq!(
            scanned("SELECT setseed(0.5); LISTEN c"),
            Some(PinReason::SetSeed)
        );
        assert!(PinReason::SetSeed.forces_close());
    }

    #[test]
    fn load_prepare_transaction_and_dblink_pin() {
        assert_eq!(scanned("LOAD 'auto_explain'"), Some(PinReason::Load));
        assert_eq!(
            scanned("PREPARE TRANSACTION 'txn-1'"),
            Some(PinReason::PreparedTransaction)
        );
        assert_eq!(
            scanned("SELECT dblink_connect('remote', 'dbname=other')"),
            Some(PinReason::Dblink)
        );
    }

    #[test]
    fn a_plain_prepare_is_not_a_prepared_transaction() {
        assert_eq!(scanned("PREPARE p AS SELECT $1::int"), None);
    }

    #[test]
    fn ordinary_traffic_never_pins() {
        for sql in [
            "SELECT 1",
            "INSERT INTO orders (id) VALUES (1)",
            "BEGIN",
            "COMMIT",
            "UPDATE t SET listened_at = now()",
            "SELECT loader FROM shipments",
            "DISCARD ALL",
        ] {
            assert_eq!(scanned(sql), None, "{sql} must not pin");
        }
    }
}
