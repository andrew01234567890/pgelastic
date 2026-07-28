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

use pgelastic_pool::PinReason;

/// The first tripwire `sql` fires, if any.
pub fn scan(sql: &[u8]) -> Option<PinReason> {
    let tokens = tokenize(sql);
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
        scan(sql.as_bytes())
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
