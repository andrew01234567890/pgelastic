//! The `GUC_REPORT`ed variable cache.
//!
//! A backend that has just been handed to a different client is still carrying
//! the previous client's reported settings. The client, meanwhile, has been told
//! whatever the last `ParameterStatus` it saw said. Diffing the two on every
//! assignment and emitting the `SET`s that close the gap — then holding the
//! client until they have flushed — is what makes a handoff invisible.
//!
//! **CVE-2025-12819 is the reason the name side of every generated statement is
//! a closed set.** [`Tracked`] is the only source of a name that can appear in a
//! statement this module builds, and the value is always a quoted literal, so no
//! client-supplied text is ever resolved as an identifier or through a
//! client-influenced `search_path`.
//!
//! The set is closed *per process*, not at compile time: `trackExtraParameters`
//! lets an operator add to it. That is a different thing from letting a client
//! add to it - the names come from the document the process was started with,
//! never from the wire - but it is only closed if the names are checked, so
//! [`Tracked::with_extra`] refuses anything that is not a plain GUC identifier
//! and does it at start-up rather than at the statement it would corrupt. Nothing here runs on a connection that is
//! executing an internal query: the caller runs the whole batch, waits for its
//! `ReadyForQuery`, and only then hands the link back to the client.

use std::collections::BTreeMap;

/// The parameters `PostgreSQL` reports with `GUC_REPORT` that a pooled session
/// is entitled to carry across a handoff.
///
/// `server_version`, `server_encoding`, `is_superuser` and the rest of the
/// `GUC_REPORT` set are deliberately absent: they are properties of the backend,
/// not of the session, and a client cannot change them.
pub const TRACKED: [&str; 6] = [
    "client_encoding",
    "DateStyle",
    "TimeZone",
    "standard_conforming_strings",
    "application_name",
    "IntervalStyle",
];

/// The parameters one process follows: [`TRACKED`], plus whatever its document
/// added.
///
/// Owned rather than borrowed from a `'static`, because two proxies in one test
/// binary follow two different sets and a leaked slice per process would make
/// the second one wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tracked {
    names: Vec<String>,
}

impl Default for Tracked {
    fn default() -> Self {
        Self {
            names: TRACKED.iter().map(|name| (*name).to_owned()).collect(),
        }
    }
}

/// A name `trackExtraParameters` may not add.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidParameterName(pub String);

impl std::fmt::Display for InvalidParameterName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} is not a parameter name: the cache builds `SET <name> = ...` from it, and \
             the name side of that statement is the one thing here that is never quoted",
            self.0
        )
    }
}

impl Tracked {
    /// The built-in set plus `extra`, or the first name that may not be added.
    ///
    /// A GUC name is a bare identifier, optionally qualified by an extension
    /// prefix. Anything else - a space, a quote, a semicolon, a comment marker -
    /// would be interpolated straight into the `SET` this module writes, which is
    /// the one place a value is not quoted and cannot be. Refused here, at
    /// start-up, rather than at the statement it would corrupt.
    pub fn with_extra(extra: &[String]) -> std::result::Result<Self, InvalidParameterName> {
        let mut tracked = Self::default();
        for name in extra {
            if !is_parameter_name(name) {
                return Err(InvalidParameterName(name.clone()));
            }
            if tracked.canonical(name).is_none() {
                tracked.names.push(name.clone());
            }
        }
        Ok(tracked)
    }

    /// The canonical spelling of a tracked parameter, or `None` if untracked.
    ///
    /// GUC names are case-insensitive, so `datestyle` and `DateStyle` are the same
    /// parameter and must not become two cache entries.
    #[must_use]
    pub fn canonical(&self, name: &str) -> Option<&str> {
        self.names
            .iter()
            .find(|known| known.eq_ignore_ascii_case(name))
            .map(String::as_str)
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.canonical(name).is_some()
    }
}

/// Whether `name` is one of the parameters the built-in set follows.
pub fn is_tracked(name: &str) -> bool {
    TRACKED.iter().any(|known| known.eq_ignore_ascii_case(name))
}

/// Whether `name` is shaped like a GUC an operator may ask to be followed.
///
/// A leading letter or underscore, then letters, digits, underscores and dollars,
/// with at most one dot for an extension prefix. Bounded at `PostgreSQL`'s own
/// `NAMEDATALEN - 1`.
fn is_parameter_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }
    let mut parts = name.split('.');
    let ok = |part: &str| {
        let mut chars = part.chars();
        chars
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    };
    let Some(first) = parts.next() else {
        return false;
    };
    if !ok(first) {
        return false;
    }
    match (parts.next(), parts.next()) {
        (None, _) => true,
        (Some(second), None) => ok(second),
        _ => false,
    }
}

/// One side's view of the tracked parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VariableCache {
    tracked: std::sync::Arc<Tracked>,
    values: BTreeMap<String, String>,
}

impl VariableCache {
    /// A cache over the built-in set, for the paths that have no document to hand.
    pub fn new() -> Self {
        Self::default()
    }

    /// A cache over the set this process follows.
    pub fn with_tracked(tracked: std::sync::Arc<Tracked>) -> Self {
        Self {
            tracked,
            values: BTreeMap::new(),
        }
    }

    /// Records a `ParameterStatus`, keeping the server's own spelling of the
    /// value. Untracked parameters are ignored rather than stored.
    pub fn observe(&mut self, name: &[u8], value: &[u8]) {
        let Ok(name) = std::str::from_utf8(name) else {
            return;
        };
        let Some(name) = self.tracked.canonical(name) else {
            return;
        };
        let name = name.to_owned();
        self.values
            .insert(name, String::from_utf8_lossy(value).into_owned());
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.tracked
            .canonical(name)
            .and_then(|name| self.values.get(name).map(String::as_str))
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

/// The statement that brings `server` in line with `client`, if they differ.
///
/// A parameter the client has never been told about is left alone: the client
/// cannot be relying on a value it has not seen, and forcing the backend to the
/// pool's first connection's value would be a change nobody asked for.
pub fn sync_statement(client: &VariableCache, server: &VariableCache) -> Option<String> {
    let mut statements = Vec::new();
    for (name, wanted) in client.iter() {
        if server.get(name) == Some(wanted) {
            continue;
        }
        statements.push(format!("SET {name} = {}", quote_literal(wanted)));
    }
    if statements.is_empty() {
        return None;
    }
    Some(statements.join("; "))
}

/// Quotes a value as a SQL string literal.
///
/// `standard_conforming_strings` is itself one of the tracked parameters, so a
/// backslash cannot be assumed to be literal. `E''` with both quote and
/// backslash doubled is unambiguous under either setting.
fn quote_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 4);
    out.push_str("E'");
    for c in value.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {

    /// The CVE the module header names. The cache writes `SET <name> = <quoted
    /// value>`, and the name is the one side of that statement nothing quotes -
    /// so an operator must not be able to put a semicolon, a quote or a comment
    /// marker into it, however they came by the document.
    #[test]
    fn a_name_that_is_not_an_identifier_is_refused() {
        for name in [
            "search_path; DROP TABLE users --",
            "a'b",
            "a\"b",
            "with space",
            "1leading_digit",
            "",
            "a.b.c",
            "-- comment",
            "a/*b*/c",
        ] {
            assert_eq!(
                Tracked::with_extra(&[name.to_owned()]),
                Err(InvalidParameterName(name.to_owned())),
                "{name:?} must not be trackable"
            );
        }
    }

    #[test]
    fn a_plain_guc_name_is_accepted_including_an_extension_prefix() {
        let tracked =
            Tracked::with_extra(&["search_path".to_owned(), "plpgsql.check_asserts".to_owned()])
                .unwrap();
        assert!(tracked.contains("search_path"));
        assert!(tracked.contains("PLPGSQL.CHECK_ASSERTS"));
        // And the built-in set is still there.
        assert!(tracked.contains("TimeZone"));
    }

    /// A name at `PostgreSQL`'s own `NAMEDATALEN` boundary, and one past it.
    #[test]
    fn a_name_longer_than_postgres_allows_is_refused() {
        assert!(Tracked::with_extra(&["a".repeat(63)]).is_ok());
        assert!(Tracked::with_extra(&["a".repeat(64)]).is_err());
    }

    /// Adding a name the built-in set already has must not make two cache entries
    /// for one parameter, whatever case the operator spelled it in.
    #[test]
    fn re_adding_a_built_in_name_does_not_duplicate_it() {
        let tracked = Tracked::with_extra(&["timezone".to_owned()]).unwrap();
        let mut cache = VariableCache::with_tracked(std::sync::Arc::new(tracked));
        cache.observe(b"TimeZone", b"UTC");
        cache.observe(b"timezone", b"Asia/Tokyo");
        assert_eq!(cache.iter().count(), 1);
        assert_eq!(cache.get("TimeZone"), Some("Asia/Tokyo"));
    }

    /// The point of the field: a client assigning a tracked GUC no longer leaves
    /// it on the link for whoever gets it next.
    #[test]
    fn an_extra_parameter_is_diffed_and_set_like_a_built_in_one() {
        let tracked =
            std::sync::Arc::new(Tracked::with_extra(&["search_path".to_owned()]).unwrap());
        let mut client = VariableCache::with_tracked(std::sync::Arc::clone(&tracked));
        client.observe(b"search_path", b"tenant_a");
        let mut server = VariableCache::with_tracked(tracked);
        server.observe(b"search_path", b"tenant_b");

        assert_eq!(
            sync_statement(&client, &server).as_deref(),
            Some("SET search_path = E'tenant_a'")
        );
    }

    /// And an untracked one is still invisible to the cache, so nothing widens by
    /// accident.
    #[test]
    fn a_parameter_nobody_asked_to_track_is_still_ignored() {
        let mut cache = VariableCache::new();
        cache.observe(b"search_path", b"tenant_a");
        assert!(cache.is_empty());
    }
    use super::*;

    fn cache(pairs: &[(&str, &str)]) -> VariableCache {
        let mut cache = VariableCache::new();
        for (name, value) in pairs {
            cache.observe(name.as_bytes(), value.as_bytes());
        }
        cache
    }

    #[test]
    fn a_parameter_name_is_matched_however_it_is_spelled() {
        let cache = cache(&[("datestyle", "ISO, MDY")]);
        assert_eq!(cache.get("DateStyle"), Some("ISO, MDY"));
        assert_eq!(cache.iter().count(), 1);
    }

    #[test]
    fn an_untracked_parameter_is_not_stored() {
        let cache = cache(&[("server_version", "18.1"), ("search_path", "audit")]);
        assert!(cache.is_empty());
    }

    #[test]
    fn matching_caches_need_no_statement() {
        let client = cache(&[("TimeZone", "UTC"), ("client_encoding", "UTF8")]);
        let server = cache(&[("client_encoding", "UTF8"), ("TimeZone", "UTC")]);
        assert_eq!(sync_statement(&client, &server), None);
    }

    #[test]
    fn a_differing_parameter_produces_one_set() {
        let client = cache(&[("TimeZone", "Europe/London")]);
        let server = cache(&[("TimeZone", "UTC")]);
        assert_eq!(
            sync_statement(&client, &server).unwrap(),
            "SET TimeZone = E'Europe/London'"
        );
    }

    #[test]
    fn a_parameter_the_server_has_never_reported_is_still_assigned() {
        let client = cache(&[("application_name", "reports")]);
        let server = VariableCache::new();
        assert_eq!(
            sync_statement(&client, &server).unwrap(),
            "SET application_name = E'reports'"
        );
    }

    #[test]
    fn a_parameter_only_the_server_knows_about_is_left_alone() {
        let client = VariableCache::new();
        let server = cache(&[("IntervalStyle", "postgres")]);
        assert_eq!(sync_statement(&client, &server), None);
    }

    #[test]
    fn every_generated_statement_names_only_a_parameter_from_the_closed_set() {
        let client = cache(&[
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO, DMY"),
            ("TimeZone", "UTC"),
            ("standard_conforming_strings", "off"),
            ("application_name", "x"),
            ("IntervalStyle", "sql_standard"),
        ]);
        let statement = sync_statement(&client, &VariableCache::new()).unwrap();
        for clause in statement.split("; ") {
            let name = clause
                .strip_prefix("SET ")
                .and_then(|rest| rest.split_once(" = "))
                .map(|(name, _)| name)
                .expect("every clause is a SET");
            assert!(TRACKED.contains(&name), "{name} is not a tracked parameter");
        }
    }

    /// The same invariant once the set is a document's rather than the const's.
    /// Closed per process is still closed, and it is what `with_extra` is for.
    #[test]
    fn a_configured_set_is_still_closed_over_the_names_it_was_given() {
        let names = ["search_path".to_owned(), "plpgsql.check_asserts".to_owned()];
        let tracked = std::sync::Arc::new(Tracked::with_extra(&names).unwrap());
        let mut client = VariableCache::with_tracked(std::sync::Arc::clone(&tracked));
        for (name, value) in [
            ("TimeZone", "UTC"),
            ("search_path", "a"),
            ("plpgsql.check_asserts", "on"),
            // Neither built in nor configured: it must not reach a statement.
            ("work_mem", "64MB"),
        ] {
            client.observe(name.as_bytes(), value.as_bytes());
        }

        let statement = sync_statement(&client, &VariableCache::with_tracked(tracked.clone()))
            .expect("three tracked parameters differ from an empty server cache");
        for clause in statement.split("; ") {
            let name = clause
                .strip_prefix("SET ")
                .and_then(|rest| rest.split_once(" = "))
                .map(|(name, _)| name)
                .expect("every clause is a SET");
            assert!(
                tracked.contains(name),
                "{name} reached a statement without being in the set"
            );
        }
        assert!(!statement.contains("work_mem"));
    }

    /// The injection shape: a value that closes the literal and appends a
    /// statement must come back out as data.
    #[test]
    fn a_hostile_value_cannot_escape_its_literal() {
        let client = cache(&[("application_name", "x'; DROP TABLE users; --")]);
        let statement = sync_statement(&client, &VariableCache::new()).unwrap();
        assert_eq!(
            statement,
            "SET application_name = E'x''; DROP TABLE users; --'"
        );
    }

    #[test]
    fn a_backslash_is_escaped_so_the_literal_survives_either_conforming_strings_setting() {
        let client = cache(&[("application_name", "back\\slash")]);
        assert_eq!(
            sync_statement(&client, &VariableCache::new()).unwrap(),
            "SET application_name = E'back\\\\slash'"
        );
    }

    #[test]
    fn the_tracked_set_is_exactly_the_six_reported_session_parameters() {
        assert!(is_tracked("client_encoding"));
        assert!(is_tracked("INTERVALSTYLE"));
        assert!(!is_tracked("search_path"));
        assert!(!is_tracked("server_version"));
    }
}
