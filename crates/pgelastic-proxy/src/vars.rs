//! The `GUC_REPORT`ed variable cache.
//!
//! A backend that has just been handed to a different client is still carrying
//! the previous client's reported settings. The client, meanwhile, has been told
//! whatever the last `ParameterStatus` it saw said. Diffing the two on every
//! assignment and emitting the `SET`s that close the gap — then holding the
//! client until they have flushed — is what makes a handoff invisible.
//!
//! **CVE-2025-12819 is the reason the name side of every generated statement is
//! a closed set.** [`TRACKED`] is the only source of a name that can appear in a
//! statement this module builds, and the value is always a quoted literal, so no
//! client-supplied text is ever resolved as an identifier or through a
//! client-influenced `search_path`. Nothing here runs on a connection that is
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

/// Whether `name` is one of the parameters the cache follows.
///
/// GUC names are case-insensitive, so `datestyle` and `DateStyle` are the same
/// parameter and must not become two cache entries.
pub fn is_tracked(name: &str) -> bool {
    TRACKED.iter().any(|known| known.eq_ignore_ascii_case(name))
}

/// The canonical spelling of a tracked parameter, or `None` if untracked.
fn canonical(name: &str) -> Option<&'static str> {
    TRACKED
        .iter()
        .copied()
        .find(|known| known.eq_ignore_ascii_case(name))
}

/// One side's view of the tracked parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VariableCache {
    values: BTreeMap<&'static str, String>,
}

impl VariableCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a `ParameterStatus`, keeping the server's own spelling of the
    /// value. Untracked parameters are ignored rather than stored.
    pub fn observe(&mut self, name: &[u8], value: &[u8]) {
        let Ok(name) = std::str::from_utf8(name) else {
            return;
        };
        let Some(name) = canonical(name) else { return };
        self.values
            .insert(name, String::from_utf8_lossy(value).into_owned());
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        canonical(name).and_then(|name| self.values.get(name).map(String::as_str))
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.values
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
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
                .unwrap();
            assert!(TRACKED.contains(&name), "{name} is not a tracked parameter");
        }
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
