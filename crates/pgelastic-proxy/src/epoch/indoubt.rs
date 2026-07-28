//! The durable in-doubt log.
//!
//! A commit that was forwarded and whose `CommandComplete` never arrived has an
//! outcome nobody observed. The proxy must not report it as a failure, must not
//! report it as a success, and must not retry it. What it can do is write down
//! that it happened, durably enough to survive its own restart, so that an
//! operator reconciling the instance afterwards has the exact set of
//! transactions to look for.
//!
//! Append-only JSON lines with an `fsync` per record. Durability is the entire
//! purpose of the file: a record still sitting in the page cache when the pod
//! is killed is a record that never existed.

use std::collections::BTreeSet;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use super::Epoch;

/// The identity of one undecidable transaction.
///
/// `(tenant, epoch, backend pid, lsn)`, exactly as the design specifies. The
/// LSN is optional and is `None` when the proxy never observed one — recording
/// a placeholder would be inventing evidence, and this log exists precisely
/// because inventing an outcome is the failure mode being defended against.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InDoubtKey {
    pub tenant: String,
    pub epoch: u64,
    pub backend_pid: Option<i32>,
    pub lsn: Option<String>,
}

impl InDoubtKey {
    pub fn new(
        tenant: impl Into<String>,
        epoch: Epoch,
        backend_pid: Option<i32>,
        lsn: Option<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            epoch: epoch.get(),
            backend_pid,
            lsn,
        }
    }
}

impl std::fmt::Display for InDoubtKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant={} epoch={} pid={} lsn={}",
            self.tenant,
            self.epoch,
            self.backend_pid
                .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string()),
            self.lsn.as_deref().unwrap_or("unknown"),
        )
    }
}

/// One line of the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InDoubtRecord {
    #[serde(flatten)]
    pub key: InDoubtKey,
    /// Milliseconds since the Unix epoch. Wall clock, because the only consumer
    /// is a human correlating this against server logs.
    pub recorded_at_ms: u64,
    /// The statement whose outcome is unknown, as the client sent it. Bounded,
    /// because a log line is not a place to keep a megabyte of SQL.
    pub statement: String,
}

/// Longest statement text kept with a record.
const MAX_STATEMENT_BYTES: usize = 512;

/// Where records go and what has already been written.
#[derive(Debug)]
pub struct InDoubtLog {
    path: Option<PathBuf>,
    state: Mutex<State>,
    recorded: AtomicU64,
}

#[derive(Debug, Default)]
struct State {
    keys: BTreeSet<InDoubtKey>,
    records: Vec<InDoubtRecord>,
}

impl InDoubtLog {
    /// A log with no file behind it. Records are still counted and still
    /// exported as a metric; they do not survive a restart.
    pub fn in_memory() -> Arc<Self> {
        Arc::new(Self {
            path: None,
            state: Mutex::new(State::default()),
            recorded: AtomicU64::new(0),
        })
    }

    /// Opens `path`, replaying whatever a previous process left there.
    ///
    /// A line that cannot be parsed is kept in the file and skipped rather than
    /// truncating everything after it: half a log is worth more than none.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Arc<Self>> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut state = State::default();
        if path.exists() {
            let file = std::fs::File::open(&path)?;
            for (number, line) in BufReader::new(file).lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<InDoubtRecord>(&line) {
                    Ok(record) => {
                        state.keys.insert(record.key.clone());
                        state.records.push(record);
                    }
                    Err(error) => warn!(
                        %error,
                        line = number + 1,
                        "an in-doubt log line could not be parsed and was skipped"
                    ),
                }
            }
        }
        Ok(Arc::new(Self {
            path: Some(path),
            state: Mutex::new(state),
            recorded: AtomicU64::new(0),
        }))
    }

    /// Records one undecidable transaction and returns whether it was new.
    ///
    /// Idempotent on the key: replaying the same fence must not inflate the
    /// count an operator is reconciling against.
    pub fn record(&self, key: InDoubtKey, statement: &str) -> bool {
        let record = InDoubtRecord {
            key,
            recorded_at_ms: unix_millis(),
            statement: truncate(statement),
        };

        let mut state = self.lock();
        if !state.keys.insert(record.key.clone()) {
            return false;
        }
        state.records.push(record.clone());
        drop(state);

        self.recorded.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = self.append(&record) {
            // The record is still in memory and still exported as a metric.
            // Losing the file is bad; losing the fact that this happened
            // because writing the file failed would be worse.
            error!(%error, key = %record.key, "the in-doubt record could not be persisted");
        }
        true
    }

    pub fn len(&self) -> usize {
        self.lock().records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many records this process has written, for the metrics exposition.
    /// Distinct from [`InDoubtLog::len`], which includes those replayed from a
    /// previous process.
    pub fn recorded(&self) -> u64 {
        self.recorded.load(Ordering::Relaxed)
    }

    pub fn contains(&self, key: &InDoubtKey) -> bool {
        self.lock().keys.contains(key)
    }

    pub fn records(&self) -> Vec<InDoubtRecord> {
        self.lock().records.clone()
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn append(&self, record: &InDoubtRecord) -> std::io::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let mut line = serde_json::to_string(record)?;
        line.push('\n');
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        file.write_all(line.as_bytes())?;
        file.sync_data()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("the in-doubt log is never poisoned")
    }
}

fn truncate(statement: &str) -> String {
    if statement.len() <= MAX_STATEMENT_BYTES {
        return statement.to_owned();
    }
    let mut end = MAX_STATEMENT_BYTES;
    while end > 0 && !statement.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &statement[..end])
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(tenant: &str, epoch: u64, pid: i32) -> InDoubtKey {
        InDoubtKey::new(
            tenant,
            Epoch::new(epoch),
            Some(pid),
            Some("0/16B3748".to_owned()),
        )
    }

    #[test]
    fn a_record_is_keyed_by_tenant_epoch_pid_and_lsn() {
        let log = InDoubtLog::in_memory();
        assert!(log.record(key("acme", 7, 4242), "COMMIT"));
        assert!(log.contains(&key("acme", 7, 4242)));
        assert!(!log.contains(&key("acme", 8, 4242)));
        assert!(!log.contains(&key("other", 7, 4242)));
    }

    #[test]
    fn recording_the_same_transaction_twice_does_not_inflate_the_count() {
        let log = InDoubtLog::in_memory();
        assert!(log.record(key("acme", 7, 4242), "COMMIT"));
        assert!(!log.record(key("acme", 7, 4242), "COMMIT"));
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn an_unobserved_lsn_is_recorded_as_unknown_rather_than_invented() {
        let log = InDoubtLog::in_memory();
        let key = InDoubtKey::new("acme", Epoch::new(3), None, None);
        log.record(key.clone(), "COMMIT");
        assert!(log.contains(&key));
        assert_eq!(log.records()[0].key.lsn, None);
        assert!(key.to_string().contains("lsn=unknown"));
        assert!(key.to_string().contains("pid=unknown"));
    }

    #[test]
    fn the_log_survives_a_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("state").join("in-doubt.jsonl");

        let first = InDoubtLog::open(&path).unwrap();
        first.record(key("acme", 7, 4242), "COMMIT");
        first.record(key("acme", 8, 4243), "COMMIT");
        drop(first);

        let reopened = InDoubtLog::open(&path).unwrap();
        assert_eq!(reopened.len(), 2);
        assert!(reopened.contains(&key("acme", 7, 4242)));
        assert!(reopened.contains(&key("acme", 8, 4243)));
        // Replayed records are not counted as recorded by this process.
        assert_eq!(reopened.recorded(), 0);
    }

    #[test]
    fn a_reopened_log_still_deduplicates_against_what_it_replayed() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("in-doubt.jsonl");
        let first = InDoubtLog::open(&path).unwrap();
        first.record(key("acme", 7, 4242), "COMMIT");
        drop(first);

        let reopened = InDoubtLog::open(&path).unwrap();
        assert!(!reopened.record(key("acme", 7, 4242), "COMMIT"));
        assert_eq!(reopened.len(), 1);
    }

    #[test]
    fn an_unparseable_line_is_skipped_and_the_rest_of_the_log_survives() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("in-doubt.jsonl");
        let good = serde_json::to_string(&InDoubtRecord {
            key: key("acme", 7, 4242),
            recorded_at_ms: 1,
            statement: "COMMIT".to_owned(),
        })
        .unwrap();
        std::fs::write(&path, format!("{{ not json\n{good}\n")).unwrap();

        let log = InDoubtLog::open(&path).unwrap();
        assert_eq!(log.len(), 1);
        assert!(log.contains(&key("acme", 7, 4242)));
    }

    #[test]
    fn a_long_statement_is_truncated_rather_than_kept_whole() {
        let log = InDoubtLog::in_memory();
        log.record(key("acme", 1, 1), &"x".repeat(4096));
        assert!(log.records()[0].statement.len() <= MAX_STATEMENT_BYTES + 4);
    }

    #[test]
    fn an_in_memory_log_has_no_file_behind_it() {
        let log = InDoubtLog::in_memory();
        assert!(log.path().is_none());
        assert!(log.is_empty());
        log.record(key("acme", 1, 1), "COMMIT");
        assert_eq!(log.recorded(), 1);
    }
}
