//! Daily-rotated event store (#495).
//!
//! Appends one [`TranscriptEvent`] per line to
//! `<root>/YYYY-MM-DD.jsonl` where the date is the event's `ts` field
//! interpreted in `Europe/Berlin`. Rotation is implicit: the writer
//! opens (and creates) the right file per event.
//!
//! The store is thread-safe via an internal tokio mutex — the daemon
//! spawns one writer task per watched file but only one [`EventStore`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Europe::Berlin;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use super::events::TranscriptEvent;

/// Default location for the event store: `~/.deskd/events/`.
pub fn default_event_root() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join(".deskd").join("events")
}

/// Append-only daily-rotated JSONL writer.
#[derive(Clone)]
pub struct EventStore {
    root: Arc<PathBuf>,
    /// Serialises concurrent appenders. Without this two tasks could
    /// interleave bytes within a JSON line.
    lock: Arc<Mutex<()>>,
}

impl EventStore {
    /// Create / open a store rooted at `root`. The directory is created
    /// lazily on first append.
    pub fn new(root: PathBuf) -> Self {
        Self {
            root: Arc::new(root),
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Append `event` to the file matching its Berlin date.
    pub async fn append(&self, event: &TranscriptEvent) -> Result<()> {
        let date_label = berlin_date_label(event.ts());
        let path = self.path_for_date(&date_label);
        let mut line =
            serde_json::to_string(event).context("serialise transcript event for store")?;
        line.push('\n');

        let _g = self.lock.lock().await;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create event store dir {}", parent.display()))?;
        }
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("open event store file {}", path.display()))?;
        f.write_all(line.as_bytes())
            .await
            .with_context(|| format!("write event store file {}", path.display()))?;
        // Force-flush so subsequent readers (panel queries, tests) see
        // the event without waiting for tokio's internal buffer to drop.
        f.flush()
            .await
            .with_context(|| format!("flush event store file {}", path.display()))?;
        Ok(())
    }

    /// Compute the path that would be used for a given pre-formatted
    /// `YYYY-MM-DD` label. Exposed for tests + day-boundary assertions.
    pub fn path_for_date(&self, date_label: &str) -> PathBuf {
        self.root.join(format!("{}.jsonl", date_label))
    }

    /// Path the event *would* be written to, for inspection / tests.
    pub fn path_for_event(&self, event: &TranscriptEvent) -> PathBuf {
        self.path_for_date(&berlin_date_label(event.ts()))
    }

    /// Root directory of the store. Used by tests / queries.
    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }
}

/// Convert an RFC 3339 timestamp into a `YYYY-MM-DD` Berlin-date label.
/// Falls back to `now()` in Berlin on parse failure (consistent with the
/// parser's `parse_ts`).
fn berlin_date_label(rfc3339: &str) -> String {
    let utc: DateTime<Utc> = DateTime::parse_from_rfc3339(rfc3339)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let berlin = Berlin.from_utc_datetime(&utc.naive_utc());
    berlin.format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn ev_start(ts: &str) -> TranscriptEvent {
        TranscriptEvent::SessionStart {
            ts: ts.to_string(),
            agent: "dev".to_string(),
            session_id: "S".to_string(),
            model: "claude-opus-4-7".to_string(),
        }
    }

    #[test]
    fn berlin_date_in_summer_offsets_by_two_hours() {
        // 2026-05-22 23:30 UTC is 01:30 next day in Berlin (CEST, UTC+2).
        let label = berlin_date_label("2026-05-22T23:30:00Z");
        assert_eq!(label, "2026-05-23");
    }

    #[test]
    fn berlin_date_in_winter_offsets_by_one_hour() {
        // 2026-01-15 23:30 UTC is 00:30 next day in Berlin (CET, UTC+1).
        let label = berlin_date_label("2026-01-15T23:30:00Z");
        assert_eq!(label, "2026-01-16");
    }

    #[test]
    fn berlin_date_same_day_midday_utc() {
        let label = berlin_date_label("2026-05-22T12:00:00Z");
        assert_eq!(label, "2026-05-22");
    }

    #[test]
    fn berlin_date_invalid_falls_back_to_today() {
        // We can't assert the exact value, but the call must not panic
        // and must return something parseable.
        let label = berlin_date_label("not-a-timestamp");
        assert_eq!(label.len(), 10, "expected YYYY-MM-DD shape");
    }

    #[tokio::test]
    async fn append_writes_to_correct_file() {
        let dir = tempdir().unwrap();
        let store = EventStore::new(dir.path().to_path_buf());
        store
            .append(&ev_start("2026-05-22T12:00:00Z"))
            .await
            .unwrap();

        let expected = dir.path().join("2026-05-22.jsonl");
        let contents = std::fs::read_to_string(&expected).unwrap();
        assert!(contents.contains("session.start"));
        assert!(contents.contains("\"agent\":\"dev\""));
        assert!(contents.ends_with('\n'));
    }

    #[tokio::test]
    async fn day_boundary_rotates_files() {
        let dir = tempdir().unwrap();
        let store = EventStore::new(dir.path().to_path_buf());
        // 23:30 UTC = next day in Berlin during DST.
        store
            .append(&ev_start("2026-05-22T23:30:00Z"))
            .await
            .unwrap();
        store
            .append(&ev_start("2026-05-22T12:00:00Z"))
            .await
            .unwrap();

        let file_a = dir.path().join("2026-05-22.jsonl");
        let file_b = dir.path().join("2026-05-23.jsonl");
        assert!(file_a.exists(), "midday-UTC event must land in 2026-05-22");
        assert!(file_b.exists(), "late-UTC event must land in 2026-05-23");

        let lines_a = std::fs::read_to_string(&file_a).unwrap();
        let lines_b = std::fs::read_to_string(&file_b).unwrap();
        assert_eq!(lines_a.lines().count(), 1);
        assert_eq!(lines_b.lines().count(), 1);
    }

    #[tokio::test]
    async fn append_is_serialised_under_concurrency() {
        // Spawn 20 tasks all appending to the same day file. With the
        // internal mutex no line interleaves.
        let dir = tempdir().unwrap();
        let store = EventStore::new(dir.path().to_path_buf());

        let mut handles = Vec::new();
        for i in 0..20 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                let ev = TranscriptEvent::SessionStart {
                    ts: "2026-05-22T12:00:00Z".to_string(),
                    agent: format!("agent-{}", i),
                    session_id: "S".to_string(),
                    model: "m".to_string(),
                };
                s.append(&ev).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let contents = std::fs::read_to_string(dir.path().join("2026-05-22.jsonl")).unwrap();
        // 20 well-formed lines.
        let n = contents.lines().count();
        assert_eq!(n, 20);
        for line in contents.lines() {
            let _: serde_json::Value =
                serde_json::from_str(line).expect("every line must be valid JSON");
        }
    }

    #[tokio::test]
    async fn path_for_event_matches_store_root() {
        let dir = tempdir().unwrap();
        let store = EventStore::new(dir.path().to_path_buf());
        let p = store.path_for_event(&ev_start("2026-05-22T12:00:00Z"));
        assert_eq!(p, dir.path().join("2026-05-22.jsonl"));
    }
}
