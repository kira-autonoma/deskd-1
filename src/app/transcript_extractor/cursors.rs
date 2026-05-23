//! Per-file byte cursor persistence (#495).
//!
//! On every emitted batch the extractor records the byte offset it has
//! consumed for each watched file at
//! `~/.deskd/extractor-cursors.json`. On daemon restart the cursors are
//! reloaded so we never re-emit transcript events.
//!
//! The store is a flat `{ path -> bytes }` map. Atomic-on-rename writes
//! protect against torn files if the daemon crashes mid-flush.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::Mutex;

/// Default location for the cursor store: `~/.deskd/extractor-cursors.json`.
pub fn default_cursor_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home)
        .join(".deskd")
        .join("extractor-cursors.json")
}

/// Shared mutable cursor store. Wraps `CursorState` in a tokio mutex
/// because the extractor spawns one task per watched file and they all
/// need to update the same store.
#[derive(Clone)]
pub struct CursorStore {
    inner: Arc<Mutex<CursorState>>,
}

struct CursorState {
    path: PathBuf,
    cursors: HashMap<String, u64>,
}

impl CursorStore {
    /// Open the cursor store at `path`, loading existing state if any.
    /// Missing or unparseable files start with an empty map (logged as a
    /// warning) — we'd rather lose cursors than refuse to start.
    pub fn open(path: PathBuf) -> Self {
        let cursors = match std::fs::read_to_string(&path) {
            Ok(s) => serde_json::from_str::<HashMap<String, u64>>(&s).unwrap_or_else(|e| {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "extractor-cursors.json unparseable, starting fresh"
                );
                HashMap::new()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "failed to read cursor store, starting fresh"
                );
                HashMap::new()
            }
        };
        Self {
            inner: Arc::new(Mutex::new(CursorState { path, cursors })),
        }
    }

    /// Get the cursor for `file_path`, or 0 if none recorded.
    pub async fn get(&self, file_path: &str) -> u64 {
        let g = self.inner.lock().await;
        g.cursors.get(file_path).copied().unwrap_or(0)
    }

    /// Record a new cursor for `file_path` and flush to disk.
    pub async fn set(&self, file_path: &str, byte_offset: u64) -> Result<()> {
        let mut g = self.inner.lock().await;
        g.cursors.insert(file_path.to_string(), byte_offset);
        flush_to_disk(&g.path, &g.cursors)
    }

    /// Number of recorded cursors. Used by tests.
    #[cfg(test)]
    pub async fn len(&self) -> usize {
        self.inner.lock().await.cursors.len()
    }

    /// True if no cursors are recorded. Companion to [`len`] (clippy).
    #[cfg(test)]
    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.cursors.is_empty()
    }

    /// Snapshot of all cursors. Used by tests.
    #[cfg(test)]
    pub async fn snapshot(&self) -> HashMap<String, u64> {
        self.inner.lock().await.cursors.clone()
    }
}

/// Write the cursor map to disk via temp-file + atomic rename so a
/// mid-flush crash can never corrupt the JSON.
fn flush_to_disk(path: &Path, cursors: &HashMap<String, u64>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create cursor parent dir {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = std::fs::File::create(&tmp)
            .with_context(|| format!("create cursor tmp {}", tmp.display()))?;
        let serialised = serde_json::to_string(cursors).context("serialise cursors")?;
        f.write_all(serialised.as_bytes())
            .with_context(|| format!("write cursor tmp {}", tmp.display()))?;
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename cursor tmp → {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn round_trips_through_disk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cursors.json");

        let store = CursorStore::open(path.clone());
        store.set("/tmp/a.jsonl", 42).await.unwrap();
        store.set("/tmp/b.jsonl", 1024).await.unwrap();

        // Open a fresh store — must re-read the same cursors.
        let reloaded = CursorStore::open(path.clone());
        assert_eq!(reloaded.get("/tmp/a.jsonl").await, 42);
        assert_eq!(reloaded.get("/tmp/b.jsonl").await, 1024);
        assert_eq!(reloaded.get("/tmp/missing.jsonl").await, 0);
        assert_eq!(reloaded.len().await, 2);
    }

    #[tokio::test]
    async fn missing_file_starts_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");
        let store = CursorStore::open(path);
        assert_eq!(store.len().await, 0);
        assert_eq!(store.get("/tmp/x.jsonl").await, 0);
    }

    #[tokio::test]
    async fn corrupt_file_starts_empty_without_panic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cursors.json");
        std::fs::write(&path, "not-json").unwrap();

        let store = CursorStore::open(path);
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn updates_overwrite_existing_cursor() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cursors.json");
        let store = CursorStore::open(path.clone());

        store.set("/tmp/x.jsonl", 10).await.unwrap();
        store.set("/tmp/x.jsonl", 50).await.unwrap();

        let snap = store.snapshot().await;
        assert_eq!(snap.get("/tmp/x.jsonl").copied(), Some(50));
        // Verify on disk too.
        let raw = std::fs::read_to_string(&path).unwrap();
        let parsed: HashMap<String, u64> = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed.get("/tmp/x.jsonl").copied(), Some(50));
    }
}
