//! Transcript extractor (#495).
//!
//! Background daemon spawned by `deskd serve`. For every agent in the
//! workspace it discovers Claude Code session JSONL files under
//! `{work_dir}/.claude/projects/**/*.jsonl`, tails them, and emits
//! structured [`TranscriptEvent`]s to:
//!
//! 1. The agent's bus on target `events:transcript`
//! 2. A daily-rotated append-only JSONL store at
//!    `~/.deskd/events/YYYY-MM-DD.jsonl` (Berlin date)
//!
//! Architecture (one task per agent):
//!
//! ```text
//! workspace.yaml ──▶ spawn_for_agent(agent)
//!                         │
//!                         ▼
//!                  ┌──────────────┐  notify::RecommendedWatcher (fsnotify)
//!                  │  poll loop   │ ◀────────────────────────────────────┐
//!                  │   1) rescan  │                                       │
//!                  │   2) tail    │     fs writes to .jsonl files ────────┘
//!                  └──────┬───────┘
//!                         │
//!                         ▼
//!                  parse_line(...) ───▶ EventStore.append() + bus.send_message()
//! ```
//!
//! ## No-backfill semantics
//!
//! When the extractor first sees a JSONL file (no prior cursor in
//! `~/.deskd/extractor-cursors.json`) it sets the cursor to the file's
//! current size — i.e. starts at EOF. Historical events stay where they
//! are; only new appended lines flow into the bus and event store.
//! This is an explicit owner decision: "we're starting fresh".

pub mod cursors;
pub mod event_store;
pub mod events;
pub mod parser;
pub mod patterns;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, info, warn};

use crate::infra::diag;

pub use cursors::{CursorStore, default_cursor_path};
pub use event_store::{EventStore, default_event_root};
pub use events::{LimitKind, TranscriptEvent};
pub use parser::SessionTracker;

/// How often to rescan for new JSONL files. The ticket requires new
/// agents to be picked up within 60s — we use 30s as a comfortable
/// margin so a file created at t=29s into the cycle is still observed
/// before the 60s boundary.
pub const RESCAN_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum interval between idle wakeups. `notify` pings short-circuit
/// the wait, so this only bounds the worst-case latency when fsnotify
/// drops events (rare but documented in `notify`'s readme).
pub const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Build the path each agent's session JSONL files live under. The
/// Claude Code on-disk convention is `~/.claude/projects/<flat-cwd>/`,
/// rooted at the user's home — for an agent with `work_dir = /home/X`,
/// that resolves to `{work_dir}/.claude/projects/`.
pub fn transcript_root_for(work_dir: &str) -> PathBuf {
    PathBuf::from(work_dir).join(".claude").join("projects")
}

/// Send target for transcript events. Any client subscribed to this
/// channel (or `events:*`) on the agent's bus receives the events.
pub const BUS_CHANNEL: &str = "events:transcript";

/// Source name the extractor identifies as on the bus.
pub const BUS_SOURCE: &str = "transcript-extractor";

/// Bundle of config + handles passed to every agent extractor.
#[derive(Clone)]
pub struct ExtractorContext {
    pub agent: String,
    pub bus_socket: String,
    pub transcript_root: PathBuf,
    pub cursors: CursorStore,
    pub events: EventStore,
}

impl ExtractorContext {
    pub fn new(
        agent: impl Into<String>,
        bus_socket: impl Into<String>,
        transcript_root: PathBuf,
        cursors: CursorStore,
        events: EventStore,
    ) -> Self {
        Self {
            agent: agent.into(),
            bus_socket: bus_socket.into(),
            transcript_root,
            cursors,
            events,
        }
    }
}

/// Spawn the transcript extractor for every agent in the workspace.
///
/// One tokio task per agent. The `cursors` and `events` stores are
/// shared across agents — they're internally synchronised. Returns
/// immediately after spawning.
pub fn spawn_for_workspace(
    agents: Vec<(String, String, String)>, // (name, work_dir, bus_socket)
    cursors: CursorStore,
    events: EventStore,
) {
    if agents.is_empty() {
        info!("transcript-extractor: no agents to watch, skipping");
        return;
    }
    info!(
        n = agents.len(),
        "transcript-extractor: spawning per-agent watchers"
    );
    for (name, work_dir, bus_socket) in agents {
        let ctx = ExtractorContext::new(
            &name,
            &bus_socket,
            transcript_root_for(&work_dir),
            cursors.clone(),
            events.clone(),
        );
        tokio::spawn(async move {
            if let Err(e) = run_agent_loop(ctx).await {
                diag::warn_event(
                    Some(&bus_socket),
                    "transcript-extractor",
                    "extractor.exited",
                    format!("transcript extractor exited: {}", e),
                    serde_json::json!({ "agent": name }),
                );
            }
        });
    }
}

/// Per-agent extractor loop. Discovers JSONL files under
/// `transcript_root`, watches them, tails new bytes, parses events,
/// publishes to bus + event store.
pub async fn run_agent_loop(ctx: ExtractorContext) -> Result<()> {
    // Ensure root exists so the notify watcher has something to attach
    // to. Missing root just means the agent hasn't started yet — we'll
    // re-check on every rescan.
    let _ = tokio::fs::create_dir_all(&ctx.transcript_root).await;

    // Channel to wake the loop on fsnotify events.
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<()>();
    let watcher = build_watcher(&ctx.transcript_root, notify_tx)?;
    // Held alive for the lifetime of the loop so the watcher keeps firing.
    let _watcher_guard = watcher;

    // Per-file session trackers — one per discovered JSONL file.
    let trackers: Arc<Mutex<HashMap<PathBuf, SessionTracker>>> =
        Arc::new(Mutex::new(HashMap::new()));

    info!(
        agent = %ctx.agent,
        root = %ctx.transcript_root.display(),
        "transcript-extractor: agent loop started"
    );

    let mut rescan_ticker = tokio::time::interval(RESCAN_INTERVAL);
    rescan_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick; do an initial scan synchronously
    // below so the first tail happens right after spawn.
    rescan_ticker.tick().await;

    // Initial discovery — register every file's cursor at EOF if it
    // isn't already known.
    let initial_files = discover_jsonl(&ctx.transcript_root).await;
    for f in initial_files {
        seed_cursor_at_eof(&ctx, &f).await;
        ensure_tracker(&trackers, &f).await;
    }
    // Process any bytes that may already be past the cursor for known
    // (previously-seen) files. Real new files are seeded at EOF so this
    // is a no-op for them.
    tail_all(&ctx, trackers.clone()).await;

    loop {
        tokio::select! {
            _ = rescan_ticker.tick() => {
                let files = discover_jsonl(&ctx.transcript_root).await;
                for f in files {
                    seed_cursor_at_eof(&ctx, &f).await;
                    ensure_tracker(&trackers, &f).await;
                }
                tail_all(&ctx, trackers.clone()).await;
            }
            _ = notify_rx.recv() => {
                // Drain any further pings queued up so we tail at most
                // once per burst.
                while notify_rx.try_recv().is_ok() {}
                tail_all(&ctx, trackers.clone()).await;
            }
            _ = tokio::time::sleep(POLL_INTERVAL) => {
                // Fallback poll — fsnotify can drop events on some
                // platforms; this guarantees forward progress.
                tail_all(&ctx, trackers.clone()).await;
            }
        }
    }
}

/// Build a `notify` watcher on `root` that forwards every event into
/// `tx`. The watcher is recursive so newly-created
/// `<flat-cwd>/<session>.jsonl` files in fresh project directories also
/// emit events.
fn build_watcher(root: &Path, tx: mpsc::UnboundedSender<()>) -> Result<RecommendedWatcher> {
    let mut watcher =
        notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
            Ok(_event) => {
                let _ = tx.send(());
            }
            Err(e) => {
                debug!(error = %e, "notify watcher error");
            }
        })
        .context("build notify watcher")?;

    // It's OK if the root doesn't exist yet; just log and keep going —
    // the periodic rescan will recover once it appears.
    if let Err(e) = watcher.watch(root, RecursiveMode::Recursive) {
        warn!(
            root = %root.display(),
            error = %e,
            "transcript-extractor: watcher attach failed, continuing with polling"
        );
    }
    Ok(watcher)
}

/// Walk `root` recursively looking for `.jsonl` files. Returns absolute
/// paths. Used both at startup and on every rescan tick.
pub async fn discover_jsonl(root: &Path) -> Vec<PathBuf> {
    // Use std::fs here — the directory tree is tiny (one session file
    // per running agent in practice) and tokio's fs::read_dir doesn't
    // recurse natively. Walk depth-first iteratively.
    let mut out = Vec::new();
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let read = match std::fs::read_dir(&dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for entry in read.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                out.push(p);
            }
        }
    }
    out
}

/// Seed the cursor for `path` to its current EOF if no cursor exists
/// yet. Honours the ticket's no-backfill rule: events that pre-date
/// first encounter never flow downstream.
async fn seed_cursor_at_eof(ctx: &ExtractorContext, path: &Path) {
    let key = path.to_string_lossy().into_owned();
    let existing = ctx.cursors.get(&key).await;
    if existing != 0 {
        return; // Already known.
    }
    // Even on first encounter we explicitly *re-record* the EOF cursor
    // so a restart (where the file existed before) doesn't start
    // streaming from byte 0. The `existing != 0` check above only skips
    // the work; the first-time path lands here.
    let size = match tokio::fs::metadata(path).await {
        Ok(m) => m.len(),
        Err(_) => 0,
    };
    if let Err(e) = ctx.cursors.set(&key, size).await {
        warn!(
            path = %path.display(),
            error = %e,
            "transcript-extractor: failed to seed cursor at EOF"
        );
    } else {
        debug!(
            path = %path.display(),
            cursor = size,
            "transcript-extractor: seeded cursor at EOF (no backfill)"
        );
    }
}

/// Ensure a [`SessionTracker`] entry exists for `path`.
async fn ensure_tracker(trackers: &Arc<Mutex<HashMap<PathBuf, SessionTracker>>>, path: &Path) {
    let mut g = trackers.lock().await;
    g.entry(path.to_path_buf())
        .or_insert_with(SessionTracker::new);
}

/// Tail every known file from its saved cursor to EOF. Each new line
/// is parsed; derived events are appended to the event store and
/// published to the bus.
async fn tail_all(ctx: &ExtractorContext, trackers: Arc<Mutex<HashMap<PathBuf, SessionTracker>>>) {
    // Snapshot the path set so we don't hold the lock across IO.
    let paths: Vec<PathBuf> = {
        let g = trackers.lock().await;
        g.keys().cloned().collect()
    };
    for path in paths {
        if let Err(e) = tail_one(ctx, &trackers, &path).await {
            debug!(
                path = %path.display(),
                error = %e,
                "transcript-extractor: tail iteration failed"
            );
        }
    }
}

/// Tail a single file from cursor → EOF. Updates the cursor after
/// every successful batch.
async fn tail_one(
    ctx: &ExtractorContext,
    trackers: &Arc<Mutex<HashMap<PathBuf, SessionTracker>>>,
    path: &Path,
) -> Result<()> {
    let key = path.to_string_lossy().into_owned();
    let cursor = ctx.cursors.get(&key).await;

    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let len = file.metadata().await?.len();

    if len == cursor {
        return Ok(()); // No new bytes.
    }
    if len < cursor {
        // File was truncated / rotated under us. Reset the cursor to
        // the new EOF to avoid replaying historical bytes. This is the
        // same no-backfill principle that applies to first encounter.
        warn!(
            path = %path.display(),
            old_cursor = cursor,
            new_len = len,
            "transcript-extractor: file shrank, resetting cursor to new EOF"
        );
        ctx.cursors.set(&key, len).await?;
        return Ok(());
    }

    file.seek(SeekFrom::Start(cursor)).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let mut new_cursor = cursor;
    let mut emitted: Vec<TranscriptEvent> = Vec::new();
    {
        let mut g = trackers.lock().await;
        let tracker = g
            .entry(path.to_path_buf())
            .or_insert_with(SessionTracker::new);
        for line in buf.split_inclusive('\n') {
            // Skip incomplete final line — wait for it to be flushed.
            if !line.ends_with('\n') {
                break;
            }
            new_cursor += line.len() as u64;
            let events = tracker.parse_line(&ctx.agent, line);
            emitted.extend(events);
        }
    }

    for ev in &emitted {
        if let Err(e) = ctx.events.append(ev).await {
            warn!(
                error = %e,
                kind = %ev.kind_label(),
                "transcript-extractor: event store append failed"
            );
        }
        if let Err(e) = publish_to_bus(&ctx.bus_socket, ev).await {
            // Bus may not be ready during early startup or in tests
            // that don't run a bus. Log and keep going — the event
            // store still has the record.
            debug!(
                error = %e,
                kind = %ev.kind_label(),
                "transcript-extractor: bus publish failed (event still in store)"
            );
        }
    }

    if new_cursor != cursor {
        ctx.cursors.set(&key, new_cursor).await?;
    }
    Ok(())
}

/// Publish a single event to the agent's bus on the `events:transcript`
/// channel. Best-effort: surface the error so callers can decide to log
/// vs retry.
pub async fn publish_to_bus(bus_socket: &str, event: &TranscriptEvent) -> Result<()> {
    let payload = serde_json::to_string(event).context("serialise transcript event for bus")?;
    crate::app::bus::send_message(bus_socket, BUS_SOURCE, BUS_CHANNEL, &payload).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn discover_jsonl_finds_recursive_files() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("-home-dev").join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("a.jsonl"), "{}\n").unwrap();
        std::fs::write(dir.path().join("b.jsonl"), "{}\n").unwrap();
        // Non-jsonl files are ignored.
        std::fs::write(dir.path().join("c.txt"), "hi").unwrap();

        let files = discover_jsonl(dir.path()).await;
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|p| p.ends_with("a.jsonl")));
        assert!(files.iter().any(|p| p.ends_with("b.jsonl")));
    }

    #[tokio::test]
    async fn discover_jsonl_missing_root_is_empty() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let files = discover_jsonl(&missing).await;
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn seed_cursor_records_eof_on_first_encounter() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("session.jsonl");
        std::fs::write(&file, "line1\nline2\n").unwrap();

        let cursor_path = dir.path().join("cursors.json");
        let cursors = CursorStore::open(cursor_path);
        let events = EventStore::new(dir.path().join("events"));
        let ctx = ExtractorContext::new(
            "dev",
            "/tmp/no-bus.sock",
            dir.path().to_path_buf(),
            cursors.clone(),
            events,
        );

        seed_cursor_at_eof(&ctx, &file).await;
        let key = file.to_string_lossy().into_owned();
        let recorded = cursors.get(&key).await;
        assert_eq!(recorded, 12, "cursor must be set to EOF (12 bytes)");
    }

    #[tokio::test]
    async fn seed_cursor_does_not_overwrite_existing() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("session.jsonl");
        std::fs::write(&file, "line1\nline2\n").unwrap();

        let cursor_path = dir.path().join("cursors.json");
        let cursors = CursorStore::open(cursor_path);
        let key = file.to_string_lossy().into_owned();
        cursors.set(&key, 5).await.unwrap();

        let events = EventStore::new(dir.path().join("events"));
        let ctx = ExtractorContext::new(
            "dev",
            "/tmp/no-bus.sock",
            dir.path().to_path_buf(),
            cursors.clone(),
            events,
        );
        seed_cursor_at_eof(&ctx, &file).await;
        assert_eq!(
            cursors.get(&key).await,
            5,
            "existing cursor must not be overwritten"
        );
    }

    fn assistant_jsonl_line(session: &str, model: &str, ts: &str) -> String {
        format!(
            r#"{{"type":"assistant","sessionId":"{}","timestamp":"{}","message":{{"role":"assistant","model":"{}","usage":{{"input_tokens":5,"output_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}}}}{}"#,
            session, ts, model, "\n"
        )
    }

    #[tokio::test]
    async fn tail_one_emits_session_start_for_new_writes() {
        // Seed a JSONL with an initial line, set cursor to EOF, then
        // append a new assistant line. tail_one must emit exactly one
        // session.start event (for the new line only — no backfill).
        let dir = tempdir().unwrap();
        let proj = dir.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();
        let file = proj.join("s1.jsonl");
        let initial = assistant_jsonl_line("S-OLD", "claude-opus-4-7", "2026-05-22T05:30:00Z");
        std::fs::write(&file, &initial).unwrap();

        let cursor_path = dir.path().join("cursors.json");
        let cursors = CursorStore::open(cursor_path);
        let key = file.to_string_lossy().into_owned();
        cursors.set(&key, initial.len() as u64).await.unwrap();

        let events_dir = dir.path().join("events");
        let events = EventStore::new(events_dir.clone());
        let ctx = ExtractorContext::new(
            "dev",
            "/tmp/no-bus.sock",
            proj.clone(),
            cursors.clone(),
            events,
        );
        let trackers: Arc<Mutex<HashMap<PathBuf, SessionTracker>>> =
            Arc::new(Mutex::new(HashMap::new()));
        ensure_tracker(&trackers, &file).await;

        // Append a new line.
        let new_line = assistant_jsonl_line("S-NEW", "claude-opus-4-7", "2026-05-22T05:31:00Z");
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap();
        f.write_all(new_line.as_bytes()).unwrap();
        drop(f);

        tail_one(&ctx, &trackers, &file).await.unwrap();

        // Cursor advanced to the new EOF.
        let final_len = std::fs::metadata(&file).unwrap().len();
        assert_eq!(cursors.get(&key).await, final_len);

        // Event store has exactly one session.start.
        let store_file = events_dir.join("2026-05-22.jsonl");
        let contents = std::fs::read_to_string(&store_file).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 1, "expected exactly one event line");
        let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(v["kind"], "session.start");
        assert_eq!(v["session_id"], "S-NEW");
    }

    #[tokio::test]
    async fn tail_one_no_double_emit_on_repeated_runs() {
        // tail_one twice without new writes must be idempotent — the
        // cursor blocks re-emission.
        let dir = tempdir().unwrap();
        let proj = dir.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();
        let file = proj.join("s1.jsonl");
        std::fs::write(&file, "").unwrap();

        let cursors = CursorStore::open(dir.path().join("cursors.json"));
        let key = file.to_string_lossy().into_owned();
        cursors.set(&key, 0).await.unwrap();

        let events_dir = dir.path().join("events");
        let events = EventStore::new(events_dir.clone());
        let ctx = ExtractorContext::new(
            "dev",
            "/tmp/no-bus.sock",
            proj.clone(),
            cursors.clone(),
            events,
        );
        let trackers: Arc<Mutex<HashMap<PathBuf, SessionTracker>>> =
            Arc::new(Mutex::new(HashMap::new()));
        ensure_tracker(&trackers, &file).await;

        let new_line = assistant_jsonl_line("S-1", "claude-opus-4-7", "2026-05-22T05:31:00Z");
        std::fs::write(&file, &new_line).unwrap();

        tail_one(&ctx, &trackers, &file).await.unwrap();
        let after_first = cursors.get(&key).await;
        tail_one(&ctx, &trackers, &file).await.unwrap();
        let after_second = cursors.get(&key).await;
        assert_eq!(after_first, after_second);

        let contents = std::fs::read_to_string(events_dir.join("2026-05-22.jsonl")).unwrap();
        assert_eq!(contents.lines().count(), 1, "must emit exactly once");
    }

    #[tokio::test]
    async fn tail_one_recovers_from_truncation() {
        // If a file is truncated below the cursor (rotation / rewrite),
        // we reset the cursor to the new EOF without replaying.
        let dir = tempdir().unwrap();
        let proj = dir.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();
        let file = proj.join("s1.jsonl");
        std::fs::write(&file, "AAAAAAAAAA").unwrap(); // 10 bytes

        let cursors = CursorStore::open(dir.path().join("cursors.json"));
        let key = file.to_string_lossy().into_owned();
        cursors.set(&key, 10).await.unwrap();

        // Truncate to 3 bytes.
        std::fs::write(&file, "BBB").unwrap();

        let events = EventStore::new(dir.path().join("events"));
        let ctx = ExtractorContext::new(
            "dev",
            "/tmp/no-bus.sock",
            proj.clone(),
            cursors.clone(),
            events,
        );
        let trackers: Arc<Mutex<HashMap<PathBuf, SessionTracker>>> =
            Arc::new(Mutex::new(HashMap::new()));
        ensure_tracker(&trackers, &file).await;

        tail_one(&ctx, &trackers, &file).await.unwrap();
        assert_eq!(cursors.get(&key).await, 3);
    }

    #[tokio::test]
    async fn tail_one_holds_back_partial_final_line() {
        // A file ending without `\n` means the writer hasn't flushed
        // the last record yet. We must NOT advance past that line.
        let dir = tempdir().unwrap();
        let proj = dir.path().join("proj");
        std::fs::create_dir_all(&proj).unwrap();
        let file = proj.join("s1.jsonl");
        // No trailing newline.
        let partial = r#"{"type":"assistant","sessionId":"S","timestamp":"2026-05-22T05:30:00Z","message":{"model":"m"}}"#;
        std::fs::write(&file, partial).unwrap();

        let cursors = CursorStore::open(dir.path().join("cursors.json"));
        let key = file.to_string_lossy().into_owned();
        cursors.set(&key, 0).await.unwrap();

        let events = EventStore::new(dir.path().join("events"));
        let ctx = ExtractorContext::new(
            "dev",
            "/tmp/no-bus.sock",
            proj.clone(),
            cursors.clone(),
            events,
        );
        let trackers: Arc<Mutex<HashMap<PathBuf, SessionTracker>>> =
            Arc::new(Mutex::new(HashMap::new()));
        ensure_tracker(&trackers, &file).await;

        tail_one(&ctx, &trackers, &file).await.unwrap();
        // Cursor must remain at 0 — we don't advance past a partial line.
        assert_eq!(cursors.get(&key).await, 0);
    }
}
