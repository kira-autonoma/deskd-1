//! Per-agent shared reload state used by `deskd reload` (#474).
//!
//! Owns the cross-task signalling and observability surface for the
//! hot-reload pipeline:
//!   * `Notify` — fired by `bus_api::reload_config` to wake the config
//!     reload watcher immediately, bypassing its 30s mtime poll.
//!   * `last_reload_at` / `last_reload_error` — captured on every reload
//!     attempt so `deskd bus status` can surface "did my edit take effect?"
//!     without scraping logs.
//!
//! There is one `ReloadState` per agent, created in `serve.rs` and shared
//! between `bus_api::run` and `config_reload::watch_and_reload`.
//!
//! The state lives entirely in memory — restarting `deskd serve` clears it.
//! That is intentional: the operator-visible `last_reload_at` only matters
//! within a single serve lifetime.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, Notify};

/// Shared per-agent reload state — see module docs.
#[derive(Debug, Default)]
pub struct ReloadStateInner {
    /// Timestamp of the most recent successful reload.
    pub last_reload_at: Option<DateTime<Utc>>,
    /// Error message from the most recent failed reload attempt.
    /// Cleared on the next successful reload.
    pub last_reload_error: Option<String>,
}

/// Cloneable handle to the shared reload state.
///
/// Internally an `Arc` over a `Mutex` + `Notify`. Cheap to clone and pass
/// between tasks.
#[derive(Clone, Debug, Default)]
pub struct ReloadState {
    inner: Arc<Mutex<ReloadStateInner>>,
    notify: Arc<Notify>,
}

impl ReloadState {
    /// Create a fresh, never-reloaded state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal the watcher to perform a reload immediately. Idempotent —
    /// repeated calls before the watcher wakes up coalesce into one reload.
    pub fn trigger(&self) {
        self.notify.notify_one();
    }

    /// Wait for the next reload trigger. Resolves once `trigger` is called.
    pub async fn wait_for_trigger(&self) {
        self.notify.notified().await;
    }

    /// Record a successful reload. Clears any prior error.
    pub async fn record_success(&self, ts: DateTime<Utc>) {
        let mut guard = self.inner.lock().await;
        guard.last_reload_at = Some(ts);
        guard.last_reload_error = None;
    }

    /// Record a failed reload attempt. Does NOT touch `last_reload_at` —
    /// the previous successful timestamp remains visible.
    pub async fn record_failure(&self, err: impl Into<String>) {
        let mut guard = self.inner.lock().await;
        guard.last_reload_error = Some(err.into());
    }

    /// Snapshot the current state for read-only consumers (`bus status`).
    pub async fn snapshot(&self) -> ReloadStateInner {
        let guard = self.inner.lock().await;
        ReloadStateInner {
            last_reload_at: guard.last_reload_at,
            last_reload_error: guard.last_reload_error.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snapshot_starts_empty() {
        let state = ReloadState::new();
        let snap = state.snapshot().await;
        assert!(snap.last_reload_at.is_none());
        assert!(snap.last_reload_error.is_none());
    }

    #[tokio::test]
    async fn record_success_sets_timestamp_clears_error() {
        let state = ReloadState::new();
        state.record_failure("boom").await;
        let snap = state.snapshot().await;
        assert_eq!(snap.last_reload_error.as_deref(), Some("boom"));
        assert!(snap.last_reload_at.is_none());

        let ts = Utc::now();
        state.record_success(ts).await;
        let snap = state.snapshot().await;
        assert_eq!(snap.last_reload_at, Some(ts));
        assert!(
            snap.last_reload_error.is_none(),
            "record_success must clear last error"
        );
    }

    #[tokio::test]
    async fn record_failure_preserves_last_reload_at() {
        let state = ReloadState::new();
        let ts = Utc::now();
        state.record_success(ts).await;
        state.record_failure("yaml parse failed").await;
        let snap = state.snapshot().await;
        assert_eq!(snap.last_reload_at, Some(ts));
        assert_eq!(snap.last_reload_error.as_deref(), Some("yaml parse failed"));
    }

    #[tokio::test]
    async fn trigger_wakes_waiter() {
        let state = ReloadState::new();
        let cloned = state.clone();
        let handle = tokio::spawn(async move {
            cloned.wait_for_trigger().await;
        });
        // Give the spawned task a chance to register the waiter.
        tokio::task::yield_now().await;
        state.trigger();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle)
            .await
            .expect("waiter should resolve after trigger")
            .expect("waiter task panicked");
    }
}
