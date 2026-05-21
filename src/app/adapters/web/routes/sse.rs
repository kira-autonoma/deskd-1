//! `GET /events` — Server-Sent Events stream for live dashboard updates (#444).
//!
//! Authenticated identically to `/`: a valid `deskd_session` cookie is
//! required. Browsers connect via htmx's `sse` extension; the page-level
//! `<section sse-connect="/events">` wrapper drives `EventSource`.
//!
//! ### Event types
//!
//! - `agent.status` — fired when an agent's coarse status changes
//!   (`idle`/`working`/`offline`).
//! - `agent.context` — fired when the live token count crosses a 1k bucket
//!   boundary (avoid spam from minor turn-over-turn drift).
//! - `agent.compacted` — fired when the live tokens drop sharply between
//!   ticks (a compaction's signature).
//! - `htmx-swap` (named after the htmx convention via the extension's `swap`
//!   support) — every change also pushes the rerendered card HTML so htmx
//!   can replace the DOM node directly.
//!
//! ### Pacing
//!
//! Per AC: «only structural changes; coalesce within 1s windows.» We poll
//! every `tick_interval` (default 1s) and emit at most one event per agent
//! per tick. Smaller token deltas are intentionally dropped to keep the
//! stream quiet on a healthy system.
//!
//! ### Keep-alive
//!
//! `axum::response::sse::KeepAlive` injects a comment line every 15s so
//! reverse proxies (caddy/nginx) don't reap idle connections.

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, header},
    response::{IntoResponse, Redirect, Response, sse::Event, sse::KeepAlive, sse::Sse},
};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};

use crate::app::adapters::web::data::{self, AgentSummary};
use crate::app::adapters::web::routes::authenticate;
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::view::{agent_card, agent_card_id, vps_strip};
use crate::app::metrics::DiskMetrics;

/// Default polling interval (1s, per AC).
const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(1_000);

/// Default keep-alive interval. Reverse proxies typically reap > 30s; 15s
/// is comfortably under every default we care about (caddy 2 min, nginx 60s
/// for proxy_read_timeout, cloudflare 100s).
const DEFAULT_KEEPALIVE: Duration = Duration::from_secs(15);

/// Token-delta threshold below which a context update is suppressed. Picks
/// up genuine changes (a single turn typically pulls in 5–20k cached
/// tokens) without spamming on tiny drifts.
const TOKEN_BUCKET: u64 = 1_000;

/// When live tokens drop by at least this many between ticks, treat it as
/// a compaction. Auto-compact triggers around the 80–90% mark, so any
/// drop > 50k is almost certainly a compaction event.
const COMPACTION_DROP_THRESHOLD: u64 = 50_000;

/// JSON payload for an `agent.status` SSE event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatusEvent {
    pub agent: String,
    pub status: String,
    pub task: String,
}

/// JSON payload for an `agent.context` SSE event.
///
/// `limit` is the model's hard context window (the bar denominator per
/// #483). `threshold` is the soft auto-compact trigger, rendered as a
/// secondary marker by the client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextEvent {
    pub agent: String,
    pub tokens: u64,
    pub threshold: Option<u64>,
    pub limit: Option<u64>,
}

/// JSON payload for an `agent.compacted` SSE event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompactedEvent {
    pub agent: String,
    pub before: u64,
    pub after: u64,
    pub reason: String,
}

/// Query string options for `/events`. All fields are test-only.
#[derive(Debug, Default, Deserialize)]
pub struct EventsQuery {
    /// Cap the number of ticks emitted by this stream. `0` (the default)
    /// means "stream until disconnect". Used by integration tests to drain
    /// a finite number of ticks via `oneshot`.
    #[serde(default)]
    pub max_ticks: u64,
    /// Override the polling interval in milliseconds (test-only).
    #[serde(default)]
    pub tick_ms: u64,
    /// Override the keep-alive interval in milliseconds (test-only).
    #[serde(default)]
    pub keepalive_ms: u64,
}

/// `GET /events` — authenticated SSE endpoint. Unauthenticated callers
/// receive a 302 to `/login` (matches the rest of the dashboard surface).
pub async fn events(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(q): Query<EventsQuery>,
) -> Response {
    if authenticate(&state, &headers).is_none() {
        return Redirect::to("/login").into_response();
    }

    let tick_interval = if q.tick_ms > 0 {
        Duration::from_millis(q.tick_ms)
    } else {
        DEFAULT_TICK_INTERVAL
    };
    let keepalive = if q.keepalive_ms > 0 {
        Duration::from_millis(q.keepalive_ms)
    } else {
        DEFAULT_KEEPALIVE
    };

    let stream = build_event_stream(tick_interval, q.max_ticks, Some(state.metrics.clone()));
    let sse = Sse::new(stream).keep_alive(KeepAlive::new().interval(keepalive));
    let mut resp = sse.into_response();
    // SSE-specific headers — axum sets content-type, but reverse proxies
    // sometimes buffer responses without these hints.
    if let Ok(v) = "no-cache".parse() {
        resp.headers_mut().insert(header::CACHE_CONTROL, v);
    }
    if let Ok(v) = "keep-alive".parse() {
        resp.headers_mut().insert(header::CONNECTION, v);
    }
    resp
}

/// Driver state for [`build_event_stream`]. Public so tests can construct
/// custom drivers (e.g. one that pulls from a `tokio::sync::mpsc` rather
/// than the registry).
pub struct StreamState {
    pub prev: HashMap<String, AgentSummary>,
    pub tick_count: u64,
    pub queued: std::collections::VecDeque<Event>,
    pub initial_sent: bool,
    pub tick_interval: Duration,
    pub max_ticks: u64,
    /// Disk-metrics handle (#446). When present, the stream watches for
    /// snapshot timestamp changes and emits a `metrics.updated` event +
    /// swaps the VPS strip HTML.
    pub metrics: Option<DiskMetrics>,
    /// Last seen disk snapshot `updated_at` — used to detect whether the
    /// collector has produced a fresh sample since the previous tick.
    pub last_metrics_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl StreamState {
    fn new(tick_interval: Duration, max_ticks: u64, metrics: Option<DiskMetrics>) -> Self {
        Self {
            prev: HashMap::new(),
            tick_count: 0,
            queued: std::collections::VecDeque::new(),
            initial_sent: false,
            tick_interval,
            max_ticks,
            metrics,
            last_metrics_at: None,
        }
    }
}

/// Build the SSE stream that drives `/events`. Public for unit tests.
pub fn build_event_stream(
    tick_interval: Duration,
    max_ticks: u64,
    metrics: Option<DiskMetrics>,
) -> Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> {
    let state = StreamState::new(tick_interval, max_ticks, metrics);
    let stream = futures::stream::unfold(state, |mut state| async move {
        if !state.initial_sent {
            state.initial_sent = true;
            return Some((Ok(Event::default().comment("connected")), state));
        }
        // Drain any queued events from the most recent diff before ticking
        // again so all of a single tick's events arrive back-to-back.
        if let Some(ev) = state.queued.pop_front() {
            return Some((Ok(ev), state));
        }
        if state.max_ticks > 0 && state.tick_count >= state.max_ticks {
            return None;
        }
        // Sleep before each tick except the first so we honour the pacing
        // requirement (1s coalescing window).
        if state.tick_count > 0 {
            tokio::time::sleep(state.tick_interval).await;
        }
        state.tick_count += 1;

        // Disk metrics: read the current snapshot so summaries are
        // hydrated with home_dir_bytes. Also detect timestamp advances
        // and emit a `metrics.updated` event + VPS strip swap.
        let disk = if let Some(m) = &state.metrics {
            Some(m.snapshot().await)
        } else {
            None
        };
        if let Some(snap) = &disk {
            let new_at = snap.updated_at;
            if new_at != state.last_metrics_at {
                state.last_metrics_at = new_at;
                if state.last_metrics_at.is_some() {
                    if let Ok(ev) = Event::default().event("metrics.updated").json_data(snap) {
                        state.queued.push_back(ev);
                    }
                    // Re-render the VPS strip and push an htmx swap so the
                    // browser refreshes the top-of-page numbers without
                    // a polling loop.
                    let overview = data::collect_vps_overview(Some(snap));
                    let strip_html = vps_strip(&overview);
                    let ev = Event::default().event("vps-strip").data(strip_html);
                    state.queued.push_back(ev);
                }
            }
        }

        let summaries = data::collect_agent_summaries(disk.as_ref()).await;
        let events = diff_to_events(&state.prev, &summaries);
        state.queued.extend(events);
        state.prev = summaries.into_iter().map(|s| (s.name.clone(), s)).collect();

        // If this tick produced no events, recurse implicitly by emitting a
        // keep-alive comment. Without that the stream would stall waiting for
        // the *next* loop iteration when no agents change.
        let next = state
            .queued
            .pop_front()
            .unwrap_or_else(|| Event::default().comment("tick"));
        Some((Ok(next), state))
    });
    Box::pin(stream)
}

/// Emit one or more SSE `Event`s for the changes between two snapshots.
///
/// Pacing rules:
/// - Status transitions always emit `agent.status`.
/// - Context-token deltas emit `agent.context` only when the bucket
///   (`tokens / TOKEN_BUCKET`) changes — i.e. a 1k boundary was crossed.
/// - A token drop of at least [`COMPACTION_DROP_THRESHOLD`] additionally
///   emits `agent.compacted`.
/// - Any change also emits an htmx-swap event carrying the rerendered card
///   HTML, so the browser can replace the DOM node by id without polling.
pub fn diff_to_events(prev: &HashMap<String, AgentSummary>, curr: &[AgentSummary]) -> Vec<Event> {
    let mut out = Vec::new();
    for s in curr {
        let was = prev.get(&s.name);
        let mut changed = false;

        // Newly observed agents always trigger a swap.
        if was.is_none() {
            changed = true;
        }

        // Status transitions.
        if let Some(p) = was
            && p.status != s.status
        {
            let payload = StatusEvent {
                agent: s.name.clone(),
                status: s.status.clone(),
                task: s.current_task.clone().unwrap_or_default(),
            };
            if let Ok(ev) = Event::default().event("agent.status").json_data(&payload) {
                out.push(ev);
                changed = true;
            }
        }

        // Token bucket transitions + compaction detection.
        if let Some(tokens) = s.context_tokens {
            let prev_tokens = was.and_then(|p| p.context_tokens);
            let bucket = tokens / TOKEN_BUCKET;
            let prev_bucket = prev_tokens.map(|t| t / TOKEN_BUCKET);
            if Some(bucket) != prev_bucket {
                let payload = ContextEvent {
                    agent: s.name.clone(),
                    tokens,
                    threshold: s.context_threshold,
                    limit: s.context_limit,
                };
                if let Ok(ev) = Event::default().event("agent.context").json_data(&payload) {
                    out.push(ev);
                    changed = true;
                }
                if let Some(p) = prev_tokens
                    && p.saturating_sub(tokens) >= COMPACTION_DROP_THRESHOLD
                {
                    let cpayload = CompactedEvent {
                        agent: s.name.clone(),
                        before: p,
                        after: tokens,
                        reason: "threshold".into(),
                    };
                    if let Ok(ev) = Event::default()
                        .event("agent.compacted")
                        .json_data(&cpayload)
                    {
                        out.push(ev);
                        // No need to set changed again — already true.
                    }
                }
            }
        }

        if changed {
            // htmx SSE extension supports `event: <id>` + element-id swap via
            // `sse-swap="<event-name>"`. We use the agent's stable card id as
            // the event name so the browser-side swap is a one-line attribute
            // (`sse-swap="agent-<name>"`).
            let card_html = agent_card(s);
            let card_id = agent_card_id(&s.name);
            let ev = Event::default()
                .event(card_id.clone())
                .data(card_html)
                .id(format!("{}.{}", card_id, s.status));
            out.push(ev);
        }
    }

    // Removed agents — emit a status="removed" event so the UI can drop the
    // card. The browser then strips it via a small inline handler.
    for (name, p) in prev.iter() {
        if !curr.iter().any(|s| &s.name == name) {
            let payload = StatusEvent {
                agent: name.clone(),
                status: "removed".to_string(),
                task: p.current_task.clone().unwrap_or_default(),
            };
            if let Ok(ev) = Event::default().event("agent.status").json_data(&payload) {
                out.push(ev);
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn summary(name: &str, status: &str, tokens: Option<u64>) -> AgentSummary {
        AgentSummary {
            name: name.into(),
            status: status.into(),
            model: "claude-opus-4-7".into(),
            last_activity: None,
            context_tokens: tokens,
            context_threshold: Some(300_000),
            context_limit: Some(1_000_000),
            home_dir_bytes: None,
            current_task: None,
            task_running_for: None,
        }
    }

    fn map(s: AgentSummary) -> HashMap<String, AgentSummary> {
        let mut m = HashMap::new();
        m.insert(s.name.clone(), s);
        m
    }

    #[test]
    fn status_transition_emits_agent_status_event() {
        let prev = map(summary("a", "idle", None));
        let curr = vec![summary("a", "working", None)];
        let events = diff_to_events(&prev, &curr);
        let names: Vec<String> = events.iter().map(|e| format!("{:?}", e)).collect();
        // Hard to introspect Event internals — assert at least 2 events
        // (status change + card swap).
        assert!(
            events.len() >= 2,
            "expected at least 2 events, got {:?}",
            names
        );
    }

    #[test]
    fn no_change_emits_nothing() {
        let prev = map(summary("a", "idle", Some(100_000)));
        let curr = vec![summary("a", "idle", Some(100_000))];
        let events = diff_to_events(&prev, &curr);
        assert!(events.is_empty());
    }

    #[test]
    fn small_token_change_below_bucket_is_coalesced() {
        let prev = map(summary("a", "idle", Some(100_000)));
        // 100_500 still falls in bucket 100 → no event.
        let curr = vec![summary("a", "idle", Some(100_500))];
        let events = diff_to_events(&prev, &curr);
        assert!(events.is_empty());
    }

    #[test]
    fn token_change_crossing_bucket_emits_context_event() {
        let prev = map(summary("a", "idle", Some(100_000)));
        // Bucket 100 → 102. Crosses 1k boundary.
        let curr = vec![summary("a", "idle", Some(102_500))];
        let events = diff_to_events(&prev, &curr);
        // At least 2: agent.context + card swap.
        assert!(events.len() >= 2);
    }

    #[test]
    fn large_token_drop_emits_compacted_event() {
        let prev = map(summary("a", "idle", Some(280_000)));
        let curr = vec![summary("a", "idle", Some(48_000))];
        let events = diff_to_events(&prev, &curr);
        // context + compacted + card swap = at least 3.
        assert!(
            events.len() >= 3,
            "expected ≥3 events for compaction, got {}",
            events.len()
        );
    }

    #[test]
    fn newly_seen_agent_triggers_card_swap() {
        let prev = HashMap::new();
        let curr = vec![summary("a", "idle", Some(50_000))];
        let events = diff_to_events(&prev, &curr);
        assert!(!events.is_empty());
    }

    #[test]
    fn removed_agent_emits_removed_status() {
        let prev = map(summary("a", "idle", None));
        let curr: Vec<AgentSummary> = vec![];
        let events = diff_to_events(&prev, &curr);
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn build_event_stream_yields_finite_count_when_max_ticks_set() {
        let mut s = build_event_stream(Duration::from_millis(10), 2, None);
        let mut count = 0;
        let timeout = tokio::time::sleep(Duration::from_secs(2));
        tokio::pin!(timeout);
        loop {
            tokio::select! {
                next = s.next() => {
                    if next.is_none() { break; }
                    count += 1;
                }
                _ = &mut timeout => panic!("stream did not terminate within 2s"),
            }
        }
        // At minimum the connect comment fires; ticks 0 and 1 may yield
        // zero diff events but the comment is always present.
        assert!(count >= 1, "expected ≥1 event, got {}", count);
    }
}
