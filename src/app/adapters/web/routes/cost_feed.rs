//! `GET /api/cost/feed` — Server-Sent Events stream for the cost dashboard
//! (#473).
//!
//! Auth-gated identically to `/events`: a valid `deskd_session` cookie is
//! required. When `cost:` is not configured the route returns `404`.
//!
//! ### Event types
//!
//! - `cost-body` (named after the htmx `sse-swap="cost-body"` attribute on
//!   the dashboard page) — re-renders the full dashboard body when a ticket
//!   opens or closes. The full-body swap is acceptable: the dashboard is
//!   small, and operators rarely keep more than one tab open.
//!
//! ### Pacing
//!
//! The SSE stream polls `collect_cost_data` at a configurable interval
//! (default 30s). Diff detection compares the previous `CostData` snapshot
//! against the new one and only emits when something actually changed. The
//! 60-second `CostCache` (see `data_cost::CACHE_TTL`) ensures repeat polls
//! don't pound `gh`.
//!
//! ### Keep-alive
//!
//! 15-second keep-alive comments via `axum::response::sse::KeepAlive`
//! mirror `/events` so reverse proxies don't reap idle connections.

use std::convert::Infallible;
use std::pin::Pin;
use std::time::{Duration, Instant};

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{
        IntoResponse, Redirect, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::stream::Stream;
use serde::Deserialize;

use crate::app::adapters::web::auth::session;
use crate::app::adapters::web::data_cost::{self, CostData, lookup_actual_tokens};
use crate::app::adapters::web::routes::read_session_cookie;
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::view::cost as view_cost;

/// Default poll interval (30s). Picked to align with the `gh` cache TTL
/// (60s): two polls per cache window guarantees the stream catches a
/// freshly-closed ticket within ~30s.
const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Default keep-alive interval — mirrors `/events`.
const DEFAULT_KEEPALIVE: Duration = Duration::from_secs(15);

/// Query string options for `/api/cost/feed`. All fields are test-only.
#[derive(Debug, Default, Deserialize)]
pub struct FeedQuery {
    /// Cap the number of ticks emitted by this stream (test-only).
    #[serde(default)]
    pub max_ticks: u64,
    /// Override poll interval in milliseconds (test-only).
    #[serde(default)]
    pub tick_ms: u64,
    /// Override keep-alive interval in milliseconds (test-only).
    #[serde(default)]
    pub keepalive_ms: u64,
}

/// `GET /api/cost/feed` — SSE channel that pushes a diff event whenever
/// the cost dashboard data changes.
pub async fn feed(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(q): Query<FeedQuery>,
) -> Response {
    let now = (state.now)();
    let cookie = read_session_cookie(&headers);
    if cookie
        .as_deref()
        .and_then(|c| session::verify(c, state.secret.as_ref(), now))
        .is_none()
    {
        return Redirect::to("/login").into_response();
    }
    if state.cost.is_none() {
        return (StatusCode::NOT_FOUND, "cost feed not configured").into_response();
    }

    let tick = if q.tick_ms > 0 {
        Duration::from_millis(q.tick_ms)
    } else {
        DEFAULT_TICK_INTERVAL
    };
    let keepalive = if q.keepalive_ms > 0 {
        Duration::from_millis(q.keepalive_ms)
    } else {
        DEFAULT_KEEPALIVE
    };

    let stream = build_feed_stream(state, tick, q.max_ticks);
    let sse = Sse::new(stream).keep_alive(KeepAlive::new().interval(keepalive));
    let mut resp = sse.into_response();
    if let Ok(v) = "no-cache".parse() {
        resp.headers_mut().insert(header::CACHE_CONTROL, v);
    }
    if let Ok(v) = "keep-alive".parse() {
        resp.headers_mut().insert(header::CONNECTION, v);
    }
    resp
}

struct FeedState {
    web: WebState,
    prev: Option<CostData>,
    tick_count: u64,
    tick_interval: Duration,
    max_ticks: u64,
    initial_sent: bool,
}

/// Build the SSE stream that drives `/api/cost/feed`. Public so unit tests
/// can exercise the diff logic without spinning up an axum server.
pub fn build_feed_stream(
    web: WebState,
    tick_interval: Duration,
    max_ticks: u64,
) -> Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>> {
    let state = FeedState {
        web,
        prev: None,
        tick_count: 0,
        tick_interval,
        max_ticks,
        initial_sent: false,
    };
    let stream = futures::stream::unfold(state, |mut state| async move {
        if !state.initial_sent {
            state.initial_sent = true;
            return Some((Ok(Event::default().comment("connected")), state));
        }
        if state.max_ticks > 0 && state.tick_count >= state.max_ticks {
            return None;
        }
        if state.tick_count > 0 {
            tokio::time::sleep(state.tick_interval).await;
        }
        state.tick_count += 1;

        let Some(cfg) = state.web.cost.clone() else {
            // Config evaporated — terminate the stream rather than hang.
            return None;
        };
        let data = data_cost::collect_cost_data(
            cfg.as_ref(),
            state.web.gh.as_ref(),
            &state.web.cost_cache,
            Instant::now(),
            &lookup_actual_tokens,
        )
        .await;

        let changed = match &state.prev {
            None => true,
            Some(prev) => !is_equivalent(prev, &data),
        };
        let ev = if changed {
            let body_html = view_cost::cost_body(&data, cfg.as_ref());
            Event::default().event("cost-body").data(body_html)
        } else {
            Event::default().comment("tick")
        };
        state.prev = Some(data);
        Some((Ok(ev), state))
    });
    Box::pin(stream)
}

/// Diff two `CostData` snapshots while ignoring the `fetched_at` timestamp
/// (which advances every tick and would otherwise produce a spurious change
/// on every poll). Two snapshots count as «equivalent» when pipeline,
/// history, and weekly total all match.
pub fn is_equivalent(a: &CostData, b: &CostData) -> bool {
    a.pipeline == b.pipeline
        && a.history == b.history
        && a.weekly_total_tokens == b.weekly_total_tokens
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::adapters::web::audit::AuditLog;
    use crate::app::adapters::web::auth::magic_link::TokenStore;
    use crate::app::adapters::web::data_cost::testing::RecordingGhClient;
    use crate::app::adapters::web::data_cost::{CostCache, GhClient, GhIssue};
    use crate::app::adapters::web::dispatch::testing::{
        RecordingAgentCommandDispatcher, RecordingBusSender, RecordingDispatcher,
    };
    use crate::app::adapters::web::middleware::rate_limit::RateLimiter;
    use crate::app::adapters::web::routes::SESSION_COOKIE_NAME;
    use crate::app::adapters::web::routes::github_webhook::shared_dedupe;
    use crate::config::{CostBuckets, CostConfig, WebConfig, WebRateLimitConfig};
    use axum::body::Body;
    use axum::http::Request;
    use chrono::Utc;
    use futures::StreamExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    const TEST_TG_ID: i64 = 11_000;
    const TEST_SECRET: [u8; 32] = [7; 32];

    fn cfg() -> WebConfig {
        WebConfig {
            enabled: true,
            bind: "127.0.0.1:0".into(),
            external_url: Some("https://deskd.example.com".into()),
            session_ttl_days: 30,
            magic_link_ttl_seconds: 300,
            allowed_telegram_ids: vec![TEST_TG_ID],
            audit_log: "/tmp/cost-feed-audit.jsonl".into(),
            rate_limit: WebRateLimitConfig {
                auth_requests_per_hour: 20,
            },
            trust_transport: false,
        }
    }

    fn cost_cfg() -> CostConfig {
        CostConfig {
            buckets: CostBuckets {
                s: 10_000,
                m: 50_000,
                l: 200_000,
                xl: 500_000,
            },
            weekly_ceiling: None,
            history_days: 7,
            repos: vec!["a/b".into()],
            ready_label: "agent-ready".into(),
        }
    }

    fn issue(number: u64, title: &str, labels: &[&str]) -> GhIssue {
        GhIssue {
            number,
            title: title.into(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            created_at: Some(Utc::now()),
            closed_at: None,
        }
    }

    fn build_state_with(cost: Option<CostConfig>, gh: Arc<dyn GhClient>) -> WebState {
        let dir = tempfile::tempdir().unwrap();
        let audit_path = dir.path().join("audit.jsonl");
        WebState {
            cfg: Arc::new(cfg()),
            secret: Arc::new(TEST_SECRET),
            tokens: Arc::new(TokenStore::new()),
            rate_limiter_ip: Arc::new(RateLimiter::new(20, 3600)),
            rate_limiter_tg: Arc::new(RateLimiter::new(20, 3600)),
            audit: AuditLog::new(audit_path),
            telegram: Arc::new(RecordingDispatcher::new()),
            github_webhooks: None,
            bus: Arc::new(RecordingBusSender::new()),
            github_deliveries: shared_dedupe(),
            agent_commands: Arc::new(RecordingAgentCommandDispatcher::new()),
            now: Arc::new(|| 1_700_000_000),
            metrics: crate::app::metrics::DiskMetrics::new(dir.path().join("disk-cache.json")),
            agent_homes: Arc::new(Vec::new()),
            metrics_bus: None,
            cost: cost.map(Arc::new),
            gh,
            cost_cache: CostCache::new(),
        }
    }

    fn auth_cookie(state: &WebState) -> String {
        let now = (state.now)();
        let payload = session::SessionPayload::new(TEST_TG_ID, now, 86_400, "csrf-x".into());
        session::sign(&payload, state.secret.as_ref())
    }

    #[tokio::test]
    async fn redirects_unauthenticated_to_login() {
        let gh: Arc<dyn GhClient> = Arc::new(RecordingGhClient::new());
        let state = build_state_with(Some(cost_cfg()), gh);
        let app = crate::app::adapters::web::router::build(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/cost/feed")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    }

    #[tokio::test]
    async fn returns_404_when_cost_not_configured() {
        let gh: Arc<dyn GhClient> = Arc::new(RecordingGhClient::new());
        let state = build_state_with(None, gh);
        let cookie = auth_cookie(&state);
        let app = crate::app::adapters::web::router::build(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/cost/feed")
                    .header(
                        header::COOKIE,
                        format!("{}={}", SESSION_COOKIE_NAME, cookie),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn build_feed_stream_emits_initial_event_when_authenticated() {
        let gh: Arc<dyn GhClient> = Arc::new(
            RecordingGhClient::new()
                .with_open("a/b", vec![issue(1, "fresh", &["est:S", "agent-ready"])]),
        );
        let state = build_state_with(Some(cost_cfg()), gh);
        let mut stream = build_feed_stream(state, Duration::from_millis(10), 1);
        let mut events = 0;
        let timeout = tokio::time::sleep(Duration::from_secs(2));
        tokio::pin!(timeout);
        loop {
            tokio::select! {
                next = stream.next() => {
                    if next.is_none() { break; }
                    events += 1;
                }
                _ = &mut timeout => panic!("stream did not terminate in 2s"),
            }
        }
        // At least 2: initial connect comment + one tick.
        assert!(events >= 2, "expected ≥2 events, got {}", events);
    }

    #[test]
    fn is_equivalent_ignores_fetched_at() {
        let mut a = CostData {
            pipeline: vec![],
            history: vec![],
            weekly_total_tokens: 0,
            fetched_at: Utc::now(),
        };
        let mut b = a.clone();
        b.fetched_at = a.fetched_at + chrono::Duration::seconds(10);
        assert!(is_equivalent(&a, &b));
        // Add a pipeline ticket → no longer equivalent.
        a.weekly_total_tokens = 1;
        assert!(!is_equivalent(&a, &b));
    }
}
