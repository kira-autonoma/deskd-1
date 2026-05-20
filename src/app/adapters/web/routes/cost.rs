//! `GET /dashboard/cost` — cost & pipeline dashboard (#473).
//!
//! Auth-gated identically to the agent dashboard: a valid `deskd_session`
//! cookie is required, otherwise the handler redirects to `/login`.
//!
//! When `cost:` is absent from `workspace.yaml` the route returns `404`
//! rather than redirecting — the AC says the feature is opt-in and 404 is
//! the clearest signal that the operator hasn't configured it.

use std::time::Instant;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};

use crate::app::adapters::web::auth::session;
use crate::app::adapters::web::data_cost::{self, lookup_actual_tokens};
use crate::app::adapters::web::routes::read_session_cookie;
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::templates;
use crate::app::adapters::web::view::cost as view_cost;

/// `GET /dashboard/cost` — server-rendered HTML dashboard.
pub async fn dashboard(State(state): State<WebState>, headers: HeaderMap) -> Response {
    let now = (state.now)();
    let cookie = read_session_cookie(&headers);
    let payload = cookie.and_then(|c| session::verify(&c, state.secret.as_ref(), now));
    let payload = match payload {
        Some(p) => p,
        None => return Redirect::to("/login").into_response(),
    };

    let cfg = match state.cost.as_ref() {
        Some(c) => c.clone(),
        None => return (StatusCode::NOT_FOUND, "cost dashboard not configured").into_response(),
    };

    let data = data_cost::collect_cost_data(
        cfg.as_ref(),
        state.gh.as_ref(),
        &state.cost_cache,
        Instant::now(),
        &lookup_actual_tokens,
    )
    .await;

    let body_html = view_cost::cost_body(&data, cfg.as_ref());
    let html = templates::cost_page(payload.telegram_id, &payload.csrf, &body_html);
    html_response(html)
}

fn html_response(body: String) -> Response {
    let mut resp = (StatusCode::OK, body).into_response();
    if let Ok(v) = "text/html; charset=utf-8".parse() {
        resp.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    resp
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
            audit_log: "/tmp/cost-test-audit.jsonl".into(),
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
            weekly_ceiling: Some(1_000_000),
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

    async fn body_string(resp: Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn redirects_unauthenticated_to_login() {
        let gh: Arc<dyn GhClient> = Arc::new(RecordingGhClient::new());
        let state = build_state_with(Some(cost_cfg()), gh);
        let app = crate::app::adapters::web::router::build(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard/cost")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let location = resp
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(location, "/login");
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
                    .uri("/dashboard/cost")
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
    async fn renders_pipeline_history_and_budget_when_authed() {
        let gh: Arc<dyn GhClient> = Arc::new(
            RecordingGhClient::new()
                .with_open("a/b", vec![issue(1, "small", &["est:S", "agent-ready"])])
                .with_closed(
                    "a/b",
                    vec![{
                        let mut closed = issue(2, "closed", &["est:M", "agent-ready"]);
                        closed.closed_at = Some(Utc::now() - chrono::Duration::hours(2));
                        closed
                    }],
                ),
        );
        let state = build_state_with(Some(cost_cfg()), gh);
        let cookie = auth_cookie(&state);
        let app = crate::app::adapters::web::router::build(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard/cost")
                    .header(
                        header::COOKIE,
                        format!("{}={}", SESSION_COOKIE_NAME, cookie),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp).await;
        assert!(body.contains("Pipeline"));
        assert!(body.contains("History"));
        assert!(body.contains("Weekly budget"));
        assert!(body.contains("Estimate accuracy"));
        // Pipeline ticket title
        assert!(body.contains("small"));
        // History ticket placeholder when actual missing
        assert!(body.contains("tracking pending"));
        // SSE wiring
        assert!(body.contains("/api/cost/feed"));
    }
}
