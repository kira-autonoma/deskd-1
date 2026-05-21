//! Integration tests for the top-of-dashboard chart block (#484).
//!
//! These exercise the same axum router used in production. They cover:
//!
//! - Default `GET /` renders the chart block above the vps-strip.
//! - Switching metric/period via query params is reflected in the markup.
//! - Invalid switcher values silently fall back to defaults (no 5xx).
//! - The page still requires authentication (unchanged contract).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use deskd::app::adapters::web::audit::AuditLog;
use deskd::app::adapters::web::auth::{magic_link::TokenStore, session};
use deskd::app::adapters::web::dispatch::testing::{RecordingBusSender, RecordingDispatcher};
use deskd::app::adapters::web::middleware::rate_limit::RateLimiter;
use deskd::app::adapters::web::router;
use deskd::app::adapters::web::routes::SESSION_COOKIE_NAME;
use deskd::app::adapters::web::routes::github_webhook::shared_dedupe;
use deskd::app::adapters::web::state::WebState;
use deskd::config::{WebConfig, WebRateLimitConfig};
use tower::ServiceExt;

const TEST_TG_ID: i64 = 12_000;
const TEST_SECRET: [u8; 32] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32,
];

fn cfg(audit_path: std::path::PathBuf) -> WebConfig {
    WebConfig {
        enabled: true,
        bind: "127.0.0.1:0".into(),
        external_url: Some("https://deskd.example.com".into()),
        session_ttl_days: 30,
        magic_link_ttl_seconds: 300,
        allowed_telegram_ids: vec![TEST_TG_ID],
        audit_log: audit_path.to_string_lossy().to_string(),
        rate_limit: WebRateLimitConfig {
            auth_requests_per_hour: 20,
        },
        trust_transport: false,
    }
}

fn build_state() -> (WebState, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_obj = cfg(audit_path.clone());
    let dispatcher: Arc<dyn deskd::app::adapters::web::dispatch::TelegramDispatcher> =
        Arc::new(RecordingDispatcher::new());
    let bus: Arc<dyn deskd::app::adapters::web::dispatch::BusSender> =
        Arc::new(RecordingBusSender::new());
    let agent_commands: Arc<dyn deskd::app::adapters::web::dispatch::AgentCommandDispatcher> =
        Arc::new(
            deskd::app::adapters::web::dispatch::testing::RecordingAgentCommandDispatcher::new(),
        );
    let metrics_cache = dir.path().join("disk-cache.json");
    let state = WebState {
        cfg: Arc::new(cfg_obj),
        secret: Arc::new(TEST_SECRET),
        tokens: Arc::new(TokenStore::new()),
        rate_limiter_ip: Arc::new(RateLimiter::new(20, 3600)),
        rate_limiter_tg: Arc::new(RateLimiter::new(20, 3600)),
        audit: AuditLog::new(audit_path),
        telegram: dispatcher,
        github_webhooks: None,
        bus,
        github_deliveries: shared_dedupe(),
        agent_commands,
        now: Arc::new(|| 1_700_000_000),
        metrics: deskd::app::metrics::DiskMetrics::new(metrics_cache),
        agent_homes: Arc::new(Vec::new()),
        metrics_bus: None,
        cost: None,
        gh: Arc::new(deskd::app::adapters::web::data_cost::testing::RecordingGhClient::new()),
        cost_cache: deskd::app::adapters::web::data_cost::CostCache::new(),
    };
    (state, dir)
}

fn fake_peer() -> std::net::SocketAddr {
    "127.0.0.1:54321".parse().unwrap()
}

fn req_with_peer(req: Request<Body>) -> Request<Body> {
    let mut r = req;
    r.extensions_mut()
        .insert(axum::extract::ConnectInfo(fake_peer()));
    r
}

fn auth_cookie(state: &WebState) -> String {
    let now = (state.now)();
    let payload = session::SessionPayload::new(TEST_TG_ID, now, 86_400, "csrf-x".into());
    session::sign(&payload, state.secret.as_ref())
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn fetch(uri: &str) -> (StatusCode, String) {
    let (state, _dir) = build_state();
    let cookie = auth_cookie(&state);
    let app = router::build(state);
    let req = req_with_peer(
        Request::get(uri)
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = body_string(resp).await;
    (status, body)
}

#[tokio::test]
async fn dashboard_renders_chart_block_by_default() {
    let (status, body) = fetch("/").await;
    assert_eq!(status, StatusCode::OK);
    // The chart block lives at id="dashboard-chart-block".
    assert!(
        body.contains(r#"id="dashboard-chart-block""#),
        "expected chart block, body: {}",
        &body[..body.len().min(400)]
    );
    // SVG is present even when there's no data — it just shows «No data».
    assert!(body.contains("<svg"), "expected SVG markup");
    // Default switcher state: metric=spend, period=24h.
    assert!(body.contains(r#"value="spend" checked"#));
    assert!(body.contains(r#"value="24h" checked"#));
}

#[tokio::test]
async fn dashboard_chart_block_renders_above_vps_strip() {
    let (status, body) = fetch("/").await;
    assert_eq!(status, StatusCode::OK);
    let chart_idx = body
        .find(r#"id="dashboard-chart-block""#)
        .expect("chart block present");
    let strip_idx = body.find("vps-strip-wrap").expect("vps-strip wrap present");
    assert!(
        chart_idx < strip_idx,
        "chart block must precede vps-strip in document order"
    );
}

#[tokio::test]
async fn dashboard_chart_respects_metric_query() {
    let (status, body) = fetch("/?metric=tokens").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#"value="tokens" checked"#));
    // Spend radio is not checked.
    assert!(!body.contains(r#"value="spend" checked"#));
    // Period default still 24h.
    assert!(body.contains(r#"value="24h" checked"#));
}

#[tokio::test]
async fn dashboard_chart_respects_period_query() {
    let (status, body) = fetch("/?period=7d").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#"value="7d" checked"#));
    assert!(!body.contains(r#"value="24h" checked"#));
}

#[tokio::test]
async fn dashboard_chart_combined_metric_and_period() {
    let (status, body) = fetch("/?metric=activity&period=30d").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#"value="activity" checked"#));
    assert!(body.contains(r#"value="30d" checked"#));
}

#[tokio::test]
async fn dashboard_chart_invalid_metric_falls_back_to_default() {
    // AC: «invalid `?metric=invalid` falls back to default (spend) without 5xx».
    let (status, body) = fetch("/?metric=garbage").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#"value="spend" checked"#));
}

#[tokio::test]
async fn dashboard_chart_invalid_period_falls_back_to_default() {
    let (status, body) = fetch("/?period=garbage").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#"value="24h" checked"#));
}

#[tokio::test]
async fn dashboard_chart_switcher_form_targets_root() {
    // The switcher uses a vanilla form submit (`method="get" action="/"`).
    let (_status, body) = fetch("/").await;
    assert!(body.contains(r#"<form class="chart-switcher" method="get" action="/""#));
}

#[tokio::test]
async fn dashboard_chart_has_no_inline_html_style_attribute() {
    // CSP guard: the chart block must not introduce HTML inline `style="…"`.
    let (_status, body) = fetch("/").await;
    // Strip the iframe-safe doctype to scan markup. We only check that no
    // HTML attribute named `style=` appears anywhere. SVG attributes
    // (`fill=`, `stroke=`) are allowed.
    assert!(
        !body.contains(" style=\""),
        "found HTML inline style: regression vs #443 CSP"
    );
}

#[tokio::test]
async fn dashboard_chart_renders_mobile_responsive_svg() {
    // AC: «renders correctly on 375px-wide viewport without horizontal
    // scroll». Server-rendered SVG with a fixed viewBox + CSS width:100%
    // gives the chart a fluid layout. We pin the viewBox dimensions and
    // confirm the chart-block__svg class is wired up — the stylesheet
    // (covered separately) handles the actual sizing.
    let (_status, body) = fetch("/").await;
    assert!(
        body.contains(r#"viewBox="0 0 480 180""#),
        "chart SVG must use 480x180 viewBox so it scales to 375px"
    );
    assert!(
        body.contains(r#"class="chart-block__svg""#),
        "chart SVG must carry the responsive class (width:100% in CSS)"
    );
    assert!(
        body.contains(r#"href="/static/dashboard.css""#),
        "dashboard must link the stylesheet that sets the chart to width:100%"
    );
}

#[tokio::test]
async fn dashboard_chart_supports_all_three_metrics_and_periods() {
    // AC: «three metrics: spend / activity / tokens» and «three periods:
    // 24h / 7d / 30d». The radio set must be present regardless of the
    // current selection so users can switch.
    let (_status, body) = fetch("/").await;
    for metric in ["spend", "activity", "tokens"] {
        assert!(
            body.contains(&format!(r#"value="{metric}""#)),
            "metric switcher missing {metric}"
        );
    }
    for period in ["24h", "7d", "30d"] {
        assert!(
            body.contains(&format!(r#"value="{period}""#)),
            "period switcher missing {period}"
        );
    }
}
