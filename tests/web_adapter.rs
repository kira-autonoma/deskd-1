//! Integration tests for the web adapter (#443).
//!
//! Drives the axum router in-process via `tower::ServiceExt::oneshot`, with a
//! recording Telegram dispatcher in place of the bus path. Covers:
//!   - healthz unauthenticated 200
//!   - dashboard redirect when unauthenticated
//!   - dashboard 200 when authenticated
//!   - login_request happy path + audit + dispatch
//!   - login_request rate-limited (429)
//!   - login_request whitelist rejection
//!   - login_request CSRF missing → 403
//!   - login_consume happy path + Set-Cookie
//!   - login_consume single-use
//!   - login_consume expired token
//!   - logout clears cookie
//!   - HSTS / CSP / X-Frame-Options present on every response
//!   - tampered cookie treated as unauthenticated
//!   - web.enabled: false → adapter not started (config dispatch test)

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use deskd::app::adapters::web::audit::AuditLog;
use deskd::app::adapters::web::auth::{magic_link::TokenStore, session};
use deskd::app::adapters::web::dispatch::testing::{RecordingBusSender, RecordingDispatcher};
use deskd::app::adapters::web::middleware::headers::{CSP_VALUE, HSTS_VALUE};
use deskd::app::adapters::web::middleware::rate_limit::RateLimiter;
use deskd::app::adapters::web::router;
use deskd::app::adapters::web::routes::SESSION_COOKIE_NAME;
use deskd::app::adapters::web::routes::github_webhook::shared_dedupe;
use deskd::app::adapters::web::state::WebState;
use deskd::config::{WebConfig, WebRateLimitConfig};
use tower::ServiceExt;

const TEST_TG_ID: i64 = 11_000;
const TEST_SECRET: [u8; 32] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32,
];

fn cfg(audit_path: std::path::PathBuf, rate_limit: u32) -> WebConfig {
    WebConfig {
        enabled: true,
        bind: "127.0.0.1:0".into(),
        external_url: Some("https://deskd.example.com".into()),
        session_ttl_days: 30,
        magic_link_ttl_seconds: 300,
        allowed_telegram_ids: vec![TEST_TG_ID],
        audit_log: audit_path.to_string_lossy().to_string(),
        rate_limit: WebRateLimitConfig {
            auth_requests_per_hour: rate_limit,
        },
        trust_transport: false,
    }
}

fn build_state(rate_limit: u32) -> (WebState, RecordingDispatcher, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_obj = cfg(audit_path.clone(), rate_limit);
    let dispatcher = RecordingDispatcher::new();
    let dispatcher_arc: Arc<dyn deskd::app::adapters::web::dispatch::TelegramDispatcher> =
        Arc::new(dispatcher.clone());
    let agent_commands: Arc<dyn deskd::app::adapters::web::dispatch::AgentCommandDispatcher> =
        Arc::new(
            deskd::app::adapters::web::dispatch::testing::RecordingAgentCommandDispatcher::new(),
        );

    // Build the state manually so we can inject the recording dispatcher AND
    // a deterministic now-fn (1_700_000_000 = 2023-11-14 22:13:20 UTC).
    let bus_sender: Arc<dyn deskd::app::adapters::web::dispatch::BusSender> =
        Arc::new(RecordingBusSender::new());
    let metrics_cache = dir.path().join("disk-cache.json");
    let state = WebState {
        cfg: Arc::new(cfg_obj.clone()),
        secret: Arc::new(TEST_SECRET),
        tokens: Arc::new(TokenStore::new()),
        rate_limiter_ip: Arc::new(RateLimiter::new(rate_limit, 3600)),
        rate_limiter_tg: Arc::new(RateLimiter::new(rate_limit, 3600)),
        audit: AuditLog::new(audit_path),
        telegram: dispatcher_arc,
        github_webhooks: None,
        bus: bus_sender,
        github_deliveries: shared_dedupe(),
        agent_commands,
        now: Arc::new(|| 1_700_000_000),
        metrics: deskd::app::metrics::DiskMetrics::new(metrics_cache),
        agent_homes: Arc::new(Vec::new()),
        metrics_bus: None,
    };
    (state, dispatcher, dir)
}

fn fake_peer() -> SocketAddr {
    "127.0.0.1:54321".parse().unwrap()
}

/// Build a request with `ConnectInfo<SocketAddr>` injected as the
/// `axum::serve` runtime would. axum looks up the peer via an extension on
/// the request.
fn req_with_peer(req: Request<Body>) -> Request<Body> {
    let mut r = req;
    r.extensions_mut()
        .insert(axum::extract::ConnectInfo(fake_peer()));
    r
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ─── healthz ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn healthz_returns_200_unauthenticated() {
    let (state, _disp, _dir) = build_state(20);
    let app = router::build(state);
    let req = req_with_peer(Request::get("/healthz").body(Body::empty()).unwrap());
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ─── headers present everywhere ──────────────────────────────────────────

#[tokio::test]
async fn responses_include_security_headers() {
    let (state, _disp, _dir) = build_state(20);
    let app = router::build(state);
    let req = req_with_peer(Request::get("/healthz").body(Body::empty()).unwrap());
    let resp = app.oneshot(req).await.unwrap();
    let h = resp.headers();
    assert_eq!(
        h.get(header::STRICT_TRANSPORT_SECURITY).unwrap(),
        HSTS_VALUE,
    );
    assert_eq!(h.get(header::CONTENT_SECURITY_POLICY).unwrap(), CSP_VALUE,);
    assert_eq!(h.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
    assert_eq!(h.get(header::REFERRER_POLICY).unwrap(), "no-referrer");
    assert_eq!(h.get("x-frame-options").unwrap(), "DENY");
}

// ─── dashboard redirect ──────────────────────────────────────────────────

#[tokio::test]
async fn dashboard_redirects_unauthenticated_to_login() {
    let (state, _disp, _dir) = build_state(20);
    let app = router::build(state);
    let req = req_with_peer(Request::get("/").body(Body::empty()).unwrap());
    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status().is_redirection(),
        "expected redirect, got {}",
        resp.status()
    );
    assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/login");
}

#[tokio::test]
async fn dashboard_renders_with_valid_session_cookie() {
    let (state, _disp, _dir) = build_state(20);
    let now = (state.now)();
    let payload = session::SessionPayload::new(TEST_TG_ID, now, 86_400, "csrf-x".into());
    let cookie = session::sign(&payload, state.secret.as_ref());

    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("dashboard"));
    assert!(body.contains(&TEST_TG_ID.to_string()));
}

// ─── tampered cookie ──────────────────────────────────────────────────────

#[tokio::test]
async fn tampered_session_cookie_redirects_to_login() {
    let (state, _disp, _dir) = build_state(20);
    let now = (state.now)();
    let payload = session::SessionPayload::new(TEST_TG_ID, now, 86_400, "x".into());
    let cookie = session::sign(&payload, state.secret.as_ref());
    // Flip the final byte of the signature segment.
    let mut bytes = cookie.into_bytes();
    let last = bytes.len() - 1;
    bytes[last] = if bytes[last] == b'A' { b'B' } else { b'A' };
    let bad = String::from_utf8(bytes).unwrap();

    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/")
            .header(header::COOKIE, format!("{}={}", SESSION_COOKIE_NAME, bad))
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/login");
}

// ─── login flow ──────────────────────────────────────────────────────────

#[tokio::test]
async fn login_request_dispatches_message_and_audits() {
    let (state, dispatcher, dir) = build_state(20);
    let audit_path = state.audit.path().to_path_buf();
    let app = router::build(state);

    let req = req_with_peer(
        Request::post("/login/request")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=abcdef"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let calls = dispatcher.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, TEST_TG_ID);
    assert!(
        calls[0].1.contains("Login to deskd:"),
        "expected magic-link wording in: {}",
        calls[0].1
    );
    assert!(
        calls[0]
            .1
            .contains("https://deskd.example.com/login/consume?token=")
    );

    // Audit log: exactly one login_request line, ok=true.
    let log = std::fs::read_to_string(&audit_path).unwrap();
    let lines: Vec<&str> = log.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 1);
    let v: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(v["event"], "login_request");
    assert_eq!(v["telegram_id"], TEST_TG_ID);
    assert_eq!(v["ok"], true);

    drop(dir);
}

#[tokio::test]
async fn login_request_without_csrf_returns_403() {
    let (state, dispatcher, _dir) = build_state(20);
    let app = router::build(state);

    let req = req_with_peer(
        Request::post("/login/request")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(""))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status() == StatusCode::FORBIDDEN || resp.status() == StatusCode::UNPROCESSABLE_ENTITY,
        "expected 403 or 422 (form parse error), got {}",
        resp.status()
    );
    assert_eq!(dispatcher.calls().len(), 0);
}

#[tokio::test]
async fn login_request_empty_csrf_returns_403() {
    let (state, dispatcher, _dir) = build_state(20);
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/login/request")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("_csrf="))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(dispatcher.calls().len(), 0);
}

#[tokio::test]
async fn login_request_rate_limited_after_threshold() {
    let (state, dispatcher, _dir) = build_state(2);
    let app = router::build(state);

    for _ in 0..2 {
        let req = req_with_peer(
            Request::post("/login/request")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("_csrf=t"))
                .unwrap(),
        );
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let req = req_with_peer(
        Request::post("/login/request")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=t"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(dispatcher.calls().len(), 2);
}

#[tokio::test]
async fn login_request_with_empty_whitelist_rejected_and_audited() {
    let (mut state, dispatcher, dir) = build_state(20);
    let audit_path = state.audit.path().to_path_buf();
    // Override config: no whitelisted ids.
    let mut cfg_mut: WebConfig = (*state.cfg).clone();
    cfg_mut.allowed_telegram_ids.clear();
    state.cfg = Arc::new(cfg_mut);
    let app = router::build(state);

    let req = req_with_peer(
        Request::post("/login/request")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=t"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(dispatcher.calls().len(), 0);

    let log = std::fs::read_to_string(&audit_path).unwrap();
    assert!(log.contains("login_failed"));
    assert!(log.contains("whitelist_empty"));
    drop(dir);
}

// ─── consume ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn login_consume_sets_session_cookie_and_redirects() {
    let (state, _disp, _dir) = build_state(20);
    let now = (state.now)();
    let token = state.tokens.issue(TEST_TG_ID, now + 200);

    let app = router::build(state);
    let req = req_with_peer(
        Request::get(format!("/login/consume?token={}", token))
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/");
    let set_cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.starts_with(SESSION_COOKIE_NAME));
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("Secure"));
    assert!(set_cookie.contains("SameSite=Strict"));
    assert!(set_cookie.contains("Path=/"));
}

#[tokio::test]
async fn login_consume_is_single_use() {
    let (state, _disp, _dir) = build_state(20);
    let now = (state.now)();
    let token = state.tokens.issue(TEST_TG_ID, now + 200);
    let app = router::build(state);

    let req = req_with_peer(
        Request::get(format!("/login/consume?token={}", token))
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());

    let req2 = req_with_peer(
        Request::get(format!("/login/consume?token={}", token))
            .body(Body::empty())
            .unwrap(),
    );
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn login_consume_rejects_expired_token() {
    let (state, _disp, _dir) = build_state(20);
    let now = (state.now)();
    let token = state.tokens.issue(TEST_TG_ID, now - 1); // already expired
    let app = router::build(state);
    let req = req_with_peer(
        Request::get(format!("/login/consume?token={}", token))
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn login_consume_rejects_unknown_token() {
    let (state, _disp, _dir) = build_state(20);
    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/login/consume?token=does-not-exist")
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ─── logout ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn logout_clears_cookie_and_redirects() {
    let (state, _disp, _dir) = build_state(20);
    let now = (state.now)();
    let csrf = "csrf-z".to_string();
    let payload = session::SessionPayload::new(TEST_TG_ID, now, 86_400, csrf.clone());
    let cookie = session::sign(&payload, state.secret.as_ref());
    let app = router::build(state);

    let body = format!("_csrf={}", csrf);
    let req = req_with_peer(
        Request::post("/logout")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .body(Body::from(body))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/login");
    let set_cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.contains("Max-Age=0"));
}

#[tokio::test]
async fn logout_rejects_csrf_mismatch() {
    let (state, _disp, _dir) = build_state(20);
    let now = (state.now)();
    let payload = session::SessionPayload::new(TEST_TG_ID, now, 86_400, "csrf-real".into());
    let cookie = session::sign(&payload, state.secret.as_ref());
    let app = router::build(state);

    let req = req_with_peer(
        Request::post("/logout")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .body(Body::from("_csrf=other"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn logout_without_session_returns_403() {
    let (state, _disp, _dir) = build_state(20);
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/logout")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=anything"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

// ─── full login → dashboard round trip ───────────────────────────────────

#[tokio::test]
async fn full_login_round_trip() {
    let (state, dispatcher, _dir) = build_state(20);
    let app = router::build(state);

    // 1. POST /login/request with CSRF
    let req = req_with_peer(
        Request::post("/login/request")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=t"))
            .unwrap(),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Extract token from the dispatched message.
    let calls = dispatcher.calls();
    assert_eq!(calls.len(), 1);
    let url_marker = "?token=";
    let token = calls[0].1.split(url_marker).nth(1).unwrap().trim();
    let token = token.split_whitespace().next().unwrap();

    // 3. GET /login/consume?token=<token>
    let req = req_with_peer(
        Request::get(format!("/login/consume?token={}", token))
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.clone().oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    let cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    let cookie_value = cookie
        .split(';')
        .next()
        .unwrap()
        .trim_start_matches(SESSION_COOKIE_NAME)
        .trim_start_matches('=');

    // 4. GET / with the cookie returns 200 with dashboard markers.
    let req = req_with_peer(
        Request::get("/")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie_value),
            )
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("dashboard"));
}

// ─── trust_transport mode (Tailscale-internal deployments) ──────────────

/// Build a WebState with `trust_transport: true` — auth is bypassed because
/// the network layer (Tailscale/VPN) already authenticates the caller.
fn build_state_trust_transport() -> (WebState, RecordingDispatcher, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let mut cfg_obj = cfg(audit_path.clone(), 20);
    cfg_obj.trust_transport = true;
    // `external_url` is unused in this mode — clear it to prove that.
    cfg_obj.external_url = None;
    cfg_obj.allowed_telegram_ids.clear();

    let dispatcher = RecordingDispatcher::new();
    let dispatcher_arc: Arc<dyn deskd::app::adapters::web::dispatch::TelegramDispatcher> =
        Arc::new(dispatcher.clone());
    let agent_commands: Arc<dyn deskd::app::adapters::web::dispatch::AgentCommandDispatcher> =
        Arc::new(
            deskd::app::adapters::web::dispatch::testing::RecordingAgentCommandDispatcher::new(),
        );
    let bus_sender: Arc<dyn deskd::app::adapters::web::dispatch::BusSender> =
        Arc::new(RecordingBusSender::new());
    let metrics_cache = dir.path().join("disk-cache.json");
    let state = WebState {
        cfg: Arc::new(cfg_obj),
        secret: Arc::new(TEST_SECRET),
        tokens: Arc::new(TokenStore::new()),
        rate_limiter_ip: Arc::new(RateLimiter::new(20, 3600)),
        rate_limiter_tg: Arc::new(RateLimiter::new(20, 3600)),
        audit: AuditLog::new(audit_path),
        telegram: dispatcher_arc,
        github_webhooks: None,
        bus: bus_sender,
        github_deliveries: shared_dedupe(),
        agent_commands,
        now: Arc::new(|| 1_700_000_000),
        metrics: deskd::app::metrics::DiskMetrics::new(metrics_cache),
        agent_homes: Arc::new(Vec::new()),
        metrics_bus: None,
    };
    (state, dispatcher, dir)
}

#[tokio::test]
async fn trust_transport_dashboard_returns_200_without_cookie() {
    // The whole point of trust_transport: no cookie → 200, not a redirect.
    let (state, _disp, _dir) = build_state_trust_transport();
    let app = router::build(state);
    let req = req_with_peer(Request::get("/").body(Body::empty()).unwrap());
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "expected 200 with trust_transport, got {}",
        resp.status()
    );
    let body = body_string(resp).await;
    assert!(body.contains("dashboard"));
}

#[tokio::test]
async fn trust_transport_off_still_redirects_unauthenticated() {
    // Regression guard: the default config path must keep its existing
    // 302→/login behaviour. Re-using the long-standing dashboard test would
    // achieve the same thing, but pairing the two assertions here makes the
    // contract obvious to anyone reading the test file.
    let (state, _disp, _dir) = build_state(20);
    let app = router::build(state);
    let req = req_with_peer(Request::get("/").body(Body::empty()).unwrap());
    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status().is_redirection(),
        "expected redirect, got {}",
        resp.status()
    );
    assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/login");
}

#[tokio::test]
async fn trust_transport_login_request_returns_503() {
    // With auth bypassed there's no magic-link flow, and `external_url` may
    // be missing — POST /login/request must return 503, not panic on unwrap.
    let (state, _disp, _dir) = build_state_trust_transport();
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/login/request")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=anything"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// ─── enabled=false dispatch decision ─────────────────────────────────────

#[tokio::test]
async fn web_disabled_means_no_router_built() {
    // When `enabled` is false, deskd serve simply skips starting the
    // adapter — there is nothing to assert against the router because no
    // router is constructed. Verify the config-level intent: a parsed
    // WebConfig with enabled=false is the signal.
    let yaml = r#"
agents:
  - name: kira
    work_dir: /tmp
web:
  enabled: false
  external_url: https://x
"#;
    let cfg: deskd::config::WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
    let web = cfg.web.expect("web block present");
    assert!(!web.enabled);
}
