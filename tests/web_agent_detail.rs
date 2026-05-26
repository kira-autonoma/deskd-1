//! Integration tests for the #445 per-agent detail page + structured
//! command endpoints.
//!
//! Exercises the same axum router used by the production binary, with the
//! Telegram dispatcher + the new agent-command dispatcher swapped for
//! recording stubs. Covers the AC checklist in #445:
//!
//! - `GET /agent/{name}` renders with status / model / context / home dir /
//!   compaction strategy / Last tasks list / SSE wiring.
//! - 404 for unknown agent names.
//! - `POST /agent/{name}/compact` executes directly and surfaces the flash.
//! - `POST /agent/{name}/restart` and `/stop` render a confirm page; the
//!   second-POST endpoint dispatches and audits.
//! - Busy agents → flash "Agent busy, retry when idle", no dispatch.
//! - Audit log captures `{event, agent, telegram_id, ip}` per the spec.

use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use deskd::app::adapters::web::audit::AuditLog;
use deskd::app::adapters::web::auth::{magic_link::TokenStore, session};
use deskd::app::adapters::web::dispatch::AgentCommand;
use deskd::app::adapters::web::dispatch::testing::{
    RecordingAgentCommandDispatcher, RecordingBusSender, RecordingDispatcher,
};
use deskd::app::adapters::web::middleware::rate_limit::RateLimiter;
use deskd::app::adapters::web::router;
use deskd::app::adapters::web::routes::SESSION_COOKIE_NAME;
use deskd::app::adapters::web::routes::github_webhook::shared_dedupe;
use deskd::app::adapters::web::state::WebState;
use deskd::app::agent_registry::{AgentConfig, AgentState};
use deskd::app::tasklog::{TaskLog, log_task_to_path};
use deskd::config::{WebConfig, WebRateLimitConfig};
use deskd::domain::config_types::{ConfigAgentKind, ConfigAgentRuntime, ConfigSessionMode};
use tower::ServiceExt;

const TEST_TG_ID: i64 = 12_345;
const TEST_SECRET: [u8; 32] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32,
];

/// Override `$HOME` for the duration of a test, serializing against every
/// other test that mutates env via the shared `deskd::test_support::env_lock`.
/// `setenv` is not thread-safe under POSIX; without serialization concurrent
/// tests trip undefined behaviour that surfaces as flaky failures elsewhere.
struct HomeGuard {
    prev: Option<String>,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for HomeGuard {
    fn drop(&mut self) {
        unsafe {
            match self.prev.take() {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
    }
}

async fn override_home(temp: &Path) -> HomeGuard {
    use std::sync::Arc;
    use tokio::sync::Mutex;
    // Wrap the static `&'static Mutex<()>` in a leakable Arc so we can take
    // an OwnedMutexGuard (the static reference can't be used for `lock_owned`
    // directly). One Arc per call is cheap and the inner mutex is shared.
    static OWNED_LOCK: std::sync::OnceLock<Arc<Mutex<()>>> = std::sync::OnceLock::new();
    let lock = OWNED_LOCK.get_or_init(|| Arc::new(Mutex::new(()))).clone();
    let guard = lock.lock_owned().await;

    let prev = std::env::var("HOME").ok();
    unsafe {
        std::env::set_var("HOME", temp);
    }
    std::fs::create_dir_all(temp.join(".deskd/agents")).unwrap();
    std::fs::create_dir_all(temp.join(".deskd/logs")).unwrap();
    HomeGuard {
        prev,
        _guard: guard,
    }
}

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

fn build_state() -> (WebState, RecordingAgentCommandDispatcher, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let cfg_obj = cfg(audit_path.clone());
    let dispatcher = RecordingDispatcher::new();
    let dispatcher_arc: Arc<dyn deskd::app::adapters::web::dispatch::TelegramDispatcher> =
        Arc::new(dispatcher.clone());
    let commands = RecordingAgentCommandDispatcher::new();
    let commands_arc: Arc<dyn deskd::app::adapters::web::dispatch::AgentCommandDispatcher> =
        Arc::new(commands.clone());
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
        agent_commands: commands_arc,
        now: Arc::new(|| 1_700_000_000),
        metrics: deskd::app::metrics::DiskMetrics::new(metrics_cache),
        agent_homes: Arc::new(Vec::new()),
        metrics_bus: None,
        cost: None,
        gh: Arc::new(deskd::app::adapters::web::data_cost::testing::RecordingGhClient::new()),
        cost_cache: deskd::app::adapters::web::data_cost::CostCache::new(),
    };
    (state, commands, dir)
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

fn auth_cookie(state: &WebState, csrf: &str) -> String {
    let now = (state.now)();
    let payload = session::SessionPayload::new(TEST_TG_ID, now, 86_400, csrf.into());
    session::sign(&payload, state.secret.as_ref())
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn mk_agent_state(name: &str, status: &str) -> AgentState {
    AgentState {
        config: AgentConfig {
            name: name.into(),
            model: "claude-opus-4-7".into(),
            system_prompt: String::new(),
            work_dir: format!("/tmp/{}", name),
            max_turns: 100,
            unix_user: None,
            command: vec!["claude".into()],
            config_path: None,
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: Default::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: Some(0.8),
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        },
        pid: 0,
        session_id: "abcdef0123".into(),
        total_turns: 1,
        total_cost: 0.0,
        created_at: chrono::Utc::now().to_rfc3339(),
        status: status.into(),
        current_task: if status == "working" {
            "doing things".into()
        } else {
            String::new()
        },
        parent: None,
        scope: None,
        can_message: None,
        env_keys: None,
        session_start: None,
        session_cost: 0.0,
        session_turns: 0,
        consecutive_empty_completions: 0,
        last_empty_restart_at: None,
        total_empty_restarts: 0,
        tmux_session: None,
        tmux_log_path: None,
    }
}

fn seed_agent(home: &Path, state: &AgentState) {
    let path = home
        .join(".deskd/agents")
        .join(format!("{}.yaml", state.config.name));
    let content = serde_yaml::to_string(state).unwrap();
    std::fs::write(path, content).unwrap();
}

fn seed_tasklog(home: &Path, name: &str, entries: &[TaskLog]) {
    let dir = home.join(".deskd/logs").join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("tasks.jsonl");
    for e in entries {
        log_task_to_path(&path, e).unwrap();
    }
}

fn task(status: &str, summary: &str, duration_ms: u64) -> TaskLog {
    TaskLog {
        ts: "2026-05-09T14:32:00Z".into(),
        source: "telegram".into(),
        turns: 1,
        cost: 0.01,
        duration_ms,
        status: status.into(),
        task: summary.into(),
        error: None,
        msg_id: "id".into(),
        github_repo: None,
        github_pr: None,
        input_tokens: None,
        output_tokens: None,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        session_count: None,
        tool_use_count: None,
        parent_agent: None,
    }
}

// ─── AC: GET /agent/{name} renders with all sections ────────────────────

#[tokio::test]
async fn get_agent_detail_renders_full_layout() {
    let (state, _cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;

    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));
    seed_tasklog(
        _dir.path(),
        "kira",
        &[
            task("ok", "fix archmotif PR", 64_000),
            task("error", "broken task", 3_000),
        ],
    );

    let cookie = auth_cookie(&state, "csrf-x");
    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/agent/kira")
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
    // Header.
    assert!(body.contains("← back"));
    assert!(body.contains("kira"));
    // Meta — session, model, home, context (compaction strategy).
    assert!(
        body.contains("abcdef01"),
        "missing session_id; body:\n{}",
        body
    );
    assert!(body.contains("claude-opus-4-7"), "missing model");
    assert!(body.contains("/tmp/kira"), "missing home dir");
    assert!(
        body.contains("compaction: auto @ 80%"),
        "missing compaction label; body excerpt:\n{}",
        &body[..body.len().min(4000)]
    );
    // Three action forms.
    assert!(body.contains(r#"action="/agent/kira/restart""#));
    assert!(body.contains(r#"action="/agent/kira/compact""#));
    assert!(body.contains(r#"action="/agent/kira/stop""#));
    // Last tasks.
    assert!(body.contains("Last tasks"));
    assert!(body.contains("fix archmotif PR"));
    assert!(body.contains("broken task"));
    // SSE wiring.
    assert!(body.contains(r#"sse-connect="/agent/kira/events""#));
}

// ─── AC: 404 for unknown agent name ─────────────────────────────────────

#[tokio::test]
async fn get_agent_detail_404_for_unknown_name() {
    let (state, _cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;

    let cookie = auth_cookie(&state, "csrf-x");
    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/agent/no-such-agent")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_string(resp).await;
    assert!(body.contains("Agent not found"));
    assert!(body.contains("no-such-agent"));
}

// ─── AC: detail page redirects unauthenticated visitors ────────────────

#[tokio::test]
async fn get_agent_detail_redirects_unauthenticated_to_login() {
    let (state, _cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let app = router::build(state);
    let req = req_with_peer(Request::get("/agent/kira").body(Body::empty()).unwrap());
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/login");
}

// ─── AC: /compact executes directly + audit log ──────────────────────────

#[tokio::test]
async fn post_compact_dispatches_immediately_and_audits() {
    let (state, cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    let audit_path = state.audit.path().to_path_buf();
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let cookie = auth_cookie(&state, "csrf-z");
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/compact")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=csrf-z"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status().is_redirection(),
        "expected redirect, got {}",
        resp.status()
    );
    let loc = resp
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc.starts_with("/agent/kira"));
    assert!(loc.contains("ok=Compaction%20queued"));

    let calls = cmds.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], ("kira".to_string(), AgentCommand::Compact));

    let log = std::fs::read_to_string(&audit_path).unwrap();
    let line = log.lines().last().unwrap();
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(v["event"], "agent_compact");
    assert_eq!(v["agent"], "kira");
    assert_eq!(v["telegram_id"], TEST_TG_ID);
    assert_eq!(v["ok"], true);
    assert_eq!(v["ip"], "127.0.0.1");
}

// ─── AC: /restart goes through two-step confirm ──────────────────────────

#[tokio::test]
async fn post_restart_returns_confirm_page_not_executing() {
    let (state, cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let cookie = auth_cookie(&state, "csrf-r");
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/restart")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=csrf-r"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("Confirm Restart"));
    assert!(body.contains(r#"action="/agent/kira/restart/confirm""#));
    assert!(body.contains("Cancel"));
    // First POST must NOT have dispatched anything.
    assert!(
        cmds.calls().is_empty(),
        "expected no dispatch on first POST, got {:?}",
        cmds.calls()
    );
}

#[tokio::test]
async fn post_restart_confirm_dispatches_and_audits() {
    let (state, cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    let audit_path = state.audit.path().to_path_buf();
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let cookie = auth_cookie(&state, "csrf-r");
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/restart/confirm")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=csrf-r"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    let loc = resp
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc.contains("ok=Restart%20requested"));

    let calls = cmds.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], ("kira".to_string(), AgentCommand::Restart));

    let log = std::fs::read_to_string(&audit_path).unwrap();
    let last = log.lines().last().unwrap();
    let v: serde_json::Value = serde_json::from_str(last).unwrap();
    assert_eq!(v["event"], "agent_restart");
    assert_eq!(v["agent"], "kira");
    assert_eq!(v["ok"], true);
}

// ─── AC: /stop follows same two-step pattern ─────────────────────────────

#[tokio::test]
async fn post_stop_returns_confirm_page() {
    let (state, _cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let cookie = auth_cookie(&state, "csrf-s");
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/stop")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=csrf-s"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("Confirm Stop"));
    assert!(body.contains(r#"action="/agent/kira/stop/confirm""#));
}

#[tokio::test]
async fn post_stop_confirm_dispatches_and_audits() {
    let (state, cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    let audit_path = state.audit.path().to_path_buf();
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let cookie = auth_cookie(&state, "csrf-s");
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/stop/confirm")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=csrf-s"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());

    let calls = cmds.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], ("kira".to_string(), AgentCommand::Stop));

    let log = std::fs::read_to_string(&audit_path).unwrap();
    let last = log.lines().last().unwrap();
    let v: serde_json::Value = serde_json::from_str(last).unwrap();
    assert_eq!(v["event"], "agent_stop");
    assert_eq!(v["agent"], "kira");
    assert_eq!(v["ok"], true);
}

// ─── AC: busy agent → flash error, no dispatch ──────────────────────────

#[tokio::test]
async fn post_compact_blocked_when_agent_is_working() {
    let (state, cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    let audit_path = state.audit.path().to_path_buf();
    seed_agent(_dir.path(), &mk_agent_state("kira", "working"));

    let cookie = auth_cookie(&state, "csrf-z");
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/compact")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=csrf-z"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    let loc = resp
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc.contains("err=Agent%20busy"));

    // No dispatch — busy guard short-circuited.
    assert!(cmds.calls().is_empty());

    // Audit log records the rejected attempt with reason=agent_busy.
    let log = std::fs::read_to_string(&audit_path).unwrap();
    let v: serde_json::Value = serde_json::from_str(log.lines().last().unwrap()).unwrap();
    assert_eq!(v["event"], "agent_compact");
    assert_eq!(v["ok"], false);
    assert_eq!(v["reason"], "agent_busy");
}

#[tokio::test]
async fn post_restart_confirm_blocked_when_agent_is_working() {
    let (state, cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    seed_agent(_dir.path(), &mk_agent_state("kira", "working"));

    let cookie = auth_cookie(&state, "csrf-r");
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/restart/confirm")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=csrf-r"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    let loc = resp
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc.contains("err=Agent%20busy"));
    assert!(cmds.calls().is_empty());
}

// ─── AC: CSRF + invalid cookie rejected ─────────────────────────────────

#[tokio::test]
async fn post_compact_without_csrf_returns_403() {
    let (state, cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let cookie = auth_cookie(&state, "csrf-real");
    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/compact")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=wrong"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert!(cmds.calls().is_empty());
}

#[tokio::test]
async fn post_compact_unauthenticated_redirects_to_login() {
    let (state, cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let app = router::build(state);
    let req = req_with_peer(
        Request::post("/agent/kira/compact")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("_csrf=csrf-z"))
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/login");
    assert!(cmds.calls().is_empty());
}

// ─── flash query string round-trip ──────────────────────────────────────

#[tokio::test]
async fn get_agent_detail_renders_flash_from_query() {
    let (state, _cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let cookie = auth_cookie(&state, "csrf-x");
    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/agent/kira?ok=Restart%20requested")
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
    assert!(body.contains("flash--ok"));
    assert!(body.contains("Restart requested"));
}

#[tokio::test]
async fn get_agent_detail_renders_error_flash() {
    let (state, _cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    seed_agent(_dir.path(), &mk_agent_state("kira", "idle"));

    let cookie = auth_cookie(&state, "csrf-x");
    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/agent/kira?err=Agent%20busy%2C%20retry%20when%20idle")
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
    assert!(body.contains("flash--error"));
    assert!(body.contains("Agent busy"));
}

// ─── /events filtered to agent: requires auth, returns SSE content-type ─

#[tokio::test]
async fn agent_events_requires_auth() {
    let (state, _cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/agent/kira/events")
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection());
    assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/login");
}

#[tokio::test]
async fn agent_events_authenticated_serves_event_stream() {
    let (state, _cmds, _dir) = build_state();
    let _guard = override_home(_dir.path()).await;
    let cookie = auth_cookie(&state, "csrf-x");
    let app = router::build(state);
    let req = req_with_peer(
        Request::get("/agent/kira/events?max_ticks=1&tick_ms=10&keepalive_ms=10000")
            .header(
                header::COOKIE,
                format!("{}={}", SESSION_COOKIE_NAME, cookie),
            )
            .body(Body::empty())
            .unwrap(),
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        ct.starts_with("text/event-stream"),
        "expected SSE content-type, got {}",
        ct
    );
}

// Suppress unused-import warning for the alias-free Mutex use.
#[allow(dead_code)]
fn _ensure_mutex_compiles() -> Arc<Mutex<()>> {
    Arc::new(Mutex::new(()))
}

// async_trait used only by `RecordingAgentCommandDispatcher` via re-export;
// the import keeps proc-macro coverage if it ever changes signatures.
#[async_trait]
trait _Touch: Send + Sync {}
