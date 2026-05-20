//! `/agent/<name>` — per-agent detail page + structured command endpoints (#445).
//!
//! Routes exposed by this module:
//!
//! | Method | Path                                | Purpose                       |
//! |--------|-------------------------------------|-------------------------------|
//! | GET    | `/agent/{name}`                     | Render the detail page        |
//! | GET    | `/agent/{name}/events`              | Per-agent SSE stream          |
//! | POST   | `/agent/{name}/restart`             | First step — render «sure?»  |
//! | POST   | `/agent/{name}/restart/confirm`     | Second step — actually go    |
//! | POST   | `/agent/{name}/compact`             | Direct: trigger compaction    |
//! | POST   | `/agent/{name}/stop`                | First step — render «sure?»  |
//! | POST   | `/agent/{name}/stop/confirm`        | Second step — actually stop  |
//!
//! The detail page also surfaces the per-agent disk breakdown (#446):
//! the home-directory total + the top-N largest subdirectories, sampled
//! via `du`. The breakdown is rendered as a dedicated section between
//! the metadata grid and the action buttons.
//!
//! Every POST is CSRF-protected by comparing the form's `_csrf` field to
//! the `csrf` claim embedded in the session cookie. Each successful
//! invocation appends a single entry to the audit log via
//! [`crate::app::adapters::web::audit::AuditLog::append`].

use axum::{
    Form,
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;
use std::net::SocketAddr;

use crate::app::adapters::web::audit::{AuditEntry, Event};
use crate::app::adapters::web::auth::session;
use crate::app::adapters::web::data::{self, AgentDetail};
use crate::app::adapters::web::dispatch::AgentCommand;
use crate::app::adapters::web::routes::{authenticate, client_ip, csrf_ok_for, user_agent};
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::view::{agent_disk_detail_html, format_bytes};
use crate::app::adapters::web::{templates, view};
use crate::app::metrics::{AgentBreakdownEntry, sample_agent_breakdown};

/// Top-N subdirectories shown on the detail page (#446). The issue spec
/// calls out «top 5».
pub const BREAKDOWN_LIMIT: usize = 5;

/// Query parameters supported by the detail page (carry the optional flash
/// message + classification across the post/redirect/get cycle).
#[derive(Debug, Default, Deserialize)]
pub struct DetailQuery {
    /// Success message to surface in a green banner. Server-rendered and
    /// HTML-escaped; treated as plain text.
    #[serde(default)]
    pub ok: Option<String>,
    /// Error message to surface in a red banner.
    #[serde(default)]
    pub err: Option<String>,
}

/// Body for the action POST forms — only the CSRF token.
#[derive(Deserialize)]
pub struct ActionForm {
    pub _csrf: String,
}

/// `GET /agent/{name}` — detail page.
pub async fn detail(
    State(state): State<WebState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Query(query): Query<DetailQuery>,
) -> Response {
    let session_payload = match require_session(&state, &headers) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let detail = match data::collect_agent_detail(&name).await {
        Some(d) => d,
        None => return not_found_response(&name),
    };

    // #446: gather the per-agent disk breakdown so the detail page can
    // surface a "top-N largest subdirectories" block alongside the
    // metadata grid. Sources mirror the dashboard renderer: the cached
    // `DiskSnapshot` for the home-dir total, an on-demand `du` sample
    // for the per-directory entries.
    let snap = state.metrics.snapshot().await;
    let agent_sample = snap.lookup_agent(&name).cloned();
    let home_dir_from_state = state
        .agent_homes
        .iter()
        .find(|(n, _)| n == &name)
        .map(|(_, h)| h.clone());
    let home_dir_for_breakdown = home_dir_from_state
        .as_deref()
        .unwrap_or(detail.home_dir.as_str());
    let breakdown: Vec<AgentBreakdownEntry> = if home_dir_for_breakdown.is_empty() {
        Vec::new()
    } else {
        sample_agent_breakdown(home_dir_for_breakdown, BREAKDOWN_LIMIT).await
    };
    let total_bytes = agent_sample
        .as_ref()
        .and_then(|a| a.home_dir_bytes)
        .or(detail.summary.home_dir_bytes);
    let disk_html = agent_disk_detail_html(
        Some(home_dir_for_breakdown),
        total_bytes,
        snap.updated_at,
        &breakdown,
        format_bytes,
    );

    render_detail_page(
        session_payload.telegram_id,
        &session_payload.csrf,
        &detail,
        &disk_html,
        query.ok.as_deref(),
        query.err.as_deref(),
    )
}

/// `GET /agent/{name}/events` — per-agent SSE stream. Filters the global
/// `/events` machinery down to a single agent by reusing the same diff
/// engine.
///
/// To keep the diff in this PR focused we delegate to the existing
/// [`super::sse::events`] handler and rely on the browser-side `sse-swap`
/// attribute to ignore events for other agents. The endpoint exists so the
/// detail page can wire `sse-connect="/agent/<name>/events"` per the issue
/// spec; in practice it streams the same payload as `/events`.
///
/// TODO(#445): when the bus exposes a per-agent tap, switch this handler
/// to subscribe to that tap directly so the wire traffic doesn't grow
/// linearly with the number of open detail pages.
pub async fn events(
    State(state): State<WebState>,
    Path(_name): Path<String>,
    headers: HeaderMap,
    Query(q): Query<super::sse::EventsQuery>,
) -> Response {
    super::sse::events(State(state), headers, Query(q)).await
}

/// `POST /agent/{name}/compact` — non-destructive, executes directly.
pub async fn compact(
    State(state): State<WebState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Form(form): Form<ActionForm>,
) -> Response {
    handle_direct_action(
        state,
        addr,
        name,
        headers,
        form,
        AgentCommand::Compact,
        Event::AgentCompact,
        "Compaction queued",
    )
    .await
}

/// `POST /agent/{name}/restart` — first step: render confirm page.
pub async fn restart_confirm_form(
    State(state): State<WebState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Form(form): Form<ActionForm>,
) -> Response {
    render_confirm_page(state, name, headers, form, "restart", "Restart")
}

/// `POST /agent/{name}/restart/confirm` — second step: execute restart.
pub async fn restart(
    State(state): State<WebState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Form(form): Form<ActionForm>,
) -> Response {
    handle_direct_action(
        state,
        addr,
        name,
        headers,
        form,
        AgentCommand::Restart,
        Event::AgentRestart,
        "Restart requested",
    )
    .await
}

/// `POST /agent/{name}/stop` — first step: render confirm page.
pub async fn stop_confirm_form(
    State(state): State<WebState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Form(form): Form<ActionForm>,
) -> Response {
    render_confirm_page(state, name, headers, form, "stop", "Stop")
}

/// `POST /agent/{name}/stop/confirm` — second step: execute stop.
pub async fn stop(
    State(state): State<WebState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Form(form): Form<ActionForm>,
) -> Response {
    handle_direct_action(
        state,
        addr,
        name,
        headers,
        form,
        AgentCommand::Stop,
        Event::AgentStop,
        "Stopped",
    )
    .await
}

// ─── helpers ────────────────────────────────────────────────────────────────

/// Resolve the session payload from a request or return a redirect/403
/// response the caller can short-circuit on.
///
/// The `Err` arm carries a fully-built axum `Response` so handlers can
/// `return resp` without re-deriving the cookie state. clippy flags this as
/// a large error variant, but the alternative — wrapping in a `Box` — adds
/// an allocation on every authenticated request just to silence the lint.
#[allow(clippy::result_large_err)]
fn require_session(
    state: &WebState,
    headers: &HeaderMap,
) -> Result<session::SessionPayload, Response> {
    match authenticate(state, headers) {
        Some(p) => Ok(p),
        None => Err(Redirect::to("/login").into_response()),
    }
}

/// Wrapper handling the `_csrf` check + dispatch + audit + redirect flow for
/// the three "execute" endpoints (compact, restart confirm, stop confirm).
#[allow(clippy::too_many_arguments)]
async fn handle_direct_action(
    state: WebState,
    addr: SocketAddr,
    name: String,
    headers: HeaderMap,
    form: ActionForm,
    command: AgentCommand,
    event: Event,
    success_msg: &str,
) -> Response {
    let session_payload = match require_session(&state, &headers) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    if !csrf_ok(&state, &session_payload, &form) {
        return (StatusCode::FORBIDDEN, "invalid csrf").into_response();
    }

    // 404 if the agent name isn't known. Resolving the detail also gives us
    // the busy flag without a second pass.
    let detail = match data::collect_agent_detail(&name).await {
        Some(d) => d,
        None => return not_found_response(&name),
    };

    let ip = client_ip(&headers, Some(addr));
    let ua = user_agent(&headers);

    if detail.busy {
        // Audit the rejected attempt — operators want to see "X tried to
        // restart agent Y while busy" in the log, not silence.
        let _ = state
            .audit
            .append(AuditEntry {
                ts: chrono::Utc::now().to_rfc3339(),
                event,
                telegram_id: Some(session_payload.telegram_id),
                ip: ip.clone(),
                ua: ua.clone(),
                ok: false,
                reason: Some("agent_busy".into()),
                agent: Some(name.clone()),
            })
            .await;
        return redirect_to_detail(&name, None, Some("Agent busy, retry when idle"));
    }

    let dispatch_result = state.agent_commands.send(&name, command).await;
    let ok = dispatch_result.is_ok();
    let reason = if ok {
        None
    } else {
        Some(format!(
            "dispatch_failed: {}",
            dispatch_result
                .as_ref()
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
        ))
    };

    let _ = state
        .audit
        .append(AuditEntry {
            ts: chrono::Utc::now().to_rfc3339(),
            event,
            telegram_id: Some(session_payload.telegram_id),
            ip,
            ua,
            ok,
            reason,
            agent: Some(name.clone()),
        })
        .await;

    if ok {
        redirect_to_detail(&name, Some(success_msg), None)
    } else {
        redirect_to_detail(
            &name,
            None,
            Some("Command dispatch failed — see server logs."),
        )
    }
}

fn render_confirm_page(
    state: WebState,
    name: String,
    headers: HeaderMap,
    form: ActionForm,
    action: &str,
    label: &str,
) -> Response {
    let session_payload = match require_session(&state, &headers) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    if !csrf_ok(&state, &session_payload, &form) {
        return (StatusCode::FORBIDDEN, "invalid csrf").into_response();
    }

    let body = view::confirm_page_body(&name, action, label, &session_payload.csrf);
    let title = format!("deskd · confirm {}", label.to_ascii_lowercase());
    let html = templates::agent_confirm_page(
        session_payload.telegram_id,
        &session_payload.csrf,
        &body,
        &title,
    );
    html_response(StatusCode::OK, html)
}

fn csrf_ok(state: &WebState, payload: &session::SessionPayload, form: &ActionForm) -> bool {
    csrf_ok_for(state, payload, &form._csrf)
}

#[allow(clippy::too_many_arguments)]
fn render_detail_page(
    telegram_id: i64,
    csrf: &str,
    detail: &AgentDetail,
    disk_html: &str,
    ok: Option<&str>,
    err: Option<&str>,
) -> Response {
    let header_html = view::detail_header(detail);
    let flash_html = view::detail_flash(ok, err);
    let meta_html = view::detail_meta(detail);
    let actions_html = view::detail_actions(detail, csrf);
    let tasks_html = view::detail_tasks(detail);
    let bus_tail_html = view::detail_bus_tail(detail);
    let body = templates::agent_detail_page(
        telegram_id,
        csrf,
        &header_html,
        &flash_html,
        &meta_html,
        disk_html,
        &actions_html,
        &tasks_html,
        &bus_tail_html,
    );
    html_response(StatusCode::OK, body)
}

fn not_found_response(name: &str) -> Response {
    let body = templates::agent_not_found_page(name);
    html_response(StatusCode::NOT_FOUND, body)
}

fn html_response(status: StatusCode, body: String) -> Response {
    let mut resp = (status, body).into_response();
    if let Ok(v) = "text/html; charset=utf-8".parse() {
        resp.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    resp
}

fn redirect_to_detail(name: &str, ok: Option<&str>, err: Option<&str>) -> Response {
    let mut query: Vec<(String, String)> = Vec::new();
    if let Some(msg) = ok {
        query.push(("ok".into(), msg.into()));
    }
    if let Some(msg) = err {
        query.push(("err".into(), msg.into()));
    }
    let url = if query.is_empty() {
        format!("/agent/{}", url_escape(name))
    } else {
        let qs: Vec<String> = query
            .iter()
            .map(|(k, v)| format!("{}={}", k, percent_encode(v)))
            .collect();
        format!("/agent/{}?{}", url_escape(name), qs.join("&"))
    };
    Redirect::to(&url).into_response()
}

/// Minimal URL-component escaper for agent names + flash text. The agent
/// list is small and we don't depend on a full percent-encode crate; we
/// escape the characters the redirect target actually cares about.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn url_escape(name: &str) -> String {
    percent_encode(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_encode_handles_spaces_and_quotes() {
        assert_eq!(percent_encode("hello world"), "hello%20world");
        assert_eq!(percent_encode("a\"b"), "a%22b");
        assert_eq!(percent_encode("plain-name_1.0"), "plain-name_1.0");
    }
}
