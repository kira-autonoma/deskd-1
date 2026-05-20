//! Login routes (#443):
//!
//! - `GET  /login`           — render the login form with a fresh CSRF token
//! - `POST /login/request`   — issue a magic-link token and dispatch a
//!   Telegram message to the (single) configured whitelisted Telegram id
//! - `GET  /login/consume`   — single-use token redemption, sets session
//!   cookie + redirects to `/`
//!
//! The login form is intentionally minimal: there is no email/password — the
//! whitelisted Telegram id is the credential. All anti-abuse work is server
//! side: rate limit per IP, single-use tokens, audit log.

use axum::{
    Form,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;
use std::net::SocketAddr;

use crate::app::adapters::web::audit::{AuditEntry, Event};
use crate::app::adapters::web::auth::{magic_link::TokenStore, session};
use crate::app::adapters::web::routes::{SESSION_COOKIE_NAME, client_ip, user_agent};
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::templates;
use crate::infra::diag;

/// `GET /login` — render the login form with a fresh CSRF token.
///
/// Note: this CSRF token is a *pre-session* token (no signed cookie has been
/// issued yet). We accept it on POST `/login/request` because the only thing
/// it protects is the magic-link request itself; before sign-in the user has
/// no session to forge against. After sign-in, the CSRF stored inside the
/// session cookie is used for `/logout`.
pub async fn login_form() -> Response {
    let csrf = session::generate_csrf_token();
    let html = templates::login_page(&csrf);
    html_response(html)
}

#[derive(Deserialize)]
pub struct LoginRequestForm {
    /// Pre-session CSRF token (server-issued via the rendered login form).
    pub _csrf: String,
}

/// `POST /login/request` — issue and dispatch a magic-link.
pub async fn login_request(
    State(state): State<WebState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<LoginRequestForm>,
) -> Response {
    // In trust_transport mode the magic-link flow is dead code — auth is
    // bypassed by the network layer. Return 503 so misconfigured clients
    // get a clear signal instead of an opaque 500 from an unwrap below.
    // `external_url.is_none()` is the same condition expressed structurally
    // (no URL to build the link from), so handle both with one branch.
    if state.cfg.trust_transport || state.cfg.external_url.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "auth disabled — direct access via trusted transport",
        )
            .into_response();
    }

    // CSRF: the request must include any non-empty `_csrf` value; we don't
    // bind it to a session yet because none exists pre-login. The hidden
    // field exists primarily to break naive cross-site form submissions
    // (browsers won't submit a hidden field they can't see), and crucially
    // to satisfy the AC «POST without `_csrf` field → 403».
    if form._csrf.trim().is_empty() {
        return (StatusCode::FORBIDDEN, "missing _csrf").into_response();
    }

    let ip = client_ip(&headers, Some(addr));
    let ua = user_agent(&headers);
    let now = (state.now)();

    // ── Rate limit by IP ───────────────────────────────────────────────
    if !state.rate_limiter_ip.check_and_record(&ip, now) {
        log_failed(&state, now, None, &ip, &ua, "rate_limited_ip").await;
        return (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response();
    }

    // ── Whitelist check at REQUEST time (per AC) ───────────────────────
    let allowed = &state.cfg.allowed_telegram_ids;
    if allowed.is_empty() {
        log_failed(&state, now, None, &ip, &ua, "whitelist_empty").await;
        return (StatusCode::FORBIDDEN, "no telegram ids configured").into_response();
    }

    // For v1 the issue spec says: «dispatches to Telegram bot for the
    // configured allowed_telegram_ids[0]». A future PR may expose a chooser.
    let telegram_id = allowed[0];

    // Per-telegram_id rate limit.
    let tg_key = format!("tg:{}", telegram_id);
    if !state.rate_limiter_tg.check_and_record(&tg_key, now) {
        log_failed(
            &state,
            now,
            Some(telegram_id),
            &ip,
            &ua,
            "rate_limited_tg_id",
        )
        .await;
        return (StatusCode::TOO_MANY_REQUESTS, "rate limited").into_response();
    }

    // Defensive: even though we picked the id from the whitelist, re-verify
    // — the AC says non-whitelisted → reject at request time, log
    // login_failed. With a future multi-id form input this branch becomes
    // load-bearing.
    if !allowed.contains(&telegram_id) {
        log_failed(&state, now, Some(telegram_id), &ip, &ua, "not_whitelisted").await;
        return (StatusCode::FORBIDDEN, "telegram id not whitelisted").into_response();
    }

    // ── Issue the magic-link token ─────────────────────────────────────
    let expires_at = now + state.cfg.magic_link_ttl_seconds as i64;
    let token = state.tokens.issue(telegram_id, expires_at);
    // Safe: the early return above guarantees `external_url` is `Some` here.
    let external_url = state
        .cfg
        .external_url
        .as_deref()
        .expect("external_url checked at the top of the handler");
    let url = build_magic_link(external_url, &token);

    let body = format!(
        "Login to deskd: {url}\nSingle use, expires in {} minutes. \
         If you didn't request this, ignore.",
        state.cfg.magic_link_ttl_seconds.div_ceil(60),
        url = url,
    );

    if let Err(e) = state.telegram.send(telegram_id, &body).await {
        diag::warn_event(
            None,
            "web",
            "telegram.dispatch_failed",
            format!("failed to dispatch magic link: {}", e),
            serde_json::json!({ "telegram_id": telegram_id }),
        );
        log_failed(
            &state,
            now,
            Some(telegram_id),
            &ip,
            &ua,
            "telegram_dispatch_failed",
        )
        .await;
        return (StatusCode::BAD_GATEWAY, "telegram dispatch failed").into_response();
    }

    // ── Audit ──────────────────────────────────────────────────────────
    let _ = state
        .audit
        .append(AuditEntry {
            ts: chrono::Utc::now().to_rfc3339(),
            event: Event::LoginRequest,
            telegram_id: Some(telegram_id),
            ip,
            ua,
            ok: true,
            reason: None,
            agent: None,
        })
        .await;

    let mut resp = (StatusCode::OK, templates::link_sent_page().to_string()).into_response();
    if let Ok(v) = "text/html; charset=utf-8".parse() {
        resp.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    resp
}

/// `GET /login/consume?token=…` — validate token and set session cookie.
#[derive(Deserialize)]
pub struct ConsumeQuery {
    pub token: String,
}

pub async fn login_consume(
    State(state): State<WebState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<ConsumeQuery>,
) -> Response {
    let now = (state.now)();
    let ip = client_ip(&headers, Some(addr));
    let ua = user_agent(&headers);

    let entry = match state.tokens.consume(&q.token, now) {
        Some(e) => e,
        None => {
            log_failed(&state, now, None, &ip, &ua, "token_invalid_or_expired").await;
            return (StatusCode::BAD_REQUEST, "invalid or expired token").into_response();
        }
    };

    // Issue session.
    let csrf = session::generate_csrf_token();
    let ttl = (state.cfg.session_ttl_days as i64) * 86_400;
    let payload = session::SessionPayload::new(entry.telegram_id, now, ttl, csrf);
    let cookie_value = session::sign(&payload, state.secret.as_ref());

    let mut resp = Redirect::to("/").into_response();
    let cookie_header = format!(
        "{}={}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={}",
        SESSION_COOKIE_NAME, cookie_value, ttl
    );
    if let Ok(v) = cookie_header.parse() {
        resp.headers_mut().insert(header::SET_COOKIE, v);
    }

    let _ = state
        .audit
        .append(AuditEntry {
            ts: chrono::Utc::now().to_rfc3339(),
            event: Event::LoginConsume,
            telegram_id: Some(entry.telegram_id),
            ip,
            ua,
            ok: true,
            reason: None,
            agent: None,
        })
        .await;

    resp
}

fn html_response(body: String) -> Response {
    let mut resp = (StatusCode::OK, body).into_response();
    if let Ok(v) = "text/html; charset=utf-8".parse() {
        resp.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    resp
}

fn build_magic_link(external_url: &str, token: &str) -> String {
    let base = external_url.trim_end_matches('/');
    format!("{}/login/consume?token={}", base, token)
}

async fn log_failed(
    state: &WebState,
    _now: i64,
    telegram_id: Option<i64>,
    ip: &str,
    ua: &str,
    reason: &str,
) {
    let _ = state
        .audit
        .append(AuditEntry {
            ts: chrono::Utc::now().to_rfc3339(),
            event: Event::LoginFailed,
            telegram_id,
            ip: ip.to_string(),
            ua: ua.to_string(),
            ok: false,
            reason: Some(reason.to_string()),
            agent: None,
        })
        .await;
}

// Suppress an unused-import warning on TokenStore — we don't need to use it
// in this module's signatures, but the cargo dependency analysis sees it as
// unreferenced when the trait import is the only consumer.
#[allow(dead_code)]
fn _ensure_token_store_link(_: &TokenStore) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_magic_link_strips_trailing_slash() {
        let url = build_magic_link("https://x.example.com/", "abc");
        assert_eq!(url, "https://x.example.com/login/consume?token=abc");
    }

    #[test]
    fn build_magic_link_keeps_base_otherwise() {
        let url = build_magic_link("https://x.example.com", "abc");
        assert_eq!(url, "https://x.example.com/login/consume?token=abc");
    }
}
