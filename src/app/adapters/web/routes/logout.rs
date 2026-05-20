//! `POST /logout` — clear session cookie + redirect to /login (#443).

use axum::{
    Form,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;
use std::net::SocketAddr;

use crate::app::adapters::web::audit::{AuditEntry, Event};
use crate::app::adapters::web::routes::{
    SESSION_COOKIE_NAME, authenticate, client_ip, csrf_ok_for, user_agent,
};
use crate::app::adapters::web::state::WebState;

#[derive(Deserialize)]
pub struct LogoutForm {
    pub _csrf: String,
}

pub async fn logout(
    State(state): State<WebState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<LogoutForm>,
) -> Response {
    let ip = client_ip(&headers, Some(addr));
    let ua = user_agent(&headers);

    // Validate CSRF: cookie must verify AND `form._csrf` must match the
    // CSRF token embedded inside the signed cookie. In trust_transport mode
    // the synthetic session always authenticates and CSRF is moot.
    let payload = authenticate(&state, &headers);
    let valid = match &payload {
        Some(p) => csrf_ok_for(&state, p, &form._csrf),
        None => false,
    };

    if !valid {
        return (StatusCode::FORBIDDEN, "invalid csrf").into_response();
    }

    let mut resp = Redirect::to("/login").into_response();
    let expire = format!(
        "{}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
        SESSION_COOKIE_NAME
    );
    if let Ok(v) = expire.parse() {
        resp.headers_mut().insert(header::SET_COOKIE, v);
    }

    let _ = state
        .audit
        .append(AuditEntry {
            ts: chrono::Utc::now().to_rfc3339(),
            event: Event::Logout,
            telegram_id: payload.map(|p| p.telegram_id),
            ip,
            ua,
            ok: true,
            reason: None,
            agent: None,
        })
        .await;

    resp
}
