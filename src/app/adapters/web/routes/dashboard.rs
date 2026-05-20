//! `GET /` — dashboard (#443/#444). Redirects unauthenticated visitors to
//! /login. Authenticated users get the live agent overview built from the
//! existing registry/context_size/tasklog data sources (#444).

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};

use crate::app::adapters::web::data;
use crate::app::adapters::web::routes::{SESSION_COOKIE_NAME, authenticate};
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::templates;
use crate::app::adapters::web::view;

pub async fn dashboard(State(state): State<WebState>, headers: HeaderMap) -> Response {
    let session_payload = authenticate(&state, &headers);

    match session_payload {
        Some(p) => {
            let disk = state.metrics.snapshot().await;
            let summaries = data::collect_agent_summaries(Some(&disk)).await;
            let overview = data::collect_vps_overview(Some(&disk));
            let strip_html = view::vps_strip(&overview);
            let agents_html = view::agents_section_with_disk(&summaries, disk.updated_at);
            // «Refresh now» form posts to /metrics/refresh with the current CSRF.
            let refresh_html = templates::metrics_refresh_form(&p.csrf);
            let html = templates::dashboard_page(
                p.telegram_id,
                &p.csrf,
                &refresh_html,
                &strip_html,
                &agents_html,
            );
            html_response(html)
        }
        None => {
            // Tampered/expired/missing — clear the cookie and redirect.
            let mut resp = Redirect::to("/login").into_response();
            // Defensive: drop any stale session cookie.
            if let Ok(v) = expire_cookie_value().parse() {
                resp.headers_mut().insert(header::SET_COOKIE, v);
            }
            resp
        }
    }
}

fn html_response(body: String) -> Response {
    let mut resp = (StatusCode::OK, body).into_response();
    if let Ok(v) = "text/html; charset=utf-8".parse() {
        resp.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    resp
}

/// Cookie value that immediately expires the session cookie (used for
/// logout and on tampered-cookie redirects).
pub fn expire_cookie_value() -> String {
    format!(
        "{}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0",
        SESSION_COOKIE_NAME
    )
}
