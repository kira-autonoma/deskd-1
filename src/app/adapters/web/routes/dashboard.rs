//! `GET /` — dashboard (#443/#444/#484). Redirects unauthenticated visitors
//! to /login. Authenticated users get the live agent overview built from
//! the existing registry/context_size/tasklog data sources (#444) plus the
//! top-of-page summary chart (#484).

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;

use crate::app::adapters::web::data;
use crate::app::adapters::web::data_chart;
use crate::app::adapters::web::routes::{SESSION_COOKIE_NAME, authenticate};
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::templates;
use crate::app::adapters::web::view;

/// Query params for the dashboard. Both keys are optional and fall back
/// to the chart's `Default` impls — the AC says invalid input must not
/// 5xx and must not lose the rest of the page, so we accept any string.
#[derive(Debug, Default, Deserialize)]
pub struct DashboardQuery {
    pub metric: Option<String>,
    pub period: Option<String>,
}

pub async fn dashboard(
    State(state): State<WebState>,
    Query(q): Query<DashboardQuery>,
    headers: HeaderMap,
) -> Response {
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
            // #484: top-of-page chart. Parse switcher state from query
            // params; unknown values fall back to defaults silently.
            let metric = q
                .metric
                .as_deref()
                .map(data_chart::Metric::parse)
                .unwrap_or_default();
            let period = q
                .period
                .as_deref()
                .map(data_chart::Period::parse)
                .unwrap_or_default();
            let series = data_chart::collect_chart_data(metric, period).await;
            let chart_html = view::chart::chart_block(&series, metric, period);
            let html = templates::dashboard_page(
                p.telegram_id,
                &p.csrf,
                &chart_html,
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
