//! `GET /agent/<name>/log` — log view (#485).
//!
//! Query params:
//! - `session` — optional session id; filters log lines containing that
//!   exact substring.
//! - `lines` — optional max line count (default 200, cap 2000).
//!
//! The agent log lives at `~/.deskd/logs/<name>.log`. Tailing is done
//! via [`data_log::tail_log`] which seeks from the end and reads in
//! 8 KiB blocks — the full file is never loaded into memory.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;

use crate::app::adapters::web::data;
use crate::app::adapters::web::data_log::{
    DEFAULT_LOG_LINES, MAX_LOG_LINES, agent_log_path, tail_log,
};
use crate::app::adapters::web::routes::authenticate;
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::{templates, view};

#[derive(Debug, Default, Deserialize)]
pub struct LogQuery {
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default)]
    pub lines: Option<usize>,
}

pub async fn log(
    State(state): State<WebState>,
    Path(name): Path<String>,
    Query(q): Query<LogQuery>,
    headers: HeaderMap,
) -> Response {
    let session_payload = match authenticate(&state, &headers) {
        Some(p) => p,
        None => return Redirect::to("/login").into_response(),
    };

    // 404 for unknown agent — symmetry with the detail page.
    if data::collect_agent_detail(&name).await.is_none() {
        return not_found_response(&name);
    }

    let max_lines = q
        .lines
        .map(|n| n.min(MAX_LOG_LINES))
        .unwrap_or(DEFAULT_LOG_LINES);
    let session_filter = q.session.as_deref().filter(|s| !s.is_empty());

    let path = agent_log_path(&name);
    let lines = tail_log(&path, max_lines, session_filter).unwrap_or_default();

    let crumbs = vec![
        view::Crumb::link("dashboard", "/"),
        view::Crumb::link(name.clone(), format!("/agent/{}", urlsafe(&name))),
        view::Crumb::current("log"),
    ];
    let breadcrumb_html = view::breadcrumb(&crumbs);
    let body_html = view::log_view(&name, session_filter, &lines);
    let html = templates::agent_drilldown_page(
        session_payload.telegram_id,
        &session_payload.csrf,
        "log view",
        &breadcrumb_html,
        &body_html,
    );
    html_response(StatusCode::OK, html)
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

fn urlsafe(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}
