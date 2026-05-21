//! `GET /agent/<name>/task/<task_id>` — task/session view (#485).
//!
//! Resolves the agent detail payload, then picks the recent-task entry
//! at index `task_id` (zero-based, newest-first to match the detail
//! page ordering). 404 when the agent is unknown or the index is out of
//! range. Wraps the page chrome in [`templates::agent_drilldown_page`]
//! with a breadcrumb that includes a back-link to the agent detail.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};

use crate::app::adapters::web::data;
use crate::app::adapters::web::routes::authenticate;
use crate::app::adapters::web::state::WebState;
use crate::app::adapters::web::{templates, view};

pub async fn task(
    State(state): State<WebState>,
    Path((name, task_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let session_payload = match authenticate(&state, &headers) {
        Some(p) => p,
        None => return Redirect::to("/login").into_response(),
    };

    let Ok(idx) = task_id.parse::<usize>() else {
        return not_found_response(&name, &task_id);
    };

    let detail = match data::collect_agent_detail(&name).await {
        Some(d) => d,
        None => return not_found_response(&name, &task_id),
    };

    let task_row = match detail.recent_tasks.get(idx) {
        Some(t) => t.clone(),
        None => return not_found_response(&name, &task_id),
    };

    let crumbs = vec![
        view::Crumb::link("dashboard", "/"),
        view::Crumb::link(name.clone(), format!("/agent/{}", urlsafe(&name))),
        view::Crumb::current(format!("task #{}", idx)),
    ];
    let breadcrumb_html = view::breadcrumb(&crumbs);
    let body_html = view::task_view(&name, &task_id, &detail.session_id, &task_row);
    let html = templates::agent_drilldown_page(
        session_payload.telegram_id,
        &session_payload.csrf,
        "task view",
        &breadcrumb_html,
        &body_html,
    );
    html_response(StatusCode::OK, html)
}

fn not_found_response(name: &str, task_id: &str) -> Response {
    let body = templates::agent_not_found_page(&format!("{}/task/{}", name, task_id));
    html_response(StatusCode::NOT_FOUND, body)
}

fn html_response(status: StatusCode, body: String) -> Response {
    let mut resp = (status, body).into_response();
    if let Ok(v) = "text/html; charset=utf-8".parse() {
        resp.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    resp
}

/// Replace anything that's not `[a-zA-Z0-9_-]` with `-` so agent names
/// can appear in URLs without further encoding. Mirrors the helpers
/// in `view/detail.rs` and `view/cards.rs`; kept local to avoid leaking
/// it through a public surface.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlsafe_replaces_special_chars() {
        assert_eq!(urlsafe("kira"), "kira");
        assert_eq!(urlsafe("dev/sub-1"), "dev-sub-1");
        assert_eq!(urlsafe("a b"), "a-b");
    }
}
