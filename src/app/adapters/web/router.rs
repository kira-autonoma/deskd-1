//! Axum router builder for the web adapter (#443).

use axum::{
    Router, middleware,
    routing::{get, post},
};

use super::middleware::headers::security_headers;
use super::routes::{
    agent_detail, cost, cost_feed, dashboard, github_webhook, health, login, logout, metrics, sse,
    static_assets,
};
use super::state::WebState;

/// Build the axum router. Public so the integration tests can drive it
/// in-process via `tower::ServiceExt::oneshot`.
pub fn build(state: WebState) -> Router {
    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/", get(dashboard::dashboard))
        .route("/events", get(sse::events))
        .route("/login", get(login::login_form))
        .route("/login/request", post(login::login_request))
        .route("/login/consume", get(login::login_consume))
        .route("/logout", post(logout::logout))
        .route("/webhooks/github", post(github_webhook::github_webhook))
        .route("/metrics/refresh", post(metrics::refresh))
        // ─── #473: cost & pipeline dashboard ────────────────────────────
        .route("/dashboard/cost", get(cost::dashboard))
        .route("/api/cost/feed", get(cost_feed::feed))
        // ─── #445: per-agent detail + structured commands ───────────────
        .route("/agent/{name}", get(agent_detail::detail))
        .route("/agent/{name}/events", get(agent_detail::events))
        .route("/agent/{name}/compact", post(agent_detail::compact))
        .route(
            "/agent/{name}/restart",
            post(agent_detail::restart_confirm_form),
        )
        .route("/agent/{name}/restart/confirm", post(agent_detail::restart))
        .route("/agent/{name}/stop", post(agent_detail::stop_confirm_form))
        .route("/agent/{name}/stop/confirm", post(agent_detail::stop))
        .route("/static/htmx.min.js", get(static_assets::htmx_js))
        .route("/static/htmx-sse.js", get(static_assets::htmx_sse_js))
        .route("/static/dashboard.css", get(static_assets::dashboard_css))
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}
