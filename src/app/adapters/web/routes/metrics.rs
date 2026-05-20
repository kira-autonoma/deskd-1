//! `POST /metrics/refresh` — manual out-of-cycle disk metrics sample (#446).
//!
//! - CSRF-protected: the form field `_csrf` must match the token embedded
//!   in the signed session cookie.
//! - Rate-limited: 1 request / 30s. Repeat callers receive `429` with a
//!   `Retry-After` header.
//! - Success: triggers a fresh sample, persists the cache, emits the bus
//!   `metrics.updated` event, and returns the JSON snapshot.

use std::time::Duration;

use axum::{
    Form, Json,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;

use crate::app::adapters::web::routes::{authenticate, csrf_ok_for};
use crate::app::adapters::web::state::WebState;
use crate::app::metrics::{self, CollectorConfig, DiskSnapshot, RefreshGate};

/// 30 second rate-limit window. Single global slot (one user, one
/// dashboard tab in v1 — see issue spec).
pub const REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
pub struct RefreshForm {
    pub _csrf: String,
}

/// Wire the `POST /metrics/refresh` handler. Returns one of:
/// - 200 + JSON `DiskSnapshot` on success
/// - 302 → `/login` if the session cookie is missing
/// - 403 on CSRF mismatch
/// - 429 + `Retry-After` if called inside the rate-limit window
pub async fn refresh(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<RefreshForm>,
) -> Response {
    let payload = match authenticate(&state, &headers) {
        Some(p) => p,
        None => return Redirect::to("/login").into_response(),
    };
    if !csrf_ok_for(&state, &payload, &form._csrf) {
        return (StatusCode::FORBIDDEN, "invalid csrf").into_response();
    }

    let gate = metrics::check_refresh_gate(
        &state.metrics,
        REFRESH_MIN_INTERVAL,
        std::time::Instant::now(),
    )
    .await;
    if let RefreshGate::TooSoon { retry_after_secs } = gate {
        let mut resp = (StatusCode::TOO_MANY_REQUESTS, "refresh rate limited").into_response();
        if let Ok(v) = retry_after_secs.to_string().parse() {
            resp.headers_mut().insert(header::RETRY_AFTER, v);
        }
        return resp;
    }

    // Reconstruct the collector config from the resolved agent_homes
    // captured at startup. Volumes are derived from the previous
    // snapshot — if it's empty (first refresh on a brand-new process),
    // fall back to `["/"]` which matches the default.
    let prev = state.metrics.snapshot().await;
    let volumes = if prev.volumes.is_empty() {
        vec!["/".to_string()]
    } else {
        prev.volumes.iter().map(|v| v.mount.clone()).collect()
    };
    let cfg = CollectorConfig {
        interval: Duration::from_secs(300),
        volumes,
        agents: (*state.agent_homes).clone(),
    };

    let bus = state.metrics_bus.as_ref().map(|s| s.as_str().to_string());
    let snap: DiskSnapshot = metrics::run_sample(&state.metrics, &cfg, bus.as_deref()).await;

    let mut resp = (StatusCode::OK, Json(&snap)).into_response();
    if let Ok(v) = "application/json".parse() {
        resp.headers_mut().insert(header::CONTENT_TYPE, v);
    }
    resp
}
