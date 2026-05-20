//! Shared state for the web adapter (#443).
//!
//! `WebState` is a single struct shared across every axum handler via
//! `axum::extract::State`. It bundles config, the secret, the magic-link
//! token store, the rate limiter, the audit log writer, and the Telegram
//! dispatcher (a trait object so tests can stub it out).

use std::sync::{Arc, Mutex};

use crate::app::metrics::DiskMetrics;
use crate::config::{CostConfig, GitHubWebhookConfig, WebConfig};

use super::audit::AuditLog;
use super::auth::magic_link::TokenStore;
use super::data_cost::{CostCache, GhClient};
use super::dispatch::{AgentCommandDispatcher, BusSender, TelegramDispatcher};
use super::middleware::rate_limit::RateLimiter;
use super::routes::github_webhook::DeliveryDedupe;

/// Aggregate state passed through axum's `State` extractor.
#[derive(Clone)]
pub struct WebState {
    pub cfg: Arc<WebConfig>,
    pub secret: Arc<[u8; 32]>,
    pub tokens: Arc<TokenStore>,
    pub rate_limiter_ip: Arc<RateLimiter>,
    pub rate_limiter_tg: Arc<RateLimiter>,
    pub audit: AuditLog,
    pub telegram: Arc<dyn TelegramDispatcher>,
    /// GitHub webhook adapter config (#457). When `Some`, the
    /// `POST /webhooks/github` route is mounted and incoming deliveries are
    /// routed to subscribed agents via `bus`. `None` → route returns 404.
    pub github_webhooks: Option<Arc<GitHubWebhookConfig>>,
    /// Bus sender used by the GitHub webhook adapter (#457). Kept on state so
    /// integration tests can inject a recording sender. Production wiring
    /// shares one `BusDispatcher` for both `telegram` and `bus`.
    pub bus: Arc<dyn BusSender>,
    /// In-memory dedupe store for `X-GitHub-Delivery` ids — 10 minute TTL.
    /// Restart-safe loss is acceptable (GitHub retries deliveries on its own).
    pub github_deliveries: Arc<Mutex<DeliveryDedupe>>,
    /// Per-agent command dispatcher (#445). Published as `{command: "…"}`
    /// envelopes to `agent:<name>` on the bus by the production
    /// implementation; tests inject a recording double.
    pub agent_commands: Arc<dyn AgentCommandDispatcher>,
    /// Cached "now" provider — defaults to system time. Tests substitute a
    /// closure that returns a fixed timestamp so cookie/expiry semantics are
    /// deterministic.
    pub now: NowFn,
    /// Disk metrics cache + sampler handle (#446). Shared with the
    /// collector task; the dashboard reads `.snapshot()` and the
    /// `POST /metrics/refresh` route triggers an out-of-cycle sample.
    pub metrics: DiskMetrics,
    /// Resolved `(agent_name, home_dir)` pairs for the per-agent disk
    /// breakdown route. Empty when no agents are configured.
    pub agent_homes: Arc<Vec<(String, String)>>,
    /// Bus socket used by `/metrics/refresh` to emit `metrics.updated`
    /// after a manual sample. `None` in tests that don't run a bus.
    pub metrics_bus: Option<Arc<String>>,
    /// Cost & pipeline dashboard config (#473). When `None` the
    /// `/dashboard/cost` + `/api/cost/feed` routes return 404.
    pub cost: Option<Arc<CostConfig>>,
    /// Shared `gh issue list` client used by the cost dashboard. Always
    /// populated — production wires `CommandGhClient`, tests inject a
    /// recording double. Routes that don't need a `gh` client ignore it.
    pub gh: Arc<dyn GhClient>,
    /// 60-second `gh issue list` cache (#473). Shared with the SSE
    /// feed so a busy dashboard with many tabs only hits gh once per
    /// minute per repo.
    pub cost_cache: CostCache,
}

/// Returns a unix timestamp in seconds. Boxed so we can stub it out in tests.
pub type NowFn = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Default "now" implementation backed by `chrono::Utc::now()`.
pub fn system_now() -> NowFn {
    Arc::new(|| chrono::Utc::now().timestamp())
}
