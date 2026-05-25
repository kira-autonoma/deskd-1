use crate::domain::config_types::{
    ConfigAgentKind, ConfigAgentRuntime, ConfigContextConfig, ConfigLaunchMode, ConfigSessionMode,
};
use crate::infra::dto::ConfigModelDef;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

// Re-export path helpers (infra layer — filesystem layout concerns).
pub use crate::infra::paths::{
    agent_bus_socket, ensure_dir_owned, log_dir, reminders_dir, reminders_dir_for, state_dir,
};

/// A reminder that fires at a specific time and posts a message to the bus.
///
/// One-shot when neither `interval` nor `cron_expression` is set (legacy
/// semantics). When `interval` or `cron_expression` is present, the reminder
/// reschedules itself after each fire — the source string is persisted so the
/// scheduler can re-parse after a deskd restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemindDef {
    /// ISO 8601 timestamp at which to fire next.
    pub at: String,
    /// Bus target (e.g. `agent:kira`).
    pub target: String,
    /// Payload text to post.
    pub message: String,
    /// Recurring interval as a duration string (e.g. "30m", "2h", "1d").
    /// Mutually exclusive with `cron_expression`. Min effective tick: 1 minute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    /// Recurring cron expression (5-field UTC). Mutually exclusive with
    /// `interval`. Min effective tick: 1 minute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron_expression: Option<String>,
}

fn default_max_turns() -> u32 {
    100
}

// ─── Root workspace.yaml ─────────────────────────────────────────────────────

/// Top-level workspace config (workspace.yaml).
/// Managed by root or the admin user. Defines top-level agents, their unix
/// users, Telegram bots, and the path to each agent's own deskd.yaml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    #[serde(default)]
    pub agents: Vec<AgentDef>,
    /// Named container profiles that agents can reference by name.
    #[serde(default)]
    pub containers: HashMap<String, ContainerConfig>,
    /// Named rooms — formal groupings of agents with shared context.
    /// When present, `room_list` returns these instead of treating each agent as a room.
    #[serde(default)]
    pub rooms: Vec<RoomDef>,
    /// Telegram user IDs allowed to run admin commands (/restart, etc.).
    #[serde(default)]
    pub admin_telegram_ids: Vec<i64>,
    /// A2A protocol configuration for cross-instance agent communication.
    #[serde(default)]
    pub a2a: Option<A2aConfig>,
    /// Proactive degradation alerts (#425). When set, deskd serve starts an
    /// alert manager per agent that fires verdict-transition alerts to the
    /// configured sinks.
    #[serde(default)]
    pub alerts: Option<AlertsConfig>,
    /// Web control panel (#443). When `enabled: true`, deskd serve starts an
    /// axum HTTP server bound to `bind` exposing magic-link Telegram auth and
    /// (eventually) agent dashboards. Absent or `enabled: false` → no impact.
    #[serde(default)]
    pub web: Option<WebConfig>,
    /// Federated bus (#462). When present, deskd participates in a federated
    /// mesh-wide bus over Tailscale. The block has optional `hub` and `peer`
    /// sub-blocks — each can be independently enabled. Absent or both
    /// disabled → no impact on existing single-host deployments.
    #[serde(default)]
    pub federation: Option<FederationConfig>,
    /// GitHub webhook adapter (#457). When set, deskd's web adapter exposes
    /// `POST /webhooks/github` and routes signed payloads to subscribed agent
    /// inboxes. Requires `web.enabled: true`; absent → endpoint not mounted.
    #[serde(default)]
    pub github_webhooks: Option<GitHubWebhookConfig>,
    /// Disk/metrics collection (#446). When absent the disk collector still
    /// runs with defaults (5 min interval, single volume `/`). The block is
    /// optional purely so existing workspace.yaml files continue to parse
    /// without modification.
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
    /// Cost & pipeline dashboard (#473). When present (with non-empty repos
    /// list) the web adapter mounts `/dashboard/cost` + `/api/cost/feed` and
    /// queries `gh` for the configured repos. Absent → routes return 404 and
    /// no cost data is collected.
    #[serde(default)]
    pub cost: Option<CostConfig>,
}

/// Top-level metrics block — `metrics:` in workspace.yaml (#446).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct MetricsConfig {
    /// Disk-metrics sub-block.
    #[serde(default)]
    pub disk: DiskMetricsConfig,
}

/// Disk-metrics configuration. Defaults mirror the issue spec: 300s and `["/"]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiskMetricsConfig {
    /// Sample interval in seconds. Default 300 (5 min).
    #[serde(default = "default_disk_interval_seconds")]
    pub interval_seconds: u64,
    /// Mountpoints passed to `df -BK`. Default `["/"]`.
    #[serde(default = "default_disk_volumes")]
    pub volumes: Vec<String>,
}

impl Default for DiskMetricsConfig {
    fn default() -> Self {
        Self {
            interval_seconds: default_disk_interval_seconds(),
            volumes: default_disk_volumes(),
        }
    }
}

fn default_disk_interval_seconds() -> u64 {
    300
}

fn default_disk_volumes() -> Vec<String> {
    vec!["/".to_string()]
}

/// Federation block — `federation:` in workspace.yaml (#462).
///
/// Holds hub + peer config. Both are optional; either or both may be enabled
/// on the same deskd instance (a hub can also peer with another hub).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct FederationConfig {
    /// Hub config — when enabled, deskd listens for incoming peer connections
    /// on a TCP socket bound to the Tailscale interface.
    #[serde(default)]
    pub hub: Option<FederationHubConfig>,
    /// Peer config — when enabled, deskd dials a remote hub on startup and
    /// stays connected with reconnect/backoff.
    #[serde(default)]
    pub peer: Option<FederationPeerConfig>,
}

/// Hub-side federation config.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FederationHubConfig {
    /// Master switch. `false` (or block absent) → listener does not bind.
    #[serde(default)]
    pub enabled: bool,
    /// Bind spec: either `interface:port` (e.g. `tailscale0:7770`) or
    /// explicit `ip:port` (e.g. `100.64.0.1:7770`). Default `tailscale0:7770`.
    #[serde(default = "default_federation_bind")]
    pub bind: String,
    /// Idle disconnect timeout in seconds. Default 60.
    #[serde(default = "default_federation_peer_timeout")]
    pub peer_timeout_secs: u64,
}

impl Default for FederationHubConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_federation_bind(),
            peer_timeout_secs: default_federation_peer_timeout(),
        }
    }
}

/// Peer-side federation config.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FederationPeerConfig {
    /// Master switch. `false` (or block absent) → dialer does not start.
    #[serde(default)]
    pub enabled: bool,
    /// Hub address (`host:port`). MagicDNS / IP / tailnet hostname accepted.
    #[serde(default)]
    pub hub_addr: String,
    /// Name this peer self-identifies as in the hub's peer registry.
    #[serde(default)]
    pub peer_name: String,
    /// Reconnect backoff sequence in seconds. Empty / missing → default
    /// `[1, 2, 5, 15, 60]`.
    #[serde(default = "default_federation_backoff")]
    pub reconnect_backoff_secs: Vec<u64>,
    /// Topic-glob subscriptions the peer issues to the hub on every
    /// connect (#463). Wildcards: `*` one segment, `>` everything below.
    #[serde(default)]
    pub subscribe: Vec<String>,
    /// Inbox subscriptions issued on every connect — preserves the existing
    /// `inbox/<name>` shape (#463).
    #[serde(default, alias = "subscribe_inbox")]
    pub subscribe_inboxes: Vec<String>,
}

impl Default for FederationPeerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            hub_addr: String::new(),
            peer_name: String::new(),
            reconnect_backoff_secs: default_federation_backoff(),
            subscribe: Vec::new(),
            subscribe_inboxes: Vec::new(),
        }
    }
}

fn default_federation_bind() -> String {
    "tailscale0:7770".to_string()
}

fn default_federation_peer_timeout() -> u64 {
    60
}

fn default_federation_backoff() -> Vec<u64> {
    vec![1, 2, 5, 15, 60]
}

/// GitHub webhook adapter config — `github_webhooks:` in workspace.yaml (#457).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct GitHubWebhookConfig {
    /// Shared HMAC-SHA256 secret configured on the GitHub webhook. Typically
    /// supplied via `${GITHUB_WEBHOOK_SECRET}`. Empty string → all incoming
    /// requests are rejected with 401 (signature can never match).
    #[serde(default)]
    pub secret: String,
    /// Per-repo subscription rules. Multiple subscriptions for the same repo
    /// are permitted (e.g. fan-out to several agents).
    #[serde(default)]
    pub subscriptions: Vec<GitHubWebhookSubscription>,
}

/// One subscription entry. Matches when `repo` equals
/// `payload.repository.full_name` and the resolved event-type is in `events`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GitHubWebhookSubscription {
    /// `owner/name`, e.g. `kgatilin/archai`.
    pub repo: String,
    /// Event-type tokens. Format: `X-GitHub-Event` value, optionally with a
    /// `.action` suffix (e.g. `pull_request.closed`, `issues.labeled`,
    /// `pull_request_review.submitted`). Bare events match any action.
    #[serde(default)]
    pub events: Vec<String>,
    /// Bus target receiving the message, e.g. `agent:kira`.
    pub deliver_to: String,
}

/// Web control panel config — `web:` in workspace.yaml (#443).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebConfig {
    /// Master switch. `false` (or block absent) → adapter is not started.
    #[serde(default)]
    pub enabled: bool,
    /// Local socket bind address. Reverse proxy is expected to terminate TLS
    /// in front of this; do not bind publicly. Default: `127.0.0.1:8127`.
    #[serde(default = "default_web_bind")]
    pub bind: String,
    /// Public-facing base URL used to construct magic-link login URLs.
    /// Example: `https://deskd.example.com`. Trailing slash is tolerated.
    /// Optional when [`trust_transport`] is `true` — the magic-link path is
    /// unreachable so no URL is needed.
    #[serde(default)]
    pub external_url: Option<String>,
    /// Session cookie lifetime in days. Default 30.
    #[serde(default = "default_session_ttl_days")]
    pub session_ttl_days: u32,
    /// Magic-link token TTL in seconds. Default 300 (5 min).
    #[serde(default = "default_magic_link_ttl_secs")]
    pub magic_link_ttl_seconds: u64,
    /// Whitelist of Telegram user IDs allowed to log in. Empty = nobody.
    #[serde(default)]
    pub allowed_telegram_ids: Vec<i64>,
    /// Path to the JSONL audit log. `~` is expanded against `$HOME`.
    /// Default: `~/.deskd/logs/web-audit.jsonl`.
    #[serde(default = "default_web_audit_log")]
    pub audit_log: String,
    /// Rate limit configuration.
    #[serde(default)]
    pub rate_limit: WebRateLimitConfig,
    /// When true, the adapter trusts the transport layer for identity (e.g.
    /// Tailscale, VPN). Bypasses magic-link auth entirely — any request
    /// reaching the bound address is treated as authenticated. ONLY safe
    /// when [`bind`] is on a non-public interface. Default `false` (auth
    /// required). When `true`, [`external_url`] becomes optional and
    /// [`allowed_telegram_ids`] is ignored.
    #[serde(default)]
    pub trust_transport: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebRateLimitConfig {
    /// Maximum auth requests per IP (and per telegram_id) per rolling hour.
    /// Default 20.
    #[serde(default = "default_auth_requests_per_hour")]
    pub auth_requests_per_hour: u32,
}

impl Default for WebRateLimitConfig {
    fn default() -> Self {
        Self {
            auth_requests_per_hour: default_auth_requests_per_hour(),
        }
    }
}

fn default_web_bind() -> String {
    "127.0.0.1:8127".to_string()
}

fn default_session_ttl_days() -> u32 {
    30
}

fn default_magic_link_ttl_secs() -> u64 {
    300
}

fn default_auth_requests_per_hour() -> u32 {
    20
}

fn default_web_audit_log() -> String {
    "~/.deskd/logs/web-audit.jsonl".to_string()
}

/// Cost & pipeline dashboard config — `cost:` in workspace.yaml (#473).
///
/// Maps `est:S | M | L | XL` issue labels to token counts so the dashboard
/// can render an estimate column. Repositories listed in `repos` are queried
/// via `gh` for open `agent-ready` issues (pipeline) and recently-closed
/// issues (history). `weekly_ceiling` drives the optional budget bar; if
/// unset, the bar is omitted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CostConfig {
    /// Bucket → token mapping. Default: S=10k, M=50k, L=200k, XL=500k.
    #[serde(default = "default_cost_buckets")]
    pub buckets: CostBuckets,
    /// Optional weekly token ceiling. Drives the budget bar at the top of
    /// `/dashboard/cost`. When `None` the bar is omitted.
    #[serde(default)]
    pub weekly_ceiling: Option<u64>,
    /// How many days of closed-ticket history to surface. Default 7.
    #[serde(default = "default_cost_history_days")]
    pub history_days: u32,
    /// `owner/repo` strings queried via `gh`. Empty → no pipeline / history.
    #[serde(default)]
    pub repos: Vec<String>,
    /// Issue label used to flag «ready for an agent to pick up». Default
    /// `agent-ready` matches the rest of the Nassau ecosystem.
    #[serde(default = "default_cost_ready_label")]
    pub ready_label: String,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            buckets: default_cost_buckets(),
            weekly_ceiling: None,
            history_days: default_cost_history_days(),
            repos: Vec::new(),
            ready_label: default_cost_ready_label(),
        }
    }
}

/// `est:<S|M|L|XL>` → token-count mapping. Configurable per the AC.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CostBuckets {
    #[serde(rename = "S", alias = "s")]
    pub s: u64,
    #[serde(rename = "M", alias = "m")]
    pub m: u64,
    #[serde(rename = "L", alias = "l")]
    pub l: u64,
    #[serde(rename = "XL", alias = "xl")]
    pub xl: u64,
}

impl CostBuckets {
    /// Resolve `est:<bucket>` to a token count. Returns `None` for unknown
    /// labels — the renderer surfaces «unknown» so the operator can spot
    /// missing labels at a glance.
    pub fn lookup(&self, bucket: &str) -> Option<u64> {
        match bucket.to_ascii_uppercase().as_str() {
            "S" => Some(self.s),
            "M" => Some(self.m),
            "L" => Some(self.l),
            "XL" => Some(self.xl),
            _ => None,
        }
    }
}

fn default_cost_buckets() -> CostBuckets {
    CostBuckets {
        s: 10_000,
        m: 50_000,
        l: 200_000,
        xl: 500_000,
    }
}

fn default_cost_history_days() -> u32 {
    7
}

fn default_cost_ready_label() -> String {
    "agent-ready".to_string()
}

/// Alert configuration block — `alerts:` in workspace.yaml.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct AlertsConfig {
    /// Sinks that receive each alert. Order is preserved; sink failures are
    /// isolated per-sink so a failing telegram does not block log/bus.
    #[serde(default)]
    pub sinks: Vec<AlertSinkConfig>,
    /// How often the alert manager polls the verdict source, in seconds.
    /// Defaults to 60s.
    #[serde(default = "default_alert_poll_secs")]
    pub poll_interval_secs: u64,
}

fn default_alert_poll_secs() -> u64 {
    60
}

/// A single alert sink. Three kinds are supported (#425):
/// `bus_message`, `telegram`, and `log`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AlertSinkConfig {
    /// Publish the alert as a bus message to `agent:<target_agent>`.
    BusMessage { target_agent: String },
    /// Send the alert as a Telegram message via the existing telegram
    /// adapter — `chat_id` is a string so `${ADMIN_CHAT_ID}` env-var
    /// expansion works without the YAML parser tripping on negative ints.
    Telegram { chat_id: String },
    /// Append the alert as a single-line JSON record to `path`.
    Log { path: String },
}

/// A2A protocol configuration in workspace.yaml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct A2aConfig {
    /// Public URL for this deskd instance (e.g. "https://dev.agent.example.com").
    pub url: String,
    /// API key for authenticating incoming A2A requests.
    /// Typically set via ${A2A_API_KEY}.
    #[serde(default)]
    pub api_key: Option<String>,
    /// HTTP listen address for the A2A server (e.g. "0.0.0.0:3000").
    #[serde(default = "default_a2a_listen")]
    pub listen: String,
    /// Instance-level description shown in the Agent Card.
    #[serde(default)]
    pub description: Option<String>,
    /// Authentication mode: "api_key" (default), "jwt", or "none".
    #[serde(default = "default_a2a_auth")]
    pub auth: String,
    /// Path to Ed25519 private key PEM for JWT signing.
    /// Default: ~/.deskd/a2a_key.pem
    #[serde(default)]
    pub private_key: Option<String>,
    /// Trusted public keys for JWT verification (base64url-encoded Ed25519 keys).
    /// Incoming JWTs are verified against these keys. Add remote agents' public keys here.
    #[serde(default)]
    pub trusted_keys: Vec<String>,
}

fn default_a2a_listen() -> String {
    "0.0.0.0:3000".to_string()
}

fn default_a2a_auth() -> String {
    "api_key".to_string()
}

/// A room is a named work context: namespace + context folder + set of agents.
/// Rooms are the primary drill-down unit in dashboard navigation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomDef {
    pub name: String,
    /// Working directory for the room (agents inherit this).
    pub work_dir: String,
    /// Path to a context file (e.g. CLAUDE.md) shared by all agents in the room.
    #[serde(default)]
    pub context: Option<String>,
    /// Names of agents that belong to this room (must be defined in `agents` section).
    #[serde(default)]
    pub agents: Vec<String>,
}

/// Telegram bot adapter config. Defined per-agent — each agent has its own bot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramConfig {
    /// Bot token from @BotFather. Typically set via ${TELEGRAM_BOT_TOKEN}.
    pub token: String,
    /// Maximum size in bytes for a single attachment (PDF, doc, voice, etc.)
    /// before it is rejected with a friendly Telegram reply. Defaults to 20 MB.
    /// Photos use the same cap when downloaded as multimodal images.
    #[serde(default = "default_max_attachment_bytes")]
    pub max_attachment_bytes: u64,
}

fn default_max_attachment_bytes() -> u64 {
    20 * 1024 * 1024
}

/// Discord bot adapter config. Defined per-agent in workspace.yaml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscordConfig {
    /// Bot token from Discord Developer Portal. Typically set via ${DISCORD_BOT_TOKEN}.
    pub token: String,
}

/// Discord channel routing config in the per-user deskd.yaml.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiscordRoutesConfig {
    pub routes: Vec<DiscordRoute>,
}

/// A single Discord channel route.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiscordRoute {
    /// Discord channel ID (u64).
    pub channel_id: u64,
    /// Human-readable name for this channel, shown to the agent as context.
    pub name: Option<String>,
}

/// Container runtime config for an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerConfig {
    /// OCI image to use (e.g. "claude-code-local:official").
    pub image: String,
    /// Host paths to bind-mount. Format: "host_path" or "host_path:container_path"
    /// or "host_path:container_path:ro" for read-only.
    #[serde(default)]
    pub mounts: Vec<String>,
    /// Docker volumes. Format: "volume_name:container_path".
    #[serde(default)]
    pub volumes: Vec<String>,
    /// Environment variables to set inside the container.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Container runtime binary. Defaults to "docker".
    #[serde(default = "default_container_runtime")]
    pub runtime: String,
}

fn default_container_runtime() -> String {
    "docker".to_string()
}

/// Definition of a top-level agent in workspace.yaml.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDef {
    pub name: String,
    /// Linux user to run the agent as. Required for isolation.
    pub unix_user: Option<String>,
    /// Agent's working directory (also determines bus socket path).
    pub work_dir: String,
    /// Path to the agent's own deskd.yaml. Defaults to {work_dir}/deskd.yaml.
    pub config: Option<String>,
    /// Telegram bot for this agent. When set, a Telegram adapter is started
    /// on the agent's bus when deskd serves this workspace.
    pub telegram: Option<TelegramConfig>,
    /// Discord bot for this agent. When set, a Discord adapter is started
    /// on the agent's bus when deskd serves this workspace.
    pub discord: Option<DiscordConfig>,
    /// Claude model override. Default is set in the agent's deskd.yaml.
    #[serde(default)]
    pub model: Option<String>,
    /// Command to run as the agent process. Defaults to ["claude"].
    #[serde(default = "default_command")]
    pub command: Vec<String>,
    /// Container config. When set, the agent process runs inside a container.
    #[serde(default)]
    pub container: Option<ContainerConfig>,
    /// Agent runtime protocol: claude (default) or acp.
    #[serde(default)]
    pub runtime: ConfigAgentRuntime,
    /// Launch mode: subprocess (default) or tmux (#452).
    /// When `tmux`, the agent's Claude REPL is launched inside a detached
    /// `tmux` session named `deskd-<agent>` rather than as a direct subprocess
    /// of `deskd serve`. Useful for long-lived REPLs that need to survive
    /// operator disconnects and receive MCP channel events (#451).
    #[serde(default)]
    pub launch_mode: ConfigLaunchMode,
}

impl AgentDef {
    /// Derive the path to the agent's deskd.yaml config file.
    pub fn config_path(&self) -> String {
        self.config.clone().unwrap_or_else(|| {
            PathBuf::from(&self.work_dir)
                .join("deskd.yaml")
                .to_string_lossy()
                .into_owned()
        })
    }

    /// Derive the agent's bus socket path.
    pub fn bus_socket(&self) -> String {
        agent_bus_socket(&self.work_dir)
    }
}

fn default_command() -> Vec<String> {
    vec!["claude".to_string()]
}

impl WorkspaceConfig {
    /// Load and parse a workspace config file, expanding ${ENV_VAR} references.
    /// Resolves named container profile references (string → inline config).
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read workspace config: {}", path))?;
        let expanded = expand_env_vars(&raw);
        let expanded = Self::resolve_container_profiles(&expanded)?;
        let cfg: WorkspaceConfig =
            serde_yaml::from_str(&expanded).context("failed to parse workspace config")?;
        Ok(cfg)
    }

    /// Pre-process YAML: resolve string container references to inline objects.
    ///
    /// When an agent's `container:` field is a string (e.g. `container: personal`),
    /// replace it with the corresponding entry from the top-level `containers:` map.
    fn resolve_container_profiles(yaml_str: &str) -> Result<String> {
        let mut doc: serde_yaml::Value =
            serde_yaml::from_str(yaml_str).context("failed to pre-parse workspace config")?;

        // Extract named container profiles.
        let profiles = doc
            .get("containers")
            .and_then(|v| v.as_mapping())
            .cloned()
            .unwrap_or_default();

        if profiles.is_empty() {
            // No named profiles — nothing to resolve, return original.
            return Ok(yaml_str.to_string());
        }

        // Resolve string references in agents[].container.
        if let Some(agents) = doc.get_mut("agents").and_then(|v| v.as_sequence_mut()) {
            for agent in agents.iter_mut() {
                if let Some(container_val) = agent.get("container")
                    && let Some(profile_name) = container_val.as_str()
                {
                    let key = serde_yaml::Value::String(profile_name.to_string());
                    let resolved = profiles.get(&key).cloned().ok_or_else(|| {
                        anyhow::anyhow!(
                            "agent references unknown container profile '{}'",
                            profile_name
                        )
                    })?;
                    agent
                        .as_mapping_mut()
                        .unwrap()
                        .insert(serde_yaml::Value::String("container".to_string()), resolved);
                }
            }
        }

        serde_yaml::to_string(&doc).context("failed to re-serialize workspace config")
    }
}

// ─── Runtime serve state ────────────────────────────────────────────────────

/// Runtime state written by `deskd serve` so other commands can auto-discover config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServeState {
    /// Path to the workspace.yaml that serve was started with.
    pub workspace_config: String,
    /// ISO 8601 timestamp when serve started.
    pub started_at: String,
    /// Per-agent runtime info.
    #[serde(default)]
    pub agents: HashMap<String, AgentServeState>,
    /// Formal room definitions from workspace config.
    #[serde(default)]
    pub rooms: Vec<RoomDef>,
}

/// Per-agent runtime info in serve state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentServeState {
    pub work_dir: String,
    pub bus_socket: String,
    pub config_path: String,
}

impl ServeState {
    /// Path to the serve state file: `~/.deskd/serve.state.yaml`.
    pub fn path() -> PathBuf {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        PathBuf::from(home).join(".deskd").join("serve.state.yaml")
    }

    /// Load serve state from disk. Returns None if not running.
    pub fn load() -> Option<Self> {
        let path = Self::path();
        let content = std::fs::read_to_string(&path).ok()?;
        serde_yaml::from_str(&content).ok()
    }

    /// Write serve state to disk.
    pub fn save(&self) -> Result<()> {
        let path = Self::path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let content = serde_yaml::to_string(self).context("failed to serialize serve state")?;
        std::fs::write(&path, content).context("failed to write serve state")?;
        Ok(())
    }

    /// Remove the serve state file (on shutdown).
    pub fn remove() {
        let _ = std::fs::remove_file(Self::path());
    }

    /// Find the first agent that has a user config with SM models.
    pub fn find_agent_config(&self) -> Option<&AgentServeState> {
        self.agents.values().next()
    }

    /// Find a specific agent by name.
    pub fn agent(&self, name: &str) -> Option<&AgentServeState> {
        self.agents.get(name)
    }

    /// Get any bus socket from the running agents.
    pub fn any_bus_socket(&self) -> Option<&str> {
        self.agents.values().next().map(|a| a.bus_socket.as_str())
    }
}

// ─── Per-user deskd.yaml ─────────────────────────────────────────────────────

/// Per-user agent config (deskd.yaml, lives in the agent's work_dir).
/// Defines the agent's own model, system prompt, sub-agents, channels,
/// Telegram routes, and schedules. Managed by the agent's unix user.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct UserConfig {
    /// Claude model for the main agent. Overridden by workspace.yaml `model` if set.
    #[serde(default = "default_model")]
    pub model: String,
    /// System prompt for the main agent.
    #[serde(default)]
    pub system_prompt: String,
    /// Max turns per task.
    #[serde(default = "default_max_turns")]
    pub max_turns: u32,
    /// Named broadcast/task channels this agent participates in.
    #[serde(default)]
    pub channels: Vec<ChannelDef>,
    /// Sub-agents spawned and managed within this agent's bus scope.
    #[serde(default)]
    pub agents: Vec<SubAgentDef>,
    /// Telegram channel routing for this agent.
    pub telegram: Option<TelegramRoutesConfig>,
    /// Discord channel routing for this agent.
    pub discord: Option<DiscordRoutesConfig>,
    /// Scheduled actions (cron → bus messages).
    #[serde(default)]
    pub schedules: Vec<ScheduleDef>,
    /// MCP server config JSON string or file path, passed to claude via --mcp-config.
    /// Example: '{"mcpServers":{"deskd":{"command":"deskd","args":["mcp","--agent","kira"]}}}'
    #[serde(default)]
    pub mcp_config: Option<String>,
    /// State machine model definitions.
    #[serde(default)]
    pub models: Vec<ConfigModelDef>,
    /// Context system configuration (main branch, compaction).
    #[serde(default)]
    pub context: Option<ConfigContextConfig>,
    /// A2A skills advertised in the Agent Card.
    #[serde(default)]
    pub skills: Vec<SkillDef>,
    /// A2A needs — what this agent wants done (custom extension to A2A spec).
    #[serde(default)]
    pub needs: Vec<NeedDef>,
    /// Optional allow-list of inboxes this top-level agent can read (glob patterns).
    /// If `None` (absent in deskd.yaml), behavior is unrestricted (current default).
    /// If `Some`, only the agent's own inbox plus any inbox matching one of the
    /// listed patterns is readable. Useful on shared-user systems (e.g. macOS dev
    /// laptops) where unix-level file permissions cannot isolate agent inboxes.
    /// Example: `inbox_acl: ["dev", "collab-*"]`.
    #[serde(default)]
    pub inbox_acl: Option<Vec<String>>,
    /// Auto-compact threshold in absolute tokens (top-level / global default for
    /// this deskd.yaml). Sub-agents inherit this when their own SubAgentDef does
    /// not set one. Built-in fallback is `DEFAULT_AUTO_COMPACT_THRESHOLD` (300k).
    #[serde(default)]
    pub auto_compact_threshold_tokens: Option<u64>,
    /// Optional directory of standalone agent files (`*.agent.md`).
    /// Path is resolved relative to this deskd.yaml's parent directory.
    /// File-defined agents merge into `agents`; on name collision the file
    /// definition replaces the inline one. See `infra::agent_file`.
    #[serde(default)]
    pub agents_dir: Option<String>,
    /// Optional path to the agent's state document (markdown). Read/written by
    /// the `agent_state` MCP tool. `~` is expanded against `$HOME`. When unset,
    /// `agent_state` falls back to `~/.claude/projects/-home-<agent>/memory/current_state.md`.
    #[serde(default)]
    pub state_file: Option<String>,
    /// Launch mode for the top-level agent this deskd.yaml belongs to:
    /// `subprocess` (default) or `tmux` (#452). When `tmux`,
    /// `deskd agent start <agent>` (no flag) launches the Claude REPL inside a
    /// detached tmux session named `deskd-<agent>`.
    #[serde(default)]
    pub launch_mode: ConfigLaunchMode,
    /// Cross-user bus routing: maps `agent:<name>` targets to a foreign unix
    /// socket path so `send_message` can reach a top-level agent owned by a
    /// different unix user. Each value is a path to that agent's bus socket
    /// (typically `/home/<user>/.deskd/bus.sock`). The remote socket and its
    /// containing directory must be reachable by this agent's unix user
    /// (permissions are not relaxed by deskd — see `bus_server.rs` which sets
    /// the socket itself to `0o777`, but the containing dir is owned by the
    /// remote user). Example:
    ///     cross_user_agents:
    ///       kira: /home/kira/.deskd/bus.sock
    /// When `agent:kira` is targeted, the message is delivered to kira's bus
    /// instead of falling back to the local internal/parent bus.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cross_user_agents: Option<HashMap<String, String>>,
}

/// An A2A skill advertised in the Agent Card (per A2A spec).
/// Defined in deskd.yaml under `skills:`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillDef {
    /// Unique skill identifier (e.g. "code-review").
    pub id: String,
    /// Human-readable name (e.g. "Code Review").
    pub name: String,
    /// What this skill does.
    #[serde(default)]
    pub description: String,
    /// Tags for discovery (e.g. ["go", "rust"]).
    #[serde(default)]
    pub tags: Vec<String>,
}

/// An A2A need — what the agent wants done (custom extension).
/// Defined in deskd.yaml under `needs:`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NeedDef {
    /// Unique need identifier (e.g. "want-restart-button").
    pub id: String,
    /// Human-readable description of what's needed.
    pub description: String,
    /// Tags for discovery (e.g. ["ux", "telegram"]).
    #[serde(default)]
    pub tags: Vec<String>,
    /// Priority: "low", "medium", "high".
    #[serde(default = "default_need_priority")]
    pub priority: String,
}

fn default_need_priority() -> String {
    "medium".to_string()
}

fn default_model() -> String {
    "claude-sonnet-4-6".to_string()
}

/// A named channel for broadcast or task-queue communication.
/// The name becomes the bus target, e.g. `news:ecosystem` or `queue:reviews`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelDef {
    pub name: String,
    pub description: String,
}

// Re-export domain types for backward compatibility.
pub use crate::domain::agent::{AgentRuntime, SessionMode};

/// Scope type for sub-agents: controls isolation level.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ScopeType {
    /// Sub-agent inherits parent's full scope — sees siblings, same FS access.
    #[default]
    Inherit,
    /// Sub-agent gets isolated sub-scope — sees only its own children.
    Narrow,
}

/// A sub-agent running within a parent agent's bus scope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubAgentDef {
    pub name: String,
    pub model: String,
    #[serde(default)]
    pub system_prompt: String,
    /// Bus targets this agent receives messages from.
    /// Supports glob patterns: `telegram.in:*`, `agent:researcher`.
    pub subscribe: Vec<String>,
    /// Optional allow-list of targets this agent can publish to.
    /// If None, publish to any target is allowed.
    pub publish: Option<Vec<String>>,
    /// Optional allow-list of inboxes this agent can read (glob patterns).
    /// If None, the agent can only read its own inbox (matching its name).
    /// Example: `["kira", "collab-*"]` allows reading the `kira` inbox and
    /// any inbox starting with `collab-`.
    pub inbox_read: Option<Vec<String>>,
    /// Scope type: inherit (default) or narrow.
    /// Inherit = shares parent scope; narrow = isolated sub-scope.
    #[serde(default)]
    pub scope: ScopeType,
    /// Optional allow-list of targets this agent can send messages to.
    /// If None, the agent can message any target (unrestricted).
    /// Example: `["agent:parent", "agent:sibling"]`.
    pub can_message: Option<Vec<String>>,
    /// Optional working directory override. Must be under parent's work_dir.
    pub work_dir: Option<String>,
    /// Optional environment variables for this agent. Isolated from parent env.
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
    /// Session mode: persistent (default) or ephemeral.
    /// Ephemeral agents start a fresh session for each task.
    #[serde(default)]
    pub session: ConfigSessionMode,
    /// Agent runtime protocol: claude (default) or acp.
    #[serde(default)]
    pub runtime: ConfigAgentRuntime,
    /// Launch mode: subprocess (default) or tmux (#452).
    /// When `tmux`, the agent's Claude REPL is launched inside a detached
    /// tmux session named `deskd-<agent>` (see `deskd agent start --tmux`).
    #[serde(default)]
    pub launch_mode: ConfigLaunchMode,
    /// Worker loop kind: executor (default, full lifecycle) or context (lightweight Q&A).
    /// Context agents have no tool access, no task queue, no inbox — they answer
    /// questions from their loaded context with minimal overhead.
    #[serde(default)]
    pub kind: ConfigAgentKind,
    /// Per-agent context configuration (overrides global UserConfig.context).
    #[serde(default)]
    pub context: Option<ConfigContextConfig>,
    /// Memory agent: context usage fraction (0.0–1.0) that triggers compaction.
    /// Default: 0.8. Only used when runtime is `memory`.
    #[serde(default)]
    pub compact_threshold: Option<f64>,
    /// Memory agent: compaction strategy name. Default: "smart".
    /// Only used when runtime is `memory`.
    #[serde(default)]
    pub compact_strategy: Option<String>,
    /// Auto-compact threshold in absolute tokens for this sub-agent.
    /// Falls back to the parent UserConfig's `auto_compact_threshold_tokens`,
    /// then to the built-in default (300k).
    #[serde(default)]
    pub auto_compact_threshold_tokens: Option<u64>,
    /// Empty-completion auto-restart threshold (issue #424). After this many
    /// consecutive zero-token / sub-2s completions, the worker is restarted.
    /// `None` falls back to the workspace default.
    #[serde(default)]
    pub empty_completion_threshold: Option<u32>,
    /// Minimum seconds between auto-restarts triggered by empty-completion
    /// detection (rate-limit). `None` falls back to the workspace default.
    #[serde(default)]
    pub empty_completion_restart_min_secs: Option<u64>,
    /// Optional path to this sub-agent's state document (markdown), read/written
    /// by the `agent_state` MCP tool. `~` is expanded against `$HOME`. When unset,
    /// `agent_state` falls back to `~/.claude/projects/-home-<name>/memory/current_state.md`.
    #[serde(default)]
    pub state_file: Option<String>,
}

impl SubAgentDef {
    /// Returns the full scoped name for this agent under the given parent.
    pub fn scoped_name(&self, parent: &str) -> String {
        format!("{}/{}", parent, self.name)
    }

    /// Validate that work_dir is within parent's work_dir (scope containment).
    pub fn validate_work_dir(&self, parent_work_dir: &str) -> anyhow::Result<()> {
        if let Some(ref wd) = self.work_dir {
            let child = std::path::Path::new(wd)
                .canonicalize()
                .unwrap_or_else(|_| wd.into());
            let parent = std::path::Path::new(parent_work_dir)
                .canonicalize()
                .unwrap_or_else(|_| parent_work_dir.into());
            if !child.starts_with(&parent) {
                anyhow::bail!(
                    "sub-agent work_dir '{}' is outside parent scope '{}'",
                    wd,
                    parent_work_dir
                );
            }
        }
        Ok(())
    }
}

/// Telegram channel routing config in the per-user deskd.yaml.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TelegramRoutesConfig {
    #[serde(default)]
    pub routes: Vec<TelegramRoute>,
}

/// A single Telegram chat route.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TelegramRoute {
    /// Telegram chat_id (positive for users/groups, negative for channels/supergroups).
    pub chat_id: i64,
    /// If true, only respond when the bot is @mentioned in this chat.
    #[serde(default)]
    pub mention_only: bool,
    /// Human-readable name for this chat, shown to the agent as context.
    pub name: Option<String>,
    /// Bus target override. When set, incoming messages from this chat are published
    /// to this target (e.g. "agent:collab") instead of the default "telegram.in:<chat_id>".
    #[serde(default)]
    pub route_to: Option<String>,
}

/// A scheduled action that fires on a cron expression and posts a message to the bus.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScheduleDef {
    /// Cron expression, e.g. `"0 9 * * *"` for 9 AM daily.
    pub cron: String,
    /// Bus target to post to.
    pub target: String,
    /// What action to take when the schedule fires.
    pub action: ScheduleAction,
    /// Action-specific configuration (e.g. repos list for github_poll).
    pub config: Option<serde_yaml::Value>,
    /// IANA timezone name (e.g. "Europe/Berlin"). Cron fires in this timezone.
    /// Falls back to UTC if not specified.
    #[serde(default)]
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleAction {
    /// Poll GitHub repos for issues with a label, post new issues to target.
    GithubPoll,
    /// Post a static payload string to the target.
    Raw,
    /// Run an arbitrary shell command via `sh -c`.
    /// `config.command` — the shell command to execute.
    /// If the command produces stdout and `target` is non-empty, stdout is posted to the bus.
    Shell,
}

pub use crate::domain::statemachine::{ModelDef, TransitionDef};

impl UserConfig {
    /// Load and parse a deskd.yaml file, expanding ${ENV_VAR} references.
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read user config: {}", path))?;
        let expanded = expand_env_vars(&raw);
        let mut cfg: UserConfig =
            serde_yaml::from_str(&expanded).context("failed to parse user config")?;
        cfg.merge_agents_dir(path)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// If `agents_dir` is set, load every `*.agent.md` file beneath it and
    /// merge the resulting sub-agents and schedules into this config.
    /// `config_path` is the path to this deskd.yaml; its parent directory
    /// is the base for resolving a relative `agents_dir`. On name collision
    /// the file-defined agent replaces the inline one.
    fn merge_agents_dir(&mut self, config_path: &str) -> Result<()> {
        let Some(dir_rel) = self.agents_dir.clone() else {
            return Ok(());
        };
        let base = std::path::Path::new(config_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let agents_dir = base.join(&dir_rel);
        let loaded = crate::infra::agent_file::load_agent_dir(&agents_dir)
            .with_context(|| format!("failed to load agents_dir: {}", agents_dir.display()))?;
        for entry in loaded {
            if let Some(idx) = self.agents.iter().position(|a| a.name == entry.agent.name) {
                self.agents[idx] = entry.agent;
            } else {
                self.agents.push(entry.agent);
            }
            self.schedules.extend(entry.schedules);
        }
        Ok(())
    }

    /// Validate config invariants that serde can't express on its own.
    pub fn validate(&self) -> Result<()> {
        if let Some(0) = self.auto_compact_threshold_tokens {
            anyhow::bail!("auto_compact_threshold_tokens must be > 0");
        }
        for sub in &self.agents {
            if let Some(0) = sub.auto_compact_threshold_tokens {
                anyhow::bail!(
                    "agent '{}': auto_compact_threshold_tokens must be > 0",
                    sub.name
                );
            }
        }
        Ok(())
    }
}

// ─── Env var expansion ────────────────────────────────────────────────────────

/// Replace `${VAR}` and `$VAR` occurrences with their environment variable values.
/// Unknown variables are left as-is.
fn expand_env_vars(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' {
            if chars.peek() == Some(&'{') {
                chars.next(); // consume '{'
                let var: String = chars.by_ref().take_while(|&c| c != '}').collect();
                if let Ok(val) = std::env::var(&var) {
                    result.push_str(&val);
                } else {
                    result.push_str(&format!("${{{}}}", var));
                }
            } else if chars
                .peek()
                .map(|c| c.is_alphanumeric() || *c == '_')
                .unwrap_or(false)
            {
                let mut var = String::new();
                while chars
                    .peek()
                    .map(|c| c.is_alphanumeric() || *c == '_')
                    .unwrap_or(false)
                {
                    var.push(chars.next().unwrap());
                }
                if let Ok(val) = std::env::var(&var) {
                    result.push_str(&val);
                } else {
                    result.push_str(&format!("${}", var));
                }
            } else {
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_env_vars_braces() {
        // Serialize env mutation; setenv is not thread-safe on POSIX.
        let _env_guard = crate::test_support::env_lock().blocking_lock();
        unsafe { std::env::set_var("TEST_TOKEN_DESKD", "abc123") };
        let result = expand_env_vars("token: ${TEST_TOKEN_DESKD}");
        assert_eq!(result, "token: abc123");
    }

    #[test]
    fn test_expand_env_vars_dollar() {
        // Serialize env mutation; setenv is not thread-safe on POSIX.
        let _env_guard = crate::test_support::env_lock().blocking_lock();
        unsafe { std::env::set_var("TEST_VAR_DESKD", "hello") };
        let result = expand_env_vars("val: $TEST_VAR_DESKD end");
        assert_eq!(result, "val: hello end");
    }

    #[test]
    fn test_expand_env_vars_unknown_left_as_is() {
        let result = expand_env_vars("val: ${DEFINITELY_NOT_SET_XYZ123}");
        assert_eq!(result, "val: ${DEFINITELY_NOT_SET_XYZ123}");
    }

    #[test]
    fn test_workspace_config_minimal() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
    unix_user: kira
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].name, "kira");
        assert_eq!(cfg.agents[0].unix_user.as_deref(), Some("kira"));
        assert!(cfg.agents[0].telegram.is_none());
        assert!(cfg.agents[0].config.is_none());
    }

    /// Legacy `workspace.yaml` files in the wild still carry `budget_usd:`
    /// per-agent (the field was dropped in #498). The parser must silently
    /// ignore the field rather than fail — serde tolerates unknown fields
    /// by default, so AgentDef must NOT opt into `deny_unknown_fields`.
    #[test]
    fn test_workspace_config_ignores_legacy_budget_usd_field() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
    unix_user: kira
    budget_usd: 50.0
"#;
        let cfg: WorkspaceConfig =
            serde_yaml::from_str(yaml).expect("legacy budget_usd field must be ignored, not error");
        assert_eq!(cfg.agents[0].name, "kira");
        assert_eq!(cfg.agents[0].work_dir, "/home/kira");
    }

    #[test]
    fn test_workspace_config_alerts_block() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
alerts:
  sinks:
    - kind: bus_message
      target_agent: dev
    - kind: telegram
      chat_id: "-1001234"
    - kind: log
      path: /var/log/deskd/alerts.jsonl
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let alerts = cfg.alerts.expect("alerts block parsed");
        assert_eq!(alerts.sinks.len(), 3);
        assert_eq!(alerts.poll_interval_secs, 60);
        assert!(matches!(
            alerts.sinks[0],
            AlertSinkConfig::BusMessage { ref target_agent } if target_agent == "dev"
        ));
        assert!(matches!(
            alerts.sinks[1],
            AlertSinkConfig::Telegram { ref chat_id } if chat_id == "-1001234"
        ));
        assert!(matches!(
            alerts.sinks[2],
            AlertSinkConfig::Log { ref path } if path == "/var/log/deskd/alerts.jsonl"
        ));
    }

    #[test]
    fn test_workspace_config_alerts_default_absent() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.alerts.is_none());
    }

    #[test]
    fn test_workspace_config_web_block_full() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
web:
  enabled: true
  bind: 127.0.0.1:8127
  external_url: https://deskd.example.com
  session_ttl_days: 30
  magic_link_ttl_seconds: 300
  allowed_telegram_ids: [123456, 987654]
  audit_log: ~/.deskd/logs/web-audit.jsonl
  rate_limit:
    auth_requests_per_hour: 20
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let web = cfg.web.expect("web block parsed");
        assert!(web.enabled);
        assert_eq!(web.bind, "127.0.0.1:8127");
        assert_eq!(
            web.external_url.as_deref(),
            Some("https://deskd.example.com")
        );
        assert!(!web.trust_transport);
        assert_eq!(web.session_ttl_days, 30);
        assert_eq!(web.magic_link_ttl_seconds, 300);
        assert_eq!(web.allowed_telegram_ids, vec![123456i64, 987654i64]);
        assert_eq!(web.audit_log, "~/.deskd/logs/web-audit.jsonl");
        assert_eq!(web.rate_limit.auth_requests_per_hour, 20);
    }

    #[test]
    fn test_workspace_config_web_block_absent() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.web.is_none());
    }

    #[test]
    fn test_workspace_config_web_minimal_uses_defaults() {
        // Every field has a default — including `external_url`, which is now
        // optional (None when `trust_transport: true`).
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
web:
  external_url: https://deskd.example.com
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let web = cfg.web.expect("web block parsed with defaults");
        assert!(!web.enabled, "default enabled is false");
        assert_eq!(web.bind, "127.0.0.1:8127");
        assert_eq!(web.session_ttl_days, 30);
        assert_eq!(web.magic_link_ttl_seconds, 300);
        assert!(web.allowed_telegram_ids.is_empty());
        assert_eq!(web.rate_limit.auth_requests_per_hour, 20);
        assert!(!web.trust_transport);
    }

    #[test]
    fn test_workspace_config_web_trust_transport_omits_external_url() {
        // `trust_transport: true` is for Tailscale-internal deployments —
        // the magic-link path is unreachable, so `external_url` can be
        // omitted entirely.
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
web:
  enabled: true
  bind: 100.64.0.1:8127
  trust_transport: true
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let web = cfg.web.expect("web block parsed");
        assert!(web.enabled);
        assert!(web.trust_transport);
        assert!(web.external_url.is_none());
    }

    #[test]
    fn test_workspace_config_cost_block_full() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
cost:
  buckets:
    S: 12000
    M: 60000
    L: 250000
    XL: 600000
  weekly_ceiling: 5000000
  history_days: 14
  repos:
    - kgatilin/deskd
    - mikeshogin/archlint
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let cost = cfg.cost.expect("cost block parsed");
        assert_eq!(cost.buckets.s, 12_000);
        assert_eq!(cost.buckets.m, 60_000);
        assert_eq!(cost.buckets.l, 250_000);
        assert_eq!(cost.buckets.xl, 600_000);
        assert_eq!(cost.weekly_ceiling, Some(5_000_000));
        assert_eq!(cost.history_days, 14);
        assert_eq!(cost.repos.len(), 2);
        assert_eq!(cost.ready_label, "agent-ready");
    }

    #[test]
    fn test_workspace_config_cost_block_defaults() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
cost:
  repos:
    - kgatilin/deskd
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let cost = cfg.cost.expect("cost block parsed");
        assert_eq!(cost.buckets.s, 10_000);
        assert_eq!(cost.buckets.m, 50_000);
        assert_eq!(cost.buckets.l, 200_000);
        assert_eq!(cost.buckets.xl, 500_000);
        assert!(cost.weekly_ceiling.is_none());
        assert_eq!(cost.history_days, 7);
        assert_eq!(cost.ready_label, "agent-ready");
    }

    #[test]
    fn test_workspace_config_cost_buckets_lookup() {
        let buckets = default_cost_buckets();
        assert_eq!(buckets.lookup("S"), Some(10_000));
        assert_eq!(buckets.lookup("M"), Some(50_000));
        assert_eq!(buckets.lookup("L"), Some(200_000));
        assert_eq!(buckets.lookup("XL"), Some(500_000));
        assert_eq!(buckets.lookup("xl"), Some(500_000));
        assert_eq!(buckets.lookup("XXL"), None);
    }

    #[test]
    fn test_workspace_config_web_disabled() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
web:
  enabled: false
  external_url: https://deskd.example.com
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let web = cfg.web.expect("web block parsed");
        assert!(!web.enabled);
    }

    #[test]
    fn test_workspace_config_federation_block_absent() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.federation.is_none());
    }

    #[test]
    fn test_workspace_config_federation_hub_full() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
federation:
  hub:
    enabled: true
    bind: tailscale0:7770
    peer_timeout_secs: 90
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let fed = cfg.federation.expect("federation block parsed");
        let hub = fed.hub.expect("hub block parsed");
        assert!(hub.enabled);
        assert_eq!(hub.bind, "tailscale0:7770");
        assert_eq!(hub.peer_timeout_secs, 90);
        assert!(fed.peer.is_none());
    }

    #[test]
    fn test_workspace_config_federation_hub_disabled_does_not_listen() {
        // Block present but disabled → caller treats this as zero-impact.
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
federation:
  hub:
    enabled: false
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let hub = cfg.federation.unwrap().hub.unwrap();
        assert!(!hub.enabled);
        // Defaults are still populated for stability.
        assert_eq!(hub.bind, "tailscale0:7770");
        assert_eq!(hub.peer_timeout_secs, 60);
    }

    #[test]
    fn test_workspace_config_federation_peer_full() {
        let yaml = r#"
agents:
  - name: mac
    work_dir: /home/mac
federation:
  peer:
    enabled: true
    hub_addr: vps.example.ts.net:7770
    peer_name: mac
    reconnect_backoff_secs: [1, 5, 30]
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let peer = cfg.federation.unwrap().peer.unwrap();
        assert!(peer.enabled);
        assert_eq!(peer.hub_addr, "vps.example.ts.net:7770");
        assert_eq!(peer.peer_name, "mac");
        assert_eq!(peer.reconnect_backoff_secs, vec![1, 5, 30]);
    }

    #[test]
    fn test_workspace_config_federation_peer_backoff_default() {
        let yaml = r#"
agents:
  - name: mac
    work_dir: /home/mac
federation:
  peer:
    enabled: true
    hub_addr: vps:7770
    peer_name: mac
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let peer = cfg.federation.unwrap().peer.unwrap();
        assert_eq!(peer.reconnect_backoff_secs, vec![1, 2, 5, 15, 60]);
    }

    #[test]
    fn test_workspace_config_admin_telegram_ids() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
admin_telegram_ids:
  - 123456789
  - 987654321
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.admin_telegram_ids, vec![123456789i64, 987654321i64]);
    }

    #[test]
    fn test_workspace_config_admin_telegram_ids_default() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.admin_telegram_ids.is_empty());
    }

    #[test]
    fn test_workspace_config_with_telegram() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
    unix_user: kira
    telegram:
      token: "bot-token-123"
  - name: dev
    work_dir: /home/dev
    unix_user: dev
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.agents[0].telegram.is_some());
        assert_eq!(
            cfg.agents[0].telegram.as_ref().unwrap().token,
            "bot-token-123"
        );
        assert!(cfg.agents[1].telegram.is_none());
    }

    #[test]
    fn test_agent_def_bus_socket() {
        let def = AgentDef {
            name: "kira".into(),
            unix_user: Some("kira".into()),
            work_dir: "/home/kira".into(),
            config: None,
            telegram: None,
            discord: None,
            model: None,
            command: vec!["claude".into()],
            container: None,
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
        };
        assert_eq!(def.bus_socket(), "/home/kira/.deskd/bus.sock");
        assert_eq!(def.config_path(), "/home/kira/deskd.yaml");
    }

    #[test]
    fn test_agent_def_explicit_config_path() {
        let def = AgentDef {
            name: "kira".into(),
            unix_user: None,
            work_dir: "/home/kira".into(),
            config: Some("/etc/agents/kira.yaml".into()),
            telegram: None,
            discord: None,
            model: None,
            command: vec!["claude".into()],
            container: None,
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
        };
        assert_eq!(def.config_path(), "/etc/agents/kira.yaml");
    }

    #[test]
    fn test_user_config_defaults() {
        let yaml = r#"
model: claude-opus-4-6
system_prompt: "You are Kira."
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.model, "claude-opus-4-6");
        assert_eq!(cfg.max_turns, 100);
        assert!(cfg.channels.is_empty());
        assert!(cfg.agents.is_empty());
        assert!(cfg.schedules.is_empty());
    }

    #[test]
    fn test_user_config_full() {
        let yaml = r#"
model: claude-opus-4-6
system_prompt: "You are Kira."

channels:
  - name: "news:ecosystem"
    description: "Ecosystem updates"
  - name: "queue:reviews"
    description: "PR review requests"

agents:
  - name: dev
    model: claude-sonnet-4-6
    system_prompt: "You implement code."
    subscribe:
      - "agent:dev"
    publish:
      - "agent:*"
      - "telegram.out:*"

  - name: researcher
    model: claude-haiku-4-5
    system_prompt: "You research topics."
    subscribe:
      - "agent:researcher"

telegram:
  routes:
    - chat_id: -1001234567890
    - chat_id: -1001234567891

schedules:
  - cron: "0 9 * * *"
    target: "telegram.out:-1001234567890"
    action: raw
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.channels.len(), 2);
        assert_eq!(cfg.agents.len(), 2);
        assert_eq!(cfg.agents[0].subscribe, vec!["agent:dev"]);
        assert_eq!(cfg.agents[0].publish.as_ref().unwrap().len(), 2);
        assert!(cfg.agents[1].publish.is_none()); // allow all
        assert_eq!(cfg.telegram.unwrap().routes[0].chat_id, -1001234567890);
        assert_eq!(cfg.schedules.len(), 1);
    }

    #[test]
    fn test_sub_agent_session_mode() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"

agents:
  - name: worker
    model: claude-haiku-4-5
    system_prompt: "Worker"
    subscribe:
      - "agent:worker"
    session: ephemeral

  - name: researcher
    model: claude-sonnet-4-6
    system_prompt: "Researcher"
    subscribe:
      - "agent:researcher"
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].session, ConfigSessionMode::Ephemeral);
        assert_eq!(cfg.agents[1].session, ConfigSessionMode::Persistent); // default
    }

    #[test]
    fn test_workspace_config_with_container() {
        let yaml = r#"
agents:
  - name: dev
    work_dir: /home/dev
    container:
      image: claude-code-local:official
      mounts:
        - "~/.ssh:ro"
        - "~/.gitconfig:ro"
      volumes:
        - "claude-history:/commandhistory"
      env:
        GH_TOKEN: "my-token"
    command: [claude, --output-format, stream-json]
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let agent = &cfg.agents[0];
        assert!(agent.container.is_some());
        let c = agent.container.as_ref().unwrap();
        assert_eq!(c.image, "claude-code-local:official");
        assert_eq!(c.mounts.len(), 2);
        assert_eq!(c.volumes.len(), 1);
        assert_eq!(c.env.get("GH_TOKEN").unwrap(), "my-token");
        assert_eq!(c.runtime, "docker");
    }

    #[test]
    fn test_workspace_config_no_container() {
        let yaml = r#"
agents:
  - name: dev
    work_dir: /home/dev
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.agents[0].container.is_none());
    }

    #[test]
    fn test_workspace_config_with_rooms() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
  - name: dev
    work_dir: /home/dev
rooms:
  - name: features
    work_dir: ~/work/project
    context: ~/work/project/CLAUDE.md
    agents: [kira]
  - name: reviews
    work_dir: ~/work/reviews
    agents: [dev]
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.rooms.len(), 2);
        assert_eq!(cfg.rooms[0].name, "features");
        assert_eq!(cfg.rooms[0].work_dir, "~/work/project");
        assert_eq!(
            cfg.rooms[0].context.as_deref(),
            Some("~/work/project/CLAUDE.md")
        );
        assert_eq!(cfg.rooms[0].agents, vec!["kira"]);
        assert_eq!(cfg.rooms[1].name, "reviews");
        assert!(cfg.rooms[1].context.is_none());
    }

    #[test]
    fn test_workspace_config_rooms_default_empty() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.rooms.is_empty());
    }

    #[test]
    fn test_telegram_route_with_route_to() {
        let yaml = r#"
telegram:
  routes:
    - chat_id: -1001234567890
      name: "personal"
    - chat_id: -1001234567891
      name: "collab"
      mention_only: true
      route_to: "agent:collab"
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        let routes = cfg.telegram.unwrap().routes;
        assert_eq!(routes.len(), 2);
        assert!(routes[0].route_to.is_none());
        assert_eq!(routes[1].route_to.as_deref(), Some("agent:collab"));
        assert!(routes[1].mention_only);
    }

    #[test]
    fn test_agent_def_config_path_uses_agent_config_when_set() {
        // When an agent defines config_path in workspace.yaml, that path is used
        // for loading schedules and other agent-level config.
        let def = AgentDef {
            name: "family".into(),
            unix_user: Some("family".into()),
            work_dir: "/home/family".into(),
            config: Some("/home/family/deskd.yaml".into()),
            telegram: None,
            discord: None,
            model: None,
            command: vec!["claude".into()],
            container: None,
            runtime: Default::default(),
            launch_mode: Default::default(),
        };
        assert_eq!(def.config_path(), "/home/family/deskd.yaml");
    }

    #[test]
    fn test_agent_def_config_path_defaults_to_work_dir() {
        // When config is not set, config_path defaults to {work_dir}/deskd.yaml.
        let def = AgentDef {
            name: "family".into(),
            unix_user: Some("family".into()),
            work_dir: "/home/family".into(),
            config: None,
            telegram: None,
            discord: None,
            model: None,
            command: vec!["claude".into()],
            container: None,
            runtime: Default::default(),
            launch_mode: Default::default(),
        };
        assert_eq!(def.config_path(), "/home/family/deskd.yaml");
    }

    #[test]
    fn test_user_config_with_schedules() {
        // Verify that schedules defined in an agent-level deskd.yaml are parsed
        // correctly, supporting all three action types.
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Family assistant"

schedules:
  - cron: "3 7 * * *"
    target: "agent:family"
    action: raw
    config: "Morning brief"
  - cron: "3 21 * * *"
    target: "agent:family"
    action: github_poll
    config:
      repos:
        - kgatilin/deskd
      label: agent-ready
  - cron: "7 22 * * *"
    target: "agent:family"
    action: shell
    config:
      command: "echo receipts"
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.schedules.len(), 3);
        assert_eq!(cfg.schedules[0].cron, "3 7 * * *");
        assert!(matches!(cfg.schedules[0].action, ScheduleAction::Raw));
        assert!(matches!(
            cfg.schedules[1].action,
            ScheduleAction::GithubPoll
        ));
        assert!(matches!(cfg.schedules[2].action, ScheduleAction::Shell));
    }

    #[test]
    fn test_sub_agent_context_config() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"

agents:
  - name: memory-arch
    model: claude-haiku-4-5
    system_prompt: "You hold architecture context."
    subscribe:
      - "agent:memory-arch"
    context:
      enabled: true
      main_path: contexts/arch-main.yaml
      main_budget_tokens: 50000
      compact_threshold_tokens: 40000

  - name: worker
    model: claude-sonnet-4-6
    system_prompt: "Worker"
    subscribe:
      - "agent:worker"

context:
  enabled: true
  main_path: contexts/default.yaml
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        // Per-agent context on memory-arch
        let ctx = cfg.agents[0].context.as_ref().unwrap();
        assert!(ctx.enabled);
        assert_eq!(ctx.main_path.as_deref(), Some("contexts/arch-main.yaml"));
        assert_eq!(ctx.main_budget_tokens, Some(50000));
        assert_eq!(ctx.compact_threshold_tokens, Some(40000));
        // Worker has no per-agent context
        assert!(cfg.agents[1].context.is_none());
        // Global context fallback exists
        let global = cfg.context.as_ref().unwrap();
        assert!(global.enabled);
        assert_eq!(global.main_path.as_deref(), Some("contexts/default.yaml"));
    }

    #[test]
    fn test_sub_agent_memory_runtime() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"

agents:
  - name: memory-all
    model: claude-haiku-4-5
    system_prompt: "You accumulate bus events as context."
    subscribe:
      - "agent:memory-all"
      - "agent:*"
    runtime: memory
    compact_threshold: 0.75
    compact_strategy: aggressive

  - name: worker
    model: claude-sonnet-4-6
    system_prompt: "Worker"
    subscribe:
      - "agent:worker"
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].runtime, ConfigAgentRuntime::Memory);
        assert_eq!(cfg.agents[0].compact_threshold, Some(0.75));
        assert_eq!(
            cfg.agents[0].compact_strategy.as_deref(),
            Some("aggressive")
        );
        // Worker has default runtime (Claude).
        assert_eq!(cfg.agents[1].runtime, ConfigAgentRuntime::Claude);
        assert!(cfg.agents[1].compact_threshold.is_none());
        assert!(cfg.agents[1].compact_strategy.is_none());
    }

    #[test]
    fn test_workspace_agent_acp_runtime() {
        // Issue #93: top-level AgentDef must accept `runtime: acp`.
        let yaml = r#"
agents:
  - name: dev
    unix_user: dev
    work_dir: /home/dev
    runtime: acp
    command:
      - gemini
      - --acp
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].runtime, ConfigAgentRuntime::Acp);
        assert_eq!(cfg.agents[0].command, vec!["gemini", "--acp"]);
    }

    #[test]
    fn test_workspace_agent_runtime_defaults_to_claude() {
        // When `runtime` is unset, behavior must be identical to today.
        let yaml = r#"
agents:
  - name: classic
    unix_user: dev
    work_dir: /home/dev
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].runtime, ConfigAgentRuntime::Claude);
    }

    #[test]
    fn test_sub_agent_acp_runtime() {
        // Sub-agents (in deskd.yaml) also accept runtime: acp.
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "parent"

agents:
  - name: acp-child
    model: ignored
    system_prompt: "ACP sub-agent"
    subscribe:
      - "agent:acp-child"
    runtime: acp
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].runtime, ConfigAgentRuntime::Acp);
    }

    #[test]
    fn test_sub_agent_memory_defaults() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"

agents:
  - name: memory-all
    model: claude-haiku-4-5
    system_prompt: "Memory agent"
    subscribe:
      - "agent:*"
    runtime: memory
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].runtime, ConfigAgentRuntime::Memory);
        // Defaults are None — worker.rs applies the actual defaults.
        assert!(cfg.agents[0].compact_threshold.is_none());
        assert!(cfg.agents[0].compact_strategy.is_none());
    }

    #[test]
    fn test_resolve_container_profiles_string_ref() {
        let yaml = r#"
containers:
  personal:
    image: claude-code:latest
    mounts:
      - "/home/dev/.ssh:/home/dev/.ssh:ro"
    env:
      TOKEN: secret
agents:
  - name: dev
    work_dir: /home/dev
    container: personal
"#;
        let resolved = WorkspaceConfig::resolve_container_profiles(yaml).unwrap();
        let cfg: WorkspaceConfig = serde_yaml::from_str(&resolved).unwrap();
        let container = cfg.agents[0].container.as_ref().unwrap();
        assert_eq!(container.image, "claude-code:latest");
        assert_eq!(container.mounts, vec!["/home/dev/.ssh:/home/dev/.ssh:ro"]);
        assert_eq!(container.env.get("TOKEN").unwrap(), "secret");
    }

    #[test]
    fn test_resolve_container_profiles_inline_unchanged() {
        let yaml = r#"
containers:
  personal:
    image: ignored
agents:
  - name: dev
    work_dir: /home/dev
    container:
      image: inline-image
      env:
        KEY: val
"#;
        let resolved = WorkspaceConfig::resolve_container_profiles(yaml).unwrap();
        let cfg: WorkspaceConfig = serde_yaml::from_str(&resolved).unwrap();
        let container = cfg.agents[0].container.as_ref().unwrap();
        assert_eq!(container.image, "inline-image");
    }

    #[test]
    fn test_resolve_container_profiles_unknown_errors() {
        let yaml = r#"
containers:
  personal:
    image: claude-code:latest
agents:
  - name: dev
    work_dir: /home/dev
    container: nonexistent
"#;
        let result = WorkspaceConfig::resolve_container_profiles(yaml);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown container profile")
        );
    }

    #[test]
    fn test_resolve_container_profiles_no_profiles_section() {
        let yaml = r#"
agents:
  - name: dev
    work_dir: /home/dev
"#;
        let resolved = WorkspaceConfig::resolve_container_profiles(yaml).unwrap();
        let cfg: WorkspaceConfig = serde_yaml::from_str(&resolved).unwrap();
        assert!(cfg.agents[0].container.is_none());
    }

    #[test]
    fn test_resolve_container_profiles_multiple_agents() {
        let yaml = r#"
containers:
  personal:
    image: claude-personal
  work:
    image: claude-work
    env:
      API_KEY: work-key
agents:
  - name: dev
    work_dir: /home/dev
    container: personal
  - name: ops
    work_dir: /home/ops
    container: work
  - name: bare
    work_dir: /home/bare
"#;
        let resolved = WorkspaceConfig::resolve_container_profiles(yaml).unwrap();
        let cfg: WorkspaceConfig = serde_yaml::from_str(&resolved).unwrap();
        assert_eq!(
            cfg.agents[0].container.as_ref().unwrap().image,
            "claude-personal"
        );
        assert_eq!(
            cfg.agents[1].container.as_ref().unwrap().image,
            "claude-work"
        );
        assert_eq!(
            cfg.agents[1]
                .container
                .as_ref()
                .unwrap()
                .env
                .get("API_KEY")
                .unwrap(),
            "work-key"
        );
        assert!(cfg.agents[2].container.is_none());
    }

    // ─── Scope tests ────────────────────────────────────────────────────────

    #[test]
    fn test_scope_type_defaults_to_inherit() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"
agents:
  - name: worker
    model: claude-haiku-4-5
    system_prompt: "Worker"
    subscribe: ["agent:worker"]
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].scope, ScopeType::Inherit);
    }

    #[test]
    fn test_scope_type_narrow() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"
agents:
  - name: worker
    model: claude-haiku-4-5
    system_prompt: "Worker"
    subscribe: ["agent:worker"]
    scope: narrow
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].scope, ScopeType::Narrow);
    }

    #[test]
    fn test_can_message_config() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"
agents:
  - name: worker
    model: claude-haiku-4-5
    system_prompt: "Worker"
    subscribe: ["agent:worker"]
    can_message: ["agent:parent", "telegram.out:*"]
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        let cm = cfg.agents[0].can_message.as_ref().unwrap();
        assert_eq!(cm, &["agent:parent", "telegram.out:*"]);
    }

    #[test]
    fn test_can_message_default_unrestricted() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"
agents:
  - name: worker
    model: claude-haiku-4-5
    system_prompt: "Worker"
    subscribe: ["agent:worker"]
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.agents[0].can_message.is_none());
    }

    #[test]
    fn test_sub_agent_work_dir_config() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"
agents:
  - name: worker
    model: claude-haiku-4-5
    system_prompt: "Worker"
    subscribe: ["agent:worker"]
    work_dir: /home/dev/tasks/abc
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            cfg.agents[0].work_dir.as_deref(),
            Some("/home/dev/tasks/abc")
        );
    }

    #[test]
    fn test_sub_agent_env_config() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"
agents:
  - name: worker
    model: claude-haiku-4-5
    system_prompt: "Worker"
    subscribe: ["agent:worker"]
    env:
      ANTHROPIC_API_KEY: sk-worker-key
      CUSTOM_VAR: hello
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        let env = cfg.agents[0].env.as_ref().unwrap();
        assert_eq!(env.get("ANTHROPIC_API_KEY").unwrap(), "sk-worker-key");
        assert_eq!(env.get("CUSTOM_VAR").unwrap(), "hello");
        assert_eq!(env.len(), 2);
    }

    #[test]
    fn test_scoped_name() {
        let yaml = r#"
model: claude-sonnet-4-6
system_prompt: "Test"
agents:
  - name: worker
    model: claude-haiku-4-5
    system_prompt: "Worker"
    subscribe: ["agent:worker"]
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.agents[0].scoped_name("dev"), "dev/worker");
    }

    #[test]
    fn test_validate_work_dir_within_parent() {
        let sub = SubAgentDef {
            name: "worker".into(),
            model: "haiku".into(),
            system_prompt: String::new(),
            subscribe: vec![],
            publish: None,
            inbox_read: None,
            scope: ScopeType::Narrow,
            can_message: None,
            work_dir: Some("/tmp/parent/child".into()),
            env: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            compact_strategy: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
            state_file: None,
        };
        assert!(sub.validate_work_dir("/tmp/parent").is_ok());
    }

    #[test]
    fn test_validate_work_dir_outside_parent_fails() {
        let sub = SubAgentDef {
            name: "worker".into(),
            model: "haiku".into(),
            system_prompt: String::new(),
            subscribe: vec![],
            publish: None,
            inbox_read: None,
            scope: ScopeType::Narrow,
            can_message: None,
            work_dir: Some("/etc/evil".into()),
            env: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            compact_strategy: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
            state_file: None,
        };
        assert!(sub.validate_work_dir("/tmp/parent").is_err());
    }

    // ─── agents_dir merge tests ──────────────────────────────────────────────

    fn write_file(path: &std::path::Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }

    #[test]
    fn user_config_load_without_agents_dir_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("deskd.yaml");
        write_file(
            &cfg_path,
            "model: claude-sonnet-4-6\nagents:\n  - name: inline\n    model: haiku\n    subscribe: []\n",
        );
        let cfg = UserConfig::load(cfg_path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.agents.len(), 1);
        assert_eq!(cfg.agents[0].name, "inline");
        assert!(cfg.agents_dir.is_none());
        assert!(cfg.schedules.is_empty());
    }

    #[test]
    fn user_config_load_merges_agents_dir() {
        let dir = tempfile::tempdir().unwrap();
        let agents_subdir = dir.path().join("agents.d");
        std::fs::create_dir_all(&agents_subdir).unwrap();
        write_file(
            &agents_subdir.join("blog.agent.md"),
            "---\nmodel: claude-sonnet-4-6\njobs:\n  - cron: \"0 30 8 * * *\"\n    prompt: \"morning\"\n---\n\nblog body prompt\n",
        );
        let cfg_path = dir.path().join("deskd.yaml");
        write_file(
            &cfg_path,
            "model: claude-sonnet-4-6\nagents_dir: agents.d\nagents:\n  - name: inline\n    model: haiku\n    subscribe: []\n",
        );

        let cfg = UserConfig::load(cfg_path.to_str().unwrap()).unwrap();
        let names: Vec<_> = cfg.agents.iter().map(|a| a.name.as_str()).collect();
        assert!(names.contains(&"inline"));
        assert!(names.contains(&"blog"));
        assert_eq!(cfg.schedules.len(), 1);
        assert_eq!(cfg.schedules[0].target, "agent:blog");
    }

    #[test]
    fn user_config_load_file_overrides_inline_on_collision() {
        let dir = tempfile::tempdir().unwrap();
        let agents_subdir = dir.path().join("agents.d");
        std::fs::create_dir_all(&agents_subdir).unwrap();
        write_file(
            &agents_subdir.join("worker.agent.md"),
            "---\nname: worker\nmodel: claude-opus-4-6\n---\n\nfile body\n",
        );
        let cfg_path = dir.path().join("deskd.yaml");
        write_file(
            &cfg_path,
            "model: claude-sonnet-4-6\nagents_dir: agents.d\nagents:\n  - name: worker\n    model: haiku\n    subscribe: []\n    system_prompt: inline body\n",
        );

        let cfg = UserConfig::load(cfg_path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.agents.len(), 1);
        assert_eq!(cfg.agents[0].name, "worker");
        assert_eq!(cfg.agents[0].model, "claude-opus-4-6");
        assert!(cfg.agents[0].system_prompt.contains("file body"));
    }

    #[test]
    fn user_config_load_missing_agents_dir_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("deskd.yaml");
        write_file(
            &cfg_path,
            "model: claude-sonnet-4-6\nagents_dir: does-not-exist\n",
        );
        let cfg = UserConfig::load(cfg_path.to_str().unwrap()).unwrap();
        assert!(cfg.agents.is_empty());
    }

    // ── #446 metrics.disk.* parsing ──────────────────────────────────────

    #[test]
    fn test_workspace_config_metrics_defaults_when_block_absent() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.metrics.is_none());
        // Calling code uses DiskMetricsConfig::default() when the block is
        // absent — verify those defaults match the issue spec.
        let d = DiskMetricsConfig::default();
        assert_eq!(d.interval_seconds, 300);
        assert_eq!(d.volumes, vec!["/".to_string()]);
    }

    #[test]
    fn test_workspace_config_metrics_disk_parses_overrides() {
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
metrics:
  disk:
    interval_seconds: 60
    volumes: ["/", "/var"]
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let disk = cfg.metrics.unwrap().disk;
        assert_eq!(disk.interval_seconds, 60);
        assert_eq!(disk.volumes, vec!["/".to_string(), "/var".to_string()]);
    }

    #[test]
    fn test_workspace_config_metrics_disk_uses_defaults_when_partial() {
        // Only `interval_seconds` set — `volumes` should fall back to default.
        let yaml = r#"
agents:
  - name: kira
    work_dir: /home/kira
metrics:
  disk:
    interval_seconds: 600
"#;
        let cfg: WorkspaceConfig = serde_yaml::from_str(yaml).unwrap();
        let disk = cfg.metrics.unwrap().disk;
        assert_eq!(disk.interval_seconds, 600);
        assert_eq!(disk.volumes, vec!["/".to_string()]);
    }
}
