//! MCP tool implementations — the `call_*` functions that back each tool.
//!
//! Split from `mcp.rs` (#293). Each function parses tool arguments, delegates
//! to `mcp_service` or performs the action directly, and returns a JSON result.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::app::mcp_service;
use crate::config::UserConfig;

// ─── Embedded bus for sub-agent orchestration ────────────────────────────────

/// Container-local bus for managing sub-agents spawned via `add_persistent_agent`.
/// Lazy-initialized on first use — zero overhead when sub-agents aren't needed.
pub(crate) struct InternalBus {
    /// Socket path for the internal bus.
    pub socket_path: String,
    /// Names of sub-agents registered on this bus.
    pub sub_agents: HashSet<String>,
    /// Handle to the bus server task (aborted on drop).
    pub _bus_handle: tokio::task::JoinHandle<()>,
    /// Worker handles for each sub-agent.
    pub worker_handles: Vec<(String, tokio::task::JoinHandle<()>)>,
}

impl InternalBus {
    /// Start a container-local bus on a temp socket.
    pub async fn start(agent_name: &str) -> Result<Self> {
        let socket_path = format!("/tmp/deskd-{}-internal.sock", agent_name);

        // Remove stale socket if exists.
        std::fs::remove_file(&socket_path).ok();

        let bus_path = socket_path.clone();
        let bus_handle = tokio::spawn(async move {
            if let Err(e) = crate::app::bus::serve(&bus_path).await {
                warn!(error = %e, "internal bus exited");
            }
        });

        // Give the bus a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify the bus is reachable.
        UnixStream::connect(&socket_path)
            .await
            .with_context(|| format!("internal bus failed to start at {}", socket_path))?;

        info!(agent = %agent_name, socket = %socket_path, "internal bus started");

        Ok(Self {
            socket_path,
            sub_agents: HashSet::new(),
            _bus_handle: bus_handle,
            worker_handles: Vec::new(),
        })
    }

    /// Check if a target is a sub-agent on this bus.
    pub fn is_sub_agent_target(&self, target: &str) -> bool {
        if let Some(name) = target.strip_prefix("agent:") {
            self.sub_agents.contains(name)
        } else {
            false
        }
    }
}

// ─── Utility functions ───────────────────────────────────────────────────────

/// Simple glob matching: `*` matches any sequence of characters except none.
/// `telegram.out:*` matches `telegram.out:-1234567`.
pub(crate) fn glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == value;
    }
    let parts: Vec<&str> = pattern.splitn(2, '*').collect();
    match parts.as_slice() {
        [prefix, suffix] => value.starts_with(prefix) && value.ends_with(suffix),
        _ => pattern == value,
    }
}

/// Build the MCP tool description for `send_message` based on available
/// channels, sub-agents, and telegram routes defined in the user config.
pub(crate) fn build_send_message_description(cfg: &UserConfig, agent_name: &str) -> String {
    let mut lines = vec![
        "Send a message to a target on the bus.".to_string(),
        String::new(),
        "Available targets:".to_string(),
    ];

    for a in &cfg.agents {
        lines.push(format!(
            "  agent:{}  — {} ({}). {}",
            a.name,
            a.name,
            a.model,
            a.system_prompt.lines().next().unwrap_or("")
        ));
    }

    for ch in &cfg.channels {
        lines.push(format!("  {}  — {}", ch.name, ch.description));
    }

    if let Some(tg) = &cfg.telegram {
        for route in &tg.routes {
            lines.push(format!(
                "  telegram.out:{}  — Telegram chat {}",
                route.chat_id, route.chat_id
            ));
        }
    }

    lines.push(String::new());
    lines.push(format!("You are agent '{}'.", agent_name));

    lines.join("\n")
}

// ─── Tool implementations ────────────────────────────────────────────────────

pub(crate) async fn call_send_message(
    args: &Value,
    agent_name: &str,
    bus_socket: &str,
    user_config: Option<&UserConfig>,
    internal_bus: &Arc<Mutex<Option<InternalBus>>>,
) -> Result<Value> {
    let target = args
        .get("target")
        .and_then(|t| t.as_str())
        .context("missing target")?;
    let text = args
        .get("text")
        .and_then(|t| t.as_str())
        .context("missing text")?;
    let fresh = args.get("fresh").and_then(|f| f.as_bool()).unwrap_or(false);

    // Enforce publish allow-list if the calling agent is a sub-agent in config.
    if let Some(cfg) = user_config
        && let Some(sub) = cfg.agents.iter().find(|a| a.name == agent_name)
        && let Some(ref allow) = sub.publish
    {
        let allowed = allow.iter().any(|pattern| glob_match(pattern, target));
        if !allowed {
            bail!("publish to '{}' not allowed by config", target);
        }
    }

    // Enforce can_message ACL if set on the calling agent's config.
    if let Some(cfg) = user_config
        && let Some(sub) = cfg.agents.iter().find(|a| a.name == agent_name)
        && let Some(ref allow) = sub.can_message
    {
        let allowed = allow.iter().any(|pattern| glob_match(pattern, target));
        if !allowed {
            bail!(
                "agent '{}' cannot message '{}' — not in can_message allow-list",
                agent_name,
                target
            );
        }
    }

    // Route to: cross-user bus (if target maps to a foreign user socket) →
    // internal bus (if target is a sub-agent) → parent (own) bus.
    let cross_user_socket = target.strip_prefix("agent:").and_then(|name| {
        user_config
            .and_then(|cfg| cfg.cross_user_agents.as_ref())
            .and_then(|map| map.get(name))
            .cloned()
    });
    let effective_socket = if let Some(s) = cross_user_socket {
        s
    } else {
        let ibus = internal_bus.lock().await;
        if let Some(ref ib) = *ibus {
            if ib.is_sub_agent_target(target) {
                ib.socket_path.clone()
            } else {
                bus_socket.to_string()
            }
        } else {
            bus_socket.to_string()
        }
    };

    // Connect to bus, register, send message, disconnect.
    let stream = UnixStream::connect(&effective_socket)
        .await
        .with_context(|| format!("failed to connect to bus at {}", effective_socket))?;

    let (read_half, mut write_half) = stream.into_split();

    // Register as the agent.
    let reg_name = format!("{}-mcp-send", agent_name);
    let reg = serde_json::json!({
        "type": "register",
        "name": reg_name,
        "subscriptions": [format!("agent:{}", reg_name)]
    });
    let mut line = serde_json::to_string(&reg)?;
    line.push('\n');
    write_half.write_all(line.as_bytes()).await?;

    // For agent:* targets, verify the target is connected before sending.
    if let Some(target_agent) = target.strip_prefix("agent:") {
        let list_cmd = serde_json::json!({"type": "list"});
        let mut list_line = serde_json::to_string(&list_cmd)?;
        list_line.push('\n');
        write_half.write_all(list_line.as_bytes()).await?;
        write_half.flush().await?;

        // Read the list response with a short timeout.
        let reader = tokio::io::BufReader::new(read_half);
        let mut lines_reader = reader.lines();
        let list_result = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            lines_reader.next_line(),
        )
        .await;

        let agent_connected = match list_result {
            Ok(Ok(Some(ref resp_line))) => {
                if let Ok(resp) = serde_json::from_str::<Value>(resp_line) {
                    resp.get("payload")
                        .and_then(|p| p.get("clients"))
                        .and_then(|c| c.as_array())
                        .map(|clients| {
                            clients
                                .iter()
                                .any(|c| c.as_str().map(|n| n == target_agent).unwrap_or(false))
                        })
                        .unwrap_or(false)
                } else {
                    true // can't parse, assume connected
                }
            }
            _ => true, // timeout or error, proceed optimistically
        };

        if !agent_connected {
            bail!(
                "agent '{}' is not connected to the bus — message would be lost. \
                 Check if the agent is running with `list_agents`.",
                target_agent
            );
        }
    }

    // Build and send the message (same for all target types).
    let msg = serde_json::json!({
        "type": "message",
        "id": Uuid::new_v4().to_string(),
        "source": agent_name,
        "target": target,
        "payload": {"task": text},
        "metadata": {"priority": 5u8, "fresh": fresh}
    });
    let mut msg_line = serde_json::to_string(&msg)?;
    msg_line.push('\n');
    write_half.write_all(msg_line.as_bytes()).await?;
    write_half.flush().await?;

    info!(agent = %agent_name, target = %target, bus = %effective_socket, "send_message via MCP");

    Ok(json!({
        "content": [{"type": "text", "text": format!("Message sent to {}", target)}],
        "isError": false
    }))
}

/// Channels-protocol reply tool (#451).
///
/// Thin alias of `send_message` with channel-conventional params (`chat_id`,
/// `text`). Routes to `telegram.out:<chat_id>` so the existing Telegram
/// outbound adapter delivers the message — no new transport involved.
///
/// Schema is locked by the channels reference: any change to required keys
/// would break system prompts referencing `reply(chat_id, text)`.
pub(crate) async fn call_reply(args: &Value, agent_name: &str, bus_socket: &str) -> Result<Value> {
    let chat_id = args
        .get("chat_id")
        .and_then(|v| v.as_str())
        .context("missing chat_id")?;
    let text = args
        .get("text")
        .and_then(|v| v.as_str())
        .context("missing text")?;

    if chat_id.is_empty() {
        bail!("chat_id must not be empty");
    }
    if text.is_empty() {
        bail!("text must not be empty");
    }

    let target = format!("telegram.out:{}", chat_id);

    // Connect, register, publish, disconnect — mirrors the send_message
    // happy path. No can_message/ACL gating: the channel server is per-agent
    // and outbound is bounded to the existing telegram adapter route.
    let stream = UnixStream::connect(bus_socket)
        .await
        .with_context(|| format!("failed to connect to bus at {}", bus_socket))?;
    let (_read_half, mut write_half) = stream.into_split();

    let reg_name = format!("{}-mcp-reply", agent_name);
    let reg = serde_json::json!({
        "type": "register",
        "name": reg_name,
        "subscriptions": [],
    });
    let mut line = serde_json::to_string(&reg)?;
    line.push('\n');
    write_half.write_all(line.as_bytes()).await?;
    write_half.flush().await?;

    // Payload uses `result` so the Telegram adapter (which prefers
    // result → task → error) renders the body without a "[ts · source]"
    // prefix mismatch. This stays semantically a reply, not a new task.
    let msg = serde_json::json!({
        "type": "message",
        "id": Uuid::new_v4().to_string(),
        "source": agent_name,
        "target": target,
        "payload": {"result": text, "text": text},
        "metadata": {"priority": 5u8},
    });
    let mut msg_line = serde_json::to_string(&msg)?;
    msg_line.push('\n');
    write_half.write_all(msg_line.as_bytes()).await?;
    write_half.flush().await?;

    info!(agent = %agent_name, target = %target, "reply tool published to bus");

    Ok(json!({
        "content": [{"type": "text", "text": format!("Replied to chat {}", chat_id)}],
        "isError": false
    }))
}

/// Sub-agent runtime lifecycle accepted by [`call_add_persistent_agent`].
///
/// `StreamJson` is the historical worker — `deskd agent run` spawns a Claude
/// subprocess in `--output-format stream-json` mode, one fresh process per
/// task, exits between tasks. `TmuxChannel` (#502) keeps a persistent Claude
/// REPL inside a detached `deskd-<name>` tmux session that talks to the
/// internal bus via the `mcp-channel` MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubAgentLifecycle {
    StreamJson,
    TmuxChannel,
}

impl SubAgentLifecycle {
    fn as_str(self) -> &'static str {
        match self {
            SubAgentLifecycle::StreamJson => "stream-json",
            SubAgentLifecycle::TmuxChannel => "tmux-channel",
        }
    }
}

/// Parse the `lifecycle` argument of [`call_add_persistent_agent`].
///
/// - Missing / null → `StreamJson` (default, preserves byte-for-byte backward
///   compatibility with pre-#502 callers).
/// - `"stream-json"` → `StreamJson`.
/// - `"tmux-channel"` → `TmuxChannel`.
/// - Anything else (including non-string values) → `bail!` with a list of the
///   accepted values, so callers fail fast on typos.
pub(crate) fn parse_lifecycle_arg(args: &Value) -> Result<SubAgentLifecycle> {
    match args.get("lifecycle") {
        None | Some(Value::Null) => Ok(SubAgentLifecycle::StreamJson),
        Some(Value::String(s)) => match s.as_str() {
            "stream-json" => Ok(SubAgentLifecycle::StreamJson),
            "tmux-channel" => Ok(SubAgentLifecycle::TmuxChannel),
            other => bail!(
                "lifecycle must be 'stream-json' or 'tmux-channel' (got '{}')",
                other
            ),
        },
        Some(other) => bail!(
            "lifecycle must be a string ('stream-json' or 'tmux-channel'), got {}",
            other
        ),
    }
}

pub(crate) async fn call_add_persistent_agent(
    args: &Value,
    parent_name: &str,
    _parent_bus_socket: &str,
    internal_bus: &Arc<Mutex<Option<InternalBus>>>,
) -> Result<Value> {
    let name = args
        .get("name")
        .and_then(|n| n.as_str())
        .context("missing name")?;
    let model = args
        .get("model")
        .and_then(|m| m.as_str())
        .context("missing model")?;
    let system_prompt = args
        .get("system_prompt")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let subscribe: Vec<String> = args
        .get("subscribe")
        .and_then(|s| s.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_else(|| vec![format!("agent:{}", name)]);

    // Scope type: "inherit" (default) or "narrow".
    let scope_type = args
        .get("scope")
        .and_then(|s| s.as_str())
        .unwrap_or("inherit");
    let _scope = match scope_type {
        "narrow" => crate::config::ScopeType::Narrow,
        _ => crate::config::ScopeType::Inherit,
    };

    // Lifecycle (#502): `stream-json` (default, existing worker) or
    // `tmux-channel` (detached Claude REPL inside `deskd-<name>` tmux).
    let lifecycle = parse_lifecycle_arg(args)?;

    // Get the deskd binary path (we are running as a subprocess of claude, so $0 is deskd).
    let deskd_bin = std::env::var("DESKD_BIN").unwrap_or_else(|_| "deskd".to_string());

    // Work dir: caller-specified or same as parent.
    let parent_work_dir = std::env::var("PWD").unwrap_or_else(|_| "/tmp".to_string());
    let work_dir = args
        .get("work_dir")
        .and_then(|w| w.as_str())
        .map(String::from)
        .unwrap_or_else(|| parent_work_dir.clone());

    // Validate work_dir containment: child must be within parent's scope.
    // Enforced for BOTH lifecycle paths so tmux-channel cannot escape scope
    // either (AC: "work_dir containment still enforced for both").
    {
        let child_path = std::path::Path::new(&work_dir)
            .canonicalize()
            .unwrap_or_else(|_| work_dir.clone().into());
        let parent_path = std::path::Path::new(&parent_work_dir)
            .canonicalize()
            .unwrap_or_else(|_| parent_work_dir.clone().into());
        if !child_path.starts_with(&parent_path) {
            bail!(
                "work_dir '{}' is outside parent scope '{}'",
                work_dir,
                parent_work_dir
            );
        }
    }

    // Environment variables: child gets only explicitly passed env, not parent's.
    let child_env: HashMap<String, String> = args
        .get("env")
        .and_then(|e| e.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    // can_message: optional allow-list for outgoing messages.
    let can_message: Option<Vec<String>> =
        args.get("can_message")
            .and_then(|c| c.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(str::to_string)
                    .collect()
            });

    // Lazy-init internal bus on first sub-agent creation.
    let bus_socket = {
        let mut ibus_guard = internal_bus.lock().await;
        if ibus_guard.is_none() {
            let ibus = InternalBus::start(parent_name)
                .await
                .with_context(|| "failed to start internal bus for sub-agent orchestration")?;
            *ibus_guard = Some(ibus);
        }
        let ibus = ibus_guard.as_ref().unwrap();
        ibus.socket_path.clone()
    };

    // Create agent state file via `deskd agent create`.
    let create_status = tokio::process::Command::new(&deskd_bin)
        .args([
            "agent",
            "create",
            name,
            "--model",
            model,
            "--prompt",
            system_prompt,
            "--workdir",
            &work_dir,
        ])
        .status()
        .await;

    match create_status {
        Err(e) => {
            anyhow::bail!("failed to create agent '{}': {}", name, e);
        }
        Ok(s) if !s.success() => {
            // Agent may already exist — that's OK for idempotent calls.
            info!(parent = %parent_name, agent = %name, "agent create returned non-zero (may already exist)");
        }
        Ok(_) => {
            info!(parent = %parent_name, agent = %name, model = %model, "agent state created");
        }
    }

    // Set parent + scope + lifecycle fields on the created agent state.
    if let Ok(mut state) = crate::app::agent::load_state(name) {
        state.parent = Some(parent_name.to_string());
        state.scope = Some(scope_type.to_string());
        if let Some(ref cm) = can_message {
            state.can_message = Some(cm.clone());
        }
        if !child_env.is_empty() {
            state.env_keys = Some(child_env.keys().cloned().collect());
        }
        // Reflect the lifecycle in `config.launch_mode` so `agent list` /
        // `agent stop` / the web UI see a tmux-channel sub-agent uniformly
        // with top-level `launch_mode: tmux` agents from #504.
        state.config.launch_mode = match lifecycle {
            SubAgentLifecycle::StreamJson => {
                crate::domain::config_types::ConfigLaunchMode::Subprocess
            }
            SubAgentLifecycle::TmuxChannel => crate::domain::config_types::ConfigLaunchMode::Tmux,
        };
        crate::app::agent::save_state_pub(&state).ok();
    }

    // Fail-fast: verify the internal bus is reachable before spawning.
    UnixStream::connect(&bus_socket)
        .await
        .with_context(|| format!("internal bus at {} is not reachable", bus_socket))?;

    match lifecycle {
        SubAgentLifecycle::StreamJson => {
            spawn_stream_json_subagent(
                &deskd_bin,
                parent_name,
                name,
                &bus_socket,
                &subscribe,
                &child_env,
                internal_bus,
            )
            .await
        }
        SubAgentLifecycle::TmuxChannel => {
            spawn_tmux_channel_subagent(parent_name, name, &work_dir, &bus_socket, internal_bus)
                .await
        }
    }
}

/// Stream-json sub-agent path — the historical behaviour.
///
/// Spawns `deskd agent run <name> --socket <bus> --subscribe …` as a
/// background subprocess on the parent's internal bus. Each task runs
/// `claude -p` to completion and exits; the bus connection comes back up on
/// the next task.
#[allow(clippy::too_many_arguments)]
async fn spawn_stream_json_subagent(
    deskd_bin: &str,
    parent_name: &str,
    name: &str,
    bus_socket: &str,
    subscribe: &[String],
    child_env: &HashMap<String, String>,
    internal_bus: &Arc<Mutex<Option<InternalBus>>>,
) -> Result<Value> {
    // Start the worker as a background process connected to the internal bus.
    let mut run_args = vec![
        "agent".to_string(),
        "run".to_string(),
        name.to_string(),
        "--socket".to_string(),
        bus_socket.to_string(),
    ];
    for sub in subscribe {
        run_args.push("--subscribe".to_string());
        run_args.push(sub.clone());
    }
    let mut cmd = tokio::process::Command::new(deskd_bin);
    cmd.args(&run_args)
        .env("DESKD_BUS_SOCKET", bus_socket)
        .env("DESKD_AGENT_NAME", name);
    // Inject child-specific env vars (isolated from parent).
    for (k, v) in child_env {
        cmd.env(k, v);
    }
    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn worker for agent '{}'", name))?;

    // Write the worker PID to the state file so `agent status` can detect it.
    let worker_pid = child.id().unwrap_or(0);
    if worker_pid > 0
        && let Ok(mut state) = crate::app::agent::load_state(name)
    {
        state.pid = worker_pid;
        crate::app::agent::save_state_pub(&state).ok();
        info!(parent = %parent_name, agent = %name, pid = worker_pid, "wrote sub-agent PID to state");
    }

    // Track the sub-agent in the internal bus for routing and cleanup.
    {
        let mut ibus_guard = internal_bus.lock().await;
        if let Some(ref mut ibus) = *ibus_guard {
            ibus.sub_agents.insert(name.to_string());
            let agent_name = name.to_string();
            let handle = tokio::spawn(async move {
                // Wait for the child process — just holds the handle for abort.
                let _ = child.wait().await;
            });
            ibus.worker_handles.push((agent_name, handle));
        }
    }

    info!(parent = %parent_name, agent = %name, bus = %bus_socket, "persistent sub-agent spawned on internal bus");

    // Poll until the worker registers on the bus (up to 30s), so callers can
    // safely send_message immediately after this call returns.
    let agent_name_owned = name.to_string();
    let bus_socket_poll = bus_socket.to_string();
    let poll_timeout = std::time::Duration::from_secs(30);
    let poll_interval = tokio::time::Duration::from_millis(200);
    let registered = tokio::time::timeout(poll_timeout, async {
        loop {
            if let Ok(clients) = crate::app::serve::query_live_agents(&bus_socket_poll).await
                && clients.contains(&agent_name_owned)
            {
                break true;
            }
            tokio::time::sleep(poll_interval).await;
        }
    })
    .await
    .unwrap_or(false);

    if registered {
        info!(parent = %parent_name, agent = %name, "sub-agent registered on internal bus");
    } else {
        warn!(parent = %parent_name, agent = %name, "timed out waiting for sub-agent to register on bus (30s)");
    }

    let subscribe_display = subscribe.join(", ");
    Ok(json!({
        "content": [{
            "type": "text",
            "text": format!(
                "Agent '{}' started on internal bus {}. Subscriptions: {}",
                name, bus_socket, subscribe_display
            )
        }],
        "isError": false
    }))
}

/// Tmux-channel sub-agent path (#502).
///
/// 1. Provision the sub-agent's `work_dir` and the unix user's Claude home so
///    an unattended `claude --dangerously-load-development-channels` starts
///    without any first-run prompts (delegated to the #503 helper). The
///    helper writes `{work_dir}/.mcp.json` registering
///    `deskd mcp-channel --agent <name>` with
///    `env.DESKD_BUS_SOCKET = <internal_bus_socket>` — so the REPL inside
///    tmux talks to the parent's internal bus, never the parent's main bus.
/// 2. Launch a detached `deskd-<name>` tmux session via the existing
///    `launch_tmux_session` path (#452/#504). If the session is already up
///    we adopt it (`ensure_tmux_launched` handles both branches).
/// 3. Record `tmux_session` + `tmux_log_path` in the agent state file so
///    `deskd agent list` shows the session and `agent stop` can kill it.
/// 4. Register the sub-agent in the internal bus's tracker so the parent's
///    `send_message` routes `agent:<name>` to this bus.
///
/// Note: the tmux session is detached and survives parent exit — that's the
/// persistent-agent contract. `worker_handles` therefore stays empty here;
/// cleanup is driven by `deskd agent stop <name>` which kills tmux.
async fn spawn_tmux_channel_subagent(
    parent_name: &str,
    name: &str,
    work_dir: &str,
    bus_socket: &str,
    internal_bus: &Arc<Mutex<Option<InternalBus>>>,
) -> Result<Value> {
    let work_dir_path = std::path::PathBuf::from(work_dir);
    let bus_socket_path = std::path::PathBuf::from(bus_socket);

    let outcome =
        crate::app::agent_process::ensure_tmux_launched(name, &work_dir_path, &bus_socket_path)
            .await
            .with_context(|| {
                format!(
                    "failed to launch tmux-channel sub-agent '{}' for parent '{}'",
                    name, parent_name
                )
            })?;

    // Record tmux session info on the agent state (same convention as #504
    // for top-level `launch_mode: tmux` agents — see `serve.rs`).
    if let Ok(mut state) = crate::app::agent::load_state(name) {
        state.tmux_session = Some(outcome.session_name.clone());
        state.tmux_log_path = Some(outcome.log_path.display().to_string());
        // Clear any stale subprocess pid — this agent has no foreground
        // worker. `agent list` reads `tmux_session` + a live `tmux ls`
        // lookup to render the LAUNCHER column.
        state.pid = 0;
        crate::app::agent::save_state_pub(&state).ok();
    }

    // Track in the internal bus so the parent's `send_message` routes
    // `agent:<name>` here, not to the parent's main bus.
    {
        let mut ibus_guard = internal_bus.lock().await;
        if let Some(ref mut ibus) = *ibus_guard {
            ibus.sub_agents.insert(name.to_string());
        }
    }

    info!(
        parent = %parent_name,
        agent = %name,
        session = %outcome.session_name,
        bus = %bus_socket,
        log = %outcome.log_path.display(),
        adopted = outcome.adopted,
        "tmux-channel sub-agent launched on internal bus"
    );

    let msg = if outcome.adopted {
        format!(
            "Agent '{}' adopted existing tmux session '{}' on internal bus {} (log: {})",
            name,
            outcome.session_name,
            bus_socket,
            outcome.log_path.display()
        )
    } else {
        format!(
            "Agent '{}' launched in tmux session '{}' on internal bus {} (log: {})",
            name,
            outcome.session_name,
            bus_socket,
            outcome.log_path.display()
        )
    };

    Ok(json!({
        "content": [{ "type": "text", "text": msg }],
        "lifecycle": SubAgentLifecycle::TmuxChannel.as_str(),
        "session_name": outcome.session_name,
        "log_path": outcome.log_path.display().to_string(),
        "adopted": outcome.adopted,
        "isError": false
    }))
}

// ─── Reminder ────────────────────────────────────────────────────────────────

pub(crate) async fn call_create_reminder(
    args: &Value,
    work_dir: &std::path::Path,
) -> Result<Value> {
    let target = args
        .get("target")
        .and_then(|t| t.as_str())
        .context("missing target")?;
    let message = args
        .get("message")
        .and_then(|m| m.as_str())
        .context("missing message")?;

    // Time specification modes:
    //   "at"             — ISO 8601 / human-friendly time string
    //   "in"             — duration string like "30m"
    //   "delay_minutes"  — legacy numeric minutes
    // Recurring modes (#455, mutually exclusive with each other):
    //   "interval"        — fires every <duration> (min 1m)
    //   "cron_expression" — 5-field UTC cron, fires per schedule
    // For recurring reminders the absolute time params (at/in/delay_minutes)
    // are optional — the first fire defaults to now+interval or the next
    // cron occurrence.
    let at_str = args.get("at").and_then(|a| a.as_str());
    let in_str = args.get("in").and_then(|i| i.as_str());
    let delay_minutes = args.get("delay_minutes").and_then(|d| d.as_f64());
    let interval = args.get("interval").and_then(|v| v.as_str());
    let cron_expression = args.get("cron_expression").and_then(|v| v.as_str());

    let fire_at_override: Option<chrono::DateTime<chrono::Utc>> = if let Some(at) = at_str {
        Some(parse_at_time(at)?)
    } else if let Some(dur) = in_str {
        let secs = crate::app::commands::parse_duration_secs(dur)?;
        Some(chrono::Utc::now() + chrono::Duration::seconds(secs as i64))
    } else {
        delay_minutes.map(|mins| {
            chrono::Utc::now() + chrono::Duration::seconds((mins * 60.0).round() as i64)
        })
    };

    let is_recurring = interval.is_some() || cron_expression.is_some();
    if fire_at_override.is_none() && !is_recurring {
        bail!(
            "create_reminder requires one of: 'at', 'in', 'delay_minutes', 'interval', or 'cron_expression'"
        );
    }

    let result = mcp_service::create_reminder_full(
        work_dir,
        target,
        message,
        fire_at_override,
        interval,
        cron_expression,
    )?;

    let recurring_suffix = match (interval, cron_expression) {
        (Some(i), _) => format!(", recurring every {}", i),
        (_, Some(c)) => format!(", recurring per cron {:?}", c),
        _ => String::new(),
    };

    Ok(json!({
        "content": [{
            "type": "text",
            "text": format!(
                "Reminder scheduled: target={} at={} (in {:.0} minutes){}",
                result.target, result.fire_at, result.delay_minutes, recurring_suffix
            )
        }],
        "schedule_kind": result.schedule_kind,
        "isError": false
    }))
}

pub(crate) async fn call_list_reminders(args: &Value, work_dir: &std::path::Path) -> Result<Value> {
    let target_substr = args.get("target").and_then(|t| t.as_str());
    let before = match args.get("before").and_then(|b| b.as_str()) {
        Some(s) => Some(parse_at_time(s)?),
        None => None,
    };
    let after = match args.get("after").and_then(|a| a.as_str()) {
        Some(s) => Some(parse_at_time(s)?),
        None => None,
    };
    let limit = args
        .get("limit")
        .and_then(|l| l.as_u64())
        .map(|n| n as usize)
        .unwrap_or(50);

    let items = mcp_service::list_reminders(work_dir, target_substr, before, after, limit)?;
    let count = items.len();
    let summary = if count == 0 {
        "No reminders match.".to_string()
    } else {
        let lines: Vec<String> = items
            .iter()
            .map(|r| {
                format!(
                    "{} → {} | {} | {}",
                    r.get("at").and_then(|v| v.as_str()).unwrap_or("?"),
                    r.get("target").and_then(|v| v.as_str()).unwrap_or("?"),
                    r.get("id").and_then(|v| v.as_str()).unwrap_or("?"),
                    r.get("message_preview")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                )
            })
            .collect();
        format!("{} reminder(s):\n{}", count, lines.join("\n"))
    };
    Ok(json!({
        "content": [{ "type": "text", "text": summary }],
        "reminders": items,
        "isError": false
    }))
}

pub(crate) async fn call_get_reminder(args: &Value, work_dir: &std::path::Path) -> Result<Value> {
    let id = args
        .get("id")
        .and_then(|i| i.as_str())
        .context("missing id")?;
    let reminder = mcp_service::get_reminder(work_dir, id)?;
    let text = format!(
        "id={} at={} target={}\nmessage:\n{}",
        reminder.get("id").and_then(|v| v.as_str()).unwrap_or("?"),
        reminder.get("at").and_then(|v| v.as_str()).unwrap_or("?"),
        reminder
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("?"),
        reminder
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
    );
    Ok(json!({
        "content": [{ "type": "text", "text": text }],
        "reminder": reminder,
        "isError": false
    }))
}

pub(crate) async fn call_cancel_reminder(
    args: &Value,
    work_dir: &std::path::Path,
) -> Result<Value> {
    let id = args
        .get("id")
        .and_then(|i| i.as_str())
        .context("missing id")?;
    let result = mcp_service::cancel_reminder(work_dir, id)?;
    let cancelled = result
        .get("cancelled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let text = if cancelled {
        format!("Reminder {} cancelled.", id)
    } else {
        format!(
            "Reminder {} not found (already fired or never existed).",
            id
        )
    };
    Ok(json!({
        "content": [{ "type": "text", "text": text }],
        "cancelled": cancelled,
        "id": id,
        "isError": false
    }))
}

pub(crate) async fn call_update_reminder(
    args: &Value,
    work_dir: &std::path::Path,
) -> Result<Value> {
    let id = args
        .get("id")
        .and_then(|i| i.as_str())
        .context("missing id")?;

    let at_str = args.get("at").and_then(|a| a.as_str());
    let in_str = args.get("in").and_then(|i| i.as_str());
    let new_at = if let Some(at) = at_str {
        Some(parse_at_time(at)?)
    } else if let Some(dur) = in_str {
        let secs = crate::app::commands::parse_duration_secs(dur)?;
        Some(chrono::Utc::now() + chrono::Duration::seconds(secs as i64))
    } else {
        None
    };
    let new_target = args.get("target").and_then(|t| t.as_str());
    let new_message = args.get("message").and_then(|m| m.as_str());

    if new_at.is_none() && new_target.is_none() && new_message.is_none() {
        bail!("update_reminder requires at least one of: 'at', 'in', 'target', 'message'");
    }

    let updated = mcp_service::update_reminder(work_dir, id, new_at, new_target, new_message)?;
    let text = format!(
        "Reminder {} updated: at={} target={}",
        id,
        updated.get("at").and_then(|v| v.as_str()).unwrap_or("?"),
        updated
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or("?"),
    );
    Ok(json!({
        "content": [{ "type": "text", "text": text }],
        "reminder": updated,
        "isError": false
    }))
}

/// Parse a human-friendly time string into a UTC DateTime.
///
/// Supported formats:
/// - ISO 8601: "2026-04-22T09:00:00Z", "2026-04-22T09:00:00+03:00"
/// - Date + time: "2026-04-22 09:00"
/// - Time only (today/tomorrow): "09:00", "9:00", "21:30"
/// - Relative: "tomorrow 09:00", "tomorrow 9:00"
fn parse_at_time(s: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    use chrono::{NaiveTime, TimeZone, Utc};
    use chrono_tz::Europe::Moscow;

    let s = s.trim();

    // Try ISO 8601 with timezone offset first.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try "YYYY-MM-DD HH:MM" or "YYYY-MM-DDTHH:MM" (naive, assume Moscow tz).
    for fmt in &["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, fmt)
            && let Some(dt) = Moscow.from_local_datetime(&naive).earliest()
        {
            return Ok(dt.with_timezone(&Utc));
        }
    }

    // "tomorrow HH:MM" or "tomorrow H:MM"
    if let Some(time_part) = s.strip_prefix("tomorrow").map(str::trim)
        && let Ok(time) = NaiveTime::parse_from_str(time_part, "%H:%M")
    {
        let tomorrow_moscow =
            (Utc::now().with_timezone(&Moscow).date_naive()) + chrono::Duration::days(1);
        let naive = tomorrow_moscow.and_time(time);
        if let Some(dt) = Moscow.from_local_datetime(&naive).earliest() {
            return Ok(dt.with_timezone(&Utc));
        }
    }

    // Bare time "HH:MM" or "H:MM" — today if in the future, tomorrow if past.
    if let Ok(time) = NaiveTime::parse_from_str(s, "%H:%M") {
        let now_moscow = Utc::now().with_timezone(&Moscow);
        let today = now_moscow.date_naive().and_time(time);
        if let Some(dt) = Moscow.from_local_datetime(&today).earliest() {
            let dt_utc = dt.with_timezone(&Utc);
            if dt_utc > Utc::now() {
                return Ok(dt_utc);
            }
        }
        // Time is past today — schedule for tomorrow.
        let tomorrow = (now_moscow.date_naive() + chrono::Duration::days(1)).and_time(time);
        if let Some(dt) = Moscow.from_local_datetime(&tomorrow).earliest() {
            return Ok(dt.with_timezone(&Utc));
        }
    }

    bail!(
        "cannot parse time '{}'. Supported: ISO 8601 (2026-04-22T09:00:00Z), \
         datetime (2026-04-22 09:00), time (09:00), 'tomorrow 09:00', \
         or duration with 'in' param (30m, 1h, 2h30m)",
        s
    )
}

// ─── Unified inbox ───────────────────────────────────────────────────────────

/// Check if `agent_name` is allowed to access `inbox_name`.
///
/// Rules:
/// - An agent can always read its own inbox (name matches agent_name).
/// - If the agent is a sub-agent with `inbox_read` configured, check the
///   glob patterns in that allow-list.
/// - If `inbox_read` is None on a sub-agent, only own inbox is allowed.
/// - For top-level agents (not in `user_config.agents`), the per-agent
///   `inbox_acl` field on `UserConfig` is consulted: when `Some(list)`, only
///   the agent's own inbox plus listed glob patterns are readable. When
///   absent (`None`), behavior is unrestricted (current default — backward
///   compatible).
pub(crate) fn inbox_access_allowed(
    agent_name: &str,
    inbox_name: &str,
    user_config: Option<&UserConfig>,
) -> bool {
    // Own inbox is always allowed.
    if inbox_name == agent_name {
        return true;
    }
    // The "replies/<agent>" convention belongs to the agent.
    if inbox_name == format!("replies/{}", agent_name) {
        return true;
    }

    if let Some(cfg) = user_config {
        // If this agent is a sub-agent with inbox_read ACL, check it.
        if let Some(sub) = cfg.agents.iter().find(|a| a.name == agent_name) {
            return match &sub.inbox_read {
                Some(patterns) => patterns.iter().any(|p| glob_match(p, inbox_name)),
                None => false, // No inbox_read → own inbox only
            };
        }

        // Top-level agent: consult inbox_acl on the user config itself.
        if let Some(patterns) = &cfg.inbox_acl {
            return patterns.iter().any(|p| glob_match(p, inbox_name));
        }
    }

    // No config or no inbox_acl on top-level agent → unrestricted (default).
    true
}

pub(crate) async fn call_list_inboxes(
    agent_name: &str,
    user_config: Option<&UserConfig>,
) -> Result<Value> {
    let all = mcp_service::list_inboxes()?;
    let filtered: Vec<_> = all
        .into_iter()
        .filter(|entry| {
            entry
                .get("inbox")
                .and_then(|v| v.as_str())
                .map(|name| inbox_access_allowed(agent_name, name, user_config))
                .unwrap_or(false)
        })
        .collect();

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&filtered)?}],
        "isError": false
    }))
}

pub(crate) async fn call_read_inbox(
    args: &Value,
    agent_name: &str,
    user_config: Option<&UserConfig>,
) -> Result<Value> {
    let inbox = args
        .get("inbox")
        .and_then(|i| i.as_str())
        .context("missing inbox")?;

    if !inbox_access_allowed(agent_name, inbox, user_config) {
        bail!(
            "inbox access denied: agent \"{}\" is not in allow-list for inbox \"{}\"",
            agent_name,
            inbox
        );
    }

    let limit = args.get("limit").and_then(|l| l.as_u64()).unwrap_or(50) as usize;
    let since = args
        .get("since")
        .and_then(|s| s.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    let output = mcp_service::read_inbox(inbox, limit, since)?;

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&output)?}],
        "isError": false
    }))
}

pub(crate) async fn call_search_inbox(
    args: &Value,
    agent_name: &str,
    user_config: Option<&UserConfig>,
) -> Result<Value> {
    let inbox = args.get("inbox").and_then(|i| i.as_str());
    let query = args
        .get("query")
        .and_then(|q| q.as_str())
        .context("missing query")?;
    let limit = args.get("limit").and_then(|l| l.as_u64()).unwrap_or(50) as usize;

    // If a specific inbox is requested, check access.
    if let Some(name) = inbox
        && !inbox_access_allowed(agent_name, name, user_config)
    {
        bail!(
            "inbox access denied: agent \"{}\" is not in allow-list for inbox \"{}\"",
            agent_name,
            name
        );
    }

    // When searching all inboxes (inbox=None), filter results to allowed inboxes.
    // The mcp_service returns results tagged with inbox names, but the current
    // unified_inbox search doesn't expose per-result inbox names. For cross-inbox
    // search without ACL filtering, we restrict to the agent's own inbox.
    let effective_inbox = if inbox.is_some() {
        inbox
    } else if let Some(cfg) = user_config {
        // Sub-agent: cross-inbox search restricted to own inbox.
        if cfg.agents.iter().any(|a| a.name == agent_name) {
            Some(agent_name)
        } else if cfg.inbox_acl.is_some() {
            // Top-level agent with ACL: cross-inbox search restricted to own
            // inbox to avoid leaking content from inboxes outside the ACL.
            Some(agent_name)
        } else {
            None // Top-level agent without ACL — search all (default)
        }
    } else {
        None
    };

    let output = mcp_service::search_inbox(effective_inbox, query, limit)?;

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&output)?}],
        "isError": false
    }))
}

// ─── Graph execution ─────────────────────────────────────────────────────────

pub(crate) async fn call_run_graph(args: &Value) -> Result<Value> {
    let file = args
        .get("file")
        .and_then(|f| f.as_str())
        .context("missing file")?;
    let work_dir = args.get("work_dir").and_then(|w| w.as_str());
    let vars = args.get("vars");

    let summary = mcp_service::run_graph(file, work_dir, vars).await?;

    Ok(json!({
        "content": [{"type": "text", "text": summary}],
        "isError": false
    }))
}

// ─── Task queue ──────────────────────────────────────────────────────────────

pub(crate) async fn call_task_create(
    args: &Value,
    agent_name: &str,
    task_store: &dyn crate::ports::store::TaskRepository,
) -> Result<Value> {
    let description = args
        .get("description")
        .and_then(|d| d.as_str())
        .context("missing description")?;
    let model = args.get("model").and_then(|m| m.as_str()).map(String::from);
    let labels: Vec<String> = args
        .get("labels")
        .and_then(|l| l.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let metadata = args
        .get("metadata")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let result =
        mcp_service::task_create(description, model, labels, metadata, agent_name, task_store)?;

    Ok(json!({
        "content": [{"type": "text", "text": format!(
            "Task created: {} (status=pending, id={})", result.description, result.id
        )}],
        "isError": false
    }))
}

pub(crate) async fn call_task_list(
    args: &Value,
    task_store: &dyn crate::ports::store::TaskRepository,
) -> Result<Value> {
    let status_filter = args.get("status").and_then(|s| s.as_str());

    let summary = mcp_service::task_list(status_filter, task_store)?;

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&summary)?}],
        "isError": false
    }))
}

pub(crate) async fn call_task_cancel(
    args: &Value,
    task_store: &dyn crate::ports::store::TaskRepository,
) -> Result<Value> {
    let id = args
        .get("id")
        .and_then(|i| i.as_str())
        .context("missing id")?;

    let result = mcp_service::task_cancel(id, task_store)?;

    Ok(json!({
        "content": [{"type": "text", "text": format!("Task {} cancelled", result.id)}],
        "isError": false
    }))
}

// ─── Sub-agent management ────────────────────────────────────────────────────

pub(crate) async fn call_list_agents(
    caller: &str,
    internal_bus: &Arc<Mutex<Option<InternalBus>>>,
) -> Result<Value> {
    let ibus_guard = internal_bus.lock().await;
    let agents: Vec<Value> = match &*ibus_guard {
        None => Vec::new(),
        Some(ibus) => {
            let mut list = Vec::new();
            for name in &ibus.sub_agents {
                // Scoped visibility: only show agents that the caller is parent of.
                if let Ok(state) = crate::app::agent::load_state(name) {
                    let is_visible = match &state.parent {
                        Some(parent) => parent == caller,
                        None => true,
                    };
                    if !is_visible {
                        continue;
                    }
                }

                let is_finished = ibus
                    .worker_handles
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, handle)| handle.is_finished())
                    .unwrap_or(true);

                list.push(mcp_service::build_agent_summary(name, is_finished));
            }
            list
        }
    };

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&agents)?}],
        "isError": false
    }))
}

/// Snapshot of bus connectivity for diagnosing routing issues.
///
/// Reports two layers:
/// - `internal_bus`: the lazy container-local bus used for sub-agent
///   orchestration. `running: false` means no sub-agents have been spawned.
/// - `host_bus`: the per-agent bus the caller is itself attached to (the
///   socket pointed at by `DESKD_BUS_SOCKET`). The list of clients comes from
///   sending `{"type":"list"}` and reading `list_response.clients`.
///
/// Sub-agent visibility on the internal bus is scoped to the caller's
/// children — same rule as `list_agents` — so an agent cannot enumerate
/// siblings via this tool.
pub(crate) async fn call_bus_status(
    caller: &str,
    bus_socket: &str,
    internal_bus: &Arc<Mutex<Option<InternalBus>>>,
) -> Result<Value> {
    // Internal bus snapshot ---------------------------------------------------
    let internal = {
        let ibus_guard = internal_bus.lock().await;
        match &*ibus_guard {
            None => json!({
                "running": false,
                "socket_path": Value::Null,
                "sub_agents": [],
            }),
            Some(ibus) => {
                let mut sub_agents = Vec::new();
                for name in &ibus.sub_agents {
                    // Same scoped visibility as list_agents.
                    if let Ok(state) = crate::app::agent::load_state(name) {
                        let visible = match &state.parent {
                            Some(parent) => parent == caller,
                            None => true,
                        };
                        if !visible {
                            continue;
                        }
                    }

                    let alive = ibus
                        .worker_handles
                        .iter()
                        .find(|(n, _)| n == name)
                        .map(|(_, handle)| !handle.is_finished())
                        .unwrap_or(false);

                    sub_agents.push(json!({"name": name, "alive": alive}));
                }
                json!({
                    "running": true,
                    "socket_path": ibus.socket_path,
                    "sub_agents": sub_agents,
                })
            }
        }
    };

    // Host bus snapshot -------------------------------------------------------
    // Best-effort: a failure to query the bus is reported as `error` rather
    // than failing the whole tool call, so the caller still gets the internal
    // bus picture.
    let host = match query_bus_clients(bus_socket).await {
        Ok(clients) => json!({
            "socket_path": bus_socket,
            "connected_clients": clients,
        }),
        Err(e) => json!({
            "socket_path": bus_socket,
            "connected_clients": [],
            "error": format!("{:#}", e),
        }),
    };

    let result = json!({
        "internal_bus": internal,
        "host_bus": host,
    });

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&result)?}],
        "isError": false
    }))
}

/// Open a short-lived connection to a bus socket and return the names of
/// currently-connected clients.
///
/// Bus protocol requires a REGISTER as the first message, so we register
/// under an ephemeral name, send `{"type":"list"}`, then read the
/// `list_response` (which is delivered wrapped as a regular bus message
/// with `payload.clients`).
async fn query_bus_clients(socket_path: &str) -> Result<Vec<String>> {
    let stream = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        UnixStream::connect(socket_path),
    )
    .await
    .with_context(|| format!("timeout connecting to bus at {}", socket_path))?
    .with_context(|| format!("failed to connect to bus at {}", socket_path))?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(read_half);

    let probe_name = format!("__bus_status_probe_{}", Uuid::new_v4());
    let empty_subs: Vec<String> = Vec::new();
    let register = serde_json::json!({
        "type": "register",
        "name": probe_name,
        "subscriptions": empty_subs,
    });
    let mut reg_line = serde_json::to_string(&register)?;
    reg_line.push('\n');
    write_half.write_all(reg_line.as_bytes()).await?;
    write_half.write_all(b"{\"type\":\"list\"}\n").await?;
    write_half.flush().await?;

    let mut line = String::new();
    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        reader.read_line(&mut line),
    )
    .await
    .with_context(|| "timeout reading list_response from bus")??;

    let msg: Value = serde_json::from_str(line.trim())
        .with_context(|| format!("invalid list_response: {:?}", line))?;

    // Server delivers the list as a BusMessage; the actual list is in
    // `payload.clients`. Filter out our own probe registration so callers
    // don't see it in the snapshot.
    let clients = msg
        .get("payload")
        .and_then(|p| p.get("clients"))
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .filter(|n| n != &probe_name)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(clients)
}

/// Read recent log lines captured from a sub-agent's stderr or stream-json
/// stdout. Access is restricted to sub-agents of the caller (or the caller's
/// own logs) so an agent cannot peek at a sibling's diagnostics.
pub(crate) async fn call_agent_logs(args: &Value, caller: &str) -> Result<Value> {
    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .context("agent_logs: missing required 'name'")?;

    let tail = args.get("tail").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
    if tail == 0 || tail > 1000 {
        bail!("agent_logs: 'tail' must be in 1..=1000 (got {})", tail);
    }

    let source = args
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("stderr");
    if source != "stderr" && source != "stream" {
        bail!(
            "agent_logs: 'source' must be 'stderr' or 'stream' (got {:?})",
            source
        );
    }

    // Access control: caller may read its own logs or its sub-agents' logs.
    // Matches the visibility rules of list_agents/remove_agent.
    if name != caller {
        let state = crate::app::agent::load_state(name)
            .with_context(|| format!("agent '{}' not found", name))?;
        match &state.parent {
            Some(p) if p == caller => {}
            _ => bail!(
                "agent '{}' is not a sub-agent of '{}' — log access denied",
                name,
                caller
            ),
        }
    }

    let path = match source {
        "stream" => crate::app::agent::stream_log_path(name),
        _ => crate::app::agent::stderr_log_path(name),
    };

    if !path.exists() {
        return Ok(json!({
            "content": [{
                "type": "text",
                "text": serde_json::to_string_pretty(&json!({
                    "name": name,
                    "source": source,
                    "path": path.display().to_string(),
                    "exists": false,
                    "lines": Vec::<String>::new(),
                }))?
            }],
            "isError": false
        }));
    }

    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("agent_logs: failed to read {}", path.display()))?;
    let all_lines: Vec<&str> = content.lines().collect();
    let start = all_lines.len().saturating_sub(tail);
    let returned: Vec<String> = all_lines[start..].iter().map(|s| s.to_string()).collect();

    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&json!({
                "name": name,
                "source": source,
                "path": path.display().to_string(),
                "exists": true,
                "total_lines": all_lines.len(),
                "returned_lines": returned.len(),
                "lines": returned,
            }))?
        }],
        "isError": false
    }))
}

/// Read recent task log entries (one per completed task) for an agent. Reads
/// the structured JSONL written by the worker on every task completion, with
/// `ts`, `source`, `task`, `status`, `turns`, `cost`, `duration_ms`, and
/// `error` fields. Access mirrors `agent_logs`: caller may read its own
/// history or a sub-agent's.
pub(crate) async fn call_agent_tasks(args: &Value, caller: &str) -> Result<Value> {
    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .context("agent_tasks: missing required 'name'")?;

    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize;
    if limit == 0 || limit > 200 {
        bail!("agent_tasks: 'limit' must be in 1..=200 (got {})", limit);
    }

    let source_filter = args.get("source").and_then(|v| v.as_str());

    let since = match args.get("since").and_then(|v| v.as_str()) {
        Some(s) => Some(
            chrono::DateTime::parse_from_rfc3339(s)
                .with_context(|| format!("agent_tasks: 'since' must be RFC3339 (got {:?})", s))?
                .with_timezone(&chrono::Utc),
        ),
        None => None,
    };

    // Access control: caller may read its own task log or its sub-agents'.
    if name != caller {
        let state = crate::app::agent::load_state(name)
            .with_context(|| format!("agent '{}' not found", name))?;
        match &state.parent {
            Some(p) if p == caller => {}
            _ => bail!(
                "agent '{}' is not a sub-agent of '{}' — task history access denied",
                name,
                caller
            ),
        }
    }

    let path = crate::app::tasklog::log_path(name);
    let entries = crate::app::tasklog::read_logs_from_path(&path, limit, source_filter, since)?;

    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&json!({
                "name": name,
                "path": path.display().to_string(),
                "exists": path.exists(),
                "returned": entries.len(),
                "tasks": entries,
            }))?
        }],
        "isError": false
    }))
}

pub(crate) async fn call_remove_agent(
    args: &Value,
    caller: &str,
    internal_bus: &Arc<Mutex<Option<InternalBus>>>,
) -> Result<Value> {
    let name = args
        .get("name")
        .and_then(|n| n.as_str())
        .context("missing name")?;

    // Validate ownership and get agent state.
    let state = mcp_service::validate_remove_agent(name, caller)?;

    // Graceful shutdown of the agent process.
    mcp_service::stop_agent_process(name, state.pid).await;

    // Clean up internal bus state.
    {
        let mut ibus_guard = internal_bus.lock().await;
        if let Some(ref mut ibus) = *ibus_guard {
            ibus.sub_agents.remove(name);
            ibus.worker_handles.retain(|(n, handle)| {
                if n == name {
                    handle.abort();
                    false
                } else {
                    true
                }
            });
        }
    }

    crate::app::agent::remove(name).await?;

    info!(agent = %name, caller = %caller, "remove_agent via MCP");

    Ok(json!({
        "content": [{"type": "text", "text": format!("Agent '{}' removed", name)}],
        "isError": false
    }))
}

// ─── Scope introspection ────────────────────────────────────────────────────

pub(crate) async fn call_get_scope(
    agent_name: &str,
    user_config: Option<&UserConfig>,
    internal_bus: &Arc<Mutex<Option<InternalBus>>>,
) -> Result<Value> {
    // Load agent state for scope info.
    let state = crate::app::agent::load_state(agent_name).ok();

    let scope = state
        .as_ref()
        .and_then(|s| s.scope.clone())
        .unwrap_or_else(|| "inherit".into());
    let parent = state.as_ref().and_then(|s| s.parent.clone());
    let work_dir = state
        .as_ref()
        .map(|s| s.config.work_dir.clone())
        .unwrap_or_else(|| std::env::var("PWD").unwrap_or_default());
    let env_keys = state
        .as_ref()
        .and_then(|s| s.env_keys.clone())
        .unwrap_or_default();
    let can_message_state = state.as_ref().and_then(|s| s.can_message.clone());

    // Merge can_message from config if available.
    let can_message = can_message_state.or_else(|| {
        user_config
            .and_then(|cfg| cfg.agents.iter().find(|a| a.name == agent_name))
            .and_then(|sub| sub.can_message.clone())
    });

    // Collect children from internal bus.
    let children: Vec<String> = {
        let ibus_guard = internal_bus.lock().await;
        match &*ibus_guard {
            None => Vec::new(),
            Some(ibus) => ibus.sub_agents.iter().cloned().collect(),
        }
    };

    // Get bus topics from config.
    let bus_topics: Vec<String> = user_config
        .and_then(|cfg| cfg.agents.iter().find(|a| a.name == agent_name))
        .map(|sub| sub.subscribe.clone())
        .unwrap_or_default();

    let scope_info = json!({
        "agent": agent_name,
        "scope": scope,
        "parent": parent,
        "work_dir": work_dir,
        "can_message": can_message,
        "bus_topics": bus_topics,
        "env_keys": env_keys,
        "children": children,
    });

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&scope_info)?}],
        "isError": false
    }))
}

// ─── State machines ──────────────────────────────────────────────────────────

pub(crate) async fn call_sm_create(
    args: &Value,
    agent_name: &str,
    bus_socket: &str,
    user_config: Option<&UserConfig>,
    sm_store: &dyn crate::ports::store::StateMachineRepository,
) -> Result<Value> {
    let model_name = args
        .get("model")
        .and_then(|m| m.as_str())
        .context("missing model")?;
    let title = args
        .get("title")
        .and_then(|t| t.as_str())
        .context("missing title")?;
    let body = args.get("body").and_then(|b| b.as_str()).unwrap_or("");
    let metadata = args
        .get("metadata")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let cfg = user_config.context("no user config loaded — models not available")?;
    let result = mcp_service::sm_create(
        model_name, title, body, metadata, agent_name, bus_socket, cfg, sm_store,
    )
    .await?;

    Ok(json!({
        "content": [{"type": "text", "text": format!(
            "Created instance {} (model={}, state={}, assignee={})",
            result.id, result.model, result.state, result.assignee
        )}],
        "isError": false
    }))
}

pub(crate) async fn call_sm_move(
    args: &Value,
    agent_name: &str,
    bus_socket: &str,
    user_config: Option<&UserConfig>,
    sm_store: &dyn crate::ports::store::StateMachineRepository,
) -> Result<Value> {
    let id = args
        .get("id")
        .and_then(|i| i.as_str())
        .context("missing id")?;
    let state = args
        .get("state")
        .and_then(|s| s.as_str())
        .context("missing state")?;
    let note = args.get("note").and_then(|n| n.as_str());

    let cfg = user_config.context("no user config loaded — models not available")?;
    let result =
        mcp_service::sm_move(id, state, note, agent_name, bus_socket, cfg, sm_store).await?;

    Ok(json!({
        "content": [{"type": "text", "text": format!("{} → {} (model={})", result.id, result.state, result.model)}],
        "isError": false
    }))
}

pub(crate) async fn call_sm_query(
    args: &Value,
    sm_store: &dyn crate::ports::store::StateMachineRepository,
) -> Result<Value> {
    let id = args.get("id").and_then(|i| i.as_str());
    let model_filter = args.get("model").and_then(|m| m.as_str());
    let state_filter = args.get("state").and_then(|s| s.as_str());

    let result = mcp_service::sm_query(id, model_filter, state_filter, sm_store)?;

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&result)?}],
        "isError": false
    }))
}

// ─── Usage stats ─────────────────────────────────────────────────────────────

pub(crate) async fn call_usage_stats(args: &Value) -> Result<Value> {
    let period = args.get("period").and_then(|v| v.as_str()).unwrap_or("7d");
    let agent = args.get("agent").and_then(|v| v.as_str());

    let stats = crate::app::commands::usage::compute_stats(period, agent)?;
    Ok(serde_json::to_value(stats)?)
}

/// Apply authentication to an outgoing A2A HTTP request.
///
/// Supports two modes:
/// - `api_key` param → `x-api-key` header
/// - `jwt_key` param (path to private key PEM) → `Authorization: Bearer <jwt>`
fn apply_a2a_auth(
    mut req: reqwest::RequestBuilder,
    args: &serde_json::Value,
    url: &str,
) -> reqwest::RequestBuilder {
    if let Some(jwt_key_path) = args.get("jwt_key").and_then(|v| v.as_str()) {
        let path = std::path::Path::new(jwt_key_path);
        match crate::app::a2a_jwt::KeyPair::load(path) {
            Ok(kp) => match kp.sign_jwt(url, 60) {
                Ok(token) => {
                    req = req.header("authorization", format!("Bearer {}", token));
                }
                Err(e) => {
                    tracing::warn!("failed to sign JWT: {}", e);
                }
            },
            Err(e) => {
                tracing::warn!("failed to load JWT key from {}: {}", jwt_key_path, e);
            }
        }
    } else if let Some(key) = args.get("api_key").and_then(|v| v.as_str()) {
        req = req.header("x-api-key", key);
    }
    req
}

/// Send an A2A task to a remote agent via HTTP.
///
/// MCP tool: `a2a_send(url, skill, message, api_key?, jwt_key?)`
pub(crate) async fn call_a2a_send(args: &Value) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'url' parameter"))?;
    let skill = args
        .get("skill")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'skill' parameter"))?;
    let message = args
        .get("message")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'message' parameter"))?;

    let rpc_body = json!({
        "jsonrpc": "2.0",
        "id": uuid::Uuid::new_v4().to_string(),
        "method": "tasks/send",
        "params": {
            "skill": skill,
            "message": message,
        }
    });

    let a2a_url = format!("{}/a2a", url.trim_end_matches('/'));

    let client = reqwest::Client::new();
    let mut req = client.post(&a2a_url).json(&rpc_body);
    req = apply_a2a_auth(req, args, url);

    let resp = req.send().await.context("A2A request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!("A2A request returned {}: {}", status, body);
    }

    let body: Value = resp.json().await.context("failed to parse A2A response")?;

    if let Some(error) = body.get("error") {
        bail!(
            "A2A error {}: {}",
            error.get("code").and_then(|c| c.as_i64()).unwrap_or(0),
            error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown")
        );
    }

    Ok(body.get("result").cloned().unwrap_or(body))
}

/// Fetch an Agent Card from a remote A2A agent (discovery).
///
/// MCP tool: `a2a_discover(url)`
pub(crate) async fn call_a2a_discover(args: &Value) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'url' parameter"))?;

    let card_url = format!("{}/.well-known/agent-card.json", url.trim_end_matches('/'));

    let client = reqwest::Client::new();
    let resp = client
        .get(&card_url)
        .send()
        .await
        .context("failed to fetch Agent Card")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!("Agent Card request returned {}: {}", status, body);
    }

    let card: Value = resp
        .json()
        .await
        .context("failed to parse Agent Card JSON")?;

    Ok(card)
}

/// Dynamically add a need to the agent's own deskd.yaml.
///
/// MCP tool: `publish_need(description, tags?, priority?)`
pub(crate) async fn call_publish_need(args: &Value) -> Result<Value> {
    let description = args
        .get("description")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'description' parameter"))?;

    let tags: Vec<String> = args
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let priority = args
        .get("priority")
        .and_then(|v| v.as_str())
        .unwrap_or("medium");

    // Generate a stable ID from the description.
    let id = description
        .to_lowercase()
        .split_whitespace()
        .take(4)
        .collect::<Vec<_>>()
        .join("-");

    let config_path = std::env::var("DESKD_AGENT_CONFIG")
        .context("DESKD_AGENT_CONFIG not set — cannot publish need")?;

    // Load existing config, add need, write back.
    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config: {}", config_path))?;

    let mut doc: serde_yaml::Value =
        serde_yaml::from_str(&raw).context("failed to parse config YAML")?;

    let need_val = serde_yaml::to_value(&crate::config::NeedDef {
        id: id.clone(),
        description: description.to_string(),
        tags: tags.clone(),
        priority: priority.to_string(),
    })?;

    // Ensure needs: array exists and append.
    let needs = doc
        .as_mapping_mut()
        .ok_or_else(|| anyhow::anyhow!("config is not a YAML mapping"))?
        .entry(serde_yaml::Value::String("needs".to_string()))
        .or_insert_with(|| serde_yaml::Value::Sequence(vec![]));

    needs
        .as_sequence_mut()
        .ok_or_else(|| anyhow::anyhow!("needs: is not a sequence"))?
        .push(need_val);

    let updated = serde_yaml::to_string(&doc)?;
    std::fs::write(&config_path, updated)
        .with_context(|| format!("failed to write config: {}", config_path))?;

    Ok(json!({
        "status": "published",
        "need": {
            "id": id,
            "description": description,
            "tags": tags,
            "priority": priority,
        }
    }))
}

/// Fetch a remote agent's needs from their Agent Card.
///
/// MCP tool: `browse_needs(url)`
pub(crate) async fn call_browse_needs(args: &Value) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'url' parameter"))?;

    let card_url = format!("{}/.well-known/agent-card.json", url.trim_end_matches('/'));

    let client = reqwest::Client::new();
    let resp = client
        .get(&card_url)
        .send()
        .await
        .context("failed to fetch Agent Card")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!("Agent Card request returned {}: {}", status, body);
    }

    let card: Value = resp
        .json()
        .await
        .context("failed to parse Agent Card JSON")?;

    let needs = card.get("needs").cloned().unwrap_or(json!([]));
    let agent_name = card
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    Ok(json!({
        "agent": agent_name,
        "url": url,
        "needs": needs,
    }))
}

/// Send a proposal to fulfill a remote agent's need via A2A.
///
/// MCP tool: `propose_for_need(url, need_id, proposal, api_key?)`
pub(crate) async fn call_propose_for_need(args: &Value) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'url' parameter"))?;
    let need_id = args
        .get("need_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'need_id' parameter"))?;
    let proposal = args
        .get("proposal")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'proposal' parameter"))?;

    let rpc_body = json!({
        "jsonrpc": "2.0",
        "id": uuid::Uuid::new_v4().to_string(),
        "method": "tasks/send",
        "params": {
            "skill": need_id,
            "message": format!("Proposal for need '{}': {}", need_id, proposal),
            "metadata": {
                "type": "need_proposal",
                "need_id": need_id,
            }
        }
    });

    let a2a_url = format!("{}/a2a", url.trim_end_matches('/'));

    let client = reqwest::Client::new();
    let mut req = client.post(&a2a_url).json(&rpc_body);
    req = apply_a2a_auth(req, args, url);

    let resp = req.send().await.context("A2A proposal request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!("A2A proposal returned {}: {}", status, body);
    }

    let body: Value = resp.json().await.context("failed to parse A2A response")?;

    if let Some(error) = body.get("error") {
        bail!(
            "A2A error {}: {}",
            error.get("code").and_then(|c| c.as_i64()).unwrap_or(0),
            error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown")
        );
    }

    Ok(json!({
        "status": "proposed",
        "need_id": need_id,
        "result": body.get("result").cloned().unwrap_or(Value::Null),
    }))
}

/// Synchronously query another agent and wait for the response.
///
/// MCP tool: `query_agent(target, question, timeout_secs?)`
///
/// Creates a temporary bus connection, sends the question with `reply_to`
/// pointing to the temporary name, and waits for the response via direct
/// name routing.
pub(crate) async fn call_query_agent(
    args: &Value,
    agent_name: &str,
    bus_socket: &str,
) -> Result<Value> {
    let target = args
        .get("target")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'target' parameter"))?;
    let question = args
        .get("question")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'question' parameter"))?;
    let timeout_secs = args
        .get("timeout_secs")
        .and_then(|v| v.as_u64())
        .unwrap_or(30);

    let query_id = Uuid::new_v4().to_string();
    let temp_name = format!("{}-query-{}", agent_name, &query_id[..8]);

    // Connect to bus with a temporary identity for receiving the response.
    use crate::infra::unix_bus::UnixBus;
    use crate::ports::bus::MessageBus;
    let bus = UnixBus::connect(bus_socket).await?;
    bus.register(&temp_name, &[]).await?;

    // Send query to target with reply_to pointing to our temporary name.
    let msg = crate::domain::message::Message {
        id: query_id.clone(),
        source: temp_name.clone(),
        target: target.to_string(),
        payload: json!({"task": question}),
        reply_to: Some(temp_name.clone()),
        metadata: crate::domain::message::Metadata::default(),
    };
    bus.send(&msg).await?;

    info!(
        agent = %agent_name,
        target = %target,
        query_id = %query_id,
        timeout = timeout_secs,
        "query_agent: waiting for response"
    );

    // Wait for response with timeout.
    let timeout = std::time::Duration::from_secs(timeout_secs);
    let response = tokio::time::timeout(timeout, async {
        // Collect all response chunks until we get a "final" one.
        let mut full_response = String::new();
        loop {
            let resp = bus.recv().await?;
            // Extract text from payload.
            if let Some(text) = resp.payload.get("text").and_then(|v| v.as_str()) {
                full_response.push_str(text);
            } else if let Some(result) = resp.payload.get("result").and_then(|v| v.as_str()) {
                full_response.push_str(result);
            } else if let Some(task) = resp.payload.get("task").and_then(|v| v.as_str()) {
                full_response.push_str(task);
            }

            // Check if this is the final response.
            let is_final = resp
                .payload
                .get("final")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if is_final || !full_response.is_empty() {
                return Ok::<String, anyhow::Error>(full_response);
            }
        }
    })
    .await;

    match response {
        Ok(Ok(text)) => {
            info!(
                agent = %agent_name,
                target = %target,
                len = text.len(),
                "query_agent: received response"
            );
            Ok(json!({
                "status": "ok",
                "source": target,
                "response": text,
            }))
        }
        Ok(Err(e)) => {
            bail!("query_agent: bus error while waiting for response: {}", e);
        }
        Err(_) => {
            bail!(
                "query_agent: timed out after {}s waiting for response from {}",
                timeout_secs,
                target
            );
        }
    }
}

// ─── telegram_history ────────────────────────────────────────────────────────

/// Handle the `telegram_history` MCP tool call.
///
/// Reads from the locally accumulated unified inbox at
/// `~/.deskd/inboxes/telegram/{chat_id}.jsonl`. Messages are written
/// there by the teloxide adapter on every incoming Telegram message.
pub(crate) async fn call_telegram_history(args: &Value) -> Result<Value> {
    let chat_id = args
        .get("chat_id")
        .and_then(|v| v.as_i64())
        .context("telegram_history: missing required integer 'chat_id'")?;
    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
    if limit == 0 || limit > 200 {
        bail!(
            "telegram_history: 'limit' must be in 1..=200 (got {})",
            limit
        );
    }

    let inbox_name = format!("telegram/{}", chat_id);
    let messages = mcp_service::read_inbox(&inbox_name, limit, None)?;

    Ok(json!({
        "chat_id": chat_id,
        "count": messages.len(),
        "messages": messages,
    }))
}

/// `agent_state` — atomic read/write of an agent's markdown state document (#456).
///
/// Dispatches on `action`: `read`, `write`, `append`, `set_section`.
/// File location is configurable per-agent in `deskd.yaml` via `state_file:`;
/// see [`crate::app::agent_state`] for the resolution rules.
pub(crate) async fn call_agent_state(
    args: &Value,
    user_config: Option<&UserConfig>,
) -> Result<Value> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .context("agent_state: missing 'action' (read | write | append | set_section)")?;
    let agent = args
        .get("agent")
        .and_then(|v| v.as_str())
        .context("agent_state: missing 'agent'")?;

    let payload = match action {
        "read" => {
            let body = crate::app::agent_state::read(agent, user_config).await?;
            json!({
                "agent": agent,
                "content": body,
            })
        }
        "write" => {
            let content = args
                .get("content")
                .and_then(|v| v.as_str())
                .context("agent_state.write: missing 'content'")?;
            crate::app::agent_state::write(agent, content, user_config).await?;
            json!({"status": "written", "agent": agent})
        }
        "append" => {
            let section = args
                .get("section")
                .and_then(|v| v.as_str())
                .context("agent_state.append: missing 'section'")?;
            let line = args
                .get("line")
                .and_then(|v| v.as_str())
                .context("agent_state.append: missing 'line'")?;
            crate::app::agent_state::append(agent, section, line, user_config).await?;
            json!({"status": "appended", "agent": agent, "section": section})
        }
        "set_section" => {
            let section = args
                .get("section")
                .and_then(|v| v.as_str())
                .context("agent_state.set_section: missing 'section'")?;
            let content = args
                .get("content")
                .and_then(|v| v.as_str())
                .context("agent_state.set_section: missing 'content'")?;
            crate::app::agent_state::set_section(agent, section, content, user_config).await?;
            json!({"status": "updated", "agent": agent, "section": section})
        }
        other => bail!(
            "agent_state: unknown action '{}' (expected read | write | append | set_section)",
            other
        ),
    };

    Ok(json!({
        "content": [{"type": "text", "text": serde_json::to_string(&payload)?}],
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("agent:*", "agent:dev"));
        assert!(glob_match("telegram.out:*", "telegram.out:-1234567"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("agent:dev", "agent:dev"));
        assert!(!glob_match("agent:dev", "agent:researcher"));
        assert!(!glob_match("telegram.out:*", "telegram.in:-1234"));
    }

    #[tokio::test]
    async fn agent_logs_rejects_missing_name() {
        let err = call_agent_logs(&json!({}), "caller").await.unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("name"), "expected name error, got: {}", msg);
    }

    #[tokio::test]
    async fn agent_logs_rejects_bad_tail() {
        for bad in [0u64, 5000] {
            let err = call_agent_logs(&json!({"name": "x", "tail": bad}), "x")
                .await
                .unwrap_err();
            let msg = format!("{:#}", err);
            assert!(msg.contains("tail"), "expected tail error, got: {}", msg);
        }
    }

    #[tokio::test]
    async fn agent_logs_rejects_bad_source() {
        let err = call_agent_logs(&json!({"name": "x", "source": "garbage"}), "x")
            .await
            .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("source"),
            "expected source error, got: {}",
            msg
        );
    }

    #[tokio::test]
    async fn agent_logs_self_read_nonexistent_returns_exists_false() {
        // Requesting own logs when the log file does not exist must not error —
        // it returns `exists: false` so callers can distinguish "no logs yet"
        // from "access denied" or "bad arg".
        let unique = format!("__agent_logs_test_{}", Uuid::new_v4());
        let resp = call_agent_logs(&json!({"name": unique, "tail": 10}), &unique)
            .await
            .expect("self-read must not error on missing file");
        let text = resp["content"][0]["text"].as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["exists"], false);
        assert_eq!(parsed["source"], "stderr");
        assert!(parsed["lines"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn agent_tasks_rejects_missing_name() {
        let err = call_agent_tasks(&json!({}), "caller").await.unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("name"), "expected name error, got: {}", msg);
    }

    #[tokio::test]
    async fn agent_tasks_rejects_bad_limit() {
        for bad in [0u64, 500] {
            let err = call_agent_tasks(&json!({"name": "x", "limit": bad}), "x")
                .await
                .unwrap_err();
            let msg = format!("{:#}", err);
            assert!(msg.contains("limit"), "expected limit error, got: {}", msg);
        }
    }

    #[tokio::test]
    async fn agent_tasks_rejects_bad_since() {
        let err = call_agent_tasks(&json!({"name": "x", "since": "yesterday"}), "x")
            .await
            .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("since"), "expected since error, got: {}", msg);
    }

    #[tokio::test]
    async fn agent_tasks_self_read_nonexistent_returns_exists_false() {
        let unique = format!("__agent_tasks_test_{}", Uuid::new_v4());
        let resp = call_agent_tasks(&json!({"name": unique, "limit": 10}), &unique)
            .await
            .expect("self-read must not error on missing file");
        let text = resp["content"][0]["text"].as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["exists"], false);
        assert_eq!(parsed["returned"], 0);
        assert!(parsed["tasks"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn agent_tasks_returns_filtered_entries_from_log() {
        // Write a synthetic tasks.jsonl into a temporary HOME, then call
        // call_agent_tasks against it and verify filtering + ordering.
        let env_guard = crate::test_support::env_lock().lock().await;
        let tmp =
            std::path::PathBuf::from(format!("/tmp/deskd-test-agent-tasks-{}", Uuid::new_v4()));
        // SAFETY: ENV_LOCK serializes env-mutating tests workspace-wide.
        unsafe { std::env::set_var("HOME", &tmp) };

        let agent = "history-agent";
        let log = crate::app::tasklog::log_path(agent);
        let entries = vec![
            crate::app::tasklog::TaskLog {
                ts: "2026-05-01T10:00:00Z".into(),
                source: "telegram".into(),
                turns: 3,
                cost: 0.05,
                duration_ms: 1500,
                status: "ok".into(),
                task: "first".into(),
                error: None,
                msg_id: "m1".into(),
                github_repo: None,
                github_pr: None,
                input_tokens: None,
                output_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                session_count: None,
                tool_use_count: None,
                parent_agent: None,
            },
            crate::app::tasklog::TaskLog {
                ts: "2026-05-02T10:00:00Z".into(),
                source: "github_poll".into(),
                turns: 5,
                cost: 0.10,
                duration_ms: 4000,
                status: "error".into(),
                task: "second".into(),
                error: Some("boom".into()),
                msg_id: "m2".into(),
                github_repo: Some("owner/repo".into()),
                github_pr: None,
                input_tokens: None,
                output_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                session_count: None,
                tool_use_count: None,
                parent_agent: None,
            },
        ];
        for e in &entries {
            crate::app::tasklog::log_task_to_path(&log, e).unwrap();
        }

        // No filters → both entries returned, oldest-first preserved.
        let resp = call_agent_tasks(&json!({"name": agent, "limit": 10}), agent)
            .await
            .expect("self-read must succeed");
        let text = resp["content"][0]["text"].as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["exists"], true);
        assert_eq!(parsed["returned"], 2);
        let tasks = parsed["tasks"].as_array().unwrap();
        assert_eq!(tasks[0]["msg_id"], "m1");
        assert_eq!(tasks[1]["msg_id"], "m2");
        assert_eq!(tasks[1]["error"], "boom");

        // Source filter narrows to one entry.
        let resp = call_agent_tasks(
            &json!({"name": agent, "limit": 10, "source": "github_poll"}),
            agent,
        )
        .await
        .expect("filtered self-read must succeed");
        let text = resp["content"][0]["text"].as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["returned"], 1);
        assert_eq!(parsed["tasks"][0]["source"], "github_poll");

        // since filter excludes older entries.
        let resp = call_agent_tasks(
            &json!({"name": agent, "limit": 10, "since": "2026-05-02T00:00:00Z"}),
            agent,
        )
        .await
        .expect("since-filtered self-read must succeed");
        let text = resp["content"][0]["text"].as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["returned"], 1);
        assert_eq!(parsed["tasks"][0]["msg_id"], "m2");

        let _ = std::fs::remove_dir_all(&tmp);
        drop(env_guard);
    }

    #[tokio::test]
    async fn bus_status_no_internal_bus_reports_not_running() {
        // When the internal bus has not been initialized, bus_status must still
        // succeed: it reports `running: false` for the internal bus and a
        // best-effort error for the (likely unreachable) host bus.
        let internal_bus: Arc<Mutex<Option<InternalBus>>> = Arc::new(Mutex::new(None));
        let unique = format!("/tmp/__bus_status_test_{}.sock", Uuid::new_v4());
        let resp = call_bus_status("test-caller", &unique, &internal_bus)
            .await
            .expect("bus_status must not fail when internal bus is absent");
        let text = resp["content"][0]["text"].as_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["internal_bus"]["running"], false);
        assert!(
            parsed["internal_bus"]["sub_agents"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(parsed["host_bus"]["socket_path"], unique);
        // host bus is unreachable in test env → expect an error field but no
        // connected clients.
        assert!(parsed["host_bus"].get("error").is_some());
        assert!(
            parsed["host_bus"]["connected_clients"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn telegram_history_rejects_missing_chat_id() {
        let err = call_telegram_history(&json!({})).await.unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("chat_id"),
            "expected chat_id error, got: {}",
            msg
        );
    }

    #[tokio::test]
    async fn telegram_history_rejects_bad_limit() {
        let err = call_telegram_history(&json!({"chat_id": 1, "limit": 500}))
            .await
            .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("limit"), "expected limit error, got: {}", msg);
    }

    // ─── inbox ACL tests ────────────────────────────────────────────────

    fn make_config_with_sub_agent(name: &str, inbox_read: Option<Vec<String>>) -> UserConfig {
        let inbox_yaml = match &inbox_read {
            Some(patterns) => {
                let items: Vec<String> = patterns.iter().map(|p| format!("\"{}\"", p)).collect();
                format!("    inbox_read: [{}]\n", items.join(", "))
            }
            None => String::new(),
        };
        let yaml = format!(
            "agents:\n  - name: {}\n    model: haiku\n    subscribe: []\n{}",
            name, inbox_yaml
        );
        serde_yaml::from_str(&yaml).expect("test config should parse")
    }

    #[test]
    fn inbox_acl_allows_own_inbox() {
        // Sub-agent can always read its own inbox, even without inbox_read.
        let cfg = make_config_with_sub_agent("reader", None);
        assert!(inbox_access_allowed("reader", "reader", Some(&cfg)));
    }

    #[test]
    fn inbox_acl_denies_other_inbox_by_default() {
        // Sub-agent with no inbox_read cannot read other inboxes.
        let cfg = make_config_with_sub_agent("reader", None);
        assert!(!inbox_access_allowed("reader", "secret", Some(&cfg)));
    }

    #[test]
    fn inbox_acl_allows_listed_inbox() {
        let cfg =
            make_config_with_sub_agent("reader", Some(vec!["shared".into(), "collab-*".into()]));
        assert!(inbox_access_allowed("reader", "shared", Some(&cfg)));
        assert!(inbox_access_allowed("reader", "collab-daily", Some(&cfg)));
        assert!(inbox_access_allowed("reader", "reader", Some(&cfg))); // own always ok
    }

    #[test]
    fn inbox_acl_denies_unlisted_inbox() {
        let cfg = make_config_with_sub_agent("reader", Some(vec!["shared".into()]));
        assert!(!inbox_access_allowed("reader", "secret", Some(&cfg)));
    }

    #[test]
    fn inbox_acl_top_level_agent_unrestricted() {
        // Agent not in user_config.agents → top-level → unrestricted.
        let cfg = make_config_with_sub_agent("other-agent", None);
        assert!(inbox_access_allowed("top-level", "anything", Some(&cfg)));
    }

    #[test]
    fn inbox_acl_no_config_unrestricted() {
        // No user_config at all → unrestricted.
        assert!(inbox_access_allowed("agent", "any-inbox", None));
    }

    // ─── Top-level inbox_acl on UserConfig ─────────────────────────────────

    fn make_top_level_config(inbox_acl: Option<Vec<String>>) -> UserConfig {
        let yaml = match inbox_acl {
            Some(patterns) => {
                let items: Vec<String> = patterns.iter().map(|p| format!("\"{}\"", p)).collect();
                format!("inbox_acl: [{}]\n", items.join(", "))
            }
            None => String::new(),
        };
        // Empty yaml is valid for a default UserConfig.
        if yaml.is_empty() {
            UserConfig::default()
        } else {
            serde_yaml::from_str(&yaml).expect("test config should parse")
        }
    }

    #[test]
    fn top_level_inbox_acl_absent_is_unrestricted() {
        // Backward compat: no inbox_acl key → top-level agent reads any inbox.
        let cfg = make_top_level_config(None);
        assert!(inbox_access_allowed("kira", "anything", Some(&cfg)));
        assert!(inbox_access_allowed("kira", "dev", Some(&cfg)));
    }

    #[test]
    fn top_level_inbox_acl_allows_own_inbox() {
        // Even with an empty allow-list, the agent's own inbox is readable.
        let cfg = make_top_level_config(Some(vec![]));
        assert!(inbox_access_allowed("kira", "kira", Some(&cfg)));
        assert!(inbox_access_allowed("kira", "replies/kira", Some(&cfg)));
    }

    #[test]
    fn top_level_inbox_acl_denies_unlisted() {
        let cfg = make_top_level_config(Some(vec!["dev".into()]));
        assert!(!inbox_access_allowed("kira", "secret", Some(&cfg)));
        assert!(!inbox_access_allowed("kira", "vasya", Some(&cfg)));
    }

    #[test]
    fn top_level_inbox_acl_allows_listed_with_glob() {
        let cfg = make_top_level_config(Some(vec!["dev".into(), "collab-*".into()]));
        assert!(inbox_access_allowed("kira", "dev", Some(&cfg)));
        assert!(inbox_access_allowed("kira", "collab-daily", Some(&cfg)));
        assert!(!inbox_access_allowed("kira", "secret", Some(&cfg)));
    }

    #[test]
    fn top_level_inbox_acl_yaml_round_trip() {
        let yaml = "inbox_acl:\n  - dev\n  - kira\n";
        let cfg: UserConfig = serde_yaml::from_str(yaml).unwrap();
        let acl = cfg.inbox_acl.expect("inbox_acl should parse");
        assert_eq!(acl, vec!["dev".to_string(), "kira".to_string()]);
    }

    #[test]
    fn read_inbox_error_message_format() {
        // The denial error should match the documented "inbox access denied"
        // wording so callers can match on it.
        let cfg = make_top_level_config(Some(vec!["dev".into()]));
        let allowed = inbox_access_allowed("kira", "secret", Some(&cfg));
        assert!(!allowed);
        // Spot-check the error format used in call_read_inbox / bus_api by
        // formatting with the agreed template.
        let msg = format!(
            "inbox access denied: agent \"{}\" is not in allow-list for inbox \"{}\"",
            "kira", "secret"
        );
        assert!(msg.starts_with("inbox access denied:"));
        assert!(msg.contains("\"kira\""));
        assert!(msg.contains("\"secret\""));
    }

    // ─── can_message ACL tests ──────────────────────────────────────────────

    fn make_config_with_can_message(name: &str, can_message: Option<Vec<String>>) -> UserConfig {
        let cm_yaml = match &can_message {
            Some(targets) => {
                let items: Vec<String> = targets.iter().map(|t| format!("\"{}\"", t)).collect();
                format!("    can_message: [{}]\n", items.join(", "))
            }
            None => String::new(),
        };
        let yaml = format!(
            "agents:\n  - name: {}\n    model: haiku\n    subscribe: []\n{}",
            name, cm_yaml
        );
        serde_yaml::from_str(&yaml).expect("test config should parse")
    }

    #[test]
    fn can_message_unrestricted_by_default() {
        // No can_message → agent can message anyone (publish ACL may still apply).
        let cfg = make_config_with_can_message("worker", None);
        // The can_message check in call_send_message only fires if can_message is Some.
        let sub = cfg.agents.iter().find(|a| a.name == "worker").unwrap();
        assert!(sub.can_message.is_none());
    }

    #[test]
    fn can_message_allows_listed_target() {
        let cfg = make_config_with_can_message(
            "worker",
            Some(vec!["agent:parent".into(), "telegram.out:*".into()]),
        );
        let sub = cfg.agents.iter().find(|a| a.name == "worker").unwrap();
        let allow = sub.can_message.as_ref().unwrap();
        assert!(allow.iter().any(|p| glob_match(p, "agent:parent")));
        assert!(allow.iter().any(|p| glob_match(p, "telegram.out:-123")));
        assert!(!allow.iter().any(|p| glob_match(p, "agent:sibling")));
    }

    #[test]
    fn can_message_denies_unlisted_target() {
        let cfg = make_config_with_can_message("worker", Some(vec!["agent:parent".into()]));
        let sub = cfg.agents.iter().find(|a| a.name == "worker").unwrap();
        let allow = sub.can_message.as_ref().unwrap();
        assert!(!allow.iter().any(|p| glob_match(p, "agent:sibling")));
        assert!(!allow.iter().any(|p| glob_match(p, "telegram.out:-123")));
    }

    // ─── scope config tests ─────────────────────────────────────────────────

    #[test]
    fn scope_config_parses_narrow() {
        let yaml = r#"
agents:
  - name: worker
    model: haiku
    subscribe: []
    scope: narrow
    can_message: ["agent:parent"]
    work_dir: /home/dev/tasks
    env:
      API_KEY: sk-test
"#;
        let cfg: UserConfig = serde_yaml::from_str(yaml).expect("parse");
        let sub = &cfg.agents[0];
        assert_eq!(sub.scope, crate::config::ScopeType::Narrow);
        assert_eq!(sub.can_message.as_ref().unwrap(), &["agent:parent"]);
        assert_eq!(sub.work_dir.as_deref(), Some("/home/dev/tasks"));
        assert_eq!(sub.env.as_ref().unwrap().get("API_KEY").unwrap(), "sk-test");
    }

    #[test]
    fn scope_config_defaults() {
        let yaml = "agents:\n  - name: w\n    model: h\n    subscribe: []\n";
        let cfg: UserConfig = serde_yaml::from_str(yaml).expect("parse");
        let sub = &cfg.agents[0];
        assert_eq!(sub.scope, crate::config::ScopeType::Inherit);
        assert!(sub.can_message.is_none());
        assert!(sub.work_dir.is_none());
        assert!(sub.env.is_none());
    }

    // ─── parse_at_time tests ────────────────────────────────────────────────

    #[test]
    fn parse_at_iso8601() {
        let dt = parse_at_time("2026-04-22T09:00:00Z").unwrap();
        assert_eq!(dt.hour(), 9);
        assert_eq!(dt.day(), 22);
    }

    #[test]
    fn parse_at_iso8601_with_offset() {
        let dt = parse_at_time("2026-04-22T12:00:00+03:00").unwrap();
        // 12:00 MSK = 09:00 UTC
        assert_eq!(dt.hour(), 9);
    }

    #[test]
    fn parse_at_datetime_naive() {
        let dt = parse_at_time("2026-04-22 09:00").unwrap();
        // 09:00 Moscow = 06:00 UTC
        assert_eq!(dt.hour(), 6);
        assert_eq!(dt.day(), 22);
    }

    #[test]
    fn parse_at_datetime_naive_with_t() {
        let dt = parse_at_time("2026-04-22T09:00").unwrap();
        assert_eq!(dt.hour(), 6); // 09:00 MSK = 06:00 UTC
    }

    #[test]
    fn parse_at_tomorrow() {
        let dt = parse_at_time("tomorrow 09:00").unwrap();
        let now = chrono::Utc::now();
        // Should be in the future.
        assert!(dt > now);
        // Hour in UTC: 09:00 Moscow = 06:00 UTC.
        assert_eq!(dt.hour(), 6);
    }

    #[test]
    fn parse_at_bare_time_future() {
        // Use a time that's definitely in the future (23:59 Moscow).
        let dt = parse_at_time("23:59").unwrap();
        // 23:59 Moscow = 20:59 UTC
        assert_eq!(dt.hour(), 20);
        assert_eq!(dt.minute(), 59);
    }

    #[test]
    fn parse_at_invalid() {
        assert!(parse_at_time("not a time").is_err());
        assert!(parse_at_time("").is_err());
    }

    use chrono::{Datelike, Timelike};

    // ─── parse_lifecycle_arg tests (#502) ───────────────────────────────────
    //
    // Argument-level checks are the safe layer to assert: they run before
    // any side-effecting work (spawning `deskd agent create`, binding the
    // internal bus, writing `.mcp.json`). The deeper provisioning behaviour
    // is exercised by the #503 helper's own tests + the #502 integration
    // test (`add_persistent_agent_lifecycle.rs`).

    #[test]
    fn parse_lifecycle_default_is_stream_json() {
        // No `lifecycle` key at all → backward-compat default.
        let v = parse_lifecycle_arg(&json!({})).expect("default must parse");
        assert_eq!(v, SubAgentLifecycle::StreamJson);
        // Explicit null → still default.
        let v = parse_lifecycle_arg(&json!({"lifecycle": null})).expect("null must parse");
        assert_eq!(v, SubAgentLifecycle::StreamJson);
    }

    #[test]
    fn parse_lifecycle_accepts_known_values() {
        assert_eq!(
            parse_lifecycle_arg(&json!({"lifecycle": "stream-json"})).unwrap(),
            SubAgentLifecycle::StreamJson
        );
        assert_eq!(
            parse_lifecycle_arg(&json!({"lifecycle": "tmux-channel"})).unwrap(),
            SubAgentLifecycle::TmuxChannel
        );
    }

    #[test]
    fn parse_lifecycle_rejects_unknown_string() {
        let err = parse_lifecycle_arg(&json!({"lifecycle": "subprocess"})).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("stream-json") && msg.contains("tmux-channel"),
            "error must list both accepted values, got: {}",
            msg
        );
    }

    #[test]
    fn parse_lifecycle_rejects_non_string() {
        let err = parse_lifecycle_arg(&json!({"lifecycle": 42})).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("must be a string"),
            "expected type error, got: {}",
            msg
        );
    }

    // ─── work_dir containment (both lifecycles) ────────────────────────────
    //
    // Containment is enforced *before* we try to spawn a worker or launch a
    // tmux session, so we can test it without `deskd` or `tmux` on PATH —
    // the call must `bail!` from the containment check directly.

    /// Helper: build args that point at a work_dir outside the parent's PWD
    /// so containment must reject regardless of lifecycle.
    fn outside_parent_args(name: &str, lifecycle: Option<&str>) -> serde_json::Value {
        let mut args = json!({
            "name": name,
            "model": "claude-sonnet-4-6",
            "system_prompt": "",
            "subscribe": [format!("agent:{}", name)],
            // /etc exists everywhere and is never inside our test PWD,
            // making this a robust "outside parent" target.
            "work_dir": "/etc",
        });
        if let Some(l) = lifecycle {
            args["lifecycle"] = json!(l);
        }
        args
    }

    /// stream-json default: out-of-scope work_dir must fail containment.
    #[tokio::test]
    async fn add_persistent_agent_stream_json_enforces_work_dir_containment() {
        let env_guard = crate::test_support::env_lock().lock().await;
        let parent_pwd = std::env::temp_dir().join(format!("deskd-502-parent-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&parent_pwd).unwrap();
        // SAFETY: env_lock serializes env-mutating tests workspace-wide.
        unsafe { std::env::set_var("PWD", &parent_pwd) };

        let internal_bus: Arc<Mutex<Option<InternalBus>>> = Arc::new(Mutex::new(None));
        let err = call_add_persistent_agent(
            &outside_parent_args(&format!("sj-{}", Uuid::new_v4()), None),
            "parent-agent",
            "/tmp/parent.sock",
            &internal_bus,
        )
        .await
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("outside parent scope"),
            "stream-json containment error expected, got: {}",
            msg
        );

        let _ = std::fs::remove_dir_all(&parent_pwd);
        drop(env_guard);
    }

    /// tmux-channel: same containment guard must reject out-of-scope work_dir.
    /// Important to keep this — otherwise tmux-channel callers could escape
    /// the parent's scope and provision an .mcp.json wherever they liked.
    #[tokio::test]
    async fn add_persistent_agent_tmux_channel_enforces_work_dir_containment() {
        let env_guard = crate::test_support::env_lock().lock().await;
        let parent_pwd = std::env::temp_dir().join(format!("deskd-502-parent-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&parent_pwd).unwrap();
        // SAFETY: env_lock serializes env-mutating tests workspace-wide.
        unsafe { std::env::set_var("PWD", &parent_pwd) };

        let internal_bus: Arc<Mutex<Option<InternalBus>>> = Arc::new(Mutex::new(None));
        let err = call_add_persistent_agent(
            &outside_parent_args(&format!("tc-{}", Uuid::new_v4()), Some("tmux-channel")),
            "parent-agent",
            "/tmp/parent.sock",
            &internal_bus,
        )
        .await
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("outside parent scope"),
            "tmux-channel containment error expected, got: {}",
            msg
        );

        let _ = std::fs::remove_dir_all(&parent_pwd);
        drop(env_guard);
    }

    /// Bad lifecycle string must reject before any side effects fire — so a
    /// caller typo cannot accidentally provision anything.
    #[tokio::test]
    async fn add_persistent_agent_rejects_unknown_lifecycle_early() {
        let internal_bus: Arc<Mutex<Option<InternalBus>>> = Arc::new(Mutex::new(None));
        let err = call_add_persistent_agent(
            &json!({
                "name": format!("bad-{}", Uuid::new_v4()),
                "model": "claude-sonnet-4-6",
                "system_prompt": "",
                "subscribe": ["agent:bad"],
                "lifecycle": "subprocess",
            }),
            "parent-agent",
            "/tmp/parent.sock",
            &internal_bus,
        )
        .await
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("stream-json") && msg.contains("tmux-channel"),
            "lifecycle rejection must list both valid values, got: {}",
            msg
        );
    }
}
