//! Agent registry — CRUD, state persistence, and one-shot execution.
//!
//! Extracted from `agent.rs` (#280). Manages agent state files on disk,
//! creates/recovers agents, and provides one-shot `send` / `spawn_ephemeral`.

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

use crate::config::{self, ContainerConfig, UserConfig};
use crate::domain::config_types::{
    ConfigAgentKind, ConfigAgentRuntime, ConfigContextConfig, ConfigLaunchMode, ConfigSessionMode,
};

use super::process_builder::{build_command, inject_required_flags};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub name: String,
    pub model: String,
    pub system_prompt: String,
    pub work_dir: String,
    pub max_turns: u32,
    /// Optional Linux user to run the agent process as.
    #[serde(default)]
    pub unix_user: Option<String>,
    /// Command to run. Defaults to ["claude"].
    #[serde(default = "default_agent_command")]
    pub command: Vec<String>,
    /// Path to the agent's deskd.yaml (injected as DESKD_AGENT_CONFIG into claude).
    #[serde(default)]
    pub config_path: Option<String>,
    /// Container config. When set, the agent runs inside a container.
    #[serde(default)]
    pub container: Option<ContainerConfig>,
    /// Session mode: persistent (default) or ephemeral.
    #[serde(default)]
    pub session: ConfigSessionMode,
    /// Agent runtime protocol: claude (default) or acp.
    #[serde(default)]
    pub runtime: ConfigAgentRuntime,
    /// Launch mode: subprocess (default) or tmux (#452).
    /// When `tmux`, deskd starts the agent's Claude REPL inside a detached
    /// tmux session named `deskd-<agent>` instead of spawning a direct child.
    #[serde(default)]
    pub launch_mode: ConfigLaunchMode,
    /// Worker loop kind: executor (default, full lifecycle) or context (lightweight Q&A).
    #[serde(default)]
    pub kind: ConfigAgentKind,
    /// Context system configuration (main branch, compaction).
    #[serde(default)]
    pub context: Option<ConfigContextConfig>,
    /// Memory agent: context usage fraction (0.0–1.0) that triggers compaction.
    /// Default: 0.8.
    #[serde(default)]
    pub compact_threshold: Option<f64>,
    /// Auto-compact threshold in absolute tokens. When the live context size
    /// crosses this value, `/context` surfaces a warning. Resolution order in
    /// configs: per-agent (SubAgentDef) > global (UserConfig) > built-in default
    /// (`context_size::DEFAULT_AUTO_COMPACT_THRESHOLD`, 300k).
    #[serde(default)]
    pub auto_compact_threshold_tokens: Option<u64>,
    /// Number of consecutive empty completions that triggers an auto-restart
    /// of the agent worker (#424). `None` falls back to the built-in default
    /// (3); see [`crate::app::worker::DEFAULT_EMPTY_COMPLETION_THRESHOLD`].
    #[serde(default)]
    pub empty_completion_threshold: Option<u32>,
    /// Minimum seconds between auto-restarts triggered by empty-completion
    /// detection (#424). `None` falls back to the built-in default (60); see
    /// [`crate::app::worker::DEFAULT_EMPTY_COMPLETION_RESTART_MIN_SECS`].
    #[serde(default)]
    pub empty_completion_restart_min_secs: Option<u64>,
}

pub fn default_agent_command() -> Vec<String> {
    vec!["claude".to_string()]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentState {
    pub config: AgentConfig,
    pub pid: u32,
    pub session_id: String,
    pub total_turns: u32,
    pub total_cost: f64,
    pub created_at: String,
    /// "idle" or "working"
    #[serde(default = "default_status")]
    pub status: String,
    /// Truncated text of the task currently being processed.
    #[serde(default)]
    pub current_task: String,
    /// Name of the parent agent that spawned this sub-agent, if any.
    #[serde(default)]
    pub parent: Option<String>,
    /// Scope type: "inherit" or "narrow". Set at creation by parent.
    #[serde(default)]
    pub scope: Option<String>,
    /// Allow-list of targets this agent can message. None = unrestricted.
    #[serde(default)]
    pub can_message: Option<Vec<String>>,
    /// Names of env var keys provided to this agent (values not stored for security).
    #[serde(default)]
    pub env_keys: Option<Vec<String>>,
    /// Timestamp (RFC 3339) of when the current Claude session started.
    /// Updated whenever session_id changes (new session or resume).
    #[serde(default)]
    pub session_start: Option<String>,
    /// Cost accumulated in the current session (resets on session_id change).
    #[serde(default)]
    pub session_cost: f64,
    /// Turns accumulated in the current session (resets on session_id change).
    #[serde(default)]
    pub session_turns: u32,
    /// Number of consecutive empty completions observed (#424).
    /// Reset to 0 on the first non-empty completion. Used for auto-restart
    /// of suspected-hung claude workers.
    #[serde(default)]
    pub consecutive_empty_completions: u32,
    /// RFC 3339 timestamp of the last auto-restart triggered by empty-completion
    /// detection (#424). Used to enforce the rate-limit between restarts.
    #[serde(default)]
    pub last_empty_restart_at: Option<String>,
    /// Cumulative number of auto-restarts triggered by empty-completion detection (#424).
    #[serde(default)]
    pub total_empty_restarts: u32,
}

fn default_status() -> String {
    "idle".to_string()
}

fn state_path(name: &str) -> PathBuf {
    config::state_dir().join(format!("{}.yaml", name))
}

/// Like `state_path`, but resolves the agents dir under an explicit home path
/// instead of `$HOME`. Used by tests so they don't have to mutate process env
/// (which races under parallel test harness; see #423 CI flake on PR #428).
fn state_path_in(home: &Path, name: &str) -> PathBuf {
    crate::infra::paths::state_dir_in(home).join(format!("{}.yaml", name))
}

fn log_path(name: &str) -> PathBuf {
    config::log_dir().join(format!("{}.log", name))
}

/// Path to the agent's stderr log file.
pub fn stderr_log_path(name: &str) -> PathBuf {
    config::log_dir().join(format!("{}.stderr.log", name))
}

/// Path to the agent's stream-json log file.
pub fn stream_log_path(name: &str) -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let dir = PathBuf::from(home).join(".deskd").join("logs").join(name);
    dir.join("stream.jsonl")
}

/// Rotate stream log: if file exceeds max_bytes, keep the last half.
pub(crate) fn rotate_stream_log(path: &PathBuf, max_bytes: u64) {
    let Ok(meta) = std::fs::metadata(path) else {
        return;
    };
    if meta.len() <= max_bytes {
        return;
    }
    let Ok(content) = std::fs::read_to_string(path) else {
        return;
    };
    let lines: Vec<&str> = content.lines().collect();
    let keep_from = lines.len() / 2;
    let truncated: String = lines[keep_from..].iter().flat_map(|l| [*l, "\n"]).collect();
    let _ = std::fs::write(path, truncated);
}

pub fn load_state(name: &str) -> Result<AgentState> {
    let path = state_path(name);
    load_state_at(&path, name)
}

/// Like `load_state`, but reads from `~/.deskd/agents/<name>.yaml` under an
/// explicit `home` directory instead of `$HOME`. Used by tests to avoid
/// mutating process env (see #423 / PR #428 CI flake).
pub fn load_state_in(home: &Path, name: &str) -> Result<AgentState> {
    let path = state_path_in(home, name);
    load_state_at(&path, name)
}

fn load_state_at(path: &Path, name: &str) -> Result<AgentState> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("Agent '{}' not found", name))?;
    let state: AgentState = serde_yaml::from_str(&content)?;
    Ok(state)
}

pub(crate) fn save_state(state: &AgentState) -> Result<()> {
    let path = state_path(&state.config.name);
    save_state_at(&path, state)
}

pub fn save_state_pub(state: &AgentState) -> Result<()> {
    save_state(state)
}

/// Like `save_state_pub`, but writes under an explicit `home` directory
/// instead of `$HOME`. Used by tests (see #423 / PR #428 CI flake).
pub fn save_state_in(home: &Path, state: &AgentState) -> Result<()> {
    let path = state_path_in(home, &state.config.name);
    save_state_at(&path, state)
}

fn save_state_at(path: &Path, state: &AgentState) -> Result<()> {
    let content = serde_yaml::to_string(state)?;
    std::fs::write(path, content)?;
    Ok(())
}

/// Create a new agent (saves state file; does not start a worker process).
pub async fn create(cfg: &AgentConfig) -> Result<AgentState> {
    if state_path(&cfg.name).exists() {
        bail!("Agent '{}' already exists. Remove it first.", cfg.name);
    }

    let state = AgentState {
        config: cfg.clone(),
        pid: 0,
        session_id: String::new(),
        total_turns: 0,
        total_cost: 0.0,
        created_at: Utc::now().to_rfc3339(),
        status: "idle".to_string(),
        current_task: String::new(),
        parent: None,
        scope: None,
        can_message: None,
        env_keys: None,
        session_start: None,
        session_cost: 0.0,
        session_turns: 0,
        consecutive_empty_completions: 0,
        last_empty_restart_at: None,
        total_empty_restarts: 0,
    };

    save_state(&state)?;
    info!(agent = %cfg.name, "agent created");
    Ok(state)
}

/// Create or update agent state from an AgentConfig.
/// If state already exists, updates the config fields but preserves session/cost/turns.
/// Used for sub-agents spawned from deskd.yaml.
pub async fn create_or_update_from_config(cfg: &AgentConfig) -> Result<AgentState> {
    let path = state_path(&cfg.name);
    if path.exists() {
        let mut state = load_state(&cfg.name)?;
        state.config = cfg.clone();
        save_state(&state)?;
        info!(agent = %cfg.name, "sub-agent state updated");
        return Ok(state);
    }
    let state = AgentState {
        config: cfg.clone(),
        pid: 0,
        session_id: String::new(),
        total_turns: 0,
        total_cost: 0.0,
        created_at: Utc::now().to_rfc3339(),
        status: "idle".to_string(),
        current_task: String::new(),
        parent: None,
        scope: None,
        can_message: None,
        env_keys: None,
        session_start: None,
        session_cost: 0.0,
        session_turns: 0,
        consecutive_empty_completions: 0,
        last_empty_restart_at: None,
        total_empty_restarts: 0,
    };
    save_state(&state)?;
    info!(agent = %cfg.name, "sub-agent created");
    Ok(state)
}

/// Create or recover agent state from workspace AgentDef + optional UserConfig.
/// If state already exists, returns it with config fields updated from current def.
/// Model priority: workspace def.model override > user_cfg.model > default.
pub async fn create_or_recover(
    def: &config::AgentDef,
    user_cfg: Option<&UserConfig>,
) -> Result<AgentState> {
    let model = def
        .model
        .clone()
        .or_else(|| user_cfg.map(|c| c.model.clone()))
        .unwrap_or_else(|| "claude-sonnet-4-6".to_string());

    let system_prompt = user_cfg
        .map(|c| c.system_prompt.clone())
        .unwrap_or_default();

    let max_turns = user_cfg.map(|c| c.max_turns).unwrap_or(100);

    let cfg = AgentConfig {
        name: def.name.clone(),
        model,
        system_prompt,
        work_dir: def.work_dir.clone(),
        max_turns,
        unix_user: def.unix_user.clone(),
        command: def.command.clone(),
        config_path: Some(def.config_path()),
        container: def.container.clone(),
        session: ConfigSessionMode::default(),
        runtime: def.runtime.clone(),
        launch_mode: def.launch_mode.clone(),
        kind: ConfigAgentKind::default(),
        context: user_cfg.and_then(|c| c.context.clone()),
        compact_threshold: None,
        auto_compact_threshold_tokens: user_cfg.and_then(|c| c.auto_compact_threshold_tokens),
        empty_completion_threshold: None,
        empty_completion_restart_min_secs: None,
    };

    let path = state_path(&def.name);
    if path.exists() {
        let mut state = load_state(&def.name)?;
        state.config = cfg;
        save_state(&state)?;
        info!(agent = %def.name, session_id = %state.session_id, "recovered existing agent state");
        return Ok(state);
    }

    let state = AgentState {
        config: cfg,
        pid: 0,
        session_id: String::new(),
        total_turns: 0,
        total_cost: 0.0,
        created_at: Utc::now().to_rfc3339(),
        status: "idle".to_string(),
        current_task: String::new(),
        parent: None,
        scope: None,
        can_message: None,
        env_keys: None,
        session_start: None,
        session_cost: 0.0,
        session_turns: 0,
        consecutive_empty_completions: 0,
        last_empty_restart_at: None,
        total_empty_restarts: 0,
    };
    save_state(&state)?;
    info!(agent = %def.name, "agent created");
    Ok(state)
}

/// Run claude for one task, return the response text.
///
/// `bus_socket`: path to the agent's bus (injected as DESKD_BUS_SOCKET).
/// Claude uses this to call the `send_message` MCP tool.
pub async fn send(
    name: &str,
    message: &str,
    max_turns: Option<u32>,
    bus_socket: Option<&str>,
) -> Result<String> {
    send_inner(name, message, max_turns, bus_socket, None, None, None).await
}

/// Format a plain text message as a stream-json user turn line (with trailing newline).
pub(crate) fn format_user_message(text: &str) -> String {
    let msg = serde_json::json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{"type": "text", "text": text}]
        }
    });
    let mut line = serde_json::to_string(&msg).expect("json serialization cannot fail");
    line.push('\n');
    line
}

async fn send_inner(
    name: &str,
    message: &str,
    max_turns: Option<u32>,
    bus_socket: Option<&str>,
    progress_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    image: Option<(&str, &str)>,
    inject_rx: Option<tokio::sync::mpsc::UnboundedReceiver<String>>,
) -> Result<String> {
    let mut state = load_state(name)?;

    let turns = max_turns.unwrap_or(state.config.max_turns);

    let mut args: Vec<String> = Vec::new();

    let use_resume =
        state.config.session == ConfigSessionMode::Persistent && !state.session_id.is_empty();

    if use_resume {
        args.push("--resume".to_string());
        args.push(state.session_id.clone());
    }

    if !state.config.system_prompt.is_empty() && !use_resume {
        args.push("--system-prompt".to_string());
        args.push(state.config.system_prompt.clone());
    }

    inject_required_flags(&mut args, &state.config.command);

    if !state
        .config
        .command
        .iter()
        .any(|a| a.contains("mcp-config"))
    {
        let deskd_bin = std::env::var("DESKD_BIN").unwrap_or_else(|_| "deskd".to_string());
        let mcp_json = serde_json::json!({
            "mcpServers": {
                "deskd": {
                    "command": deskd_bin,
                    "args": ["mcp", "--agent", name]
                }
            }
        })
        .to_string();
        args.push("--mcp-config".to_string());
        args.push(mcp_json);
    }

    if !state.config.model.is_empty()
        && !state
            .config
            .command
            .iter()
            .any(|a| a == "--model" || a.starts_with("--model="))
    {
        args.push("--model".to_string());
        args.push(state.config.model.clone());
    }

    debug!(agent = %name, turns, model = %state.config.model, multimodal = image.is_some(), "spawning claude");

    let bus_path = bus_socket
        .map(|s| s.to_string())
        .unwrap_or_else(|| config::agent_bus_socket(&state.config.work_dir));

    let config_path_str = state.config.config_path.clone().unwrap_or_default();
    let mut extra_env: Vec<(&str, &str)> =
        vec![("DESKD_AGENT_NAME", name), ("DESKD_BUS_SOCKET", &bus_path)];
    if !config_path_str.is_empty() {
        extra_env.push(("DESKD_AGENT_CONFIG", &config_path_str));
    }

    let mut cmd = build_command(&state.config, &args, &extra_env);
    cmd.stdin(Stdio::piped());
    let mut child = cmd.spawn().context("Failed to spawn claude CLI")?;

    let (stdin_line_tx, mut stdin_line_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    let initial_msg = if let Some((b64_data, media_type)) = image {
        let msg = serde_json::json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64_data,
                        }
                    },
                    {"type": "text", "text": message}
                ]
            }
        });
        let mut line = serde_json::to_string(&msg)?;
        line.push('\n');
        line
    } else {
        format_user_message(message)
    };

    stdin_line_tx
        .send(initial_msg)
        .expect("channel is open at this point");

    if let Some(mut rx) = inject_rx {
        let tx = stdin_line_tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let line = format_user_message(&msg);
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
    }

    let mut child_stdin = child.stdin.take().expect("stdin is piped");
    tokio::spawn(async move {
        while let Some(line) = stdin_line_rx.recv().await {
            if child_stdin.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    drop(stdin_line_tx);

    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");

    let stderr_task = tokio::spawn(async move {
        let mut buf = String::new();
        let mut reader = tokio::io::BufReader::new(stderr);
        let _ = reader.read_to_string(&mut buf).await;
        buf
    });

    let mut lines = tokio::io::BufReader::new(stdout).lines();
    let mut response_text = String::new();
    let mut new_session_id = String::new();
    let mut task_cost = 0.0;
    let mut task_turns = 0u32;

    while let Some(line) = lines.next_line().await? {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
            match v.get("type").and_then(|t| t.as_str()) {
                Some("assistant") => {
                    let mut block_text = String::new();
                    if let Some(blocks) = v
                        .get("message")
                        .and_then(|m| m.get("content"))
                        .and_then(|c| c.as_array())
                    {
                        for block in blocks {
                            if block.get("type").and_then(|t| t.as_str()) == Some("text")
                                && let Some(text) = block.get("text").and_then(|t| t.as_str())
                            {
                                block_text.push_str(text);
                            }
                        }
                    }
                    if !block_text.is_empty() {
                        if let Some(tx) = &progress_tx {
                            let _ = tx.send(block_text.clone());
                        }
                        response_text.push_str(&block_text);
                    }
                }
                Some("result") => {
                    if let Some(sid) = v.get("session_id").and_then(|s| s.as_str()) {
                        new_session_id = sid.to_string();
                    }
                    if let Some(cost) = v.get("total_cost_usd").and_then(|c| c.as_f64()) {
                        task_cost = cost;
                    }
                    if let Some(t) = v.get("num_turns").and_then(|t| t.as_u64()) {
                        task_turns = t as u32;
                    }
                }
                _ => {}
            }
        }
    }

    let _ = child.wait().await;
    let stderr_str = stderr_task.await.unwrap_or_default();

    if state.config.session == ConfigSessionMode::Persistent && !new_session_id.is_empty() {
        if state.session_id != new_session_id {
            state.session_start = Some(Utc::now().to_rfc3339());
            state.session_cost = 0.0;
            state.session_turns = 0;
        }
        state.session_id = new_session_id;
    }
    state.total_cost += task_cost;
    state.total_turns += task_turns;
    state.session_cost += task_cost;
    state.session_turns += task_turns;
    save_state(&state)?;

    if response_text.is_empty() {
        if !stderr_str.is_empty() {
            bail!("Claude error: {}", stderr_str.trim());
        }
        if !state.session_id.is_empty() {
            warn!(agent = %name, session_id = %state.session_id, "stale session_id — retrying without --resume");
            state.session_id = String::new();
            state.session_start = None;
            state.session_cost = 0.0;
            state.session_turns = 0;
            save_state(&state)?;
            return Box::pin(send_inner(
                name,
                message,
                max_turns,
                bus_socket,
                progress_tx,
                image,
                None,
            ))
            .await;
        }
        bail!("No response from claude");
    }

    Ok(response_text)
}

/// List all agents whose state files exist on disk.
pub async fn list() -> Result<Vec<AgentState>> {
    let dir = config::state_dir();
    let mut agents = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "yaml").unwrap_or(false)
                && let Ok(content) = std::fs::read_to_string(&path)
                && let Ok(state) = serde_yaml::from_str::<AgentState>(&content)
            {
                agents.push(state);
            }
        }
    }

    Ok(agents)
}

/// Build a domain `Agent` from an `AgentState`, checking process health.
///
/// `live_agents` is the set of agent names currently registered on a bus.
/// If the agent's PID is gone and it's not in `live_agents`, it's marked Unhealthy.
pub fn to_domain_agent(
    state: &AgentState,
    live_agents: &std::collections::HashSet<String>,
) -> crate::domain::agent::Agent {
    use crate::domain::agent::{Agent, AgentCapabilities, AgentRuntime, AgentStatus, SessionMode};

    let runtime = match state.config.runtime {
        ConfigAgentRuntime::Claude => AgentRuntime::Claude,
        ConfigAgentRuntime::Acp => AgentRuntime::Acp,
        ConfigAgentRuntime::Memory => AgentRuntime::Memory,
    };

    let session_mode = match state.config.session {
        ConfigSessionMode::Persistent => SessionMode::Persistent,
        ConfigSessionMode::Ephemeral => SessionMode::Ephemeral,
    };

    let status = if state.status == "working" {
        AgentStatus::Busy {
            task_id: state.current_task.clone(),
        }
    } else if live_agents.contains(&state.config.name)
        || (state.pid > 0 && std::path::Path::new(&format!("/proc/{}", state.pid)).exists())
    {
        AgentStatus::Ready
    } else if state.pid > 0 {
        AgentStatus::Unhealthy {
            since: chrono::Utc::now().to_rfc3339(),
            reason: format!("process {} not found", state.pid),
        }
    } else {
        AgentStatus::Ready
    };

    Agent {
        name: state.config.name.clone(),
        runtime,
        session_mode,
        capabilities: AgentCapabilities {
            model: state.config.model.clone(),
            labels: Vec::new(),
        },
        status,
    }
}

/// Remove an agent (state file + log).
pub async fn remove(name: &str) -> Result<()> {
    let path = state_path(name);
    if !path.exists() {
        bail!("Agent '{}' not found", name);
    }
    std::fs::remove_file(&path)?;
    let log = log_path(name);
    if log.exists()
        && let Err(e) = std::fs::remove_file(&log)
    {
        warn!(agent = %name, error = %e, "failed to remove log file");
    }
    let stderr_log = stderr_log_path(name);
    if stderr_log.exists()
        && let Err(e) = std::fs::remove_file(&stderr_log)
    {
        warn!(agent = %name, error = %e, "failed to remove stderr log file");
    }
    let stream_log = stream_log_path(name);
    if stream_log.exists()
        && let Err(e) = std::fs::remove_file(&stream_log)
    {
        warn!(agent = %name, error = %e, "failed to remove stream log file");
    }
    info!(agent = %name, "agent removed");
    Ok(())
}

/// Spawn an ephemeral sub-agent: create, run task, clean up.
/// Returns the agent's text response.
pub async fn spawn_ephemeral(
    name: &str,
    task: &str,
    model: &str,
    work_dir: &str,
    max_turns: u32,
    bus_socket: &str,
    parent_name: &str,
) -> Result<String> {
    let unique_name = format!("{}-{}", name, uuid::Uuid::new_v4().as_simple());

    let cfg = AgentConfig {
        name: unique_name.clone(),
        model: model.to_string(),
        system_prompt: String::new(),
        work_dir: work_dir.to_string(),
        max_turns,
        unix_user: None,
        command: default_agent_command(),
        config_path: None,
        container: None,
        session: ConfigSessionMode::default(),
        runtime: ConfigAgentRuntime::default(),
        launch_mode: ConfigLaunchMode::default(),
        kind: ConfigAgentKind::default(),
        context: None,
        compact_threshold: None,
        auto_compact_threshold_tokens: None,
        empty_completion_threshold: None,
        empty_completion_restart_min_secs: None,
    };

    create(&cfg).await?;
    info!(agent = %unique_name, parent = %parent_name, bus = %bus_socket, "spawning ephemeral sub-agent");

    let result = send(&unique_name, task, Some(max_turns), Some(bus_socket)).await;

    if let Err(e) = remove(&unique_name).await {
        warn!(agent = %unique_name, error = %e, "failed to clean up ephemeral agent");
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_config_defaults() {
        let yaml = r#"
config:
  name: test
  model: claude-opus-4-6
  system_prompt: ""
  work_dir: /tmp
  max_turns: 100
pid: 0
session_id: ""
total_turns: 0
total_cost: 0.0
created_at: "2024-01-01T00:00:00Z"
"#;
        let state: AgentState = serde_yaml::from_str(yaml).unwrap();
        assert!(state.config.unix_user.is_none());
    }

    /// Legacy state files / workspace yaml may still carry the removed
    /// `budget_usd` field (see #498). Parsing must succeed and silently
    /// ignore it — serde tolerates unknown fields by default and the
    /// struct must NOT opt into `deny_unknown_fields`.
    #[test]
    fn test_agent_config_ignores_legacy_budget_usd_field() {
        let yaml = r#"
config:
  name: test
  model: claude-opus-4-6
  system_prompt: ""
  work_dir: /tmp
  max_turns: 100
  budget_usd: 50.0
pid: 0
session_id: ""
total_turns: 0
total_cost: 0.0
created_at: "2024-01-01T00:00:00Z"
"#;
        let state: AgentState =
            serde_yaml::from_str(yaml).expect("legacy budget_usd field must be ignored, not error");
        assert_eq!(state.config.name, "test");
    }

    #[test]
    fn test_agent_state_round_trip() {
        let cfg = AgentConfig {
            name: "test-agent".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            system_prompt: String::new(),
            work_dir: "/tmp".to_string(),
            max_turns: 100,
            unix_user: Some("agent1".to_string()),
            command: vec!["claude".to_string()],
            config_path: Some("/home/agent1/deskd.yaml".to_string()),
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let state = AgentState {
            config: cfg,
            pid: 0,
            session_id: "sid-123".to_string(),
            total_turns: 5,
            total_cost: 0.42,
            created_at: Utc::now().to_rfc3339(),
            status: "idle".to_string(),
            current_task: String::new(),
            parent: None,
            scope: None,
            can_message: None,
            env_keys: None,
            session_start: Some(Utc::now().to_rfc3339()),
            session_cost: 0.0,
            session_turns: 0,
            consecutive_empty_completions: 0,
            last_empty_restart_at: None,
            total_empty_restarts: 0,
        };
        let yaml = serde_yaml::to_string(&state).unwrap();
        let restored: AgentState = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(restored.config.unix_user.as_deref(), Some("agent1"));
        assert_eq!(restored.session_id, "sid-123");
        assert_eq!(restored.total_cost, 0.42);
        assert_eq!(
            restored.config.config_path.as_deref(),
            Some("/home/agent1/deskd.yaml")
        );
    }

    #[test]
    fn test_format_user_message() {
        let line = format_user_message("hello world");
        let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(v["type"], "user");
        assert_eq!(v["message"]["role"], "user");
        let content = v["message"]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "hello world");
    }

    #[test]
    fn test_state_save_load_roundtrip() {
        // Serialize env mutation; setenv is not thread-safe on POSIX.
        let _env_guard = crate::test_support::env_lock().blocking_lock();
        let tmp =
            std::env::temp_dir().join(format!("deskd-test-agent-state-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        unsafe { std::env::set_var("HOME", &tmp) };

        let cfg = AgentConfig {
            name: "roundtrip-test".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            system_prompt: "test prompt".to_string(),
            work_dir: "/tmp".to_string(),
            max_turns: 50,
            unix_user: None,
            command: vec!["claude".to_string()],
            config_path: None,
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let state = AgentState {
            config: cfg,
            pid: 0,
            session_id: "sess-xyz".to_string(),
            total_turns: 10,
            total_cost: 1.23,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            status: "idle".to_string(),
            current_task: String::new(),
            parent: Some("parent-agent".to_string()),
            scope: Some("narrow".to_string()),
            can_message: Some(vec!["agent:parent".to_string()]),
            env_keys: Some(vec!["API_KEY".to_string()]),
            session_start: Some("2026-01-01T00:00:00Z".to_string()),
            session_cost: 0.5,
            session_turns: 3,
            consecutive_empty_completions: 0,
            last_empty_restart_at: None,
            total_empty_restarts: 0,
        };

        save_state(&state).unwrap();
        let loaded = load_state("roundtrip-test").unwrap();

        assert_eq!(loaded.config.name, "roundtrip-test");
        assert_eq!(loaded.session_id, "sess-xyz");
        assert_eq!(loaded.total_turns, 10);
        assert_eq!(loaded.total_cost, 1.23);
        assert_eq!(loaded.parent.as_deref(), Some("parent-agent"));

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
