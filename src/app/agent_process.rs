//! Persistent agent process — long-lived Claude process lifecycle.
//!
//! Extracted from `agent.rs` (#280). Manages `AgentProcess`: start, send_task,
//! inject_message, kill, stop, and implements `Executor` trait.

use anyhow::{Context, Result, bail};
use chrono::Utc;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

use crate::domain::config_types::ConfigSessionMode;
use crate::ports::executor::{Executor, ProgressSink, TaskLimits, TokenUsage, TurnResult};

use super::agent_registry::{
    format_user_message, load_state, rotate_stream_log, save_state, save_state_pub,
    stderr_log_path, stream_log_path,
};
use super::process_builder::{build_command, inject_required_flags};

/// Events emitted by the stdout reader task.
enum StdoutEvent {
    /// A text block from an `assistant` message, with optional usage data and
    /// the number of `tool_use` blocks observed in the same message.
    TextBlock(String, Option<TokenUsage>, u32),
    /// The `result` event marking end of a turn.
    Result(TurnResult),
    /// Process exited (stdout closed) with diagnostic context.
    ProcessExited {
        exit_code: Option<i32>,
        stderr_tail: String,
        lifetime_secs: u64,
        used_resume: bool,
    },
}

/// Truncate stderr output for display in error messages (max 200 chars).
fn truncate_stderr(s: &str) -> &str {
    let trimmed = s.trim();
    if trimmed.len() <= 200 {
        trimmed
    } else {
        &trimmed[trimmed.len() - 200..]
    }
}

/// Check whether a freshly spawned process stays alive for at least 5 seconds.
///
/// Returns `Ok(event_rx)` if the process is still running after the window.
/// Returns `Err(...)` with a descriptive message if the process exits immediately.
async fn check_fresh_startup(
    name: &str,
    mut event_rx: tokio::sync::mpsc::UnboundedReceiver<StdoutEvent>,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<StdoutEvent>> {
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match event_rx.try_recv() {
                Ok(StdoutEvent::ProcessExited {
                    exit_code,
                    stderr_tail,
                    ..
                }) => return Some((exit_code, stderr_tail)),
                Ok(_) => {
                    return None;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                Err(_) => {
                    return Some((None, String::new()));
                }
            }
        }
    })
    .await
    .unwrap_or(None);

    if let Some((exit_code, stderr_tail)) = result {
        let code_str = exit_code
            .map(|c| c.to_string())
            .unwrap_or_else(|| "?".to_string());
        error!(
            agent = %name,
            exit_code = %code_str,
            stderr = %truncate_stderr(&stderr_tail),
            "container/process exited immediately after fresh start — check image, mounts, and auth"
        );
        bail!(
            "process for agent '{}' exited immediately (exit_code={}). stderr: {}",
            name,
            code_str,
            truncate_stderr(&stderr_tail)
        );
    }

    Ok(event_rx)
}

/// A long-lived Claude process that accepts multiple tasks via stdin.
///
/// Usage:
///   let process = AgentProcess::start(name, bus_socket).await?;
///   let result = process.send_task(message, progress_tx, None, None).await?;
///   // process stays alive for the next task
///   process.stop().await;
pub struct AgentProcess {
    /// Send lines to Claude's stdin.
    stdin_tx: tokio::sync::mpsc::UnboundedSender<String>,
    /// Receive stdout events (text blocks + result).
    event_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<StdoutEvent>>,
    /// Child process handle for shutdown (shared with stdout reader for exit code capture).
    child: std::sync::Arc<tokio::sync::Mutex<Option<tokio::process::Child>>>,
    /// Agent name.
    name: String,
    /// Last cumulative cost reported by Claude (for computing deltas).
    /// Claude's `total_cost_usd` is session-cumulative, not per-task.
    last_reported_cost: tokio::sync::Mutex<f64>,
    /// Last cumulative turns reported by Claude.
    last_reported_turns: tokio::sync::Mutex<u32>,
}

impl AgentProcess {
    /// Spawn a persistent Claude process for the named agent.
    ///
    /// If a stale session_id causes an immediate exit (e.g. container restart
    /// cleared the session), automatically retries with a fresh session and
    /// clears the stale session_id from state. See #149.
    pub async fn start(name: &str, bus_socket: &str) -> Result<Self> {
        let state = load_state(name)?;
        let will_resume =
            state.config.session == ConfigSessionMode::Persistent && !state.session_id.is_empty();

        let (stdin_tx, event_rx, child) = Self::spawn_process(name, bus_socket, false).await?;

        if will_resume {
            let mut event_rx_guard = event_rx;
            let stale_exit = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    match event_rx_guard.try_recv() {
                        Ok(StdoutEvent::ProcessExited {
                            exit_code,
                            stderr_tail,
                            ..
                        }) => return Some((exit_code, stderr_tail)),
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        }
                        _ => {
                            return None;
                        }
                    }
                }
            })
            .await
            .unwrap_or(None);

            if let Some((exit_code, stderr_tail)) = stale_exit {
                let code_str = exit_code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "?".to_string());
                warn!(
                    agent = %name,
                    session_id = %state.session_id,
                    exit_code = %code_str,
                    stderr = %truncate_stderr(&stderr_tail),
                    "process exited immediately with --resume (stale session), retrying fresh"
                );

                if let Ok(mut s) = load_state(name) {
                    s.session_id.clear();
                    s.session_start = None;
                    s.session_cost = 0.0;
                    s.session_turns = 0;
                    save_state_pub(&s).ok();
                }

                let (stdin_tx2, event_rx2, child2) =
                    Self::spawn_process(name, bus_socket, true).await?;
                let event_rx2 = check_fresh_startup(name, event_rx2).await.map_err(|e| {
                    error!(
                        agent = %name,
                        error = %e,
                        "stale-session retry also failed — container cannot start"
                    );
                    e
                })?;
                return Ok(Self {
                    stdin_tx: stdin_tx2,
                    event_rx: tokio::sync::Mutex::new(event_rx2),
                    child: child2,
                    name: name.to_string(),
                    last_reported_cost: tokio::sync::Mutex::new(0.0),
                    last_reported_turns: tokio::sync::Mutex::new(0),
                });
            }

            return Ok(Self {
                stdin_tx,
                event_rx: tokio::sync::Mutex::new(event_rx_guard),
                child,
                name: name.to_string(),
                last_reported_cost: tokio::sync::Mutex::new(0.0),
                last_reported_turns: tokio::sync::Mutex::new(0),
            });
        }

        let event_rx = check_fresh_startup(name, event_rx).await?;

        Ok(Self {
            stdin_tx,
            event_rx: tokio::sync::Mutex::new(event_rx),
            child,
            name: name.to_string(),
            last_reported_cost: tokio::sync::Mutex::new(0.0),
            last_reported_turns: tokio::sync::Mutex::new(0),
        })
    }

    /// Spawn a persistent Claude process with a fresh session (no --resume).
    pub async fn start_fresh(name: &str, bus_socket: &str) -> Result<Self> {
        let (stdin_tx, event_rx, child) = Self::spawn_process(name, bus_socket, true).await?;

        let event_rx = check_fresh_startup(name, event_rx).await?;

        Ok(Self {
            stdin_tx,
            event_rx: tokio::sync::Mutex::new(event_rx),
            child,
            name: name.to_string(),
            last_reported_cost: tokio::sync::Mutex::new(0.0),
            last_reported_turns: tokio::sync::Mutex::new(0),
        })
    }

    /// Core spawn logic — builds args, starts child, wires stdin/stdout tasks.
    /// When `fresh` is true, skip --resume regardless of session mode/state.
    async fn spawn_process(
        name: &str,
        bus_socket: &str,
        fresh: bool,
    ) -> Result<(
        tokio::sync::mpsc::UnboundedSender<String>,
        tokio::sync::mpsc::UnboundedReceiver<StdoutEvent>,
        std::sync::Arc<tokio::sync::Mutex<Option<tokio::process::Child>>>,
    )> {
        let state = load_state(name)?;

        let mut args: Vec<String> = Vec::new();

        let use_resume = !fresh
            && state.config.session == ConfigSessionMode::Persistent
            && !state.session_id.is_empty();

        if use_resume {
            args.push("--resume".to_string());
            args.push(state.session_id.clone());
        }

        if !state.config.system_prompt.is_empty() && !use_resume {
            args.push("--system-prompt".to_string());
            args.push(state.config.system_prompt.clone());
        }

        // Context materialization: load and execute context nodes, inject as system prompt.
        if let Some(ref ctx_cfg) = state.config.context
            && ctx_cfg.enabled
        {
            let ctx_domain: crate::domain::context::ContextConfig = ctx_cfg.clone().into();
            let main_path = ctx_cfg
                .main_path
                .as_deref()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| crate::app::context::default_main_path(&state.config.work_dir));
            if main_path.exists() {
                let ctx_repo = crate::app::context::new_context_store();
                match crate::app::context::MainBranch::load(&ctx_repo, &main_path) {
                    Ok(mut branch) => match branch.materialize().await {
                        Ok(messages) if !messages.is_empty() => {
                            let combined: String = messages
                                .iter()
                                .map(|m| m.content.as_str())
                                .collect::<Vec<_>>()
                                .join("\n\n");
                            args.push("--append-system-prompt".to_string());
                            args.push(combined);
                            info!(
                                agent = %name,
                                nodes = messages.len(),
                                budget = ctx_domain.main_budget_tokens.unwrap_or(10000),
                                "injected materialized context"
                            );
                            if let Err(e) = branch.save(&ctx_repo, &main_path) {
                                warn!(agent = %name, error = %e, "failed to save context cache");
                            }
                        }
                        Ok(_) => {
                            debug!(agent = %name, "context materialized but no messages produced");
                        }
                        Err(e) => {
                            warn!(agent = %name, error = %e, "context materialization failed");
                        }
                    },
                    Err(e) => {
                        warn!(agent = %name, path = %main_path.display(), error = %e, "failed to load context branch");
                    }
                }
            } else {
                debug!(agent = %name, path = %main_path.display(), "context main path does not exist, skipping");
            }
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

        let bus_path = bus_socket.to_string();
        let config_path_str = state.config.config_path.clone().unwrap_or_default();
        let mut extra_env: Vec<(&str, &str)> =
            vec![("DESKD_AGENT_NAME", name), ("DESKD_BUS_SOCKET", &bus_path)];
        if !config_path_str.is_empty() {
            extra_env.push(("DESKD_AGENT_CONFIG", &config_path_str));
        }

        let mut cmd = build_command(&state.config, &args, &extra_env);
        cmd.stdin(Stdio::piped());

        {
            let std_cmd = cmd.as_std();
            let program = std_cmd.get_program().to_string_lossy();
            let cmd_args: Vec<String> = std_cmd
                .get_args()
                .map(|a| a.to_string_lossy().into_owned())
                .collect();
            debug!(
                agent = %name,
                command = %program,
                args = ?cmd_args,
                "spawning persistent process"
            );
        }

        let mut child = cmd
            .spawn()
            .context("Failed to spawn persistent claude process")?;

        info!(agent = %name, model = %state.config.model, "persistent process started");

        let spawn_instant = std::time::Instant::now();

        let child_stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");

        const STDERR_CAP: usize = 2048;
        let stderr_buf = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let stderr_buf_writer = stderr_buf.clone();

        let agent_name = name.to_string();
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt;

            let mut reader = tokio::io::BufReader::new(stderr);
            let log_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(stderr_log_path(&agent_name));
            let mut file_writer = log_file.ok().map(std::io::BufWriter::new);

            let mut line = String::new();
            while let Ok(n) = reader.read_line(&mut line).await {
                if n == 0 {
                    break;
                }
                if let Some(ref mut f) = file_writer {
                    use std::io::Write;
                    let ts = chrono::Utc::now().format("%H:%M:%S");
                    let _ = write!(f, "[{}] {}", ts, line);
                    let _ = f.flush();
                }
                if let Ok(mut shared) = stderr_buf_writer.lock() {
                    shared.push_str(&line);
                    if shared.len() > STDERR_CAP {
                        let excess = shared.len() - STDERR_CAP;
                        shared.drain(..excess);
                    }
                }
                line.clear();
            }
        });

        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let mut writer = child_stdin;
        tokio::spawn(async move {
            while let Some(line) = stdin_rx.recv().await {
                if writer.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
            }
        });

        let child_arc = std::sync::Arc::new(tokio::sync::Mutex::new(Some(child)));
        let child_for_reader = child_arc.clone();

        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<StdoutEvent>();
        let agent_name2 = name.to_string();
        let stderr_buf_reader = stderr_buf;
        let task_use_resume = use_resume;
        tokio::spawn(async move {
            let stream_log = stream_log_path(&agent_name2);
            if let Some(parent) = stream_log.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            let stream_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&stream_log);
            let mut stream_writer = stream_file.ok().map(std::io::BufWriter::new);

            let mut lines = tokio::io::BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(ref mut f) = stream_writer {
                    use std::io::Write;
                    let _ = writeln!(f, "{}", line);
                    let _ = f.flush();
                }

                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
                    match v.get("type").and_then(|t| t.as_str()) {
                        Some("assistant") => {
                            let mut block_text = String::new();
                            let mut tool_uses: u32 = 0;
                            if let Some(blocks) = v
                                .get("message")
                                .and_then(|m| m.get("content"))
                                .and_then(|c| c.as_array())
                            {
                                for block in blocks {
                                    match block.get("type").and_then(|t| t.as_str()) {
                                        Some("text") => {
                                            if let Some(text) =
                                                block.get("text").and_then(|t| t.as_str())
                                            {
                                                block_text.push_str(text);
                                            }
                                        }
                                        Some("tool_use") => {
                                            tool_uses = tool_uses.saturating_add(1);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            let usage = v
                                .get("message")
                                .and_then(|m| m.get("usage"))
                                .map(TokenUsage::from);
                            if (!block_text.is_empty() || usage.is_some() || tool_uses > 0)
                                && event_tx
                                    .send(StdoutEvent::TextBlock(block_text, usage, tool_uses))
                                    .is_err()
                            {
                                break;
                            }
                        }
                        Some("result") => {
                            let session_id = v
                                .get("session_id")
                                .and_then(|s| s.as_str())
                                .unwrap_or_default()
                                .to_string();
                            let cost_usd = v
                                .get("total_cost_usd")
                                .and_then(|c| c.as_f64())
                                .unwrap_or(0.0);
                            let num_turns =
                                v.get("num_turns").and_then(|t| t.as_u64()).unwrap_or(0) as u32;
                            if event_tx
                                .send(StdoutEvent::Result(TurnResult {
                                    response_text: String::new(),
                                    session_id,
                                    cost_usd,
                                    num_turns,
                                    token_usage: TokenUsage::default(),
                                    tool_use_count: 0,
                                }))
                                .is_err()
                            {
                                break;
                            }
                            rotate_stream_log(&stream_log, 10 * 1024 * 1024);
                        }
                        _ => {}
                    }
                }
            }
            let lifetime_secs = spawn_instant.elapsed().as_secs();
            let exit_code = if let Some(mut ch) = child_for_reader.lock().await.take() {
                ch.wait().await.ok().and_then(|s| s.code())
            } else {
                None
            };
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let stderr_tail = stderr_buf_reader
                .lock()
                .map(|s| s.clone())
                .unwrap_or_default();

            let _ = event_tx.send(StdoutEvent::ProcessExited {
                exit_code,
                stderr_tail,
                lifetime_secs,
                used_resume: task_use_resume,
            });
            debug!(agent = %agent_name2, "persistent process stdout closed");
        });

        Ok((stdin_tx, event_rx, child_arc))
    }

    /// Send a task to the persistent process and collect the response.
    ///
    /// Enforces `limits` in real-time: if max_turns is exceeded mid-task,
    /// the process is killed immediately.
    pub async fn send_task(
        &self,
        message: &str,
        progress: Option<&ProgressSink>,
        image: Option<(&str, &str)>,
        limits: &TaskLimits,
    ) -> Result<TurnResult> {
        // Drain stale events from the previous turn.
        {
            let mut event_rx = self.event_rx.lock().await;
            let mut drained = 0u32;
            loop {
                match event_rx.try_recv() {
                    Ok(StdoutEvent::ProcessExited {
                        exit_code,
                        stderr_tail,
                        lifetime_secs,
                        used_resume,
                    }) => {
                        bail!(
                            "persistent process exited between turns \
                             (exit_code={}, lifetime={}s, resume={}, stderr={:?})",
                            exit_code
                                .map(|c| c.to_string())
                                .unwrap_or_else(|| "signal".into()),
                            lifetime_secs,
                            used_resume,
                            truncate_stderr(&stderr_tail),
                        );
                    }
                    Ok(_) => {
                        drained += 1;
                    }
                    Err(_) => break,
                }
            }
            if drained > 0 {
                info!(
                    agent = %self.name,
                    drained_events = drained,
                    "flushed stale stdout events before new turn"
                );
            }
            drop(event_rx);
            tokio::task::yield_now().await;
            let mut event_rx = self.event_rx.lock().await;
            loop {
                match event_rx.try_recv() {
                    Ok(StdoutEvent::ProcessExited {
                        exit_code,
                        stderr_tail,
                        lifetime_secs,
                        used_resume,
                    }) => {
                        bail!(
                            "persistent process exited between turns \
                             (exit_code={}, lifetime={}s, resume={}, stderr={:?})",
                            exit_code
                                .map(|c| c.to_string())
                                .unwrap_or_else(|| "signal".into()),
                            lifetime_secs,
                            used_resume,
                            truncate_stderr(&stderr_tail),
                        );
                    }
                    Ok(_) => {
                        drained += 1;
                    }
                    Err(_) => break,
                }
            }
            if drained > 0 {
                info!(
                    agent = %self.name,
                    total_drained = drained,
                    "pre-turn drain complete"
                );
            }
        }

        // Build the user message.
        let user_msg = if let Some((b64_data, media_type)) = image {
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

        // Send to stdin.
        self.stdin_tx
            .send(user_msg)
            .map_err(|_| anyhow::anyhow!("persistent process stdin closed"))?;

        // Read events until we get a Result, enforcing limits on each event.
        let mut event_rx = self.event_rx.lock().await;
        let mut response_text = String::new();
        let mut assistant_turns = 0u32;
        let mut accumulated_usage = TokenUsage::default();
        let mut accumulated_tool_uses: u32 = 0;

        loop {
            match event_rx.recv().await {
                Some(StdoutEvent::TextBlock(text, usage, tool_uses)) => {
                    if let Some(u) = &usage {
                        accumulated_usage.merge(u);
                    }
                    accumulated_tool_uses = accumulated_tool_uses.saturating_add(tool_uses);

                    if !text.is_empty() {
                        assistant_turns += 1;

                        if let Some(max) = limits.max_turns
                            && assistant_turns > max
                        {
                            warn!(
                                agent = %self.name,
                                turns = assistant_turns,
                                max = max,
                                "turn limit exceeded mid-task, killing process"
                            );
                            self.kill().await;
                            bail!(
                                "task killed: exceeded {} turn limit ({} turns)",
                                max,
                                assistant_turns
                            );
                        }

                        if let Some(sink) = &progress {
                            sink(text.clone());
                        }
                        response_text.push_str(&text);
                    }
                }
                Some(StdoutEvent::Result(mut result)) => {
                    while let Ok(StdoutEvent::TextBlock(trailing, _, trailing_tools)) =
                        event_rx.try_recv()
                    {
                        debug!(
                            agent = %self.name,
                            len = trailing.len(),
                            "drained trailing text block after result event"
                        );
                        if let Some(sink) = &progress {
                            sink(trailing.clone());
                        }
                        response_text.push_str(&trailing);
                        accumulated_tool_uses =
                            accumulated_tool_uses.saturating_add(trailing_tools);
                    }

                    result.response_text = response_text;
                    result.token_usage = accumulated_usage;
                    result.tool_use_count = accumulated_tool_uses;

                    let mut last_cost = self.last_reported_cost.lock().await;
                    let mut last_turns = self.last_reported_turns.lock().await;
                    let cost_delta = (result.cost_usd - *last_cost).max(0.0);
                    let turns_delta = result.num_turns.saturating_sub(*last_turns);
                    *last_cost = result.cost_usd;
                    *last_turns = result.num_turns;
                    drop(last_cost);
                    drop(last_turns);

                    if let Ok(mut state) = load_state(&self.name) {
                        if state.config.session == ConfigSessionMode::Persistent
                            && !result.session_id.is_empty()
                        {
                            if state.session_id != result.session_id {
                                state.session_start = Some(Utc::now().to_rfc3339());
                                state.session_cost = 0.0;
                                state.session_turns = 0;
                            }
                            state.session_id = result.session_id.clone();
                        }
                        state.total_cost += cost_delta;
                        state.total_turns += turns_delta;
                        state.session_cost += cost_delta;
                        state.session_turns += turns_delta;
                        let _ = save_state(&state);
                    }

                    return Ok(result);
                }
                Some(StdoutEvent::ProcessExited {
                    exit_code,
                    stderr_tail,
                    lifetime_secs,
                    used_resume,
                }) => {
                    bail!(
                        "persistent process exited mid-task \
                         (exit_code={}, lifetime={}s, resume={}, stderr={:?})",
                        exit_code
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "signal".into()),
                        lifetime_secs,
                        used_resume,
                        truncate_stderr(&stderr_tail),
                    );
                }
                None => {
                    bail!("persistent process exited mid-task (channel closed)");
                }
            }
        }
    }

    /// Inject a message into the running process as a new user turn.
    pub fn inject_message(&self, message: &str) -> Result<()> {
        let line = format_user_message(message);
        self.stdin_tx
            .send(line)
            .map_err(|_| anyhow::anyhow!("persistent process stdin closed"))
    }

    /// Kill the running process immediately (e.g. budget exceeded mid-task).
    pub async fn kill(&self) {
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        warn!(agent = %self.name, "persistent process killed");
    }

    /// Gracefully stop the persistent process.
    pub async fn stop(&self) {
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        info!(agent = %self.name, "persistent process stopped");
    }
}

impl Executor for AgentProcess {
    fn send_task<'a>(
        &'a self,
        message: &'a str,
        progress: Option<&'a ProgressSink>,
        image: Option<(&'a str, &'a str)>,
        limits: &'a TaskLimits,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TurnResult>> + Send + 'a>> {
        Box::pin(self.send_task(message, progress, image, limits))
    }

    fn inject_message(&self, message: &str) -> Result<()> {
        self.inject_message(message)
    }

    fn stop(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(self.stop())
    }
}
