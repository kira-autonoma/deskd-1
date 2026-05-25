//! ACP (Agent Client Protocol) client for deskd worker.
//!
//! Implements JSON-RPC 2.0 framing over stdin/stdout to communicate with
//! ACP-compatible agents (Gemini CLI, Goose, etc.).

use anyhow::{Context, Result, bail};
use chrono::Utc;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

use crate::app::agent::{
    self, AgentConfig, Executor, ProgressSink, TaskLimits, TokenUsage, TurnResult, build_command,
};
use crate::app::jsonrpc::{
    JsonRpcRequest, JsonRpcResponse, MessageKind, classify_message, parse_response,
};
use crate::domain::config_types::ConfigSessionMode;

// ─── ACP message helpers ─────────────────────────────────────────────────────

/// Build an `initialize` request with minimal capabilities.
pub fn build_initialize(id: u64) -> JsonRpcRequest {
    JsonRpcRequest::new(
        id,
        "initialize",
        Some(serde_json::json!({
            "capabilities": {
                "fs": {
                    "readTextFile": false,
                    "writeTextFile": false
                },
                "terminal": false
            },
            "clientInfo": {
                "name": "deskd",
                "version": env!("CARGO_PKG_VERSION")
            }
        })),
    )
}

/// Build a `session/new` request.
pub fn build_session_new(
    id: u64,
    cwd: &str,
    mcp_servers: Option<HashMap<String, serde_json::Value>>,
) -> JsonRpcRequest {
    let mut params = serde_json::json!({
        "cwd": cwd,
    });
    if let Some(servers) = mcp_servers {
        params["mcpServers"] = serde_json::to_value(servers).unwrap_or_default();
    }
    JsonRpcRequest::new(id, "session/new", Some(params))
}

/// Build a `session/load` request to resume an existing session.
pub fn build_session_load(id: u64, session_id: &str) -> JsonRpcRequest {
    JsonRpcRequest::new(
        id,
        "session/load",
        Some(serde_json::json!({
            "sessionId": session_id,
        })),
    )
}

/// Build a `session/prompt` request.
pub fn build_session_prompt(id: u64, session_id: &str, text: &str) -> JsonRpcRequest {
    JsonRpcRequest::new(
        id,
        "session/prompt",
        Some(serde_json::json!({
            "sessionId": session_id,
            "text": text,
        })),
    )
}

/// Build a `session/cancel` request.
pub fn build_session_cancel(id: u64, session_id: &str) -> JsonRpcRequest {
    JsonRpcRequest::new(
        id,
        "session/cancel",
        Some(serde_json::json!({
            "sessionId": session_id,
        })),
    )
}

/// Build a permission approval response for `session/request_permission`.
pub fn build_permission_approval(request_id: u64) -> String {
    let resp = serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "approved": true
        }
    });
    let mut line = serde_json::to_string(&resp).expect("json serialization cannot fail");
    line.push('\n');
    line
}

/// Extract text content from a `session/update` notification.
/// Returns accumulated text from assistant messages in the update.
pub fn extract_update_text(params: &serde_json::Value) -> Option<String> {
    // ACP session/update notifications contain messages array.
    // Look for assistant messages with text content.
    let messages = params.get("messages").and_then(|m| m.as_array())?;
    let mut text = String::new();

    for msg in messages {
        let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or_default();
        if role != "assistant" {
            continue;
        }

        // Content can be a string or array of content blocks.
        if let Some(content_str) = msg.get("content").and_then(|c| c.as_str()) {
            text.push_str(content_str);
        } else if let Some(content_arr) = msg.get("content").and_then(|c| c.as_array()) {
            for block in content_arr {
                if block.get("type").and_then(|t| t.as_str()) == Some("text")
                    && let Some(t) = block.get("text").and_then(|t| t.as_str())
                {
                    text.push_str(t);
                }
            }
        }
    }

    if text.is_empty() { None } else { Some(text) }
}

/// Check if a session/update notification indicates the session is complete.
pub fn is_session_complete(params: &serde_json::Value) -> bool {
    params
        .get("status")
        .and_then(|s| s.as_str())
        .is_some_and(|s| s == "completed" || s == "done" || s == "ended")
}

/// Extract session ID from an ACP response (session/new or session/load result).
pub fn extract_session_id(result: &serde_json::Value) -> Option<String> {
    result
        .get("sessionId")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

/// Resolve MCP servers for an ACP session.
///
/// Reads the agent's `deskd.yaml` (if `config_path` is set) to extract
/// `mcp_config` → `mcpServers`. Always injects a default `deskd` entry
/// that points at `deskd mcp --agent <name>` so ACP agents get the same
/// bus access Claude agents do.
///
/// Returns `None` only if no servers at all could be resolved (agent name
/// is empty and no config). In practice a non-empty agent name means we
/// always return at least the default deskd entry.
pub fn resolve_mcp_servers(
    agent_name: &str,
    config_path: Option<&str>,
) -> Option<HashMap<String, serde_json::Value>> {
    let mut servers: HashMap<String, serde_json::Value> = HashMap::new();

    // Pull user-defined mcpServers from the agent's deskd.yaml, if any.
    if let Some(path) = config_path
        && !path.is_empty()
        && let Ok(content) = std::fs::read_to_string(path)
    {
        // Parse as a loose YAML Value so we don't couple to UserConfig here.
        if let Ok(yaml) = serde_yaml::from_str::<serde_json::Value>(&content)
            && let Some(mcp_str) = yaml.get("mcp_config").and_then(|v| v.as_str())
            && let Ok(mcp_json) = serde_json::from_str::<serde_json::Value>(mcp_str)
            && let Some(map) = mcp_json.get("mcpServers").and_then(|v| v.as_object())
        {
            for (k, v) in map {
                servers.insert(k.clone(), v.clone());
            }
        }
    }

    // Always inject a default deskd MCP server so ACP agents get bus tools.
    if !agent_name.is_empty() {
        servers.entry("deskd".to_string()).or_insert_with(|| {
            let deskd_bin = std::env::var("DESKD_BIN").unwrap_or_else(|_| "deskd".to_string());
            serde_json::json!({
                "command": deskd_bin,
                "args": ["mcp", "--agent", agent_name]
            })
        });
    }

    if servers.is_empty() {
        None
    } else {
        Some(servers)
    }
}

// ─── AcpProcess: long-lived ACP agent process ───────────────────────────────

/// Events emitted by the ACP stdout reader task.
enum AcpEvent {
    /// Text content from a session/update notification.
    TextBlock(String),
    /// A JSON-RPC response to a request we sent.
    Response(JsonRpcResponse),
    /// A permission request from the agent.
    PermissionRequest(u64),
    /// Session completed.
    SessionComplete,
    /// Process exited (stdout closed) with diagnostic context for debugging.
    /// Mirrors the shape used by the Claude stream-json executor
    /// (`StdoutEvent::ProcessExited`) so operators get the same signal
    /// regardless of runtime.
    ProcessExited {
        exit_code: Option<i32>,
        stderr_tail: String,
        lifetime_secs: u64,
    },
}

/// Truncate stderr output for display in error messages (max 200 chars, tail).
fn truncate_stderr(s: &str) -> &str {
    let trimmed = s.trim();
    if trimmed.len() <= 200 {
        trimmed
    } else {
        &trimmed[trimmed.len() - 200..]
    }
}

/// A long-lived ACP agent process that communicates via JSON-RPC 2.0.
pub struct AcpProcess {
    /// Send lines to the agent's stdin.
    stdin_tx: tokio::sync::mpsc::UnboundedSender<String>,
    /// Receive parsed ACP events from stdout.
    event_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<AcpEvent>>,
    /// Child process handle for shutdown. Shared with the stdout reader so it
    /// can reap the exit code when stdout closes.
    child: std::sync::Arc<tokio::sync::Mutex<Option<tokio::process::Child>>>,
    /// Agent name.
    name: String,
    /// Current ACP session ID.
    session_id: tokio::sync::Mutex<Option<String>>,
    /// Next JSON-RPC request ID.
    next_id: tokio::sync::Mutex<u64>,
}

impl AcpProcess {
    /// Spawn an ACP agent process and perform the initialize handshake.
    pub async fn start(name: &str, bus_socket: &str) -> Result<Self> {
        Self::spawn_and_init(name, bus_socket, false).await
    }

    /// Spawn an ACP agent process with a fresh session (ignore stored session_id).
    pub async fn start_fresh(name: &str, bus_socket: &str) -> Result<Self> {
        Self::spawn_and_init(name, bus_socket, true).await
    }

    async fn spawn_and_init(name: &str, bus_socket: &str, fresh: bool) -> Result<Self> {
        let state = agent::load_state(name)?;

        // ACP processes don't use Claude-specific args; just spawn the command as-is.
        let bus_path = bus_socket.to_string();
        let config_path_str = state.config.config_path.clone().unwrap_or_default();
        let mut extra_env: Vec<(&str, &str)> =
            vec![("DESKD_AGENT_NAME", name), ("DESKD_BUS_SOCKET", &bus_path)];
        if !config_path_str.is_empty() {
            extra_env.push(("DESKD_AGENT_CONFIG", &config_path_str));
        }

        // No extra args for ACP — the command in config should be complete.
        let args: Vec<String> = Vec::new();
        let mut cmd = build_command(&state.config, &args, &extra_env);
        cmd.stdin(std::process::Stdio::piped());
        let mut child = cmd.spawn().context("Failed to spawn ACP agent process")?;

        let spawn_instant = std::time::Instant::now();

        let child_stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");

        // Drain stderr in background, keeping a bounded tail for diagnostics.
        const STDERR_CAP: usize = 2048;
        let stderr_buf = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let stderr_buf_writer = stderr_buf.clone();
        let agent_name = name.to_string();
        tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(stderr);
            let mut line = String::new();
            while let Ok(n) = reader.read_line(&mut line).await {
                if n == 0 {
                    break;
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
            if let Ok(shared) = stderr_buf_writer.lock()
                && !shared.is_empty()
            {
                warn!(agent = %agent_name, stderr = %shared.trim(), "ACP process stderr drained");
            }
        });

        // Stdin writer task.
        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let mut writer = child_stdin;
        tokio::spawn(async move {
            while let Some(line) = stdin_rx.recv().await {
                if writer.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
            }
        });

        // Wrap child in Arc so the stdout reader can reap it on exit.
        let child_arc = std::sync::Arc::new(tokio::sync::Mutex::new(Some(child)));
        let child_for_reader = child_arc.clone();
        let stderr_buf_reader = stderr_buf;

        // Stdout reader task — parses JSON-RPC messages.
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<AcpEvent>();
        let agent_name2 = name.to_string();
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let resp = match parse_response(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        debug!(agent = %agent_name2, error = %e, line = %line, "unparseable ACP output");
                        continue;
                    }
                };

                match classify_message(&resp) {
                    MessageKind::ServerRequest => {
                        // Server request: has both id and method.
                        // e.g. session/request_permission — the server asks us
                        // to approve something; we must reply with the same id.
                        let method = resp.method.as_deref().unwrap_or_default();
                        match method {
                            "session/request_permission" => {
                                if let Some(req_id) = resp.id
                                    && event_tx.send(AcpEvent::PermissionRequest(req_id)).is_err()
                                {
                                    break;
                                }
                            }
                            _ => {
                                debug!(agent = %agent_name2, method = %method, "unknown ACP server request");
                            }
                        }
                    }
                    MessageKind::Notification => {
                        // Notification: has method but no id.
                        let method = resp.method.as_deref().unwrap_or_default();
                        let params = resp.params.as_ref().cloned().unwrap_or_default();

                        match method {
                            "session/update" => {
                                if let Some(text) = extract_update_text(&params)
                                    && event_tx.send(AcpEvent::TextBlock(text)).is_err()
                                {
                                    break;
                                }
                                if is_session_complete(&params)
                                    && event_tx.send(AcpEvent::SessionComplete).is_err()
                                {
                                    break;
                                }
                            }
                            _ => {
                                debug!(agent = %agent_name2, method = %method, "unknown ACP notification");
                            }
                        }
                    }
                    MessageKind::Response => {
                        // Response to a request we sent: has id but no method.
                        if event_tx.send(AcpEvent::Response(resp)).is_err() {
                            break;
                        }
                    }
                }
            }
            // Reap exit code and snapshot stderr tail so the error surfaced
            // to the caller includes process lifetime and last stderr output.
            let lifetime_secs = spawn_instant.elapsed().as_secs();
            let exit_code = if let Some(mut ch) = child_for_reader.lock().await.take() {
                ch.wait().await.ok().and_then(|s| s.code())
            } else {
                None
            };
            // Small delay so any trailing stderr lines are captured.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let stderr_tail = stderr_buf_reader
                .lock()
                .map(|s| s.clone())
                .unwrap_or_default();

            let _ = event_tx.send(AcpEvent::ProcessExited {
                exit_code,
                stderr_tail,
                lifetime_secs,
            });
            debug!(agent = %agent_name2, "ACP process stdout closed");
        });

        let process = Self {
            stdin_tx,
            event_rx: tokio::sync::Mutex::new(event_rx),
            child: child_arc,
            name: name.to_string(),
            session_id: tokio::sync::Mutex::new(None),
            next_id: tokio::sync::Mutex::new(1),
        };

        // Perform initialize handshake.
        process.do_initialize().await?;

        // Create or load session.
        let session_id = if !fresh
            && state.config.session == ConfigSessionMode::Persistent
            && !state.session_id.is_empty()
        {
            process.do_session_load(&state.session_id).await?
        } else {
            process.do_session_new(&state.config).await?
        };
        *process.session_id.lock().await = Some(session_id);

        info!(agent = %name, "ACP process initialized");
        Ok(process)
    }

    /// Get the next request ID.
    async fn next_request_id(&self) -> u64 {
        let mut id = self.next_id.lock().await;
        let current = *id;
        *id += 1;
        current
    }

    /// Send a JSON-RPC request and wait for the response with matching ID.
    /// Processes notifications (permission requests, updates) while waiting.
    async fn send_request(
        &self,
        req: &JsonRpcRequest,
        progress: Option<&ProgressSink>,
    ) -> Result<JsonRpcResponse> {
        let line = req.to_line()?;
        self.stdin_tx
            .send(line)
            .map_err(|_| anyhow::anyhow!("ACP process stdin closed"))?;

        let expected_id = req.id;
        let mut event_rx = self.event_rx.lock().await;

        loop {
            match event_rx.recv().await {
                Some(AcpEvent::Response(resp)) => {
                    if resp.id == Some(expected_id) {
                        return Ok(resp);
                    }
                    // Response for a different request — skip.
                    debug!(
                        agent = %self.name,
                        resp_id = ?resp.id,
                        expected = expected_id,
                        "unexpected response ID"
                    );
                }
                Some(AcpEvent::PermissionRequest(req_id)) => {
                    let approval = build_permission_approval(req_id);
                    let _ = self.stdin_tx.send(approval);
                }
                Some(AcpEvent::TextBlock(text)) => {
                    if let Some(sink) = progress {
                        sink(text);
                    }
                }
                Some(AcpEvent::SessionComplete) => {
                    // Session completed while waiting for response — keep waiting.
                }
                Some(AcpEvent::ProcessExited {
                    exit_code,
                    stderr_tail,
                    lifetime_secs,
                }) => {
                    bail!(
                        "ACP process exited while waiting for response \
                         (exit_code={}, lifetime={}s, stderr={:?})",
                        exit_code
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "signal".into()),
                        lifetime_secs,
                        truncate_stderr(&stderr_tail),
                    );
                }
                None => {
                    bail!("ACP process exited while waiting for response (event channel closed)");
                }
            }
        }
    }

    /// Perform the initialize handshake.
    async fn do_initialize(&self) -> Result<()> {
        let id = self.next_request_id().await;
        let req = build_initialize(id);
        let resp = self.send_request(&req, None).await?;

        if let Some(err) = resp.error {
            bail!("ACP initialize failed: {}", err);
        }

        debug!(agent = %self.name, "ACP initialize succeeded");
        Ok(())
    }

    /// Create a new session and return its ID.
    async fn do_session_new(&self, cfg: &AgentConfig) -> Result<String> {
        let id = self.next_request_id().await;
        let mcp_servers = resolve_mcp_servers(&self.name, cfg.config_path.as_deref());
        let req = build_session_new(id, &cfg.work_dir, mcp_servers);
        let resp = self.send_request(&req, None).await?;

        if let Some(err) = resp.error {
            bail!("ACP session/new failed: {}", err);
        }

        let result = resp.result.unwrap_or_default();
        extract_session_id(&result)
            .ok_or_else(|| anyhow::anyhow!("session/new response missing sessionId"))
    }

    /// Load an existing session and return its ID.
    async fn do_session_load(&self, session_id: &str) -> Result<String> {
        let id = self.next_request_id().await;
        let req = build_session_load(id, session_id);
        let resp = self.send_request(&req, None).await?;

        if let Some(err) = resp.error {
            // If session load fails, fall back to creating a new session.
            warn!(
                agent = %self.name,
                session_id = %session_id,
                error = %err,
                "session/load failed, creating new session"
            );
            let state = agent::load_state(&self.name)?;
            return self.do_session_new(&state.config).await;
        }

        let result = resp.result.unwrap_or_default();
        extract_session_id(&result).ok_or_else(|| {
            anyhow::anyhow!("session/load response missing sessionId, using provided")
        })
    }

    /// Send a task to the ACP agent and collect the streamed response.
    pub async fn send_task(
        &self,
        message: &str,
        progress: Option<&ProgressSink>,
        limits: &TaskLimits,
    ) -> Result<TurnResult> {
        let session_id = self
            .session_id
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no ACP session established"))?;

        let id = self.next_request_id().await;
        let req = build_session_prompt(id, &session_id, message);
        let line = req.to_line()?;
        self.stdin_tx
            .send(line)
            .map_err(|_| anyhow::anyhow!("ACP process stdin closed"))?;

        let expected_id = id;

        // Read events until session completes or we get the prompt response.
        let mut event_rx = self.event_rx.lock().await;
        let mut response_text = String::new();
        let mut assistant_turns = 0u32;

        loop {
            match event_rx.recv().await {
                Some(AcpEvent::TextBlock(text)) => {
                    assistant_turns += 1;

                    // Check turn limit.
                    if let Some(max) = limits.max_turns
                        && assistant_turns > max
                    {
                        warn!(
                            agent = %self.name,
                            turns = assistant_turns,
                            max = max,
                            "turn limit exceeded, killing ACP process"
                        );
                        self.kill().await;
                        bail!(
                            "task killed: exceeded {} turn limit ({} turns)",
                            max,
                            assistant_turns
                        );
                    }

                    if let Some(sink) = progress {
                        sink(text.clone());
                    }
                    response_text.push_str(&text);
                }
                Some(AcpEvent::Response(resp)) => {
                    if resp.id == Some(expected_id) {
                        if let Some(err) = resp.error {
                            bail!("ACP session/prompt failed: {}", err);
                        }
                        // Prompt response received — task is done.
                        break;
                    }
                }
                Some(AcpEvent::PermissionRequest(req_id)) => {
                    let approval = build_permission_approval(req_id);
                    let _ = self.stdin_tx.send(approval);
                }
                Some(AcpEvent::SessionComplete) => {
                    // Session complete notification — task is done.
                    break;
                }
                Some(AcpEvent::ProcessExited {
                    exit_code,
                    stderr_tail,
                    lifetime_secs,
                }) => {
                    bail!(
                        "ACP process exited mid-task \
                         (exit_code={}, lifetime={}s, stderr={:?})",
                        exit_code
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "signal".into()),
                        lifetime_secs,
                        truncate_stderr(&stderr_tail),
                    );
                }
                None => {
                    bail!("ACP process exited mid-task (event channel closed)");
                }
            }
        }

        // Update agent state.
        if let Ok(mut state) = agent::load_state(&self.name) {
            if state.config.session == ConfigSessionMode::Persistent {
                if state.session_id != session_id {
                    state.session_start = Some(Utc::now().to_rfc3339());
                    state.session_cost = 0.0;
                    state.session_turns = 0;
                }
                state.session_id = session_id;
            }
            // ACP doesn't report cost — we track turns only.
            state.total_turns += assistant_turns;
            state.session_turns += assistant_turns;
            let _ = agent::save_state_pub(&state);
        }

        Ok(TurnResult {
            response_text,
            session_id: self.session_id.lock().await.clone().unwrap_or_default(),
            cost_usd: 0.0, // ACP doesn't report cost.
            num_turns: assistant_turns,
            token_usage: TokenUsage::default(),
            tool_use_count: 0,
        })
    }

    /// Kill the running process immediately.
    pub async fn kill(&self) {
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        warn!(agent = %self.name, "ACP process killed");
    }

    /// Gracefully stop the ACP process.
    pub async fn stop(&self) {
        // Try to cancel the current session if we have one.
        if let Some(sid) = self.session_id.lock().await.clone() {
            let id = *self.next_id.lock().await;
            let req = build_session_cancel(id, &sid);
            if let Ok(line) = req.to_line() {
                let _ = self.stdin_tx.send(line);
            }
        }

        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        info!(agent = %self.name, "ACP process stopped");
    }
}

impl Executor for AcpProcess {
    fn send_task<'a>(
        &'a self,
        message: &'a str,
        progress: Option<&'a ProgressSink>,
        image: Option<(&'a str, &'a str)>,
        limits: &'a TaskLimits,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TurnResult>> + Send + 'a>> {
        Box::pin(async move {
            // ACP doesn't support image content — include note if image was provided.
            let effective_message = if image.is_some() {
                format!(
                    "[Note: image attachment not supported via ACP]\n{}",
                    message
                )
            } else {
                message.to_string()
            };
            self.send_task(&effective_message, progress, limits).await
        })
    }

    fn inject_message(&self, _message: &str) -> Result<()> {
        warn!("ACP runtime does not support mid-task message injection");
        Ok(())
    }

    fn stop(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(self.stop())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_update_text_with_string_content() {
        let params = serde_json::json!({
            "messages": [{
                "role": "assistant",
                "content": "Hello, I am helping you."
            }]
        });
        let text = extract_update_text(&params).unwrap();
        assert_eq!(text, "Hello, I am helping you.");
    }

    #[test]
    fn test_extract_update_text_with_content_blocks() {
        let params = serde_json::json!({
            "messages": [{
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Part one. "},
                    {"type": "text", "text": "Part two."}
                ]
            }]
        });
        let text = extract_update_text(&params).unwrap();
        assert_eq!(text, "Part one. Part two.");
    }

    #[test]
    fn test_extract_update_text_skips_user_messages() {
        let params = serde_json::json!({
            "messages": [
                {"role": "user", "content": "User message"},
                {"role": "assistant", "content": "Assistant reply"}
            ]
        });
        let text = extract_update_text(&params).unwrap();
        assert_eq!(text, "Assistant reply");
    }

    #[test]
    fn test_extract_update_text_no_assistant() {
        let params = serde_json::json!({
            "messages": [{"role": "user", "content": "Hello"}]
        });
        assert!(extract_update_text(&params).is_none());
    }

    #[test]
    fn test_extract_update_text_empty_messages() {
        let params = serde_json::json!({"messages": []});
        assert!(extract_update_text(&params).is_none());
    }

    #[test]
    fn test_extract_update_text_no_messages() {
        let params = serde_json::json!({"status": "working"});
        assert!(extract_update_text(&params).is_none());
    }

    #[test]
    fn test_is_session_complete() {
        assert!(is_session_complete(
            &serde_json::json!({"status": "completed"})
        ));
        assert!(is_session_complete(&serde_json::json!({"status": "done"})));
        assert!(is_session_complete(&serde_json::json!({"status": "ended"})));
        assert!(!is_session_complete(
            &serde_json::json!({"status": "working"})
        ));
        assert!(!is_session_complete(&serde_json::json!({})));
    }

    #[test]
    fn test_extract_session_id() {
        let result = serde_json::json!({"sessionId": "sess-abc-123"});
        assert_eq!(
            extract_session_id(&result),
            Some("sess-abc-123".to_string())
        );

        let empty = serde_json::json!({});
        assert!(extract_session_id(&empty).is_none());
    }

    #[test]
    fn test_build_initialize() {
        let req = build_initialize(1);
        assert_eq!(req.method, "initialize");
        assert_eq!(req.id, 1);
        let params = req.params.unwrap();
        assert_eq!(params["capabilities"]["terminal"], false);
        assert_eq!(params["capabilities"]["fs"]["readTextFile"], false);
        assert_eq!(params["clientInfo"]["name"], "deskd");
    }

    #[test]
    fn test_build_session_new() {
        let req = build_session_new(2, "/home/dev", None);
        assert_eq!(req.method, "session/new");
        let params = req.params.unwrap();
        assert_eq!(params["cwd"], "/home/dev");
    }

    #[test]
    fn test_build_session_new_with_mcp() {
        let mut servers = HashMap::new();
        servers.insert(
            "deskd".to_string(),
            serde_json::json!({"command": "deskd", "args": ["mcp"]}),
        );
        let req = build_session_new(3, "/home/dev", Some(servers));
        let params = req.params.unwrap();
        assert_eq!(params["mcpServers"]["deskd"]["command"], "deskd");
    }

    #[test]
    fn test_build_session_load() {
        let req = build_session_load(4, "sess-123");
        assert_eq!(req.method, "session/load");
        let params = req.params.unwrap();
        assert_eq!(params["sessionId"], "sess-123");
    }

    #[test]
    fn test_build_session_prompt() {
        let req = build_session_prompt(5, "sess-123", "Write tests");
        assert_eq!(req.method, "session/prompt");
        let params = req.params.unwrap();
        assert_eq!(params["sessionId"], "sess-123");
        assert_eq!(params["text"], "Write tests");
    }

    #[test]
    fn test_build_session_cancel() {
        let req = build_session_cancel(6, "sess-123");
        assert_eq!(req.method, "session/cancel");
        let params = req.params.unwrap();
        assert_eq!(params["sessionId"], "sess-123");
    }

    #[test]
    fn test_truncate_stderr_short_passthrough() {
        assert_eq!(truncate_stderr("short error"), "short error");
        assert_eq!(truncate_stderr("  padded  "), "padded");
        assert_eq!(truncate_stderr(""), "");
    }

    #[test]
    fn test_truncate_stderr_keeps_tail_when_long() {
        let long = "A".repeat(500);
        let truncated = truncate_stderr(&long);
        assert_eq!(truncated.len(), 200);
        // Must be the *tail*, not the head — the most recent output is what
        // points at the failure.
        assert!(truncated.chars().all(|c| c == 'A'));
    }

    #[test]
    fn test_truncate_stderr_preserves_end_of_multiline() {
        let mut s = String::new();
        for i in 0..50 {
            s.push_str(&format!("line {i}: some output here\n"));
        }
        let tail = truncate_stderr(&s);
        // Tail includes the *last* line number, not the first.
        assert!(tail.contains("line 49"));
        assert!(!tail.contains("line 0:"));
    }

    #[test]
    fn test_resolve_mcp_servers_injects_default_deskd() {
        // No config path => only the default deskd server.
        let servers = resolve_mcp_servers("kira", None).unwrap();
        assert!(servers.contains_key("deskd"));
        assert_eq!(servers["deskd"]["args"][2], "kira");
    }

    #[test]
    fn test_resolve_mcp_servers_merges_user_config() {
        // Write a temp deskd.yaml with an mcp_config field.
        let dir = std::env::temp_dir().join(format!(
            "deskd-acp-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("deskd.yaml");
        let yaml = r#"
mcp_config: '{"mcpServers":{"filesystem":{"command":"mcp-fs","args":["--root","/tmp"]}}}'
"#;
        std::fs::write(&path, yaml).unwrap();

        let servers = resolve_mcp_servers("kira", Some(path.to_str().unwrap())).unwrap();
        assert!(servers.contains_key("deskd"));
        assert!(servers.contains_key("filesystem"));
        assert_eq!(servers["filesystem"]["command"], "mcp-fs");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_resolve_mcp_servers_user_deskd_takes_precedence() {
        // If the user defines their own `deskd` entry, don't clobber it.
        let dir = std::env::temp_dir().join(format!(
            "deskd-acp-test-prec-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("deskd.yaml");
        let yaml = r#"
mcp_config: '{"mcpServers":{"deskd":{"command":"/custom/deskd","args":["mcp","--agent","override"]}}}'
"#;
        std::fs::write(&path, yaml).unwrap();

        let servers = resolve_mcp_servers("kira", Some(path.to_str().unwrap())).unwrap();
        assert_eq!(servers["deskd"]["command"], "/custom/deskd");
        assert_eq!(servers["deskd"]["args"][2], "override");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_resolve_mcp_servers_missing_file_ok() {
        // Non-existent config path should not crash; default deskd still injected.
        let servers = resolve_mcp_servers("kira", Some("/nonexistent/path/deskd.yaml")).unwrap();
        assert!(servers.contains_key("deskd"));
    }

    #[test]
    fn test_build_permission_approval() {
        let line = build_permission_approval(42);
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["id"], 42);
        assert_eq!(parsed["result"]["approved"], true);
        assert!(line.ends_with('\n'));
    }
}
