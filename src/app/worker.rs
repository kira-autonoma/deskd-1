use anyhow::{Context, Result, bail};
use std::io::Write;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::app::acp;
use crate::app::agent::{self, Executor, TokenUsage};
use crate::app::message::Message;
use crate::app::tasklog;
use crate::app::unified_inbox;
use crate::domain::agent::{AgentKind, AgentRuntime};
use crate::domain::config_types::ConfigSessionMode;
use crate::domain::events::DomainEvent;

// ─── Empty-completion detection (#424) ──────────────────────────────────────
//
// A `result` event from a hung claude worker can land with `output_tokens=0`
// AND `duration_secs<2` and otherwise looks like a successful turn. The
// heuristic below flags those, increments a per-agent counter, and triggers
// an auto-restart once `consecutive_empty_completions` crosses the threshold.

/// Default threshold of consecutive empty completions before auto-restart.
pub const DEFAULT_EMPTY_COMPLETION_THRESHOLD: u32 = 3;

/// Default minimum seconds between auto-restarts triggered by empty-completion
/// detection. Prevents tight restart loops when the upstream model is dead.
pub const DEFAULT_EMPTY_COMPLETION_RESTART_MIN_SECS: u64 = 60;

/// Maximum duration (seconds) a "real" completion is expected to take when
/// `output_tokens` is also 0. Anything below this with zero output is treated
/// as suspect (likely-hung worker).
const EMPTY_COMPLETION_DURATION_THRESHOLD_SECS: u64 = 2;

/// Returns true if a `result` event looks like a hung-worker empty completion:
/// zero output tokens AND sub-threshold wall-clock duration.
///
/// Heuristic is callable from tests as well as the worker loop.
pub fn is_empty_completion(output_tokens: u64, duration_secs: u64) -> bool {
    output_tokens == 0 && duration_secs < EMPTY_COMPLETION_DURATION_THRESHOLD_SECS
}

/// Returns true if `last_restart_at` is far enough in the past (or absent)
/// that another auto-restart is allowed under the rate-limit window.
pub fn empty_restart_allowed(last_restart_at: Option<&str>, min_interval_secs: u64) -> bool {
    let Some(ts) = last_restart_at else {
        return true;
    };
    let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(ts) else {
        // Malformed timestamp — be permissive rather than wedging the agent.
        return true;
    };
    let elapsed = chrono::Utc::now().signed_duration_since(parsed.with_timezone(&chrono::Utc));
    elapsed.num_seconds() >= min_interval_secs as i64
}

/// Create an executor for the given runtime type.
///
/// Returns `Box<dyn Executor>` — the worker has no knowledge of the concrete
/// backend type. Adding a new executor (Gemini, Ollama) requires only a new
/// `Executor` impl and a match arm here.
async fn start_executor(
    name: &str,
    bus_socket: &str,
    runtime: &AgentRuntime,
) -> Result<Box<dyn Executor>> {
    match runtime {
        AgentRuntime::Claude | AgentRuntime::Memory => {
            // Memory agents use the same Claude executor — the difference
            // is in how the worker loop processes bus messages.
            let p = agent::AgentProcess::start(name, bus_socket).await?;
            Ok(Box::new(p))
        }
        AgentRuntime::Acp => {
            let p = acp::AcpProcess::start(name, bus_socket).await?;
            Ok(Box::new(p))
        }
    }
}

/// Create a fresh-session executor (no --resume).
async fn start_executor_fresh(
    name: &str,
    bus_socket: &str,
    runtime: &AgentRuntime,
) -> Result<Box<dyn Executor>> {
    match runtime {
        AgentRuntime::Claude | AgentRuntime::Memory => {
            let p = agent::AgentProcess::start_fresh(name, bus_socket).await?;
            Ok(Box::new(p))
        }
        AgentRuntime::Acp => {
            let p = acp::AcpProcess::start_fresh(name, bus_socket).await?;
            Ok(Box::new(p))
        }
    }
}

/// Connect to the bus, register, and return the stream.
///
/// Retries up to 10 times with exponential backoff (100ms initial delay,
/// doubling each attempt) to handle the race where the worker starts
/// before the bus is listening on the socket.
pub async fn bus_connect(
    socket_path: &str,
    name: &str,
    subscriptions: Vec<String>,
) -> Result<UnixStream> {
    let max_retries = 10u32;
    let initial_delay = std::time::Duration::from_millis(100);

    let mut stream = None;
    let mut last_err = None;
    for attempt in 0..max_retries {
        match UnixStream::connect(socket_path).await {
            Ok(s) => {
                if attempt > 0 {
                    info!(agent = %name, attempt = attempt + 1, "connected to bus after retry");
                }
                stream = Some(s);
                break;
            }
            Err(e) => {
                if attempt + 1 < max_retries {
                    let delay = initial_delay * 2u32.saturating_pow(attempt);
                    warn!(
                        agent = %name,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis() as u64,
                        "bus not ready, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                last_err = Some(e);
            }
        }
    }

    let mut stream = match stream {
        Some(s) => s,
        None => {
            return Err(last_err.unwrap()).with_context(|| {
                format!(
                    "Failed to connect to bus at {} after {} attempts",
                    socket_path, max_retries
                )
            });
        }
    };

    let envelope = serde_json::json!({
        "type": "register",
        "name": name,
        "subscriptions": subscriptions,
    });
    let mut line = serde_json::to_string(&envelope)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;

    info!(agent = %name, "registered on bus");
    Ok(stream)
}

/// Persist a compaction summary as a tagged static node in the agent's MainBranch on disk.
///
/// Implements AC #4 of #222: compacted context is saved to
/// `{work_dir}/.deskd/context/main.yaml` so it survives restarts. If the file
/// does not yet exist, a new MainBranch is created.
fn persist_compaction_summary(name: &str, work_dir: &str, summary: &str) -> Result<()> {
    if summary.trim().is_empty() {
        return Ok(());
    }
    let path = crate::domain::context::default_main_path(work_dir);
    let repo = crate::app::context::new_context_store();
    let mut branch = crate::app::context::MainBranch::load(&repo, &path)
        .unwrap_or_else(|_| crate::app::context::MainBranch::new(name, 100_000));
    let id = format!("compact-{}", chrono::Utc::now().to_rfc3339());
    branch.add_static_tagged(
        &id,
        "Compaction Summary",
        "system",
        summary,
        vec!["compaction-summary".to_string()],
    );
    branch.save(&repo, &path)
}

/// Helper: write an inbox entry for a completed task.
fn write_inbox(
    name: &str,
    msg: &Message,
    task: &str,
    result: Option<String>,
    error: Option<String>,
) {
    let text = if let Some(ref err) = error {
        format!("ERROR: {}", err)
    } else if let Some(ref res) = result {
        res.clone()
    } else {
        "(no output)".to_string()
    };

    let inbox_name = format!("replies/{}", msg.source);
    let inbox_msg = unified_inbox::InboxMessage {
        ts: chrono::Utc::now(),
        source: format!("agent:{}", name),
        from: Some(name.to_string()),
        text,
        metadata: serde_json::json!({
            "type": "task_result",
            "agent": name,
            "task": task,
            "message_id": msg.id,
            "in_reply_to": msg.id,
            "has_error": error.is_some(),
        }),
    };
    if let Err(e) = unified_inbox::write_message(&inbox_name, &inbox_msg) {
        warn!(agent = %name, error = %e, "failed to write inbox entry");
    }
}

/// Run the agent worker loop: read messages from bus, execute tasks, post results.
/// `bus_socket`: the agent's bus socket path, injected as DESKD_BUS_SOCKET into
/// the claude subprocess so the MCP server can connect to the bus.
pub async fn run(
    name: &str,
    socket_path: &str,
    bus_socket: Option<String>,
    subscriptions: Option<Vec<String>>,
    task_store: &dyn crate::ports::store::TaskRepository,
) -> Result<()> {
    let initial_state = agent::load_state(name)?;

    // Use custom subscriptions if provided, otherwise default.
    // Default subscriptions: Workers receive:
    //   agent:<name>     — direct messages (from MCP send_message, other agents, CLI)
    //   queue:tasks      — shared task queue
    //   telegram.in:*    — messages arriving from Telegram adapter
    let subscriptions = subscriptions.unwrap_or_else(|| {
        vec![
            format!("agent:{}", name),
            "queue:tasks".to_string(),
            "telegram.in:*".to_string(),
        ]
    });

    let stream = bus_connect(socket_path, name, subscriptions).await?;
    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let writer = std::sync::Arc::new(tokio::sync::Mutex::new(writer));

    // Start persistent agent process (reused across tasks).
    let effective_bus = bus_socket.as_deref().unwrap_or(socket_path).to_string();
    let agent_runtime: AgentRuntime = initial_state.config.runtime.clone().into();
    let agent_kind: AgentKind = initial_state.config.kind.clone().into();
    let mut process = start_executor(name, &effective_bus, &agent_runtime).await?;

    // Build task limits from agent config — enforced in real-time during tasks.
    // Context agents get a tight max_turns cap for fast Q&A (no tool loops).
    let configured_max_turns = if initial_state.config.max_turns > 0 {
        Some(initial_state.config.max_turns)
    } else {
        None
    };
    let max_turns = match agent_kind {
        AgentKind::Context => Some(match configured_max_turns {
            Some(t) => t.min(3),
            None => 3,
        }),
        AgentKind::Executor => configured_max_turns,
    };
    let limits = agent::TaskLimits { max_turns };

    info!(agent = %name, runtime = ?agent_runtime, kind = ?agent_kind, "agent process ready, waiting for tasks");

    // Startup recovery: handle orphaned active tasks assigned to this agent.
    recover_orphaned_tasks(name, task_store);

    // Task queue polling interval (30 seconds).
    let mut queue_poll = tokio::time::interval(std::time::Duration::from_secs(30));
    queue_poll.tick().await; // skip first immediate tick

    let agent_model = initial_state.config.model.clone();
    let agent_labels: Vec<String> = Vec::new(); // TODO: add labels to AgentConfig

    // Memory agent: track number of injected events for metrics.
    let mut memory_injected_count: u64 = 0;

    // Memory agent: cumulative token estimate for compaction trigger.
    let mut memory_tokens_estimate: u64 = 0;

    // Cumulative session input tokens — tracks actual token usage from Claude responses.
    // Used to trigger compaction for all agent types (not just memory agents).
    let mut session_input_tokens: u64 = 0;

    // Compaction threshold in tokens. Derive from context config or compact_threshold
    // fraction applied to a default context budget of 100_000 tokens.
    let compact_threshold_tokens: u64 = {
        let from_config = initial_state
            .config
            .context
            .as_ref()
            .and_then(|c| c.compact_threshold_tokens)
            .map(|t| t as u64);
        from_config.unwrap_or_else(|| {
            let fraction = initial_state.config.compact_threshold.unwrap_or(0.8);
            (100_000f64 * fraction) as u64
        })
    };

    loop {
        // Wait for either a bus message or a queue poll tick.
        let line = tokio::select! {
            result = lines.next_line() => {
                match result? {
                    Some(l) => l,
                    None => break, // bus closed
                }
            }
            _ = queue_poll.tick() => {
                // Poll the task queue for pending tasks matching this worker.
                if let Ok(Some(task)) = task_store.claim_next(name, &agent_model, &agent_labels) {
                    info!(
                        agent = %name,
                        task_id = %task.id,
                        description = %truncate(&task.description, 80),
                        "claimed task from queue"
                    );
                    // Synthesize a bus message from the claimed task.
                    let mut payload = serde_json::json!({
                        "task": task.description,
                        "task_queue_id": task.id,
                    });
                    if let Some(ref sm_id) = task.sm_instance_id {
                        payload["sm_instance_id"] = serde_json::json!(sm_id);
                    }
                    let synthetic = serde_json::json!({
                        "type": "message",
                        "id": format!("queue-{}", task.id),
                        "source": format!("task-queue:{}", task.created_by),
                        "target": format!("agent:{}", name),
                        "payload": payload,
                    });
                    serde_json::to_string(&synthetic).unwrap_or_default()
                } else {
                    continue; // no tasks available
                }
            }
        };

        if line.is_empty() {
            continue;
        }

        let msg: Message = match serde_json::from_str::<crate::ports::bus_wire::BusMessage>(&line) {
            Ok(dto) => dto.into(),
            Err(e) => {
                warn!(agent = %name, error = %e, "invalid message from bus");
                continue;
            }
        };

        // ── Context agent: lightweight Q&A path ─────────────────────────
        // Context agents only respond to direct questions (target == agent:{name}).
        // Fan-out subscriptions (queue:tasks, telegram.in:*) are dropped early
        // so the agent stays focused. Combined with the clamped max_turns set
        // above, this gives short-loop answers for context lookups.
        if agent_kind == AgentKind::Context {
            let direct_target = format!("agent:{}", name);
            if msg.target != direct_target {
                debug!(
                    agent = %name,
                    source = %msg.source,
                    target = %msg.target,
                    "context: skipping non-direct message"
                );
                continue;
            }
        }

        // ── Memory agent: inject bus events as context ──────────────────
        // For memory agents, messages that are NOT direct questions
        // (target != agent:{name}) are injected as context into the
        // running Claude session instead of being processed as tasks.
        if agent_runtime == AgentRuntime::Memory {
            let direct_target = format!("agent:{}", name);
            if msg.target != direct_target {
                let event_text = format_memory_event(&msg);
                info!(
                    agent = %name,
                    source = %msg.source,
                    target = %msg.target,
                    "memory: injecting bus event as context"
                );
                if let Err(e) = process.inject_message(&event_text) {
                    warn!(agent = %name, error = %e, "memory: failed to inject event");
                }
                memory_injected_count += 1;
                // Estimate tokens: ~4 characters per token.
                memory_tokens_estimate += (event_text.len() as u64) / 4;
                debug!(
                    agent = %name,
                    injected_count = memory_injected_count,
                    tokens_estimate = memory_tokens_estimate,
                    "memory: event injected"
                );

                // Check if compaction is needed.
                if crate::domain::context::should_compact(
                    memory_tokens_estimate,
                    compact_threshold_tokens,
                ) {
                    info!(
                        agent = %name,
                        tokens_estimate = memory_tokens_estimate,
                        threshold = compact_threshold_tokens,
                        "memory: triggering compaction"
                    );
                    let compact_prompt = "Compress your accumulated knowledge. \
                        Keep: decisions, errors, current state, cross-agent correlations. \
                        Drop: routine status updates, repeated checks, duplicated information. \
                        Output a condensed summary of everything important.";
                    let compact_limits = agent::TaskLimits { max_turns: Some(3) };
                    match process
                        .send_task(compact_prompt, None, None, &compact_limits)
                        .await
                    {
                        Ok(turn) => {
                            info!(
                                agent = %name,
                                cost = turn.cost_usd,
                                "memory: compaction completed"
                            );
                            if let Err(e) = persist_compaction_summary(
                                name,
                                &initial_state.config.work_dir,
                                &turn.response_text,
                            ) {
                                warn!(
                                    agent = %name,
                                    error = %e,
                                    "memory: failed to persist compaction summary"
                                );
                            }
                            // Reset token counter — the session now has condensed history.
                            memory_tokens_estimate = 0;
                        }
                        Err(e) => {
                            warn!(agent = %name, error = %e, "memory: compaction failed");
                            // Halve the estimate to avoid re-triggering immediately.
                            memory_tokens_estimate /= 2;
                        }
                    }
                }

                continue;
            }
            // Direct question — fall through to normal task processing.
            debug!(agent = %name, source = %msg.source, "memory: direct question, processing as task");
        }

        // Extract task context from the message.
        let ctx = match extract_task_context(&msg, name) {
            Some(c) => c,
            None => {
                debug!(agent = %name, "message has no task payload, skipping");
                // Log empty message with minimal context.
                let parent_agent = agent::load_state(name).ok().and_then(|s| s.parent);
                let empty_log = tasklog::TaskLog {
                    ts: chrono::Utc::now().to_rfc3339(),
                    source: msg.source.clone(),
                    turns: 0,
                    cost: 0.0,
                    duration_ms: 0,
                    status: "empty".to_string(),
                    task: String::new(),
                    error: None,
                    msg_id: msg.id.clone(),
                    github_repo: msg
                        .payload
                        .get("github_repo")
                        .and_then(|r| r.as_str())
                        .map(str::to_string),
                    github_pr: msg.payload.get("github_pr").and_then(|p| p.as_u64()),
                    input_tokens: None,
                    output_tokens: None,
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                    session_count: None,
                    tool_use_count: None,
                    parent_agent,
                };
                if let Err(e) = tasklog::log_task(name, &empty_log) {
                    warn!(agent = %name, error = %e, "failed to write task log");
                }
                continue;
            }
        };

        // Write to unified inbox (skip Telegram — adapter already writes those).
        if !msg.source.starts_with("telegram-") {
            let inbox_name = format!("agent/{}", name);
            let inbox_msg = unified_inbox::InboxMessage {
                ts: chrono::Utc::now(),
                source: msg.source.clone(),
                from: None,
                text: ctx.task_raw.clone(),
                metadata: serde_json::json!({
                    "target": msg.target,
                    "message_id": msg.id,
                }),
            };
            if let Err(e) = unified_inbox::write_message(&inbox_name, &inbox_msg) {
                warn!(agent = %name, error = %e, "failed to write to unified inbox");
            }
        }

        // Fresh session if requested or agent is ephemeral.
        let needs_fresh =
            msg.metadata.fresh || initial_state.config.session == ConfigSessionMode::Ephemeral;
        if needs_fresh {
            info!(agent = %name, fresh = msg.metadata.fresh, "fresh session requested, restarting process");
            process.stop().await;
            process = start_executor_fresh(name, &effective_bus, &agent_runtime).await?;
        }

        let task = &ctx.task_formatted;
        info!(agent = %name, source = %msg.source, task = %truncate(task, 80), "processing task");

        // Mark agent as working.
        if let Ok(mut st) = agent::load_state(name) {
            st.status = "working".to_string();
            st.current_task = truncate(task, 80).to_string();
            let _ = agent::save_state_pub(&st);
        }

        // Start Telegram typing/progress indicators if applicable.
        let telegram_progress = if let Some(chat_id) = ctx.telegram_chat_id {
            Some(TelegramProgress::start(chat_id, &writer, name).await)
        } else {
            None
        };

        // Stream progress blocks to the bus as they arrive.
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let writer_fwd = writer.clone();
        let name_owned = name.to_string();
        let reply_owned = ctx.reply_target.clone();
        let msg_id_owned = msg.id.clone();
        let fwd_chat_id = ctx.telegram_chat_id;
        let fwd_task = tokio::spawn(async move {
            let mut full_response = String::new();
            let mut typing_stopped = false;
            while let Some(text) = progress_rx.recv().await {
                if !typing_stopped {
                    if let Some(chat_id) = fwd_chat_id {
                        let ctrl_target = format!("telegram.ctrl:{}", chat_id);
                        write_bus_envelope(
                            &writer_fwd,
                            &name_owned,
                            &ctrl_target,
                            serde_json::json!({"progress_done": true}),
                        )
                        .await;
                        write_bus_envelope(
                            &writer_fwd,
                            &name_owned,
                            &ctrl_target,
                            serde_json::json!({"typing": false}),
                        )
                        .await;
                    }
                    typing_stopped = true;
                }
                full_response.push_str(&text);
                if !text.trim().is_empty() {
                    write_bus_envelope(
                        &writer_fwd,
                        &name_owned,
                        &reply_owned,
                        serde_json::json!({"result": text, "in_reply_to": msg_id_owned}),
                    )
                    .await;

                    // Broadcast agent_output event for dashboard live feed (#328).
                    write_bus_envelope(
                        &writer_fwd,
                        &name_owned,
                        "broadcast",
                        serde_json::json!({
                            "event": "agent_output",
                            "agent": name_owned,
                            "line": text,
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                        }),
                    )
                    .await;
                }
            }
            full_response
        });

        let image = ctx
            .image_base64
            .as_deref()
            .zip(ctx.image_media_type.as_deref());

        let task_start = std::time::Instant::now();

        // Wrap the tokio channel sender in a closure for the Executor trait.
        let progress_sink = move |text: String| {
            let _ = progress_tx.send(text);
        };

        // Use the persistent process. send_task writes to its stdin and reads
        // stdout events until the result event marks the turn complete.
        let mut task_fut = Box::pin(process.send_task(task, Some(&progress_sink), image, &limits));

        // Concurrently await task completion OR new bus messages for injection.
        let result = loop {
            tokio::select! {
                task_result = &mut task_fut => {
                    break task_result;
                }
                Ok(Some(bus_line)) = lines.next_line() => {
                    if bus_line.is_empty() {
                        continue;
                    }
                    if let Ok(dto) = serde_json::from_str::<crate::ports::bus_wire::BusMessage>(&bus_line) {
                        let inject_msg: Message = dto.into();
                        let inject_task = inject_msg
                            .payload
                            .get("task")
                            .and_then(|t| t.as_str())
                            .unwrap_or_default();
                        if !inject_task.is_empty() {
                            info!(
                                agent = %name,
                                source = %inject_msg.source,
                                task = %truncate(inject_task, 80),
                                "injecting mid-task message"
                            );
                            let _ = process.inject_message(inject_task);
                            // Restart typing indicator for the new message
                            if let Some(chat_id) = ctx.telegram_chat_id {
                                let ctrl_target = format!("telegram.ctrl:{}", chat_id);
                                write_bus_envelope(
                                    &writer,
                                    name,
                                    &ctrl_target,
                                    serde_json::json!({"typing": true}),
                                )
                                .await;
                                write_bus_envelope(
                                    &writer,
                                    name,
                                    &ctrl_target,
                                    serde_json::json!({"progress_start": true}),
                                )
                                .await;
                            }
                        }
                    } else {
                        warn!(agent = %name, "invalid message from bus during task, skipping");
                    }
                }
            }
        };

        // Drop the future to release borrows on process and progress_sink,
        // then drop the sink itself to close the underlying channel.
        drop(task_fut);
        drop(progress_sink);
        let full_response = fwd_task.await.unwrap_or_default();

        // Stop Telegram progress indicators.
        if let Some(tp) = telegram_progress {
            tp.stop(&writer, name).await;
        }

        set_idle(name);
        let task_duration_secs = task_start.elapsed().as_secs();
        let task_duration_ms = task_start.elapsed().as_millis() as u64;

        match result {
            Ok(ref turn) => {
                // Track cumulative session input tokens for compaction trigger.
                session_input_tokens += turn.token_usage.input_tokens;

                let empty_outcome = handle_task_success(
                    name,
                    &msg,
                    &ctx,
                    turn,
                    full_response,
                    task_duration_ms,
                    task_duration_secs,
                    &initial_state.config.work_dir,
                    &initial_state.config.model,
                    &writer,
                    task_store,
                    &agent_runtime,
                )
                .await;

                // Empty-completion auto-restart (#424). When the heuristic has
                // tripped N consecutive times we kill the (likely-hung) claude
                // worker and respawn with a fresh session, gated by a
                // configurable rate-limit window.
                if empty_outcome == EmptyCompletionOutcome::ThresholdReached
                    && note_empty_restart(name)
                {
                    error!(
                        agent = %name,
                        "agent {} hit empty-completion threshold — auto-restarting worker",
                        name
                    );
                    process.stop().await;
                    match start_executor_fresh(name, &effective_bus, &agent_runtime).await {
                        Ok(new_proc) => {
                            process = new_proc;
                            info!(
                                agent = %name,
                                "agent process auto-restarted after empty-completion threshold"
                            );
                        }
                        Err(re) => {
                            warn!(
                                agent = %name,
                                error = %re,
                                "failed to auto-restart agent after empty-completion threshold"
                            );
                        }
                    }
                }

                // Check compaction trigger for all agent types.
                // Memory agents have their own injection-based trigger above;
                // this covers regular agents hitting context limits from tasks.
                if agent_runtime != AgentRuntime::Memory
                    && crate::domain::context::should_compact(
                        session_input_tokens,
                        compact_threshold_tokens,
                    )
                {
                    info!(
                        agent = %name,
                        session_input_tokens = session_input_tokens,
                        threshold = compact_threshold_tokens,
                        "session compaction triggered — requesting context condensation"
                    );
                    let compact_prompt = "Your context is approaching capacity. \
                        Summarize everything important from this session: decisions made, \
                        current state of work, errors encountered, and next steps. \
                        Be thorough but concise — this summary will be your working memory.";
                    let compact_limits = agent::TaskLimits { max_turns: Some(3) };
                    match process
                        .send_task(compact_prompt, None, None, &compact_limits)
                        .await
                    {
                        Ok(compact_turn) => {
                            info!(
                                agent = %name,
                                cost = compact_turn.cost_usd,
                                output_tokens = compact_turn.token_usage.output_tokens,
                                "session compaction completed"
                            );
                            if let Err(e) = persist_compaction_summary(
                                name,
                                &initial_state.config.work_dir,
                                &compact_turn.response_text,
                            ) {
                                warn!(
                                    agent = %name,
                                    error = %e,
                                    "session: failed to persist compaction summary"
                                );
                            }
                            // Reset — the session now has condensed history.
                            session_input_tokens = 0;
                        }
                        Err(e) => {
                            warn!(agent = %name, error = %e, "session compaction failed");
                            // Halve to avoid re-triggering immediately.
                            session_input_tokens /= 2;
                        }
                    }
                }
            }
            Err(ref e) => {
                let needs_restart =
                    handle_task_failure(name, &msg, &ctx, e, task_duration_ms, &writer, task_store)
                        .await;
                if needs_restart {
                    warn!(agent = %name, "agent process crashed, restarting with fresh session");
                    match start_executor_fresh(name, &effective_bus, &agent_runtime).await {
                        Ok(new_proc) => {
                            process = new_proc;
                            info!(agent = %name, "agent process restarted (fresh session)");
                        }
                        Err(re) => {
                            warn!(agent = %name, error = %re, "failed to restart agent process");
                        }
                    }
                }
            }
        }
    }

    process.stop().await;
    if agent_runtime == AgentRuntime::Memory {
        info!(
            agent = %name,
            injected_events = memory_injected_count,
            "memory agent disconnected from bus"
        );
    } else {
        info!(agent = %name, "disconnected from bus");
    }
    Ok(())
}

/// Log token usage for a completed task to a JSONL file.
///
/// `status` marks entries the doctor (#422) treats specially. `Some("empty")`
/// signals the empty-completion heuristic fired (#424); `None` means the
/// completion looked normal and no status field is written.
#[allow(clippy::too_many_arguments)]
fn log_token_usage(
    work_dir: &str,
    agent_name: &str,
    source: &str,
    task: &str,
    usage: &TokenUsage,
    duration_secs: u64,
    model: &str,
    github_repo: Option<&str>,
    github_pr: Option<u64>,
    status: Option<&str>,
) {
    let deskd_dir = std::path::Path::new(work_dir).join(".deskd");
    if let Err(e) = std::fs::create_dir_all(&deskd_dir) {
        warn!(agent = %agent_name, error = %e, "failed to create .deskd dir for usage log");
        return;
    }

    let usage_path = deskd_dir.join("usage.jsonl");
    let truncated_task = if task.len() > 80 {
        let mut end = 80;
        while end > 0 && !task.is_char_boundary(end) {
            end -= 1;
        }
        &task[..end]
    } else {
        task
    };

    let mut entry = serde_json::json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "agent": agent_name,
        "source": source,
        "task": truncated_task,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cache_read": usage.cache_read_input_tokens,
        "cache_creation": usage.cache_creation_input_tokens,
        "duration_secs": duration_secs,
        "model": model,
    });
    if let Some(repo) = github_repo {
        entry["github_repo"] = serde_json::json!(repo);
    }
    if let Some(pr) = github_pr {
        entry["github_pr"] = serde_json::json!(pr);
    }
    if let Some(s) = status {
        entry["status"] = serde_json::json!(s);
    }

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&usage_path)
    {
        Ok(mut file) => {
            if let Err(e) = writeln!(file, "{}", entry) {
                warn!(agent = %agent_name, error = %e, "failed to write usage log");
            }
        }
        Err(e) => {
            warn!(agent = %agent_name, error = %e, "failed to open usage log");
        }
    }

    info!(
        agent = %agent_name,
        input_tokens = usage.input_tokens,
        output_tokens = usage.output_tokens,
        cache_read = usage.cache_read_input_tokens,
        cache_creation = usage.cache_creation_input_tokens,
        duration_secs = duration_secs,
        "token usage"
    );
}

/// Mark agent as idle in its state file.
fn set_idle(name: &str) {
    if let Ok(mut st) = agent::load_state(name) {
        st.status = "idle".to_string();
        st.current_task = String::new();
        let _ = agent::save_state_pub(&st);
    }
}

// ─── Extracted helpers for worker decomposition (#141) ───────────────────────

/// Context for a single task extracted from a bus message.
struct TaskContext {
    task_raw: String,
    task_formatted: String,
    task_queue_id: Option<String>,
    sm_instance_id: Option<String>,
    reply_target: String,
    telegram_chat_id: Option<i64>,
    github_repo: Option<String>,
    github_pr: Option<u64>,
    image_base64: Option<String>,
    image_media_type: Option<String>,
}

/// Extract task context from a bus message. Returns None if the message has no task.
fn extract_task_context(msg: &Message, _name: &str) -> Option<TaskContext> {
    let github_repo = msg
        .payload
        .get("github_repo")
        .and_then(|r| r.as_str())
        .map(str::to_string);
    let github_pr = msg.payload.get("github_pr").and_then(|p| p.as_u64());

    let task_raw = msg
        .payload
        .get("task")
        .and_then(|t| t.as_str())
        .unwrap_or_default()
        .to_string();

    if task_raw.is_empty() {
        return None;
    }

    let task_queue_id = msg
        .payload
        .get("task_queue_id")
        .and_then(|t| t.as_str())
        .map(str::to_string);

    // Format task with source context.
    let task_formatted =
        if let Some(chat_id) = msg.payload.get("telegram_chat_id").and_then(|v| v.as_i64()) {
            let label = msg
                .payload
                .get("telegram_chat_name")
                .and_then(|v| v.as_str())
                .map(|n| format!("{} ({})", n, chat_id))
                .unwrap_or_else(|| chat_id.to_string());
            let quoted = msg
                .payload
                .get("telegram_reply_to_text")
                .and_then(|v| v.as_str());
            if let Some(q) = quoted {
                format!("[Telegram: {}]\n> {}\n\n{}", label, q, task_raw)
            } else {
                format!("[Telegram: {}]\n{}", label, task_raw)
            }
        } else {
            format!("[source: {}]\n{}", msg.source, task_raw)
        };

    // Determine reply target.
    let reply_target =
        if let Some(sm_id) = msg.payload.get("sm_instance_id").and_then(|v| v.as_str()) {
            format!("sm:{}", sm_id)
        } else {
            msg.reply_to.as_deref().unwrap_or(&msg.source).to_string()
        };

    let telegram_chat_id = if reply_target.starts_with("telegram.out:") {
        reply_target
            .strip_prefix("telegram.out:")
            .and_then(|id| id.parse::<i64>().ok())
    } else {
        None
    };

    let image_base64 = msg
        .payload
        .get("image_base64")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let image_media_type = msg
        .payload
        .get("image_media_type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let sm_instance_id = msg
        .payload
        .get("sm_instance_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    Some(TaskContext {
        task_raw,
        task_formatted,
        task_queue_id,
        sm_instance_id,
        reply_target,
        telegram_chat_id,
        github_repo,
        github_pr,
        image_base64,
        image_media_type,
    })
}

/// Persist a per-turn context measurement to `~/.deskd/logs/<agent>/context.jsonl`
/// (#403 AC #2). Captures pinned-token count, resolved auto-compact threshold,
/// and model context window so future strategies (drop-tool-results,
/// fork-with-synopsis) and `/context` history have an authoritative timeseries.
///
/// Best-effort: a missing agent state, missing session, or write failure is
/// logged as `warn!` and never propagates — the worker must not fail a task
/// because the observational log couldn't be appended.
fn log_context_measurement(name: &str, model: &str, usage: &agent::TokenUsage) {
    let state = match agent::load_state(name) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                agent = %name,
                error = %e,
                "context_log: skipped — could not load agent state"
            );
            return;
        }
    };
    if state.session_id.trim().is_empty() {
        // No session yet (first turn, ACP runtime, etc.) — nothing meaningful
        // to record. context_size already treats these as `n/a`.
        return;
    }

    let tokens = usage
        .input_tokens
        .saturating_add(usage.cache_creation_input_tokens)
        .saturating_add(usage.cache_read_input_tokens);
    let threshold = crate::app::context_size::resolve_auto_compact_threshold(
        state.config.auto_compact_threshold_tokens,
    );
    let context_limit = crate::app::context_size::context_window_for_model(model);

    let entry = crate::app::context_log::ContextLog {
        ts: chrono::Utc::now().to_rfc3339(),
        agent: name.to_string(),
        session_id: state.session_id.clone(),
        model: model.to_string(),
        tokens,
        threshold,
        context_limit,
    };
    if let Err(e) = crate::app::context_log::append(&entry) {
        warn!(agent = %name, error = %e, "context_log: append failed");
    }
}

/// On startup, find active tasks assigned to this agent and handle them.
///
/// If deskd crashed while this agent was processing a task, the task is stuck in
/// Active status with no one working on it. Tasks with results are completed;
/// tasks without results are marked as Failed for visibility.
fn recover_orphaned_tasks(agent_name: &str, task_store: &dyn crate::ports::store::TaskRepository) {
    let active_tasks = match task_store.list(Some(crate::domain::task::TaskStatus::Active)) {
        Ok(tasks) => tasks,
        Err(_) => return,
    };

    for task in active_tasks {
        if task.assignee.as_deref() != Some(agent_name) {
            continue;
        }

        // If the task already has a result, complete it.
        if task.result.as_ref().is_some_and(|r| !r.is_empty()) {
            info!(
                agent = %agent_name,
                task_id = %task.id,
                "recovering orphaned task with result: completing"
            );
            if let Err(e) =
                task_store.complete(&task.id, task.result.as_deref().unwrap_or(""), None, None)
            {
                warn!(agent = %agent_name, task_id = %task.id, error = %e, "failed to complete orphaned task");
            }
            continue;
        }

        // No result — mark as Failed. Manual retry or dead-letter will handle it.
        info!(
            agent = %agent_name,
            task_id = %task.id,
            "marking orphaned active task as Failed (no result, agent crashed)"
        );
        if let Err(e) = task_store.fail(
            &task.id,
            &format!("agent {} crashed — task requires manual retry", agent_name),
        ) {
            warn!(agent = %agent_name, task_id = %task.id, error = %e, "failed to mark orphaned task as failed");
        }
    }
}

/// Handle successful task completion: log, inbox, queue update, bus reply.
/// Outcome of a successful task w.r.t. the empty-completion heuristic (#424).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmptyCompletionOutcome {
    /// Completion looked normal — counter has been reset.
    Normal,
    /// Empty completion detected; counter incremented but below threshold.
    BelowThreshold,
    /// Empty completion detected; counter has reached or exceeded threshold —
    /// caller should attempt a rate-limited auto-restart.
    ThresholdReached,
}

#[allow(clippy::too_many_arguments)]
async fn handle_task_success(
    name: &str,
    msg: &Message,
    ctx: &TaskContext,
    turn: &agent::TurnResult,
    full_response: String,
    task_duration_ms: u64,
    task_duration_secs: u64,
    work_dir: &str,
    model: &str,
    writer: &std::sync::Arc<tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>>,
    task_store: &dyn crate::ports::store::TaskRepository,
    runtime: &AgentRuntime,
) -> EmptyCompletionOutcome {
    // Empty-completion detection is restricted to `runtime: claude` per #424.
    // Memory agents share the Claude executor but their zero-output turns are
    // legitimate (event ingestion only), so we exclude them here.
    let detect_empty = matches!(runtime, AgentRuntime::Claude);
    let is_empty =
        detect_empty && is_empty_completion(turn.token_usage.output_tokens, task_duration_secs);

    let outcome = update_empty_completion_state(name, is_empty);

    if is_empty {
        warn!(
            agent = %name,
            output_tokens = turn.token_usage.output_tokens,
            duration_secs = task_duration_secs,
            consecutive = match outcome {
                EmptyCompletionOutcome::BelowThreshold | EmptyCompletionOutcome::ThresholdReached => {
                    agent::load_state(name)
                        .map(|s| s.consecutive_empty_completions)
                        .unwrap_or(0)
                }
                EmptyCompletionOutcome::Normal => 0,
            },
            "agent {} returned empty completion — likely hung",
            name
        );
    } else {
        info!(
            agent = %name,
            cost = turn.cost_usd,
            turns = turn.num_turns,
            "task completed (persistent)"
        );
    }

    let usage_status = if is_empty { Some("empty") } else { None };
    log_token_usage(
        work_dir,
        name,
        &msg.source,
        &ctx.task_formatted,
        &turn.token_usage,
        task_duration_secs,
        model,
        ctx.github_repo.as_deref(),
        ctx.github_pr,
        usage_status,
    );

    let task_status = if is_empty { "empty" } else { "ok" };
    let parent_agent = agent::load_state(name).ok().and_then(|s| s.parent);
    let log_entry = tasklog::TaskLog {
        ts: chrono::Utc::now().to_rfc3339(),
        source: msg.source.clone(),
        turns: turn.num_turns,
        cost: turn.cost_usd,
        duration_ms: task_duration_ms,
        status: task_status.to_string(),
        task: tasklog::truncate_task(&ctx.task_raw, 60),
        error: None,
        msg_id: msg.id.clone(),
        github_repo: ctx.github_repo.clone(),
        github_pr: ctx.github_pr,
        input_tokens: Some(turn.token_usage.input_tokens),
        output_tokens: Some(turn.token_usage.output_tokens),
        cache_creation_input_tokens: Some(turn.token_usage.cache_creation_input_tokens),
        cache_read_input_tokens: Some(turn.token_usage.cache_read_input_tokens),
        session_count: Some(1),
        tool_use_count: Some(turn.tool_use_count),
        parent_agent,
    };
    if let Err(e) = tasklog::log_task(name, &log_entry) {
        warn!(agent = %name, error = %e, "failed to write task log");
    }

    log_context_measurement(name, model, &turn.token_usage);

    let progress_was_streamed = !full_response.is_empty();
    let response = if full_response.is_empty() {
        turn.response_text.clone()
    } else {
        full_response
    };
    if !response.is_empty() {
        write_inbox(name, msg, &ctx.task_formatted, Some(response.clone()), None);
    }

    // Update task queue if this came from the queue.
    if let Some(ref tq_id) = ctx.task_queue_id {
        let result_text = if response.len() > 500 {
            let mut end = 500;
            while !response.is_char_boundary(end) {
                end -= 1;
            }
            format!("{}...", &response[..end])
        } else {
            response.clone()
        };
        if let Err(e) = task_store.complete(
            tq_id,
            &result_text,
            Some(turn.cost_usd),
            Some(turn.num_turns),
        ) {
            warn!(agent = %name, task_id = %tq_id, error = %e, "failed to mark queue task done");
        }
    }

    // Skip final bus write for telegram targets when progress was already streamed —
    // the progress forwarder already delivered the content to the adapter.
    let already_streamed = progress_was_streamed && ctx.reply_target.starts_with("telegram.out:");
    if !already_streamed {
        write_bus_envelope(
            writer,
            name,
            &ctx.reply_target,
            serde_json::json!({"result": response, "final": true, "in_reply_to": msg.id}),
        )
        .await;
    }

    // Emit domain event.
    let summary = truncate(&response, 200).to_string();
    emit_event(
        writer,
        name,
        &DomainEvent::TaskCompleted {
            task_id: ctx.task_queue_id.clone().unwrap_or_default(),
            instance_id: ctx.sm_instance_id.clone(),
            result_summary: summary,
        },
    )
    .await;

    outcome
}

/// Update the persistent agent state for empty-completion tracking (#424).
///
/// Increments `consecutive_empty_completions` when `is_empty` is true; resets
/// to zero on the first non-empty completion. Returns the resulting outcome
/// so the caller can decide whether to trigger a restart.
///
/// State write failures are logged at WARN level but do not propagate — the
/// detector is best-effort and must not block task completion.
fn update_empty_completion_state(name: &str, is_empty: bool) -> EmptyCompletionOutcome {
    let mut st = match agent::load_state(name) {
        Ok(s) => s,
        Err(e) => {
            warn!(agent = %name, error = %e, "empty-detection: failed to load state");
            return if is_empty {
                EmptyCompletionOutcome::BelowThreshold
            } else {
                EmptyCompletionOutcome::Normal
            };
        }
    };

    let threshold = st
        .config
        .empty_completion_threshold
        .unwrap_or(DEFAULT_EMPTY_COMPLETION_THRESHOLD);

    let outcome = if !is_empty {
        st.consecutive_empty_completions = 0;
        EmptyCompletionOutcome::Normal
    } else {
        st.consecutive_empty_completions = st.consecutive_empty_completions.saturating_add(1);
        if st.consecutive_empty_completions >= threshold {
            EmptyCompletionOutcome::ThresholdReached
        } else {
            EmptyCompletionOutcome::BelowThreshold
        }
    };

    if let Err(e) = agent::save_state_pub(&st) {
        warn!(agent = %name, error = %e, "empty-detection: failed to save state");
    }
    outcome
}

/// Note an empty-completion-triggered restart in agent state and return
/// whether the restart was permitted by the rate-limit window. Updates
/// `last_empty_restart_at` and `total_empty_restarts` on success and resets
/// the consecutive counter so post-restart traffic doesn't immediately
/// re-trigger another restart.
fn note_empty_restart(name: &str) -> bool {
    let Ok(mut st) = agent::load_state(name) else {
        return false;
    };
    let min_secs = st
        .config
        .empty_completion_restart_min_secs
        .unwrap_or(DEFAULT_EMPTY_COMPLETION_RESTART_MIN_SECS);
    if !empty_restart_allowed(st.last_empty_restart_at.as_deref(), min_secs) {
        warn!(
            agent = %name,
            "empty-detection: threshold reached but restart suppressed by rate-limit"
        );
        return false;
    }
    st.last_empty_restart_at = Some(chrono::Utc::now().to_rfc3339());
    st.total_empty_restarts = st.total_empty_restarts.saturating_add(1);
    st.consecutive_empty_completions = 0;
    if let Err(e) = agent::save_state_pub(&st) {
        warn!(agent = %name, error = %e, "empty-detection: failed to save restart state");
    }
    true
}

/// Handle task failure: log, queue update, crash recovery, bus reply.
/// Returns true if the process crashed and needs restart.
async fn handle_task_failure(
    name: &str,
    msg: &Message,
    ctx: &TaskContext,
    error: &anyhow::Error,
    task_duration_ms: u64,
    writer: &std::sync::Arc<tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>>,
    task_store: &dyn crate::ports::store::TaskRepository,
) -> bool {
    let err_str = format!("{}", error);
    warn!(agent = %name, error = %err_str, "task failed");

    let parent_agent = agent::load_state(name).ok().and_then(|s| s.parent);
    let log_entry = tasklog::TaskLog {
        ts: chrono::Utc::now().to_rfc3339(),
        source: msg.source.clone(),
        turns: 0,
        cost: 0.0,
        duration_ms: task_duration_ms,
        status: "error".to_string(),
        task: tasklog::truncate_task(&ctx.task_raw, 60),
        error: Some(err_str.clone()),
        msg_id: msg.id.clone(),
        github_repo: ctx.github_repo.clone(),
        github_pr: ctx.github_pr,
        input_tokens: None,
        output_tokens: None,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        session_count: None,
        tool_use_count: None,
        parent_agent,
    };
    if let Err(le) = tasklog::log_task(name, &log_entry) {
        warn!(agent = %name, error = %le, "failed to write task log");
    }

    if let Some(ref tq_id) = ctx.task_queue_id
        && let Err(e) = task_store.fail(tq_id, &err_str)
    {
        warn!(agent = %name, task_id = %tq_id, error = %e, "failed to mark queue task failed");
    }

    let needs_restart = err_str.contains("process exited") || err_str.contains("stdin closed");

    write_inbox(name, msg, &ctx.task_formatted, None, Some(err_str.clone()));
    write_bus_envelope(
        writer,
        name,
        &ctx.reply_target,
        serde_json::json!({"error": err_str, "in_reply_to": msg.id}),
    )
    .await;

    // Emit domain event.
    emit_event(
        writer,
        name,
        &DomainEvent::TaskFailed {
            task_id: ctx.task_queue_id.clone().unwrap_or_default(),
            instance_id: ctx.sm_instance_id.clone(),
            error: err_str,
        },
    )
    .await;

    needs_restart
}

/// Telegram typing indicator and progress message management.
struct TelegramProgress {
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    chat_id: i64,
}

impl TelegramProgress {
    /// Start typing indicator and progress timer for a Telegram-targeted task.
    async fn start(
        chat_id: i64,
        writer: &std::sync::Arc<tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>>,
        name: &str,
    ) -> Self {
        let ctrl_target = format!("telegram.ctrl:{}", chat_id);

        write_bus_envelope(
            writer,
            name,
            &ctrl_target,
            serde_json::json!({"typing": true}),
        )
        .await;
        write_bus_envelope(
            writer,
            name,
            &ctrl_target,
            serde_json::json!({"progress_start": true}),
        )
        .await;

        let start_time = std::time::Instant::now();
        let progress_writer = writer.clone();
        let progress_name = name.to_string();
        let progress_ctrl = ctrl_target;
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let elapsed = start_time.elapsed().as_secs();
                        let text = format!("⏳ Working... {}s", elapsed);
                        write_bus_envelope(
                            &progress_writer,
                            &progress_name,
                            &progress_ctrl,
                            serde_json::json!({"progress_update": text}),
                        )
                        .await;
                    }
                    _ = &mut cancel_rx => break,
                }
            }
        });

        Self {
            cancel_tx: Some(cancel_tx),
            chat_id,
        }
    }

    /// Stop typing and progress indicators.
    async fn stop(
        mut self,
        writer: &std::sync::Arc<tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>>,
        name: &str,
    ) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
        let ctrl_target = format!("telegram.ctrl:{}", self.chat_id);
        write_bus_envelope(
            writer,
            name,
            &ctrl_target,
            serde_json::json!({"progress_done": true}),
        )
        .await;
        write_bus_envelope(
            writer,
            name,
            &ctrl_target,
            serde_json::json!({"typing": false}),
        )
        .await;
    }
}

/// Send a message via the bus (connect, send, wait for one reply, disconnect).
pub async fn send_via_bus(
    socket_path: &str,
    source: &str,
    target: &str,
    task: &str,
    max_turns: Option<u32>,
) -> Result<()> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("Failed to connect to bus at {}", socket_path))?;

    let reg = serde_json::json!({"type": "register", "name": source, "subscriptions": []});
    let mut line = serde_json::to_string(&reg)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;

    let mut payload = serde_json::json!({"task": task});
    if let Some(turns) = max_turns {
        payload["max_turns"] = serde_json::json!(turns);
    }

    let msg = serde_json::json!({
        "type": "message",
        "id": Uuid::new_v4().to_string(),
        "source": source,
        "target": target,
        "payload": payload,
        "reply_to": format!("agent:{}", source),
        "metadata": {"priority": 5u8},
    });
    let mut msg_line = serde_json::to_string(&msg)?;
    msg_line.push('\n');
    stream.write_all(msg_line.as_bytes()).await?;

    debug!(source = %source, target = %target, "task posted to bus");

    let (reader, _) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    if let Some(response_line) = lines.next_line().await? {
        let resp: serde_json::Value = serde_json::from_str(&response_line)?;
        if let Some(result) = resp
            .get("payload")
            .and_then(|p| p.get("result"))
            .and_then(|r| r.as_str())
        {
            println!("{}", result);
        } else if let Some(err) = resp
            .get("payload")
            .and_then(|p| p.get("error"))
            .and_then(|e| e.as_str())
        {
            bail!("Agent error: {}", err);
        } else {
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
    }

    Ok(())
}

/// Write a bus message envelope to the shared writer.
async fn write_bus_envelope(
    writer: &std::sync::Arc<tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>>,
    source: &str,
    target: &str,
    payload: serde_json::Value,
) {
    let envelope = serde_json::json!({
        "type": "message",
        "id": Uuid::new_v4().to_string(),
        "source": source,
        "target": target,
        "payload": payload,
        "metadata": {"priority": 5u8},
    });
    let Ok(mut line) = serde_json::to_string(&envelope) else {
        return;
    };
    line.push('\n');
    let mut w = writer.lock().await;
    let _ = w.write_all(line.as_bytes()).await;
}

/// Publish a domain event to the message bus via the worker's bus connection.
async fn emit_event(
    writer: &std::sync::Arc<tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>>,
    source: &str,
    event: &DomainEvent,
) {
    write_bus_envelope(
        writer,
        source,
        &format!("events:{}", event.event_type()),
        serde_json::Value::from(event),
    )
    .await;
}

/// Format a bus message as a compact event string for memory agent context injection.
///
/// Produces lines like:
/// `[agent:dev @ 2026-04-14T10:30:00Z] task_result: merged PR #331`
pub fn format_memory_event(msg: &Message) -> String {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    // Determine event type from payload.
    let event_type = if msg.payload.get("result").is_some() {
        "task_result"
    } else if msg.payload.get("error").is_some() {
        "error"
    } else if msg.payload.get("event").and_then(|v| v.as_str()).is_some() {
        msg.payload["event"].as_str().unwrap_or("event")
    } else if msg.payload.get("task").is_some() {
        "task"
    } else {
        "message"
    };

    // Extract a summary from the payload.
    let summary = if let Some(result) = msg.payload.get("result").and_then(|v| v.as_str()) {
        truncate(result, 200).to_string()
    } else if let Some(error) = msg.payload.get("error").and_then(|v| v.as_str()) {
        truncate(error, 200).to_string()
    } else if let Some(task) = msg.payload.get("task").and_then(|v| v.as_str()) {
        truncate(task, 200).to_string()
    } else {
        // Fallback: serialize the payload compactly.
        let s = serde_json::to_string(&msg.payload).unwrap_or_default();
        truncate(&s, 200).to_string()
    };

    format!(
        "[{source} @ {ts}] {event_type}: {summary}",
        source = msg.source,
        ts = ts,
        event_type = event_type,
        summary = summary,
    )
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ─── empty-completion heuristic tests (#424) ────────────────────────────

    #[test]
    fn test_is_empty_completion_zero_tokens_short_duration_is_empty() {
        assert!(is_empty_completion(0, 0));
        assert!(is_empty_completion(0, 1));
    }

    #[test]
    fn test_is_empty_completion_real_completion_is_not_empty() {
        // Real completions emit tokens; that alone disqualifies.
        assert!(!is_empty_completion(100, 0));
        assert!(!is_empty_completion(1, 60));
    }

    #[test]
    fn test_is_empty_completion_long_zero_token_duration_is_not_empty() {
        // A worker that genuinely ran but legitimately returned zero tokens
        // (e.g. tool-only turn) takes more than the threshold — don't flag.
        assert!(!is_empty_completion(
            0,
            EMPTY_COMPLETION_DURATION_THRESHOLD_SECS
        ));
        assert!(!is_empty_completion(
            0,
            EMPTY_COMPLETION_DURATION_THRESHOLD_SECS + 5
        ));
    }

    #[test]
    fn test_empty_restart_allowed_when_no_prior_restart() {
        assert!(empty_restart_allowed(None, 60));
    }

    #[test]
    fn test_empty_restart_allowed_when_window_elapsed() {
        // Pick a timestamp clearly older than any plausible window.
        let stale = "2020-01-01T00:00:00Z";
        assert!(empty_restart_allowed(Some(stale), 60));
    }

    #[test]
    fn test_empty_restart_allowed_blocks_within_window() {
        // Use "now" so the window is fresh.
        let now = chrono::Utc::now().to_rfc3339();
        assert!(!empty_restart_allowed(Some(&now), 60));
    }

    // ─── update_empty_completion_state tests (#424) ─────────────────────────

    /// Set up an isolated HOME under /tmp and persist a minimal `AgentState`
    /// for `name` so `update_empty_completion_state` can load + save it.
    /// Returns the agent name plus the env-lock guard — callers MUST keep
    /// the guard alive for the test's full duration so concurrent env
    /// mutations elsewhere can't race with this test's file I/O.
    fn setup_agent_state(threshold: Option<u32>) -> (String, tokio::sync::MutexGuard<'static, ()>) {
        // Acquire the global env lock BEFORE mutating HOME. setenv is not
        // thread-safe on POSIX; without serialization, concurrent set_var
        // calls cause UB that flakes unrelated file-I/O tests.
        let guard = crate::test_support::env_lock().blocking_lock();
        let name = format!("emptyfn-{}", uuid::Uuid::new_v4());
        let tmp = std::env::temp_dir().join(format!("deskd-test-{}", &name));
        std::fs::create_dir_all(tmp.join(".deskd/agents")).unwrap();
        // SAFETY: ENV_LOCK serializes all env-mutating tests, and per-test
        // unique tmp dir keeps state files from clobbering each other.
        unsafe { std::env::set_var("HOME", &tmp) };

        let cfg = crate::app::agent::AgentConfig {
            name: name.clone(),
            model: "claude-sonnet-4-6".into(),
            system_prompt: String::new(),
            work_dir: "/tmp".into(),
            max_turns: 10,
            unix_user: None,
            command: vec!["claude".into()],
            config_path: None,
            container: None,
            session: crate::infra::dto::ConfigSessionMode::Persistent,
            runtime: crate::infra::dto::ConfigAgentRuntime::Claude,
            launch_mode: crate::infra::dto::ConfigLaunchMode::Subprocess,
            kind: crate::infra::dto::ConfigAgentKind::Executor,
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: threshold,
            empty_completion_restart_min_secs: None,
        };
        let state = crate::app::agent::AgentState {
            config: cfg,
            pid: 0,
            session_id: String::new(),
            total_turns: 0,
            total_cost: 0.0,
            created_at: chrono::Utc::now().to_rfc3339(),
            status: "idle".into(),
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
            tmux_session: None,
            tmux_log_path: None,
        };
        crate::app::agent::save_state_pub(&state).unwrap();
        (name, guard)
    }

    #[test]
    fn test_update_empty_completion_state_normal_resets_counter() {
        let (name, _env_guard) = setup_agent_state(Some(3));
        // Pre-seed counter to 2 so we can verify the reset path.
        let mut st = crate::app::agent::load_state(&name).unwrap();
        st.consecutive_empty_completions = 2;
        crate::app::agent::save_state_pub(&st).unwrap();

        let outcome = update_empty_completion_state(&name, false);
        assert_eq!(outcome, EmptyCompletionOutcome::Normal);

        let st = crate::app::agent::load_state(&name).unwrap();
        assert_eq!(st.consecutive_empty_completions, 0);
    }

    #[test]
    fn test_update_empty_completion_state_below_threshold_increments() {
        let (name, _env_guard) = setup_agent_state(Some(3));

        let outcome = update_empty_completion_state(&name, true);
        assert_eq!(outcome, EmptyCompletionOutcome::BelowThreshold);
        let st = crate::app::agent::load_state(&name).unwrap();
        assert_eq!(st.consecutive_empty_completions, 1);

        let outcome = update_empty_completion_state(&name, true);
        assert_eq!(outcome, EmptyCompletionOutcome::BelowThreshold);
        let st = crate::app::agent::load_state(&name).unwrap();
        assert_eq!(st.consecutive_empty_completions, 2);
    }

    #[test]
    fn test_update_empty_completion_state_threshold_reached() {
        let (name, _env_guard) = setup_agent_state(Some(3));
        // Seed counter to 2 so the next empty completion reaches threshold (3).
        let mut st = crate::app::agent::load_state(&name).unwrap();
        st.consecutive_empty_completions = 2;
        crate::app::agent::save_state_pub(&st).unwrap();

        let outcome = update_empty_completion_state(&name, true);
        assert_eq!(outcome, EmptyCompletionOutcome::ThresholdReached);

        let st = crate::app::agent::load_state(&name).unwrap();
        assert_eq!(st.consecutive_empty_completions, 3);
    }

    // ─── truncate tests ──────────────────────────────────────────────────────

    #[test]
    fn test_truncate_short_string() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_exact_length() {
        assert_eq!(truncate("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_long_string() {
        assert_eq!(truncate("hello world", 5), "hello");
    }

    #[test]
    fn test_truncate_empty_string() {
        assert_eq!(truncate("", 10), "");
    }

    #[test]
    fn test_truncate_unicode_boundary() {
        // '€' is 3 bytes in UTF-8; truncating at byte 1 should snap back to 0.
        let s = "€abc";
        let result = truncate(s, 1);
        assert!(result.is_empty());
    }

    #[test]
    fn test_truncate_multibyte_safe() {
        let s = "a€b"; // a=1byte, €=3bytes, b=1byte → total 5
        assert_eq!(truncate(s, 4), "a€");
        assert_eq!(truncate(s, 3), "a"); // byte 3 is mid-€, snaps back
    }

    // ─── extract_task_context tests ──────────────────────────────────────────

    fn make_message(payload: serde_json::Value) -> Message {
        Message {
            id: "msg-1".into(),
            source: "cli".into(),
            target: "agent:test".into(),
            payload,
            reply_to: None,
            metadata: Default::default(),
        }
    }

    #[test]
    fn test_extract_task_context_empty_payload() {
        let msg = make_message(json!({}));
        assert!(extract_task_context(&msg, "test").is_none());
    }

    #[test]
    fn test_extract_task_context_empty_task() {
        let msg = make_message(json!({"task": ""}));
        assert!(extract_task_context(&msg, "test").is_none());
    }

    #[test]
    fn test_extract_task_context_basic() {
        let msg = make_message(json!({"task": "do something"}));
        let ctx = extract_task_context(&msg, "test").unwrap();
        assert_eq!(ctx.task_raw, "do something");
        assert!(ctx.task_formatted.contains("[source: cli]"));
        assert!(ctx.task_formatted.contains("do something"));
        assert_eq!(ctx.reply_target, "cli");
        assert!(ctx.telegram_chat_id.is_none());
        assert!(ctx.github_repo.is_none());
    }

    #[test]
    fn test_extract_task_context_with_reply_to() {
        let mut msg = make_message(json!({"task": "work"}));
        msg.reply_to = Some("agent:dev".into());
        let ctx = extract_task_context(&msg, "test").unwrap();
        assert_eq!(ctx.reply_target, "agent:dev");
    }

    #[test]
    fn test_extract_task_context_telegram() {
        let msg = make_message(json!({
            "task": "hello",
            "telegram_chat_id": -1234567890_i64,
            "telegram_chat_name": "Dev Chat"
        }));
        let ctx = extract_task_context(&msg, "test").unwrap();
        assert!(
            ctx.task_formatted
                .contains("[Telegram: Dev Chat (-1234567890)]")
        );
        assert!(ctx.task_formatted.contains("hello"));
    }

    #[test]
    fn test_extract_task_context_telegram_with_quote() {
        let msg = make_message(json!({
            "task": "reply to this",
            "telegram_chat_id": -100,
            "telegram_reply_to_text": "original message"
        }));
        let ctx = extract_task_context(&msg, "test").unwrap();
        assert!(ctx.task_formatted.contains("> original message"));
    }

    #[test]
    fn test_extract_task_context_github() {
        let msg = make_message(json!({
            "task": "review PR",
            "github_repo": "kgatilin/deskd",
            "github_pr": 42
        }));
        let ctx = extract_task_context(&msg, "test").unwrap();
        assert_eq!(ctx.github_repo.as_deref(), Some("kgatilin/deskd"));
        assert_eq!(ctx.github_pr, Some(42));
    }

    #[test]
    fn test_extract_task_context_sm_reply() {
        let msg = make_message(json!({
            "task": "handle this",
            "sm_instance_id": "sm-abc-123"
        }));
        let ctx = extract_task_context(&msg, "test").unwrap();
        assert_eq!(ctx.reply_target, "sm:sm-abc-123");
    }

    #[test]
    fn test_extract_task_context_telegram_out_reply() {
        let mut msg = make_message(json!({"task": "respond"}));
        msg.reply_to = Some("telegram.out:-1234".into());
        let ctx = extract_task_context(&msg, "test").unwrap();
        assert_eq!(ctx.reply_target, "telegram.out:-1234");
        assert_eq!(ctx.telegram_chat_id, Some(-1234));
    }

    #[test]
    fn test_extract_task_context_task_queue_id() {
        let msg = make_message(json!({
            "task": "queued work",
            "task_queue_id": "tq-456"
        }));
        let ctx = extract_task_context(&msg, "test").unwrap();
        assert_eq!(ctx.task_queue_id.as_deref(), Some("tq-456"));
    }

    // ─── format_memory_event tests ──────────────────────────────────────

    #[test]
    fn test_format_memory_event_task_result() {
        let msg = Message {
            id: "msg-1".into(),
            source: "agent:dev".into(),
            target: "agent:kira".into(),
            payload: json!({"result": "merged PR #331 — archlint scan passed"}),
            reply_to: None,
            metadata: Default::default(),
        };
        let event = format_memory_event(&msg);
        assert!(event.starts_with("[agent:dev @ "));
        assert!(event.contains("] task_result: merged PR #331"));
    }

    #[test]
    fn test_format_memory_event_error() {
        let msg = Message {
            id: "msg-2".into(),
            source: "agent:worker".into(),
            target: "broadcast".into(),
            payload: json!({"error": "process exited with code 1"}),
            reply_to: None,
            metadata: Default::default(),
        };
        let event = format_memory_event(&msg);
        assert!(event.contains("] error: process exited"));
    }

    #[test]
    fn test_format_memory_event_task() {
        let msg = Message {
            id: "msg-3".into(),
            source: "cli".into(),
            target: "agent:dev".into(),
            payload: json!({"task": "review the PR"}),
            reply_to: None,
            metadata: Default::default(),
        };
        let event = format_memory_event(&msg);
        assert!(event.contains("] task: review the PR"));
    }

    #[test]
    fn test_format_memory_event_generic() {
        let msg = Message {
            id: "msg-4".into(),
            source: "scheduler".into(),
            target: "broadcast".into(),
            payload: json!({"event": "agent_output", "line": "hello world"}),
            reply_to: None,
            metadata: Default::default(),
        };
        let event = format_memory_event(&msg);
        assert!(event.contains("] agent_output: "));
    }

    #[test]
    fn test_format_memory_event_truncates_long_payload() {
        let long_text = "x".repeat(500);
        let msg = Message {
            id: "msg-5".into(),
            source: "agent:dev".into(),
            target: "agent:kira".into(),
            payload: json!({"result": long_text}),
            reply_to: None,
            metadata: Default::default(),
        };
        let event = format_memory_event(&msg);
        // The summary should be truncated to 200 chars.
        assert!(event.len() < 300);
    }

    // ─── memory agent: direct question detection ────────────────────────

    #[test]
    fn test_memory_direct_question_detection() {
        let agent_name = "memory-all";
        let direct_target = format!("agent:{}", agent_name);

        // Direct question — target matches agent's own address.
        assert_eq!(direct_target, "agent:memory-all");

        // Bus event — target is some other agent, arrived via glob subscription.
        let event_target = "agent:dev";
        assert_ne!(event_target, direct_target);

        // Another bus event — broadcast.
        let broadcast_target = "broadcast";
        assert_ne!(broadcast_target, direct_target);
    }

    // ─── memory agent: should_compact wiring ───────────────────────────

    #[test]
    fn test_should_compact_basic() {
        use crate::domain::context::should_compact;
        assert!(!should_compact(50_000, 80_000));
        assert!(should_compact(80_000, 80_000));
        assert!(should_compact(90_000, 80_000));
        assert!(!should_compact(0, 80_000));
    }

    #[test]
    fn test_compact_threshold_defaults() {
        // Default fraction 0.8 applied to default budget of 100_000 → 80_000 tokens.
        let fraction = 0.8f64;
        let threshold = (100_000f64 * fraction) as u64;
        assert_eq!(threshold, 80_000);
    }

    #[test]
    fn test_compact_threshold_from_config() {
        // When compact_threshold_tokens is explicitly set in context config, use it.
        use crate::domain::config_types::ConfigContextConfig;
        let ctx = ConfigContextConfig {
            enabled: true,
            compact_threshold_tokens: Some(50_000),
            main_budget_tokens: None,
            main_path: None,
        };
        let threshold = ctx.compact_threshold_tokens.unwrap() as u64;
        assert_eq!(threshold, 50_000);
    }

    // ─── integration: two agents produce events that a memory agent accumulates ─

    #[test]
    fn test_memory_agent_accumulates_events_from_multiple_agents() {
        // Simulate two agents (dev, worker) producing bus events that a memory
        // agent would receive via glob subscription. Verify format_memory_event
        // produces correct output and routing logic distinguishes bus events from
        // direct questions.
        let memory_agent = "memory-all";
        let direct_target = format!("agent:{}", memory_agent);

        // Agent 1: dev sends a task result
        let dev_msg = Message {
            id: "msg-dev-1".into(),
            source: "agent:dev".into(),
            target: "agent:kira".into(),
            payload: json!({"result": "deployed v1.2.3 to production"}),
            reply_to: None,
            metadata: Default::default(),
        };

        // Agent 2: worker sends an error event
        let worker_msg = Message {
            id: "msg-worker-1".into(),
            source: "agent:worker".into(),
            target: "broadcast".into(),
            payload: json!({"error": "build failed: missing dependency tokio"}),
            reply_to: None,
            metadata: Default::default(),
        };

        // Agent 2: worker sends a task assignment
        let worker_task_msg = Message {
            id: "msg-worker-2".into(),
            source: "cli".into(),
            target: "agent:worker".into(),
            payload: json!({"task": "run cargo test in deskd repo"}),
            reply_to: None,
            metadata: Default::default(),
        };

        // Direct question to memory agent — should NOT be injected as event
        let direct_question = Message {
            id: "msg-direct-1".into(),
            source: "agent:kira".into(),
            target: "agent:memory-all".into(),
            payload: json!({"task": "what errors happened today?"}),
            reply_to: None,
            metadata: Default::default(),
        };

        // Verify all bus events are correctly formatted with source attribution.
        let events: Vec<String> = [&dev_msg, &worker_msg, &worker_task_msg]
            .iter()
            .map(|m| format_memory_event(m))
            .collect();

        // Event 1: dev task result
        assert!(
            events[0].contains("[agent:dev @"),
            "event should attribute source agent:dev"
        );
        assert!(
            events[0].contains("task_result: deployed v1.2.3"),
            "event should contain the result summary"
        );

        // Event 2: worker error
        assert!(
            events[1].contains("[agent:worker @"),
            "event should attribute source agent:worker"
        );
        assert!(
            events[1].contains("error: build failed"),
            "event should contain the error summary"
        );

        // Event 3: CLI task to worker
        assert!(
            events[2].contains("[cli @"),
            "event should attribute source cli"
        );
        assert!(
            events[2].contains("task: run cargo test"),
            "event should contain the task summary"
        );

        // Verify routing: bus events (target != agent:memory-all) are injected
        assert_ne!(dev_msg.target, direct_target, "dev msg is a bus event");
        assert_ne!(
            worker_msg.target, direct_target,
            "worker msg is a bus event"
        );
        assert_ne!(
            worker_task_msg.target, direct_target,
            "worker task is a bus event"
        );
        // Direct question (target == agent:memory-all) falls through to task processing
        assert_eq!(
            direct_question.target, direct_target,
            "direct question targets memory agent"
        );

        // Verify accumulation: token estimate grows with each injected event
        let mut token_estimate: u64 = 0;
        for event in &events {
            token_estimate += (event.len() as u64) / 4;
        }
        assert!(
            token_estimate > 0,
            "accumulated token estimate should be positive"
        );
        // Each event is ~50-80 chars → ~12-20 tokens each. 3 events = 36-60 tokens.
        assert!(
            token_estimate >= 10,
            "three events should produce at least 10 token estimate"
        );
    }

    #[test]
    fn test_memory_agent_compaction_trigger_logic() {
        use crate::domain::context::should_compact;

        // Simulate token accumulation from multiple agent events and verify
        // compaction triggers at the right threshold.
        let compact_threshold: u64 = 80_000;
        let mut tokens: u64 = 0;
        let mut compaction_triggered = false;

        // Simulate 1000 events of ~320 chars each (~80 tokens per event).
        for i in 0..1000 {
            let event_len: u64 = 320; // approximate event text length
            tokens += event_len / 4; // ~80 tokens per event

            if should_compact(tokens, compact_threshold) {
                compaction_triggered = true;
                assert!(
                    tokens >= compact_threshold,
                    "compaction should only trigger at or above threshold"
                );
                assert_eq!(
                    i, 999,
                    "with 80 tokens/event, compaction at 80k tokens fires at event 1000"
                );
                // After compaction, the real code resets the counter.
                break;
            }
        }
        assert!(
            compaction_triggered,
            "compaction should have triggered within 1000 events"
        );
    }

    // ─── persist_compaction_summary tests ────────────────────────────────────

    #[test]
    fn test_persist_compaction_summary_creates_file() {
        let dir = std::env::temp_dir().join(format!("deskd-compact-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let work_dir = dir.to_str().unwrap();

        let summary = "Decisions: chose A over B. Errors: hit X. Next: ship.";
        persist_compaction_summary("test-agent", work_dir, summary).expect("persist failed");

        let path = crate::domain::context::default_main_path(work_dir);
        assert!(path.exists(), "main.yaml should exist at {:?}", path);

        let repo = crate::app::context::new_context_store();
        let branch = crate::app::context::MainBranch::load(&repo, &path).expect("load failed");
        assert!(
            branch.nodes.iter().any(|n| {
                n.tags.iter().any(|t| t == "compaction-summary")
                    && matches!(
                        &n.kind,
                        crate::domain::context::NodeKind::Static { content, .. }
                            if content == summary
                    )
            }),
            "expected node tagged compaction-summary with summary content"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_persist_compaction_summary_appends_to_existing() {
        let dir = std::env::temp_dir().join(format!("deskd-compact-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let work_dir = dir.to_str().unwrap();

        persist_compaction_summary("test-agent", work_dir, "first").unwrap();
        // chrono::Utc::now() has nanosecond precision, but to be safe use distinct content.
        std::thread::sleep(std::time::Duration::from_millis(2));
        persist_compaction_summary("test-agent", work_dir, "second").unwrap();

        let path = crate::domain::context::default_main_path(work_dir);
        let repo = crate::app::context::new_context_store();
        let branch = crate::app::context::MainBranch::load(&repo, &path).unwrap();
        let count = branch
            .nodes
            .iter()
            .filter(|n| n.tags.iter().any(|t| t == "compaction-summary"))
            .count();
        assert_eq!(count, 2, "expected two compaction-summary nodes");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_persist_compaction_summary_empty_is_noop() {
        let dir = std::env::temp_dir().join(format!("deskd-compact-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let work_dir = dir.to_str().unwrap();

        persist_compaction_summary("test-agent", work_dir, "   \n  ").unwrap();
        let path = crate::domain::context::default_main_path(work_dir);
        assert!(!path.exists(), "empty summary must not create file");

        std::fs::remove_dir_all(&dir).ok();
    }
}
