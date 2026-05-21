//! Bus API — structured query/command handler for TUI and external clients.
//!
//! Connects to the agent's bus as `deskd:api`, subscribes to `deskd:query` and
//! `deskd:command`, and routes incoming requests to existing stores/services.
//! Responses are sent back to the requester's `source` address.
//!
//! # Message format
//!
//! Request (sent to `deskd:query` or `deskd:command`):
//! ```json
//! {"type":"message","target":"deskd:query","source":"tui","payload":{
//!   "method": "agent_list",
//!   "params": {},
//!   "request_id": "req-1"
//! }}
//! ```
//!
//! Response (sent back to `source`):
//! ```json
//! {"type":"message","target":"tui","source":"deskd:api","payload":{
//!   "method": "agent_list",
//!   "request_id": "req-1",
//!   "result": [...]
//! }}
//! ```

use anyhow::{Result, bail};
use serde_json::{Value, json};
use tracing::{info, warn};

use crate::app::mcp_service;
use crate::app::reload_state::ReloadState;
use crate::config::UserConfig;
use crate::ports::bus::MessageBus;
use crate::ports::store::{StateMachineRepository, TaskRepository};

const API_CLIENT_NAME: &str = "deskd:api";

/// Run the bus API handler. Connects to the bus, subscribes to query/command
/// topics, and dispatches incoming requests to the appropriate handler.
///
/// This function runs forever (until the bus connection drops).
///
/// `agent_cfg_path` is the path to this agent's `deskd.yaml` — used by the
/// `reload_config` RPC to know which file to re-parse (#474).
/// `reload_state` is the shared per-agent reload state; the `reload_config`
/// RPC fires its `Notify` to wake the watcher, and the `bus_status` RPC
/// surfaces `last_reload_at` for observability.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    bus_socket: &str,
    task_store: &(dyn TaskRepository + Send + Sync),
    sm_store: &(dyn StateMachineRepository + Send + Sync),
    user_config: Option<&UserConfig>,
    agent_name: &str,
    agent_cfg_path: &str,
    reload_state: ReloadState,
) -> Result<()> {
    let bus = crate::app::bus::connect_bus(bus_socket).await?;
    bus.register(
        API_CLIENT_NAME,
        &["deskd:query".to_string(), "deskd:command".to_string()],
    )
    .await?;
    info!("bus_api registered on {}", bus_socket);

    loop {
        let msg = bus.recv().await?;

        let method = msg
            .payload
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let params = msg
            .payload
            .get("params")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let request_id = msg
            .payload
            .get("request_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let reply_target = msg.source.clone();

        let result = dispatch(
            &method,
            &params,
            bus_socket,
            task_store,
            sm_store,
            user_config,
            agent_name,
            agent_cfg_path,
            &reload_state,
        )
        .await;

        let response_payload = match result {
            Ok(value) => json!({
                "method": method,
                "request_id": request_id,
                "result": value,
            }),
            Err(e) => json!({
                "method": method,
                "request_id": request_id,
                "error": e.to_string(),
            }),
        };

        let response = crate::domain::message::Message {
            id: uuid::Uuid::new_v4().to_string(),
            source: API_CLIENT_NAME.to_string(),
            target: reply_target,
            payload: response_payload,
            reply_to: None,
            metadata: Default::default(),
        };

        if let Err(e) = bus.send(&response).await {
            warn!(error = %e, "bus_api: failed to send response");
        }
    }
}

/// Dispatch a method call to the appropriate handler.
#[allow(clippy::too_many_arguments)]
async fn dispatch(
    method: &str,
    params: &Value,
    bus_socket: &str,
    task_store: &(dyn TaskRepository + Send + Sync),
    sm_store: &(dyn StateMachineRepository + Send + Sync),
    user_config: Option<&UserConfig>,
    agent_name: &str,
    agent_cfg_path: &str,
    reload_state: &ReloadState,
) -> Result<Value> {
    match method {
        // ── Queries ─────────────────────────────────────────────────────
        "agent_list" => handle_agent_list(bus_socket).await,
        "agent_detail" => handle_agent_detail(params).await,
        "task_list" => handle_task_list(params, task_store),
        "task_detail" => handle_task_detail(params, task_store),
        "sm_list" => handle_sm_list(params, sm_store),
        "sm_detail" => handle_sm_detail(params, sm_store),
        "sm_models" => handle_sm_models(user_config),
        "usage_stats" => handle_usage_stats(params).await,
        "agent_turn_stats" => handle_agent_turn_stats(params),
        "schedule_list" => handle_schedule_list(user_config),
        "inbox_list" => handle_inbox_list(agent_name, user_config),
        "inbox_read" => handle_inbox_read(params, agent_name, user_config),
        "inbox_search" => handle_inbox_search(params, agent_name, user_config),
        "bus_status" => handle_bus_status(bus_socket, reload_state).await,
        "room_list" => handle_room_list().await,
        "room_children" => handle_room_children(params),
        "context_stats" => handle_context_stats(params, bus_socket).await,
        "agent_requests" => handle_agent_requests(params, task_store),
        "agent_messages" => handle_agent_messages(params),
        "agent_config_list" => handle_agent_config_list(),

        // ── Mutations ───────────────────────────────────────────────────
        "send_message" => handle_send_message(params, bus_socket, agent_name).await,
        "task_create" => handle_task_create(params, task_store, agent_name),
        "task_cancel" => handle_task_cancel(params, task_store),
        "sm_create" => {
            handle_sm_create(params, bus_socket, sm_store, user_config, agent_name).await
        }
        "sm_move" => handle_sm_move(params, bus_socket, sm_store, user_config, agent_name).await,
        "sm_cancel" => handle_sm_cancel(params, sm_store),
        "schedule_add" => handle_schedule_add(params, user_config, agent_name),
        "schedule_remove" => handle_schedule_remove(params, user_config, agent_name),
        "agent_restart" => handle_agent_restart(params, bus_socket, agent_name).await,
        "agent_compress" => handle_agent_compress(params, bus_socket, agent_name).await,
        "reload_config" => {
            handle_reload_config(params, agent_cfg_path, agent_name, reload_state).await
        }

        _ => bail!("unknown method: {}", method),
    }
}

// ─── Query handlers ─────────────────────────────────────────────────────────

async fn handle_agent_list(bus_socket: &str) -> Result<Value> {
    let live = crate::app::serve::query_live_agents(bus_socket)
        .await
        .unwrap_or_default();
    let agents = crate::app::agent::list().await?;
    let summaries: Vec<Value> = agents
        .iter()
        .map(|state| {
            let is_finished = !live.contains(&state.config.name);
            mcp_service::build_agent_summary(&state.config.name, is_finished)
        })
        .collect();
    Ok(json!(summaries))
}

async fn handle_agent_detail(params: &Value) -> Result<Value> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'name' parameter"))?;
    let state = crate::app::agent::load_state(name)?;

    // Aggregate token breakdown from task logs.
    let entries = crate::app::tasklog::read_logs(name, usize::MAX, None, None).unwrap_or_default();
    let mut input_tokens: u64 = 0;
    let mut output_tokens: u64 = 0;
    let mut cache_creation_input_tokens: u64 = 0;
    let mut cache_read_input_tokens: u64 = 0;
    for e in &entries {
        input_tokens += e.input_tokens.unwrap_or(0);
        output_tokens += e.output_tokens.unwrap_or(0);
        cache_creation_input_tokens += e.cache_creation_input_tokens.unwrap_or(0);
        cache_read_input_tokens += e.cache_read_input_tokens.unwrap_or(0);
    }
    let total_all_input = input_tokens + cache_creation_input_tokens + cache_read_input_tokens;
    let cache_hit_rate = if total_all_input > 0 {
        cache_read_input_tokens as f64 / total_all_input as f64
    } else {
        0.0
    };

    // Compute session duration dynamically from session_start.
    let session_duration_ms = state
        .session_start
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|start| {
            let now = chrono::Utc::now();
            (now - start.with_timezone(&chrono::Utc))
                .num_milliseconds()
                .max(0) as u64
        })
        .unwrap_or(0);

    Ok(json!({
        "name": state.config.name,
        "model": state.config.model,
        "system_prompt": state.config.system_prompt,
        "work_dir": state.config.work_dir,
        "max_turns": state.config.max_turns,
        "pid": state.pid,
        "session_id": state.session_id,
        "claude_session_id": state.session_id,
        "session_start": state.session_start,
        "session_cost_usd": state.session_cost,
        "session_turns": state.session_turns,
        "session_duration_ms": session_duration_ms,
        "lifetime_cost_usd": state.total_cost,
        "lifetime_turns": state.total_turns,
        "total_turns": state.total_turns,
        "total_cost": state.total_cost,
        "created_at": state.created_at,
        "status": state.status,
        "current_task": state.current_task,
        "parent": state.parent,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_input_tokens": cache_creation_input_tokens,
        "cache_read_input_tokens": cache_read_input_tokens,
        "cache_hit_rate": cache_hit_rate,
    }))
}

fn handle_task_list(
    params: &Value,
    task_store: &(dyn TaskRepository + Send + Sync),
) -> Result<Value> {
    let status_filter = params.get("status_filter").and_then(|v| v.as_str());
    let tasks = mcp_service::task_list(status_filter, task_store)?;
    Ok(json!(tasks))
}

fn handle_task_detail(
    params: &Value,
    task_store: &(dyn TaskRepository + Send + Sync),
) -> Result<Value> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'id' parameter"))?;
    let task = task_store.load(id)?;
    Ok(json!({
        "id": task.id,
        "description": task.description,
        "status": task.status.to_string(),
        "assignee": task.assignee,
        "created_by": task.created_by,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
        "result": task.result,
        "error": task.error,
        "cost_usd": task.cost_usd,
        "turns": task.turns,
        "sm_instance_id": task.sm_instance_id,
        "metadata": task.metadata,
    }))
}

fn handle_sm_list(
    params: &Value,
    sm_store: &(dyn StateMachineRepository + Send + Sync),
) -> Result<Value> {
    let model_filter = params.get("model").and_then(|v| v.as_str());
    let state_filter = params.get("state").and_then(|v| v.as_str());
    mcp_service::sm_query(None, model_filter, state_filter, sm_store)
}

fn handle_sm_detail(
    params: &Value,
    sm_store: &(dyn StateMachineRepository + Send + Sync),
) -> Result<Value> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'id' parameter"))?;
    mcp_service::sm_query(Some(id), None, None, sm_store)
}

fn handle_sm_models(user_config: Option<&UserConfig>) -> Result<Value> {
    let config = user_config.ok_or_else(|| anyhow::anyhow!("no user config loaded"))?;
    let models: Vec<Value> = config
        .models
        .iter()
        .map(|m| {
            json!({
                "name": m.name,
                "states": m.states,
                "transitions": m.transitions.iter().map(|t| {
                    json!({
                        "from": t.from,
                        "to": t.to,
                        "trigger": t.trigger,
                    })
                }).collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(json!(models))
}

async fn handle_usage_stats(params: &Value) -> Result<Value> {
    let period = params
        .get("period")
        .and_then(|v| v.as_str())
        .unwrap_or("7d");
    let agent = params.get("agent").and_then(|v| v.as_str());
    let stats = crate::app::commands::usage::compute_stats(period, agent)?;
    Ok(serde_json::to_value(stats)?)
}

fn handle_agent_turn_stats(params: &Value) -> Result<Value> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'name' parameter"))?;
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;

    let entries = crate::app::tasklog::read_logs(name, limit, None, None)?;

    let turns: Vec<Value> = entries
        .iter()
        .map(|e| {
            let input = e.input_tokens.unwrap_or(0);
            let output = e.output_tokens.unwrap_or(0);
            let cache_creation = e.cache_creation_input_tokens.unwrap_or(0);
            let cache_read = e.cache_read_input_tokens.unwrap_or(0);
            let total_input = input + cache_creation + cache_read;
            let cache_hit_rate = if total_input > 0 {
                cache_read as f64 / total_input as f64
            } else {
                0.0
            };
            json!({
                "timestamp": e.ts,
                "source": e.source,
                "task": e.task,
                "turns": e.turns,
                "cost_usd": e.cost,
                "duration_ms": e.duration_ms,
                "status": e.status,
                "input_tokens": input,
                "output_tokens": output,
                "cache_creation_input_tokens": cache_creation,
                "cache_read_input_tokens": cache_read,
                "cache_hit_rate": cache_hit_rate,
            })
        })
        .collect();

    Ok(json!(turns))
}

fn handle_schedule_list(user_config: Option<&UserConfig>) -> Result<Value> {
    let config = user_config.ok_or_else(|| anyhow::anyhow!("no user config loaded"))?;
    let schedules: Vec<Value> = config
        .schedules
        .iter()
        .enumerate()
        .map(|(i, s)| {
            json!({
                "index": i,
                "cron": s.cron,
                "target": s.target,
                "action": format!("{:?}", s.action),
                "timezone": s.timezone,
            })
        })
        .collect();
    Ok(json!(schedules))
}

fn handle_inbox_list(agent_name: &str, user_config: Option<&UserConfig>) -> Result<Value> {
    let inboxes = mcp_service::list_inboxes()?;
    let filtered: Vec<Value> = inboxes
        .into_iter()
        .filter(|entry| {
            entry
                .get("inbox")
                .and_then(|v| v.as_str())
                .map(|name| {
                    crate::app::mcp_tools::inbox_access_allowed(agent_name, name, user_config)
                })
                .unwrap_or(false)
        })
        .collect();
    Ok(json!(filtered))
}

fn handle_inbox_read(
    params: &Value,
    agent_name: &str,
    user_config: Option<&UserConfig>,
) -> Result<Value> {
    let inbox = params
        .get("inbox")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'inbox' parameter"))?;
    if !crate::app::mcp_tools::inbox_access_allowed(agent_name, inbox, user_config) {
        bail!(
            "inbox access denied: agent \"{}\" is not in allow-list for inbox \"{}\"",
            agent_name,
            inbox
        );
    }
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
    let since = params
        .get("since")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));
    let messages = mcp_service::read_inbox(inbox, limit, since)?;
    Ok(json!(messages))
}

fn handle_inbox_search(
    params: &Value,
    agent_name: &str,
    user_config: Option<&UserConfig>,
) -> Result<Value> {
    let query = params
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'query' parameter"))?;
    let inbox = params.get("inbox").and_then(|v| v.as_str());
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
    if let Some(name) = inbox
        && !crate::app::mcp_tools::inbox_access_allowed(agent_name, name, user_config)
    {
        bail!(
            "inbox access denied: agent \"{}\" is not in allow-list for inbox \"{}\"",
            agent_name,
            name
        );
    }
    let results = mcp_service::search_inbox(inbox, query, limit)?;
    Ok(json!(results))
}

async fn handle_bus_status(bus_socket: &str, reload_state: &ReloadState) -> Result<Value> {
    let clients = crate::app::serve::query_live_agents(bus_socket)
        .await
        .unwrap_or_default();
    let client_list: Vec<&str> = clients.iter().map(|s| s.as_str()).collect();
    let snap = reload_state.snapshot().await;
    Ok(json!({
        "socket": bus_socket,
        "clients": client_list,
        "last_reload_at": snap.last_reload_at.map(|t| t.to_rfc3339()),
        "last_reload_error": snap.last_reload_error,
    }))
}

async fn handle_room_list() -> Result<Value> {
    let serve_state = crate::config::ServeState::load()
        .ok_or_else(|| anyhow::anyhow!("no serve state found (deskd serve not running?)"))?;

    // If formal rooms are defined, use them. Otherwise fall back to treating each
    // top-level agent as a room (backward compatible).
    if !serve_state.rooms.is_empty() {
        let rooms: Vec<Value> = serve_state
            .rooms
            .iter()
            .map(|room| {
                let mut total_cost = 0.0;
                let mut active_count = 0;
                for agent_name in &room.agents {
                    if let Ok(state) = crate::app::agent::load_state(agent_name) {
                        total_cost += state.total_cost;
                        if state.status == "active" || state.status == "idle" {
                            active_count += 1;
                        }
                    }
                }
                json!({
                    "name": room.name,
                    "work_dir": room.work_dir,
                    "context": room.context,
                    "agent_count": room.agents.len(),
                    "active_agents": active_count,
                    "cost_usd": total_cost,
                })
            })
            .collect();
        return Ok(json!(rooms));
    }

    // Fallback: each top-level agent is a room.
    let mut rooms: Vec<Value> = Vec::new();
    for (name, agent_serve) in &serve_state.agents {
        let (status, cost_usd, _turns) = match crate::app::agent::load_state(name) {
            Ok(state) => (state.status, state.total_cost, state.total_turns),
            Err(_) => ("unknown".to_string(), 0.0, 0),
        };

        let agent_count = UserConfig::load(&agent_serve.config_path)
            .map(|cfg| cfg.agents.len())
            .unwrap_or(0);

        rooms.push(json!({
            "name": name,
            "work_dir": agent_serve.work_dir,
            "bus_socket": agent_serve.bus_socket,
            "status": status,
            "agent_count": agent_count,
            "cost_usd": cost_usd,
        }));
    }
    Ok(json!(rooms))
}

fn handle_room_children(params: &Value) -> Result<Value> {
    let room = params
        .get("room")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'room' parameter"))?;

    let serve_state = crate::config::ServeState::load()
        .ok_or_else(|| anyhow::anyhow!("no serve state found (deskd serve not running?)"))?;

    // If formal rooms are defined, look up children from the room definition.
    if let Some(room_def) = serve_state.rooms.iter().find(|r| r.name == room) {
        let children: Vec<Value> = room_def
            .agents
            .iter()
            .map(|agent_name| {
                let (status, model) = match crate::app::agent::load_state(agent_name) {
                    Ok(state) => (state.status, state.config.model),
                    Err(_) => ("configured".to_string(), String::new()),
                };
                json!({
                    "name": agent_name,
                    "model": model,
                    "status": status,
                })
            })
            .collect();
        return Ok(json!(children));
    }

    // Fallback: look up room as an agent name, list its sub-agents.
    let agent_serve = serve_state
        .agent(room)
        .ok_or_else(|| anyhow::anyhow!("room '{}' not found in serve state", room))?;

    let user_config = UserConfig::load(&agent_serve.config_path)?;

    let children: Vec<Value> = user_config
        .agents
        .iter()
        .map(|sub| {
            let status = crate::app::agent::load_state(&sub.name)
                .map(|s| s.status)
                .unwrap_or_else(|_| "configured".to_string());
            json!({
                "name": sub.name,
                "model": sub.model,
                "status": status,
            })
        })
        .collect();

    Ok(json!(children))
}

async fn handle_context_stats(params: &Value, bus_socket: &str) -> Result<Value> {
    let agent_filter = params.get("agent").and_then(|v| v.as_str());

    let live = crate::app::serve::query_live_agents(bus_socket)
        .await
        .unwrap_or_default();
    let agents = crate::app::agent::list().await?;

    let stats: Vec<Value> = agents
        .iter()
        .filter(|s| agent_filter.is_none_or(|f| s.config.name == f))
        .map(|state| {
            let is_live = live.contains(&state.config.name);
            json!({
                "agent": state.config.name,
                "context_tokens": null,
                "context_limit": null,
                "context_utilization": null,
                "session_turns": state.total_turns,
                "session_start": state.created_at,
                "session_cost_usd": state.total_cost,
                "last_turn_cost_usd": null,
                "cache_hit_rate": null,
                "status": if is_live { &state.status } else { "stopped" },
                "model": state.config.model,
            })
        })
        .collect();

    if let Some(name) = agent_filter {
        if let Some(stat) = stats.into_iter().next() {
            return Ok(stat);
        }
        bail!("agent '{}' not found", name);
    }

    Ok(json!(stats))
}

fn handle_agent_requests(
    params: &Value,
    task_store: &(dyn TaskRepository + Send + Sync),
) -> Result<Value> {
    let agent = params
        .get("agent")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'agent' parameter"))?;

    let all_tasks = task_store.list(None)?;

    let matching: Vec<Value> = all_tasks
        .iter()
        .filter(|t| t.assignee.as_deref() == Some(agent) || t.created_by == agent)
        .map(|t| {
            json!({
                "id": t.id,
                "description": t.description,
                "turns": t.turns,
                "cost_usd": t.cost_usd,
                "status": t.status.to_string(),
                "created_at": t.created_at,
            })
        })
        .collect();

    Ok(json!(matching))
}

fn handle_agent_config_list() -> Result<Value> {
    let serve_state = crate::config::ServeState::load()
        .ok_or_else(|| anyhow::anyhow!("no serve state found (deskd serve not running?)"))?;

    let mut configs: Vec<Value> = Vec::new();
    for (name, agent_serve) in &serve_state.agents {
        let user_config = match UserConfig::load(&agent_serve.config_path) {
            Ok(cfg) => cfg,
            Err(_) => continue,
        };

        configs.push(json!({
            "name": name,
            "model": user_config.model,
            "system_prompt": user_config.system_prompt,
            "max_turns": user_config.max_turns,
        }));

        // Also include sub-agents from this room's config.
        for sub in &user_config.agents {
            configs.push(json!({
                "name": sub.name,
                "model": sub.model,
                "system_prompt": sub.system_prompt,
                "max_turns": null,
            }));
        }
    }

    Ok(json!(configs))
}

fn handle_agent_messages(params: &Value) -> Result<Value> {
    let agent = params
        .get("agent")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'agent' parameter"))?;
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;

    let messages = crate::app::unified_inbox::read_messages(agent, limit, None)?;
    let result: Vec<Value> = messages
        .iter()
        .map(|m| {
            json!({
                "role": m.from,
                "text": m.text,
                "timestamp": m.ts,
            })
        })
        .collect();
    Ok(json!(result))
}

// ─── Mutation handlers ──────────────────────────────────────────────────────

async fn handle_send_message(params: &Value, bus_socket: &str, agent_name: &str) -> Result<Value> {
    let target = params
        .get("target")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'target' parameter"))?;

    // Support `text` shorthand: wraps as {"task": text} matching MCP send_message format.
    // Falls back to `payload` for callers needing arbitrary JSON.
    let payload = if let Some(text) = params.get("text").and_then(|v| v.as_str()) {
        json!({"task": text})
    } else {
        params
            .get("payload")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing 'text' or 'payload' parameter"))?
    };
    let fresh = params
        .get("fresh")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Use a one-shot bus connection to send the message.
    let sender = crate::app::bus::connect_bus(bus_socket).await?;
    sender
        .register(&format!("{}-api-send", agent_name), &[])
        .await?;

    let msg = crate::domain::message::Message {
        id: uuid::Uuid::new_v4().to_string(),
        source: API_CLIENT_NAME.to_string(),
        target: target.to_string(),
        payload,
        reply_to: None,
        metadata: crate::domain::message::Metadata { priority: 5, fresh },
    };
    sender.send(&msg).await?;

    Ok(json!({"sent": true, "id": msg.id}))
}

fn handle_task_create(
    params: &Value,
    task_store: &(dyn TaskRepository + Send + Sync),
    agent_name: &str,
) -> Result<Value> {
    let description = params
        .get("description")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'description' parameter"))?;
    let model = params
        .get("model")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let labels: Vec<String> = params
        .get("labels")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let metadata = params.get("metadata").cloned().unwrap_or(Value::Null);

    let created =
        mcp_service::task_create(description, model, labels, metadata, agent_name, task_store)?;
    Ok(json!({
        "id": created.id,
        "description": created.description,
    }))
}

fn handle_task_cancel(
    params: &Value,
    task_store: &(dyn TaskRepository + Send + Sync),
) -> Result<Value> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'id' parameter"))?;
    let cancelled = mcp_service::task_cancel(id, task_store)?;
    Ok(json!({"id": cancelled.id, "cancelled": true}))
}

async fn handle_sm_create(
    params: &Value,
    bus_socket: &str,
    sm_store: &(dyn StateMachineRepository + Send + Sync),
    user_config: Option<&UserConfig>,
    agent_name: &str,
) -> Result<Value> {
    let config = user_config.ok_or_else(|| anyhow::anyhow!("no user config loaded"))?;
    let model = params
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'model' parameter"))?;
    let title = params
        .get("title")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'title' parameter"))?;
    let body = params.get("body").and_then(|v| v.as_str()).unwrap_or("");
    let metadata = params.get("metadata").cloned().unwrap_or(Value::Null);

    let created = mcp_service::sm_create(
        model, title, body, metadata, agent_name, bus_socket, config, sm_store,
    )
    .await?;

    Ok(json!({
        "id": created.id,
        "model": created.model,
        "state": created.state,
        "assignee": created.assignee,
    }))
}

async fn handle_sm_move(
    params: &Value,
    bus_socket: &str,
    sm_store: &(dyn StateMachineRepository + Send + Sync),
    user_config: Option<&UserConfig>,
    agent_name: &str,
) -> Result<Value> {
    let config = user_config.ok_or_else(|| anyhow::anyhow!("no user config loaded"))?;
    let instance_id = params
        .get("instance_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'instance_id' parameter"))?;
    let to_state = params
        .get("to_state")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'to_state' parameter"))?;
    let note = params.get("note").and_then(|v| v.as_str());

    let moved = mcp_service::sm_move(
        instance_id,
        to_state,
        note,
        agent_name,
        bus_socket,
        config,
        sm_store,
    )
    .await?;

    Ok(json!({
        "id": moved.id,
        "state": moved.state,
        "model": moved.model,
    }))
}

fn handle_sm_cancel(
    params: &Value,
    sm_store: &(dyn StateMachineRepository + Send + Sync),
) -> Result<Value> {
    let instance_id = params
        .get("instance_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'instance_id' parameter"))?;
    sm_store.delete(instance_id)?;
    Ok(json!({"id": instance_id, "cancelled": true}))
}

fn handle_schedule_add(
    params: &Value,
    user_config: Option<&UserConfig>,
    agent_name: &str,
) -> Result<Value> {
    let _config = user_config.ok_or_else(|| anyhow::anyhow!("no user config loaded"))?;
    let cron = params
        .get("cron")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'cron' parameter"))?;
    let action = params
        .get("action")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'action' parameter"))?;
    let target = params
        .get("target")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'target' parameter"))?;

    // Validate cron expression.
    use std::str::FromStr;
    let _schedule = cron::Schedule::from_str(cron)
        .map_err(|e| anyhow::anyhow!("invalid cron expression: {}", e))?;

    // Persist by appending to deskd.yaml.
    let config_path = std::env::var("DESKD_AGENT_CONFIG")
        .unwrap_or_else(|_| format!("/home/{}/deskd.yaml", agent_name));

    let schedule_action = match action {
        "raw" => crate::config::ScheduleAction::Raw,
        "github_poll" => crate::config::ScheduleAction::GithubPoll,
        "shell" => crate::config::ScheduleAction::Shell,
        other => bail!("unknown schedule action: {}", other),
    };

    let schedule_config = params
        .get("config")
        .and_then(|v| serde_json::from_value::<serde_yaml::Value>(v.clone()).ok());

    let new_schedule = crate::config::ScheduleDef {
        cron: cron.to_string(),
        target: target.to_string(),
        action: schedule_action,
        config: schedule_config,
        timezone: None,
    };

    // Load, append, and save config.
    let mut cfg = UserConfig::load(&config_path).unwrap_or_default();
    let index = cfg.schedules.len();
    cfg.schedules.push(new_schedule);
    let yaml = serde_yaml::to_string(&cfg)?;
    std::fs::write(&config_path, yaml)?;

    info!(agent = %agent_name, index = index, cron = %cron, "schedule_add");

    Ok(json!({
        "index": index,
        "cron": cron,
        "target": target,
        "action": action,
    }))
}

fn handle_schedule_remove(
    params: &Value,
    user_config: Option<&UserConfig>,
    agent_name: &str,
) -> Result<Value> {
    let _config = user_config.ok_or_else(|| anyhow::anyhow!("no user config loaded"))?;
    let index = params
        .get("index")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("missing 'index' parameter"))? as usize;

    let config_path = std::env::var("DESKD_AGENT_CONFIG")
        .unwrap_or_else(|_| format!("/home/{}/deskd.yaml", agent_name));

    let mut cfg = UserConfig::load(&config_path).unwrap_or_default();

    if index >= cfg.schedules.len() {
        bail!(
            "schedule index {} out of range (have {} schedules)",
            index,
            cfg.schedules.len()
        );
    }

    let removed = cfg.schedules.remove(index);
    let yaml = serde_yaml::to_string(&cfg)?;
    std::fs::write(&config_path, yaml)?;

    info!(agent = %agent_name, index = index, cron = %removed.cron, "schedule_remove");

    Ok(json!({
        "index": index,
        "removed": true,
        "cron": removed.cron,
    }))
}

async fn handle_agent_restart(params: &Value, _bus_socket: &str, caller: &str) -> Result<Value> {
    let agent = params
        .get("agent")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'agent' parameter"))?;
    // When true, drop session_id so the next claude invocation does not
    // pass --resume (starts a brand-new conversation). When false (default),
    // session_id is preserved so the worker resumes the previous session.
    let fresh_session = params
        .get("fresh_session")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Shared kill-and-mutate path; lives in mcp_service so the CLI can call
    // the same logic without going through the bus (see commands/agent.rs
    // restart_one_agent for the recovery-scenario rationale).
    let outcome = mcp_service::perform_agent_restart(agent, fresh_session).await?;

    info!(
        agent = %agent,
        pid = outcome.previous_pid,
        fresh_session = outcome.fresh_session,
        caller = %caller,
        "agent_restart via bus API"
    );

    Ok(json!({
        "agent": agent,
        "restarted": true,
        "previous_pid": outcome.previous_pid,
        "fresh_session": outcome.fresh_session,
    }))
}

async fn handle_agent_compress(params: &Value, bus_socket: &str, caller: &str) -> Result<Value> {
    let agent = params
        .get("agent")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing 'agent' parameter"))?;

    // Send a summarization task to the agent before restarting.
    let sender = crate::app::bus::connect_bus(bus_socket).await?;
    sender
        .register(&format!("{}-compress", caller), &[])
        .await?;

    let summary_task = "Summarize the key decisions, artifacts, and context from this session \
        into a concise summary. Include: (1) what was accomplished, (2) any pending work, \
        (3) important decisions made. Write the summary to your inbox so the next session can read it.";

    let msg = crate::domain::message::Message {
        id: uuid::Uuid::new_v4().to_string(),
        source: API_CLIENT_NAME.to_string(),
        target: format!("agent:{}", agent),
        payload: json!({"task": summary_task}),
        reply_to: None,
        metadata: crate::domain::message::Metadata {
            priority: 10,
            fresh: false,
        },
    };
    sender.send(&msg).await?;

    info!(agent = %agent, caller = %caller, "agent_compress: summary task sent, will restart after processing");

    Ok(json!({
        "agent": agent,
        "compress_initiated": true,
        "summary_task_id": msg.id,
        "note": "Agent will process the summary task, then restart manually or via next agent_restart call",
    }))
}

/// `reload_config` RPC handler — implements the daemon side of `deskd reload` (#474).
///
/// Validates the YAML at the daemon's own `cfg_path` (the path the agent was
/// launched against), and on success fires `reload_state.trigger()` to wake
/// the config_reload watcher. The watcher then performs the actual changeset
/// classification, swap, and `last_reload_at` recording — see
/// `config_reload::watch_and_reload`.
///
/// The handler intentionally does NOT honour any caller-supplied path —
/// previously `params.path` would be validated but the watcher reloaded a
/// different file, producing a misleading `validated: true` response (see
/// #478 review). `--config <path>` on the CLI is now a client-side assertion
/// only: the CLI compares the operator's path against the agent's recorded
/// `config_path` in `ServeState` and fails before issuing the RPC on
/// mismatch. Any `params.path` sent by an older client is silently ignored.
///
/// On parse failure: records the error in `ReloadState`, returns the error
/// to the caller, and leaves the running daemon on its previous config. The
/// watcher is NOT triggered.
///
/// Note: "validated" means "parsed at RPC time," not "applied" — the watcher
/// re-parses the file on its own tick (TOCTOU window). Safety is preserved
/// on subsequent parse failure (old config kept), but the RPC's success
/// response is a soft guarantee.
///
/// Returns `{validated: true, path: "...", triggered: true}` on success,
/// where `path` is always the daemon's own `cfg_path`.
async fn handle_reload_config(
    _params: &Value,
    default_cfg_path: &str,
    agent_name: &str,
    reload_state: &ReloadState,
) -> Result<Value> {
    // The daemon always reloads its own config path. Any caller-supplied
    // path is ignored — `--config` is verified client-side in the CLI.
    let path = default_cfg_path.to_string();

    // Validate by parsing. If this fails, the daemon stays on its previous
    // config — we never trigger the watcher.
    match UserConfig::load(&path) {
        Ok(_) => {
            reload_state.trigger();
            info!(
                agent = %agent_name,
                path = %path,
                "reload_config RPC accepted, watcher triggered"
            );
            Ok(json!({
                "validated": true,
                "triggered": true,
                "path": path,
                "agent": agent_name,
            }))
        }
        Err(e) => {
            let err = format!("failed to parse {}: {}", path, e);
            reload_state.record_failure(err.clone()).await;
            warn!(
                agent = %agent_name,
                path = %path,
                error = %e,
                "reload_config RPC rejected — malformed config, daemon unchanged"
            );
            bail!("{}", err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dispatch_unknown_method() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let task_store =
            crate::app::task::TaskStore::new(std::path::PathBuf::from("/tmp/bus_api_test_tasks"));
        let sm_store = crate::app::statemachine::StateMachineStore::new(std::path::PathBuf::from(
            "/tmp/bus_api_test_sm",
        ));
        let reload_state = ReloadState::new();

        let result = rt.block_on(dispatch(
            "nonexistent_method",
            &json!({}),
            "/tmp/nonexistent.sock",
            &task_store,
            &sm_store,
            None,
            "test",
            "/tmp/nonexistent.yaml",
            &reload_state,
        ));

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown method"));
    }

    #[tokio::test]
    async fn test_reload_config_rejects_malformed_yaml() {
        // Malformed YAML → returns error, records failure, does NOT trigger.
        let tmp = tempfile::tempdir().unwrap();
        let cfg_path = tmp.path().join("deskd.yaml");
        std::fs::write(&cfg_path, "this is: : not valid yaml :::\n").unwrap();

        let reload_state = ReloadState::new();
        let result = handle_reload_config(
            &json!({}),
            cfg_path.to_str().unwrap(),
            "test-agent",
            &reload_state,
        )
        .await;

        assert!(result.is_err(), "malformed YAML should reject");
        let snap = reload_state.snapshot().await;
        assert!(snap.last_reload_error.is_some());
        assert!(
            snap.last_reload_at.is_none(),
            "failure must not set last_reload_at"
        );
    }

    #[tokio::test]
    async fn test_reload_config_accepts_valid_yaml_and_triggers() {
        // Valid YAML → returns success, fires the watcher notification.
        let tmp = tempfile::tempdir().unwrap();
        let cfg_path = tmp.path().join("deskd.yaml");
        std::fs::write(
            &cfg_path,
            "model: claude-sonnet-4-6\nsystem_prompt: hello\n",
        )
        .unwrap();

        let reload_state = ReloadState::new();
        let waiter = {
            let state = reload_state.clone();
            tokio::spawn(async move {
                state.wait_for_trigger().await;
            })
        };
        // Give the spawned waiter a chance to register on the Notify.
        tokio::task::yield_now().await;

        let result = handle_reload_config(
            &json!({}),
            cfg_path.to_str().unwrap(),
            "test-agent",
            &reload_state,
        )
        .await;

        assert!(result.is_ok(), "valid YAML should succeed");
        let v = result.unwrap();
        assert_eq!(v["validated"], json!(true));
        assert_eq!(v["triggered"], json!(true));

        // The watcher Notify must fire.
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("waiter should resolve after trigger")
            .expect("waiter task panicked");
    }

    #[tokio::test]
    async fn test_reload_config_ignores_path_param() {
        // params.path is intentionally ignored — the daemon always reloads
        // its own default_cfg_path (see #478 review). Caller-supplied paths
        // from older clients are silently dropped; verification happens in
        // the CLI before the RPC is issued.
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("daemon.yaml");
        std::fs::write(&real, "model: claude-sonnet-4-6\n").unwrap();

        let reload_state = ReloadState::new();
        let result = handle_reload_config(
            // Caller tries to override with a path that *does not exist*.
            &json!({"path": "/tmp/operator-supplied-bogus.yaml"}),
            // Daemon's actual path is `real` — valid.
            real.to_str().unwrap(),
            "test-agent",
            &reload_state,
        )
        .await;
        assert!(
            result.is_ok(),
            "handler must use default_cfg_path and ignore params.path; got {:?}",
            result.err()
        );
        let v = result.unwrap();
        assert_eq!(
            v["path"],
            json!(real.to_str().unwrap()),
            "response should report the daemon's own path, not the caller's override"
        );
    }

    #[test]
    fn test_task_list_empty() {
        let dir = std::path::PathBuf::from("/tmp/deskd_bus_api_test_tasks_empty");
        let _ = std::fs::remove_dir_all(&dir);
        let task_store = crate::app::task::TaskStore::new(dir);
        let result = handle_task_list(&json!({}), &task_store);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!([]));
    }

    #[test]
    fn test_task_detail_missing_id() {
        let dir = std::path::PathBuf::from("/tmp/deskd_bus_api_test_tasks_detail");
        let _ = std::fs::remove_dir_all(&dir);
        let task_store = crate::app::task::TaskStore::new(dir);
        let result = handle_task_detail(&json!({}), &task_store);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'id'"));
    }

    #[test]
    fn test_sm_models_no_config() {
        let result = handle_sm_models(None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no user config"));
    }

    #[test]
    fn test_schedule_list_no_config() {
        let result = handle_schedule_list(None);
        assert!(result.is_err());
    }

    #[test]
    fn test_inbox_read_missing_inbox() {
        let result = handle_inbox_read(&json!({}), "test-agent", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'inbox'"));
    }

    #[test]
    fn test_inbox_search_missing_query() {
        let result = handle_inbox_search(&json!({}), "test-agent", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'query'"));
    }

    #[test]
    fn test_agent_messages_missing_agent() {
        let result = handle_agent_messages(&json!({}));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'agent'"));
    }

    #[test]
    fn test_agent_messages_returns_array() {
        // With a non-existent agent we get an empty array (no inbox files).
        let result = handle_agent_messages(&json!({"agent": "nonexistent-agent-xyz"}));
        assert!(result.is_ok());
        let val = result.unwrap();
        assert!(val.is_array());
        assert_eq!(val.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_agent_turn_stats_missing_name() {
        let result = handle_agent_turn_stats(&json!({}));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing 'name'"));
    }

    #[test]
    fn test_agent_turn_stats_empty() {
        let result = handle_agent_turn_stats(&json!({"name": "nonexistent-agent-xyz"}));
        assert!(result.is_ok());
        let val = result.unwrap();
        assert!(val.is_array());
        assert_eq!(val.as_array().unwrap().len(), 0);
    }
}
