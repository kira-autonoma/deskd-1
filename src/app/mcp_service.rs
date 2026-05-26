//! Business logic extracted from MCP handlers.
//!
//! Each public function here performs the core operation — argument validation,
//! store access, side-effects — returning a domain result. The MCP layer
//! (`mcp.rs`) stays thin: parse JSON args, call into this module, format the
//! JSON-RPC response.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::info;

use crate::app::statemachine;
use crate::app::unified_inbox;
use crate::app::workflow;
use crate::config::UserConfig;
use crate::domain::events::DomainEvent;
use crate::ports::bus::MessageBus;
use crate::ports::store::{StateMachineRepository, TaskRepository};

// ─── Reminder ────────────────────────────────────────────────────────────────

/// How a reminder reschedules itself after firing.
///
/// `OneShot` preserves legacy behaviour — the reminder is deleted after fire.
/// `Interval` and `Cron` re-arm the reminder; the source string lives on the
/// `RemindDef` row so deskd-restart re-parses it.
#[derive(Debug, Clone)]
pub enum ScheduleKind {
    OneShot,
    Interval(std::time::Duration),
    Cron(Box<cron::Schedule>),
}

impl ScheduleKind {
    /// Stable label used in MCP `list_reminders` output.
    pub fn label(&self) -> &'static str {
        match self {
            ScheduleKind::OneShot => "one_shot",
            ScheduleKind::Interval(_) => "interval",
            ScheduleKind::Cron(_) => "cron",
        }
    }

    /// Compute the next fire time strictly after `now` for a recurring kind.
    /// `OneShot` returns `None` — callers delete the reminder instead.
    ///
    /// For `Interval`, jumps forward by whole intervals from `last_fire` so
    /// missed firings collapse to one ("fire-once-and-move-on" restart-storm
    /// policy from #455).
    pub fn next_after(
        &self,
        last_fire: chrono::DateTime<chrono::Utc>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            ScheduleKind::OneShot => None,
            ScheduleKind::Interval(dur) => {
                let interval = chrono::Duration::from_std(*dur).ok()?;
                if interval <= chrono::Duration::zero() {
                    return None;
                }
                let mut next = last_fire + interval;
                if next <= now {
                    // Collapse missed firings to a single re-arm.
                    let elapsed = (now - last_fire).num_milliseconds().max(0);
                    let step = interval.num_milliseconds().max(1);
                    let skips = (elapsed / step) + 1;
                    next = last_fire + chrono::Duration::milliseconds(skips * step);
                }
                Some(next)
            }
            ScheduleKind::Cron(schedule) => {
                // cron::Schedule::after returns the next occurrences strictly
                // after the given anchor. We want strictly after `now` so a
                // deskd restart that lost time still re-arms to the future.
                schedule.after(&now).next()
            }
        }
    }
}

/// Parse the optional recurring fields on a reminder into a `ScheduleKind`,
/// returning an MCP-shaped error if validation fails (#455).
///
/// Rules:
/// - `interval` and `cron_expression` are mutually exclusive.
/// - `interval` parses via the shared duration parser; must be >= 60 seconds.
/// - `cron_expression` must be 5-field; 6-field (with seconds) is rejected.
pub fn parse_schedule_kind(
    interval: Option<&str>,
    cron_expression: Option<&str>,
) -> Result<ScheduleKind> {
    match (interval, cron_expression) {
        (Some(_), Some(_)) => {
            bail!("interval and cron_expression are mutually exclusive — pass only one")
        }
        (Some(s), None) => {
            let secs = crate::app::commands::parse_duration_secs(s)
                .with_context(|| format!("invalid interval: {:?}", s))?;
            if secs < 60 {
                bail!(
                    "interval must be >= 1m (got {:?} = {}s); deskd does not support sub-minute reminders",
                    s,
                    secs
                );
            }
            Ok(ScheduleKind::Interval(std::time::Duration::from_secs(secs)))
        }
        (None, Some(expr)) => {
            // Reject 6-field cron (includes seconds). Field count is counted
            // by whitespace-separated tokens.
            let fields = expr.split_whitespace().count();
            if fields != 5 {
                bail!(
                    "cron_expression must be 5 fields (minute hour day month weekday), got {} field(s) in {:?}",
                    fields,
                    expr
                );
            }
            // The `cron` crate expects a leading seconds field. Prepend "0"
            // so we keep cron's minute-floor semantics.
            let normalized = format!("0 {}", expr);
            let schedule = cron::Schedule::from_str(&normalized)
                .with_context(|| format!("invalid cron_expression {:?}", expr))?;
            Ok(ScheduleKind::Cron(Box::new(schedule)))
        }
        (None, None) => Ok(ScheduleKind::OneShot),
    }
}

/// Result of creating a reminder.
#[derive(Debug)]
pub struct ReminderCreated {
    pub target: String,
    pub fire_at: String,
    pub delay_minutes: f64,
    pub schedule_kind: &'static str,
}

/// Create a one-shot reminder persisted to disk under `{work_dir}/.deskd/reminders/`.
pub fn create_reminder(
    work_dir: &Path,
    target: &str,
    message: &str,
    delay_minutes: f64,
) -> Result<ReminderCreated> {
    let fire_at =
        chrono::Utc::now() + chrono::Duration::seconds((delay_minutes * 60.0).round() as i64);
    create_reminder_at(work_dir, target, message, fire_at)
}

/// Create a one-shot reminder at a specific absolute time.
pub fn create_reminder_at(
    work_dir: &Path,
    target: &str,
    message: &str,
    fire_at: chrono::DateTime<chrono::Utc>,
) -> Result<ReminderCreated> {
    create_reminder_full(work_dir, target, message, Some(fire_at), None, None)
}

/// Create a reminder with optional recurring schedule.
///
/// When `interval` or `cron_expression` is set, the reminder reschedules
/// itself after each fire. If `fire_at` is `None` and a recurring kind is
/// supplied, the first fire time is computed from "now":
///   - `interval` → `now + interval`
///   - `cron_expression` → next cron occurrence after now
///
/// `interval` and `cron_expression` are mutually exclusive (validated by
/// `parse_schedule_kind`).
///
/// The file is written under `{work_dir}/.deskd/reminders/` — same convention
/// as `bus.sock`, and the location the firing scanner reads from (#467).
pub fn create_reminder_full(
    work_dir: &Path,
    target: &str,
    message: &str,
    fire_at: Option<chrono::DateTime<chrono::Utc>>,
    interval: Option<&str>,
    cron_expression: Option<&str>,
) -> Result<ReminderCreated> {
    let kind = parse_schedule_kind(interval, cron_expression)?;
    let now = chrono::Utc::now();
    let resolved_fire_at = match (fire_at, &kind) {
        (Some(t), _) => t,
        (None, ScheduleKind::OneShot) => {
            bail!("create_reminder requires a fire time (at/in/delay_minutes) when not recurring")
        }
        (None, ScheduleKind::Interval(d)) => {
            now + chrono::Duration::from_std(*d).unwrap_or(chrono::Duration::zero())
        }
        (None, ScheduleKind::Cron(schedule)) => schedule.after(&now).next().ok_or_else(|| {
            anyhow::anyhow!("cron_expression has no upcoming occurrences after now")
        })?,
    };

    let delay_minutes = (resolved_fire_at - now).num_seconds() as f64 / 60.0;

    let remind = crate::config::RemindDef {
        at: resolved_fire_at.to_rfc3339(),
        target: target.to_string(),
        message: message.to_string(),
        interval: interval.map(|s| s.to_string()),
        cron_expression: cron_expression.map(|s| s.to_string()),
    };

    let dir = crate::config::reminders_dir_for(work_dir);
    let filename = format!("{}.json", uuid::Uuid::new_v4());
    let path = dir.join(&filename);

    let json = serde_json::to_string_pretty(&remind).context("failed to serialize reminder")?;
    std::fs::write(&path, json)
        .with_context(|| format!("failed to write reminder file: {}", path.display()))?;

    info!(target = %target, at = %resolved_fire_at, kind = kind.label(), work_dir = %work_dir.display(), "create_reminder");

    Ok(ReminderCreated {
        target: target.to_string(),
        fire_at: resolved_fire_at.to_rfc3339(),
        delay_minutes,
        schedule_kind: kind.label(),
    })
}

/// One pending reminder record loaded from disk.
struct LoadedReminder {
    id: String,
    at: chrono::DateTime<chrono::Utc>,
    def: crate::config::RemindDef,
}

/// Read all reminder files from disk, parsing each into `LoadedReminder`.
/// Files that fail to parse are skipped silently (the scheduler does the same).
fn load_all_reminders(work_dir: &Path) -> Result<Vec<LoadedReminder>> {
    let dir = crate::config::reminders_dir_for(work_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(it) => it,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("read_dir {}", dir.display())),
    };

    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(def) = serde_json::from_str::<crate::config::RemindDef>(&content) else {
            continue;
        };
        let Ok(at) = chrono::DateTime::parse_from_rfc3339(&def.at) else {
            continue;
        };
        out.push(LoadedReminder {
            id: stem.to_string(),
            at: at.with_timezone(&chrono::Utc),
            def,
        });
    }
    Ok(out)
}

/// Truncate a message to a preview of at most `max_chars` characters,
/// counted by Unicode scalar values to avoid splitting multi-byte chars.
/// Newlines collapse to spaces so previews stay single-line.
fn message_preview(message: &str, max_chars: usize) -> String {
    let collapsed: String = message
        .chars()
        .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
        .collect();
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    let truncated: String = collapsed.chars().take(max_chars).collect();
    format!("{}…", truncated)
}

/// List pending reminders, sorted by fire time ascending.
///
/// Filters (all optional, AND-combined):
/// - `target_substr` — substring match against the reminder's `target` field
/// - `before` — only reminders firing strictly before this UTC time
/// - `after` — only reminders firing strictly after this UTC time
/// - `limit` — cap returned entries (default 50 at the tool layer)
pub fn list_reminders(
    work_dir: &Path,
    target_substr: Option<&str>,
    before: Option<chrono::DateTime<chrono::Utc>>,
    after: Option<chrono::DateTime<chrono::Utc>>,
    limit: usize,
) -> Result<Vec<Value>> {
    let mut all = load_all_reminders(work_dir)?;
    all.sort_by_key(|r| r.at);

    let filtered = all.into_iter().filter(|r| {
        if let Some(s) = target_substr
            && !r.def.target.contains(s)
        {
            return false;
        }
        if let Some(b) = before
            && r.at >= b
        {
            return false;
        }
        if let Some(a) = after
            && r.at <= a
        {
            return false;
        }
        true
    });

    Ok(filtered
        .take(limit)
        .map(|r| {
            let kind_label = match parse_schedule_kind(
                r.def.interval.as_deref(),
                r.def.cron_expression.as_deref(),
            ) {
                Ok(k) => k.label(),
                Err(_) => "one_shot",
            };
            json!({
                "id": r.id,
                "at": r.def.at,
                "next_fire_at": r.def.at,
                "target": r.def.target,
                "message_preview": message_preview(&r.def.message, 120),
                "schedule_kind": kind_label,
                "interval": r.def.interval,
                "cron_expression": r.def.cron_expression,
            })
        })
        .collect())
}

/// Validate a reminder id — must be a simple filename component (no slashes,
/// no traversal, no leading dot).
fn validate_reminder_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.contains('/')
        || id.contains('\\')
        || id.contains("..")
        || id.starts_with('.')
    {
        bail!("invalid reminder id: {:?}", id);
    }
    Ok(())
}

/// Path on disk for a given reminder id under `work_dir`.
fn reminder_path(work_dir: &Path, id: &str) -> Result<PathBuf> {
    validate_reminder_id(id)?;
    Ok(crate::config::reminders_dir_for(work_dir).join(format!("{}.json", id)))
}

/// Read a reminder file and return its full record as JSON.
pub fn get_reminder(work_dir: &Path, id: &str) -> Result<Value> {
    let path = reminder_path(work_dir, id)?;
    let content =
        std::fs::read_to_string(&path).with_context(|| format!("reminder not found: {}", id))?;
    let def: crate::config::RemindDef =
        serde_json::from_str(&content).context("failed to parse reminder file")?;
    let kind_label =
        match parse_schedule_kind(def.interval.as_deref(), def.cron_expression.as_deref()) {
            Ok(k) => k.label(),
            Err(_) => "one_shot",
        };
    Ok(json!({
        "id": id,
        "at": def.at,
        "next_fire_at": def.at,
        "target": def.target,
        "message": def.message,
        "schedule_kind": kind_label,
        "interval": def.interval,
        "cron_expression": def.cron_expression,
    }))
}

/// Delete a reminder file. Idempotent: missing file returns `cancelled: false`.
pub fn cancel_reminder(work_dir: &Path, id: &str) -> Result<Value> {
    let path = reminder_path(work_dir, id)?;
    match std::fs::remove_file(&path) {
        Ok(()) => {
            info!(id = %id, "cancel_reminder");
            Ok(json!({ "cancelled": true, "id": id }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(json!({ "cancelled": false, "id": id }))
        }
        Err(e) => {
            Err(e).with_context(|| format!("failed to delete reminder file: {}", path.display()))
        }
    }
}

/// Update one or more fields on an existing reminder.
///
/// Atomicity: write to `<id>.json.tmp` then `rename` to `<id>.json` so the
/// scheduler tick (which reads files every 10s) never observes a partial write.
pub fn update_reminder(
    work_dir: &Path,
    id: &str,
    new_at: Option<chrono::DateTime<chrono::Utc>>,
    new_target: Option<&str>,
    new_message: Option<&str>,
) -> Result<Value> {
    let path = reminder_path(work_dir, id)?;
    let content =
        std::fs::read_to_string(&path).with_context(|| format!("reminder not found: {}", id))?;
    let mut def: crate::config::RemindDef =
        serde_json::from_str(&content).context("failed to parse reminder file")?;

    if let Some(at) = new_at {
        def.at = at.to_rfc3339();
    }
    if let Some(t) = new_target {
        def.target = t.to_string();
    }
    if let Some(m) = new_message {
        def.message = m.to_string();
    }

    let json_text = serde_json::to_string_pretty(&def).context("failed to serialize reminder")?;
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, json_text)
        .with_context(|| format!("failed to write tmp file: {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &path).with_context(|| {
        format!(
            "failed to rename {} -> {}",
            tmp_path.display(),
            path.display()
        )
    })?;

    info!(id = %id, "update_reminder");
    Ok(json!({
        "id": id,
        "at": def.at,
        "target": def.target,
        "message": def.message,
    }))
}

// ─── Unified inbox ───────────────────────────────────────────────────────────

/// List all inboxes with message counts.
pub fn list_inboxes() -> Result<Vec<Value>> {
    let inboxes = unified_inbox::list_inboxes().context("failed to list inboxes")?;
    Ok(inboxes
        .iter()
        .map(|(name, count)| {
            json!({
                "inbox": name,
                "messages": count,
            })
        })
        .collect())
}

/// Read messages from a unified inbox, returning JSON-ready values.
pub fn read_inbox(
    inbox: &str,
    limit: usize,
    since: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<Vec<Value>> {
    let messages =
        unified_inbox::read_messages(inbox, limit, since).context("failed to read inbox")?;
    Ok(messages
        .iter()
        .map(|m| {
            json!({
                "ts": m.ts.to_rfc3339(),
                "source": m.source,
                "from": m.from,
                "text": m.text,
                "metadata": m.metadata,
            })
        })
        .collect())
}

/// Search messages across inboxes, returning JSON-ready values.
pub fn search_inbox(inbox: Option<&str>, query: &str, limit: usize) -> Result<Vec<Value>> {
    let results =
        unified_inbox::search_messages(inbox, query, limit).context("failed to search inbox")?;
    Ok(results
        .iter()
        .map(|m| {
            json!({
                "ts": m.ts.to_rfc3339(),
                "source": m.source,
                "from": m.from,
                "text": m.text,
                "metadata": m.metadata,
            })
        })
        .collect())
}

// ─── Graph execution ─────────────────────────────────────────────────────────

/// Resolve graph file path to absolute, load YAML, execute, and return summary.
pub async fn run_graph(file: &str, work_dir: Option<&str>, vars: Option<&Value>) -> Result<String> {
    let file_path = Path::new(file);
    let abs_path = if file_path.is_absolute() {
        file_path.to_path_buf()
    } else {
        let cwd = std::env::var("PWD").unwrap_or_else(|_| ".".to_string());
        Path::new(&cwd).join(file_path)
    };

    let effective_work_dir = if let Some(wd) = work_dir {
        PathBuf::from(wd)
    } else {
        abs_path.parent().unwrap_or(Path::new(".")).to_path_buf()
    };

    let yaml = std::fs::read_to_string(&abs_path)
        .with_context(|| format!("failed to read graph file: {}", abs_path.display()))?;
    let graph_def: crate::app::graph::GraphDef = serde_yaml::from_str(&yaml)
        .with_context(|| format!("failed to parse graph YAML: {}", abs_path.display()))?;

    let inputs: Option<HashMap<String, String>> = vars.and_then(|v| v.as_object()).map(|obj| {
        obj.iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect()
    });

    info!(graph = %graph_def.graph, steps = graph_def.steps.len(), "run_graph");

    let ctx = crate::app::graph::execute(&graph_def, &effective_work_dir, None, inputs)
        .await
        .with_context(|| format!("graph execution failed: {}", graph_def.graph))?;

    // Build summary.
    let mut summary = format!(
        "Graph '{}' completed: {} steps executed\n",
        graph_def.graph,
        ctx.results.len()
    );

    for step in &graph_def.steps {
        if let Some(result) = ctx.results.get(&step.id) {
            let status = if result.skipped { "SKIP" } else { "DONE" };
            summary.push_str(&format!(
                "  [{status}] {} ({}ms)\n",
                result.id, result.duration_ms
            ));

            for tr in &result.tool_results {
                if !tr.stdout.is_empty() && !tr.skipped {
                    let output = if tr.stdout.len() > 500 {
                        format!("{}...", &tr.stdout[..500])
                    } else {
                        tr.stdout.clone()
                    };
                    summary.push_str(&format!("    {}: {}\n", tr.tool, output.trim()));
                }
                if !tr.stderr.is_empty() && tr.exit_code != 0 {
                    let err_output = if tr.stderr.len() > 200 {
                        format!("{}...", &tr.stderr[..200])
                    } else {
                        tr.stderr.clone()
                    };
                    summary.push_str(&format!("    {} stderr: {}\n", tr.tool, err_output.trim()));
                }
            }

            if let Some(ref llm_out) = result.llm_output {
                let display = if llm_out.len() > 500 {
                    format!("{}...", &llm_out[..500])
                } else {
                    llm_out.clone()
                };
                summary.push_str(&format!("    LLM: {}\n", display.trim()));
            }
        }
    }

    if !ctx.variables.is_empty() {
        summary.push_str("\nVariables:\n");
        for (k, v) in &ctx.variables {
            let display = if v.len() > 200 {
                format!("{}...", &v[..200])
            } else {
                v.clone()
            };
            summary.push_str(&format!("  {}: {}\n", k, display));
        }
    }

    Ok(summary)
}

// ─── Task queue ──────────────────────────────────────────────────────────────

/// Result of creating a task.
pub struct TaskCreated {
    pub id: String,
    pub description: String,
}

/// Create a task in the pull-based queue.
pub fn task_create(
    description: &str,
    model: Option<String>,
    labels: Vec<String>,
    metadata: Value,
    created_by: &str,
    task_store: &dyn TaskRepository,
) -> Result<TaskCreated> {
    let criteria = crate::app::task::TaskCriteria { model, labels };
    let task = if metadata.is_null() {
        task_store.create(description, criteria, created_by)?
    } else {
        task_store.create_with_metadata(description, criteria, created_by, metadata)?
    };

    info!(agent = %created_by, task_id = %task.id, "task_create");

    Ok(TaskCreated {
        id: task.id,
        description: task.description,
    })
}

/// Parse a task status string into the domain enum.
fn parse_task_status(s: &str) -> Option<crate::app::task::TaskStatus> {
    match s {
        "pending" => Some(crate::app::task::TaskStatus::Pending),
        "active" => Some(crate::app::task::TaskStatus::Active),
        "done" => Some(crate::app::task::TaskStatus::Done),
        "failed" => Some(crate::app::task::TaskStatus::Failed),
        "cancelled" => Some(crate::app::task::TaskStatus::Cancelled),
        "dead_letter" => Some(crate::app::task::TaskStatus::DeadLetter),
        _ => None,
    }
}

/// List tasks, optionally filtered by status string.
pub fn task_list(
    status_filter: Option<&str>,
    task_store: &dyn TaskRepository,
) -> Result<Vec<Value>> {
    let filter = status_filter.and_then(parse_task_status);
    let tasks = task_store.list(filter)?;

    Ok(tasks
        .iter()
        .map(|t| {
            json!({
                "id": t.id,
                "description": t.description,
                "status": t.status.to_string(),
                "assignee": t.assignee,
                "created_by": t.created_by,
                "created_at": t.created_at,
                "sm_instance_id": t.sm_instance_id,
            })
        })
        .collect())
}

/// Cancel a pending task by ID.
pub struct TaskCancelled {
    pub id: String,
}

pub fn task_cancel(id: &str, task_store: &dyn TaskRepository) -> Result<TaskCancelled> {
    let task = task_store.cancel(id)?;
    info!(task_id = %task.id, "task_cancel");
    Ok(TaskCancelled { id: task.id })
}

// ─── State machine ───────────────────────────────────────────────────────────

/// Result of creating an SM instance.
pub struct SmCreated {
    pub id: String,
    pub model: String,
    pub state: String,
    pub assignee: String,
}

/// Create a new state machine instance. If the initial state has an assignee,
/// dispatches the first task to the bus.
#[allow(clippy::too_many_arguments)]
pub async fn sm_create(
    model_name: &str,
    title: &str,
    body: &str,
    metadata: Value,
    agent_name: &str,
    bus_socket: &str,
    user_config: &UserConfig,
    sm_store: &dyn StateMachineRepository,
) -> Result<SmCreated> {
    let model: statemachine::ModelDef = user_config
        .models
        .iter()
        .find(|m| m.name == model_name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("model '{}' not found", model_name))?
        .try_into()
        .map_err(|e: String| anyhow::anyhow!("{e}"))?;

    let mut inst = sm_store.create(&model, title, body, agent_name)?;
    if !metadata.is_null() {
        inst.metadata = metadata;
        sm_store.save(&inst)?;
    }
    info!(agent = %agent_name, instance = %inst.id, model = %model_name, "sm_create");

    // Emit InstanceCreated event.
    if let Ok(bus) = crate::app::bus::connect_bus(bus_socket).await {
        let _ = bus
            .register(&format!("{}-event-pub", agent_name), &[])
            .await;
        let _ = workflow::publish_event(
            &bus,
            agent_name,
            &DomainEvent::InstanceCreated {
                instance_id: inst.id.clone(),
                model: model_name.to_string(),
                title: title.to_string(),
                created_by: agent_name.to_string(),
            },
        )
        .await;
    }

    // If the initial state has an assignee, dispatch the first task via the bus.
    if !inst.assignee.is_empty() {
        dispatch_sm_task(bus_socket, agent_name, &inst).await?;
    }

    Ok(SmCreated {
        id: inst.id,
        model: inst.model,
        state: inst.state,
        assignee: inst.assignee,
    })
}

/// Dispatch an SM task to the bus for the current assignee.
async fn dispatch_sm_task(
    bus_socket: &str,
    agent_name: &str,
    inst: &statemachine::Instance,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;
    use uuid::Uuid;

    let task_text = format!(
        "---\n## Task: {}\n\n{}\n\n---\n## Metadata\ninstance_id: {}\nmodel: {}\nstate: {}",
        inst.title, inst.body, inst.id, inst.model, inst.state
    );

    let mut stream = UnixStream::connect(bus_socket)
        .await
        .with_context(|| format!("failed to connect to bus at {}", bus_socket))?;

    let reg = serde_json::json!({
        "type": "register",
        "name": format!("{}-mcp-sm", agent_name),
        "subscriptions": []
    });
    let mut line = serde_json::to_string(&reg)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;

    let msg = serde_json::json!({
        "type": "message",
        "id": Uuid::new_v4().to_string(),
        "source": "workflow-engine",
        "target": &inst.assignee,
        "payload": {
            "task": task_text,
            "sm_instance_id": inst.id,
        },
        "reply_to": format!("sm:{}", inst.id),
        "metadata": {"priority": 5u8},
    });
    let mut msg_line = serde_json::to_string(&msg)?;
    msg_line.push('\n');
    stream.write_all(msg_line.as_bytes()).await?;
    stream.flush().await?;
    info!(instance = %inst.id, assignee = %inst.assignee, "dispatched initial task");

    Ok(())
}

/// Result of moving an SM instance.
pub struct SmMoved {
    pub id: String,
    pub state: String,
    pub model: String,
}

/// Move a state machine instance to a new state and notify the workflow engine.
pub async fn sm_move(
    id: &str,
    state: &str,
    note: Option<&str>,
    agent_name: &str,
    bus_socket: &str,
    user_config: &UserConfig,
    sm_store: &dyn StateMachineRepository,
) -> Result<SmMoved> {
    let mut inst = sm_store.load(id)?;
    let model: statemachine::ModelDef = user_config
        .models
        .iter()
        .find(|m| m.name == inst.model)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("model '{}' not found in config", inst.model))?
        .try_into()
        .map_err(|e: String| anyhow::anyhow!("{e}"))?;

    let from = inst.state.clone();
    sm_store.move_to(&mut inst, &model, state, agent_name, note, None, None)?;
    info!(agent = %agent_name, instance = %id, from = %from, to = %state, "sm_move");

    // Emit TransitionApplied event and notify workflow engine via one-shot bus.
    if let Ok(bus) = crate::app::bus::connect_bus(bus_socket).await {
        let _ = bus
            .register(&format!("{}-event-pub", agent_name), &[])
            .await;
        let _ = workflow::publish_event(
            &bus,
            agent_name,
            &DomainEvent::TransitionApplied {
                instance_id: id.to_string(),
                from: from.clone(),
                to: state.to_string(),
                trigger: agent_name.to_string(),
            },
        )
        .await;

        // Notify workflow engine to dispatch if the new state has an assignee.
        if !inst.assignee.is_empty()
            && !statemachine::is_terminal(&model, &inst)
            && let Err(e) = crate::app::workflow::notify_moved(&bus, id, agent_name).await
        {
            tracing::warn!(agent = %agent_name, instance = %id, error = %e, "failed to notify workflow engine after sm_move");
        }
    }

    Ok(SmMoved {
        id: id.to_string(),
        state: inst.state,
        model: inst.model,
    })
}

/// Query SM instances — either by ID or with optional model/state filters.
pub fn sm_query(
    id: Option<&str>,
    model_filter: Option<&str>,
    state_filter: Option<&str>,
    sm_store: &dyn StateMachineRepository,
) -> Result<Value> {
    if let Some(id) = id {
        let inst = sm_store.load(id)?;
        let dto: crate::app::statemachine::StoredInstance = (&inst).into();
        return Ok(serde_json::to_value(&dto)?);
    }

    let mut instances = sm_store.list_all()?;
    if let Some(mf) = model_filter {
        instances.retain(|i| i.model == mf);
    }
    if let Some(sf) = state_filter {
        instances.retain(|i| i.state == sf);
    }

    let summary: Vec<Value> = instances
        .iter()
        .map(|i| {
            json!({
                "id": i.id,
                "model": i.model,
                "title": i.title,
                "state": i.state,
                "assignee": i.assignee,
                "updated_at": i.updated_at,
            })
        })
        .collect();

    Ok(json!(summary))
}

// ─── Agent listing ───────────────────────────────────────────────────────────

/// Build a JSON summary for a sub-agent, given its name and worker handle status.
pub fn build_agent_summary(name: &str, is_finished: bool) -> Value {
    let (model, turns, cost_usd, agent_status) = match crate::app::agent::load_state(name) {
        Ok(state) => {
            let live = if is_finished {
                std::collections::HashSet::new()
            } else {
                let mut s = std::collections::HashSet::new();
                s.insert(name.to_string());
                s
            };
            let domain = crate::app::agent::to_domain_agent(&state, &live);
            (
                state.config.model.clone(),
                state.total_turns,
                state.total_cost,
                domain.status.to_string(),
            )
        }
        Err(_) => {
            let status = if is_finished { "finished" } else { "unknown" };
            ("unknown".to_string(), 0, 0.0, status.to_string())
        }
    };

    json!({
        "name": name,
        "model": model,
        "status": agent_status,
        "turns": turns,
        "cost_usd": cost_usd,
    })
}

/// Validate that a target agent is a sub-agent of the caller before removal.
pub fn validate_remove_agent(name: &str, caller: &str) -> Result<crate::app::agent::AgentState> {
    let state = crate::app::agent::load_state(name)
        .with_context(|| format!("agent '{}' not found", name))?;

    match &state.parent {
        Some(parent) if parent == caller => Ok(state),
        _ => bail!(
            "agent '{}' is not a sub-agent of '{}' — removal denied",
            name,
            caller
        ),
    }
}

/// Gracefully stop a sub-agent process (SIGTERM, then SIGKILL after timeout).
pub async fn stop_agent_process(name: &str, pid: u32) {
    if pid == 0 {
        return;
    }

    let _ = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status();
    info!(agent = %name, pid = pid, "sent SIGTERM, waiting for exit");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !alive {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(agent = %name, pid = pid, "process did not exit in 30s, force killing");
            let _ = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .status();
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Outcome of `perform_agent_restart` — the previous PID and whether the
/// session was reset.
pub struct RestartOutcome {
    /// PID of the worker child that was running before the kill (0 if none).
    pub previous_pid: u32,
    /// Whether session_id / session_cost / session_turns / session_start were
    /// cleared as part of this restart.
    pub fresh_session: bool,
}

/// Perform the shared "kill worker + mutate state file" portion of an agent
/// restart. Used by both the bus-API handler (`agent_restart`) and the CLI
/// (`deskd agent restart`) so the state-file mutation logic lives in one
/// place.
///
/// Steps:
/// 1. Load current state, capture `pid`.
/// 2. SIGTERM the worker child if a live PID is present (no-op for pid=0 or
///    a pid whose `/proc/<pid>` no longer exists — we still succeed so a
///    restart on an already-stopped agent is a no-panic no-op).
/// 3. Re-load state, optionally clear the session fields (`fresh_session`),
///    set `status = "restarting"`, save.
///
/// Does NOT poll for `idle` — callers that need to wait for the supervisor
/// to respawn the worker do so themselves.
pub async fn perform_agent_restart(name: &str, fresh_session: bool) -> Result<RestartOutcome> {
    perform_agent_restart_inner(name, fresh_session, None).await
}

/// Like `perform_agent_restart`, but reads/writes the state file under an
/// explicit home directory (`{home}/.deskd/agents/{name}.yaml`) instead of
/// `$HOME`. Tests use this to avoid mutating process env, which races under
/// the parallel test harness (#423 CI flake on PR #428).
pub async fn perform_agent_restart_in(
    home: &Path,
    name: &str,
    fresh_session: bool,
) -> Result<RestartOutcome> {
    perform_agent_restart_inner(name, fresh_session, Some(home)).await
}

async fn perform_agent_restart_inner(
    name: &str,
    fresh_session: bool,
    home: Option<&Path>,
) -> Result<RestartOutcome> {
    let load = |name: &str| match home {
        Some(h) => crate::app::agent::load_state_in(h, name),
        None => crate::app::agent::load_state(name),
    };
    let save = |state: &crate::app::agent::AgentState| match home {
        Some(h) => crate::app::agent::save_state_in(h, state),
        None => crate::app::agent::save_state_pub(state),
    };

    let state = load(name)?;
    let pid = state.pid;

    // Only call SIGTERM if /proc/<pid> still exists. stop_agent_process
    // already short-circuits on pid=0, but we also want to avoid a blocking
    // 30s wait when the pid has been recycled or never existed.
    if pid > 0 && Path::new(&format!("/proc/{}", pid)).exists() {
        stop_agent_process(name, pid).await;
    }

    let mut updated = load(name)?;
    if fresh_session {
        updated.session_id.clear();
        updated.session_cost = 0.0;
        updated.session_turns = 0;
        updated.session_start = None;
    }
    updated.status = "restarting".to_string();
    save(&updated)?;

    Ok(RestartOutcome {
        previous_pid: pid,
        fresh_session,
    })
}

#[cfg(test)]
mod restart_tests {
    use super::*;
    use crate::app::agent::{AgentConfig, AgentState};
    use tempfile::TempDir;

    /// Persist a minimal `AgentState` for a unique `name` under an isolated
    /// tempdir-as-home. Returns the `(TempDir, name)` pair so tests can pass
    /// the path explicitly to `perform_agent_restart_in` instead of mutating
    /// the process-wide `HOME` env var (POSIX setenv is not thread-safe;
    /// concurrent tests racing on `HOME` caused the #423 CI flake on
    /// PR #428).
    fn seed_agent_state(pid: u32, session_id: &str) -> (TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        let name = format!("restart-test-{}", uuid::Uuid::new_v4());

        let cfg = AgentConfig {
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
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let state = AgentState {
            config: cfg,
            pid,
            session_id: session_id.to_string(),
            total_turns: 5,
            total_cost: 1.23,
            created_at: chrono::Utc::now().to_rfc3339(),
            status: "idle".into(),
            current_task: String::new(),
            parent: None,
            scope: None,
            can_message: None,
            env_keys: None,
            session_start: Some("2026-01-01T00:00:00Z".into()),
            session_cost: 0.5,
            session_turns: 3,
            consecutive_empty_completions: 0,
            last_empty_restart_at: None,
            total_empty_restarts: 0,
            tmux_session: None,
            tmux_log_path: None,
        };
        crate::app::agent::save_state_in(tmp.path(), &state).unwrap();
        (tmp, name)
    }

    /// Verifies `--fresh-session` clears `session_id` (and the related
    /// session counters) before the worker respawns.
    #[tokio::test]
    async fn test_restart_fresh_session_clears_session_id() {
        let (tmp, name) = seed_agent_state(0, "sess-original");

        let outcome = perform_agent_restart_in(tmp.path(), &name, true)
            .await
            .unwrap();
        assert!(outcome.fresh_session);
        assert_eq!(outcome.previous_pid, 0);

        let st = crate::app::agent::load_state_in(tmp.path(), &name).unwrap();
        assert_eq!(st.session_id, "");
        assert_eq!(st.session_cost, 0.0);
        assert_eq!(st.session_turns, 0);
        assert!(st.session_start.is_none());
        // total_turns / total_cost are NOT reset.
        assert_eq!(st.total_turns, 5);
        assert!((st.total_cost - 1.23).abs() < f64::EPSILON);
        assert_eq!(st.status, "restarting");
    }

    /// Verifies the default (non-fresh) restart preserves the session_id so
    /// claude resumes the previous conversation on next task.
    #[tokio::test]
    async fn test_restart_preserves_session_id() {
        let (tmp, name) = seed_agent_state(0, "sess-preserved");

        let outcome = perform_agent_restart_in(tmp.path(), &name, false)
            .await
            .unwrap();
        assert!(!outcome.fresh_session);

        let st = crate::app::agent::load_state_in(tmp.path(), &name).unwrap();
        assert_eq!(st.session_id, "sess-preserved");
        // session counters are also untouched.
        assert!((st.session_cost - 0.5).abs() < f64::EPSILON);
        assert_eq!(st.session_turns, 3);
        assert_eq!(st.session_start.as_deref(), Some("2026-01-01T00:00:00Z"));
        assert_eq!(st.status, "restarting");
    }

    /// Verifies a restart against an already-stopped agent (pid=0) succeeds
    /// without panicking and without trying to SIGTERM a non-existent PID.
    #[tokio::test]
    async fn test_restart_stopped_agent_no_panic() {
        let (tmp, name) = seed_agent_state(0, "sess-stopped");

        let outcome = perform_agent_restart_in(tmp.path(), &name, false)
            .await
            .unwrap();
        assert_eq!(outcome.previous_pid, 0);
        // State file mutated even when there's no live worker — supervisor
        // will respawn on next task.
        let st = crate::app::agent::load_state_in(tmp.path(), &name).unwrap();
        assert_eq!(st.status, "restarting");
    }

    /// Verifies `perform_agent_restart` runs end-to-end without any bus
    /// connection — the explicit design choice from the issue spec is that
    /// the CLI must work when `deskd serve` is hung. This test exercises that
    /// path: no bus_socket env var, no UnixStream, just direct state-file
    /// mutation. `perform_agent_restart` does not read `DESKD_BUS_SOCKET`,
    /// so we don't need to (and must not, under parallel tests) clear it.
    #[tokio::test]
    async fn test_restart_with_bus_disconnected() {
        let (tmp, name) = seed_agent_state(0, "sess-no-bus");

        // No connect_bus / no UnixStream::connect — call should still succeed.
        let outcome = perform_agent_restart_in(tmp.path(), &name, true)
            .await
            .unwrap();
        assert!(outcome.fresh_session);
        let st = crate::app::agent::load_state_in(tmp.path(), &name).unwrap();
        assert_eq!(st.session_id, "");
        assert_eq!(st.status, "restarting");
    }
}
