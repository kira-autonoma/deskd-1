/// Unified config hot-reload — watches deskd.yaml and restarts restartable
/// components when the config changes.
///
/// Restartable components (cancelled/aborted and respawned on config change):
///   - Adapters (Telegram, Discord)
///   - Schedule watcher
///   - Reminder runner
///   - Sub-agent workers
///
/// Session-persistent components (NOT restarted):
///   - Bus server (transport layer)
///   - Main worker (Claude session)
///   - Bus API handler
///   - Workflow engine
///
/// System prompt changes are still injected via bus message (existing pattern
/// in config_watcher.rs) rather than restarting the worker.
///
/// # Graceful adapter shutdown (Part A)
///
/// Adapters that support cooperative cancellation (currently Telegram) are
/// cancelled via their `CancellationToken` first, giving them up to 1 second
/// to drain in-flight messages before the task handle is aborted.
///
/// # Selective reload (Part B)
///
/// `classify_config_change` inspects what actually changed and returns a
/// `ConfigChangeset` so only the affected components are restarted, avoiding
/// unnecessary adapter disruption on system-prompt-only or schedule-only edits.
use tracing::info;

use crate::app::agent_components::AgentComponents;
use crate::app::config_changeset::{ConfigChangeset, classify_config_change};
use crate::app::reload_state::ReloadState;
use crate::app::{adapters, config_watcher, schedule, worker};
use crate::config;
use crate::infra::diag;

/// Spawn all restartable components for an agent and return their handles.
pub async fn spawn_components(
    def: &config::AgentDef,
    user_cfg: Option<&config::UserConfig>,
    admin_telegram_ids: &[i64],
    bus_socket: &str,
    agent_name: &str,
    cfg_path: &str,
) -> anyhow::Result<AgentComponents> {
    let mut components = AgentComponents {
        adapter_handles: Vec::new(),
        adapter_cancel_tokens: Vec::new(),
        schedule_watcher: None,
        config_watcher: None,
        reminder_runner: None,
        sub_agent_handles: Vec::new(),
    };

    // Adapters (Telegram, Discord, etc.)
    for (adapter, cancel_token) in adapters::build_adapters(def, user_cfg, admin_telegram_ids) {
        let bus = bus_socket.to_string();
        let name = agent_name.to_string();
        let adapter_name = adapter.name().to_string();
        let handle = tokio::spawn(async move {
            if let Err(e) = adapter.run(bus.clone(), name.clone()).await {
                diag::error_event(
                    Some(&bus),
                    "supervisor",
                    "adapter.failed",
                    format!("adapter failed: {}", e),
                    serde_json::json!({ "adapter": adapter_name, "agent": name }),
                );
            }
        });
        components.adapter_handles.push(handle);
        components.adapter_cancel_tokens.push(cancel_token);
    }

    // Schedule watcher
    {
        let bus = bus_socket.to_string();
        let name = agent_name.to_string();
        let config = cfg_path.to_string();
        let home = def.work_dir.clone();
        let handle = tokio::spawn(async move {
            schedule::watch_and_reload(config, bus, name, home).await;
        });
        components.schedule_watcher = Some(handle);
    }

    // Config watcher (system_prompt injection)
    {
        let bus = bus_socket.to_string();
        let name = agent_name.to_string();
        let config = cfg_path.to_string();
        let handle = tokio::spawn(async move {
            config_watcher::watch_system_prompt(config, bus, name).await;
        });
        components.config_watcher = Some(handle);
    }

    // Reminder runner — work_dir threaded through so the scanner reads from
    // the SAME location the MCP `create_reminder` tool writes to (#467).
    {
        let bus = bus_socket.to_string();
        let name = agent_name.to_string();
        let work_dir = def.work_dir.clone();
        let handle = tokio::spawn(async move {
            schedule::run_reminders(bus, name, work_dir).await;
        });
        components.reminder_runner = Some(handle);
    }

    // Sub-agent workers
    if let Some(ucfg) = user_cfg {
        for sub in &ucfg.agents {
            let is_context = matches!(
                sub.kind,
                crate::domain::config_types::ConfigAgentKind::Context
            );

            // Context agents are lightweight Q&A workers — no MCP server, no
            // tool access. Executor agents get the full deskd MCP plumbing.
            let mcp_json = serde_json::json!({
                "mcpServers": {
                    "deskd": {
                        "command": "deskd",
                        "args": ["mcp", "--agent", &sub.name]
                    }
                }
            })
            .to_string();

            let context_cfg = sub.context.clone().or_else(|| ucfg.context.clone());

            let mut command: Vec<String> = vec![
                "claude".into(),
                "--output-format".into(),
                "stream-json".into(),
                "--verbose".into(),
                "--dangerously-skip-permissions".into(),
                "--model".into(),
                sub.model.clone(),
                "--max-turns".into(),
                ucfg.max_turns.to_string(),
            ];
            if !is_context {
                command.push("--mcp-config".into());
                command.push(mcp_json);
            }

            let sub_cfg = crate::app::agent::AgentConfig {
                name: sub.name.clone(),
                model: sub.model.clone(),
                system_prompt: sub.system_prompt.clone(),
                work_dir: def.work_dir.clone(),
                max_turns: ucfg.max_turns,
                unix_user: def.unix_user.clone(),
                command,
                config_path: Some(cfg_path.to_string()),
                container: def.container.clone(),
                session: sub.session.clone(),
                runtime: sub.runtime.clone(),
                launch_mode: sub.launch_mode.clone(),
                kind: sub.kind.clone(),
                context: context_cfg,
                compact_threshold: sub.compact_threshold,
                auto_compact_threshold_tokens: sub
                    .auto_compact_threshold_tokens
                    .or(ucfg.auto_compact_threshold_tokens),
                empty_completion_threshold: sub.empty_completion_threshold,
                empty_completion_restart_min_secs: sub.empty_completion_restart_min_secs,
            };
            crate::app::agent::create_or_update_from_config(&sub_cfg).await?;

            let sub_name = sub.name.clone();
            let bus = bus_socket.to_string();
            let subs = sub.subscribe.clone();
            let sub_task_store = crate::app::task::TaskStore::default_for_home();
            let handle = tokio::spawn(async move {
                if let Err(e) = worker::run(
                    &sub_name,
                    &bus,
                    Some(bus.clone()),
                    Some(subs),
                    &sub_task_store,
                )
                .await
                {
                    diag::error_event(
                        Some(&bus),
                        "supervisor",
                        "sub_agent.exited",
                        format!("sub-agent worker exited: {}", e),
                        serde_json::json!({ "agent": sub_name }),
                    );
                }
            });
            components.sub_agent_handles.push(handle);
        }
    }

    Ok(components)
}

/// Spawn only adapter components and append them to existing `components`.
async fn spawn_adapters(
    def: &config::AgentDef,
    user_cfg: Option<&config::UserConfig>,
    admin_telegram_ids: &[i64],
    bus_socket: &str,
    agent_name: &str,
    components: &mut AgentComponents,
) {
    for (adapter, cancel_token) in adapters::build_adapters(def, user_cfg, admin_telegram_ids) {
        let bus = bus_socket.to_string();
        let name = agent_name.to_string();
        let adapter_name = adapter.name().to_string();
        let handle = tokio::spawn(async move {
            if let Err(e) = adapter.run(bus.clone(), name.clone()).await {
                diag::error_event(
                    Some(&bus),
                    "supervisor",
                    "adapter.failed",
                    format!("adapter failed: {}", e),
                    serde_json::json!({ "adapter": adapter_name, "agent": name }),
                );
            }
        });
        components.adapter_handles.push(handle);
        components.adapter_cancel_tokens.push(cancel_token);
    }
}

/// Spawn only schedule/reminder components and store them in `components`.
fn spawn_schedules(
    def: &config::AgentDef,
    bus_socket: &str,
    agent_name: &str,
    cfg_path: &str,
    components: &mut AgentComponents,
) {
    {
        let bus = bus_socket.to_string();
        let name = agent_name.to_string();
        let config = cfg_path.to_string();
        let home = def.work_dir.clone();
        let handle = tokio::spawn(async move {
            schedule::watch_and_reload(config, bus, name, home).await;
        });
        components.schedule_watcher = Some(handle);
    }
    {
        let bus = bus_socket.to_string();
        let name = agent_name.to_string();
        let work_dir = def.work_dir.clone();
        let handle = tokio::spawn(async move {
            schedule::run_reminders(bus, name, work_dir).await;
        });
        components.reminder_runner = Some(handle);
    }
}

/// Watch the agent's deskd.yaml for changes and hot-reload components selectively.
///
/// Wakes on whichever fires first:
///   * a 30-second mtime poll tick (file-watcher fallback path), or
///   * an explicit `ReloadState::trigger()` (driven by `deskd reload`, #474).
///
/// On change, uses `classify_config_change` to determine which components need
/// restarting:
///   - `system_prompt_only` → nothing restarted (system_prompt injected via bus by config_watcher)
///   - `adapters_changed` → cooperative cancel + respawn adapters only
///   - `schedules_changed` → abort + respawn schedule_watcher only
///   - otherwise → full abort_all + respawn
///
/// Bus server, main worker, bus API, and workflow engine are never affected.
pub async fn watch_and_reload(
    def: config::AgentDef,
    initial_components: AgentComponents,
    admin_telegram_ids: Vec<i64>,
    bus_socket: String,
    agent_name: String,
    cfg_path: String,
    reload_state: ReloadState,
) {
    let mut components = initial_components;
    let mut last_modified = file_mtime(&cfg_path);
    // Track the last known config for diffing.
    let mut last_user_cfg = config::UserConfig::load(&cfg_path).ok();
    // Track the agents_dir directory mtime so that creating/removing/renaming
    // *.agent.md files (which doesn't touch deskd.yaml) still triggers reload.
    let mut last_agents_dir_mtime = agents_dir_mtime(&cfg_path, last_user_cfg.as_ref());

    loop {
        // Wake on either the 30s poll tick OR an explicit reload trigger.
        // The trigger path is what `deskd reload` uses; the poll is the
        // fallback for direct YAML edits.
        let triggered = tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => false,
            _ = reload_state.wait_for_trigger() => true,
        };

        let current_mtime = file_mtime(&cfg_path);
        let current_agents_dir_mtime = agents_dir_mtime(&cfg_path, last_user_cfg.as_ref());
        // Explicit triggers always proceed even if no mtime delta is visible
        // — e.g. the operator may want to re-apply the current config to
        // recover from a transient adapter failure.
        if !triggered
            && current_mtime == last_modified
            && current_agents_dir_mtime == last_agents_dir_mtime
        {
            continue;
        }
        last_modified = current_mtime;
        last_agents_dir_mtime = current_agents_dir_mtime;

        info!(
            agent = %agent_name,
            triggered = triggered,
            "config changed, analysing diff"
        );

        // Reload config.
        let new_user_cfg = match config::UserConfig::load(&cfg_path) {
            Ok(cfg) => cfg,
            Err(e) => {
                diag::warn_event(
                    Some(&bus_socket),
                    "config_reload",
                    "config.reload_failed",
                    format!("failed to reload config, components unchanged: {}", e),
                    serde_json::json!({ "agent": agent_name, "path": cfg_path }),
                );
                reload_state
                    .record_failure(format!("failed to parse {}: {}", cfg_path, e))
                    .await;
                continue;
            }
        };

        // Classify what changed.
        let changeset = if let Some(ref old_cfg) = last_user_cfg {
            classify_config_change(old_cfg, &new_user_cfg)
        } else {
            // No previous config — treat everything as changed.
            ConfigChangeset {
                adapters_changed: true,
                schedules_changed: true,
                sub_agents_changed: true,
                system_prompt_only: false,
                removed_sub_agents: Vec::new(),
            }
        };
        last_user_cfg = Some(new_user_cfg.clone());

        if changeset.system_prompt_only {
            // system_prompt_only: config_watcher.rs injects the new prompt via bus.
            // No adapter or schedule disruption needed.
            info!(agent = %agent_name, "system_prompt changed only — no component restart needed");
            reload_state.record_success(chrono::Utc::now()).await;
            continue;
        }

        if !changeset.adapters_changed
            && !changeset.schedules_changed
            && !changeset.sub_agents_changed
        {
            // Only non-restartable fields changed (e.g. mcp_config, context config).
            info!(agent = %agent_name, "config change requires no component restart");
            reload_state.record_success(chrono::Utc::now()).await;
            continue;
        }

        // Selective restart.
        if changeset.adapters_changed
            && !changeset.schedules_changed
            && !changeset.sub_agents_changed
        {
            info!(agent = %agent_name, "adapter config changed — restarting adapters only");
            components.abort_adapters().await;
            spawn_adapters(
                &def,
                Some(&new_user_cfg),
                &admin_telegram_ids,
                &bus_socket,
                &agent_name,
                &mut components,
            )
            .await;
            info!(agent = %agent_name, adapters = components.adapter_handles.len(), "adapters restarted");
            reload_state.record_success(chrono::Utc::now()).await;
            continue;
        }

        if changeset.schedules_changed
            && !changeset.adapters_changed
            && !changeset.sub_agents_changed
        {
            info!(agent = %agent_name, "schedule config changed — restarting schedules only");
            components.abort_schedules();
            spawn_schedules(&def, &bus_socket, &agent_name, &cfg_path, &mut components);
            info!(agent = %agent_name, "schedules restarted");
            reload_state.record_success(chrono::Utc::now()).await;
            continue;
        }

        // Full restart for combined or sub-agent changes.
        info!(agent = %agent_name, "config change requires full component restart");
        let old_summary = components.summary();
        components.abort_all().await;
        info!(agent = %agent_name, old = %old_summary, "aborted old components");

        // Evict state files for sub-agents that vanished from config (e.g. their
        // *.agent.md was deleted). Without this they could be respawned on the
        // next `deskd serve` because state files survive aborts.
        for removed in &changeset.removed_sub_agents {
            match crate::app::agent_registry::remove(removed).await {
                Ok(()) => info!(agent = %agent_name, removed = %removed, "evicted sub-agent state"),
                Err(e) => diag::warn_event(
                    Some(&bus_socket),
                    "config_reload",
                    "sub_agent.evict_failed",
                    format!("failed to evict sub-agent state: {}", e),
                    serde_json::json!({ "agent": agent_name, "removed": removed }),
                ),
            }
        }

        match spawn_components(
            &def,
            Some(&new_user_cfg),
            &admin_telegram_ids,
            &bus_socket,
            &agent_name,
            &cfg_path,
        )
        .await
        {
            Ok(new_components) => {
                let summary = new_components.summary();
                components = new_components;
                info!(
                    agent = %agent_name,
                    summary = %summary,
                    "config reloaded: {}", summary
                );
                reload_state.record_success(chrono::Utc::now()).await;
            }
            Err(e) => {
                diag::warn_event(
                    Some(&bus_socket),
                    "config_reload",
                    "components.respawn_failed",
                    format!("failed to respawn components after config reload: {}", e),
                    serde_json::json!({ "agent": agent_name }),
                );
                reload_state
                    .record_failure(format!("respawn failed: {}", e))
                    .await;
            }
        }
    }
}

fn file_mtime(path: &str) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

/// Resolve `user_cfg.agents_dir` relative to the deskd.yaml parent and return
/// the latest mtime among the directory itself and any contained `*.agent.md`
/// file. Returns `None` when no agents_dir is configured or the directory is
/// missing — callers compare for equality so two `None`s match.
fn agents_dir_mtime(
    cfg_path: &str,
    user_cfg: Option<&config::UserConfig>,
) -> Option<std::time::SystemTime> {
    let dir_rel = user_cfg.and_then(|c| c.agents_dir.as_deref())?;
    let base = std::path::Path::new(cfg_path).parent()?;
    let dir = base.join(dir_rel);
    let mut latest = std::fs::metadata(&dir)
        .ok()
        .and_then(|m| m.modified().ok())?;
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "md")
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".agent.md"))
                && let Ok(meta) = entry.metadata()
                && let Ok(mtime) = meta.modified()
                && mtime > latest
            {
                latest = mtime;
            }
        }
    }
    Some(latest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_file_mtime_missing() {
        assert!(file_mtime("/tmp/nonexistent-deskd-config-reload-test").is_none());
    }

    #[test]
    fn test_file_mtime_existing() {
        assert!(file_mtime("Cargo.toml").is_some());
    }

    #[test]
    fn test_agents_dir_mtime_none_without_dir_field() {
        let cfg = config::UserConfig::default();
        // No agents_dir field configured → returns None.
        assert!(agents_dir_mtime("/tmp/whatever.yaml", Some(&cfg)).is_none());
    }

    #[test]
    fn test_agents_dir_mtime_changes_when_agent_file_added_or_removed() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg_path = tmp.path().join("deskd.yaml");
        std::fs::write(&cfg_path, "model: haiku\nagents_dir: agents.d\n").unwrap();

        let agents_dir = tmp.path().join("agents.d");
        std::fs::create_dir(&agents_dir).unwrap();

        let cfg = config::UserConfig {
            agents_dir: Some("agents.d".into()),
            ..Default::default()
        };

        let before = agents_dir_mtime(cfg_path.to_str().unwrap(), Some(&cfg));
        assert!(
            before.is_some(),
            "empty agents_dir should still report mtime"
        );

        // Sleep briefly so mtime resolution can register a change.
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let agent_file = agents_dir.join("foo.agent.md");
        let mut f = std::fs::File::create(&agent_file).unwrap();
        f.write_all(b"---\nname: foo\nmodel: haiku\n---\nbody\n")
            .unwrap();
        f.sync_all().unwrap();
        drop(f);

        let after_create = agents_dir_mtime(cfg_path.to_str().unwrap(), Some(&cfg));
        assert!(after_create.is_some());
        assert_ne!(
            before, after_create,
            "creating *.agent.md must change mtime"
        );

        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::remove_file(&agent_file).unwrap();

        let after_remove = agents_dir_mtime(cfg_path.to_str().unwrap(), Some(&cfg));
        assert!(after_remove.is_some());
        assert_ne!(
            after_create, after_remove,
            "removing *.agent.md must change mtime"
        );
    }
}
