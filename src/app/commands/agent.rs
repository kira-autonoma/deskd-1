//! `deskd agent` subcommand handlers.

use anyhow::{Context, Result};
use tracing::info;

use crate::app::cli::AgentAction;
use crate::app::{agent, tasklog, unified_inbox, worker};
use crate::config;

use super::{format_relative_time, parse_duration_secs, truncate};

pub async fn handle(action: AgentAction) -> Result<()> {
    match action {
        AgentAction::Create {
            name,
            prompt,
            model,
            workdir,
            max_turns,
            unix_user,
            command,
        } => {
            let cfg = agent::AgentConfig {
                name,
                model,
                system_prompt: prompt.unwrap_or_default(),
                work_dir: workdir.unwrap_or_else(|| ".".into()),
                max_turns,
                unix_user,
                command: if command.is_empty() {
                    vec!["claude".to_string()]
                } else {
                    command
                },
                config_path: None,
                container: None,
                session: crate::domain::config_types::ConfigSessionMode::default(),
                runtime: crate::domain::config_types::ConfigAgentRuntime::default(),
                launch_mode: crate::domain::config_types::ConfigLaunchMode::default(),
                kind: crate::domain::config_types::ConfigAgentKind::default(),
                context: None,
                compact_threshold: None,
                auto_compact_threshold_tokens: None,
                empty_completion_threshold: None,
                empty_completion_restart_min_secs: None,
            };
            let state = agent::create(&cfg).await?;
            println!("Agent {} created", state.config.name);
        }
        AgentAction::Send {
            name,
            message,
            max_turns,
            socket,
        } => {
            let effective_socket = if let Some(ref s) = socket {
                if std::path::Path::new(s).exists() {
                    socket
                } else {
                    None
                }
            } else {
                // Try agent state file first, then serve state.
                agent::load_state(&name)
                    .ok()
                    .and_then(|s| {
                        let bus = config::agent_bus_socket(&s.config.work_dir);
                        if std::path::Path::new(&bus).exists() {
                            Some(bus)
                        } else {
                            None
                        }
                    })
                    .or_else(|| {
                        config::ServeState::load().and_then(|state| {
                            state.agent(&name).and_then(|a| {
                                if std::path::Path::new(&a.bus_socket).exists() {
                                    Some(a.bus_socket.clone())
                                } else {
                                    None
                                }
                            })
                        })
                    })
            };

            if let Some(sock) = effective_socket {
                let target = format!("agent:{}", name);
                worker::send_via_bus(&sock, "cli", &target, &message, max_turns).await?;
            } else {
                let response = agent::send(&name, &message, max_turns, None).await?;
                println!("{}", response);
            }
        }
        AgentAction::Run {
            name,
            socket,
            subscribe,
        } => {
            agent::load_state(&name)?;
            let subs = if subscribe.is_empty() {
                None
            } else {
                Some(subscribe)
            };
            let task_store = crate::app::task::TaskStore::default_for_home();
            info!(agent = %name, "starting worker");
            tokio::select! {
                result = worker::run(&name, &socket, Some(socket.clone()), subs, &task_store) => { result?; }
                _ = tokio::signal::ctrl_c() => {
                    info!(agent = %name, "shutting down");
                }
            }
        }
        AgentAction::List { socket } => {
            let agents = agent::list().await?;
            // Query all known bus sockets for live agents.
            let mut live = crate::app::serve::query_live_agents(&socket)
                .await
                .unwrap_or_default();
            // Also check sockets from serve state (per-agent buses).
            if let Some(state) = config::ServeState::load() {
                for agent_state in state.agents.values() {
                    if agent_state.bus_socket != socket
                        && let Ok(more) =
                            crate::app::serve::query_live_agents(&agent_state.bus_socket).await
                    {
                        live.extend(more);
                    }
                }
            }
            // Also check internal buses for sub-agents: each parent agent may
            // have an internal bus at /tmp/deskd-{parent}-internal.sock.
            let parent_names: std::collections::HashSet<String> =
                agents.iter().filter_map(|a| a.parent.clone()).collect();
            for parent in &parent_names {
                let internal_sock = format!("/tmp/deskd-{}-internal.sock", parent);
                if let Ok(more) = crate::app::serve::query_live_agents(&internal_sock).await {
                    live.extend(more);
                }
            }

            // Discover tmux sessions once so we can surface `tmux: deskd-<n>
            // (attached/detached)` per agent. Missing tmux is fine — empty map
            // means "no tmux sessions"; subprocess agents are unaffected.
            let tmux_sessions = crate::app::tmux_launcher::list_tmux_sessions().unwrap_or_default();

            if agents.is_empty() {
                println!("No agents registered");
            } else {
                println!(
                    "{:<15} {:<14} {:<8} {:<10} {:<12} {:<28} MODEL",
                    "NAME", "STATUS", "TURNS", "COST", "USER", "LAUNCHER"
                );
                let thresholds = crate::app::doctor::DoctorThresholds::default();
                for a in &agents {
                    let domain = agent::to_domain_agent(a, &live);
                    // Run the same heuristic the `doctor` command uses so the
                    // STATUS column is honest (no more `ready` for hung agents
                    // — see #422).
                    let verdict = super::doctor::diagnose_one(
                        &a.config.name,
                        thresholds.empty_completion_threshold.max(3) * 3,
                        &thresholds,
                    )
                    .map(|(v, _, _)| v)
                    .ok();
                    let status_str = format_list_status(verdict.as_ref(), &domain, a);
                    let launcher = format_launcher_column(a, &tmux_sessions);
                    println!(
                        "{:<15} {:<14} {:<8} ${:<9.2} {:<12} {:<28} {}",
                        domain.name,
                        status_str,
                        a.total_turns,
                        a.total_cost,
                        a.config.unix_user.as_deref().unwrap_or("-"),
                        launcher,
                        domain.capabilities.model,
                    );
                }
            }
        }
        AgentAction::Stats { name } => {
            let s = agent::load_state(&name)?;
            println!("Agent:      {}", s.config.name);
            println!("Model:      {}", s.config.model);
            println!(
                "Unix user:  {}",
                s.config.unix_user.as_deref().unwrap_or("-")
            );
            println!("Work dir:   {}", s.config.work_dir);
            println!(
                "Bus:        {}",
                config::agent_bus_socket(&s.config.work_dir)
            );
            println!(
                "Config:     {}",
                s.config.config_path.as_deref().unwrap_or("-")
            );
            println!("Total turns:{}", s.total_turns);
            println!("Total cost: ${:.4}", s.total_cost);
            println!(
                "Session:    {}",
                if s.session_id.is_empty() {
                    "-"
                } else {
                    &s.session_id
                }
            );
            println!("Created:    {}", s.created_at);
            // Empty-completion detection state (#424).
            println!("Empty cons: {}", s.consecutive_empty_completions);
            println!("Empty rstr: {}", s.total_empty_restarts);
            if let Some(ref ts) = s.last_empty_restart_at {
                println!("Last empty restart: {}", ts);
            }
        }
        AgentAction::Read {
            name,
            clear: _,
            follow,
        } => {
            let inbox_name = format!("replies/{}", name);
            // If invoked from inside an agent's context (DESKD_AGENT_NAME set),
            // enforce inbox_acl from that agent's deskd.yaml. When invoked from
            // a human shell (no env), the operator is trusted (file permissions
            // remain the OS-level guard).
            if let Ok(caller) = std::env::var("DESKD_AGENT_NAME") {
                let cfg_path = std::env::var("DESKD_AGENT_CONFIG").ok();
                let cfg = cfg_path
                    .as_deref()
                    .and_then(|p| crate::config::UserConfig::load(p).ok());
                if !crate::app::mcp_tools::inbox_access_allowed(&caller, &inbox_name, cfg.as_ref())
                    && !crate::app::mcp_tools::inbox_access_allowed(&caller, &name, cfg.as_ref())
                {
                    anyhow::bail!(
                        "inbox access denied: agent \"{}\" is not in allow-list for inbox \"{}\"",
                        caller,
                        name
                    );
                }
            }
            let entries = unified_inbox::read_messages(&inbox_name, 100, None)?;
            if entries.is_empty() && !follow {
                println!("No messages for {}", name);
            } else {
                for entry in &entries {
                    print_inbox_message(entry);
                }
            }
            if follow {
                let mut last_ts = entries.last().map(|m| m.ts);
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let newer = unified_inbox::read_messages(&inbox_name, 100, last_ts)?;
                    for entry in &newer {
                        print_inbox_message(entry);
                        last_ts = Some(entry.ts);
                    }
                }
            }
        }
        AgentAction::Tasks { name, limit } => {
            let show_all = name == "all";

            let mut filtered: Vec<unified_inbox::InboxMessage> = if show_all {
                let inboxes = unified_inbox::list_inboxes()?;
                let mut all = Vec::new();
                for (inbox_name, _) in &inboxes {
                    if inbox_name.starts_with("replies/") {
                        let msgs = unified_inbox::read_messages(inbox_name, 1000, None)?;
                        all.extend(msgs.into_iter().filter(|m| {
                            m.metadata.get("type").and_then(|v| v.as_str()) == Some("task_result")
                        }));
                    }
                }
                all
            } else {
                let inbox_name = format!("replies/{}", name);
                let msgs = unified_inbox::read_messages(&inbox_name, 1000, None)?;
                msgs.into_iter()
                    .filter(|m| {
                        m.metadata.get("type").and_then(|v| v.as_str()) == Some("task_result")
                    })
                    .collect()
            };

            filtered.sort_by_key(|b| std::cmp::Reverse(b.ts));
            filtered.truncate(limit);

            if filtered.is_empty() {
                if show_all {
                    println!("No completed tasks found");
                } else {
                    println!("No completed tasks for {}", name);
                }
            } else {
                println!("COMPLETED ({}):", filtered.len());
                let now = chrono::Utc::now();
                for entry in &filtered {
                    let dur = now.signed_duration_since(entry.ts);
                    let age = format_relative_time(dur);
                    let agent = entry
                        .metadata
                        .get("agent")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");
                    let task_text = entry
                        .metadata
                        .get("task")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let task_excerpt = truncate(task_text, 36);
                    let has_error = entry
                        .metadata
                        .get("has_error")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let status = if has_error { "err" } else { "done" };
                    let msg_id = entry
                        .metadata
                        .get("message_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let id_short = if msg_id.len() > 6 {
                        &msg_id[..6]
                    } else {
                        msg_id
                    };
                    if show_all {
                        println!(
                            "  {:<8} {:<12} from:{:<6} {:38} {} {} ago",
                            id_short,
                            agent,
                            entry.source,
                            format!("\"{}\"", task_excerpt),
                            status,
                            age,
                        );
                    } else {
                        println!(
                            "  {:<8} from:{:<6} {:38} {} {} ago",
                            id_short,
                            entry.source,
                            format!("\"{}\"", task_excerpt),
                            status,
                            age,
                        );
                    }
                }
            }
        }
        AgentAction::Logs {
            name,
            limit,
            source,
            since,
            json,
            cost,
            by_pr,
        } => {
            let since_dt = if let Some(ref dur) = since {
                let secs = parse_duration_secs(dur)?;
                Some(chrono::Utc::now() - chrono::Duration::seconds(secs as i64))
            } else {
                None
            };

            let entries = tasklog::read_logs(
                &name,
                if cost || by_pr { usize::MAX } else { limit },
                source.as_deref(),
                since_dt,
            )?;

            if entries.is_empty() {
                println!("No task logs for {}", name);
            } else if by_pr {
                tasklog::print_pr_summary(&name, &entries, since.as_deref());
            } else if cost {
                tasklog::print_cost_summary(&name, &entries, since.as_deref());
            } else if json {
                tasklog::print_json(&entries);
            } else {
                tasklog::print_table(&entries);
            }
        }
        AgentAction::Status { name } => match name {
            Some(name) => {
                let s = agent::load_state(&name)?;
                let pid_alive =
                    s.pid > 0 && std::path::Path::new(&format!("/proc/{}", s.pid)).exists();
                // For sub-agents on an internal bus, also check the internal bus socket.
                let on_internal_bus = if let Some(ref parent) = s.parent {
                    let internal_sock = format!("/tmp/deskd-{}-internal.sock", parent);
                    crate::app::serve::query_live_agents(&internal_sock)
                        .await
                        .map(|live| live.contains(&s.config.name))
                        .unwrap_or(false)
                } else {
                    false
                };
                let status_str = if pid_alive || on_internal_bus {
                    s.status.as_str()
                } else {
                    "offline"
                };
                let is_sub_agent = s.parent.is_some();
                println!("Agent:       {}", s.config.name);
                if is_sub_agent {
                    println!("Type:        sub-agent");
                }
                println!("Status:      {}", status_str);
                println!(
                    "PID:         {}",
                    if pid_alive {
                        s.pid.to_string()
                    } else {
                        "-".into()
                    }
                );
                println!("Model:       {}", s.config.model);
                println!("Turns:       {}", s.total_turns);
                println!("Cost:        ${:.4}", s.total_cost);
                println!("Created:     {}", s.created_at);
                println!("Work dir:    {}", s.config.work_dir);
                if !s.current_task.is_empty() {
                    println!("Current:     {}", truncate(&s.current_task, 60));
                }
                if let Some(ref parent) = s.parent {
                    println!("Parent:      {}", parent);
                }
                // Show stderr tail if available.
                let stderr_path = agent::stderr_log_path(&name);
                if stderr_path.exists() {
                    let content = std::fs::read_to_string(&stderr_path).unwrap_or_default();
                    let lines: Vec<&str> = content.lines().collect();
                    if !lines.is_empty() {
                        let start = lines.len().saturating_sub(5);
                        println!("Stderr (last {}):", lines.len().min(5));
                        for line in &lines[start..] {
                            println!("  {}", line);
                        }
                    }
                }
            }
            None => {
                let agents = agent::list().await?;
                if agents.is_empty() {
                    println!("No agents registered");
                } else {
                    println!(
                        "{:<15} {:<12} {:<6} {:<10} {:<20} MODEL",
                        "NAME", "STATUS", "TURNS", "COST", "CREATED"
                    );
                    println!("{}", "─".repeat(78));
                    for a in &agents {
                        let pid_alive =
                            a.pid > 0 && std::path::Path::new(&format!("/proc/{}", a.pid)).exists();
                        // Sub-agents: treat as alive if their PID is alive even if
                        // we can't reach the internal bus from the CLI.
                        let status = if pid_alive {
                            if a.parent.is_some() {
                                format!("{}[sub]", a.status)
                            } else {
                                a.status.clone()
                            }
                        } else if a.parent.is_some() {
                            "offline[sub]".to_string()
                        } else {
                            "offline".to_string()
                        };
                        let created = if a.created_at.len() > 19 {
                            &a.created_at[..19]
                        } else {
                            &a.created_at
                        };
                        println!(
                            "{:<15} {:<12} {:<6} ${:<9.4} {:<20} {}",
                            a.config.name,
                            status,
                            a.total_turns,
                            a.total_cost,
                            created,
                            a.config.model,
                        );
                    }
                }
            }
        },
        AgentAction::Stderr { name, tail, follow } => {
            let path = agent::stderr_log_path(&name);
            if !path.exists() {
                println!("No stderr logs for {}", name);
                return Ok(());
            }
            let content = std::fs::read_to_string(&path)?;
            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(tail);
            for line in &lines[start..] {
                println!("{}", line);
            }
            if follow {
                let mut pos = std::fs::metadata(&path)?.len();
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    if let Ok(meta) = std::fs::metadata(&path) {
                        let new_len = meta.len();
                        if new_len > pos {
                            let mut f = std::fs::File::open(&path)?;
                            use std::io::{Read, Seek, SeekFrom};
                            f.seek(SeekFrom::Start(pos))?;
                            let mut buf = String::new();
                            f.read_to_string(&mut buf)?;
                            print!("{}", buf);
                            pos = new_len;
                        }
                    }
                }
            }
        }
        AgentAction::Stream {
            name,
            tail,
            follow,
            raw,
        } => {
            let path = agent::stream_log_path(&name);
            if !path.exists() {
                println!("No stream log for {}", name);
                return Ok(());
            }
            let content = std::fs::read_to_string(&path)?;
            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(tail);
            for line in &lines[start..] {
                if raw {
                    println!("{}", line);
                } else {
                    print_stream_event(line);
                }
            }
            if follow {
                let mut pos = std::fs::metadata(&path)?.len();
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    if let Ok(meta) = std::fs::metadata(&path) {
                        let new_len = meta.len();
                        if new_len > pos {
                            let mut f = std::fs::File::open(&path)?;
                            use std::io::{Read, Seek, SeekFrom};
                            f.seek(SeekFrom::Start(pos))?;
                            let mut buf = String::new();
                            f.read_to_string(&mut buf)?;
                            for line in buf.lines() {
                                if raw {
                                    println!("{}", line);
                                } else {
                                    print_stream_event(line);
                                }
                            }
                            pos = new_len;
                        }
                    }
                }
            }
        }
        AgentAction::Rm { name } => {
            agent::remove(&name).await?;
            println!("Agent {} removed", name);
        }
        AgentAction::Set {
            name,
            container,
            config: config_opt,
        } => {
            let workspace_path = resolve_workspace_path(config_opt)?;
            let Some(profile) = container else {
                anyhow::bail!(
                    "`deskd agent set` requires at least one field to update (e.g. --container)"
                );
            };
            set_agent_container(&workspace_path, &name, &profile)?;
            println!(
                "Updated agent '{}' container profile to '{}' in {}",
                name, profile, workspace_path
            );
            println!(
                "Run `deskd restart` (or restart the agent's process) for the change to take effect."
            );
        }
        AgentAction::Doctor {
            name,
            last,
            empty_threshold,
            idle_minutes,
            stuck_minutes,
        } => {
            super::doctor::handle(name, last, empty_threshold, idle_minutes, stuck_minutes).await?;
        }
        AgentAction::Restart {
            name,
            all,
            fresh_session,
            timeout,
        } => {
            return restart_agents(name, all, fresh_session, timeout).await;
        }
        AgentAction::Start {
            name,
            tmux,
            persistent,
            config,
            log_dir,
        } => {
            handle_start(
                &name,
                tmux,
                persistent,
                config.as_deref(),
                log_dir.as_deref(),
            )?;
        }
        AgentAction::Stop {
            name,
            config,
            uninstall_unit,
        } => {
            handle_stop(&name, config.as_deref(), uninstall_unit)?;
        }
        AgentAction::Session { action } => {
            handle_session(action)?;
        }
        AgentAction::Spawn {
            name,
            task,
            socket,
            work_dir,
            model,
            max_turns,
        } => {
            let bus_socket = socket
                .or_else(|| std::env::var("DESKD_BUS_SOCKET").ok())
                .ok_or_else(|| {
                    anyhow::anyhow!("No bus socket: pass --socket or set DESKD_BUS_SOCKET")
                })?;

            let parent = std::env::var("DESKD_AGENT_NAME").unwrap_or_else(|_| "unknown".into());
            let resolved_work_dir = work_dir.unwrap_or_else(|| ".".into());

            let response = agent::spawn_ephemeral(
                &name,
                &task,
                &model,
                &resolved_work_dir,
                max_turns,
                &bus_socket,
                &parent,
            )
            .await?;

            println!("{}", response);
        }
    }
    Ok(())
}

/// Format the STATUS column for `agent list`, honoring the doctor verdict
/// when it surfaces a real problem.
///
/// Falls back to the original `ready / busy / unhealthy` labels when the
/// verdict is `Healthy` so existing consumers (TUI, scripts) keep working.
fn format_list_status(
    verdict: Option<&crate::app::doctor::Verdict>,
    domain: &crate::domain::agent::Agent,
    state: &agent::AgentState,
) -> String {
    use crate::app::doctor::Verdict;
    let suffix = if state.parent.is_some() { "[sub]" } else { "" };
    if let Some(v) = verdict {
        match v {
            Verdict::Hung { .. } => return format!("🔴 hung{}", suffix),
            Verdict::Stuck { .. } => return format!("🔴 stuck{}", suffix),
            Verdict::Dead { .. } => return format!("🔴 dead{}", suffix),
            Verdict::Idle { .. } => return format!("🟡 idle{}", suffix),
            Verdict::Healthy { .. } => {} // fall through to base label
        }
    }
    match &domain.status {
        crate::domain::agent::AgentStatus::Ready => format!("ready{}", suffix),
        crate::domain::agent::AgentStatus::Busy { .. } => format!("busy{}", suffix),
        crate::domain::agent::AgentStatus::Unhealthy { .. } => "unhealthy".to_string(),
    }
}

/// Render the LAUNCHER column for `agent list` — `tmux: deskd-<n> (attached)`
/// or `subprocess: pid <pid>` (or `-`).
fn format_launcher_column(
    state: &agent::AgentState,
    tmux_sessions: &std::collections::HashMap<String, crate::app::tmux_launcher::TmuxStatus>,
) -> String {
    let session = crate::app::tmux_launcher::session_name_for(&state.config.name);
    if let Some(status) = tmux_sessions.get(&session) {
        let suffix = if status.attached {
            "attached"
        } else {
            "detached"
        };
        return format!("tmux: {} ({})", status.name, suffix);
    }
    if state.pid > 0 && std::path::Path::new(&format!("/proc/{}", state.pid)).exists() {
        return format!("subprocess: pid {}", state.pid);
    }
    "-".to_string()
}

/// Resolve the per-agent deskd.yaml path for the tmux launcher commands.
///
/// Order: explicit `--config` > running serve state > `~/<name>/deskd.yaml`.
fn resolve_agent_yaml(name: &str, explicit: Option<&str>) -> Option<String> {
    if let Some(p) = explicit {
        return Some(p.to_string());
    }
    if let Some(state) = config::ServeState::load()
        && let Some(agent) = state.agent(name)
    {
        return Some(agent.config_path.clone());
    }
    if let Ok(state) = agent::load_state(name) {
        if let Some(p) = state.config.config_path.clone() {
            return Some(p);
        }
        // Fall back to the standard workspace location.
        return Some(format!("{}/deskd.yaml", state.config.work_dir));
    }
    None
}

/// Read the agent's `launch_mode:` from its deskd.yaml, if any.
fn read_launch_mode_from_yaml(yaml_path: &str) -> crate::domain::config_types::ConfigLaunchMode {
    use crate::domain::config_types::ConfigLaunchMode;
    let raw = match std::fs::read_to_string(yaml_path) {
        Ok(s) => s,
        Err(_) => return ConfigLaunchMode::default(),
    };
    // The agent's deskd.yaml is a UserConfig; for portability we only peek at
    // the top-level `launch_mode:` field via raw YAML.
    let doc: serde_yaml::Value = match serde_yaml::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return ConfigLaunchMode::default(),
    };
    match doc.get("launch_mode").and_then(|v| v.as_str()) {
        Some("tmux") => ConfigLaunchMode::Tmux,
        _ => ConfigLaunchMode::default(),
    }
}

/// Resolve the agent's home directory (cwd for the tmux session).
fn resolve_agent_home(name: &str) -> Option<std::path::PathBuf> {
    if let Ok(state) = agent::load_state(name) {
        return Some(std::path::PathBuf::from(state.config.work_dir));
    }
    if let Some(state) = config::ServeState::load()
        && let Some(agent) = state.agent(name)
    {
        // The state file maps agent → bus_socket; the home is the socket's parent's parent.
        let bus = std::path::Path::new(&agent.bus_socket);
        if let Some(deskd_dir) = bus.parent()
            && let Some(home) = deskd_dir.parent()
        {
            return Some(home.to_path_buf());
        }
    }
    // Fall back to `$HOME/<name>` then `/home/<name>`.
    if let Some(home_root) = std::env::var_os("HOME") {
        let candidate = std::path::PathBuf::from(home_root).join(name);
        if candidate.is_dir() {
            return Some(candidate);
        }
    }
    let vps_candidate = std::path::PathBuf::from(format!("/home/{}", name));
    if vps_candidate.is_dir() {
        Some(vps_candidate)
    } else {
        None
    }
}

/// `deskd agent start <name> [--tmux]` handler (#452).
///
/// Tmux launch is opt-in. If neither `--tmux` nor `launch_mode: tmux` is set,
/// this command exits non-zero with a message pointing to `deskd serve` (which
/// supervises subprocess agents). This is deliberate — we don't silently fall
/// back to subprocess mode here, since the subprocess path is owned by
/// `deskd serve`, not this command.
fn handle_start(
    name: &str,
    tmux_flag: bool,
    persistent: bool,
    config_arg: Option<&str>,
    log_dir_arg: Option<&str>,
) -> Result<()> {
    use crate::app::tmux_launcher::{
        LaunchTarget, check_tmux_runtime_prereqs, current_user_linger, install_systemd_unit,
        launch_tmux_session, resolve_log_dir,
    };
    use crate::domain::config_types::ConfigLaunchMode;

    // Decide whether tmux is the chosen launcher.
    let yaml_mode = resolve_agent_yaml(name, config_arg)
        .as_deref()
        .map(read_launch_mode_from_yaml)
        .unwrap_or_default();
    let use_tmux = tmux_flag || yaml_mode == ConfigLaunchMode::Tmux;

    if !use_tmux {
        anyhow::bail!(
            "no launcher configured for agent `{}` — pass `--tmux` or set `launch_mode: tmux` in the agent's deskd.yaml. Subprocess agents are supervised by `deskd serve`.",
            name
        );
    }

    if persistent && !use_tmux {
        // Defensive: clap already requires --tmux for --persistent semantically,
        // but the flag is independent so guard the combination here too.
        anyhow::bail!("--persistent requires --tmux (the persistent unit governs a tmux session)");
    }

    let home_dir = resolve_agent_home(name).ok_or_else(|| {
        anyhow::anyhow!(
            "could not resolve home directory for agent `{}` — register it with `deskd agent create` first, or run from a workspace where it is defined",
            name
        )
    })?;

    let target = LaunchTarget {
        name,
        home_dir: home_dir.as_path(),
    };
    check_tmux_runtime_prereqs(&target)?;

    let log_dir_path = match log_dir_arg {
        Some(p) => std::path::PathBuf::from(p),
        None => resolve_log_dir()?,
    };

    let session = launch_tmux_session(&target, &log_dir_path)?;
    println!(
        "started tmux session `{}` (home={}, logs={})",
        session.session_name,
        home_dir.display(),
        session.log_path.display()
    );
    println!("attach with: tmux attach -t {}", session.session_name);

    if persistent {
        // Canonicalise the binary so the unit refers to a stable absolute path
        // (avoids worktree symlinks pointing the unit at a transient location).
        let binary = std::env::current_exe()
            .and_then(|p| p.canonicalize())
            .unwrap_or_else(|_| std::path::PathBuf::from("/usr/local/bin/deskd"));
        let path = install_systemd_unit(name, &binary)?;
        println!("persistent unit installed: deskd-{}.service", name);
        println!("  path: {}", path.display());
        if current_user_linger() == Some(false) {
            println!(
                "Note: linger is disabled. Run `sudo loginctl enable-linger $(whoami)` to enable persistent user services."
            );
        }
    }

    Ok(())
}

/// `deskd agent stop <name>` handler (#452).
///
/// Stops the tmux session for the agent if one exists. Subprocess-launched
/// agents are not touched here — operators continue to use `deskd serve` /
/// `deskd agent restart` for those (the issue is explicit that subprocess
/// behaviour stays unchanged).
fn handle_stop(name: &str, config_arg: Option<&str>, uninstall_unit: bool) -> Result<()> {
    use crate::app::tmux_launcher::{
        kill_tmux_session, session_name_for, systemd_unit_install_path, tmux_session_exists,
        uninstall_systemd_unit,
    };

    let session = session_name_for(name);
    let yaml_mode = resolve_agent_yaml(name, config_arg)
        .as_deref()
        .map(read_launch_mode_from_yaml)
        .unwrap_or_default();
    let session_alive = tmux_session_exists(&session).unwrap_or(false);

    if !session_alive && yaml_mode != crate::domain::config_types::ConfigLaunchMode::Tmux {
        // Even when there's no tmux session, the operator may still want to
        // tear down a leftover unit file. Honor --uninstall-unit in that case;
        // otherwise preserve the original no-action behavior.
        if uninstall_unit {
            let removed = uninstall_systemd_unit(name)?;
            if removed {
                println!("removed systemd user unit: deskd-{}.service", name);
            } else {
                println!(
                    "no systemd user unit installed for `{}` — nothing to remove",
                    name
                );
            }
            return Ok(());
        }
        println!(
            "no tmux session `{}` is running; agent `{}` is not configured for tmux launch — no action",
            session, name
        );
        return Ok(());
    }

    kill_tmux_session(name)?;
    println!("stopped tmux session `{}`", session);

    if uninstall_unit {
        let removed = uninstall_systemd_unit(name)?;
        if removed {
            println!("removed systemd user unit: deskd-{}.service", name);
        } else {
            println!(
                "no systemd user unit installed for `{}` — nothing to remove",
                name
            );
        }
    } else if let Ok(path) = systemd_unit_install_path(name)
        && path.exists()
    {
        println!(
            "tmux session stopped; persistent unit deskd-{}.service is still enabled. \
             Disable with `systemctl --user disable deskd-{}.service` if you want it gone.",
            name, name
        );
    }
    Ok(())
}

/// `deskd agent session` dispatcher.
fn handle_session(action: crate::app::cli::AgentSessionAction) -> Result<()> {
    use crate::app::cli::AgentSessionAction;
    use crate::app::tmux_launcher::{render_systemd_unit, systemd_unit_install_path};
    match action {
        AgentSessionAction::SystemdUnit { name, install } => {
            // current_exe() may return a non-canonical path (e.g. through a
            // worktree symlink); canonicalise so the unit refers to a stable
            // absolute path.
            let binary = std::env::current_exe()
                .and_then(|p| p.canonicalize())
                .unwrap_or_else(|_| std::path::PathBuf::from("/usr/local/bin/deskd"));
            let unit = render_systemd_unit(&name, &binary);
            if install {
                let path = systemd_unit_install_path(&name)?;
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create systemd user dir {}", parent.display())
                    })?;
                }
                std::fs::write(&path, &unit)
                    .with_context(|| format!("failed to write {}", path.display()))?;
                println!("wrote systemd user unit: {}", path.display());
                println!(
                    "enable with: systemctl --user daemon-reload && systemctl --user enable --now deskd-{}.service",
                    name
                );
            } else {
                print!("{}", unit);
            }
            Ok(())
        }
    }
}

/// Restart one or all agents and wait for them to return to ready.
///
/// Per-target work is delegated to `restart_one_agent`, which calls into the
/// shared `mcp_service::perform_agent_restart` helper used by the bus API
/// (so the kill-and-mutate logic lives in one place). Exit code is non-zero
/// if any agent fails to return to ready in time. See
/// `restart_one_agent`'s doc-comment for the rationale on bypassing the bus.
async fn restart_agents(
    name: Option<String>,
    all: bool,
    fresh_session: bool,
    timeout_secs: u64,
) -> Result<()> {
    if all && name.is_some() {
        anyhow::bail!("pass either <name> or --all, not both");
    }
    if !all && name.is_none() {
        anyhow::bail!("missing agent name (or pass --all)");
    }

    let targets: Vec<String> = if all {
        let agents = agent::list().await?;
        if agents.is_empty() {
            println!("No agents registered");
            return Ok(());
        }
        agents.into_iter().map(|s| s.config.name).collect()
    } else {
        vec![name.unwrap()]
    };

    let mut had_failure = false;
    for target in &targets {
        match restart_one_agent(target, fresh_session, timeout_secs).await {
            Ok(()) => {}
            Err(e) => {
                eprintln!("agent restart {}: {}", target, e);
                had_failure = true;
            }
        }
    }

    if had_failure {
        anyhow::bail!("one or more agents failed to return to ready");
    }
    Ok(())
}

/// Restart a single agent and poll for `idle` within `timeout_secs`.
///
/// # Design: why the CLI bypasses the bus
///
/// The CLI intentionally does NOT route the restart through the bus
/// (`bus_api::handle_agent_restart`). The motivation is the recovery
/// scenario described in kgatilin/deskd#423: an operator runs `deskd agent
/// restart <name>` precisely *because* the agent (and possibly the bus
/// itself) is hung. Sending an RPC over a hung bus would block forever and
/// defeat the purpose of the command.
///
/// To avoid maintaining two copies of the kill-and-mutate logic, both the
/// CLI and the bus handler share `mcp_service::perform_agent_restart`,
/// which performs the load/kill/save cycle without touching the bus. The
/// CLI then layers polling and user-facing feedback on top.
async fn restart_one_agent(name: &str, fresh_session: bool, timeout_secs: u64) -> Result<()> {
    use std::time::Duration;

    // ── Tmux launch mode: tear down + re-provision + re-launch (#504) ────
    //
    // The subprocess restart path below SIGTERMs a child pid that doesn't
    // exist for tmux agents — the REPL lives inside a detached tmux
    // session, not under deskd's supervision tree. Branch here so
    // operators get the right behaviour from `deskd agent restart <name>`.
    let launch_mode = agent::load_state(name)
        .map(|s| s.config.launch_mode)
        .unwrap_or_default();
    if launch_mode == crate::domain::config_types::ConfigLaunchMode::Tmux {
        crate::app::tmux_launcher::kill_tmux_session(name)?;
        // Allow tmux a brief beat to drop the session entry before we
        // probe again on launch.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let state = agent::load_state(name)?;
        let work_dir = std::path::PathBuf::from(&state.config.work_dir);
        let bus_path = std::path::PathBuf::from(config::agent_bus_socket(&state.config.work_dir));
        let outcome =
            crate::app::agent_process::ensure_tmux_launched(name, &work_dir, &bus_path).await?;

        if fresh_session {
            // For tmux agents `session_id` is owned by the claude REPL
            // inside tmux. We still honour --fresh-session by clearing
            // the locally-cached id so the next inbound MCP turn picks a
            // new id and the state-file numbers reset.
            if let Ok(mut s) = agent::load_state(name) {
                s.session_id.clear();
                s.session_cost = 0.0;
                s.session_turns = 0;
                s.session_start = None;
                let _ = agent::save_state_pub(&s);
            }
        }
        if let Ok(mut s) = agent::load_state(name) {
            s.tmux_session = Some(outcome.session_name.clone());
            s.tmux_log_path = Some(outcome.log_path.display().to_string());
            s.pid = 0;
            let _ = agent::save_state_pub(&s);
        }
        println!(
            "agent {}: tmux session re-launched as `{}` (logs={})",
            name,
            outcome.session_name,
            outcome.log_path.display()
        );
        return Ok(());
    }

    // Shared with bus_api::handle_agent_restart — see the doc-comment above
    // for why we deliberately do not dispatch through the bus.
    let outcome = crate::app::mcp_service::perform_agent_restart(name, fresh_session).await?;

    if outcome.fresh_session {
        info!(agent = %name, "cleared session_id (--fresh-session)");
    }

    if outcome.previous_pid > 0
        && std::path::Path::new(&format!("/proc/{}", outcome.previous_pid)).exists()
    {
        // perform_agent_restart already SIGTERM'd it; if /proc still shows
        // the pid here, the kernel is still tearing it down — log for the
        // user but no action needed.
        println!(
            "agent {}: sent SIGTERM to pid {}",
            name, outcome.previous_pid
        );
    } else if outcome.previous_pid == 0 {
        // Already stopped — restart still succeeds (no-op kill, supervisor
        // will respawn on next task).
        println!("agent {}: already stopped (no live pid)", name);
    } else {
        println!(
            "agent {}: stopped pid {} (exited)",
            name, outcome.previous_pid
        );
    }

    // Poll the state file for status=idle (ready). The worker loop sets
    // status to "idle" via set_idle() once it has finished its current task
    // and is waiting on the bus.
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Ok(s) = agent::load_state(name) {
            // "idle" or "ready" both count as ready. "working" / "restarting"
            // do not.
            if s.status == "idle" || s.status == "ready" {
                println!("agent {}: ready", name);
                return Ok(());
            }
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "agent '{}' did not return to ready within {}s",
                name,
                timeout_secs
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Parse a stream-json line and print a human-readable summary.
fn print_stream_event(line: &str) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        return;
    };
    match v.get("type").and_then(|t| t.as_str()) {
        Some("assistant") => {
            if let Some(blocks) = v
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_array())
            {
                for block in blocks {
                    match block.get("type").and_then(|t| t.as_str()) {
                        Some("text") => {
                            if let Some(text) = block.get("text").and_then(|t| t.as_str())
                                && !text.is_empty()
                            {
                                println!("[assistant] {}", truncate(text, 200));
                            }
                        }
                        Some("tool_use") => {
                            let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                            let input = block
                                .get("input")
                                .map(|i| {
                                    let s = i.to_string();
                                    truncate(&s, 120).to_string()
                                })
                                .unwrap_or_default();
                            println!("[tool_use] {} {}", name, input);
                        }
                        _ => {}
                    }
                }
            }
        }
        Some("tool") | Some("tool_result") => {
            let tool_id = v
                .get("content")
                .and_then(|c| c.as_array())
                .and_then(|a| a.first())
                .and_then(|b| b.get("text"))
                .and_then(|t| t.as_str())
                .unwrap_or("");
            let is_error = v
                .get("content")
                .and_then(|c| c.as_array())
                .and_then(|a| a.first())
                .and_then(|b| b.get("is_error"))
                .and_then(|e| e.as_bool())
                .unwrap_or(false);
            if is_error {
                println!("[tool_error] {}", truncate(tool_id, 200));
            } else if !tool_id.is_empty() {
                println!("[tool_result] {}", truncate(tool_id, 200));
            }
        }
        Some("result") => {
            let cost = v
                .get("total_cost_usd")
                .and_then(|c| c.as_f64())
                .unwrap_or(0.0);
            let turns = v.get("num_turns").and_then(|t| t.as_u64()).unwrap_or(0);
            println!("[result] turns={} cost=${:.4}", turns, cost);
        }
        Some(other) => {
            println!("[{}]", other);
        }
        None => {}
    }
}

fn print_inbox_message(msg: &unified_inbox::InboxMessage) {
    let ts = msg.ts.format("%Y-%m-%dT%H:%M:%S").to_string();
    let from = msg.from.as_deref().unwrap_or(&msg.source);
    println!("─── {} [{}] ───", from, ts);
    println!("{}", msg.text);
    println!();
}

/// Resolve the workspace.yaml path for `deskd agent set`:
/// explicit flag > running serve state > error.
fn resolve_workspace_path(explicit: Option<String>) -> Result<String> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(state) = config::ServeState::load() {
        return Ok(state.workspace_config);
    }
    anyhow::bail!("no --config provided and deskd serve is not running (no serve state found)")
}

/// Edit `workspace.yaml` in place: set the named agent's `container:` field
/// to the string `profile_name`. Validates that the profile is defined under
/// the top-level `containers:` map and that the agent exists.
pub(crate) fn set_agent_container(
    workspace_path: &str,
    agent_name: &str,
    profile_name: &str,
) -> Result<()> {
    let raw = std::fs::read_to_string(workspace_path).map_err(|e| {
        anyhow::anyhow!("failed to read workspace config {}: {}", workspace_path, e)
    })?;
    let mut doc: serde_yaml::Value = serde_yaml::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("failed to parse workspace yaml: {}", e))?;

    // Validate profile exists in top-level containers map.
    let profile_keys: Vec<String> = doc
        .get("containers")
        .and_then(|v| v.as_mapping())
        .map(|m| {
            m.keys()
                .filter_map(|k| k.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    if !profile_keys.iter().any(|k| k == profile_name) {
        let available = if profile_keys.is_empty() {
            "<none defined>".to_string()
        } else {
            profile_keys.join(", ")
        };
        anyhow::bail!(
            "container profile '{}' not defined (available: {})",
            profile_name,
            available
        );
    }

    // Find the named agent in agents[] and overwrite its `container:` field
    // with the profile name string.
    let agents = doc
        .get_mut("agents")
        .and_then(|v| v.as_sequence_mut())
        .ok_or_else(|| anyhow::anyhow!("workspace yaml has no `agents:` list"))?;

    let mut updated = false;
    for agent in agents.iter_mut() {
        let name_matches = agent
            .get("name")
            .and_then(|v| v.as_str())
            .map(|n| n == agent_name)
            .unwrap_or(false);
        if !name_matches {
            continue;
        }
        let mapping = agent
            .as_mapping_mut()
            .ok_or_else(|| anyhow::anyhow!("agent entry is not a mapping"))?;
        mapping.insert(
            serde_yaml::Value::String("container".to_string()),
            serde_yaml::Value::String(profile_name.to_string()),
        );
        updated = true;
        break;
    }
    if !updated {
        anyhow::bail!(
            "agent '{}' not found in {} (check workspace.yaml `agents:` list)",
            agent_name,
            workspace_path
        );
    }

    let serialized = serde_yaml::to_string(&doc)
        .map_err(|e| anyhow::anyhow!("failed to re-serialize workspace yaml: {}", e))?;
    std::fs::write(workspace_path, serialized).map_err(|e| {
        anyhow::anyhow!("failed to write workspace config {}: {}", workspace_path, e)
    })?;
    info!(
        agent = agent_name,
        profile = profile_name,
        path = workspace_path,
        "updated container profile"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a unique temp dir under std::env::temp_dir() (no tempfile crate).
    /// Caller is responsible for cleanup; tests below remove it via `drop_dir`.
    struct TmpDir(std::path::PathBuf);
    impl TmpDir {
        fn new(tag: &str) -> Self {
            let id = uuid::Uuid::new_v4().to_string();
            let p = std::env::temp_dir().join(format!("deskd-set-{}-{}", tag, id));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_tmp(path: &std::path::Path, content: &str) {
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn set_agent_container_updates_string_ref() {
        let tmp = TmpDir::new("string-ref");
        let path = tmp.path().join("workspace.yaml");
        let yaml = r#"containers:
  personal:
    image: claude-personal
  work:
    image: claude-work
agents:
  - name: uagent
    work_dir: /home/uagent
    container: personal
"#;
        write_tmp(&path, yaml);
        set_agent_container(path.to_str().unwrap(), "uagent", "work").unwrap();
        let after = std::fs::read_to_string(&path).unwrap();
        let doc: serde_yaml::Value = serde_yaml::from_str(&after).unwrap();
        let container = doc["agents"][0]["container"].as_str().unwrap();
        assert_eq!(container, "work");
    }

    #[test]
    fn set_agent_container_replaces_inline_with_named_ref() {
        let tmp = TmpDir::new("inline-replace");
        let path = tmp.path().join("workspace.yaml");
        let yaml = r#"containers:
  work:
    image: claude-work
agents:
  - name: uagent
    work_dir: /home/uagent
    container:
      image: inline-image
      env:
        FOO: bar
"#;
        write_tmp(&path, yaml);
        set_agent_container(path.to_str().unwrap(), "uagent", "work").unwrap();
        let after = std::fs::read_to_string(&path).unwrap();
        let doc: serde_yaml::Value = serde_yaml::from_str(&after).unwrap();
        // After update, container is a string reference.
        let container = doc["agents"][0]["container"].as_str().unwrap();
        assert_eq!(container, "work");
    }

    #[test]
    fn set_agent_container_unknown_profile_errors() {
        let tmp = TmpDir::new("unknown-profile");
        let path = tmp.path().join("workspace.yaml");
        let yaml = r#"containers:
  personal:
    image: claude-personal
agents:
  - name: uagent
    work_dir: /home/uagent
    container: personal
"#;
        write_tmp(&path, yaml);
        let err = set_agent_container(path.to_str().unwrap(), "uagent", "ghost").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ghost"));
        assert!(msg.contains("personal"));
    }

    #[test]
    fn set_agent_container_unknown_agent_errors() {
        let tmp = TmpDir::new("unknown-agent");
        let path = tmp.path().join("workspace.yaml");
        let yaml = r#"containers:
  work:
    image: claude-work
agents:
  - name: uagent
    work_dir: /home/uagent
    container: work
"#;
        write_tmp(&path, yaml);
        let err = set_agent_container(path.to_str().unwrap(), "missing", "work").unwrap_err();
        assert!(err.to_string().contains("missing"));
    }

    #[test]
    fn set_agent_container_no_profiles_section_errors() {
        let tmp = TmpDir::new("no-profiles");
        let path = tmp.path().join("workspace.yaml");
        let yaml = r#"agents:
  - name: uagent
    work_dir: /home/uagent
    container:
      image: foo
"#;
        write_tmp(&path, yaml);
        let err = set_agent_container(path.to_str().unwrap(), "uagent", "work").unwrap_err();
        assert!(err.to_string().contains("not defined"));
    }
}
