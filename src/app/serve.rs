//! `deskd serve` — start per-agent buses, workers, adapters, and schedules.

use anyhow::Result;
use tracing::info;

use crate::app::adapters::federation;
use crate::app::adapters::web;
use crate::app::metrics;
use crate::app::reload_state::ReloadState;
use crate::app::{agent, alerts, bus, bus_api, config_reload, worker, workflow};
use crate::config;
use crate::infra::diag;

/// Start per-agent buses and workers for all agents in workspace config.
/// Each agent has its own isolated bus at {work_dir}/.deskd/bus.sock.
pub async fn serve(config_path: String) -> Result<()> {
    let workspace = config::WorkspaceConfig::load(&config_path)?;
    info!(path = %config_path, agents = workspace.agents.len(), "loaded workspace config");

    if workspace.agents.is_empty() {
        tracing::warn!("No agents defined in workspace config");
    }

    // Write serve state so other commands can auto-discover config.
    let mut serve_state = config::ServeState {
        workspace_config: std::fs::canonicalize(&config_path)
            .unwrap_or_else(|_| config_path.clone().into())
            .to_string_lossy()
            .into_owned(),
        started_at: chrono::Utc::now().to_rfc3339(),
        agents: std::collections::HashMap::new(),
        rooms: workspace.rooms.clone(),
    };
    for def in &workspace.agents {
        serve_state.agents.insert(
            def.name.clone(),
            config::AgentServeState {
                work_dir: def.work_dir.clone(),
                bus_socket: def.bus_socket(),
                config_path: def.config_path(),
            },
        );
    }
    if let Err(e) = serve_state.save() {
        let bus_for_diag = workspace
            .agents
            .first()
            .map(|a| a.bus_socket())
            .unwrap_or_default();
        diag::warn_event(
            Some(&bus_for_diag),
            "supervisor",
            "state.write_failed",
            format!("failed to write serve state: {}", e),
            serde_json::json!({}),
        );
    }

    for def in &workspace.agents {
        let cfg_path = def.config_path();
        let user_cfg = config::UserConfig::load(&cfg_path).ok();
        if user_cfg.is_some() {
            info!(agent = %def.name, config = %cfg_path, "loaded user config");
        } else {
            info!(agent = %def.name, "no user config at {}, using defaults", cfg_path);
        }

        let state = agent::create_or_recover(def, user_cfg.as_ref()).await?;
        let name = state.config.name.clone();
        let bus_socket = def.bus_socket();
        // Per-agent shared reload state — drives `deskd reload` (#474).
        // Shared between the bus API (RPC handler) and the config_reload
        // watcher (consumer of triggers, producer of last_reload_at).
        let reload_state = ReloadState::new();

        // Ensure {work_dir}/.deskd/ exists and is owned by the agent's unix user.
        let bus_dir = std::path::Path::new(&def.work_dir).join(".deskd");
        config::ensure_dir_owned(&bus_dir, def.unix_user.as_deref())?;

        // ── Session-persistent components (NOT restarted on config change) ──

        // Start the agent's isolated bus.
        {
            let bus = bus_socket.clone();
            let agent_name = name.clone();
            tokio::spawn(async move {
                if let Err(e) = bus::serve(&bus).await {
                    diag::error_event(
                        Some(&bus),
                        "supervisor",
                        "bus.serve_failed",
                        format!("bus failed: {}", e),
                        serde_json::json!({
                            "agent": agent_name,
                            "socket": bus,
                        }),
                    );
                }
            });
        }
        info!(agent = %name, bus = %bus_socket, "started agent bus");

        // Start bus API handler for TUI / external clients.
        {
            let bus = bus_socket.clone();
            let agent_name = name.clone();
            let ucfg_clone = user_cfg.clone();
            let cfg_path_clone = cfg_path.clone();
            let bus_api_reload_state = reload_state.clone();
            tokio::spawn(async move {
                let task_store = crate::app::task::TaskStore::default_for_home();
                let sm_store = crate::app::statemachine::StateMachineStore::default_for_home();
                if let Err(e) = bus_api::run(
                    &bus,
                    &task_store,
                    &sm_store,
                    ucfg_clone.as_ref(),
                    &agent_name,
                    &cfg_path_clone,
                    bus_api_reload_state,
                )
                .await
                {
                    diag::error_event(
                        Some(&bus),
                        "supervisor",
                        "bus_api.exited",
                        format!("bus_api exited: {}", e),
                        serde_json::json!({ "agent": agent_name }),
                    );
                }
            });
            info!(agent = %name, "started bus API handler");
        }

        // Start worker on the agent's bus.
        {
            let bus = bus_socket.clone();
            let worker_name = name.clone();
            let worker_task_store = crate::app::task::TaskStore::default_for_home();
            tokio::spawn(async move {
                if let Err(e) = worker::run(
                    &worker_name,
                    &bus,
                    Some(bus.clone()),
                    None,
                    &worker_task_store,
                )
                .await
                {
                    diag::error_event(
                        Some(&bus),
                        "supervisor",
                        "worker.exited",
                        format!("worker exited with error: {}", e),
                        serde_json::json!({ "agent": worker_name }),
                    );
                }
            });
        }

        // Start workflow engine if models are defined.
        if let Some(ref ucfg) = user_cfg
            && !ucfg.models.is_empty()
        {
            let bus = bus_socket.clone();
            let models: Vec<crate::domain::statemachine::ModelDef> = ucfg
                .models
                .iter()
                .cloned()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, String>>()
                .unwrap_or_else(|e| {
                    diag::error_event(
                        Some(&bus_socket),
                        "supervisor",
                        "config.invalid_model",
                        format!("invalid model definition: {}", e),
                        serde_json::json!({ "agent": def.name }),
                    );
                    vec![]
                });
            let agent_name = def.name.clone();

            // Start timeout sweep loop alongside the workflow engine.
            let sweep_models = models.clone();
            let sweep_interval = std::time::Duration::from_secs(30);
            let sweep_bus = bus.clone();
            tokio::spawn(async move {
                crate::app::timeout_sweep::run_timeout_sweep(
                    sweep_models,
                    sweep_interval,
                    sweep_bus,
                )
                .await;
            });
            info!(agent = %def.name, "started timeout sweep loop (interval=30s)");

            let sm_store = crate::app::statemachine::StateMachineStore::default_for_home();
            let task_store = crate::app::task::TaskStore::default_for_home();
            tokio::spawn(async move {
                let bus_client = match crate::app::bus::connect_bus(&bus).await {
                    Ok(b) => b,
                    Err(e) => {
                        diag::error_event(
                            Some(&bus),
                            "supervisor",
                            "workflow.bus_connect_failed",
                            format!("workflow engine failed to connect to bus: {}", e),
                            serde_json::json!({ "agent": agent_name }),
                        );
                        return;
                    }
                };
                if let Err(e) = workflow::run(&bus_client, models, &sm_store, &task_store).await {
                    diag::error_event(
                        Some(&bus),
                        "supervisor",
                        "workflow.exited",
                        format!("workflow engine exited: {}", e),
                        serde_json::json!({ "agent": agent_name }),
                    );
                }
            });
            info!(agent = %def.name, models = ucfg.models.len(), "started workflow engine");
        }

        // ── Restartable components (hot-reloaded on config change) ───────

        // Spawn initial restartable components.
        let components = config_reload::spawn_components(
            def,
            user_cfg.as_ref(),
            &workspace.admin_telegram_ids,
            &bus_socket,
            &name,
            &cfg_path,
        )
        .await?;

        let initial_summary = components.summary();
        info!(agent = %name, summary = %initial_summary, "started restartable components");

        // Start the unified config reload watcher.
        {
            let reload_def = def.clone();
            let reload_admin_ids = workspace.admin_telegram_ids.clone();
            let reload_bus = bus_socket.clone();
            let reload_name = name.clone();
            let reload_cfg = cfg_path.clone();
            let watcher_reload_state = reload_state.clone();
            tokio::spawn(async move {
                config_reload::watch_and_reload(
                    reload_def,
                    components,
                    reload_admin_ids,
                    reload_bus,
                    reload_name,
                    reload_cfg,
                    watcher_reload_state,
                )
                .await;
            });
            info!(agent = %name, "started unified config reload watcher");
        }
    }

    // ── Proactive degradation alerts (#425) ──────────────────────────────
    // When workspace.yaml defines an `alerts:` block, start a single poll
    // loop that polls a verdict source and dispatches transition alerts to
    // every configured sink. The verdict source is currently a placeholder
    // pending #422; sink + dedup machinery is fully exercised regardless.
    if let Some(alerts_cfg) = workspace.alerts.clone() {
        if alerts_cfg.sinks.is_empty() {
            info!("alerts config present but no sinks defined — skipping alert manager");
        } else if let Some(any_socket) = workspace.agents.first().map(|a| a.bus_socket()) {
            let agents_for_source: Vec<(String, std::path::PathBuf)> = workspace
                .agents
                .iter()
                .map(|a| {
                    (
                        a.name.clone(),
                        std::path::PathBuf::from(&a.work_dir)
                            .join(".deskd")
                            .join("usage.jsonl"),
                    )
                })
                .collect();
            let manager = alerts::AlertManager::from_config(&alerts_cfg, &any_socket, "deskd");
            let source = alerts::HeuristicVerdictSource::new(agents_for_source);
            let interval = std::time::Duration::from_secs(alerts_cfg.poll_interval_secs.max(1));
            let diag_bus = any_socket.clone();
            tokio::spawn(async move {
                run_alerts_loop(manager, source, interval, diag_bus).await;
            });
            info!(
                sinks = alerts_cfg.sinks.len(),
                interval_secs = alerts_cfg.poll_interval_secs,
                "started alert manager"
            );
        } else {
            info!("alerts config present but no agents — skipping alert manager");
        }
    }

    // ── Disk metrics collector (#446) ────────────────────────────────────
    // Runs independently of the web adapter — disabling `web:` does not
    // disable metrics. The collector is shared with the web adapter via
    // `DiskMetrics`, so we have to spin it up before the web block.
    let metrics_cfg = metrics::CollectorConfig::from_workspace(&workspace);
    let disk_metrics = metrics::DiskMetrics::new(metrics::default_cache_path());
    let metrics_bus_socket = workspace.agents.first().map(|a| a.bus_socket());
    {
        let collector_cancel = tokio_util::sync::CancellationToken::new();
        metrics::spawn_collector(
            disk_metrics.clone(),
            metrics_cfg.clone(),
            metrics_bus_socket.clone(),
            collector_cancel.clone(),
        );
        info!(
            interval_secs = metrics_cfg.interval.as_secs(),
            volumes = metrics_cfg.volumes.len(),
            agents = metrics_cfg.agents.len(),
            "started disk metrics collector"
        );
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            collector_cancel.cancel();
        });
    }

    // ── Web control panel (#443) ──────────────────────────────────────────
    // When workspace.yaml defines `web:` with `enabled: true`, start a single
    // axum HTTP server. The dispatch bus socket is taken from the first agent
    // — every Telegram adapter subscribes to `telegram.out:*` on its agent
    // bus, so any reachable agent bus suffices for dispatching the magic-link
    // message. If no agents are defined, the web adapter cannot dispatch
    // links and is skipped with a warning.
    if let Some(web_cfg) = workspace.web.clone() {
        if !web_cfg.enabled {
            info!("web adapter config present but disabled — skipping");
        } else if let Some(bus_socket) = workspace.agents.first().map(|a| a.bus_socket()) {
            let cancel = tokio_util::sync::CancellationToken::new();
            let cancel_handle = cancel.clone();
            let bind = web_cfg.bind.clone();
            let github_webhooks = workspace.github_webhooks.clone();
            let metrics_for_web = disk_metrics.clone();
            let agent_homes_for_web = metrics_cfg.agents.clone();
            let cost_for_web = workspace.cost.clone();
            tokio::spawn(async move {
                if let Err(e) = web::run(
                    web_cfg,
                    bus_socket.clone(),
                    github_webhooks,
                    metrics_for_web,
                    agent_homes_for_web,
                    cost_for_web,
                    cancel_handle,
                )
                .await
                {
                    diag::error_event(
                        Some(&bus_socket),
                        "web",
                        "adapter.exited",
                        format!("web adapter exited: {}", e),
                        serde_json::json!({}),
                    );
                }
            });
            info!(bind = %bind, "started web adapter");
            // Cancel on Ctrl-C alongside the rest of the shutdown.
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                cancel.cancel();
            });
        } else {
            tracing::warn!(
                "web adapter enabled but no agents defined — cannot dispatch magic links"
            );
        }
    }

    // ── Federation (#462) ─────────────────────────────────────────────────
    // Foundation only — hub binds, peer dials, hello/welcome + keepalive.
    // Bus routing is intentionally not wired (lands in #463). When both
    // `federation.hub.enabled` and `federation.peer.enabled` are false (or
    // the block is absent) this is a complete no-op.
    if let Some(fed_cfg) = workspace.federation.clone() {
        let hub_handle = if let Some(hub) = fed_cfg.hub.clone() {
            if hub.enabled {
                let cancel = tokio_util::sync::CancellationToken::new();
                let cancel_clone = cancel.clone();
                let registry_path = federation::PeerRegistry::default_path();
                match federation::PeerRegistry::open(&registry_path) {
                    Ok(reg) => {
                        let registry = std::sync::Arc::new(reg);
                        let identity = federation::hub::shared_identity_from_cli();
                        let hub_name = hostname_or("deskd-hub");
                        let cfg = federation::hub::HubConfig {
                            bind: hub.bind.clone(),
                            hub_name,
                            tailscale_binary: "tailscale".into(),
                            peer_timeout_secs: hub.peer_timeout_secs,
                        };
                        let bind = hub.bind.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                federation::run_hub(cfg, identity, registry, cancel_clone).await
                            {
                                tracing::error!(error = %e, "federation hub exited");
                            }
                        });
                        info!(bind = %bind, "started federation hub");
                        Some(cancel)
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            path = %registry_path.display(),
                            "federation hub disabled — failed to open peer registry"
                        );
                        None
                    }
                }
            } else {
                info!("federation.hub present but disabled — skipping");
                None
            }
        } else {
            None
        };

        let peer_handle = if let Some(peer) = fed_cfg.peer.clone() {
            if peer.enabled {
                let cancel = tokio_util::sync::CancellationToken::new();
                let cancel_clone = cancel.clone();
                let cfg = federation::peer::PeerDialerConfig {
                    hub_addr: peer.hub_addr.clone(),
                    peer_name: peer.peer_name.clone(),
                    reconnect_backoff_secs: peer.reconnect_backoff_secs.clone(),
                    deskd_version: env!("CARGO_PKG_VERSION").to_string(),
                    subscribe_patterns: peer.subscribe.clone(),
                    subscribe_inboxes: peer.subscribe_inboxes.clone(),
                };
                tokio::spawn(async move {
                    if let Err(e) = federation::run_peer(cfg, cancel_clone).await {
                        tracing::error!(error = %e, "federation peer exited");
                    }
                });
                info!(hub = %peer.hub_addr, peer = %peer.peer_name, "started federation peer dialer");
                Some(cancel)
            } else {
                info!("federation.peer present but disabled — skipping");
                None
            }
        } else {
            None
        };

        // Plumb cancellation through Ctrl-C alongside the rest of shutdown.
        if hub_handle.is_some() || peer_handle.is_some() {
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                if let Some(h) = hub_handle {
                    h.cancel();
                }
                if let Some(p) = peer_handle {
                    p.cancel();
                }
            });
        }
    }

    info!("all agents started — press Ctrl-C to stop");

    tokio::signal::ctrl_c().await?;
    config::ServeState::remove();
    info!("shutting down");
    Ok(())
}

fn hostname_or(default: &str) -> String {
    std::env::var("HOSTNAME")
        .ok()
        .unwrap_or_else(|| default.to_string())
}

/// Run a single poll-and-dispatch loop for the alert manager.
/// Exits only when the deskd serve process stops.
async fn run_alerts_loop<S>(
    manager: alerts::AlertManager,
    source: S,
    interval: std::time::Duration,
    bus_socket: String,
) where
    S: alerts::VerdictSource,
{
    if manager.is_empty() {
        return;
    }
    let mut ticker = tokio::time::interval(interval);
    // First tick fires immediately by default; skip it so the source sees a
    // somewhat-warm runtime before the first poll.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        match source.poll().await {
            Ok(reports) => manager.observe(reports).await,
            Err(e) => {
                diag::warn_event(
                    Some(&bus_socket),
                    "alerts",
                    "verdict.poll_failed",
                    format!("alert verdict source poll failed: {}", e),
                    serde_json::json!({}),
                );
            }
        }
    }
}

/// Query which agents are currently connected to a bus socket.
pub async fn query_live_agents(socket: &str) -> anyhow::Result<std::collections::HashSet<String>> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    if !std::path::Path::new(socket).exists() {
        return Ok(Default::default());
    }

    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|e| anyhow::anyhow!("connect: {}", e))?;

    let reg =
        serde_json::json!({"type": "register", "name": "deskd-cli-list", "subscriptions": []});
    let mut line = serde_json::to_string(&reg)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;

    let list_req = serde_json::json!({"type": "list"});
    let mut req_line = serde_json::to_string(&list_req)?;
    req_line.push('\n');
    stream.write_all(req_line.as_bytes()).await?;

    let (reader, _) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let timeout = tokio::time::Duration::from_secs(2);
    let result = tokio::time::timeout(timeout, async {
        while let Some(l) = lines.next_line().await? {
            let v: serde_json::Value = serde_json::from_str(&l)?;
            if v.get("type").and_then(|t| t.as_str()) == Some("list_response")
                && let Some(arr) = v.get("clients").and_then(|c| c.as_array())
            {
                return Ok::<_, anyhow::Error>(
                    arr.iter()
                        .filter_map(|n| n.as_str())
                        .map(|s| s.to_string())
                        .collect(),
                );
            }
        }
        Ok(Default::default())
    })
    .await;

    result.unwrap_or(Ok(Default::default()))
}
