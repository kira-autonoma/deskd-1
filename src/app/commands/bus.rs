//! `deskd bus` subcommand handlers.

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::info;

use crate::app::cli::BusAction;
use crate::config;

pub async fn handle(action: BusAction) -> Result<()> {
    match action {
        BusAction::Api {
            socket,
            config: config_opt,
            agent,
        } => {
            return handle_api(socket, config_opt, agent).await;
        }
        BusAction::Subscribe {
            patterns,
            socket,
            name,
        } => {
            return handle_subscribe(patterns, socket, name).await;
        }
        BusAction::Status { socket } => {
            if !std::path::Path::new(&socket).exists() {
                println!("Bus is not running (socket not found: {})", socket);
                return Ok(());
            }

            let mut stream = UnixStream::connect(&socket)
                .await
                .map_err(|e| anyhow::anyhow!("Cannot connect to bus at {}: {}", socket, e))?;

            // Register as temporary client.
            let reg = serde_json::json!({
                "type": "register",
                "name": "deskd-bus-status",
                "subscriptions": []
            });
            let mut line = serde_json::to_string(&reg)?;
            line.push('\n');
            stream.write_all(line.as_bytes()).await?;

            // Request client list.
            let list_req = serde_json::json!({"type": "list"});
            let mut req_line = serde_json::to_string(&list_req)?;
            req_line.push('\n');
            stream.write_all(req_line.as_bytes()).await?;

            // Read response with timeout.
            let (reader, _) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();

            let timeout = tokio::time::Duration::from_secs(3);
            let result = tokio::time::timeout(timeout, async {
                while let Some(l) = lines.next_line().await? {
                    let v: serde_json::Value = serde_json::from_str(&l)?;

                    // Check for list_response in payload (comes as a bus message).
                    let payload = if v.get("type").and_then(|t| t.as_str()) == Some("list_response")
                    {
                        v
                    } else if let Some(p) = v.get("payload") {
                        if p.get("type").and_then(|t| t.as_str()) == Some("list_response") {
                            p.clone()
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    };

                    return Ok::<_, anyhow::Error>(Some(payload));
                }
                Ok(None)
            })
            .await;

            let payload = match result {
                Ok(Ok(Some(p))) => p,
                Ok(Ok(None)) => {
                    println!("Bus is running but returned no client list.");
                    return Ok(());
                }
                Ok(Err(e)) => {
                    anyhow::bail!("Error reading bus response: {}", e);
                }
                Err(_) => {
                    println!("Bus is running but did not respond in time.");
                    return Ok(());
                }
            };

            println!("Bus:    running");
            println!("Socket: {}", socket);
            println!();

            // Use detailed client info if available, fall back to names.
            if let Some(detail) = payload.get("clients_detail").and_then(|d| d.as_array()) {
                let client_count = detail.len();
                // Exclude our own status query client.
                let clients: Vec<_> = detail
                    .iter()
                    .filter(|c| c.get("name").and_then(|n| n.as_str()) != Some("deskd-bus-status"))
                    .collect();

                println!("Clients: {} connected", clients.len());
                if clients.is_empty() {
                    return Ok(());
                }
                println!();
                println!("{:<25} SUBSCRIPTIONS", "NAME");
                for c in &clients {
                    let name = c.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
                    let subs: Vec<&str> = c
                        .get("subscriptions")
                        .and_then(|s| s.as_array())
                        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                        .unwrap_or_default();
                    let subs_str = if subs.is_empty() {
                        "-".to_string()
                    } else {
                        subs.join(", ")
                    };
                    println!("{:<25} {}", name, subs_str);
                }

                // Show count excluding ourselves.
                if client_count > clients.len() + 1 {
                    println!(
                        "\n({} internal clients hidden)",
                        client_count - clients.len() - 1
                    );
                }
            } else if let Some(clients) = payload.get("clients").and_then(|c| c.as_array()) {
                let names: Vec<&str> = clients
                    .iter()
                    .filter_map(|n| n.as_str())
                    .filter(|n| *n != "deskd-bus-status")
                    .collect();
                println!("Clients: {} connected", names.len());
                if !names.is_empty() {
                    println!();
                    println!("NAME");
                    for name in &names {
                        println!("{}", name);
                    }
                }
            } else {
                println!("Clients: unknown (unexpected response format)");
            }
        }
    }
    Ok(())
}

/// Resolve the bus socket path from: explicit flag > serve state > error.
fn resolve_bus_socket(explicit: Option<String>) -> Result<String> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(state) = config::ServeState::load()
        && let Some(agent) = state.find_agent_config()
    {
        return Ok(agent.bus_socket.clone());
    }
    anyhow::bail!(
        "no --socket provided, $DESKD_BUS_SOCKET not set, and no running serve state found"
    )
}

/// Resolve the agent config (deskd.yaml) path from: explicit flag > serve state > None.
fn resolve_config(explicit: Option<String>) -> Option<String> {
    if let Some(path) = explicit {
        return Some(path);
    }
    config::ServeState::load()
        .and_then(|state| state.find_agent_config().map(|a| a.config_path.clone()))
}

/// Subscribe to one or more bus topics and emit each received message as a
/// single JSON line on stdout. Runs until the bus connection drops or the
/// process is interrupted.
async fn handle_subscribe(
    patterns: Vec<String>,
    socket: Option<String>,
    name: String,
) -> Result<()> {
    let bus_socket = resolve_bus_socket(socket)?;

    if !std::path::Path::new(&bus_socket).exists() {
        anyhow::bail!("Bus socket not found: {}", bus_socket);
    }

    let mut stream = UnixStream::connect(&bus_socket)
        .await
        .map_err(|e| anyhow::anyhow!("Cannot connect to bus at {}: {}", bus_socket, e))?;

    let reg = serde_json::json!({
        "type": "register",
        "name": name,
        "subscriptions": patterns,
    });
    let mut line = serde_json::to_string(&reg)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;

    info!(
        socket = %bus_socket,
        client = %name,
        patterns = ?patterns,
        "subscribed; tailing messages as JSONL"
    );

    let (reader, _writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(l) = lines.next_line().await? {
        let trimmed = l.trim();
        if trimmed.is_empty() {
            continue;
        }
        let v: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // Emit just the payload — that's the structured event the producer published.
        let payload = v.get("payload").cloned().unwrap_or(v);
        println!("{}", serde_json::to_string(&payload)?);
    }

    Ok(())
}

async fn handle_api(
    socket: Option<String>,
    config_opt: Option<String>,
    agent: Option<String>,
) -> Result<()> {
    let bus_socket = resolve_bus_socket(socket)?;
    let agent_name = agent.unwrap_or_else(|| "cli".to_string());

    if !std::path::Path::new(&bus_socket).exists() {
        anyhow::bail!("Bus socket not found: {}", bus_socket);
    }

    let resolved_cfg_path = resolve_config(config_opt);
    let user_config = resolved_cfg_path
        .as_ref()
        .and_then(|path| match config::UserConfig::load(path) {
            Ok(cfg) => {
                info!(config = %path, "loaded user config");
                Some(cfg)
            }
            Err(e) => {
                tracing::warn!(error = %e, path = %path, "failed to load user config, continuing without");
                None
            }
        });

    let task_store = crate::app::task::TaskStore::default_for_home();
    let sm_store = crate::app::statemachine::StateMachineStore::default_for_home();

    info!(socket = %bus_socket, agent = %agent_name, "starting bus API handler");
    // Standalone `deskd bus api` does not own a config_reload watcher, so we
    // hand it a fresh detached ReloadState. The `reload_config` RPC still
    // validates YAML and records last_reload_error, but the watcher trigger
    // is a no-op because nothing is waiting. Callers that want full hot-reload
    // semantics must use `deskd serve`.
    let detached_reload_state = crate::app::reload_state::ReloadState::new();
    let cfg_path_for_reload = resolved_cfg_path.unwrap_or_default();
    crate::app::bus_api::run(
        &bus_socket,
        &task_store,
        &sm_store,
        user_config.as_ref(),
        &agent_name,
        &cfg_path_for_reload,
        detached_reload_state,
    )
    .await
}
