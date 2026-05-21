//! `deskd reload` subcommand handler (#474).
//!
//! Sends a `reload_config` RPC to the running daemon's bus API and prints
//! the daemon's response. On parse failure (malformed YAML) the CLI exits
//! non-zero so scripted operators can surface the error.
//!
//! `--config <path>` is a client-side **assertion**: if provided, the CLI
//! looks up the agent's recorded `config_path` in `ServeState` and bails
//! before sending the RPC if the paths don't match. The daemon always
//! reloads its own launch path — see `bus_api::handle_reload_config` and
//! the #478 review for why a server-side override was removed.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tracing::info;
use uuid::Uuid;

use crate::config;

const REQ_TIMEOUT_SECS: u64 = 5;
const CLIENT_NAME_PREFIX: &str = "deskd-reload-cli";

/// Entry point for `deskd reload`. Talks to the daemon over the bus.
pub async fn handle(
    config_override: Option<String>,
    agent_override: Option<String>,
    socket_override: Option<String>,
) -> Result<()> {
    // Resolve the bus socket. Priority:
    //   1. explicit --socket / $DESKD_BUS_SOCKET
    //   2. running serve state, scoped to --agent if provided
    let bus_socket = resolve_bus_socket(socket_override.clone(), agent_override.as_deref())?;

    if !std::path::Path::new(&bus_socket).exists() {
        anyhow::bail!(
            "bus socket not found at {} — is `deskd serve` running?",
            bus_socket
        );
    }

    // If the operator passed --config, verify it matches the daemon's
    // recorded path. We can only check when ServeState is available; when
    // the operator went through --socket directly, we trust them.
    if let Some(ref operator_path) = config_override {
        verify_operator_config_path(operator_path, &bus_socket, &socket_override)?;
    }

    let request_id = Uuid::new_v4().to_string();
    let client_name = format!("{}-{}", CLIENT_NAME_PREFIX, &request_id[..8]);

    let mut stream = UnixStream::connect(&bus_socket)
        .await
        .with_context(|| format!("connecting to bus at {}", bus_socket))?;

    // Register so the bus_api can route the response back to us.
    let reg = json!({
        "type": "register",
        "name": client_name,
        "subscriptions": [],
    });
    write_line(&mut stream, &reg).await?;

    // The daemon ignores any `params.path` (see #478 review) — we send an
    // empty params object. `--config` is verified client-side above.
    let req_payload = json!({
        "method": "reload_config",
        "params": {},
        "request_id": request_id,
    });
    let envelope = json!({
        "type": "message",
        "id": Uuid::new_v4().to_string(),
        "source": client_name,
        "target": "deskd:command",
        "payload": req_payload,
    });
    write_line(&mut stream, &envelope).await?;

    info!(socket = %bus_socket, "sent reload_config RPC, awaiting response");

    // Read until we see a response matching our request_id, or time out.
    // Keep the writer half alive while we read — dropping it would shutdown
    // the write end of the socket, and the bus server interprets that as a
    // client disconnect and removes us from its routing table before the
    // response arrives.
    let (reader, _writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let response = tokio::time::timeout(std::time::Duration::from_secs(REQ_TIMEOUT_SECS), async {
        while let Some(line) = lines.next_line().await? {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let v: Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let payload = v.get("payload").cloned().unwrap_or(v);
            if payload.get("request_id").and_then(|r| r.as_str()) == Some(&request_id) {
                return Ok::<_, anyhow::Error>(Some(payload));
            }
        }
        Ok(None)
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "timed out waiting for daemon response after {}s",
            REQ_TIMEOUT_SECS
        )
    })??;

    let payload =
        response.ok_or_else(|| anyhow::anyhow!("bus closed connection before daemon responded"))?;

    if let Some(err) = payload.get("error").and_then(|e| e.as_str()) {
        anyhow::bail!("daemon rejected reload: {}", err);
    }

    let result = payload
        .get("result")
        .ok_or_else(|| anyhow::anyhow!("daemon response missing 'result' field: {}", payload))?;

    let path = result
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let agent = result
        .get("agent")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");

    println!(
        "reload accepted — daemon will re-apply config from {} (agent: {})",
        path, agent
    );
    Ok(())
}

/// Verify that the operator-supplied `--config` path matches the agent's
/// recorded `config_path` in `ServeState`. Bails on mismatch.
///
/// Skipped when the operator used `--socket` directly: there's no serve
/// state to consult, and we trust the operator's manual routing.
fn verify_operator_config_path(
    operator_path: &str,
    bus_socket: &str,
    socket_override: &Option<String>,
) -> Result<()> {
    if socket_override.is_some() {
        // Operator drove the socket explicitly — no ServeState to consult.
        return Ok(());
    }
    let Some(state) = config::ServeState::load() else {
        // No serve state file — nothing to verify against. Don't fail; the
        // daemon will still validate its own config when it parses.
        return Ok(());
    };
    let Some(agent) = state.agents.values().find(|a| a.bus_socket == bus_socket) else {
        // The socket we resolved isn't recorded in ServeState. Don't fail
        // — the agent may have been started outside `deskd serve`.
        return Ok(());
    };
    if agent.config_path != operator_path {
        anyhow::bail!(
            "--config mismatch: you passed {}, but the daemon is reading {}. \
             Drop --config to reload the daemon's actual config, or restart the \
             daemon with --config {} if you meant to change the file.",
            operator_path,
            agent.config_path,
            operator_path,
        );
    }
    Ok(())
}

/// Resolve the bus socket: explicit > $DESKD_BUS_SOCKET > running serve.
fn resolve_bus_socket(explicit: Option<String>, agent: Option<&str>) -> Result<String> {
    if let Some(s) = explicit {
        return Ok(s);
    }
    let state = config::ServeState::load().ok_or_else(|| {
        anyhow::anyhow!(
            "no --socket provided and no running serve detected (no ~/.deskd/serve.state.yaml)"
        )
    })?;
    if let Some(name) = agent {
        let agent_state = state.agent(name).ok_or_else(|| {
            anyhow::anyhow!("agent '{}' not present in running serve state", name)
        })?;
        return Ok(agent_state.bus_socket.clone());
    }
    state
        .find_agent_config()
        .map(|a| a.bus_socket.clone())
        .ok_or_else(|| anyhow::anyhow!("running serve has no agents configured"))
}

async fn write_line(stream: &mut UnixStream, value: &Value) -> Result<()> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_bus_socket_uses_explicit_first() {
        // Explicit --socket wins over everything.
        let got = resolve_bus_socket(Some("/tmp/explicit.sock".into()), None).unwrap();
        assert_eq!(got, "/tmp/explicit.sock");
    }

    #[test]
    fn resolve_bus_socket_errors_without_state_or_socket() {
        // env-isolated; HOME points somewhere with no ~/.deskd/serve.state.yaml.
        let _env_guard = crate::test_support::env_lock().blocking_lock();
        let tmp = tempfile::tempdir().unwrap();
        let prev_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", tmp.path());
        }
        let err = resolve_bus_socket(None, None).unwrap_err();
        assert!(err.to_string().contains("no running serve"));
        unsafe {
            match prev_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
    }

    #[test]
    fn verify_skipped_when_socket_override_set() {
        // --socket given → no ServeState to consult; verification is a no-op.
        let got = verify_operator_config_path(
            "/tmp/some-yaml.yaml",
            "/tmp/sock",
            &Some("/tmp/sock".into()),
        );
        assert!(got.is_ok());
    }

    #[test]
    fn verify_skipped_when_no_serve_state() {
        // No serve state file → don't fail; the daemon will validate itself.
        let _env_guard = crate::test_support::env_lock().blocking_lock();
        let tmp = tempfile::tempdir().unwrap();
        let prev_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", tmp.path());
        }
        let got = verify_operator_config_path("/tmp/some.yaml", "/tmp/sock", &None);
        unsafe {
            match prev_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
        assert!(got.is_ok());
    }

    #[test]
    fn verify_bails_on_path_mismatch() {
        // ServeState records cfg_path X; operator passed Y → bail.
        let _env_guard = crate::test_support::env_lock().blocking_lock();
        let tmp = tempfile::tempdir().unwrap();
        let prev_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", tmp.path());
        }
        // Write a ServeState with one agent at known socket + config_path.
        let state_dir = tmp.path().join(".deskd");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state_yaml = r#"
workspace_config: /tmp/workspace.yaml
started_at: "2026-05-20T00:00:00Z"
agents:
  alpha:
    work_dir: /tmp/alpha-wd
    bus_socket: /tmp/alpha.sock
    config_path: /home/op/deskd.yaml
"#;
        std::fs::write(state_dir.join("serve.state.yaml"), state_yaml).unwrap();
        let got = verify_operator_config_path("/tmp/wrong.yaml", "/tmp/alpha.sock", &None);
        unsafe {
            match prev_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
        let err = got.unwrap_err().to_string();
        assert!(err.contains("--config mismatch"), "got: {}", err);
        assert!(err.contains("/tmp/wrong.yaml"), "got: {}", err);
        assert!(err.contains("/home/op/deskd.yaml"), "got: {}", err);
    }

    #[test]
    fn verify_ok_on_path_match() {
        // ServeState records cfg_path X; operator passed X → ok.
        let _env_guard = crate::test_support::env_lock().blocking_lock();
        let tmp = tempfile::tempdir().unwrap();
        let prev_home = std::env::var("HOME").ok();
        unsafe {
            std::env::set_var("HOME", tmp.path());
        }
        let state_dir = tmp.path().join(".deskd");
        std::fs::create_dir_all(&state_dir).unwrap();
        let state_yaml = r#"
workspace_config: /tmp/workspace.yaml
started_at: "2026-05-20T00:00:00Z"
agents:
  alpha:
    work_dir: /tmp/alpha-wd
    bus_socket: /tmp/alpha.sock
    config_path: /home/op/deskd.yaml
"#;
        std::fs::write(state_dir.join("serve.state.yaml"), state_yaml).unwrap();
        let got = verify_operator_config_path("/home/op/deskd.yaml", "/tmp/alpha.sock", &None);
        unsafe {
            match prev_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
        assert!(got.is_ok(), "match should pass; got {:?}", got.err());
    }
}
