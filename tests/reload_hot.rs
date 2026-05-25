//! Integration tests for `deskd reload` (#474).
//!
//! Exercises the daemon-side path end-to-end without a real `serve`:
//!   * `bus_api::run` listens on a real Unix socket.
//!   * A CLI-style client sends a `reload_config` RPC and reads the response.
//!   * In parallel, `config_reload::watch_and_reload` is observed via its
//!     interaction with `ReloadState`.
//!
//! These tests use the in-process `bus_server::serve` from `infra`, so there
//! is no subprocess overhead.

use std::time::Duration;

use deskd::app::bus_api;
use deskd::app::config_reload;
use deskd::app::reload_state::ReloadState;
use deskd::app::statemachine::StateMachineStore;
use deskd::app::task::TaskStore;
use deskd::config::UserConfig;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

fn temp_socket() -> String {
    format!("/tmp/deskd-test-reload-{}.sock", uuid::Uuid::new_v4())
}

async fn start_bus(socket: &str) {
    let socket_owned = socket.to_string();
    tokio::spawn(async move {
        let _ = deskd::infra::bus_server::serve(&socket_owned).await;
    });
    // Give the bus a moment to bind. The bus is in-process so this is fast.
    for _ in 0..40 {
        if std::path::Path::new(socket).exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("bus did not bind socket within 1s: {}", socket);
}

async fn start_bus_api(
    socket: &str,
    cfg_path: &str,
    reload_state: ReloadState,
) -> tokio::task::JoinHandle<()> {
    let socket_owned = socket.to_string();
    let cfg_owned = cfg_path.to_string();
    tokio::spawn(async move {
        let task_dir =
            std::env::temp_dir().join(format!("deskd-test-reload-tasks-{}", uuid::Uuid::new_v4()));
        let sm_dir =
            std::env::temp_dir().join(format!("deskd-test-reload-sm-{}", uuid::Uuid::new_v4()));
        let task_store = TaskStore::new(task_dir);
        let sm_store = StateMachineStore::new(sm_dir);
        let _ = bus_api::run(
            &socket_owned,
            &task_store,
            &sm_store,
            None,
            "test-agent",
            &cfg_owned,
            reload_state,
        )
        .await;
    })
}

async fn rpc_call(
    socket: &str,
    method: &str,
    params: Value,
    timeout_secs: u64,
) -> anyhow::Result<Value> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let client_name = format!("test-client-{}", &request_id[..8]);

    let mut stream = UnixStream::connect(socket).await?;
    let reg = json!({
        "type": "register",
        "name": client_name,
        "subscriptions": [],
    });
    let mut line = serde_json::to_string(&reg)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;

    let req_payload = json!({
        "method": method,
        "params": params,
        "request_id": request_id,
    });
    let envelope = json!({
        "type": "message",
        "id": uuid::Uuid::new_v4().to_string(),
        "source": client_name,
        "target": "deskd:command",
        "payload": req_payload,
    });
    let mut line = serde_json::to_string(&envelope)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;

    // Keep the writer half alive — dropping it half-closes the socket and the
    // bus_server treats that as a disconnect, removing the client from its
    // routing table before the response arrives.
    let (reader, _writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let payload = tokio::time::timeout(Duration::from_secs(timeout_secs), async {
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
    .map_err(|_| anyhow::anyhow!("RPC timed out"))??;

    payload.ok_or_else(|| anyhow::anyhow!("no response"))
}

/// Reload with valid YAML: RPC accepted, daemon fires the Notify, and the
/// reload_state ends up with `last_reload_at` set (when the watcher applies
/// changes).
#[tokio::test]
async fn reload_valid_yaml_triggers_watcher() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg_path = tmp.path().join("deskd.yaml");
    std::fs::write(
        &cfg_path,
        "model: claude-sonnet-4-6\nsystem_prompt: initial\n",
    )
    .unwrap();

    let reload_state = ReloadState::new();
    let socket = temp_socket();
    start_bus(&socket).await;

    let _api_task = start_bus_api(&socket, cfg_path.to_str().unwrap(), reload_state.clone()).await;

    // Wait a moment for bus_api to register.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spin up a waiter that records whether the trigger fired.
    let trigger_seen = std::sync::Arc::new(tokio::sync::Notify::new());
    {
        let seen = trigger_seen.clone();
        let watcher_state = reload_state.clone();
        tokio::spawn(async move {
            watcher_state.wait_for_trigger().await;
            seen.notify_one();
        });
    }
    tokio::task::yield_now().await;

    // Edit the config so a subsequent re-parse picks up the change.
    std::fs::write(
        &cfg_path,
        "model: claude-sonnet-4-6\nsystem_prompt: updated\n",
    )
    .unwrap();

    let resp = rpc_call(&socket, "reload_config", json!({}), 5)
        .await
        .expect("RPC should succeed");
    let result = resp.get("result").expect("result field");
    assert_eq!(result["validated"], json!(true));
    assert_eq!(result["triggered"], json!(true));

    // Watcher must observe the Notify.
    tokio::time::timeout(Duration::from_secs(2), trigger_seen.notified())
        .await
        .expect("watcher should be woken by reload_config trigger");
}

/// Malformed YAML must be rejected with an error response. The shared
/// reload_state's `last_reload_error` field must be populated, and
/// `last_reload_at` must remain `None`.
#[tokio::test]
async fn reload_malformed_yaml_returns_error_and_records_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg_path = tmp.path().join("deskd.yaml");
    std::fs::write(&cfg_path, "this is: : not valid : :: yaml\n").unwrap();

    let reload_state = ReloadState::new();
    let socket = temp_socket();
    start_bus(&socket).await;

    let _api_task = start_bus_api(&socket, cfg_path.to_str().unwrap(), reload_state.clone()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = rpc_call(&socket, "reload_config", json!({}), 5)
        .await
        .expect("RPC delivered");
    assert!(
        resp.get("error").is_some(),
        "malformed YAML must surface as error: {}",
        resp
    );

    let snap = reload_state.snapshot().await;
    assert!(snap.last_reload_error.is_some());
    assert!(
        snap.last_reload_at.is_none(),
        "failure must not set last_reload_at"
    );
}

/// `bus_status` RPC must include `last_reload_at` (null when never reloaded)
/// and `last_reload_error` so operators can verify the most recent reload.
#[tokio::test]
async fn bus_status_includes_reload_observability() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg_path = tmp.path().join("deskd.yaml");
    std::fs::write(&cfg_path, "model: claude-sonnet-4-6\n").unwrap();

    let reload_state = ReloadState::new();
    let socket = temp_socket();
    start_bus(&socket).await;

    let _api_task = start_bus_api(&socket, cfg_path.to_str().unwrap(), reload_state.clone()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Before any reload: both fields must be present (null) on the wire.
    let before = rpc_call(&socket, "bus_status", json!({}), 5)
        .await
        .expect("bus_status RPC delivered");
    let r = before.get("result").expect("result field present");
    assert!(r.get("last_reload_at").is_some());
    assert!(r.get("last_reload_error").is_some());
    assert!(r["last_reload_at"].is_null());

    // Simulate a successful reload by recording it directly.
    let ts = chrono::Utc::now();
    reload_state.record_success(ts).await;

    let after = rpc_call(&socket, "bus_status", json!({}), 5)
        .await
        .expect("bus_status RPC delivered");
    let r = after.get("result").expect("result field present");
    let last = r["last_reload_at"]
        .as_str()
        .expect("last_reload_at must be a string after success");
    assert!(
        last.starts_with(&ts.format("%Y-%m-%d").to_string()),
        "expected rfc3339 timestamp, got {}",
        last
    );
}

/// AC: in-flight agent turns are NOT killed by a reload.
///
/// The hot-reload pipeline only touches `AgentComponents` (adapters,
/// schedule watcher, reminder runner, sub-agent workers). The main worker
/// and bus API are session-persistent — they are not stored in components
/// and are not aborted. This test simulates the main-worker invariant
/// by spawning a long-running task that does NOT live in components and
/// asserting it survives a reload that empties+respawns components.
#[tokio::test]
async fn reload_does_not_kill_in_flight_worker_task() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg_path = tmp.path().join("deskd.yaml");
    std::fs::write(&cfg_path, "model: claude-sonnet-4-6\nsystem_prompt: a\n").unwrap();

    let reload_state = ReloadState::new();

    // "Worker" simulation — a task that lives OUTSIDE AgentComponents and
    // simply waits forever. If reload aborts it, the joinhandle will report
    // cancelled.
    let worker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        "completed"
    });

    let def = deskd::config::AgentDef {
        name: "test-agent".to_string(),
        work_dir: tmp.path().to_string_lossy().into_owned(),
        config: Some(cfg_path.to_string_lossy().into_owned()),
        command: vec!["echo".to_string()],
        unix_user: None,
        model: None,
        telegram: None,
        discord: None,
        container: None,
        runtime: Default::default(),
        launch_mode: Default::default(),
    };
    let components = deskd::app::agent_components::AgentComponents {
        adapter_handles: Vec::new(),
        adapter_cancel_tokens: Vec::new(),
        schedule_watcher: None,
        config_watcher: None,
        reminder_runner: None,
        sub_agent_handles: Vec::new(),
    };
    let bus_socket = format!(
        "/tmp/deskd-test-reload-bus-noki-{}.sock",
        uuid::Uuid::new_v4()
    );

    let watcher_state = reload_state.clone();
    let cfg_str = cfg_path.to_str().unwrap().to_string();
    let watcher = tokio::spawn(async move {
        config_reload::watch_and_reload(
            def,
            components,
            Vec::new(),
            bus_socket,
            "test-agent".to_string(),
            cfg_str,
            watcher_state,
        )
        .await;
    });

    // Drive a reload through the watcher.
    tokio::time::sleep(Duration::from_millis(50)).await;
    std::fs::write(&cfg_path, "model: claude-sonnet-4-6\nsystem_prompt: b\n").unwrap();
    reload_state.trigger();

    // Give the watcher time to run the reload.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let snap = reload_state.snapshot().await;
        if snap.last_reload_at.is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("reload never recorded success");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Worker handle must still be alive — reload must not abort it.
    assert!(
        !worker.is_finished(),
        "in-flight worker task was killed by reload (AC violation)"
    );

    worker.abort();
    watcher.abort();
}

/// AC: schedule add/remove/change must apply on reload. We start the
/// watcher with an empty config, edit the file to add a cron schedule,
/// trigger reload, and verify the watcher recorded success — which (per
/// `classify_config_change`) means it took the schedules-changed branch
/// and respawned the schedule registry.
#[tokio::test]
async fn reload_applies_schedule_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg_path = tmp.path().join("deskd.yaml");
    std::fs::write(
        &cfg_path,
        "model: claude-sonnet-4-6\nsystem_prompt: a\nschedules: []\n",
    )
    .unwrap();

    let reload_state = ReloadState::new();
    let def = deskd::config::AgentDef {
        name: "test-agent".to_string(),
        work_dir: tmp.path().to_string_lossy().into_owned(),
        config: Some(cfg_path.to_string_lossy().into_owned()),
        command: vec!["echo".to_string()],
        unix_user: None,
        model: None,
        telegram: None,
        discord: None,
        container: None,
        runtime: Default::default(),
        launch_mode: Default::default(),
    };
    let components = deskd::app::agent_components::AgentComponents {
        adapter_handles: Vec::new(),
        adapter_cancel_tokens: Vec::new(),
        schedule_watcher: None,
        config_watcher: None,
        reminder_runner: None,
        sub_agent_handles: Vec::new(),
    };
    let bus_socket = format!(
        "/tmp/deskd-test-reload-bus-sched-{}.sock",
        uuid::Uuid::new_v4()
    );
    let watcher_state = reload_state.clone();
    let cfg_str = cfg_path.to_str().unwrap().to_string();
    let watcher = tokio::spawn(async move {
        config_reload::watch_and_reload(
            def,
            components,
            Vec::new(),
            bus_socket,
            "test-agent".to_string(),
            cfg_str,
            watcher_state,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Add a schedule to the YAML.
    std::fs::write(
        &cfg_path,
        r#"model: claude-sonnet-4-6
system_prompt: a
schedules:
  - cron: "0 0 9 * * *"
    target: "agent:test-agent"
    action: raw
    config: "morning brief"
"#,
    )
    .unwrap();
    reload_state.trigger();

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let snap = reload_state.snapshot().await;
        if snap.last_reload_at.is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("schedule reload never recorded success: {:?}", snap);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Reload after schedule-only change must NOT set last_reload_error.
    let snap = reload_state.snapshot().await;
    assert!(snap.last_reload_error.is_none());

    watcher.abort();
}

/// Reload with valid YAML actually drives `config_reload::watch_and_reload`
/// to set `last_reload_at`. This wires the full pipeline: RPC → Notify →
/// watcher → record_success. We use a minimal AgentDef and stub work_dir.
#[tokio::test]
async fn reload_pipeline_updates_last_reload_at() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg_path = tmp.path().join("deskd.yaml");
    std::fs::write(
        &cfg_path,
        "model: claude-sonnet-4-6\nsystem_prompt: first\n",
    )
    .unwrap();

    let user_cfg = UserConfig::load(cfg_path.to_str().unwrap()).unwrap();
    assert_eq!(user_cfg.system_prompt, "first");

    let reload_state = ReloadState::new();

    // Build a minimal AgentDef. work_dir is tmp; other fields use defaults.
    let def = deskd::config::AgentDef {
        name: "test-agent".to_string(),
        work_dir: tmp.path().to_string_lossy().into_owned(),
        config: Some(cfg_path.to_string_lossy().into_owned()),
        command: vec!["echo".to_string()],
        unix_user: None,
        model: None,
        telegram: None,
        discord: None,
        container: None,
        runtime: Default::default(),
        launch_mode: Default::default(),
    };

    let components = deskd::app::agent_components::AgentComponents {
        adapter_handles: Vec::new(),
        adapter_cancel_tokens: Vec::new(),
        schedule_watcher: None,
        config_watcher: None,
        reminder_runner: None,
        sub_agent_handles: Vec::new(),
    };

    let bus_socket = format!("/tmp/deskd-test-reload-bus-{}.sock", uuid::Uuid::new_v4());

    let watcher = {
        let state = reload_state.clone();
        let def = def.clone();
        let bus = bus_socket.clone();
        let cfg = cfg_path.to_str().unwrap().to_string();
        tokio::spawn(async move {
            config_reload::watch_and_reload(
                def,
                components,
                Vec::new(),
                bus,
                "test-agent".to_string(),
                cfg,
                state,
            )
            .await;
        })
    };

    // Edit config so the changeset isn't empty.
    tokio::time::sleep(Duration::from_millis(50)).await;
    std::fs::write(
        &cfg_path,
        "model: claude-sonnet-4-6\nsystem_prompt: second\n",
    )
    .unwrap();

    // Fire trigger directly (simulates the RPC path).
    reload_state.trigger();

    // Wait for last_reload_at to land. The system_prompt_only branch records
    // success without spawning components, so this should be quick.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let snap = reload_state.snapshot().await;
        if snap.last_reload_at.is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "last_reload_at never set after trigger; snapshot: {:?}",
                snap
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    watcher.abort();
}
