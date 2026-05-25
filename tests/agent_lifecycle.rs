//! Functional test: end-to-end agent lifecycle (#117).
//!
//! Tests: create → configure → send task via bus → receive result → stop/remove.
//!
//! Since we can't spawn a real Claude process in tests, we test the lifecycle
//! at the state management + bus routing level.
//!
//! Note: agent state functions use $HOME, so all state tests run in a single
//! test to avoid env var races between parallel tests.

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

fn temp_socket() -> String {
    format!("/tmp/deskd-test-lifecycle-{}.sock", uuid::Uuid::new_v4())
}

async fn connect_and_register(
    socket: &str,
    name: &str,
    subscriptions: &[&str],
) -> (
    tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    tokio::net::unix::OwnedWriteHalf,
) {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (reader, mut writer) = stream.into_split();

    let reg = serde_json::json!({
        "type": "register",
        "name": name,
        "subscriptions": subscriptions,
    });
    let mut line = serde_json::to_string(&reg).unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();

    (BufReader::new(reader).lines(), writer)
}

async fn read_one(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    timeout_ms: u64,
) -> Option<serde_json::Value> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), lines.next_line())
        .await
        .ok()?
        .ok()?
        .and_then(|l| serde_json::from_str(&l).ok())
}

fn make_config(name: &str) -> deskd::app::agent::AgentConfig {
    deskd::app::agent::AgentConfig {
        name: name.into(),
        model: "claude-sonnet-4-6".into(),
        system_prompt: "You are a test agent.".into(),
        work_dir: "/tmp/test-agent-work".into(),
        max_turns: 10,
        unix_user: None,
        command: vec!["echo".into()],
        config_path: None,
        container: None,
        session: deskd::infra::dto::ConfigSessionMode::Ephemeral,
        runtime: deskd::infra::dto::ConfigAgentRuntime::Claude,
        kind: deskd::infra::dto::ConfigAgentKind::Executor,
        launch_mode: Default::default(),
        context: None,
        compact_threshold: None,
        auto_compact_threshold_tokens: None,
        empty_completion_threshold: None,
        empty_completion_restart_min_secs: None,
    }
}

/// Full agent lifecycle in one sequential test to avoid HOME env var races.
///
/// Covers: create, load, update, list, duplicate check, remove, remove-nonexistent.
#[tokio::test]
async fn test_agent_state_lifecycle() {
    // Serialize env mutation; setenv is not thread-safe on POSIX.
    let _env_guard = deskd::test_support::env_lock().lock().await;
    // Set up isolated HOME.
    let tmp = std::path::PathBuf::from(format!("/tmp/deskd-test-state-{}", uuid::Uuid::new_v4()));
    // SAFETY: ENV_LOCK serializes all env-mutating tests across the workspace.
    unsafe { std::env::set_var("HOME", &tmp) };
    std::fs::create_dir_all(tmp.join(".deskd/agents")).unwrap();

    // --- CREATE ---
    let cfg = make_config("lifecycle-agent");
    let state = deskd::app::agent::create(&cfg).await.unwrap();
    assert_eq!(state.config.name, "lifecycle-agent");
    assert_eq!(state.status, "idle");
    assert_eq!(state.total_cost, 0.0);
    assert_eq!(state.total_turns, 0);

    // --- LOAD and verify persistence ---
    let loaded = deskd::app::agent::load_state("lifecycle-agent").unwrap();
    assert_eq!(loaded.config.name, "lifecycle-agent");
    assert_eq!(loaded.config.model, "claude-sonnet-4-6");

    // --- UPDATE state (simulate worker updating after task) ---
    let mut updated = loaded;
    updated.status = "working".into();
    updated.current_task = "Process incoming PR".into();
    updated.total_turns = 5;
    updated.total_cost = 0.42;
    updated.session_id = "session-abc123".into();
    deskd::app::agent::save_state_pub(&updated).unwrap();

    let reloaded = deskd::app::agent::load_state("lifecycle-agent").unwrap();
    assert_eq!(reloaded.status, "working");
    assert_eq!(reloaded.current_task, "Process incoming PR");
    assert_eq!(reloaded.total_turns, 5);
    assert_eq!(reloaded.total_cost, 0.42);
    assert_eq!(reloaded.session_id, "session-abc123");

    // --- DUPLICATE CREATE fails ---
    let dup_result = deskd::app::agent::create(&cfg).await;
    assert!(dup_result.is_err());
    assert!(
        dup_result
            .unwrap_err()
            .to_string()
            .contains("already exists")
    );

    // --- LIST ---
    let cfg_b = make_config("lifecycle-agent-b");
    let cfg_c = make_config("lifecycle-agent-c");
    deskd::app::agent::create(&cfg_b).await.unwrap();
    deskd::app::agent::create(&cfg_c).await.unwrap();

    let agents = deskd::app::agent::list().await.unwrap();
    let names: Vec<&str> = agents.iter().map(|a| a.config.name.as_str()).collect();
    assert!(names.contains(&"lifecycle-agent"));
    assert!(names.contains(&"lifecycle-agent-b"));
    assert!(names.contains(&"lifecycle-agent-c"));

    // --- REMOVE ---
    deskd::app::agent::remove("lifecycle-agent").await.unwrap();
    assert!(deskd::app::agent::load_state("lifecycle-agent").is_err());

    deskd::app::agent::remove("lifecycle-agent-b")
        .await
        .unwrap();
    deskd::app::agent::remove("lifecycle-agent-c")
        .await
        .unwrap();

    // --- REMOVE nonexistent fails ---
    let rm_result = deskd::app::agent::remove("ghost-agent").await;
    assert!(rm_result.is_err());

    // --- Set idle after task ---
    let cfg_d = make_config("idle-agent");
    deskd::app::agent::create(&cfg_d).await.unwrap();
    let mut st = deskd::app::agent::load_state("idle-agent").unwrap();
    st.status = "working".into();
    st.current_task = "Task in progress".into();
    deskd::app::agent::save_state_pub(&st).unwrap();

    st.status = "idle".into();
    st.current_task = String::new();
    st.total_turns = 15;
    st.total_cost = 1.23;
    deskd::app::agent::save_state_pub(&st).unwrap();

    let final_st = deskd::app::agent::load_state("idle-agent").unwrap();
    assert_eq!(final_st.status, "idle");
    assert_eq!(final_st.current_task, "");
    assert_eq!(final_st.total_turns, 15);

    deskd::app::agent::remove("idle-agent").await.unwrap();

    let _ = std::fs::remove_dir_all(&tmp);
}

/// Bus-level task routing: CLI → agent → result back to CLI.
/// This doesn't modify HOME so it's safe to run in parallel.
#[tokio::test]
async fn test_agent_bus_task_round_trip() {
    let socket = temp_socket();

    let sock = socket.clone();
    tokio::spawn(async move {
        deskd::app::bus::serve(&sock).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Worker registers as the agent on the bus.
    let (mut agent_rx, mut agent_tx) =
        connect_and_register(&socket, "test-agent", &["agent:test-agent"]).await;

    // CLI sends a task and expects a reply back (via agent:cli direct routing).
    let (mut cli_rx, mut cli_tx) = connect_and_register(&socket, "cli", &[]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // CLI → agent.
    let task_msg = serde_json::json!({
        "type": "message",
        "id": "task-001",
        "source": "cli",
        "target": "agent:test-agent",
        "payload": {"task": "Review PR #42"},
        "reply_to": "agent:cli",
        "metadata": {"priority": 5},
    });
    let mut line = serde_json::to_string(&task_msg).unwrap();
    line.push('\n');
    cli_tx.write_all(line.as_bytes()).await.unwrap();

    // Agent receives the task.
    let received = read_one(&mut agent_rx, 1000).await;
    assert!(received.is_some(), "agent should receive task");
    let received = received.unwrap();
    assert_eq!(received["payload"]["task"], "Review PR #42");
    assert_eq!(received["reply_to"], "agent:cli");

    // Agent → CLI (simulating worker completion, replies via agent:cli direct routing).
    let result_msg = serde_json::json!({
        "type": "message",
        "id": "result-001",
        "source": "test-agent",
        "target": "agent:cli",
        "payload": {
            "result": "PR #42 looks good. LGTM.",
        },
        "metadata": {"priority": 5},
    });
    let mut result_line = serde_json::to_string(&result_msg).unwrap();
    result_line.push('\n');
    agent_tx.write_all(result_line.as_bytes()).await.unwrap();

    // CLI receives the result (direct routing by name).
    let reply = read_one(&mut cli_rx, 1000).await;
    assert!(reply.is_some(), "cli should receive result from agent");
    let reply = reply.unwrap();
    assert_eq!(reply["payload"]["result"], "PR #42 looks good. LGTM.");

    let _ = std::fs::remove_file(&socket);
}
