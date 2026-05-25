//! Functional test: process crash detection and restart recovery (#120).
//!
//! The worker detects crashes via error strings ("process exited", "stdin closed")
//! and restarts the agent. Since we can't spawn real Claude processes in tests,
//! we test:
//! 1. State persistence survives simulated crash (state file intact after error)
//! 2. Session recovery: state with session_id is preserved for --resume
//! 3. Bus reconnection: agent reconnects to bus after crash
//! 4. Error message delivery: crash error is sent back to sender via bus

#![allow(clippy::approx_constant, clippy::collapsible_if)]

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

fn temp_socket() -> String {
    format!("/tmp/deskd-test-crash-{}.sock", uuid::Uuid::new_v4())
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

async fn setup_state_dir() -> (std::path::PathBuf, tokio::sync::MutexGuard<'static, ()>) {
    // Serialize env mutation; setenv is not thread-safe on POSIX. Caller MUST
    // keep the returned guard alive for the test's full duration so concurrent
    // env-mutating tests can't race with this test's file I/O.
    let guard = deskd::test_support::env_lock().lock().await;
    let tmp = std::path::PathBuf::from(format!(
        "/tmp/deskd-test-crash-state-{}",
        uuid::Uuid::new_v4()
    ));
    // SAFETY: ENV_LOCK serializes all env-mutating tests across the workspace.
    unsafe { std::env::set_var("HOME", &tmp) };
    std::fs::create_dir_all(tmp.join(".deskd/agents")).unwrap();
    (tmp, guard)
}

fn test_agent_config(name: &str) -> deskd::app::agent::AgentConfig {
    deskd::app::agent::AgentConfig {
        name: name.into(),
        model: "claude-sonnet-4-6".into(),
        system_prompt: "Test agent".into(),
        work_dir: "/tmp".into(),
        max_turns: 10,
        unix_user: None,
        command: vec!["echo".into()],
        config_path: None,
        container: None,
        session: deskd::infra::dto::ConfigSessionMode::Persistent,
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

/// State persistence survives crash: session_id, cost, turns preserved.
#[tokio::test]
async fn test_state_survives_crash() {
    let (_tmp, _env_guard) = setup_state_dir().await;

    let cfg = test_agent_config("crash-agent");
    let state = deskd::app::agent::create(&cfg).await.unwrap();
    assert_eq!(state.session_id, "");

    // Simulate worker updating state during task (before crash).
    let mut working = deskd::app::agent::load_state("crash-agent").unwrap();
    working.session_id = "session-xyz789".into();
    working.status = "working".into();
    working.current_task = "Processing important task".into();
    working.total_turns = 42;
    working.total_cost = 3.14;
    working.pid = 12345;
    deskd::app::agent::save_state_pub(&working).unwrap();

    // === CRASH HAPPENS HERE (process dies) ===
    // Simulate crash by dropping the process reference.
    // In the real worker, this triggers "process exited" error.

    // After crash: state file should still be intact.
    let recovered = deskd::app::agent::load_state("crash-agent").unwrap();
    assert_eq!(recovered.session_id, "session-xyz789");
    assert_eq!(recovered.total_turns, 42);
    assert_eq!(recovered.total_cost, 3.14);
    assert_eq!(recovered.config.name, "crash-agent");

    // Worker restarts: sets status back to idle, preserves session_id for --resume.
    let mut restarted = recovered;
    restarted.status = "idle".into();
    restarted.current_task = String::new();
    // pid would be updated with new process pid
    restarted.pid = 12346;
    deskd::app::agent::save_state_pub(&restarted).unwrap();

    let final_state = deskd::app::agent::load_state("crash-agent").unwrap();
    assert_eq!(final_state.status, "idle");
    assert_eq!(
        final_state.session_id, "session-xyz789",
        "session_id preserved for --resume after crash"
    );
    assert_eq!(final_state.pid, 12346, "pid updated to new process");

    deskd::app::agent::remove("crash-agent").await.unwrap();
}

/// Bus reconnection after crash: agent re-registers and receives new tasks.
#[tokio::test]
async fn test_bus_reconnection_after_crash() {
    let socket = temp_socket();

    let sock = socket.clone();
    tokio::spawn(async move {
        deskd::app::bus::serve(&sock).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // First connection (before crash).
    let (mut agent_rx1, _agent_tx1) =
        connect_and_register(&socket, "crash-worker", &["agent:crash-worker"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Send a task — agent receives it.
    {
        let (_tx_rx, mut tx_tx) = connect_and_register(&socket, "sender1", &[]).await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        let msg = serde_json::json!({
            "type": "message",
            "id": "pre-crash-task",
            "source": "sender1",
            "target": "agent:crash-worker",
            "payload": {"task": "Task before crash"},
            "metadata": {"priority": 5},
        });
        let mut line = serde_json::to_string(&msg).unwrap();
        line.push('\n');
        tx_tx.write_all(line.as_bytes()).await.unwrap();
    }

    let pre_crash = read_one(&mut agent_rx1, 1000).await;
    assert!(pre_crash.is_some(), "agent should receive pre-crash task");

    // === CRASH: drop the connection (simulates process dying) ===
    drop(_agent_tx1);
    drop(agent_rx1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Reconnect (simulates worker restart and re-registration).
    let (mut agent_rx2, _agent_tx2) =
        connect_and_register(&socket, "crash-worker", &["agent:crash-worker"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Send another task — agent should receive it after reconnection.
    {
        let (_tx_rx, mut tx_tx) = connect_and_register(&socket, "sender2", &[]).await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        let msg = serde_json::json!({
            "type": "message",
            "id": "post-crash-task",
            "source": "sender2",
            "target": "agent:crash-worker",
            "payload": {"task": "Task after crash recovery"},
            "metadata": {"priority": 5},
        });
        let mut line = serde_json::to_string(&msg).unwrap();
        line.push('\n');
        tx_tx.write_all(line.as_bytes()).await.unwrap();
    }

    let post_crash = read_one(&mut agent_rx2, 1000).await;
    assert!(
        post_crash.is_some(),
        "agent should receive task after bus reconnection"
    );
    let post_crash = post_crash.unwrap();
    assert_eq!(post_crash["payload"]["task"], "Task after crash recovery");

    let _ = std::fs::remove_file(&socket);
}

/// Error message delivery: worker sends crash error back to sender.
#[tokio::test]
async fn test_crash_error_delivered_to_sender() {
    let socket = temp_socket();

    let sock = socket.clone();
    tokio::spawn(async move {
        deskd::app::bus::serve(&sock).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Worker/agent on the bus.
    let (_agent_rx, mut agent_tx) =
        connect_and_register(&socket, "failing-agent", &["agent:failing-agent"]).await;

    // Sender waiting for reply.
    let (mut sender_rx, mut sender_tx) =
        connect_and_register(&socket, "sender", &["agent:sender"]).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Sender sends task.
    let task = serde_json::json!({
        "type": "message",
        "id": "task-crash",
        "source": "sender",
        "target": "agent:failing-agent",
        "payload": {"task": "Do something risky"},
        "reply_to": "agent:sender",
        "metadata": {"priority": 5},
    });
    let mut line = serde_json::to_string(&task).unwrap();
    line.push('\n');
    sender_tx.write_all(line.as_bytes()).await.unwrap();

    // Agent processes task but crashes — worker sends error reply.
    // (Simulating what worker does on crash detection.)
    let error_reply = serde_json::json!({
        "type": "message",
        "id": "error-reply-001",
        "source": "failing-agent",
        "target": "agent:sender",
        "payload": {
            "error": "agent process crashed: persistent process exited mid-task",
            "result": "",
        },
        "metadata": {"priority": 5},
    });
    let mut err_line = serde_json::to_string(&error_reply).unwrap();
    err_line.push('\n');
    agent_tx.write_all(err_line.as_bytes()).await.unwrap();

    // Sender receives the error.
    // First message might be own task echo if subscribed, so read up to 2.
    let mut error_received = false;
    for _ in 0..2 {
        if let Some(msg) = read_one(&mut sender_rx, 1000).await {
            if msg["payload"]["error"].is_string() {
                let err = msg["payload"]["error"].as_str().unwrap();
                assert!(
                    err.contains("process exited") || err.contains("crashed"),
                    "error should mention crash: {}",
                    err
                );
                error_received = true;
                break;
            }
        }
    }
    assert!(error_received, "sender should receive crash error reply");

    let _ = std::fs::remove_file(&socket);
}

/// Task log records error status on crash.
#[tokio::test]
async fn test_tasklog_records_crash_error() {
    let log_dir =
        std::path::PathBuf::from(format!("/tmp/deskd-test-crashlog-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&log_dir).unwrap();

    // Create a task log entry for a crashed task (what worker does on error).
    let entry = deskd::app::tasklog::TaskLog {
        ts: chrono::Utc::now().to_rfc3339(),
        source: "github_poll".into(),
        turns: 3,
        cost: 0.05,
        duration_ms: 12000,
        status: "error".into(),
        task: "Important task that crashed".into(),
        error: Some("persistent process exited mid-task".into()),
        msg_id: "crash-msg-001".into(),
        github_repo: Some("kgatilin/deskd".into()),
        github_pr: Some(42),
        input_tokens: Some(1500),
        output_tokens: Some(200),
        cache_creation_input_tokens: Some(500),
        cache_read_input_tokens: Some(800),
        session_count: Some(1),
        tool_use_count: Some(0),
        parent_agent: None,
    };

    let log_file = log_dir.join("crash-test-agent.jsonl");
    deskd::app::tasklog::log_task_to_path(&log_file, &entry).unwrap();

    // Verify the log was written and contains error info.
    let entries = deskd::app::tasklog::read_logs_from_path(&log_file, 100, None, None).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].status, "error");
    assert_eq!(
        entries[0].error.as_deref(),
        Some("persistent process exited mid-task")
    );
    assert_eq!(entries[0].github_pr, Some(42));
}
