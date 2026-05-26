//! Integration test for #502: `add_persistent_agent` `lifecycle` arg.
//!
//! End-to-end coverage that boots a real `deskd mcp` subprocess + spawns
//! `claude` inside tmux cannot live in CI — it needs tmux + claude on PATH
//! + a Claude account. Instead this test pins the two contract surfaces a
//!   caller actually depends on:
//!
//! 1. **Provisioning shape**: when `lifecycle: "tmux-channel"` is used, the
//!    `.mcp.json` written into the sub-agent's work_dir must register
//!    `deskd mcp-channel --agent <name>` with
//!    `env.DESKD_BUS_SOCKET = /tmp/deskd-<parent>-internal.sock`. This is
//!    the single line the running REPL uses to reach the parent's
//!    *internal* bus (and crucially **not** the parent's main bus) — if
//!    this string drifts, telegram isolation breaks and the sub-agent
//!    silently joins the wrong bus.
//!
//! 2. **`agent list` visibility**: after provisioning, an agent list call
//!    sees both a stream-json sub-agent (Subprocess launch mode, has a pid)
//!    and a tmux-channel sub-agent (Tmux launch mode, has tmux_session +
//!    tmux_log_path) under the same parent, with the right launch-mode
//!    indicator on each. This is the smoke we promise in the ticket
//!    ("integration test: spawn a stream-json sub-agent, then a tmux-channel
//!    sub-agent … both visible in internal-bus agent list with correct
//!    lifecycle indicator").

use std::path::PathBuf;

use deskd::app::agent_provisioning::provision_for_channel_tmux;
use deskd::domain::config_types::ConfigLaunchMode;

fn unique_tmp(prefix: &str) -> PathBuf {
    PathBuf::from(format!(
        "/tmp/deskd-test-{}-{}",
        prefix,
        uuid::Uuid::new_v4()
    ))
}

/// AC: ".mcp.json env contains /tmp/deskd-<parent>-internal.sock"
///
/// The provisioning helper is the single source of truth that #504 and #502
/// both use (#504 for the top-level tmux REPL, #502 for tmux-channel sub
/// agents). We assert the exact contract `call_add_persistent_agent` relies
/// on: passing the internal-bus path produces a `.mcp.json` whose deskd
/// entry pins `DESKD_BUS_SOCKET` to that path.
#[tokio::test]
async fn tmux_channel_provisioning_writes_internal_bus_socket() {
    let _env_guard = deskd::test_support::env_lock().lock().await;

    let home = unique_tmp("502-prov-home");
    let work_dir = home.join("work");
    std::fs::create_dir_all(&work_dir).unwrap();

    // SAFETY: env_lock serializes env-mutating tests workspace-wide.
    unsafe { std::env::set_var("HOME", &home) };

    let parent = "parent-agent";
    let agent = "tmux-sub";
    // Same shape `call_add_persistent_agent` constructs via `InternalBus::start`.
    let internal_bus = PathBuf::from(format!("/tmp/deskd-{}-internal.sock", parent));

    provision_for_channel_tmux(&work_dir, agent, &internal_bus).expect("provisioning");

    // The .mcp.json lives inside the sub-agent's work_dir.
    let mcp_path = work_dir.join(".mcp.json");
    assert!(
        mcp_path.exists(),
        "tmux-channel provisioning must write {}",
        mcp_path.display()
    );

    let mcp: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&mcp_path).unwrap()).unwrap();
    let deskd_entry = &mcp["mcpServers"]["deskd"];
    assert_eq!(deskd_entry["command"].as_str(), Some("deskd"));
    let args = deskd_entry["args"].as_array().expect("args is array");
    assert_eq!(args[0].as_str(), Some("mcp-channel"));
    assert_eq!(args[1].as_str(), Some("--agent"));
    assert_eq!(args[2].as_str(), Some(agent));

    // The critical assertion: DESKD_BUS_SOCKET points at the *parent's
    // internal* socket, not at the parent's main bus.
    let bus_env = deskd_entry["env"]["DESKD_BUS_SOCKET"]
        .as_str()
        .expect("DESKD_BUS_SOCKET present in .mcp.json env");
    assert_eq!(
        bus_env,
        internal_bus.to_str().unwrap(),
        "tmux-channel sub-agent must talk to the parent's internal bus"
    );
    // Guard against the regression where someone wires the parent's main
    // bus path through here (the whole point of #502 is bus separation).
    assert!(
        bus_env.contains(&format!("deskd-{}-internal.sock", parent)),
        "DESKD_BUS_SOCKET must follow the /tmp/deskd-<parent>-internal.sock convention, got: {}",
        bus_env
    );

    // Cleanup so parallel test runs don't see stale fixtures.
    let _ = std::fs::remove_dir_all(&home);
}

/// AC: "both visible in internal-bus agent list with correct lifecycle
/// indicator". Builds two `AgentState` files under a fake HOME — one
/// stream-json (subprocess), one tmux-channel (tmux) — and asserts
/// `agent::list()` returns both with the right launch mode.
#[tokio::test]
async fn agent_list_surfaces_both_lifecycles_under_same_parent() {
    let _env_guard = deskd::test_support::env_lock().lock().await;

    let home = unique_tmp("502-list-home");
    std::fs::create_dir_all(home.join(".deskd/agents")).unwrap();
    // SAFETY: env_lock serializes env-mutating tests workspace-wide.
    unsafe { std::env::set_var("HOME", &home) };

    let parent_pwd = home.join("parent");
    std::fs::create_dir_all(&parent_pwd).unwrap();

    let sj_work = parent_pwd.join("sj");
    let tc_work = parent_pwd.join("tc");
    std::fs::create_dir_all(&sj_work).unwrap();
    std::fs::create_dir_all(&tc_work).unwrap();

    // Build a stream-json sub-agent state (post-create with launch_mode left
    // at the Subprocess default — exactly what the new code path produces
    // when `lifecycle: "stream-json"` is the resolved variant).
    let sj_cfg = deskd::app::agent::AgentConfig {
        name: "sj-sub".into(),
        model: "claude-sonnet-4-6".into(),
        system_prompt: "stream-json sub".into(),
        work_dir: sj_work.to_string_lossy().into(),
        max_turns: 50,
        unix_user: None,
        command: vec![],
        config_path: None,
        container: None,
        session: Default::default(),
        runtime: Default::default(),
        launch_mode: ConfigLaunchMode::Subprocess,
        kind: Default::default(),
        context: None,
        compact_threshold: None,
        auto_compact_threshold_tokens: None,
        empty_completion_threshold: None,
        empty_completion_restart_min_secs: None,
    };
    let sj_state = deskd::app::agent::create(&sj_cfg)
        .await
        .expect("create stream-json sub");
    // Mark as parent-of, mimicking the post-create state mutation
    // `call_add_persistent_agent` performs.
    {
        let mut s = sj_state.clone();
        s.parent = Some("parent-agent".into());
        s.pid = 12345; // fake pid so list() sees a subprocess indicator
        deskd::app::agent::save_state_pub(&s).unwrap();
    }

    // tmux-channel sub-agent: launch_mode flipped to Tmux + tmux_session
    // populated, mirroring spawn_tmux_channel_subagent's state writes.
    let tc_cfg = deskd::app::agent::AgentConfig {
        name: "tc-sub".into(),
        model: "claude-sonnet-4-6".into(),
        system_prompt: "tmux-channel sub".into(),
        work_dir: tc_work.to_string_lossy().into(),
        max_turns: 50,
        unix_user: None,
        command: vec![],
        config_path: None,
        container: None,
        session: Default::default(),
        runtime: Default::default(),
        launch_mode: ConfigLaunchMode::Tmux,
        kind: Default::default(),
        context: None,
        compact_threshold: None,
        auto_compact_threshold_tokens: None,
        empty_completion_threshold: None,
        empty_completion_restart_min_secs: None,
    };
    let tc_state = deskd::app::agent::create(&tc_cfg)
        .await
        .expect("create tmux-channel sub");
    {
        let mut s = tc_state.clone();
        s.parent = Some("parent-agent".into());
        s.pid = 0; // tmux agents carry no foreground pid
        s.tmux_session = Some("deskd-tc-sub".into());
        s.tmux_log_path = Some("/tmp/deskd-tc-sub.log".into());
        deskd::app::agent::save_state_pub(&s).unwrap();
    }

    // `agent::list` is the data source `deskd agent list` uses (which the
    // internal-bus path renders by reading state files under $HOME, see
    // commands/agent.rs `format_launcher_column`).
    let listed = deskd::app::agent::list().await.expect("list");
    let by_name: std::collections::HashMap<&str, &deskd::app::agent::AgentState> =
        listed.iter().map(|a| (a.config.name.as_str(), a)).collect();

    let sj = by_name
        .get("sj-sub")
        .expect("stream-json sub-agent must appear in list");
    assert_eq!(
        sj.config.launch_mode,
        ConfigLaunchMode::Subprocess,
        "stream-json sub must be Subprocess (regression guard for default)"
    );
    assert_eq!(sj.parent.as_deref(), Some("parent-agent"));
    assert!(sj.tmux_session.is_none());

    let tc = by_name
        .get("tc-sub")
        .expect("tmux-channel sub-agent must appear in list");
    assert_eq!(
        tc.config.launch_mode,
        ConfigLaunchMode::Tmux,
        "tmux-channel sub must be Tmux so `agent list` LAUNCHER column renders correctly"
    );
    assert_eq!(tc.parent.as_deref(), Some("parent-agent"));
    assert_eq!(tc.tmux_session.as_deref(), Some("deskd-tc-sub"));
    assert_eq!(tc.tmux_log_path.as_deref(), Some("/tmp/deskd-tc-sub.log"));
    assert_eq!(tc.pid, 0);

    // Cleanup.
    let _ = deskd::app::agent::remove("sj-sub").await;
    let _ = deskd::app::agent::remove("tc-sub").await;
    let _ = std::fs::remove_dir_all(&home);
}
