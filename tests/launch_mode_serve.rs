//! Integration test for #504: `deskd serve` honoring `launch_mode: tmux`.
//!
//! True end-to-end coverage (forking real `tmux new-session -d 'claude …'`)
//! cannot live in CI — it needs tmux + a working claude binary + a Claude
//! account. Instead this test exercises the *resolution + state recording*
//! path that `serve` invokes before the actual launch:
//!
//! 1. A workspace.yaml with two top-level agents:
//!    - `subprocess-agent` — `launch_mode: subprocess` (default)
//!    - `tmux-agent` — `launch_mode: tmux`
//! 2. For each agent, parse the workspace, load its per-agent deskd.yaml,
//!    call `create_or_recover` (the same call `serve.rs` makes), and assert
//!    the resulting `AgentState.config.launch_mode` matches the YAML.
//! 3. Verify `agent::list()` returns both agents with the right modes.
//! 4. Verify `serve_with_options(_, include_tmux=true)` is the entry point
//!    (compile-only — actually running `serve` would block on Ctrl-C).
//!
//! AC#7 (default = Subprocess when absent) is covered by the no-launch_mode
//! yaml + asserting `Subprocess`.

use std::path::PathBuf;

use deskd::app::agent;
use deskd::config::{AgentDef, UserConfig, WorkspaceConfig};
use deskd::infra::dto::ConfigLaunchMode;

fn unique_tmp(prefix: &str) -> PathBuf {
    PathBuf::from(format!(
        "/tmp/deskd-test-{}-{}",
        prefix,
        uuid::Uuid::new_v4()
    ))
}

/// Two-agent workspace exercise: subprocess + tmux, both end up in agent
/// state with the right `launch_mode`. This is the substrate `deskd serve`
/// builds on; the real subprocess spawn / tmux launch happens after this
/// resolution, so verifying it pins down AC#1, AC#7, AC#8 without spawning
/// real claude processes.
#[tokio::test]
async fn serve_resolves_launch_mode_for_workspace() {
    let _env_guard = deskd::test_support::env_lock().lock().await;

    let home = unique_tmp("504-serve");
    // SAFETY: ENV_LOCK serializes env-mutating tests workspace-wide.
    unsafe { std::env::set_var("HOME", &home) };
    std::fs::create_dir_all(home.join(".deskd/agents")).unwrap();

    let sub_work = home.join("subprocess-agent");
    let tmux_work = home.join("tmux-agent");
    std::fs::create_dir_all(&sub_work).unwrap();
    std::fs::create_dir_all(&tmux_work).unwrap();

    // Per-agent deskd.yaml for the tmux agent. The subprocess agent omits
    // launch_mode entirely → must default to Subprocess (AC#7).
    std::fs::write(
        sub_work.join("deskd.yaml"),
        "model: claude-sonnet-4-6\nsystem_prompt: subprocess test agent\n",
    )
    .unwrap();
    std::fs::write(
        tmux_work.join("deskd.yaml"),
        "model: claude-sonnet-4-6\nsystem_prompt: tmux test agent\nlaunch_mode: tmux\n",
    )
    .unwrap();

    // Workspace.yaml on disk — the file form `WorkspaceConfig::load` parses.
    let workspace_yaml = format!(
        "agents:\n  - name: subprocess-agent\n    work_dir: {sub}\n  - name: tmux-agent\n    work_dir: {tmx}\n",
        sub = sub_work.display(),
        tmx = tmux_work.display(),
    );
    let workspace_path = home.join("workspace.yaml");
    std::fs::write(&workspace_path, &workspace_yaml).unwrap();

    let workspace =
        WorkspaceConfig::load(&workspace_path.to_string_lossy()).expect("parse workspace");
    assert_eq!(workspace.agents.len(), 2);

    // Mirror what `serve.rs` does for each AgentDef: load per-agent
    // deskd.yaml, then `create_or_recover` with it.
    for def in &workspace.agents {
        let cfg_path = def.config_path();
        let user_cfg = UserConfig::load(&cfg_path).ok();
        let state = agent::create_or_recover(def, user_cfg.as_ref())
            .await
            .expect("create_or_recover");
        if def.name == "subprocess-agent" {
            assert_eq!(
                state.config.launch_mode,
                ConfigLaunchMode::Subprocess,
                "missing launch_mode in yaml must default to Subprocess (AC#7)"
            );
        } else {
            assert_eq!(
                state.config.launch_mode,
                ConfigLaunchMode::Tmux,
                "per-agent yaml `launch_mode: tmux` must propagate into AgentState"
            );
        }
    }

    // `agent::list` (the source `deskd agent list` reads) must surface both.
    let listed = agent::list().await.unwrap();
    let by_name: std::collections::HashMap<&str, &ConfigLaunchMode> = listed
        .iter()
        .map(|a| (a.config.name.as_str(), &a.config.launch_mode))
        .collect();
    assert_eq!(by_name.len(), 2);
    assert_eq!(by_name["subprocess-agent"], &ConfigLaunchMode::Subprocess);
    assert_eq!(by_name["tmux-agent"], &ConfigLaunchMode::Tmux);

    // Compile-time guard: the new `serve_with_options(_, include_tmux)`
    // entry point exists and accepts the documented `(String, bool)`
    // signature. We don't actually run it — it would block on Ctrl-C and
    // try to launch real processes — but referencing it pins the symbol.
    let _entry = deskd::app::serve::serve_with_options;
    let _ = |cfg: String, flag: bool| _entry(cfg, flag);

    // Clean up.
    for name in ["subprocess-agent", "tmux-agent"] {
        let _ = agent::remove(name).await;
    }
    let _ = std::fs::remove_dir_all(&home);
}

/// Workspace `AgentDef.launch_mode: tmux` (set in workspace.yaml itself
/// rather than the per-agent deskd.yaml) is also honored — covers the
/// fallback branch of the resolution order.
#[tokio::test]
async fn workspace_def_launch_mode_tmux_propagates() {
    let _env_guard = deskd::test_support::env_lock().lock().await;

    let home = unique_tmp("504-def");
    // SAFETY: ENV_LOCK serializes env-mutating tests workspace-wide.
    unsafe { std::env::set_var("HOME", &home) };
    std::fs::create_dir_all(home.join(".deskd/agents")).unwrap();

    let work = home.join("def-tmux-agent");
    std::fs::create_dir_all(&work).unwrap();

    // No per-agent yaml at all — fallback path uses `def.launch_mode`.
    let def = AgentDef {
        name: "def-tmux-agent".into(),
        unix_user: None,
        work_dir: work.to_string_lossy().into(),
        config: None,
        telegram: None,
        discord: None,
        model: None,
        command: vec!["claude".into()],
        container: None,
        runtime: Default::default(),
        launch_mode: ConfigLaunchMode::Tmux,
        bus_socket: None,
    };

    let state = agent::create_or_recover(&def, None).await.unwrap();
    assert_eq!(state.config.launch_mode, ConfigLaunchMode::Tmux);

    let _ = agent::remove("def-tmux-agent").await;
    let _ = std::fs::remove_dir_all(&home);
}
