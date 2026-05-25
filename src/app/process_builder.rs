//! Process construction — pure functions for building agent commands.
//!
//! Extracted from `agent.rs` (#280). Handles command assembly, container
//! wrapping, flag injection, tilde expansion, and mount normalization.

use std::process::Stdio;
use tokio::process::Command;

use crate::config::ContainerConfig;

use super::agent_registry::AgentConfig;
use super::context_size;

/// Compute the `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` env tuple for an agent.
/// Always emitted so the built-in 300k default is honoured even when the
/// user did not set a per-agent override. Logs a warning when the chosen
/// threshold would be silently clamped by Claude Code.
fn auto_compact_env_for(cfg: &AgentConfig) -> Option<(&'static str, String)> {
    let threshold = context_size::resolve_auto_compact_threshold(cfg.auto_compact_threshold_tokens);
    let window = context_size::context_window_for_model(&cfg.model);
    let pct = context_size::auto_compact_override_pct(threshold, window)?;
    if context_size::threshold_would_clamp(threshold, window) {
        tracing::warn!(
            agent = %cfg.name,
            model = %cfg.model,
            threshold_tokens = threshold,
            window_tokens = window,
            ceiling_pct = context_size::AUTO_COMPACT_PCT_CEILING,
            "auto_compact_threshold_tokens >= {}% of model window — Claude Code will clamp the override; effective compaction will fire earlier than configured",
            context_size::AUTO_COMPACT_PCT_CEILING,
        );
    }
    Some(("CLAUDE_AUTOCOMPACT_PCT_OVERRIDE", pct.to_string()))
}

/// Build the tokio Command for running the agent process.
/// Inject flags required for persistent stream-json operation.
///
/// Checks the agent's command array to avoid duplicating flags that were
/// explicitly set in workspace.yaml. Sub-agents created via MCP typically
/// have `command: ["claude"]` with no flags, so all are injected.
pub fn build_command(cfg: &AgentConfig, args: &[String], extra_env: &[(&str, &str)]) -> Command {
    let auto_compact = auto_compact_env_for(cfg);

    if let Some(ref container) = cfg.container {
        return build_container_command(cfg, container, args, extra_env, &auto_compact);
    }

    let (bin, prefix) = split_command(&cfg.command);
    let mut cmd = match &cfg.unix_user {
        Some(user) => {
            let mut c = Command::new("sudo");
            c.args(["-u", user, "-H", "--", "env"]);
            for (k, v) in extra_env {
                c.arg(format!("{}={}", k, v));
            }
            if let Some((k, v)) = &auto_compact {
                c.arg(format!("{}={}", k, v));
            }
            c.arg(bin);
            c.args(prefix);
            c.args(args);
            c.env_remove("SSH_AUTH_SOCK");
            c.env_remove("SSH_AGENT_PID");
            c
        }
        None => {
            let mut c = Command::new(bin);
            c.args(prefix);
            c.args(args);
            c
        }
    };
    if cfg.unix_user.is_none() {
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        if let Some((k, v)) = &auto_compact {
            cmd.env(k, v);
        }
    }
    cmd.current_dir(&cfg.work_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Build a command that runs the agent process inside a container.
///
/// Constructs: `<runtime> run --rm -i --name deskd-<agent>
///   -v <work_dir>:<work_dir> -w <work_dir>
///   -v <bus_socket_dir>:<bus_socket_dir>
///   [-v <mount>...] [-v <volume>...] [-e <key>=<val>...]
///   <image> <command> <args>`
fn build_container_command(
    cfg: &AgentConfig,
    container: &ContainerConfig,
    args: &[String],
    extra_env: &[(&str, &str)],
    auto_compact_env: &Option<(&'static str, String)>,
) -> Command {
    let runtime = &container.runtime;
    let mut cmd = Command::new(runtime);

    cmd.args(["run", "--rm", "-i"]);
    cmd.args(["--name", &format!("deskd-{}", cfg.name)]);

    // Mount the agent's work_dir so the process can access the repo.
    cmd.args(["-v", &format!("{}:{}", cfg.work_dir, cfg.work_dir)]);
    cmd.args(["-w", &cfg.work_dir]);

    // User-defined mounts from container config.
    for mount in &container.mounts {
        let expanded = normalize_mount(&expand_tilde(mount));
        cmd.args(["-v", &expanded]);
    }

    // Docker volumes.
    for vol in &container.volumes {
        cmd.args(["-v", vol]);
    }

    // Environment variables from container config.
    for (k, v) in &container.env {
        cmd.args(["-e", &format!("{}={}", k, v)]);
    }

    // Extra env vars (DESKD_BUS_SOCKET, DESKD_AGENT_NAME, DESKD_AGENT_CONFIG).
    for (k, v) in extra_env {
        cmd.args(["-e", &format!("{}={}", k, v)]);
    }

    // CLAUDE_AUTOCOMPACT_PCT_OVERRIDE — Claude Code's per-process knob for
    // when auto-compaction kicks in (percentage of context window).
    if let Some((k, v)) = auto_compact_env {
        cmd.args(["-e", &format!("{}={}", k, v)]);
    }

    // If there's a config_path, mount it into the container.
    if let Some(ref config_path) = cfg.config_path {
        let cp = std::path::PathBuf::from(config_path);
        if let Some(parent) = cp.parent() {
            let parent_str = parent.to_string_lossy();
            // Only mount if not already under work_dir.
            if !parent_str.starts_with(&cfg.work_dir) {
                cmd.args(["-v", &format!("{}:{}", parent_str, parent_str)]);
            }
        }
    }

    // Image.
    cmd.arg(&container.image);

    // The actual command to run inside the container.
    let (bin, prefix) = split_command(&cfg.command);
    cmd.arg(bin);
    cmd.args(prefix);
    cmd.args(args);

    // Don't set current_dir on the host — the container uses -w.
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Auto-inject flags required for stream-json operation (#151).
pub fn inject_required_flags(args: &mut Vec<String>, command: &[String]) {
    // --input-format=stream-json — needed for multimodal content and mid-task injection.
    args.push("--input-format=stream-json".to_string());

    // --output-format stream-json — worker expects JSON stdout, not plain text.
    if !command.iter().any(|a| a.contains("output-format")) {
        args.push("--output-format".to_string());
        args.push("stream-json".to_string());
    }

    // --verbose — required when using --output-format stream-json.
    if !command.iter().any(|a| a == "--verbose") {
        args.push("--verbose".to_string());
    }

    // --dangerously-skip-permissions — without this, Claude blocks on permission prompts.
    if !command
        .iter()
        .any(|a| a.contains("dangerously-skip-permissions"))
    {
        args.push("--dangerously-skip-permissions".to_string());
    }
}

/// Expand ~ to $HOME in mount paths.
/// Handles formats: "~/foo", "~/foo:ro", "~/foo:~/bar", "~/foo:~/bar:ro".
pub fn expand_tilde(path: &str) -> String {
    let home = match std::env::var("HOME") {
        Ok(h) => h,
        Err(_) => return path.to_string(),
    };

    let parts: Vec<&str> = path.splitn(3, ':').collect();
    let expand = |s: &str| -> String {
        if s.starts_with("~/") || s == "~" {
            s.replacen('~', &home, 1)
        } else {
            s.to_string()
        }
    };

    match parts.len() {
        1 => expand(parts[0]),
        2 => format!("{}:{}", expand(parts[0]), expand(parts[1])),
        3 => format!("{}:{}:{}", expand(parts[0]), expand(parts[1]), parts[2]),
        _ => path.to_string(),
    }
}

/// Normalize a mount spec to always have src:dst[:opts].
/// "path" → "path:path"
/// "path:ro" → "path:path:ro"
/// "src:dst" → "src:dst" (unchanged)
/// "src:dst:ro" → "src:dst:ro" (unchanged)
pub fn normalize_mount(mount: &str) -> String {
    let parts: Vec<&str> = mount.splitn(3, ':').collect();
    match parts.len() {
        1 => {
            format!("{}:{}", parts[0], parts[0])
        }
        2 => {
            if parts[1] == "ro" || parts[1] == "rw" {
                format!("{}:{}:{}", parts[0], parts[0], parts[1])
            } else {
                mount.to_string()
            }
        }
        _ => mount.to_string(),
    }
}

pub fn split_command(command: &[String]) -> (&str, &[String]) {
    match command {
        [] => ("claude", &[]),
        [bin] => (bin.as_str(), &[]),
        [bin, rest @ ..] => (bin.as_str(), rest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::config_types::{
        ConfigAgentKind, ConfigAgentRuntime, ConfigLaunchMode, ConfigSessionMode,
    };
    use std::collections::HashMap;

    #[test]
    fn test_expand_tilde_simple() {
        let result = expand_tilde("/absolute/path");
        assert_eq!(result, "/absolute/path");
    }

    #[test]
    fn test_expand_tilde_home() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_tilde("~/.ssh");
        assert_eq!(result, format!("{}/.ssh", home));
    }

    #[test]
    fn test_expand_tilde_with_ro() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_tilde("~/.ssh:ro");
        assert_eq!(result, format!("{}/.ssh:ro", home));
    }

    #[test]
    fn test_expand_tilde_src_dst() {
        let home = std::env::var("HOME").unwrap();
        let result = expand_tilde("~/.gitconfig:~/.gitconfig:ro");
        assert_eq!(
            result,
            format!("{}/.gitconfig:{}/.gitconfig:ro", home, home)
        );
    }

    #[test]
    fn test_normalize_mount_path_only() {
        assert_eq!(normalize_mount("/foo"), "/foo:/foo");
    }

    #[test]
    fn test_normalize_mount_path_ro() {
        assert_eq!(normalize_mount("/foo:ro"), "/foo:/foo:ro");
    }

    #[test]
    fn test_normalize_mount_src_dst() {
        assert_eq!(normalize_mount("/foo:/bar"), "/foo:/bar");
    }

    #[test]
    fn test_normalize_mount_src_dst_ro() {
        assert_eq!(normalize_mount("/foo:/bar:ro"), "/foo:/bar:ro");
    }

    #[test]
    fn test_split_command_empty() {
        let cmd: Vec<String> = vec![];
        let (bin, args) = split_command(&cmd);
        assert_eq!(bin, "claude");
        assert!(args.is_empty());
    }

    #[test]
    fn test_split_command_single() {
        let cmd = vec!["my-agent".to_string()];
        let (bin, args) = split_command(&cmd);
        assert_eq!(bin, "my-agent");
        assert!(args.is_empty());
    }

    #[test]
    fn test_split_command_with_args() {
        let cmd = vec![
            "claude".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
        ];
        let (bin, args) = split_command(&cmd);
        assert_eq!(bin, "claude");
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], "--output-format");
    }

    #[test]
    fn test_inject_required_flags_empty_command() {
        let mut args = Vec::new();
        let command = vec!["claude".to_string()];
        inject_required_flags(&mut args, &command);
        assert!(args.contains(&"--input-format=stream-json".to_string()));
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"--verbose".to_string()));
        assert!(args.contains(&"--dangerously-skip-permissions".to_string()));
    }

    #[test]
    fn test_inject_required_flags_no_duplicates() {
        let mut args = Vec::new();
        let command = vec![
            "claude".to_string(),
            "--output-format".to_string(),
            "stream-json".to_string(),
            "--verbose".to_string(),
            "--dangerously-skip-permissions".to_string(),
        ];
        inject_required_flags(&mut args, &command);
        assert!(args.contains(&"--input-format=stream-json".to_string()));
        assert!(!args.contains(&"--verbose".to_string()));
        assert!(!args.contains(&"--dangerously-skip-permissions".to_string()));
        let output_format_count = args.iter().filter(|a| a.contains("output-format")).count();
        assert_eq!(output_format_count, 0);
    }

    #[test]
    fn test_build_container_command() {
        let mut env = HashMap::new();
        env.insert("GH_TOKEN".to_string(), "test-token".to_string());

        let container = ContainerConfig {
            image: "claude-code-local:official".to_string(),
            mounts: vec!["/host/path:/container/path:ro".to_string()],
            volumes: vec!["my-vol:/data".to_string()],
            env,
            runtime: "docker".to_string(),
        };

        let cfg = AgentConfig {
            name: "test-agent".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            system_prompt: String::new(),
            work_dir: "/home/test".to_string(),
            max_turns: 100,
            unix_user: None,
            command: vec![
                "claude".to_string(),
                "--output-format".to_string(),
                "stream-json".to_string(),
            ],
            config_path: Some("/home/test/deskd.yaml".to_string()),
            container: Some(container),
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };

        let extra_env = [("DESKD_BUS_SOCKET", "/home/test/.deskd/bus.sock")];
        let args = vec!["--resume".to_string(), "session-1".to_string()];

        let cmd = build_command(&cfg, &args, &extra_env);
        let program = cmd.as_std().get_program().to_string_lossy().to_string();
        assert_eq!(program, "docker");

        let cmd_args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();

        assert!(cmd_args.contains(&"run".to_string()));
        assert!(cmd_args.contains(&"--rm".to_string()));
        assert!(cmd_args.contains(&"-i".to_string()));
        assert!(cmd_args.contains(&"deskd-test-agent".to_string()));
        assert!(cmd_args.contains(&"/home/test:/home/test".to_string()));
        assert!(cmd_args.contains(&"/home/test".to_string()));
        assert!(cmd_args.contains(&"claude-code-local:official".to_string()));
        assert!(cmd_args.contains(&"/host/path:/container/path:ro".to_string()));
        assert!(cmd_args.contains(&"my-vol:/data".to_string()));
        assert!(cmd_args.contains(&"GH_TOKEN=test-token".to_string()));
        assert!(cmd_args.contains(&"DESKD_BUS_SOCKET=/home/test/.deskd/bus.sock".to_string()));
        assert!(cmd_args.contains(&"claude".to_string()));
        assert!(cmd_args.contains(&"--output-format".to_string()));
        assert!(cmd_args.contains(&"stream-json".to_string()));
        assert!(cmd_args.contains(&"--resume".to_string()));
        assert!(cmd_args.contains(&"session-1".to_string()));
    }

    #[test]
    fn test_build_command_no_container() {
        let cfg = AgentConfig {
            name: "test".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            system_prompt: String::new(),
            work_dir: "/tmp".to_string(),
            max_turns: 100,
            unix_user: None,
            command: vec!["claude".to_string()],
            config_path: None,
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let cmd = build_command(&cfg, &[], &[]);
        let program = cmd.as_std().get_program().to_string_lossy().to_string();
        assert_eq!(program, "claude");
    }

    #[test]
    fn test_build_command_with_unix_user() {
        let cfg = AgentConfig {
            name: "test".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            system_prompt: String::new(),
            work_dir: "/tmp".to_string(),
            max_turns: 100,
            unix_user: Some("agent-user".to_string()),
            command: vec!["claude".to_string()],
            config_path: None,
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let extra_env = [("DESKD_BUS_SOCKET", "/tmp/bus.sock")];
        let cmd = build_command(&cfg, &[], &extra_env);
        let program = cmd.as_std().get_program().to_string_lossy().to_string();
        assert_eq!(program, "sudo");

        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(args.contains(&"-u".to_string()));
        assert!(args.contains(&"agent-user".to_string()));
        assert!(args.contains(&"DESKD_BUS_SOCKET=/tmp/bus.sock".to_string()));
    }

    #[test]
    fn auto_compact_env_for_4x_model_uses_default_300k_as_30pct() {
        // Default threshold (300k) on a 1M-window 4.x model → 30%.
        let cfg = AgentConfig {
            name: "test".into(),
            model: "claude-opus-4-7".into(),
            system_prompt: String::new(),
            work_dir: "/tmp".into(),
            max_turns: 100,
            unix_user: None,
            command: vec!["claude".into()],
            config_path: None,
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let env = auto_compact_env_for(&cfg).expect("env tuple");
        assert_eq!(env.0, "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE");
        assert_eq!(env.1, "30");
    }

    #[test]
    fn auto_compact_env_for_per_agent_override_wins() {
        let cfg = AgentConfig {
            name: "test".into(),
            model: "claude-opus-4-7".into(),
            system_prompt: String::new(),
            work_dir: "/tmp".into(),
            max_turns: 100,
            unix_user: None,
            command: vec!["claude".into()],
            config_path: None,
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: Some(500_000),
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let env = auto_compact_env_for(&cfg).expect("env tuple");
        // 500k of 1M = 50%.
        assert_eq!(env.1, "50");
    }

    #[test]
    fn auto_compact_env_clamps_above_ceiling_for_3x_model() {
        // 300k threshold on a 200k-window model would be 150% → clamped to 83.
        let cfg = AgentConfig {
            name: "test".into(),
            model: "claude-3-5-sonnet".into(),
            system_prompt: String::new(),
            work_dir: "/tmp".into(),
            max_turns: 100,
            unix_user: None,
            command: vec!["claude".into()],
            config_path: None,
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: None,
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let env = auto_compact_env_for(&cfg).expect("env tuple");
        assert_eq!(env.1, "83");
    }

    #[test]
    fn build_command_injects_auto_compact_env() {
        let cfg = AgentConfig {
            name: "test".into(),
            model: "claude-opus-4-7".into(),
            system_prompt: String::new(),
            work_dir: "/tmp".into(),
            max_turns: 100,
            unix_user: None,
            command: vec!["claude".into()],
            config_path: None,
            container: None,
            session: ConfigSessionMode::default(),
            runtime: ConfigAgentRuntime::default(),
            launch_mode: ConfigLaunchMode::default(),
            kind: ConfigAgentKind::default(),
            context: None,
            compact_threshold: None,
            auto_compact_threshold_tokens: Some(400_000),
            empty_completion_threshold: None,
            empty_completion_restart_min_secs: None,
        };
        let cmd = build_command(&cfg, &[], &[]);
        let envs: Vec<(String, String)> = cmd
            .as_std()
            .get_envs()
            .filter_map(|(k, v)| {
                let key = k.to_string_lossy().to_string();
                let val = v?.to_string_lossy().to_string();
                Some((key, val))
            })
            .collect();
        let auto = envs
            .iter()
            .find(|(k, _)| k == "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE")
            .expect("auto-compact env injected");
        // 400k / 1M = 40%.
        assert_eq!(auto.1, "40");
    }
}
