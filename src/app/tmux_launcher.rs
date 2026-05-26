//! tmux launcher for agent Claude REPLs (#452).
//!
//! Launches the agent's Claude REPL in a detached tmux session named
//! `deskd-<agent>`, so the session survives SSH disconnects and can be
//! attached for inspection. Designed to coexist with the existing subprocess
//! agent runtime — opt-in via `--tmux` flag on `deskd agent start` or
//! `launch_mode: tmux` in per-agent yaml.
//!
//! The session runs `claude --dangerously-skip-permissions
//! --dangerously-load-development-channels server:deskd` so it receives MCP
//! channel events from deskd (#451) without blocking on the Bypass-Permissions
//! mode-acceptance consent prompt (#510).
//!
//! Logs are captured via `tmux pipe-pane` to `/var/log/deskd/sessions/<agent>.log`
//! (or `~/.local/state/deskd/sessions/<agent>.log` when the system path is not
//! writable, typical for non-root local dev).

use anyhow::{Context, Result, anyhow, bail};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Default system-wide log dir for tmux session output.
pub const DEFAULT_SYSTEM_LOG_DIR: &str = "/var/log/deskd/sessions";

/// Result of parsing `tmux list-sessions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TmuxStatus {
    pub name: String,
    pub attached: bool,
}

/// Information returned after launching a tmux session.
#[derive(Debug, Clone)]
pub struct TmuxSession {
    pub session_name: String,
    pub log_path: PathBuf,
}

/// Compose the tmux session name for an agent.
pub fn session_name_for(agent: &str) -> String {
    format!("deskd-{}", agent)
}

/// Minimal description of an agent for pre-flight / launch.
///
/// Only the fields needed by the tmux launcher are included so this module
/// stays decoupled from the registry/config structs.
pub struct LaunchTarget<'a> {
    pub name: &'a str,
    pub home_dir: &'a Path,
}

/// Pre-flight: verify the host is ready to launch a tmux Claude REPL.
///
/// Checks performed (filesystem-first, fail-cheap before PATH lookups):
/// - agent home dir exists and is readable
/// - `.mcp.json` exists in agent home dir
/// - `tmux` binary on PATH
/// - `claude` binary on PATH
///
/// Each failure carries a specific, actionable message so the user can fix
/// the underlying issue without re-reading code.
pub fn check_tmux_runtime_prereqs(target: &LaunchTarget<'_>) -> Result<()> {
    let home = target.home_dir;
    let meta = std::fs::metadata(home).with_context(|| {
        format!(
            "agent home directory does not exist or is not readable: {}",
            home.display()
        )
    })?;
    if !meta.is_dir() {
        bail!(
            "agent home path is not a directory: {} (set `work_dir:` to a directory the agent owns)",
            home.display()
        );
    }

    let mcp_json = home.join(".mcp.json");
    if !mcp_json.exists() {
        bail!(
            "missing {} — tmux Claude REPL needs an `.mcp.json` registering deskd as an MCP server (see contrib/example-mcp.json)",
            mcp_json.display()
        );
    }

    which_in_path("tmux")
        .context("tmux is required to launch agents in tmux mode but was not found on PATH; install tmux (e.g. `apt install tmux` or `brew install tmux`)")?;
    which_in_path("claude")
        .context("`claude` CLI is required to launch agents in tmux mode but was not found on PATH; install Claude Code from https://docs.claude.com/en/docs/claude-code")?;

    Ok(())
}

/// Look up a binary on $PATH. Returns Ok(path) if found.
fn which_in_path(bin: &str) -> Result<PathBuf> {
    let path_env = std::env::var_os("PATH")
        .ok_or_else(|| anyhow!("PATH is not set; cannot locate `{}`", bin))?;
    for dir in std::env::split_paths(&path_env) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(anyhow!("`{}` not found on PATH", bin))
}

/// Resolve the log directory to use for tmux sessions.
///
/// Tries (in order):
/// 1. `DEFAULT_SYSTEM_LOG_DIR` (`/var/log/deskd/sessions`) if it exists and is
///    writable, or can be created.
/// 2. `$XDG_STATE_HOME/deskd/sessions` if `XDG_STATE_HOME` is set.
/// 3. `$HOME/.local/state/deskd/sessions`.
///
/// The chosen directory is created (recursively) if missing.
pub fn resolve_log_dir() -> Result<PathBuf> {
    // First choice: system log dir.
    let system = PathBuf::from(DEFAULT_SYSTEM_LOG_DIR);
    if can_use_dir(&system) {
        return Ok(system);
    }

    // Second: $XDG_STATE_HOME/deskd/sessions.
    if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
        let p = PathBuf::from(xdg).join("deskd").join("sessions");
        if can_use_dir(&p) {
            return Ok(p);
        }
    }

    // Third: $HOME/.local/state/deskd/sessions.
    let home = std::env::var_os("HOME")
        .ok_or_else(|| anyhow!("$HOME is not set; cannot resolve fallback log directory"))?;
    let p = PathBuf::from(home)
        .join(".local")
        .join("state")
        .join("deskd")
        .join("sessions");
    if can_use_dir(&p) {
        return Ok(p);
    }

    bail!(
        "no writable log directory: tried {}, $XDG_STATE_HOME/deskd/sessions, and ~/.local/state/deskd/sessions",
        DEFAULT_SYSTEM_LOG_DIR
    )
}

/// Return true if `dir` exists and is writable, or can be created with the
/// current user's permissions.
fn can_use_dir(dir: &Path) -> bool {
    if dir.exists() {
        // Probe writability by attempting to create a temp file.
        let probe = dir.join(".deskd-write-probe");
        match std::fs::File::create(&probe) {
            Ok(_) => {
                let _ = std::fs::remove_file(&probe);
                true
            }
            Err(_) => false,
        }
    } else {
        std::fs::create_dir_all(dir).is_ok()
    }
}

/// Run `tmux has-session -t <name>` and return true if the session exists.
pub fn tmux_session_exists(name: &str) -> Result<bool> {
    let out = Command::new("tmux")
        .args(["has-session", "-t", name])
        .output()
        .context("failed to invoke `tmux has-session`")?;
    Ok(out.status.success())
}

/// Launch a detached tmux session running the agent's Claude REPL.
///
/// The session runs `claude --dangerously-skip-permissions
/// --dangerously-load-development-channels server:deskd` in the agent's home
/// directory. Output is mirrored to a log file via `tmux pipe-pane`.
///
/// Returns an `Err` if a session named `deskd-<agent>` already exists; the
/// caller should print the user-facing "attach with `tmux attach -t ...`" hint.
pub fn launch_tmux_session(target: &LaunchTarget<'_>, log_dir: &Path) -> Result<TmuxSession> {
    let session = session_name_for(target.name);

    if tmux_session_exists(&session)? {
        bail!(
            "tmux session `{}` already running; attach with `tmux attach -t {}`",
            session,
            session
        );
    }

    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("failed to create log dir {}", log_dir.display()))?;
    let log_path = log_dir.join(format!("{}.log", target.name));

    // Build the command tmux will execute inside the session.
    let claude_cmd = "claude --dangerously-skip-permissions --dangerously-load-development-channels server:deskd";

    // tmux new-session -d -s <name> -c <home_dir> '<cmd>'
    let new_session_status = Command::new("tmux")
        .args([
            "new-session",
            "-d",
            "-s",
            &session,
            "-c",
            target
                .home_dir
                .to_str()
                .ok_or_else(|| anyhow!("agent home dir is not valid UTF-8"))?,
            claude_cmd,
        ])
        .status()
        .context("failed to invoke `tmux new-session`")?;
    if !new_session_status.success() {
        bail!(
            "`tmux new-session` exited with status {:?}",
            new_session_status.code()
        );
    }

    // Pipe pane output to the log file. `-o` toggles the pipe; we want it on,
    // and since the session is fresh, this enables it.
    let pipe_cmd = format!(
        "cat >> {}",
        shell_escape(
            log_path
                .to_str()
                .ok_or_else(|| anyhow!("log path is not valid UTF-8"))?,
        )
    );
    let pipe_status = Command::new("tmux")
        .args(["pipe-pane", "-t", &session, "-o", &pipe_cmd])
        .status()
        .context("failed to invoke `tmux pipe-pane`")?;
    if !pipe_status.success() {
        // Don't tear down the session if logging setup fails — surface a
        // warning instead. The session is still running and operators can
        // recover by attaching directly.
        eprintln!(
            "warning: `tmux pipe-pane` exited with status {:?} (session is up but logs are not being captured to {})",
            pipe_status.code(),
            log_path.display()
        );
    }

    Ok(TmuxSession {
        session_name: session,
        log_path,
    })
}

/// Kill the tmux session for `agent`. Returns Ok(()) whether the session
/// existed or not — "no such session" is not an error from the user's
/// perspective (idempotent stop).
pub fn kill_tmux_session(agent: &str) -> Result<()> {
    let session = session_name_for(agent);
    let out = Command::new("tmux")
        .args(["kill-session", "-t", &session])
        .output()
        .context("failed to invoke `tmux kill-session`")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("can't find session") || stderr.contains("session not found") {
            // Idempotent: nothing to kill.
            return Ok(());
        }
        bail!(
            "`tmux kill-session -t {}` failed: {}",
            session,
            stderr.trim()
        );
    }
    Ok(())
}

/// List all `deskd-*` tmux sessions on the host, keyed by session name.
pub fn list_tmux_sessions() -> Result<HashMap<String, TmuxStatus>> {
    let out = Command::new("tmux")
        .args([
            "list-sessions",
            "-F",
            "#{session_name}\t#{?session_attached,attached,detached}",
        ])
        .output()
        .context("failed to invoke `tmux list-sessions`")?;

    // `tmux list-sessions` exits 1 with "no server running" when there are no
    // sessions at all — treat that as an empty list.
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("no server running") || stderr.contains("no sessions") {
            return Ok(HashMap::new());
        }
        bail!("`tmux list-sessions` failed: {}", stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&out.stdout);
    Ok(parse_list_sessions(&stdout))
}

/// Parse the output of `tmux list-sessions -F '#{session_name}\t#{?session_attached,attached,detached}'`.
///
/// Only `deskd-*` sessions are returned — other tmux sessions on the host are
/// ignored.
pub fn parse_list_sessions(stdout: &str) -> HashMap<String, TmuxStatus> {
    let mut out = HashMap::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let name = match parts.next() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let state = parts.next().unwrap_or("detached");
        if !name.starts_with("deskd-") {
            continue;
        }
        out.insert(
            name.clone(),
            TmuxStatus {
                name,
                attached: state == "attached",
            },
        );
    }
    out
}

/// POSIX-shell single-quote escape for use inside `tmux pipe-pane` commands.
fn shell_escape(s: &str) -> String {
    // Wrap in single quotes; escape any embedded single quotes via the
    // standard `'\''` trick.
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

/// Generate a systemd user unit file for managing the tmux session for `agent`.
///
/// `binary_path` is the absolute path to the deskd binary that should ExecStart
/// the session. The unit is `Type=forking` because `tmux new-session -d` exits
/// immediately after forking the session.
pub fn render_systemd_unit(agent: &str, binary_path: &Path) -> String {
    let binary = binary_path.display();
    format!(
        "[Unit]\n\
Description=deskd agent: {agent} (tmux REPL)\n\
After=network.target\n\
\n\
[Service]\n\
Type=forking\n\
ExecStart={binary} agent start {agent} --tmux\n\
ExecStop={binary} agent stop {agent}\n\
Restart=on-failure\n\
RestartSec=5s\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        agent = agent,
        binary = binary,
    )
}

/// Default install path for the systemd user unit for `agent`.
pub fn systemd_unit_install_path(agent: &str) -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .ok_or_else(|| anyhow!("$HOME is not set; cannot resolve systemd unit install path"))?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("systemd")
        .join("user")
        .join(format!("deskd-{}.service", agent)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_name_uses_deskd_prefix() {
        assert_eq!(session_name_for("dev"), "deskd-dev");
        assert_eq!(session_name_for("kira"), "deskd-kira");
    }

    #[test]
    fn parse_list_sessions_extracts_deskd_only() {
        let stdout = "deskd-dev\tdetached\n\
                      deskd-kira\tattached\n\
                      personal\tdetached\n\
                      other-thing\tattached\n";
        let parsed = parse_list_sessions(stdout);
        assert_eq!(parsed.len(), 2);
        assert_eq!(
            parsed.get("deskd-dev"),
            Some(&TmuxStatus {
                name: "deskd-dev".to_string(),
                attached: false,
            })
        );
        assert_eq!(
            parsed.get("deskd-kira"),
            Some(&TmuxStatus {
                name: "deskd-kira".to_string(),
                attached: true,
            })
        );
    }

    #[test]
    fn parse_list_sessions_empty_string() {
        let parsed = parse_list_sessions("");
        assert!(parsed.is_empty());
    }

    #[test]
    fn parse_list_sessions_handles_trailing_whitespace() {
        let stdout = "deskd-dev\tdetached\n\n  \n";
        let parsed = parse_list_sessions(stdout);
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn render_systemd_unit_includes_required_lines() {
        let unit = render_systemd_unit("dev", Path::new("/usr/local/bin/deskd"));
        assert!(unit.contains("[Unit]"));
        assert!(unit.contains("[Service]"));
        assert!(unit.contains("[Install]"));
        assert!(unit.contains("Description=deskd agent: dev (tmux REPL)"));
        assert!(unit.contains("ExecStart=/usr/local/bin/deskd agent start dev --tmux"));
        assert!(unit.contains("ExecStop=/usr/local/bin/deskd agent stop dev"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WantedBy=default.target"));
        assert!(unit.contains("Type=forking"));
    }

    #[test]
    fn shell_escape_wraps_in_single_quotes() {
        assert_eq!(shell_escape("hello"), "'hello'");
        assert_eq!(shell_escape("/var/log/deskd.log"), "'/var/log/deskd.log'");
    }

    #[test]
    fn shell_escape_handles_embedded_quotes() {
        assert_eq!(shell_escape("it's"), "'it'\\''s'");
    }

    /// Pre-flight returns a specific error when tmux is missing.
    #[test]
    fn preflight_missing_tmux_or_claude_error_is_actionable() {
        // Set PATH to an empty dir so neither tmux nor claude resolves.
        // SAFETY: tests in this crate are run with --test-threads=4; serialising
        // env access here would be ideal, but since we only mutate within the
        // test and restore, the risk is low. Use a scope guard pattern.
        let prev_path = std::env::var_os("PATH");
        let tmp = std::env::temp_dir().join(format!(
            "deskd-tmux-preflight-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        // SAFETY: env::set_var is called within a single-threaded test scope.
        unsafe {
            std::env::set_var("PATH", &tmp);
        }
        let result = which_in_path("tmux");
        // Restore PATH before asserting so a failure doesn't leak state.
        unsafe {
            match prev_path {
                Some(v) => std::env::set_var("PATH", v),
                None => std::env::remove_var("PATH"),
            }
        }
        let _ = std::fs::remove_dir_all(&tmp);
        let err = result.unwrap_err();
        assert!(err.to_string().contains("tmux"));
    }

    #[test]
    fn preflight_missing_home_dir_specific_error() {
        let target = LaunchTarget {
            name: "ghost",
            home_dir: Path::new("/this/path/does/not/exist/deskd-test"),
        };
        let err = check_tmux_runtime_prereqs(&target).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("agent home directory") || msg.contains("does not exist"),
            "expected actionable home-missing error, got: {}",
            msg
        );
    }

    #[test]
    fn preflight_missing_mcp_json_specific_error() {
        // Build a tmpdir that exists but has no .mcp.json.
        let tmp = std::env::temp_dir().join(format!(
            "deskd-tmux-mcp-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();

        // Only run this assertion if tmux + claude are on PATH. Otherwise the
        // earlier checks short-circuit before reaching the .mcp.json probe.
        if which_in_path("tmux").is_ok() && which_in_path("claude").is_ok() {
            let target = LaunchTarget {
                name: "ghost",
                home_dir: &tmp,
            };
            let err = check_tmux_runtime_prereqs(&target).unwrap_err();
            let msg = format!("{:#}", err);
            assert!(
                msg.contains(".mcp.json"),
                "expected .mcp.json-missing error, got: {}",
                msg
            );
        }

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn systemd_unit_install_path_is_under_home() {
        // Set $HOME for the duration of the test.
        let prev_home = std::env::var_os("HOME");
        // SAFETY: scoped env mutation, restored before assertion.
        unsafe {
            std::env::set_var("HOME", "/home/testuser");
        }
        let path = systemd_unit_install_path("dev").unwrap();
        unsafe {
            match prev_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
        assert_eq!(
            path,
            PathBuf::from("/home/testuser/.config/systemd/user/deskd-dev.service")
        );
    }
}
