//! `deskd session` subcommand handlers (#453).
//!
//! Discovers, attaches to, and tails logs of deskd-managed tmux
//! sessions across the local host and SSH-reachable remotes
//! configured in `~/.deskd/config.yaml`.
//!
//! Naming + log layout come from the tmux launcher (#452):
//! sessions are `deskd-<agent>`, logs are
//! `/var/log/deskd/sessions/<agent>.log` (with the fallback chain
//! documented on [`crate::app::tmux_launcher::DEFAULT_SYSTEM_LOG_DIR`]).
//!
//! Read-only attach assumption: tmux `>=2.6` supports `attach -r`. Older
//! tmux versions had known bypasses; deskd targets a recent enough tmux
//! that the flag is reliable. Documented in README; no defensive
//! version check is performed beyond invoking the flag.

use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};

use crate::app::cli::SessionAction;
use crate::app::config_remotes::{RemoteEntry, RemotesConfig};
use crate::app::session_discover::{
    DiscoverScope, DiscoveryResult, Location, LocationStatus, RealSshExecutor, SessionEntry,
    discover,
};
use crate::app::tmux_launcher::{DEFAULT_SYSTEM_LOG_DIR, session_name_for};

use super::format_relative_time;

pub async fn handle(action: SessionAction) -> Result<()> {
    let config = RemotesConfig::load_default()?;
    match action {
        SessionAction::List {
            remote,
            local,
            json,
        } => list(&config, remote, local, json).await,
        SessionAction::Attach {
            agent,
            remote,
            read_only,
            new_window,
        } => attach(&config, &agent, remote, read_only, new_window).await,
        SessionAction::Log {
            agent,
            remote,
            log_dir,
        } => log_tail(&config, &agent, remote, log_dir).await,
    }
}

fn resolve_scope(
    config: &RemotesConfig,
    remote: Option<String>,
    local_only: bool,
) -> Result<DiscoverScope> {
    if local_only && remote.is_some() {
        anyhow::bail!("--local and --remote are mutually exclusive");
    }
    if local_only {
        return Ok(DiscoverScope::LocalOnly);
    }
    if let Some(name) = remote {
        if config.get(&name).is_none() {
            anyhow::bail!(
                "no remote named `{}` in ~/.deskd/config.yaml (configured: {})",
                name,
                if config.is_empty() {
                    "<none>".to_string()
                } else {
                    config
                        .iter()
                        .map(|(n, _)| n.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            );
        }
        return Ok(DiscoverScope::OnlyRemote(name));
    }
    Ok(DiscoverScope::All)
}

async fn list(
    config: &RemotesConfig,
    remote: Option<String>,
    local_only: bool,
    json: bool,
) -> Result<()> {
    let scope = resolve_scope(config, remote, local_only)?;
    let executor = Arc::new(RealSshExecutor);
    let result = discover(executor, config, scope).await;

    if json {
        let json_str = serde_json::to_string_pretty(&result)
            .context("failed to serialise discovery result as JSON")?;
        println!("{}", json_str);
        return Ok(());
    }

    render_table(&result);
    Ok(())
}

fn render_table(result: &DiscoveryResult) {
    if result.entries.is_empty() && result.location_status.is_empty() {
        println!("No deskd-* tmux sessions found.");
        return;
    }

    println!(
        "{:<10} {:<18} {:<10} {:<10} {:<10} ACTIVITY",
        "LOCATION", "SESSION", "AGENT", "STATUS", "ATTACHED"
    );
    println!("{}", "─".repeat(72));

    let now = chrono::Utc::now();

    for entry in &result.entries {
        let attached_label = if entry.attached {
            format!("yes ({})", entry.attached_count.max(1))
        } else {
            "no".to_string()
        };
        let activity = if entry.activity_epoch > 0 {
            let activity_dt =
                chrono::DateTime::<chrono::Utc>::from_timestamp(entry.activity_epoch, 0)
                    .unwrap_or(now);
            let dur = now.signed_duration_since(activity_dt);
            format!("{} ago", format_relative_time(dur))
        } else {
            "-".to_string()
        };
        println!(
            "{:<10} {:<18} {:<10} {:<10} {:<10} {}",
            entry.location.as_label(),
            entry.session,
            entry.agent,
            "running",
            attached_label,
            activity,
        );
    }

    // Surface unreachable / error locations as their own row, so users
    // never silently lose a remote.
    for (label, status) in &result.location_status {
        match status {
            LocationStatus::Ok => {}
            LocationStatus::Unreachable(msg) => {
                println!(
                    "{:<10} {:<18} {:<10} {:<10} {:<10} {}",
                    label,
                    "-",
                    "-",
                    "unreachable",
                    "-",
                    short_msg(msg, 32)
                );
            }
            LocationStatus::Error(msg) => {
                println!(
                    "{:<10} {:<18} {:<10} {:<10} {:<10} error: {}",
                    label,
                    "-",
                    "-",
                    "error",
                    "-",
                    short_msg(msg, 24)
                );
            }
        }
    }
}

fn short_msg(s: &str, max: usize) -> String {
    let first_line = s.lines().next().unwrap_or("").trim();
    if first_line.chars().count() <= max {
        first_line.to_string()
    } else {
        let mut taken = String::new();
        for (i, c) in first_line.chars().enumerate() {
            if i >= max {
                break;
            }
            taken.push(c);
        }
        format!("{}…", taken)
    }
}

async fn attach(
    config: &RemotesConfig,
    agent: &str,
    remote: Option<String>,
    read_only: bool,
    new_window: bool,
) -> Result<()> {
    // Explicit `--remote` bypasses discovery (saves an SSH round-trip
    // and avoids the collision UX).
    if let Some(remote_name) = remote {
        let entry = config
            .get(&remote_name)
            .ok_or_else(|| anyhow!("no remote named `{}` in ~/.deskd/config.yaml", remote_name))?;
        return run_remote_attach(&remote_name, entry, agent, read_only, new_window);
    }

    let scope = DiscoverScope::All;
    let executor = Arc::new(RealSshExecutor);
    let result = discover(executor, config, scope).await;

    let matches = result.find_agent(agent, None);
    if matches.is_empty() {
        anyhow::bail!(
            "no tmux session `deskd-{}` found locally or on any configured remote",
            agent
        );
    }

    let local_match = matches.iter().find(|e| e.location == Location::Local);
    let remote_match = matches
        .iter()
        .find(|e| matches!(e.location, Location::Remote(_)));

    match (local_match, remote_match) {
        (Some(_), Some(remote_entry)) => {
            // Collision: local + at least one remote also has a session
            // with the same agent name. Prefer local + emit a one-line
            // stderr warning so the user notices.
            let remote_label = remote_entry.location.as_label();
            eprintln!(
                "warning: agent `{}` also has a tmux session on `{}`; attaching to local. Pass `--remote {}` to attach there instead.",
                agent, remote_label, remote_label
            );
            run_local_attach(agent, read_only, new_window)
        }
        (Some(_), None) => run_local_attach(agent, read_only, new_window),
        (None, Some(rentry)) => {
            let Location::Remote(name) = &rentry.location else {
                unreachable!("filtered to remote above")
            };
            let entry = config
                .get(name)
                .ok_or_else(|| anyhow!("remote `{}` vanished from config", name))?;
            run_remote_attach(name, entry, agent, read_only, new_window)
        }
        (None, None) => unreachable!("matches non-empty but neither local nor remote — impossible"),
    }
}

fn run_local_attach(agent: &str, read_only: bool, new_window: bool) -> Result<()> {
    let session = session_name_for(agent);
    if new_window {
        // Nest into the user's current tmux session instead of
        // replacing it. `tmux new-window` only works when we're already
        // inside a tmux client (TMUX env var). Bail otherwise so the
        // user knows the flag was a no-op.
        if std::env::var_os("TMUX").is_none() {
            anyhow::bail!(
                "--new-window requires being inside a tmux session (TMUX env not set); run `tmux` first or omit the flag to attach in-place"
            );
        }
        // Use `tmux new-window` with the attach command — opens a new
        // window in the current session that immediately runs
        // `tmux attach -t deskd-<agent>` against the same server.
        let mut args = vec!["new-window".to_string()];
        let mut attach_cmd = format!("tmux attach -t {}", shell_escape(&session));
        if read_only {
            attach_cmd = format!("tmux attach -r -t {}", shell_escape(&session));
        }
        args.push(attach_cmd);
        return exec_replace("tmux", &args);
    }
    let mut args: Vec<String> = vec!["attach".to_string()];
    if read_only {
        args.push("-r".to_string());
    }
    args.push("-t".to_string());
    args.push(session);
    exec_replace("tmux", &args)
}

fn run_remote_attach(
    remote_name: &str,
    entry: &RemoteEntry,
    agent: &str,
    read_only: bool,
    new_window: bool,
) -> Result<()> {
    let session = session_name_for(agent);
    let attach_args = if read_only { "-r -t" } else { "-t" };
    let remote_cmd = format!("tmux attach {} {}", attach_args, shell_escape(&session));
    let mut args: Vec<String> = Vec::new();
    // -tt forces PTY allocation (required for interactive tmux attach).
    args.push("-tt".to_string());
    for opt in &entry.ssh_options {
        args.push(opt.clone());
    }
    args.push(entry.host.clone());
    args.push(remote_cmd.clone());

    if new_window {
        if std::env::var_os("TMUX").is_none() {
            anyhow::bail!(
                "--new-window requires being inside a tmux session (TMUX env not set); run `tmux` first or omit the flag to attach in-place"
            );
        }
        // Wrap the ssh command inside a new local tmux window.
        let inner = std::iter::once("ssh".to_string())
            .chain(args.iter().cloned())
            .collect::<Vec<_>>();
        let joined = inner
            .iter()
            .map(|p| shell_escape(p))
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!(
            "opening new tmux window for attach to {} via {}",
            agent, remote_name
        );
        return exec_replace("tmux", &["new-window".to_string(), joined]);
    }

    eprintln!(
        "attaching to {} via {} ({})",
        agent, remote_name, entry.host
    );
    exec_replace("ssh", &args)
}

/// Replace the current process with `program <args…>`. `exec_replace`
/// never returns on success (the new process takes over the PID), which
/// is what we want for `tmux attach` and `ssh -t`: control flow back to
/// the operator's terminal, then back to `deskd`'s shell on detach.
fn exec_replace(program: &str, args: &[String]) -> Result<()> {
    let mut cmd = Command::new(program);
    cmd.args(args);
    // On Unix, exec swaps the process image; any error returned is from
    // the exec call itself (e.g. binary not found).
    let err = cmd.exec();
    Err(anyhow::Error::from(err).context(format!("failed to exec `{}`", program)))
}

async fn log_tail(
    config: &RemotesConfig,
    agent: &str,
    remote: Option<String>,
    log_dir_override: Option<String>,
) -> Result<()> {
    // Explicit `--remote`: don't discover; tail straight on that host.
    if let Some(remote_name) = remote {
        let entry = config
            .get(&remote_name)
            .ok_or_else(|| anyhow!("no remote named `{}` in ~/.deskd/config.yaml", remote_name))?;
        return run_remote_log_tail(entry, agent, log_dir_override.as_deref());
    }

    // No --remote: try local first (matches the attach precedence).
    let local_log = match log_dir_override.as_deref() {
        Some(dir) => PathBuf::from(dir).join(format!("{}.log", agent)),
        None => local_log_path(agent),
    };
    if local_log.exists() {
        return run_local_log_tail(&local_log);
    }

    // Fall back to discovery: if a single remote has a deskd-<agent>
    // session, tail that. If more than one matches, ask the user to
    // pick.
    let executor = Arc::new(RealSshExecutor);
    let result = discover(executor, config, DiscoverScope::All).await;
    let remotes: Vec<&SessionEntry> = result
        .find_agent(agent, None)
        .into_iter()
        .filter(|e| matches!(e.location, Location::Remote(_)))
        .collect();
    match remotes.as_slice() {
        [] => anyhow::bail!(
            "no log found for agent `{}`: tried local path {} and no remote session matched",
            agent,
            local_log.display()
        ),
        [single] => {
            let Location::Remote(name) = &single.location else {
                unreachable!()
            };
            let entry = config
                .get(name)
                .ok_or_else(|| anyhow!("remote `{}` vanished from config", name))?;
            run_remote_log_tail(entry, agent, log_dir_override.as_deref())
        }
        many => {
            let names: Vec<String> = many
                .iter()
                .map(|e| e.location.as_label().to_string())
                .collect();
            anyhow::bail!(
                "agent `{}` exists on multiple remotes ({}); pass `--remote <name>` to disambiguate",
                agent,
                names.join(", ")
            );
        }
    }
}

/// Local log path for the agent, matching the #452 launcher's
/// resolution order. We don't try to *create* anything here — if the
/// file is absent, the caller falls through to the remote path.
fn local_log_path(agent: &str) -> PathBuf {
    let system = PathBuf::from(DEFAULT_SYSTEM_LOG_DIR).join(format!("{}.log", agent));
    if system.exists() {
        return system;
    }
    if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
        let p = PathBuf::from(xdg)
            .join("deskd")
            .join("sessions")
            .join(format!("{}.log", agent));
        if p.exists() {
            return p;
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("deskd")
            .join("sessions")
            .join(format!("{}.log", agent));
    }
    system
}

fn run_local_log_tail(path: &std::path::Path) -> Result<()> {
    // Use `tail -F` (capital F) so the tail survives log rotation —
    // matches what an operator would have typed by hand.
    let args = vec!["-F".to_string(), path.display().to_string()];
    exec_replace("tail", &args)
}

fn run_remote_log_tail(
    entry: &RemoteEntry,
    agent: &str,
    log_dir_override: Option<&str>,
) -> Result<()> {
    let remote_cmd = build_remote_log_cmd(agent, log_dir_override);
    let mut args: Vec<String> = Vec::new();
    args.push("-T".to_string());
    args.push("-o".to_string());
    args.push("BatchMode=yes".to_string());
    for opt in &entry.ssh_options {
        args.push(opt.clone());
    }
    args.push(entry.host.clone());
    args.push(remote_cmd);
    exec_replace("ssh", &args)
}

/// Build the remote shell command for `session log` over SSH.
///
/// All operator-supplied strings are passed through `shell_escape` so a
/// hostile agent name (e.g. `foo'; bad; echo '`) cannot break out of the
/// single-quoted echo and execute commands on the remote (#480).
fn build_remote_log_cmd(agent: &str, log_dir_override: Option<&str>) -> String {
    let log_path = match log_dir_override {
        Some(dir) => format!("{}/{}.log", dir.trim_end_matches('/'), agent),
        None => format!("{}/{}.log", DEFAULT_SYSTEM_LOG_DIR, agent),
    };
    // Try the system path first, fall back to the XDG state path. The
    // remote shell does the existence check so we don't need a
    // round-trip.
    let xdg_path = format!("$HOME/.local/state/deskd/sessions/{}.log", agent);
    format!(
        "if [ -f {sys} ]; then tail -F {sys}; elif [ -f {xdg} ]; then tail -F {xdg}; else echo 'no log for {a} at {sys} or {xdg}' >&2; exit 1; fi",
        sys = shell_escape(&log_path),
        xdg = shell_escape(&xdg_path),
        a = shell_escape(agent),
    )
}

/// POSIX-shell single-quote escape — same trick as `tmux_launcher`. We
/// duplicate the helper here so the modules don't have to expose
/// internal helpers.
fn shell_escape(s: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::session_discover::{Location, SessionEntry};

    fn entry(loc: Location, agent: &str) -> SessionEntry {
        SessionEntry {
            location: loc,
            session: format!("deskd-{}", agent),
            agent: agent.to_string(),
            attached: false,
            attached_count: 0,
            activity_epoch: 0,
            created_epoch: 0,
        }
    }

    #[test]
    fn collision_local_wins_with_warning_intent() {
        // The collision rule: when both local and a remote have the
        // same agent and no `--remote` was given, prefer local. This
        // unit test exercises the decision logic via
        // `DiscoveryResult::find_agent`, which the attach handler
        // consumes.
        let mut r = DiscoveryResult::new();
        r.entries.push(entry(Location::Local, "dev"));
        r.entries
            .push(entry(Location::Remote("vps".to_string()), "dev"));

        let matches = r.find_agent("dev", None);
        let local = matches.iter().find(|e| e.location == Location::Local);
        let remote = matches
            .iter()
            .find(|e| matches!(e.location, Location::Remote(_)));
        assert!(
            local.is_some(),
            "local match required for warn-and-prefer-local"
        );
        assert!(
            remote.is_some(),
            "remote match required for the warning to fire"
        );
    }

    #[test]
    fn collision_remote_only_no_warning() {
        let mut r = DiscoveryResult::new();
        r.entries
            .push(entry(Location::Remote("vps".to_string()), "dev"));
        let matches = r.find_agent("dev", None);
        assert!(
            matches
                .iter()
                .all(|e| matches!(e.location, Location::Remote(_)))
        );
    }

    #[test]
    fn resolve_scope_local_flag() {
        let cfg = RemotesConfig::default();
        let scope = resolve_scope(&cfg, None, true).unwrap();
        assert_eq!(scope, DiscoverScope::LocalOnly);
    }

    #[test]
    fn resolve_scope_unknown_remote_errors() {
        let cfg = RemotesConfig::default();
        let err = resolve_scope(&cfg, Some("ghost".to_string()), false).unwrap_err();
        assert!(err.to_string().contains("ghost"));
    }

    #[test]
    fn resolve_scope_local_and_remote_conflict_errors() {
        let cfg = RemotesConfig::default();
        let err = resolve_scope(&cfg, Some("vps".to_string()), true).unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn resolve_scope_known_remote_only() {
        let mut cfg = RemotesConfig::default();
        cfg.remotes.insert(
            "vps".to_string(),
            RemoteEntry {
                host: "vps".to_string(),
                ssh_options: vec![],
            },
        );
        let scope = resolve_scope(&cfg, Some("vps".to_string()), false).unwrap();
        assert_eq!(scope, DiscoverScope::OnlyRemote("vps".to_string()));
    }

    #[test]
    fn shell_escape_single_quotes_simple() {
        assert_eq!(shell_escape("hello"), "'hello'");
        assert_eq!(shell_escape("deskd-kira"), "'deskd-kira'");
    }

    #[test]
    fn shell_escape_handles_embedded_quote() {
        assert_eq!(shell_escape("it's"), "'it'\\''s'");
    }

    #[test]
    fn short_msg_truncates_with_ellipsis() {
        let s = "ssh: connect to host vps.example.com port 22: Connection refused";
        let out = short_msg(s, 20);
        assert!(out.ends_with('…'));
        assert!(out.chars().count() <= 21);
    }

    #[test]
    fn short_msg_keeps_short_strings_unchanged() {
        let out = short_msg("short", 32);
        assert_eq!(out, "short");
    }

    #[test]
    fn build_remote_log_cmd_normal_agent_quotes_name() {
        // Sanity: the agent name is shell-escaped in the error echo.
        let cmd = build_remote_log_cmd("kira", None);
        assert!(
            cmd.contains("'kira'"),
            "expected shell-escaped agent in cmd; got: {}",
            cmd
        );
        // The fall-through paths are shell-escaped too.
        assert!(
            cmd.contains("/var/log/deskd/sessions/kira.log"),
            "expected log path; got: {}",
            cmd
        );
    }

    #[test]
    fn build_remote_log_cmd_escapes_malicious_agent_name() {
        // #480: a hostile agent name with embedded single quotes must not
        // break out of the error echo's single-quoted string. The format
        // string previously substituted {a} as raw, allowing
        // `foo'; bad_cmd; echo '` to execute on the remote.
        let malicious = "alpha'; pwned; echo '";
        let cmd = build_remote_log_cmd(malicious, None);

        // The substituted slot now holds the shell-escaped form.
        let escaped = shell_escape(malicious);
        assert!(
            cmd.contains(&escaped),
            "expected escaped agent in cmd; got cmd={}",
            cmd
        );

        // The unsafe pre-fix shape `for alpha';` must NOT appear: that's
        // the literal pattern where the unquoted agent broke out before.
        assert!(
            !cmd.contains("for alpha';"),
            "agent name must not appear unquoted right after 'for '; got: {}",
            cmd
        );
    }
}
