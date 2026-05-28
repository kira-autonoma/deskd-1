//! `deskd agent doctor` CLI handler — collects on-disk signals and runs
//! the heuristic engine in `app::doctor` for each agent.

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::app::doctor::{
    DEFAULT_EMPTY_COMPLETION_THRESHOLD, DoctorInputs, DoctorThresholds, Verdict, diagnose, fmt_secs,
};
use crate::app::{agent, tasklog, unified_inbox};

/// Collect inputs for one agent's diagnosis from on-disk state.
///
/// Pure-data return — no decision-making here, the heuristic engine in
/// `doctor::diagnose` consumes the resulting `DoctorInputs`.
fn collect_inputs(
    agent_name: &str,
    state_pid: u32,
    last_n: usize,
    now: DateTime<Utc>,
) -> Result<(Vec<tasklog::TaskLog>, Option<DateTime<Utc>>)> {
    // Recent task log entries (oldest-first, capped to last_n).
    let recent_tasks = tasklog::read_logs(agent_name, last_n, None, None).unwrap_or_default();

    // Latest message in the agent's input inbox (`agent/<name>` inbox).
    let inbox_name = format!("agent/{}", agent_name);
    let latest_inbox_ts = unified_inbox::read_messages(&inbox_name, 1, None)
        .ok()
        .and_then(|msgs| msgs.last().map(|m| m.ts));

    let _ = state_pid;
    let _ = now;
    Ok((recent_tasks, latest_inbox_ts))
}

/// Whether the OS process for `pid` is alive.
fn process_alive(pid: u32) -> bool {
    pid > 0 && std::path::Path::new(&format!("/proc/{}", pid)).exists()
}

/// Run the doctor for one agent and return the verdict + collected tasks.
pub fn diagnose_one(
    agent_name: &str,
    last: usize,
    thresholds: &DoctorThresholds,
) -> Result<(Verdict, Vec<tasklog::TaskLog>, Option<DateTime<Utc>>)> {
    let state = agent::load_state(agent_name)?;
    let now = Utc::now();
    let (tasks, latest_inbox_ts) = collect_inputs(agent_name, state.pid, last, now)?;
    let input = DoctorInputs {
        agent_name,
        state_pid: state.pid,
        process_alive: process_alive(state.pid),
        recent_tasks: &tasks,
        latest_inbox_ts,
        now,
    };
    let verdict = diagnose(&input, thresholds);
    Ok((verdict, tasks, latest_inbox_ts))
}

/// Top-level CLI handler for `deskd agent doctor [name]`.
pub async fn handle(
    name: Option<String>,
    last: usize,
    empty_threshold: Option<usize>,
    idle_minutes: Option<i64>,
    stuck_minutes: Option<i64>,
) -> Result<()> {
    let mut thresholds = DoctorThresholds::default();
    if let Some(n) = empty_threshold {
        thresholds.empty_completion_threshold = n;
    }
    if let Some(n) = idle_minutes {
        thresholds.idle_minutes_threshold = n;
    }
    if let Some(n) = stuck_minutes {
        thresholds.stuck_queue_minutes = n;
    }

    match name {
        Some(name) => print_detailed(&name, last, &thresholds)?,
        None => print_summary(last, &thresholds).await?,
    }
    Ok(())
}

fn print_summary_header() {
    println!(
        "{:<15} {:<11} {:<13} SIGNAL",
        "NAME", "VERDICT", "LAST GOOD"
    );
}

async fn print_summary(last: usize, thresholds: &DoctorThresholds) -> Result<()> {
    let agents = agent::list().await?;
    if agents.is_empty() {
        println!("No agents registered");
        return Ok(());
    }
    print_summary_header();
    let mut problems: Vec<(String, String)> = Vec::new();
    for a in &agents {
        let name = &a.config.name;
        let (verdict, _tasks, _inbox) = match diagnose_one(name, last, thresholds) {
            Ok(v) => v,
            Err(e) => {
                println!("{:<15} ⚠ error      —             {}", name, e);
                continue;
            }
        };
        let last_good = match &verdict {
            Verdict::Healthy {
                last_good_age_secs: Some(s),
            }
            | Verdict::Idle {
                last_good_age_secs: s,
            } => fmt_secs(*s) + " ago",
            Verdict::Hung {
                last_good_age_secs: Some(s),
                ..
            } => fmt_secs(*s) + " ago",
            _ => "—".to_string(),
        };
        let signal = verdict.signal(thresholds.empty_completion_threshold);
        println!(
            "{:<15} {} {:<8} {:<13} {}",
            name,
            verdict.glyph(),
            verdict.label(),
            last_good,
            signal
        );
        if let Some(action) = verdict.recommended_action(name) {
            problems.push((name.clone(), action));
        }
    }
    if !problems.is_empty() {
        println!();
        println!("Recommended actions:");
        for (_n, action) in problems {
            println!("  {}", action);
        }
    }
    Ok(())
}

fn print_detailed(name: &str, last: usize, thresholds: &DoctorThresholds) -> Result<()> {
    let state = match agent::load_state(name) {
        Ok(s) => s,
        Err(e) => {
            println!("Agent '{}' not found: {}", name, e);
            return Ok(());
        }
    };
    let now = Utc::now();
    let (tasks, latest_inbox_ts) = collect_inputs(name, state.pid, last, now)?;
    let alive = process_alive(state.pid);
    let input = DoctorInputs {
        agent_name: name,
        state_pid: state.pid,
        process_alive: alive,
        recent_tasks: &tasks,
        latest_inbox_ts,
        now,
    };
    let verdict = diagnose(&input, thresholds);

    println!("Agent:       {}", name);
    println!("Verdict:     {} {}", verdict.glyph(), verdict.label());
    println!(
        "Signal:      {}",
        verdict.signal(thresholds.empty_completion_threshold)
    );
    println!(
        "Process:     pid={} alive={}",
        if state.pid == 0 {
            "-".to_string()
        } else {
            state.pid.to_string()
        },
        alive
    );
    println!("State file:  status={}", state.status);
    if let Some(ts) = latest_inbox_ts {
        let age = (now - ts).num_seconds();
        println!("Inbox tail:  {} ({} ago)", ts.to_rfc3339(), fmt_secs(age));
    } else {
        println!("Inbox tail:  (empty)");
    }
    println!();
    println!(
        "Last {} task log entries (oldest first):",
        tasks.len().min(last)
    );
    if tasks.is_empty() {
        println!("  (none — no tasks logged yet)");
    } else {
        println!(
            "  {:<20} {:<10} {:>7} {:>9} {:>6}",
            "TIMESTAMP", "SOURCE", "OUT_TOK", "DUR", "COST"
        );
        for t in &tasks {
            let ts_short = if t.ts.len() >= 19 { &t.ts[..19] } else { &t.ts };
            let ts_short = ts_short.replace('T', " ");
            let out = t.output_tokens.unwrap_or(0);
            let empty_marker = if crate::app::doctor::is_empty_completion(t) {
                " (empty)"
            } else {
                ""
            };
            println!(
                "  {:<20} {:<10} {:>7} {:>8}ms ${:>5.2}{}",
                ts_short, t.source, out, t.duration_ms, t.cost, empty_marker
            );
        }
    }
    if let Some(action) = verdict.recommended_action(name) {
        println!();
        println!("Recommend: {}", action);
    }

    // Tmux session + systemd-user unit + linger summary (#505). Cheap probes
    // so we always print this section; missing systemd is conveyed as
    // `unit missing` / `(unit missing)`.
    print_tmux_unit_section(name);

    // Surface threshold transparency for the operator.
    println!();
    println!(
        "(thresholds: empty>={} idle>={}m stuck>={}m; default empty={})",
        thresholds.empty_completion_threshold,
        thresholds.idle_minutes_threshold,
        thresholds.stuck_queue_minutes,
        DEFAULT_EMPTY_COMPLETION_THRESHOLD
    );
    Ok(())
}

/// Print the tmux/unit/linger status table for `agent` (#505).
///
/// Probes are best-effort and never error: if `tmux`, `systemctl --user`, or
/// `loginctl` is missing or fails, the relevant row falls back to a benign
/// placeholder rather than aborting the doctor command.
fn print_tmux_unit_section(agent: &str) {
    use crate::app::tmux_launcher::{
        current_user_linger, session_name_for, systemd_unit_install_path, systemd_unit_is_enabled,
        tmux_session_exists,
    };

    let session = session_name_for(agent);
    let session_alive = tmux_session_exists(&session).unwrap_or(false);
    let unit_path = systemd_unit_install_path(agent).ok();
    let unit_exists = unit_path.as_ref().is_some_and(|p| p.exists());

    println!();
    println!("agent: {}", agent);
    println!(
        "tmux session     : {}",
        if session_alive {
            "alive"
        } else {
            "not running"
        }
    );
    let unit_path_display = unit_path
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| format!("~/.config/systemd/user/deskd-{}.service", agent));
    println!(
        "unit file        : {} [{}]",
        unit_path_display,
        if unit_exists { "exists" } else { "missing" }
    );
    let enabled_str = if !unit_exists {
        "(unit missing)".to_string()
    } else if systemd_unit_is_enabled(agent) {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    println!("unit enabled     : {}", enabled_str);

    let linger = current_user_linger();
    let linger_str = match linger {
        Some(true) => "yes",
        Some(false) => "no",
        None => "unknown",
    };
    println!("linger           : {}", linger_str);

    if linger == Some(false) && unit_exists {
        println!(
            "Note: linger is disabled. Run `sudo loginctl enable-linger $(whoami)` to enable persistent user services."
        );
    }
}
