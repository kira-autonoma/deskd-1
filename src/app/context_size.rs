//! Live context-size aggregation across all active agents.
//!
//! Implements issue #393: a snapshot of how much of each session's context
//! window is currently consumed, derived from the latest task log entry for
//! every live agent.
//!
//! "Current live context size" is approximated by the latest task's
//! `input_tokens + cache_creation_input_tokens + cache_read_input_tokens`
//! recorded in `~/.deskd/logs/<agent>/tasks.jsonl`. Each turn's input total
//! reflects everything Claude pinned in the window for that turn — the most
//! recent entry is therefore a good proxy for the live size.

use chrono::DateTime;

use crate::app::agent_registry::AgentState;
use crate::app::tasklog::{self, TaskLog};

/// Default context window for unrecognised / Claude 3.x models (200k tokens).
pub const DEFAULT_CONTEXT_WINDOW: u64 = 200_000;

/// Context window for the Claude 4.x family (Opus / Sonnet / Haiku 4.x all
/// ship with a 1M token window).
pub const CONTEXT_WINDOW_4X: u64 = 1_000_000;

/// Built-in fallback for the auto-compact threshold when no per-agent or
/// global override is configured. Konstantin's chosen default per #402.
pub const DEFAULT_AUTO_COMPACT_THRESHOLD: u64 = 300_000;

/// Fraction of the auto-compact threshold at which we surface a warning
/// indicator. Matches the conventional 80% compaction trigger but is now
/// expressed relative to the configured threshold, not the model window.
pub const WARN_THRESHOLD: f64 = 0.80;

/// One row in the `/context` snapshot — a single agent's current session.
#[derive(Debug, Clone)]
pub struct SessionContext {
    pub agent: String,
    pub model: String,
    pub session_id: String,
    /// Tokens currently pinned in the session window (input + cache).
    /// `None` when no task log data is available for the current session
    /// (e.g. ACP runtimes that don't emit per-turn token usage yet).
    pub context_tokens: Option<u64>,
    /// Context window for the model in use.
    pub context_limit: u64,
    /// Resolved auto-compact threshold for this agent (per-agent override,
    /// global default, or built-in fallback).
    pub auto_compact_threshold: u64,
    /// True when `context_tokens` came from a task log entry recorded
    /// before the current session began (e.g. surviving a daemon reload).
    /// Display layers should mark these visually so the user knows the
    /// number is last-known rather than fresh.
    pub stale: bool,
}

impl SessionContext {
    /// First 8 chars of the session UUID — enough for at-a-glance distinction
    /// without overwhelming the Telegram line.
    pub fn session_short(&self) -> String {
        let trimmed = self.session_id.trim();
        let take = trimmed.chars().take(8).collect::<String>();
        if take.is_empty() {
            "(no session)".to_string()
        } else {
            take
        }
    }

    /// Fraction of the configured auto-compact threshold consumed.
    /// Returns 0.0 when token data is unavailable so callers can branch
    /// cleanly without unwrapping.
    pub fn utilization(&self) -> f64 {
        match self.context_tokens {
            Some(t) if self.auto_compact_threshold > 0 => {
                t as f64 / self.auto_compact_threshold as f64
            }
            _ => 0.0,
        }
    }

    pub fn is_warning(&self) -> bool {
        self.utilization() >= WARN_THRESHOLD
    }
}

/// Pick the context window size for a given Claude model name.
///
/// Claude 4.x family (Opus, Sonnet, Haiku) ships with a 1M window. The
/// 3.x family and any unrecognised id fall back to the 200k default.
///
/// Match is done on a normalised lowercase version of the model id and
/// looks for the family prefix `claude-(opus|sonnet|haiku)-4-` so it
/// covers `claude-opus-4-7`, `claude-sonnet-4-6`, `claude-haiku-4-5`,
/// etc. Future families should be added explicitly.
pub fn context_window_for_model(model: &str) -> u64 {
    let m = model.to_ascii_lowercase();
    if m.starts_with("claude-opus-4-")
        || m.starts_with("claude-sonnet-4-")
        || m.starts_with("claude-haiku-4-")
    {
        CONTEXT_WINDOW_4X
    } else {
        DEFAULT_CONTEXT_WINDOW
    }
}

/// Resolve the auto-compact threshold for an agent, falling back to the
/// built-in default when the config does not specify one.
pub fn resolve_auto_compact_threshold(configured: Option<u64>) -> u64 {
    configured
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_AUTO_COMPACT_THRESHOLD)
}

/// Upper bound for `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE`. Claude Code silently
/// clamps anything above ~83% to its default (per anthropics/claude-code#31806),
/// so we cap there ourselves and log a warning when the configured threshold
/// would have wanted more.
pub const AUTO_COMPACT_PCT_CEILING: u8 = 83;

/// Compute the `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` percentage to inject into
/// the agent process. Claude Code expects an integer 1–100; values >83 are
/// dropped. Returns `None` when we cannot meaningfully derive a percentage
/// (e.g. zero window).
pub fn auto_compact_override_pct(threshold_tokens: u64, window_tokens: u64) -> Option<u8> {
    if window_tokens == 0 {
        return None;
    }
    let raw = (threshold_tokens as f64 / window_tokens as f64 * 100.0).round() as i64;
    Some(raw.clamp(1, AUTO_COMPACT_PCT_CEILING as i64) as u8)
}

/// True when the configured threshold lies on or above the ceiling and would
/// therefore be silently clamped by Claude Code. Caller can log a warning so
/// the user knows their override is ineffective.
pub fn threshold_would_clamp(threshold_tokens: u64, window_tokens: u64) -> bool {
    if window_tokens == 0 {
        return false;
    }
    let pct = threshold_tokens as f64 / window_tokens as f64 * 100.0;
    pct >= AUTO_COMPACT_PCT_CEILING as f64
}

/// Determine whether an agent's process is currently running.
///
/// Mirrors the check used by `deskd agent status` (`/proc/<pid>` existence).
/// We deliberately avoid talking to per-agent buses here — gathering context
/// sizes must work even when the caller doesn't have access to every socket.
fn is_pid_alive(pid: u32) -> bool {
    pid > 0 && std::path::Path::new(&format!("/proc/{}", pid)).exists()
}

/// Whether a given agent state should be considered "live" for the purposes
/// of `/context`. We require a non-empty `session_id` (otherwise there is no
/// session to report on). Process liveness is opportunistic:
///   * `state.pid == 0` — top-level agents started by `deskd serve` don't
///     record a PID (only sub-agents spawned via MCP do). Treat as live.
///   * `state.pid > 0` — sub-agent path; verify the worker process exists.
///
/// This trades a small risk of reporting stale data for a top-level agent
/// that crashed mid-session against the much worse failure mode of showing
/// "No active sessions" when sessions clearly exist.
fn is_live(state: &AgentState) -> bool {
    if state.session_id.is_empty() {
        return false;
    }
    state.pid == 0 || is_pid_alive(state.pid)
}

/// Pure-logic helper for the stale-fallback decision: given the entries
/// matching the current-session window and the entries from the full log,
/// return `(entry, is_stale)` per the rules described on
/// [`latest_session_entry`]. Extracted so the policy can be unit-tested
/// without touching the filesystem.
fn pick_entry_with_staleness(
    session_window: Vec<TaskLog>,
    full_log: impl FnOnce() -> Vec<TaskLog>,
    had_session_cutoff: bool,
) -> Option<(TaskLog, bool)> {
    if let Some(entry) = session_window.into_iter().last() {
        return Some((entry, false));
    }
    if !had_session_cutoff {
        // Without a cutoff, the session-window read already saw everything;
        // empty here means the log itself is empty.
        return None;
    }
    full_log().into_iter().last().map(|e| (e, true))
}

/// Find the most recent task log entry that belongs to the agent's current
/// session, falling back to the latest entry overall when nothing matches
/// the session window. Returns `(entry, is_stale)`:
///   * `is_stale = false` — entry was recorded after `session_start` (or
///     `session_start` is missing, in which case we can't tell and assume
///     fresh).
///   * `is_stale = true` — current-session lookup found nothing, but the
///     log file has older entries; we surface the most recent one so the
///     user sees a last-known value instead of `n/a`. This matters after
///     a daemon reload, when `session_start` resets but the underlying
///     Claude session is still pinned to its previous context.
///   * `None` — `tasks.jsonl` doesn't exist or is empty, so the size is
///     genuinely unknown.
fn latest_session_entry(state: &AgentState) -> Option<(TaskLog, bool)> {
    let since = state
        .session_start
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    // Read with a generous limit so the latest entry is included even when
    // the file is large.
    let session_window = tasklog::read_logs(&state.config.name, usize::MAX, None, since).ok()?;
    let agent_name = state.config.name.clone();
    pick_entry_with_staleness(
        session_window,
        || tasklog::read_logs(&agent_name, usize::MAX, None, None).unwrap_or_default(),
        since.is_some(),
    )
}

/// Compute live context tokens from a task log entry: the input total plus
/// cache reads/creations represents what's currently pinned in the window.
fn entry_context_tokens(entry: &TaskLog) -> Option<u64> {
    let input = entry.input_tokens?;
    let cache_creation = entry.cache_creation_input_tokens.unwrap_or(0);
    let cache_read = entry.cache_read_input_tokens.unwrap_or(0);
    Some(input + cache_creation + cache_read)
}

/// Build a `SessionContext` for a live agent. Returns `None` when the agent
/// is not live (no session / dead PID).
fn snapshot_for(state: &AgentState) -> Option<SessionContext> {
    if !is_live(state) {
        return None;
    }

    let (context_tokens, stale) = match latest_session_entry(state) {
        Some((entry, is_stale)) => (entry_context_tokens(&entry), is_stale),
        None => (None, false),
    };

    Some(SessionContext {
        agent: state.config.name.clone(),
        model: state.config.model.clone(),
        session_id: state.session_id.clone(),
        context_tokens,
        context_limit: context_window_for_model(&state.config.model),
        auto_compact_threshold: resolve_auto_compact_threshold(
            state.config.auto_compact_threshold_tokens,
        ),
        stale,
    })
}

/// Gather a context-size snapshot across every agent registered on the host.
///
/// Only live agents (running PID with an active session) are included.
pub async fn gather() -> anyhow::Result<Vec<SessionContext>> {
    let agents = crate::app::agent::list().await?;
    let mut out: Vec<SessionContext> = agents.iter().filter_map(snapshot_for).collect();
    out.sort_by(|a, b| a.agent.cmp(&b.agent).then(a.session_id.cmp(&b.session_id)));
    Ok(out)
}

/// Format a token count compactly: 45_000 → "45k", 1_500 → "1.5k", small
/// values stay as-is. Designed to fit in narrow Telegram lines.
fn format_tokens_compact(n: u64) -> String {
    if n < 1_000 {
        return n.to_string();
    }
    let k = n as f64 / 1_000.0;
    if k >= 10.0 {
        format!("{}k", k.round() as u64)
    } else {
        // Small enough to want one decimal — e.g. "1.5k".
        let rounded = (k * 10.0).round() / 10.0;
        if (rounded - rounded.round()).abs() < f64::EPSILON {
            format!("{}k", rounded as u64)
        } else {
            format!("{:.1}k", rounded)
        }
    }
}

/// Render a snapshot as a Telegram-friendly plain-text reply.
pub fn format_reply(snapshot: &[SessionContext]) -> String {
    if snapshot.is_empty() {
        return "No active sessions.".to_string();
    }

    let mut lines = Vec::with_capacity(snapshot.len() + 1);
    lines.push("/context".to_string());
    lines.push(String::new()); // blank line under the header

    for s in snapshot {
        let limit = format_tokens_compact(s.context_limit);
        // Show the percentage Claude Code will actually see (matches the
        // CLAUDE_AUTOCOMPACT_PCT_OVERRIDE we inject) so the displayed
        // threshold and the runtime behaviour stay in sync.
        let pct_str = match auto_compact_override_pct(s.auto_compact_threshold, s.context_limit) {
            Some(p) => format!("  {}%", p),
            None => String::new(),
        };
        let line = match s.context_tokens {
            Some(tokens) => {
                let used = format_tokens_compact(tokens);
                let stale = if s.stale { " (stale)" } else { "" };
                let warn = if s.is_warning() { " ⚠️" } else { "" };
                format!(
                    "{}  ~{}{} / {}{}{}",
                    s.agent, used, stale, limit, pct_str, warn
                )
            }
            None => format!("{}  n/a / {}{}", s.agent, limit, pct_str),
        };
        lines.push(line);
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_entry(input: u64, cache_creation: u64, cache_read: u64) -> TaskLog {
        TaskLog {
            ts: chrono::Utc::now().to_rfc3339(),
            source: "test".into(),
            turns: 1,
            cost: 0.0,
            duration_ms: 0,
            status: "ok".into(),
            task: "t".into(),
            error: None,
            msg_id: "m".into(),
            github_repo: None,
            github_pr: None,
            input_tokens: Some(input),
            output_tokens: None,
            cache_creation_input_tokens: Some(cache_creation),
            cache_read_input_tokens: Some(cache_read),
            session_count: None,
            tool_use_count: None,
            parent_agent: None,
        }
    }

    #[test]
    fn entry_context_tokens_sums_input_and_cache() {
        let e = mk_entry(1_000, 200, 50_000);
        assert_eq!(entry_context_tokens(&e), Some(51_200));
    }

    #[test]
    fn entry_context_tokens_returns_none_without_input() {
        let mut e = mk_entry(0, 0, 0);
        e.input_tokens = None;
        assert_eq!(entry_context_tokens(&e), None);
    }

    #[test]
    fn session_short_takes_first_eight_chars() {
        let s = SessionContext {
            agent: "kira".into(),
            model: "claude-opus-4".into(),
            session_id: "abcdef0123456789".into(),
            context_tokens: Some(100),
            context_limit: 200_000,
            auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
            stale: false,
        };
        assert_eq!(s.session_short(), "abcdef01");
    }

    #[test]
    fn session_short_handles_short_or_blank_ids() {
        let mut s = SessionContext {
            agent: "kira".into(),
            model: "claude-opus-4".into(),
            session_id: "abc".into(),
            context_tokens: None,
            context_limit: 200_000,
            auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
            stale: false,
        };
        assert_eq!(s.session_short(), "abc");
        s.session_id = "   ".into();
        assert_eq!(s.session_short(), "(no session)");
    }

    #[test]
    fn warning_triggers_above_eighty_percent_of_threshold() {
        // Warning is now relative to auto_compact_threshold, not context_limit.
        // 80% of 300k = 240k.
        let mut s = SessionContext {
            agent: "a".into(),
            model: "claude-opus-4-7".into(),
            session_id: "xxxxxxxx".into(),
            context_tokens: Some(240_000),
            context_limit: CONTEXT_WINDOW_4X,
            auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
            stale: false,
        };
        assert!(s.is_warning());
        s.context_tokens = Some(239_999);
        assert!(!s.is_warning());
    }

    #[test]
    fn warning_uses_per_agent_threshold_not_model_window() {
        // 600k tokens with a 1M model window would be 60% of the window
        // (no warning under the old logic). With auto_compact at 500k,
        // it's 120% of the threshold → warning.
        let s = SessionContext {
            agent: "dev".into(),
            model: "claude-opus-4-7".into(),
            session_id: "yyyy".into(),
            context_tokens: Some(600_000),
            context_limit: CONTEXT_WINDOW_4X,
            auto_compact_threshold: 500_000,
            stale: false,
        };
        assert!(s.is_warning());
    }

    #[test]
    fn format_tokens_compact_buckets() {
        assert_eq!(format_tokens_compact(0), "0");
        assert_eq!(format_tokens_compact(999), "999");
        assert_eq!(format_tokens_compact(1_000), "1k");
        assert_eq!(format_tokens_compact(1_500), "1.5k");
        assert_eq!(format_tokens_compact(45_000), "45k");
        assert_eq!(format_tokens_compact(180_000), "180k");
    }

    #[test]
    fn format_reply_empty_snapshot_says_no_sessions() {
        assert_eq!(format_reply(&[]), "No active sessions.");
    }

    #[test]
    fn format_reply_renders_lines_with_warning() {
        // Mix of 4.x (1M window) and unknown (200k window) agents, with the
        // default 300k auto-compact threshold used throughout. The middle
        // entry crosses 80% of 300k = 240k, so it should show ⚠️. The last
        // entry has stale data and should be marked accordingly.
        let snap = vec![
            SessionContext {
                agent: "agent-a".into(),
                model: "claude-opus-4-7".into(),
                session_id: "abc12345xx".into(),
                context_tokens: Some(45_000),
                context_limit: CONTEXT_WINDOW_4X,
                auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
                stale: false,
            },
            SessionContext {
                agent: "agent-b".into(),
                model: "claude-opus-4-7".into(),
                session_id: "def45678yy".into(),
                context_tokens: Some(260_000),
                context_limit: CONTEXT_WINDOW_4X,
                auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
                stale: false,
            },
            SessionContext {
                agent: "agent-c".into(),
                model: "claude-sonnet".into(),
                session_id: "ghi78901zz".into(),
                context_tokens: None,
                context_limit: 200_000,
                auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
                stale: false,
            },
            SessionContext {
                agent: "agent-d".into(),
                model: "claude-opus-4-7".into(),
                session_id: "jkl01234aa".into(),
                context_tokens: Some(120_000),
                context_limit: CONTEXT_WINDOW_4X,
                auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
                stale: true,
            },
        ];
        let reply = format_reply(&snap);
        assert!(reply.starts_with("/context\n"));
        // No session IDs anywhere in the output.
        assert!(!reply.contains("session "));
        assert!(!reply.contains("abc12345"));
        // 300k of 1M = 30%; healthy line, no warning.
        assert!(reply.contains("agent-a  ~45k / 1000k  30%"));
        assert!(!reply.contains("agent-a  ~45k / 1000k  30% ⚠️"));
        // Warning line: 260k > 80% of 300k threshold.
        assert!(reply.contains("agent-b  ~260k / 1000k  30% ⚠️"));
        // 300k of 200k clamps to 83%.
        assert!(reply.contains("agent-c  n/a / 200k  83%"));
        // Stale marker shows after the token count, before the limit.
        assert!(reply.contains("agent-d  ~120k (stale) / 1000k  30%"));
    }

    #[test]
    fn format_reply_drops_session_id() {
        // Regression for #406: session IDs should not appear in /context.
        let snap = vec![SessionContext {
            agent: "kira".into(),
            model: "claude-opus-4-7".into(),
            session_id: "deadbeefcafebabe".into(),
            context_tokens: Some(100_000),
            context_limit: CONTEXT_WINDOW_4X,
            auto_compact_threshold: DEFAULT_AUTO_COMPACT_THRESHOLD,
            stale: false,
        }];
        let reply = format_reply(&snap);
        assert!(!reply.contains("deadbeef"));
        assert!(!reply.contains("session"));
    }

    #[test]
    fn context_window_for_4x_family_is_one_million() {
        assert_eq!(context_window_for_model("claude-opus-4-7"), 1_000_000);
        assert_eq!(context_window_for_model("claude-opus-4-6"), 1_000_000);
        assert_eq!(context_window_for_model("claude-sonnet-4-6"), 1_000_000);
        assert_eq!(context_window_for_model("claude-haiku-4-5"), 1_000_000);
        // Case-insensitive matching.
        assert_eq!(context_window_for_model("Claude-Opus-4-7"), 1_000_000);
    }

    #[test]
    fn context_window_for_3x_and_unknown_is_two_hundred_k() {
        assert_eq!(context_window_for_model("claude-3-5-sonnet"), 200_000);
        assert_eq!(context_window_for_model("claude-3-opus"), 200_000);
        assert_eq!(context_window_for_model("claude-haiku-3"), 200_000);
        assert_eq!(context_window_for_model("anything-else"), 200_000);
        // Bare "claude-opus-4" without the family digit suffix is unknown.
        assert_eq!(context_window_for_model("claude-opus-4"), 200_000);
    }

    #[test]
    fn auto_compact_override_pct_rounds_to_integer() {
        // 300k / 1M = 30%.
        assert_eq!(auto_compact_override_pct(300_000, 1_000_000), Some(30));
        // 500k / 1M = 50%.
        assert_eq!(auto_compact_override_pct(500_000, 1_000_000), Some(50));
        // Zero window → None.
        assert_eq!(auto_compact_override_pct(300_000, 0), None);
    }

    #[test]
    fn auto_compact_override_pct_clamps_to_ceiling() {
        // 900k / 1M = 90% → clamped to 83.
        assert_eq!(auto_compact_override_pct(900_000, 1_000_000), Some(83));
        // 300k / 200k = 150% → clamped to 83.
        assert_eq!(auto_compact_override_pct(300_000, 200_000), Some(83));
    }

    #[test]
    fn auto_compact_override_pct_floor_is_one() {
        // Tiny threshold rounds to <1 → clamped to 1, not 0.
        assert_eq!(auto_compact_override_pct(1, 1_000_000), Some(1));
    }

    #[test]
    fn threshold_would_clamp_at_or_above_ceiling() {
        // 83% exactly clamps.
        assert!(threshold_would_clamp(830_000, 1_000_000));
        // Above ceiling clamps.
        assert!(threshold_would_clamp(900_000, 1_000_000));
        // Below ceiling does not.
        assert!(!threshold_would_clamp(820_000, 1_000_000));
    }

    #[test]
    fn resolve_auto_compact_threshold_falls_back_to_default() {
        assert_eq!(
            resolve_auto_compact_threshold(None),
            DEFAULT_AUTO_COMPACT_THRESHOLD
        );
        // Zero is treated as "unset" — fall back to default rather than
        // emit a never-triggering 0 threshold.
        assert_eq!(
            resolve_auto_compact_threshold(Some(0)),
            DEFAULT_AUTO_COMPACT_THRESHOLD
        );
        assert_eq!(resolve_auto_compact_threshold(Some(500_000)), 500_000);
    }

    fn mk_state(pid: u32, session_id: &str) -> AgentState {
        use crate::app::agent_registry::AgentConfig;
        AgentState {
            config: AgentConfig {
                name: "kira".into(),
                model: "claude-opus-4".into(),
                system_prompt: String::new(),
                work_dir: "/tmp".into(),
                max_turns: 100,
                unix_user: None,
                command: vec!["claude".into()],
                config_path: None,
                container: None,
                session: Default::default(),
                runtime: Default::default(),
                launch_mode: Default::default(),
                kind: Default::default(),
                context: None,
                compact_threshold: None,
                auto_compact_threshold_tokens: None,
                empty_completion_threshold: None,
                empty_completion_restart_min_secs: None,
            },
            pid,
            session_id: session_id.into(),
            total_turns: 0,
            total_cost: 0.0,
            created_at: String::new(),
            status: "idle".into(),
            current_task: String::new(),
            parent: None,
            scope: None,
            can_message: None,
            env_keys: None,
            session_start: None,
            session_cost: 0.0,
            session_turns: 0,
            consecutive_empty_completions: 0,
            last_empty_restart_at: None,
            total_empty_restarts: 0,
            tmux_session: None,
            tmux_log_path: None,
        }
    }

    #[test]
    fn is_live_treats_top_level_agent_with_zero_pid_as_live() {
        // Regression: top-level agents started via `deskd serve` never have
        // their state.pid updated from the default 0 (only MCP-spawned
        // sub-agents do). They must still appear in `/context`.
        let state = mk_state(0, "abcdef0123456789");
        assert!(is_live(&state));
    }

    #[test]
    fn is_live_rejects_empty_session_id() {
        // No session means nothing to report on, regardless of pid.
        let state = mk_state(0, "");
        assert!(!is_live(&state));
        let state = mk_state(std::process::id(), "");
        assert!(!is_live(&state));
    }

    #[test]
    fn is_live_accepts_subagent_with_running_pid() {
        // Sub-agent path: pid > 0 and process exists.
        let state = mk_state(std::process::id(), "abcdef0123456789");
        assert!(is_live(&state));
    }

    #[test]
    fn pick_entry_returns_session_entry_when_present() {
        let session_entry = mk_entry(100, 0, 0);
        let full_entry = mk_entry(50, 0, 0);
        let result = pick_entry_with_staleness(
            vec![session_entry.clone()],
            || vec![full_entry, session_entry.clone()],
            true,
        )
        .unwrap();
        assert_eq!(result.0.input_tokens, Some(100));
        assert!(!result.1, "current-session entry should not be stale");
    }

    #[test]
    fn pick_entry_falls_back_to_full_log_when_session_empty() {
        // session_start is set (had_session_cutoff = true), but no entries
        // match — daemon was reloaded and current session has no completed
        // tasks yet.
        let older = mk_entry(42, 0, 0);
        let result = pick_entry_with_staleness(vec![], || vec![older], true).unwrap();
        assert_eq!(result.0.input_tokens, Some(42));
        assert!(result.1, "fallback entry must be marked stale");
    }

    #[test]
    fn pick_entry_returns_none_when_no_log_at_all() {
        // Empty session window AND empty full log → genuinely n/a.
        let result = pick_entry_with_staleness(vec![], Vec::new, true);
        assert!(result.is_none());
    }

    #[test]
    fn pick_entry_returns_none_when_no_cutoff_and_empty() {
        // Without a session cutoff the first read already saw everything;
        // an empty result means the log itself is empty, so no stale
        // fallback is possible.
        let result = pick_entry_with_staleness(vec![], || panic!("must not re-read"), false);
        assert!(result.is_none());
    }

    #[test]
    fn is_live_rejects_subagent_with_dead_pid() {
        // Sub-agent path: pid > 0 but process is gone.
        // PID 1 is the init/systemd process and always alive on Linux, so
        // pick a deliberately implausible pid that won't exist.
        let state = mk_state(u32::MAX - 1, "abcdef0123456789");
        assert!(!is_live(&state));
    }
}
