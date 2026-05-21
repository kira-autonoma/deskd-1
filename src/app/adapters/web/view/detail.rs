//! Per-agent detail-page rendering (#445).
//!
//! Pure functions: take an [`AgentDetail`] and emit HTML fragments. The
//! page-level template stitches these together with the surrounding chrome
//! (topbar, SSE wiring).

use chrono::{DateTime, Utc};

use crate::app::adapters::web::data::{AgentDetail, TaskLogRow};
use crate::app::metrics::AgentBreakdownEntry;

use super::cards::{agent_card_id, format_bytes, format_relative};
use super::html_escape;

/// Status pill class — mirrors the one used on the dashboard so cards on
/// the index and the detail page look identical.
fn status_class(status: &str) -> &'static str {
    match status {
        "working" => "working",
        "offline" => "offline",
        _ => "idle",
    }
}

fn em_dash() -> String {
    "<span class=\"em\">—</span>".to_string()
}

/// Format the status-icon column for the task log.
fn status_icon(status: &str) -> &'static str {
    match status {
        "ok" => "✓",
        "error" => "✗",
        "skip" => "○",
        "empty" => "·",
        _ => "?",
    }
}

/// Render the «back to dashboard» link + heading block.
pub fn detail_header(detail: &AgentDetail) -> String {
    let name = html_escape(&detail.summary.name);
    let class = status_class(&detail.summary.status);
    let status_label = html_escape(&detail.summary.status);
    format!(
        r#"<header class="detail-head">
  <p class="detail-head__back"><a href="/">← back</a></p>
  <h1 class="detail-head__title">
    <span class="detail-head__name">{name}</span>
    <span class="agent-card__status agent-card__status--{class}">{status_label}</span>
  </h1>
</header>"#,
        name = name,
        class = class,
        status_label = status_label,
    )
}

/// Render the metadata grid (session, model, home dir, context, compaction
/// strategy, last activity).
pub fn detail_meta(detail: &AgentDetail) -> String {
    let session = if detail.session_id.is_empty() {
        em_dash()
    } else {
        format!("<code>{}…</code>", html_escape(&detail.session_id))
    };
    let model = html_escape(&detail.summary.model);
    let home_dir = html_escape(&detail.home_dir);
    let home_dir_size = detail
        .summary
        .home_dir_bytes
        .map(format_bytes)
        .unwrap_or_else(em_dash);
    // #483: the denominator is the model's hard context_limit, NOT the
    // auto-compact threshold. Fall back to threshold only when limit is
    // missing (older payloads). The compaction strategy (threshold) is
    // surfaced separately by the `compaction` field a few lines below.
    let context_limit = detail
        .summary
        .context_limit
        .or(detail.summary.context_threshold);
    let context = match (detail.summary.context_tokens, context_limit) {
        (Some(used), Some(limit)) if limit > 0 => format!(
            "{} / {}",
            html_escape(&format!("{}k", used / 1_000)),
            html_escape(&format!("{}k", limit / 1_000)),
        ),
        _ => em_dash(),
    };
    let compaction = format!(
        "<small>compaction: {}</small>",
        html_escape(&detail.compaction_strategy)
    );
    let last_activity = detail
        .summary
        .last_activity
        .map(format_relative)
        .unwrap_or_else(em_dash);

    format!(
        r#"<dl class="detail-meta">
  <dt>session</dt><dd>{session}</dd>
  <dt>model</dt><dd><code>{model}</code></dd>
  <dt>home</dt><dd><code>{home_dir}</code> <small>({home_dir_size})</small></dd>
  <dt>context</dt><dd>{context} {compaction}</dd>
  <dt>last activity</dt><dd>{last_activity}</dd>
</dl>"#,
        session = session,
        model = model,
        home_dir = home_dir,
        home_dir_size = home_dir_size,
        context = context,
        compaction = compaction,
        last_activity = last_activity,
    )
}

/// Render the three action buttons. Each one is a real `<form method="POST">`
/// so the page works without JavaScript per the issue spec.
///
/// When `busy = true` the buttons are still rendered (so the layout is
/// stable) but each form posts to the same destination — the handler
/// reads the agent's status fresh and rejects with a flash message. The
/// disabled-looking visual treatment is purely CSS-driven via the
/// `--busy` modifier class.
pub fn detail_actions(detail: &AgentDetail, csrf: &str) -> String {
    let name = html_escape(&detail.summary.name);
    let csrf_token = html_escape(csrf);
    let modifier = if detail.busy {
        " detail-actions--busy"
    } else {
        ""
    };
    format!(
        r#"<section class="detail-actions{modifier}">
  <form method="post" action="/agent/{name}/restart">
    <input type="hidden" name="_csrf" value="{csrf_token}">
    <button type="submit" class="action action--restart">Restart</button>
  </form>
  <form method="post" action="/agent/{name}/compact">
    <input type="hidden" name="_csrf" value="{csrf_token}">
    <button type="submit" class="action action--compact">Force compact</button>
  </form>
  <form method="post" action="/agent/{name}/stop">
    <input type="hidden" name="_csrf" value="{csrf_token}">
    <button type="submit" class="action action--stop">Stop</button>
  </form>
</section>"#,
        modifier = modifier,
        name = name,
        csrf_token = csrf_token,
    )
}

/// Render the optional flash banner (success or error). HTML-escapes the
/// caller-supplied message text. Returns an empty string when both args
/// are `None`.
pub fn detail_flash(success: Option<&str>, error: Option<&str>) -> String {
    if let Some(msg) = error {
        return format!(
            r#"<div class="flash flash--error" role="alert">{}</div>"#,
            html_escape(msg)
        );
    }
    if let Some(msg) = success {
        return format!(
            r#"<div class="flash flash--ok" role="status">{}</div>"#,
            html_escape(msg)
        );
    }
    String::new()
}

/// Render the last-N tasklog table.
pub fn detail_tasks(detail: &AgentDetail) -> String {
    if detail.recent_tasks.is_empty() {
        return r#"<section class="detail-tasks">
  <h2>Last tasks</h2>
  <p class="detail-tasks__empty">No tasks recorded yet.</p>
</section>"#
            .to_string();
    }

    let mut rows = String::new();
    for row in &detail.recent_tasks {
        rows.push_str(&render_task_row(row));
    }

    format!(
        r#"<section class="detail-tasks">
  <h2>Last tasks ({count})</h2>
  <ul class="detail-tasks__list">
    {rows}
  </ul>
</section>"#,
        count = detail.recent_tasks.len(),
        rows = rows,
    )
}

fn render_task_row(row: &TaskLogRow) -> String {
    let icon = status_icon(&row.status);
    let status = html_escape(&row.status);
    let ts = format_ts_short(&row.ts);
    let summary = html_escape(&row.summary);
    let duration = crate::app::tasklog::format_duration(row.duration_ms);
    format!(
        r#"<li class="detail-tasks__row detail-tasks__row--{status_class}">
  <span class="detail-tasks__icon" title="{status}">{icon}</span>
  <time>{ts}</time>
  <span class="detail-tasks__summary">{summary}</span>
  <span class="detail-tasks__duration">{duration}</span>
</li>"#,
        status_class = status,
        status = status,
        icon = icon,
        ts = ts,
        summary = summary,
        duration = duration,
    )
}

/// Trim a tasklog RFC 3339 timestamp to a short HH:MM:SS view for the
/// last-N column. Falls back to the original string when parsing fails.
fn format_ts_short(ts: &str) -> String {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
        return dt.format("%H:%M:%S").to_string();
    }
    html_escape(ts)
}

/// Render the SSE-driven bus tail card. The placeholder list element is
/// replaced by htmx as new events arrive on `/agent/<name>/events`.
pub fn detail_bus_tail(detail: &AgentDetail) -> String {
    let name = html_escape(&detail.summary.name);
    let card_id = agent_card_id(&detail.summary.name);
    let events_url = format!("/agent/{}/events", urlsafe(&detail.summary.name));
    let events_url = html_escape(&events_url);
    format!(
        r#"<section class="detail-bus" hx-ext="sse" sse-connect="{events_url}">
  <h2>Recent bus traffic (live)</h2>
  <ul class="detail-bus__list" id="{card_id}-bus" sse-swap="{card_id}">
    <li class="detail-bus__empty">Waiting for events for <code>{name}</code>…</li>
  </ul>
</section>"#,
        events_url = events_url,
        card_id = html_escape(&card_id),
        name = name,
    )
}

/// Replace anything that's not `[a-zA-Z0-9_-]` with `-` so agent names can
/// safely appear in URLs without further encoding. Mirrors the
/// `cards::sanitise_id` helper, kept private to avoid leaking a tiny
/// helper through the cards module.
fn urlsafe(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Render the confirm-page body (used for `restart` / `stop` second-step).
pub fn confirm_page_body(agent_name: &str, action: &str, action_label: &str, csrf: &str) -> String {
    let agent = html_escape(agent_name);
    let action_e = html_escape(action);
    let label = html_escape(action_label);
    let csrf_token = html_escape(csrf);
    format!(
        r#"<main class="confirm">
  <h1>Confirm {label}</h1>
  <p>You are about to <strong>{label}</strong> agent <code>{agent}</code>.</p>
  <p>This is a destructive action; click confirm to proceed.</p>
  <form method="post" action="/agent/{agent}/{action_e}/confirm">
    <input type="hidden" name="_csrf" value="{csrf_token}">
    <button type="submit" class="action action--confirm">Yes, {label}</button>
  </form>
  <p><a href="/agent/{agent}">Cancel</a></p>
</main>"#,
        agent = agent,
        action_e = action_e,
        label = label,
        csrf_token = csrf_token,
    )
}

/// Render the disk-detail block for one agent (#446). Surface the home-dir
/// size, the last-sample timestamp, and the top-N subdirectory breakdown.
/// `format_bytes_fn` is taken as a function pointer so this module doesn't
/// depend on the cards layout.
pub fn agent_disk_detail_html(
    home_dir: Option<&str>,
    total_bytes: Option<u64>,
    updated_at: Option<DateTime<Utc>>,
    breakdown: &[AgentBreakdownEntry],
    format_bytes_fn: fn(u64) -> String,
) -> String {
    let home_label = home_dir.map(html_escape).unwrap_or_else(|| "—".to_string());
    let total_label = total_bytes
        .map(format_bytes_fn)
        .unwrap_or_else(|| "—".to_string());
    let updated_label = updated_at
        .map(format_relative)
        .unwrap_or_else(|| "—".to_string());

    let breakdown_html = if breakdown.is_empty() {
        r#"<p class="agent-disk__empty">No subdirectory data available.</p>"#.to_string()
    } else {
        let mut items = String::new();
        for entry in breakdown {
            items.push_str(&format!(
                r#"    <li class="agent-disk__row"><span class="agent-disk__name">{name}</span> <span class="agent-disk__bytes">{bytes}</span></li>
"#,
                name = html_escape(&entry.name),
                bytes = format_bytes_fn(entry.bytes),
            ));
        }
        format!(
            r#"<ol class="agent-disk__breakdown">
{items}</ol>"#,
        )
    };

    format!(
        r#"<section class="agent-disk">
  <dl class="agent-disk__summary">
    <dt>home dir</dt><dd><code>{home}</code></dd>
    <dt>total</dt><dd>{total}</dd>
    <dt>updated</dt><dd title="{updated_full}">{updated_short}</dd>
  </dl>
  <h3>top {n} subdirectories</h3>
  {breakdown_html}
</section>"#,
        home = home_label,
        total = total_label,
        updated_full = updated_at
            .map(|t| html_escape(&t.to_rfc3339()))
            .unwrap_or_default(),
        updated_short = html_escape(&updated_label),
        n = breakdown.len().max(1),
        breakdown_html = breakdown_html,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::adapters::web::data::{AgentDetail, AgentSummary};

    fn fixture(name: &str) -> AgentDetail {
        AgentDetail {
            summary: AgentSummary {
                name: name.into(),
                status: "idle".into(),
                model: "claude-opus-4-7".into(),
                last_activity: None,
                context_tokens: Some(120_000),
                context_threshold: Some(300_000),
                context_limit: Some(1_000_000),
                home_dir_bytes: Some(412 * 1024 * 1024),
                current_task: None,
                task_running_for: None,
            },
            session_id: "abcdef01".into(),
            home_dir: "/home/dev".into(),
            compaction_strategy: "auto @ 80%".into(),
            recent_tasks: vec![TaskLogRow {
                status: "ok".into(),
                ts: "2026-05-09T14:32:00Z".into(),
                summary: "fix archmotif PR".into(),
                duration_ms: 64_000,
            }],
            busy: false,
        }
    }

    #[test]
    fn detail_header_includes_back_link_and_status_pill() {
        let html = detail_header(&fixture("kira"));
        assert!(html.contains("← back"));
        assert!(html.contains("agent-card__status--idle"));
        assert!(html.contains("kira"));
    }

    #[test]
    fn detail_meta_renders_every_documented_field() {
        let html = detail_meta(&fixture("kira"));
        assert!(html.contains("abcdef01"));
        assert!(html.contains("claude-opus-4-7"));
        assert!(html.contains("/home/dev"));
        assert!(html.contains("412 MiB"));
        // #483: denominator is the hard context_limit (1M for Sonnet 4.6),
        // not the soft auto-compact threshold (300k). The threshold is
        // still surfaced by the `compaction` field below.
        assert!(html.contains("120k / 1000k"));
        assert!(html.contains("compaction: auto @ 80%"));
    }

    #[test]
    fn detail_actions_include_three_buttons_each_with_csrf() {
        let html = detail_actions(&fixture("kira"), "tok-1");
        assert!(html.contains(r#"action="/agent/kira/restart""#));
        assert!(html.contains(r#"action="/agent/kira/compact""#));
        assert!(html.contains(r#"action="/agent/kira/stop""#));
        assert!(html.matches(r#"value="tok-1""#).count() == 3);
        // Idle agent → no busy modifier class.
        assert!(!html.contains("detail-actions--busy"));
    }

    #[test]
    fn detail_actions_carry_busy_modifier_when_agent_working() {
        let mut d = fixture("kira");
        d.busy = true;
        d.summary.status = "working".into();
        let html = detail_actions(&d, "tok");
        assert!(html.contains("detail-actions--busy"));
    }

    #[test]
    fn detail_tasks_renders_one_row_per_entry() {
        let mut d = fixture("kira");
        d.recent_tasks = vec![
            TaskLogRow {
                status: "ok".into(),
                ts: "2026-05-09T14:32:00Z".into(),
                summary: "task one".into(),
                duration_ms: 1_500,
            },
            TaskLogRow {
                status: "error".into(),
                ts: "2026-05-09T14:18:00Z".into(),
                summary: "task two".into(),
                duration_ms: 3_000,
            },
        ];
        let html = detail_tasks(&d);
        assert!(html.contains("Last tasks (2)"));
        assert!(html.contains("task one"));
        assert!(html.contains("task two"));
        assert!(html.contains("14:32:00"));
        assert!(html.contains("14:18:00"));
        // status icons.
        assert!(html.contains("✓"));
        assert!(html.contains("✗"));
    }

    #[test]
    fn detail_tasks_handles_empty_tasklog() {
        let mut d = fixture("kira");
        d.recent_tasks.clear();
        let html = detail_tasks(&d);
        assert!(html.contains("No tasks recorded yet."));
    }

    #[test]
    fn detail_bus_tail_wires_sse_endpoint_per_agent() {
        let html = detail_bus_tail(&fixture("kira"));
        assert!(html.contains(r#"sse-connect="/agent/kira/events""#));
    }

    #[test]
    fn detail_flash_renders_success_when_only_ok_passed() {
        let html = detail_flash(Some("Compaction queued"), None);
        assert!(html.contains("flash--ok"));
        assert!(html.contains("Compaction queued"));
    }

    #[test]
    fn detail_flash_renders_error_when_provided() {
        let html = detail_flash(None, Some("Agent busy, retry when idle"));
        assert!(html.contains("flash--error"));
        assert!(html.contains("Agent busy, retry when idle"));
    }

    #[test]
    fn detail_flash_escapes_xss() {
        let html = detail_flash(None, Some("<script>alert(1)</script>"));
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn confirm_page_body_targets_action_confirm_endpoint() {
        let html = confirm_page_body("kira", "restart", "Restart", "tok-99");
        assert!(html.contains(r#"action="/agent/kira/restart/confirm""#));
        assert!(html.contains(r#"value="tok-99""#));
        assert!(html.contains("Restart"));
        assert!(html.contains("Cancel"));
    }

    #[test]
    fn confirm_page_body_escapes_agent_name() {
        let html = confirm_page_body("<x>", "stop", "Stop", "tok");
        assert!(!html.contains("<x>"));
        assert!(html.contains("&lt;x&gt;"));
    }

    // ─── #446 agent_disk_detail_html ─────────────────────────────────────

    #[test]
    fn agent_disk_detail_renders_em_dash_when_no_data() {
        let html = agent_disk_detail_html(None, None, None, &[], format_bytes);
        assert!(html.contains("—"));
        assert!(html.contains("No subdirectory data available"));
    }

    #[test]
    fn agent_disk_detail_lists_breakdown_largest_first() {
        let breakdown = vec![
            AgentBreakdownEntry {
                name: ".cargo".into(),
                bytes: 800 * 1024 * 1024,
            },
            AgentBreakdownEntry {
                name: "projects".into(),
                bytes: 400 * 1024 * 1024,
            },
        ];
        let html = agent_disk_detail_html(
            Some("/home/kira"),
            Some(1_200 * 1024 * 1024),
            None,
            &breakdown,
            format_bytes,
        );
        assert!(html.contains("<code>/home/kira</code>"));
        assert!(html.contains(".cargo"));
        assert!(html.contains("projects"));
        // .cargo is listed before projects.
        let i_cargo = html.find(".cargo").unwrap();
        let i_proj = html.find("projects").unwrap();
        assert!(i_cargo < i_proj);
    }

    #[test]
    fn agent_disk_detail_updated_tooltip_carries_rfc3339() {
        let when: chrono::DateTime<chrono::Utc> =
            chrono::DateTime::parse_from_rfc3339("2026-01-01T12:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc);
        let html =
            agent_disk_detail_html(Some("/home/kira"), Some(0), Some(when), &[], format_bytes);
        // tooltip uses title="..." with the full RFC 3339 timestamp.
        assert!(html.contains(r#"title="2026-01-01T12:00:00+00:00""#));
    }

    #[test]
    fn agent_disk_detail_escapes_subdir_names_for_xss_safety() {
        let breakdown = vec![AgentBreakdownEntry {
            name: "<x>".into(),
            bytes: 1,
        }];
        let html = agent_disk_detail_html(None, None, None, &breakdown, format_bytes);
        assert!(html.contains("&lt;x&gt;"));
        assert!(!html.contains("<x>"));
    }
}
