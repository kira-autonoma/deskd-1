//! Task/session view rendering (#485). Shows session metadata for one
//! tasklog entry plus a «view logs» action.

use super::html_escape;
use crate::app::adapters::web::data::TaskLogRow;
use crate::app::tasklog::format_duration;

/// Render the task/session view body. `task_id` is the index from the
/// route (`/agent/<name>/task/<task_id>`) and is surfaced in the header
/// purely so deep-links are intelligible.
pub fn task_view(agent: &str, task_id: &str, session_id: &str, task: &TaskLogRow) -> String {
    let agent_esc = html_escape(agent);
    let task_id_esc = html_escape(task_id);
    let session_short = if session_id.is_empty() {
        em_dash()
    } else {
        format!("<code>{}</code>", html_escape(session_id))
    };
    let status = html_escape(&task.status);
    let status_class = status_css_class(&task.status);
    let ts = html_escape(&task.ts);
    let summary = html_escape(&task.summary);
    let duration = format_duration(task.duration_ms);
    let log_href = if session_id.is_empty() {
        format!("/agent/{}/log", html_escape(agent))
    } else {
        format!(
            "/agent/{}/log?session={}",
            html_escape(agent),
            html_escape(session_id),
        )
    };

    format!(
        r#"<section class="task-view">
  <header class="task-view__head">
    <h2>Task <code>#{task_id}</code></h2>
    <span class="task-view__status task-view__status--{status_class}">{status}</span>
  </header>
  <dl class="task-view__meta">
    <dt>agent</dt><dd><code>{agent}</code></dd>
    <dt>started</dt><dd><time>{ts}</time></dd>
    <dt>duration</dt><dd>{duration}</dd>
    <dt>session</dt><dd>{session}</dd>
    <dt>summary</dt><dd>{summary}</dd>
  </dl>
  <p class="task-view__actions">
    <a class="task-view__log-link" href="{log_href}">view logs →</a>
  </p>
</section>"#,
        task_id = task_id_esc,
        status = status,
        status_class = status_class,
        agent = agent_esc,
        ts = ts,
        duration = duration,
        session = session_short,
        summary = summary,
        log_href = html_escape(&log_href),
    )
}

fn em_dash() -> String {
    "<span class=\"em\">—</span>".to_string()
}

fn status_css_class(status: &str) -> &'static str {
    match status {
        "ok" => "ok",
        "error" => "error",
        "skip" => "skip",
        "empty" => "empty",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> TaskLogRow {
        TaskLogRow {
            status: "ok".into(),
            ts: "2026-05-09T14:32:00Z".into(),
            summary: "fix archmotif PR".into(),
            duration_ms: 64_000,
        }
    }

    #[test]
    fn task_view_renders_all_metadata_fields() {
        let html = task_view("kira", "3", "abc12345", &row());
        assert!(html.contains("#3"));
        assert!(html.contains("kira"));
        assert!(html.contains("2026-05-09T14:32:00Z"));
        assert!(html.contains("fix archmotif PR"));
        assert!(html.contains("1m4s"));
        assert!(html.contains("abc12345"));
        // Log link includes session filter.
        assert!(html.contains(r#"href="/agent/kira/log?session=abc12345""#));
    }

    #[test]
    fn task_view_log_link_omits_session_when_empty() {
        let html = task_view("kira", "0", "", &row());
        assert!(html.contains(r#"href="/agent/kira/log""#));
        // Session line shows em-dash.
        assert!(html.contains("—"));
    }

    #[test]
    fn task_view_escapes_xss_in_summary_and_session() {
        let mut r = row();
        r.summary = "<script>".into();
        let html = task_view("kira", "0", "<s>", &r);
        assert!(!html.contains("<script>"));
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("&lt;s&gt;"));
    }

    #[test]
    fn task_view_carries_status_class() {
        let mut r = row();
        r.status = "error".into();
        let html = task_view("kira", "1", "abc", &r);
        assert!(html.contains("task-view__status--error"));
    }
}
