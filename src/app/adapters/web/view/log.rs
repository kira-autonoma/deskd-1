//! Log-view rendering (#485). Renders the tailed log lines inside a
//! `<pre>` block. ANSI escapes are stripped upstream by
//! `data_log::tail_log`, so this layer only HTML-escapes for safety.

use super::html_escape;
use crate::app::adapters::web::data_log::LogLine;

/// Render the log view body: header + line count + `<pre>` block.
///
/// `agent` and `session_filter` are surfaced in the header so the user
/// knows what they're looking at. `lines` is rendered oldest-first
/// (matches file order) inside a single `<pre>` element.
pub fn log_view(agent: &str, session_filter: Option<&str>, lines: &[LogLine]) -> String {
    let agent_esc = html_escape(agent);
    let filter_html = match session_filter {
        Some(s) if !s.is_empty() => format!(
            r#" <small class="log-view__filter">session=<code>{}</code></small>"#,
            html_escape(s),
        ),
        _ => String::new(),
    };
    let count = lines.len();
    let lines_html = render_lines(lines);
    let clear_link = match session_filter {
        Some(s) if !s.is_empty() => format!(
            r#"<p class="log-view__actions"><a href="/agent/{agent}/log">clear session filter</a></p>"#,
            agent = html_escape(agent),
        ),
        _ => String::new(),
    };

    format!(
        r#"<section class="log-view">
  <header class="log-view__head">
    <h2>Log for <code>{agent}</code>{filter}</h2>
    <p class="log-view__meta"><small>{count} line(s) shown — most recent at bottom</small></p>
  </header>
  {clear}
  {body}
</section>"#,
        agent = agent_esc,
        filter = filter_html,
        count = count,
        clear = clear_link,
        body = lines_html,
    )
}

fn render_lines(lines: &[LogLine]) -> String {
    if lines.is_empty() {
        return r#"<p class="log-view__empty">No log lines available.</p>"#.to_string();
    }
    let mut body = String::from(r#"<pre class="log-view__pre">"#);
    for line in lines {
        body.push_str(&html_escape(&line.body));
        body.push('\n');
    }
    body.push_str("</pre>");
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_view_renders_pre_block_with_lines() {
        let lines = vec![
            LogLine {
                body: "2026-01-01 INFO hello".into(),
            },
            LogLine {
                body: "2026-01-01 WARN trouble".into(),
            },
        ];
        let html = log_view("kira", None, &lines);
        assert!(html.contains(r#"<code>kira</code>"#));
        assert!(html.contains("2 line(s) shown"));
        assert!(html.contains("<pre"));
        assert!(html.contains("INFO hello"));
        assert!(html.contains("WARN trouble"));
    }

    #[test]
    fn log_view_surfaces_session_filter_in_header() {
        let html = log_view(
            "kira",
            Some("abc123"),
            &[LogLine {
                body: "line".into(),
            }],
        );
        assert!(html.contains("session=<code>abc123</code>"));
        assert!(html.contains("clear session filter"));
    }

    #[test]
    fn log_view_empty_state_when_no_lines() {
        let html = log_view("kira", None, &[]);
        assert!(html.contains("No log lines available"));
        assert!(!html.contains("<pre"));
    }

    #[test]
    fn log_view_escapes_html_in_log_lines() {
        let html = log_view(
            "kira",
            None,
            &[LogLine {
                body: "<script>alert(1)</script>".into(),
            }],
        );
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn log_view_escapes_agent_name() {
        let html = log_view("<x>", None, &[]);
        assert!(!html.contains("<code><x></code>"));
        assert!(html.contains("&lt;x&gt;"));
    }
}
