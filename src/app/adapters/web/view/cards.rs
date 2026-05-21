//! Agent card rendering (#444).
//!
//! Pure functions: take an `AgentSummary`, return an HTML fragment. The
//! same renderer is used both for the initial server-side render and for
//! SSE-driven htmx swaps so cards stay structurally consistent.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::app::adapters::web::data::AgentSummary;

use super::html_escape;

/// Stable DOM id for an agent's card. Used by htmx for SSE swap targeting.
pub fn agent_card_id(name: &str) -> String {
    format!("agent-{}", sanitise_id(name))
}

/// Render a single agent card. The output is an `<article>` element wrapping
/// every field listed in the issue spec.
pub fn agent_card(summary: &AgentSummary) -> String {
    agent_card_with_disk(summary, None)
}

/// Render a single agent card with a known disk-snapshot timestamp.
/// `disk_updated_at` populates the «updated N ago» tooltip on the home-dir
/// row (#446). When `None`, the row carries no tooltip — same as the
/// pre-#446 rendering.
pub fn agent_card_with_disk(
    summary: &AgentSummary,
    disk_updated_at: Option<DateTime<Utc>>,
) -> String {
    let id = agent_card_id(&summary.name);
    let status_class = status_class(&summary.status);
    let status_label = html_escape(&summary.status);
    let model = html_escape(&summary.model);
    let last_activity = summary
        .last_activity
        .map(format_relative)
        .unwrap_or_else(em_dash);
    let context_line = render_context(summary);
    let home_dir_value = summary
        .home_dir_bytes
        .map(format_bytes)
        .unwrap_or_else(em_dash);
    let home_dir_cell = match disk_updated_at {
        Some(ts) => format!(
            r#"<dd title="updated {short} ({iso})">{value}</dd>"#,
            short = html_escape(&format_relative(ts)),
            iso = html_escape(&ts.to_rfc3339()),
            value = home_dir_value,
        ),
        None => format!("<dd>{}</dd>", home_dir_value),
    };
    let task_block = render_task_block(summary);

    // #485: whole-card click target. The body is wrapped in a single
    // `<a class="agent-card__link">` so any tap inside the card (>44px
    // height once CSS lays it out) navigates to the agent detail page.
    // The `<h2 class="agent-card__name">` element keeps its own
    // semantic heading but no longer hosts a redundant inner anchor —
    // nesting `<a>` inside `<a>` would break flat HTML.
    format!(
        r#"<article id="{id}" class="agent-card">
  <a class="agent-card__link" href="/agent/{name}">
    <header class="agent-card__head">
      <h2 class="agent-card__name">{name}</h2>
      <span class="agent-card__status agent-card__status--{status_class}">{status_label}</span>
      <span class="agent-card__model">{model}</span>
    </header>
    <dl class="agent-card__rows">
      <dt>last activity</dt><dd>{last_activity}</dd>
      {context_line}
      <dt>home dir</dt>{home_dir_cell}
      {task_block}
    </dl>
  </a>
</article>"#,
        id = html_escape(&id),
        name = html_escape(&summary.name),
        status_class = status_class,
        status_label = status_label,
        model = model,
        last_activity = last_activity,
        context_line = context_line,
        home_dir_cell = home_dir_cell,
        task_block = task_block,
    )
}

/// Render the agent list section (used by the initial dashboard render).
/// Empty state is friendly: encourages the user to add an agent rather than
/// looking like an error.
///
/// Backward-compatible wrapper around [`agents_section_with_disk`] that
/// passes `None` for the disk timestamp.
pub fn agents_section(summaries: &[AgentSummary]) -> String {
    agents_section_with_disk(summaries, None)
}

/// Same as [`agents_section`] but with the cached disk-metrics
/// `updated_at` propagated to each card so the home-dir cell carries an
/// «updated N ago» tooltip (#446).
pub fn agents_section_with_disk(
    summaries: &[AgentSummary],
    disk_updated_at: Option<DateTime<Utc>>,
) -> String {
    if summaries.is_empty() {
        return r#"<section class="agents agents--empty" hx-ext="sse" sse-connect="/events">
  <p class="agents__empty">No agents configured. Add one in <code>workspace.yaml</code>.</p>
</section>"#
            .to_string();
    }

    let mut out = String::new();
    out.push_str(r#"<section class="agents" hx-ext="sse" sse-connect="/events">"#);
    out.push('\n');
    for s in summaries {
        out.push_str(&agent_card_with_disk(s, disk_updated_at));
        out.push('\n');
    }
    out.push_str("</section>");
    out
}

fn render_context(summary: &AgentSummary) -> String {
    // #483: the progress-bar denominator is the model's hard context_limit,
    // NOT the auto-compact threshold. The threshold is rendered as a small
    // annotation so the "compact-soon" signal isn't lost. Fall back to the
    // threshold ONLY when context_limit is missing (older payloads).
    let limit = summary.context_limit.or(summary.context_threshold);
    match (summary.context_tokens, limit) {
        (Some(tokens), Some(limit)) if limit > 0 => {
            let pct = ((tokens as f64 / limit as f64) * 100.0).clamp(0.0, 999.9);
            let bar_pct = pct.min(100.0);
            let bucket = nearest_bucket_class(bar_pct);
            let used = format_tokens_compact(tokens);
            let limit_str = format_tokens_compact(limit);
            // No inline `style=` attribute: width comes from the bucket
            // class (`.ctx-bar__fill--w-0` … `--w-100` in 10% steps) so
            // the strict CSP from #443 holds. See #450 review.
            //
            // Auto-compact threshold annotation: only rendered when both
            // threshold and limit are known and the threshold isn't the
            // limit itself (avoid noise).
            let compact_marker = match summary.context_threshold {
                Some(threshold) if threshold > 0 && threshold < limit => {
                    let t = format_tokens_compact(threshold);
                    format!(
                        r#" <small class="ctx-compact-at">compact at {t}</small>"#,
                        t = html_escape(&t),
                    )
                }
                _ => String::new(),
            };
            format!(
                r#"<dt>context</dt><dd>
      <span class="ctx-numbers">{used} / {limit_str}</span>
      <span class="ctx-bar"><span class="ctx-bar__fill ctx-bar__fill--w-{bucket}"></span></span>
      <span class="ctx-pct">{pct:.0}%</span>{compact_marker}
    </dd>"#,
                used = html_escape(&used),
                limit_str = html_escape(&limit_str),
                bucket = bucket,
                pct = pct,
                compact_marker = compact_marker,
            )
        }
        _ => format!("<dt>context</dt><dd>{}</dd>", em_dash()),
    }
}

/// Pick the nearest 10%-bucket class for the progress-bar fill. Returns
/// one of 0, 10, 20, …, 100. Values below 5% snap to 0; values at or above
/// 95% snap to 100. Eliminates the inline `style="width: X%"` attribute
/// that the strict CSP from #443 (`style-src 'self'`) would otherwise
/// reject.
fn nearest_bucket_class(pct: f64) -> u8 {
    let clamped = pct.clamp(0.0, 100.0);
    // Round to nearest 10% via `(x + 5) / 10 * 10` semantics. `as u8`
    // truncates, which is fine because clamped ≤ 100.
    let bucket = ((clamped + 5.0) / 10.0) as u8 * 10;
    bucket.min(100)
}

fn render_task_block(summary: &AgentSummary) -> String {
    match (&summary.current_task, summary.task_running_for) {
        (Some(task), Some(dur)) => format!(
            r#"<dt>current task</dt><dd>"{task}" <small>(running for {dur})</small></dd>"#,
            task = html_escape(task),
            dur = html_escape(&format_duration_short(dur)),
        ),
        (Some(task), None) => format!(
            r#"<dt>current task</dt><dd>"{task}"</dd>"#,
            task = html_escape(task),
        ),
        _ => String::new(),
    }
}

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

/// `123` → `123`, `1500` → `1.5k`, `300_000` → `300k`, `1_000_000` → `1M`.
/// Matches the formatting used by `/context` so dashboard and Telegram
/// report the same.
fn format_tokens_compact(n: u64) -> String {
    if n < 1_000 {
        return n.to_string();
    }
    if n >= 1_000_000 {
        let m = n as f64 / 1_000_000.0;
        if m >= 10.0 {
            return format!("{}M", m.round() as u64);
        }
        let rounded = (m * 10.0).round() / 10.0;
        if (rounded - rounded.round()).abs() < f64::EPSILON {
            return format!("{}M", rounded as u64);
        }
        return format!("{:.1}M", rounded);
    }
    let k = n as f64 / 1_000.0;
    if k >= 10.0 {
        format!("{}k", k.round() as u64)
    } else {
        let rounded = (k * 10.0).round() / 10.0;
        if (rounded - rounded.round()).abs() < f64::EPSILON {
            format!("{}k", rounded as u64)
        } else {
            format!("{:.1}k", rounded)
        }
    }
}

/// Format a relative time like "5s ago", "2 min ago", "3 h ago", "yesterday".
pub fn format_relative(t: DateTime<Utc>) -> String {
    let now = Utc::now();
    let delta = now.signed_duration_since(t);
    let secs = delta.num_seconds();
    if secs < 0 {
        return "just now".to_string();
    }
    if secs < 5 {
        return "just now".to_string();
    }
    if secs < 60 {
        return format!("{}s ago", secs);
    }
    let mins = delta.num_minutes();
    if mins < 60 {
        return format!("{} min ago", mins);
    }
    let hours = delta.num_hours();
    if hours < 24 {
        return format!("{}h ago", hours);
    }
    let days = delta.num_days();
    if days < 7 {
        return format!("{}d ago", days);
    }
    t.format("%Y-%m-%d").to_string()
}

fn format_duration_short(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        return format!("{}s", secs);
    }
    let mins = secs / 60;
    let rem = secs % 60;
    if mins < 60 {
        if rem == 0 {
            return format!("{}m", mins);
        }
        return format!("{}m {}s", mins, rem);
    }
    let hours = mins / 60;
    let rem_min = mins % 60;
    if rem_min == 0 {
        format!("{}h", hours)
    } else {
        format!("{}h {}m", hours, rem_min)
    }
}

/// Format bytes with binary units. 1023 B, 1.5 KiB, 412 MiB, 4.2 GiB.
pub fn format_bytes(b: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;
    if b < KIB {
        return format!("{} B", b);
    }
    if b < MIB {
        return format!("{:.1} KiB", b as f64 / KIB as f64);
    }
    if b < GIB {
        // Whole MiB for values >= 10 MiB, otherwise one decimal.
        let mib = b as f64 / MIB as f64;
        if mib >= 10.0 {
            return format!("{} MiB", mib.round() as u64);
        }
        return format!("{:.1} MiB", mib);
    }
    if b < TIB {
        return format!("{:.2} GiB", b as f64 / GIB as f64);
    }
    format!("{:.2} TiB", b as f64 / TIB as f64)
}

/// Replace anything that's not `[a-zA-Z0-9_-]` with `-` so agent names can
/// safely appear in DOM ids.
fn sanitise_id(name: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn summary(name: &str) -> AgentSummary {
        AgentSummary {
            name: name.into(),
            status: "idle".into(),
            model: "claude-opus-4-7".into(),
            last_activity: None,
            context_tokens: None,
            context_threshold: None,
            context_limit: None,
            home_dir_bytes: None,
            current_task: None,
            task_running_for: None,
        }
    }

    #[test]
    fn agent_card_id_is_stable_and_safe() {
        assert_eq!(agent_card_id("kira"), "agent-kira");
        assert_eq!(agent_card_id("dev/sub-1"), "agent-dev-sub-1");
    }

    #[test]
    fn empty_section_renders_friendly_message() {
        let html = agents_section(&[]);
        assert!(html.contains("No agents configured"));
        assert!(html.contains("sse-connect=\"/events\""));
    }

    #[test]
    fn card_renders_em_dash_for_missing_disk_size() {
        let s = summary("kira");
        let html = agent_card(&s);
        assert!(html.contains(r#"id="agent-kira""#));
        assert!(html.contains("home dir"));
        // Em-dash wrapped in span for styling.
        assert!(html.contains("—"));
    }

    #[test]
    fn card_renders_context_progress_when_data_available() {
        // #483: denominator is the model context_limit, not the auto-compact
        // threshold. With limit=1M and tokens=120k we expect ~12%, not 40%.
        let mut s = summary("kira");
        s.context_tokens = Some(120_000);
        s.context_threshold = Some(300_000);
        s.context_limit = Some(1_000_000);
        let html = agent_card(&s);
        assert!(html.contains("120k / 1M"), "got: {}", html);
        // 120/1000 = 12% → bucket class --w-10 (no inline `style=` attr).
        assert!(html.contains("ctx-bar__fill--w-10"));
        assert!(!html.contains("style=\"width"));
        assert!(html.contains("12%"));
        // The compact-at threshold is still surfaced as a marker.
        assert!(html.contains("compact at 300k"), "got: {}", html);
    }

    #[test]
    fn card_context_indicator_never_exceeds_100_percent_when_above_threshold() {
        // #483 regression: when tokens cross the auto-compact threshold but
        // are below the model limit, the bar must clamp to ≤100% AND the
        // pct should reflect (tokens / limit), not (tokens / threshold).
        let mut s = summary("dev");
        s.context_tokens = Some(583_000);
        s.context_threshold = Some(300_000);
        s.context_limit = Some(1_000_000);
        let html = agent_card(&s);
        // 583/1000 = 58.3% — bucket bar fills at 60%, pct shows 58%.
        assert!(html.contains("583k / 1M"), "got: {}", html);
        assert!(html.contains("ctx-bar__fill--w-60"));
        assert!(html.contains("58%"));
        // The dangerous >100% number from the bug report must NOT appear.
        assert!(
            !html.contains("194%"),
            "pre-fix pct must not appear; got: {}",
            html
        );
    }

    #[test]
    fn card_context_indicator_omits_compact_marker_when_threshold_equals_limit() {
        // If threshold == limit (no soft trigger configured), don't render
        // a redundant "compact at" annotation.
        let mut s = summary("kira");
        s.context_tokens = Some(500_000);
        s.context_threshold = Some(1_000_000);
        s.context_limit = Some(1_000_000);
        let html = agent_card(&s);
        assert!(!html.contains("compact at"), "got: {}", html);
    }

    #[test]
    fn card_context_falls_back_to_threshold_when_limit_missing() {
        // Backward-compat: older SessionContext payloads without limit
        // should still render something sensible (use threshold as
        // denominator like before).
        let mut s = summary("legacy");
        s.context_tokens = Some(120_000);
        s.context_threshold = Some(300_000);
        s.context_limit = None;
        let html = agent_card(&s);
        assert!(html.contains("120k / 300k"), "got: {}", html);
        assert!(html.contains("40%"));
    }

    #[test]
    fn card_handles_above_100_percent_context_without_overflow() {
        let mut s = summary("kira");
        s.context_tokens = Some(450_000);
        s.context_threshold = Some(300_000);
        let html = agent_card(&s);
        // Bar fill is clamped at 100% bucket; numeric label can exceed.
        assert!(html.contains("ctx-bar__fill--w-100"));
        assert!(!html.contains("style=\"width"));
        assert!(html.contains("150%"));
    }

    #[test]
    fn nearest_bucket_class_snaps_to_ten_percent_steps() {
        // Boundary points: <5 → 0, ≥5 → 10, … ≥95 → 100.
        assert_eq!(nearest_bucket_class(0.0), 0);
        assert_eq!(nearest_bucket_class(4.999), 0);
        assert_eq!(nearest_bucket_class(5.0), 10);
        assert_eq!(nearest_bucket_class(14.999), 10);
        assert_eq!(nearest_bucket_class(15.0), 20);
        assert_eq!(nearest_bucket_class(49.0), 50);
        assert_eq!(nearest_bucket_class(94.999), 90);
        assert_eq!(nearest_bucket_class(95.0), 100);
        assert_eq!(nearest_bucket_class(100.0), 100);
        // Defensive: anything over 100 still maps to 100 (caller clamps).
        assert_eq!(nearest_bucket_class(150.0), 100);
    }

    #[test]
    fn card_renders_current_task_with_duration() {
        let mut s = summary("dev");
        s.status = "working".into();
        s.current_task = Some("review PR #441".into());
        s.task_running_for = Some(Duration::from_secs(102));
        let html = agent_card(&s);
        assert!(html.contains(r#""review PR #441""#));
        assert!(html.contains("running for 1m 42s"));
    }

    #[test]
    fn card_skips_task_block_when_idle() {
        let s = summary("kira");
        let html = agent_card(&s);
        assert!(!html.contains("current task"));
    }

    #[test]
    fn card_body_wrapped_in_anchor_for_full_click_target() {
        // #485 AC: cards must be fully clickable. The card body is wrapped
        // in a single `<a class="agent-card__link">` whose href points at
        // the agent detail page.
        let s = summary("kira");
        let html = agent_card(&s);
        assert!(
            html.contains(r#"<a class="agent-card__link" href="/agent/kira">"#),
            "missing whole-card anchor; got:\n{}",
            html,
        );
        // Anchor wraps the head + dl, so look for both inside the anchor
        // by checking the anchor opens before the header and closes after
        // the closing dl.
        let a_open = html.find(r#"<a class="agent-card__link""#).unwrap();
        let dl_close = html.rfind("</dl>").unwrap();
        let a_close = html.rfind("</a>").unwrap();
        assert!(a_open < dl_close, "anchor must open before <dl>");
        assert!(a_close > dl_close, "anchor must close after </dl>");
        // No nested anchors (h2 no longer carries its own <a>).
        assert!(
            !html.contains(r#"<h2 class="agent-card__name"><a"#),
            "h2 must not carry a nested anchor; got:\n{}",
            html
        );
    }

    #[test]
    fn card_status_uses_dedicated_class() {
        let mut s = summary("kira");
        s.status = "working".into();
        let html = agent_card(&s);
        assert!(html.contains("agent-card__status--working"));
    }

    #[test]
    fn card_escapes_xss_in_user_supplied_fields() {
        let mut s = summary("<x>");
        s.current_task = Some("<script>alert(1)</script>".into());
        s.task_running_for = Some(Duration::from_secs(1));
        let html = agent_card(&s);
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;"));
        // Name appears in id (sanitised) and h2 (escaped).
        assert!(html.contains("&lt;x&gt;"));
    }

    #[test]
    fn agents_section_renders_one_card_per_agent() {
        let summaries = vec![summary("a"), summary("b"), summary("c")];
        let html = agents_section(&summaries);
        assert!(html.contains(r#"id="agent-a""#));
        assert!(html.contains(r#"id="agent-b""#));
        assert!(html.contains(r#"id="agent-c""#));
    }

    #[test]
    fn agents_section_handles_ten_agents_without_panic() {
        // AC: «Page renders correctly with 0 agents, 1 agent, 10 agents.»
        // The 0-agent case is covered by `empty_section_renders_friendly_message`
        // and the 1-agent case by the various single-card tests above; this
        // verifies the upper bound from the issue spec.
        let mut summaries = Vec::with_capacity(10);
        for i in 0..10 {
            let mut s = summary(&format!("agent-{}", i));
            s.context_tokens = Some(1_000 * (i as u64 + 1) * 30);
            s.context_threshold = Some(300_000);
            s.home_dir_bytes = Some(412 * 1024 * 1024);
            summaries.push(s);
        }
        let html = agents_section(&summaries);
        for i in 0..10 {
            assert!(
                html.contains(&format!(r#"id="agent-agent-{}""#, i)),
                "missing card for agent-{}",
                i
            );
        }
        // Spot-check that disk size renders.
        assert!(html.contains("412 MiB"));
    }

    #[test]
    fn format_bytes_buckets() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(2_097_152), "2.0 MiB");
        assert_eq!(format_bytes(11 * 1024 * 1024), "11 MiB");
        // 412 MiB target from the issue mockup.
        assert_eq!(format_bytes(412 * 1024 * 1024), "412 MiB");
    }

    #[test]
    fn format_relative_buckets() {
        let now = Utc::now();
        assert_eq!(format_relative(now), "just now");
        assert!(format_relative(now - chrono::Duration::seconds(30)).ends_with("s ago"));
        assert!(format_relative(now - chrono::Duration::minutes(5)).contains("min ago"));
        assert!(format_relative(now - chrono::Duration::hours(3)).ends_with("h ago"));
    }
}
