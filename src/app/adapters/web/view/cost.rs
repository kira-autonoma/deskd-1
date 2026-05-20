//! HTML rendering for `/dashboard/cost` (#473).
//!
//! Mobile-first table layout — every section is a single column on phones
//! and lets the operator eyeball calibration without scrolling. Section
//! order matches the issue spec:
//!
//! 1. Budget bar (when `weekly_ceiling` is configured)
//! 2. Pipeline (open `agent-ready` tickets, newest first)
//! 3. History (recently-closed tickets with delta column)
//! 4. Accuracy table (per-ticket delta sparkline alternative)
//!
//! The renderers are pure: take an immutable `CostData` snapshot and a
//! `CostConfig` reference, return an HTML fragment. No state, no async,
//! no IO. Tests drive each section individually.

use crate::app::adapters::web::data_cost::{CostData, HistoryTicket, PipelineTicket};
use crate::app::adapters::web::view::html_escape;
use crate::config::CostConfig;

/// «tracking pending» placeholder text — exact AC wording.
pub const TRACKING_PENDING: &str = "tracking pending";

/// Render the entire `/dashboard/cost` body. Returns an HTML fragment
/// suitable for embedding inside the `cost_page` template wrapper.
pub fn cost_body(data: &CostData, cfg: &CostConfig) -> String {
    let mut out = String::new();
    out.push_str(&budget_bar(data, cfg));
    out.push_str(&pipeline_section(&data.pipeline));
    out.push_str(&history_section(&data.history));
    out.push_str(&accuracy_section(&data.history));
    out
}

/// Render the budget bar — only when `weekly_ceiling` is set. Otherwise
/// returns an empty string per AC.
pub fn budget_bar(data: &CostData, cfg: &CostConfig) -> String {
    let Some(ceiling) = cfg.weekly_ceiling else {
        return String::new();
    };
    if ceiling == 0 {
        return String::new();
    }
    let used = data.weekly_total_tokens;
    let pct = ((used as f64 / ceiling as f64) * 100.0).min(999.0);
    let bucket = pct_bucket(pct);
    let warn = if pct >= 80.0 { " budget-bar--warn" } else { "" };
    format!(
        r#"<section class="budget-bar{warn}">
  <h2>Weekly budget</h2>
  <p class="budget-bar__label">{used} / {ceiling} tokens ({pct:.0}%)</p>
  <div class="budget-bar__track">
    <div class="budget-bar__fill budget-bar__fill--w-{bucket}"></div>
  </div>
</section>"#,
        warn = warn,
        used = format_int(used),
        ceiling = format_int(ceiling),
        pct = pct,
        bucket = bucket,
    )
}

/// Render the pipeline (open `agent-ready` tickets) table. Empty list →
/// friendly placeholder so the page never looks broken.
pub fn pipeline_section(pipeline: &[PipelineTicket]) -> String {
    if pipeline.is_empty() {
        return r#"<section class="cost-section cost-pipeline">
  <h2>Pipeline</h2>
  <p class="cost-empty">No open agent-ready tickets.</p>
</section>"#
            .to_string();
    }
    let mut rows = String::new();
    for t in pipeline {
        let estimate = match t.estimate_tokens {
            Some(n) => format_tokens(n),
            None => "unknown".to_string(),
        };
        let bucket = t
            .estimate_bucket
            .as_deref()
            .map(html_escape)
            .unwrap_or_else(|| "—".to_string());
        rows.push_str(&format!(
            r#"    <tr>
      <td class="cost-cell__repo">{repo}</td>
      <td class="cost-cell__num">#{num}</td>
      <td class="cost-cell__title">{title}</td>
      <td class="cost-cell__bucket">{bucket}</td>
      <td class="cost-cell__estimate">{estimate}</td>
    </tr>
"#,
            repo = html_escape(&t.repo),
            num = t.number,
            title = html_escape(&t.title),
            bucket = bucket,
            estimate = html_escape(&estimate),
        ));
    }
    format!(
        r#"<section class="cost-section cost-pipeline">
  <h2>Pipeline</h2>
  <table class="cost-table">
    <thead>
      <tr><th>Repo</th><th>#</th><th>Title</th><th>Bucket</th><th>Estimate</th></tr>
    </thead>
    <tbody>
{rows}    </tbody>
  </table>
</section>"#,
        rows = rows,
    )
}

/// Render the history (recently closed) table with estimate / actual /
/// delta columns. `tracking pending` placeholder when `actual_tokens` is
/// missing.
pub fn history_section(history: &[HistoryTicket]) -> String {
    if history.is_empty() {
        return r#"<section class="cost-section cost-history">
  <h2>History</h2>
  <p class="cost-empty">No closed agent-ready tickets in window.</p>
</section>"#
            .to_string();
    }
    let mut rows = String::new();
    for h in history {
        let estimate = match h.estimate_tokens {
            Some(n) => format_tokens(n),
            None => "unknown".to_string(),
        };
        let actual = match h.actual_tokens {
            Some(n) => format_tokens(n),
            None => TRACKING_PENDING.to_string(),
        };
        let (delta_text, delta_class) = match h.delta_tokens() {
            Some(d) => {
                let sign = if d > 0 { "+" } else { "" };
                let class = if d > 0 {
                    " cost-cell__delta--over"
                } else if d < 0 {
                    " cost-cell__delta--under"
                } else {
                    ""
                };
                (
                    format!("{sign}{d}", sign = sign, d = format_int_signed(d)),
                    class,
                )
            }
            None => ("—".to_string(), ""),
        };
        rows.push_str(&format!(
            r#"    <tr>
      <td class="cost-cell__repo">{repo}</td>
      <td class="cost-cell__num">#{num}</td>
      <td class="cost-cell__title">{title}</td>
      <td class="cost-cell__estimate">{estimate}</td>
      <td class="cost-cell__actual">{actual}</td>
      <td class="cost-cell__delta{delta_class}">{delta}</td>
    </tr>
"#,
            repo = html_escape(&h.repo),
            num = h.number,
            title = html_escape(&h.title),
            estimate = html_escape(&estimate),
            actual = html_escape(&actual),
            delta = html_escape(&delta_text),
            delta_class = delta_class,
        ));
    }
    format!(
        r#"<section class="cost-section cost-history">
  <h2>History</h2>
  <table class="cost-table">
    <thead>
      <tr><th>Repo</th><th>#</th><th>Title</th><th>Est</th><th>Actual</th><th>Δ</th></tr>
    </thead>
    <tbody>
{rows}    </tbody>
  </table>
</section>"#,
        rows = rows,
    )
}

/// Accuracy delta table — closed tickets with a ratio column. Pure HTML
/// rather than SVG: easier to swap, easier to scan on a phone, and the
/// AC explicitly allows «table delta» as a valid visualization.
pub fn accuracy_section(history: &[HistoryTicket]) -> String {
    let with_ratio: Vec<&HistoryTicket> = history.iter().filter(|h| h.ratio().is_some()).collect();
    if with_ratio.is_empty() {
        return r#"<section class="cost-section cost-accuracy">
  <h2>Estimate accuracy</h2>
  <p class="cost-empty">No closed tickets with both estimate and actual yet.</p>
</section>"#
            .to_string();
    }
    let mut rows = String::new();
    for h in &with_ratio {
        let ratio = h.ratio().unwrap_or(0.0);
        let pct = (ratio * 100.0).round() as i64;
        let class = if ratio > 1.10 {
            " cost-cell__ratio--over"
        } else if ratio < 0.90 {
            " cost-cell__ratio--under"
        } else {
            " cost-cell__ratio--accurate"
        };
        rows.push_str(&format!(
            r#"    <tr>
      <td class="cost-cell__repo">{repo}</td>
      <td class="cost-cell__num">#{num}</td>
      <td class="cost-cell__title">{title}</td>
      <td class="cost-cell__ratio{class}">{pct}%</td>
    </tr>
"#,
            repo = html_escape(&h.repo),
            num = h.number,
            title = html_escape(&h.title),
            class = class,
            pct = pct,
        ));
    }
    format!(
        r#"<section class="cost-section cost-accuracy">
  <h2>Estimate accuracy</h2>
  <p class="cost-accuracy__hint">100% = on-budget. Over → red, under → green.</p>
  <table class="cost-table">
    <thead>
      <tr><th>Repo</th><th>#</th><th>Title</th><th>Actual / Est</th></tr>
    </thead>
    <tbody>
{rows}    </tbody>
  </table>
</section>"#,
        rows = rows,
    )
}

/// Pick a width-bucket class for the budget bar so CSP forbids inline
/// `style="width: …"`. Buckets are 10% steps clamped to [0, 100].
fn pct_bucket(pct: f64) -> u32 {
    let rounded = pct.round() as i64;
    let clamped = rounded.clamp(0, 100);
    ((clamped / 10) * 10) as u32
}

/// Pretty-print a token count: `12_345` → `12.3k`, `1_234_567` → `1.23M`.
/// Designed for the table columns where horizontal space is at a premium.
fn format_tokens(n: u64) -> String {
    if n < 1_000 {
        return n.to_string();
    }
    if n < 1_000_000 {
        return format!("{:.1}k", n as f64 / 1_000.0);
    }
    format!("{:.2}M", n as f64 / 1_000_000.0)
}

/// Thousands-separated integer.
fn format_int(n: u64) -> String {
    insert_thousand_separators(&n.to_string())
}

fn format_int_signed(n: i64) -> String {
    let neg = n < 0;
    let s = insert_thousand_separators(&n.unsigned_abs().to_string());
    if neg { format!("-{}", s) } else { s }
}

fn insert_thousand_separators(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len() + bytes.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        let from_end = bytes.len() - i;
        if i > 0 && from_end.is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::adapters::web::data_cost::CostData;
    use crate::config::CostBuckets;
    use chrono::Utc;

    fn cfg_with_ceiling(ceiling: Option<u64>) -> CostConfig {
        CostConfig {
            buckets: CostBuckets {
                s: 10_000,
                m: 50_000,
                l: 200_000,
                xl: 500_000,
            },
            weekly_ceiling: ceiling,
            history_days: 7,
            repos: vec!["a/b".into()],
            ready_label: "agent-ready".into(),
        }
    }

    fn empty_data() -> CostData {
        CostData {
            pipeline: vec![],
            history: vec![],
            weekly_total_tokens: 0,
            fetched_at: Utc::now(),
        }
    }

    #[test]
    fn budget_bar_omitted_when_ceiling_unset() {
        let cfg = cfg_with_ceiling(None);
        let html = budget_bar(&empty_data(), &cfg);
        assert!(html.is_empty(), "expected empty budget bar, got: {}", html);
    }

    #[test]
    fn budget_bar_rendered_when_ceiling_set() {
        let cfg = cfg_with_ceiling(Some(1_000_000));
        let data = CostData {
            weekly_total_tokens: 500_000,
            ..empty_data()
        };
        let html = budget_bar(&data, &cfg);
        assert!(html.contains("Weekly budget"));
        assert!(html.contains("500,000"));
        assert!(html.contains("1,000,000"));
        assert!(html.contains("50%"));
        // CSP guard — no inline style attribute.
        assert!(
            !html.contains("style="),
            "budget bar must use class-based width"
        );
        // Bucket class for 50%.
        assert!(html.contains("budget-bar__fill--w-50"));
    }

    #[test]
    fn budget_bar_warn_class_above_80_pct() {
        let cfg = cfg_with_ceiling(Some(100_000));
        let data = CostData {
            weekly_total_tokens: 95_000,
            ..empty_data()
        };
        let html = budget_bar(&data, &cfg);
        assert!(html.contains("budget-bar--warn"));
    }

    #[test]
    fn pipeline_section_renders_unknown_for_missing_bucket() {
        let pipeline = vec![PipelineTicket {
            repo: "a/b".into(),
            number: 7,
            title: "no bucket".into(),
            estimate_bucket: None,
            estimate_tokens: None,
            created_at: None,
        }];
        let html = pipeline_section(&pipeline);
        assert!(html.contains("unknown"));
    }

    #[test]
    fn pipeline_section_renders_bucket_estimate() {
        let pipeline = vec![PipelineTicket {
            repo: "a/b".into(),
            number: 7,
            title: "medium".into(),
            estimate_bucket: Some("M".into()),
            estimate_tokens: Some(50_000),
            created_at: None,
        }];
        let html = pipeline_section(&pipeline);
        assert!(html.contains("50.0k"));
        assert!(html.contains("M"));
    }

    #[test]
    fn history_section_renders_tracking_pending() {
        let history = vec![HistoryTicket {
            repo: "a/b".into(),
            number: 12,
            title: "closed".into(),
            estimate_bucket: Some("M".into()),
            estimate_tokens: Some(50_000),
            actual_tokens: None,
            closed_at: Some(Utc::now()),
        }];
        let html = history_section(&history);
        assert!(html.contains(TRACKING_PENDING));
        assert!(html.contains("50.0k"));
    }

    #[test]
    fn history_section_renders_signed_delta() {
        let history = vec![HistoryTicket {
            repo: "a/b".into(),
            number: 1,
            title: "over".into(),
            estimate_bucket: Some("S".into()),
            estimate_tokens: Some(10_000),
            actual_tokens: Some(12_000),
            closed_at: Some(Utc::now()),
        }];
        let html = history_section(&history);
        assert!(html.contains("cost-cell__delta--over"));
        // Delta is +2_000.
        assert!(html.contains("+2,000"));
    }

    #[test]
    fn accuracy_section_renders_ratio_pct() {
        let history = vec![HistoryTicket {
            repo: "a/b".into(),
            number: 1,
            title: "under".into(),
            estimate_bucket: Some("M".into()),
            estimate_tokens: Some(100_000),
            actual_tokens: Some(80_000),
            closed_at: Some(Utc::now()),
        }];
        let html = accuracy_section(&history);
        assert!(html.contains("80%"));
        assert!(html.contains("cost-cell__ratio--under"));
    }

    #[test]
    fn accuracy_section_skips_rows_without_actual() {
        let history = vec![
            HistoryTicket {
                repo: "a/b".into(),
                number: 1,
                title: "no actual".into(),
                estimate_bucket: Some("M".into()),
                estimate_tokens: Some(50_000),
                actual_tokens: None,
                closed_at: None,
            },
            HistoryTicket {
                repo: "a/b".into(),
                number: 2,
                title: "no estimate".into(),
                estimate_bucket: None,
                estimate_tokens: None,
                actual_tokens: Some(50_000),
                closed_at: None,
            },
        ];
        let html = accuracy_section(&history);
        // Both rows lack a ratio → friendly empty state.
        assert!(html.contains("No closed tickets"));
    }

    #[test]
    fn cost_body_assembles_sections_in_order() {
        let cfg = cfg_with_ceiling(Some(100_000));
        let data = empty_data();
        let html = cost_body(&data, &cfg);
        let budget_idx = html.find("Weekly budget").unwrap();
        let pipeline_idx = html.find("Pipeline").unwrap();
        let history_idx = html.find("History").unwrap();
        let accuracy_idx = html.find("Estimate accuracy").unwrap();
        assert!(budget_idx < pipeline_idx);
        assert!(pipeline_idx < history_idx);
        assert!(history_idx < accuracy_idx);
    }

    #[test]
    fn format_tokens_compact_units() {
        assert_eq!(format_tokens(500), "500");
        assert_eq!(format_tokens(12_500), "12.5k");
        assert_eq!(format_tokens(1_234_567), "1.23M");
    }

    #[test]
    fn pct_bucket_clamps_and_rounds() {
        assert_eq!(pct_bucket(0.0), 0);
        assert_eq!(pct_bucket(15.0), 10);
        assert_eq!(pct_bucket(99.0), 90);
        assert_eq!(pct_bucket(150.0), 100);
    }

    #[test]
    fn html_escapes_user_strings() {
        let pipeline = vec![PipelineTicket {
            repo: "a/b".into(),
            number: 1,
            title: "<script>".into(),
            estimate_bucket: None,
            estimate_tokens: None,
            created_at: None,
        }];
        let html = pipeline_section(&pipeline);
        assert!(html.contains("&lt;script&gt;"));
        assert!(!html.contains("<script>"));
    }
}
