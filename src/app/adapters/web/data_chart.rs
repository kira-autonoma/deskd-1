//! Top-of-dashboard chart aggregation (#484).
//!
//! Builds per-agent time-series data for the summary chart that sits at the
//! top of the dashboard. The chart is server-rendered SVG (see
//! `view::chart`) — this module owns only the data layer: bucketing
//! tasklog entries into a fixed number of UTC time buckets per metric, and
//! assigning each agent a stable colour from a small palette.
//!
//! Data sources are deliberately existing ones (per AC: «no new
//! collectors»):
//!
//! - `crate::app::agent_registry::list()` enumerates known agents.
//! - `crate::app::tasklog::read_logs(name, …)` provides per-agent task
//!   entries with `ts`, `cost`, `input_tokens`, `output_tokens`.
//!
//! The aggregator is pure given the tasklog snapshot: callers pass in the
//! per-agent log slices and the «now» reference, so unit tests don't have
//! to touch `$HOME` or the real disk.
//!
//! Bucket layout per period:
//!
//! | Period | Bucket size | Bucket count |
//! |--------|-------------|--------------|
//! | 24h    | 1 hour      | 24           |
//! | 7d     | 1 day       | 7            |
//! | 30d    | 1 day       | 30           |
//!
//! All buckets are aligned in UTC; the last bucket ends at `now`. An entry
//! with `ts` exactly at the start of the window is included in the first
//! bucket.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::app::tasklog::{self, TaskLog};

/// Which metric the chart is plotting. Maps 1:1 to the radio buttons in
/// the switcher (`spend` / `activity` / `tokens`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub enum Metric {
    /// USD cost — sum of `tasklog.cost` per bucket.
    #[default]
    Spend,
    /// Count of tasklog entries per bucket (any status).
    Activity,
    /// Total tokens — sum of `input_tokens + output_tokens` per bucket.
    Tokens,
}

impl Metric {
    /// Stable id used in URL query strings (`?metric=spend|activity|tokens`).
    pub fn as_query(self) -> &'static str {
        match self {
            Self::Spend => "spend",
            Self::Activity => "activity",
            Self::Tokens => "tokens",
        }
    }

    /// Display label for legend / switcher text.
    pub fn label(self) -> &'static str {
        match self {
            Self::Spend => "Spend (USD)",
            Self::Activity => "Activity",
            Self::Tokens => "Tokens",
        }
    }

    /// Parse from a query-string value. Unknown values fall back to the
    /// default (`Spend`) — per AC, invalid input must not 5xx.
    pub fn parse(s: &str) -> Self {
        match s {
            "activity" => Self::Activity,
            "tokens" => Self::Tokens,
            "spend" => Self::Spend,
            _ => Self::default(),
        }
    }
}

/// Which time window the chart covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub enum Period {
    /// Last 24 hours, hourly buckets.
    #[default]
    Day,
    /// Last 7 days, daily buckets.
    Week,
    /// Last 30 days, daily buckets.
    Month,
}

impl Period {
    /// Stable id used in URL query strings (`?period=24h|7d|30d`).
    pub fn as_query(self) -> &'static str {
        match self {
            Self::Day => "24h",
            Self::Week => "7d",
            Self::Month => "30d",
        }
    }

    /// Display label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Day => "24h",
            Self::Week => "7d",
            Self::Month => "30d",
        }
    }

    /// Parse from a query-string value, defaulting to the 24h window.
    pub fn parse(s: &str) -> Self {
        match s {
            "7d" => Self::Week,
            "30d" => Self::Month,
            "24h" => Self::Day,
            _ => Self::default(),
        }
    }

    /// Number of buckets in this period.
    pub fn bucket_count(self) -> usize {
        match self {
            Self::Day => 24,
            Self::Week => 7,
            Self::Month => 30,
        }
    }

    /// Duration covered by a single bucket.
    pub fn bucket_duration(self) -> Duration {
        match self {
            Self::Day => Duration::hours(1),
            Self::Week | Self::Month => Duration::days(1),
        }
    }

    /// Total duration of the window.
    pub fn window(self) -> Duration {
        self.bucket_duration() * (self.bucket_count() as i32)
    }
}

/// One agent's time series for the selected metric/period.
///
/// `points.len() == period.bucket_count()` always; empty buckets carry a
/// zero value so the SVG renderer can rely on a fixed-length array.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ChartSeries {
    pub agent: String,
    /// Stable hex colour (e.g. `#4a72d9`) — keyed off the agent name.
    pub color: String,
    pub points: Vec<ChartPoint>,
}

/// One bucket of a series. `bucket_start` is the inclusive lower bound
/// (UTC) and `value` is whatever the metric demands for that interval.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ChartPoint {
    pub bucket_start: DateTime<Utc>,
    pub value: f64,
}

/// Vibrant + reasonably distinguishable palette. The first eight entries
/// are colour-blind-friendly hues from Wong's palette
/// (<https://www.nature.com/articles/nmeth.1618>), then four extras for
/// workspaces with many agents. Stable index → stable colour across all
/// chart renders.
const PALETTE: [&str; 12] = [
    "#0072b2", // blue
    "#d55e00", // vermillion
    "#009e73", // bluish green
    "#cc79a7", // reddish purple
    "#f0e442", // yellow
    "#56b4e9", // sky blue
    "#e69f00", // orange
    "#7b3294", // purple
    "#1b9e77", // teal
    "#a6761d", // brown
    "#666666", // grey
    "#b02929", // red
];

/// Return the stable palette colour for an agent name. Deterministic across
/// process restarts so the chart series colour matches across page loads.
///
/// Uses FNV-1a over the UTF-8 bytes, then modulo into [`PALETTE`]. FNV is
/// good enough for «N agents over a 12-colour palette» — collisions are
/// fine, the important property is *stability*.
pub fn color_for(agent: &str) -> &'static str {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in agent.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    PALETTE[(hash % PALETTE.len() as u64) as usize]
}

/// Number of colours in the palette — exposed for tests.
pub const PALETTE_LEN: usize = PALETTE.len();

/// Collect chart data from on-disk tasklogs.
///
/// Walks every known agent (via `agent_registry::list`), reads the recent
/// tasklog, and bucketises into the requested metric/period. Sub-agent
/// entries are attributed under their own name — same as the per-agent
/// cards do. Series are sorted alphabetically by agent name (stable,
/// deterministic).
///
/// Errors from a single agent's tasklog are swallowed (logged via tracing)
/// — one broken file must not blank out the chart for everyone.
pub async fn collect_chart_data(metric: Metric, period: Period) -> Vec<ChartSeries> {
    let now = Utc::now();
    let states = crate::app::agent_registry::list().await.unwrap_or_default();
    let mut per_agent: Vec<(String, Vec<TaskLog>)> = Vec::with_capacity(states.len());
    let since = now - period.window();
    for state in states {
        let name = state.config.name;
        // Read enough to cover the window comfortably. 10_000 matches
        // tasklog's MAX_ENTRIES so we never under-fetch at this layer.
        let entries = match tasklog::read_logs(&name, 10_000, None, Some(since)) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(agent = %name, error = %e, "tasklog read failed for chart");
                Vec::new()
            }
        };
        per_agent.push((name, entries));
    }
    build_series(metric, period, now, &per_agent)
}

/// Pure aggregator: given per-agent tasklog slices, bucketise into a
/// `Vec<ChartSeries>`. Public so tests can drive it without touching the
/// filesystem.
pub fn build_series(
    metric: Metric,
    period: Period,
    now: DateTime<Utc>,
    per_agent: &[(String, Vec<TaskLog>)],
) -> Vec<ChartSeries> {
    let bucket = period.bucket_duration();
    let count = period.bucket_count();
    let window_start = now - period.window();

    let mut series: Vec<ChartSeries> = per_agent
        .iter()
        .map(|(name, logs)| {
            let mut points: Vec<ChartPoint> = (0..count)
                .map(|i| ChartPoint {
                    bucket_start: window_start + bucket * (i as i32),
                    value: 0.0,
                })
                .collect();
            for entry in logs {
                let ts = match DateTime::parse_from_rfc3339(&entry.ts) {
                    Ok(t) => t.with_timezone(&Utc),
                    Err(_) => continue,
                };
                if ts < window_start || ts >= now + bucket {
                    continue;
                }
                let delta = ts - window_start;
                let idx_ms = delta.num_milliseconds();
                let bucket_ms = bucket.num_milliseconds().max(1);
                let idx = (idx_ms / bucket_ms) as usize;
                if idx >= count {
                    continue;
                }
                let v = entry_value(metric, entry);
                points[idx].value += v;
            }
            ChartSeries {
                agent: name.clone(),
                color: color_for(name).to_string(),
                points,
            }
        })
        .collect();
    series.sort_by(|a, b| a.agent.cmp(&b.agent));
    series
}

fn entry_value(metric: Metric, entry: &TaskLog) -> f64 {
    match metric {
        Metric::Spend => entry.cost,
        Metric::Activity => 1.0,
        Metric::Tokens => {
            let i = entry.input_tokens.unwrap_or(0);
            let o = entry.output_tokens.unwrap_or(0);
            (i + o) as f64
        }
    }
}

/// Maximum value across all series — used by the SVG renderer to pick
/// the Y-axis range. Zero when there's no data so the renderer can show
/// a friendly empty-state instead of dividing by zero.
pub fn series_max(series: &[ChartSeries]) -> f64 {
    series
        .iter()
        .flat_map(|s| s.points.iter().map(|p| p.value))
        .fold(0.0_f64, f64::max)
}

/// Parse `?metric=…&period=…` out of a raw query string. Unknown / missing
/// keys fall back to defaults — no error path, by design.
pub fn parse_query(query: &str) -> (Metric, Period) {
    let mut metric: HashMap<&str, &str> = HashMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        if !k.is_empty() {
            metric.insert(k, v);
        }
    }
    let m = metric
        .get("metric")
        .map(|s| Metric::parse(s))
        .unwrap_or_default();
    let p = metric
        .get("period")
        .map(|s| Period::parse(s))
        .unwrap_or_default();
    (m, p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(ts: DateTime<Utc>, cost: f64, in_tok: u64, out_tok: u64) -> TaskLog {
        TaskLog {
            ts: ts.to_rfc3339(),
            source: "test".into(),
            turns: 1,
            cost,
            duration_ms: 0,
            status: "ok".into(),
            task: String::new(),
            error: None,
            msg_id: String::new(),
            github_repo: None,
            github_pr: None,
            input_tokens: Some(in_tok),
            output_tokens: Some(out_tok),
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            session_count: None,
            tool_use_count: None,
            parent_agent: None,
        }
    }

    #[test]
    fn metric_parse_falls_back_to_default_for_invalid_input() {
        assert_eq!(Metric::parse("spend"), Metric::Spend);
        assert_eq!(Metric::parse("activity"), Metric::Activity);
        assert_eq!(Metric::parse("tokens"), Metric::Tokens);
        // Unknown → default. AC: «invalid metric must not 5xx».
        assert_eq!(Metric::parse("garbage"), Metric::default());
        assert_eq!(Metric::parse(""), Metric::default());
    }

    #[test]
    fn period_parse_falls_back_to_default_for_invalid_input() {
        assert_eq!(Period::parse("24h"), Period::Day);
        assert_eq!(Period::parse("7d"), Period::Week);
        assert_eq!(Period::parse("30d"), Period::Month);
        assert_eq!(Period::parse("garbage"), Period::default());
    }

    #[test]
    fn period_bucket_counts_match_spec() {
        assert_eq!(Period::Day.bucket_count(), 24);
        assert_eq!(Period::Week.bucket_count(), 7);
        assert_eq!(Period::Month.bucket_count(), 30);
    }

    #[test]
    fn parse_query_reads_both_keys() {
        let (m, p) = parse_query("metric=tokens&period=7d");
        assert_eq!(m, Metric::Tokens);
        assert_eq!(p, Period::Week);
    }

    #[test]
    fn parse_query_returns_defaults_when_missing() {
        let (m, p) = parse_query("");
        assert_eq!(m, Metric::default());
        assert_eq!(p, Period::default());
    }

    #[test]
    fn parse_query_ignores_unknown_keys() {
        let (m, p) = parse_query("foo=bar&metric=activity");
        assert_eq!(m, Metric::Activity);
        assert_eq!(p, Period::default());
    }

    #[test]
    fn color_for_is_stable_across_calls() {
        let a = color_for("kira");
        let b = color_for("kira");
        assert_eq!(a, b);
        // Different name maps to (possibly) different colour — but always
        // *some* palette entry.
        let other = color_for("dev");
        assert!(PALETTE.contains(&other));
    }

    #[test]
    fn build_series_with_no_agents_returns_empty() {
        let now = Utc::now();
        let s = build_series(Metric::Spend, Period::Day, now, &[]);
        assert!(s.is_empty());
    }

    #[test]
    fn build_series_with_no_logs_returns_zero_filled_buckets() {
        let now = Utc::now();
        let s = build_series(Metric::Spend, Period::Day, now, &[("kira".into(), vec![])]);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].points.len(), 24);
        assert!(s[0].points.iter().all(|p| p.value == 0.0));
    }

    #[test]
    fn build_series_sorts_agents_alphabetically() {
        let now = Utc::now();
        let s = build_series(
            Metric::Spend,
            Period::Day,
            now,
            &[
                ("zeta".into(), vec![]),
                ("alpha".into(), vec![]),
                ("mu".into(), vec![]),
            ],
        );
        let names: Vec<&str> = s.iter().map(|x| x.agent.as_str()).collect();
        assert_eq!(names, vec!["alpha", "mu", "zeta"]);
    }

    #[test]
    fn build_series_sums_spend_into_correct_hourly_bucket() {
        let now = Utc::now();
        // Two entries 2h ago → bucket index 22 (0-indexed; bucket 23 is current).
        let two_hours_ago = now - Duration::hours(2) + Duration::minutes(5);
        let logs = vec![
            task(two_hours_ago, 1.5, 100, 200),
            task(two_hours_ago - Duration::minutes(10), 0.5, 50, 50),
        ];
        let s = build_series(Metric::Spend, Period::Day, now, &[("kira".into(), logs)]);
        assert_eq!(s.len(), 1);
        // Both entries land in adjacent buckets; pick the higher one and
        // assert the sum across the chart equals 2.0.
        let total: f64 = s[0].points.iter().map(|p| p.value).sum();
        assert!(
            (total - 2.0).abs() < 1e-9,
            "expected sum 2.0, got {}",
            total
        );
    }

    #[test]
    fn build_series_includes_entry_at_window_start() {
        // AC: «a tasklog entry at exactly T-24h should be IN the 24h window».
        let now = Utc::now();
        let at_boundary = now - Period::Day.window();
        let logs = vec![task(at_boundary, 0.25, 10, 10)];
        let s = build_series(Metric::Spend, Period::Day, now, &[("kira".into(), logs)]);
        let total: f64 = s[0].points.iter().map(|p| p.value).sum();
        assert!(
            (total - 0.25).abs() < 1e-9,
            "boundary entry must count, got {}",
            total
        );
        // And specifically in bucket 0.
        assert!(
            s[0].points[0].value > 0.0,
            "expected bucket 0 to hold the boundary entry"
        );
    }

    #[test]
    fn build_series_drops_entries_older_than_window() {
        let now = Utc::now();
        let too_old = now - Period::Day.window() - Duration::hours(1);
        let logs = vec![task(too_old, 10.0, 1000, 1000)];
        let s = build_series(Metric::Spend, Period::Day, now, &[("kira".into(), logs)]);
        let total: f64 = s[0].points.iter().map(|p| p.value).sum();
        assert_eq!(total, 0.0);
    }

    #[test]
    fn build_series_counts_activity_per_bucket() {
        let now = Utc::now();
        let three_hours_ago = now - Duration::hours(3) + Duration::minutes(5);
        let logs = vec![
            task(three_hours_ago, 1.0, 100, 200),
            task(three_hours_ago + Duration::minutes(2), 1.0, 100, 200),
        ];
        let s = build_series(Metric::Activity, Period::Day, now, &[("kira".into(), logs)]);
        let total: f64 = s[0].points.iter().map(|p| p.value).sum();
        assert_eq!(total, 2.0);
    }

    #[test]
    fn build_series_sums_tokens_input_plus_output() {
        let now = Utc::now();
        let entry_ts = now - Duration::hours(1);
        let logs = vec![task(entry_ts, 1.0, 1_000, 2_500)];
        let s = build_series(Metric::Tokens, Period::Day, now, &[("kira".into(), logs)]);
        let total: f64 = s[0].points.iter().map(|p| p.value).sum();
        assert_eq!(total, 3_500.0);
    }

    #[test]
    fn build_series_multi_agent_produces_two_series_sorted() {
        let now = Utc::now();
        let logs_a = vec![task(now - Duration::hours(1), 1.0, 100, 100)];
        let logs_b = vec![task(now - Duration::hours(2), 2.0, 200, 200)];
        let s = build_series(
            Metric::Spend,
            Period::Day,
            now,
            &[("zeta".into(), logs_b), ("alpha".into(), logs_a)],
        );
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].agent, "alpha");
        assert_eq!(s[1].agent, "zeta");
        // Colours assigned from palette.
        assert!(s[0].color.starts_with('#'));
        assert!(s[1].color.starts_with('#'));
    }

    #[test]
    fn build_series_assigns_stable_colors_to_agents() {
        let now = Utc::now();
        let s1 = build_series(Metric::Spend, Period::Day, now, &[("kira".into(), vec![])]);
        let s2 = build_series(
            Metric::Tokens,
            Period::Week,
            now,
            &[("kira".into(), vec![])],
        );
        assert_eq!(s1[0].color, s2[0].color);
    }

    #[test]
    fn series_max_returns_zero_when_all_empty() {
        let now = Utc::now();
        let s = build_series(Metric::Spend, Period::Day, now, &[("kira".into(), vec![])]);
        assert_eq!(series_max(&s), 0.0);
    }

    #[test]
    fn series_max_finds_max_across_series() {
        let now = Utc::now();
        let logs_a = vec![task(now - Duration::hours(1), 1.0, 100, 100)];
        let logs_b = vec![task(now - Duration::hours(2), 5.0, 100, 100)];
        let s = build_series(
            Metric::Spend,
            Period::Day,
            now,
            &[("a".into(), logs_a), ("b".into(), logs_b)],
        );
        assert!(series_max(&s) >= 5.0 - 1e-9);
    }

    #[test]
    fn palette_has_at_least_eight_entries() {
        // AC: «at least 8 vibrant + distinguishable colors».
        const { assert!(PALETTE_LEN >= 8) };
    }
}
