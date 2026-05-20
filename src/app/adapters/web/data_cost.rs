//! Cost & pipeline dashboard data collection (#473).
//!
//! Pulls open + recently-closed `agent-ready` issues from every repo
//! configured under `cost.repos`, parses the `est:<S|M|L|XL>` label, and
//! correlates against the local `TaskLog` to surface actual token spend
//! per closed ticket. The result feeds both the server-rendered HTML view
//! (`view::cost`) and the SSE diff feed (`routes::cost_feed`).
//!
//! ## Caching
//!
//! `gh issue list` is a network round-trip per repo, so we cache the raw
//! per-repo result inside [`CostCache`] for 60 seconds. The dashboard route
//! reads from this cache on every request; the SSE feed re-fetches at most
//! once per minute. Both the browser refresh button and the SSE tick share
//! the same cache, so dashboards with multiple tabs don't multiply the
//! upstream load.
//!
//! ## `actual_tokens`
//!
//! The issue spec calls out a future per-ticket `actual_tokens` field on
//! the deskd task record that is populated by a separate spend collector.
//! Until that lands we attempt a best-effort scan of every agent's
//! `tasks.jsonl` for entries tagged with `github_repo` + `github_pr` /
//! `github_issue` matching the ticket. If nothing matches the renderer
//! shows «tracking pending» per the AC.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::app::tasklog;
use crate::config::{CostBuckets, CostConfig};

/// Default cache TTL for `gh issue list` results (60s — see issue spec).
pub const CACHE_TTL: Duration = Duration::from_secs(60);

/// Hard cap for how many issues `gh issue list` returns per repo.
const GH_PAGE_LIMIT: usize = 100;

/// One open ticket on the «Pipeline» panel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PipelineTicket {
    pub repo: String,
    pub number: u64,
    pub title: String,
    /// Resolved bucket label (`S`, `M`, `L`, `XL`) or `None` when no
    /// `est:*` label is set.
    pub estimate_bucket: Option<String>,
    /// Token estimate derived from `estimate_bucket` + `CostBuckets`.
    pub estimate_tokens: Option<u64>,
    pub created_at: Option<DateTime<Utc>>,
}

/// One closed ticket on the «History» panel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HistoryTicket {
    pub repo: String,
    pub number: u64,
    pub title: String,
    pub estimate_bucket: Option<String>,
    pub estimate_tokens: Option<u64>,
    /// Actual tokens used to close the ticket. `None` → «tracking pending».
    pub actual_tokens: Option<u64>,
    pub closed_at: Option<DateTime<Utc>>,
}

impl HistoryTicket {
    /// Ratio `actual / estimate` — `None` when either side is missing or
    /// the estimate is zero.
    pub fn ratio(&self) -> Option<f64> {
        match (self.actual_tokens, self.estimate_tokens) {
            (Some(actual), Some(estimate)) if estimate > 0 => Some(actual as f64 / estimate as f64),
            _ => None,
        }
    }

    /// Signed delta `actual - estimate` in tokens.
    pub fn delta_tokens(&self) -> Option<i64> {
        match (self.actual_tokens, self.estimate_tokens) {
            (Some(actual), Some(estimate)) => Some(actual as i64 - estimate as i64),
            _ => None,
        }
    }
}

/// Aggregate payload — everything `/dashboard/cost` renders.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct CostData {
    pub pipeline: Vec<PipelineTicket>,
    pub history: Vec<HistoryTicket>,
    /// Tokens consumed over the last 7 days from `actual_tokens`. Used by
    /// the budget bar when `weekly_ceiling` is configured.
    pub weekly_total_tokens: u64,
    /// Generated-at timestamp (best-effort, derived from the most recent
    /// per-repo fetch).
    pub fetched_at: DateTime<Utc>,
}

/// Trait around `gh issue list`. Production wires `CommandGhClient`; tests
/// inject a `RecordingGhClient`.
#[async_trait]
pub trait GhClient: Send + Sync + 'static {
    /// List open issues with the given label. The implementation must
    /// return at most [`GH_PAGE_LIMIT`] entries; pagination is intentionally
    /// not handled here — the dashboard wants a recent slice, not the
    /// historical tail.
    async fn list_open_with_label(&self, repo: &str, label: &str) -> anyhow::Result<Vec<GhIssue>>;

    /// List issues closed since `since` (RFC 3339). When `label` is
    /// non-empty, filter on it; otherwise no label filter.
    async fn list_closed_since(
        &self,
        repo: &str,
        label: &str,
        since: DateTime<Utc>,
    ) -> anyhow::Result<Vec<GhIssue>>;
}

/// One issue row in the gh API response, normalised to the subset we
/// actually use. Public so tests can construct fixtures.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GhIssue {
    pub number: u64,
    pub title: String,
    /// All labels on the issue. The `est:*` parser walks this list.
    pub labels: Vec<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
}

/// Production `GhClient` — shells out to `gh issue list ... --json ...`.
pub struct CommandGhClient {
    pub gh_binary: String,
}

impl Default for CommandGhClient {
    fn default() -> Self {
        Self {
            gh_binary: "gh".to_string(),
        }
    }
}

#[async_trait]
impl GhClient for CommandGhClient {
    async fn list_open_with_label(&self, repo: &str, label: &str) -> anyhow::Result<Vec<GhIssue>> {
        let limit_str = GH_PAGE_LIMIT.to_string();
        let mut args = vec![
            "issue",
            "list",
            "--repo",
            repo,
            "--state",
            "open",
            "--limit",
            limit_str.as_str(),
            "--json",
            "number,title,labels,createdAt,closedAt",
        ];
        if !label.is_empty() {
            args.push("--label");
            args.push(label);
        }
        run_gh(&self.gh_binary, &args).await
    }

    async fn list_closed_since(
        &self,
        repo: &str,
        label: &str,
        since: DateTime<Utc>,
    ) -> anyhow::Result<Vec<GhIssue>> {
        // gh CLI doesn't accept a --since flag for `issue list`. Use a
        // generous limit + post-filter; the history panel only needs a
        // few dozen rows.
        let limit_str = GH_PAGE_LIMIT.to_string();
        let mut args = vec![
            "issue",
            "list",
            "--repo",
            repo,
            "--state",
            "closed",
            "--limit",
            limit_str.as_str(),
            "--json",
            "number,title,labels,createdAt,closedAt",
        ];
        if !label.is_empty() {
            args.push("--label");
            args.push(label);
        }
        let all = run_gh(&self.gh_binary, &args).await?;
        Ok(all
            .into_iter()
            .filter(|i| i.closed_at.map(|t| t >= since).unwrap_or(false))
            .collect())
    }
}

async fn run_gh(bin: &str, args: &[&str]) -> anyhow::Result<Vec<GhIssue>> {
    let output = tokio::process::Command::new(bin)
        .args(args)
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("failed to spawn `{} {}`: {}", bin, args.join(" "), e))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "`{} {}` failed (status {}): {}",
            bin,
            args.join(" "),
            output.status,
            stderr.trim()
        );
    }
    parse_gh_issue_list(&output.stdout)
}

/// Parse `gh issue list --json …` output. Public for tests so a fixture
/// JSON can be compared against the production parser.
pub fn parse_gh_issue_list(json_bytes: &[u8]) -> anyhow::Result<Vec<GhIssue>> {
    #[derive(Deserialize)]
    struct RawLabel {
        name: String,
    }
    #[derive(Deserialize)]
    struct RawIssue {
        number: u64,
        title: String,
        #[serde(default)]
        labels: Vec<RawLabel>,
        #[serde(default, rename = "createdAt")]
        created_at: Option<String>,
        #[serde(default, rename = "closedAt")]
        closed_at: Option<String>,
    }
    let raw: Vec<RawIssue> = serde_json::from_slice(json_bytes)
        .map_err(|e| anyhow::anyhow!("failed to parse gh issue list output: {}", e))?;
    Ok(raw
        .into_iter()
        .map(|r| GhIssue {
            number: r.number,
            title: r.title,
            labels: r.labels.into_iter().map(|l| l.name).collect(),
            created_at: r
                .created_at
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|d| d.with_timezone(&Utc)),
            closed_at: r
                .closed_at
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|d| d.with_timezone(&Utc)),
        })
        .collect())
}

/// Extract the first `est:<bucket>` label from an issue's label list.
/// Returns the bucket string (e.g. `"M"`) — case-insensitive on the
/// bucket portion, case-sensitive on the prefix to avoid accidental
/// matches on labels like `Estimate:foo`.
pub fn extract_estimate_label(labels: &[String]) -> Option<String> {
    for lbl in labels {
        if let Some(stripped) = lbl.strip_prefix("est:") {
            let bucket = stripped.trim().to_ascii_uppercase();
            if matches!(bucket.as_str(), "S" | "M" | "L" | "XL") {
                return Some(bucket);
            }
        }
    }
    None
}

/// Tokens consumed for ticket `repo#number` according to the local task
/// log. Best-effort: returns `None` when no matching entry is found.
///
/// The lookup scans every per-agent `tasks.jsonl` under
/// `~/.deskd/logs/<agent>/`, accumulating `input_tokens + output_tokens`
/// across entries tagged with `github_repo == repo` and either
/// `github_pr == number` or the ticket number embedded in `msg_id` /
/// `task`. The accumulator handles the common case where a ticket
/// triggers multiple Claude invocations.
pub fn lookup_actual_tokens(repo: &str, number: u64) -> Option<u64> {
    let logs_dir = std::env::var("HOME")
        .ok()
        .map(|h| std::path::PathBuf::from(h).join(".deskd").join("logs"))?;
    let read_dir = std::fs::read_dir(&logs_dir).ok()?;
    let mut total: u64 = 0;
    let mut any_match = false;
    for entry in read_dir.flatten() {
        let path = entry.path().join("tasks.jsonl");
        if !path.exists() {
            continue;
        }
        let Ok(logs) = read_tasks_jsonl(&path) else {
            continue;
        };
        for log in logs {
            if matches_ticket(&log, repo, number) {
                any_match = true;
                let input = log.input_tokens.unwrap_or(0);
                let output = log.output_tokens.unwrap_or(0);
                total = total.saturating_add(input).saturating_add(output);
            }
        }
    }
    if any_match { Some(total) } else { None }
}

fn read_tasks_jsonl(path: &std::path::Path) -> std::io::Result<Vec<tasklog::TaskLog>> {
    use std::io::BufRead;
    let f = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(f);
    let mut out = Vec::new();
    for line in reader.lines().map_while(Result::ok) {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(log) = serde_json::from_str::<tasklog::TaskLog>(&line) {
            out.push(log);
        }
    }
    Ok(out)
}

fn matches_ticket(log: &tasklog::TaskLog, repo: &str, number: u64) -> bool {
    if log.github_repo.as_deref() == Some(repo) {
        if log.github_pr == Some(number) {
            return true;
        }
        // Fallback: when github_pr is unset (issue, not PR), some flows
        // embed the issue number in the task text or msg_id.
        let needle = format!("#{}", number);
        if log.task.contains(&needle) || log.msg_id.contains(&needle) {
            return true;
        }
    }
    false
}

/// In-memory per-repo cache. Behind `Arc<Mutex<>>` so the SSE feed, the
/// dashboard handler, and the manual refresh button share one source of
/// truth across concurrent requests.
#[derive(Debug, Clone, Default)]
pub struct CostCache {
    inner: Arc<Mutex<CostCacheInner>>,
}

#[derive(Debug, Default)]
struct CostCacheInner {
    open: HashMap<String, CachedEntry<Vec<GhIssue>>>,
    closed: HashMap<String, CachedEntry<Vec<GhIssue>>>,
}

#[derive(Debug, Clone)]
struct CachedEntry<T> {
    value: T,
    /// Wall-clock instant the value was fetched.
    fetched_at: Instant,
}

impl CostCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch open issues for `repo`, hitting the cache when fresh.
    pub async fn get_open(
        &self,
        repo: &str,
        label: &str,
        gh: &dyn GhClient,
        ttl: Duration,
        now: Instant,
    ) -> anyhow::Result<Vec<GhIssue>> {
        let key = open_cache_key(repo, label);
        {
            let guard = self.inner.lock().await;
            if let Some(entry) = guard.open.get(&key)
                && now.saturating_duration_since(entry.fetched_at) < ttl
            {
                return Ok(entry.value.clone());
            }
        }
        let fresh = gh.list_open_with_label(repo, label).await?;
        let mut guard = self.inner.lock().await;
        guard.open.insert(
            key,
            CachedEntry {
                value: fresh.clone(),
                fetched_at: now,
            },
        );
        Ok(fresh)
    }

    /// Fetch closed issues for `repo` since `since`, hitting the cache when
    /// fresh. Cache key includes the `since` so a config change in
    /// `history_days` invalidates naturally.
    pub async fn get_closed(
        &self,
        repo: &str,
        label: &str,
        since: DateTime<Utc>,
        gh: &dyn GhClient,
        ttl: Duration,
        now: Instant,
    ) -> anyhow::Result<Vec<GhIssue>> {
        let key = closed_cache_key(repo, label, since);
        {
            let guard = self.inner.lock().await;
            if let Some(entry) = guard.closed.get(&key)
                && now.saturating_duration_since(entry.fetched_at) < ttl
            {
                return Ok(entry.value.clone());
            }
        }
        let fresh = gh.list_closed_since(repo, label, since).await?;
        let mut guard = self.inner.lock().await;
        guard.closed.insert(
            key,
            CachedEntry {
                value: fresh.clone(),
                fetched_at: now,
            },
        );
        Ok(fresh)
    }
}

fn open_cache_key(repo: &str, label: &str) -> String {
    format!("open::{}::{}", repo, label)
}

fn closed_cache_key(repo: &str, label: &str, since: DateTime<Utc>) -> String {
    format!("closed::{}::{}::{}", repo, label, since.timestamp())
}

/// Sole entry-point for the cost dashboard view layer. Walks every
/// configured repo, hydrates pipeline + history, computes the weekly
/// total, and returns the aggregate payload.
///
/// Errors from individual repos are logged via `tracing::warn!` and the
/// repo is skipped — a single down upstream cannot blank the entire
/// dashboard.
pub async fn collect_cost_data(
    cfg: &CostConfig,
    gh: &dyn GhClient,
    cache: &CostCache,
    now: Instant,
    actual_lookup: &(dyn Fn(&str, u64) -> Option<u64> + Send + Sync),
) -> CostData {
    let utc_now = Utc::now();
    let since = utc_now - chrono::Duration::days(cfg.history_days.max(1) as i64);

    let mut pipeline: Vec<PipelineTicket> = Vec::new();
    let mut history: Vec<HistoryTicket> = Vec::new();

    for repo in &cfg.repos {
        match cache
            .get_open(repo, &cfg.ready_label, gh, CACHE_TTL, now)
            .await
        {
            Ok(open) => {
                for issue in open {
                    pipeline.push(to_pipeline_ticket(repo, &issue, &cfg.buckets));
                }
            }
            Err(e) => tracing::warn!(repo = %repo, error = %e, "cost.pipeline.fetch_failed"),
        }
        match cache
            .get_closed(repo, &cfg.ready_label, since, gh, CACHE_TTL, now)
            .await
        {
            Ok(closed) => {
                for issue in closed {
                    let actual = actual_lookup(repo, issue.number);
                    history.push(to_history_ticket(repo, &issue, &cfg.buckets, actual));
                }
            }
            Err(e) => tracing::warn!(repo = %repo, error = %e, "cost.history.fetch_failed"),
        }
    }

    pipeline.sort_by_key(|p| std::cmp::Reverse(p.created_at));
    history.sort_by_key(|h| std::cmp::Reverse(h.closed_at));

    let weekly_cutoff = utc_now - chrono::Duration::days(7);
    let weekly_total_tokens: u64 = history
        .iter()
        .filter(|h| h.closed_at.map(|t| t >= weekly_cutoff).unwrap_or(false))
        .filter_map(|h| h.actual_tokens)
        .sum();

    CostData {
        pipeline,
        history,
        weekly_total_tokens,
        fetched_at: utc_now,
    }
}

fn to_pipeline_ticket(repo: &str, issue: &GhIssue, buckets: &CostBuckets) -> PipelineTicket {
    let bucket = extract_estimate_label(&issue.labels);
    let estimate_tokens = bucket.as_deref().and_then(|b| buckets.lookup(b));
    PipelineTicket {
        repo: repo.to_string(),
        number: issue.number,
        title: issue.title.clone(),
        estimate_bucket: bucket,
        estimate_tokens,
        created_at: issue.created_at,
    }
}

fn to_history_ticket(
    repo: &str,
    issue: &GhIssue,
    buckets: &CostBuckets,
    actual_tokens: Option<u64>,
) -> HistoryTicket {
    let bucket = extract_estimate_label(&issue.labels);
    let estimate_tokens = bucket.as_deref().and_then(|b| buckets.lookup(b));
    HistoryTicket {
        repo: repo.to_string(),
        number: issue.number,
        title: issue.title.clone(),
        estimate_bucket: bucket,
        estimate_tokens,
        actual_tokens,
        closed_at: issue.closed_at,
    }
}

/// Testing-friendly recording double for [`GhClient`]. Public so integration
/// tests under `tests/` can wire it into [`WebState`] without rebuilding the
/// trait impl.
pub mod testing {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// In-memory `GhClient` that returns canned responses keyed by repo.
    /// Records call counts so cache-hit assertions are trivial.
    #[derive(Default, Clone)]
    pub struct RecordingGhClient {
        inner: Arc<Mutex<RecordingInner>>,
    }

    #[derive(Default)]
    struct RecordingInner {
        open: HashMap<String, Vec<GhIssue>>,
        closed: HashMap<String, Vec<GhIssue>>,
        open_calls: usize,
        closed_calls: usize,
    }

    impl RecordingGhClient {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_open(self, repo: &str, issues: Vec<GhIssue>) -> Self {
            self.inner
                .lock()
                .unwrap()
                .open
                .insert(repo.to_string(), issues);
            self
        }

        pub fn with_closed(self, repo: &str, issues: Vec<GhIssue>) -> Self {
            self.inner
                .lock()
                .unwrap()
                .closed
                .insert(repo.to_string(), issues);
            self
        }

        pub fn open_call_count(&self) -> usize {
            self.inner.lock().unwrap().open_calls
        }

        pub fn closed_call_count(&self) -> usize {
            self.inner.lock().unwrap().closed_calls
        }
    }

    #[async_trait]
    impl GhClient for RecordingGhClient {
        async fn list_open_with_label(
            &self,
            repo: &str,
            _label: &str,
        ) -> anyhow::Result<Vec<GhIssue>> {
            let mut guard = self.inner.lock().unwrap();
            guard.open_calls += 1;
            Ok(guard.open.get(repo).cloned().unwrap_or_default())
        }

        async fn list_closed_since(
            &self,
            repo: &str,
            _label: &str,
            _since: DateTime<Utc>,
        ) -> anyhow::Result<Vec<GhIssue>> {
            let mut guard = self.inner.lock().unwrap();
            guard.closed_calls += 1;
            Ok(guard.closed.get(repo).cloned().unwrap_or_default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::RecordingGhClient;
    use super::*;

    fn default_cfg(repos: Vec<&str>) -> CostConfig {
        CostConfig {
            buckets: CostBuckets {
                s: 10_000,
                m: 50_000,
                l: 200_000,
                xl: 500_000,
            },
            weekly_ceiling: None,
            history_days: 7,
            repos: repos.into_iter().map(|s| s.to_string()).collect(),
            ready_label: "agent-ready".to_string(),
        }
    }

    fn issue(number: u64, title: &str, labels: &[&str]) -> GhIssue {
        GhIssue {
            number,
            title: title.to_string(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            created_at: Some(Utc::now()),
            closed_at: None,
        }
    }

    #[test]
    fn extracts_est_bucket_label() {
        assert_eq!(
            extract_estimate_label(&["est:M".into(), "bug".into()]),
            Some("M".to_string())
        );
        assert_eq!(
            extract_estimate_label(&["bug".into(), "est:xl".into()]),
            Some("XL".to_string())
        );
        assert_eq!(extract_estimate_label(&["bug".into()]), None);
        // Reject unknown buckets so the renderer surfaces "unknown".
        assert_eq!(extract_estimate_label(&["est:bogus".into()]), None);
    }

    #[test]
    fn history_ticket_ratio_and_delta() {
        let h = HistoryTicket {
            repo: "x/y".into(),
            number: 1,
            title: "t".into(),
            estimate_bucket: Some("M".into()),
            estimate_tokens: Some(50_000),
            actual_tokens: Some(60_000),
            closed_at: None,
        };
        assert!((h.ratio().unwrap() - 1.2).abs() < 1e-9);
        assert_eq!(h.delta_tokens(), Some(10_000));
    }

    #[test]
    fn history_ticket_ratio_none_when_missing() {
        let h = HistoryTicket {
            repo: "x/y".into(),
            number: 1,
            title: "t".into(),
            estimate_bucket: None,
            estimate_tokens: None,
            actual_tokens: Some(60_000),
            closed_at: None,
        };
        assert!(h.ratio().is_none());
        assert!(h.delta_tokens().is_none());
    }

    #[test]
    fn parse_gh_issue_list_handles_full_payload() {
        let json = br#"[
            {
                "number": 42,
                "title": "feat: dashboard",
                "labels": [{"name": "agent-ready"}, {"name": "est:L"}],
                "createdAt": "2026-05-01T12:00:00Z",
                "closedAt": null
            }
        ]"#;
        let parsed = parse_gh_issue_list(json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].number, 42);
        assert_eq!(parsed[0].labels, vec!["agent-ready", "est:L"]);
        assert!(parsed[0].created_at.is_some());
        assert!(parsed[0].closed_at.is_none());
    }

    #[tokio::test]
    async fn collect_pipeline_attaches_estimate_tokens() {
        let cfg = default_cfg(vec!["a/b"]);
        let gh = RecordingGhClient::new().with_open(
            "a/b",
            vec![
                issue(1, "small", &["est:S", "agent-ready"]),
                issue(2, "medium", &["est:M", "agent-ready"]),
                issue(3, "no est", &["agent-ready"]),
            ],
        );
        let cache = CostCache::new();
        let data = collect_cost_data(&cfg, &gh, &cache, Instant::now(), &|_, _| None).await;
        assert_eq!(data.pipeline.len(), 3);
        let by_n: HashMap<u64, &PipelineTicket> =
            data.pipeline.iter().map(|p| (p.number, p)).collect();
        assert_eq!(by_n[&1].estimate_tokens, Some(10_000));
        assert_eq!(by_n[&2].estimate_tokens, Some(50_000));
        assert_eq!(by_n[&3].estimate_tokens, None);
        assert_eq!(by_n[&3].estimate_bucket, None);
    }

    #[tokio::test]
    async fn collect_history_renders_tracking_pending_when_actual_missing() {
        let cfg = default_cfg(vec!["a/b"]);
        let mut closed = issue(7, "closed-no-actual", &["agent-ready", "est:M"]);
        closed.closed_at = Some(Utc::now() - chrono::Duration::hours(2));
        let gh = RecordingGhClient::new().with_closed("a/b", vec![closed]);
        let cache = CostCache::new();
        let data = collect_cost_data(&cfg, &gh, &cache, Instant::now(), &|_, _| None).await;
        assert_eq!(data.history.len(), 1);
        assert!(data.history[0].actual_tokens.is_none());
        assert_eq!(data.history[0].estimate_tokens, Some(50_000));
    }

    #[tokio::test]
    async fn cache_serves_second_call_without_hitting_gh() {
        let cfg = default_cfg(vec!["a/b"]);
        let gh = RecordingGhClient::new().with_open("a/b", vec![issue(1, "t", &["agent-ready"])]);
        let cache = CostCache::new();
        let now = Instant::now();
        let _ = collect_cost_data(&cfg, &gh, &cache, now, &|_, _| None).await;
        let _ = collect_cost_data(&cfg, &gh, &cache, now, &|_, _| None).await;
        // Two collect_cost_data calls in the same TTL window must only
        // hit gh once per (repo, query-type).
        assert_eq!(gh.open_call_count(), 1);
        assert_eq!(gh.closed_call_count(), 1);
    }

    #[tokio::test]
    async fn cache_refetches_after_ttl_expires() {
        let cfg = default_cfg(vec!["a/b"]);
        let gh = RecordingGhClient::new().with_open("a/b", vec![issue(1, "t", &["agent-ready"])]);
        let cache = CostCache::new();
        let now = Instant::now();
        let _ = collect_cost_data(&cfg, &gh, &cache, now, &|_, _| None).await;
        let later = now + CACHE_TTL + Duration::from_secs(1);
        let _ = collect_cost_data(&cfg, &gh, &cache, later, &|_, _| None).await;
        assert_eq!(gh.open_call_count(), 2);
    }

    #[tokio::test]
    async fn weekly_total_aggregates_recent_actuals() {
        let cfg = default_cfg(vec!["a/b"]);
        let mut recent = issue(1, "recent", &["agent-ready", "est:M"]);
        recent.closed_at = Some(Utc::now() - chrono::Duration::hours(6));
        let mut old = issue(2, "old", &["agent-ready", "est:M"]);
        old.closed_at = Some(Utc::now() - chrono::Duration::days(30));
        // The pre-filter inside `list_closed_since` would normally drop
        // the old issue, but RecordingGhClient returns whatever we hand
        // it. The weekly_total filter must still discard it.
        let gh = RecordingGhClient::new().with_closed("a/b", vec![recent, old]);
        let cache = CostCache::new();
        let data = collect_cost_data(&cfg, &gh, &cache, Instant::now(), &|_, n| {
            Some(if n == 1 { 60_000 } else { 999_999 })
        })
        .await;
        assert_eq!(data.weekly_total_tokens, 60_000);
    }
}
