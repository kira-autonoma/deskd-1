//! Remote tmux session discovery (#453).
//!
//! Cross-references the tmux launcher conventions from #452:
//!
//! - Session naming: `deskd-<agent>` (see [`crate::app::tmux_launcher::session_name_for`]).
//! - Log file: `/var/log/deskd/sessions/<agent>.log` on the remote
//!   (or the fallback chain documented on
//!   [`crate::app::tmux_launcher::DEFAULT_SYSTEM_LOG_DIR`]).
//!
//! The discovery model is pull-based: we fan out a single
//! `tmux ls -F …` invocation per location (local + each remote), parse
//! the output, merge, and present a unified table. SSH parallelism is
//! capped per-call by `tokio::time::timeout` (5 seconds) so a slow or
//! unreachable host can't stall the whole listing — the slow remote
//! surfaces as a single row with `STATUS=unreachable` (or `error: …`),
//! never silently dropped.
//!
//! ## Testing strategy
//!
//! Real SSH is intentionally out of scope for the CI — `tokio::process`
//! spawning `ssh` is verified by hand during PR review. Code that talks
//! to the network goes behind the [`SshExecutor`] trait so unit tests
//! can substitute [`MockSshExecutor`] with canned outputs.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::process::Command as TokioCommand;

use crate::app::config_remotes::{RemoteEntry, RemotesConfig};

/// Per-remote SSH timeout. A slow or unreachable host yields an
/// unreachable row instead of blocking the entire listing.
pub const DEFAULT_REMOTE_TIMEOUT: Duration = Duration::from_secs(5);

/// tmux `-F` format string used for `tmux ls`. Single tab between
/// fields, one row per session. Picked to be cheap-to-parse and survive
/// shell quoting via single-quote escape inside `ssh '…'`.
///
/// Fields: name, attached (`attached`/`detached`), last activity
/// (unix epoch seconds), creation (unix epoch seconds).
pub const TMUX_LS_FORMAT: &str = "#{session_name}\t#{?session_attached,attached,detached}\t#{session_activity}\t#{session_created}";

/// Location of a discovered session: local host or one named remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "kind", content = "name")]
pub enum Location {
    Local,
    Remote(String),
}

impl Location {
    pub fn as_label(&self) -> &str {
        match self {
            Location::Local => "local",
            Location::Remote(n) => n.as_str(),
        }
    }
}

/// Per-location discovery status. Distinguishes "no sessions" from
/// "couldn't reach the host" so the table never silently hides a
/// remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "kind", content = "detail")]
pub enum LocationStatus {
    /// Listed successfully (zero or more sessions).
    Ok,
    /// `ssh` exited non-zero or connection timed out / refused.
    Unreachable(String),
    /// Other error — tmux missing on the remote, malformed output, …
    Error(String),
}

/// One row in the discovery table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEntry {
    pub location: Location,
    pub session: String,
    /// Agent name, derived from the `deskd-` prefix.
    pub agent: String,
    pub attached: bool,
    /// Number of clients attached (always 0 or 1 with tmux's default
    /// `#{?session_attached,…}` boolean; surfaced for forward-compat
    /// when we switch to `#{session_attached}` raw count).
    pub attached_count: u32,
    /// Last activity (unix epoch seconds). `0` if unknown.
    pub activity_epoch: i64,
    /// Creation time (unix epoch seconds). `0` if unknown.
    pub created_epoch: i64,
}

/// The full discovery result: one entry per session row + a status per
/// location so the renderer can surface unreachable hosts as their own
/// row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryResult {
    pub entries: Vec<SessionEntry>,
    pub location_status: BTreeMap<String, LocationStatus>,
}

impl DiscoveryResult {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            location_status: BTreeMap::new(),
        }
    }

    /// Find all entries whose `agent` field matches `agent`, optionally
    /// restricted to `location`.
    pub fn find_agent(&self, agent: &str, location: Option<&str>) -> Vec<&SessionEntry> {
        self.entries
            .iter()
            .filter(|e| e.agent == agent)
            .filter(|e| match location {
                Some(loc) => match &e.location {
                    Location::Local => loc == "local",
                    Location::Remote(n) => n.as_str() == loc,
                },
                None => true,
            })
            .collect()
    }
}

impl Default for DiscoveryResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Pluggable executor for tmux invocations. The real implementation
/// uses `tokio::process::Command`; tests use [`MockSshExecutor`] so we
/// don't shell out during `cargo test`.
#[async_trait]
pub trait SshExecutor: Send + Sync {
    /// Run `tmux ls -F <format>` locally and return stdout.
    async fn run_local_tmux_ls(&self) -> Result<String>;

    /// Run `tmux ls -F <format>` over SSH against `entry`.
    async fn run_remote_tmux_ls(&self, entry: &RemoteEntry) -> Result<String>;
}

/// Default executor — actually spawns `tmux` / `ssh`.
pub struct RealSshExecutor;

#[async_trait]
impl SshExecutor for RealSshExecutor {
    async fn run_local_tmux_ls(&self) -> Result<String> {
        let out = TokioCommand::new("tmux")
            .args(["list-sessions", "-F", TMUX_LS_FORMAT])
            .output()
            .await
            .map_err(|e| anyhow!("failed to invoke `tmux list-sessions`: {}", e))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            // tmux exits 1 with "no server running" when no sessions
            // exist at all; treat that as empty output, same as the
            // local list_tmux_sessions helper in tmux_launcher.
            if stderr.contains("no server running") || stderr.contains("no sessions") {
                return Ok(String::new());
            }
            return Err(anyhow!("tmux ls failed: {}", stderr.trim()));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    async fn run_remote_tmux_ls(&self, entry: &RemoteEntry) -> Result<String> {
        // Quote the format with single quotes — tmux's `-F` value has
        // `#{…}` which the remote shell mustn't interpret. Embedded
        // single quotes in the format string are escaped via the
        // standard `'\''` trick.
        let remote_cmd = format!(
            "tmux list-sessions -F '{}' 2>&1 || true",
            TMUX_LS_FORMAT.replace('\'', "'\\''")
        );
        let mut cmd = TokioCommand::new("ssh");
        // `-T`: no pseudo-terminal — pure non-interactive listing.
        // `-o BatchMode=yes`: never prompt for password / passphrase.
        // `-o ConnectTimeout=4`: fail fast on dead hosts (the outer
        //  `tokio::time::timeout` is a hard cap of 5s).
        cmd.arg("-T")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=4");
        for opt in &entry.ssh_options {
            cmd.arg(opt);
        }
        cmd.arg(&entry.host);
        cmd.arg(&remote_cmd);

        let out = cmd
            .output()
            .await
            .map_err(|e| anyhow!("failed to invoke ssh: {}", e))?;
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            return Err(anyhow!("ssh {} failed: {}", entry.host, stderr.trim()));
        }
        // Filter the "no server running" / "no sessions" sentinel that
        // tmux writes to stderr (we redirect 2>&1 in the remote
        // command). Lines that don't match our `-F` format are dropped
        // by the parser, so the sentinel just produces zero rows.
        Ok(stdout)
    }
}

/// Mock executor for unit / integration tests. Returns canned output
/// or errors per location.
#[derive(Default, Clone)]
pub struct MockSshExecutor {
    pub local: Option<Result<String, String>>,
    pub remotes: BTreeMap<String, Result<String, String>>,
}

impl MockSshExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_local(mut self, output: impl Into<String>) -> Self {
        self.local = Some(Ok(output.into()));
        self
    }

    pub fn with_local_error(mut self, msg: impl Into<String>) -> Self {
        self.local = Some(Err(msg.into()));
        self
    }

    pub fn with_remote(mut self, name: impl Into<String>, output: impl Into<String>) -> Self {
        self.remotes.insert(name.into(), Ok(output.into()));
        self
    }

    pub fn with_remote_error(mut self, name: impl Into<String>, msg: impl Into<String>) -> Self {
        self.remotes.insert(name.into(), Err(msg.into()));
        self
    }
}

#[async_trait]
impl SshExecutor for MockSshExecutor {
    async fn run_local_tmux_ls(&self) -> Result<String> {
        match &self.local {
            Some(Ok(s)) => Ok(s.clone()),
            Some(Err(e)) => Err(anyhow!("{}", e)),
            None => Ok(String::new()),
        }
    }

    async fn run_remote_tmux_ls(&self, entry: &RemoteEntry) -> Result<String> {
        // Lookup by host *and* by alias — the test fixtures key by
        // alias (`vps`, `homelab`) for readability.
        for (alias, result) in &self.remotes {
            if alias == &entry.host {
                return result.clone().map_err(|e| anyhow!(e));
            }
        }
        // Fallback: use the host directly.
        Err(anyhow!("no canned output for remote {}", entry.host))
    }
}

/// Filter for which locations to query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoverScope {
    /// Local + every configured remote.
    All,
    /// Local only.
    LocalOnly,
    /// One named remote (no local).
    OnlyRemote(String),
}

/// Discover deskd-* tmux sessions across local + configured remotes.
///
/// SSH fan-out is concurrent. Each remote has a 5-second hard cap via
/// `tokio::time::timeout`; the row surfaces as `Unreachable` on
/// timeout. Local discovery is also concurrent but practically
/// instantaneous.
pub async fn discover<E: SshExecutor + 'static>(
    executor: Arc<E>,
    config: &RemotesConfig,
    scope: DiscoverScope,
) -> DiscoveryResult {
    discover_with_timeout(executor, config, scope, DEFAULT_REMOTE_TIMEOUT).await
}

/// Like [`discover`] but with a caller-controlled per-remote timeout.
/// Used by tests that don't want to wait 5 seconds for a deliberately
/// stuck mock.
pub async fn discover_with_timeout<E: SshExecutor + 'static>(
    executor: Arc<E>,
    config: &RemotesConfig,
    scope: DiscoverScope,
    timeout: Duration,
) -> DiscoveryResult {
    let mut tasks = Vec::new();

    // Local task — only when the scope includes local.
    if matches!(scope, DiscoverScope::All | DiscoverScope::LocalOnly) {
        let exec = executor.clone();
        tasks.push(tokio::spawn(async move {
            let result = tokio::time::timeout(timeout, exec.run_local_tmux_ls()).await;
            (
                "local".to_string(),
                Location::Local,
                result_to_outcome(result),
            )
        }));
    }

    // Remote tasks.
    let remote_entries: Vec<(String, RemoteEntry)> = match &scope {
        DiscoverScope::All => config.iter().map(|(n, e)| (n.clone(), e.clone())).collect(),
        DiscoverScope::OnlyRemote(name) => match config.get(name) {
            Some(entry) => vec![(name.clone(), entry.clone())],
            None => Vec::new(),
        },
        DiscoverScope::LocalOnly => Vec::new(),
    };

    for (name, entry) in remote_entries {
        let exec = executor.clone();
        let entry_clone = entry.clone();
        let name_clone = name.clone();
        tasks.push(tokio::spawn(async move {
            let result = tokio::time::timeout(timeout, exec.run_remote_tmux_ls(&entry_clone)).await;
            (
                name_clone.clone(),
                Location::Remote(name_clone),
                result_to_outcome(result),
            )
        }));
    }

    let mut result = DiscoveryResult::new();

    for handle in tasks {
        let (label, location, outcome) = match handle.await {
            Ok(v) => v,
            Err(e) => {
                // Tokio join error is rare (panic in the task) — surface
                // it but don't crash the listing.
                result.location_status.insert(
                    "?".to_string(),
                    LocationStatus::Error(format!("task join: {}", e)),
                );
                continue;
            }
        };
        match outcome {
            DiscoverOutcome::Output(s) => {
                result.location_status.insert(label, LocationStatus::Ok);
                let parsed = parse_tmux_ls(&s, location.clone());
                result.entries.extend(parsed);
            }
            DiscoverOutcome::Unreachable(msg) => {
                result
                    .location_status
                    .insert(label, LocationStatus::Unreachable(msg));
            }
            DiscoverOutcome::Error(msg) => {
                result
                    .location_status
                    .insert(label, LocationStatus::Error(msg));
            }
        }
    }

    // Stable ordering: local first, then remotes alphabetised, then
    // session name for tie-breaking. Makes table output deterministic.
    result.entries.sort_by(|a, b| {
        location_order(&a.location)
            .cmp(&location_order(&b.location))
            .then_with(|| a.location.as_label().cmp(b.location.as_label()))
            .then_with(|| a.session.cmp(&b.session))
    });

    result
}

fn location_order(loc: &Location) -> u8 {
    match loc {
        Location::Local => 0,
        Location::Remote(_) => 1,
    }
}

enum DiscoverOutcome {
    Output(String),
    Unreachable(String),
    Error(String),
}

fn result_to_outcome(
    r: std::result::Result<Result<String>, tokio::time::error::Elapsed>,
) -> DiscoverOutcome {
    match r {
        Err(_) => DiscoverOutcome::Unreachable("timed out after 5s".to_string()),
        Ok(Err(e)) => {
            let msg = format!("{e}");
            // Classify SSH connection failures vs other errors. Anything
            // network-shaped surfaces as `unreachable`; tmux/format
            // issues as `error`.
            let lower = msg.to_lowercase();
            if lower.contains("connection refused")
                || lower.contains("connect to host")
                || lower.contains("port 22")
                || lower.contains("connection timed out")
                || lower.contains("no route to host")
                || lower.contains("permission denied")
                || lower.contains("host key verification failed")
                || lower.contains("could not resolve")
            {
                DiscoverOutcome::Unreachable(msg)
            } else {
                DiscoverOutcome::Error(msg)
            }
        }
        Ok(Ok(stdout)) => DiscoverOutcome::Output(stdout),
    }
}

/// Parse output of `tmux ls -F <TMUX_LS_FORMAT>`. Lines that don't
/// start with `deskd-` are skipped (we only surface deskd-managed
/// sessions; the host can have unrelated tmux sessions).
pub fn parse_tmux_ls(stdout: &str, location: Location) -> Vec<SessionEntry> {
    let mut out = Vec::new();
    for raw_line in stdout.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 2 {
            continue;
        }
        let name = parts[0];
        if !name.starts_with("deskd-") {
            continue;
        }
        let agent = name.strip_prefix("deskd-").unwrap_or(name).to_string();
        let attached_str = parts.get(1).copied().unwrap_or("detached");
        let attached = attached_str == "attached";
        let activity_epoch: i64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
        let created_epoch: i64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
        out.push(SessionEntry {
            location: location.clone(),
            session: name.to_string(),
            agent,
            attached,
            attached_count: if attached { 1 } else { 0 },
            activity_epoch,
            created_epoch,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_line(name: &str, attached: bool, activity: i64, created: i64) -> String {
        format!(
            "{}\t{}\t{}\t{}",
            name,
            if attached { "attached" } else { "detached" },
            activity,
            created
        )
    }

    #[test]
    fn parses_single_deskd_session() {
        let stdout = fake_line("deskd-kira", true, 1_700_000_000, 1_699_999_000);
        let entries = parse_tmux_ls(&stdout, Location::Remote("vps".to_string()));
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.session, "deskd-kira");
        assert_eq!(e.agent, "kira");
        assert!(e.attached);
        assert_eq!(e.attached_count, 1);
        assert_eq!(e.activity_epoch, 1_700_000_000);
        assert_eq!(e.created_epoch, 1_699_999_000);
        assert_eq!(e.location, Location::Remote("vps".to_string()));
    }

    #[test]
    fn ignores_non_deskd_sessions() {
        let stdout = format!(
            "{}\n{}\n",
            fake_line("personal", false, 1_700_000_000, 1_700_000_000),
            fake_line("deskd-dev", false, 1_700_000_100, 1_699_999_000),
        );
        let entries = parse_tmux_ls(&stdout, Location::Local);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].agent, "dev");
    }

    #[test]
    fn skips_blank_and_malformed_lines() {
        let stdout = format!(
            "\n  \n{}\nnotabseparated\n{}\n",
            fake_line("deskd-a", false, 0, 0),
            fake_line("deskd-b", true, 0, 0),
        );
        let entries = parse_tmux_ls(&stdout, Location::Local);
        // `notabseparated` has no tabs → split → 1 field → skipped.
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].agent, "a");
        assert_eq!(entries[1].agent, "b");
    }

    #[test]
    fn missing_activity_field_is_zero() {
        let stdout = "deskd-kira\tdetached";
        let entries = parse_tmux_ls(stdout, Location::Local);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].activity_epoch, 0);
        assert_eq!(entries[0].created_epoch, 0);
    }

    fn cfg_with_two_remotes() -> RemotesConfig {
        let mut cfg = RemotesConfig::default();
        cfg.remotes.insert(
            "vps".to_string(),
            RemoteEntry {
                host: "vps".to_string(),
                ssh_options: vec![],
            },
        );
        cfg.remotes.insert(
            "homelab".to_string(),
            RemoteEntry {
                host: "homelab".to_string(),
                ssh_options: vec![],
            },
        );
        cfg
    }

    #[tokio::test]
    async fn discover_merges_local_and_remotes() {
        let mock = MockSshExecutor::new()
            .with_local(fake_line("deskd-uagent", false, 100, 50))
            .with_remote("vps", fake_line("deskd-kira", true, 200, 100))
            .with_remote("homelab", "");
        let cfg = cfg_with_two_remotes();
        let result = discover(Arc::new(mock), &cfg, DiscoverScope::All).await;

        assert_eq!(result.entries.len(), 2);
        // Local rows precede remote rows (sort order).
        assert_eq!(result.entries[0].location, Location::Local);
        assert_eq!(result.entries[0].agent, "uagent");
        assert_eq!(
            result.entries[1].location,
            Location::Remote("vps".to_string())
        );
        assert_eq!(result.entries[1].agent, "kira");

        // Every queried location has an `Ok` status.
        assert_eq!(
            result.location_status.get("local"),
            Some(&LocationStatus::Ok)
        );
        assert_eq!(result.location_status.get("vps"), Some(&LocationStatus::Ok));
        assert_eq!(
            result.location_status.get("homelab"),
            Some(&LocationStatus::Ok)
        );
    }

    #[tokio::test]
    async fn discover_classifies_unreachable() {
        let mock = MockSshExecutor::new()
            .with_local("")
            .with_remote_error("vps", "ssh: connect to host vps: Connection refused");
        let mut cfg = RemotesConfig::default();
        cfg.remotes.insert(
            "vps".to_string(),
            RemoteEntry {
                host: "vps".to_string(),
                ssh_options: vec![],
            },
        );
        let result = discover(Arc::new(mock), &cfg, DiscoverScope::All).await;
        assert!(matches!(
            result.location_status.get("vps"),
            Some(LocationStatus::Unreachable(_))
        ));
        // No session rows from a failed remote — they all surface via
        // the location status row.
        assert!(result.entries.iter().all(|e| e.location == Location::Local));
    }

    #[tokio::test]
    async fn discover_classifies_other_error_as_error() {
        let mock = MockSshExecutor::new()
            .with_local("")
            .with_remote_error("vps", "tmux: command not found");
        let mut cfg = RemotesConfig::default();
        cfg.remotes.insert(
            "vps".to_string(),
            RemoteEntry {
                host: "vps".to_string(),
                ssh_options: vec![],
            },
        );
        let result = discover(Arc::new(mock), &cfg, DiscoverScope::All).await;
        assert!(matches!(
            result.location_status.get("vps"),
            Some(LocationStatus::Error(_))
        ));
    }

    #[tokio::test]
    async fn discover_only_remote_skips_local() {
        let mock = MockSshExecutor::new()
            .with_local(fake_line("deskd-uagent", false, 100, 50))
            .with_remote("vps", fake_line("deskd-kira", true, 200, 100));
        let cfg = cfg_with_two_remotes();
        let result = discover(
            Arc::new(mock),
            &cfg,
            DiscoverScope::OnlyRemote("vps".to_string()),
        )
        .await;
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].agent, "kira");
        assert!(!result.location_status.contains_key("local"));
        assert!(!result.location_status.contains_key("homelab"));
    }

    #[tokio::test]
    async fn discover_local_only_skips_remotes() {
        let mock = MockSshExecutor::new()
            .with_local(fake_line("deskd-uagent", false, 100, 50))
            .with_remote("vps", "should not be queried");
        let cfg = cfg_with_two_remotes();
        let result = discover(Arc::new(mock), &cfg, DiscoverScope::LocalOnly).await;
        assert_eq!(result.entries.len(), 1);
        assert!(!result.location_status.contains_key("vps"));
    }

    #[tokio::test]
    async fn discover_unknown_remote_yields_empty() {
        let mock = MockSshExecutor::new();
        let cfg = cfg_with_two_remotes();
        let result = discover(
            Arc::new(mock),
            &cfg,
            DiscoverScope::OnlyRemote("ghost".to_string()),
        )
        .await;
        assert!(result.entries.is_empty());
        assert!(result.location_status.is_empty());
    }

    #[tokio::test]
    async fn discover_timeout_surfaces_as_unreachable() {
        struct SlowExec;
        #[async_trait]
        impl SshExecutor for SlowExec {
            async fn run_local_tmux_ls(&self) -> Result<String> {
                Ok(String::new())
            }
            async fn run_remote_tmux_ls(&self, _entry: &RemoteEntry) -> Result<String> {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(String::new())
            }
        }
        let cfg = cfg_with_two_remotes();
        let result = discover_with_timeout(
            Arc::new(SlowExec),
            &cfg,
            DiscoverScope::OnlyRemote("vps".to_string()),
            Duration::from_millis(30),
        )
        .await;
        assert!(matches!(
            result.location_status.get("vps"),
            Some(LocationStatus::Unreachable(_))
        ));
    }

    #[test]
    fn find_agent_local_and_remote_collision() {
        let mut r = DiscoveryResult::new();
        r.entries.push(SessionEntry {
            location: Location::Local,
            session: "deskd-dev".to_string(),
            agent: "dev".to_string(),
            attached: false,
            attached_count: 0,
            activity_epoch: 0,
            created_epoch: 0,
        });
        r.entries.push(SessionEntry {
            location: Location::Remote("vps".to_string()),
            session: "deskd-dev".to_string(),
            agent: "dev".to_string(),
            attached: false,
            attached_count: 0,
            activity_epoch: 0,
            created_epoch: 0,
        });
        let all = r.find_agent("dev", None);
        assert_eq!(all.len(), 2);
        let local_only = r.find_agent("dev", Some("local"));
        assert_eq!(local_only.len(), 1);
        assert_eq!(local_only[0].location, Location::Local);
        let vps_only = r.find_agent("dev", Some("vps"));
        assert_eq!(vps_only.len(), 1);
        assert_eq!(vps_only[0].location, Location::Remote("vps".to_string()));
    }
}
