//! Parser for `~/.deskd/config.yaml` — operator-level config for remote
//! tmux discovery (#453).
//!
//! Keeps scope tiny: just the `remotes:` map. Other top-level keys are
//! ignored so this file can grow new sections later without breaking
//! older code.
//!
//! ## Format
//!
//! ```yaml
//! remotes:
//!   vps:
//!     host: root@vps.example.com
//!     ssh_options: ["-i", "~/.ssh/vps_id"]   # optional
//!   homelab:
//!     host: kgatilin@homelab.local
//! ```
//!
//! `host:` is the SSH destination (anything `ssh` accepts: `user@host`,
//! `host`, an `~/.ssh/config` alias, etc.). `ssh_options:` is appended
//! verbatim before the destination so per-host keys / ports work.
//!
//! ## Defaults
//!
//! - File missing → empty `RemotesConfig` (no remotes; commands operate
//!   on local only).
//! - File present but no `remotes:` key → empty.
//! - Malformed YAML or wrong types → bubble up the parser error.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Default path to the operator config file.
///
/// Matches the existing per-agent state convention (`~/.deskd/agents/…`
/// — see `infra::paths`). Documented in README so operators know where
/// to drop the file.
pub fn default_config_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".deskd").join("config.yaml"))
}

/// A single SSH-reachable remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteEntry {
    /// SSH destination (`user@host`, `host`, or `~/.ssh/config` alias).
    pub host: String,
    /// Extra arguments to pass to `ssh` before the destination
    /// (e.g. `["-i", "~/.ssh/vps_id", "-p", "2222"]`).
    #[serde(default)]
    pub ssh_options: Vec<String>,
}

/// The full operator config (only the `remotes:` section is parsed here).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemotesConfig {
    #[serde(default)]
    pub remotes: BTreeMap<String, RemoteEntry>,
}

impl RemotesConfig {
    /// Load from the default path (`~/.deskd/config.yaml`). Missing file
    /// is not an error — returns `RemotesConfig::default()`.
    pub fn load_default() -> Result<Self> {
        match default_config_path() {
            Some(p) => Self::load_optional(&p),
            None => Ok(Self::default()),
        }
    }

    /// Load from `path`. Missing file → default (empty). Parse errors
    /// propagate.
    pub fn load_optional(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read remotes config {}", path.display()))?;
        parse_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
    }

    /// Look up a remote by name.
    pub fn get(&self, name: &str) -> Option<&RemoteEntry> {
        self.remotes.get(name)
    }

    /// Iterate over `(name, entry)` pairs in deterministic order
    /// (BTreeMap → lexicographic by name).
    pub fn iter(&self) -> impl Iterator<Item = (&String, &RemoteEntry)> {
        self.remotes.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.remotes.is_empty()
    }
}

/// Parse a YAML string into a `RemotesConfig`. Empty or whitespace-only
/// input maps to an empty config.
pub fn parse_str(raw: &str) -> Result<RemotesConfig> {
    if raw.trim().is_empty() {
        return Ok(RemotesConfig::default());
    }
    let cfg: RemotesConfig = serde_yaml::from_str(raw).context("invalid YAML")?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_yields_empty_config() {
        let cfg = parse_str("").unwrap();
        assert!(cfg.remotes.is_empty());
    }

    #[test]
    fn missing_remotes_key_yields_empty_config() {
        // Other top-level keys must not break parsing — the config file
        // can grow new sections without bumping this struct.
        let cfg = parse_str("some_unrelated: 42\n").unwrap();
        assert!(cfg.remotes.is_empty());
    }

    #[test]
    fn parses_single_remote() {
        let cfg = parse_str(
            r#"
remotes:
  vps:
    host: root@vps.example.com
"#,
        )
        .unwrap();
        assert_eq!(cfg.remotes.len(), 1);
        let vps = cfg.get("vps").unwrap();
        assert_eq!(vps.host, "root@vps.example.com");
        assert!(vps.ssh_options.is_empty());
    }

    #[test]
    fn parses_remote_with_ssh_options() {
        let cfg = parse_str(
            r#"
remotes:
  vps:
    host: root@vps.example.com
    ssh_options: ["-i", "~/.ssh/vps_id", "-p", "2222"]
"#,
        )
        .unwrap();
        let vps = cfg.get("vps").unwrap();
        assert_eq!(vps.ssh_options, vec!["-i", "~/.ssh/vps_id", "-p", "2222"]);
    }

    #[test]
    fn parses_multiple_remotes_alphabetised() {
        let cfg = parse_str(
            r#"
remotes:
  zeta:
    host: zeta.example.com
  alpha:
    host: alpha.example.com
"#,
        )
        .unwrap();
        let names: Vec<_> = cfg.iter().map(|(n, _)| n.as_str()).collect();
        // BTreeMap yields sorted order — that drives a stable `list`
        // output ordering for users.
        assert_eq!(names, vec!["alpha", "zeta"]);
    }

    #[test]
    fn malformed_yaml_errors() {
        let result = parse_str("remotes:\n  vps:\n    host: : :");
        assert!(result.is_err());
    }

    #[test]
    fn malformed_remote_errors() {
        // `host:` missing — required field.
        let result = parse_str(
            r#"
remotes:
  vps:
    ssh_options: []
"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn load_optional_missing_file_is_default() {
        let p = std::env::temp_dir().join(format!(
            "deskd-test-remotes-missing-{}.yaml",
            uuid::Uuid::new_v4()
        ));
        // File doesn't exist.
        let cfg = RemotesConfig::load_optional(&p).unwrap();
        assert!(cfg.remotes.is_empty());
    }

    #[test]
    fn load_optional_reads_real_file() {
        let dir = std::env::temp_dir().join(format!("deskd-test-remotes-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("config.yaml");
        std::fs::write(
            &p,
            r#"
remotes:
  vps:
    host: root@vps.example.com
"#,
        )
        .unwrap();
        let cfg = RemotesConfig::load_optional(&p).unwrap();
        assert_eq!(cfg.remotes.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
