//! Provisioning helper for unattended `claude --dangerously-load-development-channels`
//! launches under tmux (#503).
//!
//! On a fresh Claude Code home, three first-run prompts block an unattended
//! REPL from reaching the "Listening for channel messages from: server:deskd"
//! state:
//!
//! 1. Folder trust ("Is this a project you created or one you trust?")
//! 2. MCP server approval ("New MCP server found in .mcp.json: deskd")
//! 3. Dev-channels warning ("WARNING: Loading development channels …")
//!
//! [`provision_for_channel_tmux`] pre-accepts all three by writing the
//! relevant JSON config files into the user's home and the agent's work_dir:
//!
//! - `~/.claude.json` — folder-trust + onboarding flags.
//! - `~/.claude/settings.json` — user-scope permission mode + dev-channels
//!   ack + `enableAllProjectMcpServers`.
//! - `{work_dir}/.mcp.json` — minimal deskd MCP-server registration with
//!   `DESKD_BUS_SOCKET` env injected.
//!
//! All writes are **merge-aware** (existing user content is preserved),
//! **idempotent** (re-running on an already-provisioned home is a no-op),
//! and **atomic** (write-temp-then-rename, so a crash never leaves a corrupt
//! half-written JSON file).
//!
//! NOTE: the helper is intentionally *not* wired into `launch_tmux_session()`
//! by this change — that integration is tracked separately by #504.

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Map, Value, json};
use std::fs;
use std::path::{Path, PathBuf};

/// Resolve the user's actual home directory.
///
/// Mirrors the convention used by [`crate::app::tmux_launcher`] — we read the
/// `$HOME` env var rather than pulling in the `dirs` crate. Returns an error
/// if `$HOME` is unset (e.g. running under a stripped-down systemd unit).
fn user_home_dir() -> Result<PathBuf> {
    let h = std::env::var_os("HOME")
        .ok_or_else(|| anyhow!("$HOME is not set; cannot locate user home directory"))?;
    Ok(PathBuf::from(h))
}

/// Provision the user's Claude Code home + the agent's work_dir so an
/// unattended `claude --dangerously-load-development-channels server:deskd`
/// launch under tmux clears all three first-run prompts.
///
/// Writes / merges three files (see module docs):
///
/// 1. `~/.claude.json`
/// 2. `~/.claude/settings.json`
/// 3. `{work_dir}/.mcp.json`
///
/// Idempotent: safe to re-run on an already-provisioned home.
pub fn provision_for_channel_tmux(
    work_dir: &Path,
    agent_name: &str,
    bus_socket: &Path,
) -> Result<()> {
    let home = user_home_dir()?;
    let work_dir_abs = absolutise(work_dir)
        .with_context(|| format!("failed to absolutise work_dir {}", work_dir.display()))?;

    // 1. ~/.claude.json
    let claude_json = home.join(".claude.json");
    update_claude_json(&claude_json, &work_dir_abs)
        .with_context(|| format!("failed to update {}", claude_json.display()))?;

    // 2. ~/.claude/settings.json
    let settings_dir = home.join(".claude");
    fs::create_dir_all(&settings_dir).with_context(|| {
        format!(
            "failed to create user settings directory {}",
            settings_dir.display()
        )
    })?;
    let settings_json = settings_dir.join("settings.json");
    update_user_settings(&settings_json)
        .with_context(|| format!("failed to update {}", settings_json.display()))?;

    // 3. {work_dir}/.mcp.json
    let mcp_json = work_dir_abs.join(".mcp.json");
    update_project_mcp_json(&mcp_json, agent_name, bus_socket)
        .with_context(|| format!("failed to update {}", mcp_json.display()))?;

    Ok(())
}

/// Make `p` absolute. If `p` is already absolute, return it unchanged.
/// Otherwise resolve against the current working directory. We deliberately
/// do *not* canonicalise (which would resolve symlinks and require the path
/// to exist) — Claude Code stores paths as-typed.
fn absolutise(p: &Path) -> Result<PathBuf> {
    if p.is_absolute() {
        Ok(p.to_path_buf())
    } else {
        let cwd = std::env::current_dir().context("failed to read current working directory")?;
        Ok(cwd.join(p))
    }
}

/// Load JSON from `path`, or return `Value::Null` if the file does not exist.
/// Errors if the file exists but is unreadable or malformed.
fn load_json_or_null(path: &Path) -> Result<Value> {
    match fs::read_to_string(path) {
        Ok(s) => {
            if s.trim().is_empty() {
                return Ok(Value::Null);
            }
            serde_json::from_str(&s)
                .with_context(|| format!("failed to parse JSON at {}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Value::Null),
        Err(e) => {
            Err(anyhow::Error::new(e)
                .context(format!("failed to read {} for merge", path.display())))
        }
    }
}

/// Coerce `v` into a JSON object, converting `Null` to `{}` and erroring on
/// any non-object value (arrays, scalars).
fn coerce_to_object(v: Value, path: &Path) -> Result<Map<String, Value>> {
    match v {
        Value::Object(m) => Ok(m),
        Value::Null => Ok(Map::new()),
        other => bail!(
            "{} is not a JSON object (found {}); refusing to overwrite",
            path.display(),
            type_of(&other)
        ),
    }
}

fn type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Atomically write `value` (pretty-printed) to `path`. Writes to a sibling
/// temp file in the same directory then renames into place — the rename is
/// atomic on POSIX filesystems, so a crash mid-write can never leave a
/// corrupt half-JSON.
fn write_json_atomic(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    let serialised =
        serde_json::to_string_pretty(value).context("failed to serialise JSON for atomic write")?;

    // Place the temp file next to the destination so rename(2) stays on the
    // same filesystem.
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("destination path has no file name: {}", path.display()))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp_name = format!(
        ".{}.deskd-tmp.{}",
        file_name.to_string_lossy(),
        std::process::id()
    );
    let tmp_path = parent.join(tmp_name);

    fs::write(&tmp_path, serialised.as_bytes())
        .with_context(|| format!("failed to write temp file {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to rename {} -> {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

/// Merge the channel-trust fields into `~/.claude.json`.
///
/// Pre-accepts the folder-trust prompt by:
/// - setting `hasTrustDialogAccepted: true`
/// - setting `hasCompletedProjectOnboarding: true`
/// - setting `hasCompletedOnboarding: true`
/// - appending the absolute `work_dir` to `trustedProjects` (deduplicated).
///
/// Unrelated keys are preserved.
fn update_claude_json(path: &Path, work_dir_abs: &Path) -> Result<()> {
    let existing = load_json_or_null(path)?;
    let mut obj = coerce_to_object(existing, path)?;

    obj.insert("hasTrustDialogAccepted".to_string(), Value::Bool(true));
    obj.insert(
        "hasCompletedProjectOnboarding".to_string(),
        Value::Bool(true),
    );
    obj.insert("hasCompletedOnboarding".to_string(), Value::Bool(true));

    let work_dir_str = work_dir_abs
        .to_str()
        .ok_or_else(|| anyhow!("work_dir is not valid UTF-8: {}", work_dir_abs.display()))?
        .to_string();

    // trustedProjects — array of absolute path strings. Dedup.
    let trusted_entry = obj
        .entry("trustedProjects".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let trusted = match trusted_entry {
        Value::Array(a) => a,
        _ => bail!(
            "{}: `trustedProjects` exists but is not an array; refusing to overwrite",
            path.display()
        ),
    };
    let already_present = trusted.iter().any(|v| v.as_str() == Some(&work_dir_str));
    if !already_present {
        trusted.push(Value::String(work_dir_str));
    }

    write_json_atomic(path, &Value::Object(obj))
}

/// Merge the channel-launch permissions into `~/.claude/settings.json`.
///
/// Sets:
/// - `permissions.defaultMode = "bypassPermissions"`
/// - `permissions.skipDangerousModePermissionPrompt = true`
/// - `enableAllProjectMcpServers = true`
///
/// `skipDangerousModePermissionPrompt` is *ignored* in project-scoped
/// settings per the Claude Code docs — this function must therefore target
/// the user-scope `~/.claude/settings.json`. The caller in
/// [`provision_for_channel_tmux`] already does that; this function trusts
/// the path it receives.
fn update_user_settings(path: &Path) -> Result<()> {
    let existing = load_json_or_null(path)?;
    let mut obj = coerce_to_object(existing, path)?;

    // permissions.{defaultMode, skipDangerousModePermissionPrompt}
    let perms_entry = obj
        .entry("permissions".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let perms = match perms_entry {
        Value::Object(m) => m,
        _ => bail!(
            "{}: `permissions` exists but is not an object; refusing to overwrite",
            path.display()
        ),
    };
    perms.insert(
        "defaultMode".to_string(),
        Value::String("bypassPermissions".to_string()),
    );
    perms.insert(
        "skipDangerousModePermissionPrompt".to_string(),
        Value::Bool(true),
    );

    obj.insert("enableAllProjectMcpServers".to_string(), Value::Bool(true));

    write_json_atomic(path, &Value::Object(obj))
}

/// Merge a minimal `deskd` MCP-server registration into `{work_dir}/.mcp.json`.
///
/// The registration is:
///
/// ```jsonc
/// {
///   "mcpServers": {
///     "deskd": {
///       "command": "deskd",
///       "args": ["mcp-channel", "--agent", "<name>"],
///       "env": { "DESKD_BUS_SOCKET": "<bus_socket>" }
///     }
///   }
/// }
/// ```
///
/// If `.mcp.json` already exists with other MCP servers, those are preserved;
/// only the `deskd` entry is overwritten/inserted.
fn update_project_mcp_json(path: &Path, agent_name: &str, bus_socket: &Path) -> Result<()> {
    let existing = load_json_or_null(path)?;
    let mut obj = coerce_to_object(existing, path)?;

    let bus_socket_str = bus_socket
        .to_str()
        .ok_or_else(|| anyhow!("bus_socket is not valid UTF-8: {}", bus_socket.display()))?
        .to_string();

    let servers_entry = obj
        .entry("mcpServers".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let servers = match servers_entry {
        Value::Object(m) => m,
        _ => bail!(
            "{}: `mcpServers` exists but is not an object; refusing to overwrite",
            path.display()
        ),
    };

    let deskd_entry = json!({
        "command": "deskd",
        "args": ["mcp-channel", "--agent", agent_name],
        "env": { "DESKD_BUS_SOCKET": bus_socket_str }
    });
    servers.insert("deskd".to_string(), deskd_entry);

    write_json_atomic(path, &Value::Object(obj))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    // Tests below mutate the process-wide `$HOME` env var. Cargo runs unit
    // tests in parallel by default, so we serialise via a mutex.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// RAII guard that points `$HOME` at a tempdir for the duration of a test.
    struct HomeGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prev: Option<std::ffi::OsString>,
        _tmp: TempDir,
    }

    impl HomeGuard {
        fn new() -> (Self, PathBuf) {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev = std::env::var_os("HOME");
            let tmp = TempDir::new().expect("create tempdir");
            let path = tmp.path().to_path_buf();
            // SAFETY: serialised via ENV_LOCK; restored in Drop.
            unsafe { std::env::set_var("HOME", &path) };
            (
                HomeGuard {
                    _lock: lock,
                    prev,
                    _tmp: tmp,
                },
                path,
            )
        }
    }

    impl Drop for HomeGuard {
        fn drop(&mut self) {
            // SAFETY: serialised via ENV_LOCK held in self._lock.
            unsafe {
                match self.prev.take() {
                    Some(v) => std::env::set_var("HOME", v),
                    None => std::env::remove_var("HOME"),
                }
            }
        }
    }

    fn read_json(path: &Path) -> Value {
        let s = fs::read_to_string(path).expect("read JSON file");
        serde_json::from_str(&s).expect("parse JSON file")
    }

    #[test]
    fn fresh_provisioning_writes_all_three_files() {
        let (_home, home) = HomeGuard::new();
        let work_dir = home.join("agents").join("kira");
        fs::create_dir_all(&work_dir).unwrap();
        let bus_socket = work_dir.join(".deskd").join("bus.sock");

        provision_for_channel_tmux(&work_dir, "kira", &bus_socket).unwrap();

        // ~/.claude.json
        let claude_json = read_json(&home.join(".claude.json"));
        assert_eq!(claude_json["hasTrustDialogAccepted"], Value::Bool(true));
        assert_eq!(
            claude_json["hasCompletedProjectOnboarding"],
            Value::Bool(true)
        );
        assert_eq!(claude_json["hasCompletedOnboarding"], Value::Bool(true));
        let trusted = claude_json["trustedProjects"].as_array().unwrap();
        assert_eq!(trusted.len(), 1);
        assert_eq!(trusted[0].as_str().unwrap(), work_dir.to_str().unwrap());

        // ~/.claude/settings.json
        let settings = read_json(&home.join(".claude").join("settings.json"));
        assert_eq!(
            settings["permissions"]["defaultMode"].as_str().unwrap(),
            "bypassPermissions"
        );
        assert_eq!(
            settings["permissions"]["skipDangerousModePermissionPrompt"],
            Value::Bool(true)
        );
        assert_eq!(settings["enableAllProjectMcpServers"], Value::Bool(true));

        // {work_dir}/.mcp.json
        let mcp = read_json(&work_dir.join(".mcp.json"));
        let deskd = &mcp["mcpServers"]["deskd"];
        assert_eq!(deskd["command"].as_str().unwrap(), "deskd");
        assert_eq!(
            deskd["args"].as_array().unwrap(),
            &vec![
                Value::String("mcp-channel".to_string()),
                Value::String("--agent".to_string()),
                Value::String("kira".to_string()),
            ]
        );
        assert_eq!(
            deskd["env"]["DESKD_BUS_SOCKET"].as_str().unwrap(),
            bus_socket.to_str().unwrap()
        );
    }

    #[test]
    fn merges_preserve_unrelated_keys() {
        let (_home, home) = HomeGuard::new();
        let work_dir = home.join("agents").join("dev");
        fs::create_dir_all(&work_dir).unwrap();
        let bus_socket = work_dir.join(".deskd").join("bus.sock");

        // Seed ~/.claude.json with a custom user-only key.
        let claude_json_path = home.join(".claude.json");
        fs::write(
            &claude_json_path,
            serde_json::to_string_pretty(&json!({
                "userID": "u-1234",
                "theme": "dark",
                "trustedProjects": ["/some/other/project"]
            }))
            .unwrap(),
        )
        .unwrap();

        // Seed ~/.claude/settings.json with custom keys + a partial
        // permissions block.
        let settings_dir = home.join(".claude");
        fs::create_dir_all(&settings_dir).unwrap();
        let settings_path = settings_dir.join("settings.json");
        fs::write(
            &settings_path,
            serde_json::to_string_pretty(&json!({
                "model": "claude-sonnet-4-6",
                "permissions": { "allow": ["Read(**)"], "defaultMode": "ask" }
            }))
            .unwrap(),
        )
        .unwrap();

        // Seed {work_dir}/.mcp.json with another MCP server.
        let mcp_path = work_dir.join(".mcp.json");
        fs::write(
            &mcp_path,
            serde_json::to_string_pretty(&json!({
                "mcpServers": {
                    "archlint": {
                        "command": "archlint",
                        "args": ["mcp"]
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        provision_for_channel_tmux(&work_dir, "dev", &bus_socket).unwrap();

        // ~/.claude.json: unrelated keys preserved; trustedProjects appended.
        let claude_json = read_json(&claude_json_path);
        assert_eq!(claude_json["userID"].as_str().unwrap(), "u-1234");
        assert_eq!(claude_json["theme"].as_str().unwrap(), "dark");
        assert_eq!(claude_json["hasTrustDialogAccepted"], Value::Bool(true));
        let trusted: Vec<&str> = claude_json["trustedProjects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(trusted.len(), 2);
        assert!(trusted.contains(&"/some/other/project"));
        assert!(trusted.contains(&work_dir.to_str().unwrap()));

        // settings.json: model preserved; permissions.allow preserved;
        // defaultMode overwritten to bypassPermissions; skipDangerous added.
        let settings = read_json(&settings_path);
        assert_eq!(settings["model"].as_str().unwrap(), "claude-sonnet-4-6");
        assert_eq!(
            settings["permissions"]["allow"].as_array().unwrap(),
            &vec![Value::String("Read(**)".to_string())]
        );
        assert_eq!(
            settings["permissions"]["defaultMode"].as_str().unwrap(),
            "bypassPermissions"
        );
        assert_eq!(
            settings["permissions"]["skipDangerousModePermissionPrompt"],
            Value::Bool(true)
        );
        assert_eq!(settings["enableAllProjectMcpServers"], Value::Bool(true));

        // .mcp.json: archlint preserved; deskd added.
        let mcp = read_json(&mcp_path);
        let servers = mcp["mcpServers"].as_object().unwrap();
        assert!(servers.contains_key("archlint"));
        assert!(servers.contains_key("deskd"));
        assert_eq!(servers["archlint"]["command"].as_str().unwrap(), "archlint");
        assert_eq!(servers["deskd"]["command"].as_str().unwrap(), "deskd");
    }

    #[test]
    fn idempotent_rerun_is_noop() {
        let (_home, home) = HomeGuard::new();
        let work_dir = home.join("agents").join("kira");
        fs::create_dir_all(&work_dir).unwrap();
        let bus_socket = work_dir.join(".deskd").join("bus.sock");

        provision_for_channel_tmux(&work_dir, "kira", &bus_socket).unwrap();

        let claude_after_first = fs::read_to_string(home.join(".claude.json")).unwrap();
        let settings_after_first =
            fs::read_to_string(home.join(".claude").join("settings.json")).unwrap();
        let mcp_after_first = fs::read_to_string(work_dir.join(".mcp.json")).unwrap();

        // Second run should produce byte-identical files.
        provision_for_channel_tmux(&work_dir, "kira", &bus_socket).unwrap();

        let claude_after_second = fs::read_to_string(home.join(".claude.json")).unwrap();
        let settings_after_second =
            fs::read_to_string(home.join(".claude").join("settings.json")).unwrap();
        let mcp_after_second = fs::read_to_string(work_dir.join(".mcp.json")).unwrap();

        assert_eq!(claude_after_first, claude_after_second);
        assert_eq!(settings_after_first, settings_after_second);
        assert_eq!(mcp_after_first, mcp_after_second);
    }

    #[test]
    fn trusted_projects_dedup_on_rerun() {
        let (_home, home) = HomeGuard::new();
        let work_dir = home.join("agents").join("kira");
        fs::create_dir_all(&work_dir).unwrap();
        let bus_socket = work_dir.join(".deskd").join("bus.sock");

        for _ in 0..5 {
            provision_for_channel_tmux(&work_dir, "kira", &bus_socket).unwrap();
        }
        let claude_json = read_json(&home.join(".claude.json"));
        let trusted = claude_json["trustedProjects"].as_array().unwrap();
        assert_eq!(trusted.len(), 1, "trustedProjects must dedup on rerun");
        assert_eq!(trusted[0].as_str().unwrap(), work_dir.to_str().unwrap());
    }

    #[test]
    fn writes_use_user_home_not_work_dir() {
        let (_home, home) = HomeGuard::new();
        let work_dir = home.join("agents").join("kira");
        fs::create_dir_all(&work_dir).unwrap();
        let bus_socket = work_dir.join(".deskd").join("bus.sock");

        provision_for_channel_tmux(&work_dir, "kira", &bus_socket).unwrap();

        // The user-scoped files must land in $HOME, NOT inside work_dir.
        assert!(home.join(".claude.json").exists());
        assert!(home.join(".claude").join("settings.json").exists());
        assert!(
            !work_dir.join(".claude.json").exists(),
            "user .claude.json must not be inside work_dir"
        );
        assert!(
            !work_dir.join(".claude").exists(),
            "user .claude/ must not be inside work_dir"
        );
    }

    #[test]
    fn malformed_existing_claude_json_returns_error() {
        let (_home, home) = HomeGuard::new();
        let work_dir = home.join("agents").join("kira");
        fs::create_dir_all(&work_dir).unwrap();
        let bus_socket = work_dir.join(".deskd").join("bus.sock");

        fs::write(home.join(".claude.json"), "{ this is not json").unwrap();

        let err = provision_for_channel_tmux(&work_dir, "kira", &bus_socket).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("failed to parse JSON") || msg.contains(".claude.json"),
            "expected parse-failure error, got: {}",
            msg
        );
    }

    #[test]
    fn existing_array_root_is_rejected() {
        let (_home, home) = HomeGuard::new();
        let work_dir = home.join("agents").join("kira");
        fs::create_dir_all(&work_dir).unwrap();
        let bus_socket = work_dir.join(".deskd").join("bus.sock");

        fs::write(
            home.join(".claude.json"),
            serde_json::to_string(&json!(["not", "an", "object"])).unwrap(),
        )
        .unwrap();

        let err = provision_for_channel_tmux(&work_dir, "kira", &bus_socket).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("not a JSON object"),
            "expected object-type error, got: {}",
            msg
        );
    }

    #[test]
    fn empty_existing_files_are_treated_as_blank() {
        let (_home, home) = HomeGuard::new();
        let work_dir = home.join("agents").join("kira");
        fs::create_dir_all(&work_dir).unwrap();
        let bus_socket = work_dir.join(".deskd").join("bus.sock");

        // Create empty files at all three destinations.
        fs::write(home.join(".claude.json"), "").unwrap();
        fs::create_dir_all(home.join(".claude")).unwrap();
        fs::write(home.join(".claude").join("settings.json"), "   \n  ").unwrap();
        fs::write(work_dir.join(".mcp.json"), "").unwrap();

        provision_for_channel_tmux(&work_dir, "kira", &bus_socket).unwrap();

        let claude_json = read_json(&home.join(".claude.json"));
        assert_eq!(claude_json["hasTrustDialogAccepted"], Value::Bool(true));
        let settings = read_json(&home.join(".claude").join("settings.json"));
        assert_eq!(settings["enableAllProjectMcpServers"], Value::Bool(true));
        let mcp = read_json(&work_dir.join(".mcp.json"));
        assert!(mcp["mcpServers"]["deskd"].is_object());
    }

    #[test]
    fn absolutise_returns_absolute_for_relative_input() {
        // Direct unit test of the helper — avoids mutating the process-wide
        // cwd, which would race with parallel tests elsewhere in the crate.
        let rel = Path::new("some/relative/path");
        let out = absolutise(rel).unwrap();
        assert!(out.is_absolute(), "expected absolute path, got {:?}", out);
        assert!(out.ends_with("some/relative/path"));
    }

    #[test]
    fn absolutise_passes_through_absolute() {
        let abs = Path::new("/already/absolute");
        let out = absolutise(abs).unwrap();
        assert_eq!(out, PathBuf::from("/already/absolute"));
    }
}
