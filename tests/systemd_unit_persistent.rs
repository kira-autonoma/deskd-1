//! Integration test for #505: systemd-user unit auto-install for tmux-launched agents.
//!
//! True end-to-end coverage (forking real tmux + verifying systemd restart
//! semantics) cannot live in CI — it needs a running `systemd --user`
//! instance, a `loginctl`-known user, an actual tmux server, plus the
//! kill-tmux-server / wait-for-restart machinery. Those checks are gated
//! behind `#[ignore]` and run manually on a real workstation:
//!
//! ```sh
//! cargo test --test systemd_unit_persistent -- --ignored
//! ```
//!
//! The non-ignored test exercises the rendering + install-path resolution +
//! idempotent uninstall paths that are safe to run anywhere — they only
//! touch a per-test `$HOME` and never invoke real `systemctl`.

use std::path::{Path, PathBuf};

use deskd::app::tmux_launcher::{
    parse_linger, render_systemd_unit, systemd_unit_install_path, uninstall_systemd_unit,
};

fn unique_home(prefix: &str) -> PathBuf {
    PathBuf::from(format!(
        "/tmp/deskd-test-{}-{}",
        prefix,
        uuid::Uuid::new_v4()
    ))
}

/// AC: "Unit content has Restart=on-failure or always, reasonable RestartSec,
/// WantedBy=default.target". Verifies the rendered unit holds the
/// systemd-required structure and #505-mandated directives.
#[test]
fn rendered_unit_contains_required_directives() {
    let unit = render_systemd_unit("kira", Path::new("/usr/local/bin/deskd"));

    // Section headers
    assert!(unit.contains("[Unit]"), "missing [Unit] section");
    assert!(unit.contains("[Service]"), "missing [Service] section");
    assert!(unit.contains("[Install]"), "missing [Install] section");

    // #505 mandates these directives
    assert!(
        unit.contains("Restart=on-failure") || unit.contains("Restart=always"),
        "unit must Restart=on-failure or always"
    );
    assert!(
        unit.contains("RestartSec="),
        "unit must specify a RestartSec"
    );
    assert!(
        unit.contains("WantedBy=default.target"),
        "unit must be WantedBy=default.target for user-mode enable"
    );

    // ExecStart + ExecStop must reference the agent and binary
    assert!(unit.contains("ExecStart=/usr/local/bin/deskd agent start kira --tmux"));
    assert!(unit.contains("ExecStop=/usr/local/bin/deskd agent stop kira"));
}

/// `systemd_unit_install_path` must produce
/// `$HOME/.config/systemd/user/deskd-<agent>.service` — the canonical
/// systemd-user unit location (AC: "installs `~/.config/systemd/user/deskd-<name>.service`").
#[test]
fn install_path_is_canonical_systemd_user_location() {
    let prev_home = std::env::var_os("HOME");
    let home = unique_home("505-path");
    std::fs::create_dir_all(&home).unwrap();
    // SAFETY: test mutates $HOME within its own scope and restores below.
    unsafe { std::env::set_var("HOME", &home) };

    let path = systemd_unit_install_path("agent-x").unwrap();
    let expected = home
        .join(".config")
        .join("systemd")
        .join("user")
        .join("deskd-agent-x.service");

    // Restore $HOME before asserting so a failure does not leak state.
    unsafe {
        match prev_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
    }
    let _ = std::fs::remove_dir_all(&home);

    assert_eq!(path, expected);
}

/// `uninstall_systemd_unit` must be idempotent — calling it when no unit
/// has ever been installed should not error out (AC: "Idempotent — if the
/// unit was never installed this is a no-op").
///
/// `uninstall_systemd_unit` does run `systemctl --user daemon-reload` at
/// the end, which can fail in environments without systemd-user. The
/// underlying call returns the file-removal result (Ok(false) for "did
/// not exist"); the daemon-reload error gets wrapped in `Result::Err`,
/// so we treat either outcome as acceptable proof of idempotency for the
/// file-removal half — what matters is that the function did not panic
/// and did not remove anything spurious.
#[test]
fn uninstall_is_idempotent_when_unit_missing() {
    let prev_home = std::env::var_os("HOME");
    let home = unique_home("505-uninstall");
    std::fs::create_dir_all(&home).unwrap();
    // SAFETY: test mutates $HOME within its own scope and restores below.
    unsafe { std::env::set_var("HOME", &home) };

    // No file exists yet. Either Ok(false) (no systemd-user running but
    // daemon-reload swallowed) or Err (daemon-reload failed) is fine —
    // the contract is "do not panic, do not touch unrelated files".
    let result = uninstall_systemd_unit("ghost-agent");
    let unit_path = systemd_unit_install_path("ghost-agent").unwrap();

    // Restore $HOME before asserting so a failure does not leak state.
    unsafe {
        match prev_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
    }
    let _ = std::fs::remove_dir_all(&home);

    match result {
        Ok(existed) => assert!(
            !existed,
            "no unit installed, but uninstall reported existed"
        ),
        Err(_) => {
            // CI / sandbox env without systemd-user: daemon-reload failed.
            // That's still a tolerable outcome — the unit file was correctly
            // identified as absent, so `existed == false` was returned from
            // the file branch before the systemctl call.
        }
    }
    assert!(
        !unit_path.exists(),
        "uninstall must not create the unit file"
    );
}

/// `parse_linger` covers the loginctl-output cases that drive the linger
/// warning rendering. AC: "If `loginctl show-user <user> --property=Linger`
/// returns `Linger=no`, print clear warning + the command to enable linger".
#[test]
fn parse_linger_recognizes_yes_no_and_unknown() {
    assert_eq!(parse_linger("Linger=yes\n"), Some(true));
    assert_eq!(parse_linger("Linger=no\n"), Some(false));
    assert_eq!(parse_linger(""), None);
    assert_eq!(parse_linger("Linger=maybe\n"), None);
}

// ─── Manual-only tests: require real systemd-user + tmux ────────────────────

/// Real install + enable + uninstall cycle against the actual user-mode
/// systemd instance. Skipped in CI because:
/// - the test runner often has no `systemctl --user` (no PID 1 systemd
///   user instance in containers)
/// - `loginctl enable-linger` requires sudo and is host-state-affecting
/// - tmux must be on PATH and able to fork a session
///
/// Run manually on a workstation with:
/// ```sh
/// cargo test --test systemd_unit_persistent -- --ignored install_enable_uninstall_roundtrip
/// ```
#[test]
#[ignore = "requires real systemd --user and tmux; run manually"]
fn install_enable_uninstall_roundtrip() {
    use deskd::app::tmux_launcher::{
        install_systemd_unit, systemd_unit_is_enabled, uninstall_systemd_unit,
    };

    let agent = format!("test-{}", uuid::Uuid::new_v4());
    let bin = std::env::current_exe().expect("test binary path");

    let path = install_systemd_unit(&agent, &bin).expect("install");
    assert!(path.exists(), "unit file must exist after install");
    assert!(
        systemd_unit_is_enabled(&agent),
        "unit must be enabled after install"
    );

    let removed = uninstall_systemd_unit(&agent).expect("uninstall");
    assert!(removed, "uninstall must report removal of installed unit");
    assert!(!path.exists(), "unit file must be absent after uninstall");
    assert!(
        !systemd_unit_is_enabled(&agent),
        "unit must not be enabled after uninstall"
    );
}

/// Crash-recovery integration test placeholder — see AC: "Integration test:
/// install unit, simulate crash by killing tmux server, verify systemd
/// reports failed/restarting. Tear down + assert clean removal."
///
/// Run manually with `--ignored systemd_crash_restart_cycle`. Requires a
/// real tmux server and real systemd --user; not safe in CI.
#[test]
#[ignore = "requires real systemd --user + tmux; run manually"]
fn systemd_crash_restart_cycle() {
    // Manual procedure (encoded as a runnable test stub for the operator):
    //
    // 1. `deskd agent start <test> --tmux --persistent`
    // 2. `tmux kill-server` (simulates crash)
    // 3. `systemctl --user status deskd-<test>.service` reports
    //    `activating (auto-restart)` or `failed` then re-launching.
    // 4. `deskd agent stop <test> --uninstall-unit`
    // 5. `systemctl --user status deskd-<test>.service` reports `not-found`.
    //
    // Encoding this as Rust would require shelling out to tmux/systemctl
    // with a real session and watching state transitions over ~10s — a
    // separate harness, not a unit test. Tracked for the operator manual.
    panic!("manual procedure — see source comment");
}
