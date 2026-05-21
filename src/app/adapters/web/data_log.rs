//! Log-tail data layer for the agent drill-down view (#485).
//!
//! Reads the last N lines from an agent log file using a seek-from-end
//! strategy so the implementation never loads the full log into memory.
//! Block size is fixed at 8 KiB; the caller can cap the returned line
//! count up to [`MAX_LOG_LINES`].
//!
//! The agent log path follows the convention used by `agent_registry`:
//! `~/.deskd/logs/<agent>.log` (single file). Lines are stored as the
//! agent process writes them — typically tracing's default formatter
//! with ANSI escapes; the renderer strips ANSI for readability.
//!
//! When a `session_filter` is provided, only lines whose body contains
//! the exact session id substring are returned. This matches the common
//! tracing convention where session ids appear in the structured `[3m`
//! key/value tail of a line.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Hard cap on the number of log lines a single response can carry.
/// Higher values defeat the purpose of the tail-from-end optimisation.
pub const MAX_LOG_LINES: usize = 2_000;

/// Default tail size when no `?lines=` query parameter is supplied.
pub const DEFAULT_LOG_LINES: usize = 200;

/// Block read when walking backward from the end of the file.
const BLOCK: usize = 8 * 1024;

/// Resolve the agent log path under `$HOME/.deskd/logs/<name>.log`.
/// Mirrors the convention used by `agent_registry::log_path` but is
/// re-implemented locally to avoid leaking that private helper.
pub fn agent_log_path(agent: &str) -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home)
        .join(".deskd")
        .join("logs")
        .join(format!("{}.log", agent))
}

/// A single rendered log line. The body has ANSI escape codes stripped so
/// the HTML view can render it inside `<pre>` without leaking control
/// sequences to the browser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    /// Original line text (no trailing newline; ANSI escapes stripped).
    pub body: String,
}

/// Return the last `max_lines` lines from `path`. When `session_filter`
/// is `Some`, only lines whose body contains the exact filter substring
/// are returned. The returned vector is oldest-first (top of file order).
///
/// Capped at [`MAX_LOG_LINES`]; values above the cap are silently clamped
/// down. Missing files return an empty vector rather than an error so the
/// view can render a friendly "no log yet" state.
///
/// Implementation: opens the file, seeks to the end, walks backward in
/// 8 KiB blocks, accumulating complete lines in reverse until enough
/// matching lines are collected or the start of file is reached. NO
/// `BufReader::lines()` over the whole file.
pub fn tail_log(
    path: &Path,
    max_lines: usize,
    session_filter: Option<&str>,
) -> Result<Vec<LogLine>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let cap = max_lines.min(MAX_LOG_LINES);
    if cap == 0 {
        return Ok(Vec::new());
    }

    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut pos = file
        .seek(SeekFrom::End(0))
        .with_context(|| format!("seek end {}", path.display()))?;
    if pos == 0 {
        return Ok(Vec::new());
    }

    // Lines accumulated newest-first; we'll reverse before returning.
    let mut collected: Vec<String> = Vec::with_capacity(cap);
    // Remainder from the front of the most recently read block (incomplete
    // first line). Prepended to the next block read further back.
    let mut remainder: Vec<u8> = Vec::new();

    while pos > 0 && collected.len() < cap {
        let read_size = BLOCK.min(pos as usize);
        pos -= read_size as u64;
        file.seek(SeekFrom::Start(pos))
            .with_context(|| format!("seek {}", path.display()))?;
        let mut buf = vec![0u8; read_size];
        file.read_exact(&mut buf)
            .with_context(|| format!("read {}", path.display()))?;
        buf.extend_from_slice(&remainder);
        remainder.clear();

        // Walk the block from the end, splitting on '\n'. The bytes before
        // the first newline (from the start of `buf`) become the new
        // remainder — they may join with the next block read further back.
        let mut end = buf.len();
        // Skip a trailing '\n' so an EOL at EOF doesn't yield an empty
        // line on the first iteration.
        if pos == 0 && end > 0 && buf[end - 1] != b'\n' {
            // File ends without a trailing newline — the final partial
            // line is still a real line; fall through.
        }
        loop {
            let mut newline_pos: Option<usize> = None;
            // Skip past trailing newline if present.
            let scan_end = if end > 0 && buf[end - 1] == b'\n' {
                end - 1
            } else {
                end
            };
            for i in (0..scan_end).rev() {
                if buf[i] == b'\n' {
                    newline_pos = Some(i);
                    break;
                }
            }
            match newline_pos {
                Some(nl) => {
                    let line_bytes = &buf[nl + 1..scan_end];
                    push_if_matches(line_bytes, session_filter, &mut collected, cap);
                    if collected.len() >= cap {
                        break;
                    }
                    end = nl;
                }
                None => {
                    // No more newlines in this block. If we've reached the
                    // start of the file, the remaining bytes form the
                    // first line of the file. Otherwise, they are an
                    // incomplete prefix to merge with the previous block.
                    if pos == 0 {
                        if scan_end > 0 {
                            let line_bytes = &buf[0..scan_end];
                            push_if_matches(line_bytes, session_filter, &mut collected, cap);
                        }
                    } else {
                        remainder = buf[0..end].to_vec();
                    }
                    break;
                }
            }
        }
    }

    collected.reverse();
    Ok(collected.into_iter().map(|s| LogLine { body: s }).collect())
}

fn push_if_matches(
    line_bytes: &[u8],
    session_filter: Option<&str>,
    collected: &mut Vec<String>,
    cap: usize,
) {
    if collected.len() >= cap {
        return;
    }
    let cleaned = strip_ansi(line_bytes);
    if cleaned.is_empty() {
        return;
    }
    if let Some(filter) = session_filter
        && !filter.is_empty()
        && !cleaned.contains(filter)
    {
        return;
    }
    collected.push(cleaned);
}

/// Strip ANSI CSI escape sequences (e.g. `\x1b[32m`) from a byte slice
/// and return a UTF-8 string with any malformed bytes replaced. Tracing's
/// default formatter wraps every field in colour escapes; we drop them so
/// the HTML view doesn't render `[32m INFO` as-is.
fn strip_ansi(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1b && i + 1 < bytes.len() && bytes[i + 1] == b'[' {
            // CSI: skip until a final byte in 0x40..=0x7e.
            i += 2;
            while i < bytes.len() {
                let b = bytes[i];
                i += 1;
                if (0x40..=0x7e).contains(&b) {
                    break;
                }
            }
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(path: &Path, contents: &str) {
        let mut f = File::create(path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
    }

    #[test]
    fn tail_log_returns_empty_for_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("missing.log");
        let lines = tail_log(&p, 200, None).unwrap();
        assert!(lines.is_empty());
    }

    #[test]
    fn tail_log_returns_empty_for_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("empty.log");
        write(&p, "");
        let lines = tail_log(&p, 200, None).unwrap();
        assert!(lines.is_empty());
    }

    #[test]
    fn tail_log_returns_all_lines_when_small_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("small.log");
        write(&p, "alpha\nbeta\ngamma\n");
        let lines = tail_log(&p, 200, None).unwrap();
        let bodies: Vec<&str> = lines.iter().map(|l| l.body.as_str()).collect();
        assert_eq!(bodies, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn tail_log_handles_file_without_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("no-trail.log");
        write(&p, "alpha\nbeta\ngamma");
        let lines = tail_log(&p, 200, None).unwrap();
        let bodies: Vec<&str> = lines.iter().map(|l| l.body.as_str()).collect();
        assert_eq!(bodies, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn tail_log_returns_only_last_n_lines() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("big.log");
        let mut contents = String::new();
        for i in 0..50 {
            contents.push_str(&format!("line{}\n", i));
        }
        write(&p, &contents);
        let lines = tail_log(&p, 5, None).unwrap();
        let bodies: Vec<&str> = lines.iter().map(|l| l.body.as_str()).collect();
        assert_eq!(
            bodies,
            vec!["line45", "line46", "line47", "line48", "line49"]
        );
    }

    #[test]
    fn tail_log_handles_file_larger_than_block_size() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("multi-block.log");
        // Each line padded to ~100 bytes; 1000 lines = ~100 KiB, ~12 blocks.
        let mut contents = String::new();
        for i in 0..1000 {
            contents.push_str(&format!(
                "line{:04} padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding\n",
                i
            ));
        }
        write(&p, &contents);
        let lines = tail_log(&p, 3, None).unwrap();
        let bodies: Vec<String> = lines.iter().map(|l| l.body.clone()).collect();
        assert_eq!(bodies.len(), 3);
        assert!(bodies[0].starts_with("line0997"));
        assert!(bodies[1].starts_with("line0998"));
        assert!(bodies[2].starts_with("line0999"));
    }

    #[test]
    fn tail_log_filters_by_session_substring() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sessioned.log");
        write(
            &p,
            "2026-01-01 INFO no-session here\n\
             2026-01-01 INFO session=abc123 hello\n\
             2026-01-01 INFO session=xyz999 other\n\
             2026-01-01 INFO session=abc123 world\n",
        );
        let lines = tail_log(&p, 200, Some("session=abc123")).unwrap();
        let bodies: Vec<&str> = lines.iter().map(|l| l.body.as_str()).collect();
        assert_eq!(bodies.len(), 2);
        assert!(bodies[0].contains("hello"));
        assert!(bodies[1].contains("world"));
    }

    #[test]
    fn tail_log_strips_ansi_csi_escape_sequences() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ansi.log");
        // \x1b[2m, \x1b[33m, \x1b[0m — typical tracing formatter output.
        write(&p, "\x1b[2m2026-01-01\x1b[0m \x1b[33mWARN\x1b[0m hello\n");
        let lines = tail_log(&p, 10, None).unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].body, "2026-01-01 WARN hello");
    }

    #[test]
    fn tail_log_clamps_max_lines_to_hard_cap() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("clamp.log");
        let mut contents = String::new();
        for i in 0..50 {
            contents.push_str(&format!("line{}\n", i));
        }
        write(&p, &contents);
        // Asking for 50_000 must yield at most MAX_LOG_LINES (and in this
        // small file, just 50).
        let lines = tail_log(&p, 50_000, None).unwrap();
        assert_eq!(lines.len(), 50);
    }

    #[test]
    fn tail_log_empty_filter_string_returns_everything() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("empty-filter.log");
        write(&p, "alpha\nbeta\n");
        let lines = tail_log(&p, 10, Some("")).unwrap();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn strip_ansi_passes_plain_ascii_unchanged() {
        assert_eq!(strip_ansi(b"hello world"), "hello world");
    }

    #[test]
    fn strip_ansi_handles_multibyte_utf8() {
        // The middle dot in the tracing formatter is U+00B7.
        let s = strip_ansi("\x1b[2m·\x1b[0m end".as_bytes());
        assert_eq!(s, "· end");
    }
}
