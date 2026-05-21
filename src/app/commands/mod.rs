//! Command handlers — one module per subcommand group.

pub mod a2a;
pub mod agent;
pub mod bus;
pub mod context;
pub mod dashboard;
pub mod doctor;
pub mod graph;
pub mod reload;
pub mod remind;
pub mod restart;
pub mod schedule;
pub mod session;
pub mod sm;
pub mod status;
pub mod task;
pub mod tui;
pub mod upgrade;
pub mod usage;

/// Truncate a string to a maximum length, appending an ellipsis if truncated.
pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &s[..end])
    }
}

/// Format a chrono::Duration as a human-readable relative time string (e.g. "5m", "2h", "3d").
pub fn format_relative_time(dur: chrono::Duration) -> String {
    let secs = dur.num_seconds();
    if secs < 0 {
        return "now".to_string();
    }
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86400)
    }
}

/// Parse a simple duration string into total seconds.
///
/// Supported formats:
///   `30m`    → 1800 seconds
///   `1h`     → 3600 seconds
///   `2h30m`  → 9000 seconds
///   `90s`    → 90 seconds
///   Combined forms: `1h30m`, `2h15m30s`, etc.
pub fn parse_duration_secs(s: &str) -> anyhow::Result<u64> {
    let mut total: u64 = 0;
    let mut current_num = String::new();
    let mut found_any = false;

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current_num.push(ch);
        } else {
            let n: u64 = if current_num.is_empty() {
                anyhow::bail!("expected number before '{}' in duration '{}'", ch, s)
            } else {
                current_num
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid number in duration '{}'", s))?
            };
            current_num.clear();

            match ch {
                'd' => {
                    total += n * 86400;
                    found_any = true;
                }
                'h' => {
                    total += n * 3600;
                    found_any = true;
                }
                'm' => {
                    total += n * 60;
                    found_any = true;
                }
                's' => {
                    total += n;
                    found_any = true;
                }
                other => {
                    anyhow::bail!(
                        "unknown unit '{}' in duration '{}' (use d, h, m, s)",
                        other,
                        s
                    )
                }
            }
        }
    }

    if !current_num.is_empty() {
        anyhow::bail!(
            "trailing number '{}' without unit in duration '{}' (use h, m, s)",
            current_num,
            s
        );
    }
    if !found_any {
        anyhow::bail!(
            "empty or invalid duration '{}' — expected e.g. 30m, 1h, 2h30m, 90s",
            s
        );
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration_secs("90s").unwrap(), 90);
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration_secs("30m").unwrap(), 30 * 60);
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_secs("1h").unwrap(), 3600);
    }

    #[test]
    fn test_parse_duration_combined() {
        assert_eq!(parse_duration_secs("2h30m").unwrap(), 2 * 3600 + 30 * 60);
    }

    #[test]
    fn test_parse_duration_full() {
        assert_eq!(
            parse_duration_secs("1h15m30s").unwrap(),
            3600 + 15 * 60 + 30
        );
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration_secs("5d").unwrap(), 5 * 86400);
    }

    #[test]
    fn test_parse_duration_days_and_hours() {
        assert_eq!(parse_duration_secs("7d12h").unwrap(), 7 * 86400 + 12 * 3600);
    }

    #[test]
    fn test_parse_duration_invalid_unit() {
        assert!(parse_duration_secs("5w").is_err());
    }

    #[test]
    fn test_parse_duration_empty() {
        assert!(parse_duration_secs("").is_err());
    }

    #[test]
    fn test_parse_duration_trailing_number() {
        assert!(parse_duration_secs("30").is_err());
    }
}
