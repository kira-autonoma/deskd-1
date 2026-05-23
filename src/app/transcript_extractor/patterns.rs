//! `limit.hit` pattern detection (#495).
//!
//! Conservative classifier: an error message → at most one [`LimitKind`].
//! False negatives are acceptable (we'll log `other`). False positives are
//! not (a misclassified `weekly_cap` would mislead alerting / SLA
//! tracking), so the matchers require multiple anchor terms before
//! claiming a specific category.
//!
//! Matching is case-insensitive on the lowered ASCII form of the message.

use super::events::LimitKind;

/// Classify an error blob into a [`LimitKind`]. The function never
/// returns `None` — every error gets at least the `other` catch-all so
/// nothing is silently dropped from the event stream.
///
/// `error_code` is the raw `error` field on the transcript line (e.g.
/// `rate_limit`); `message` is the human text the synthetic assistant
/// surfaced (e.g. `"You're out of extra usage · resets 9pm (UTC)"`).
pub fn classify_limit(error_code: Option<&str>, message: &str) -> LimitKind {
    let code = error_code.unwrap_or("").to_ascii_lowercase();
    let msg = message.to_ascii_lowercase();

    // rate_429: explicit code wins, otherwise text matches.
    if code == "rate_limit" || code.contains("429") {
        return LimitKind::Rate429;
    }
    if msg.contains("http 429") || msg.contains("rate_limit") || msg.contains("rate limit") {
        return LimitKind::Rate429;
    }

    // weekly_cap: "weekly" + ("limit"|"cap") anywhere in the text.
    if msg.contains("weekly") && (msg.contains("limit") || msg.contains("cap")) {
        return LimitKind::WeeklyCap;
    }
    // Claude Code's text for weekly cap exhaustion. Anchor on both
    // "out of" + "usage" to avoid catching unrelated "usage stats" lines.
    if msg.contains("out of") && msg.contains("usage") {
        return LimitKind::WeeklyCap;
    }

    // ctx_overflow: "context" + one of the overflow indicators.
    if msg.contains("context")
        && (msg.contains("overflow") || msg.contains("too large") || msg.contains("exceeded"))
    {
        return LimitKind::CtxOverflow;
    }

    LimitKind::Other
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_429_from_error_code() {
        let k = classify_limit(Some("rate_limit"), "");
        assert_eq!(k, LimitKind::Rate429);
    }

    #[test]
    fn rate_429_from_http_status() {
        let k = classify_limit(None, "request failed: HTTP 429 Too Many Requests");
        assert_eq!(k, LimitKind::Rate429);
    }

    #[test]
    fn rate_429_from_rate_limit_text() {
        let k = classify_limit(None, "rate_limit exceeded for model");
        assert_eq!(k, LimitKind::Rate429);
    }

    #[test]
    fn weekly_cap_from_weekly_limit() {
        let k = classify_limit(None, "Your weekly limit has been reached");
        assert_eq!(k, LimitKind::WeeklyCap);
    }

    #[test]
    fn weekly_cap_from_weekly_cap() {
        let k = classify_limit(None, "Weekly cap exhausted — resets Monday");
        assert_eq!(k, LimitKind::WeeklyCap);
    }

    #[test]
    fn weekly_cap_from_out_of_extra_usage() {
        // Real text from Claude Code transcripts.
        let k = classify_limit(None, "You're out of extra usage · resets 9pm (UTC)");
        assert_eq!(k, LimitKind::WeeklyCap);
    }

    #[test]
    fn ctx_overflow_too_large() {
        let k = classify_limit(None, "context too large for the model");
        assert_eq!(k, LimitKind::CtxOverflow);
    }

    #[test]
    fn ctx_overflow_exceeded() {
        let k = classify_limit(None, "context window exceeded");
        assert_eq!(k, LimitKind::CtxOverflow);
    }

    #[test]
    fn ctx_overflow_overflow() {
        let k = classify_limit(None, "context overflow detected");
        assert_eq!(k, LimitKind::CtxOverflow);
    }

    #[test]
    fn other_catches_unknown_errors() {
        let k = classify_limit(Some("internal_error"), "something exploded server-side");
        assert_eq!(k, LimitKind::Other);
    }

    #[test]
    fn case_insensitive_matching() {
        let k = classify_limit(Some("RATE_LIMIT"), "");
        assert_eq!(k, LimitKind::Rate429);
        let k = classify_limit(None, "WEEKLY LIMIT REACHED");
        assert_eq!(k, LimitKind::WeeklyCap);
    }

    #[test]
    fn weekly_takes_priority_over_overflow_when_both_match() {
        // Practically rare, but the order in classify_limit guarantees a
        // deterministic answer.
        let k = classify_limit(None, "weekly limit + context exceeded");
        assert_eq!(k, LimitKind::WeeklyCap);
    }
}
