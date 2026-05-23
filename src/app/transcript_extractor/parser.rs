//! JSONL → [`TranscriptEvent`] derivation (#495).
//!
//! Stateful per-file parser. As assistant / user / error lines come in,
//! we maintain per-session aggregates (tokens, iterations, start time,
//! last seen model) and emit the structured events described in the
//! ticket.
//!
//! ## Event derivation rules
//!
//! - `session.start` — first assistant line we see per `session_id`,
//!   carrying that line's `message.model` field.
//! - `model.switch` — subsequent assistant line whose `message.model`
//!   differs from the last model we recorded for the session.
//! - `limit.hit` — any line with `isApiErrorMessage: true` OR a non-empty
//!   top-level `error` field. Classification via [`patterns::classify_limit`].
//! - `session.end` — emitted alongside a terminal `limit.hit`
//!   (`rate_429` / `weekly_cap` — Claude Code halts the session on
//!   those). The accumulated aggregates are bundled into the event.
//!   Other end-of-session signals are out of scope for v1 (see the
//!   sibling panel ticket for richer detection).
//!
//! Lines without a `sessionId`, or lines we don't recognise, are
//! silently skipped — the daemon advances the byte cursor regardless so
//! a stuck malformed line never blocks the tail.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::events::{LimitKind, TranscriptEvent};
use super::patterns;

/// Per-session accumulator. Lives in a [`SessionTracker`] keyed by
/// `session_id`.
#[derive(Debug, Clone)]
struct SessionAgg {
    /// Model recorded by the most recent assistant line. Used to detect
    /// `model.switch`.
    last_model: String,
    /// Wall-clock start of the session (UTC).
    started_at: DateTime<Utc>,
    /// Cumulative input tokens (sum of `usage.input_tokens` +
    /// `usage.cache_creation_input_tokens` + `usage.cache_read_input_tokens`).
    tokens_in: u64,
    /// Cumulative output tokens.
    tokens_out: u64,
    /// Number of assistant lines observed.
    iterations: u64,
    /// Optional cost (if any line carries an explicit `cost_usd` field —
    /// none do in Claude Code today, but the field is reserved).
    cost_usd: f64,
    /// True once a `session.end` has been emitted for this session — we
    /// don't want to double-emit if more error lines appear after.
    ended: bool,
}

impl SessionAgg {
    fn new(started_at: DateTime<Utc>, model: String) -> Self {
        Self {
            last_model: model,
            started_at,
            tokens_in: 0,
            tokens_out: 0,
            iterations: 0,
            cost_usd: 0.0,
            ended: false,
        }
    }
}

/// Per-file parser state: a map of session_id → aggregates.
///
/// One [`SessionTracker`] is owned per watched JSONL file. Internally
/// it's just a `HashMap` — no IO, no locking — so the tail loop holds it
/// directly without arc/mutex.
#[derive(Debug, Default)]
pub struct SessionTracker {
    sessions: HashMap<String, SessionAgg>,
}

impl SessionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of tracked sessions. Used in tests.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// True if no sessions are tracked. Companion to [`len`] (clippy).
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Parse one JSONL line and emit any derived events. Lines that
    /// fail JSON parse return an empty vector so the daemon can advance
    /// past them.
    pub fn parse_line(&mut self, agent: &str, line: &str) -> Vec<TranscriptEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let v: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };
        self.parse_value(agent, &v)
    }

    /// Parse a pre-decoded JSON value. Useful in tests so we don't have
    /// to stringify and reparse.
    pub fn parse_value(&mut self, agent: &str, v: &Value) -> Vec<TranscriptEvent> {
        let mut out = Vec::new();

        let session_id = match v.get("sessionId").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return out, // No session id → nothing to attribute to.
        };

        let ts = v
            .get("timestamp")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| Utc::now().to_rfc3339());

        let is_error = is_error_line(v);
        let line_type = v.get("type").and_then(Value::as_str).unwrap_or("");

        // ─── error / limit.hit path ─────────────────────────────────────
        if is_error {
            let error_code = v.get("error").and_then(Value::as_str);
            let detail = error_detail(v);
            let kind = patterns::classify_limit(error_code, &detail);
            out.push(TranscriptEvent::LimitHit {
                ts: ts.clone(),
                agent: agent.to_string(),
                session_id: session_id.clone(),
                limit_kind: kind.clone(),
                detail: truncate_detail(&detail),
            });

            // Terminal limit kinds also end the session.
            if matches!(kind, LimitKind::Rate429 | LimitKind::WeeklyCap)
                && let Some(end) = self.finalize_internal(agent, &session_id, &ts)
            {
                out.push(end);
            }
            return out;
        }

        // ─── assistant path ─────────────────────────────────────────────
        if line_type == "assistant" {
            let model = v
                .pointer("/message/model")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let started_at = parse_ts(&ts);
            let agg = self
                .sessions
                .entry(session_id.clone())
                .or_insert_with(|| SessionAgg::new(started_at, model.clone()));

            if agg.iterations == 0 && !model.is_empty() {
                // First assistant line for this session → emit session.start.
                out.push(TranscriptEvent::SessionStart {
                    ts: ts.clone(),
                    agent: agent.to_string(),
                    session_id: session_id.clone(),
                    model: model.clone(),
                });
            } else if !model.is_empty() && model != agg.last_model {
                out.push(TranscriptEvent::ModelSwitch {
                    ts: ts.clone(),
                    agent: agent.to_string(),
                    session_id: session_id.clone(),
                    from: agg.last_model.clone(),
                    to: model.clone(),
                });
                agg.last_model = model;
            }

            // Accumulate usage for the eventual session.end.
            let usage = v.pointer("/message/usage");
            if let Some(u) = usage {
                agg.tokens_in += u.get("input_tokens").and_then(Value::as_u64).unwrap_or(0)
                    + u.get("cache_creation_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                    + u.get("cache_read_input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                agg.tokens_out += u.get("output_tokens").and_then(Value::as_u64).unwrap_or(0);
            }
            agg.iterations += 1;
        }

        out
    }

    /// Explicitly mark a session as ended (e.g. fsnotify reported the
    /// file removed). Returns the `session.end` event with aggregates,
    /// or `None` if the session was never seen / already ended.
    pub fn finalize(&mut self, agent: &str, session_id: &str, ts: &str) -> Option<TranscriptEvent> {
        self.finalize_internal(agent, session_id, ts)
    }

    fn finalize_internal(
        &mut self,
        agent: &str,
        session_id: &str,
        ts: &str,
    ) -> Option<TranscriptEvent> {
        let agg = self.sessions.get_mut(session_id)?;
        if agg.ended {
            return None;
        }
        agg.ended = true;
        let ended_at = parse_ts(ts);
        let duration_s = (ended_at - agg.started_at).num_seconds().max(0) as u64;
        Some(TranscriptEvent::SessionEnd {
            ts: ts.to_string(),
            agent: agent.to_string(),
            session_id: session_id.to_string(),
            tokens_in: agg.tokens_in,
            tokens_out: agg.tokens_out,
            cost_usd: agg.cost_usd,
            iterations: agg.iterations,
            duration_s,
        })
    }
}

/// True when the JSONL line represents an API error or carries an
/// explicit top-level `error` field.
fn is_error_line(v: &Value) -> bool {
    if v.get("isApiErrorMessage").and_then(Value::as_bool) == Some(true) {
        return true;
    }
    if let Some(err) = v.get("error").and_then(Value::as_str)
        && !err.is_empty()
    {
        return true;
    }
    false
}

/// Extract a human-readable detail string from an error line: prefer
/// the first text content block of the synthetic assistant message,
/// fall back to the top-level `error` field.
fn error_detail(v: &Value) -> String {
    // assistant `message.content[0].text` is where Claude Code's
    // synthetic error text lives ("You're out of extra usage · …").
    if let Some(arr) = v.pointer("/message/content").and_then(Value::as_array) {
        for block in arr {
            if let Some(t) = block.get("text").and_then(Value::as_str)
                && !t.is_empty()
            {
                return t.to_string();
            }
        }
    }
    if let Some(err) = v.get("error").and_then(Value::as_str) {
        return err.to_string();
    }
    String::new()
}

/// Truncate the detail so the bus / event store don't carry MB-sized
/// stack traces. 512 bytes is plenty for human triage.
fn truncate_detail(s: &str) -> String {
    const MAX: usize = 512;
    if s.len() <= MAX {
        s.to_string()
    } else {
        let mut t = s[..MAX].to_string();
        t.push('…');
        t
    }
}

/// Parse a timestamp string. Tolerates RFC 3339 with `Z`, `+00:00`, or
/// no timezone. Falls back to `now` on parse failure so aggregates
/// never silently break.
fn parse_ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ts(s: &str) -> String {
        s.to_string()
    }

    fn assistant_line(session_id: &str, model: &str, timestamp: &str) -> Value {
        json!({
            "type": "assistant",
            "sessionId": session_id,
            "timestamp": timestamp,
            "message": {
                "role": "assistant",
                "model": model,
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "cache_creation_input_tokens": 100,
                    "cache_read_input_tokens": 50
                }
            }
        })
    }

    fn error_line(session_id: &str, text: &str, err_code: &str, timestamp: &str) -> Value {
        json!({
            "type": "assistant",
            "isApiErrorMessage": true,
            "error": err_code,
            "sessionId": session_id,
            "timestamp": timestamp,
            "message": {
                "role": "assistant",
                "model": "<synthetic>",
                "content": [{"type": "text", "text": text}]
            }
        })
    }

    #[test]
    fn first_assistant_line_emits_session_start() {
        let mut t = SessionTracker::new();
        let line = assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:00Z");
        let out = t.parse_value("dev", &line);
        assert_eq!(out.len(), 1);
        match &out[0] {
            TranscriptEvent::SessionStart {
                ts,
                agent,
                session_id,
                model,
            } => {
                assert_eq!(ts, "2026-05-22T05:30:00Z");
                assert_eq!(agent, "dev");
                assert_eq!(session_id, "S1");
                assert_eq!(model, "claude-opus-4-7");
            }
            other => panic!("expected SessionStart, got {:?}", other),
        }
    }

    #[test]
    fn second_assistant_line_does_not_re_emit_start() {
        let mut t = SessionTracker::new();
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:00Z"),
        );
        let out = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:10Z"),
        );
        assert!(
            out.is_empty(),
            "should not emit any event for same-model continuation"
        );
    }

    #[test]
    fn different_model_emits_model_switch() {
        let mut t = SessionTracker::new();
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-sonnet-4-6", "2026-05-22T05:30:00Z"),
        );
        let out = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:30Z"),
        );
        assert_eq!(out.len(), 1);
        match &out[0] {
            TranscriptEvent::ModelSwitch { from, to, .. } => {
                assert_eq!(from, "claude-sonnet-4-6");
                assert_eq!(to, "claude-opus-4-7");
            }
            other => panic!("expected ModelSwitch, got {:?}", other),
        }
    }

    #[test]
    fn error_line_emits_limit_hit() {
        let mut t = SessionTracker::new();
        // Seed a session start so subsequent end has aggregates.
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:00Z"),
        );
        let out = t.parse_value(
            "dev",
            &error_line(
                "S1",
                "rate_limit exceeded for model",
                "rate_limit",
                "2026-05-22T05:31:00Z",
            ),
        );
        // Limit hit + session end (rate_limit is terminal).
        assert_eq!(out.len(), 2);
        let kinds: Vec<&str> = out.iter().map(|e| e.kind_label()).collect();
        assert!(kinds.contains(&"limit.hit"));
        assert!(kinds.contains(&"session.end"));
    }

    #[test]
    fn error_other_does_not_emit_session_end() {
        let mut t = SessionTracker::new();
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:00Z"),
        );
        let out = t.parse_value(
            "dev",
            &error_line(
                "S1",
                "weird internal error",
                "internal",
                "2026-05-22T05:31:00Z",
            ),
        );
        // Just a limit.hit (Other), no session.end — the session may
        // continue.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind_label(), "limit.hit");
    }

    #[test]
    fn weekly_cap_terminates_session() {
        let mut t = SessionTracker::new();
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:00Z"),
        );
        let out = t.parse_value(
            "dev",
            &error_line(
                "S1",
                "You're out of extra usage · resets 9pm (UTC)",
                "rate_limit",
                "2026-05-22T05:35:00Z",
            ),
        );
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn lines_without_session_id_are_dropped() {
        let mut t = SessionTracker::new();
        let line = json!({"type":"queue-operation","operation":"enqueue"});
        let out = t.parse_value("dev", &line);
        assert!(out.is_empty());
    }

    #[test]
    fn malformed_json_returns_empty() {
        let mut t = SessionTracker::new();
        let out = t.parse_line("dev", "{not-json");
        assert!(out.is_empty());
        let out = t.parse_line("dev", "");
        assert!(out.is_empty());
    }

    #[test]
    fn finalize_emits_session_end_with_aggregates() {
        let mut t = SessionTracker::new();
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:00Z"),
        );
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:30Z"),
        );
        let end = t
            .finalize("dev", "S1", &ts("2026-05-22T05:31:00Z"))
            .expect("session should finalize");
        match end {
            TranscriptEvent::SessionEnd {
                tokens_in,
                tokens_out,
                iterations,
                duration_s,
                ..
            } => {
                // Two assistant lines × (10 + 100 + 50) input tokens each = 320.
                assert_eq!(tokens_in, 320);
                // Two × 5 output tokens = 10.
                assert_eq!(tokens_out, 10);
                assert_eq!(iterations, 2);
                assert_eq!(duration_s, 60);
            }
            other => panic!("expected SessionEnd, got {:?}", other),
        }
    }

    #[test]
    fn finalize_is_idempotent() {
        let mut t = SessionTracker::new();
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:00Z"),
        );
        let first = t.finalize("dev", "S1", "2026-05-22T05:31:00Z");
        let second = t.finalize("dev", "S1", "2026-05-22T05:32:00Z");
        assert!(first.is_some());
        assert!(second.is_none());
    }

    #[test]
    fn finalize_returns_none_for_unknown_session() {
        let mut t = SessionTracker::new();
        let end = t.finalize("dev", "S-MISSING", "2026-05-22T05:30:00Z");
        assert!(end.is_none());
    }

    #[test]
    fn error_with_only_top_level_error_field_is_detected() {
        let mut t = SessionTracker::new();
        let line = json!({
            "type": "assistant",
            "error": "internal_server_error",
            "sessionId": "S1",
            "timestamp": "2026-05-22T05:30:00Z",
            "message": {"role":"assistant","model":"<synthetic>","content":[
                {"type":"text","text":"oops"}
            ]}
        });
        let out = t.parse_value("dev", &line);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind_label(), "limit.hit");
    }

    #[test]
    fn long_detail_is_truncated() {
        let mut t = SessionTracker::new();
        let big = "x".repeat(2000);
        let line = json!({
            "type": "assistant",
            "isApiErrorMessage": true,
            "error": "rate_limit",
            "sessionId": "S1",
            "timestamp": "2026-05-22T05:30:00Z",
            "message": {"role":"assistant","model":"<synthetic>","content":[
                {"type":"text","text": big}
            ]}
        });
        let out = t.parse_value("dev", &line);
        let limit_hit = out.iter().find(|e| e.kind_label() == "limit.hit").unwrap();
        if let TranscriptEvent::LimitHit { detail, .. } = limit_hit {
            // 512 ASCII chars + ellipsis (3 bytes UTF-8).
            assert!(detail.len() > 510 && detail.len() < 600);
            assert!(detail.ends_with('…'));
        } else {
            panic!("expected LimitHit");
        }
    }

    #[test]
    fn session_id_partitions_state() {
        let mut t = SessionTracker::new();
        let _ = t.parse_value(
            "dev",
            &assistant_line("S1", "claude-opus-4-7", "2026-05-22T05:30:00Z"),
        );
        let out = t.parse_value(
            "dev",
            &assistant_line("S2", "claude-opus-4-7", "2026-05-22T05:31:00Z"),
        );
        // Different session → another SessionStart.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind_label(), "session.start");
        assert_eq!(t.len(), 2);
    }
}
