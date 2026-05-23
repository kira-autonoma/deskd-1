//! Transcript event schema (#495).
//!
//! Pure data types — the 4 event kinds derived from Claude Code session
//! JSONL transcripts. Serialised to JSON for both the bus payload and the
//! `~/.deskd/events/YYYY-MM-DD.jsonl` event store.

use serde::{Deserialize, Serialize};

/// Coarse classification of `limit.hit` events. Conservative: false
/// negatives are acceptable, false positives are not (a misclassified
/// `weekly_cap` would mislead alerting).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    /// HTTP 429 / rate_limit error from the Anthropic API.
    #[serde(rename = "rate_429")]
    Rate429,
    /// Weekly usage cap exhausted ("you're out of extra usage").
    WeeklyCap,
    /// Context window overflow ("context too large" / "exceeded").
    CtxOverflow,
    /// Any other error event — catch-all so nothing is silently dropped.
    Other,
}

/// One transcript event. Tag is `kind` for both serialisation and bus
/// routing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TranscriptEvent {
    #[serde(rename = "session.start")]
    SessionStart {
        ts: String,
        agent: String,
        session_id: String,
        model: String,
    },
    #[serde(rename = "session.end")]
    SessionEnd {
        ts: String,
        agent: String,
        session_id: String,
        tokens_in: u64,
        tokens_out: u64,
        cost_usd: f64,
        iterations: u64,
        duration_s: u64,
    },
    #[serde(rename = "limit.hit")]
    LimitHit {
        ts: String,
        agent: String,
        session_id: String,
        limit_kind: LimitKind,
        detail: String,
    },
    #[serde(rename = "model.switch")]
    ModelSwitch {
        ts: String,
        agent: String,
        session_id: String,
        from: String,
        to: String,
    },
}

impl TranscriptEvent {
    /// Event timestamp (RFC 3339). Used by the daily-rotated event store
    /// to bucket events by Berlin date.
    pub fn ts(&self) -> &str {
        match self {
            Self::SessionStart { ts, .. }
            | Self::SessionEnd { ts, .. }
            | Self::LimitHit { ts, .. }
            | Self::ModelSwitch { ts, .. } => ts,
        }
    }

    /// Agent name.
    pub fn agent(&self) -> &str {
        match self {
            Self::SessionStart { agent, .. }
            | Self::SessionEnd { agent, .. }
            | Self::LimitHit { agent, .. }
            | Self::ModelSwitch { agent, .. } => agent,
        }
    }

    /// Session id.
    pub fn session_id(&self) -> &str {
        match self {
            Self::SessionStart { session_id, .. }
            | Self::SessionEnd { session_id, .. }
            | Self::LimitHit { session_id, .. }
            | Self::ModelSwitch { session_id, .. } => session_id,
        }
    }

    /// Event kind label for tracing / log lines (matches the serde tag).
    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::SessionStart { .. } => "session.start",
            Self::SessionEnd { .. } => "session.end",
            Self::LimitHit { .. } => "limit.hit",
            Self::ModelSwitch { .. } => "model.switch",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialises_session_start_with_kind_tag() {
        let ev = TranscriptEvent::SessionStart {
            ts: "2026-05-22T05:30:00Z".to_string(),
            agent: "dev".to_string(),
            session_id: "01HXY".to_string(),
            model: "claude-opus-4-7".to_string(),
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["kind"], "session.start");
        assert_eq!(json["agent"], "dev");
        assert_eq!(json["model"], "claude-opus-4-7");
    }

    #[test]
    fn serialises_limit_hit_with_snake_case_kind() {
        let ev = TranscriptEvent::LimitHit {
            ts: "2026-05-22T05:30:00Z".to_string(),
            agent: "dev".to_string(),
            session_id: "s".to_string(),
            limit_kind: LimitKind::Rate429,
            detail: "HTTP 429".to_string(),
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["kind"], "limit.hit");
        assert_eq!(json["limit_kind"], "rate_429");
    }

    #[test]
    fn roundtrips_session_end() {
        let ev = TranscriptEvent::SessionEnd {
            ts: "2026-05-22T05:30:00Z".to_string(),
            agent: "dev".to_string(),
            session_id: "s".to_string(),
            tokens_in: 100,
            tokens_out: 50,
            cost_usd: 0.01,
            iterations: 3,
            duration_s: 12,
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: TranscriptEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn accessors_pick_correct_field() {
        let ev = TranscriptEvent::ModelSwitch {
            ts: "T".into(),
            agent: "A".into(),
            session_id: "S".into(),
            from: "X".into(),
            to: "Y".into(),
        };
        assert_eq!(ev.ts(), "T");
        assert_eq!(ev.agent(), "A");
        assert_eq!(ev.session_id(), "S");
        assert_eq!(ev.kind_label(), "model.switch");
    }
}
