//! Route handlers for the web adapter (#443).

pub mod agent_detail;
pub mod cost;
pub mod cost_feed;
pub mod dashboard;
pub mod github_webhook;
pub mod health;
pub mod login;
pub mod logout;
pub mod metrics;
pub mod sse;
pub mod static_assets;

use axum::http::HeaderMap;

use crate::app::adapters::web::auth::session::{self, SessionPayload};
use crate::app::adapters::web::state::WebState;

/// Synthetic Telegram id used for the session payload when `trust_transport`
/// is enabled. Audit log entries carrying this id mean «request authorized by
/// the trusted transport, no human Telegram identity».
pub const TRUST_TRANSPORT_TELEGRAM_ID: i64 = 0;

/// Synthetic CSRF token embedded in the trusted-transport session payload.
/// CSRF protection is moot when the network already provides identity (only
/// a trusted client can reach the bind address), so the value is constant
/// and the handlers below accept it unconditionally — see [`csrf_ok_for`].
pub const TRUST_TRANSPORT_CSRF: &str = "trust-transport";

/// Resolve the caller's session.
///
/// When `state.cfg.trust_transport == true` the request is treated as
/// authenticated unconditionally and a synthetic payload is returned. This
/// is safe ONLY because the operator promised that `bind` is on a private
/// interface (Tailscale, VPN, loopback behind an authenticating proxy).
///
/// Otherwise this falls back to verifying the signed `deskd_session` cookie
/// the way the rest of the dashboard always has.
pub fn authenticate(state: &WebState, headers: &HeaderMap) -> Option<SessionPayload> {
    if state.cfg.trust_transport {
        let now = (state.now)();
        return Some(SessionPayload::new(
            TRUST_TRANSPORT_TELEGRAM_ID,
            now,
            // 1 day window — value is irrelevant because we never re-verify
            // this payload via the cookie path.
            86_400,
            TRUST_TRANSPORT_CSRF.to_string(),
        ));
    }
    let now = (state.now)();
    let cookie = read_session_cookie(headers)?;
    session::verify(&cookie, state.secret.as_ref(), now)
}

/// Return true if the supplied CSRF form value matches the session — or if
/// the adapter is in trust_transport mode, in which case CSRF is moot
/// because origin is already constrained by network topology.
pub fn csrf_ok_for(state: &WebState, payload: &SessionPayload, supplied: &str) -> bool {
    if state.cfg.trust_transport {
        return true;
    }
    !supplied.is_empty() && supplied == payload.csrf
}

/// Best-effort client IP extraction. Prefers the leftmost `X-Forwarded-For`
/// hop (set by the reverse proxy) and falls back to the literal connection
/// peer when the header is missing — only useful when deskd is exposed
/// directly, which production deployments must not do.
pub fn client_ip(headers: &HeaderMap, peer: Option<std::net::SocketAddr>) -> String {
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
        && let Some(first) = xff.split(',').next()
    {
        let ip = first.trim();
        if !ip.is_empty() {
            return ip.to_string();
        }
    }
    if let Some(real) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        let s = real.trim();
        if !s.is_empty() {
            return s.to_string();
        }
    }
    match peer {
        Some(addr) => addr.ip().to_string(),
        None => "unknown".to_string(),
    }
}

/// Best-effort User-Agent extraction.
pub fn user_agent(headers: &HeaderMap) -> String {
    headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Read a session cookie value from the `Cookie` header, if any. Looks up
/// the cookie named [`SESSION_COOKIE_NAME`].
pub fn read_session_cookie(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    parse_cookie(raw, SESSION_COOKIE_NAME)
}

/// Name of the session cookie we set after a successful login.
pub const SESSION_COOKIE_NAME: &str = "deskd_session";

/// Locate `name=value` in a `Cookie:` header. Returns the value with leading
/// quote stripped.
fn parse_cookie(raw: &str, name: &str) -> Option<String> {
    for pair in raw.split(';') {
        let pair = pair.trim();
        let (k, v) = pair.split_once('=')?;
        if k.trim() == name {
            return Some(v.trim().trim_matches('"').to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn client_ip_prefers_x_forwarded_for() {
        let mut h = HeaderMap::new();
        h.insert(
            "x-forwarded-for",
            HeaderValue::from_static("203.0.113.5, 10.0.0.1"),
        );
        assert_eq!(client_ip(&h, None), "203.0.113.5");
    }

    #[test]
    fn client_ip_falls_back_to_peer() {
        let h = HeaderMap::new();
        let peer: std::net::SocketAddr = "127.0.0.1:1234".parse().unwrap();
        assert_eq!(client_ip(&h, Some(peer)), "127.0.0.1");
    }

    #[test]
    fn parse_cookie_finds_named_value() {
        assert_eq!(
            parse_cookie("a=1; deskd_session=hello.world; b=2", "deskd_session"),
            Some("hello.world".into())
        );
    }

    #[test]
    fn parse_cookie_missing() {
        assert_eq!(parse_cookie("a=1; b=2", "deskd_session"), None);
    }
}
