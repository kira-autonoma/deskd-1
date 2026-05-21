//! Breadcrumb rendering for the agent drill-down (#485).
//!
//! Server-rendered `<nav aria-label="breadcrumb">` with an `<ol>` of
//! links, one per level in the hierarchy. The last crumb has no `<a>`
//! (it's the current page) per WAI-ARIA convention.

use super::html_escape;

/// One crumb in the breadcrumb trail.
///
/// `href` is `None` for the current page (rendered as plain text). All
/// other crumbs render as anchors so the user can climb back up the
/// hierarchy at any level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Crumb {
    pub label: String,
    pub href: Option<String>,
}

impl Crumb {
    pub fn link(label: impl Into<String>, href: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: Some(href.into()),
        }
    }

    pub fn current(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: None,
        }
    }
}

/// Render the breadcrumb trail. Empty `crumbs` returns an empty string so
/// the caller can drop this block on pages that don't need one.
pub fn breadcrumb(crumbs: &[Crumb]) -> String {
    if crumbs.is_empty() {
        return String::new();
    }
    let mut items = String::new();
    for (i, c) in crumbs.iter().enumerate() {
        let label = html_escape(&c.label);
        let is_last = i + 1 == crumbs.len();
        let item = match (&c.href, is_last) {
            (Some(href), false) => format!(
                r#"<li class="breadcrumb__item"><a href="{href}">{label}</a></li>"#,
                href = html_escape(href),
                label = label,
            ),
            _ => format!(
                r#"<li class="breadcrumb__item breadcrumb__item--current" aria-current="page">{label}</li>"#,
                label = label,
            ),
        };
        items.push_str(&item);
    }
    format!(
        r#"<nav aria-label="breadcrumb" class="breadcrumb"><ol class="breadcrumb__list">{items}</ol></nav>"#,
        items = items,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn breadcrumb_empty_returns_empty_string() {
        assert!(breadcrumb(&[]).is_empty());
    }

    #[test]
    fn breadcrumb_renders_one_li_per_crumb() {
        let html = breadcrumb(&[
            Crumb::link("dashboard", "/"),
            Crumb::link("kira", "/agent/kira"),
            Crumb::current("log"),
        ]);
        assert!(html.contains(r#"aria-label="breadcrumb""#));
        assert_eq!(html.matches("<li").count(), 3);
        assert!(html.contains(r#"<a href="/">dashboard</a>"#));
        assert!(html.contains(r#"<a href="/agent/kira">kira</a>"#));
        // Last crumb is current, no anchor.
        assert!(html.contains(r#"aria-current="page""#));
        // No <a> wrapping the literal "log" label.
        assert!(!html.contains(r#"<a href="log""#));
    }

    #[test]
    fn breadcrumb_escapes_xss_in_label_and_href() {
        let html = breadcrumb(&[Crumb::link("<x>", "/?\"<x>"), Crumb::current("</span>")]);
        assert!(!html.contains("<x>"));
        assert!(!html.contains("</span>"));
        assert!(html.contains("&lt;x&gt;"));
        assert!(html.contains("&lt;/span&gt;"));
    }

    #[test]
    fn breadcrumb_single_crumb_treated_as_current() {
        // Edge case: a breadcrumb of length 1 is the current page only.
        let html = breadcrumb(&[Crumb::link("dashboard", "/")]);
        assert!(html.contains(r#"aria-current="page""#));
        assert!(!html.contains("<a href"));
    }
}
