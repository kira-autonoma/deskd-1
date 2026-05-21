//! Top-of-dashboard chart rendering (#484).
//!
//! Server-rendered SVG — no client charting library — so the strict CSP
//! from #443 (`script-src 'self'; style-src 'self'`) keeps holding. The
//! chart is a single `<svg viewBox="0 0 480 180">`, one `<polyline>` per
//! agent series, plus a legend below.
//!
//! Width is 100% of the parent and the `viewBox` lets it scale down to
//! a 375px phone viewport without horizontal scroll.

use crate::app::adapters::web::data_chart::{ChartSeries, Metric, Period, series_max};
use crate::app::adapters::web::view::html_escape;

/// Width in SVG user units. Combined with `VIEW_HEIGHT` this fixes the
/// 16:6 aspect ratio called out in the design.
const VIEW_WIDTH: f64 = 480.0;
const VIEW_HEIGHT: f64 = 180.0;
const PADDING_LEFT: f64 = 8.0;
const PADDING_RIGHT: f64 = 8.0;
const PADDING_TOP: f64 = 8.0;
const PADDING_BOTTOM: f64 = 22.0;

/// Render the chart block (switcher form + SVG + legend) as an HTML
/// fragment. Empty `series` → friendly «no data» state.
pub fn chart_block(series: &[ChartSeries], metric: Metric, period: Period) -> String {
    let svg = render_svg(series);
    let switcher = render_switcher(metric, period);
    let legend = render_legend(series);
    format!(
        r#"<section class="chart-block" id="dashboard-chart-block">
  <header class="chart-block__head">
    <h2 class="chart-block__title">Overview · {metric_label} · {period_label}</h2>
  </header>
  {switcher}
  {svg}
  {legend}
</section>"#,
        metric_label = html_escape(metric.label()),
        period_label = html_escape(period.label()),
    )
}

/// Build the `<form method="get">` switcher with metric + period radio
/// groups. No JS — submitting the form re-renders the dashboard with the
/// chosen params.
fn render_switcher(metric: Metric, period: Period) -> String {
    let mut out = String::new();
    out.push_str(r#"<form class="chart-switcher" method="get" action="/">"#);
    out.push_str(r#"  <fieldset class="chart-switcher__group">"#);
    out.push_str(r#"    <legend>Metric</legend>"#);
    for m in [Metric::Spend, Metric::Activity, Metric::Tokens] {
        let checked = if m == metric { " checked" } else { "" };
        out.push_str(&format!(
            r#"    <label><input type="radio" name="metric" value="{q}"{checked}> {label}</label>"#,
            q = m.as_query(),
            label = html_escape(m.label()),
        ));
    }
    out.push_str(r#"  </fieldset>"#);
    out.push_str(r#"  <fieldset class="chart-switcher__group">"#);
    out.push_str(r#"    <legend>Period</legend>"#);
    for p in [Period::Day, Period::Week, Period::Month] {
        let checked = if p == period { " checked" } else { "" };
        out.push_str(&format!(
            r#"    <label><input type="radio" name="period" value="{q}"{checked}> {label}</label>"#,
            q = p.as_query(),
            label = html_escape(p.label()),
        ));
    }
    out.push_str(r#"  </fieldset>"#);
    out.push_str(r#"  <button type="submit" class="chart-switcher__apply">Apply</button>"#);
    out.push_str(r#"</form>"#);
    out
}

/// Render the SVG itself. One `<polyline>` per series, plus axes and
/// «No data» text when the maximum value is zero.
fn render_svg(series: &[ChartSeries]) -> String {
    let max = series_max(series);
    let bucket_count = series.first().map(|s| s.points.len()).unwrap_or(0);

    let mut svg = String::new();
    svg.push_str(&format!(
        r#"<svg class="chart-block__svg" viewBox="0 0 {w} {h}" preserveAspectRatio="none" role="img" aria-label="Aggregate usage chart">"#,
        w = VIEW_WIDTH,
        h = VIEW_HEIGHT,
    ));
    // Plot area background grid (top + bottom axis lines).
    svg.push_str(&format!(
        r#"<line class="chart-axis" x1="{lx}" y1="{ty}" x2="{rx}" y2="{ty}" />"#,
        lx = PADDING_LEFT,
        rx = VIEW_WIDTH - PADDING_RIGHT,
        ty = PADDING_TOP,
    ));
    svg.push_str(&format!(
        r#"<line class="chart-axis" x1="{lx}" y1="{by}" x2="{rx}" y2="{by}" />"#,
        lx = PADDING_LEFT,
        rx = VIEW_WIDTH - PADDING_RIGHT,
        by = VIEW_HEIGHT - PADDING_BOTTOM,
    ));

    if series.is_empty() || max <= 0.0 || bucket_count == 0 {
        svg.push_str(&format!(
            r#"<text class="chart-empty" x="{cx}" y="{cy}" text-anchor="middle">No data in this period</text>"#,
            cx = VIEW_WIDTH / 2.0,
            cy = (VIEW_HEIGHT - PADDING_BOTTOM + PADDING_TOP) / 2.0,
        ));
        svg.push_str(r#"</svg>"#);
        return svg;
    }

    // One polyline per series.
    let plot_w = VIEW_WIDTH - PADDING_LEFT - PADDING_RIGHT;
    let plot_h = VIEW_HEIGHT - PADDING_TOP - PADDING_BOTTOM;
    // Avoid divide-by-zero when there's a single bucket.
    let dx = if bucket_count > 1 {
        plot_w / (bucket_count as f64 - 1.0)
    } else {
        0.0
    };
    for s in series {
        let mut points = String::new();
        for (i, p) in s.points.iter().enumerate() {
            let x = PADDING_LEFT + dx * (i as f64);
            let norm = (p.value / max).clamp(0.0, 1.0);
            let y = PADDING_TOP + plot_h * (1.0 - norm);
            if i > 0 {
                points.push(' ');
            }
            points.push_str(&format!("{:.2},{:.2}", x, y));
        }
        svg.push_str(&format!(
            r#"<polyline class="chart-line" fill="none" stroke="{color}" stroke-width="2" points="{points}" />"#,
            color = html_escape(&s.color),
        ));
    }
    svg.push_str(r#"</svg>"#);
    svg
}

/// Render the legend below the chart: agent name + colour swatch per
/// series. Stable alphabetical order (matches series order).
fn render_legend(series: &[ChartSeries]) -> String {
    if series.is_empty() {
        return String::new();
    }
    let mut out = String::from(r#"<ul class="chart-legend">"#);
    for s in series {
        // Inline `style=` on the swatch is disallowed by CSP. Use an
        // inline `<svg>` square coloured via the SVG `fill=` attribute
        // (CSS forbids inline `style="…"` but SVG presentation
        // attributes are unaffected).
        out.push_str(&format!(
            r#"<li class="chart-legend__item"><svg class="chart-legend__swatch" viewBox="0 0 10 10" aria-hidden="true"><rect width="10" height="10" fill="{color}" /></svg><span class="chart-legend__name">{name}</span></li>"#,
            color = html_escape(&s.color),
            name = html_escape(&s.agent),
        ));
    }
    out.push_str(r#"</ul>"#);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::adapters::web::data_chart::{ChartPoint, ChartSeries};
    use chrono::{Duration, Utc};

    fn series(name: &str, color: &str, values: &[f64]) -> ChartSeries {
        let now = Utc::now();
        let points = values
            .iter()
            .enumerate()
            .map(|(i, v)| ChartPoint {
                bucket_start: now - Duration::hours((values.len() - i) as i64),
                value: *v,
            })
            .collect();
        ChartSeries {
            agent: name.into(),
            color: color.into(),
            points,
        }
    }

    #[test]
    fn empty_series_renders_no_data_text() {
        let html = chart_block(&[], Metric::Spend, Period::Day);
        assert!(html.contains("No data in this period"));
        assert!(html.contains(r#"id="dashboard-chart-block""#));
    }

    #[test]
    fn empty_series_omits_legend() {
        let html = chart_block(&[], Metric::Spend, Period::Day);
        assert!(!html.contains("chart-legend"));
    }

    #[test]
    fn renders_polyline_per_series() {
        let series = vec![
            series("alpha", "#0072b2", &[1.0, 2.0, 3.0]),
            series("beta", "#d55e00", &[3.0, 2.0, 1.0]),
        ];
        let html = chart_block(&series, Metric::Spend, Period::Day);
        assert_eq!(html.matches("<polyline").count(), 2);
        assert!(html.contains("#0072b2"));
        assert!(html.contains("#d55e00"));
    }

    #[test]
    fn switcher_marks_active_metric_and_period() {
        let html = chart_block(&[], Metric::Tokens, Period::Week);
        // Active radio carries the `checked` attribute.
        assert!(html.contains(r#"value="tokens" checked"#));
        assert!(html.contains(r#"value="7d" checked"#));
        // Inactive does not.
        assert!(!html.contains(r#"value="spend" checked"#));
        assert!(!html.contains(r#"value="24h" checked"#));
    }

    #[test]
    fn switcher_form_targets_root() {
        let html = chart_block(&[], Metric::Spend, Period::Day);
        assert!(html.contains(r#"action="/""#));
        assert!(html.contains(r#"method="get""#));
    }

    #[test]
    fn legend_lists_each_agent_name() {
        let series = vec![
            series("alpha", "#0072b2", &[1.0]),
            series("zeta", "#d55e00", &[2.0]),
        ];
        let html = chart_block(&series, Metric::Spend, Period::Day);
        assert!(html.contains("alpha"));
        assert!(html.contains("zeta"));
        // Order in the HTML matches the series order (already sorted).
        let alpha_idx = html.find(">alpha<").unwrap();
        let zeta_idx = html.find(">zeta<").unwrap();
        assert!(alpha_idx < zeta_idx);
    }

    #[test]
    fn svg_has_no_inline_html_style_attr() {
        // Strict CSP forbids HTML `style="…"`. SVG presentation
        // attributes (`fill=`, `stroke=`) are allowed.
        let series = vec![series("a", "#0072b2", &[1.0, 2.0])];
        let html = chart_block(&series, Metric::Spend, Period::Day);
        // `style="` would catch HTML inline styles. Anti-regression.
        assert!(!html.contains(" style=\""), "found inline style: {html}");
    }

    #[test]
    fn escapes_agent_name_in_legend() {
        let series = vec![series("<script>", "#0072b2", &[1.0])];
        let html = chart_block(&series, Metric::Spend, Period::Day);
        assert!(html.contains("&lt;script&gt;"));
        assert!(!html.contains(">script<"));
    }

    #[test]
    fn svg_uses_responsive_viewbox_for_375px_mobile() {
        // AC: «renders correctly on 375px-wide viewport without horizontal
        // scroll». The chart uses a fixed `viewBox` so it scales down via
        // CSS width:100%, and `preserveAspectRatio="none"` lets it fit any
        // parent without cropping.
        let html = chart_block(&[], Metric::Spend, Period::Day);
        assert!(
            html.contains(r#"viewBox="0 0 480 180""#),
            "SVG must declare a fixed viewBox so it scales on mobile"
        );
        assert!(
            html.contains(r#"preserveAspectRatio="none""#),
            "preserveAspectRatio=none lets the SVG fit a 375px viewport"
        );
        // No fixed width/height in pixels on the chart SVG — the
        // stylesheet handles sizing via width:100%.
        assert!(
            !html.contains(r#"<svg class="chart-block__svg" width="#),
            "chart SVG must not pin a pixel width"
        );
    }
}
