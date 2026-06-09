"""
Eagle 3D Traffic Intelligence Hub v3
======================================
COMPLETE REBUILD with fixes:
  - Full date range filter (custom, last month, 3/6 months, comparison)
  - Intelligent source deduplication (Google/google/Google Search → one)
  - GA4 + CRM lead source merging
  - Enterprise theme
  - AI tools section
  - Fixed import for app.py integration
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys, os

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Traffic Intelligence | Eagle 3D",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Module loading with status tracking ──────────────────────
MODULE_STATUS = {}

try:
    from ga4_responsive_css import get_responsive_css, get_chart_config, get_chart_theme
    st.markdown(get_responsive_css(), unsafe_allow_html=True)
    TH = get_chart_theme()
    CHART_CFG = get_chart_config()
    MODULE_STATUS["responsive_css"] = True
except ImportError:
    TH = {"paper_bgcolor": "#0A1628", "plot_bgcolor": "#0A1628", "font_color": "#a0b4cc"}
    CHART_CFG = {"responsive": True}
    MODULE_STATUS["responsive_css"] = False

try:
    from ga4_connector import (
        fetch_utm_traffic, fetch_daily_traffic_summary,
        fetch_page_performance, fetch_event_performance,
        fetch_geo_traffic, fetch_device_traffic,
        fetch_signup_source_correlation, fetch_event_attribution,
        fetch_event_attribution_extended, fetch_available_events,
    )
    MODULE_STATUS["ga4_connector"] = True
except ImportError as e:
    MODULE_STATUS["ga4_connector"] = False
    MODULE_STATUS["ga4_connector_error"] = str(e)

try:
    from kpi_bridge import (
        fetch_daily_kpis, fetch_signup_details,
        calculate_funnel_metrics, attribute_signups_by_lead_source,
        diagnose_sheet,
    )
    MODULE_STATUS["kpi_bridge"] = True
except ImportError as e:
    MODULE_STATUS["kpi_bridge"] = False
    MODULE_STATUS["kpi_bridge_error"] = str(e)

try:
    from ga4_source_intel import (
        classify_source, classify_dataframe,
        filter_real_visitors, get_filtered_summary,
    )
    MODULE_STATUS["source_intel"] = True
except ImportError:
    MODULE_STATUS["source_intel"] = False

try:
    from ga4_smart_qa import answer_free_text_question
    MODULE_STATUS["smart_qa"] = True
except ImportError as e:
    MODULE_STATUS["smart_qa"] = False
    MODULE_STATUS["smart_qa_error"] = str(e)

try:
    from ga4_strategic import get_all_strategic_questions, answer_question
    MODULE_STATUS["strategic"] = True
except ImportError:
    MODULE_STATUS["strategic"] = False

try:
    from ga4_notifications import (
        detect_page_anomalies, detect_event_anomalies,
        detect_source_anomalies, detect_conversion_anomalies,
        build_notification_summary,
    )
    MODULE_STATUS["notifications"] = True
except ImportError:
    MODULE_STATUS["notifications"] = False

try:
    from ga4_intelligence import generate_traffic_analysis, generate_daily_notification
    MODULE_STATUS["intelligence"] = True
except ImportError:
    MODULE_STATUS["intelligence"] = False

try:
    from source_normalizer import normalize_source, normalize_dataframe_sources
    MODULE_STATUS["source_normalizer"] = True
except ImportError:
    MODULE_STATUS["source_normalizer"] = False


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _delta(pct):
    if pct > 0: return f'<span style="color:#00E676">▲ {pct:+.1f}%</span>'
    if pct < 0: return f'<span style="color:#FF5252">▼ {abs(pct):.1f}%</span>'
    return '<span style="color:#666">→ flat</span>'

def _pct(new_v, old_v):
    return round((new_v - old_v) / old_v * 100, 1) if old_v else 0.0

def _safe(df, col):
    return float(df[col].sum()) if not df.empty and col in df.columns else 0.0

# ── COMPREHENSIVE DATE RANGE SYSTEM ────────────────────────

DATE_PRESETS = [
    "Yesterday",
    "Last 7 Days",
    "Last 14 Days",
    "Last 28 Days",
    "Last 30 Days",
    "Last 90 Days",
    "Last 3 Months",
    "Last 6 Months",
    "Last 12 Months",
    "This Month",
    "Last Month",
    "This Year",
    "Last Year",
    "Custom Range",
]

def date_range(opt, custom_start=None, custom_end=None):
    today = datetime.now()
    yd = today - timedelta(days=1)
    
    m = {
        "Yesterday":      (yd, yd),
        "Last 7 Days":    (today - timedelta(days=7), yd),
        "Last 14 Days":   (today - timedelta(days=14), yd),
        "Last 28 Days":   (today - timedelta(days=28), yd),
        "Last 30 Days":   (today - timedelta(days=30), yd),
        "Last 90 Days":   (today - timedelta(days=90), yd),
        "Last 3 Months":  (today - timedelta(days=90), yd),
        "Last 6 Months":  (today - timedelta(days=180), yd),
        "Last 12 Months": (today - timedelta(days=365), yd),
        "This Month":     (today.replace(day=1), yd),
        "This Year":      (today.replace(month=1, day=1), yd),
    }
    
    if opt == "Last Month":
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        m["Last Month"] = (first_prev, last_prev)
    
    if opt == "Last Year":
        ly = today.year - 1
        m["Last Year"] = (datetime(ly, 1, 1), datetime(ly, 12, 31))
    
    if opt == "Custom Range" and custom_start and custom_end:
        return custom_start.strftime("%Y-%m-%d"), custom_end.strftime("%Y-%m-%d")
    
    s, e = m.get(opt, m["Last 7 Days"])
    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")

def prev_period(s, e):
    sd = datetime.strptime(s, "%Y-%m-%d")
    ed = datetime.strptime(e, "%Y-%m-%d")
    d = (ed - sd).days + 1
    pe = sd - timedelta(days=1)
    ps = pe - timedelta(days=d - 1)
    return ps.strftime("%Y-%m-%d"), pe.strftime("%Y-%m-%d")


# ── Data loaders with caching ────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def load_utm(s, e):
    return fetch_utm_traffic(s, e) if MODULE_STATUS["ga4_connector"] else pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def load_daily(s, e):
    return fetch_daily_traffic_summary(s, e) if MODULE_STATUS["ga4_connector"] else pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def load_pages(s, e):
    return fetch_page_performance(s, e) if MODULE_STATUS["ga4_connector"] else pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def load_events(s, e):
    return fetch_event_performance(s, e) if MODULE_STATUS["ga4_connector"] else pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def load_geo(s, e):
    return fetch_geo_traffic(s, e) if MODULE_STATUS["ga4_connector"] else pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def load_dev(s, e):
    return fetch_device_traffic(s, e) if MODULE_STATUS["ga4_connector"] else pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def load_signup(s, e):
    return fetch_signup_source_correlation(s, e) if MODULE_STATUS["ga4_connector"] else pd.DataFrame()

@st.cache_data(ttl=1800, show_spinner=False)
def load_kpi(s, e):
    return fetch_daily_kpis(s, e) if MODULE_STATUS["kpi_bridge"] else pd.DataFrame()

@st.cache_data(ttl=1800, show_spinner=False)
def load_leads(s, e):
    return attribute_signups_by_lead_source(s, e) if MODULE_STATUS["kpi_bridge"] else pd.DataFrame()


# ── Intelligent Source Deduplication for GA4 ─────────────────

def normalize_ga4_sources(utm_df):
    """Normalize GA4 sessionSource values using intelligent dedup."""
    if utm_df is None or utm_df.empty:
        return utm_df
    
    if not MODULE_STATUS["source_normalizer"]:
        return utm_df
    
    df = utm_df.copy()
    normalized = []
    categories = []
    
    for src in df["sessionSource"]:
        canonical, cat = normalize_source(str(src))
        normalized.append(canonical)
        categories.append(cat)
    
    df["source_normalized"] = normalized
    df["source_category"] = categories
    
    return df


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    st.markdown("""
    <div style="border-bottom:2px solid #1B3054;padding-bottom:10px;margin-bottom:16px;">
        <h2 style="color:#00D4FF;margin:0;">🚦 Traffic Intelligence Hub</h2>
        <p style="color:#5B6B85;margin:4px 0 0 0;font-size:0.85rem;">UTM tracking · CRM linking · AI Q&A · Source Deduplication</p>
    </div>
    """, unsafe_allow_html=True)

    if not MODULE_STATUS["ga4_connector"]:
        st.error(f"GA4 module not available: {MODULE_STATUS.get('ga4_connector_error', 'unknown')}")
        return

    with st.sidebar:
        st.markdown("### 🚦 Controls")

        date_opt = st.selectbox(
            "📅 Date Range",
            DATE_PRESETS,
            index=3,
        )
        
        custom_start = None
        custom_end = None
        if date_opt == "Custom Range":
            col_cs1, col_cs2 = st.columns(2)
            with col_cs1:
                custom_start = st.date_input("Start", value=datetime.now().date() - timedelta(days=30))
            with col_cs2:
                custom_end = st.date_input("End", value=datetime.now().date())
        
        s_str, e_str = date_range(date_opt, custom_start, custom_end)
        ps_str, pe_str = prev_period(s_str, e_str)

        st.caption(f"📊 Current: {s_str} → {e_str}")
        st.caption(f"🔄 Compare: {ps_str} → {pe_str}")
        
        dedup_mode = st.toggle("🔗 Smart Source Dedup", value=True,
                               help="Merge Google/Google Search/google → 'Google'")
        st.markdown("---")
        
        st.markdown("**Modules:**")
        for key in ["ga4_connector", "kpi_bridge", "source_intel", "smart_qa", "source_normalizer"]:
            icon = "✅" if MODULE_STATUS.get(key) else "❌"
            st.caption(f"{icon} {key}")

    # Load data
    with st.spinner("Loading traffic data..."):
        utm = normalize_ga4_sources(load_utm(s_str, e_str)) if dedup_mode else load_utm(s_str, e_str)
        p_utm = normalize_ga4_sources(load_utm(ps_str, pe_str)) if dedup_mode else load_utm(ps_str, pe_str)
        daily = load_daily(s_str, e_str)
        p_daily = load_daily(ps_str, pe_str)
        pages = load_pages(s_str, e_str)
        p_pages = load_pages(ps_str, pe_str)
        events = load_events(s_str, e_str)
        p_events = load_events(ps_str, pe_str)
        geo = load_geo(s_str, e_str)
        dev = load_dev(s_str, e_str)
        kpi = load_kpi(s_str, e_str)
        p_kpi = load_kpi(ps_str, pe_str)
        leads = load_leads(s_str, e_str)

    # Overview KPIs
    st.markdown("### 📊 Overview")
    sess = _safe(utm, "sessions") or _safe(utm, "totalUsers")
    users = _safe(utm, "totalUsers")
    conv = _safe(utm, "conversions")
    p_sess = _safe(p_utm, "sessions") or _safe(p_utm, "totalUsers")
    p_conv = _safe(p_utm, "conversions")

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Sessions", f"{int(sess):,}", f"{_pct(sess, p_sess):+.1f}%")
    with k2:
        st.metric("Users", f"{int(users):,}")
    with k3:
        st.metric("Conversions", f"{int(conv):,}", f"{_pct(conv, p_conv):+.1f}%")
    with k4:
        cr = (conv / sess * 100) if sess > 0 else 0
        p_cr = (p_conv / p_sess * 100) if p_sess > 0 else 0
        st.metric("Conv Rate", f"{cr:.2f}%", f"{(cr-p_cr):+.2f}pp")
    with k5:
        funnel = calculate_funnel_metrics(kpi) if MODULE_STATUS["kpi_bridge"] and kpi is not None and not kpi.empty else {}
        st.metric("Sign-ups", funnel.get("signups", 0))

    # Tabs
    tabs = st.tabs([
        "🌐 Sources", "📄 Pages", "⚡ Events", "🗺️ Geo",
        "🎯 Leads", "📊 Funnel", "🔔 Alerts", "🤖 AI", "💡 Q&A"
    ])

    with tabs[0]:
        tab_sources(utm, p_utm, dedup_mode)
    with tabs[1]:
        tab_pages(pages, p_pages)
    with tabs[2]:
        tab_events(events, p_events)
    with tabs[3]:
        tab_geo(geo, dev)
    with tabs[4]:
        tab_leads(leads)
    with tabs[5]:
        tab_funnel(kpi, utm, funnel)
    with tabs[6]:
        tab_alerts(pages, p_pages, events, p_events, utm, p_utm, daily, p_daily)
    with tabs[7]:
        tab_ai(utm, pages, events, daily, kpi)
    with tabs[8]:
        tab_strategic(utm, pages, events, kpi, p_kpi, leads, geo, dev)


def tab_sources(utm, p_utm, dedup_mode):
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">🌐 Traffic Sources</div>', unsafe_allow_html=True)
    if utm.empty:
        st.info("No UTM data for this period")
        return

    src_col = "source_normalized" if "source_normalized" in utm.columns else "sessionSource"
    sessions_col = "sessions" if "sessions" in utm.columns else src_col
    conv_col = "conversions" if "conversions" in utm.columns else None

    by_src = utm.groupby(src_col).agg(
        Sessions=(sessions_col, "sum"),
    ).reset_index().sort_values("Sessions", ascending=False)

    if conv_col:
        conv_agg = utm.groupby(src_col)[conv_col].sum().reset_index()
        by_src = by_src.merge(conv_agg, on=src_col, how="left")
        by_src["Conv_%"] = (by_src[conv_col] / by_src["Sessions"].replace(0, 1) * 100).round(2)
    
    total = by_src["Sessions"].sum()
    by_src["Share_%"] = (by_src["Sessions"] / total * 100).round(1)

    # Source category breakdown
    if "source_category" in utm.columns:
        cats = utm.groupby("source_category").agg(Sessions=(sessions_col, "sum")).reset_index()
        cats = cats.sort_values("Sessions", ascending=False)
        
        cat_cols = st.columns(len(cats))
        for i, (_, row) in enumerate(cats.iterrows()):
            with cat_cols[i % len(cat_cols)]:
                cat_colors = {"search": "#00D4FF", "social": "#6C5CE7", "ai": "#B388FF", "referral": "#FF9100", "direct": "#5B6B85", "community": "#00E676", "email": "#FFD600"}
                color = cat_colors.get(row["source_category"], "#94A3C1")
                share = row["Sessions"] / total * 100 if total > 0 else 0
                st.markdown(f'<div style="background:#111D32;border:1px solid #1B3054;border-radius:10px;padding:10px;text-align:center;"><div style="font-size:0.7rem;color:#5B6B85;text-transform:uppercase;">{row["source_category"]}</div><div style="font-size:1.2rem;font-weight:700;color:{color};">{share:.0f}%</div><div style="font-size:0.7rem;color:#94A3C1;">{int(row["Sessions"]):,} sessions</div></div>', unsafe_allow_html=True)

    fig = px.bar(by_src.head(20), x="Sessions", y=src_col, orientation="h",
                 color="Conv_%" if "Conv_%" in by_src.columns else None,
                 color_continuous_scale="RdYlGn" if "Conv_%" in by_src.columns else None,
                 color_discrete_sequence=["#00D4FF"] if "Conv_%" not in by_src.columns else None)
    fig.update_layout(height=max(400, len(by_src.head(20)) * 25), **TH, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)
    st.dataframe(by_src, use_container_width=True, height=400, hide_index=True)


def tab_pages(pages, p_pages):
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">📄 Pages</div>', unsafe_allow_html=True)
    if pages.empty:
        st.info("No data")
        return

    top = (pages.groupby("pagePath").agg(
        Views=("screenPageViews", "sum"), Sessions=("sessions", "sum"),
        Conversions=("conversions", "sum"),
    ).reset_index().sort_values("Views", ascending=False))
    top["Conv_%"] = ((top["Conversions"] / top["Sessions"].replace(0, 1)) * 100).round(2)
    st.dataframe(top.head(50), use_container_width=True, height=400)

    fig = px.bar(top.head(15), x="Views", y="pagePath", orientation="h",
                 color="Conv_%", color_continuous_scale="RdYlGn")
    fig.update_layout(height=500, **TH, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)


def tab_events(events, p_events):
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">⚡ Events</div>', unsafe_allow_html=True)
    if events.empty:
        st.info("No data")
        return

    signals = ["sign_up", "signup", "form_submit", "purchase", "trial"]
    agg = (events.groupby("eventName").agg(
        Count=("eventCount", "sum"), Conversions=("conversions", "sum"),
    ).reset_index().sort_values("Count", ascending=False))
    agg["Signal"] = agg["eventName"].apply(lambda e: "🎯" if any(s in str(e).lower() for s in signals) else "—")
    st.dataframe(agg, use_container_width=True, height=400)

    fig = px.bar(agg.head(20), x="Count", y="eventName", orientation="h",
                 color="Conversions", color_continuous_scale="Viridis")
    fig.update_layout(height=500, **TH, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)


def tab_geo(geo, dev):
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">🗺️ Geo & Devices</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        if not geo.empty:
            country = geo.groupby("country").agg(Sessions=("sessions", "sum")).reset_index().sort_values("Sessions", ascending=False)
            fig = px.choropleth(country, locations="country", locationmode="country names",
                                color="Sessions", color_continuous_scale="Blues")
            fig.update_layout(height=350, **TH, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)
            st.dataframe(country.head(15), use_container_width=True, height=250)
    with c2:
        if not dev.empty:
            dc = dev.groupby("deviceCategory")["sessions"].sum().reset_index()
            fig = px.pie(dc, names="deviceCategory", values="sessions", hole=0.45,
                         color_discrete_sequence=["#00D4FF", "#6C5CE7", "#FFD600"])
            fig.update_layout(height=350, **TH, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)


def tab_leads(leads):
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">🎯 Lead Sources (CRM)</div>', unsafe_allow_html=True)
    if leads is None or leads.empty:
        st.info("No Lead Source data for this period")
        return
    st.dataframe(leads, use_container_width=True, height=400, hide_index=True)
    if "Signups" in leads.columns:
        fig = px.pie(leads.head(10), values="Signups", names="Lead Source", hole=0.5,
                     color_discrete_sequence=["#00D4FF", "#6C5CE7", "#00E676", "#FFD600", "#FF5252", "#B388FF"])
        fig.update_layout(height=350, **TH, margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
        st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)


def tab_funnel(kpi, utm, funnel):
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">📊 Conversion Funnel</div>', unsafe_allow_html=True)
    if not funnel:
        st.info("No funnel data")
        return

    sessions_col = "sessions" if "sessions" in utm.columns else "totalUsers"
    sess = int(utm[sessions_col].sum()) if not utm.empty else 0
    fig = go.Figure(go.Funnel(
        y=["GA4 Sessions", "Sign-ups", "Uploads", "Paid"],
        x=[sess, funnel.get("signups", 0), funnel.get("uploads", 0), funnel.get("paid", 0)],
        textposition="inside", textinfo="value+percent initial",
        marker=dict(color=["#00D4FF", "#6C5CE7", "#FFD700", "#00E676"]),
    ))
    fig.update_layout(title="Visitor → Customer", height=400, **TH, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True, config=CHART_CFG)

    st.markdown("#### 📅 Daily")
    display = kpi.copy()
    display["date"] = pd.to_datetime(display["date"]).dt.strftime("%b %d")
    display["S→U %"] = (display["first_uploads"] / display["signups"].replace(0, 1) * 100).round(2)
    display["U→P %"] = (display["paid_customers"] / display["first_uploads"].replace(0, 1) * 100).round(2)
    show_cols = [c for c in ["date", "signups", "first_uploads", "paid_customers", "S→U %", "U→P %"] if c in display.columns]
    st.dataframe(display[show_cols], use_container_width=True, height=400)


def tab_alerts(pages, p_pages, events, p_events, utm, p_utm, daily, p_daily):
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">🔔 Alerts</div>', unsafe_allow_html=True)

    if not MODULE_STATUS["notifications"]:
        st.info("Notifications module not loaded")
        return

    page_al = detect_page_anomalies(pages, p_pages)
    event_al = detect_event_anomalies(events, p_events)
    src_al = detect_source_anomalies(utm, p_utm)
    conv_al = detect_conversion_anomalies(daily, p_daily, None, None)

    all_al = conv_al + src_al + event_al + page_al
    summary = build_notification_summary(all_al)

    bc = "#FF4B6E" if summary["has_urgency"] else "#00E676"
    st.markdown(f'<div style="background:{bc}22;border:1px solid {bc};border-radius:10px;padding:16px;margin-bottom:16px"><b style="color:{bc}">{summary.get("emoji", "✅")} {"URGENT" if summary["has_urgency"] else "Healthy"}</b><br><span style="color:#ccc">{summary["summary_line"]}</span></div>', unsafe_allow_html=True)

    for sev in ["critical", "warnings", "positives", "info"]:
        alerts = summary.get(sev, [])
        if alerts:
            st.markdown(f"#### {alerts[0].emoji if alerts else ''} {sev.title()} ({len(alerts)})")
            for a in alerts[:10]:
                st.markdown(f'<div style="background:#111D32;border-left:3px solid {"#FF5252" if sev=="critical" else "#FFD600" if sev=="warnings" else "#00E676"};border-radius:8px;padding:10px 14px;margin:6px 0;"><b>{a.emoji} {a.title}</b><br><span style="color:#ccc">{a.message}</span><br><span style="color:#5B6B85;font-size:0.8rem;">💡 {a.recommendation}</span></div>', unsafe_allow_html=True)

    if not all_al:
        st.success("✅ No anomalies")


def tab_ai(utm, pages, events, daily, kpi):
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">🤖 AI Daily Briefing</div>', unsafe_allow_html=True)

    if not MODULE_STATUS["intelligence"]:
        st.info("AI intelligence module not loaded")
        return

    c1, c2 = st.columns(2)
    with c1:
        if st.button("🤖 Generate Analysis", type="primary", use_container_width=True):
            with st.spinner("Analyzing..."):
                signups_ext = int(kpi["signups"].sum()) if kpi is not None and not kpi.empty else None
                st.session_state["ai_analysis"] = generate_traffic_analysis(utm, pages, events, signups_ext)
    with c2:
        if st.button("📬 Daily Briefing", use_container_width=True):
            with st.spinner("Generating..."):
                st.session_state["ai_briefing"] = generate_daily_notification(pages, events, utm)

    if "ai_analysis" in st.session_state:
        st.markdown("---")
        st.markdown(st.session_state["ai_analysis"])
    if "ai_briefing" in st.session_state:
        st.markdown("---")
        st.markdown(st.session_state["ai_briefing"])


def tab_strategic(utm, pages, events, kpi, p_kpi, leads, geo, dev):
    """Strategic Q&A with free-text and pre-built questions"""
    st.markdown('<div style="font-size:1.1rem;font-weight:700;color:#00D4FF;border-bottom:2px solid #1B3054;padding-bottom:8px;margin:12px 0 14px 0;">💡 Strategic Q&A</div>', unsafe_allow_html=True)

    st.markdown("#### 🤖 Ask Your Own Question")
    st.caption("Searches GA4 + CRM signups + uploads + paid + lead sources")

    if not MODULE_STATUS["smart_qa"]:
        st.warning(f"Q&A engine not loaded: {MODULE_STATUS.get('smart_qa_error', 'unknown')}")
    else:
        with st.expander("💭 Example questions", expanded=False):
            st.markdown("""
            **Numbers:** How many signups? · What is the conversion rate? · How many paid?
            **Sources:** Where are signups coming from? · Google traffic details · Why is direct traffic high?
            **Strategy:** How to rank in AI search? · Backlink strategy? · Predict next week
            """)

        user_question = st.text_input("Your question:", placeholder="e.g., How many signups from Google?",
                                      key="user_question")

        if st.button("🚀 Get Answer", type="primary", key="qa_btn"):
            if user_question:
                with st.spinner("Analyzing..."):
                    try:
                        answer = answer_free_text_question(
                            user_question, utm_df=utm, pages_df=pages, events_df=events,
                            geo_df=geo, dev_df=dev, kpi_df=kpi, p_kpi_df=p_kpi,
                            lead_sources_df=leads,
                        )
                        st.session_state["qa_answer"] = answer
                        st.session_state["qa_q"] = user_question
                    except Exception as e:
                        st.error(f"Error: {e}")

        if "qa_answer" in st.session_state:
            st.markdown("---")
            st.markdown(f"**Q:** {st.session_state.get('qa_q', '')}")
            st.markdown(st.session_state["qa_answer"])

    st.markdown("---")
    st.markdown("#### 📚 Pre-built Strategic Questions")

    if MODULE_STATUS["strategic"]:
        questions = get_all_strategic_questions()
        selected = st.selectbox("Pick a question:", [q[0] for q in questions], key="strat_sel")
        selected_key = next(q[1] for q in questions if q[0] == selected)

        if st.button("🧠 Get Strategic Answer", key="strat_btn"):
            with st.spinner("Analyzing..."):
                answer = answer_question(selected_key, utm_df=utm, pages_df=pages, events_df=events, kpi_df=kpi)
                st.session_state["strat_answer"] = answer

        if "strat_answer" in st.session_state:
            st.markdown("---")
            st.markdown(st.session_state["strat_answer"])


if __name__ == "__main__":
    main()
