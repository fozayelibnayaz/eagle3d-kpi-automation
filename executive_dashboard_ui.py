#!/usr/bin/env python3
"""Executive Dashboard UI - Core Business Metrics Only"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def render_executive_dashboard():
    """Render the core business metrics dashboard."""
    try:
        from executive_dashboard import get_core_metrics, get_signup_definition
    except ImportError:
        st.error("executive_dashboard.py not found")
        return

    st.markdown("### 📊 Executive Dashboard — Core Business Metrics")

    # Period selector
    period = st.selectbox("📅 Period", [
        "this_month", "last_month", "this_quarter", "this_year", "last_year",
    ], format_func=lambda x: {
        "this_month":   "This Month",
        "last_month":   "Last Month",
        "this_quarter": "This Quarter",
        "this_year":    "This Year",
        "last_year":    "Last Year",
    }.get(x, x), index=0, key="exec_period")

    with st.spinner("Loading core metrics..."):
        metrics = get_core_metrics(period)

    if metrics.get("error"):
        st.error(metrics["error"])
        return

    st.caption(f"Period: {metrics['period']} ({metrics['period_start']} to {metrics['period_end']})")

    # ── TOP ROW: Revenue + Key Numbers ──
    st.divider()
    r1, r2, r3, r4 = st.columns(4)

    def fmt_delta(pct):
        if pct is None:
            return None
        return f"{'+' if pct >= 0 else ''}{pct:.1f}%"

    r1.metric("💰 Revenue",
              f"${metrics['revenue']:,.2f}",
              fmt_delta(metrics['revenue_pct']),
              help=f"vs {metrics['prev_period']}: ${metrics['prev_revenue']:,.2f}")
    r2.metric("👥 Signups (Verified)",
              f"{metrics['signups']:,}",
              fmt_delta(metrics['signup_pct']),
              help=f"vs {metrics['prev_period']}: {metrics['prev_signups']:,}")
    r3.metric("📤 Project Uploads",
              f"{metrics['uploads']:,}",
              fmt_delta(metrics['upload_pct']),
              help=f"vs {metrics['prev_period']}: {metrics['prev_uploads']:,}")
    r4.metric("💳 Paid Customers",
              f"{metrics['paid']:,}",
              fmt_delta(metrics['paid_pct']),
              help=f"vs {metrics['prev_period']}: {metrics['prev_paid']:,}")

    # ── YoY GROWTH ROW ──
    y1, y2, y3, y4 = st.columns(4)
    y1.metric("📈 Revenue YoY",
              fmt_delta(metrics['yoy_revenue_pct']) or "N/A",
              help=f"Same period last year: ${metrics['yoy_revenue']:,.2f}")
    y2.metric("📈 Signups YoY",
              fmt_delta(metrics['yoy_signup_pct']) or "N/A",
              help=f"Same period last year: {metrics['yoy_signups']:,}")
    y3.metric("🔄 Sign→Upload",
              f"{metrics['s2u_rate']}%",
              help="Conversion rate: signups to first project upload")
    y4.metric("🎯 Sign→Paid",
              f"{metrics['s2p_rate']}%",
              help="Conversion rate: signups to paid customer")

    # ── TOTALS ROW ──
    t1, t2, t3 = st.columns(3)
    t1.metric("💎 Total Revenue (all time)", f"${metrics['total_revenue']:,.2f}")
    t2.metric("💳 Total Paid Customers", f"{metrics['total_paid']:,}")
    t3.metric("📊 Avg Subscription", f"${metrics['avg_subscription']:,.2f}")

    st.divider()

    # ── MONTHLY TREND CHART ──
    trend = metrics.get("monthly_trend", [])
    if trend:
        st.subheader("📈 Monthly Trend (Last 12 Months)")
        df = pd.DataFrame(trend)
        tab1, tab2 = st.tabs(["📊 Chart", "📋 Table"])
        with tab1:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["month"], y=df["signups"], name="Signups", marker_color="#3b82f6"))
            fig.add_trace(go.Bar(x=df["month"], y=df["uploads"], name="Uploads", marker_color="#22c55e"))
            fig.add_trace(go.Bar(x=df["month"], y=df["paid"], name="Paid", marker_color="#f59e0b"))
            fig.add_trace(go.Scatter(x=df["month"], y=df["revenue"], name="Revenue ($)", yaxis="y2",
                                     mode="lines+markers", marker_color="#ef4444"))
            fig.update_layout(
                barmode="group",
                yaxis=dict(title="Count"),
                yaxis2=dict(title="Revenue ($)", overlaying="y", side="right"),
                height=400,
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig, use_container_width=True)
        with tab2:
            display_df = df.copy()
            display_df["revenue"] = display_df["revenue"].apply(lambda x: f"${x:,.2f}")
            st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()

    # ── MARKETING: LEAD SOURCES ──
    st.subheader("📊 Marketing — Lead Sources")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**This Period ({metrics['period']})**")
        sources = metrics.get("lead_sources_period", {})
        if sources:
            df = pd.DataFrame([{"Source": k, "Signups": v} for k, v in sources.items()])
            fig = px.bar(df, x="Source", y="Signups", color="Signups",
                         color_continuous_scale="Blues", text="Signups")
            fig.update_traces(textposition="outside")
            fig.update_layout(xaxis_tickangle=-45, height=350)
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.markdown("**All Time**")
        sources_all = metrics.get("lead_sources_all", {})
        if sources_all:
            df = pd.DataFrame([{"Source": k, "Signups": v} for k, v in list(sources_all.items())[:10]])
            fig = px.pie(df, values="Signups", names="Source", hole=0.4)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── DATA QUALITY SECTION ──
    st.subheader("🔍 Data Quality & Signup Definition")

    dq1, dq2, dq3, dq4 = st.columns(4)
    dq1.metric("�� Total Raw Signups", f"{metrics['total_raw']:,}")
    dq2.metric("✅ Accepted (Clean)", f"{metrics['total_accepted']:,}")
    dq3.metric("❌ Rejected (Spam/Dup)", f"{metrics['total_rejected']:,}")
    dq4.metric("🚫 Spam Rate", f"{metrics['spam_rate']}%")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Rejection Breakdown**")
        reasons = metrics.get("rejection_reasons", {})
        if reasons:
            df = pd.DataFrame([{"Reason": k, "Count": v} for k, v in reasons.items()])
            fig = px.bar(df, x="Reason", y="Count", color="Count",
                         color_continuous_scale="Reds", text="Count")
            fig.update_traces(textposition="outside")
            fig.update_layout(xaxis_tickangle=-45, height=300)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**What Counts as a Signup?**")
        defn = get_signup_definition()
        st.info(f"**Definition:** {defn['definition']}")
        with st.expander("Accepted Criteria"):
            for c in defn["accepted_criteria"]:
                st.write(f"✅ {c}")
        with st.expander("Rejection Reasons"):
            for r in defn["rejected_reasons"]:
                st.write(f"❌ {r}")

    st.caption(f"Internal emails ({metrics['internal_count']} @eagle3d) are excluded from all counts.")
    st.caption(f"Data generated: {metrics['generated_at'][:19]} UTC")
