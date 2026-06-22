#!/usr/bin/env python3
"""Customer Success Analytics UI - rich visualizations + tables"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def render_cs_analytics():
    st.markdown('<div class="sec-head">�� Customer Success Analytics</div>', unsafe_allow_html=True)
    st.caption("Deep analysis: streaming time, churn, plans, recurring payments, lifecycle")

    try:
        from customer_success_analytics import get_all_insights
        with st.spinner("Computing insights from 2,950 CS rows..."):
            data = get_all_insights()
    except Exception as e:
        st.error(f"Error: {e}")
        return

    # ── 7 SECTIONS ──
    sections = st.tabs([
        "💔 Churn Analysis",
        "📊 Plan Distribution",
        "⏱️ Streaming Time",
        "💳 Recurring Payments",
        "🎯 Perfect Fit",
        "📅 Subscription Lifecycle",
        "📺 Last Streamed",
    ])

    # ── CHURN ──
    with sections[0]:
        st.subheader("How Many Churned Each Month?")
        ch = data["churn"]
        c1, c2 = st.columns([2, 1])
        with c1:
            if ch["by_month"]:
                df = pd.DataFrame([{"Month": m, "Churned": v} for m, v in ch["by_month"].items()])
                fig = px.bar(df, x="Month", y="Churned", title="Churn by Month",
                             color="Churned", color_continuous_scale="Reds", text="Churned")
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No churn data with end dates found")
        with c2:
            st.metric("Total Churned", ch["total_churned"])
            if ch["by_month"]:
                latest = list(ch["by_month"].items())[-1]
                st.metric(f"Latest ({latest[0]})", latest[1])
                avg = sum(ch["by_month"].values()) / len(ch["by_month"])
                st.metric("Avg per month", f"{avg:.1f}")

    # ── PLANS ──
    with sections[1]:
        st.subheader("Which Plan is Most Popular? Which Plan Churns Most?")
        plans = data["plans"]

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**📊 All Customers by Plan**")
            df = pd.DataFrame([{"Plan": p, "Count": c} for p, c in plans["total_by_plan"].items()])
            if not df.empty:
                fig = px.pie(df, values="Count", names="Plan", title="Plan Popularity")
                st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.markdown("**❌ Churned by Plan**")
            df = pd.DataFrame([{"Plan": p, "Churned": c} for p, c in plans["churned_by_plan"].items()])
            if not df.empty:
                fig = px.bar(df, x="Plan", y="Churned", color="Churned", color_continuous_scale="Reds")
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("**📈 Churn Rate by Plan (%)**")
        rate_df = pd.DataFrame([
            {"Plan": p, "Total": plans["total_by_plan"].get(p, 0),
             "Active": plans["active_by_plan"].get(p, 0),
             "Churned": plans["churned_by_plan"].get(p, 0),
             "Churn Rate %": r}
            for p, r in plans["churn_rate_by_plan"].items()
        ]).sort_values("Churn Rate %", ascending=False)
        st.dataframe(rate_df, use_container_width=True, hide_index=True)
        if not rate_df.empty:
            worst = rate_df.iloc[0]
            st.error(f"⚠️ **{worst['Plan']}** has the highest churn rate at **{worst['Churn Rate %']}%** ({worst['Churned']} of {worst['Total']})")

    # ── STREAMING TIME ──
    with sections[2]:
        st.subheader("Streaming Time Analysis")
        stream = data["streaming"]
        s1, s2, s3 = st.columns(3)
        s1.metric("Total Stream Hours", f"{stream['total_stream_time']:,.1f}")
        s2.metric("Users Streaming", f"{stream['users_with_streaming']:,}")
        s3.metric("Avg per User", f"{stream['avg_stream_time']:,.1f}h")

        if stream["top_streamers"]:
            st.markdown("**🏆 Top 20 Streamers**")
            df_top = pd.DataFrame(stream["top_streamers"])
            st.dataframe(df_top, use_container_width=True, hide_index=True)
            fig = px.bar(df_top.head(15), x="email", y="stream_time",
                         title="Top 15 Streamers by Hours",
                         color="stream_time", color_continuous_scale="Viridis")
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    # ── RECURRING PAYMENTS ──
    with sections[3]:
        st.subheader("Recurring Payment Analysis")
        rec = data["recurring"]
        r1, r2, r3 = st.columns(3)
        r1.metric("Total Paying Customers", rec["total_paying_customers"])
        r2.metric("Total Recurring Payments", rec["total_recurring_payments"])
        r3.metric("Avg per Customer", f"{rec['avg_recurring']}")

        st.markdown("**💎 Most Loyal Customers (6+ recurring payments)**")
        if rec["loyal_customers"]:
            df_loyal = pd.DataFrame(rec["loyal_customers"])
            st.dataframe(df_loyal, use_container_width=True, hide_index=True)

        st.markdown("**📊 Top 30 by Recurring Payment Count**")
        df_top = pd.DataFrame(rec["top_recurring"])
        st.dataframe(df_top, use_container_width=True, hide_index=True)

        st.markdown(f"**⚠️ One-time Customers ({len(rec['one_time_customers'])})** - paid once only")
        with st.expander("Show one-time customers"):
            if rec["one_time_customers"]:
                st.dataframe(pd.DataFrame(rec["one_time_customers"]), use_container_width=True, hide_index=True)

    # ── PERFECT FIT ──
    with sections[4]:
        st.subheader("Perfect Fit Customer & Demographics")
        pf = data["perfect_fit"]

        if pf["perfect_fit"]:
            st.markdown("**🎯 Perfect Fit Customer**")
            df = pd.DataFrame([{"Fit": k, "Count": v} for k, v in pf["perfect_fit"].items()])
            fig = px.pie(df, values="Count", names="Fit")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)

        if pf["company_size"]:
            st.markdown("**🏢 Company Size Distribution**")
            df = pd.DataFrame([{"Size": k, "Count": v} for k, v in pf["company_size"].items()])
            fig = px.bar(df, x="Size", y="Count", color="Count", color_continuous_scale="Blues")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)

        if pf["industries"]:
            st.markdown("**🏭 Industries**")
            df = pd.DataFrame([{"Industry": k, "Count": v} for k, v in pf["industries"].items()])
            st.dataframe(df, use_container_width=True, hide_index=True)

    # ── SUBSCRIPTION LIFECYCLE ──
    with sections[5]:
        st.subheader("Subscription Lifecycle")
        sub = data["subscriptions"]
        s1, s2, s3 = st.columns(3)
        s1.metric("🆕 New This Month", sub["count_new"])
        s2.metric("⏰ Ending in 30 days", sub["count_ending"])
        s3.metric("❌ Expired", sub["count_expired"])

        st.markdown("**�� At Risk - Ending in 30 days**")
        if sub["ending_in_30d"]:
            df = pd.DataFrame(sub["ending_in_30d"])
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.warning(f"⚠️ {len(sub['ending_in_30d'])} subscriptions ending soon - reach out NOW")

        st.markdown("**🆕 New This Month**")
        if sub["new_this_month"]:
            st.dataframe(pd.DataFrame(sub["new_this_month"]), use_container_width=True, hide_index=True)

        st.markdown("**❌ Recently Expired**")
        if sub["expired"]:
            st.dataframe(pd.DataFrame(sub["expired"]), use_container_width=True, hide_index=True)

    # ── LAST STREAMED ──
    with sections[6]:
        st.subheader("Last Streamed - Who and When?")
        ls = data["last_streamed"]
        l1, l2, l3 = st.columns(3)
        l1.metric("Total Streamers", ls["total_streamers"])
        l2.metric("🚨 Paying but Inactive (>30d)", len(ls["inactive_paying"]))
        l3.metric("👻 Never Streamed (paying)", len(ls["never_streamed"]))

        st.markdown("**🚨 DANGER: Paying customers not streaming in 30+ days**")
        st.caption("These need immediate outreach - they're paying but not using")
        if ls["inactive_paying"]:
            df = pd.DataFrame(ls["inactive_paying"])
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.error(f"⚠️ Lost engagement signal: {len(ls['inactive_paying'])} customers")

        st.markdown("**🔥 Most Recent Streamers**")
        if ls["most_recent"]:
            st.dataframe(pd.DataFrame(ls["most_recent"]), use_container_width=True, hide_index=True)

        st.markdown("**👻 Paying but Never Streamed**")
        if ls["never_streamed"]:
            st.dataframe(pd.DataFrame(ls["never_streamed"]), use_container_width=True, hide_index=True)
