#!/usr/bin/env python3
"""Customer Success Deep Analytics UI - ALL data dimensions"""

import streamlit as st
import pandas as pd
import plotly.express as px


def render_cs_analytics():
    st.markdown('### 📊 Customer Success Deep Analytics')
    st.caption("Every dimension from all 9 CS sheet tabs")

    try:
        from customer_success_analytics import get_all_insights
        with st.spinner("Computing comprehensive insights..."):
            data = get_all_insights()
    except Exception as e:
        st.error(f"Error: {e}")
        return

    sections = st.tabs([
        "💔 Churn",
        "📊 Health Index",
        "⏱️ Streaming Time",
        "🎯 Sessions & Success",
        "📺 Last Streamed (Sheet1)",
        "📅 Subscriptions",
        "💵 Revenue",
        "🔗 Correlations",
        "🚫 No Sub Users",
    ])

    # ── CHURN ──
    with sections[0]:
        ch = data["churn"]
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Churned", ch["total_churned"])
        c2.metric("Monthly Revenue Lost", f"${ch['monthly_revenue_lost']:,.2f}")
        c3.metric("Annualized Loss", f"${ch['annual_revenue_lost']:,.2f}")

        st.subheader("Churn by Month")
        if ch["by_month"]:
            df = pd.DataFrame([{"Month": m, "Churned": v} for m, v in ch["by_month"].items()])
            fig = px.bar(df, x="Month", y="Churned", color="Churned",
                         color_continuous_scale="Reds", text="Churned")
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Churned Customer Details")
        if ch["customers"]:
            st.dataframe(pd.DataFrame(ch["customers"]), use_container_width=True, hide_index=True)

    # ── HEALTH INDEX ──
    with sections[1]:
        h = data["health"]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Customers", h["total_customers"])
        m2.metric("Avg Recurring", h["avg_recurring"])
        m3.metric("Max Recurring", h["max_recurring"])
        m4.metric("No Payments", h["no_recurring_count"])

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📊 Plans")
            df = pd.DataFrame([{"Plan": k, "Count": v} for k,v in h["plans"].items()])
            st.plotly_chart(px.pie(df, values="Count", names="Plan"), use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
        with col2:
            st.subheader("📋 Status")
            df = pd.DataFrame([{"Status": k, "Count": v} for k,v in h["statuses"].items()])
            st.plotly_chart(px.bar(df, x="Status", y="Count", color="Count"), use_container_width=True)

        col3, col4 = st.columns(2)
        with col3:
            st.subheader("🏢 Company Size")
            df = pd.DataFrame([{"Size": k, "Count": v} for k,v in h["company_sizes"].items()])
            st.plotly_chart(px.bar(df, x="Size", y="Count", color="Count", color_continuous_scale="Blues"), use_container_width=True)
        with col4:
            st.subheader("🎯 Perfect Fit Customer")
            df = pd.DataFrame([{"Fit": k, "Count": v} for k,v in h["perfect_fit"].items()])
            st.plotly_chart(px.pie(df, values="Count", names="Fit"), use_container_width=True)

        st.subheader("Recurring Payment Distribution")
        if h["recurring_distribution"]:
            fig = px.histogram(x=h["recurring_distribution"], nbins=30,
                               labels={"x": "Recurring Payment Count"},
                               title="How many payments customers make")
            st.plotly_chart(fig, use_container_width=True)

    # ── STREAMING TIME ──
    with sections[2]:
        s = data["streaming"]
        m1, m2, m3 = st.columns(3)
        m1.metric("Active Streamers", s["active_streamers"])
        m2.metric("Non-Streamers", s["non_streamers"])
        m3.metric("Total Stream Hours", f"{s['total_stream_hours']:,.1f}")

        st.subheader("📅 Stream Time by Period")
        if s["period_totals"]:
            df = pd.DataFrame([{"Period": k, "Hours": v} for k,v in s["period_totals"].items()])
            df["Period_Short"] = df["Period"].str.replace("Total Stream Time ", "").str[:30]
            fig = px.line(df, x="Period_Short", y="Hours", markers=True,
                          title="Bi-Weekly Stream Time Trend")
            fig.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("🏆 Top 50 Streamers")
        df = pd.DataFrame(s["top_50_streamers"])
        st.dataframe(df, use_container_width=True, hide_index=True)

    # ── SESSIONS ──
    with sections[3]:
        sn = data["sessions"]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Clients", sn["total_clients"])
        m2.metric("Total Sessions", f"{sn['total_sessions']:,}")
        m3.metric("Connected Success Rate", f"{sn['connected_success_rate']}%")
        m4.metric("Streamed Success Rate", f"{sn['streamed_success_rate']}%")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("✅ vs ❌ Connection")
            df = pd.DataFrame({
                "Type": ["Success", "Failure"],
                "Count": [sn["total_connected_success"], sn["total_connected_failure"]],
            })
            st.plotly_chart(px.pie(df, values="Count", names="Type", color_discrete_map={"Success":"green","Failure":"red"}), use_container_width=True)
        with col2:
            st.subheader("✅ vs ❌ Streaming")
            df = pd.DataFrame({
                "Type": ["Success", "Failure"],
                "Count": [sn["total_streamed_success"], sn["total_streamed_failure"]],
            })
            st.plotly_chart(px.pie(df, values="Count", names="Type", color_discrete_map={"Success":"green","Failure":"red"}), use_container_width=True)

        st.subheader("Video vs Channel Minutes")
        df = pd.DataFrame({
            "Type": ["Video", "Channel"],
            "Minutes": [sn["total_video_minutes"], sn["total_channel_minutes"]],
        })
        st.plotly_chart(px.bar(df, x="Type", y="Minutes", color="Type"), use_container_width=True)

        st.subheader("🏆 Top 30 Clients by Sessions")
        if sn["top_30_clients"]:
            st.dataframe(pd.DataFrame(sn["top_30_clients"]), use_container_width=True, hide_index=True)

        st.subheader("🚨 High Failure Rate Clients (need attention)")
        if sn["high_failure_clients"]:
            st.dataframe(pd.DataFrame(sn["high_failure_clients"]), use_container_width=True, hide_index=True)

    # ── SHEET1 LAST STREAMED ──
    with sections[4]:
        s1 = data["sheet1"]
        m1, m2 = st.columns(2)
        m1.metric("Total Customers", s1["total_customers"])
        m2.metric("Dormant Paying (>30d)", len(s1["dormant_paying"]))

        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader("🎨 Color Distribution")
            df = pd.DataFrame([{"Color": k, "Count": v} for k,v in s1["color_dist"].items() if k])
            if not df.empty:
                st.plotly_chart(px.pie(df, values="Count", names="Color"), use_container_width=True)
        with col2:
            st.subheader("📊 CHI Distribution")
            df = pd.DataFrame([{"CHI": k, "Count": v} for k,v in s1["chi_dist"].items() if k])
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
        with col3:
            st.subheader("📋 Plans")
            df = pd.DataFrame([{"Plan": k, "Count": v} for k,v in s1["plan_dist"].items()])
            st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("🚨 DANGER: Paying but Dormant 30+ days")
        if s1["dormant_paying"]:
            st.dataframe(pd.DataFrame(s1["dormant_paying"]), use_container_width=True, hide_index=True)
            st.error(f"⚠️ {len(s1['dormant_paying'])} paying customers haven't streamed in 30+ days")

        st.subheader("🏆 Top 30 2025 Streamers")
        if s1["top_2025_streamers"]:
            st.dataframe(pd.DataFrame(s1["top_2025_streamers"]), use_container_width=True, hide_index=True)

    # ── SUBSCRIPTIONS ──
    with sections[5]:
        sub = data["subscriptions"]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("🆕 New This Month", sub["count_new"])
        m2.metric("⏰ Ending in 30d", sub["count_ending"])
        m3.metric("❌ Expired", sub["count_expired"])
        m4.metric("💎 Long-term (12+)", sub["count_long_term"])

        st.subheader("🔥 At Risk - Ending Soon")
        if sub["ending_in_30d"]:
            st.dataframe(pd.DataFrame(sub["ending_in_30d"]), use_container_width=True, hide_index=True)
            st.error(f"⚠️ {len(sub['ending_in_30d'])} subscriptions ending in 30 days - act NOW")

        st.subheader("🆕 New This Month")
        if sub["new_this_month"]:
            st.dataframe(pd.DataFrame(sub["new_this_month"]), use_container_width=True, hide_index=True)

        st.subheader("💎 Long-term Loyal Customers (12+ recurring)")
        if sub["long_term_customers"]:
            st.dataframe(pd.DataFrame(sub["long_term_customers"]), use_container_width=True, hide_index=True)

        st.subheader("❌ Recently Expired")
        if sub["expired"]:
            st.dataframe(pd.DataFrame(sub["expired"]), use_container_width=True, hide_index=True)

    # ── REVENUE ──
    with sections[6]:
        r = data["revenue"]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Previous Total", f"${r['total_previous']:,.2f}")
        m2.metric("Current Total", f"${r['total_current']:,.2f}")
        m3.metric("Net Deviation", f"${r['total_deviation']:+,.2f}")
        m4.metric("Loss Customers", r["loss_customers"])

        st.subheader("Revenue Changes (Net Loss/Gain by Customer)")
        if r["conversion_losses"]:
            df = pd.DataFrame(r["conversion_losses"])
            st.dataframe(df, use_container_width=True, hide_index=True)
            fig = px.bar(df, x="email", y="deviation", color="deviation",
                         color_continuous_scale=["red","gray","green"],
                         title="Revenue Change per Customer")
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("�� Stream Time Trend (All Periods)")
        st_trend = data["stream_trend"]
        if st_trend:
            df = pd.DataFrame(st_trend)
            fig = px.line(df, x="period", y="stream_time", markers=True, title="Stream Time Over Time")
            st.plotly_chart(fig, use_container_width=True)

    # ── CORRELATIONS ──
    with sections[7]:
        cor = data["correlations"]
        st.subheader("🏢 Churn Rate by Company Size")
        rate = cor["company_size_churn_rate"]
        if rate:
            df = pd.DataFrame([{"Size": k, "Total": cor["size_totals"].get(k,0),
                               "Churn Rate %": v} for k,v in rate.items()]).sort_values("Churn Rate %", ascending=False)
            st.dataframe(df, use_container_width=True, hide_index=True)
            fig = px.bar(df, x="Size", y="Churn Rate %", color="Churn Rate %",
                         color_continuous_scale="Reds", title="Which company sizes churn most")
            st.plotly_chart(fig, use_container_width=True)
            worst_size = df.iloc[0]
            st.error(f"⚠️ **{worst_size['Size']}** companies have highest churn: **{worst_size['Churn Rate %']}%** ({cor['size_totals'].get(worst_size['Size'],0)} customers)")

        st.subheader("📊 Churn Rate by Plan")
        rate = cor["plan_churn_rate"]
        if rate:
            df = pd.DataFrame([{"Plan": k, "Total": cor["plan_totals"].get(k,0),
                               "Churn Rate %": v} for k,v in rate.items()]).sort_values("Churn Rate %", ascending=False)
            st.dataframe(df, use_container_width=True, hide_index=True)
            fig = px.bar(df, x="Plan", y="Churn Rate %", color="Churn Rate %", color_continuous_scale="Reds")
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("🎯 Churn Rate by Perfect Fit")
        rate = cor["perfect_fit_churn_rate"]
        if rate:
            df = pd.DataFrame([{"Fit": k, "Total": cor["fit_totals"].get(k,0),
                               "Churn Rate %": v} for k,v in rate.items()]).sort_values("Churn Rate %", ascending=False)
            st.dataframe(df, use_container_width=True, hide_index=True)

    # ── NO SUB USERS ──
    with sections[8]:
        n = data["no_sub"]
        st.metric("Total No-Sub Users", n["total"])

        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader("By Plan")
            df = pd.DataFrame([{"Plan": k, "Count": v} for k,v in n["by_plan"].items()])
            st.dataframe(df, use_container_width=True, hide_index=True)
        with col2:
            st.subheader("By Company Size")
            df = pd.DataFrame([{"Size": k, "Count": v} for k,v in n["by_size"].items()])
            st.dataframe(df, use_container_width=True, hide_index=True)
        with col3:
            st.subheader("By Perfect Fit")
            df = pd.DataFrame([{"Fit": k, "Count": v} for k,v in n["by_fit"].items()])
            st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("Users (First 50)")
        if n["users"]:
            st.dataframe(pd.DataFrame(n["users"]), use_container_width=True, hide_index=True)
