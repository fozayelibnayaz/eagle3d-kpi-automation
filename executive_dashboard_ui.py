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

    # DEBUG: Test Supabase connection directly
    _debug_sb = None
    try:
        from sheets_writer import _get_sb as _debug_get_sb
        _debug_sb = _debug_get_sb()
        if _debug_sb:
            _test = _debug_sb.table("signups").select("count", count="exact").limit(1).execute()
            st.success(f"Supabase OK via sheets_writer: {_test.count} signups")
        else:
            st.warning("sheets_writer._get_sb returned None")
    except Exception as _de:
        st.warning(f"sheets_writer debug: {_de}")

    try:
        from executive_dashboard import _get_sb as _ed_get_sb
        _ed_sb = _ed_get_sb()
        if _ed_sb:
            st.success("executive_dashboard._get_sb: OK")
        else:
            st.error("executive_dashboard._get_sb: returned None")
            # Show why
            import os
            st.write(f"SUPABASE_URL env: {'SET' if os.environ.get('SUPABASE_URL') else 'NOT SET'}")
            st.write(f"SUPABASE_SERVICE_KEY env: {'SET' if os.environ.get('SUPABASE_SERVICE_KEY') else 'NOT SET'}")
            try:
                import streamlit as _st_dbg
                st.write(f"st.secrets has SUPABASE_URL: {'SUPABASE_URL' in _st_dbg.secrets}")
                st.write(f"st.secrets keys: {list(_st_dbg.secrets.keys())[:10]}")
            except Exception as _se:
                st.write(f"st.secrets error: {_se}")
    except Exception as _de2:
        st.error(f"executive_dashboard debug: {_de2}")

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
    
    st.divider()

    # ── CONTENT RELEASE VOLUME ──
    st.subheader("📝 Content Release Volume")
    st.caption("How much content published per month across all channels")

    cv = metrics.get("content_volume", {})
    blog = cv.get("blog_pages", {})
    cv1, cv2, cv3, cv4 = st.columns(4)
    cv1.metric("📝 Total Content This Month",
               cv.get("total_this_month", 0),
               help="LinkedIn posts + YouTube videos + new website pages")
    cv2.metric("🌐 New Website Pages",
               blog.get("new_pages_this_month", 0),
               help="Pages that appeared this month but not last month")
    cv3.metric("💼 LinkedIn Posts",
               cv.get("linkedin", {}).get("this_month", 0),
               help=f"vs last month: {cv.get('linkedin', {}).get('last_month', 0)}")
    cv4.metric("�� YouTube Videos",
               cv.get("youtube", {}).get("this_month", 0),
               help=f"vs last month: {cv.get('youtube', {}).get('last_month', 0)}")

    # Content volume trend chart
    li_by_month = cv.get("linkedin", {}).get("by_month", {})
    yt_by_month = cv.get("youtube", {}).get("by_month", {})
    if li_by_month or yt_by_month:
        all_months = sorted(set(list(li_by_month.keys()) + list(yt_by_month.keys())))[-12:]
        content_df = pd.DataFrame([{
            "Month": m,
            "LinkedIn Posts": li_by_month.get(m, 0),
            "YouTube Videos": yt_by_month.get(m, 0),
            "Total": li_by_month.get(m, 0) + yt_by_month.get(m, 0),
        } for m in all_months])

        if not content_df.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=content_df["Month"], y=content_df["LinkedIn Posts"],
                                name="LinkedIn Posts", marker_color="#0077b5"))
            fig.add_trace(go.Bar(x=content_df["Month"], y=content_df["YouTube Videos"],
                                name="YouTube Videos", marker_color="#ff0000"))
            fig.update_layout(barmode="stack", title="Monthly Content Output",
                            yaxis_title="Pieces Published", height=350)
            st.plotly_chart(fig, use_container_width=True)

    # Top performing content
    # Website pages performance
    # ── NEW PAGES THIS MONTH (created/indexed this month, not seen last month) ──
    if blog.get("new_pages"):
        st.markdown(f"### 🆕 New Pages Created This Month ({blog.get('new_pages_this_month',0)} pages)")
        st.caption("Pages that appeared for the first time this month (not seen in previous month)")
        new_df = pd.DataFrame(blog["new_pages"])
        if not new_df.empty:
            cols = [c for c in ["title","path","views","users","sessions"] if c in new_df.columns]
            st.dataframe(new_df[cols].astype(str), use_container_width=True, hide_index=True)
    else:
        st.info("No new pages detected this month")

    # ── ALL PAGES WITH PERFORMANCE (this month) ──
    if blog.get("top_pages"):
        st.markdown(f"### 🌐 All Pages Performance This Month ({blog.get('total_pages_this_month',0)} pages)")
        st.caption("Every page that received at least 1 view this month, sorted by views")
        page_df = pd.DataFrame(blog["top_pages"])
        if not page_df.empty:
            # Search filter
            page_search = st.text_input("�� Search pages by title or path", key="page_search")
            if page_search:
                mask = page_df["title"].str.contains(page_search, case=False, na=False) | page_df["path"].str.contains(page_search, case=False, na=False)
                page_df = page_df[mask]
            cols = [c for c in ["title","path","views","users","sessions"] if c in page_df.columns]
            st.dataframe(page_df[cols].astype(str), use_container_width=True, hide_index=True)
            st.caption(f"Showing {len(page_df)} pages")

    # ── BLOG/ARTICLE PAGES ──
    if blog.get("blog_pages"):
        with st.expander(f"📝 Blog & Article Pages ({blog.get('blog_pages_this_month',0)} pages)", expanded=False):
            blog_df = pd.DataFrame(blog["blog_pages"])
            if not blog_df.empty:
                cols = [c for c in ["title","path","views","users","sessions"] if c in blog_df.columns]
                st.dataframe(blog_df[cols].astype(str), use_container_width=True, hide_index=True)

    tc1, tc2 = st.columns(2)
    with tc1:
        st.markdown("**💼 LinkedIn Posts (all tracked)**")
        all_li = cv.get("linkedin", {}).get("all_posts", cv.get("linkedin", {}).get("top_posts", []))
        if all_li:
            li_df = pd.DataFrame([{
                "Published": str(p.get("published_at",""))[:10] if p.get("published_at") else "Unknown",
                "Title": str(p.get("title",""))[:80],
                "Impressions": p.get("impressions", 0) or 0,
                "Clicks": p.get("clicks", 0) or 0,
                "CTR %": p.get("ctr", 0) or 0,
                "Reactions": p.get("reactions", 0) or 0,
                "Engagement %": p.get("engagement_rate", 0) or 0,
            } for p in all_li])
            li_df = li_df.sort_values("Impressions", ascending=False)
            st.dataframe(li_df.astype(str), use_container_width=True, hide_index=True)
            st.caption(f"{len(all_li)} posts tracked | This month: {cv.get('linkedin',{}).get('this_month',0)} | Last month: {cv.get('linkedin',{}).get('last_month',0)}")
        else:
            st.caption("No LinkedIn post data")
    with tc2:
        st.markdown("**🏆 Top YouTube Videos (by views)**")
        top_yt = cv.get("youtube", {}).get("top_videos", [])
        if top_yt:
            for v in top_yt[:5]:
                title = str(v.get("title",""))[:60]
                views = v.get("views", 0)
                st.write(f"- {title}... ({views:,} views)")
        else:
            st.caption("No YouTube video data")

    st.divider()

    # ── CHANNEL GROWTH OVER TIME ──
    st.subheader("📈 Channel Growth / Decrease Over Time")
    st.caption("Track growth across LinkedIn followers, YouTube subscribers, and website traffic")

    cg = metrics.get("channel_growth", {})

    # LinkedIn Followers
    li_g = cg.get("linkedin_followers", {})
    yt_g = cg.get("youtube_subs", {})
    web_g = cg.get("website_traffic", [])

    g1, g2, g3 = st.columns(3)
    with g1:
        st.markdown("**💼 LinkedIn Followers**")
        if li_g:
            st.metric("Current", f"{li_g.get('current',0):,}",
                      f"+{li_g.get('growth_30d',0)} (30d)")
            st.caption(f"90d growth: +{li_g.get('growth_90d',0)}")
        else:
            st.caption("No LinkedIn follower data")
    with g2:
        st.markdown("**📺 YouTube**")
        if yt_g:
            st.metric("Subscribers", f"{yt_g.get('current',0):,}")
            st.caption(f"Views: {yt_g.get('total_views',0):,} | Videos: {yt_g.get('video_count',0)}")
        else:
            st.caption("No YouTube data")
    with g3:
        st.markdown("**🌐 Website Traffic**")
        if web_g and len(web_g) >= 2:
            latest = web_g[-1]
            prev_m = web_g[-2]
            st.metric("Sessions (latest month)",
                      f"{latest.get('sessions',0):,}",
                      f"{latest.get('sessions',0) - prev_m.get('sessions',0):+,} vs prev month")
            st.caption(f"Users: {latest.get('users',0):,}")
        elif web_g:
            st.metric("Sessions", f"{web_g[-1].get('sessions',0):,}")
        else:
            st.caption("No GA4 traffic data")

    # Growth timeline charts
    growth_tabs = st.tabs(["�� LinkedIn Followers", "🌐 Website Traffic"])
    with growth_tabs[0]:
        li_hist = li_g.get("history", [])
        if li_hist and len(li_hist) > 1:
            df = pd.DataFrame(li_hist)
            fig = px.line(df, x="date", y="total", title="LinkedIn Follower Growth",
                         markers=True)
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need more daily data points. Run pipeline daily to build history.")

    with growth_tabs[1]:
        if web_g and len(web_g) > 1:
            df = pd.DataFrame(web_g)
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["month"], y=df["sessions"], name="Sessions", marker_color="#3b82f6"))
            fig.add_trace(go.Scatter(x=df["month"], y=df["users"], name="Users",
                                   mode="lines+markers", marker_color="#22c55e", yaxis="y2"))
            fig.update_layout(
                title="Website Traffic (Monthly)",
                yaxis=dict(title="Sessions"),
                yaxis2=dict(title="Users", overlaying="y", side="right"),
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("GA4 data not available or insufficient history")


    st.caption(f"Data generated: {metrics['generated_at'][:19]} UTC")
