#!/usr/bin/env python3
"""
YouTube Command Center UI - Streamlit component
Renders the full dashboard replicating youtube-command-center-eight.vercel.app
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta


def render_youtube_command_center():
    """Main render function for YouTube Command Center page."""
    try:
        from youtube_command_center import get_cached_or_fetch
    except ImportError:
        st.error("youtube_command_center.py module not found")
        return

    st.markdown('<div class="sec-head">📺 YouTube Command Center</div>', unsafe_allow_html=True)

    # Period selector
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        period = st.selectbox("📅 Period", [
            "Last 7 Days", "Last 28 Days", "Last 90 Days", "Last 365 Days"
        ], index=1, key="yt_cc_period")
    with c2:
        if st.button("🔄 Refresh", key="yt_cc_refresh"):
            st.cache_data.clear()
            st.rerun()
    with c3:
        days_map = {"Last 7 Days": 7, "Last 28 Days": 28, "Last 90 Days": 90, "Last 365 Days": 365}
        days = days_map.get(period, 28)
        st.metric("Days", days)

    # Fetch data
    with st.spinner(f"Loading {period} data..."):
        data = get_cached_or_fetch(period_days=days)

    # OAuth status banner
    if data.get("oauth_status") != "ok":
        st.error(f"🔴 YouTube Analytics OAuth: {data.get('oauth_status')} - Revenue/Watch Hours/Demographics unavailable")
        st.caption("Add valid YOUTUBE_REFRESH_TOKEN + YOUTUBE_CLIENT_ID + YOUTUBE_CLIENT_SECRET to Streamlit secrets")
    else:
        st.success(f"🟢 YouTube Analytics OAuth: Connected")

    if data.get("error"):
        st.error(f"Error: {data['error']}")
        return

    ch = data.get("channel", {})
    ana = data.get("analytics", {})
    rev = data.get("revenue", {})
    vids = data.get("videos", [])

    # ── HEADER: Channel info ──
    st.divider()
    h1, h2 = st.columns([1, 3])
    with h1:
        if ch.get("thumbnail"):
            st.image(ch["thumbnail"], width=120)
    with h2:
        st.subheader(ch.get("title", "Channel"))
        st.caption(ch.get("description", "")[:200])

    # ── TOP METRICS ROW ──
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("👥 Subscribers", f"{ch.get('subscribers', 0):,}")
    m2.metric("👁️ Total Views", f"{ch.get('total_views', 0):,}")
    m3.metric("📹 Videos", ch.get("video_count", 0))
    m4.metric("⏱️ Watch Hours", f"{ana.get('watch_hours', 0):,.0f}h" if ana else "N/A")
    m5.metric("➕ Subs Gained", ana.get("subscribers_gained", 0) if ana else "N/A")
    m6.metric("💰 Revenue", f"${rev.get('estimated', 0):,.2f}" if rev else "N/A")

    # ── TABS ──
    tabs = st.tabs([
        "📊 Dashboard", "🎬 Videos", "📈 Analytics", "👥 Audience",
        "💰 Revenue", "🌍 Traffic", "🎵 Playlists",
    ])

    # ── TAB 1: DASHBOARD ──
    with tabs[0]:
        st.subheader("Channel Performance Overview")
        if ana:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Views (period)", f"{ana.get('views', 0):,}")
            c2.metric("Avg Duration", f"{ana.get('avg_view_duration', 0)}s")
            c3.metric("Likes", f"{ana.get('likes', 0):,}")
            c4.metric("Comments", f"{ana.get('comments', 0):,}")

            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Shares", f"{ana.get('shares', 0):,}")
            c6.metric("Subs Lost", ana.get("subscribers_lost", 0))
            c7.metric("Net Subs", ana.get("net_subs", 0))
            engagement = (
                (ana.get("likes", 0) + ana.get("comments", 0))
                / max(ana.get("views", 1), 1) * 100
            )
            c8.metric("Engagement", f"{engagement:.2f}%")
        else:
            st.warning("Analytics data not available - OAuth required")

        # Top + worst videos
        if vids:
            colA, colB = st.columns(2)
            with colA:
                st.markdown("**�� Top 5 Performing**")
                top5 = sorted(vids, key=lambda v: v["score"], reverse=True)[:5]
                for v in top5:
                    st.write(f"**{v['title'][:60]}**")
                    st.caption(f"Views: {v['views']:,} | Eng: {v['engagement']}% | Score: {v['score']}/100")
                    st.markdown(f"[▶ Watch]({v['url']})")
            with colB:
                st.markdown("**💀 Bottom 5 Performing**")
                worst = sorted([v for v in vids if v["views"] > 0], key=lambda v: v["score"])[:5]
                for v in worst:
                    st.write(f"**{v['title'][:60]}**")
                    st.caption(f"Views: {v['views']:,} | Eng: {v['engagement']}% | Score: {v['score']}/100")
                    st.markdown(f"[▶ Watch]({v['url']})")

    # ── TAB 2: VIDEOS ──
    with tabs[1]:
        st.subheader(f"All Videos ({len(vids)})")
        if vids:
            df = pd.DataFrame(vids)
            sort_col = st.selectbox("Sort by:", [
                "score", "views", "engagement", "likes", "comments",
                "views_per_day", "published_at"
            ], key="yt_vid_sort")
            sort_dir = st.radio("Order:", ["Descending", "Ascending"], horizontal=True, key="yt_vid_dir")
            df = df.sort_values(by=sort_col, ascending=(sort_dir == "Ascending"))

            display_cols = ["title", "views", "likes", "comments", "engagement",
                            "views_per_day", "age_days", "score", "published_at"]
            display_df = df[[c for c in display_cols if c in df.columns]].copy()
            if "published_at" in display_df.columns:
                display_df["published_at"] = display_df["published_at"].str[:10]
            st.dataframe(display_df, use_container_width=True, height=600)

    # ── TAB 3: ANALYTICS ──
    with tabs[2]:
        st.subheader("Analytics Deep Dive")
        if ana:
            # Engagement chart
            eng_data = pd.DataFrame({
                "Metric": ["Views", "Likes", "Comments", "Shares"],
                "Count":  [
                    ana.get("views", 0), ana.get("likes", 0),
                    ana.get("comments", 0), ana.get("shares", 0),
                ],
            })
            fig = px.bar(eng_data, x="Metric", y="Count", title="Engagement Metrics", text="Count")
            st.plotly_chart(fig, use_container_width=True)

            # Watch time analysis
            c1, c2 = st.columns(2)
            with c1:
                st.metric("Total Watch Hours", f"{ana.get('watch_hours', 0):,.1f}h")
                st.metric("Average View Duration", f"{ana.get('avg_view_duration', 0)}s")
            with c2:
                avg_views_per_video = ana.get("views", 0) / max(len(vids), 1)
                st.metric("Avg Views per Video", f"{avg_views_per_video:,.0f}")
                avg_eng = (
                    (ana.get("likes", 0) + ana.get("comments", 0))
                    / max(ana.get("views", 1), 1) * 100
                )
                st.metric("Engagement Rate", f"{avg_eng:.2f}%")
        else:
            st.warning("Analytics requires OAuth - add tokens to secrets")

    # ── TAB 4: AUDIENCE ──
    with tabs[3]:
        st.subheader("Audience Demographics")
        demo = data.get("demographics", [])
        if demo:
            df = pd.DataFrame(demo)
            fig = px.bar(df, x="age", y="percentage", color="gender",
                         title="Age + Gender Distribution", barmode="group")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("Demographics requires OAuth")

    # ── TAB 5: REVENUE ──
    with tabs[4]:
        st.subheader("Revenue Analytics")
        if rev:
            r1, r2, r3 = st.columns(3)
            r1.metric("💰 Estimated Revenue", f"${rev.get('estimated', 0):,.2f}")
            r2.metric("📺 Ad Revenue", f"${rev.get('ad_revenue', 0):,.2f}")
            r3.metric("💵 Gross Revenue", f"${rev.get('gross', 0):,.2f}")

            r4, r5, r6 = st.columns(3)
            r4.metric("CPM", f"${rev.get('cpm', 0):.2f}")
            r5.metric("Playback CPM", f"${rev.get('playback_cpm', 0):.2f}")
            r6.metric("Monetized Plays", f"{rev.get('monetized_plays', 0):,}")
        else:
            st.warning("Revenue requires YouTube Partner Program + Monetary OAuth scope")

    # ── TAB 6: TRAFFIC ──
    with tabs[5]:
        st.subheader("Traffic Sources")
        ts = data.get("traffic_sources", [])
        if ts:
            df = pd.DataFrame(ts)
            fig = px.pie(df, values="views", names="source", title="Traffic Source Breakdown")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("Traffic sources requires OAuth")

        st.subheader("🌍 Geography")
        geo = data.get("geography", [])
        if geo:
            df = pd.DataFrame(geo)
            fig = px.bar(df.head(20), x="country", y="views", title="Top Countries by Views")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("Geography requires OAuth")

        st.subheader("📱 Devices")
        dev = data.get("devices", [])
        if dev:
            df = pd.DataFrame(dev)
            fig = px.pie(df, values="views", names="device", title="Device Breakdown")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Device data requires OAuth")

    # ── TAB 7: PLAYLISTS ──
    with tabs[6]:
        st.subheader("Playlists")
        pl = data.get("playlists", [])
        if pl:
            for p in pl:
                with st.container():
                    c1, c2 = st.columns([1, 4])
                    with c1:
                        if p.get("thumbnail"):
                            st.image(p["thumbnail"], width=120)
                    with c2:
                        st.markdown(f"**{p['title']}**")
                        st.caption(p.get("description", "")[:150])
                        st.caption(f"📹 {p.get('video_count', 0)} videos | 📅 {p.get('published_at', '')[:10]}")
                    st.divider()
        else:
            st.info("No playlists found")

    # Footer
    st.caption(f"Data fetched: {data.get('fetched_at','')[:19]} UTC | Period: {period}")


if __name__ == "__main__":
    render_youtube_command_center()
