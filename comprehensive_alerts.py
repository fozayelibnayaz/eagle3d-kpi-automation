#!/usr/bin/env python3
"""
Comprehensive Multi-System Alert Engine
Sends Telegram alerts for ALL systems:
1. KPI (signups/uploads/paid + anomalies)
2. GA4 (traffic + country shifts)
3. YouTube (channel + AI insights)
4. LinkedIn (posts + followers + competitor)
5. Stripe (payments + churn)
6. Customer Success (15 sections)
7. Pipeline Health (all stages)
8. AI-detected anomalies (spike/drop/unusual patterns)
"""

import os
import json
import urllib.request
from datetime import datetime, date, timedelta
from collections import Counter


def _get_sb():
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        try:
            import streamlit as st
            url = str(st.secrets.get("SUPABASE_URL", "")).strip()
            key = str(st.secrets.get("SUPABASE_SERVICE_KEY", "")).strip()
        except Exception:
            pass
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def _send_telegram(message, parse_mode="HTML", max_retries=3):
    """Use proven send_telegram from reporting_engine - urllib works."""
    import time
    try:
        from reporting_engine import send_telegram as _proven_send
    except Exception:
        return False

    CHUNK_SIZE = 3500
    chunks = []
    if len(message) <= CHUNK_SIZE:
        chunks = [message]
    else:
        remaining = message
        while remaining:
            if len(remaining) <= CHUNK_SIZE:
                chunks.append(remaining)
                break
            split = remaining.rfind("\n", 0, CHUNK_SIZE)
            if split < 1000:
                split = CHUNK_SIZE
            chunks.append(remaining[:split])
            remaining = remaining[split:].lstrip()

    success = 0
    for chunk in chunks:
        if not chunk.strip():
            continue
        for attempt in range(max_retries):
            try:
                if _proven_send(chunk, parse_mode=parse_mode):
                    success += 1
                    break
            except Exception as e:
                print(f"Send err {attempt+1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(3 + attempt * 2)
    return success > 0



def _esc(s):
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")


# ═══════════════════════════════════════════════
# ALERT 1: KPI WITH MoM + ANOMALY DETECTION
# ═══════════════════════════════════════════════
def alert_kpi_detailed():
    sb = _get_sb()
    if not sb:
        return ""

    today = date.today().strftime("%Y-%m-%d")
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    week_ago = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    month_start = date.today().strftime("%Y-%m-01")
    last_month = (date.today().replace(day=1) - timedelta(days=1))
    last_month_start = last_month.replace(day=1).strftime("%Y-%m-%d")
    last_month_end = last_month.strftime("%Y-%m-%d")

    def cnt(table, col, gte, lte=None):
        q = sb.table(table).select("count", count="exact").eq("final_status","ACCEPTED").gte(col, gte)
        if lte:
            q = q.lte(col, lte)
        return q.execute().count or 0

    # Today
    s_today = cnt("signups", "signup_date", today)
    u_today = cnt("uploads", "upload_date", today)
    p_today = cnt("payments", "first_payment_date", today)

    # Yesterday for comparison
    s_yest = cnt("signups", "signup_date", yesterday, yesterday)
    u_yest = cnt("uploads", "upload_date", yesterday, yesterday)
    p_yest = cnt("payments", "first_payment_date", yesterday, yesterday)

    # Week
    s_week = cnt("signups", "signup_date", week_ago)
    u_week = cnt("uploads", "upload_date", week_ago)
    p_week = cnt("payments", "first_payment_date", week_ago)

    # Month
    s_month = cnt("signups", "signup_date", month_start)
    u_month = cnt("uploads", "upload_date", month_start)
    p_month = cnt("payments", "first_payment_date", month_start)

    # Last Month
    s_lmonth = cnt("signups", "signup_date", last_month_start, last_month_end)
    u_lmonth = cnt("uploads", "upload_date", last_month_start, last_month_end)
    p_lmonth = cnt("payments", "first_payment_date", last_month_start, last_month_end)

    def delta(curr, prev):
        if prev == 0:
            return "🆕" if curr > 0 else ""
        pct = (curr - prev) / prev * 100
        if pct > 50:  return f"🚀+{pct:.0f}%"
        if pct > 10:  return f"📈+{pct:.0f}%"
        if pct > 0:   return f"⬆+{pct:.0f}%"
        if pct < -50: return f"💀{pct:.0f}%"
        if pct < -10: return f"📉{pct:.0f}%"
        if pct < 0:   return f"⬇{pct:.0f}%"
        return "➡0%"

    # Anomalies
    anomalies = []
    if s_today == 0:
        anomalies.append("🚨 ZERO signups today!")
    if s_today > s_yest * 2 and s_yest > 0:
        anomalies.append(f"🔥 Signup SPIKE: {s_today} today vs {s_yest} yesterday")
    if p_today >= 3:
        anomalies.append(f"💎 {p_today} new paid customers today!")
    if u_today == 0 and s_today > 0:
        anomalies.append(f"⚠️ {s_today} signups but ZERO uploads - onboarding broken?")

    msg = (
        f"📊 <b>KPI DETAILED ALERT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"📅 <b>Today vs Yesterday</b>\n"
        f"├ Signups: <code>{s_today}</code> (yest {s_yest}) {delta(s_today, s_yest)}\n"
        f"├ Uploads: <code>{u_today}</code> (yest {u_yest}) {delta(u_today, u_yest)}\n"
        f"└ Paid:    <code>{p_today}</code> (yest {p_yest}) {delta(p_today, p_yest)}\n"
        f"\n"
        f"📅 <b>Last 7 Days</b>\n"
        f"├ Signups: <code>{s_week}</code>\n"
        f"├ Uploads: <code>{u_week}</code>\n"
        f"└ Paid:    <code>{p_week}</code>\n"
        f"\n"
        f"📆 <b>This Month vs Last Month</b>\n"
        f"├ Signups: <code>{s_month}</code> (last {s_lmonth}) {delta(s_month, s_lmonth)}\n"
        f"├ Uploads: <code>{u_month}</code> (last {u_lmonth}) {delta(u_month, u_lmonth)}\n"
        f"└ Paid:    <code>{p_month}</code> (last {p_lmonth}) {delta(p_month, p_lmonth)}\n"
    )

    if anomalies:
        msg += "\n⚠️ <b>ANOMALIES DETECTED</b>\n"
        for a in anomalies:
            msg += f"• {a}\n"

    return msg


# ═══════════════════════════════════════════════
# ALERT 2: CUSTOMER SUCCESS HEALTH
# ═══════════════════════════════════════════════
def alert_customer_success():
    sb = _get_sb()
    if not sb:
        return ""
    try:
        from customer_success_analytics import (
            churn_by_month, subscription_lifecycle, sheet1_analysis,
            customer_value_tiers, recent_activity_analysis, phone_call_analytics
        )
        churn = churn_by_month()
        sub = subscription_lifecycle()
        sheet1 = sheet1_analysis()
        tiers = customer_value_tiers()
        recent = recent_activity_analysis()
        phones = phone_call_analytics()
    except Exception as e:
        return f"\n🎯 <b>CUSTOMER SUCCESS</b> Error: {_esc(str(e))[:200]}\n"

    msg = (
        f"\n🎯 <b>CUSTOMER SUCCESS ALERT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💔 Total Churned: <code>{churn['total_churned']}</code>\n"
        f"💸 Annual Loss: <code>${churn['annual_revenue_lost']:,.2f}</code>\n"
        f"\n"
        f"📅 <b>Subscriptions</b>\n"
        f"├ 🆕 New This Month: <code>{sub['count_new']}</code>\n"
        f"├ ⏰ Ending in 30d: <code>{sub['count_ending']}</code> ⚠️\n"
        f"├ ❌ Recently Expired: <code>{sub['count_expired']}</code>\n"
        f"└ 💎 Long-term (12+): <code>{sub['count_long_term']}</code>\n"
        f"\n"
        f"💎 <b>Customer Tiers</b>\n"
        f"├ VIP (18+): <code>{tiers['vip_count']}</code>\n"
        f"├ High (12+): <code>{tiers['high_count']}</code>\n"
        f"├ Mid (6+): <code>{tiers['mid_count']}</code>\n"
        f"└ Low (1-5): <code>{tiers['low_count']}</code>\n"
        f"\n"
        f"📈 <b>Activity Trend</b>\n"
        f"├ 🚀 Growing: <code>{recent['count_growing']}</code>\n"
        f"├ ➡️ Steady: <code>{recent['count_steady']}</code>\n"
        f"└ 📉 Declining: <code>{recent['count_declining']}</code> ⚠️\n"
        f"\n"
        f"☎️ <b>Phone Campaign</b>\n"
        f"├ Total Calls: <code>{phones['total_calls_made']}</code>\n"
        f"├ Customers Reached: <code>{phones['customers_called']}</code>\n"
        f"└ Answer Rate: <code>{phones['answer_rate']}%</code>\n"
        f"\n"
        f"🚨 <b>Critical:</b>\n"
        f"• {len(sheet1.get('dormant_paying', []))} paying customers DORMANT 30+ days\n"
        f"• {sub['count_ending']} subscriptions ending in 30 days\n"
        f"• {recent['count_declining']} customers declining usage\n"
    )
    return msg


# ═══════════════════════════════════════════════
# ALERT 3: LINKEDIN DETAILED
# ═══════════════════════════════════════════════
def alert_linkedin():
    sb = _get_sb()
    if not sb:
        return ""
    try:
        hl = sb.table("linkedin_highlights_daily").select("*").order("snapshot_date", desc=True).limit(2).execute().data
        if not hl:
            return "\n💼 <b>LINKEDIN</b> No data\n"
        latest = hl[0]
        prev = hl[1] if len(hl) > 1 else {}

        def d(curr, prv):
            if prv == 0: return ""
            pct = (curr - prv) / prv * 100
            return f"{'+' if pct >= 0 else ''}{pct:.0f}%"

        posts_count = sb.table("linkedin_posts").select("count", count="exact").execute().count or 0
        top_post = sb.table("linkedin_posts").select("title,impressions,reactions").order("impressions", desc=True).limit(1).execute().data
        top = top_post[0] if top_post else {}

        msg = (
            f"\n💼 <b>LINKEDIN ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 Snapshot: <code>{latest.get('snapshot_date', 'N/A')}</code>\n"
            f"\n"
            f"📊 <b>Engagement (vs yesterday)</b>\n"
            f"├ 👁 Impressions: <code>{latest.get('impressions', 0):,}</code> {d(latest.get('impressions',0), prev.get('impressions',0))}\n"
            f"├ 👍 Reactions: <code>{latest.get('reactions', 0)}</code> {d(latest.get('reactions',0), prev.get('reactions',0))}\n"
            f"├ 💬 Comments: <code>{latest.get('comments', 0)}</code>\n"
            f"└ 🔁 Reposts: <code>{latest.get('reposts', 0)}</code>\n"
            f"\n"
            f"👥 <b>Audience</b>\n"
            f"├ Followers: <code>{latest.get('total_followers', 0):,}</code>\n"
            f"├ Page Views: <code>{latest.get('page_views', 0):,}</code>\n"
            f"├ Unique Visitors: <code>{latest.get('unique_visitors', 0):,}</code>\n"
            f"└ Newsletter Subs: <code>{latest.get('newsletter_subscribers', 0):,}</code>\n"
            f"\n"
            f"📝 <b>Content</b>\n"
            f"├ Posts Tracked: <code>{posts_count}</code>\n"
        )
        if top:
            msg += f"└ 🏆 Top Post: <code>{_esc(top.get('title','')[:60])}</code> ({top.get('impressions', 0):,} imp)\n"
        return msg
    except Exception as e:
        return f"\n💼 <b>LINKEDIN</b> Error: {_esc(str(e))[:200]}\n"


# ═══════════════════════════════════════════════
# ALERT 4: STRIPE + CHURN
# ═══════════════════════════════════════════════
def alert_stripe():
    sb = _get_sb()
    if not sb:
        return ""
    try:
        today = date.today().strftime("%Y-%m-%d")
        month_start = date.today().strftime("%Y-%m-01")
        last_month = date.today().replace(day=1) - timedelta(days=1)
        last_month_start = last_month.replace(day=1).strftime("%Y-%m-%d")

        all_paid = sb.table("payments").select("count", count="exact").eq("final_status","ACCEPTED").execute().count or 0
        today_paid = sb.table("payments").select("count", count="exact").eq("final_status","ACCEPTED").gte("first_payment_date", today).execute().count or 0
        month_paid = sb.table("payments").select("count", count="exact").eq("final_status","ACCEPTED").gte("first_payment_date", month_start).execute().count or 0
        last_month_paid = sb.table("payments").select("count", count="exact").eq("final_status","ACCEPTED").gte("first_payment_date", last_month_start).lte("first_payment_date", last_month.strftime("%Y-%m-%d")).execute().count or 0

        # Revenue
        pays = sb.table("payments").select("total_spend,first_payment_date").eq("final_status","ACCEPTED").execute().data or []
        total_rev = sum(float(p.get("total_spend") or 0) for p in pays)
        month_rev = sum(float(p.get("total_spend") or 0) for p in pays if str(p.get("first_payment_date","")).startswith(date.today().strftime("%Y-%m")))

        msg = (
            f"\n💳 <b>STRIPE ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 Today: <code>{today_paid}</code> new paid\n"
            f"📆 This Month: <code>{month_paid}</code> (last mo: {last_month_paid})\n"
            f"🏆 All Time: <code>{all_paid}</code> customers\n"
            f"💰 Month Revenue: <code>${month_rev:,.2f}</code>\n"
            f"💎 Total Revenue: <code>${total_rev:,.2f}</code>\n"
            f"📊 Avg Subscription: <code>${total_rev/all_paid if all_paid else 0:,.2f}</code>\n"
        )
        return msg
    except Exception as e:
        return f"\n💳 <b>STRIPE</b> Error: {_esc(str(e))[:200]}\n"


# ═══════════════════════════════════════════════
# ALERT 5: AI INSIGHTS - cross-platform anomalies
# ═══════════════════════════════════════════════
def alert_ai_insights():
    try:
        from ai_assistant_engine import get_full_context
        from ai_assistant_engine import _call_groq
        ctx = get_full_context("all")

        prompt = f"""You are a business analyst. Look at this REAL data and identify the TOP 3 most important things to know about right now.

DATA:
{json.dumps(ctx, indent=2, default=str)[:4000]}

Return exactly 3 short alerts:
1. [Most positive thing]
2. [Most concerning thing]
3. [Most actionable opportunity]

Each alert should be 1-2 sentences max, with specific numbers from the data."""

        ans, err = _call_groq([{"role":"user","content":prompt}], max_tokens=600, temperature=0.2)
        if ans:
            return f"\n🤖 <b>AI INSIGHTS (Top 3)</b>\n━━━━━━━━━━━━━━━━━━━━━━\n{_esc(ans)[:1500]}\n"
    except Exception as e:
        return f"\n🤖 <b>AI INSIGHTS</b> Error: {_esc(str(e))[:200]}\n"
    return ""




# ═══════════════════════════════════════════════
# ALERT 6: YOUTUBE DETAILED
# ═══════════════════════════════════════════════
def alert_youtube():
    try:
        from youtube_command_center import get_cached_or_fetch
        d = get_cached_or_fetch(period_days=30)
        ch = d.get("channel", {})
        ana = d.get("analytics", {})
        vids = d.get("videos", [])
        if not ch:
            return "\n📺 <b>YOUTUBE</b> No data\n"
        top5 = sorted(vids, key=lambda v: v.get("views",0), reverse=True)[:5]
        worst5 = sorted([v for v in vids if v.get("views",0)>0], key=lambda v: v.get("views",0))[:5]
        total_likes = sum(v.get("likes",0) for v in vids)
        total_comments = sum(v.get("comments",0) for v in vids)
        engagement = ((total_likes+total_comments) / max(ch.get("total_views",1),1)) * 100
        dead_count = sum(1 for v in vids if v.get("views_per_day",0) < 1)

        msg = (
            f"\n📺 <b>YOUTUBE ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 Channel: <code>{_esc(ch.get('title',''))[:40]}</code>\n"
            f"\n"
            f"📊 <b>Channel Stats</b>\n"
            f"├ 👥 Subscribers: <code>{ch.get('subscribers',0):,}</code>\n"
            f"├ 👁 Total Views: <code>{ch.get('total_views',0):,}</code>\n"
            f"├ 📹 Videos: <code>{ch.get('video_count',0)}</code>\n"
            f"├ 👍 Total Likes: <code>{total_likes:,}</code>\n"
            f"├ 💬 Comments: <code>{total_comments}</code>\n"
            f"└ 📊 Engagement: <code>{engagement:.2f}%</code>\n"
            f"\n"
            f"📊 <b>Last 30 Days</b>\n"
            f"├ 👁 Views: <code>{ana.get('views',0):,}</code>\n"
            f"├ ⏱ Watch Hours: <code>{ana.get('watch_hours',0):,.0f}h</code>\n"
            f"├ ➕ Subs Gained: <code>{ana.get('subscribers_gained',0)}</code>\n"
            f"└ 💰 Revenue: <code>${d.get('revenue',{}).get('estimated',0):,.2f}</code>\n"
            f"\n"
            f"🚨 <b>Health</b>\n"
            f"├ 🪦 Dead videos: <code>{dead_count}</code>\n"
            f"└ ❌ Engagement: " + ("🔴 Critical (<1%)" if engagement < 1 else "🟡 Low" if engagement < 2 else "🟢 Healthy") + "\n"
            f"\n"
            f"🏆 <b>Top 3 Videos</b>\n"
        )
        for v in top5[:3]:
            msg += f"• {_esc(v.get('title','')[:50])}: {v.get('views',0):,} views\n"
        return msg
    except Exception as e:
        return f"\n📺 <b>YOUTUBE</b> Error: {_esc(str(e))[:200]}\n"


# ═══════════════════════════════════════════════
# ALERT 7: GA4 DETAILED
# ═══════════════════════════════════════════════
def alert_ga4():
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
        from google.oauth2 import service_account
        import json as _json
        SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
        creds = None

        # Try 1: Streamlit secrets ga4_service_account
        try:
            import streamlit as st
            sa = dict(st.secrets["ga4_service_account"])
            if "private_key" in sa:
                sa["private_key"] = sa["private_key"].replace("\\n","\n")
            creds = service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)
        except Exception:
            pass

        # Try 2: google_creds.json file (GitHub Actions writes this)
        if not creds and os.path.exists("google_creds.json"):
            try:
                creds = service_account.Credentials.from_service_account_file("google_creds.json", scopes=SCOPES)
            except Exception as e:
                print(f"google_creds.json load err: {e}")

        # Try 3: GOOGLE_CREDS_JSON env var
        if not creds:
            gc_json = os.environ.get("GOOGLE_CREDS_JSON", "")
            if gc_json:
                try:
                    sa = _json.loads(gc_json)
                    if "private_key" in sa:
                        sa["private_key"] = sa["private_key"].replace("\\n","\n")
                    creds = service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)
                except Exception as e:
                    print(f"GOOGLE_CREDS_JSON parse err: {e}")

        # Try 4: Streamlit GOOGLE_CREDS
        if not creds:
            try:
                import streamlit as st
                sa = dict(st.secrets["GOOGLE_CREDS"])
                if "private_key" in sa:
                    sa["private_key"] = sa["private_key"].replace("\\n","\n")
                creds = service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)
            except Exception:
                pass

        if not creds:
            return "\n🌐 <b>GA4</b> No credentials (tried ga4_service_account, google_creds.json, GOOGLE_CREDS_JSON, GOOGLE_CREDS)\n"

        pid = os.environ.get("GA4_PROPERTY_ID","374525971")
        try:
            import streamlit as st
            pid = str(st.secrets.get("GA4_PROPERTY_ID", pid)).strip()
        except Exception:
            pass

        client = BetaAnalyticsDataClient(credentials=creds)
        today = date.today().strftime("%Y-%m-%d")
        month_start = date.today().strftime("%Y-%m-01")
        last_month_end = (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
        last_month_start = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")

        def _q(start, end):
            r = client.run_report(RunReportRequest(
                property=f"properties/{pid}",
                date_ranges=[DateRange(start_date=start, end_date=end)],
                metrics=[Metric(name="sessions"), Metric(name="totalUsers"), Metric(name="screenPageViews")],
            ))
            if r.rows:
                row = r.rows[0]
                return (int(row.metric_values[0].value), int(row.metric_values[1].value), int(row.metric_values[2].value))
            return (0,0,0)

        t_s, t_u, t_p = _q(today, today)
        m_s, m_u, m_p = _q(month_start, today)
        l_s, l_u, l_p = _q(last_month_start, last_month_end)

        # Top countries (last 30d)
        rc = client.run_report(RunReportRequest(
            property=f"properties/{pid}",
            date_ranges=[DateRange(start_date=month_start, end_date=today)],
            dimensions=[Dimension(name="country")],
            metrics=[Metric(name="sessions")],
            limit=5,
        ))
        countries = [(rw.dimension_values[0].value, int(rw.metric_values[0].value)) for rw in rc.rows]

        def d(c,p):
            if p == 0: return ""
            pct = (c-p)/p*100
            return f"{'+' if pct>=0 else ''}{pct:.0f}%"

        msg = (
            f"\n🌐 <b>GA4 ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 <b>Today</b>\n"
            f"├ 👥 Users: <code>{t_u:,}</code>\n"
            f"├ 📊 Sessions: <code>{t_s:,}</code>\n"
            f"└ 📄 Page Views: <code>{t_p:,}</code>\n"
            f"\n"
            f"📆 <b>This Month vs Last Month</b>\n"
            f"├ 👥 Users: <code>{m_u:,}</code> (last {l_u:,}) {d(m_u,l_u)}\n"
            f"├ 📊 Sessions: <code>{m_s:,}</code> (last {l_s:,}) {d(m_s,l_s)}\n"
            f"└ 📄 Page Views: <code>{m_p:,}</code> (last {l_p:,}) {d(m_p,l_p)}\n"
            f"\n"
            f"�� <b>Top 5 Countries (this month)</b>\n"
        )
        for country, sess in countries:
            msg += f"• {_esc(country)}: <code>{sess:,}</code>\n"
        return msg
    except Exception as e:
        return f"\n🌐 <b>GA4</b> Error: {_esc(str(e))[:200]}\n"


# ═══════════════════════════════════════════════
# ALERT 8: CROSS-PLATFORM CORRELATION
# ═══════════════════════════════════════════════
def alert_cross_platform():
    sb = _get_sb()
    if not sb:
        return ""
    try:
        # Get all platform snapshots
        today = date.today().strftime("%Y-%m-%d")
        month_start = date.today().strftime("%Y-%m-01")

        # KPI today
        s_t = sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",today).execute().count or 0
        p_t = sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",today).execute().count or 0

        # LinkedIn latest
        hl = sb.table("linkedin_highlights_daily").select("*").order("snapshot_date",desc=True).limit(1).execute().data
        li_imp = hl[0].get("impressions",0) if hl else 0
        li_followers = hl[0].get("total_followers",0) if hl else 0

        # Detect cross-platform patterns
        msg = (
            f"\n🔗 <b>CROSS-PLATFORM CORRELATION</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        # Pattern detections
        patterns = []
        if s_t == 0 and li_imp > 1000:
            patterns.append("⚠️ LinkedIn has impressions but ZERO signups - landing page issue?")
        if s_t > 5 and p_t == 0:
            patterns.append(f"⚠️ {s_t} signups but 0 paid today - check upgrade flow")
        if li_followers > 2500 and s_t < 3:
            patterns.append(f"⚠️ {li_followers:,} LinkedIn followers but only {s_t} signups today - audience-product mismatch")
        if not patterns:
            patterns.append("✅ No critical cross-platform issues detected today")

        msg += "\n".join(patterns) + "\n\n"

        msg += (
            f"📊 <b>System Health</b>\n"
            f"├ KPI: " + ("🟢" if s_t > 0 else "🔴") + f" Today {s_t} signups\n"
            f"├ LinkedIn: " + ("��" if li_imp > 100 else "🟡") + f" {li_imp:,} impressions\n"
            f"└ Stripe: " + ("🟢" if p_t > 0 else "🟡") + f" {p_t} paid today\n"
        )

        return msg
    except Exception as e:
        return f"\n🔗 <b>CROSS-PLATFORM</b> Error: {_esc(str(e))[:200]}\n"


# ═══════════════════════════════════════════════
# MAIN: send all alerts
# ═══════════════════════════════════════════════
def send_all_alerts():
    print("Building comprehensive alerts...")
    today = date.today().strftime("%Y-%m-%d")

    header = f"🦅 <b>EAGLE3D COMPREHENSIVE ALERT — {today}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"

    parts = []
    print("  Building KPI...")
    parts.append(alert_kpi_detailed())
    print("  Building GA4...")
    parts.append(alert_ga4())
    print("  Building YouTube...")
    parts.append(alert_youtube())
    print("  Building LinkedIn...")
    parts.append(alert_linkedin())
    print("  Building Stripe...")
    parts.append(alert_stripe())
    print("  Building Customer Success...")
    parts.append(alert_customer_success())
    print("  Building Cross-Platform...")
    parts.append(alert_cross_platform())
    print("  Building AI insights...")
    parts.append(alert_ai_insights())

    footer = (
        f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <a href=\"https://eagle3d-kpi-automation.streamlit.app/\">Open Dashboard</a>\n"
        f"<i>Sent at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC</i>"
    )

    # Send each part separately with delay to avoid Telegram rate limit
    import time as _time
    sent = 0
    for idx, p in enumerate(parts):
        if p and p.strip():
            print(f"  Sending section {idx+1}/{len(parts)} ({len(p)} chars)...")
            if _send_telegram(p):
                sent += 1
                print(f"    SENT")
            else:
                print(f"    FAILED")
            _time.sleep(4)  # Conservative delay - Telegram bot 1msg/sec per chat

    if footer:
        _time.sleep(2)
        _send_telegram(footer)

    print(f"Sent {sent}/{len(parts)} alert sections")
    return sent


if __name__ == "__main__":
    send_all_alerts()
