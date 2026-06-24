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
    """Show daily/period DELTAS instead of cumulative totals."""
    sb = _get_sb()
    if not sb:
        return ""
    try:
        today_dt = date.today()
        today_str = today_dt.isoformat()
        yest_str  = (today_dt - timedelta(days=1)).isoformat()
        week_ago  = (today_dt - timedelta(days=7)).isoformat()
        month_ago = (today_dt - timedelta(days=30)).isoformat()

        def snap_on(date_str):
            """Get exact snapshot on a specific date."""
            r = sb.table("linkedin_highlights_daily").select("*").eq("snapshot_date", date_str).limit(1).execute().data or []
            return r[0] if r else {}

        def latest_snap():
            r = sb.table("linkedin_highlights_daily").select("*").order("snapshot_date", desc=True).limit(1).execute().data or []
            return r[0] if r else {}

        latest = latest_snap()
        if not latest:
            return "\n" + chr(128188) + " <b>LINKEDIN</b> No data yet\n"

        latest_date = latest.get("snapshot_date", "")

        # Get snapshots at boundaries for delta calc
        yest_snap = snap_on(yest_str)
        week_ago_snap = snap_on(week_ago)
        month_ago_snap = snap_on(month_ago)

        # DELTA calculations (today vs previous)
        def delta(curr_val, prev_val):
            return curr_val - prev_val if prev_val else curr_val

        today_imp_delta = delta(latest.get("impressions",0), yest_snap.get("impressions",0)) if yest_snap else 0
        today_rxn_delta = delta(latest.get("reactions",0),   yest_snap.get("reactions",0))   if yest_snap else 0
        today_com_delta = delta(latest.get("comments",0),    yest_snap.get("comments",0))    if yest_snap else 0
        today_rep_delta = delta(latest.get("reposts",0),     yest_snap.get("reposts",0))     if yest_snap else 0

        week_imp_delta = delta(latest.get("impressions",0), week_ago_snap.get("impressions",0)) if week_ago_snap else 0
        week_rxn_delta = delta(latest.get("reactions",0),   week_ago_snap.get("reactions",0))   if week_ago_snap else 0
        week_com_delta = delta(latest.get("comments",0),    week_ago_snap.get("comments",0))    if week_ago_snap else 0

        month_imp_delta = delta(latest.get("impressions",0), month_ago_snap.get("impressions",0)) if month_ago_snap else 0
        month_rxn_delta = delta(latest.get("reactions",0),   month_ago_snap.get("reactions",0))   if month_ago_snap else 0
        month_com_delta = delta(latest.get("comments",0),    month_ago_snap.get("comments",0))    if month_ago_snap else 0

        # Follower deltas
        today_fol_delta = delta(latest.get("total_followers",0), yest_snap.get("total_followers",0)) if yest_snap else 0
        week_fol_delta  = delta(latest.get("total_followers",0), week_ago_snap.get("total_followers",0)) if week_ago_snap else 0
        month_fol_delta = delta(latest.get("total_followers",0), month_ago_snap.get("total_followers",0)) if month_ago_snap else 0

        posts_count = sb.table("linkedin_posts").select("count", count="exact").execute().count or 0
        top_post = sb.table("linkedin_posts").select("title,impressions,reactions").order("impressions", desc=True).limit(1).execute().data
        top = top_post[0] if top_post else {}

        def fmt_d(v):
            return ("+" + str(v)) if v > 0 else str(v)

        msg = "\n" + chr(128188) + " <b>LINKEDIN ALERT</b>\n"
        msg += "----------------------------------------\n"
        msg += "Latest snapshot: <code>" + str(latest_date) + "</code>\n\n"

        msg += "<b>Today (delta vs yesterday)</b>\n"
        msg += "- Impressions: " + fmt_d(today_imp_delta) + "\n"
        msg += "- Reactions:   " + fmt_d(today_rxn_delta) + "\n"
        msg += "- Comments:    " + fmt_d(today_com_delta) + "\n"
        msg += "- Reposts:     " + fmt_d(today_rep_delta) + "\n"
        msg += "- Followers:   " + fmt_d(today_fol_delta) + "\n\n"

        msg += "<b>Last 7 Days (delta)</b>\n"
        msg += "- Impressions: " + fmt_d(week_imp_delta) + "\n"
        msg += "- Reactions:   " + fmt_d(week_rxn_delta) + "\n"
        msg += "- Comments:    " + fmt_d(week_com_delta) + "\n"
        msg += "- Followers:   " + fmt_d(week_fol_delta) + "\n\n"

        msg += "<b>Last 30 Days (delta)</b>\n"
        msg += "- Impressions: " + fmt_d(month_imp_delta) + "\n"
        msg += "- Reactions:   " + fmt_d(month_rxn_delta) + "\n"
        msg += "- Comments:    " + fmt_d(month_com_delta) + "\n"
        msg += "- Followers:   " + fmt_d(month_fol_delta) + "\n\n"

        msg += "<b>Current Totals (snapshot)</b>\n"
        msg += "- Total Impressions: <code>" + "{:,}".format(latest.get("impressions",0)) + "</code>\n"
        msg += "- Total Reactions:   <code>" + str(latest.get("reactions",0)) + "</code>\n"
        msg += "- Total Followers:   <code>" + "{:,}".format(latest.get("total_followers",0)) + "</code>\n"
        msg += "- Newsletter Subs:   <code>" + "{:,}".format(latest.get("newsletter_subscribers",0)) + "</code>\n\n"

        msg += "<b>Content</b>\n"
        msg += "- Posts tracked: <code>" + str(posts_count) + "</code>\n"
        if top:
            msg += "- Top: <code>" + _esc(top.get('title','')[:50]) + "</code> (" + "{:,}".format(top.get('impressions',0)) + " imp)\n"
        return msg
    except Exception as e:
        return "\n" + chr(128188) + " <b>LINKEDIN</b> Error: " + _esc(str(e))[:200] + "\n"



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
    """YouTube alert with daily/period deltas."""
    try:
        from youtube_command_center import get_cached_or_fetch
        d_today = get_cached_or_fetch(period_days=1)
        d_yest  = get_cached_or_fetch(period_days=2)
        d_week  = get_cached_or_fetch(period_days=7)
        d_2week = get_cached_or_fetch(period_days=14)
        d_month = get_cached_or_fetch(period_days=30)
        d_2month = get_cached_or_fetch(period_days=60)

        ch = d_month.get("channel", {}) or {}
        vids = d_month.get("videos", []) or []
        if not ch:
            return "\n" + chr(128250) + " <b>YOUTUBE</b> No data\n"

        def ana(d):
            a = d.get("analytics", {}) or {}
            return {
                "views":              a.get("views", 0),
                "watch_hours":        a.get("watch_hours", 0),
                "subscribers_gained": a.get("subscribers_gained", 0),
                "likes":              a.get("likes", 0),
                "comments":           a.get("comments", 0),
            }

        t = ana(d_today)
        y = ana(d_yest)
        w = ana(d_week)
        w2 = ana(d_2week)
        m = ana(d_month)
        m2 = ana(d_2month)

        # Deltas - prev period = total - current period
        y_views = max(0, y["views"] - t["views"])
        prev_w_views = max(0, w2["views"] - w["views"])
        prev_m_views = max(0, m2["views"] - m["views"])
        prev_w_watch = max(0, w2["watch_hours"] - w["watch_hours"])
        prev_m_watch = max(0, m2["watch_hours"] - m["watch_hours"])
        prev_w_subs = max(0, w2["subscribers_gained"] - w["subscribers_gained"])
        prev_m_subs = max(0, m2["subscribers_gained"] - m["subscribers_gained"])

        def fmt_d(v, p):
            if p == 0:
                return ("+" + str(round(v))) if v > 0 else str(round(v))
            d = v - p
            pct = (d/p*100) if p else 0
            sign = "+" if d >= 0 else ""
            return sign + str(round(d)) + " (" + sign + str(round(pct)) + "%)"

        total_likes = sum(v.get("likes",0) for v in vids)
        total_comments = sum(v.get("comments",0) for v in vids)
        engagement = ((total_likes+total_comments) / max(ch.get("total_views",1),1)) * 100
        dead_count = sum(1 for v in vids if v.get("views_per_day",0) < 1)
        top3 = sorted(vids, key=lambda v: v.get("views",0), reverse=True)[:3]

        msg = "\n" + chr(128250) + " <b>YOUTUBE ALERT</b>\n"
        msg += "----------------------------------------\n"
        msg += "Channel: <code>" + _esc(ch.get('title',''))[:40] + "</code>\n\n"

        msg += "<b>Today (vs yesterday)</b>\n"
        msg += "- Views: <code>" + str(t['views']) + "</code> " + fmt_d(t['views'], y_views) + "\n"
        msg += "- Watch: <code>" + str(round(t['watch_hours'],1)) + "h</code>\n"
        msg += "- Subs Gained: <code>" + str(t['subscribers_gained']) + "</code>\n\n"

        msg += "<b>Last 7 Days (vs previous 7d)</b>\n"
        msg += "- Views: <code>" + str(w['views']) + "</code> " + fmt_d(w['views'], prev_w_views) + "\n"
        msg += "- Watch: <code>" + str(round(w['watch_hours'],1)) + "h</code> " + fmt_d(w['watch_hours'], prev_w_watch) + "\n"
        msg += "- Subs Gained: <code>" + str(w['subscribers_gained']) + "</code> " + fmt_d(w['subscribers_gained'], prev_w_subs) + "\n\n"

        msg += "<b>Last 30 Days (vs previous 30d)</b>\n"
        msg += "- Views: <code>" + str(m['views']) + "</code> " + fmt_d(m['views'], prev_m_views) + "\n"
        msg += "- Watch: <code>" + str(round(m['watch_hours'],1)) + "h</code> " + fmt_d(m['watch_hours'], prev_m_watch) + "\n"
        msg += "- Subs Gained: <code>" + str(m['subscribers_gained']) + "</code> " + fmt_d(m['subscribers_gained'], prev_m_subs) + "\n\n"

        msg += "<b>Channel Totals (current)</b>\n"
        msg += "- Subs:   <code>" + "{:,}".format(ch.get('subscribers',0)) + "</code>\n"
        msg += "- Views:  <code>" + "{:,}".format(ch.get('total_views',0)) + "</code>\n"
        msg += "- Videos: <code>" + str(ch.get('video_count',0)) + "</code>\n"
        msg += "- Engagement: <code>" + str(round(engagement,2)) + "%</code>\n"
        msg += "- Dead videos: <code>" + str(dead_count) + "</code>\n\n"

        msg += "<b>Top 3 All-Time</b>\n"
        for v in top3:
            msg += "- <code>" + _esc(v.get('title','')[:50]) + "</code>: " + "{:,}".format(v.get('views',0)) + "v\n"
        return msg
    except Exception as e:
        return "\n" + chr(128250) + " <b>YOUTUBE</b> Error: " + _esc(str(e))[:200] + "\n"



def alert_ga4():
    """Show daily deltas - what NEW today, this week, this month."""
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
        from google.oauth2 import service_account
        import json as _json
        SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
        creds = None
        try:
            import streamlit as st
            sa = dict(st.secrets["ga4_service_account"])
            if "private_key" in sa:
                sa["private_key"] = sa["private_key"].replace("\\n","\n")
            creds = service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)
        except Exception:
            pass
        if not creds and os.path.exists("google_creds.json"):
            try:
                creds = service_account.Credentials.from_service_account_file("google_creds.json", scopes=SCOPES)
            except Exception:
                pass
        if not creds:
            gc = os.environ.get("GOOGLE_CREDS_JSON","")
            if gc:
                try:
                    sa = _json.loads(gc)
                    if "private_key" in sa:
                        sa["private_key"] = sa["private_key"].replace("\\n","\n")
                    creds = service_account.Credentials.from_service_account_info(sa, scopes=SCOPES)
                except Exception:
                    pass
        if not creds:
            return "\n" + chr(127760) + " <b>GA4</b> No credentials\n"

        pid = os.environ.get("GA4_PROPERTY_ID","374525971")
        try:
            import streamlit as st
            pid = str(st.secrets.get("GA4_PROPERTY_ID", pid)).strip()
        except Exception:
            pass

        client = BetaAnalyticsDataClient(credentials=creds)
        today_dt = date.today()
        today = today_dt.strftime("%Y-%m-%d")
        yest = (today_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        week_ago = (today_dt - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_week_ago = (today_dt - timedelta(days=14)).strftime("%Y-%m-%d")
        month_ago = (today_dt - timedelta(days=30)).strftime("%Y-%m-%d")
        prev_month_ago = (today_dt - timedelta(days=60)).strftime("%Y-%m-%d")

        def q(start, end):
            try:
                r = client.run_report(RunReportRequest(
                    property=f"properties/{pid}",
                    date_ranges=[DateRange(start_date=start, end_date=end)],
                    metrics=[Metric(name="sessions"), Metric(name="totalUsers"), Metric(name="screenPageViews")],
                ))
                if r.rows:
                    row = r.rows[0]
                    return (int(row.metric_values[0].value), int(row.metric_values[1].value), int(row.metric_values[2].value))
            except Exception:
                pass
            return (0, 0, 0)

        # Period values
        t_s, t_u, t_p = q(today, today)
        y_s, y_u, y_p = q(yest, yest)
        w_s, w_u, w_p = q(week_ago, today)
        pw_s, pw_u, pw_p = q(prev_week_ago, week_ago)
        m_s, m_u, m_p = q(month_ago, today)
        pm_s, pm_u, pm_p = q(prev_month_ago, month_ago)

        # If today is 0 (GA4 delay), use yesterday as today
        if t_s == 0:
            t_s, t_u, t_p = y_s, y_u, y_p
            today_label = "Yesterday (GA4 delay)"
        else:
            today_label = "Today"

        # Deltas
        def fmt_d(v, p):
            if p == 0:
                return ("+" + str(v)) if v > 0 else str(v)
            d = v - p
            pct = (d/p*100) if p else 0
            sign = "+" if d >= 0 else ""
            return sign + str(d) + " (" + sign + str(round(pct)) + "%)"

        # Top countries last 30d
        try:
            rc = client.run_report(RunReportRequest(
                property=f"properties/{pid}",
                date_ranges=[DateRange(start_date=month_ago, end_date=today)],
                dimensions=[Dimension(name="country")],
                metrics=[Metric(name="sessions")],
                limit=5,
            ))
            countries = [(rw.dimension_values[0].value, int(rw.metric_values[0].value)) for rw in rc.rows]
        except Exception:
            countries = []

        msg = "\n" + chr(127760) + " <b>GA4 ALERT</b>\n"
        msg += "----------------------------------------\n"
        msg += "<b>" + today_label + " (vs yesterday)</b>\n"
        msg += "- Users:      <code>" + str(t_u) + "</code> " + fmt_d(t_u, y_u) + "\n"
        msg += "- Sessions:   <code>" + str(t_s) + "</code> " + fmt_d(t_s, y_s) + "\n"
        msg += "- Page Views: <code>" + str(t_p) + "</code> " + fmt_d(t_p, y_p) + "\n\n"

        msg += "<b>Last 7 Days (vs previous 7d)</b>\n"
        msg += "- Users:      <code>" + str(w_u) + "</code> " + fmt_d(w_u, pw_u) + "\n"
        msg += "- Sessions:   <code>" + str(w_s) + "</code> " + fmt_d(w_s, pw_s) + "\n"
        msg += "- Page Views: <code>" + str(w_p) + "</code> " + fmt_d(w_p, pw_p) + "\n\n"

        msg += "<b>Last 30 Days (vs previous 30d)</b>\n"
        msg += "- Users:      <code>" + str(m_u) + "</code> " + fmt_d(m_u, pm_u) + "\n"
        msg += "- Sessions:   <code>" + str(m_s) + "</code> " + fmt_d(m_s, pm_s) + "\n"
        msg += "- Page Views: <code>" + str(m_p) + "</code> " + fmt_d(m_p, pm_p) + "\n\n"

        msg += "<b>Top 5 Countries (30d)</b>\n"
        for country, sess in countries:
            msg += "- " + _esc(country) + ": <code>" + "{:,}".format(sess) + "</code>\n"
        return msg
    except Exception as e:
        return "\n" + chr(127760) + " <b>GA4</b> Error: " + _esc(str(e))[:200] + "\n"



def alert_cross_platform():
    """Cross-platform correlation - daily deltas across all systems."""
    sb = _get_sb()
    if not sb:
        return ""
    try:
        today_dt = date.today()
        today = today_dt.strftime("%Y-%m-%d")
        yest = (today_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        week_ago = (today_dt - timedelta(days=7)).strftime("%Y-%m-%d")

        # KPI today vs yesterday
        s_t = sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",today).execute().count or 0
        s_y = sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",yest).lte("signup_date",yest).execute().count or 0
        s_w = sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",week_ago).execute().count or 0

        p_t = sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",today).execute().count or 0
        p_y = sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",yest).lte("first_payment_date",yest).execute().count or 0
        p_w = sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",week_ago).execute().count or 0

        u_t = sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",today).execute().count or 0
        u_y = sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",yest).lte("upload_date",yest).execute().count or 0

        # LinkedIn deltas
        li_today = sb.table("linkedin_highlights_daily").select("*").eq("snapshot_date", today).limit(1).execute().data or []
        li_yest = sb.table("linkedin_highlights_daily").select("*").eq("snapshot_date", yest).limit(1).execute().data or []
        li_imp_today = li_today[0].get("impressions",0) if li_today else 0
        li_imp_yest = li_yest[0].get("impressions",0) if li_yest else 0
        li_imp_delta = li_imp_today - li_imp_yest if li_yest else 0
        li_followers = (li_today[0] if li_today else (li_yest[0] if li_yest else {})).get("total_followers",0)

        # Patterns detection
        patterns = []
        if s_t == 0 and li_imp_today > 100:
            patterns.append("- LinkedIn has impressions today but ZERO signups - landing page issue?")
        if s_t > 5 and p_t == 0:
            patterns.append("- " + str(s_t) + " signups today but 0 paid - check upgrade flow")
        if li_followers > 2500 and s_t < 3:
            patterns.append("- " + "{:,}".format(li_followers) + " LinkedIn followers but only " + str(s_t) + " signups today - audience mismatch")
        if p_t > p_y * 2 and p_y > 0:
            patterns.append("- Paid customers SPIKED today (" + str(p_t) + " vs " + str(p_y) + " yesterday)")
        if not patterns:
            patterns.append("- No critical cross-platform issues detected today")

        def fmt_d(v, p):
            if p == 0:
                return ("+" + str(v)) if v > 0 else str(v)
            d = v - p
            sign = "+" if d >= 0 else ""
            return sign + str(d)

        msg = "\n" + chr(128279) + " <b>CROSS-PLATFORM CORRELATION</b>\n"
        msg += "----------------------------------------\n\n"

        msg += "<b>Today vs Yesterday</b>\n"
        msg += "- Signups:   <code>" + str(s_t) + "</code> " + fmt_d(s_t, s_y) + "\n"
        msg += "- Uploads:   <code>" + str(u_t) + "</code> " + fmt_d(u_t, u_y) + "\n"
        msg += "- Paid:      <code>" + str(p_t) + "</code> " + fmt_d(p_t, p_y) + "\n"
        msg += "- LI Impressions: " + fmt_d(li_imp_delta, 0) + "\n\n"

        msg += "<b>Last 7 Days Total</b>\n"
        msg += "- Signups: <code>" + str(s_w) + "</code>\n"
        msg += "- Paid:    <code>" + str(p_w) + "</code>\n\n"

        msg += "<b>Pattern Detection</b>\n"
        for p in patterns:
            msg += p + "\n"
        msg += "\n<b>System Health</b>\n"
        msg += "- KPI: " + ("[OK]" if s_t > 0 else "[!]") + " " + str(s_t) + " signups today\n"
        msg += "- LinkedIn: " + ("[OK]" if li_imp_today > 100 else "[!]") + " " + "{:,}".format(li_imp_today) + " impressions\n"
        msg += "- Stripe: " + ("[OK]" if p_t > 0 else "[!]") + " " + str(p_t) + " paid today\n"
        return msg
    except Exception as e:
        return "\n" + chr(128279) + " <b>CROSS-PLATFORM</b> Error: " + _esc(str(e))[:200] + "\n"



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
