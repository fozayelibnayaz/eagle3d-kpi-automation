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
        from datetime import date as _d, timedelta as _td
        today = _d.today()
        week_ago  = (today - _td(days=7)).isoformat()
        month_ago = (today - _td(days=30)).isoformat()
        year_ago  = (today - _td(days=365)).isoformat()
        today_str = today.isoformat()
        yest_str  = (today - _td(days=1)).isoformat()

        def hl(start, end=today_str):
            r = sb.table("linkedin_highlights_daily").select("*").gte("snapshot_date", start).lte("snapshot_date", end).order("snapshot_date", desc=True).execute().data or []
            if not r:
                return {}
            # Sum impressions/reactions/comments (deltas) over range
            agg = {"impressions":0, "reactions":0, "comments":0, "reposts":0, "clicks":0,
                   "page_views":0, "unique_visitors":0}
            for row in r:
                for k in agg:
                    agg[k] += row.get(k, 0) or 0
            agg["total_followers"]        = r[0].get("total_followers", 0)
            agg["newsletter_subscribers"] = r[0].get("newsletter_subscribers", 0)
            agg["snapshot_date"]          = r[0].get("snapshot_date", "")
            return agg

        latest = hl(today_str, today_str)
        if not latest:
            # Try yesterday as "latest"
            latest = hl(yest_str, yest_str)
        if not latest:
            return "
💼 <b>LINKEDIN</b> No data yet
"

        week  = hl(week_ago)
        month = hl(month_ago)
        all_time = hl("2020-01-01")

        posts_count = sb.table("linkedin_posts").select("count", count="exact").execute().count or 0
        top_post = sb.table("linkedin_posts").select("title,impressions,reactions").order("impressions", desc=True).limit(1).execute().data
        top = top_post[0] if top_post else {}

        msg = (
            f"
�� <b>LINKEDIN ALERT</b>
"
            f"━━━━━━━━━━━━━━━━━━━━━━
"
            f"📅 Latest snapshot: <code>{latest.get('snapshot_date', 'N/A')}</code>

"
            f"📊 <b>Today</b>
"
            f"├ 👁 Impressions: <code>{latest.get('impressions',0):,}</code>
"
            f"├ 👍 Reactions:   <code>{latest.get('reactions',0)}</code>
"
            f"├ 💬 Comments:    <code>{latest.get('comments',0)}</code>
"
            f"└ 🔁 Reposts:     <code>{latest.get('reposts',0)}</code>

"
            f"📅 <b>Last 7 Days</b>
"
            f"├ 👁 Impressions: <code>{week.get('impressions',0):,}</code>
"
            f"├ 👍 Reactions:   <code>{week.get('reactions',0)}</code>
"
            f"└ 💬 Comments:    <code>{week.get('comments',0)}</code>

"
            f"📆 <b>Last 30 Days</b>
"
            f"├ 👁 Impressions: <code>{month.get('impressions',0):,}</code>
"
            f"├ 👍 Reactions:   <code>{month.get('reactions',0)}</code>
"
            f"└ 💬 Comments:    <code>{month.get('comments',0)}</code>

"
            f"🏆 <b>All Time</b>
"
            f"├ 👁 Impressions: <code>{all_time.get('impressions',0):,}</code>
"
            f"├ 👍 Reactions:   <code>{all_time.get('reactions',0)}</code>
"
            f"└ 💬 Comments:    <code>{all_time.get('comments',0)}</code>

"
            f"👥 <b>Audience (now)</b>
"
            f"├ Followers:        <code>{latest.get('total_followers',0):,}</code>
"
            f"├ Page Views:       <code>{latest.get('page_views',0):,}</code>
"
            f"├ Unique Visitors:  <code>{latest.get('unique_visitors',0):,}</code>
"
            f"└ Newsletter:       <code>{latest.get('newsletter_subscribers',0):,}</code>

"
            f"📝 <b>Content</b>
"
            f"├ Posts tracked: <code>{posts_count}</code>
"
        )
        if top:
            msg += f"└ 🏆 Top: <code>{_esc(top.get('title','')[:50])}</code> ({top.get('impressions',0):,} imp)
"
        return msg
    except Exception as e:
        return f"
💼 <b>LINKEDIN</b> Error: {_esc(str(e))[:200]}
"


if __name__ == "__main__":
    send_all_alerts()
