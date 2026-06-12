"""
reporting_engine.py — v4 COMPREHENSIVE
LAYER 6 — REPORTING + NOTIFICATIONS
Sends Telegram (group) + Email + Slack with FULL SYSTEM report covering:
  1. KPI System (sign-ups, uploads, paid)
  2. GA4 Analytics (traffic, top pages, top sources)
  3. YouTube (channel stats, top videos, subscriber change)
  4. LinkedIn (followers, company info)
  5. Cross-Platform (correlations, unified summary)
  6. Stripe (revenue, payments)
  7. Pipeline health
"""
import os
import json
import re
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path("data_output")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Report] {msg}", flush=True)


def escape_md(text):
    """Escape special chars for Telegram MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(text))


def si(v):
    """Safe int conversion."""
    try:
        return int(float(v or 0))
    except Exception:
        return 0


def sf(v, decimals=1):
    """Safe float conversion."""
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _parse_report_date(raw):
    """Parse any date string to YYYY-MM-DD format for reporting."""
    if not raw or str(raw).strip() in ("", "nan", "None", "—", "-"):
        return ""
    from email.utils import parsedate_to_datetime as _pdt
    import re as _re
    raw = str(raw).strip()
    # Try RFC 2822
    try:
        dt = _pdt(raw)
        if dt:
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    # Try common formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%y, %I:%M %p",
                "%m/%d/%Y, %I:%M %p", "%b %d, %Y", "%d %b %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    # Regex: YYYY-MM-DD
    m = _re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    if m:
        try:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        except Exception:
            pass
    return ""


def fmt_num(n):
    """Format number with commas."""
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def send_telegram(message, parse_mode="MarkdownV2"):
    """Send message to Telegram group via Bot API."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token or not chat_id:
        log("Telegram skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
        return False

    try:
        url     = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = json.dumps({
            "chat_id":    chat_id,
            "text":       message,
            "parse_mode": parse_mode,
        }).encode("utf-8")

        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
            if body.get("ok"):
                log("✅ Telegram sent to group")
                return True
            log(f"Telegram API error: {body}")
            return False
    except Exception as e:
        log(f"Telegram failed: {e}")
        return False


def send_email(subject, body):
    sender   = os.environ.get("EMAIL_FROM", "").strip()
    password = os.environ.get("EMAIL_APP_PASSWORD", "").strip()
    receiver = os.environ.get("EMAIL_TO", "").strip()

    if not sender or not password or not receiver:
        log("Email skipped: EMAIL_FROM/EMAIL_APP_PASSWORD/EMAIL_TO not set")
        return False

    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        msg            = MIMEMultipart()
        msg["From"]    = sender
        msg["To"]      = receiver
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.send_message(msg)

        log(f"✅ Email sent to {receiver}")
        return True
    except Exception as e:
        log(f"Email failed: {e}")
        return False


def send_slack(text):
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        log("Slack skipped: SLACK_WEBHOOK_URL not set")
        return False
    try:
        payload = json.dumps({"text": f"```\n{text}\n```"}).encode()
        req = urllib.request.Request(
            webhook, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=15)
        log("✅ Slack sent")
        return True
    except Exception as e:
        log(f"Slack failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# DATA BUILDERS — each subsystem
# ═══════════════════════════════════════════════════════════════

def build_kpi_stats():
    """KPI System stats from Daily_Counts + verified tabs with overrides."""
    from sheets_writer import read_tab_data
    from manual_override_engine import load_overrides, normalize_email, ACTION_TO_STATUS

    today_str     = datetime.now().strftime("%Y-%m-%d")
    cur_month_str = datetime.now().strftime("%Y-%m")

    rows = read_tab_data("Daily_Counts")
    today_row  = next((r for r in rows if r.get("Date") == today_str), {})
    month_rows = [r for r in rows if str(r.get("Date", "")).startswith(cur_month_str)]

    stats = {
        "signups_today":  si(today_row.get("SignUps_Accepted", 0)),
        "uploads_today":  si(today_row.get("FirstUploads_Accepted", 0)),
        "paid_today":     si(today_row.get("PaidSubscribers_Accepted", 0)),
        "signups_month":  sum(si(r.get("SignUps_Accepted", 0))         for r in month_rows),
        "uploads_month":  sum(si(r.get("FirstUploads_Accepted", 0))    for r in month_rows),
        "paid_month":     sum(si(r.get("PaidSubscribers_Accepted", 0)) for r in month_rows),
        "signups_all":    sum(si(r.get("SignUps_Accepted", 0))         for r in rows),
        "uploads_all":    sum(si(r.get("FirstUploads_Accepted", 0))    for r in rows),
        "paid_all":       sum(si(r.get("PaidSubscribers_Accepted", 0)) for r in rows),
    }

    # Apply overrides to verified tabs for accurate live counts
    overrides = load_overrides()
    if overrides:
        try:
            for tab, date_col in [
                ("Verified_FREE", "Account Created On"),
                ("Verified_FIRST_UPLOAD", "Upload Date"),
                ("Verified_STRIPE", "Created"),
            ]:
                tab_rows = read_tab_data(tab)
                _ov_count = 0
                for r in tab_rows:
                    em = ""
                    for ek in ("Email", "email", "__email_normalized__"):
                        if r.get(ek) and "@" in str(r[ek]):
                            em = str(r[ek]).strip().lower()
                            break
                    norm = normalize_email(em) if em else ""
                    if norm and norm in overrides:
                        ov = overrides[norm]
                        action = ov.get("action", "")
                        m = ACTION_TO_STATUS.get(action, {})
                        if m:
                            r["final_status"] = m["final_status"]
                            _ov_count += 1

            # Rebuild counts from overridden data
            free_rows = read_tab_data("Verified_FREE")
            upload_rows = read_tab_data("Verified_FIRST_UPLOAD")
            stripe_rows = read_tab_data("Verified_STRIPE")

            stats["signups_all_override"] = sum(
                1 for r in free_rows
                if str(r.get("final_status", "")).upper() == "ACCEPTED"
            )
            stats["uploads_all_override"] = sum(
                1 for r in upload_rows
                if str(r.get("final_status", "")).upper() == "ACCEPTED"
            )
            stats["paid_all_override"] = sum(
                1 for r in stripe_rows
                if str(r.get("final_status", "")).upper() == "ACCEPTED"
            )
            # Month-level override counts
            stats["signups_month_override"] = sum(
                1 for r in free_rows
                if str(r.get("final_status", "")).upper() == "ACCEPTED"
                and any(
                    _parse_report_date(str(r.get(f, ""))).startswith(cur_month_str)
                    for f in ("row_date_used", "Account Created On", "__scraped_at__")
                )
            )
            stats["uploads_month_override"] = sum(
                1 for r in upload_rows
                if str(r.get("final_status", "")).upper() == "ACCEPTED"
                and any(
                    _parse_report_date(str(r.get(f, ""))).startswith(cur_month_str)
                    for f in ("row_date_used", "Upload Date", "__scraped_at__")
                )
            )
            stats["paid_month_override"] = sum(
                1 for r in stripe_rows
                if str(r.get("final_status", "")).upper() == "ACCEPTED"
                and any(
                    _parse_report_date(str(r.get(f, ""))).startswith(cur_month_str)
                    for f in ("row_date_used", "Created", "First payment", "__scraped_at__")
                )
            )
        except Exception as e:
            log(f"Override rebuild error: {e}")

    stats["today_str"] = today_str
    stats["month_str"] = cur_month_str
    stats["total_overrides"] = len(overrides) if overrides else 0
    return stats


def build_ga4_stats():
    """GA4 traffic summary."""
    import pandas as pd
    result = {"connected": False, "today_users": 0, "month_users": 0,
              "month_sessions": 0, "month_pageviews": 0, "top_sources": [],
              "top_pages": [], "top_countries": []}
    try:
        from ga4_connector import get_status
        status = get_status()
        result["connected"] = status.get("connected", False)
        if not result["connected"]:
            return result
    except Exception:
        return result

    try:
        from ga4_connector import fetch_utm_traffic, fetch_geo_traffic
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        utm = fetch_utm_traffic(start, end)
        if not utm.empty:
            result["month_sessions"] = si(utm.get("sessions", pd.Series([0])).sum())
            result["month_users"] = si(utm.get("activeUsers", pd.Series([0])).sum())
            if "sourceMedium" in utm.columns:
                top = utm.groupby("sourceMedium")["sessions"].sum().sort_values(ascending=False).head(5)
                result["top_sources"] = [(s, int(v)) for s, v in top.items()]

        geo = fetch_geo_traffic(start, end)
        if not geo.empty and "country" in geo.columns:
            top = geo.groupby("country")["sessions"].sum().sort_values(ascending=False).head(5)
            result["top_countries"] = [(c, int(v)) for c, v in top.items()]
    except Exception as e:
        log(f"GA4 stats error: {e}")
    return result


def build_youtube_stats():
    """YouTube channel summary."""
    import pandas as pd
    result = {"connected": False, "subscribers": 0, "total_views": 0,
              "video_count": 0, "top_videos": [], "period_views": 0,
              "period_subs_gained": 0, "period_watch_hours": 0,
              "has_analytics": False}
    try:
        from youtube_connector import is_configured, get_status
        if not is_configured():
            return result
        result["connected"] = True
    except Exception:
        return result

    try:
        from youtube_connector import get_channel_info, get_channel_videos, has_analytics_access
        ch = get_channel_info()
        result["subscribers"] = si(ch.get("subscribers", 0))
        result["total_views"] = si(ch.get("total_views", 0))
        result["video_count"] = si(ch.get("video_count", 0))

        vids = get_channel_videos(max_videos=200)
        if vids:
            top = sorted(vids, key=lambda v: v.get("views", 0), reverse=True)[:5]
            result["top_videos"] = [
                (v.get("title", "Untitled")[:40], si(v.get("views", 0)), si(v.get("likes", 0)))
                for v in top
            ]

        if has_analytics_access():
            result["has_analytics"] = True
            from youtube_connector import get_daily_analytics
            end = datetime.now().strftime("%Y-%m-%d")
            start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            daily = get_daily_analytics(start, end)
            if not daily.empty:
                result["period_views"] = si(daily.get("views", pd.Series([0])).sum())
                result["period_subs_gained"] = si(daily.get("subscribersGained", pd.Series([0])).sum())
                watch_min = sf(daily.get("estimatedMinutesWatched", pd.Series([0])).sum())
                result["period_watch_hours"] = round(watch_min / 60, 1)
    except Exception as e:
        log(f"YouTube stats error: {e}")
    return result


def build_linkedin_stats():
    """LinkedIn company page summary."""
    result = {"connected": False, "followers": 0, "company_name": "",
              "employees": "", "industry": ""}
    try:
        from linkedin_connector import is_configured, get_cached_metrics
        if not is_configured():
            return result
        result["connected"] = True
        metrics = get_cached_metrics()
        result["followers"] = si(metrics.get("followers", 0))
        result["company_name"] = metrics.get("company_name", "")
        result["employees"] = metrics.get("employees", "")
        result["industry"] = metrics.get("industry", "")
    except Exception as e:
        log(f"LinkedIn stats error: {e}")
    return result


def build_cross_platform_stats():
    """Cross-platform correlation summary."""
    result = {"available": False, "metrics": [], "top_correlations": []}
    try:
        cp_path = DATA_DIR / "cross_platform_cache.json"
        if cp_path.exists():
            with open(cp_path) as f:
                data = json.load(f)
                result["available"] = True
                result["metrics"] = list(data.get("unified_metrics", {}).keys())[:10]
                if "correlations" in data:
                    corrs = data["correlations"]
                    sorted_corrs = sorted(
                        [(k, abs(v)) for k, v in corrs.items() if abs(v) > 0.1],
                        key=lambda x: x[1], reverse=True
                    )[:5]
                    result["top_correlations"] = sorted_corrs
    except Exception as e:
        log(f"Cross-platform stats error: {e}")
    return result


def build_stripe_stats():
    """Stripe revenue summary."""
    result = {"connected": False, "total_paid": 0, "month_paid": 0,
              "today_paid": 0, "total_revenue": 0.0, "month_revenue": 0.0}
    try:
        from sheets_writer import read_tab_data
        stripe_rows = read_tab_data("Verified_STRIPE")
        today_str = datetime.now().strftime("%Y-%m-%d")
        month_str = datetime.now().strftime("%Y-%m")
        accepted = [r for r in stripe_rows if str(r.get("final_status", "")).upper() == "ACCEPTED"]
        result["total_paid"] = len(accepted)
        for r in accepted:
            # Try multiple date fields and parse properly
            date_val = ""
            for field in ("row_date_used", "Created", "First payment", "__scraped_at__"):
                raw = str(r.get(field, "")).strip()
                if raw and raw not in ("nan", "None", ""):
                    date_val = _parse_report_date(raw)
                    if date_val:
                        break
            # Revenue
            for amt_field in ("amount", "Total spend", "Total Spend", "__amount__"):
                amt_raw = r.get(amt_field, "")
                if amt_raw and str(amt_raw) not in ("nan", "None", ""):
                    try:
                        amt = float(str(amt_raw).replace("$", "").replace(",", "").strip())
                        result["total_revenue"] += amt
                        if date_val and date_val.startswith(month_str):
                            result["month_revenue"] += amt
                        break
                    except Exception:
                        pass
            if date_val:
                if date_val.startswith(today_str):
                    result["today_paid"] += 1
                if date_val.startswith(month_str):
                    result["month_paid"] += 1
        result["connected"] = len(stripe_rows) > 0
    except Exception as e:
        log(f"Stripe stats error: {e}")
    return result


def build_pipeline_health():
    """Pipeline health from cache."""
    result = {"last_run": "Never", "stages_passed": 0, "total_stages": 7,
              "duration": 0}
    try:
        hp_path = DATA_DIR / "pipeline_health.json"
        if hp_path.exists():
            with open(hp_path) as f:
                data = json.load(f)
                result["last_run"] = data.get("last_run", "Unknown")
                result["stages_passed"] = data.get("stages_passed", 0)
                result["duration"] = data.get("duration_seconds", 0)
    except Exception:
        pass
    return result


# ═══════════════════════════════════════════════════════════════
# MESSAGE BUILDERS
# ═══════════════════════════════════════════════════════════════

def build_telegram_kpi_section(stats):
    """Build KPI section for Telegram."""
    today = escape_md(stats["today_str"])
    month = escape_md(stats["month_str"])

    # Use override counts if available (more accurate)
    s_all = stats.get("signups_all_override", stats["signups_all"])
    u_all = stats.get("uploads_all_override", stats["uploads_all"])
    p_all = stats.get("paid_all_override", stats["paid_all"])
    s_month = stats.get("signups_month_override", stats["signups_month"])
    u_month = stats.get("uploads_month_override", stats["uploads_month"])
    p_month = stats.get("paid_month_override", stats["paid_month"])

    s2u = (u_all / s_all * 100) if s_all > 0 else 0
    u2p = (p_all / u_all * 100) if u_all > 0 else 0
    s2p = (p_all / s_all * 100) if s_all > 0 else 0

    section = (
        f"🦅 *EAGLE3D KPI — {today}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"📊 *KPI SYSTEM*\n"
        f"┌────────────────────────\n"
        f"│ 📅 *Today*\n"
        f"│ ✅ Sign\\\\-ups: `{stats['signups_today']}`\n"
        f"│ 📤 Uploads:  `{stats['uploads_today']}`\n"
        f"│ 💳 Paid:     `{stats['paid_today']}`\n"
        f"├────────────────────────\n"
        f"│ 📆 *Month \\\\({month}\\\\)*\n"
        f"│ ✅ Sign\\\\-ups: `{s_month}`\n"
        f"│ 📤 Uploads:  `{u_month}`\n"
        f"│ 💳 Paid:     `{p_month}`\n"
        f"├────────────────────────\n"
        f"│ 🏆 *All Time*\n"
        f"│ ✅ Sign\\\\-ups: `{s_all}`\n"
        f"│ 📤 Uploads:  `{u_all}`\n"
        f"│ 💳 Paid:     `{p_all}`\n"
        f"├────────────────────────\n"
        f"│ 🔄 *Conversion Rates*\n"
        f"│ S→U: `{s2u:.1f}%` | U→P: `{u2p:.1f}%` | S→P: `{s2p:.1f}%`\n"
        f"└────────────────────────\n"
    )
    if stats.get("total_overrides", 0) > 0:
        section += f"│ ✏️ Active Overrides: `{stats['total_overrides']}`\n"
    return section


def build_telegram_ga4_section(ga4):
    """Build GA4 section."""
    if not ga4["connected"]:
        return "🌐 *GA4 ANALYTICS* — Not connected\n"
    section = (
        f"\n🌐 *GA4 ANALYTICS* ✅\n"
        f"┌────────────────────────\n"
        f"│ 👥 Users \\(30d\\): `{ga4['month_users']}`\n"
        f"│ 📊 Sessions \\(30d\\): `{ga4['month_sessions']}`\n"
    )
    if ga4["top_sources"]:
        section += "│ 📈 *Top Sources:*\n"
        for src, count in ga4["top_sources"][:3]:
            section += f"│   • {escape_md(src)}: `{count}`\n"
    if ga4["top_countries"]:
        section += "│ 🌍 *Top Countries:*\n"
        for country, count in ga4["top_countries"][:3]:
            section += f"│   • {escape_md(country)}: `{count}`\n"
    section += "└────────────────────────\n"
    return section


def build_telegram_youtube_section(yt):
    """Build YouTube section."""
    if not yt["connected"]:
        return "📺 *YOUTUBE* — Not connected\n"
    section = (
        f"\n📺 *YOUTUBE* ✅\n"
        f"┌────────────────────────\n"
        f"│ 👥 Subscribers: `{yt['subscribers']}`\n"
        f"│ 👁 Total Views: `{yt['total_views']}`\n"
        f"│ 🎬 Videos: `{yt['video_count']}`\n"
    )
    if yt["has_analytics"]:
        section += (
            f"│ ─── 30d Analytics ───\n"
            f"│ 👁 Views: `{yt['period_views']}`\n"
            f"│ ⏱ Watch: `{yt['period_watch_hours']}`h\n"
            f"│ 👤 Subs Gained: `{yt['period_subs_gained']}`\n"
        )
    if yt["top_videos"]:
        section += "│ 🔥 *Top Videos:*\n"
        for title, views, likes in yt["top_videos"][:3]:
            section += f"│   • {escape_md(title)}: `{views}` views\n"
    section += "└────────────────────────\n"
    return section


def build_telegram_linkedin_section(li):
    """Build LinkedIn section."""
    if not li["connected"]:
        return "💼 *LINKEDIN* — Not connected\n"
    section = (
        f"\n💼 *LINKEDIN* ✅\n"
        f"┌────────────────────────\n"
        f"│ 👥 Followers: `{li['followers']}`\n"
    )
    if li["company_name"]:
        section += f"│ 🏢 {escape_md(li['company_name'])}\n"
    if li["employees"]:
        section += f"│ 👔 Employees: {escape_md(li['employees'])}\n"
    if li["industry"]:
        section += f"│ 🏭 {escape_md(li['industry'])}\n"
    section += "└────────────────────────\n"
    return section


def build_telegram_stripe_section(stripe):
    """Build Stripe section."""
    if not stripe["connected"]:
        return "💳 *STRIPE* — No data\n"
    section = (
        f"\n💳 *STRIPE PAYMENTS*\n"
        f"┌────────────────────────\n"
        f"│ 📅 Today: `{stripe['today_paid']}` paid\n"
        f"│ 📆 Month: `{stripe['month_paid']}` paid\n"
        f"│ 🏆 All Time: `{stripe['total_paid']}` paid\n"
    )
    if stripe.get("month_revenue", 0) > 0:
        section += f"│ 💰 Month Revenue: `${stripe['month_revenue']:,.2f}`\n"
    if stripe.get("total_revenue", 0) > 0:
        section += f"│ 💰 Total Revenue: `${stripe['total_revenue']:,.2f}`\n"
    section += "└────────────────────────\n"
    return section


def build_telegram_pipeline_section(health):
    """Build pipeline health section."""
    section = (
        f"\n⚙️ *PIPELINE HEALTH*\n"
        f"┌────────────────────────\n"
        f"│ 🕐 Last Run: {escape_md(health['last_run'][:19] if health['last_run'] != 'Never' else 'Never')}\n"
        f"│ ✅ Stages: `{health['stages_passed']}/{health['total_stages']}`\n"
        f"│ ⏱ Duration: `{health['duration']:.0f}s`\n"
        f"└────────────────────────\n"
    )
    return section


def build_full_telegram_message(kpi, ga4, yt, li, stripe, health):
    """Build the comprehensive multi-section Telegram message."""
    msg = build_telegram_kpi_section(kpi)
    msg += build_telegram_ga4_section(ga4)
    msg += build_telegram_youtube_section(yt)
    msg += build_telegram_linkedin_section(li)
    msg += build_telegram_stripe_section(stripe)
    msg += build_telegram_pipeline_section(health)
    msg += (
        f"\n🔗 [Dashboard](https://eagle3d\\\\-kpi\\\\-automation\\\\.streamlit\\\\.app/)\n"
        f"_Auto\\\\-generated at " + datetime.utcnow().strftime("%H:%M") + " UTC_"
    )
    return msg


# ═══════════════════════════════════════════════════════════════
# INDIVIDUAL SUBSYSTEM TELEGRAM MESSAGES (sent separately for detail)
# ═══════════════════════════════════════════════════════════════

def send_subsystem_reports():
    """Send individual detailed reports for each subsystem."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        return

    import pandas as pd

    # ── KPI DETAILED ──
    try:
        kpi = build_kpi_stats()
        kpi_msg = build_telegram_kpi_section(kpi)
        send_telegram(kpi_msg)
    except Exception as e:
        log(f"KPI report error: {e}")

    # ── GA4 DETAILED ──
    try:
        ga4 = build_ga4_stats()
        if ga4["connected"]:
            ga4_msg = build_telegram_ga4_section(ga4)
            # Add page-level details
            try:
                from ga4_connector import fetch_page_performance
                end = datetime.now().strftime("%Y-%m-%d")
                start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                pages = fetch_page_performance(start, end)
                if not pages.empty and "pagePath" in pages.columns:
                    top_pages = pages.nlargest(5, "screenPageViews") if "screenPageViews" in pages.columns else pages.head(5)
                    ga4_msg += "\n📄 *Top Pages:*\n"
                    for _, r in top_pages.iterrows():
                        path = str(r.get("pagePath", ""))[:50]
                        views = si(r.get("screenPageViews", 0))
                        ga4_msg += f"  • {escape_md(path)}: `{views}` views\n"
            except Exception:
                pass
            send_telegram(ga4_msg)
    except Exception as e:
        log(f"GA4 report error: {e}")

    # ── YOUTUBE DETAILED ──
    try:
        yt = build_youtube_stats()
        if yt["connected"]:
            yt_msg = build_telegram_youtube_section(yt)
            # Add video performance table
            if yt["top_videos"]:
                yt_msg += "\n🎬 *Video Performance:*\n"
                for i, (title, views, likes) in enumerate(yt["top_videos"], 1):
                    eng = (likes / views * 100) if views > 0 else 0
                    yt_msg += f"  {i}\\. {escape_md(title)}\n"
                    yt_msg += f"     👁 `{views}` 👍 `{likes}` 📊 `{eng:.1f}%`\n"
            send_telegram(yt_msg)
    except Exception as e:
        log(f"YouTube report error: {e}")

    # ── LINKEDIN DETAILED ──
    try:
        li = build_linkedin_stats()
        if li["connected"]:
            li_msg = build_telegram_linkedin_section(li)
            send_telegram(li_msg)
    except Exception as e:
        log(f"LinkedIn report error: {e}")

    # ── STRIPE DETAILED ──
    try:
        stripe = build_stripe_stats()
        if stripe["connected"]:
            stripe_msg = build_telegram_stripe_section(stripe)
            send_telegram(stripe_msg)
    except Exception as e:
        log(f"Stripe report error: {e}")

    # ── CROSS-PLATFORM ──
    try:
        cp = build_cross_platform_stats()
        if cp["available"]:
            cp_msg = "🔗 *CROSS-PLATFORM CORRELATION*\n┌────────────────────────\n"
            if cp["top_correlations"]:
                for metric, strength in cp["top_correlations"][:5]:
                    name = escape_md(metric.replace("_", " ").title())
                    cp_msg += f"│ {name}: `{strength:.2f}`\n"
            else:
                cp_msg += "│ No strong correlations found\n"
            cp_msg += "└────────────────────────\n"
            send_telegram(cp_msg)
    except Exception as e:
        log(f"Cross-platform report error: {e}")

    # ── PIPELINE HEALTH ──
    try:
        health = build_pipeline_health()
        pipe_msg = build_telegram_pipeline_section(health)
        send_telegram(pipe_msg)
    except Exception as e:
        log(f"Pipeline report error: {e}")


def build_text_report(kpi, ga4, yt, li, stripe, health):
    """Build plain text version for email/Slack."""
    today = kpi["today_str"]
    month = kpi["month_str"]
    s_all = kpi.get("signups_all_override", kpi["signups_all"])
    u_all = kpi.get("uploads_all_override", kpi["uploads_all"])
    p_all = kpi.get("paid_all_override", kpi["paid_all"])
    s_month = kpi.get("signups_month_override", kpi["signups_month"])
    u_month = kpi.get("uploads_month_override", kpi["uploads_month"])
    p_month = kpi.get("paid_month_override", kpi["paid_month"])

    lines = [
        "🦅 EAGLE3D FULL SYSTEM REPORT",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 60,
        "",
        "📊 KPI SYSTEM",
        "-" * 40,
        f"  TODAY ({today}):",
        f"    Sign-ups:      {kpi['signups_today']}",
        f"    First Uploads: {kpi['uploads_today']}",
        f"    Paid:          {kpi['paid_today']}",
        "",
        f"  MONTH ({month}):",
        f"    Sign-ups:      {s_month}",
        f"    First Uploads: {u_month}",
        f"    Paid:          {p_month}",
        "",
        "  ALL TIME:",
        f"    Sign-ups:      {s_all}",
        f"    First Uploads: {u_all}",
        f"    Paid:          {p_all}",
        "",
        f"  Active Overrides: {kpi.get('total_overrides', 0)}",
        "",
    ]

    lines.append("🌐 GA4 ANALYTICS")
    lines.append("-" * 40)
    if ga4["connected"]:
        lines.append(f"  Users (30d):    {ga4['month_users']:,}")
        lines.append(f"  Sessions (30d): {ga4['month_sessions']:,}")
        if ga4["top_sources"]:
            lines.append("  Top Sources:")
            for src, count in ga4["top_sources"][:5]:
                lines.append(f"    {src}: {count:,}")
        if ga4["top_countries"]:
            lines.append("  Top Countries:")
            for c, count in ga4["top_countries"][:5]:
                lines.append(f"    {c}: {count:,}")
    else:
        lines.append("  Not connected")
    lines.append("")

    lines.append("📺 YOUTUBE")
    lines.append("-" * 40)
    if yt["connected"]:
        lines.append(f"  Subscribers:  {yt['subscribers']:,}")
        lines.append(f"  Total Views:  {yt['total_views']:,}")
        lines.append(f"  Videos:       {yt['video_count']}")
        if yt["has_analytics"]:
            lines.append(f"  30d Views:    {yt['period_views']:,}")
            lines.append(f"  30d Watch:    {yt['period_watch_hours']}h")
            lines.append(f"  30d Subs:     +{yt['period_subs_gained']}")
        if yt["top_videos"]:
            lines.append("  Top Videos:")
            for title, views, likes in yt["top_videos"][:5]:
                lines.append(f"    {title}: {views:,} views, {likes:,} likes")
    else:
        lines.append("  Not connected")
    lines.append("")

    lines.append("💼 LINKEDIN")
    lines.append("-" * 40)
    if li["connected"]:
        lines.append(f"  Followers: {li['followers']:,}")
        if li["company_name"]:
            lines.append(f"  Company:   {li['company_name']}")
        if li["employees"]:
            lines.append(f"  Employees: {li['employees']}")
        if li["industry"]:
            lines.append(f"  Industry:  {li['industry']}")
    else:
        lines.append("  Not connected")
    lines.append("")

    lines.append("💳 STRIPE")
    lines.append("-" * 40)
    if stripe["connected"]:
        lines.append(f"  Today:     {stripe['today_paid']} paid")
        lines.append(f"  Month:     {stripe['month_paid']} paid")
        lines.append(f"  All Time:  {stripe['total_paid']} paid")
    else:
        lines.append("  No data")
    lines.append("")

    lines.append("⚙️ PIPELINE HEALTH")
    lines.append("-" * 40)
    lines.append(f"  Last Run:  {health['last_run'][:19] if health['last_run'] != 'Never' else 'Never'}")
    lines.append(f"  Stages:    {health['stages_passed']}/{health['total_stages']}")
    lines.append(f"  Duration:  {health['duration']:.0f}s")
    lines.append("")
    lines.append("Dashboard: https://eagle3d-kpi-automation.streamlit.app/")

    return "\n".join(lines)


def main():
    import pandas as pd

    log("=" * 60)
    log("REPORTING ENGINE v4 — FULL SYSTEM")
    log("=" * 60)

    # Build all subsystem data
    log("Building KPI stats...")
    try:
        kpi = build_kpi_stats()
    except Exception as e:
        log(f"KPI stats failed: {e}")
        kpi = {
            "signups_today": 0, "uploads_today": 0, "paid_today": 0,
            "signups_month": 0, "uploads_month": 0, "paid_month": 0,
            "signups_all": 0, "uploads_all": 0, "paid_all": 0,
            "today_str": datetime.now().strftime("%Y-%m-%d"),
            "month_str": datetime.now().strftime("%Y-%m"),
            "total_overrides": 0,
        }

    log("Building GA4 stats...")
    try:
        ga4 = build_ga4_stats()
    except Exception as e:
        log(f"GA4 stats failed: {e}")
        ga4 = {"connected": False}

    log("Building YouTube stats...")
    try:
        yt = build_youtube_stats()
    except Exception as e:
        log(f"YouTube stats failed: {e}")
        yt = {"connected": False}

    log("Building LinkedIn stats...")
    try:
        li = build_linkedin_stats()
    except Exception as e:
        log(f"LinkedIn stats failed: {e}")
        li = {"connected": False}

    log("Building Stripe stats...")
    try:
        stripe = build_stripe_stats()
    except Exception as e:
        log(f"Stripe stats failed: {e}")
        stripe = {"connected": False}

    log("Building pipeline health...")
    try:
        health = build_pipeline_health()
    except Exception as e:
        log(f"Pipeline health failed: {e}")
        health = {"last_run": "Unknown", "stages_passed": 0, "total_stages": 7, "duration": 0}

    # Build combined text report
    text = build_text_report(kpi, ga4, yt, li, stripe, health)
    log("\n" + text)

    # Save report
    report_file = DATA_DIR / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report_file.parent.mkdir(exist_ok=True)
    report_file.write_text(text)
    log(f"Saved: {report_file}")

    # Send comprehensive Telegram (combined summary message)
    log("Sending combined Telegram summary...")
    combined_msg = build_full_telegram_message(kpi, ga4, yt, li, stripe, health)
    tg_combined = send_telegram(combined_msg)

    # Send individual subsystem Telegram messages for detail
    log("Sending individual subsystem Telegram reports...")
    try:
        send_subsystem_reports()
    except Exception as e:
        log(f"Subsystem reports error: {e}")

    # Send email + Slack with full text report
    log("Sending email + Slack...")
    em_sent = send_email(f"Eagle3D Full System Report — {kpi['today_str']}", text)
    sl_sent = send_slack(text)

    log("\nNotification results:")
    log(f"  Telegram: {'✅ sent' if tg_combined else '❌ not sent'}")
    log(f"  Email:    {'✅ sent' if em_sent else '❌ not sent'}")
    log(f"  Slack:    {'✅ sent' if sl_sent else '❌ not sent'}")


if __name__ == "__main__":
    main()
