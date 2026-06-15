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


def escape_html(text):
    """Escape special chars for Telegram HTML parse mode (reliable, unlike MarkdownV2)."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# Keep escape_md as alias for backward compat
def escape_md(text):
    return escape_html(text)


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


def send_telegram(message, parse_mode="HTML"):
    """Send message to Telegram group via Bot API. Uses HTML (reliable)."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    
    # Fallback to st.secrets (for Streamlit Cloud dashboard buttons)
    if not bot_token or not chat_id:
        try:
            import streamlit as st
            if "TELEGRAM_BOT_TOKEN" in st.secrets:
                val = st.secrets["TELEGRAM_BOT_TOKEN"]
                if val and str(val).strip():
                    bot_token = str(val).strip()
            if "TELEGRAM_CHAT_ID" in st.secrets:
                val = st.secrets["TELEGRAM_CHAT_ID"]
                if val and str(val).strip():
                    chat_id = str(val).strip()
        except Exception:
            pass

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

    # Fallback to local JSON if Sheets returned empty
    if not rows:
        try:
            _dc_path = Path("data_output") / "daily_counts.json"
            if _dc_path.exists():
                rows = json.loads(_dc_path.read_text())
                log(f"KPI stats: using local JSON fallback ({len(rows)} rows)")
        except Exception:
            pass
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
    """GA4 traffic summary — tries API, then cached files."""
    import pandas as pd
    result = {"connected": False, "today_users": 0, "month_users": 0,
              "month_sessions": 0, "month_pageviews": 0, "top_sources": [],
              "top_pages": [], "top_countries": []}

    # Try API first
    try:
        from ga4_connector import get_status
        status = get_status()
        if status.get("connected"):
            from ga4_connector import fetch_utm_traffic, fetch_geo_traffic
            end = datetime.now().strftime("%Y-%m-%d")
            start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            utm = fetch_utm_traffic(start, end)
            if not utm.empty:
                result["connected"] = True
                result["month_sessions"] = si(utm.get("sessions", pd.Series([0])).sum())
                result["month_users"] = si(utm.get("activeUsers", pd.Series([0])).sum())
                if "sourceMedium" in utm.columns:
                    top = utm.groupby("sourceMedium")["sessions"].sum().sort_values(ascending=False).head(5)
                    result["top_sources"] = [(s, int(v)) for s, v in top.items()]
            geo = fetch_geo_traffic(start, end)
            if not geo.empty and "country" in geo.columns:
                top = geo.groupby("country")["sessions"].sum().sort_values(ascending=False).head(5)
                result["top_countries"] = [(c, int(v)) for c, v in top.items()]
            if result["connected"]:
                return result
    except Exception as e:
        log(f"GA4 API error: {e}")

    # Fallback: read cached GA4 data from pipeline
    try:
        _ga4_cache = DATA_DIR / "ga4_traffic_cache.json"
        if _ga4_cache.exists():
            _data = json.loads(_ga4_cache.read_text())
            if _data:
                result["connected"] = True
                result["month_sessions"] = si(_data.get("total_sessions", 0))
                result["month_users"] = si(_data.get("total_users", 0))
                if _data.get("top_sources"):
                    result["top_sources"] = _data["top_sources"][:5]
                if _data.get("top_countries"):
                    result["top_countries"] = _data["top_countries"][:5]
                log("GA4 stats: using cached data")
    except Exception as e:
        log(f"GA4 cache error: {e}")
    return result


def build_youtube_stats():
    """YouTube channel summary — tries API, then cached files."""
    import pandas as pd
    result = {"connected": False, "subscribers": 0, "total_views": 0,
              "video_count": 0, "top_videos": [], "period_views": 0,
              "period_subs_gained": 0, "period_watch_hours": 0,
              "has_analytics": False}

    # Try API first
    try:
        from youtube_connector import is_configured
        if is_configured():
            from youtube_connector import get_channel_info, get_channel_videos, has_analytics_access
            result["connected"] = True
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
            if result["connected"]:
                return result
    except Exception as e:
        log(f"YouTube API error: {e}")

    # Fallback: read cached YouTube data from pipeline
    try:
        _yt_ch = DATA_DIR / "youtube_channel.json"
        if _yt_ch.exists():
            _ch = json.loads(_yt_ch.read_text())
            result["connected"] = True
            result["subscribers"] = si(_ch.get("subscribers", 0))
            result["total_views"] = si(_ch.get("total_views", 0))
            result["video_count"] = si(_ch.get("video_count", 0))
        _yt_vids = DATA_DIR / "youtube_videos.json"
        if _yt_vids.exists():
            _vids = json.loads(_yt_vids.read_text())
            if _vids:
                top = sorted(_vids, key=lambda v: v.get("views", 0), reverse=True)[:5]
                result["top_videos"] = [
                    (v.get("title", "Untitled")[:40], si(v.get("views", 0)), si(v.get("likes", 0)))
                    for v in top
                ]
        if result.get("connected"):
            log("YouTube stats: using cached data")
    except Exception as e:
        log(f"YouTube cache error: {e}")
    return result


def build_linkedin_stats():
    """LinkedIn company page summary."""
    result = {"connected": False, "followers": 0, "company_name": "",
              "employees": "", "industry": ""}
    try:
        from linkedin_connector import is_configured, get_cached_metrics, get_manual_history, get_posts
        if not is_configured():
            # Still try to read cached data
            pass
        else:
            result["connected"] = True
        metrics = get_cached_metrics()
        if metrics and not metrics.get("error"):
            result["followers"] = si(metrics.get("followers", 0))
            result["company_name"] = metrics.get("company_name", "")
            result["employees"] = metrics.get("employees", "")
            result["industry"] = metrics.get("industry", "")
            result["connected"] = True
        # Fallback: try daily history for latest follower count
        if result["followers"] == 0:
            try:
                hist = get_manual_history()
                if not hist.empty and "followers" in hist.columns:
                    latest = hist.dropna(subset=["followers"]).tail(1)
                    if not latest.empty:
                        result["followers"] = si(latest["followers"].iloc[0])
                        result["connected"] = True
            except Exception:
                pass
        # Add posts summary
        try:
            posts = get_posts()
            if posts:
                result["post_count"] = len(posts)
                result["total_likes"] = sum(p.get("likes", 0) for p in posts)
                result["total_comments"] = sum(p.get("comments", 0) for p in posts)
        except Exception:
            pass
    except Exception as e:
        log(f"LinkedIn stats error: {e}")
    return result


def build_cross_platform_stats():
    """Cross-platform correlation summary — builds from all available data."""
    result = {"available": False, "metrics": [], "top_correlations": []}
    try:
        # Try cached data first
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

        # If no cache, build from available data sources
        if not result["available"]:
            import pandas as pd
            from cross_platform_engine import build_unified_timeline, compute_correlations
            # Get KPI data
            _kpi_df = pd.DataFrame()
            try:
                _dc_path = DATA_DIR / "daily_counts.json"
                if _dc_path.exists():
                    _kpi_df = pd.read_json(str(_dc_path))
            except Exception:
                pass
            # Get YouTube data
            _yt_df = pd.DataFrame()
            try:
                _yt_path = DATA_DIR / "youtube_daily.json"
                if _yt_path.exists():
                    _yt_data = json.loads(_yt_path.read_text())
                    _yt_rows = [{"date": d, **v} for d, v in _yt_data.items()]
                    _yt_df = pd.DataFrame(_yt_rows)
            except Exception:
                pass
            # Get LinkedIn data
            _li_df = pd.DataFrame()
            try:
                from linkedin_connector import get_manual_history
                _li_df = get_manual_history()
            except Exception:
                pass
            # Get GA4 data
            _ga4_df = pd.DataFrame()
            try:
                _ga4_cache = DATA_DIR / "ga4_traffic_cache.json"
                if _ga4_cache.exists():
                    _ga4_data = json.loads(_ga4_cache.read_text())
                    if _ga4_data.get("daily"):
                        _ga4_df = pd.DataFrame(_ga4_data["daily"])
            except Exception:
                pass

            if not _kpi_df.empty or not _yt_df.empty or not _li_df.empty:
                _unified = build_unified_timeline(
                    kpi_df=_kpi_df, ga4_df=_ga4_df,
                    youtube_daily=_yt_df, linkedin_daily=_li_df,
                )
                if not _unified.empty:
                    _corrs = compute_correlations(_unified)
                    result["available"] = True
                    _num_cols = [c for c in _unified.columns if c != "date" and pd.api.types.is_numeric_dtype(_unified[c])]
                    result["metrics"] = _num_cols[:10]
                    if _corrs:
                        sorted_corrs = sorted(
                            [(k, abs(v)) for k, v in _corrs.items() if abs(v) > 0.1],
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
            for field in ("First payment", "row_date_used", "Created", "__scraped_at__"):
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
    """Build KPI section for Telegram (HTML parse mode)."""
    today = escape_html(stats["today_str"])
    month = escape_html(stats["month_str"])

    # Use override counts if available (more accurate)
    s_all = stats.get("signups_all_override", stats["signups_all"])
    u_all = stats.get("uploads_all_override", stats["uploads_all"])
    p_all = stats.get("paid_all_override", stats["paid_all"])
    s_month = stats.get("signups_month_override", stats["signups_month"])
    u_month = stats.get("uploads_month_override", stats["uploads_month"])
    p_month = stats.get("paid_month_override", stats["paid_month"])

    s2u = (u_all / s_all * 100) if s_all > 0 else 0
    s2p = (p_all / s_all * 100) if s_all > 0 else 0
    u2p = (p_all / u_all * 100) if u_all > 0 else 0

    section = (
        f"🦅 <b>EAGLE3D KPI — {today}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"📊 <b>KPI SYSTEM</b>\n"
        f"┌────────────────────────\n"
        f"│ 📅 <b>Today</b>\n"
        f"│ ✅ Sign-ups: <code>{stats['signups_today']}</code>\n"
        f"│ 📤 Uploads:  <code>{stats['uploads_today']}</code>\n"
        f"│ 💳 Paid:     <code>{stats['paid_today']}</code>\n"
        f"├────────────────────────\n"
        f"│ 📆 <b>Month ({month})</b>\n"
        f"│ ✅ Sign-ups: <code>{s_month}</code>\n"
        f"│ 📤 Uploads:  <code>{u_month}</code>\n"
        f"│ 💳 Paid:     <code>{p_month}</code>\n"
        f"├────────────────────────\n"
        f"│ 🏆 <b>All Time</b>\n"
        f"│ ✅ Sign-ups: <code>{s_all}</code>\n"
        f"│ 📤 Uploads:  <code>{u_all}</code>\n"
        f"│ 💳 Paid:     <code>{p_all}</code>\n"
        f"├────────────────────────\n"
        f"│ 🔄 <b>Conversion Rates</b>\n"
        f"│ Sign→Upload: <code>{s2u:.1f}%</code> | Sign→Paid: <code>{s2p:.1f}%</code>\n"
        f"└────────────────────────\n"
    )
    if stats.get("total_overrides", 0) > 0:
        section += f"│ ✏️ Active Overrides: <code>{stats['total_overrides']}</code>\n"
    return section


def build_telegram_ga4_section(ga4):
    """Build GA4 section (HTML)."""
    if not ga4["connected"]:
        return "🌐 <b>GA4 ANALYTICS</b> — Not connected\n"
    section = (
        f"\n🌐 <b>GA4 ANALYTICS</b> ✅\n"
        f"┌────────────────────────\n"
        f"│ 👥 Users (30d): <code>{ga4['month_users']}</code>\n"
        f"│ 📊 Sessions (30d): <code>{ga4['month_sessions']}</code>\n"
    )
    if ga4["top_sources"]:
        section += "│ 📈 <b>Top Sources:</b>\n"
        for src, count in ga4["top_sources"][:3]:
            section += f"│   • {escape_html(src)}: <code>{count}</code>\n"
    if ga4["top_countries"]:
        section += "│ 🌍 <b>Top Countries:</b>\n"
        for country, count in ga4["top_countries"][:3]:
            section += f"│   • {escape_html(country)}: <code>{count}</code>\n"
    section += "└────────────────────────\n"
    return section


def build_telegram_youtube_section(yt):
    """Build YouTube section (HTML)."""
    if not yt["connected"]:
        return "📺 <b>YOUTUBE</b> — Not connected\n"
    section = (
        f"\n📺 <b>YOUTUBE</b> ✅\n"
        f"┌────────────────────────\n"
        f"│ 👥 Subscribers: <code>{yt['subscribers']}</code>\n"
        f"│ 👁 Total Views: <code>{yt['total_views']}</code>\n"
        f"│ 🎬 Videos: <code>{yt['video_count']}</code>\n"
    )
    if yt["has_analytics"]:
        section += (
            f"│ ─── 30d Analytics ───\n"
            f"│ 👁 Views: <code>{yt['period_views']}</code>\n"
            f"│ ⏱ Watch: <code>{yt['period_watch_hours']}</code>h\n"
            f"│ 👤 Subs Gained: <code>{yt['period_subs_gained']}</code>\n"
        )
    if yt["top_videos"]:
        section += "│ 🔥 <b>Top Videos:</b>\n"
        for title, views, likes in yt["top_videos"][:3]:
            section += f"│   • {escape_html(title)}: <code>{views}</code> views\n"
    section += "└────────────────────────\n"
    return section


def build_telegram_linkedin_section(li):
    """Build LinkedIn section (HTML)."""
    if not li["connected"]:
        return "💼 <b>LINKEDIN</b> — Not connected\n"
    section = (
        f"\n💼 <b>LINKEDIN</b> ✅\n"
        f"┌────────────────────────\n"
        f"│ 👥 Followers: <code>{li['followers']}</code>\n"
    )
    if li["company_name"]:
        section += f"│ 🏢 {escape_html(li['company_name'])}\n"
    if li["employees"]:
        section += f"│ 👔 Employees: {escape_html(li['employees'])}\n"
    if li["industry"]:
        section += f"│ 🏭 {escape_html(li['industry'])}\n"
    # Add posts summary if available
    try:
        from linkedin_connector import get_posts
        _posts = get_posts()
        if _posts:
            _total_likes = sum(p.get("likes", 0) for p in _posts)
            _total_comments = sum(p.get("comments", 0) for p in _posts)
            section += f"│ 📝 Posts: <code>{len(_posts)}</code> | 👍 <code>{_total_likes}</code> | 💬 <code>{_total_comments}</code>\n"
    except Exception:
        pass
    section += "└────────────────────────\n"
    return section


def build_telegram_stripe_section(stripe):
    """Build Stripe section (HTML)."""
    if not stripe["connected"]:
        return "💳 <b>STRIPE</b> — No data (cookies may be expired)\n"
    section = (
        f"\n💳 <b>STRIPE PAYMENTS</b>\n"
        f"┌────────────────────────\n"
        f"│ 📅 Today: <code>{stripe['today_paid']}</code> paid\n"
        f"│ 📆 Month: <code>{stripe['month_paid']}</code> paid\n"
        f"│ 🏆 All Time: <code>{stripe['total_paid']}</code> paid\n"
    )
    if stripe.get("month_revenue", 0) > 0:
        section += f"│ 💰 Month Revenue: <code>${stripe['month_revenue']:,.2f}</code>\n"
    if stripe.get("total_revenue", 0) > 0:
        section += f"│ 💰 Total Revenue: <code>${stripe['total_revenue']:,.2f}</code>\n"
    section += "└────────────────────────\n"
    return section


def build_telegram_pipeline_section(health):
    """Build pipeline health section (HTML)."""
    section = (
        f"\n⚙️ <b>PIPELINE HEALTH</b>\n"
        f"┌────────────────────────\n"
        f"│ 🕐 Last Run: {escape_html(health['last_run'][:19] if health['last_run'] != 'Never' else 'Never')}\n"
        f"│ ✅ Stages: <code>{health['stages_passed']}/{health['total_stages']}</code>\n"
        f"│ ⏱ Duration: <code>{health['duration']:.0f}s</code>\n"
        f"└────────────────────────\n"
    )
    return section


def build_full_telegram_message(kpi, ga4, yt, li, stripe, health):
    """Build the comprehensive multi-section Telegram message (HTML)."""
    msg = build_telegram_kpi_section(kpi)
    msg += build_telegram_ga4_section(ga4)
    msg += build_telegram_youtube_section(yt)
    msg += build_telegram_linkedin_section(li)
    msg += build_telegram_stripe_section(stripe)
    msg += build_telegram_pipeline_section(health)
    msg += (
        f"\n🔗 <a href=\"https://eagle3d-kpi-automation.streamlit.app/\">Dashboard</a>\n"
        f"<i>Auto-generated at {datetime.utcnow().strftime('%H:%M')} UTC</i>"
    )
    return msg


# ═══════════════════════════════════════════════════════════════
# INDIVIDUAL SUBSYSTEM TELEGRAM MESSAGES (sent separately for detail)
# ═══════════════════════════════════════════════════════════════

def send_subsystem_reports():
    """Send individual detailed reports for each subsystem + performance alerts."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    # Fallback to st.secrets
    if not bot_token or not chat_id:
        try:
            import streamlit as st
            if "TELEGRAM_BOT_TOKEN" in st.secrets:
                val = st.secrets["TELEGRAM_BOT_TOKEN"]
                if val and str(val).strip():
                    bot_token = str(val).strip()
            if "TELEGRAM_CHAT_ID" in st.secrets:
                val = st.secrets["TELEGRAM_CHAT_ID"]
                if val and str(val).strip():
                    chat_id = str(val).strip()
        except Exception:
            pass
    if not bot_token or not chat_id:
        return

    import pandas as pd

    # ── KPI DETAILED (always send) ──
    try:
        kpi = build_kpi_stats()
        kpi_msg = build_telegram_kpi_section(kpi)
        send_telegram(kpi_msg)
    except Exception as e:
        log(f"KPI report error: {e}")

    # ── GA4 DETAILED (always send, even if not connected) ──
    try:
        ga4 = build_ga4_stats()
        ga4_msg = build_telegram_ga4_section(ga4)
        if ga4["connected"]:
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

    # ── YOUTUBE DETAILED (always send) ──
    try:
        yt = build_youtube_stats()
        yt_msg = build_telegram_youtube_section(yt)
        if yt["connected"] and yt["top_videos"]:
            yt_msg += "\n🎬 *Video Performance:*\n"
            for i, (title, views, likes) in enumerate(yt["top_videos"], 1):
                eng = (likes / views * 100) if views > 0 else 0
                yt_msg += f"  {i}\\. {escape_md(title)}\n"
                yt_msg += f"     👁 `{views}` 👍 `{likes}` 📊 `{eng:.1f}%`\n"
        send_telegram(yt_msg)
    except Exception as e:
        log(f"YouTube report error: {e}")

    # ── LINKEDIN DETAILED (always send) ──
    try:
        li = build_linkedin_stats()
        li_msg = build_telegram_linkedin_section(li)
        # Add daily history summary even if not fully connected
        if li["connected"]:
            try:
                from linkedin_connector import get_manual_history, get_posts
                hist = get_manual_history()
                if not hist.empty:
                    li_msg += f"│ 📅 History: `{len(hist)}` days\n"
                posts = get_posts()
                if posts:
                    total_likes = sum(p.get("likes", 0) for p in posts)
                    total_comments = sum(p.get("comments", 0) for p in posts)
                    li_msg += f"│ 📝 Posts: `{len(posts)}` | 👍 `{total_likes}` | 💬 `{total_comments}`\n"
            except Exception:
                pass
        send_telegram(li_msg)
    except Exception as e:
        log(f"LinkedIn report error: {e}")

    # ── STRIPE DETAILED (always send) ──
    try:
        stripe = build_stripe_stats()
        stripe_msg = build_telegram_stripe_section(stripe)
        send_telegram(stripe_msg)
    except Exception as e:
        log(f"Stripe report error: {e}")

    # ── CROSS-PLATFORM (always send) ──
    try:
        cp = build_cross_platform_stats()
        cp_msg = "🔗 *CROSS-PLATFORM CORRELATION*\n┌────────────────────────\n"
        if cp["available"] and cp["top_correlations"]:
            for metric, strength in cp["top_correlations"][:5]:
                name = escape_md(metric.replace("_", " ").title())
                cp_msg += f"│ {name}: `{strength:.2f}`\n"
        else:
            cp_msg += "│ Waiting for multi-platform data\n"
        cp_msg += "└────────────────────────\n"
        send_telegram(cp_msg)
    except Exception as e:
        log(f"Cross-platform report error: {e}")

    # ── PIPELINE HEALTH (always send) ──
    try:
        health = build_pipeline_health()
        pipe_msg = build_telegram_pipeline_section(health)
        send_telegram(pipe_msg)
    except Exception as e:
        log(f"Pipeline report error: {e}")

    # ── PERFORMANCE ALERTS (smart anomaly detection) ──
    try:
        from pathlib import Path as _P
        _kpi_path = _P("data_output") / "daily_counts.json"
        _prev_kpi_path = _P("data_output") / "daily_counts_prev.json"
        kpi_df = None
        prev_kpi_df = None
        if _kpi_path.exists():
            try:
                kpi_df = pd.read_json(_kpi_path.read_text())
            except Exception:
                pass
        if _prev_kpi_path.exists():
            try:
                prev_kpi_df = pd.read_json(_prev_kpi_path.read_text())
            except Exception:
                pass

        if kpi_df is not None and not kpi_df.empty:
            try:
                from telegram_alerts import anomaly_alerts
                alerts = anomaly_alerts(kpi_df, prev_kpi_df)
                for alert in alerts:
                    try:
                        send_telegram(alert["msg"])
                    except Exception:
                        pass
            except Exception as e:
                log(f"Anomaly alerts error: {e}")
    except Exception as e:
        log(f"Performance alerts error: {e}")

    # ── MONTHLY GOALS PROGRESS ──
    try:
        _goals_path = Path("monthly_goals.json")
        if _goals_path.exists():
            _goals = json.loads(_goals_path.read_text())
            if _goals:
                _cur_month = datetime.now().strftime("%Y-%m")
                _cur_goals = _goals.get(_cur_month, {})
                if _cur_goals:
                    _gmsg = f"🎯 <b>MONTHLY GOALS — {_cur_month}</b>\n┌────────────────────────\n"
                    # Get actual KPI data for comparison
                    _kpi = {}
                    try:
                        _kpi = build_kpi_stats()
                    except Exception:
                        pass
                    _s_month = _kpi.get("signups_month_override", _kpi.get("signups_month", 0))
                    _u_month = _kpi.get("uploads_month_override", _kpi.get("uploads_month", 0))
                    _p_month = _kpi.get("paid_month_override", _kpi.get("paid_month", 0))
                    _goal_map = {
                        "SignUps": ("✅ Sign-ups", _s_month),
                        "FirstUploads": ("📤 Uploads", _u_month),
                        "Paid": ("💳 Paid", _p_month),
                    }
                    for _gk, _gv in _cur_goals.items():
                        if _gv > 0:
                            _label, _actual = _goal_map.get(_gk, (_gk, 0))
                            _pct = (_actual / _gv * 100) if _gv > 0 else 0
                            _icon = "🟢" if _pct >= 100 else ("🟡" if _pct >= 50 else "🔴")
                            _gmsg += f"│ {_icon} {escape_html(_label)}: <code>{_actual}</code>/<code>{_gv}</code> ({_pct:.0f}%)\n"
                    _gmsg += "└────────────────────────\n"
                    send_telegram(_gmsg)
    except Exception as e:
        log(f"Monthly goals report error: {e}")


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
