"""
telegram_alerts.py — Smart Telegram Alert System for Eagle3D KPI
================================================================
Sends rich, formatted Telegram messages:
  - Daily KPI report
  - Weekly / Biweekly / Monthly report
  - Anomaly alerts (drops, spikes, patterns)
  - Marketing team performance
  - Pipeline health
  - Traffic intelligence alerts
"""

import os
import json
import re
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path


def _esc(t):
    """Escape Telegram MarkdownV2 special chars."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(t))


def _send(message: str, parse_mode="MarkdownV2") -> dict:
    """Send to Telegram. Checks st.secrets first, then env vars."""
    bot_token = ""
    chat_id = ""
    # Try Streamlit secrets first (for cloud dashboard)
    try:
        import streamlit as st
        bot_token = st.secrets.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = st.secrets.get("TELEGRAM_CHAT_ID", "").strip()
    except Exception:
        pass
    # Fallback to env vars (for GitHub Actions / local)
    if not bot_token:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not chat_id:
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token or not chat_id:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in secrets"}

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id,
            "text": message,
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
                return {"ok": True, "error": None}
            return {"ok": False, "error": str(body)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════
# DAILY KPI REPORT
# ══════════════════════════════════════════════════════════════

def daily_kpi_report(kpi_df, prev_kpi_df=None, utm_df=None, leads_df=None) -> str:
    """Generate rich daily KPI Telegram message."""
    import pandas as pd
    now = datetime.now()

    # Current period metrics
    signups = int(pd.to_numeric(kpi_df.get("signups", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0
    uploads = int(pd.to_numeric(kpi_df.get("first_uploads", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0
    paid = int(pd.to_numeric(kpi_df.get("paid_customers", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0

    # Previous period
    prev_s = prev_u = prev_p = 0
    if prev_kpi_df is not None and not prev_kpi_df.empty:
        prev_s = int(pd.to_numeric(prev_kpi_df.get("signups", 0), errors="coerce").fillna(0).sum())
        prev_u = int(pd.to_numeric(prev_kpi_df.get("first_uploads", 0), errors="coerce").fillna(0).sum())
        prev_p = int(pd.to_numeric(prev_kpi_df.get("paid_customers", 0), errors="coerce").fillna(0).sum())

    def chg(cur, prev):
        if prev == 0: return "N/A"
        return f"{((cur-prev)/prev*100):+.1f}%"

    # Funnel rates
    s2u = (uploads / signups * 100) if signups > 0 else 0
    u2p = (paid / uploads * 100) if uploads > 0 else 0

    # Traffic
    total_sess = 0
    top_src = "N/A"
    if utm_df is not None and not utm_df.empty and "sessions" in utm_df.columns:
        total_sess = int(pd.to_numeric(utm_df["sessions"], errors="coerce").sum())
        src_col = "source_normalized" if "source_normalized" in utm_df.columns else "sessionSource"
        if src_col in utm_df.columns:
            top_sources = utm_df.groupby(src_col)["sessions"].sum().sort_values(ascending=False)
            if len(top_sources) > 0:
                top_src = f"{top_sources.index[0]} ({int(top_sources.iloc[0])})"

    # Health score
    if signups > 0 and s2u > 20 and u2p > 5:
        health = "🟢 EXCELLENT"
    elif signups > 0 and s2u > 10:
        health = "🟡 GOOD"
    elif signups > 0:
        health = "🟠 NEEDS ATTENTION"
    else:
        health = "🔴 NO DATA"

    # Today's data (last row)
    today_s = today_u = today_p = 0
    today_date = now.strftime("%Y-%m-%d")
    if kpi_df is not None and not kpi_df.empty and "date" in kpi_df.columns:
        today_row = kpi_df[kpi_df["date"].astype(str) == today_date]
        if today_row.empty:
            today_row = kpi_df.head(1)
        if not today_row.empty:
            today_s = int(pd.to_numeric(today_row["signups"], errors="coerce").fillna(0).sum())
            today_u = int(pd.to_numeric(today_row["first_uploads"], errors="coerce").fillna(0).sum())
            today_p = int(pd.to_numeric(today_row["paid_customers"], errors="coerce").fillna(0).sum())

    msg = (
        f"🦅 *EAGLE3D DAILY KPI REPORT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📅 {_esc(now.strftime('%A, %B %d, %Y'))}\n\n"
        f"📊 *TODAY'S NUMBERS*\n"
        f"├ Sign\\-ups: `{today_s}`\n"
        f"├ Uploads: `{today_u}`\n"
        f"└ Paid: `{today_p}`\n\n"
        f"📈 *PERIOD TOTAL*\n"
        f"├ 👥 Sign\\-ups: `{signups}` {_esc(chg(signups, prev_s))}\n"
        f"├ 📤 Uploads: `{uploads}` {_esc(chg(uploads, prev_u))}\n"
        f"└ 💳 Paid: `{paid}` {_esc(chg(paid, prev_p))}\n\n"
        f"🔄 *FUNNEL*\n"
        f"├ S→U Rate: `{s2u:.1f}%`\n"
        f"└ U→P Rate: `{u2p:.1f}%`\n\n"
        f"🌐 *TRAFFIC*: `{total_sess:,}` sessions\n"
        f"🏆 *Top Source*: {_esc(top_src)}\n\n"
        f"🏥 Health: {health}\n\n"
        f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
    )
    return msg


# ══════════════════════════════════════════════════════════════
# WEEKLY / BIWEEKLY / MONTHLY REPORT
# ══════════════════════════════════════════════════════════════

def period_report(kpi_df, prev_kpi_df=None, utm_df=None, leads_df=None, pages_df=None, period_type="weekly") -> str:
    """Generate rich period report."""
    import pandas as pd
    now = datetime.now()

    signups = int(pd.to_numeric(kpi_df.get("signups", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0
    uploads = int(pd.to_numeric(kpi_df.get("first_uploads", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0
    paid = int(pd.to_numeric(kpi_df.get("paid_customers", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0

    prev_s = prev_u = prev_p = 0
    if prev_kpi_df is not None and not prev_kpi_df.empty:
        prev_s = int(pd.to_numeric(prev_kpi_df.get("signups", 0), errors="coerce").fillna(0).sum())
        prev_u = int(pd.to_numeric(prev_kpi_df.get("first_uploads", 0), errors="coerce").fillna(0).sum())
        prev_p = int(pd.to_numeric(prev_kpi_df.get("paid_customers", 0), errors="coerce").fillna(0).sum())

    def chg(cur, prev):
        if prev == 0: return "N/A"
        c = (cur - prev) / prev * 100
        icon = "🟢" if c > 0 else "🔴" if c < -10 else "🟡"
        return f"{icon} {c:+.1f}%"

    # Best day
    best_day = "N/A"
    best_day_s = 0
    if kpi_df is not None and not kpi_df.empty and "date" in kpi_df.columns and "signups" in kpi_df.columns:
        daily = kpi_df.groupby("date")["signups"].sum()
        if len(daily) > 0:
            best_day = str(daily.idxmax())
            best_day_s = int(daily.max())

    # Top sources
    sources_text = "N/A"
    if utm_df is not None and not utm_df.empty and "sessions" in utm_df.columns:
        src_col = "source_normalized" if "source_normalized" in utm_df.columns else "sessionSource"
        if src_col in utm_df.columns:
            by_src = utm_df.groupby(src_col)["sessions"].sum().sort_values(ascending=False).head(5)
            lines = []
            total = by_src.sum()
            for src, sess in by_src.items():
                pct = sess / total * 100 if total > 0 else 0
                lines.append(f"  ├ {_esc(str(src))}: `{int(sess)}` \\({_esc(f'{pct:.0f}%')}\\)")
            sources_text = "\n".join(lines)

    # Lead sources
    leads_text = ""
    if leads_df is not None and not leads_df.empty:
        if "Lead Source" in leads_df.columns and "Signups" in leads_df.columns:
            top_leads = leads_df.sort_values("Signups", ascending=False).head(5)
            leads_text = "\n🎯 *TOP LEAD SOURCES*\n"
            for _, row in top_leads.iterrows():
                leads_text += f"  ├ {_esc(str(row['Lead Source']))}: `{int(row['Signups'])}`\n"

    period_labels = {"weekly": "WEEKLY", "biweekly": "BIWEEKLY", "monthly": "MONTHLY", "quarterly": "QUARTERLY"}

    msg = (
        f"📊 *EAGLE3D {period_labels.get(period_type, 'PERIOD').upper()} REPORT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📅 {_esc(now.strftime('%B %d, %Y'))}\n\n"
        f"📈 *KEY METRICS*\n"
        f"├ 👥 Sign\\-ups: `{signups:,}` {chg(signups, prev_s)}\n"
        f"├ 📤 Uploads: `{uploads:,}` {chg(uploads, prev_u)}\n"
        f"└ 💳 Paid: `{paid:,}` {chg(paid, prev_p)}\n\n"
        f"🏆 *BEST DAY*: {_esc(best_day)} \\(`{best_day_s}` signups\\)\n\n"
        f"🌐 *TOP TRAFFIC SOURCES*\n{sources_text}\n"
        f"{leads_text}\n"
        f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
    )
    return msg


# ══════════════════════════════════════════════════════════════
# ANOMALY ALERTS
# ══════════════════════════════════════════════════════════════

def anomaly_alerts(kpi_df, prev_kpi_df=None) -> list:
    """Generate list of anomaly alert messages."""
    import pandas as pd
    alerts = []

    if kpi_df is None or kpi_df.empty:
        return [{"type": "info", "msg": "⚠️ No KPI data available for anomaly detection"}]

    now = datetime.now()

    # 1. SIGNIFICANT DROPS/SPIKES
    if prev_kpi_df is not None and not prev_kpi_df.empty:
        for metric, label, icon in [
            ("signups", "Sign-ups", "👥"),
            ("first_uploads", "Uploads", "📤"),
            ("paid_customers", "Paid", "💳"),
        ]:
            if metric in kpi_df.columns and metric in prev_kpi_df.columns:
                cur = int(pd.to_numeric(kpi_df[metric], errors="coerce").fillna(0).sum())
                prev = int(pd.to_numeric(prev_kpi_df[metric], errors="coerce").fillna(0).sum())
                if prev > 0:
                    change = (cur - prev) / prev * 100
                    if change <= -30:
                        alerts.append({
                            "type": "critical",
                            "msg": (
                                f"🚨 *{label} DROP ALERT*\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                                f"{icon} {label} dropped by `{abs(change):.1f}%`\n\n"
                                f"📋 Details\n"
                                f"• Previous: `{prev}`\n"
                                f"• Current: `{cur}`\n"
                                f"• Change: `{change:+.1f}%`\n\n"
                                f"💡 *Action*: Investigate marketing changes, check sign\\-up flow, review ad campaigns\n\n"
                                f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
                            ),
                        })
                    elif change >= 30:
                        alerts.append({
                            "type": "positive",
                            "msg": (
                                f"📈 *{label} SPIKE*\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                                f"{icon} {label} surged by `{change:.1f}%`\n\n"
                                f"📋 Details\n"
                                f"• Previous: `{prev}`\n"
                                f"• Current: `{cur}`\n"
                                f"• Change: `{change:+.1f}%`\n\n"
                                f"💡 *Action*: Identify what's working and double down\\!\n\n"
                                f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
                            ),
                        })

    # 2. ZERO-DAY ALERT
    if "signups" in kpi_df.columns and "date" in kpi_df.columns:
        daily = kpi_df.groupby("date")["signups"].sum()
        zero_days = daily[daily == 0]
        if len(zero_days) > 0 and len(daily) > 3:
            last_zero = str(zero_days.index[-1])
            alerts.append({
                "type": "warning",
                "msg": (
                    f"⚠️ *ZERO SIGNUP DAYS*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"`{len(zero_days)}` days with zero signups found\n\n"
                    f"📋 Details\n"
                    f"• Zero days: `{len(zero_days)}`\n"
                    f"• Last zero: {_esc(last_zero)}\n\n"
                    f"💡 Check if pipeline ran correctly on these days\n\n"
                    f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
                ),
            })

    # 3. FUNNEL HEALTH
    signups = int(pd.to_numeric(kpi_df.get("signups", 0), errors="coerce").fillna(0).sum()) if not kpi_df.empty else 0
    uploads = int(pd.to_numeric(kpi_df.get("first_uploads", 0), errors="coerce").fillna(0).sum()) if not kpi_df.empty else 0
    paid = int(pd.to_numeric(kpi_df.get("paid_customers", 0), errors="coerce").fillna(0).sum()) if not kpi_df.empty else 0

    s2u = (uploads / signups * 100) if signups > 0 else 0
    u2p = (paid / uploads * 100) if uploads > 0 else 0

    if signups > 10:
        if s2u < 15:
            alerts.append({
                "type": "warning",
                "msg": (
                    f"⚠️ *LOW UPLOAD RATE*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Sign\\-up → Upload rate is only `{s2u:.1f}%`\n\n"
                    f"📋 Details\n"
                    f"• Sign\\-ups: `{signups}`\n"
                    f"• Uploads: `{uploads}`\n"
                    f"• Rate: `{s2u:.1f}%`\n\n"
                    f"💡 Improve onboarding, add tutorials, simplify upload flow\n\n"
                    f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
                ),
            })
        if u2p < 5:
            alerts.append({
                "type": "warning",
                "msg": (
                    f"⚠️ *LOW CONVERSION TO PAID*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Upload → Paid rate is only `{u2p:.1f}%`\n\n"
                    f"📋 Details\n"
                    f"• Uploads: `{uploads}`\n"
                    f"• Paid: `{paid}`\n"
                    f"• Rate: `{u2p:.1f}%`\n\n"
                    f"💡 Review pricing, add upgrade prompts, improve value prop\n\n"
                    f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
                ),
            })

    # 4. DAILY ANOMALY (Z-SCORE)
    if "signups" in kpi_df.columns and len(kpi_df) >= 5:
        vals = pd.to_numeric(kpi_df["signups"], errors="coerce").dropna()
        if len(vals) >= 5:
            mean = vals.mean()
            std = vals.std()
            if std > 0:
                last_val = float(vals.iloc[0])
                z = (last_val - mean) / std
                if abs(z) >= 2.0:
                    direction = "SPIKE" if z > 0 else "DROP"
                    icon = "📈" if z > 0 else "🚨"
                    alerts.append({
                        "type": "critical" if abs(z) >= 3 else "warning",
                        "msg": (
                            f"{icon} *DAILY {direction} ANOMALY*\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"Sign\\-ups deviate `{abs(z):.1f}` standard deviations from mean\n\n"
                            f"📋 Details\n"
                            f"• Last value: `{int(last_val)}`\n"
                            f"• Daily average: `{mean:.1f}`\n"
                            f"• Z\\-score: `{z:.1f}`\n\n"
                            f"💡 {'Capitalize on this spike — what drove it?' if z > 0 else 'Investigate the drop — is it a data issue or real?'}\n\n"
                            f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
                        ),
                    })

    if not alerts:
        alerts.append({
            "type": "positive",
            "msg": (
                f"✅ *ALL SYSTEMS NORMAL*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"No anomalies detected\n"
                f"Sign\\-ups: `{signups}` | Uploads: `{uploads}` | Paid: `{paid}`\n\n"
                f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
            ),
        })

    return alerts


# ══════════════════════════════════════════════════════════════
# MARKETING TEAM PERFORMANCE
# ══════════════════════════════════════════════════════════════

def marketing_performance(kpi_df, prev_kpi_df=None, utm_df=None, leads_df=None) -> str:
    """Marketing team daily scorecard."""
    import pandas as pd
    now = datetime.now()

    signups = int(pd.to_numeric(kpi_df.get("signups", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0
    uploads = int(pd.to_numeric(kpi_df.get("first_uploads", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0
    paid = int(pd.to_numeric(kpi_df.get("paid_customers", 0), errors="coerce").fillna(0).sum()) if kpi_df is not None and not kpi_df.empty else 0

    # Grade
    if signups >= 20:
        grade = "🥇 A+"
    elif signups >= 10:
        grade = "🥈 A"
    elif signups >= 5:
        grade = "🥉 B"
    elif signups >= 1:
        grade = "📋 C"
    else:
        grade = "🔴 F"

    s2u = (uploads / signups * 100) if signups > 0 else 0
    u2p = (paid / uploads * 100) if uploads > 0 else 0

    # Top sources
    sources_text = ""
    if utm_df is not None and not utm_df.empty and "sessions" in utm_df.columns:
        src_col = "source_normalized" if "source_normalized" in utm_df.columns else "sessionSource"
        if src_col in utm_df.columns:
            by_src = utm_df.groupby(src_col)["sessions"].sum().sort_values(ascending=False).head(5)
            total = by_src.sum()
            for src, sess in by_src.items():
                pct = sess / total * 100 if total > 0 else 0
                sources_text += f"  ├ {_esc(str(src))}: `{int(sess)}` \\({_esc(f'{pct:.0f}%')}\\)\n"

    # Trend
    prev_s = 0
    if prev_kpi_df is not None and not prev_kpi_df.empty:
        prev_s = int(pd.to_numeric(prev_kpi_df.get("signups", 0), errors="coerce").fillna(0).sum())
    trend = "📈 UP" if signups > prev_s else "📉 DOWN" if signups < prev_s else "➡️ FLAT"

    msg = (
        f"📊 *MARKETING TEAM SCORECARD*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📅 {_esc(now.strftime('%A, %B %d'))}\n\n"
        f"🏆 *GRADE*: {grade}\n\n"
        f"📊 *METRICS*\n"
        f"├ 👥 Sign\\-ups: `{signups}`\n"
        f"├ 📤 Uploads: `{uploads}`\n"
        f"├ 💳 Paid: `{paid}`\n"
        f"├ 🔄 S→U: `{s2u:.1f}%`\n"
        f"└ 💰 U→P: `{u2p:.1f}%`\n\n"
        f"📈 *Trend*: {trend}\n\n"
        f"🌐 *Traffic Sources*\n{sources_text}\n"
        f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Marketing Team"
    )
    return msg


# ══════════════════════════════════════════════════════════════
# PIPELINE HEALTH ALERT
# ══════════════════════════════════════════════════════════════

def pipeline_health_alert(success=True, stages=None, error_msg="") -> str:
    """Pipeline run notification."""
    now = datetime.now()

    if success:
        stage_text = ""
        if stages:
            for s in stages:
                stage_text += f"  ├ ✅ {_esc(str(s))}\n"
        msg = (
            f"✅ *PIPELINE COMPLETED*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📋 All stages passed\n"
            f"{stage_text}\n"
            f"⏰ {_esc(now.strftime('%I:%M %p'))} | 🔄 Pipeline"
        )
    else:
        msg = (
            f"🚨 *PIPELINE FAILED*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📋 Error: {_esc(error_msg[:200])}\n\n"
            f"💡 Check GitHub Actions for details\n\n"
            f"⏰ {_esc(now.strftime('%I:%M %p'))} | 🔄 Pipeline"
        )
    return msg


# ══════════════════════════════════════════════════════════════
# TOP PERFORMER ALERT
# ══════════════════════════════════════════════════════════════

def top_performer_alert(kpi_df, utm_df=None) -> str:
    """Best performing day/source."""
    import pandas as pd
    now = datetime.now()

    best_date = "N/A"
    best_signups = 0
    if kpi_df is not None and not kpi_df.empty and "date" in kpi_df.columns and "signups" in kpi_df.columns:
        daily = kpi_df.groupby("date")["signups"].sum()
        if len(daily) > 0:
            best_date = str(daily.idxmax())
            best_signups = int(daily.max())

    msg = (
        f"✅ 🥇 *TOP PERFORMER*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Best performing day by sign\\-ups\n\n"
        f"📋 Details\n"
        f"• Date: {_esc(best_date)}\n"
        f"• Sign\\-ups: `{best_signups}`\n\n"
        f"💡 What was different on this day? Check campaigns and content\\.\n\n"
        f"⏰ {_esc(now.strftime('%I:%M %p'))} | 👥 Team"
    )
    return msg


# ══════════════════════════════════════════════════════════════
# SEND HELPER — used by app.py buttons
# ══════════════════════════════════════════════════════════════

def send(msg: str) -> dict:
    """Convenience wrapper."""
    return _send(msg)
