"""
reporting_engine.py — v3
LAYER 6 — REPORTING + NOTIFICATIONS
Sends Telegram (group) + Email + Slack with daily KPI summary.
"""
import os
import json
import re
import urllib.request
from datetime import datetime
from pathlib import Path

DATA_DIR = Path("data_output")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Report] {msg}", flush=True)


def escape_md(text):
    """Escape special chars for Telegram MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(text))


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


def build_daily_stats():
    """Pull today + month + all-time stats from Daily_Counts sheet."""
    from sheets_writer import read_tab_data

    today_str     = datetime.now().strftime("%Y-%m-%d")
    cur_month_str = datetime.now().strftime("%Y-%m")

    rows = read_tab_data("Daily_Counts")

    today_row  = next((r for r in rows if r.get("Date") == today_str), {})
    month_rows = [r for r in rows if str(r.get("Date", "")).startswith(cur_month_str)]

    def si(v):
        try:
            return int(float(v or 0))
        except Exception:
            return 0

    return {
        "signups_today":  si(today_row.get("SignUps_Accepted", 0)),
        "uploads_today":  si(today_row.get("FirstUploads_Accepted", 0)),
        "paid_today":     si(today_row.get("PaidSubscribers_Accepted", 0)),
        "signups_month":  sum(si(r.get("SignUps_Accepted", 0))         for r in month_rows),
        "uploads_month":  sum(si(r.get("FirstUploads_Accepted", 0))    for r in month_rows),
        "paid_month":     sum(si(r.get("PaidSubscribers_Accepted", 0)) for r in month_rows),
        "signups_all":    sum(si(r.get("SignUps_Accepted", 0))         for r in rows),
        "uploads_all":    sum(si(r.get("FirstUploads_Accepted", 0))    for r in rows),
        "paid_all":       sum(si(r.get("PaidSubscribers_Accepted", 0)) for r in rows),
        "today_str":      today_str,
        "month_str":      cur_month_str,
    }


def build_data_counts():
    """Row counts from all Verified tabs."""
    try:
        from sheets_writer import read_tab_data
        lines = []
        for tab in ["Verified_FREE", "Verified_FIRST_UPLOAD", "Verified_STRIPE"]:
            try:
                rows = read_tab_data(tab)
                acc  = sum(1 for r in rows if str(r.get("final_status","")).upper() == "ACCEPTED")
                lines.append(f"  {tab}: {len(rows)} total / {acc} accepted")
            except Exception:
                lines.append(f"  {tab}: ERROR")
        return "\n".join(lines)
    except Exception:
        return "  (counts unavailable)"


def build_telegram_message(stats):
    """Build MarkdownV2-safe Telegram message."""
    today = escape_md(stats["today_str"])
    month = escape_md(stats["month_str"])

    return (
        f"🦅 *Eagle3D KPI — {today}*\n"
        f"\n"
        f"📅 *Today*\n"
        f"✅ Sign\\-ups:      `{stats['signups_today']}`\n"
        f"📤 First Uploads: `{stats['uploads_today']}`\n"
        f"💳 Paid:          `{stats['paid_today']}`\n"
        f"\n"
        f"📆 *This Month \\({month}\\)*\n"
        f"✅ Sign\\-ups:      `{stats['signups_month']}`\n"
        f"📤 First Uploads: `{stats['uploads_month']}`\n"
        f"💳 Paid:          `{stats['paid_month']}`\n"
        f"\n"
        f"🏆 *All Time*\n"
        f"✅ Sign\\-ups:      `{stats['signups_all']}`\n"
        f"📤 First Uploads: `{stats['uploads_all']}`\n"
        f"💳 Paid:          `{stats['paid_all']}`\n"
        f"\n"
        f"🔗 [View Dashboard](https://eagle3d\\-kpi\\-automation\\.streamlit\\.app/)\n"
        f"_Auto\\-generated at 04:00 UTC_"
    )


def build_text_report(stats):
    today = stats["today_str"]
    month = stats["month_str"]

    return "\n".join([
        "Eagle3D KPI Daily Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 50,
        "",
        f"TODAY ({today}):",
        f"  Sign-ups:      {stats['signups_today']}",
        f"  First Uploads: {stats['uploads_today']}",
        f"  Paid:          {stats['paid_today']}",
        "",
        f"THIS MONTH ({month}):",
        f"  Sign-ups:      {stats['signups_month']}",
        f"  First Uploads: {stats['uploads_month']}",
        f"  Paid:          {stats['paid_month']}",
        "",
        "ALL TIME:",
        f"  Sign-ups:      {stats['signups_all']}",
        f"  First Uploads: {stats['uploads_all']}",
        f"  Paid:          {stats['paid_all']}",
        "",
        "DATA COUNTS:",
        build_data_counts(),
        "",
        "Dashboard: https://eagle3d-kpi-automation.streamlit.app/",
    ])


def main():
    log("=" * 60)
    log("REPORTING ENGINE v3")
    log("=" * 60)

    try:
        stats = build_daily_stats()
    except Exception as e:
        log(f"Stats build failed: {e}")
        stats = {
            "signups_today": 0, "uploads_today": 0, "paid_today": 0,
            "signups_month": 0, "uploads_month": 0, "paid_month": 0,
            "signups_all":   0, "uploads_all":   0, "paid_all":   0,
            "today_str": datetime.now().strftime("%Y-%m-%d"),
            "month_str": datetime.now().strftime("%Y-%m"),
        }

    text = build_text_report(stats)
    log("\n" + text)

    # Save report
    report_file = DATA_DIR / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report_file.write_text(text)
    log(f"Saved: {report_file}")

    # Send all channels
    tg_sent = send_telegram(build_telegram_message(stats))
    em_sent = send_email(f"Eagle3D KPI Report — {stats['today_str']}", text)
    sl_sent = send_slack(text)

    log(f"\nNotification results:")
    log(f"  Telegram: {'✅ sent' if tg_sent else '❌ not sent'}")
    log(f"  Email:    {'✅ sent' if em_sent else '❌ not sent'}")
    log(f"  Slack:    {'✅ sent' if sl_sent else '❌ not sent'}")


if __name__ == "__main__":
    main()
