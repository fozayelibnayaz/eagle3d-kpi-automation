"""
daily_counts.py
SOURCE:  Google Sheets Verified_FREE, Verified_FIRST_UPLOAD, Verified_STRIPE
OUTPUT:  Google Sheets Daily_Counts, Monthly_Counts
FALLBACK: CSV only if Sheets fails
"""
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from sheets_writer import read_tab_data, write_tab_data

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DailyCounts] {msg}", flush=True)


def extract_email(row: dict) -> str:
    for k in ("Email","email","EMAIL","__email_normalized__"):
        if k in row and row[k] and "@" in str(row[k]):
            return str(row[k]).strip()
    return ""


def extract_date(row: dict) -> str:
    DATE_HINTS = [
        "account created","created on","created","signup date",
        "registered","date","joined",
    ]
    for hint in DATE_HINTS:
        for k, v in row.items():
            if hint in k.lower() and v and str(v).strip():
                return str(v).strip()[:10]
    return "unknown"


def group_by_date(rows: list) -> dict:
    grouped = defaultdict(list)
    for row in rows:
        grouped[extract_date(row)].append(row)
    return grouped


def build_daily_counts_table():
    log("=" * 60)
    log("Building Daily_Counts and Monthly_Counts")
    log("=" * 60)

    # Read Verified_* from Sheets (primary) or CSV fallback
    free_rows    = read_tab_data("Verified_FREE")
    upload_rows  = read_tab_data("Verified_FIRST_UPLOAD")
    stripe_rows  = read_tab_data("Verified_STRIPE")

    log(
        f"Loaded: free={len(free_rows)}, "
        f"upload={len(upload_rows)}, stripe={len(stripe_rows)}"
    )

    free_by_date   = group_by_date(free_rows)
    upload_by_date = group_by_date(upload_rows)
    stripe_by_date = group_by_date(stripe_rows)

    log(
        f"Free dates: {len(free_by_date)} | "
        f"Upload dates: {len(upload_by_date)} | "
        f"Stripe dates: {len(stripe_by_date)}"
    )

    # All dates combined
    all_dates = sorted(
        set(list(free_by_date) + list(upload_by_date) + list(stripe_by_date))
    )

    def emails_str(rows: list) -> str:
        emails = set()
        for r in rows:
            e = extract_email(r)
            if e:
                emails.add(e)
        return "; ".join(sorted(emails))

    # ── Daily_Counts ──
    daily_headers = [
        "Date","Free Signups","First Uploads","Paid Customers",
        "Free Emails","Upload Emails","Stripe Emails",
    ]
    daily_rows = []
    for date in all_dates:
        if date == "unknown":
            continue
        fr = free_by_date.get(date, [])
        up = upload_by_date.get(date, [])
        st = stripe_by_date.get(date, [])
        daily_rows.append([
            date, len(fr), len(up), len(st),
            emails_str(fr), emails_str(up), emails_str(st),
        ])

    log(f"Daily_Counts: {len(daily_rows)} date rows")

    # Convert to list of dicts for write_tab_data
    daily_dicts = [
        dict(zip(daily_headers, row)) for row in daily_rows
    ]
    ok_daily = write_tab_data("Daily_Counts", daily_dicts)
    log(f"Daily_Counts: Sheets={'OK' if ok_daily else 'FAILED->CSV'}")

    # ── Monthly_Counts ──
    monthly = defaultdict(lambda: [0, 0, 0])
    for row in daily_rows:
        month = str(row[0])[:7]  # YYYY-MM
        monthly[month][0] += row[1]
        monthly[month][1] += row[2]
        monthly[month][2] += row[3]

    monthly_headers = [
        "Month","Free Signups","First Uploads","Paid Customers"
    ]
    monthly_dicts = [
        {"Month": m, "Free Signups": v[0],
         "First Uploads": v[1], "Paid Customers": v[2]}
        for m, v in sorted(monthly.items())
    ]
    log(f"Monthly_Counts: {len(monthly_dicts)} month rows")

    ok_monthly = write_tab_data("Monthly_Counts", monthly_dicts)
    log(f"Monthly_Counts: Sheets={'OK' if ok_monthly else 'FAILED->CSV'}")

    log("Done.")
    return {
        "daily_rows":   len(daily_rows),
        "monthly_rows": len(monthly_dicts),
        "sheets_ok":    ok_daily and ok_monthly,
    }


if __name__ == "__main__":
    build_daily_counts_table()
