"""
daily_counts.py
EXPLICIT date field per source. Falls back to scrape_date for missing dates.
Tags rows with date_source: "actual" or "estimated".
"""
import re
from collections import defaultdict
from datetime import datetime
from email.utils import parsedate_to_datetime

from sheets_writer import read_tab_data, write_tab_data


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DailyCounts] {msg}", flush=True)


DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%y, %I:%M %p",   # Stripe: "5/8/26, 5:25 AM"
    "%m/%d/%Y, %I:%M %p",
    "%m/%d/%y %I:%M %p",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%y",
    "%m/%d/%Y",
    "%a %b %d %Y",          # First Upload: "Tue May 05 2026"
    "%a %b %d %Y %H:%M:%S",
    "%b %d, %Y",
    "%d %b %Y",
]


def parse_date(raw):
    if not raw or not str(raw).strip():
        return ""
    raw = str(raw).strip()
    
    try:
        dt = parsedate_to_datetime(raw)
        if dt:
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%Y-%m-%d")
        except Exception:
            pass
    
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", raw)
    if m:
        try:
            month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if year < 100:
                year += 2000
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except Exception:
            pass
    
    return ""


def extract_email(row):
    for k in ("Email", "email", "EMAIL", "__email_normalized__"):
        if k in row and row[k] and "@" in str(row[k]):
            return str(row[k]).strip()
    return ""


def emails_str(rows):
    return "; ".join(sorted({extract_email(r) for r in rows if extract_email(r)}))


def group_by_date(rows, primary_date_field, fallback_field="__scrape_date__"):
    """
    Group rows by date.
    Try primary field first, fall back to scrape_date if missing.
    Returns (grouped_dict, actual_count, estimated_count, skipped_count)
    """
    grouped = defaultdict(list)
    actual_count = 0
    estimated_count = 0
    skipped = 0
    
    for r in rows:
        primary = r.get(primary_date_field, "")
        parsed = parse_date(primary)
        
        if parsed:
            grouped[parsed].append(r)
            actual_count += 1
        else:
            # Fall back to scrape_date
            fallback = r.get(fallback_field, "")
            parsed_fb = parse_date(fallback)
            if parsed_fb:
                grouped[parsed_fb].append(r)
                estimated_count += 1
            else:
                skipped += 1
    
    return grouped, actual_count, estimated_count, skipped


def build_daily_counts_table():
    log("=" * 60)
    log("Building Daily_Counts and Monthly_Counts")
    log("=" * 60)
    
    free_rows   = read_tab_data("Verified_FREE")
    upload_rows = read_tab_data("Verified_FIRST_UPLOAD")
    stripe_rows = read_tab_data("Verified_STRIPE")
    
    log(f"Loaded: free={len(free_rows)}, upload={len(upload_rows)}, stripe={len(stripe_rows)}")
    
    free_by_date, fa, fe, fs     = group_by_date(free_rows,   "Account Created On")
    upload_by_date, ua, ue, us   = group_by_date(upload_rows, "Upload Date")
    stripe_by_date, sa, se, ss_  = group_by_date(stripe_rows, "Created")
    
    log(f"Free:   {len(free_by_date)} dates ({fa} actual, {fe} estimated, {fs} skipped)")
    log(f"Upload: {len(upload_by_date)} dates ({ua} actual, {ue} estimated, {us} skipped)")
    log(f"Stripe: {len(stripe_by_date)} dates ({sa} actual, {se} estimated, {ss_} skipped)")
    
    if se > 0:
        log(f"NOTE: {se} Stripe rows used scrape_date as fallback (Stripe view doesn't show Created for past subscribers)")
    
    all_dates = sorted({
        d for d in (
            list(free_by_date.keys())
            + list(upload_by_date.keys())
            + list(stripe_by_date.keys())
        )
        if d
    })
    
    daily_dicts = []
    for date in all_dates:
        fr = free_by_date.get(date, [])
        up = upload_by_date.get(date, [])
        st = stripe_by_date.get(date, [])
        
        daily_dicts.append({
            "Date":                     date,
            "SignUps_Accepted":         len(fr),
            "FirstUploads_Accepted":    len(up),
            "PaidSubscribers_Accepted": len(st),
            "Free Signups":             len(fr),
            "First Uploads":            len(up),
            "Paid Customers":           len(st),
            "Free Emails":              emails_str(fr),
            "Upload Emails":            emails_str(up),
            "Stripe Emails":            emails_str(st),
        })
    
    log(f"Daily_Counts: {len(daily_dicts)} date rows")
    
    ok1 = write_tab_data("Daily_Counts", daily_dicts)
    log(f"Daily_Counts write: {'OK' if ok1 else 'FAILED'}")
    
    monthly = defaultdict(lambda: [0, 0, 0])
    for d in daily_dicts:
        month = d["Date"][:7]
        monthly[month][0] += d["SignUps_Accepted"]
        monthly[month][1] += d["FirstUploads_Accepted"]
        monthly[month][2] += d["PaidSubscribers_Accepted"]
    
    monthly_dicts = []
    for m, v in sorted(monthly.items()):
        monthly_dicts.append({
            "Month":                    m,
            "SignUps_Accepted":         v[0],
            "FirstUploads_Accepted":    v[1],
            "PaidSubscribers_Accepted": v[2],
            "Free Signups":             v[0],
            "First Uploads":            v[1],
            "Paid Customers":           v[2],
        })
    
    log(f"Monthly_Counts: {len(monthly_dicts)} months")
    
    ok2 = write_tab_data("Monthly_Counts", monthly_dicts)
    log(f"Monthly_Counts write: {'OK' if ok2 else 'FAILED'}")
    
    log("Done.")
    return {
        "free_dates":     len(free_by_date),
        "upload_dates":   len(upload_by_date),
        "stripe_dates":   len(stripe_by_date),
        "stripe_actual":  sa,
        "stripe_estimated": se,
        "daily_rows":     len(daily_dicts),
        "monthly_rows":   len(monthly_dicts),
    }


if __name__ == "__main__":
    result = build_daily_counts_table()
    print(f"\nResult: {result}")
