"""
DAILY COUNTS ANALYTICS
Reads Verified_* tabs and computes TRUE daily sign-up counts
based on Account Created On dates (not pipeline run dates).
Writes results to Daily_Counts tab.
"""
import gspread
import pandas as pd
from datetime import datetime
from dateutil import parser as dateparser
from google.oauth2.service_account import Credentials

from config import GOOGLE_CREDS_FILE, MASTER_SHEET_URL

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DailyCounts] {msg}", flush=True)


def parse_date_safe(val):
    """Parse any date format. Returns date or None."""
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        dt = dateparser.parse(s, fuzzy=True)
        return dt.date()
    except Exception:
        return None


def find_col_by_header(headers, *keywords):
    for i, h in enumerate(headers):
        h_low = (h or "").strip().lower()
        for kw in keywords:
            if kw in h_low:
                return i
    return -1


def compute_daily_counts(df, date_col_hints):
    """Group accepted rows by their actual sign-up date."""
    if df.empty:
        return {}

    headers = list(df.columns)
    date_idx = -1
    for h in date_col_hints:
        date_idx = find_col_by_header(headers, h)
        if date_idx != -1:
            break

    if date_idx == -1:
        log(f"   No date column found in: {headers}")
        return {}

    date_col = headers[date_idx]
    log(f"   Using date column: {date_col}")

    counts = {}
    for _, row in df.iterrows():
        # Only count ACCEPTED rows
        if "final_status" in row and str(row["final_status"]).upper() != "ACCEPTED":
            continue

        date_val = row[date_col]
        d = parse_date_safe(date_val)
        if d is None:
            continue

        key = d.isoformat()
        counts[key] = counts.get(key, 0) + 1

    return counts


def build_daily_counts_table():
    log("=" * 60)
    log("Building Daily_Counts from actual sign-up dates")
    log("=" * 60)

    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(MASTER_SHEET_URL)

    # Load Verified_* tabs
    def load_tab(name):
        try:
            ws = sh.worksheet(name)
            data = ws.get_all_values()
            if len(data) < 2:
                return pd.DataFrame()
            return pd.DataFrame(data[1:], columns=data[0])
        except gspread.WorksheetNotFound:
            return pd.DataFrame()

    free = load_tab("Verified_FREE")
    upload = load_tab("Verified_FIRST_UPLOAD")
    stripe = load_tab("Verified_STRIPE")

    log(f"Loaded: free={len(free)}, upload={len(upload)}, stripe={len(stripe)}")

    # Compute daily counts from ACTUAL dates
    free_counts = compute_daily_counts(free, ["created", "account", "date"])
    upload_counts = compute_daily_counts(upload, ["upload", "created", "date"])
    stripe_counts = compute_daily_counts(stripe, ["created", "date"])

    log(f"Free daily counts: {len(free_counts)} unique dates")
    log(f"Upload daily counts: {len(upload_counts)} unique dates")
    log(f"Stripe daily counts: {len(stripe_counts)} unique dates")

    # Combine into one table - one row per date
    all_dates = sorted(set(free_counts.keys()) | set(upload_counts.keys()) | set(stripe_counts.keys()))

    rows = []
    for d in all_dates:
        rows.append({
            "Date": d,
            "Year": d[:4],
            "Month": d[:7],
            "SignUps": free_counts.get(d, 0),
            "FirstUploads": upload_counts.get(d, 0),
            "PaidSubscribers": stripe_counts.get(d, 0),
        })

    if not rows:
        log("No data to write")
        return

    # Add cumulative totals
    df_out = pd.DataFrame(rows)
    df_out["CumSignUps"] = df_out["SignUps"].cumsum()
    df_out["CumFirstUploads"] = df_out["FirstUploads"].cumsum()
    df_out["CumPaidSubscribers"] = df_out["PaidSubscribers"].cumsum()

    # Add a "last updated" timestamp column
    df_out["LastUpdated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Write to Daily_Counts tab (overwrite each run with fresh truth)
    try:
        ws = sh.worksheet("Daily_Counts")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Daily_Counts", rows=max(1000, len(df_out)+50), cols=15)

    ws.clear()
    values = [df_out.columns.tolist()] + df_out.astype(str).values.tolist()
    ws.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
    log(f"Wrote {len(df_out)} daily rows to Daily_Counts")

    # Also write per-month rollup
    df_month = df_out.groupby("Month").agg({
        "SignUps": "sum",
        "FirstUploads": "sum",
        "PaidSubscribers": "sum",
    }).reset_index()
    df_month["LastUpdated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        ws_m = sh.worksheet("Monthly_Counts")
    except gspread.WorksheetNotFound:
        ws_m = sh.add_worksheet(title="Monthly_Counts", rows=200, cols=10)

    ws_m.clear()
    values_m = [df_month.columns.tolist()] + df_month.astype(str).values.tolist()
    ws_m.update(range_name="A1", values=values_m, value_input_option="USER_ENTERED")
    log(f"Wrote {len(df_month)} monthly rows to Monthly_Counts")


def main():
    build_daily_counts_table()


if __name__ == "__main__":
    main()
