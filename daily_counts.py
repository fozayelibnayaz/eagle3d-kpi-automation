"""
DAILY COUNTS ANALYTICS
Reads Verified_* tabs and computes TRUE per-day counts from Account Created On dates.
Writes Daily_Counts (with email details) and Monthly_Counts.
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
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return dateparser.parse(s, fuzzy=True).date()
    except Exception:
        return None


def find_col(headers, *kw_groups):
    for kw in kw_groups:
        for i, h in enumerate(headers):
            if kw in (h or "").strip().lower():
                return i
    return -1


def collect_per_day(df, date_kw_groups, label):
    if df.empty:
        return {}
    headers = list(df.columns)
    date_idx = -1
    for kw in date_kw_groups:
        date_idx = find_col(headers, kw)
        if date_idx != -1:
            break
    if date_idx == -1:
        log(f"   {label}: no date col in {headers}")
        return {}

    email_idx = find_col(headers, "email")
    name_idx = find_col(headers, "username", "customer", "name")

    counts = {}
    for _, row in df.iterrows():
        if "final_status" in row and str(row["final_status"]).upper() != "ACCEPTED":
            continue
        d = parse_date_safe(row.iloc[date_idx])
        if d is None:
            continue
        key = d.isoformat()
        if key not in counts:
            counts[key] = {"count": 0, "details": []}
        counts[key]["count"] += 1

        email = str(row.iloc[email_idx]).strip() if email_idx != -1 else ""
        name = str(row.iloc[name_idx]).strip() if name_idx != -1 else ""
        if name and email:
            d_str = f"{name} <{email}>"
        elif email:
            d_str = email
        elif name:
            d_str = name
        else:
            d_str = ""
        if d_str:
            counts[key]["details"].append(d_str)

    return counts


def build_daily_counts_table():
    """Main function called by daily_pipeline.py"""
    log("=" * 60)
    log("Building Daily_Counts and Monthly_Counts")
    log("=" * 60)

    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(MASTER_SHEET_URL)

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

    free_data = collect_per_day(free, ["created", "account", "date"], "Free")
    upload_data = collect_per_day(upload, ["upload", "created", "date"], "Upload")
    stripe_data = collect_per_day(stripe, ["created", "date"], "Stripe")

    log(f"Free dates: {len(free_data)} | Upload dates: {len(upload_data)} | Stripe dates: {len(stripe_data)}")

    all_dates = sorted(set(free_data) | set(upload_data) | set(stripe_data))

    rows = []
    for d in all_dates:
        f = free_data.get(d, {"count": 0, "details": []})
        u = upload_data.get(d, {"count": 0, "details": []})
        s = stripe_data.get(d, {"count": 0, "details": []})
        rows.append({
            "Date": d,
            "Year": d[:4],
            "Month": d[:7],
            "SignUps": f["count"],
            "FirstUploads": u["count"],
            "PaidSubscribers": s["count"],
            "SignUp_Details": "; ".join(f["details"][:50]),
            "Upload_Details": "; ".join(u["details"][:50]),
            "Paid_Details": "; ".join(s["details"][:50]),
        })

    if not rows:
        log("No data to write")
        return

    df_out = pd.DataFrame(rows)
    df_out["CumSignUps"] = df_out["SignUps"].cumsum()
    df_out["CumFirstUploads"] = df_out["FirstUploads"].cumsum()
    df_out["CumPaidSubscribers"] = df_out["PaidSubscribers"].cumsum()
    df_out["LastUpdated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    col_order = ["Date", "Year", "Month",
                "SignUps", "FirstUploads", "PaidSubscribers",
                "CumSignUps", "CumFirstUploads", "CumPaidSubscribers",
                "SignUp_Details", "Upload_Details", "Paid_Details",
                "LastUpdated"]
    df_out = df_out[col_order]

    try:
        ws = sh.worksheet("Daily_Counts")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Daily_Counts", rows=max(1000, len(df_out)+50), cols=20)

    ws.clear()
    values = [df_out.columns.tolist()] + df_out.astype(str).values.tolist()
    ws.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
    log(f"Wrote {len(df_out)} daily rows to Daily_Counts")

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


# Aliases for compatibility
build_daily_counts = build_daily_counts_table
main = build_daily_counts_table


if __name__ == "__main__":
    main()
