"""
SHEET PROCESSOR v6
Processes ALL rows from Raw_* tabs (no today-only filter).
Writes to Verified_* + appends to Daily_Report (one row per pipeline run).
The Daily_Counts module then groups by actual sign-up date.
"""
import gspread
import pandas as pd
from datetime import datetime
from dateutil import parser as dateparser
from google.oauth2.service_account import Credentials

from config import GOOGLE_CREDS_FILE, MASTER_SHEET_URL, INTERNAL_EMAIL_KEYWORDS
from dedup_engine import check_lead_status, normalize_email, get_history_dates_for_email
from ml_intelligence import predict_scores
from email_validator_engine import validate_batch


def load_raw_data(tab_name: str) -> list:
    """Load raw data - tries CSV snapshot first, then archive."""
    from storage_adapter import read_tab_data, read_archive
    rows = read_tab_data(tab_name)
    if not rows:
        print(f"[ProcessData] {tab_name}: no snapshot, trying archive...", flush=True)
        rows = read_archive(tab_name)
    print(f"[ProcessData] {tab_name}: loaded {len(rows)} rows for processing", flush=True)
    return rows


SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
RUN_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
TODAY = datetime.now().date()


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Processor] {msg}", flush=True)


def find_col_by_header(headers, *keywords):
    for i, h in enumerate(headers):
        h_low = (h or "").strip().lower()
        for kw in keywords:
            if kw in h_low:
                return i
    return -1


def find_email_col_by_content(rows, max_check=20):
    if not rows:
        return -1
    n_cols = max(len(r) for r in rows[:max_check])
    best_idx, best_score = -1, 0
    for col_idx in range(n_cols):
        score = sum(1 for r in rows[:max_check]
                    if col_idx < len(r) and "@" in r[col_idx] and "." in r[col_idx])
        if score > best_score:
            best_score, best_idx = score, col_idx
    return best_idx if best_score >= 2 else -1


def parse_row_date(val):
    if not val:
        return None
    val = str(val).strip()
    if not val:
        return None
    try:
        return dateparser.parse(val, fuzzy=True).date()
    except Exception:
        return None


def process_one_tab(sh, raw_tab, is_paid=False):
    log(f"--- Processing {raw_tab} (is_paid={is_paid}) ---")
    try:
        ws = sh.worksheet(raw_tab)
    except gspread.WorksheetNotFound:
        log(f"   {raw_tab} not found, skipping.")
        return 0

    data = ws.get_all_values()
    if not data:
        log(f"   Empty.")
        return 0

    first_row = data[0]
    looks_like_header = any(any(kw in (c or "").strip().lower()
                               for kw in ("email", "name", "user", "phone", "source",
                                          "date", "customer", "created"))
                           for c in first_row)

    if looks_like_header:
        headers = first_row
        rows = data[1:]
    else:
        n_cols = max(len(r) for r in data)
        headers = [f"col_{i}" for i in range(n_cols)]
        rows = data

    log(f"   Headers: {headers}")
    log(f"   Total raw rows: {len(rows)}")

    e_idx = find_col_by_header(headers, "email")
    if e_idx == -1:
        e_idx = find_email_col_by_content(rows)
        if e_idx != -1:
            headers[e_idx] = "Email"

    if e_idx == -1:
        log(f"   No email column. Skipping.")
        return 0

    s_idx = find_col_by_header(headers, "source", "lead")
    d_idx = find_col_by_header(headers, "created", "date", "signed", "registered", "added")

    log(f"   Email col: {e_idx} | Source col: {s_idx} | Date col: {d_idx}")

    keep_rows, emails, scrape_dates = [], [], []
    skipped_internal = 0

    for row in rows:
        padded = row + [""] * (len(headers) - len(row))
        email = padded[e_idx].strip() if e_idx < len(padded) else ""
        if not email or "@" not in email:
            continue
        if any(kw in email.lower() for kw in INTERNAL_EMAIL_KEYWORDS):
            skipped_internal += 1
            continue

        # Use the row's actual sign-up date for dedup comparison
        row_date = TODAY
        if d_idx != -1 and d_idx < len(padded):
            parsed = parse_row_date(padded[d_idx])
            if parsed:
                row_date = parsed

        keep_rows.append(padded)
        emails.append(email)
        scrape_dates.append(row_date)

    log(f"   Internal skipped: {skipped_internal} | To validate: {len(emails)}")
    if not emails:
        return 0

    log(f"   Validating {len(emails)} emails...")
    val_results = validate_batch(emails)

    enriched = []
    accepted_count = 0

    for raw, vres, scrape_date in zip(keep_rows, val_results, scrape_dates):
        email = raw[e_idx].strip()
        src = raw[s_idx].strip() if s_idx != -1 and s_idx < len(raw) else ""

        dedup = check_lead_status(email, current_scrape_date=scrape_date)
        history_dates = get_history_dates_for_email(email)
        history_dates_str = ", ".join(str(d) for d in history_dates if d) if history_dates else ""

        ml = predict_scores(email, src)

        if is_paid:
            is_accepted = (vres["verdict"] == "VALID")
        else:
            is_accepted = (vres["verdict"] == "VALID" and dedup == "NEW")

        if is_accepted:
            accepted_count += 1

        rd = {headers[i]: raw[i] if i < len(raw) else "" for i in range(len(headers))}
        rd["normalized_email"] = normalize_email(email)
        rd["row_date_used"] = str(scrape_date)
        rd["history_dates_in_db"] = history_dates_str
        rd["email_verdict"] = vres["verdict"]
        rd["verdict_reason"] = vres["reason"]
        rd["deduplication_status"] = dedup
        rd["legitimacy_score"] = ml.get("score", 0)
        rd["ml_quality_tier"] = ml.get("tier", "")
        rd["final_status"] = "ACCEPTED" if is_accepted else "REJECTED"
        rd["processed_at"] = RUN_TS
        rd["report_year"] = datetime.now().year
        rd["report_month"] = datetime.now().strftime("%Y-%m")
        enriched.append(rd)

    ver_tab = raw_tab.replace("Raw_", "Verified_")
    try:
        out_ws = sh.worksheet(ver_tab)
    except gspread.WorksheetNotFound:
        out_ws = sh.add_worksheet(title=ver_tab,
                                  rows=max(1000, len(enriched)+100),
                                  cols=max(30, len(enriched[0])+2))

    df = pd.DataFrame(enriched)
    out_ws.clear()
    out_ws.update(range_name="A1",
                  values=[df.columns.tolist()] + df.astype(str).values.tolist(),
                  value_input_option="USER_ENTERED")

    log(f"   Wrote {len(enriched)} rows to {ver_tab} | ACCEPTED: {accepted_count}")
    return accepted_count


def append_daily_report(sh, metrics):
    try:
        ws = sh.worksheet("Daily_Report")
        existing = ws.get_all_values()
        if not existing:
            ws.update(range_name="A1", values=[list(metrics.keys())])
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Daily_Report", rows=5000, cols=15)
        ws.update(range_name="A1", values=[list(metrics.keys())])

    ws.append_row(list(metrics.values()), value_input_option="USER_ENTERED")
    log(f"Daily_Report row appended: {metrics}")


def main():
    log("=" * 60)
    log("SHEET PROCESSOR v6 - Process ALL rows (no today-only filter)")
    log("=" * 60)

    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(MASTER_SHEET_URL)
    log(f"Opened: {sh.title}")

    now = datetime.now()
    metrics = {
        "Timestamp": RUN_TS,
        "Date": now.strftime("%Y-%m-%d"),
        "Year": now.year,
        "Month": now.strftime("%Y-%m"),
    }

    for raw_tab, key, is_paid in [
        ("Raw_FREE", "SignUps_Accepted", False),
        ("Raw_FIRST_UPLOAD", "FirstUploads_Accepted", False),
        ("Raw_STRIPE", "PaidSubscribers_Accepted", True),
    ]:
        accepted = process_one_tab(sh, raw_tab, is_paid=is_paid)
        metrics[key] = accepted

    append_daily_report(sh, metrics)
    log("Done.")


if __name__ == "__main__":
    main()
