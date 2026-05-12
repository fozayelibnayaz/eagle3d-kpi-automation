"""
SHEET PROCESSOR v5 - DATE-AWARE DEDUP + TODAY-ONLY COUNTING
- Filters raw rows to TODAY only before counting
- Stops cumulative totals being repeated every day
- Appends one new row to Daily_Report per pipeline run
- Verified_ tabs store today's processed results only
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

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

RUN_TS   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
TODAY    = datetime.now().date()
TODAY_STR = TODAY.strftime("%Y-%m-%d")


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
        score = sum(
            1 for r in rows[:max_check]
            if col_idx < len(r) and "@" in r[col_idx] and "." in r[col_idx]
        )
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
    """
    Process raw tab and return count of ACCEPTED records scraped TODAY only.
    FIX: previously counted ALL rows on every run producing same cumulative
    number repeated daily. Now filters rows to today before processing.
    """
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
    looks_like_header = any(
        any(kw in (c or "").strip().lower()
            for kw in ("email", "name", "user", "phone", "source",
                       "date", "customer", "created"))
        for c in first_row
    )

    if looks_like_header:
        headers  = first_row
        all_rows = data[1:]
    else:
        n_cols   = max(len(r) for r in data)
        headers  = [f"col_{i}" for i in range(n_cols)]
        all_rows = data

    log(f"   Headers: {headers}")
    log(f"   Total raw rows (all time): {len(all_rows)}")

    e_idx = find_col_by_header(headers, "email")
    if e_idx == -1:
        e_idx = find_email_col_by_content(all_rows)
        if e_idx != -1:
            headers[e_idx] = "Email"
            log(f"   Auto-detected email at index {e_idx}")

    if e_idx == -1:
        log(f"   No email column found. Skipping.")
        return 0

    s_idx = find_col_by_header(headers, "source", "lead")
    d_idx = find_col_by_header(
        headers, "created", "date", "signed", "registered", "added", "processed_at"
    )
    log(f"   Email col: {e_idx} | Source col: {s_idx} | Date col: {d_idx}")

    keep_rows     = []
    emails        = []
    scrape_dates  = []
    skipped_internal  = 0
    skipped_old_date  = 0

    for row in all_rows:
        padded = row + [""] * (len(headers) - len(row))
        email  = padded[e_idx].strip() if e_idx < len(padded) else ""

        if not email or "@" not in email:
            continue
        if any(kw in email.lower() for kw in INTERNAL_EMAIL_KEYWORDS):
            skipped_internal += 1
            continue

        row_date = TODAY
        if d_idx != -1 and d_idx < len(padded):
            parsed = parse_row_date(padded[d_idx])
            if parsed:
                row_date = parsed

        # KEY FIX: skip rows from previous days
        if row_date != TODAY:
            skipped_old_date += 1
            continue

        keep_rows.append(padded)
        emails.append(email)
        scrape_dates.append(row_date)

    log(f"   Skipped internal:               {skipped_internal}")
    log(f"   Skipped (not today {TODAY_STR}): {skipped_old_date}")
    log(f"   Today's rows to validate:       {len(emails)}")

    if not emails:
        log(f"   No new rows for today. Returning 0.")
        return 0

    log(f"   Validating emails...")
    val_results = validate_batch(emails)

    enriched = []
    accepted = 0

    for raw, vres, scrape_date in zip(keep_rows, val_results, scrape_dates):
        email = raw[e_idx].strip()
        src   = raw[s_idx].strip() if s_idx != -1 and s_idx < len(raw) else ""

        dedup          = check_lead_status(email, current_scrape_date=scrape_date)
        history_dates  = get_history_dates_for_email(email)
        history_dates_str = (
            ", ".join(str(d) for d in history_dates if d) if history_dates else ""
        )
        ml = predict_scores(email, src)

        if is_paid:
            is_accepted = (vres["verdict"] == "VALID")
        else:
            is_accepted = (vres["verdict"] == "VALID" and dedup == "NEW")

        if is_accepted:
            accepted += 1

        rd = {headers[i]: raw[i] if i < len(raw) else "" for i in range(len(headers))}
        rd["normalized_email"]     = normalize_email(email)
        rd["row_date_used"]        = str(scrape_date)
        rd["history_dates_in_db"]  = history_dates_str
        rd["email_verdict"]        = vres["verdict"]
        rd["verdict_reason"]       = vres["reason"]
        rd["deduplication_status"] = dedup
        rd["legitimacy_score"]     = ml["score"]
        rd["ml_quality_tier"]      = ml["tier"]
        rd["final_status"]         = "ACCEPTED" if is_accepted else "REJECTED"
        rd["processed_at"]         = RUN_TS
        rd["report_year"]          = datetime.now().year
        rd["report_month"]         = datetime.now().strftime("%Y-%m")
        enriched.append(rd)

    ver_tab = raw_tab.replace("Raw_", "Verified_")
    try:
        out_ws = sh.worksheet(ver_tab)
    except gspread.WorksheetNotFound:
        out_ws = sh.add_worksheet(
            title=ver_tab,
            rows=max(1000, len(enriched) + 100),
            cols=max(30, len(enriched[0]) + 2)
        )

    df = pd.DataFrame(enriched)
    out_ws.clear()
    out_ws.update(
        range_name="A1",
        values=[df.columns.tolist()] + df.astype(str).values.tolist(),
        value_input_option="USER_ENTERED"
    )

    log(f"   Wrote {len(enriched)} today's rows to {ver_tab} | ACCEPTED: {accepted}")
    return accepted


def append_daily_report(sh, metrics: dict):
    """
    Appends exactly ONE row per pipeline run.
    Never overwrites — Daily_Report is an append-only ledger.
    """
    EXPECTED_HEADERS = list(metrics.keys())

    try:
        ws       = sh.worksheet("Daily_Report")
        existing = ws.get_all_values()

        if not existing:
            ws.update(
                range_name="A1",
                values=[EXPECTED_HEADERS],
                value_input_option="USER_ENTERED"
            )
            log("Daily_Report: wrote header (was empty).")
        else:
            sheet_headers = existing[0]
            if sheet_headers != EXPECTED_HEADERS:
                log(
                    f"[WARN] Daily_Report header mismatch!\n"
                    f"  Sheet:    {sheet_headers}\n"
                    f"  Expected: {EXPECTED_HEADERS}\n"
                    f"  Not auto-fixing — check manually."
                )

    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Daily_Report", rows=5000, cols=15)
        ws.update(
            range_name="A1",
            values=[EXPECTED_HEADERS],
            value_input_option="USER_ENTERED"
        )
        log("Daily_Report: created tab + header.")

    ws.append_row(list(metrics.values()), value_input_option="USER_ENTERED")
    log(f"Daily_Report: appended row → {metrics}")


def main():
    log("=" * 60)
    log("SHEET PROCESSOR v5 - DATE-AWARE DEDUP + TODAY-ONLY COUNTING")
    log("=" * 60)

    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    gc    = gspread.authorize(creds)
    sh    = gc.open_by_url(MASTER_SHEET_URL)
    log(f"Opened: {sh.title}")

    now     = datetime.now()
    metrics = {
        "Timestamp": RUN_TS,
        "Date":      now.strftime("%Y-%m-%d"),
        "Year":      now.year,
        "Month":     now.strftime("%Y-%m"),
    }

    for raw_tab, key, is_paid in [
        ("Raw_FREE",         "SignUps_Accepted",       False),
        ("Raw_FIRST_UPLOAD", "FirstUploads_Accepted",  False),
        ("Raw_STRIPE",       "PaidSubscribers_Accepted", True),
    ]:
        accepted      = process_one_tab(sh, raw_tab, is_paid=is_paid)
        metrics[key]  = accepted

    append_daily_report(sh, metrics)
    log("Done.")


if __name__ == "__main__":
    main()
