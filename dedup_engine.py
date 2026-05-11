"""
DEDUPLICATION ENGINE v2 - DATE AWARE
Pulls full history from old DB with their original dates.
A user is DUPLICATE only if they appear in DB on a date EARLIER than today.
Same-date entries are NOT duplicates (it is the same record being captured).
"""
import gspread
from datetime import datetime
from dateutil import parser as dateparser
from google.oauth2.service_account import Credentials
from config import GOOGLE_CREDS_FILE, OLD_DATABASE_SHEET_URL, OLD_SHEET_EMAIL_COLUMN_HINTS

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Cache structure: {normalized_email: [list of dates seen in old DB]}
_history_cache = None

# Date column hints in old DB
DATE_COLUMN_HINTS = ["date", "created", "registered", "signed", "signup",
                     "added", "updated", "time", "timestamp"]


def normalize_email(email: str) -> str:
    if not email or "@" not in email:
        return (email or "").strip().lower()
    email = email.strip().lower()
    local, domain = email.rsplit("@", 1)
    if "+" in local:
        local = local.split("+", 1)[0]
    if domain in ("gmail.com", "googlemail.com"):
        local = local.replace(".", "")
        domain = "gmail.com"
    return f"{local}@{domain}"


def parse_any_date(s):
    """Try to parse any date string. Returns date object or None."""
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    try:
        return dateparser.parse(s, fuzzy=True).date()
    except Exception:
        return None


def find_email_columns(headers):
    return [i for i, h in enumerate(headers)
            if any(hint in (h or "").strip().lower()
                   for hint in OLD_SHEET_EMAIL_COLUMN_HINTS)]


def find_date_columns(headers):
    return [i for i, h in enumerate(headers)
            if any(hint in (h or "").strip().lower()
                   for hint in DATE_COLUMN_HINTS)]


def fetch_historical_emails():
    """Build {email: [dates]} map from all historical entries."""
    global _history_cache
    if _history_cache is not None:
        return _history_cache

    print("   [Dedup v2] Loading history with dates from old DB...")
    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    gc = gspread.authorize(creds)

    try:
        sh = gc.open_by_url(OLD_DATABASE_SHEET_URL)
    except Exception as e:
        print(f"   [Dedup v2] Failed to open old DB: {e}")
        _history_cache = {}
        return _history_cache

    history = {}  # {email: [date1, date2, ...]}

    for ws in sh.worksheets():
        try:
            records = ws.get_all_values()
            if len(records) < 2:
                continue

            headers = [h.strip() for h in records[0]]
            email_cols = find_email_columns(headers)
            date_cols = find_date_columns(headers)

            if not email_cols:
                continue

            for row in records[1:]:
                # Try to find the row date - first non-empty parseable date column
                row_date = None
                for d_idx in date_cols:
                    if d_idx < len(row):
                        parsed = parse_any_date(row[d_idx])
                        if parsed:
                            row_date = parsed
                            break

                # Process all email columns in this row
                for e_idx in email_cols:
                    if e_idx < len(row):
                        val = row[e_idx].strip()
                        if val and "@" in val:
                            norm = normalize_email(val)
                            if norm:
                                if norm not in history:
                                    history[norm] = []
                                history[norm].append(row_date)
        except Exception as e:
            print(f"   [Dedup v2] Tab '{ws.title}' skipped: {e}")
            continue

    print(f"   [Dedup v2] Loaded {len(history)} unique emails with date history.")
    _history_cache = history
    return history


def check_lead_status(email, current_scrape_date=None):
    """
    Determine if email is NEW or DUPLICATE based on date logic.

    Args:
        email: the email to check
        current_scrape_date: a date object representing TODAY (when the user
                            was captured). Defaults to today.

    Returns:
        "DUPLICATE" if email exists in old DB on a date EARLIER than current_scrape_date.
        "NEW" otherwise (including same-date matches and not-in-DB).
    """
    if current_scrape_date is None:
        current_scrape_date = datetime.now().date()
    elif isinstance(current_scrape_date, datetime):
        current_scrape_date = current_scrape_date.date()

    history = fetch_historical_emails()
    norm = normalize_email(email)

    if norm not in history:
        return "NEW"

    past_dates = history[norm]

    # Filter for valid dates that are STRICTLY EARLIER than today
    earlier_dates = [d for d in past_dates if d is not None and d < current_scrape_date]

    if earlier_dates:
        return "DUPLICATE"

    # All matches are either same-date or undated -> treat as NEW
    return "NEW"


def get_history_dates_for_email(email):
    """Diagnostic helper - returns list of dates this email was seen."""
    history = fetch_historical_emails()
    norm = normalize_email(email)
    return history.get(norm, [])
