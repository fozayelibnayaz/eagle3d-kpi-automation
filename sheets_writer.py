"""
sheets_writer.py
Google Sheets I/O with rate limiting and CSV fallback.
"""
import csv
import os
import json
import time
import tempfile
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

MASTER_SHEET_URL = os.environ.get(
    "MASTER_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1E5PI3-m7mTMKRQ4Cy-WqpVCo5dQjbICcA2EnrC9ORE4"
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_client = None
_ss     = None

# Rate limiting state
_last_request_time = [0.0]
MIN_INTERVAL = 1.2  # seconds between requests (~50/min, safe under 60/min limit)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Sheets] {msg}", flush=True)


def _rate_limit():
    """Sleep if we're calling Sheets API too fast."""
    elapsed = time.time() - _last_request_time[0]
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_request_time[0] = time.time()


def _retry_on_quota(func, *args, max_retries=4, **kwargs):
    """Retry a Sheets API call on 429 / quota errors."""
    for attempt in range(max_retries):
        try:
            _rate_limit()
            return func(*args, **kwargs)
        except Exception as e:
            err = str(e).lower()
            is_quota = (
                "429" in err or
                "quota" in err or
                "rate" in err or
                "rateLimitExceeded" in str(e)
            )
            if is_quota and attempt < max_retries - 1:
                wait = 30 * (attempt + 1)
                log(f"Quota hit, waiting {wait}s (retry {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            raise
    raise Exception("Max retries exhausted")


def _get_client():
    global _client, _ss
    if _client is not None:
        return _client, _ss

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds_env  = os.environ.get("GOOGLE_CREDS_JSON", "")
        creds_file = "google_creds.json"

        if creds_env:
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            tmp.write(creds_env)
            tmp.close()
            creds_file = tmp.name

        if not os.path.exists(creds_file):
            raise FileNotFoundError(f"No credentials: {creds_file}")

        creds  = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        client = gspread.authorize(creds)
        ss     = client.open_by_url(MASTER_SHEET_URL)

        _client = client
        _ss     = ss
        log(f"Connected: '{ss.title}'")
        return _client, _ss

    except Exception as e:
        log(f"Connection failed: {e}")
        raise


def test_connection() -> bool:
    try:
        _get_client()
        return True
    except Exception:
        return False


def _get_or_create_ws(ss, tab_name: str):
    try:
        import gspread
        try:
            return ss.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(title=tab_name, rows=10000, cols=50)
            log(f"Created tab: '{tab_name}'")
            return ws
    except Exception as e:
        raise RuntimeError(f"Cannot get/create worksheet '{tab_name}': {e}")


def write_tab_data(tab_name: str, rows: list) -> bool:
    """Write rows to Sheets. CSV fallback if Sheets fails."""
    if not rows:
        log(f"{tab_name}: 0 rows")
        return False

    fields = sorted({k for r in rows for k in r.keys()})
    log(f"{tab_name}: writing {len(rows)} rows...")

    try:
        client, ss = _get_client()
        ws = _get_or_create_ws(ss, tab_name)

        matrix = [fields] + [[str(r.get(f, "")) for f in fields] for r in rows]

        _retry_on_quota(ws.clear)
        _retry_on_quota(ws.update, "A1", matrix)

        # Verify
        rb = _retry_on_quota(ws.get_all_values)
        if len(rb) - 1 == len(rows):
            log(f"{tab_name}: Sheets OK - {len(rows)} rows")
            return True
        log(f"{tab_name}: verify mismatch wrote={len(rows)} read={len(rb)-1}")

    except Exception as e:
        log(f"{tab_name}: Sheets failed ({e}) -> CSV")

    # CSV fallback
    try:
        path = DATA_DIR / f"{tab_name}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        log(f"{tab_name}: CSV fallback -> {path}")
    except Exception as e:
        log(f"{tab_name}: CSV failed: {e}")
    return False


def read_tab_data(tab_name: str) -> list:
    """Read from Sheets. CSV fallback if Sheets fails."""
    try:
        client, ss = _get_client()
        ws   = ss.worksheet(tab_name)
        rows = _retry_on_quota(ws.get_all_records)
        log(f"{tab_name}: read {len(rows)} rows from Sheets")
        return rows
    except Exception as e:
        log(f"{tab_name}: Sheets read failed ({e}) -> CSV")

    for fname in (f"{tab_name}.csv", f"Raw_{tab_name}.csv", f"Verified_{tab_name}.csv"):
        path = DATA_DIR / fname
        if path.exists() and path.stat().st_size > 0:
            try:
                with open(path, "r", newline="", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
                log(f"{tab_name}: read {len(rows)} rows from {fname}")
                return rows
            except Exception:
                pass
    return []


def write_run_summary(summary: dict) -> bool:
    """Append one row to Daily_Report tab."""
    try:
        client, ss = _get_client()
        ws = _get_or_create_ws(ss, "Daily_Report")
        existing = _retry_on_quota(ws.get_all_values)
        headers  = existing[0] if existing else sorted(summary.keys())
        if not existing:
            _retry_on_quota(ws.append_row, headers)
        _retry_on_quota(ws.append_row, [str(summary.get(h, "")) for h in headers])
        log("Daily_Report: appended")
        return True
    except Exception as e:
        log(f"Summary append failed: {e}")
        try:
            path = DATA_DIR / "run_summaries.json"
            data = json.load(open(path)) if path.exists() else []
            data.append({"ts": datetime.now().isoformat(), **summary})
            json.dump(data[-200:], open(path, "w"), indent=2, default=str)
        except Exception:
            pass
        return False
