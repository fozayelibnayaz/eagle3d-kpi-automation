"""
sheets_writer.py
NEW SHEET: https://docs.google.com/spreadsheets/d/1skn4jfyctfusixCfK1_zr8IzVIDNcm-5qY1xWx6AH0E
PRIMARY destination: Google Sheets
FALLBACK: CSV only if Sheets write fails
DATA SOURCE: KPI Dashboard scraper + Stripe scraper (NOT old sheet data)
"""
import csv
import os
import tempfile
import json
from pathlib import Path
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ← NEW CLEAN SHEET
MASTER_SHEET_URL = os.environ.get(
    "MASTER_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1skn4jfyctfusixCfK1_zr8IzVIDNcm-5qY1xWx6AH0E"
)

_client = None
_ss     = None


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Sheets] {msg}", flush=True)


def _get_client():
    global _client, _ss
    if _client is not None:
        return _client, _ss

    creds_file = "google_creds.json"
    creds_env  = os.environ.get("GOOGLE_CREDS_JSON", "")

    if creds_env:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        tmp.write(creds_env)
        tmp.close()
        creds_file = tmp.name

    if not os.path.exists(creds_file):
        raise FileNotFoundError(f"No credentials file: {creds_file}")

    creds   = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    client  = gspread.authorize(creds)
    ss      = client.open_by_url(MASTER_SHEET_URL)

    _client = client
    _ss     = ss
    log(f"Connected: '{ss.title}' (ID: {ss.id})")
    return _client, _ss


def test_connection() -> bool:
    try:
        _get_client()
        return True
    except Exception as e:
        log(f"Connection failed: {e}")
        return False


def _get_or_create_ws(ss, tab_name: str, rows=10000, cols=50):
    try:
        return ss.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=tab_name, rows=rows, cols=cols)
        log(f"Created new tab: '{tab_name}'")
        return ws


def write_tab_data(tab_name: str, rows: list) -> bool:
    """
    Write rows to Sheets tab (PRIMARY).
    Overwrites tab completely each time.
    Falls back to CSV only if Sheets fails.
    Returns True if Sheets write succeeded.
    """
    if not rows:
        log(f"{tab_name}: 0 rows - nothing to write")
        return False

    fields = sorted({k for r in rows for k in r.keys()})
    log(f"{tab_name}: writing {len(rows)} rows, {len(fields)} columns...")

    # ── PRIMARY: Google Sheets ──
    try:
        client, ss = _get_client()
        ws = _get_or_create_ws(ss, tab_name)

        matrix = [fields]
        for row in rows:
            matrix.append([str(row.get(f, "")) for f in fields])

        ws.clear()
        ws.update("A1", matrix)

        # Verify
        rb     = ws.get_all_values()
        actual = len(rb) - 1
        if actual == len(rows):
            log(f"{tab_name}: Sheets OK - {len(rows)} rows written and verified")
            return True
        else:
            log(f"{tab_name}: MISMATCH wrote={len(rows)} verified={actual}")

    except Exception as e:
        log(f"{tab_name}: Sheets FAILED ({e}) -> CSV fallback")

    # ── FALLBACK: CSV ──
    _csv_fallback_write(tab_name, rows, fields)
    return False


def read_tab_data(tab_name: str) -> list:
    """
    Read rows from Sheets tab (PRIMARY).
    Falls back to CSV if Sheets fails.
    """
    try:
        client, ss = _get_client()
        ws   = ss.worksheet(tab_name)
        rows = ws.get_all_records()
        log(f"{tab_name}: read {len(rows)} rows from Sheets")
        return rows
    except gspread.WorksheetNotFound:
        log(f"{tab_name}: tab not found in Sheets")
    except Exception as e:
        log(f"{tab_name}: Sheets read failed ({e}) -> CSV fallback")

    # ── FALLBACK: CSV ──
    for fname in (
        f"{tab_name}.csv",
        f"Raw_{tab_name}.csv",
        f"Verified_{tab_name}.csv",
    ):
        path = DATA_DIR / fname
        if path.exists() and path.stat().st_size > 0:
            try:
                with open(path, "r", newline="", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
                log(f"{tab_name}: read {len(rows)} rows from {fname}")
                return rows
            except Exception as e:
                log(f"CSV read error: {e}")

    log(f"{tab_name}: no data found")
    return []


def append_row_to_tab(tab_name: str, row: dict) -> bool:
    """Append a single row to a tab (for Daily_Report log)."""
    try:
        client, ss = _get_client()
        ws = _get_or_create_ws(ss, tab_name)

        existing = ws.get_all_values()
        if existing:
            headers = existing[0]
        else:
            headers = sorted(row.keys())
            ws.append_row(headers)

        ws.append_row([str(row.get(h, "")) for h in headers])
        log(f"{tab_name}: row appended")
        return True

    except Exception as e:
        log(f"{tab_name}: append failed ({e})")
        return False


def write_run_summary(summary: dict) -> bool:
    """Append pipeline run summary to Daily_Report tab."""
    try:
        return append_row_to_tab("Daily_Report", summary)
    except Exception as e:
        log(f"write_run_summary failed: {e}")
        return False


def _csv_fallback_write(tab_name: str, rows: list, fields: list):
    path = DATA_DIR / f"{tab_name}.csv"
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        log(f"{tab_name}: CSV fallback -> {path} ({len(rows)} rows)")
    except Exception as e:
        log(f"{tab_name}: CSV fallback ALSO failed: {e}")
