"""
sheets_writer.py
PRIMARY destination: Google Sheets
FALLBACK destination: CSV (ONLY if Sheets write fails)
Google Drive storage is fixed - Sheets is the main store.
"""
import csv
import json
import os
import tempfile
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

MASTER_SHEET_URL = os.environ.get(
    "MASTER_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1E5PI3-m7mTMKRQ4Cy-WqpVCo5dQjbICcA2EnrC9ORE4"
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
        raise FileNotFoundError(f"No credentials: {creds_file}")

    creds   = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    client  = gspread.authorize(creds)
    ss      = client.open_by_url(MASTER_SHEET_URL)

    _client = client
    _ss     = ss
    log(f"Connected to: '{ss.title}'")
    return _client, _ss


def test_connection() -> bool:
    try:
        _get_client()
        return True
    except Exception as e:
        log(f"Connection test failed: {e}")
        return False


def _get_or_create_worksheet(ss, tab_name: str, rows=5000, cols=50):
    try:
        return ss.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=tab_name, rows=rows, cols=cols)
        log(f"Created new worksheet: {tab_name}")
        return ws


def write_tab_data(tab_name: str, rows: list) -> bool:
    """
    Write rows to Google Sheets (PRIMARY).
    If Sheets fails -> write to CSV (FALLBACK).
    Returns True if Sheets write succeeded.
    """
    if not rows:
        log(f"{tab_name}: 0 rows - nothing to write")
        return False

    fields = sorted({k for r in rows for k in r.keys()})
    log(f"{tab_name}: writing {len(rows)} rows to Sheets...")

    # ── PRIMARY: Google Sheets ──
    try:
        client, ss = _get_client()
        ws = _get_or_create_worksheet(ss, tab_name)

        # Build matrix
        matrix = [fields]
        for row in rows:
            matrix.append([str(row.get(f, "")) for f in fields])

        ws.clear()
        ws.update("A1", matrix)

        # Verify
        rb = ws.get_all_values()
        actual = len(rb) - 1  # minus header
        if actual == len(rows):
            log(f"{tab_name}: Sheets write OK - {len(rows)} rows verified")
            return True
        else:
            log(f"{tab_name}: Sheets verify MISMATCH wrote={len(rows)} read={actual}")
            # Fall through to CSV fallback

    except Exception as e:
        log(f"{tab_name}: Sheets write FAILED: {e}")
        log(f"{tab_name}: Falling back to CSV...")

    # ── FALLBACK: CSV (only if Sheets failed) ──
    _write_csv_fallback(tab_name, rows, fields)
    return False


def append_tab_rows(tab_name: str, new_rows: list) -> bool:
    """
    Append new rows to a Sheets tab (for Daily_Report style accumulation).
    If Sheets fails -> append to CSV.
    """
    if not new_rows:
        return False

    try:
        client, ss = _get_client()
        ws = _get_or_create_worksheet(ss, tab_name)

        existing = ws.get_all_values()
        if existing:
            headers = existing[0]
        else:
            headers = sorted({k for r in new_rows for k in r.keys()})
            ws.append_row(headers)

        for row in new_rows:
            ws.append_row([str(row.get(h, "")) for h in headers])

        log(f"{tab_name}: appended {len(new_rows)} rows to Sheets")
        return True

    except Exception as e:
        log(f"{tab_name}: Sheets append failed: {e} -> CSV fallback")
        _append_csv_fallback(tab_name, new_rows)
        return False


def read_tab_data(tab_name: str) -> list:
    """
    Read from Google Sheets (PRIMARY).
    If fails -> read from CSV.
    """
    # ── PRIMARY: Sheets ──
    try:
        client, ss = _get_client()
        ws   = ss.worksheet(tab_name)
        rows = ws.get_all_records()
        log(f"{tab_name}: read {len(rows)} rows from Sheets")
        return rows
    except gspread.WorksheetNotFound:
        log(f"{tab_name}: worksheet not found in Sheets")
    except Exception as e:
        log(f"{tab_name}: Sheets read failed: {e} -> trying CSV")

    # ── FALLBACK: CSV ──
    for fname in (f"{tab_name}.csv", f"Raw_{tab_name}.csv",
                  f"Verified_{tab_name}.csv", f"ARCHIVE_{tab_name}.csv"):
        path = DATA_DIR / fname
        if path.exists() and path.stat().st_size > 0:
            try:
                with open(path, "r", newline="", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
                log(f"{tab_name}: read {len(rows)} rows from {fname}")
                return rows
            except Exception as e:
                log(f"CSV read error {fname}: {e}")

    log(f"{tab_name}: no data found anywhere")
    return []


def write_run_summary(summary: dict) -> bool:
    """Append one summary row to Daily_Report sheet."""
    try:
        client, ss = _get_client()
        ws = _get_or_create_worksheet(ss, "Daily_Report")

        existing = ws.get_all_values()
        headers  = existing[0] if existing else list(summary.keys())

        if not existing:
            ws.append_row(headers)

        ws.append_row([str(summary.get(h, "")) for h in headers])
        log(f"Daily_Report: summary appended to Sheets")
        return True

    except Exception as e:
        log(f"Daily_Report Sheets failed: {e} -> CSV fallback")
        _append_csv_fallback("Daily_Report", [summary])
        return False


def _write_csv_fallback(tab_name: str, rows: list, fields: list):
    """Write CSV only as fallback when Sheets is unavailable."""
    csv_path = DATA_DIR / f"{tab_name}.csv"
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        log(f"{tab_name}: CSV fallback written -> {csv_path} ({len(rows)} rows)")
    except Exception as e:
        log(f"{tab_name}: CSV fallback ALSO failed: {e}")


def _append_csv_fallback(tab_name: str, rows: list):
    """Append to CSV fallback file."""
    csv_path = DATA_DIR / f"{tab_name}.csv"
    try:
        fields   = sorted({k for r in rows for k in r.keys()})
        existing = []
        if csv_path.exists():
            with open(csv_path, "r", newline="", encoding="utf-8") as f:
                existing = list(csv.DictReader(f))
                if existing:
                    for k in existing[0].keys():
                        if k not in fields:
                            fields.append(k)
        existing.extend(rows)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(existing)
        log(f"{tab_name}: CSV append fallback -> {csv_path}")
    except Exception as e:
        log(f"{tab_name}: CSV append fallback failed: {e}")
