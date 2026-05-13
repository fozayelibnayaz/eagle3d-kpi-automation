"""
sheets_writer.py - Google Sheets interface
PRIMARY: Google Sheets
FALLBACK: CSV only if Sheets fails
"""
import csv
import os
import json
import tempfile
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

MASTER_SHEET_URL = os.environ.get(
    "MASTER_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1skn4jfyctfusixCfK1_zr8IzVIDNcm-5qY1xWx6AH0E"
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_client = None
_ss     = None


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Sheets] {msg}", flush=True)


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
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            )
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
        matrix = [fields] + [[str(r.get(f,"")) for f in fields] for r in rows]
        ws.clear()
        ws.update("A1", matrix)
        rb = ws.get_all_values()
        if len(rb) - 1 == len(rows):
            log(f"{tab_name}: Sheets OK - {len(rows)} rows")
            return True
        log(f"{tab_name}: verify mismatch")
    except Exception as e:
        log(f"{tab_name}: Sheets failed ({e}) -> CSV")

    # CSV fallback
    try:
        p = DATA_DIR / f"{tab_name}.csv"
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        log(f"{tab_name}: CSV fallback -> {p}")
    except Exception as e:
        log(f"{tab_name}: CSV also failed: {e}")
    return False


def read_tab_data(tab_name: str) -> list:
    """Read from Sheets. CSV fallback if Sheets fails."""
    try:
        client, ss = _get_client()
        ws   = ss.worksheet(tab_name)
        rows = ws.get_all_records()
        log(f"{tab_name}: read {len(rows)} rows from Sheets")
        return rows
    except Exception as e:
        log(f"{tab_name}: Sheets read failed ({e}) -> CSV")

    for fname in (f"{tab_name}.csv", f"Raw_{tab_name}.csv", f"Verified_{tab_name}.csv"):
        p = DATA_DIR / fname
        if p.exists() and p.stat().st_size > 0:
            try:
                with open(p, "r", newline="", encoding="utf-8") as f:
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
        existing = ws.get_all_values()
        headers  = existing[0] if existing else sorted(summary.keys())
        if not existing:
            ws.append_row(headers)
        ws.append_row([str(summary.get(h,"")) for h in headers])
        log("Daily_Report: summary appended")
        return True
    except Exception as e:
        log(f"Summary append failed: {e}")
        # Save to local JSON
        try:
            p = DATA_DIR / "run_summaries.json"
            data = json.load(open(p)) if p.exists() else []
            data.append({"ts": datetime.now().isoformat(), **summary})
            json.dump(data[-200:], open(p,"w"), indent=2, default=str)
        except Exception:
            pass
        return False
