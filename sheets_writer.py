"""
Google Sheets writer - the canonical interface for writing to master sheet.
"""
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pandas as pd
from config import GOOGLE_CREDS_FILE, MASTER_SHEET_URL

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _client():
    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    return gspread.authorize(creds)


def _open_sheet():
    gc = _client()
    return gc.open_by_url(MASTER_SHEET_URL)


def _get_or_create_worksheet(sh, title, rows=2000, cols=20):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def write_tab_data(tab_label, rows):
    """Write scraped rows to Raw_<TAB> tab. Always overwrites."""
    if not rows:
        print(f"   [Sheets] No rows to write for {tab_label}.")
        return

    sh = _open_sheet()
    ws_title = f"Raw_{tab_label.replace(' ', '_')}"
    ws = _get_or_create_worksheet(sh, ws_title, rows=max(2000, len(rows) + 100))

    df = pd.DataFrame(rows)
    cols = [c for c in df.columns if not c.startswith("__")] + \
           [c for c in df.columns if c.startswith("__")]
    df = df[cols]

    ws.clear()
    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    ws.update(range_name="A1", values=values, value_input_option="USER_ENTERED")
    print(f"   [Sheets] Wrote {len(df)} rows to '{ws_title}'.")


def write_run_summary(summary):
    """Append a one-line summary of this run to Run_Log tab."""
    sh = _open_sheet()
    ws = _get_or_create_worksheet(sh, "Run_Log", rows=2000, cols=10)
    existing = ws.get_all_values()
    if not existing:
        ws.update(range_name="A1", values=[["Timestamp", "FREE", "PAID",
                                            "500_MIN", "FIRST_UPLOAD", "Total"]])
    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        summary.get("FREE", 0),
        summary.get("PAID", 0),
        summary.get("500 MIN", 0),
        summary.get("FIRST UPLOAD", 0),
        sum(int(v) if str(v).isdigit() else 0 for v in summary.values()),
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    print("   [Sheets] Run logged to 'Run_Log'.")


def test_connection():
    """Verify we can connect to the master sheet."""
    try:
        sh = _open_sheet()
        print(f"Connected to Google Sheet: '{sh.title}'")
        return True
    except Exception as e:
        print(f"Google Sheets connection failed: {e}")
        return False
