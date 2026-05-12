# sheets_writer.py

import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
from config import GOOGLE_CREDS_FILE, MASTER_SHEET_URL

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [SheetsWriter] {msg}", flush=True)


def get_sheet_client():
    creds = Credentials.from_service_account_file(
        str(GOOGLE_CREDS_FILE), scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    return gc.open_by_url(MASTER_SHEET_URL)


def write_tab_data(sh, tab_name: str, rows: list):
    """
    Append-only writer — never clears existing data.
    - Creates tab + header row if tab does not exist yet.
    - Checks existing header to keep columns consistent.
    - Appends only NEW rows below existing data.
    """
    if not rows:
        log(f"No rows to write for {tab_name}, skipping.")
        return

    headers = list(rows[0].keys())

    try:
        ws = sh.worksheet(tab_name)
        existing_values = ws.get_all_values()

        if existing_values:
            existing_headers = existing_values[0]
            if existing_headers != headers:
                log(
                    f"[WARN] {tab_name} header mismatch. "
                    f"Sheet has {existing_headers}, writer has {headers}. "
                    f"Using sheet existing header order."
                )
                headers = existing_headers
        else:
            ws.update(
                range_name="A1",
                values=[headers],
                value_input_option="USER_ENTERED"
            )
            log(f"{tab_name}: wrote header row (tab was empty).")

    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=tab_name,
            rows=max(5000, len(rows) + 100),
            cols=max(30, len(headers) + 5)
        )
        ws.update(
            range_name="A1",
            values=[headers],
            value_input_option="USER_ENTERED"
        )
        log(f"{tab_name}: created new tab + header row.")

    new_rows = [
        [str(row.get(h, "")) for h in headers]
        for row in rows
    ]

    for row_values in new_rows:
        ws.append_row(row_values, value_input_option="USER_ENTERED")

    log(f"{tab_name}: appended {len(new_rows)} rows. Raw data preserved.")
