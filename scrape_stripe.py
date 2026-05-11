"""
STRIPE SCRAPER v4 - DEFINITIVE
Handles your exact Stripe layout:
  - Headers: Customer, Email, Description, Country, Created, Total spend
  - Rows have 10 cells (2 leading empties for checkbox + avatar)
  - Date format: M/D/YY (US format) - parsed strictly
  - Filters to current month (auto-rolls to June, July, etc.)
"""
import os
import re
import time
from datetime import datetime, timedelta
import pandas as pd
from playwright.sync_api import sync_playwright
import gspread
from google.oauth2.service_account import Credentials

from config import (
    STRIPE_SESSION_DIR, STRIPE_CUSTOMERS_URL, STRIPE_PAGE_LOAD_WAIT,
    STRIPE_MAX_PAGES, GOOGLE_CREDS_FILE, MASTER_SHEET_URL, DEBUG_DIR
)

RUN_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
NOW = datetime.now()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

# Stripe date format: "5/8/26, 5:25 AM" = Month/Day/2-digit-Year
DATE_PATTERN = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{2,4})")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Stripe] {msg}", flush=True)


def parse_stripe_date(s):
    """Parse Stripe date strings like '5/8/26, 5:25 AM' as M/D/YY US format."""
    if not s:
        return None
    s = s.strip()
    sl = s.lower()

    # Relative
    if any(k in sl for k in ("today", "just now", "minute", "hour")):
        return NOW
    if "yesterday" in sl:
        return NOW - timedelta(days=1)

    # Match M/D/YY pattern explicitly
    m = DATE_PATTERN.search(s)
    if m:
        month = int(m.group(1))
        day = int(m.group(2))
        year = int(m.group(3))
        if year < 100:
            year += 2000  # 26 -> 2026
        try:
            return datetime(year, month, day)
        except ValueError:
            return None

    return None


def is_current_month(dt):
    if dt is None:
        return False
    return dt.year == NOW.year and dt.month == NOW.month


def debug_shot(page, tag):
    try:
        path = DEBUG_DIR / f"stripe_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{tag}.png"
        page.screenshot(path=str(path), full_page=True)
    except Exception:
        pass


def scrape():
    if not STRIPE_SESSION_DIR.exists() or not any(STRIPE_SESSION_DIR.iterdir()):
        raise RuntimeError("No Stripe session. Run: python stripe_load_cookies.py")

    log(f"URL: {STRIPE_CUSTOMERS_URL}")
    log(f"Filter: {NOW.strftime('%B %Y')} only (auto-updates each month)")

    chrome_paths = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    chrome_exec = next((p for p in chrome_paths if os.path.exists(p)), None)

    all_rows = []
    out_of_month_streak = 0
    THRESHOLD = 25

    with sync_playwright() as p:
        launch_args = dict(
            user_data_dir=str(STRIPE_SESSION_DIR),
            headless=False,
            slow_mo=80,
            viewport={"width": 1600, "height": 1000},
            args=["--disable-blink-features=AutomationControlled"],
            ignore_default_args=["--enable-automation"],
        )
        if chrome_exec:
            launch_args["executable_path"] = chrome_exec

        ctx = p.chromium.launch_persistent_context(**launch_args)
        page = ctx.new_page()
        page.goto(STRIPE_CUSTOMERS_URL, wait_until="domcontentloaded")
        log(f"Waiting {STRIPE_PAGE_LOAD_WAIT}s for table...")
        time.sleep(STRIPE_PAGE_LOAD_WAIT)

        if "login" in page.url.lower():
            log("Session expired. Run: python stripe_load_cookies.py")
            debug_shot(page, "login")
            ctx.close()
            return []

        debug_shot(page, "initial")

        page_num = 1
        while page_num <= STRIPE_MAX_PAGES:
            log(f"--- Page {page_num} ---")

            # Scroll to load lazy rows
            for _ in range(15):
                page.evaluate("window.scrollBy(0, 600)")
                time.sleep(0.2)

            # Get headers
            headers = page.locator("table thead th").all_text_contents()
            headers = [h.strip() for h in headers if h.strip()]
            log(f"   Headers: {headers}")

            # Build column index map - account for 2 leading empty cells
            col_map = {}
            for i, h in enumerate(headers):
                hl = h.lower()
                if "customer" in hl:
                    col_map["customer"] = i
                elif "email" in hl:
                    col_map["email"] = i
                elif "description" in hl:
                    col_map["description"] = i
                elif "country" in hl:
                    col_map["country"] = i
                elif "created" in hl or "date" in hl:
                    col_map["created"] = i
                elif "spend" in hl or "amount" in hl or "total" in hl:
                    col_map["total_spend"] = i

            log(f"   Column map: {col_map}")

            rows = page.locator("table tbody tr").all()
            log(f"   Found {len(rows)} row elements")
            kept, skipped = 0, 0

            for row in rows:
                cells = row.locator("td").all_text_contents()
                cells = [c.strip() for c in cells]
                if not cells or len(cells) < 4:
                    continue

                # Strip leading empty cells (checkbox/avatar columns in Stripe)
                while cells and cells[0] == "":
                    cells.pop(0)

                if len(cells) < 4:
                    continue

                # Now cells should align with headers
                row_dict = {}
                for key, idx in col_map.items():
                    if idx < len(cells):
                        row_dict[key] = cells[idx]
                    else:
                        row_dict[key] = ""

                # Validate we have email
                email_val = row_dict.get("email", "")
                if not email_val or "@" not in email_val:
                    continue

                # Date filter - current month only
                date_str = row_dict.get("created", "")
                parsed_date = parse_stripe_date(date_str)
                if parsed_date and not is_current_month(parsed_date):
                    skipped += 1
                    out_of_month_streak += 1
                    continue
                if parsed_date:
                    out_of_month_streak = 0

                row_dict["__scraped_at__"] = RUN_TS
                row_dict["__parsed_date__"] = parsed_date.strftime("%Y-%m-%d") if parsed_date else ""
                all_rows.append(row_dict)
                kept += 1

            log(f"   Page {page_num}: kept={kept}, skipped(old)={skipped}")
            debug_shot(page, f"page_{page_num}")

            if out_of_month_streak >= THRESHOLD:
                log(f"   {THRESHOLD}+ consecutive old rows. Stopping.")
                break

            # Try Next Page
            next_btn = page.locator("button[aria-label*='Next'], [data-test='pagination-next']").first
            if next_btn.count() == 0:
                log("   No next button. Stopping.")
                break
            try:
                if next_btn.is_disabled():
                    log("   Next button disabled. Stopping.")
                    break
                next_btn.click()
                time.sleep(3)
                page_num += 1
            except Exception:
                break

        ctx.close()

    # Dedup by email
    seen_emails = set()
    unique = []
    for r in all_rows:
        email = r.get("email", "").lower().strip()
        if email in seen_emails:
            continue
        seen_emails.add(email)
        unique.append(r)

    log(f"Total unique current-month paid customers: {len(unique)}")
    for r in unique:
        log(f"   - {r.get('customer','')} | {r.get('email','')} | {r.get('total_spend','')} | {r.get('__parsed_date__','')}")
    return unique


def write_to_sheet(rows):
    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(MASTER_SHEET_URL)

    try:
        ws = sh.worksheet("Raw_STRIPE")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Raw_STRIPE", rows=2000, cols=20)

    if not rows:
        ws.clear()
        ws.update(range_name="A1", values=[["Customer", "Email", "Description", "Country", "Created", "Total spend", "scraped_at", "parsed_date"]])
        log("No current-month rows. Cleared Raw_STRIPE with headers only.")
        return

    # Standardize column names
    out = []
    for r in rows:
        out.append({
            "Customer": r.get("customer", ""),
            "Email": r.get("email", ""),
            "Description": r.get("description", ""),
            "Country": r.get("country", ""),
            "Created": r.get("created", ""),
            "Total spend": r.get("total_spend", ""),
            "scraped_at": r.get("__scraped_at__", ""),
            "parsed_date": r.get("__parsed_date__", ""),
        })

    df = pd.DataFrame(out)
    ws.clear()
    ws.update(range_name="A1",
              values=[df.columns.tolist()] + df.astype(str).values.tolist(),
              value_input_option="USER_ENTERED")
    log(f"Wrote {len(df)} rows to Raw_STRIPE")


def main():
    rows = scrape()
    write_to_sheet(rows)


if __name__ == "__main__":
    main()
