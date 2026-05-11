"""
PRODUCTION SCRAPER v4 — Eagle 3D Streaming KPI Dashboard
- Waits for month dropdown to enable (~54s typically)
- Switches to 'Current Month' and waits for data refresh
- Scrapes all 4 tabs with virtualization + pagination handling
- Writes to Google Sheets (and optional CSV backup)
"""
import sqlite3
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
from playwright.sync_api import sync_playwright, Page

from config import (
    BROWSER_SESSION_DIR, DATA_DIR, DEBUG_DIR, KPI_URL,
    HEADLESS, SLOW_MO_MS, TABS, MONTH_FILTER,
    DROPDOWN_ENABLE_TIMEOUT, DATA_REFRESH_WAIT,
)
from sheets_writer import write_tab_data, write_run_summary, test_connection

TODAY = datetime.now().strftime("%Y-%m-%d")
RUN_TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ────────────────────────────────────────────────────────────────────────────
#  DROPDOWN HANDLING
# ────────────────────────────────────────────────────────────────────────────

def is_dropdown_enabled(page: Page) -> bool:
    try:
        el = page.locator(".MuiSelect-select").first
        if el.count() == 0:
            return False
        if el.get_attribute("aria-disabled") == "true":
            return False
        if "Mui-disabled" in (el.get_attribute("class") or ""):
            return False
        return True
    except Exception:
        return False


def wait_for_dropdown_enabled(page: Page, max_wait: int) -> bool:
    log(f"⏳ Waiting up to {max_wait}s for month dropdown to become clickable...")
    elapsed = 0
    while elapsed < max_wait:
        if is_dropdown_enabled(page):
            log(f"🎉 Dropdown ENABLED after {elapsed}s.")
            return True
        if elapsed > 0 and elapsed % 10 == 0:
            log(f"   ...still waiting ({elapsed}s)")
        time.sleep(1)
        elapsed += 1
    log(f"❌ Dropdown did not enable within {max_wait}s.")
    return False


def change_month_filter(page: Page, target: str = "Current Month") -> bool:
    log(f"📅 Changing month filter to '{target}'...")
    try:
        page.locator(".MuiSelect-select").first.click(timeout=5000)
        time.sleep(1.5)

        options = page.locator("li[role='option']").all()
        log(f"   Menu opened with {len(options)} options.")

        for opt in options:
            txt = (opt.text_content() or "").strip()
            if txt == target:
                opt.click(timeout=5000)
                log(f"   ✅ Clicked '{target}'.")

                # Wait for data to refresh
                log(f"   ⏳ Waiting {DATA_REFRESH_WAIT}s for data to refresh...")
                time.sleep(DATA_REFRESH_WAIT)
                log("   ✅ Data refresh wait complete.")
                return True

        log(f"   ❌ '{target}' not found in menu options.")
        return False
    except Exception as e:
        log(f"   ❌ Error changing month: {e}")
        return False


# ────────────────────────────────────────────────────────────────────────────
#  TAB & TABLE SCRAPING
# ────────────────────────────────────────────────────────────────────────────

def click_tab(page: Page, tab_label: str) -> bool:
    log(f"🔄 Switching to tab: {tab_label}")
    strategies = [
        lambda: page.locator(f"button:has-text('{tab_label}')").first,
        lambda: page.get_by_role("button", name=tab_label),
        lambda: page.locator(f".MuiButton-root:has-text('{tab_label}')").first,
    ]
    for i, strategy in enumerate(strategies, 1):
        try:
            el = strategy()
            el.wait_for(state="visible", timeout=5000)
            el.click(timeout=5000)
            time.sleep(3)  # let table render
            log(f"   ✅ Clicked '{tab_label}' (strategy {i})")
            return True
        except Exception:
            continue
    log(f"   ❌ Could not click tab '{tab_label}'.")
    return False


def get_total_row_count(page: Page) -> int:
    """Read 'X-Y of Z' pagination footer."""
    try:
        text = page.locator(".MuiTablePagination-displayedRows").first.text_content(timeout=3000)
        if text and "of" in text:
            total = text.split("of")[-1].strip()
            return int(total)
    except Exception:
        pass
    return 0


def set_max_page_size(page: Page):
    """Set rows-per-page to maximum to minimize pagination clicks."""
    try:
        rpp = page.locator(".MuiTablePagination-select").first
        if rpp.count() == 0:
            return
        rpp.click(timeout=3000)
        time.sleep(1)
        options = page.locator("li[role='option']").all()
        values = []
        for opt in options:
            try:
                v = (opt.text_content() or "").strip()
                if v.isdigit():
                    values.append((int(v), opt))
            except Exception:
                continue
        if values:
            values.sort(key=lambda x: x[0], reverse=True)
            largest_val, largest_opt = values[0]
            largest_opt.click(timeout=3000)
            log(f"   📊 Set rows-per-page to {largest_val}")
            time.sleep(2.5)
        else:
            page.keyboard.press("Escape")
    except Exception:
        pass


def get_headers(page: Page) -> list[str]:
    headers = []
    try:
        h_loc = page.locator(".MuiDataGrid-columnHeaderTitle")
        for i in range(h_loc.count()):
            t = h_loc.nth(i).text_content()
            if t and t.strip():
                headers.append(t.strip())
    except Exception:
        pass
    return headers


def harvest_visible_rows(page: Page, headers: list[str], collected: dict):
    """Collect all currently rendered rows by data-id."""
    rows = page.locator(".MuiDataGrid-row").all()
    for row in rows:
        try:
            row_id = row.get_attribute("data-id") or row.get_attribute("data-rowindex")
            if row_id is None or row_id in collected:
                continue
            cells = row.locator(".MuiDataGrid-cell").all_text_contents()
            cells = [c.strip() for c in cells]
            if not cells:
                continue
            row_dict = {}
            for i, cell in enumerate(cells):
                col_name = headers[i] if i < len(headers) else f"col_{i}"
                row_dict[col_name] = cell
            collected[row_id] = row_dict
        except Exception:
            continue


def scroll_and_collect(page: Page, expected_total: int) -> list[dict]:
    """Scroll the virtual grid and collect every row."""
    headers = get_headers(page)
    log(f"   Columns: {headers}")

    collected = {}
    harvest_visible_rows(page, headers, collected)

    scroller = page.locator(".MuiDataGrid-virtualScroller").first
    if scroller.count() > 0:
        last_count = -1
        stagnant = 0
        for step in range(150):
            try:
                scroller.evaluate("el => el.scrollBy(0, 150)")
            except Exception:
                break
            time.sleep(0.3)
            harvest_visible_rows(page, headers, collected)
            if len(collected) == last_count:
                stagnant += 1
                if stagnant >= 8:
                    break
            else:
                stagnant = 0
                last_count = len(collected)
            if expected_total and len(collected) >= expected_total:
                break

    return list(collected.values())


def scrape_tab(page: Page, tab_label: str) -> list[dict]:
    if not click_tab(page, tab_label):
        return []

    try:
        page.wait_for_selector(".MuiDataGrid-root", timeout=15000)
    except Exception:
        log(f"   ⚠️  No DataGrid on tab '{tab_label}'.")
        return []
    time.sleep(2)

    expected = get_total_row_count(page)
    log(f"   📋 Footer shows total: {expected}")

    set_max_page_size(page)
    expected = get_total_row_count(page) or expected
    log(f"   📋 After page-size max, total: {expected}")

    all_rows = []
    page_num = 1
    while True:
        log(f"   📄 Page {page_num}...")
        rows = scroll_and_collect(page, expected)
        log(f"      Got {len(rows)} rows on this page.")
        for r in rows:
            r["__tab__"] = tab_label
            r["__scraped_at__"] = RUN_TS
        all_rows.extend(rows)

        if expected and len(all_rows) >= expected:
            break

        # Try next page
        try:
            next_btn = page.locator("button[aria-label='Go to next page']").first
            if next_btn.count() == 0 or next_btn.is_disabled():
                break
            next_btn.click(timeout=3000)
            time.sleep(2.5)
            page_num += 1
            if page_num > 50:
                break
        except Exception:
            break

    # Deduplicate
    seen = set()
    unique = []
    for r in all_rows:
        key = str({k: v for k, v in r.items() if not k.startswith("__")})
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)

    log(f"✅ Tab '{tab_label}' final unique rows: {len(unique)}")
    return unique


def debug_screenshot(page: Page, tag: str):
    try:
        path = DEBUG_DIR / f"{RUN_TS}_{tag}.png"
        page.screenshot(path=str(path), full_page=True)
    except Exception:
        pass


# ────────────────────────────────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────────────────────────────────

def main():
    log("=" * 65)
    log("  Eagle 3D KPI Dashboard — PRODUCTION SCRAPER v4")
    log("=" * 65)

    # Test Google Sheets connection FIRST
    log("Testing Google Sheets connection...")
    if not test_connection():
        log("❌ Aborting: fix Google Sheets setup first.")
        return

    if not BROWSER_SESSION_DIR.exists() or not any(BROWSER_SESSION_DIR.iterdir()):
        raise RuntimeError("No browser session. Run: python login_once.py")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_SESSION_DIR),
            headless=HEADLESS,
            slow_mo=SLOW_MO_MS,
            viewport={"width": 1600, "height": 1000},
        )
        page = context.new_page()
        page.goto(KPI_URL, wait_until="domcontentloaded")

        log("Waiting for dashboard...")
        page.wait_for_selector("text=Free Trial Accounts", timeout=30000)
        log("✅ Dashboard loaded.")

        # STEP 1: Wait for dropdown to enable
        if not wait_for_dropdown_enabled(page, DROPDOWN_ENABLE_TIMEOUT):
            debug_screenshot(page, "dropdown_never_enabled")
            log("❌ Aborting.")
            context.close()
            return

        # STEP 2: Change to Current Month
        if not change_month_filter(page, MONTH_FILTER):
            debug_screenshot(page, "month_change_failed")
            log("❌ Aborting.")
            context.close()
            return

        debug_screenshot(page, "after_month_set")

        # STEP 3: Scrape all tabs
        summary = {}
        for tab in TABS:
            try:
                rows = scrape_tab(page, tab)
                write_tab_data(tab, rows)
                summary[tab] = len(rows)
                debug_screenshot(page, f"after_{tab.replace(' ', '_')}")
            except Exception as e:
                log(f"❌ Error on tab '{tab}': {e}")
                summary[tab] = 0

        # STEP 4: Log run summary to Sheets
        write_run_summary(summary)

        context.close()

    log("=" * 65)
    log("✅ ALL DONE")
    for tab, count in summary.items():
        log(f"   {tab}: {count} rows")
    log(f"   Total: {sum(summary.values())} rows")
    log("=" * 65)


if __name__ == "__main__":
    main()
