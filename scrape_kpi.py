"""
KPI DASHBOARD SCRAPER v7 - FULL HISTORICAL
Iterates: Last 6 Months option -> each tab -> paginates ALL pages
Captures every row across all visible months.
"""
import os
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

from config import KPI_URL, KPI_USERNAME, KPI_PASSWORD
from sheets_writer import write_tab_data, write_run_summary

TABS = ["FREE", "PAID", "500 MIN", "FIRST UPLOAD"]
SCRAPE_TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [KPI] {msg}", flush=True)


def login(page):
    log(f"Loading {KPI_URL}")
    page.goto(KPI_URL, wait_until="domcontentloaded", timeout=60000)
    time.sleep(3)

    # Try to find login form
    try:
        if page.locator('input[type="text"], input[type="email"], input[name*="user" i]').count() > 0:
            log("Login form detected")
            user_input = page.locator('input[type="text"], input[type="email"], input[name*="user" i]').first
            pass_input = page.locator('input[type="password"]').first
            user_input.fill(KPI_USERNAME)
            pass_input.fill(KPI_PASSWORD)
            # Click submit
            for sel in ['button[type="submit"]', 'button:has-text("Login")', 'button:has-text("Sign in")', 'input[type="submit"]']:
                if page.locator(sel).count() > 0:
                    page.locator(sel).first.click()
                    break
            time.sleep(5)
            log("Login submitted")
    except Exception as e:
        log(f"Login skip/error: {e}")

    # Wait for dashboard ready
    page.wait_for_load_state("networkidle", timeout=60000)
    time.sleep(5)
    log("Dashboard loaded")


def select_month_option(page, option_text):
    """Click the month dropdown and select the given option. Returns True if successful."""
    try:
        # Find dropdown - it's usually top-right showing "Last Month" by default
        dropdown_selectors = [
            'div[role="combobox"]',
            'button:has-text("Last Month")',
            'button:has-text("Month")',
            '[class*="select" i]:has-text("Month")',
            'div:has-text("Last Month")',
        ]
        clicked = False
        for sel in dropdown_selectors:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=2000):
                    el.click()
                    clicked = True
                    log(f"Opened dropdown via: {sel}")
                    break
            except Exception:
                continue

        if not clicked:
            log(f"Could not open dropdown for option '{option_text}'")
            return False

        time.sleep(1.5)

        # Now find and click the option
        option_selectors = [
            f'li:has-text("{option_text}")',
            f'div[role="option"]:has-text("{option_text}")',
            f'[role="menuitem"]:has-text("{option_text}")',
            f'text="{option_text}"',
        ]
        for sel in option_selectors:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=2000):
                    el.click()
                    log(f"Selected option: {option_text}")
                    time.sleep(4)  # wait for data to refresh
                    return True
            except Exception:
                continue

        log(f"Could not click option '{option_text}'")
        return False
    except Exception as e:
        log(f"select_month_option error: {e}")
        return False


def click_tab(page, tab_name):
    """Click a tab button (FREE / PAID / 500 MIN / FIRST UPLOAD)."""
    selectors = [
        f'button:has-text("{tab_name}")',
        f'[role="tab"]:has-text("{tab_name}")',
        f'div[role="button"]:has-text("{tab_name}")',
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=3000):
                el.click()
                log(f"Clicked tab: {tab_name}")
                time.sleep(3)
                return True
        except Exception:
            continue
    log(f"Could not click tab: {tab_name}")
    return False


def extract_table_rows(page):
    """Extract current page's table data as list of dicts."""
    try:
        # Try to grab the data grid
        rows_data = page.evaluate("""
            () => {
                // Find the table - MUI DataGrid typically
                const grids = document.querySelectorAll('[role="grid"], table, [class*="MuiDataGrid"]');
                if (grids.length === 0) return {headers: [], rows: []};

                // Get the most likely candidate (largest)
                let bestGrid = grids[0];
                let maxRows = 0;
                for (const g of grids) {
                    const r = g.querySelectorAll('[role="row"], tr').length;
                    if (r > maxRows) { maxRows = r; bestGrid = g; }
                }

                // Get headers
                const headerCells = bestGrid.querySelectorAll('[role="columnheader"], th');
                const headers = Array.from(headerCells).map(h => h.innerText.trim()).filter(h => h);

                // Get rows
                const rowEls = bestGrid.querySelectorAll('[role="row"], tr');
                const rows = [];
                for (const r of rowEls) {
                    const cells = r.querySelectorAll('[role="cell"], [role="gridcell"], td');
                    if (cells.length === 0) continue;
                    const rowVals = Array.from(cells).map(c => c.innerText.trim());
                    // Skip if it's a header row
                    if (rowVals.every(v => headers.includes(v))) continue;
                    if (rowVals.some(v => v.length > 0)) rows.push(rowVals);
                }

                return {headers, rows};
            }
        """)
        return rows_data
    except Exception as e:
        log(f"extract error: {e}")
        return {"headers": [], "rows": []}


def paginate_and_collect(page, tab_label):
    """Click through all pages of the table, collecting rows."""
    all_rows = []
    headers = []
    page_num = 1
    max_pages = 200  # safety limit

    while page_num <= max_pages:
        time.sleep(1.5)
        data = extract_table_rows(page)
        if data["headers"]:
            headers = data["headers"]
        if not data["rows"]:
            log(f"  Page {page_num}: no rows, stopping")
            break

        all_rows.extend(data["rows"])
        log(f"  Page {page_num}: +{len(data['rows'])} rows (total: {len(all_rows)})")

        # Try to click "next page"
        next_btn_selectors = [
            'button[aria-label="Go to next page"]',
            'button[title="Go to next page"]',
            'button[aria-label*="next" i]',
            'button:has(svg[data-testid*="NextIcon" i])',
        ]
        clicked_next = False
        for sel in next_btn_selectors:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=1500):
                    if btn.is_disabled():
                        log(f"  Next button disabled - reached end")
                        return headers, all_rows
                    btn.click()
                    clicked_next = True
                    page_num += 1
                    time.sleep(2)
                    break
            except Exception:
                continue

        if not clicked_next:
            log(f"  No next button found - done")
            break

    return headers, all_rows


def scrape_tab_for_period(page, tab_label, period_label):
    """Click the tab, paginate through it, return enriched rows."""
    if not click_tab(page, tab_label):
        return []

    headers, rows = paginate_and_collect(page, tab_label)
    if not rows:
        return []

    enriched = []
    for r in rows:
        d = {}
        for i, h in enumerate(headers):
            d[h] = r[i] if i < len(r) else ""
        d["__period__"] = period_label
        d["__tab__"] = tab_label
        d["__scraped_at__"] = SCRAPE_TS
        enriched.append(d)
    log(f"  {tab_label} [{period_label}]: {len(enriched)} total rows collected")
    return enriched


def deduplicate_rows(rows, key_field_candidates=("Email", "email")):
    """Remove duplicates based on email field (across months)."""
    seen = set()
    out = []
    for r in rows:
        key = None
        for k in key_field_candidates:
            if k in r and r[k]:
                key = r[k].strip().lower()
                break
        if key is None:
            # Try to find any email-like value
            for v in r.values():
                if isinstance(v, str) and "@" in v and "." in v:
                    key = v.strip().lower()
                    break
        if not key:
            out.append(r)  # keep if no email found
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def main():
    log("=" * 60)
    log("KPI SCRAPER v7 - FULL HISTORICAL")
    log("=" * 60)

    # Try multiple period options to maximize data capture
    PERIODS_TO_TRY = ["Last 6 Months", "Last 3 Months", "Last Month"]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(viewport={"width": 1600, "height": 1000})
        page = context.new_page()

        try:
            login(page)

            # Collect data per tab across all periods
            all_data = {tab: [] for tab in TABS}

            # First, try the longest period available
            chosen_period = None
            for per in PERIODS_TO_TRY:
                if select_month_option(page, per):
                    chosen_period = per
                    log(f"Using period: {per}")
                    break
                else:
                    log(f"Period '{per}' not available, trying next...")

            if not chosen_period:
                chosen_period = "Last Month (default)"
                log("Could not change period - using default")

            # Now scrape each tab
            for tab in TABS:
                log(f"--- Scraping tab: {tab} ---")
                rows = scrape_tab_for_period(page, tab, chosen_period)
                all_data[tab].extend(rows)

            # Dedupe per tab
            summary = {}
            for tab in TABS:
                rows = deduplicate_rows(all_data[tab])
                log(f"Final {tab}: {len(rows)} unique rows")
                if rows:
                    write_tab_data(tab, rows)
                summary[tab] = len(rows)

            write_run_summary(summary)
            log("KPI scrape complete")

        except Exception as e:
            log(f"FATAL: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            try:
                browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
