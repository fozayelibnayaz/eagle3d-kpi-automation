"""
scrape_kpi.py
DATA SOURCE: https://kpidashboard.eagle3dstreaming.com/ ONLY
Scrapes ALL historical data by selecting longest time period.
Paginates through ALL pages.
Writes to NEW Google Sheet (primary). CSV fallback if Sheets fails.
"""
import time
import json
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

from config import KPI_URL, KPI_USERNAME, KPI_PASSWORD
from sheets_writer import write_tab_data, write_run_summary

DATA_DIR    = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)
SCRAPE_TS   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
SCRAPE_DATE = datetime.now().strftime("%Y-%m-%d")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [KPI] {msg}", flush=True)


def login(page):
    log(f"Navigating to {KPI_URL}")
    page.goto(KPI_URL, wait_until="domcontentloaded", timeout=90000)
    time.sleep(3)

    if page.locator('input[type="password"]').count() > 0:
        log("Login form found - logging in")
        try:
            page.locator(
                'input[type="text"], input[type="email"], input[name*="user" i]'
            ).first.fill(KPI_USERNAME)
            page.locator('input[type="password"]').first.fill(KPI_PASSWORD)

            for btn_sel in [
                'button[type="submit"]',
                'button:has-text("Login")',
                'button:has-text("Sign In")',
                'input[type="submit"]',
            ]:
                if page.locator(btn_sel).count() > 0:
                    page.locator(btn_sel).first.click()
                    break

            time.sleep(6)
            log("Login submitted")
        except Exception as e:
            log(f"Login error: {e}")

    page.wait_for_load_state("networkidle", timeout=90000)
    time.sleep(5)
    log("Page ready")


def set_all_time_period(page) -> str:
    """
    Try to select the LONGEST available time period.
    Priority: All Time > Last 12 Months > Last 6 Months > Last 3 Months
    """
    WANT = [
        "All Time", "All time", "Lifetime",
        "Last 12 Months", "Last 6 Months",
        "Last 3 Months", "Last Month",
    ]

    log("Looking for time period filter...")

    # Screenshot current state for debug
    try:
        page.screenshot(path="data_output/debug_before_period.png")
        log("Screenshot saved: debug_before_period.png")
    except Exception:
        pass

    # Find any dropdown/select that looks like a period filter
    dropdown_texts = page.evaluate("""
        () => {
            const results = [];
            const sels = [
                'select', '[role="combobox"]', '[role="listbox"]',
                'button', '[class*="select" i]', '[class*="dropdown" i]',
                '[class*="filter" i]', '[class*="period" i]',
            ];
            for (const sel of sels) {
                const els = document.querySelectorAll(sel);
                for (const el of els) {
                    const txt = el.innerText?.trim() || el.value || '';
                    if (txt && txt.length < 60) {
                        results.push({
                            tag: el.tagName,
                            text: txt,
                            class: el.className?.toString().substring(0,50),
                        });
                    }
                }
            }
            return results;
        }
    """)

    log(f"Found {len(dropdown_texts)} interactive elements:")
    for item in dropdown_texts[:20]:
        log(f"  [{item['tag']}] '{item['text']}'")

    # Try clicking elements that look like period dropdowns
    for el_info in dropdown_texts:
        txt = el_info.get("text","")
        if any(
            hint in txt.lower()
            for hint in ["month","time","period","last","all","lifetime","filter"]
        ):
            log(f"Trying to click: '{txt}'")
            try:
                page.get_by_text(txt, exact=True).first.click()
                time.sleep(2)

                # Look for options that appeared
                for want in WANT:
                    for opt_sel in [
                        f'li:has-text("{want}")',
                        f'[role="option"]:has-text("{want}")',
                        f'div:has-text("{want}")',
                        f'span:has-text("{want}")',
                    ]:
                        try:
                            opt = page.locator(opt_sel).first
                            if opt.is_visible(timeout=1000):
                                opt.click()
                                time.sleep(5)
                                log(f"Period selected: {want}")
                                return want
                        except Exception:
                            continue

                # Close if nothing selected
                page.keyboard.press("Escape")
                time.sleep(1)
            except Exception as e:
                log(f"Click failed: {e}")
                continue

    log("Could not change period - using default (whatever is loaded)")
    return "default"


def click_tab(page, tab_name: str) -> bool:
    for sel in [
        f'button:has-text("{tab_name}")',
        f'[role="tab"]:has-text("{tab_name}")',
        f'div[role="button"]:has-text("{tab_name}")',
        f'a:has-text("{tab_name}")',
        f'span:has-text("{tab_name}")',
    ]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=3000):
                el.click()
                time.sleep(4)
                log(f"Tab clicked: {tab_name}")
                return True
        except Exception:
            continue
    log(f"Tab not found: {tab_name}")
    return False


def extract_table_data(page) -> dict:
    """Extract ALL data from the current visible table."""
    return page.evaluate("""
        () => {
            // Find the largest grid/table on the page
            const candidates = [
                ...document.querySelectorAll('[role="grid"]'),
                ...document.querySelectorAll('[class*="MuiDataGrid"]'),
                ...document.querySelectorAll('[class*="datagrid" i]'),
                ...document.querySelectorAll('table'),
                ...document.querySelectorAll('[role="table"]'),
            ];

            if (!candidates.length) return {headers: [], rows: [], total: 0};

            let best = candidates[0];
            let maxR = 0;
            for (const c of candidates) {
                const r = c.querySelectorAll('[role="row"], tr').length;
                if (r > maxR) { maxR = r; best = c; }
            }

            // Get column headers
            const hEls = best.querySelectorAll(
                '[role="columnheader"], th'
            );
            const headers = Array.from(hEls)
                .map(h => h.innerText.trim())
                .filter(h => h && h.length > 0 && h.length < 100);

            // Get data rows
            const rowEls = best.querySelectorAll('[role="row"], tr');
            const rows   = [];
            for (const r of rowEls) {
                const cells = r.querySelectorAll(
                    '[role="cell"], [role="gridcell"], td'
                );
                if (!cells.length) continue;
                const vals = Array.from(cells).map(c => c.innerText.trim());
                // Skip empty rows and header rows
                if (vals.every(v => !v)) continue;
                if (headers.length > 0 && vals[0] === headers[0]) continue;
                if (vals.filter(v => v.length > 0).length < 2) continue;
                rows.push(vals);
            }

            // Try to get total count from pagination text
            let total = rows.length;
            const pagTexts = document.querySelectorAll(
                '[class*="pagination" i], [class*="rowCount" i], [class*="total" i]'
            );
            for (const p of pagTexts) {
                const m = p.innerText.match(/of\\s+([\\d,]+)/i);
                if (m) {
                    total = parseInt(m[1].replace(/,/g, ''));
                    break;
                }
            }

            return {headers, rows, total};
        }
    """)


def paginate_and_collect(page, tab_label: str) -> tuple:
    """Paginate through ALL pages, collect every row."""
    all_rows    = []
    headers     = []
    seen_hashes = set()
    page_num    = 1

    # Try to set rows per page to maximum
    try:
        for rpp_sel in [
            '[aria-label*="rows per page" i]',
            'select[class*="page" i]',
            '[class*="rowsPerPage" i] select',
        ]:
            el = page.locator(rpp_sel).first
            if el.is_visible(timeout=2000):
                options = el.evaluate(
                    "el => Array.from(el.options).map(o=>o.value)"
                )
                if options:
                    biggest = sorted(
                        [o for o in options if o.isdigit()],
                        key=int
                    )
                    if biggest:
                        el.select_option(biggest[-1])
                        time.sleep(3)
                        log(f"Rows per page set to {biggest[-1]}")
                break
    except Exception:
        pass

    while page_num <= 1000:
        time.sleep(2)
        data = extract_table_data(page)

        if data.get("headers"):
            headers = data["headers"]

        if not data.get("rows"):
            log(f"  Page {page_num}: no rows extracted")
            break

        new_count = 0
        for row in data["rows"]:
            row_hash = "|".join(str(v) for v in row)
            if row_hash in seen_hashes:
                continue
            seen_hashes.add(row_hash)
            all_rows.append(row)
            new_count += 1

        total_hint = data.get("total", -1)
        total_str  = f" / ~{total_hint} total" if total_hint > 0 else ""
        log(
            f"  Page {page_num}: +{new_count} new rows"
            f" (collected: {len(all_rows)}{total_str})"
        )

        if new_count == 0:
            log(f"  No new rows on page {page_num} - done")
            break

        # Next page button
        clicked = False
        for sel in [
            'button[aria-label="Go to next page"]',
            'button[aria-label="Next page"]',
            'button[title="Next page"]',
            'button[title="Go to next page"]',
            '[data-testid="NavigateNextIcon"]',
            'button:has([data-testid="NavigateNextIcon"])',
            'button:has([data-testid="KeyboardArrowRightIcon"])',
        ]:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=1000):
                    disabled = (
                        btn.get_attribute("disabled") is not None
                        or btn.get_attribute("aria-disabled") == "true"
                        or "Mui-disabled" in (btn.get_attribute("class") or "")
                    )
                    if disabled:
                        log(f"  Reached last page ({page_num})")
                        return headers, all_rows
                    btn.click()
                    clicked = True
                    page_num += 1
                    time.sleep(2.5)
                    break
            except Exception:
                continue

        if not clicked:
            log(f"  No next button found - done at page {page_num}")
            break

    log(f"  {tab_label}: {len(all_rows)} total rows collected")
    return headers, all_rows


def build_enriched_rows(
    headers: list, raw_rows: list, tab: str, period: str
) -> list:
    result = []
    for r in raw_rows:
        d = {}
        for i, h in enumerate(headers):
            d[h] = r[i] if i < len(r) else ""
        d["__tab__"]         = tab
        d["__period__"]      = period
        d["__scraped_at__"]  = SCRAPE_TS
        d["__scrape_date__"] = SCRAPE_DATE
        result.append(d)
    return result


# Tabs to scrape on the KPI dashboard
TABS = {
    "FREE":          "FREE",
    "FIRST UPLOAD":  "FIRST_UPLOAD",
}


def main():
    log("=" * 60)
    log("KPI DASHBOARD SCRAPER")
    log(f"Source: {KPI_URL}")
    log(f"Time:   {SCRAPE_TS}")
    log("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        try:
            login(page)
            period = set_all_time_period(page)
            log(f"Time period: {period}")

            summary = {}

            for tab_label, tab_key in TABS.items():
                log(f"\n{'='*50}")
                log(f"SCRAPING TAB: {tab_label}")
                log(f"{'='*50}")

                if not click_tab(page, tab_label):
                    log(f"Skipping {tab_label} - tab not clickable")
                    summary[tab_key] = 0
                    continue

                headers, raw_rows = paginate_and_collect(page, tab_label)

                if not raw_rows:
                    log(f"{tab_label}: NO DATA SCRAPED")
                    summary[tab_key] = 0
                    continue

                log(f"{tab_label}: scraped {len(raw_rows)} rows")
                enriched = build_enriched_rows(
                    headers, raw_rows, tab_label, period
                )

                # Write to Sheets (primary). CSV fallback inside write_tab_data.
                sheet_tab = f"Raw_{tab_key}"
                ok = write_tab_data(sheet_tab, enriched)
                log(
                    f"{tab_label} -> Sheets '{sheet_tab}': "
                    f"{'OK' if ok else 'FAILED (CSV fallback used)'}"
                )
                summary[tab_key] = len(enriched)

            log(f"\n{'='*60}")
            log("KPI SCRAPE SUMMARY")
            log(f"{'='*60}")
            total = 0
            for tab, count in summary.items():
                log(f"  {tab:20s}: {count:>5} rows")
                total += count
            log(f"  {'TOTAL':20s}: {total:>5} rows")

            write_run_summary({
                "run_at":        SCRAPE_TS,
                "stage":         "kpi_scrape",
                "period":        period,
                "rows_free":     summary.get("FREE", 0),
                "rows_upload":   summary.get("FIRST_UPLOAD", 0),
                "total_scraped": total,
            })

        except Exception as e:
            log(f"FATAL ERROR: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            try:
                page.screenshot(path="data_output/debug_final.png")
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
