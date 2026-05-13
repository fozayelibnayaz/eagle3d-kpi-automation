"""
scrape_kpi.py
Scrapes KPI Dashboard for ALL historical data (last 6 months+).
Paginates through ALL pages.
Writes to Google Sheets (primary). CSV only if Sheets fails.
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
    log(f"Loading {KPI_URL}")
    page.goto(KPI_URL, wait_until="domcontentloaded", timeout=90000)
    time.sleep(3)

    for sel in ['input[type="password"]']:
        if page.locator(sel).count() > 0:
            log("Login form detected")
            try:
                user = page.locator(
                    'input[type="text"], input[type="email"], '
                    'input[name*="user" i]'
                ).first
                pwd = page.locator('input[type="password"]').first
                user.fill(KPI_USERNAME)
                pwd.fill(KPI_PASSWORD)
                for btn in [
                    'button[type="submit"]', 'button:has-text("Login")',
                    'button:has-text("Sign")', 'input[type="submit"]',
                ]:
                    if page.locator(btn).count() > 0:
                        page.locator(btn).first.click()
                        break
                time.sleep(6)
            except Exception as e:
                log(f"Login error: {e}")
            break

    page.wait_for_load_state("networkidle", timeout=60000)
    time.sleep(4)
    log("Dashboard ready")


def set_longest_period(page) -> str:
    PERIODS = [
        "All Time", "All time",
        "Last 12 Months", "Last 6 Months",
        "Last 3 Months", "Last Month",
    ]
    log("Setting time period to maximum available...")

    dropdown_selectors = [
        '[class*="select" i]', 'div[role="combobox"]',
        'button:has-text("Month")', 'button:has-text("Time")',
        '[class*="dropdown" i]', '[class*="filter" i]',
    ]

    for drop_sel in dropdown_selectors:
        try:
            els = page.locator(drop_sel).all()
            for el in els:
                text = el.inner_text().strip()
                if any(
                    p.lower() in text.lower()
                    for p in ["month","time","period","filter","last"]
                ):
                    el.click()
                    time.sleep(2)
                    for period in PERIODS:
                        for opt_sel in [
                            f'li:has-text("{period}")',
                            f'[role="option"]:has-text("{period}")',
                            f'div:has-text("{period}")',
                        ]:
                            try:
                                opt = page.locator(opt_sel).first
                                if opt.is_visible(timeout=1500):
                                    opt.click()
                                    time.sleep(5)
                                    log(f"Period set to: {period}")
                                    return period
                            except Exception:
                                continue
        except Exception:
            continue

    log("Could not change period - using default")
    return "default"


def click_tab(page, tab_name: str) -> bool:
    for sel in [
        f'button:has-text("{tab_name}")',
        f'[role="tab"]:has-text("{tab_name}")',
        f'div[role="button"]:has-text("{tab_name}")',
        f'a:has-text("{tab_name}")',
    ]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=3000):
                el.click()
                time.sleep(3)
                log(f"Clicked tab: {tab_name}")
                return True
        except Exception:
            continue
    log(f"Tab not found: {tab_name}")
    return False


def extract_table(page) -> dict:
    return page.evaluate("""
        () => {
            const candidates = [
                ...document.querySelectorAll('[role="grid"]'),
                ...document.querySelectorAll('[class*="MuiDataGrid"]'),
                ...document.querySelectorAll('table'),
                ...document.querySelectorAll('[role="table"]'),
            ];
            let best = null, maxRows = 0;
            for (const c of candidates) {
                const r = c.querySelectorAll('[role="row"],tr').length;
                if (r > maxRows) { maxRows = r; best = c; }
            }
            if (!best) return {headers:[], rows:[]};

            const hCells = best.querySelectorAll('[role="columnheader"],th');
            const headers = Array.from(hCells)
                .map(h => h.innerText.trim()).filter(h => h);

            const rowEls = best.querySelectorAll('[role="row"],tr');
            const rows = [];
            for (const r of rowEls) {
                const cells = r.querySelectorAll(
                    '[role="cell"],[role="gridcell"],td'
                );
                if (!cells.length) continue;
                const vals = Array.from(cells).map(c => c.innerText.trim());
                if (vals.every(v => !v)) continue;
                if (headers.length && vals[0] === headers[0]) continue;
                rows.push(vals);
            }
            return {headers, rows};
        }
    """)


def paginate_all(page, tab_label: str) -> tuple:
    all_rows    = []
    headers     = []
    seen_hashes = set()
    page_num    = 1

    while page_num <= 500:
        time.sleep(1.5)
        data = extract_table(page)

        if data["headers"]:
            headers = data["headers"]
        if not data["rows"]:
            log(f"  Page {page_num}: no rows - done")
            break

        new_count = 0
        for row in data["rows"]:
            h = "|".join(row)
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            all_rows.append(row)
            new_count += 1

        log(f"  Page {page_num}: +{new_count} rows (total: {len(all_rows)})")

        if new_count == 0:
            break

        clicked = False
        for sel in [
            'button[aria-label="Go to next page"]',
            'button[aria-label="Next page"]',
            'button[title="Next page"]',
            'button[data-testid*="next" i]',
        ]:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=1000):
                    if (btn.get_attribute("disabled") is not None or
                            btn.get_attribute("aria-disabled") == "true"):
                        log(f"  Last page reached")
                        return headers, all_rows
                    btn.click()
                    clicked = True
                    page_num += 1
                    time.sleep(2.5)
                    break
            except Exception:
                continue

        if not clicked:
            break

    log(f"  {tab_label}: {len(all_rows)} total rows")
    return headers, all_rows


def rows_to_dicts(
    headers: list, rows: list, tab: str, period: str
) -> list:
    result = []
    for r in rows:
        d = {h: (r[i] if i < len(r) else "") for i, h in enumerate(headers)}
        d["__tab__"]         = tab
        d["__period__"]      = period
        d["__scraped_at__"]  = SCRAPE_TS
        d["__scrape_date__"] = SCRAPE_DATE
        result.append(d)
    return result


TABS = {
    "FREE":         "FREE",
    "FIRST UPLOAD": "FIRST_UPLOAD",
    "PAID":         "PAID",
}


def main():
    log("=" * 60)
    log("KPI SCRAPER - FULL HISTORICAL + DAILY")
    log(f"Time: {SCRAPE_TS}")
    log("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(viewport={"width": 1600, "height": 1000})
        page    = context.new_page()

        try:
            login(page)
            period = set_longest_period(page)
            summary = {}

            for tab_label, tab_key in TABS.items():
                log(f"\n{'─'*50}")
                log(f"Tab: {tab_label}")

                if not click_tab(page, tab_label):
                    summary[tab_key] = 0
                    continue

                headers, raw_rows = paginate_all(page, tab_label)
                if not raw_rows:
                    summary[tab_key] = 0
                    continue

                enriched = rows_to_dicts(headers, raw_rows, tab_label, period)
                log(f"{tab_label}: {len(enriched)} rows -> writing to Sheets...")

                # PRIMARY: Sheets. FALLBACK: CSV (inside write_tab_data)
                ok = write_tab_data(f"Raw_{tab_key}", enriched)
                log(
                    f"{tab_label}: Sheets={'OK' if ok else 'FAILED->CSV'}"
                )
                summary[tab_key] = len(enriched)

            log(f"\n{'='*60}")
            log("KPI SCRAPE COMPLETE")
            for tab, count in summary.items():
                log(f"  {tab:20s}: {count} rows")

            write_run_summary({
                "run_at": SCRAPE_TS,
                "stage": "kpi_scrape",
                "period": period,
                **{f"raw_{k}": v for k, v in summary.items()},
            })

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
