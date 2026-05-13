"""
scrape_kpi.py
Scrapes https://kpidashboard.eagle3dstreaming.com/
Gets ALL historical data by selecting longest time period.
Paginates ALL pages.
Writes to Google Sheets (primary). CSV fallback if Sheets fails.
"""
import os
import time
import json
import csv
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

# ── Get credentials from config ──
# Try multiple possible variable names
def get_kpi_credentials():
    import config as cfg
    
    # KPI URL
    url = None
    for name in ["KPI_URL", "KPIDASHBOARD_URL", "DASHBOARD_URL"]:
        if hasattr(cfg, name):
            url = getattr(cfg, name)
            break
    if not url:
        url = "https://kpidashboard.eagle3dstreaming.com/"
    
    # Username
    username = None
    for name in ["KPI_USERNAME", "KPI_USER", "DASHBOARD_USERNAME", 
                 "DASHBOARD_USER", "USERNAME", "LOGIN_USER"]:
        if hasattr(cfg, name):
            username = getattr(cfg, name)
            break
    
    # Password  
    password = None
    for name in ["KPI_PASSWORD", "KPI_PASS", "DASHBOARD_PASSWORD",
                 "DASHBOARD_PASS", "PASSWORD", "LOGIN_PASS", "LOGIN_PASSWORD"]:
        if hasattr(cfg, name):
            password = getattr(cfg, name)
            break

    # Also try environment variables
    if not username:
        username = os.environ.get("KPI_USERNAME", os.environ.get("KPI_USER", ""))
    if not password:
        password = os.environ.get("KPI_PASSWORD", os.environ.get("KPI_PASS", ""))

    return url, username, password


KPI_URL, KPI_USERNAME, KPI_PASSWORD = get_kpi_credentials()

from sheets_writer import write_tab_data, write_run_summary

DATA_DIR    = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)
SCRAPE_TS   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
SCRAPE_DATE = datetime.now().strftime("%Y-%m-%d")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [KPI] {msg}", flush=True)


def login(page):
    log(f"Loading: {KPI_URL}")
    log(f"Username: {KPI_USERNAME}")
    log(f"Password: {'SET' if KPI_PASSWORD else 'MISSING'}")
    
    page.goto(KPI_URL, wait_until="domcontentloaded", timeout=90000)
    time.sleep(4)
    
    page.screenshot(path=str(DATA_DIR/"debug_01_loaded.png"))
    log(f"URL after load: {page.url}")
    log(f"Title: {page.title()}")

    # Check for login form
    has_pass = page.locator('input[type="password"]').count() > 0
    has_user = page.locator('input[type="text"], input[type="email"]').count() > 0
    log(f"Login form: username_field={has_user}, password_field={has_pass}")

    if has_pass and KPI_USERNAME and KPI_PASSWORD:
        log("Filling login form...")
        try:
            # Fill username
            for sel in ['input[type="text"]', 'input[type="email"]',
                        'input[name*="user" i]', 'input[name*="email" i]',
                        'input[id*="user" i]', 'input[id*="email" i]']:
                if page.locator(sel).count() > 0:
                    page.locator(sel).first.fill(KPI_USERNAME)
                    log(f"  Username filled via: {sel}")
                    break

            # Fill password
            page.locator('input[type="password"]').first.fill(KPI_PASSWORD)
            log("  Password filled")

            page.screenshot(path=str(DATA_DIR/"debug_02_filled.png"))

            # Submit
            submitted = False
            for btn_sel in [
                'button[type="submit"]',
                'button:has-text("Login")',
                'button:has-text("Sign In")',
                'button:has-text("Sign in")',
                'button:has-text("Log In")',
                'input[type="submit"]',
            ]:
                if page.locator(btn_sel).count() > 0:
                    page.locator(btn_sel).first.click()
                    log(f"  Submitted via: {btn_sel}")
                    submitted = True
                    break

            if not submitted:
                page.keyboard.press("Enter")
                log("  Submitted via Enter key")

            time.sleep(6)
            page.screenshot(path=str(DATA_DIR/"debug_03_after_login.png"))
            log(f"URL after login: {page.url}")
            log(f"Title after login: {page.title()}")

        except Exception as e:
            log(f"Login error: {e}")
    elif not KPI_USERNAME or not KPI_PASSWORD:
        log("WARNING: No credentials found - trying to access without login")
    else:
        log("No login form - may already be logged in")

    try:
        page.wait_for_load_state("networkidle", timeout=60000)
    except Exception:
        pass
    time.sleep(5)
    
    page.screenshot(path=str(DATA_DIR/"debug_04_dashboard.png"))
    log(f"Final URL: {page.url}")
    log(f"Final title: {page.title()}")


def find_and_set_max_period(page) -> str:
    """Set time period to maximum available."""
    WANT = [
        "All Time", "All time", "Lifetime", "All",
        "Last 12 Months", "Last 6 Months", "Last 3 Months",
    ]

    log("Looking for time period selector...")

    # Get all clickable elements
    clickables = page.evaluate("""
        () => {
            const results = [];
            const sels = ['button','select','[role="combobox"]',
                          '[class*="select" i]','[class*="dropdown" i]'];
            for (const sel of sels) {
                for (const el of document.querySelectorAll(sel)) {
                    const txt = (el.innerText || el.value || '').trim();
                    if (txt && txt.length > 1 && txt.length < 80) {
                        results.push({tag: el.tagName, text: txt});
                    }
                }
            }
            return results;
        }
    """)

    log(f"Clickable elements found: {len(clickables)}")
    for item in clickables[:20]:
        log(f"  [{item['tag']}] {item['text']}")

    # Try clicking period-related elements
    period_keywords = ["month","time","period","last","all","lifetime","filter","date range"]
    
    for item in clickables:
        txt = item['text'].lower()
        if any(kw in txt for kw in period_keywords):
            log(f"Trying period element: '{item['text']}'")
            try:
                page.get_by_text(item['text'], exact=True).first.click()
                time.sleep(2)
                
                # Look for dropdown options
                for want in WANT:
                    opts = page.locator(
                        f'li:has-text("{want}"), [role="option"]:has-text("{want}")'
                    )
                    if opts.count() > 0 and opts.first.is_visible():
                        opts.first.click()
                        time.sleep(5)
                        log(f"Period set to: {want}")
                        page.screenshot(path=str(DATA_DIR/"debug_05_period_set.png"))
                        return want
                
                page.keyboard.press("Escape")
                time.sleep(1)
            except Exception as e:
                log(f"  Error: {e}")

    log("Could not change period - using default")
    return "default"


def click_tab(page, tab_name: str) -> bool:
    """Click a dashboard tab."""
    selectors = [
        f'button:has-text("{tab_name}")',
        f'[role="tab"]:has-text("{tab_name}")',
        f'div[role="button"]:has-text("{tab_name}")',
        f'a:has-text("{tab_name}")',
        f'span:has-text("{tab_name}")',
        f'li:has-text("{tab_name}")',
    ]
    for sel in selectors:
        try:
            els = page.locator(sel)
            if els.count() > 0 and els.first.is_visible(timeout=2000):
                els.first.click()
                time.sleep(4)
                page.screenshot(path=str(
                    DATA_DIR/f"debug_tab_{tab_name.replace(' ','_')}.png"
                ))
                log(f"Clicked tab '{tab_name}' via: {sel}")
                return True
        except Exception:
            continue
    
    log(f"Tab '{tab_name}' not found with any selector")
    return False


def extract_all_table_data(page) -> dict:
    """Extract all data from the current table."""
    return page.evaluate("""
        () => {
            const candidates = [
                ...document.querySelectorAll('[role="grid"]'),
                ...document.querySelectorAll('[class*="MuiDataGrid"]'),
                ...document.querySelectorAll('[class*="datagrid" i]'),
                ...document.querySelectorAll('table'),
                ...document.querySelectorAll('[role="table"]'),
            ];

            let best = null, maxRows = 0;
            for (const c of candidates) {
                const r = c.querySelectorAll('[role="row"],tr').length;
                if (r > maxRows) { maxRows = r; best = c; }
            }

            if (!best) {
                return {headers:[], rows:[], page_total:-1, error:"no table found"};
            }

            // Headers
            const hEls = best.querySelectorAll('[role="columnheader"],th');
            const headers = Array.from(hEls)
                .map(h => h.innerText.trim())
                .filter(h => h && h.length > 0 && h.length < 100);

            // Rows
            const rowEls = best.querySelectorAll('[role="row"],tr');
            const rows = [];
            for (const r of rowEls) {
                const cells = r.querySelectorAll(
                    '[role="cell"],[role="gridcell"],td'
                );
                if (!cells.length) continue;
                const vals = Array.from(cells).map(c => c.innerText.trim());
                if (vals.every(v => !v)) continue;
                if (headers.length > 0 && vals[0] === headers[0]) continue;
                if (vals.filter(v=>v.length>0).length < 2) continue;
                rows.push(vals);
            }

            // Try to read total count
            let page_total = -1;
            const paginationEls = document.querySelectorAll(
                '[class*="pagination" i],[class*="rowCount" i],' +
                '[class*="footer" i],[aria-label*="rows" i]'
            );
            for (const el of paginationEls) {
                const txt = el.innerText || '';
                const m = txt.match(/(\\d[\\d,]*)\\s*(?:–|of|\\/)\\s*(\\d[\\d,]*)/);
                if (m) {
                    page_total = parseInt(m[2].replace(/,/g,''));
                    break;
                }
                const m2 = txt.match(/(\\d[\\d,]+)\\s+rows/i);
                if (m2) {
                    page_total = parseInt(m2[1].replace(/,/g,''));
                    break;
                }
            }

            return {headers, rows, page_total};
        }
    """)


def paginate_all(page, tab_label: str) -> tuple:
    """Go through all pages and collect every row."""
    all_rows    = []
    headers     = []
    seen_hashes = set()
    page_num    = 1

    # Try to maximize rows per page
    for rpp_sel in [
        '[aria-label*="rows per page" i]',
        'select:near([class*="pagination" i])',
    ]:
        try:
            el = page.locator(rpp_sel).first
            if el.is_visible(timeout=2000):
                options = el.evaluate(
                    "el => Array.from(el.options).map(o=>({v:o.value,t:o.text}))"
                )
                nums = [
                    o for o in options
                    if o['v'].isdigit() or o['t'].isdigit()
                ]
                if nums:
                    biggest = sorted(nums, key=lambda x: int(x['v'] or x['t']))[-1]
                    el.select_option(biggest['v'])
                    time.sleep(3)
                    log(f"Rows per page: {biggest['v']}")
                break
        except Exception:
            continue

    while page_num <= 500:
        time.sleep(2)
        data = extract_all_table_data(page)

        if data.get("headers"):
            headers = data["headers"]

        rows = data.get("rows", [])
        if not rows:
            log(f"  p{page_num}: no rows")
            break

        new_count = 0
        for row in rows:
            h = "|".join(str(v) for v in row)
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            all_rows.append(row)
            new_count += 1

        total_hint = data.get("page_total", -1)
        hint_str   = f" (~{total_hint} total)" if total_hint > 0 else ""
        log(
            f"  p{page_num}: +{new_count} rows"
            f" | running total: {len(all_rows)}{hint_str}"
        )

        if new_count == 0:
            log("  No new rows - done")
            break

        # Find and click next page
        clicked = False
        for sel in [
            'button[aria-label="Go to next page"]',
            'button[aria-label="Next page"]',
            'button[title="Next page"]',
            'button[title="Go to next page"]',
            'button:has([data-testid="NavigateNextIcon"])',
            'button:has([data-testid="KeyboardArrowRightIcon"])',
            '[aria-label="next"]:not([disabled])',
        ]:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=800):
                    cls      = btn.get_attribute("class") or ""
                    disabled = (
                        btn.get_attribute("disabled") is not None
                        or btn.get_attribute("aria-disabled") == "true"
                        or "disabled" in cls.lower()
                        or "Mui-disabled" in cls
                    )
                    if disabled:
                        log(f"  Last page ({page_num})")
                        return headers, all_rows
                    btn.click()
                    clicked  = True
                    page_num += 1
                    time.sleep(3)
                    break
            except Exception:
                continue

        if not clicked:
            log(f"  No next button at page {page_num}")
            break

    log(f"{tab_label}: {len(all_rows)} total rows, {len(headers)} columns")
    return headers, all_rows


def rows_to_dicts(headers, raw_rows, tab, period):
    result = []
    for r in raw_rows:
        d = {headers[i]: r[i] if i < len(r) else "" for i in range(len(headers))}
        d["__tab__"]         = tab
        d["__period__"]      = period
        d["__scraped_at__"]  = SCRAPE_TS
        d["__scrape_date__"] = SCRAPE_DATE
        result.append(d)
    return result


TABS = {
    "FREE":         "FREE",
    "FIRST UPLOAD": "FIRST_UPLOAD",
}


def main():
    log("="*60)
    log("KPI SCRAPER")
    log(f"URL:      {KPI_URL}")
    log(f"Username: {KPI_USERNAME}")
    log(f"Password: {'SET' if KPI_PASSWORD else 'MISSING - WILL FAIL'}")
    log(f"Time:     {SCRAPE_TS}")
    log("="*60)

    if not KPI_PASSWORD:
        log("ERROR: No KPI password found in config.py or environment")
        log("Add KPI_PASSWORD to config.py")
        return

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
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        try:
            login(page)
            period = find_and_set_max_period(page)

            summary = {}
            for tab_label, tab_key in TABS.items():
                log(f"\n{'─'*50}")
                log(f"TAB: {tab_label}")

                if not click_tab(page, tab_label):
                    summary[tab_key] = 0
                    continue

                headers, raw_rows = paginate_all(page, tab_label)
                if not raw_rows:
                    log(f"NO DATA for {tab_label}")
                    summary[tab_key] = 0
                    continue

                enriched = rows_to_dicts(headers, raw_rows, tab_label, period)
                log(f"{tab_label}: writing {len(enriched)} rows to Sheets...")

                ok = write_tab_data(f"Raw_{tab_key}", enriched)
                log(f"Sheets write: {'OK' if ok else 'FAILED/CSV fallback'}")
                summary[tab_key] = len(enriched)

            log(f"\n{'='*60}")
            log("SCRAPE SUMMARY")
            log(f"{'='*60}")
            total = 0
            for tab, count in summary.items():
                log(f"  {tab:20s}: {count} rows")
                total += count
            log(f"  TOTAL: {total} rows")

            if total == 0:
                log("WARNING: 0 rows scraped - check screenshots in data_output/")

            write_run_summary({
                "run_at": SCRAPE_TS,
                "stage": "kpi_scrape",
                "period": period,
                "rows_free": summary.get("FREE",0),
                "rows_upload": summary.get("FIRST_UPLOAD",0),
                "total": total,
            })

        except Exception as e:
            log(f"FATAL: {e}")
            import traceback
            traceback.print_exc()
            try:
                page.screenshot(path=str(DATA_DIR/"debug_FATAL.png"))
            except Exception:
                pass
            raise
        finally:
            try:
                browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
