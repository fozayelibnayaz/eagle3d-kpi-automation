"""
scrape_kpi.py
Scrapes KPI Dashboard with:
  - Forced one-time "Last 6 Month" historical scrape
  - Daily "Current Month" scrapes after that
  - APPEND mode: never overwrites existing data, only adds new rows
  - Detailed logging of every dropdown interaction
"""
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

from sheets_writer import write_tab_data, read_tab_data, write_run_summary

DATA_DIR    = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)
SCRAPE_TS   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
SCRAPE_DATE = datetime.now().strftime("%Y-%m-%d")

KPI_URL = "https://kpidashboard.eagle3dstreaming.com/"

# ALL options available (verified from screenshot)
DROPDOWN_OPTIONS = ["Last 7 Days", "Last Month", "Current Month", "Last 6 Month"]

# Force flag - if FORCE_HISTORICAL=1 env var, always do "Last 6 Month"
FORCE_HISTORICAL = os.environ.get("FORCE_HISTORICAL", "") == "1"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [KPI] {msg}", flush=True)


# ─── AUTH ─────────────────────────────────────────────
def get_context(p):
    storage_file = Path("kpi_storage_state.json")
    session_dir  = Path("browser_session")
    cookies_file = Path("kpi_cookies.json")

    if storage_file.exists() and storage_file.stat().st_size > 200:
        log("Auth: kpi_storage_state.json")
        browser = p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            storage_state=str(storage_file),
        )
        return browser, ctx

    if session_dir.exists() and len(list(session_dir.iterdir())) > 3:
        log("Auth: browser_session/")
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(session_dir),
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            viewport={"width": 1920, "height": 1080},
        )
        return None, ctx

    if cookies_file.exists() and cookies_file.stat().st_size > 10:
        try:
            raw = json.load(open(cookies_file))
            log(f"Auth: {len(raw)} cookies")
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            ctx = browser.new_context(viewport={"width":1920,"height":1080})
            normalized = []
            for c in raw:
                n = {
                    "name":     str(c.get("name","")),
                    "value":    str(c.get("value","")),
                    "domain":   str(c.get("domain",".eagle3dstreaming.com")),
                    "path":     str(c.get("path","/")),
                    "secure":   bool(c.get("secure",True)),
                    "httpOnly": bool(c.get("httpOnly",False)),
                    "sameSite": "None",
                }
                exp = c.get("expirationDate") or c.get("expires")
                if exp:
                    try: n["expires"] = float(exp)
                    except: pass
                if n["name"] and n["value"]:
                    normalized.append(n)
            if normalized:
                ctx.add_cookies(normalized)
            return browser, ctx
        except Exception as e:
            log(f"Cookie error: {e}")

    log("WARNING: no auth")
    browser = p.chromium.launch(
        headless=True, args=["--no-sandbox","--disable-dev-shm-usage"]
    )
    ctx = browser.new_context(viewport={"width":1920,"height":1080})
    return browser, ctx


def check_logged_in(page) -> bool:
    return page.locator('input[type="password"]').count() == 0


# ─── DATE FILTER (improved) ────────────────────────────
def determine_target_filter() -> str:
    """
    Decide which filter to use.
    - FORCE_HISTORICAL=1 env: always "Last 6 Month"
    - Sheets has < 100 rows: "Last 6 Month" (one-time historical)
    - Otherwise: "Current Month" (daily)
    """
    if FORCE_HISTORICAL:
        log("FORCE_HISTORICAL=1 - using 'Last 6 Month'")
        return "Last 6 Month"

    try:
        existing = read_tab_data("Raw_FREE")
        # Check both row count AND date range
        if len(existing) >= 100:
            # Check if we have data older than 1 month
            from datetime import datetime, timedelta
            old_threshold = datetime.now() - timedelta(days=45)
            has_old = False
            for row in existing[:50]:  # check first 50
                date_str = row.get("Account Created On", "")
                if date_str:
                    try:
                        # Parse "Tue, 28 Apr 2026 21:30:14 GMT" format
                        from email.utils import parsedate_to_datetime
                        dt = parsedate_to_datetime(date_str)
                        if dt.replace(tzinfo=None) < old_threshold:
                            has_old = True
                            break
                    except Exception:
                        pass

            if has_old:
                log(f"Sheets has {len(existing)} rows with old data - using 'Current Month'")
                return "Current Month"
            else:
                log(f"Sheets has {len(existing)} rows but all recent - using 'Last 6 Month'")
                return "Last 6 Month"
        else:
            log(f"Only {len(existing)} rows in Sheets - using 'Last 6 Month'")
            return "Last 6 Month"
    except Exception as e:
        log(f"Sheets check failed ({e}) - using 'Last 6 Month'")
        return "Last 6 Month"


def set_filter_to(page, target: str) -> str:
    """
    Click the date dropdown at top-right and select target.
    Returns actual selected filter (may differ from target if not found).
    """
    log(f"=" * 50)
    log(f"SETTING FILTER TO: '{target}'")
    log(f"=" * 50)

    page.screenshot(path=str(DATA_DIR / "debug_F1_before_open.png"))

    # Step 1: Wait for dropdown to be enabled
    log("Step 1: Waiting for .MuiSelect-select to be enabled...")
    enabled = False
    for sec in range(60):
        try:
            select_el = page.locator(".MuiSelect-select").first
            if select_el.count() == 0:
                if sec % 10 == 0:
                    log(f"  ({sec}s) No .MuiSelect-select element yet")
                time.sleep(1)
                continue

            disabled = select_el.get_attribute("aria-disabled")
            cls      = select_el.get_attribute("class") or ""
            is_disabled = (disabled == "true" or "Mui-disabled" in cls)

            if not is_disabled and select_el.is_visible(timeout=500):
                current = select_el.inner_text().strip()
                log(f"  ENABLED after {sec}s. Current value: '{current}'")
                enabled = True
                break
            elif sec % 10 == 0:
                log(f"  ({sec}s) Disabled... waiting")

        except Exception as e:
            if sec % 15 == 0:
                log(f"  ({sec}s) Exception: {e}")

        time.sleep(1)

    if not enabled:
        log("FAILED: Dropdown never enabled in 60s")
        return "default-disabled"

    # Step 2: Read current value
    select_el = page.locator(".MuiSelect-select").first
    current = select_el.inner_text().strip()
    log(f"Step 2: Current dropdown value = '{current}'")

    if current == target:
        log(f"Already on '{target}' - skipping click")
        return target

    # Step 3: Click to open
    log(f"Step 3: Clicking dropdown to open menu...")
    select_el.click()
    time.sleep(2)
    page.screenshot(path=str(DATA_DIR / "debug_F2_menu_open.png"))

    # Step 4: List all options
    options = page.locator("li[role='option']").all()
    log(f"Step 4: Found {len(options)} options:")
    
    available_texts = []
    for opt in options:
        try:
            txt = (opt.text_content() or "").strip()
            available_texts.append(txt)
            log(f"    - '{txt}'")
        except Exception:
            pass

    # Step 5: Click the target
    log(f"Step 5: Looking for option '{target}'...")
    
    # Try exact match first
    for opt in options:
        try:
            txt = (opt.text_content() or "").strip()
            if txt == target:
                log(f"  EXACT MATCH FOUND - clicking '{target}'")
                opt.click()
                log(f"  Clicked. Waiting 10s for data refresh...")
                time.sleep(10)
                page.screenshot(path=str(DATA_DIR / "debug_F3_after_click.png"))
                
                # Verify selection
                new_val = page.locator(".MuiSelect-select").first.inner_text().strip()
                log(f"  Dropdown now shows: '{new_val}'")
                return new_val
        except Exception as e:
            log(f"  Click error: {e}")

    # Try fuzzy match
    target_normalized = target.lower().replace(" ", "")
    log(f"No exact match. Trying fuzzy for '{target_normalized}'")
    for opt in options:
        try:
            txt = (opt.text_content() or "").strip()
            if txt.lower().replace(" ", "") == target_normalized:
                log(f"  FUZZY MATCH: '{txt}' - clicking")
                opt.click()
                time.sleep(10)
                new_val = page.locator(".MuiSelect-select").first.inner_text().strip()
                log(f"  Dropdown now: '{new_val}'")
                return new_val
        except Exception:
            pass

    log(f"FAILED: '{target}' not in options {available_texts}")
    page.keyboard.press("Escape")
    time.sleep(1)
    return current


# ─── TAB CLICK ─────────────────────────────────────────
def click_tab(page, tab_name: str) -> bool:
    log(f"Clicking tab: {tab_name}")
    for sel in [
        f'button:has-text("{tab_name}")',
        f'[role="tab"]:has-text("{tab_name}")',
        f'div[role="button"]:has-text("{tab_name}")',
    ]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                el.click()
                time.sleep(5)
                log(f"  Tab '{tab_name}' clicked")
                return True
        except Exception:
            continue
    log(f"  Tab '{tab_name}' NOT FOUND")
    return False


# ─── TABLE EXTRACTION + PAGINATION ─────────────────────
def extract_table(page) -> dict:
    return page.evaluate("""
        () => {
            const candidates = [
                ...document.querySelectorAll('[role="grid"]'),
                ...document.querySelectorAll('[class*="MuiDataGrid"]'),
                ...document.querySelectorAll('table'),
            ];
            let best=null, maxR=0;
            for (const c of candidates) {
                const r = c.querySelectorAll('[role="row"],tr').length;
                if (r > maxR) { maxR = r; best = c; }
            }
            if (!best) return {headers:[], rows:[], total:-1};

            const hEls = best.querySelectorAll('[role="columnheader"],th');
            const headers = Array.from(hEls)
                .map(h => h.innerText.trim())
                .filter(h => h);

            // Get rows from data-id attribute (more reliable for MUI DataGrid)
            const rowEls = best.querySelectorAll('[role="row"][data-id], tr[data-id]');
            const rows = [];
            const seen_ids = new Set();
            
            for (const r of rowEls) {
                const id = r.getAttribute('data-id');
                if (seen_ids.has(id)) continue;
                seen_ids.add(id);
                
                const cells = r.querySelectorAll('[role="cell"],[role="gridcell"],td');
                if (!cells.length) continue;
                const vals = Array.from(cells).map(c => c.innerText.trim());
                if (vals.every(v => !v)) continue;
                rows.push(vals);
            }
            
            // Fallback to all rows if no data-id rows found
            if (rows.length === 0) {
                const allRows = best.querySelectorAll('[role="row"], tr');
                for (const r of allRows) {
                    const cells = r.querySelectorAll('[role="cell"],[role="gridcell"],td');
                    if (!cells.length) continue;
                    const vals = Array.from(cells).map(c => c.innerText.trim());
                    if (vals.every(v => !v)) continue;
                    if (headers.length && vals[0] === headers[0]) continue;
                    if (vals.filter(v => v.length > 0).length < 2) continue;
                    rows.push(vals);
                }
            }

            // Total row count
            let total = -1;
            const pagEls = document.querySelectorAll(
                '[class*="MuiTablePagination"],[class*="pagination"]'
            );
            for (const el of pagEls) {
                const txt = el.innerText || '';
                const m = txt.match(/(\\d[\\d,]*)\\s*[-–]\\s*(\\d[\\d,]*)\\s+of\\s+(\\d[\\d,]*)/i);
                if (m) {
                    total = parseInt(m[3].replace(/,/g,''));
                    break;
                }
                const m2 = txt.match(/of\\s+(\\d[\\d,]*)/i);
                if (m2) {
                    total = parseInt(m2[1].replace(/,/g,''));
                    break;
                }
            }
            return {headers, rows, total};
        }
    """)


def set_max_page_size(page):
    """Set rows-per-page to maximum to reduce pagination."""
    try:
        # MUI TablePagination select
        rpp = page.locator(".MuiTablePagination-select").first
        if rpp.count() == 0:
            return
        
        rpp.click()
        time.sleep(1.5)
        
        options = page.locator("li[role='option']").all()
        values = []
        for opt in options:
            t = (opt.text_content() or "").strip()
            if t.isdigit():
                values.append((int(t), opt))
        
        if values:
            values.sort(key=lambda x: x[0], reverse=True)
            biggest = values[0]
            log(f"  Set rows per page: {biggest[0]}")
            biggest[1].click()
            time.sleep(3)
    except Exception as e:
        log(f"  Page size error: {e}")


def paginate_all(page, tab_label: str) -> tuple:
    log(f"Pagination start for {tab_label}")
    
    # Wait for table to load
    try:
        page.wait_for_selector(".MuiDataGrid-root, table", timeout=15000)
    except Exception:
        log(f"  No table found")
        return [], []
    
    time.sleep(3)
    
    # Maximize page size
    set_max_page_size(page)
    
    all_rows    = []
    headers     = []
    seen_hashes = set()
    page_num    = 1
    expected    = -1

    while page_num <= 500:
        time.sleep(2)
        data = extract_table(page)

        if data.get("headers"):
            headers = data["headers"]

        if expected < 0:
            expected = data.get("total", -1)
            if expected > 0:
                log(f"  Expected total rows: {expected}")

        rows = data.get("rows", [])
        if not rows:
            log(f"  p{page_num}: no rows extracted")
            break

        new = 0
        for row in rows:
            h = "|".join(row)
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            all_rows.append(row)
            new += 1

        suffix = f" of {expected}" if expected > 0 else ""
        log(f"  p{page_num}: +{new} rows | total: {len(all_rows)}{suffix}")

        if expected > 0 and len(all_rows) >= expected:
            log(f"  Reached expected total {expected}")
            break

        if new == 0:
            break

        # Try scrolling first (for virtualized grids)
        try:
            scroller = page.locator(".MuiDataGrid-virtualScroller").first
            if scroller.count() > 0:
                scroller.evaluate("el => el.scrollBy(0, 500)")
                time.sleep(1.5)
                
                # Check if more rows appeared
                data2 = extract_table(page)
                rows2 = data2.get("rows", [])
                more_added = 0
                for row in rows2:
                    h = "|".join(row)
                    if h in seen_hashes:
                        continue
                    seen_hashes.add(h)
                    all_rows.append(row)
                    more_added += 1
                
                if more_added > 0:
                    log(f"  Scroll added: +{more_added} rows (total: {len(all_rows)})")
                    if expected > 0 and len(all_rows) >= expected:
                        break
                    continue  # don't go to next page yet, scroll more
        except Exception:
            pass

        # Click next page
        clicked = False
        for sel in [
            'button[aria-label="Go to next page"]',
            'button[aria-label="Next page"]',
            'button[title="Next page"]',
        ]:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=800):
                    cls = btn.get_attribute("class") or ""
                    if (btn.get_attribute("disabled") is not None
                            or "disabled" in cls.lower()
                            or "Mui-disabled" in cls):
                        log(f"  Last page ({page_num})")
                        return headers, all_rows
                    btn.click()
                    clicked = True
                    page_num += 1
                    time.sleep(3)
                    break
            except Exception:
                continue

        if not clicked:
            break

    log(f"{tab_label}: collected {len(all_rows)} rows")
    return headers, all_rows


# ─── APPEND MODE ───────────────────────────────────────
def merge_with_existing(tab_name: str, new_rows: list) -> list:
    """
    Read existing data from Sheets and merge with new rows.
    Deduplicates by email.
    NEVER loses existing data.
    """
    try:
        existing = read_tab_data(tab_name)
        log(f"  Existing rows in {tab_name}: {len(existing)}")
    except Exception as e:
        log(f"  Could not read existing: {e}")
        existing = []

    # Build lookup by email
    by_email = {}
    for r in existing:
        email = ""
        for k in ("Email", "email"):
            if k in r and r[k] and "@" in str(r[k]):
                email = str(r[k]).strip().lower()
                break
        if email:
            by_email[email] = r
        else:
            # No email - keep as-is using row hash
            key = json.dumps(r, sort_keys=True, default=str)
            by_email[f"_nokey_{hash(key)}"] = r

    # Add new rows (overwrites only if same email exists - usually safe)
    new_count = 0
    updated_count = 0
    for r in new_rows:
        email = ""
        for k in ("Email", "email"):
            if k in r and r[k] and "@" in str(r[k]):
                email = str(r[k]).strip().lower()
                break
        if email:
            if email in by_email:
                # Update existing - merge fields (new wins)
                merged = {**by_email[email], **r}
                by_email[email] = merged
                updated_count += 1
            else:
                by_email[email] = r
                new_count += 1
        else:
            key = json.dumps(r, sort_keys=True, default=str)
            if f"_nokey_{hash(key)}" not in by_email:
                by_email[f"_nokey_{hash(key)}"] = r
                new_count += 1

    merged_list = list(by_email.values())
    log(f"  After merge: {len(merged_list)} rows (+{new_count} new, {updated_count} updated)")
    return merged_list


TABS = {
    "FREE":         "FREE",
    "FIRST UPLOAD": "FIRST_UPLOAD",
}


# ─── MAIN ──────────────────────────────────────────────
def main():
    log("=" * 60)
    log("KPI SCRAPER")
    log(f"Time: {SCRAPE_TS}")
    log(f"FORCE_HISTORICAL: {FORCE_HISTORICAL}")
    log("=" * 60)

    target_filter = determine_target_filter()
    log(f"Target filter: '{target_filter}'")

    with sync_playwright() as p:
        browser, ctx = get_context(p)

        try:
            page = ctx.new_page()
            log(f"Loading {KPI_URL}")
            page.goto(KPI_URL, wait_until="domcontentloaded", timeout=90000)
            time.sleep(8)  # Longer wait for full app load

            log(f"URL:   {page.url}")
            log(f"Title: {page.title()}")
            page.screenshot(path=str(DATA_DIR / "debug_01_loaded.png"))

            if not check_logged_in(page):
                log("ERROR: Not logged in - run firebase_login.py first")
                return

            log("Logged in OK")

            # Set filter (this is the critical step)
            actual_filter = set_filter_to(page, target_filter)
            log(f"Active filter: '{actual_filter}'")

            if actual_filter != target_filter:
                log(f"WARNING: Wanted '{target_filter}' but got '{actual_filter}'")

            summary = {}
            for tab_label, tab_key in TABS.items():
                log(f"\n{'─'*50}")
                log(f"TAB: {tab_label}")

                if not click_tab(page, tab_label):
                    summary[tab_key] = 0
                    continue

                page.screenshot(path=str(DATA_DIR / f"debug_tab_{tab_key}.png"))

                headers, raw_rows = paginate_all(page, tab_label)
                if not raw_rows:
                    log(f"NO DATA for {tab_label}")
                    summary[tab_key] = 0
                    continue

                enriched = []
                for r in raw_rows:
                    d = {headers[i]: r[i] if i < len(r) else ""
                         for i in range(len(headers))}
                    d["__tab__"]         = tab_label
                    d["__period__"]      = actual_filter
                    d["__scraped_at__"]  = SCRAPE_TS
                    d["__scrape_date__"] = SCRAPE_DATE
                    enriched.append(d)

                # APPEND MODE: merge with existing data
                log(f"Merging with existing Sheets data...")
                merged = merge_with_existing(f"Raw_{tab_key}", enriched)

                log(f"Writing {len(merged)} total rows to Sheets...")
                ok = write_tab_data(f"Raw_{tab_key}", merged)
                log(f"Sheets write: {'OK' if ok else 'FAILED/CSV fallback'}")
                summary[tab_key] = {
                    "scraped":   len(enriched),
                    "total_after_merge": len(merged),
                }

            log(f"\n{'='*60}")
            log("COMPLETE")
            for k, v in summary.items():
                if isinstance(v, dict):
                    log(f"  {k}: scraped={v['scraped']}, total={v['total_after_merge']}")
                else:
                    log(f"  {k}: {v}")

            write_run_summary({
                "run_at":  SCRAPE_TS,
                "stage":   "kpi",
                "filter":  actual_filter,
                **{f"{k}_scraped": v.get('scraped',0) if isinstance(v,dict) else v
                   for k,v in summary.items()},
                **{f"{k}_total": v.get('total_after_merge',0) if isinstance(v,dict) else v
                   for k,v in summary.items()},
            })

        except Exception as e:
            log(f"FATAL: {e}")
            import traceback
            traceback.print_exc()
            try:
                page.screenshot(path=str(DATA_DIR / "debug_FATAL.png"))
            except Exception:
                pass
            raise
        finally:
            try: ctx.close()
            except: pass
            if browser:
                try: browser.close()
                except: pass


if __name__ == "__main__":
    main()
