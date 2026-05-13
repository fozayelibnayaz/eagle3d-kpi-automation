"""
STRIPE SCRAPER v4 - FULL HISTORICAL
Loads Stripe customers (with subscription = Yes), scrolls/paginates through ALL,
returns ALL paid customer history.
"""
import os
import json
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

from config import STRIPE_URL, STRIPE_SESSION_DIR
from sheets_writer import write_tab_data

SCRAPE_TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Stripe] {msg}", flush=True)


def normalize_cookies(raw_cookies):
    out = []
    for c in raw_cookies:
        cookie = {
            "name": c.get("name"),
            "value": c.get("value"),
            "domain": c.get("domain"),
            "path": c.get("path", "/"),
        }
        if "expirationDate" in c:
            cookie["expires"] = float(c["expirationDate"])
        if "httpOnly" in c:
            cookie["httpOnly"] = bool(c["httpOnly"])
        if "secure" in c:
            cookie["secure"] = bool(c["secure"])
        ss = str(c.get("sameSite", "no_restriction")).lower()
        cookie["sameSite"] = {
            "no_restriction": "None", "lax": "Lax", "strict": "Strict",
            "unspecified": "Lax", "none": "None"
        }.get(ss, "Lax")
        if cookie["name"] and cookie["value"] is not None:
            out.append(cookie)
    return out


def extract_customers(page):
    """Extract customer rows from current view."""
    try:
        data = page.evaluate("""
            () => {
                // Find the data table
                const tables = document.querySelectorAll('table, [role="table"], [role="grid"]');
                let bestTable = null;
                let maxRows = 0;
                for (const t of tables) {
                    const r = t.querySelectorAll('tr, [role="row"]').length;
                    if (r > maxRows) { maxRows = r; bestTable = t; }
                }
                if (!bestTable) return {headers: [], rows: []};

                // Headers
                const headerCells = bestTable.querySelectorAll('th, [role="columnheader"]');
                const headers = Array.from(headerCells).map(h => h.innerText.trim()).filter(h => h);

                // Rows
                const rowEls = bestTable.querySelectorAll('tbody tr, [role="row"]');
                const rows = [];
                for (const r of rowEls) {
                    const cells = r.querySelectorAll('td, [role="cell"], [role="gridcell"]');
                    if (cells.length === 0) continue;
                    const vals = Array.from(cells).map(c => c.innerText.trim());
                    // Skip empty / checkbox-only rows
                    const meaningful = vals.filter(v => v && v.length > 1).length;
                    if (meaningful < 2) continue;
                    rows.push(vals);
                }

                return {headers, rows};
            }
        """)
        return data
    except Exception as e:
        log(f"extract error: {e}")
        return {"headers": [], "rows": []}


def click_load_more(page):
    """Try to click pagination 'next' or 'load more' button."""
    selectors = [
        'button:has-text("Next")',
        'button[aria-label*="next" i]',
        'button:has-text("Load more")',
        'button:has-text("Show more")',
        'button[data-test*="next" i]',
        'button[data-testid*="next" i]',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=1500) and not btn.is_disabled():
                btn.click()
                time.sleep(2)
                return True
        except Exception:
            continue
    return False


def main():
    log("=" * 60)
    log("STRIPE SCRAPER v4 - FULL HISTORICAL")
    log("=" * 60)

    cookies_file = Path("stripe_cookies.json")
    has_session = STRIPE_SESSION_DIR.exists() and any(STRIPE_SESSION_DIR.iterdir()) if STRIPE_SESSION_DIR.exists() else False
    has_cookies = cookies_file.exists() and cookies_file.stat().st_size > 0

    if not has_session and not has_cookies:
        log("WARNING: No Stripe session or cookies. Skipping Stripe scrape.")
        return

    with sync_playwright() as p:
        STRIPE_SESSION_DIR.mkdir(parents=True, exist_ok=True)
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(STRIPE_SESSION_DIR),
            headless=True,
            viewport={"width": 1600, "height": 1000},
            args=["--no-sandbox"],
        )

        # Inject cookies
        if has_cookies:
            try:
                raw = json.loads(cookies_file.read_text())
                normalized = normalize_cookies(raw)
                ctx.add_cookies(normalized)
                log(f"Injected {len(normalized)} cookies")
            except Exception as e:
                log(f"Cookie injection failed: {e}")

        page = ctx.new_page()

        try:
            log(f"Loading {STRIPE_URL}")
            page.goto(STRIPE_URL, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            # Check if logged in (look for "Customers" header or login screen)
            page_content = page.content()[:5000].lower()
            if "log in" in page_content or "sign in" in page_content:
                if "stripe" in page.url and "login" in page.url:
                    log("ERROR: Stripe session expired - need fresh cookies")
                    return

            log("Stripe loaded")
            page.wait_for_load_state("networkidle", timeout=30000)
            time.sleep(3)

            # Collect all rows across all pages
            all_rows = []
            headers = []
            seen_emails = set()
            page_num = 1
            max_pages = 100

            while page_num <= max_pages:
                # Scroll to bottom to trigger lazy load
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)

                data = extract_customers(page)
                if data["headers"]:
                    headers = data["headers"]
                if not data["rows"]:
                    log(f"  Page {page_num}: no rows extracted")
                    break

                # Dedup by email-like field
                new_count = 0
                for row in data["rows"]:
                    email_val = None
                    for v in row:
                        if isinstance(v, str) and "@" in v and "." in v:
                            email_val = v.strip().lower()
                            break
                    if email_val and email_val in seen_emails:
                        continue
                    if email_val:
                        seen_emails.add(email_val)
                    all_rows.append(row)
                    new_count += 1

                log(f"  Page {page_num}: +{new_count} new rows (total: {len(all_rows)})")

                if new_count == 0:
                    log(f"  No new rows - done")
                    break

                # Try to go next page
                if not click_load_more(page):
                    log(f"  No more pages")
                    break
                page_num += 1
                time.sleep(2)

            # Build enriched rows
            enriched = []
            for row in all_rows:
                d = {}
                for i, h in enumerate(headers):
                    d[h] = row[i] if i < len(row) else ""
                # Make sure we have an Email field even if header was different
                if "Email" not in d and "email" not in d:
                    for v in row:
                        if isinstance(v, str) and "@" in v and "." in v:
                            d["Email"] = v
                            break
                d["__scraped_at__"] = SCRAPE_TS
                enriched.append(d)

            log(f"Total Stripe customers: {len(enriched)}")
            if enriched:
                write_tab_data("STRIPE", enriched)

        except Exception as e:
            log(f"FATAL: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            try:
                ctx.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
