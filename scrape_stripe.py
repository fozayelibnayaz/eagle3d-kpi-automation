"""
scrape_stripe.py
Scrapes ALL Stripe paid customers.
Writes to Google Sheets (primary). CSV only if Sheets fails.
"""
import json
import time
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

from config import STRIPE_SESSION_DIR
from sheets_writer import write_tab_data

DATA_DIR    = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)
SCRAPE_TS   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
SCRAPE_DATE = datetime.now().strftime("%Y-%m-%d")
STRIPE_URL  = (
    "https://dashboard.stripe.com/acct_1J7M5XIKrnGFhGm1"
    "/customers?has_subscription=true"
)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Stripe] {msg}", flush=True)


def normalize_cookies(raw):
    out = []
    for c in raw:
        cookie = {
            "name":   c.get("name"),
            "value":  c.get("value"),
            "domain": c.get("domain"),
            "path":   c.get("path", "/"),
        }
        if "expirationDate" in c:
            cookie["expires"] = float(c["expirationDate"])
        if "httpOnly" in c:
            cookie["httpOnly"] = bool(c["httpOnly"])
        if "secure" in c:
            cookie["secure"] = bool(c["secure"])
        ss_val = str(c.get("sameSite", "no_restriction")).lower()
        cookie["sameSite"] = {
            "no_restriction": "None", "lax": "Lax",
            "strict": "Strict", "unspecified": "Lax", "none": "None",
        }.get(ss_val, "Lax")
        if cookie["name"] and cookie["value"] is not None:
            out.append(cookie)
    return out


def extract_table(page) -> dict:
    return page.evaluate("""
        () => {
            const tables = [
                ...document.querySelectorAll('table'),
                ...document.querySelectorAll('[role="grid"]'),
                ...document.querySelectorAll('[role="table"]'),
            ];
            let best = null, maxR = 0;
            for (const t of tables) {
                const r = t.querySelectorAll('tr,[role="row"]').length;
                if (r > maxR) { maxR = r; best = t; }
            }
            if (!best) return {headers:[], rows:[]};

            const hEls = best.querySelectorAll('th,[role="columnheader"]');
            const headers = Array.from(hEls)
                .map(h => h.innerText.trim()).filter(h => h);

            const rowEls = best.querySelectorAll('tbody tr,[role="row"]');
            const rows = [];
            for (const r of rowEls) {
                const cells = r.querySelectorAll(
                    'td,[role="cell"],[role="gridcell"]'
                );
                if (!cells.length) continue;
                const vals = Array.from(cells).map(c => c.innerText.trim());
                if (vals.filter(v => v && v.length > 1).length < 2) continue;
                rows.push(vals);
            }
            return {headers, rows};
        }
    """)


def main():
    log("=" * 60)
    log("STRIPE SCRAPER - FULL HISTORY")
    log(f"Time: {SCRAPE_TS}")
    log("=" * 60)

    cookies_file = Path("stripe_cookies.json")
    has_session  = (
        STRIPE_SESSION_DIR.exists()
        and any(STRIPE_SESSION_DIR.iterdir())
    ) if STRIPE_SESSION_DIR.exists() else False
    has_cookies  = cookies_file.exists() and cookies_file.stat().st_size > 100

    if not has_session and not has_cookies:
        log("No Stripe session or cookies - skipping")
        return

    with sync_playwright() as p:
        STRIPE_SESSION_DIR.mkdir(parents=True, exist_ok=True)
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(STRIPE_SESSION_DIR),
            headless=True,
            viewport={"width": 1600, "height": 1000},
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        if has_cookies:
            try:
                raw  = json.loads(cookies_file.read_text())
                norm = normalize_cookies(raw)
                ctx.add_cookies(norm)
                log(f"Injected {len(norm)} cookies")
            except Exception as e:
                log(f"Cookie injection error: {e}")

        page = ctx.new_page()

        try:
            log(f"Loading Stripe...")
            page.goto(
                STRIPE_URL, wait_until="domcontentloaded", timeout=90000
            )
            time.sleep(6)

            content = page.content().lower()
            if "log in" in content or "sign in" in content:
                if "login" in page.url.lower():
                    log("ERROR: Session expired - need fresh cookies")
                    return

            page.wait_for_load_state("networkidle", timeout=30000)
            time.sleep(3)
            log("Stripe loaded")

            all_rows    = []
            headers     = []
            seen_hashes = set()
            page_num    = 1

            while page_num <= 200:
                page.evaluate(
                    "window.scrollTo(0, document.body.scrollHeight)"
                )
                time.sleep(2)

                data = extract_table(page)
                if data["headers"]:
                    headers = data["headers"]
                if not data["rows"]:
                    log(f"Page {page_num}: no rows")
                    break

                new_count = 0
                for row in data["rows"]:
                    h = "|".join(row)
                    if h in seen_hashes:
                        continue
                    seen_hashes.add(h)
                    all_rows.append(row)
                    new_count += 1

                log(f"Page {page_num}: +{new_count} (total: {len(all_rows)})")

                if new_count == 0:
                    break

                clicked = False
                for sel in [
                    'button:has-text("Next")',
                    'button[aria-label*="next" i]',
                    'button[data-testid*="next" i]',
                ]:
                    try:
                        btn = page.locator(sel).first
                        if btn.is_visible(timeout=1500):
                            if not btn.is_disabled():
                                btn.click()
                                clicked = True
                                page_num += 1
                                time.sleep(3)
                                break
                    except Exception:
                        continue
                if not clicked:
                    break

            # Build enriched rows
            enriched = []
            for row in all_rows:
                d = {
                    headers[i]: row[i] if i < len(row) else ""
                    for i, _ in enumerate(headers)
                }
                if "Email" not in d:
                    for v in row:
                        if isinstance(v, str) and "@" in v:
                            d["Email"] = v
                            break
                d["__scraped_at__"]  = SCRAPE_TS
                d["__scrape_date__"] = SCRAPE_DATE
                enriched.append(d)

            log(f"Total: {len(enriched)} Stripe customers")

            if enriched:
                # PRIMARY: Sheets. FALLBACK: CSV
                ok = write_tab_data("Raw_STRIPE", enriched)
                log(f"Sheets write: {'OK' if ok else 'FAILED -> CSV fallback used'}")

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
