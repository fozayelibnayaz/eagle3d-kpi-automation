#!/usr/bin/env python3
"""LinkedIn Post Scraper - ALL pages with pagination + date range selection."""
import os
import json
import time
import re
import hashlib
from datetime import datetime, date, timedelta
from pathlib import Path

DATA_DIR = Path("data_output")
COMPANY_ID = "68624141"
UPDATES_URL = f"https://www.linkedin.com/company/{COMPANY_ID}/admin/analytics/updates/"


def log(m):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [LI] {m}", flush=True)


def _get_sb():
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        try:
            import streamlit as st
            try: url = str(st.secrets["SUPABASE_URL"]).strip()
            except: pass
            try: key = str(st.secrets["SUPABASE_SERVICE_KEY"]).strip()
            except: pass
        except: pass
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def _set_date_range(page):
    """Click the date range picker in Content engagement section and set Last 365 days."""
    try:
        # There are TWO date sections on LinkedIn analytics
        # We need the one in "Content engagement" table area (bottom)
        # Strategy: find ALL date picker buttons and click the SECOND one
        # (first is highlights, second is content engagement table)

        date_btns = []
        for sel in ["button:has-text('Last')", "button:has-text('days')",
                    "button:has-text('2026')", "button:has-text('2025')",
                    "button:has-text('Jun ')", "button:has-text('May ')"]:
            btns = page.query_selector_all(sel)
            for btn in btns:
                txt = (btn.inner_text() or "").strip()
                if any(m in txt for m in ["2025","2026","days","Last","Past","Jun","May","Apr","Mar"]):
                    date_btns.append(btn)

        log(f"  Found {len(date_btns)} date picker buttons")

        # Click EACH date button and set to Last 365 days
        for idx, btn in enumerate(date_btns):
            try:
                btn.scroll_into_view_if_needed()
                time.sleep(1)
                btn.click()
                time.sleep(2)
                log(f"  Clicked date picker {idx+1}")

                # Select Last 365 days
                for opt in ["Last 365 days", "Last year", "Past year", "Past 365 days"]:
                    el = page.query_selector(
                        f"button:has-text('{opt}'), "
                        f"li:has-text('{opt}'), "
                        f"[role='option']:has-text('{opt}'), "
                        f"[role='menuitem']:has-text('{opt}')"
                    )
                    if el:
                        el.click()
                        time.sleep(4)
                        log(f"  Selected: {opt}")
                        break
            except Exception as e:
                log(f"  Date picker {idx+1} error: {e}")
                continue

    except Exception as e:
        log(f"  Date range error: {e}")


def _extract_posts(page):
    """Extract posts from current page table."""
    posts = []
    try:
        rows = page.query_selector_all("tr")
        for row in rows:
            try:
                cells = row.query_selector_all("td")
                if len(cells) < 4:
                    continue
                texts = [c.inner_text().strip() for c in cells]
                if any(h in texts[0] for h in ["Post title","Impressions","Title"]):
                    continue

                def si(s):
                    try: return int(re.sub(r"[^\d]", "", str(s)) or "0")
                    except: return 0

                def sf(s):
                    try: return float(re.sub(r"[^\d.]", "", str(s)) or "0")
                    except: return 0.0

                # Find date in first cell text
                pub_date = None
                full = texts[0]
                # Pattern: "6/22/2026" or "M/D/YYYY"
                dm = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", full)
                if dm:
                    try:
                        pub_date = datetime.strptime(dm.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                    except: pass

                post = {
                    "title":           texts[0][:500],
                    "post_type":       texts[1] if len(texts) > 1 else "",
                    "audience":        texts[2] if len(texts) > 2 else "",
                    "impressions":     si(texts[3]) if len(texts) > 3 else 0,
                    "views":           si(texts[4]) if len(texts) > 4 else 0,
                    "clicks":          si(texts[5]) if len(texts) > 5 else 0,
                    "ctr":             sf(texts[6]) if len(texts) > 6 else 0,
                    "reactions":       si(texts[7]) if len(texts) > 7 else 0,
                    "comments":        si(texts[8]) if len(texts) > 8 else 0,
                    "reposts":         si(texts[9]) if len(texts) > 9 else 0,
                    "follows":         si(texts[10]) if len(texts) > 10 else 0,
                    "engagement_rate": sf(texts[11]) if len(texts) > 11 else 0,
                    "published_at":    pub_date,
                }
                if post["impressions"] > 0 or post["reactions"] > 0:
                    posts.append(post)
            except: continue
    except Exception as e:
        log(f"  Extract error: {e}")
    return posts


def scrape_all_posts():
    """Scrape ALL LinkedIn posts across ALL pages."""
    from playwright.sync_api import sync_playwright

    session_file = DATA_DIR / "linkedin_session_state.json"
    cookies_file = DATA_DIR / "linkedin_cookies.json"

    if not session_file.exists() and not cookies_file.exists():
        log("No session/cookies")
        return []

    all_posts = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"]
        )
        ctx_args = {
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "viewport": {"width": 1440, "height": 900},
        }
        if session_file.exists():
            ctx_args["storage_state"] = str(session_file)
        context = browser.new_context(**ctx_args)

        if not session_file.exists() and cookies_file.exists():
            try:
                cookies = json.loads(cookies_file.read_text())
                pw_cookies = []
                for c in cookies:
                    try:
                        domain = c.get("domain",".linkedin.com")
                        if not domain.startswith("."): domain = "." + domain
                        pw_c = {"name":c["name"],"value":c["value"],"domain":domain,
                                "path":c.get("path","/"),"secure":c.get("secure",True),
                                "httpOnly":c.get("httpOnly",False)}
                        exp = c.get("expirationDate")
                        if exp and not c.get("session"): pw_c["expires"] = int(exp)
                        pw_cookies.append(pw_c)
                    except: continue
                context.add_cookies(pw_cookies)
            except: pass

        page = context.new_page()
        page.goto(UPDATES_URL, timeout=45000, wait_until="domcontentloaded")
        time.sleep(5)

        if "login" in page.url.lower():
            log("LOGIN REQUIRED")
            browser.close()
            return []

        log("Page loaded. Setting date range...")
        _set_date_range(page)
        time.sleep(3)

        # Paginate through ALL pages
        page_num = 1
        while True:
            log(f"Scraping page {page_num}...")
            time.sleep(3)
            posts = _extract_posts(page)
            log(f"  Page {page_num}: {len(posts)} posts")
            all_posts.extend(posts)

            # Click Next
            try:
                next_btn = page.query_selector(
                    "button:has-text('Next'), "
                    "button[aria-label='Next'], "
                    "[aria-label='Next page'], "
                    "a:has-text('Next')"
                )
                if next_btn and next_btn.is_visible() and next_btn.is_enabled():
                    next_btn.scroll_into_view_if_needed()
                    time.sleep(1)
                    next_btn.click()
                    time.sleep(4)
                    page_num += 1
                else:
                    log("  No more pages")
                    break
            except Exception as e:
                log(f"  Next error: {e}")
                break

            if page_num > 15:
                log("  Safety limit")
                break

        browser.close()

    log(f"Total: {len(all_posts)} posts from {page_num} pages")
    return all_posts


def save_to_supabase(posts):
    sb = _get_sb()
    if not sb:
        log("Supabase not configured")
        return 0
    rows = []
    for p in posts:
        title = p.get("title","")[:200]
        pub = p.get("published_at") or ""
        urn = "li::" + hashlib.sha1((title + pub).encode()).hexdigest()[:16]
        rows.append({
            "urn": urn,
            "title": p["title"][:500],
            "post_type": p.get("post_type","")[:50],
            "audience": p.get("audience","")[:100],
            "published_at": (pub + "T00:00:00+00:00") if pub else None,
            "impressions": p.get("impressions",0),
            "views": p.get("views",0),
            "clicks": p.get("clicks",0),
            "ctr": p.get("ctr",0),
            "reactions": p.get("reactions",0),
            "comments": p.get("comments",0),
            "reposts": p.get("reposts",0),
            "follows": p.get("follows",0),
            "engagement_rate": p.get("engagement_rate",0),
            "last_updated": datetime.utcnow().isoformat(),
        })
    errors = 0
    for i in range(0, len(rows), 20):
        try:
            sb.table("linkedin_posts").upsert(rows[i:i+20], on_conflict="urn").execute()
        except Exception as e:
            errors += 1
            if errors <= 3: log(f"Upsert error: {e}")
    log(f"Saved {len(rows)} posts, {errors} errors")
    return len(rows)


def run():
    log("=" * 60)
    log("LINKEDIN ALL POSTS SCRAPER")
    log("=" * 60)
    posts = scrape_all_posts()
    if posts:
        saved = save_to_supabase(posts)
        cache = {"scraped_at": datetime.utcnow().isoformat(), "posts": posts, "count": len(posts)}
        (DATA_DIR / "linkedin_all_posts.json").write_text(json.dumps(cache, indent=2, default=str))
        return saved
    return 0


if __name__ == "__main__":
    count = run()
    print(f"\nTotal: {count} posts saved")
