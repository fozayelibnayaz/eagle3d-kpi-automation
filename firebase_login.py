"""
firebase_login.py
Gets KPI Dashboard session using Playwright form login.
Saves browser storage state (works with Firebase localStorage auth).
Credentials from environment variables ONLY - never hardcoded.

Usage:
    export KPI_EMAIL="..."
    export KPI_PASSWORD="..."
    python3 firebase_login.py
"""
import json
import os
import re
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

KPI_URL       = "https://kpidashboard.eagle3dstreaming.com/"
OUTPUT        = Path("kpi_cookies.json")
STORAGE_STATE = Path("kpi_storage_state.json")


def log(msg):
    print(f"[firebase] {msg}", flush=True)


def main():
    log("=" * 50)
    log("KPI LOGIN")
    log("=" * 50)

    email    = os.environ.get("KPI_EMAIL", "")
    password = os.environ.get("KPI_PASSWORD", "")

    if not email or not password:
        log("ERROR: KPI_EMAIL and KPI_PASSWORD must be set as environment variables")
        return False

    log(f"Email: {email}")
    log(f"Password: SET ({len(password)} chars)")

    id_token    = None
    all_cookies = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx  = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = ctx.new_page()

        # Capture Firebase token from responses
        def on_response(resp):
            nonlocal id_token
            if "identitytoolkit" in resp.url and not id_token:
                try:
                    body = resp.json()
                    if "idToken" in body:
                        id_token = body["idToken"]
                        log("Firebase token captured from network")
                except Exception:
                    pass

        page.on("response", on_response)

        # Load login page
        log(f"Loading {KPI_URL}")
        page.goto(KPI_URL, wait_until="domcontentloaded", timeout=90000)
        time.sleep(3)

        log(f"URL: {page.url}")
        log(f"Title: {page.title()}")

        # Fill login form
        if page.locator('input[type="password"]').count() > 0:
            log("Filling login form...")

            for sel in ['input[type="email"]', 'input[type="text"]',
                        'input[placeholder*="mail" i]']:
                if page.locator(sel).count() > 0:
                    page.locator(sel).first.fill(email)
                    log(f"  Email filled")
                    break

            page.locator('input[type="password"]').first.fill(password)
            log("  Password filled")

            for btn in ['button[type="submit"]', 'button:has-text("SIGN IN")',
                        'button:has-text("Sign In")', 'button:has-text("Login")']:
                if page.locator(btn).count() > 0:
                    page.locator(btn).first.click()
                    log("  Form submitted")
                    break

            # Wait for auth to complete
            log("Waiting for auth...")
            time.sleep(8)

            log(f"URL after login: {page.url}")
        else:
            log("No login form - may already be authenticated")

        # Check if logged in
        still_on_login = page.locator('input[type="password"]').count() > 0
        body = page.inner_text("body")[:200]
        log(f"On login page: {still_on_login}")
        log(f"Page content: {body[:100]}")

        # Save full storage state (includes localStorage with Firebase token)
        ctx.storage_state(path=str(STORAGE_STATE))
        log(f"Storage state saved to {STORAGE_STATE}")

        # Also get regular cookies
        all_cookies = ctx.cookies()
        log(f"Cookies found: {len(all_cookies)}")
        for c in all_cookies:
            log(f"  {c['name']:30s} {c['domain']}")

        # Screenshot for debugging
        try:
            page.screenshot(path="data_output/login_result.png")
        except Exception:
            pass

        browser.close()

    # Build kpi_cookies.json from what we have
    # Priority: storage state localStorage > cookies > id_token
    storage = json.load(open(STORAGE_STATE))
    storage_cookies  = storage.get("cookies", [])
    storage_origins  = storage.get("origins", [])

    log(f"\nStorage state: {len(storage_cookies)} cookies, {len(storage_origins)} origins")

    # Extract Firebase token from localStorage in storage state
    firebase_token = id_token  # from network intercept

    for origin in storage_origins:
        for item in origin.get("localStorage", []):
            k = item.get("name", "")
            v = item.get("value", "")
            log(f"  localStorage[{k[:50]}] = {str(v)[:60]}")

            # Firebase stores user data in keys like:
            # firebase:authUser:API_KEY:[DEFAULT]
            if "firebase:authUser" in k or "firebaseAuth" in k.lower():
                try:
                    user_data = json.loads(v)
                    # Try to get token from nested structure
                    sts = user_data.get("stsTokenManager", {})
                    t   = (sts.get("accessToken") or
                           sts.get("idToken") or
                           user_data.get("idToken") or
                           user_data.get("accessToken"))
                    if t:
                        firebase_token = t
                        log(f"  Firebase token from localStorage: {t[:40]}...")
                except Exception:
                    if len(v) > 100 and v.startswith("eyJ"):
                        firebase_token = v
                        log(f"  Raw JWT from localStorage")

    # Build final cookie data
    # Use storage state cookies + firebase token
    final_cookies = []

    # Add all storage state cookies
    for c in storage_cookies:
        final_cookies.append(c)

    # Add firebase token as __session if not already present
    if firebase_token:
        has_session = any(c.get("name") == "__session" for c in final_cookies)
        if not has_session:
            final_cookies.append({
                "name":     "__session",
                "value":    firebase_token,
                "domain":   ".eagle3dstreaming.com",
                "path":     "/",
                "secure":   True,
                "httpOnly": False,
                "sameSite": "no_restriction",
            })

    # Save
    with open(OUTPUT, "w") as f:
        json.dump(final_cookies, f, indent=2)

    log(f"\nSaved {len(final_cookies)} items to {OUTPUT}")
    log(f"Firebase token: {'YES' if firebase_token else 'NO'}")

    # Now test if scraping works WITH the storage state
    log("\nTesting with storage state...")
    ok = test_with_storage_state()
    return ok


def test_with_storage_state() -> bool:
    """Test dashboard access using saved storage state."""
    if not STORAGE_STATE.exists():
        return False

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        # Load storage state into context
        ctx  = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            storage_state=str(STORAGE_STATE),
        )
        page = ctx.new_page()
        page.goto(KPI_URL, wait_until="domcontentloaded", timeout=90000)
        time.sleep(5)

        on_login = page.locator('input[type="password"]').count() > 0
        body     = page.inner_text("body")[:300]

        log(f"URL: {page.url}")
        log(f"On login: {on_login}")
        log(f"Content: {body[:150]}")

        try:
            page.screenshot(path="data_output/storage_state_test.png")
        except Exception:
            pass

        browser.close()

        if not on_login:
            log("SUCCESS: Storage state works!")
            return True
        else:
            log("FAILED: Storage state did not authenticate")
            return False


if __name__ == "__main__":
    ok = main()
    print(f"\nResult: {'SUCCESS' if ok else 'FAILED'}")
