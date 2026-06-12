"""
LinkedIn Connector — Eagle 3D KPI System v7.2
===============================================
3 methods to get LinkedIn data:
1. Public page scrape (urllib, no auth) — works on Streamlit Cloud
2. Authenticated Playwright scrape (pipeline only)
3. Manual entry + CSV import (dashboard UI)

NO Playwright required for basic operation.
"""

import os
import json
import re
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

LI_METRICS_CACHE = DATA_DIR / "linkedin_metrics.json"
LI_POSTS_CACHE = DATA_DIR / "linkedin_posts.json"
LI_DAILY_CACHE = DATA_DIR / "linkedin_daily.json"
LI_FOLLOWERS_CACHE = DATA_DIR / "linkedin_followers.json"
LI_COOKIES_PATH = DATA_DIR / "linkedin_cookies.json"


def _get_secret(key: str, default: str = "") -> str:
    val = os.environ.get(key, "")
    if val:
        return val
    try:
        import streamlit as st
        if key in st.secrets:
            val = st.secrets[key]
            if val is not None and str(val).strip():
                return str(val).strip()
    except Exception:
        pass
    return default


def _load_json(path: Path, max_age_hours: int = 6) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        ts = data.get("_cached_at", "")
        if ts:
            cached_time = datetime.fromisoformat(ts)
            if datetime.now() - cached_time < timedelta(hours=max_age_hours):
                return data
    except Exception:
        pass
    return None


def _save_json(path: Path, data: dict):
    data["_cached_at"] = datetime.now().isoformat()
    path.write_text(json.dumps(data, default=str, indent=2))


def _get_cookies() -> str:
    val = _get_secret("LINKEDIN_COOKIES_JSON", "")
    if val:
        return val
    if LI_COOKIES_PATH.exists():
        return LI_COOKIES_PATH.read_text()
    return ""


def _get_company_page() -> str:
    return _get_secret("LINKEDIN_COMPANY_PAGE", "")


# ═══════════════════════════════════════════════════════════════
# METHOD 1: Public Page Scraping (Works everywhere, no Playwright)
# ═══════════════════════════════════════════════════════════════

def scrape_public_metrics() -> Dict[str, Any]:
    """Scrape public metrics from LinkedIn company page using urllib."""
    import urllib.request
    import urllib.error

    company_page = _get_company_page()
    if not company_page:
        return {"error": "LINKEDIN_COMPANY_PAGE not configured", "demo": True}

    if not company_page.startswith("http"):
        company_page = f"https://www.linkedin.com/company/{company_page.strip('/')}/"

    try:
        req = urllib.request.Request(
            company_page,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        result = _extract_public_data(html, company_page)
        _save_json(LI_METRICS_CACHE, result)
        return result

    except Exception as e:
        print(f"[LinkedIn] Public scrape error: {e}")
        return {"error": str(e), "demo": True}


def _extract_public_data(html: str, url: str) -> Dict[str, Any]:
    """Extract metrics from LinkedIn public page HTML."""
    result = {"company_page": url, "scraped_at": datetime.now().isoformat(), "demo": False}

    # Extract follower count
    for pattern in [
        r'"followers":(\d+)', r'(\d[\d,]+)\s*followers',
        r'followerCount["\s:]+(\d+)', r'"totalFollowers":(\d+)',
        r'"standardizedFollowerCount":(\d+)',
    ]:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            result["followers"] = int(m.group(1).replace(",", ""))
            break

    # Company name
    for pattern in [
        r'<title>([^<]+?)(?:\s*\|\s*LinkedIn|\s*-\s*LinkedIn)',
        r'"name"\s*:\s*"([^"]+)"', r'"companyName"\s*:\s*"([^"]+)"',
    ]:
        m = re.search(pattern, html)
        if m:
            result["company_name"] = m.group(1).strip()
            break

    # Employee count
    for pattern in [r'(\d[\d,+]*)\s*(?:employees|associates)', r'"employeeCountRange"[^}]*?"end"\s*:\s*(\d+)']:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            result["employees"] = m.group(1).replace(",", "").replace("+", "")
            break

    # Industry
    ind_match = re.search(r'"industry"\s*:\s*"([^"]+)"', html)
    if ind_match:
        result["industry"] = ind_match.group(1)

    # Description
    desc_match = re.search(r'"description"\s*:\s*"([^"]{10,500})"', html)
    if desc_match:
        result["description"] = desc_match.group(1)

    return result


# ═══════════════════════════════════════════════════════════════
# METHOD 2: LinkedIn API (via OAuth access token if available)
# ═══════════════════════════════════════════════════════════════

def scrape_via_api() -> Dict[str, Any]:
    """Use LinkedIn API with OAuth token if configured."""
    import urllib.request
    token = _get_secret("LINKEDIN_ACCESS_TOKEN", "")
    if not token:
        return {"error": "No LinkedIn access token", "demo": True}

    result = {"scraped_at": datetime.now().isoformat(), "demo": False}

    try:
        # Get organization info
        req = urllib.request.Request(
            "https://api.linkedin.com/rest/organizations?q=vanityName&vanityName=eagle-3d-streaming",
            headers={"Authorization": f"Bearer {token}", "LinkedIn-Version": "202401", "X-Restli-Protocol-Version": "2.0.0"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data.get("elements"):
                org = data["elements"][0]
                result["followers"] = org.get("followerCount", 0)
                result["company_name"] = org.get("localizedName", "")
                result["company_id"] = org.get("id", "")
    except Exception as e:
        result["api_error"] = str(e)

    _save_json(LI_METRICS_CACHE, result)
    return result


# ═══════════════════════════════════════════════════════════════
# METHOD 3: Authenticated Playwright Scrape (Pipeline only)
# ═══════════════════════════════════════════════════════════════

def scrape_with_playwright(historical: bool = False) -> Dict[str, Any]:
    """Scrape LinkedIn analytics using Playwright. Pipeline only."""
    cookies_json = _get_cookies()
    if not cookies_json:
        return {"error": "No LinkedIn cookies configured", "demo": True}

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return scrape_public_metrics()  # Fallback to public

    company_page = _get_company_page()
    if not company_page:
        return {"error": "LINKEDIN_COMPANY_PAGE not configured", "demo": True}

    try:
        cookies = json.loads(cookies_json) if isinstance(cookies_json, str) else cookies_json
    except json.JSONDecodeError:
        return scrape_public_metrics()

    result = {"scraped_at": datetime.now().isoformat(), "demo": False}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", viewport={"width": 1920, "height": 1080})

            if isinstance(cookies, list):
                formatted = [{"name": c.get("name", ""), "value": c.get("value", ""), "domain": c.get("domain", ".linkedin.com"), "path": c.get("path", "/")} for c in cookies]
                context.add_cookies(formatted)

            page = context.new_page()

            try:
                analytics_url = company_page.rstrip("/") + "/admin/analytics/"
                page.goto(analytics_url, wait_until="networkidle", timeout=30000)
                time.sleep(3)
                analytics = _extract_analytics_from_page(page.content())
                result.update(analytics)
            except Exception as e:
                print(f"[LinkedIn] Analytics scrape failed: {e}")

            try:
                follower_url = company_page.rstrip("/") + "/admin/followers/"
                page.goto(follower_url, wait_until="networkidle", timeout=30000)
                time.sleep(2)
                followers = _extract_followers_from_page(page.content())
                result.update(followers)
            except Exception as e:
                print(f"[LinkedIn] Follower scrape failed: {e}")

            browser.close()
    except Exception as e:
        result["error"] = str(e)

    _save_json(LI_METRICS_CACHE, result)
    return result


def _extract_analytics_from_page(html: str) -> Dict[str, Any]:
    result = {}
    for pattern in [r'"impressionCount"\s*:\s*(\d+)', r'"uniqueVisitors"\s*:\s*(\d+)', r'"totalPageViews"\s*:\s*(\d+)']:
        m = re.search(pattern, html)
        if m:
            key = pattern.split('"')[1]
            result[key] = int(m.group(1))
    return result


def _extract_followers_from_page(html: str) -> Dict[str, Any]:
    result = {}
    for pattern in [r'"totalFollowers"\s*:\s*(\d+)', r'"followerCount"\s*:\s*(\d+)', r'"organicFollowers"\s*:\s*(\d+)']:
        m = re.search(pattern, html)
        if m:
            key = pattern.split('"')[1]
            result[key] = int(m.group(1))
    return result


# ═══════════════════════════════════════════════════════════════
# MANUAL DATA INPUT
# ═══════════════════════════════════════════════════════════════

def save_manual_entry(data: Dict[str, Any]) -> bool:
    try:
        existing = {}
        if LI_DAILY_CACHE.exists():
            existing = json.loads(LI_DAILY_CACHE.read_text())
        entries = existing.get("entries", [])
        entries.append({"date": datetime.now().strftime("%Y-%m-%d"), "scraped_at": datetime.now().isoformat(), **data})
        existing["entries"] = entries[-365:]
        _save_json(LI_DAILY_CACHE, existing)
        return True
    except Exception as e:
        print(f"[LinkedIn] Save failed: {e}")
        return False


def get_manual_history() -> pd.DataFrame:
    if not LI_DAILY_CACHE.exists():
        return pd.DataFrame()
    try:
        data = json.loads(LI_DAILY_CACHE.read_text())
        entries = data.get("entries", [])
        if not entries:
            return pd.DataFrame()
        df = pd.DataFrame(entries)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date")
        return df
    except Exception:
        return pd.DataFrame()


def import_csv_data(csv_content: str) -> bool:
    import io
    try:
        df = pd.read_csv(io.StringIO(csv_content))
        if df.empty:
            return False
        existing = {}
        if LI_DAILY_CACHE.exists():
            existing = json.loads(LI_DAILY_CACHE.read_text())
        entries = existing.get("entries", [])
        for _, row in df.iterrows():
            entry = row.to_dict()
            for k, v in entry.items():
                if pd.isna(v):
                    entry[k] = None
            entries.append(entry)
        existing["entries"] = entries[-365:]
        _save_json(LI_DAILY_CACHE, existing)
        return True
    except Exception as e:
        print(f"[LinkedIn] CSV import failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# COMBINED / STATUS
# ═══════════════════════════════════════════════════════════════

def get_cached_metrics() -> Dict[str, Any]:
    cache = _load_json(LI_METRICS_CACHE, max_age_hours=24)
    if cache:
        return {k: v for k, v in cache.items() if k != "_cached_at"}
    return {}


def is_configured() -> bool:
    return bool(_get_company_page())


def has_cookies() -> bool:
    return bool(_get_cookies())


def get_status() -> Dict[str, Any]:
    return {
        "company_page": bool(_get_company_page()),
        "cookies": bool(_get_cookies()),
        "public_scrape": bool(_get_company_page()),
        "authenticated_scrape": bool(_get_cookies() and _get_company_page()),
        "configured": bool(_get_company_page()),
        "cached_data": LI_METRICS_CACHE.exists(),
    }
