"""
LinkedIn Connector — Eagle 3D KPI System
==========================================
Scrapes LinkedIn company page analytics via Playwright since
the LinkedIn Marketing/Analytics API requires app approval.

Strategy:
1. Historical: Full scrape of all available metrics from company analytics page
2. Daily: Incremental daily scrape via GitHub Actions (cookies-based auth)
3. Fallback: Manual CSV upload via dashboard UI

Data stored in data_output/linkedin_*.json files.

Required secrets:
  LINKEDIN_COOKIES_JSON — Exported cookies from browser (JSON array of cookie objects)
  LINKEDIN_COMPANY_PAGE — LinkedIn company page URL (e.g., https://www.linkedin.com/company/eagle3d/)
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

# ── Cache/data paths ──
LI_METRICS_CACHE = DATA_DIR / "linkedin_metrics.json"
LI_POSTS_CACHE = DATA_DIR / "linkedin_posts.json"
LI_DAILY_CACHE = DATA_DIR / "linkedin_daily.json"
LI_FOLLOWERS_CACHE = DATA_DIR / "linkedin_followers.json"
LI_COOKIES_PATH = DATA_DIR / "linkedin_cookies.json"


def _get_secret(key: str, default: str = "") -> str:
    """Get secret from env, then Streamlit secrets."""
    val = os.environ.get(key, "")
    if val:
        return val
    try:
        import streamlit as st
        val = st.secrets.get(key, "")
        if isinstance(val, str) and val:
            return val
    except Exception:
        pass
    return default


def _load_json(path: Path, max_age_hours: int = 6) -> Optional[dict]:
    """Load JSON cache if fresh."""
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
    """Save JSON with timestamp."""
    data["_cached_at"] = datetime.now().isoformat()
    path.write_text(json.dumps(data, default=str, indent=2))


def _get_cookies() -> str:
    """Get LinkedIn cookies JSON string."""
    val = _get_secret("LINKEDIN_COOKIES_JSON", "")
    if val:
        return val
    # Check file
    if LI_COOKIES_PATH.exists():
        return LI_COOKIES_PATH.read_text()
    return ""


def _get_company_page() -> str:
    """Get LinkedIn company page URL."""
    return _get_secret("LINKEDIN_COMPANY_PAGE", "")


# ═══════════════════════════════════════════════════════════════
# METHOD 1: LinkedIn Public Page Scraping (No auth required)
# ═══════════════════════════════════════════════════════════════

def scrape_public_metrics() -> Dict[str, Any]:
    """
    Scrape public metrics from LinkedIn company page.
    Uses urllib to fetch the page and extract follower count, etc.
    No authentication needed — just parses the public HTML.
    """
    import urllib.request
    import urllib.error

    company_page = _get_company_page()
    if not company_page:
        return {"error": "LINKEDIN_COMPANY_PAGE not configured", "demo": True}

    # Normalize URL
    if not company_page.startswith("http"):
        company_page = f"https://www.linkedin.com/company/{company_page.strip('/')}/"

    try:
        req = urllib.request.Request(
            company_page,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
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
    result = {
        "company_page": url,
        "scraped_at": datetime.now().isoformat(),
        "demo": False,
    }

    # Extract follower count from various LinkedIn page patterns
    # Pattern 1: "X followers" in meta or text
    follower_patterns = [
        r'"followers":(\d+)',
        r'(\d[\d,]+)\s*followers',
        r'followerCount["\s:]+(\d+)',
        r'"totalFollowers":(\d+)',
    ]
    for pattern in follower_patterns:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            result["followers"] = int(m.group(1).replace(",", ""))
            break

    # Extract company name
    name_patterns = [
        r'<title>([^<]+?)(?:\s*\|\s*LinkedIn|\s*-\s*LinkedIn)',
        r'"name"\s*:\s*"([^"]+)"',
        r'"companyName"\s*:\s*"([^"]+)"',
    ]
    for pattern in name_patterns:
        m = re.search(pattern, html)
        if m:
            result["company_name"] = m.group(1).strip()
            break

    # Extract employee count
    emp_patterns = [
        r'(\d[\d,+]*)\s*(?:employees|associates)',
        r'"employeeCountRange"\s*:\s*{[^}]*?"end"\s*:\s*(\d+)',
    ]
    for pattern in emp_patterns:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            val = m.group(1).replace(",", "").replace("+", "")
            result["employees"] = val
            break

    # Extract industry
    ind_match = re.search(r'"industry"\s*:\s*"([^"]+)"', html)
    if ind_match:
        result["industry"] = ind_match.group(1)

    # Extract description
    desc_match = re.search(r'"description"\s*:\s*"([^"]{10,500})"', html)
    if desc_match:
        result["description"] = desc_match.group(1)

    return result


# ═══════════════════════════════════════════════════════════════
# METHOD 2: Authenticated Playwright Scrape (Pipeline)
# ═══════════════════════════════════════════════════════════════

def scrape_with_playwright(historical: bool = False) -> Dict[str, Any]:
    """
    Scrape LinkedIn analytics using Playwright with stored cookies.
    Used by GitHub Actions pipeline for daily automated scraping.
    
    Gets: impressions, unique visitors, follower count, post stats
    """
    cookies_json = _get_cookies()
    if not cookies_json:
        return {"error": "No LinkedIn cookies configured", "demo": True}

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"error": "Playwright not installed", "demo": True}

    company_page = _get_company_page()
    if not company_page:
        return {"error": "LINKEDIN_COMPANY_PAGE not configured", "demo": True}

    try:
        cookies = json.loads(cookies_json) if isinstance(cookies_json, str) else cookies_json
    except json.JSONDecodeError:
        return {"error": "Invalid cookies JSON", "demo": True}

    result = {"scraped_at": datetime.now().isoformat(), "demo": False}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={"width": 1920, "height": 1080},
            )

            # Add cookies
            if cookies:
                cookie_list = cookies if isinstance(cookies, list) else [cookies]
                formatted = []
                for c in cookie_list:
                    fc = {
                        "name": c.get("name", ""),
                        "value": c.get("value", ""),
                        "domain": c.get("domain", ".linkedin.com"),
                        "path": c.get("path", "/"),
                    }
                    formatted.append(fc)
                context.add_cookies(formatted)

            page = context.new_page()

            # ── Scrape Company Analytics Page ──
            analytics_url = company_page.rstrip("/") + "/admin/analytics/"
            page.goto(analytics_url, wait_until="networkidle", timeout=30000)
            time.sleep(3)

            # Extract analytics data from page
            page_content = page.content()
            analytics = _extract_analytics_from_page(page_content)

            # ── Scrape Followers ──
            try:
                follower_url = company_page.rstrip("/") + "/admin/followers/"
                page.goto(follower_url, wait_until="networkidle", timeout=30000)
                time.sleep(2)
                follower_content = page.content()
                followers = _extract_followers_from_page(follower_content)
                analytics.update(followers)
            except Exception as e:
                print(f"[LinkedIn] Follower scrape failed: {e}")

            # ── Scrape Recent Posts ──
            try:
                posts_url = company_page.rstrip("/") + "/admin/posts/"
                page.goto(posts_url, wait_until="networkidle", timeout=30000)
                time.sleep(2)
                posts_content = page.content()
                posts = _extract_posts_from_page(posts_content)
                result["posts"] = posts
            except Exception as e:
                print(f"[LinkedIn] Posts scrape failed: {e}")

            browser.close()
            result.update(analytics)

    except Exception as e:
        result["error"] = str(e)

    # Save
    _save_json(LI_METRICS_CACHE, result)
    return result


def _extract_analytics_from_page(html: str) -> Dict[str, Any]:
    """Extract analytics metrics from LinkedIn admin page."""
    result = {}

    # Try to find JSON data embedded in page
    json_patterns = [
        r'"visitorMetrics"\s*:\s*({[^}]+})',
        r'"impressionCount"\s*:\s*(\d+)',
        r'"uniqueVisitors"\s*:\s*(\d+)',
        r'"totalPageViews"\s*:\s*(\d+)',
    ]

    for pattern in json_patterns:
        m = re.search(pattern, html)
        if m:
            key = pattern.split('"')[1]
            try:
                result[key] = json.loads(m.group(1)) if m.group(1).startswith("{") else int(m.group(1))
            except Exception:
                pass

    # Look for data-react-props or similar embedded data
    props_match = re.search(r'data-react-props="([^"]+)"', html)
    if props_match:
        try:
            props = json.loads(props_match.group(1).replace("&quot;", '"'))
            result["react_props"] = props
        except Exception:
            pass

    return result


def _extract_followers_from_page(html: str) -> Dict[str, Any]:
    """Extract follower data from LinkedIn admin page."""
    result = {}

    follower_patterns = [
        r'"totalFollowers"\s*:\s*(\d+)',
        r'"followerCount"\s*:\s*(\d+)',
        r'"organicFollowers"\s*:\s*(\d+)',
    ]

    for pattern in follower_patterns:
        m = re.search(pattern, html)
        if m:
            key = pattern.split('"')[1].split('"')[0]
            result[key] = int(m.group(1))

    return result


def _extract_posts_from_page(html: str) -> List[Dict]:
    """Extract post data from LinkedIn admin page."""
    posts = []

    # Try to find post data in embedded JSON
    post_patterns = [
        r'"updateMetadata"\s*:\s*({[^}]+?})',
    ]

    for m in re.finditer(r'"text"\s*:\s*"([^"]{10,500})"', html):
        text = m.group(1)
        # Look for nearby engagement metrics
        start = max(0, m.start() - 500)
        end = min(len(html), m.end() + 500)
        context = html[start:end]

        likes = 0
        comments = 0
        impressions = 0

        like_m = re.search(r'"numLikes"\s*:\s*(\d+)', context)
        if like_m:
            likes = int(like_m.group(1))

        comment_m = re.search(r'"numComments"\s*:\s*(\d+)', context)
        if comment_m:
            comments = int(comment_m.group(1))

        imp_m = re.search(r'"impressionCount"\s*:\s*(\d+)', context)
        if imp_m:
            impressions = int(imp_m.group(1))

        date_m = re.search(r'"createdDate"\s*:\s*{[^}]*?"year"\s*:\s*(\d+)[^}]*?"month"\s*:\s*(\d+)[^}]*?"day"\s*:\s*(\d+)', context)

        post = {
            "text": text[:200],
            "likes": likes,
            "comments": comments,
            "impressions": impressions,
        }

        if date_m:
            post["date"] = f"{date_m.group(1)}-{int(date_m.group(2)):02d}-{int(date_m.group(3)):02d}"

        posts.append(post)

    return posts[:50]


# ═══════════════════════════════════════════════════════════════
# METHOD 3: Manual Data Input (Dashboard UI)
# ═══════════════════════════════════════════════════════════════

def save_manual_entry(data: Dict[str, Any]) -> bool:
    """Save manually entered LinkedIn metrics."""
    try:
        existing = {}
        if LI_DAILY_CACHE.exists():
            existing = json.loads(LI_DAILY_CACHE.read_text())

        entries = existing.get("entries", [])
        entry = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "scraped_at": datetime.now().isoformat(),
            **data,
        }
        entries.append(entry)
        existing["entries"] = entries[-365:]  # Keep last year
        _save_json(LI_DAILY_CACHE, existing)
        return True
    except Exception as e:
        print(f"[LinkedIn] Save failed: {e}")
        return False


def get_manual_history() -> pd.DataFrame:
    """Get manually entered LinkedIn metrics as DataFrame."""
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
    """Import LinkedIn data from CSV export."""
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
# Combined / Status
# ═══════════════════════════════════════════════════════════════

def get_cached_metrics() -> Dict[str, Any]:
    """Get cached LinkedIn metrics (no API call)."""
    cache = _load_json(LI_METRICS_CACHE, max_age_hours=24)
    if cache:
        return {k: v for k, v in cache.items() if k != "_cached_at"}
    return {}


def get_all_data() -> Dict[str, Any]:
    """Get all LinkedIn data combined."""
    return {
        "metrics": get_cached_metrics(),
        "daily": json.loads(LI_DAILY_CACHE.read_text()) if LI_DAILY_CACHE.exists() else {},
        "posts": json.loads(LI_POSTS_CACHE.read_text()) if LI_POSTS_CACHE.exists() else {},
        "followers": json.loads(LI_FOLLOWERS_CACHE.read_text()) if LI_FOLLOWERS_CACHE.exists() else {},
    }


def is_configured() -> bool:
    """Check if any LinkedIn method is configured."""
    return bool(_get_company_page())


def has_cookies() -> bool:
    """Check if authenticated scraping is available."""
    return bool(_get_cookies())


def get_status() -> Dict[str, Any]:
    """Get connection status."""
    return {
        "company_page": bool(_get_company_page()),
        "cookies": bool(_get_cookies()),
        "public_scrape": bool(_get_company_page()),
        "authenticated_scrape": bool(_get_cookies() and _get_company_page()),
        "configured": bool(_get_company_page()),
        "cached_data": LI_METRICS_CACHE.exists(),
    }
