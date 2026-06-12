"""
LinkedIn Connector — Eagle Analytics Hub v7.2
==============================================
Scrapes LinkedIn company page data including:
- Company metrics (followers, employees, industry)
- Individual posts with impressions, likes, comments, engagement
- Daily time-series auto-accumulation
- Historical data for trends

3 methods:
1. Public page scrape (urllib, no auth) — limited
2. Authenticated Playwright scrape (pipeline) — full data
3. Manual entry + CSV import (dashboard UI)
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
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
# METHOD 2: Authenticated Playwright Scrape (Pipeline only)
# ═══════════════════════════════════════════════════════════════

def scrape_with_playwright(historical: bool = False) -> Dict[str, Any]:
    """Scrape LinkedIn analytics using Playwright. Pipeline only.
    Gets: company metrics + individual posts with engagement data."""
    cookies_json = _get_cookies()
    if not cookies_json:
        return scrape_public_metrics()

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return scrape_public_metrics()

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
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={"width": 1920, "height": 1080}
            )

            if isinstance(cookies, list):
                formatted = [{
                    "name": c.get("name", ""),
                    "value": c.get("value", ""),
                    "domain": c.get("domain", ".linkedin.com"),
                    "path": c.get("path", "/"),
                } for c in cookies]
                context.add_cookies(formatted)

            page = context.new_page()

            # ── Scrape company metrics ──
            try:
                analytics_url = company_page.rstrip("/") + "/admin/analytics/"
                page.goto(analytics_url, wait_until="networkidle", timeout=30000)
                time.sleep(3)
                analytics = _extract_analytics_from_page(page.content())
                result.update(analytics)
            except Exception as e:
                print(f"[LinkedIn] Analytics scrape failed: {e}")

            # ── Scrape followers ──
            try:
                follower_url = company_page.rstrip("/") + "/admin/followers/"
                page.goto(follower_url, wait_until="networkidle", timeout=30000)
                time.sleep(2)
                followers = _extract_followers_from_page(page.content())
                result.update(followers)
            except Exception as e:
                print(f"[LinkedIn] Follower scrape failed: {e}")

            # ── Scrape posts with engagement ──
            try:
                posts_url = company_page.rstrip("/") + "/admin/posts/"
                page.goto(posts_url, wait_until="networkidle", timeout=30000)
                time.sleep(3)
                # Scroll to load more posts
                for _ in range(5):
                    page.evaluate("window.scrollBy(0, 1000)")
                    time.sleep(1)
                posts = _extract_posts_from_page(page)
                result["posts"] = posts
                result["post_count"] = len(posts)
                print(f"[LinkedIn] Scraped {len(posts)} posts")
            except Exception as e:
                print(f"[LinkedIn] Posts scrape failed: {e}")

            browser.close()
    except Exception as e:
        result["error"] = str(e)

    # Also try public scrape for basic info if Playwright missed some
    if "followers" not in result:
        pub = scrape_public_metrics()
        if "followers" in pub:
            result["followers"] = pub["followers"]
        if "company_name" not in result and "company_name" in pub:
            result["company_name"] = pub["company_name"]

    _save_json(LI_METRICS_CACHE, result)
    return result


def _extract_analytics_from_page(html: str) -> Dict[str, Any]:
    result = {}
    for pattern in [
        r'"impressionCount"\s*:\s*(\d+)',
        r'"uniqueVisitors"\s*:\s*(\d+)',
        r'"totalPageViews"\s*:\s*(\d+)',
        r'"likeCount"\s*:\s*(\d+)',
        r'"commentCount"\s*:\s*(\d+)',
        r'"shareCount"\s*:\s*(\d+)',
        r'"clickCount"\s*:\s*(\d+)',
    ]:
        m = re.search(pattern, html)
        if m:
            key = pattern.split('"')[1]
            result[key] = int(m.group(1))
    return result


def _extract_followers_from_page(html: str) -> Dict[str, Any]:
    result = {}
    for pattern in [
        r'"totalFollowers"\s*:\s*(\d+)',
        r'"followerCount"\s*:\s*(\d+)',
        r'"organicFollowers"\s*:\s*(\d+)',
    ]:
        m = re.search(pattern, html)
        if m:
            key = pattern.split('"')[1]
            result[key] = int(m.group(1))
    return result


def _extract_posts_from_page(page) -> List[Dict[str, Any]]:
    """Extract post data from LinkedIn admin posts page."""
    posts = page.evaluate("""
        () => {
            const postElements = document.querySelectorAll('[data-urn], .update-components-text, .feed-shared-update-v2');
            const results = [];
            const seen = new Set();
            
            // Try to find post containers with engagement data
            document.querySelectorAll('article, [class*="update"], [class*="post"]').forEach(el => {
                const text = el.innerText || '';
                if (text.length < 10) return;
                
                // Extract post URN/ID
                const urn = el.getAttribute('data-urn') || el.getAttribute('data-id') || '';
                if (seen.has(urn) && urn) return;
                if (urn) seen.add(urn);
                
                // Extract engagement numbers from text
                const likeMatch = text.match(/(\\d[\\d,]*)\\s*likes?/i) || text.match(/like.*?(\\d[\\d,]*)/i);
                const commentMatch = text.match(/(\\d[\\d,]*)\\s*comments?/i) || text.match(/comment.*?(\\d[\\d,]*)/i);
                const repostMatch = text.match(/(\\d[\\d,]*)\\s*reposts?/i) || text.match(/repost.*?(\\d[\\d,]*)/i);
                const impressionMatch = text.match(/(\\d[\\d,]*)\\s*impressions?/i);
                const viewMatch = text.match(/(\\d[\\d,]*)\\s*views?/i);
                
                // Get first line as title
                const lines = text.split('\\n').filter(l => l.trim().length > 3);
                const title = lines.slice(0, 2).join(' ').substring(0, 200);
                
                const post = {
                    urn: urn,
                    title: title,
                    likes: likeMatch ? parseInt(likeMatch[1].replace(/,/g, '')) : 0,
                    comments: commentMatch ? parseInt(commentMatch[1].replace(/,/g, '')) : 0,
                    reposts: repostMatch ? parseInt(repostMatch[1].replace(/,/g, '')) : 0,
                    impressions: impressionMatch ? parseInt(impressionMatch[1].replace(/,/g, '')) : (viewMatch ? parseInt(viewMatch[1].replace(/,/g, '')) : 0),
                };
                
                if (post.title || post.urn) {
                    results.push(post);
                }
            });
            
            // If no posts found via article elements, try broader approach
            if (results.length === 0) {
                const allText = document.body.innerText;
                const blocks = allText.split(/\\n{3,}/);
                blocks.forEach(block => {
                    if (block.length < 20) return;
                    const likeMatch = block.match(/(\\d[\\d,]*)\\s*likes?/i);
                    const commentMatch = block.match(/(\\d[\\d,]*)\\s*comments?/i);
                    if (likeMatch || commentMatch) {
                        const lines = block.split('\\n').filter(l => l.trim().length > 3);
                        results.push({
                            urn: '',
                            title: lines.slice(0, 2).join(' ').substring(0, 200),
                            likes: likeMatch ? parseInt(likeMatch[1].replace(/,/g, '')) : 0,
                            comments: commentMatch ? parseInt(commentMatch[1].replace(/,/g, '')) : 0,
                            reposts: 0,
                            impressions: 0,
                        });
                    }
                });
            }
            
            return results.slice(0, 50);  // Max 50 posts
        }
    """)
    return posts if posts else []


# ═══════════════════════════════════════════════════════════════
# METHOD 2b: API (if access token available)
# ═══════════════════════════════════════════════════════════════

def scrape_via_api() -> Dict[str, Any]:
    """Use LinkedIn API with OAuth token if configured."""
    import urllib.request
    token = _get_secret("LINKEDIN_ACCESS_TOKEN", "")
    if not token:
        return {"error": "No LinkedIn access token", "demo": True}

    result = {"scraped_at": datetime.now().isoformat(), "demo": False}

    try:
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
# POST-LEVEL DATA
# ═══════════════════════════════════════════════════════════════

def get_posts() -> List[Dict[str, Any]]:
    """Get cached post data. Returns list of posts with metrics."""
    if not LI_POSTS_CACHE.exists():
        return []
    try:
        data = json.loads(LI_POSTS_CACHE.read_text())
        if isinstance(data, list):
            return data
        return data.get("posts", [])
    except Exception:
        return []


def save_posts(posts: List[Dict[str, Any]]) -> bool:
    """Save posts data to cache."""
    try:
        data = {
            "posts": posts,
            "_saved_at": datetime.now().isoformat(),
        }
        LI_POSTS_CACHE.write_text(json.dumps(data, default=str, indent=2))
        return True
    except Exception as e:
        print(f"[LinkedIn] Save posts failed: {e}")
        return False


def calculate_post_score(post: Dict[str, Any]) -> int:
    """Calculate engagement score 0-100 for a LinkedIn post."""
    impressions = post.get("impressions", 0) or 0
    likes = post.get("likes", 0) or 0
    comments = post.get("comments", 0) or 0
    reposts = post.get("reposts", 0) or 0

    if impressions == 0 and likes == 0 and comments == 0:
        return 0

    # Engagement rate calculation
    if impressions > 0:
        eng_rate = (likes + comments * 3 + reposts * 2) / impressions * 100
    else:
        eng_rate = (likes + comments * 3) / max(1, likes + comments + 1) * 10

    # Score based on engagement rate benchmarks for LinkedIn
    # Avg LinkedIn engagement: ~2%, Good: 4%, Great: 6%+
    score = min(100, int(eng_rate * 15))

    # Bonus for absolute engagement
    if likes > 20:
        score = min(100, score + 5)
    if comments > 5:
        score = min(100, score + 5)
    if reposts > 3:
        score = min(100, score + 5)

    return max(0, score)


def get_score_label(score: int) -> tuple:
    """Get label and color for a score."""
    if score >= 80:
        return "🟢 Excellent", "#2ecc71"
    elif score >= 60:
        return "🟣 Strong", "#9b59b6"
    elif score >= 40:
        return "🔵 Good", "#3498db"
    elif score >= 20:
        return "🟡 Moderate", "#f39c12"
    elif score > 0:
        return "🟠 Low", "#e67e22"
    else:
        return "⚪ No Data", "#95a5a6"


# ═══════════════════════════════════════════════════════════════
# DAILY TIME-SERIES AUTO-ACCUMULATION
# ═══════════════════════════════════════════════════════════════

def save_manual_entry(data: Dict[str, Any]) -> bool:
    """Save a daily entry to the time-series. Called by pipeline and manual entry."""
    try:
        existing = {}
        if LI_DAILY_CACHE.exists():
            existing = json.loads(LI_DAILY_CACHE.read_text())

        entries = existing.get("entries", [])

        today_str = datetime.now().strftime("%Y-%m-%d")

        # Check if today already has an entry — update it instead of appending
        today_idx = None
        for i, e in enumerate(entries):
            if e.get("date") == today_str:
                today_idx = i
                break

        entry = {"date": today_str, "scraped_at": datetime.now().isoformat(), **data}

        if today_idx is not None:
            # Merge: update existing fields, keep any that new data doesn't have
            for k, v in entry.items():
                if v is not None and v != 0 and v != "":
                    entries[today_idx][k] = v
        else:
            entries.append(entry)

        # Keep last 365 days
        entries = sorted(entries, key=lambda x: x.get("date", ""))
        existing["entries"] = entries[-365:]
        _save_json(LI_DAILY_CACHE, existing)
        return True
    except Exception as e:
        print(f"[LinkedIn] Save failed: {e}")
        return False


def get_manual_history() -> pd.DataFrame:
    """Get daily time-series data as DataFrame."""
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
    """Import CSV data into the daily time-series."""
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
        "has_posts": LI_POSTS_CACHE.exists(),
        "has_daily": LI_DAILY_CACHE.exists(),
    }


def get_aggregate_stats() -> Dict[str, Any]:
    """Get aggregate LinkedIn stats from all sources."""
    stats = {
        "followers": 0,
        "company_name": "",
        "industry": "",
        "employees": "",
        "total_impressions": 0,
        "total_likes": 0,
        "total_comments": 0,
        "total_reposts": 0,
        "post_count": 0,
        "avg_engagement_rate": 0.0,
        "connected": False,
    }

    # From cached metrics
    cached = get_cached_metrics()
    if cached and not cached.get("error"):
        stats["followers"] = cached.get("followers", 0)
        stats["company_name"] = cached.get("company_name", "")
        stats["industry"] = cached.get("industry", "")
        stats["employees"] = cached.get("employees", "")
        stats["total_impressions"] = cached.get("impressionCount", 0)
        stats["connected"] = True

    # From posts
    posts = get_posts()
    if posts:
        stats["post_count"] = len(posts)
        stats["total_likes"] = sum(p.get("likes", 0) for p in posts)
        stats["total_comments"] = sum(p.get("comments", 0) for p in posts)
        stats["total_reposts"] = sum(p.get("reposts", 0) for p in posts)
        total_imp = sum(p.get("impressions", 0) for p in posts)
        if total_imp > 0:
            stats["total_impressions"] = max(stats["total_impressions"], total_imp)
            total_eng = stats["total_likes"] + stats["total_comments"] + stats["total_reposts"]
            stats["avg_engagement_rate"] = round(total_eng / total_imp * 100, 2) if total_imp > 0 else 0.0
        stats["connected"] = True

    # From daily history
    hist = get_manual_history()
    if not hist.empty:
        stats["connected"] = True
        if "followers" in hist.columns and not hist["followers"].empty:
            latest_f = hist["followers"].dropna()
            if not latest_f.empty:
                stats["followers"] = max(stats["followers"], int(latest_f.iloc[-1]))

    return stats
