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
    """Scrape public metrics from LinkedIn company page using urllib.
    BULLETPROOF: Extracts data from JSON-LD structured data + HTML patterns.
    No cookies or authentication required."""
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
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "identity",
                "Cache-Control": "no-cache",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        result = _extract_public_data(html, company_page)
        
        # Also extract posts from JSON-LD
        posts = _extract_posts_from_jsonld(html)
        if posts:
            result["posts"] = posts
            result["post_count"] = len(posts)
            result["total_post_likes"] = sum(p.get("likes", 0) for p in posts)
            result["total_post_comments"] = sum(p.get("comments", 0) for p in posts)
        
        _save_json(LI_METRICS_CACHE, result)
        return result

    except Exception as e:
        print(f"[LinkedIn] Public scrape error: {e}")
        return {"error": str(e), "demo": True}


def _extract_posts_from_jsonld(html: str) -> List[Dict]:
    """Extract recent posts from LinkedIn's JSON-LD structured data.
    LinkedIn public pages include DiscussionForumPosting entries for recent posts.
    This is 100% free and requires no authentication."""
    posts = []
    
    # Method 1: Parse the full JSON-LD @graph array
    try:
        m = re.search(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL)
        if m:
            ld_data = json.loads(m.group(1))
            graph = ld_data.get("@graph", [])
            if not graph and isinstance(ld_data, list):
                graph = ld_data
            if not graph and ld_data.get("@type") == "DiscussionForumPosting":
                graph = [ld_data]
            
            for item in graph:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") != "DiscussionForumPosting":
                    continue
                
                post = {
                    "title": item.get("name", ""),
                    "text": item.get("text", "")[:500],
                    "url": item.get("mainEntityOfPage", item.get("url", "")),
                    "published_at": item.get("datePublished", ""),
                    "author": item.get("author", {}).get("name", ""),
                    "likes": 0,
                    "comments": 0,
                    "shares": 0,
                    "source": "linkedin_jsonld",
                }
                
                # Extract engagement from interactionStatistic
                stats = item.get("interactionStatistic", [])
                if isinstance(stats, dict):
                    stats = [stats]
                for stat in stats:
                    if not isinstance(stat, dict):
                        continue
                    stat_type = stat.get("interactionType", "")
                    count = 0
                    try:
                        count = int(stat.get("userInteractionCount", 0))
                    except (ValueError, TypeError):
                        pass
                    if "Like" in str(stat_type) or "like" in str(stat_type).lower():
                        post["likes"] = count
                    elif "Comment" in str(stat_type) or "comment" in str(stat_type).lower():
                        post["comments"] = count
                    elif "Share" in str(stat_type) or "share" in str(stat_type).lower():
                        post["shares"] = count
                
                posts.append(post)
    except Exception as e:
        print(f"[LinkedIn] JSON-LD extraction error: {e}")
    
    # Method 2: Regex fallback for posts if JSON-LD didn't work
    if not posts:
        try:
            # Look for activity URLs in the page
            activity_urls = re.findall(
                r'https://www\.linkedin\.com/posts/eagle-3d-streaming_[\w-]+-activity-(\d+)-[\w]+',
                html
            )
            if activity_urls:
                seen = set()
                for act_id in activity_urls[:10]:
                    if act_id not in seen:
                        seen.add(act_id)
                        posts.append({
                            "title": "",
                            "text": "",
                            "url": f"https://www.linkedin.com/posts/eagle-3d-streaming_activity-{act_id}",
                            "published_at": "",
                            "likes": 0,
                            "comments": 0,
                            "shares": 0,
                            "source": "linkedin_regex",
                        })
        except Exception:
            pass
    
    return posts


def _extract_public_data(html: str, url: str) -> Dict[str, Any]:
    """Extract metrics from LinkedIn public page HTML.
    BULLETPROOF: Uses multiple extraction methods — JSON-LD, meta tags, regex patterns."""
    result = {"company_page": url, "scraped_at": datetime.now().isoformat(), "demo": False}

    # ── Method 1: Extract from JSON-LD @graph (most reliable) ──
    try:
        m = re.search(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL)
        if m:
            ld_data = json.loads(m.group(1))
            graph = ld_data.get("@graph", [])
            if isinstance(ld_data, list):
                graph = ld_data
            for item in graph:
                if not isinstance(item, dict):
                    continue
                author = item.get("author", {})
                if isinstance(author, dict) and author.get("name"):
                    if not result.get("company_name"):
                        result["company_name"] = author["name"]
                    if not result.get("company_url"):
                        result["company_url"] = author.get("url", "")
                # Extract follower count from interactionStatistic
                for stat in item.get("interactionStatistic", []):
                    if not isinstance(stat, dict):
                        continue
                    if "Follow" in str(stat.get("interactionType", "")):
                        try:
                            result["followers"] = int(stat.get("userInteractionCount", 0))
                        except (ValueError, TypeError):
                            pass
    except Exception:
        pass

    # ── Method 2: Regex patterns (fallback) ──
    # Follower count
    if not result.get("followers"):
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
    if not result.get("company_name"):
        for pattern in [
            r'<title>([^<]+?)(?:\s*\|\s*LinkedIn|\s*-\s*LinkedIn)',
            r'"name"\s*:\s*"([^"]+)"', r'"companyName"\s*:\s*"([^"]+)"',
        ]:
            m = re.search(pattern, html)
            if m:
                result["company_name"] = m.group(1).strip()
                break

    # Employee count
    if not result.get("employees"):
        for pattern in [
            r'(\d[\d,+]+)\s*(?:employees|associates|members)',
            r'"employeeCountRange"[^}]*?"end"\s*:\s*(\d+)',
            r'"staffCount"\s*:\s*(\d+)',
        ]:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                result["employees"] = m.group(1).replace(",", "").replace("+", "")
                break

    # Industry
    if not result.get("industry"):
        for pattern in [r'"industry"\s*:\s*"([^"]+)"', r'"industryName"\s*:\s*"([^"]+)"']:
            m = re.search(pattern, html)
            if m:
                result["industry"] = m.group(1)
                break

    # Description
    if not result.get("description"):
        for pattern in [r'"description"\s*:\s*"([^"]{10,500})"', r'"tagline"\s*:\s*"([^"]{10,500})"']:
            m = re.search(pattern, html)
            if m:
                result["description"] = m.group(1)
                break

    # Website URL
    if not result.get("website"):
        for pattern in [r'"website"\s*:\s*"([^"]+)"', r'"companyPageUrl"\s*:\s*"([^"]+)"']:
            m = re.search(pattern, html)
            if m:
                result["website"] = m.group(1)
                break

    # Company type / size
    if not result.get("company_size"):
        m = re.search(r'"companySize"\s*:\s*"([^"]+)"', html)
        if m:
            result["company_size"] = m.group(1)

    # Headquarters
    if not result.get("headquarters"):
        m = re.search(r'"headquarters"[^}]*?"city"\s*:\s*"([^"]*)"[^}]*?"country"\s*:\s*"([^"]*)"', html)
        if m:
            result["headquarters"] = f"{m.group(1)}, {m.group(2)}"

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
# METHOD 3: Cookie-based authenticated urllib scrape (no Playwright)
# Works on Streamlit Cloud where Playwright is not available
# ═══════════════════════════════════════════════════════════════

def scrape_with_cookies() -> Dict[str, Any]:
    """Scrape LinkedIn using cookies via urllib (no Playwright needed).
    Gets company page data, follower count, and basic analytics.
    Works on Streamlit Cloud and in GitHub Actions."""
    cookies_json = _get_cookies()
    company_page = _get_company_page()
    if not company_page:
        return {"error": "LINKEDIN_COMPANY_PAGE not configured", "demo": True}

    result = {"scraped_at": datetime.now().isoformat(), "demo": False}

    # Parse cookies into a cookie string
    cookie_str = ""
    if cookies_json:
        try:
            cookies = json.loads(cookies_json) if isinstance(cookies_json, str) else cookies_json
            cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies if c.get("name") and c.get("value"))
        except Exception:
            pass

    # First try public scrape for basics
    pub = scrape_public_metrics()
    if not pub.get("error"):
        result.update(pub)

    # If we have cookies, try to get authenticated data
    if cookie_str:
        import urllib.request
        import urllib.error

        # Try company analytics API endpoint
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Cookie": cookie_str,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": company_page,
            }
            # Try the company admin analytics page
            analytics_url = company_page.rstrip("/") + "/admin/analytics/visitors/"
            req = urllib.request.Request(analytics_url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
                analytics = _extract_analytics_from_page(html)
                if analytics:
                    result.update(analytics)
        except Exception as e:
            print(f"[LinkedIn] Cookie-based analytics scrape: {e}")

        # Try followers page
        try:
            headers["Referer"] = company_page.rstrip("/") + "/admin/analytics/"
            follower_url = company_page.rstrip("/") + "/admin/followers/"
            req = urllib.request.Request(follower_url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
                followers = _extract_followers_from_page(html)
                if followers:
                    result.update(followers)
                    if "totalFollowers" in followers:
                        result["followers"] = followers["totalFollowers"]
                    elif "followerCount" in followers:
                        result["followers"] = followers["followerCount"]
        except Exception as e:
            print(f"[LinkedIn] Cookie-based followers scrape: {e}")

        # Try posts page
        try:
            posts_url = company_page.rstrip("/") + "/admin/posts/"
            req = urllib.request.Request(posts_url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
                posts = _extract_posts_from_html(html)
                if posts:
                    result["posts"] = posts
                    result["post_count"] = len(posts)
                    print(f"[LinkedIn] Cookie scrape: got {len(posts)} posts")
        except Exception as e:
            print(f"[LinkedIn] Cookie-based posts scrape: {e}")

        result["authenticated"] = True

    _save_json(LI_METRICS_CACHE, result)
    return result


def _extract_posts_from_html(html: str) -> List[Dict[str, Any]]:
    """Extract post data from LinkedIn HTML (non-JS, server-rendered content)."""
    posts = []
    try:
        # LinkedIn embeds post data in JSON inside script tags
        # Look for post URN patterns
        urn_pattern = re.compile(r'urn:li:activity:(\d+)')
        urns = urn_pattern.findall(html)
        
        # Look for engagement data patterns
        like_patterns = re.findall(r'(\d[\d,]*)\s*likes?', html, re.IGNORECASE)
        comment_patterns = re.findall(r'(\d[\d,]*)\s*comments?', html, re.IGNORECASE)
        repost_patterns = re.findall(r'(\d[\d,]*)\s*reposts?', html, re.IGNORECASE)
        impression_patterns = re.findall(r'(\d[\d,]*)\s*impressions?', html, re.IGNORECASE)
        
        # If we found URNs, create posts from them
        for i, urn in enumerate(urns[:50]):
            post = {
                "urn": f"urn:li:activity:{urn}",
                "title": f"Post {i+1}",
                "likes": int(like_patterns[i].replace(",", "")) if i < len(like_patterns) else 0,
                "comments": int(comment_patterns[i].replace(",", "")) if i < len(comment_patterns) else 0,
                "reposts": int(repost_patterns[i].replace(",", "")) if i < len(repost_patterns) else 0,
                "impressions": int(impression_patterns[i].replace(",", "")) if i < len(impression_patterns) else 0,
            }
            posts.append(post)
    except Exception as e:
        print(f"[LinkedIn] HTML post extraction error: {e}")
    return posts


# ═══════════════════════════════════════════════════════════════
# POST-LEVEL DATA
# ═══════════════════════════════════════════════════════════════

def get_posts() -> List[Dict[str, Any]]:
    """Get cached post data. Returns list of posts with metrics.
    Tries linkedin_posts.json first, then falls back to posts embedded in linkedin_metrics.json.
    """
    # Try dedicated posts cache first
    if LI_POSTS_CACHE.exists():
        try:
            data = json.loads(LI_POSTS_CACHE.read_text())
            if isinstance(data, list):
                return data
            posts = data.get("posts", [])
            if posts:
                return posts
        except Exception:
            pass

    # Fallback: read posts from linkedin_metrics.json (which contains posts from public scrape)
    try:
        if LI_METRICS_CACHE.exists():
            metrics = json.loads(LI_METRICS_CACHE.read_text())
            posts = metrics.get("posts", [])
            if posts:
                return posts
    except Exception:
        pass

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
    _has_posts = LI_POSTS_CACHE.exists()
    # Also check linkedin_metrics.json for embedded posts
    if not _has_posts and LI_METRICS_CACHE.exists():
        try:
            _m = json.loads(LI_METRICS_CACHE.read_text())
            if _m.get("posts"):
                _has_posts = True
        except Exception:
            pass
    return {
        "company_page": bool(_get_company_page()),
        "cookies": bool(_get_cookies()),
        "public_scrape": bool(_get_company_page()),
        "authenticated_scrape": bool(_get_cookies() and _get_company_page()),
        "configured": bool(_get_company_page()),
        "cached_data": LI_METRICS_CACHE.exists(),
        "has_posts": _has_posts,
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
