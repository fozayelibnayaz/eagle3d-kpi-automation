"""
YouTube Connector — Eagle 3D KPI System
========================================
Fetches channel data, video stats, and analytics using:
  - YouTube Data API v3 (public data: views, likes, comments, channel info)
  - YouTube Analytics API v2 (private data: CTR, watch time, revenue, demographics)
  
Requires secrets:
  YOUTUBE_API_KEY         — YouTube Data API key (free, always needed)
  YOUTUBE_CHANNEL_ID      — Channel ID (e.g., UCxxxxx)
  YOUTUBE_OAUTH_TOKEN     — OAuth2 access token for Analytics API (optional, for private data)
  
Alternatively, can read from existing YouTube Command Center's stored data.
"""

import os
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

# ── Cache paths ──
YT_CACHE = DATA_DIR / "youtube_cache.json"
YT_VIDEO_CACHE = DATA_DIR / "youtube_videos.json"
YT_ANALYTICS_CACHE = DATA_DIR / "youtube_analytics.json"
YT_DAILY_CACHE = DATA_DIR / "youtube_daily.json"


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


def _get_api_key() -> str:
    return _get_secret("YOUTUBE_API_KEY")


def _get_channel_id() -> str:
    return _get_secret("YOUTUBE_CHANNEL_ID")


def _get_oauth_token() -> str:
    return _get_secret("YOUTUBE_OAUTH_TOKEN")


def _load_cache(path: Path, max_age_hours: int = 6) -> Optional[dict]:
    """Load cache if it exists and is fresh enough."""
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


def _save_cache(path: Path, data: dict):
    """Save data to cache with timestamp."""
    data["_cached_at"] = datetime.now().isoformat()
    path.write_text(json.dumps(data, default=str, indent=2))


def _api_request(url: str, params: dict = None) -> Optional[dict]:
    """Make YouTube Data API request."""
    import urllib.request
    import urllib.parse
    import urllib.error

    api_key = _get_api_key()
    if not api_key:
        return None

    if params is None:
        params = {}
    params["key"] = api_key

    full_url = url + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(full_url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[YouTube] API error: {e}")
        return None


def _analytics_request(params: dict) -> Optional[dict]:
    """Make YouTube Analytics API request (requires OAuth)."""
    import urllib.request
    import urllib.parse

    token = _get_oauth_token()
    if not token:
        return None

    base_url = "https://youtubeanalytics.googleapis.com/v2/reports"
    full_url = base_url + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(full_url)
        req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[YouTube Analytics] API error: {e}")
        return None


def _parse_analytics_rows(data: dict) -> List[dict]:
    """Parse Analytics API response into list of dicts."""
    if not data or "rows" not in data or "columnHeaders" not in data:
        return []
    headers = [h["name"] for h in data["columnHeaders"]]
    return [dict(zip(headers, row)) for row in data["rows"]]


def get_channel_info() -> Dict[str, Any]:
    """Get channel overview: subscribers, total views, video count."""
    cache = _load_cache(YT_CACHE, max_age_hours=6)
    if cache:
        return {k: v for k, v in cache.items() if k != "_cached_at"}

    channel_id = _get_channel_id()
    if not channel_id:
        return {"error": "YOUTUBE_CHANNEL_ID not configured", "demo": True}

    data = _api_request(
        "https://www.googleapis.com/youtube/v3/channels",
        {"part": "snippet,statistics,brandingSettings", "id": channel_id},
    )

    if not data or not data.get("items"):
        return {"error": "Channel not found", "demo": True}

    ch = data["items"][0]
    stats = ch.get("statistics", {})
    snippet = ch.get("snippet", {})

    result = {
        "channel_id": channel_id,
        "title": snippet.get("title", ""),
        "description": snippet.get("description", "")[:500],
        "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
        "published_at": snippet.get("publishedAt", ""),
        "subscribers": int(stats.get("subscriberCount", 0)),
        "total_views": int(stats.get("viewCount", 0)),
        "video_count": int(stats.get("videoCount", 0)),
        "custom_url": ch.get("snippet", {}).get("customUrl", ""),
        "demo": False,
        "fetched_at": datetime.now().isoformat(),
    }

    _save_cache(YT_CACHE, result)
    return result


def get_channel_videos(max_videos: int = 200) -> List[Dict]:
    """Get all videos with stats from YouTube Data API."""
    cache = _load_cache(YT_VIDEO_CACHE, max_age_hours=6)
    if cache and "videos" in cache:
        return cache["videos"]

    channel_id = _get_channel_id()
    if not channel_id:
        return []

    # Step 1: Get uploads playlist ID
    ch_data = _api_request(
        "https://www.googleapis.com/youtube/v3/channels",
        {"part": "contentDetails", "id": channel_id},
    )
    if not ch_data or not ch_data.get("items"):
        return []

    uploads_id = ch_data["items"][0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads", "")
    if not uploads_id:
        return []

    # Step 2: Paginate through playlist items to get video IDs
    video_ids = []
    next_page = None
    safety = 0

    while len(video_ids) < max_videos and safety < 50:
        params = {
            "part": "contentDetails",
            "playlistId": uploads_id,
            "maxResults": min(50, max_videos - len(video_ids)),
        }
        if next_page:
            params["pageToken"] = next_page

        pl_data = _api_request(
            "https://www.googleapis.com/youtube/v3/playlistItems",
            params,
        )
        if not pl_data or not pl_data.get("items"):
            break

        for item in pl_data["items"]:
            vid = item.get("contentDetails", {}).get("videoId", "")
            if vid:
                video_ids.append(vid)

        next_page = pl_data.get("nextPageToken")
        if not next_page:
            break
        safety += 1
        time.sleep(0.2)

    # Step 3: Batch get video details (50 at a time)
    all_videos = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        vid_data = _api_request(
            "https://www.googleapis.com/youtube/v3/videos",
            {"part": "snippet,statistics,contentDetails", "id": ",".join(batch)},
        )
        if not vid_data or not vid_data.get("items"):
            continue

        for v in vid_data["items"]:
            stats_v = v.get("statistics", {})
            snippet_v = v.get("snippet", {})
            duration_str = v.get("contentDetails", {}).get("duration", "PT0S")
            duration_sec = _parse_duration(duration_str)
            views = int(stats_v.get("viewCount", 0))
            likes = int(stats_v.get("likeCount", 0))
            comments = int(stats_v.get("commentCount", 0))

            all_videos.append({
                "video_id": v["id"],
                "title": snippet_v.get("title", ""),
                "published_at": snippet_v.get("publishedAt", ""),
                "thumbnail": snippet_v.get("thumbnails", {}).get("high", {}).get("url", ""),
                "duration_seconds": duration_sec,
                "duration_label": _format_duration(duration_sec),
                "views": views,
                "likes": likes,
                "comments": comments,
                "engagement_rate": round((likes + comments) / views * 100, 3) if views > 0 else 0,
                "like_rate": round(likes / views * 100, 3) if views > 0 else 0,
                "tags": snippet_v.get("tags", []),
                "category_id": snippet_v.get("categoryId", ""),
                "demo": False,
            })
        time.sleep(0.3)

    # Sort by published date (newest first)
    all_videos.sort(key=lambda x: x.get("published_at", ""), reverse=True)

    _save_cache(YT_VIDEO_CACHE, {"videos": all_videos})
    return all_videos


def get_daily_analytics(start_date: str, end_date: str) -> pd.DataFrame:
    """Get daily views, watch time, subscribers from Analytics API."""
    cache = _load_cache(YT_DAILY_CACHE, max_age_hours=6)
    if cache and "data" in cache:
        df = pd.DataFrame(cache["data"])
        if not df.empty and "day" in df.columns:
            return df

    channel_id = _get_channel_id()
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "views,estimatedMinutesWatched,averageViewDuration,likes,comments,shares,subscribersGained,subscribersLost",
        "dimensions": "day",
        "sort": "day",
    }

    data = _analytics_request(params)
    if not data:
        return pd.DataFrame()

    rows = _parse_analytics_rows(data)
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    for col in df.columns:
        if col != "day":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    _save_cache(YT_DAILY_CACHE, {"data": rows})
    return df


def get_video_analytics_batch(video_ids: List[str], start_date: str, end_date: str) -> Dict[str, Dict]:
    """Get per-video analytics (CTR, watch time, impressions) from Analytics API."""
    channel_id = _get_channel_id()
    result = {}

    for vid in video_ids[:50]:  # Analytics API rate limits
        params = {
            "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
            "startDate": start_date,
            "endDate": end_date,
            "metrics": "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,likes,comments,shares,subscribersGained,impressions,ctr",
            "dimensions": "video",
            "filters": f"video=={vid}",
        }
        data = _analytics_request(params)
        rows = _parse_analytics_rows(data)
        if rows:
            result[vid] = rows[0]
        time.sleep(0.3)

    return result


def get_traffic_sources(start_date: str, end_date: str) -> pd.DataFrame:
    """Get traffic source breakdown from Analytics API."""
    channel_id = _get_channel_id()
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "views,estimatedMinutesWatched,averageViewDuration",
        "dimensions": "insightTrafficSourceType",
        "sort": "-views",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_demographics(start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
    """Get audience demographics from Analytics API."""
    channel_id = _get_channel_id()
    channel_filter = f"channel=={channel_id}" if channel_id else "channel==MINE"

    results = {}
    queries = {
        "age_gender": {
            "metrics": "viewerPercentage",
            "dimensions": "ageGroup,gender",
        },
        "geography": {
            "metrics": "views,estimatedMinutesWatched",
            "dimensions": "country",
            "sort": "-views",
            "maxResults": "20",
        },
        "devices": {
            "metrics": "views,estimatedMinutesWatched",
            "dimensions": "deviceType",
        },
        "subscribed_status": {
            "metrics": "views",
            "dimensions": "subscribedStatus",
        },
    }

    for name, q in queries.items():
        params = {
            "ids": channel_filter,
            "startDate": start_date,
            "endDate": end_date,
            **q,
        }
        data = _analytics_request(params)
        rows = _parse_analytics_rows(data)
        results[name] = pd.DataFrame(rows) if rows else pd.DataFrame()
        time.sleep(0.3)

    return results


def get_revenue(start_date: str, end_date: str) -> Dict:
    """Get revenue data from Analytics API."""
    channel_id = _get_channel_id()
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "estimatedRevenue,estimatedAdRevenue,grossRevenue,cpm,playbackBasedCpm,adImpressions,monetizedPlaybacks",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return rows[0] if rows else {}


def get_revenue_daily(start_date: str, end_date: str) -> pd.DataFrame:
    """Get daily revenue from Analytics API."""
    channel_id = _get_channel_id()
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "estimatedRevenue,cpm,adImpressions",
        "dimensions": "day",
        "sort": "day",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if not df.empty:
        for col in df.columns:
            if col != "day":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def get_top_videos(metric: str = "views", start_date: str = "", end_date: str = "", limit: int = 10) -> pd.DataFrame:
    """Get top videos by metric from Analytics API."""
    channel_id = _get_channel_id()
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        "endDate": end_date or datetime.now().strftime("%Y-%m-%d"),
        "metrics": metric,
        "dimensions": "video",
        "sort": f"-{metric}",
        "maxResults": str(limit),
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_subscriber_growth(start_date: str, end_date: str) -> pd.DataFrame:
    """Get daily subscriber gained/lost."""
    channel_id = _get_channel_id()
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "subscribersGained,subscribersLost,views",
        "dimensions": "day",
        "sort": "day",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if not df.empty:
        for col in df.columns:
            if col != "day":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def get_search_terms(start_date: str, end_date: str, video_id: str = "") -> pd.DataFrame:
    """Get YouTube search terms that led to videos."""
    channel_id = _get_channel_id()
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date,
        "endDate": end_date,
        "metrics": "views",
        "dimensions": "insightTrafficSourceDetail",
        "filters": f"insightTrafficSourceType==YT_SEARCH{';video==' + video_id if video_id else ''}",
        "sort": "-views",
        "maxResults": "50",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Helper functions ──

def _parse_duration(iso: str) -> int:
    """Parse ISO 8601 duration to seconds."""
    import re
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso)
    if not m:
        return 0
    return (int(m.group(1) or 0) * 3600) + (int(m.group(2) or 0) * 60) + int(m.group(3) or 0)


def _format_duration(seconds: int) -> str:
    """Format seconds to human-readable duration."""
    if seconds >= 3600:
        h, remainder = divmod(seconds, 3600)
        m, s = divmod(remainder, 60)
        return f"{h}:{m:02d}:{s:02d}"
    else:
        m, s = divmod(seconds, 60)
        return f"{m}:{s:02d}"


def is_configured() -> bool:
    """Check if YouTube Data API is configured."""
    return bool(_get_api_key() and _get_channel_id())


def has_analytics_access() -> bool:
    """Check if YouTube Analytics API is available."""
    return bool(_get_oauth_token())


def get_status() -> Dict[str, Any]:
    """Get connection status."""
    api_key = _get_api_key()
    channel_id = _get_channel_id()
    oauth = _get_oauth_token()

    return {
        "data_api": bool(api_key),
        "channel_id": bool(channel_id),
        "analytics_api": bool(oauth),
        "configured": bool(api_key and channel_id),
        "full_access": bool(api_key and channel_id and oauth),
    }
