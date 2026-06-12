"""
YouTube Connector — Eagle 3D KPI System v7
=============================================
Complete YouTube analytics matching AI YouTube Command Center features:
  - Channel info, video library, performance scoring
  - YouTube Data API v3 (public data)
  - YouTube Analytics API v2 (private data via OAuth)
  - Retention curves, demographics, traffic sources
  - Revenue, playlist analytics, sharing data
  - Video scoring & diagnostics engine
  - Smart caching with auto-refresh

Required secrets:
  YOUTUBE_API_KEY      — YouTube Data API key (free)
  YOUTUBE_CHANNEL_ID   — Channel ID (UCxxxxx)
  YOUTUBE_OAUTH_TOKEN  — OAuth2 access/refresh token for Analytics API (optional)
"""

import os
import json
import re
import time
import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
import urllib.request
import urllib.parse
import urllib.error

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
        if key in st.secrets:
            val = st.secrets[key]
            if val is not None and str(val).strip():
                return str(val).strip()
    except Exception:
        pass
    return default


def _load_cache(path: Path, max_age_hours: int = 6) -> Optional[dict]:
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
    data["_cached_at"] = datetime.now().isoformat()
    path.write_text(json.dumps(data, default=str, indent=2))


def _api_request(url: str, params: dict = None) -> Optional[dict]:
    api_key = _get_secret("YOUTUBE_API_KEY")
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
    except urllib.error.HTTPError as e:
        if e.code == 401:
            # Token expired — try refresh
            refreshed = _try_refresh_token()
            if refreshed:
                req.add_header("Authorization", f"Bearer {refreshed}")
                with urllib.request.urlopen(req, timeout=30) as resp2:
                    return json.loads(resp2.read().decode())
        print(f"[YouTube Analytics] HTTP {e.code}: {e.reason}")
        return None
    except Exception as e:
        print(f"[YouTube Analytics] API error: {e}")
        return None


def _parse_analytics_rows(data: dict) -> List[dict]:
    if not data or "rows" not in data or "columnHeaders" not in data:
        return []
    headers = [h["name"] for h in data["columnHeaders"]]
    return [dict(zip(headers, row)) for row in data["rows"]]


def _get_oauth_token() -> str:
    return _get_secret("YOUTUBE_OAUTH_TOKEN")


def _get_refresh_token() -> str:
    return _get_secret("YOUTUBE_REFRESH_TOKEN")


def _try_refresh_token() -> Optional[str]:
    """Try to refresh OAuth token using refresh_token + client_id/secret."""
    refresh = _get_refresh_token()
    client_id = _get_secret("YOUTUBE_CLIENT_ID")
    client_secret = _get_secret("YOUTUBE_CLIENT_SECRET")
    if not all([refresh, client_id, client_secret]):
        return None
    try:
        data = urllib.parse.urlencode({
            "refresh_token": refresh,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
        }).encode()
        req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data)
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode())
            return result.get("access_token")
    except Exception as e:
        print(f"[YouTube OAuth] Refresh failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# CHANNEL INFO
# ═══════════════════════════════════════════════════════════════

def get_channel_info() -> Dict[str, Any]:
    cache = _load_cache(YT_CACHE, max_age_hours=6)
    if cache:
        return {k: v for k, v in cache.items() if k != "_cached_at"}

    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    if not channel_id:
        return {"error": "YOUTUBE_CHANNEL_ID not configured", "demo": True}

    data = _api_request(
        "https://www.googleapis.com/youtube/v3/channels",
        {"part": "snippet,statistics,brandingSettings,contentDetails", "id": channel_id},
    )
    if not data or not data.get("items"):
        return {"error": "Channel not found", "demo": True}

    ch = data["items"][0]
    stats = ch.get("statistics", {})
    snippet = ch.get("snippet", {})
    branding = ch.get("brandingSettings", {})

    result = {
        "channel_id": channel_id,
        "title": snippet.get("title", ""),
        "description": snippet.get("description", "")[:500],
        "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
        "banner": branding.get("image", {}).get("bannerExternalUrl", ""),
        "published_at": snippet.get("publishedAt", ""),
        "subscribers": int(stats.get("subscriberCount", 0)),
        "total_views": int(stats.get("viewCount", 0)),
        "video_count": int(stats.get("videoCount", 0)),
        "custom_url": snippet.get("customUrl", ""),
        "keywords": branding.get("channel", {}).get("keywords", []),
        "demo": False,
        "fetched_at": datetime.now().isoformat(),
    }

    _save_cache(YT_CACHE, result)
    return result


# ═══════════════════════════════════════════════════════════════
# VIDEO LIBRARY
# ═══════════════════════════════════════════════════════════════

def get_channel_videos(max_videos: int = 300) -> List[Dict]:
    cache = _load_cache(YT_VIDEO_CACHE, max_age_hours=6)
    if cache and "videos" in cache:
        return cache["videos"]

    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    if not channel_id:
        return []

    # Get uploads playlist ID
    ch_data = _api_request(
        "https://www.googleapis.com/youtube/v3/channels",
        {"part": "contentDetails", "id": channel_id},
    )
    if not ch_data or not ch_data.get("items"):
        return []
    uploads_id = ch_data["items"][0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads", "")
    if not uploads_id:
        return []

    # Paginate playlist items
    video_ids = []
    next_page = None
    safety = 0
    while len(video_ids) < max_videos and safety < 60:
        params = {"part": "contentDetails", "playlistId": uploads_id, "maxResults": min(50, max_videos - len(video_ids))}
        if next_page:
            params["pageToken"] = next_page
        pl_data = _api_request("https://www.googleapis.com/youtube/v3/playlistItems", params)
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
        time.sleep(0.15)

    # Batch get video details
    all_videos = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        vid_data = _api_request(
            "https://www.googleapis.com/youtube/v3/videos",
            {"part": "snippet,statistics,contentDetails", "id": ",".join(batch)},
        )
        if not vid_data or not vid_data.get("items"):
            continue
        for v in vid_data["items"]:
            sv = v.get("statistics", {})
            sn = v.get("snippet", {})
            dur = v.get("contentDetails", {}).get("duration", "PT0S")
            duration_sec = _parse_duration(dur)
            views = int(sv.get("viewCount", 0))
            likes = int(sv.get("likeCount", 0))
            comments = int(sv.get("commentCount", 0))

            all_videos.append({
                "video_id": v["id"],
                "title": sn.get("title", ""),
                "published_at": sn.get("publishedAt", ""),
                "thumbnail": sn.get("thumbnails", {}).get("high", {}).get("url", ""),
                "duration_seconds": duration_sec,
                "duration_label": _format_duration(duration_sec),
                "views": views,
                "likes": likes,
                "comments": comments,
                "engagement_rate": round((likes + comments) / views * 100, 3) if views > 0 else 0,
                "like_rate": round(likes / views * 100, 3) if views > 0 else 0,
                "tags": sn.get("tags", []),
                "category_id": sn.get("categoryId", ""),
                "demo": False,
            })
        time.sleep(0.25)

    all_videos.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    _save_cache(YT_VIDEO_CACHE, {"videos": all_videos})
    return all_videos


# ═══════════════════════════════════════════════════════════════
# ANALYTICS API (Requires OAuth)
# ═══════════════════════════════════════════════════════════════

def get_daily_analytics(start_date: str, end_date: str) -> pd.DataFrame:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,likes,comments,shares,subscribersGained,subscribersLost,impressions,ctr",
        "dimensions": "day", "sort": "day",
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
    return df


def get_subscriber_growth(start_date: str, end_date: str) -> pd.DataFrame:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "subscribersGained,subscribersLost,views",
        "dimensions": "day", "sort": "day",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if not df.empty:
        for col in df.columns:
            if col != "day":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def get_video_analytics_batch(video_ids: List[str], start_date: str, end_date: str) -> Dict[str, Dict]:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    result = {}
    for vid in video_ids[:50]:
        params = {
            "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
            "startDate": start_date, "endDate": end_date,
            "metrics": "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,likes,comments,shares,subscribersGained,subscribersLost,impressions,ctr",
            "dimensions": "video", "filters": f"video=={vid}",
        }
        data = _analytics_request(params)
        rows = _parse_analytics_rows(data)
        if rows:
            result[vid] = rows[0]
        time.sleep(0.25)
    return result


def get_retention_curve(video_id: str) -> List[Dict]:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        "endDate": datetime.now().strftime("%Y-%m-%d"),
        "metrics": "audienceWatchRatio,relativeRetentionPerformance",
        "dimensions": "elapsedVideoTimeRatio",
        "filters": f"video=={video_id}", "sort": "elapsedVideoTimeRatio",
    }
    data = _analytics_request(params)
    return _parse_analytics_rows(data) if data else []


def get_traffic_sources(start_date: str, end_date: str, video_id: str = "") -> pd.DataFrame:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "views,estimatedMinutesWatched,averageViewDuration",
        "dimensions": "insightTrafficSourceType", "sort": "-views",
    }
    if video_id:
        params["filters"] = f"video=={video_id}"
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_demographics(start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    ch = f"channel=={channel_id}" if channel_id else "channel==MINE"
    results = {}
    queries = {
        "age_gender": {"metrics": "viewerPercentage", "dimensions": "ageGroup,gender"},
        "geography": {"metrics": "views,estimatedMinutesWatched", "dimensions": "country", "sort": "-views", "maxResults": "20"},
        "devices": {"metrics": "views,estimatedMinutesWatched", "dimensions": "deviceType"},
        "os": {"metrics": "views", "dimensions": "operatingSystem", "sort": "-views", "maxResults": "10"},
        "subscribed_status": {"metrics": "views", "dimensions": "subscribedStatus"},
    }
    for name, q in queries.items():
        params = {"ids": ch, "startDate": start_date, "endDate": end_date, **q}
        data = _analytics_request(params)
        rows = _parse_analytics_rows(data)
        results[name] = pd.DataFrame(rows) if rows else pd.DataFrame()
        time.sleep(0.25)
    return results


def get_revenue(start_date: str, end_date: str) -> Dict:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "estimatedRevenue,estimatedAdRevenue,grossRevenue,cpm,playbackBasedCpm,adImpressions,monetizedPlaybacks",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return rows[0] if rows else {}


def get_revenue_daily(start_date: str, end_date: str) -> pd.DataFrame:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "estimatedRevenue,cpm,adImpressions",
        "dimensions": "day", "sort": "day",
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
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        "endDate": end_date or datetime.now().strftime("%Y-%m-%d"),
        "metrics": metric, "dimensions": "video", "sort": f"-{metric}", "maxResults": str(limit),
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_search_terms(start_date: str, end_date: str, video_id: str = "") -> pd.DataFrame:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    filters = "insightTrafficSourceType==YT_SEARCH"
    if video_id:
        filters += f";video=={video_id}"
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "views", "dimensions": "insightTrafficSourceDetail",
        "filters": filters, "sort": "-views", "maxResults": "50",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_views_by_playback(start_date: str, end_date: str) -> pd.DataFrame:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "views,estimatedMinutesWatched",
        "dimensions": "insightPlaybackLocationType", "sort": "-views",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_sharing_services(start_date: str, end_date: str) -> pd.DataFrame:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "shares", "dimensions": "sharingService", "sort": "-shares", "maxResults": "20",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_playlist_analytics(start_date: str, end_date: str) -> pd.DataFrame:
    channel_id = _get_secret("YOUTUBE_CHANNEL_ID")
    params = {
        "ids": f"channel=={channel_id}" if channel_id else "channel==MINE",
        "startDate": start_date, "endDate": end_date,
        "metrics": "views,estimatedMinutesWatched,averageViewDuration,playlistStarts,viewsPerPlaylistStart,averageTimeInPlaylist",
        "dimensions": "playlist", "sort": "-views", "maxResults": "25",
    }
    data = _analytics_request(params)
    rows = _parse_analytics_rows(data)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # Try to enrich with playlist titles
    if "playlist" in df.columns:
        ids = df["playlist"].tolist()
        titles = _get_playlist_titles(ids)
        if titles:
            df["playlist_title"] = df["playlist"].map(titles)
    return df


def _get_playlist_titles(playlist_ids: List[str]) -> Dict[str, str]:
    titles = {}
    for pid in playlist_ids[:25]:
        data = _api_request(
            "https://www.googleapis.com/youtube/v3/playlists",
            {"part": "snippet", "id": pid},
        )
        if data and data.get("items"):
            titles[pid] = data["items"][0].get("snippet", {}).get("title", pid)
        time.sleep(0.1)
    return titles


# ═══════════════════════════════════════════════════════════════
# SCORING ENGINE (from YouTube Command Center)
# ═══════════════════════════════════════════════════════════════

def calculate_performance_score(
    views: int, likes: int, comments: int,
    published_at: str = "", subscribers: int = 1000,
    ctr: float = None, retention: float = None,
) -> int:
    """
    Calculate video performance score (0-100).
    Path A (has OAuth CTR+Retention): CTR(35) + Retention(35) + Engagement(20) + Velocity(10)
    Path B (public data only): Velocity(40) + Engagement(40) + Reach(20)
    """
    if views == 0:
        return 0

    days_since = 1
    if published_at:
        try:
            pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            days_since = max(1, (datetime.now(pub.tzinfo) - pub).total_seconds() / 86400)
        except Exception:
            pass

    views_per_day = views / days_since
    engagement_rate = ((likes + comments) / views) * 100
    views_vs_subs = (views / max(subscribers, 1)) * 100

    # Path A: Has real CTR + Retention
    if ctr is not None and retention is not None:
        # CTR score (35 max)
        if ctr >= 10: ctr_s = 35
        elif ctr >= 7: ctr_s = 28 + ((ctr - 7) / 3) * 7
        elif ctr >= 4: ctr_s = 18 + ((ctr - 4) / 3) * 10
        elif ctr >= 2: ctr_s = 8 + ((ctr - 2) / 2) * 10
        else: ctr_s = (ctr / 2) * 8

        # Retention score (35 max)
        if retention >= 50: ret_s = 35
        elif retention >= 40: ret_s = 28 + ((retention - 40) / 10) * 7
        elif retention >= 30: ret_s = 18 + ((retention - 30) / 10) * 10
        elif retention >= 20: ret_s = 8 + ((retention - 20) / 10) * 10
        else: ret_s = (retention / 20) * 8

        # Engagement (20 max)
        if engagement_rate >= 8: eng_s = 20
        elif engagement_rate >= 5: eng_s = 15 + ((engagement_rate - 5) / 3) * 5
        elif engagement_rate >= 2: eng_s = 8 + ((engagement_rate - 2) / 3) * 7
        elif engagement_rate >= 0.5: eng_s = 3 + ((engagement_rate - 0.5) / 1.5) * 5
        else: eng_s = (engagement_rate / 0.5) * 3

        # Velocity (10 max)
        if views_per_day >= 1000: vpd_s = 10
        elif views_per_day >= 100: vpd_s = 7 + ((views_per_day - 100) / 900) * 3
        elif views_per_day >= 10: vpd_s = 3 + ((views_per_day - 10) / 90) * 4
        else: vpd_s = (views_per_day / 10) * 3

        return round(ctr_s + ret_s + eng_s + vpd_s)

    # Path B: Public data only
    if views_per_day >= 1000: vpd_s = 40
    elif views_per_day >= 100: vpd_s = 30 + ((views_per_day - 100) / 900) * 10
    elif views_per_day >= 20: vpd_s = 20 + ((views_per_day - 20) / 80) * 10
    elif views_per_day >= 5: vpd_s = 10 + ((views_per_day - 5) / 15) * 10
    elif views_per_day >= 1: vpd_s = 5 + ((views_per_day - 1) / 4) * 5
    else: vpd_s = views_per_day * 5

    if engagement_rate >= 8: eng_s = 40
    elif engagement_rate >= 5: eng_s = 30 + ((engagement_rate - 5) / 3) * 10
    elif engagement_rate >= 3: eng_s = 20 + ((engagement_rate - 3) / 2) * 10
    elif engagement_rate >= 1.5: eng_s = 10 + ((engagement_rate - 1.5) / 1.5) * 10
    elif engagement_rate >= 0.5: eng_s = 5 + ((engagement_rate - 0.5) / 1) * 5
    else: eng_s = engagement_rate * 10

    if views_vs_subs >= 100: reach_s = 20
    elif views_vs_subs >= 50: reach_s = 15 + ((views_vs_subs - 50) / 50) * 5
    elif views_vs_subs >= 20: reach_s = 10 + ((views_vs_subs - 20) / 30) * 5
    elif views_vs_subs >= 10: reach_s = 5 + ((views_vs_subs - 10) / 10) * 5
    else: reach_s = (views_vs_subs / 10) * 5

    return round(vpd_s + eng_s + reach_s)


def get_engagement_rating(rate: float) -> Dict[str, str]:
    if rate >= 8: return {"label": "Excellent", "color": "🟢", "desc": "Top 5% of creators"}
    if rate >= 5: return {"label": "Very Good", "color": "🔵", "desc": "Strong engagement"}
    if rate >= 3: return {"label": "Good", "color": "🟣", "desc": "Above average"}
    if rate >= 1.5: return {"label": "Average", "color": "🟡", "desc": "Industry average"}
    if rate >= 0.5: return {"label": "Below Avg", "color": "🟠", "desc": "Needs improvement"}
    if rate > 0: return {"label": "Low", "color": "🔴", "desc": "Audience not engaging"}
    return {"label": "No data", "color": "⚪", "desc": "Need views to measure"}


def get_retention_rating(retention: float) -> Dict[str, str]:
    if retention >= 60: return {"label": "Excellent", "desc": "Top creator level"}
    if retention >= 45: return {"label": "Good", "desc": "Above average retention"}
    if retention >= 30: return {"label": "Fair", "desc": "Average retention"}
    if retention >= 20: return {"label": "Poor", "desc": "Below average retention"}
    return {"label": "Critical", "desc": "Critical retention issues"}


def diagnose_video(
    views: int, likes: int, comments: int,
    published_at: str = "", subscribers: int = 1000,
    ctr: float = None, retention: float = None,
) -> List[Dict[str, str]]:
    issues = []
    if views == 0:
        return [{"issue": "Video has 0 views", "severity": "critical",
                  "fix": "Check if public/unlisted. Promote on social. Update thumbnail & title."}]

    days_since = 1
    if published_at:
        try:
            pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            days_since = max(1, (datetime.now(pub.tzinfo) - pub).total_seconds() / 86400)
        except Exception:
            pass
    vpd = views / days_since
    eng = ((likes + comments) / views) * 100
    reach = (views / max(subscribers, 1)) * 100

    if ctr is not None:
        if ctr < 2:
            issues.append({"issue": f"CTR critically low: {ctr:.1f}% (target: >4%)", "severity": "critical",
                           "fix": f"Thumbnail failing. {(100-ctr):.0f}% skip your video. Redesign thumbnail."})
        elif ctr < 4:
            issues.append({"issue": f"CTR below average: {ctr:.1f}%", "severity": "warning",
                           "fix": "Try A/B testing thumbnails. Make titles more specific."})
    if retention is not None:
        if retention < 20:
            issues.append({"issue": f"Retention critically low: {retention:.1f}%", "severity": "critical",
                           "fix": "Viewer drop-off is severe. Improve hook in first 30 seconds."})
        elif retention < 40:
            issues.append({"issue": f"Retention below average: {retention:.1f}%", "severity": "warning",
                           "fix": "Add pattern interrupts, tighter editing, or chapter markers."})

    if eng < 1:
        issues.append({"issue": f"Engagement very low: {eng:.2f}%", "severity": "warning",
                       "fix": "Ask questions, add CTAs, create controversy or emotional hook."})
    if vpd < 1 and days_since > 7:
        issues.append({"issue": f"Views velocity low: {vpd:.1f}/day", "severity": "minor",
                       "fix": "Share on social media, optimize SEO, cross-promote."})
    if reach < 5 and subscribers > 100:
        issues.append({"issue": f"Only reaching {reach:.1f}% of subscribers", "severity": "minor",
                       "fix": "Check if notifications are on. YouTube may not push to all subs."})

    if not issues:
        issues.append({"issue": "No major issues detected", "severity": "good",
                       "fix": "This video is performing well. Keep this content style."})
    return issues


def get_score_label(score: int) -> str:
    if score >= 70: return "🔥 Excellent"
    if score >= 50: return "✅ Good"
    if score >= 30: return "⚠️ Fair"
    if score > 0: return "❌ Poor"
    return "⚪ No data"


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _parse_duration(iso: str) -> int:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "PT0S")
    if not m: return 0
    return (int(m.group(1) or 0) * 3600) + (int(m.group(2) or 0) * 60) + int(m.group(3) or 0)


def _format_duration(seconds: int) -> str:
    if seconds >= 3600:
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        return f"{h}:{m:02d}:{s:02d}"
    m, s = divmod(seconds, 60)
    return f"{m}:{s:02d}"


def format_number(num: int) -> str:
    if num >= 1_000_000_000: return f"{num/1e9:.1f}B"
    if num >= 1_000_000: return f"{num/1e6:.1f}M"
    if num >= 1_000: return f"{num/1e3:.1f}K"
    return str(num)


def is_configured() -> bool:
    return bool(_get_secret("YOUTUBE_API_KEY") and _get_secret("YOUTUBE_CHANNEL_ID"))


def has_analytics_access() -> bool:
    return bool(_get_oauth_token())


def get_status() -> Dict[str, Any]:
    return {
        "data_api": bool(_get_secret("YOUTUBE_API_KEY")),
        "channel_id": bool(_get_secret("YOUTUBE_CHANNEL_ID")),
        "analytics_api": bool(_get_oauth_token()),
        "configured": bool(_get_secret("YOUTUBE_API_KEY") and _get_secret("YOUTUBE_CHANNEL_ID")),
        "full_access": bool(_get_secret("YOUTUBE_API_KEY") and _get_secret("YOUTUBE_CHANNEL_ID") and _get_oauth_token()),
    }
