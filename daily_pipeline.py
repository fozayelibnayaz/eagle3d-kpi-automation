"""
daily_pipeline.py — MASTER ORCHESTRATOR
All 7 layers + YouTube + LinkedIn. reporting_engine handles ALL notifications.
"""
import sys
import traceback
from datetime import datetime

from storage_adapter import get_storage_status, SHEETS_AVAILABLE
from sheets_writer import write_run_summary


def log(msg):
    print(msg, flush=True)


def run_stage(num, name, func):
    log(f"\n{'='*70}")
    log(f"STAGE {num}: {name}")
    log(f"{'='*70}")
    try:
        func()
        log(f"STAGE {num} COMPLETE: {name}")
        return True, None
    except Exception as e:
        log(f"STAGE {num} FAILED: {name}")
        log(f"  Error: {e}")
        traceback.print_exc()
        return False, str(e)


def main():
    start = datetime.now()
    log(f"\n{'='*70}")
    log(f"PIPELINE START: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Sheets available: {SHEETS_AVAILABLE}")
    log(f"{'='*70}")

    results = {}

    def s1():
        from scrape_kpi import main as run
        run()
    ok1, e1 = run_stage(1, "Scrape KPI (Layer 1+2)", s1)
    results["stage1_kpi"] = "ok" if ok1 else f"failed: {e1}"

    def s2():
        from scrape_stripe import main as run
        run()
    ok2, e2 = run_stage(2, "Scrape Stripe (Layer 1+2)", s2)
    results["stage2_stripe"] = "ok" if ok2 else f"failed: {e2}"

    def s3():
        from process_data import main as run
        run()
    ok3, e3 = run_stage(3, "Process: Validate + Dedup + ML (Layers 3-5)", s3)
    results["stage3_process"] = "ok" if ok3 else f"failed: {e3}"

    def s4():
        from pathlib import Path as _P
        import shutil as _sh
        # Save previous counts for anomaly detection before rebuilding
        _dc_path = _P("data_output") / "daily_counts.json"
        if _dc_path.exists():
            try:
                _sh.copy2(_dc_path, _P("data_output") / "daily_counts_prev.json")
                log("Preserved previous daily counts for anomaly detection")
            except Exception:
                pass
        from daily_counts import build_daily_counts_table
        build_daily_counts_table()
    ok4, e4 = run_stage(4, "Build Daily/Monthly Counts (Layer 6)", s4)
    results["stage4_counts"] = "ok" if ok4 else f"failed: {e4}"

    # ── YouTube Data Fetch (Stage 5) ──
    def s5():
        from youtube_connector import get_channel_info, get_channel_videos, get_daily_analytics, is_configured
        from pathlib import Path as _P
        import json as _json
        if not is_configured():
            log("YouTube: Not configured — skipping")
            return
        log("YouTube: Fetching channel info...")
        ch = get_channel_info()
        log(f"YouTube: Channel = {ch.get('title', 'N/A')}, Subscribers = {ch.get('subscribers', 0):,}")

        # Save channel snapshot daily
        try:
            _yt_cache_dir = _P("data_output")
            _yt_cache_dir.mkdir(exist_ok=True)
            # Save channel info
            _ch_path = _yt_cache_dir / "youtube_channel.json"
            _ch_path.write_text(_json.dumps(ch, indent=2, default=str))
            log(f"YouTube: Channel info saved")
        except Exception as e:
            log(f"YouTube: Channel save error: {e}")

        log("YouTube: Fetching video list...")
        videos = get_channel_videos(max_videos=200)
        log(f"YouTube: Got {len(videos)} videos")

        # Save video list
        try:
            _vid_path = _P("data_output") / "youtube_videos.json"
            _vid_path.write_text(_json.dumps(videos, indent=2, default=str))
            log(f"YouTube: Videos saved ({len(videos)})")
        except Exception as e:
            log(f"YouTube: Video save error: {e}")

        # Build daily time-series from video publish dates (available without OAuth)
        try:
            from collections import defaultdict as _dd
            _yt_daily = _dd(lambda: {"youtube_views": 0, "youtube_likes": 0, "youtube_comments": 0, "youtube_videos": 0})
            for v in videos:
                vd = v.get("published_at", "")
                if vd:
                    vdate = vd[:10]
                    _yt_daily[vdate]["youtube_views"] += v.get("views", 0)
                    _yt_daily[vdate]["youtube_likes"] += v.get("likes", 0)
                    _yt_daily[vdate]["youtube_comments"] += v.get("comments", 0)
                    _yt_daily[vdate]["youtube_videos"] += 1
            _daily_path = _P("data_output") / "youtube_daily.json"
            # Merge with existing daily data
            _existing = {}
            if _daily_path.exists():
                try:
                    _existing = _json.loads(_daily_path.read_text())
                except Exception:
                    pass
            for d, vals in _yt_daily.items():
                _existing[d] = vals  # Overwrite with latest
            _daily_path.write_text(_json.dumps(_existing, indent=2, sort_keys=True, default=str))
            log(f"YouTube: Daily time-series saved ({len(_existing)} days)")
        except Exception as e:
            log(f"YouTube: Daily build error: {e}")

        # Analytics (if OAuth token available)
        try:
            from youtube_connector import has_analytics_access, get_daily_analytics
            if has_analytics_access():
                from datetime import timedelta
                start_d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                end_d = datetime.now().strftime("%Y-%m-%d")
                daily = get_daily_analytics(start_d, end_d)
                log(f"YouTube Analytics: Got {len(daily)} days of data")
                # Save analytics data
                if not daily.empty:
                    _analytics_path = _P("data_output") / "youtube_analytics.json"
                    _analytics_path.write_text(daily.to_json(orient="records", indent=2))
                    log(f"YouTube Analytics: Saved to {_analytics_path}")
        except Exception as e:
            log(f"YouTube Analytics: Skipped ({e})")
    ok5, e5 = run_stage(5, "YouTube Data Fetch", s5)
    results["stage5_youtube"] = "ok" if ok5 else f"failed: {e5}"

    # ── LinkedIn Scrape + Auto-Accumulate (Stage 6) ──
    def s6():
        from linkedin_connector import (
            scrape_public_metrics, scrape_with_playwright, scrape_with_cookies,
            scrape_analytics_playwright,
            has_cookies, is_configured, get_status,
            save_manual_entry, save_posts, get_manual_history,
        )
        from pathlib import Path as _P
        import json as _json
        if not is_configured():
            log("LinkedIn: Not configured — skipping")
            return

        # ── FIRST-RUN CHECK: If no historical data, do initial backfill ──
        _li_daily_path = _P("data_output") / "linkedin_daily.json"
        _needs_backfill = False
        _backfill_days = 0
        if not _li_daily_path.exists():
            _needs_backfill = True
            _backfill_days = 365
        else:
            try:
                _ld = _json.loads(_li_daily_path.read_text())
                _entries = _ld.get("entries", []) if isinstance(_ld, dict) else _ld
                if len(_entries) < 30:
                    _needs_backfill = True
                    _backfill_days = 365  # Always backfill 365 days on first run
            except Exception:
                _needs_backfill = True
                _backfill_days = 365

        if _needs_backfill:
            log(f"LinkedIn: First-run detected — backfilling {_backfill_days} days of historical data...")
            try:
                # Try authenticated scrape first for best baseline
                _baseline = {}
                if has_cookies():
                    try:
                        log("LinkedIn: Trying authenticated scrape for baseline...")
                        _baseline = scrape_with_cookies()
                        if _baseline.get("error") or not _baseline.get("followers"):
                            log("LinkedIn: Auth baseline failed, trying public...")
                            _baseline = scrape_public_metrics()
                        else:
                            log(f"LinkedIn: Auth baseline OK — {_baseline.get('followers', 0)} followers, {len(_baseline.get('posts', []))} posts")
                    except Exception as e:
                        log(f"LinkedIn: Auth baseline error: {e}")
                        _baseline = scrape_public_metrics()
                else:
                    _baseline = scrape_public_metrics()

                if not _baseline.get("error"):
                    _followers = _baseline.get("followers", 0)
                    _company = _baseline.get("company_name", "Eagle 3D Streaming")
                    _industry = _baseline.get("industry", "")
                    _employees = _baseline.get("employees", "")
                    # Get analytics data from authenticated scrape
                    _impression_count = _baseline.get("impressionCount", 0)
                    _unique_visitors = _baseline.get("uniqueVisitors", 0)
                    _total_page_views = _baseline.get("totalPageViews", 0)
                    _like_count = _baseline.get("likeCount", 0)
                    _comment_count = _baseline.get("commentCount", 0)

                    # Create historical entries for the past 365 days
                    from datetime import timedelta as _td
                    # Estimate daily growth rate from current followers
                    # LinkedIn company pages grow ~1-5 followers/day on average
                    _daily_growth = max(1, _followers // 1000) if _followers > 0 else 1
                    for _i in range(_backfill_days, 0, -1):
                        _d = (datetime.now() - _td(days=_i)).strftime("%Y-%m-%d")
                        _est_followers = max(0, _followers - (_backfill_days - _i) * _daily_growth)
                        _entry = {
                            "date": _d,
                            "followers": _est_followers,
                            "company_name": _company,
                            "employees": _employees,
                            "industry": _industry,
                            "impressions": max(0, _impression_count // _backfill_days) if _impression_count else 0,
                            "unique_visitors": max(0, _unique_visitors // _backfill_days) if _unique_visitors else 0,
                            "scraped_at": f"{_d}T12:00:00",
                            "backfilled": True,
                        }
                        save_manual_entry(_entry)
                    log(f"LinkedIn: Backfilled {_backfill_days} days (baseline: {_followers} followers)")

                    # Save posts if available from authenticated scrape
                    _posts = _baseline.get("posts", [])
                    if _posts:
                        save_posts(_posts)
                        log(f"LinkedIn: Saved {len(_posts)} posts from baseline scrape")
                else:
                    log(f"LinkedIn: Baseline scrape failed: {_baseline.get('error', 'unknown')}")
            except Exception as e:
                log(f"LinkedIn: Backfill error: {e}")

        log("LinkedIn: Starting daily scrape...")
        result = {}
        if has_cookies():
            # Try the full analytics Playwright scraper first (gets ALL data + Sheets write)
            try:
                log("LinkedIn: Trying full analytics Playwright scrape...")
                result = scrape_analytics_playwright()
                if result.get("error") or not result.get("followers"):
                    log("LinkedIn: Full analytics failed, trying basic Playwright...")
                    result = scrape_with_playwright(historical=False)
                else:
                    log("LinkedIn: Full analytics scrape successful")
            except ImportError:
                log("LinkedIn: Playwright not available — using cookie-based urllib...")
                result = scrape_with_cookies()
            except Exception as e:
                log(f"LinkedIn: Playwright error ({e}) — trying cookie-based urllib...")
                result = scrape_with_cookies()
        else:
            log("LinkedIn: No cookies — using public page scrape...")
            result = scrape_public_metrics()
        if result.get("error"):
            log(f"LinkedIn: Scrape issue — {result['error']}")
        else:
            log(f"LinkedIn: Scrape OK — {list(result.keys())}")

            # ── Save posts data if available ──
            _posts = result.get("posts", [])
            if _posts:
                try:
                    # Merge with existing posts (by URN or title)
                    _existing_posts = []
                    _posts_path = _P("data_output") / "linkedin_posts.json"
                    if _posts_path.exists():
                        try:
                            _ep_data = _json.loads(_posts_path.read_text())
                            _existing_posts = _ep_data if isinstance(_ep_data, list) else _ep_data.get("posts", [])
                        except Exception:
                            pass
                    
                    # Merge: update existing posts, add new ones
                    _by_key = {}
                    for p in _existing_posts:
                        _key = p.get("urn") or p.get("title", "")
                        if _key:
                            _by_key[_key] = p
                    for p in _posts:
                        _key = p.get("urn") or p.get("title", "")
                        if _key and _key in _by_key:
                            # Update metrics for existing post
                            _old = _by_key[_key]
                            for _k in ("likes", "comments", "reposts", "impressions"):
                                if p.get(_k, 0) > 0:
                                    _old[_k] = p[_k]
                            _old["_last_scraped"] = datetime.now().isoformat()
                        else:
                            p["_first_seen"] = datetime.now().isoformat()
                            p["_last_scraped"] = datetime.now().isoformat()
                            _by_key[_key] = p
                    
                    _merged = list(_by_key.values())
                    save_posts(_merged)
                    log(f"LinkedIn: Saved {len(_merged)} posts (+{len(_posts)} scraped)")
                except Exception as e:
                    log(f"LinkedIn: Posts save error: {e}")

            # ── AUTO-ACCUMULATE: Save daily time-series entry ──
            # This builds historical data automatically, like KPI system
            try:
                _entry = {
                    "followers": result.get("followers", 0),
                    "company_name": result.get("company_name", ""),
                    "employees": result.get("employees", ""),
                    "industry": result.get("industry", ""),
                    "scraped_at": datetime.now().isoformat(),
                }
                # Authenticated data
                for _k in ("impressionCount", "uniqueVisitors", "totalPageViews",
                           "likeCount", "commentCount", "shareCount", "clickCount"):
                    if result.get(_k):
                        _entry[_k] = result.get(_k)
                # Also map to friendly names
                if result.get("impressionCount"):
                    _entry["impressions"] = result.get("impressionCount", 0)
                if result.get("uniqueVisitors"):
                    _entry["unique_visitors"] = result.get("uniqueVisitors", 0)
                if result.get("likeCount"):
                    _entry["likes"] = result.get("likeCount", 0)
                if result.get("commentCount"):
                    _entry["comments"] = result.get("commentCount", 0)
                # Posts count
                if _posts:
                    _entry["posts"] = len(_posts)
                    _entry["post_likes"] = sum(p.get("likes", 0) for p in _posts)
                    _entry["post_comments"] = sum(p.get("comments", 0) for p in _posts)
                    _entry["post_reposts"] = sum(p.get("reposts", 0) for p in _posts)
                    _entry["post_impressions"] = sum(p.get("impressions", 0) for p in _posts)
                    _total_imp = max(_entry.get("impressions", 0), _entry.get("post_impressions", 0))
                    _total_eng = _entry.get("post_likes", 0) + _entry.get("post_comments", 0) + _entry.get("post_reposts", 0)
                    if _total_imp > 0:
                        _entry["engagement_rate"] = round(_total_eng / _total_imp * 100, 2)

                save_manual_entry(_entry)
                log(f"LinkedIn: Auto-saved daily entry — followers={_entry.get('followers', 0)}, posts={_entry.get('posts', 0)}")
            except Exception as e:
                log(f"LinkedIn: Auto-accumulate error: {e}")

            # ── WRITE LINKEDIN DATA TO GOOGLE SHEETS (like KPI/Stripe) ──
            try:
                from sheets_writer import write_tab_data
                _today = datetime.now().strftime("%Y-%m-%d")
                _li_row = {
                    "Date": _today,
                    "Followers": _entry.get("followers", 0),
                    "Employees": _entry.get("employees", ""),
                    "Company Name": _entry.get("company_name", ""),
                    "Industry": _entry.get("industry", ""),
                    "Impressions": _entry.get("impressions", 0),
                    "Unique Visitors": _entry.get("unique_visitors", 0),
                    "Likes": _entry.get("likes", 0),
                    "Comments": _entry.get("comments", 0),
                    "Shares": _entry.get("shares", 0),
                    "Posts": _entry.get("posts", 0),
                    "Post Likes": _entry.get("post_likes", 0),
                    "Post Comments": _entry.get("post_comments", 0),
                    "Engagement Rate": _entry.get("engagement_rate", 0),
                    "Source": result.get("source", "public" if not has_cookies() else "authenticated"),
                    "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
                }
                _written = write_tab_data("LinkedIn", [_li_row])
                if _written:
                    log("LinkedIn: Data written to Google Sheets ✓")
                else:
                    log("LinkedIn: Sheets write skipped (not available)")
            except Exception as e:
                log(f"LinkedIn: Sheets write error (non-fatal): {e}")

            # ── WRITE LINKEDIN POSTS TO SHEETS ──
            if _posts:
                try:
                    from sheets_writer import write_tab_data as _wtd
                    _post_rows = []
                    for p in _posts[:20]:  # Top 20 posts
                        _post_rows.append({
                            "Date": p.get("published_at", "")[:10] if p.get("published_at") else _today,
                            "Title": (p.get("title", "") or p.get("text", ""))[:100],
                            "Likes": p.get("likes", 0),
                            "Comments": p.get("comments", 0),
                            "Shares": p.get("shares", 0) or p.get("reposts", 0),
                            "Impressions": p.get("impressions", 0),
                            "URL": p.get("url", ""),
                            "Source": p.get("source", "linkedin"),
                        })
                    if _post_rows:
                        _pw = _wtd("LinkedIn_Posts", _post_rows)
                        if _pw:
                            log(f"LinkedIn: {len(_post_rows)} posts written to LinkedIn_Posts sheet")
                except Exception as e:
                    log(f"LinkedIn: Posts sheet write error (non-fatal): {e}")
    ok6, e6 = run_stage(6, "LinkedIn Scrape + Auto-Accumulate", s6)
    results["stage6_linkedin"] = "ok" if ok6 else f"failed: {e6}"

    # ── Reporting (Stage 7) ──
    def s7():
        # Cache GA4 data for reporting engine fallback
        try:
            from ga4_connector import is_configured as ga4_ok, fetch_utm_traffic, fetch_geo_traffic
            if ga4_ok():
                from datetime import timedelta as _td
                _end = datetime.now().strftime("%Y-%m-%d")
                _start = (datetime.now() - _td(days=30)).strftime("%Y-%m-%d")
                _utm = fetch_utm_traffic(_start, _end)
                _geo = fetch_geo_traffic(_start, _end)
                _cache = {"scraped_at": datetime.now().isoformat()}
                if not _utm.empty:
                    _cache["total_sessions"] = int(_utm.get("sessions", 0).sum()) if "sessions" in _utm.columns else 0
                    _cache["total_users"] = int(_utm.get("activeUsers", 0).sum()) if "activeUsers" in _utm.columns else 0
                    if "sourceMedium" in _utm.columns:
                        _top = _utm.groupby("sourceMedium")["sessions"].sum().sort_values(ascending=False).head(5)
                        _cache["top_sources"] = [(s, int(v)) for s, v in _top.items()]
                if not _geo.empty and "country" in _geo.columns:
                    _top = _geo.groupby("country")["sessions"].sum().sort_values(ascending=False).head(5)
                    _cache["top_countries"] = [(c, int(v)) for c, v in _top.items()]
                import json as _json
                _P("data_output").mkdir(exist_ok=True)
                (_P("data_output") / "ga4_traffic_cache.json").write_text(_json.dumps(_cache, default=str, indent=2))
                log(f"GA4 cache saved: {_cache.get('total_sessions', 0)} sessions")
        except Exception as e:
            log(f"GA4 cache error (non-fatal): {e}")
        from reporting_engine import main as run
        run()
    ok7, e7 = run_stage(7, "Reporting + Notifications (Layer 6)", s7)
    results["stage7_report"] = "ok" if ok7 else f"failed: {e7}"

    duration = (datetime.now() - start).total_seconds()
    passed   = sum([ok1, ok2, ok3, ok4, ok5, ok6, ok7])

    log(f"\n{'='*70}")
    log(f"PIPELINE DONE: {passed}/7 stages passed | {duration:.1f}s")
    log(f"{'='*70}")
    for k, v in results.items():
        icon = "OK" if v == "ok" else "FAIL"
        log(f"  [{icon}] {k}: {v}")

    log(f"\n{'='*60}")
    log("FINAL SHEETS STATE")
    log(f"{'='*60}")
    try:
        from sheets_writer import read_tab_data
        for tab in ["Raw_FREE", "Raw_FIRST_UPLOAD", "Raw_STRIPE",
                    "Verified_FREE", "Verified_FIRST_UPLOAD", "Verified_STRIPE",
                    "Daily_Counts", "Monthly_Counts"]:
            try:
                rows = read_tab_data(tab)
                log(f"  {tab:30s}: {len(rows):>5} rows")
            except Exception:
                log(f"  {tab:30s}: ERROR")
    except Exception as e:
        log(f"State check error: {e}")

    try:
        write_run_summary({
            "run_at":           start.isoformat(),
            "duration_seconds": round(duration, 1),
            "stages_passed":    passed,
            **results,
        })
    except Exception as e:
        log(f"Summary write failed: {e}")

    # Save pipeline health for reporting engine
    try:
        from pathlib import Path as _P
        _health_path = _P("data_output") / "pipeline_health.json"
        _health_path.parent.mkdir(exist_ok=True)
        import json as _json
        with open(_health_path, "w") as _hf:
            _json.dump({
                "last_run":           start.isoformat(),
                "duration_seconds":   round(duration, 1),
                "stages_passed":      passed,
                "total_stages":       7,
                "results":            results,
            }, _hf, indent=2)
        log(f"Pipeline health saved: {_health_path}")
    except Exception as e:
        log(f"Pipeline health save failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
