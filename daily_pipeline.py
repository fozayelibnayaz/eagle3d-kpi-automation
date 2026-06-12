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
            scrape_public_metrics, scrape_with_playwright,
            has_cookies, is_configured, get_status,
            save_manual_entry,
        )
        from pathlib import Path as _P
        import json as _json
        if not is_configured():
            log("LinkedIn: Not configured — skipping")
            return
        log("LinkedIn: Starting scrape...")
        result = {}
        if has_cookies():
            log("LinkedIn: Using authenticated scrape...")
            result = scrape_with_playwright(historical=False)
        else:
            log("LinkedIn: Using public page scrape...")
            result = scrape_public_metrics()
        if result.get("error"):
            log(f"LinkedIn: Scrape issue — {result['error']}")
        else:
            log(f"LinkedIn: Scrape OK — {list(result.keys())}")

            # AUTO-ACCUMULATE: Save daily time-series entry from scraped data
            # This is the KEY FIX — LinkedIn gets historical data automatically, like KPI system
            try:
                _entry = {
                    "followers": result.get("followers", 0),
                    "company_name": result.get("company_name", ""),
                    "employees": result.get("employees", ""),
                    "industry": result.get("industry", ""),
                    "scraped_at": datetime.now().isoformat(),
                }
                # Also add authenticated data if available
                if result.get("impressions"):
                    _entry["impressions"] = result.get("impressions", 0)
                if result.get("engagement_rate"):
                    _entry["engagement_rate"] = result.get("engagement_rate", 0)
                if result.get("unique_visitors"):
                    _entry["unique_visitors"] = result.get("unique_visitors", 0)
                if result.get("likes"):
                    _entry["likes"] = result.get("likes", 0)
                if result.get("comments"):
                    _entry["comments"] = result.get("comments", 0)
                if result.get("posts"):
                    _entry["posts"] = result.get("posts", 0)

                save_manual_entry(_entry)
                log(f"LinkedIn: Auto-saved daily entry — followers={_entry.get('followers', 0)}")
            except Exception as e:
                log(f"LinkedIn: Auto-accumulate error: {e}")
    ok6, e6 = run_stage(6, "LinkedIn Scrape + Auto-Accumulate", s6)
    results["stage6_linkedin"] = "ok" if ok6 else f"failed: {e6}"

    # ── Reporting (Stage 7) ──
    def s7():
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
