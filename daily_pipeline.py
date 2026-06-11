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
        if not is_configured():
            log("YouTube: Not configured — skipping")
            return
        log("YouTube: Fetching channel info...")
        ch = get_channel_info()
        log(f"YouTube: Channel = {ch.get('title', 'N/A')}, Subscribers = {ch.get('subscribers', 0):,}")
        log("YouTube: Fetching video list...")
        videos = get_channel_videos(max_videos=200)
        log(f"YouTube: Got {len(videos)} videos")
        # Analytics (if OAuth token available)
        try:
            from youtube_connector import has_analytics_access, get_daily_analytics
            if has_analytics_access():
                from datetime import timedelta
                start_d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                end_d = datetime.now().strftime("%Y-%m-%d")
                daily = get_daily_analytics(start_d, end_d)
                log(f"YouTube Analytics: Got {len(daily)} days of data")
        except Exception as e:
            log(f"YouTube Analytics: Skipped ({e})")
    ok5, e5 = run_stage(5, "YouTube Data Fetch", s5)
    results["stage5_youtube"] = "ok" if ok5 else f"failed: {e5}"

    # ── LinkedIn Scrape (Stage 6) ──
    def s6():
        from linkedin_connector import (
            scrape_public_metrics, scrape_with_playwright,
            has_cookies, is_configured, get_status,
        )
        if not is_configured():
            log("LinkedIn: Not configured — skipping")
            return
        log("LinkedIn: Starting scrape...")
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
    ok6, e6 = run_stage(6, "LinkedIn Scrape", s6)
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
