from notifications import daily_summary, alert_pipeline_failure
from pipeline_health import record_success, record_failure
"""
daily_pipeline.py
MASTER ORCHESTRATOR - all 7 layers

Layer 1: Scrape sources (KPI + Stripe)
Layer 2: Raw data lake (Sheets)
Layer 3: Email validation
Layer 4: Deduplication
Layer 5: ML scoring
Layer 6: Reporting (Daily_Counts + email/Slack notifications)
Layer 7: Scheduling (this script - runs via GitHub Actions cron)
"""
import sys
import traceback
from datetime import datetime

from storage_adapter import get_storage_status, SHEETS_AVAILABLE
from sheets_writer import write_run_summary


def log(msg):
    print(msg, flush=True)


def run_stage(num: int, name: str, func) -> tuple:
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

    # Stage 1: Scrape KPI dashboard (Layer 1+2)
    def s1():
        from scrape_kpi import main as run
        run()
    ok1, e1 = run_stage(1, "Scrape KPI (Layer 1+2)", s1)
    results["stage1_kpi"] = "ok" if ok1 else f"failed: {e1}"

    # Stage 2: Scrape Stripe (Layer 1+2)
    def s2():
        from scrape_stripe import main as run
        run()
    ok2, e2 = run_stage(2, "Scrape Stripe (Layer 1+2)", s2)
    results["stage2_stripe"] = "ok" if ok2 else f"failed: {e2}"

    # Stage 3: Process - validation + dedup + ML (Layers 3, 4, 5)
    def s3():
        from process_data import main as run
        run()
    ok3, e3 = run_stage(3, "Process: Validate + Dedup + ML (Layers 3-5)", s3)
    results["stage3_process"] = "ok" if ok3 else f"failed: {e3}"

    # Stage 4: Daily counts (Layer 6 part 1)
    def s4():
        from daily_counts import build_daily_counts_table
        build_daily_counts_table()
    ok4, e4 = run_stage(4, "Build Daily/Monthly Counts (Layer 6)", s4)
    results["stage4_counts"] = "ok" if ok4 else f"failed: {e4}"

    # Stage 5: Reporting + notifications (Layer 6 part 2)
    def s5():
        from reporting_engine import main as run
        run()
    ok5, e5 = run_stage(5, "Reporting + Notifications (Layer 6)", s5)
    results["stage5_report"] = "ok" if ok5 else f"failed: {e5}"

    duration = (datetime.now() - start).total_seconds()
    passed   = sum([ok1, ok2, ok3, ok4, ok5])

    log(f"\n{'='*70}")
    log(f"PIPELINE DONE: {passed}/5 stages passed | {duration:.1f}s")
    log(f"{'='*70}")
    for k, v in results.items():
        icon = "OK" if v == "ok" else "FAIL"
        log(f"  [{icon}] {k}: {v}")

    # Final state
    log(f"\n{'='*60}")
    log("FINAL SHEETS STATE")
    log(f"{'='*60}")
    try:
        from sheets_writer import read_tab_data
        for tab in ["Raw_FREE","Raw_FIRST_UPLOAD","Raw_STRIPE",
                    "Verified_FREE","Verified_FIRST_UPLOAD","Verified_STRIPE",
                    "Daily_Counts","Monthly_Counts"]:
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

    # Send daily summary notification
    try:
        from sheets_writer import read_tab_data
        
        today = datetime.now().strftime("%Y-%m-%d")
        current_month = datetime.now().strftime("%Y-%m")
        
        rows = read_tab_data("Daily_Counts")
        
        today_row = next((r for r in rows if r.get("Date") == today), {})
        month_rows = [r for r in rows if r.get("Date","").startswith(current_month)]
        
        stats = {
            "signups_today": int(today_row.get("SignUps_Accepted", 0) or 0),
            "uploads_today": int(today_row.get("FirstUploads_Accepted", 0) or 0),
            "paid_today": int(today_row.get("PaidSubscribers_Accepted", 0) or 0),
            "signups_month": sum(int(r.get("SignUps_Accepted", 0) or 0) for r in month_rows),
            "uploads_month": sum(int(r.get("FirstUploads_Accepted", 0) or 0) for r in month_rows),
            "paid_month": sum(int(r.get("PaidSubscribers_Accepted", 0) or 0) for r in month_rows),
            "signups_alltime": sum(int(r.get("SignUps_Accepted", 0) or 0) for r in rows),
            "uploads_alltime": sum(int(r.get("FirstUploads_Accepted", 0) or 0) for r in rows),
            "paid_alltime": sum(int(r.get("PaidSubscribers_Accepted", 0) or 0) for r in rows),
        }
        
        log(f"\nSending daily summary notifications...")
        daily_summary(stats)
    except Exception as e:
        log(f"Daily summary error (non-fatal): {e}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
