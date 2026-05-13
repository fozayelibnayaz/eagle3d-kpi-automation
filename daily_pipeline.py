"""
daily_pipeline.py - Master pipeline
Runs 4 stages. Each stage is independent - failure of one does not stop others.
"""
import sys
import traceback
from datetime import datetime

# Direct imports - no circular dependencies
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

    # Stage 1: Scrape KPI
    def s1():
        from scrape_kpi import main as run
        run()
    ok1, e1 = run_stage(1, "Scrape KPI Dashboard", s1)
    results["stage1_kpi"] = "ok" if ok1 else f"failed: {e1}"

    # Stage 2: Scrape Stripe
    def s2():
        from scrape_stripe import main as run
        run()
    ok2, e2 = run_stage(2, "Scrape Stripe", s2)
    results["stage2_stripe"] = "ok" if ok2 else f"failed: {e2}"

    # Stage 3: Process data
    def s3():
        from process_data import main as run
        run()
    ok3, e3 = run_stage(3, "Process -> Verified", s3)
    results["stage3_process"] = "ok" if ok3 else f"failed: {e3}"

    # Stage 4: Daily counts
    def s4():
        from daily_counts import build_daily_counts_table
        build_daily_counts_table()
    ok4, e4 = run_stage(4, "Build Daily/Monthly Counts", s4)
    results["stage4_counts"] = "ok" if ok4 else f"failed: {e4}"

    # Summary
    duration = (datetime.now() - start).total_seconds()
    passed   = sum([ok1, ok2, ok3, ok4])

    log(f"\n{'='*70}")
    log(f"PIPELINE DONE: {passed}/4 stages passed | {duration:.1f}s")
    log(f"{'='*70}")
    for k, v in results.items():
        icon = "OK" if v == "ok" else "FAIL"
        log(f"  [{icon}] {k}: {v}")

    # Storage status
    log(f"\n{'='*60}")
    log("STORAGE STATUS")
    log(f"{'='*60}")
    try:
        status = get_storage_status()
        log(f"Sheets: {'YES' if status['sheets_available'] else 'NO (CSV fallback)'}")
        for fname, info in sorted(status["files"].items()):
            rows = info.get("rows","?")
            size = info.get("size_kb","?")
            log(f"  {fname:45s} {rows:>5} rows  {size:>6}KB")
    except Exception as e:
        log(f"Status error: {e}")
    log(f"{'='*60}")

    # Write run summary
    try:
        write_run_summary({
            "run_at":    start.isoformat(),
            "duration":  round(duration, 1),
            "passed":    passed,
            **results,
        })
    except Exception as e:
        log(f"Summary write failed: {e}")

    # Exit 0 always (partial success is still useful)
    return 0


if __name__ == "__main__":
    sys.exit(main())
