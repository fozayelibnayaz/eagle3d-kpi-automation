"""
daily_pipeline.py - Master runner
All stages write to Google Sheets (primary).
CSV only if Sheets fails.
"""
import traceback
from datetime import datetime
from sheets_writer import write_run_summary
from storage_adapter import get_storage_status


def log(msg):
    print(msg, flush=True)


def run_stage(num: int, name: str, func) -> tuple:
    log(f"\n{'='*70}")
    log(f">>> STAGE {num} - {name}")
    log(f"{'='*70}")
    try:
        func()
        log(f"STAGE {num} COMPLETE: {name}")
        return True, None
    except Exception as e:
        log(f"STAGE {num} FAILED: {name} - {e}")
        traceback.print_exc()
        return False, str(e)


def main():
    start = datetime.now()
    log(f"\n{'='*70}")
    log(f"PIPELINE START: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"{'='*70}")

    results = {}

    ok1, e1 = run_stage(1, "Scrape KPI dashboard",
        lambda: __import__("scrape_kpi").main())
    results["stage1"] = "ok" if ok1 else f"failed: {e1}"

    ok2, e2 = run_stage(2, "Scrape Stripe customers",
        lambda: __import__("scrape_stripe").main())
    results["stage2"] = "ok" if ok2 else f"failed: {e2}"

    ok3, e3 = run_stage(3, "Process -> Verified sheets",
        lambda: __import__("process_data").main())
    results["stage3"] = "ok" if ok3 else f"failed: {e3}"

    ok4, e4 = run_stage(4, "Build Daily + Monthly counts",
        lambda: __import__("daily_counts").build_daily_counts_table())
    results["stage4"] = "ok" if ok4 else f"failed: {e4}"

    duration = (datetime.now() - start).total_seconds()

    log(f"\n{'='*70}")
    log(f"PIPELINE COMPLETE - {duration:.1f}s")
    log(f"{'='*70}")
    log(f"Stages: {sum([ok1,ok2,ok3,ok4])}/4 passed")
    for k, v in results.items():
        icon = "OK" if v == "ok" else "FAILED"
        log(f"  {k}: {icon} - {v}")

    # Storage status
    log(f"\n{'='*60}")
    log("STORAGE STATUS")
    log(f"{'='*60}")
    status = get_storage_status()
    sheets = "YES" if status["sheets_available"] else "NO (CSV fallback)"
    log(f"Google Sheets: {sheets}")
    for fname, info in sorted(status["files"].items()):
        if not any(x in fname for x in ("TEST","FREE_TEST")):
            rows = info.get("rows","?")
            size = info.get("size_kb","?")
            log(f"  {fname:45s} {rows:>5} rows  {size:>6}KB")
    log(f"{'='*60}")

    try:
        write_run_summary({
            "run_at":           start.isoformat(),
            "duration_seconds": round(duration, 1),
            "stages_passed":    sum([ok1, ok2, ok3, ok4]),
            **results,
        })
    except Exception as e:
        log(f"Summary write failed: {e}")

    return 0


if __name__ == "__main__":
    exit(main())
