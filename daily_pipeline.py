from storage_adapter import write_tab_data, write_run_summary, get_storage_status
"""
DAILY PIPELINE
Runs all stages in order:
  1. Scrape KPI dashboard -> Raw sheets
  2. Scrape Stripe -> Raw_STRIPE sheet
  3. Process ALL Raw rows -> Verified + Daily_Report
  4. Build Daily_Counts (true per-day from sign-up dates)
"""
import traceback
from datetime import datetime


def stage(name, func):
    print()
    print("=" * 70)
    print(f">>> {name}")
    print("=" * 70)
    try:
        func()
        print(f"OK: {name}")
        return True
    except Exception as e:
        print(f"FAILED: {name} - {e}")
        traceback.print_exc()
        return False


def main():
    print("=" * 70)
    print(f"PIPELINE START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    def s1():
        from scrape_kpi import main as f
        f()

    def s2():
        from scrape_stripe import main as f
        f()

    def s3():
        from process_data import main as f
        f()

    def s4():
        from daily_counts import build_daily_counts_table
        build_daily_counts_table()

    stage("STAGE 1 - Scrape KPI dashboard", s1)
    stage("STAGE 2 - Scrape Stripe customers", s2)
    stage("STAGE 3 - Process all rows -> Verified + Daily_Report", s3)
    stage("STAGE 4 - Build Daily_Counts (true per-day)", s4)

    print()
    print("=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)



    # === STORAGE STATUS REPORT ===
    print("\n" + "="*60, flush=True)
    print("STORAGE STATUS REPORT", flush=True)
    print("="*60, flush=True)
    try:
        from storage_adapter import get_storage_status
        status = get_storage_status()
        sheets_label = "YES" if status["sheets_available"] else "NO (CSV fallback active)"
        print(f"Google Sheets available : {sheets_label}", flush=True)
        print(f"Local data directory    : {status['data_dir']}", flush=True)
        print(f"Files captured:", flush=True)
        for fname, info in status["files"].items():
            rows = info.get("rows", "?")
            size = info.get("size_kb", "?")
            mod  = str(info.get("modified", "?"))[:19]
            print(f"  {fname:40s} {rows:>6} rows  {size:>8}KB  {mod}", flush=True)
    except Exception as e:
        print(f"Status report error: {e}", flush=True)
    print("="*60, flush=True)

if __name__ == "__main__":
    main()
