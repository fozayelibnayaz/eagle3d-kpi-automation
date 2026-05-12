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


if __name__ == "__main__":
    main()
