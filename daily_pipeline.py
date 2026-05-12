"""
DAILY PIPELINE
Runs 3 stages in order:
  1. Scrape KPI dashboard -> Raw_FREE, Raw_FIRST_UPLOAD, etc.
  2. Scrape Stripe -> Raw_STRIPE
  3. Process all Raw_* tabs -> Verified_* + Daily_Report
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

    stage("STAGE 1 - Scrape KPI dashboard -> Raw sheets", s1)
    stage("STAGE 2 - Scrape Stripe -> Raw_STRIPE sheet", s2)
    stage("STAGE 3 - Process all Raw sheets -> Verified + Daily_Report", s3)

    print()
    

    try:
        print()
        print("=" * 70)
        print(">>> STAGE 4 — Compute true daily counts from sign-up dates")
        print("=" * 70)
        from daily_counts import build_daily_counts_table
        build_daily_counts_table()
        print("OK: Daily counts updated")
    except Exception as e:
        print(f"FAILED: Daily counts - {e}")
        traceback.print_exc()

    print("=" * 70)
    print("PIPELINE COMPLETE - dashboard now has fresh data")
    print("=" * 70)


if __name__ == "__main__":
    main()
