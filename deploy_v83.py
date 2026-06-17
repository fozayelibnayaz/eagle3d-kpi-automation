#!/usr/bin/env python3
"""
Eagle Analytics Hub v8.3 — FULL DEPLOY
======================================
Run from your eagle3d-kpi-automation directory:
    python3 deploy_v83.py

This script:
1. Reads .b64 files and decodes them to the correct Python files
2. Writes the updated GitHub Actions workflow
3. Updates .gitignore
4. Commits everything and pushes to GitHub
5. Cleans up old deploy files

You need these files in the SAME directory as this script:
  - v83_app.py.b64
  - v83_linkedin_connector.py.b64
  - v83_youtube_connector.py.b64
  - v83_ga4_connector.py.b64
  - v83_reporting_engine.py.b64
  - v83_daily_pipeline.py.b64

Download all .b64 files from the workspace before running.
"""
import base64
import glob
import os
import subprocess
import sys


def run(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return r.returncode == 0, r.stdout.strip(), r.stderr.strip()


def main():
    print()
    print("=" * 60)
    print("Eagle Analytics Hub v8.3 -- FULL DEPLOY")
    print("=" * 60)

    if not os.path.exists("app.py"):
        print("ERROR: Run from eagle3d-kpi-automation directory")
        sys.exit(1)

    ok, _, _ = run("git remote get-url origin")
    if not ok:
        print("ERROR: No git remote 'origin' configured")
        sys.exit(1)

    b64_files = sorted(glob.glob("v83_*.b64"))
    if not b64_files:
        print("ERROR: No v83_*.b64 files found!")
        print("Download these files from the workspace first:")
        print("  v83_app.py.b64")
        print("  v83_linkedin_connector.py.b64")
        print("  v83_youtube_connector.py.b64")
        print("  v83_ga4_connector.py.b64")
        print("  v83_reporting_engine.py.b64")
        print("  v83_daily_pipeline.py.b64")
        sys.exit(1)

    print(f"\nFound {len(b64_files)} .b64 files")

    print("\nStep 1: Fetching latest from GitHub...")
    run("git stash")
    run("git fetch origin main")
    ok, _, _ = run("git rebase origin/main")
    if not ok:
        run("git rebase --abort")
        run("git pull --rebase origin main")

    print("Step 2: Decoding .b64 files...")
    for b64path in b64_files:
        target = b64path.replace("v83_", "", 1).replace(".b64", "")
        try:
            with open(b64path, "rb") as f:
                raw = base64.b64decode(f.read())
            with open(target, "wb") as f:
                f.write(raw)
            lines = raw.decode().count("\n")
            print(f"  OK {target} ({lines} lines)")
        except Exception as e:
            print(f"  FAIL {target}: {e}")
            sys.exit(1)

    print("Step 3: Writing updated workflow...")
    os.makedirs(".github/workflows", exist_ok=True)
    workflow = r'''name: Daily KPI Pipeline

on:
  workflow_dispatch:
    inputs:
      force_historical:
        description: 'Force full historical re-scrape (true/false)'
        required: false
        default: 'false'
  schedule:
    - cron: '0 12 * * *'
    - cron: '0 0 * * *'

permissions:
  contents: write

concurrency:
  group: daily-kpi-pipeline
  cancel-in-progress: false

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    timeout-minutes: 90

    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        with:
          fetch-depth: 1

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Cache pip dependencies
        uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: pip-${{ hashFiles('requirements-pipeline.txt') }}

      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install -r requirements-pipeline.txt

      - name: Install Playwright browsers
        run: playwright install --with-deps chromium

      - name: Restore Google credentials
        env:
          CREDS: ${{ secrets.GOOGLE_CREDS_JSON }}
        run: |
          if [ -z "$CREDS" ]; then
            echo "GOOGLE_CREDS_JSON empty"
            exit 1
          fi
          echo "$CREDS" > google_creds.json

      - name: Show schedule context
        env:
          YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY }}
          YOUTUBE_CHANNEL_ID: ${{ secrets.YOUTUBE_CHANNEL_ID }}
          STRIPE_SECRET_KEY: ${{ secrets.STRIPE_SECRET_KEY }}
          GROQ_API_KEY: ${{ secrets.GROQ_API_KEY }}
        run: |
          echo "=== Pipeline Context ==="
          NOW_UTC=$(date -u '+%Y-%m-%d %H:%M:%S')
          echo "Triggered at UTC: $NOW_UTC"
          REPORT_DATE=$(date -u -d '12 hours ago' '+%Y-%m-%d')
          echo "Reporting on day: $REPORT_DATE"
          echo ""
          echo "=== Secret Availability ==="
          if [ -n "$YOUTUBE_API_KEY" ]; then echo "YouTube API Key: SET"; else echo "YouTube API Key: NOT SET"; fi
          if [ -n "$YOUTUBE_CHANNEL_ID" ]; then echo "YouTube Channel ID: SET"; else echo "YouTube Channel ID: NOT SET"; fi
          if [ -n "$STRIPE_SECRET_KEY" ]; then echo "Stripe Secret Key: SET"; else echo "Stripe Secret Key: NOT SET"; fi
          if [ -n "$GROQ_API_KEY" ]; then echo "Groq API Key: SET"; else echo "Groq API Key: NOT SET"; fi

      - name: Login to KPI Dashboard
        env:
          KPI_EMAIL: ${{ secrets.KPI_EMAIL }}
          KPI_PASSWORD: ${{ secrets.KPI_PASSWORD }}
        run: |
          if [ -z "$KPI_EMAIL" ] || [ -z "$KPI_PASSWORD" ]; then
            echo "KPI_EMAIL or KPI_PASSWORD missing"
            exit 1
          fi
          python3 firebase_login.py || echo "Storage state login failed - scrape_kpi.py fallback will handle"

      - name: Restore Stripe cookies
        env:
          COOKIES: ${{ secrets.STRIPE_COOKIES_JSON }}
        run: |
          if [ -n "$COOKIES" ]; then
            echo "$COOKIES" > stripe_cookies.json
            echo "Stripe cookies restored"
          fi

      - name: Restore LinkedIn cookies
        env:
          COOKIES: ${{ secrets.LINKEDIN_COOKIES_JSON }}
        run: |
          if [ -n "$COOKIES" ]; then
            echo "$COOKIES" > data_output/linkedin_cookies.json
            echo "LinkedIn cookies restored"
          else
            echo "LINKEDIN_COOKIES_JSON not set - public scrape only"
          fi

      - name: Run pipeline
        env:
          MASTER_SHEET_URL: ${{ secrets.MASTER_SHEET_URL }}
          GOOGLE_CREDS_JSON: ${{ secrets.GOOGLE_CREDS_JSON }}
          KPI_EMAIL: ${{ secrets.KPI_EMAIL }}
          KPI_PASSWORD: ${{ secrets.KPI_PASSWORD }}
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          EMAIL_FROM: ${{ secrets.EMAIL_FROM }}
          EMAIL_TO: ${{ secrets.EMAIL_TO }}
          EMAIL_APP_PASSWORD: ${{ secrets.EMAIL_APP_PASSWORD }}
          HEADLESS_MODE: 'true'
          FORCE_HISTORICAL: ${{ github.event.inputs.force_historical || 'false' }}
          ENABLE_SMTP_VERIFY: '1'
          REPORT_TZ: 'UTC'
          YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY }}
          YOUTUBE_CHANNEL_ID: ${{ secrets.YOUTUBE_CHANNEL_ID }}
          YOUTUBE_OAUTH_TOKEN: ${{ secrets.YOUTUBE_OAUTH_TOKEN }}
          YOUTUBE_REFRESH_TOKEN: ${{ secrets.YOUTUBE_REFRESH_TOKEN }}
          YOUTUBE_CLIENT_ID: ${{ secrets.YOUTUBE_CLIENT_ID }}
          YOUTUBE_CLIENT_SECRET: ${{ secrets.YOUTUBE_CLIENT_SECRET }}
          LINKEDIN_COMPANY_PAGE: ${{ secrets.LINKEDIN_COMPANY_PAGE }}
          LINKEDIN_COOKIES_JSON: ${{ secrets.LINKEDIN_COOKIES_JSON }}
          GA4_PROPERTY_ID: '374525971'
          STRIPE_SECRET_KEY: ${{ secrets.STRIPE_SECRET_KEY }}
          GROQ_API_KEY: ${{ secrets.GROQ_API_KEY }}
        run: |
          echo "=== Pipeline Start ==="
          python3 daily_pipeline.py

      - name: Show state files AFTER run
        if: always()
        run: |
          ls -la data_output/ | head -40
          echo ""
          echo "=== Key data files ==="
          for f in youtube_channel.json youtube_videos.json youtube_daily.json ga4_traffic_cache.json linkedin_metrics.json linkedin_daily.json linkedin_posts.json daily_counts.json; do
            if [ -f "data_output/$f" ]; then
              echo "  OK data_output/$f ($(wc -c < data_output/$f) bytes)"
            else
              echo "  MISSING data_output/$f"
            fi
          done

      - name: Verify data freshness
        if: always()
        run: |
          python3 << 'PYEOF'
          from datetime import datetime, timedelta
          from pathlib import Path
          import json
          utc_now = datetime.utcnow()
          aoe_completed = (utc_now - timedelta(hours=12)).strftime('%Y-%m-%d')
          print(f"UTC now: {utc_now.strftime('%Y-%m-%d %H:%M')}")
          print(f"Reporting on: {aoe_completed}")
          checks = {
              "pipeline_health.json": lambda d: f"stages: {d.get('stages_passed', '?')}/{d.get('total_stages', '?')}",
              "youtube_channel.json": lambda d: f"channel: {d.get('title', '?')}, subs: {d.get('subscribers', 0):,}",
              "linkedin_metrics.json": lambda d: f"followers: {d.get('followers', 0):,}, posts: {len(d.get('posts', []))}",
              "ga4_traffic_cache.json": lambda d: f"sessions: {d.get('total_sessions', 0):,}",
          }
          for fname, desc_fn in checks.items():
              p = Path(f"data_output/{fname}")
              if p.exists():
                  try:
                      data = json.loads(p.read_text())
                      print(f"OK {fname}: {desc_fn(data)}")
                  except Exception as e:
                      print(f"WARN {fname}: parse error ({e})")
              else:
                  print(f"MISSING {fname}")
          PYEOF

      - name: Commit persistent state
        if: always()
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          mkdir -p data_output
          for f in data_output/.historical_done data_output/.stripe_historical_done; do
            if [ -f "$f" ]; then
              git add -f "$f"
            fi
          done
          for f in \
            data_output/upload_history.json \
            data_output/first_upload_ledger.json \
            data_output/upload_registry.json \
            data_output/historical_accounts.json \
            data_output/historical_paid.json \
            data_output/old_db_with_dates.json \
            data_output/pipeline_health.json \
            data_output/domain_cache.json \
            data_output/smtp_cache.json \
            data_output/manual_overrides.json \
            data_output/youtube_cache.json \
            data_output/youtube_videos.json \
            data_output/youtube_analytics.json \
            data_output/youtube_daily.json \
            data_output/youtube_channel.json \
            data_output/linkedin_metrics.json \
            data_output/linkedin_posts.json \
            data_output/linkedin_daily.json \
            data_output/linkedin_followers.json \
            data_output/ga4_traffic_cache.json \
            data_output/daily_counts.json \
            data_output/cross_platform_cache.json \
            kpi_storage_state.json; do
            if [ -f "$f" ]; then
              git add -f "$f"
            fi
          done
          if git diff --cached --quiet; then
            echo "No state changes"
          else
            REPORT_DATE=$(date -u -d '12 hours ago' '+%Y-%m-%d')
            git commit -m "Pipeline state - reporting day ${REPORT_DATE} (AoE) [skip ci]"
            for i in 1 2 3; do
              if git push origin main; then
                echo "Pushed"
                break
              else
                echo "Push $i failed, retrying..."
                git pull --rebase origin main || true
                sleep 3
              fi
            done
          fi

      - name: Upload debug artifacts
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: kpi-run-${{ github.run_number }}
          path: |
            data_output/*.csv
            data_output/*.json
            data_output/*.log
            data_output/.historical_done
            data_output/.stripe_historical_done
          retention-days: 30
          include-hidden-files: true
'''
    with open(".github/workflows/daily_pipeline.yml", "w") as f:
        f.write(workflow)
    print("  OK .github/workflows/daily_pipeline.yml")

    print("Step 4: Updating .gitignore...")
    gitignore_additions = [
        "!data_output/youtube_channel.json",
        "!data_output/youtube_videos.json",
        "!data_output/youtube_daily.json",
        "!data_output/youtube_cache.json",
        "!data_output/youtube_analytics.json",
        "!data_output/ga4_traffic_cache.json",
        "!data_output/daily_counts.json",
        "!data_output/cross_platform_cache.json",
        "!data_output/linkedin_metrics.json",
        "!data_output/linkedin_posts.json",
        "!data_output/linkedin_followers.json",
    ]
    with open(".gitignore", "r") as f:
        existing = f.read()
    added = []
    for line in gitignore_additions:
        if line.strip() and line.strip() not in existing:
            added.append(line)
    if added:
        with open(".gitignore", "a") as f:
            f.write("\n".join(added) + "\n")
        print(f"  Added {len(added)} entries")
    else:
        print("  Already up to date")

    print("Step 5: Cleaning up old files...")
    cleanup = [
        "apply_v81_fixes.py", "apply_v82_fixes.py", "fix_and_deploy.py",
        "fix_daily_yml.sh", "v81_fixes.patch", "CHANGES_SUMMARY.md",
        "FIX_SUMMARY_v7.1.md", "SESSION_SUMMARY.md",
        "v82_app.py.b64", "v82_reporting_engine.py.b64",
        "v82_linkedin_connector.py.b64", "v82_youtube_connector.py.b64",
        "v82_ga4_connector.py.b64",
    ]
    for f in cleanup:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"  Removed {f}")
            except Exception:
                pass

    print("Step 6: Committing...")
    run("git add -A")
    ok_clean, _, _ = run("git diff --cached --quiet")
    if not ok_clean:
        run('git commit -m "v8.3: Fix paid count, YouTube public analytics, LinkedIn engagement scrape, full Playwright analytics, pipeline env vars, GA4 env fallback"')
        print("  Committed")
    else:
        print("  No changes")

    print("Step 7: Pushing to GitHub...")
    pushed = False
    for attempt in range(5):
        ok, out, err = run("git push origin main")
        if ok:
            print("  PUSHED SUCCESSFULLY!")
            pushed = True
            break
        print(f"  Attempt {attempt+1}/5 failed, retrying...")
        run("git pull --rebase origin main")

    run("git stash pop 2>/dev/null || true")

    if pushed:
        print()
        print("=" * 60)
        print("DEPLOYED SUCCESSFULLY!")
        print("=" * 60)
        print()
        print("  Fixes deployed:")
        print("  1. Paid count: verified against Verified_STRIPE (ACCEPTED only)")
        print("  2. YouTube: per-video analytics works WITHOUT OAuth")
        print("  3. LinkedIn: engagement scraped from HTML aria-labels")
        print("  4. LinkedIn: full Playwright analytics scraper added")
        print("  5. Pipeline: all env vars (YouTube/GA4/LinkedIn/Stripe/Groq)")
        print("  6. Pipeline: commits YouTube/GA4/LinkedIn data files")
        print("  7. GA4: GOOGLE_CREDS_JSON env var fallback")
        print("  8. LinkedIn: default company page URL fallback")
        print("  9. LinkedIn: improved post scoring algorithm")
        print()
        print("  IMPORTANT: Add these GitHub Secrets if not set:")
        print("  https://github.com/fozayelibnayaz/eagle3d-kpi-automation/settings/secrets/actions")
        print("  - YOUTUBE_API_KEY")
        print("  - YOUTUBE_CHANNEL_ID")
        print("  - STRIPE_SECRET_KEY")
        print("  - GROQ_API_KEY")
        print("  - LINKEDIN_COOKIES_JSON (export from browser)")
        print()
        print("  Then trigger pipeline:")
        print("  https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions")
        print()
        print("  Streamlit rebuilds in ~90s:")
        print("  https://eagle3d-kpi-automation.streamlit.app/")
        print()
    else:
        print("\n  PUSH FAILED - Try: git push origin main --force")


if __name__ == "__main__":
    main()
