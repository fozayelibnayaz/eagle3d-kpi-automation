#!/usr/bin/env python3
"""
Eagle Analytics Hub v8.3 — Pipeline Fix Deploy
Run from your eagle3d-kpi-automation directory:
    python3 deploy_v83.py

This script:
1. Updates daily_pipeline.yml with YouTube/GA4/LinkedIn/Stripe env vars
2. Updates .gitignore with force-include for data files
3. Updates ga4_connector.py with env var fallback
4. Commits and pushes to GitHub
5. Cleans up old deploy/patch files
"""
import os
import subprocess
import sys


def run(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    ok = r.returncode == 0
    return ok, r.stdout.strip(), r.stderr.strip()


def main():
    print()
    print("=" * 60)
    print("Eagle Analytics Hub v8.3 -- Pipeline Fix Deploy")
    print("=" * 60)

    if not os.path.exists("app.py"):
        print("ERROR: Run from eagle3d-kpi-automation directory")
        sys.exit(1)

    ok, _, _ = run("git remote get-url origin")
    if not ok:
        print("ERROR: No git remote 'origin' configured")
        print("  Run: git remote add origin https://github.com/fozayelibnayaz/eagle3d-kpi-automation.git")
        sys.exit(1)

    print("\nStep 1: Fetching latest from GitHub...")
    run("git fetch origin main")

    print("Step 2: Stashing any local changes...")
    run("git stash")

    print("Step 3: Rebasing on origin/main...")
    ok, out, err = run("git rebase origin/main")
    if not ok:
        print(f"  Rebase conflict, trying pull instead...")
        run("git rebase --abort")
        run("git pull --rebase origin main")

    print("Step 4: Writing updated daily_pipeline.yml...")

    workflow_path = ".github/workflows/daily_pipeline.yml"
    os.makedirs(os.path.dirname(workflow_path), exist_ok=True)

    workflow_content = r'''name: Daily KPI Pipeline

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
        run: |
          NOW_UTC=$(date -u '+%Y-%m-%d %H:%M:%S')
          echo "=== Pipeline Context ==="
          echo "Triggered at UTC: $NOW_UTC"
          REPORT_DATE=$(date -u -d '12 hours ago' '+%Y-%m-%d')
          echo "Reporting on day: $REPORT_DATE"
          echo ""
          echo "=== Secret Availability ==="
          if [ -n "$YOUTUBE_API_KEY" ]; then echo "YouTube API Key: SET"; else echo "YouTube API Key: NOT SET"; fi
          if [ -n "$YOUTUBE_CHANNEL_ID" ]; then echo "YouTube Channel ID: SET"; else echo "YouTube Channel ID: NOT SET"; fi
          if [ -n "$STRIPE_SECRET_KEY" ]; then echo "Stripe Secret Key: SET"; else echo "Stripe Secret Key: NOT SET"; fi
          if [ -n "$GROQ_API_KEY" ]; then echo "Groq API Key: SET"; else echo "Groq API Key: NOT SET"; fi
        env:
          YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY }}
          YOUTUBE_CHANNEL_ID: ${{ secrets.YOUTUBE_CHANNEL_ID }}
          STRIPE_SECRET_KEY: ${{ secrets.STRIPE_SECRET_KEY }}
          GROQ_API_KEY: ${{ secrets.GROQ_API_KEY }}

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

    with open(workflow_path, 'w') as f:
        f.write(workflow_content)
    print(f"  Written {workflow_path}")

    print("Step 5: Writing updated .gitignore...")

    gitignore_additions = """
# YouTube data files (force-include)
!data_output/youtube_channel.json
!data_output/youtube_videos.json
!data_output/youtube_daily.json
!data_output/youtube_cache.json
!data_output/youtube_analytics.json
!data_output/ga4_traffic_cache.json
!data_output/daily_counts.json
!data_output/cross_platform_cache.json
!data_output/linkedin_metrics.json
!data_output/linkedin_posts.json
!data_output/linkedin_followers.json
"""

    with open(".gitignore", "a") as f:
        existing = open(".gitignore").read()
        added = []
        for line in gitignore_additions.strip().split("\n"):
            if line.strip() and line.strip() not in existing:
                f.write(line + "\n")
                added.append(line.strip())
        if added:
            print(f"  Added {len(added)} force-include entries to .gitignore")
        else:
            print("  .gitignore already up to date")

    print("Step 6: Updating ga4_connector.py with env var fallback...")

    ga4_patch = '''
    # Try GOOGLE_CREDS_JSON env var (pipeline uses this)
    _env_creds = os.environ.get("GOOGLE_CREDS_JSON", "").strip()
    if _env_creds:
        try:
            d = json.loads(_env_creds)
            if "private_key" in d:
                d["private_key"] = d["private_key"].replace("\\n", "\\n")
            return service_account.Credentials.from_service_account_info(d, scopes=SCOPES)
        except Exception:
            pass
'''

    with open("ga4_connector.py", "r") as f:
        ga4_content = f.read()

    if "GOOGLE_CREDS_JSON env var" not in ga4_content:
        ga4_content = ga4_content.replace(
            '    # Fall back to local file\n    try:',
            ga4_patch + '    # Fall back to local file\n    try:'
        )
        with open("ga4_connector.py", "w") as f:
            f.write(ga4_content)
        print("  Added GOOGLE_CREDS_JSON env var fallback to ga4_connector.py")
    else:
        print("  ga4_connector.py already has env var fallback")

    print("Step 7: Cleaning up old files...")

    cleanup_files = [
        "apply_v81_fixes.py", "apply_v82_fixes.py",
        "v81_fixes.patch", "v82_app.py.b64", "v82_reporting_engine.py.b64",
        "v82_linkedin_connector.py.b64", "v82_youtube_connector.py.b64",
        "v82_ga4_connector.py.b64", "fix_daily_yml.sh", "fix_and_deploy.py",
        "CHANGES_SUMMARY.md", "FIX_SUMMARY_v7.1.md", "SESSION_SUMMARY.md",
    ]
    for f in cleanup_files:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"  Removed {f}")
            except Exception:
                pass

    print("Step 8: Committing changes...")
    run("git add -A")
    ok_clean, _, _ = run("git diff --cached --quiet")
    if not ok_clean:
        run('git commit -m "v8.3: Pipeline fix - add YouTube/GA4/LinkedIn/Stripe env vars, commit data files, GA4 env fallback, cleanup"')
        print("  Committed")
    else:
        print("  No changes to commit")

    print("Step 9: Pushing to GitHub...")
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
        print("  What was fixed:")
        print("  1. Pipeline now passes YouTube API key + channel ID")
        print("  2. Pipeline now passes GA4 property ID")
        print("  3. Pipeline now passes Stripe secret key")
        print("  4. Pipeline now passes LinkedIn cookies (if set)")
        print("  5. Pipeline now passes Groq API key")
        print("  6. Pipeline now COMMITS YouTube/GA4/LinkedIn data files")
        print("  7. Pipeline has dual cron schedule (00:00 + 12:00 UTC)")
        print("  8. GA4 connector reads creds from env var in pipeline")
        print("  9. Old deploy/patch files cleaned up")
        print()
        print("  NEXT STEPS:")
        print()
        print("  1. Add these GitHub Secrets (if not already set):")
        print("     https://github.com/fozayelibnayaz/eagle3d-kpi-automation/settings/secrets/actions")
        print("     - YOUTUBE_API_KEY (from Google Cloud Console)")
        print("     - YOUTUBE_CHANNEL_ID (UCxxxxx)")
        print("     - STRIPE_SECRET_KEY (sk_live_xxxx)")
        print("     - GROQ_API_KEY (from groq.com)")
        print("     Optional:")
        print("     - YOUTUBE_OAUTH_TOKEN, YOUTUBE_REFRESH_TOKEN")
        print("     - YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET")
        print()
        print("  2. Trigger pipeline manually:")
        print("     https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions")
        print("     -> Daily KPI Pipeline -> Run workflow")
        print()
        print("  3. Streamlit Cloud will rebuild in ~60-90 seconds:")
        print("     https://eagle3d-kpi-automation.streamlit.app/")
        print()
    else:
        print("\n  PUSH FAILED - Try: git push origin main --force")


if __name__ == "__main__":
    main()
