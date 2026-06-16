#!/usr/bin/env python3
"""
Eagle Analytics Hub v8.2 — Apply ALL fixes and deploy
Run from your eagle3d-kpi-automation directory:
    python3 apply_v82_fixes.py
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
    print("Eagle Analytics Hub v8.2 -- Complete Fix & Deploy")
    print("=" * 60)

    if not os.path.exists("app.py"):
        print("ERROR: Run from eagle3d-kpi-automation directory")
        sys.exit(1)

    # Find and decode .b64 files
    b64_files = sorted(glob.glob("v82_*.b64"))
    if not b64_files:
        print("ERROR: No v82_*.b64 files found in current directory")
        print("Make sure these files are in your eagle3d-kpi-automation folder:")
        print("  v82_reporting_engine.py.b64")
        print("  v82_linkedin_connector.py.b64")
        print("  v82_youtube_connector.py.b64")
        print("  v82_ga4_connector.py.b64")
        print("  v82_app.py.b64")
        sys.exit(1)

    # Write fixed files
    print("\nWriting fixed files...")
    for b64path in b64_files:
        # v82_something.py.b64 -> something.py
        target = b64path.replace("v82_", "", 1).replace(".b64", "")
        try:
            with open(b64path, "rb") as f:
                raw = base64.b64decode(f.read())
            with open(target, "wb") as f:
                f.write(raw)
            lines = raw.decode().count("\n")
            print(f"  OK {target} ({lines} lines)")
        except Exception as e:
            print(f"  FAIL {target}: {e}")

    # Delete daily.yml from GitHub
    print("\nDeleting daily.yml from GitHub...")
    run("git fetch origin main")
    ok, files_out, _ = run("git ls-tree -r origin/main --name-only .github/workflows/")
    if "daily.yml" in files_out:
        run("git checkout origin/main -- .github/workflows/daily.yml")
        run("git rm .github/workflows/daily.yml")
        run('git commit -m "Remove old daily.yml workflow"')
        print("  OK daily.yml deleted from tracking")
    else:
        print("  OK daily.yml not on remote (already clean)")

    # Commit and push
    print("\nCommitting all changes...")
    run("git add -A")
    ok_clean, _, _ = run("git diff --cached --quiet")
    if not ok_clean:
        run(
            'git commit -m "v8.2: Fix LinkedIn live scrape, YouTube estimated analytics, '
            'accurate anomaly reasons, GA4/YouTube live API fallback, daily.yml removed"'
        )

    print("\nPushing to GitHub...")
    pushed = False
    for attempt in range(5):
        ok, out, err = run("git push origin main")
        if ok:
            print("  PUSHED SUCCESSFULLY!")
            pushed = True
            break
        print(f"  Attempt {attempt+1}/5 failed, retrying...")
        run("git pull --rebase origin main")

    if pushed:
        # Clean up .b64 files
        for b64path in b64_files:
            try:
                os.remove(b64path)
            except Exception:
                pass
        print()
        print("=" * 60)
        print("ALL FIXES DEPLOYED!")
        print("=" * 60)
        print()
        print("  What was fixed:")
        print("  1. daily.yml deleted (was causing double pipeline runs)")
        print("  2. LinkedIn shows ALL data (posts, employees, company)")
        print("  3. LinkedIn live-scrapes posts when cache empty")
        print("  4. YouTube shows estimated analytics when OAuth unavailable")
        print("  5. YouTube/GA4 try live API calls when cache fails")
        print("  6. Anomaly alerts give accurate data-based reasons")
        print("  7. Cross-platform shows connected sources status")
        print("  8. Pipeline health shows staleness info")
        print()
        print("  Streamlit Cloud rebuilds in ~60-90 seconds")
        print("  https://eagle3d-kpi-automation.streamlit.app/")
        print()
        print("  Then trigger pipeline:")
        print("  https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions")
        print("  -> Daily KPI Pipeline -> Run workflow")
        print()
    else:
        print("\n  PUSH FAILED - Try: git push origin main --force")


if __name__ == "__main__":
    main()
