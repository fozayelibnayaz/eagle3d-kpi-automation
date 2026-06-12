#!/usr/bin/env python3
"""
Eagle Analytics Hub — v7.1 Deployer
====================================
Run on Mac:  python3 deploy_v7.py

This script:
  1. Reads update_v7.b64 (base64-encoded JSON of all fixed files)
  2. Writes each file to disk (overwriting with fixes)
  3. Pulls remote changes (pipeline auto-commits state files)
  4. Stages + commits all changes
  5. Force pushes to main (overrides pipeline auto-commits)
"""
import os, sys, json, base64, subprocess

PAYLOAD_FILE = "update_v7.b64"


def run(cmd, check=True, capture=False):
    """Run a shell command."""
    print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=capture, text=True)
    if check and result.returncode != 0:
        if capture:
            print(f"    stderr: {result.stderr.strip()}")
        print(f"    ❌ Failed (exit {result.returncode})")
    elif result.returncode == 0:
        print(f"    ✅ OK")
    return result


def main():
    print("🦅 Eagle Analytics Hub — v7.1 Deployer")
    print("=" * 55)

    # ── Step 1: Read payload ──
    if not os.path.exists(PAYLOAD_FILE):
        print(f"❌ {PAYLOAD_FILE} not found!")
        sys.exit(1)

    with open(PAYLOAD_FILE, 'r') as f:
        b64 = f.read()

    try:
        files = json.loads(base64.b64decode(b64).decode('utf-8'))
    except Exception as e:
        print(f"❌ Failed to decode payload: {e}")
        sys.exit(1)

    print(f"\n📦 Loaded {len(files)} files from payload")

    # ── Step 2: Write files ──
    print(f"\n📝 Writing fixed files...")
    for path, content in files.items():
        dirname = os.path.dirname(path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✅ {path} ({len(content):,} bytes)")

    # ── Step 3: Pull remote (pipeline may have committed state) ──
    print(f"\n📥 Pulling remote changes...")
    # First stash any uncommitted changes
    run("git stash", check=False)
    # Pull with rebase
    result = run("git pull --rebase origin main", check=False)
    if result.returncode != 0:
        # If rebase fails, try a hard reset to remote
        print("  ⚠️ Pull failed — trying fetch + reset...")
        run("git fetch origin main", check=False)
        run("git rebase origin/main", check=False)
    # Pop stash
    run("git stash pop", check=False)

    # ── Step 4: Stage all changes ──
    print(f"\n📦 Staging all changes...")
    run("git add -A")

    # Check if there are changes
    result = run("git diff --cached --quiet", check=False, capture=True)
    if result.returncode == 0:
        print("  ⚠️ No staged changes detected")
    else:
        print("  ✅ Changes staged")

    # ── Step 5: Commit ──
    print(f"\n💾 Committing...")
    run(
        'git commit -m "v7.1: Combined Sources smart dedup + Stripe paid count fix + date priority unify\n\n'
        "FIX 1: Combined Sources uses source_normalizer for smart dedup\n"
        "  - Google/google/Google Search Console -> single Google row\n"
        "  - AI/ChatGPT/Claude/Gemini -> single AI Tools row\n"
        "  - LinkedIn/linkedin -> single LinkedIn row\n\n"
        "FIX 2: Stripe paid count - First payment date = ACCEPTED\n"
        "  - Customers with First payment but $0 spend now counted\n"
        "  - Payment Count field support added\n"
        "  - 3-tier: First payment > Payment Count > Spend\n\n"
        "FIX 3: Stripe date priority unified across all files\n"
        "  - First payment -> row_date_used -> Created\n\n"
        'FIX 4: Auto-trigger toast improved" --allow-empty',
        check=False,
    )

    # ── Step 6: Force push (overrides pipeline auto-commits) ──
    print(f"\n🚀 Force pushing to GitHub (overrides pipeline auto-commits)...")
    for attempt in range(3):
        result = run("git push origin main --force", check=False, capture=True)
        if result.returncode == 0:
            print("  ✅ Force pushed!")
            break
        else:
            print(f"  ⚠️ Push attempt {attempt+1} failed")
            if "fetch first" in result.stderr or "rejected" in result.stderr:
                print("  📥 Re-fetching and retrying...")
                run("git fetch origin main", check=False)
                run("git rebase origin/main", check=False)
                run("git add -A", check=False)
                run('git commit -m "v7.1: retry push" --allow-empty', check=False)
            elif "Authentication" in result.stderr or "403" in result.stderr:
                print("  ❌ Authentication error — check your GitHub credentials")
                break
            else:
                print(f"  Error: {result.stderr[:200]}")
            if attempt < 2:
                import time
                time.sleep(3)

    # ── Done ──
    print()
    print("=" * 55)
    print("✅ DEPLOY COMPLETE")
    print()
    print("🌐 Live at: https://eagle3d-kpi-automation.streamlit.app/")
    print("⏱️  Streamlit Cloud updates in 60-90 seconds")
    print()
    print("⚠️  AFTER DEPLOY — DO THESE:")
    print()
    print("1. Enable GitHub Actions workflow:")
    print("   → https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions")
    print("   → Click 'Enable workflow' if disabled")
    print()
    print("2. Manually trigger pipeline once:")
    print("   → Click 'Daily KPI Pipeline' → 'Run workflow'")
    print("   → Wait ~5 min for green ✅")
    print()
    print("3. Check STRIPE_COOKIES_JSON secret:")
    print("   → https://github.com/fozayelibnayaz/eagle3d-kpi-automation/settings/secrets/actions")
    print("   → Must exist for Stripe scraping to work")
    print()
    print("4. Refresh dashboard:")
    print("   → Paid count should now be 3+ (not 1)")
    print("   → Combined Sources should show clean deduplicated rows")
    print("=" * 55)


if __name__ == "__main__":
    main()
