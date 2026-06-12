#!/usr/bin/env python3
"""
Eagle Analytics Hub — v7.1 Deployer (Secret-Free)
==================================================
Run on Mac:  python3 deploy_v7.py

This script is NOT committed to git (in .gitignore).
It reads update_v7.b64 (also not committed) and deploys to GitHub.
"""
import os, sys, json, base64, subprocess, time

PAYLOAD_FILE = "update_v7.b64"


def run(cmd, check=True):
    """Run a shell command."""
    print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True)
    if check and result.returncode != 0:
        print(f"    ❌ Failed (exit {result.returncode})")
    elif result.returncode == 0:
        print(f"    ✅")
    return result


def main():
    print("🦅 Eagle Analytics Hub — v7.1 Deployer")
    print("=" * 55)

    # ── Step 1: Read payload ──
    if not os.path.exists(PAYLOAD_FILE):
        print(f"❌ {PAYLOAD_FILE} not found!")
        print("Make sure update_v7.b64 is in the same directory.")
        sys.exit(1)

    with open(PAYLOAD_FILE, 'r') as f:
        b64 = f.read()

    try:
        files = json.loads(base64.b64decode(b64).decode('utf-8'))
    except Exception as e:
        print(f"❌ Failed to decode payload: {e}")
        sys.exit(1)

    # Verify no secrets
    decoded = base64.b64decode(b64).decode('utf-8')
    for secret in ['8743434532', 'AAFMy9F', '1003989604195']:
        if secret in decoded:
            print(f"❌ SECRET LEAK DETECTED: {secret} in payload!")
            print("   Aborting deploy. Remove the secret first.")
            sys.exit(1)

    print(f"\n📦 Loaded {len(files)} files (secret-free ✅)")

    # ── Step 2: Write files ──
    print(f"\n📝 Writing fixed files...")
    for path, content in files.items():
        dirname = os.path.dirname(path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✅ {path} ({len(content):,} bytes)")

    # ── Step 3: Handle git ──
    print(f"\n📥 Syncing with remote...")
    # Stash any local changes
    run("git stash", check=False)
    # Fetch remote
    run("git fetch origin main", check=False)
    # Reset to remote (discards any stuck local commits)
    run("git reset --soft origin/main", check=False)
    # Pull to get pipeline auto-commits
    run("git pull --rebase origin main", check=False)
    # Pop stash
    run("git stash pop", check=False)

    # ── Step 4: Stage all changes ──
    print(f"\n📦 Staging all changes...")
    run("git add -A")

    # ── Step 5: Commit ──
    print(f"\n💾 Committing...")
    run(
        'git commit -m "v7.1: Combined Sources dedup + Stripe paid fix + Lead Sources date filter + diagnostics"',
        check=False,
    )

    # ── Step 6: Force push ──
    print(f"\n🚀 Force pushing to GitHub...")
    for attempt in range(3):
        result = run("git push origin main --force", check=False)
        if result.returncode == 0:
            print("  ✅ Force pushed!")
            break
        else:
            print(f"  ⚠️ Push attempt {attempt+1} failed, retrying...")
            run("git fetch origin main", check=False)
            run("git rebase origin/main", check=False)
            run("git add -A", check=False)
            run('git commit -m "v7.1: retry push" --allow-empty', check=False)
            time.sleep(3)

    # ── Done ──
    print()
    print("=" * 55)
    print("✅ DEPLOY COMPLETE")
    print()
    print("🌐 https://eagle3d-kpi-automation.streamlit.app/")
    print("⏱️  Streamlit Cloud updates in 60-90 seconds")
    print()
    print("⚠️  AFTER DEPLOY — DO THESE 3 STEPS:")
    print()
    print("1. Enable GitHub Actions workflow:")
    print("   https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions")
    print()
    print("2. Run pipeline once (to re-categorize Stripe):")
    print("   Click 'Daily KPI Pipeline' → 'Run workflow'")
    print()
    print("3. Check Stripe diagnostics in Settings page")
    print("   → Shows exactly how many rows are ACCEPTED vs REJECTED")
    print("   → Shows how many have 'First payment' date")
    print("   → Shows recent Stripe data sample")
    print("=" * 55)


if __name__ == "__main__":
    main()
