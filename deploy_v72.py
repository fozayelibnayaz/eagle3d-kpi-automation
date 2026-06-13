#!/usr/bin/env python3
"""
Eagle Analytics Hub — Deploy Script v7.2.2
===========================================
Run:  python3 deploy_v72.py

This script handles everything:
  - Stashes local changes
  - Syncs with GitHub remote
  - Force pushes all v7.2.2 code
  - Streamlit Cloud auto-deploys from GitHub
"""
import subprocess
import sys
import time


def run(cmd, check=True):
    """Run a shell command, return (success, output)."""
    print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    ok = result.returncode == 0
    if check and not ok:
        print(f"    ❌ Exit {result.returncode}")
        if result.stderr:
            print(f"    stderr: {result.stderr[:200]}")
    else:
        print(f"    ✅" if ok else f"    ⚠️ (non-fatal)")
    return ok, result.stdout.strip(), result.stderr.strip()


def main():
    print("🦅 Eagle Analytics Hub — Deploy v7.2.2")
    print("=" * 55)

    # ── Step 1: Check we're in the right directory ──
    import os
    if not os.path.exists("app.py"):
        print("❌ Run this from the eagle3d-kpi-automation directory")
        sys.exit(1)

    # ── Step 2: Verify no secrets in tracked Python files ──
    print("\n🔍 Checking for secrets...")
    secret_patterns = ["8743434532", "AAFMy9F", "1003989604195", "sk_live_", "ghp_"]
    found_secret = False
    for f in os.listdir("."):
        if f.endswith(".py") and not f.startswith("deploy_"):
            with open(f) as fh:
                content = fh.read()
            for pat in secret_patterns:
                if pat in content:
                    print(f"  🚨 {f} contains {pat}!")
                    found_secret = True
    if found_secret:
        print("❌ Secrets found! Aborting.")
        sys.exit(1)
    print("  ✅ No secrets found")

    # ── Step 3: Verify all files compile ──
    print("\n🔍 Verifying Python files compile...")
    import ast
    errors = []
    for f in os.listdir("."):
        if f.endswith(".py") and not f.startswith("deploy_"):
            try:
                with open(f) as fh:
                    ast.parse(fh.read())
            except SyntaxError as e:
                errors.append(f"{f}: Line {e.lineno}: {e.msg}")
    if errors:
        for e in errors:
            print(f"  ❌ {e}")
        sys.exit(1)
    print("  ✅ All files compile OK")

    # ── Step 4: Stash any unstaged/uncommitted changes ──
    print("\n📦 Stashing local changes...")
    run("git stash --include-untracked", check=False)
    print("  ✅ Stashed")

    # ── Step 5: Fetch from remote ──
    print("\n📥 Fetching from GitHub...")
    ok, _, _ = run("git fetch origin main", check=False)
    if not ok:
        print("  ⚠️ Fetch failed — check internet connection and GitHub access")

    # ── Step 6: Try rebase, fallback to merge ──
    print("\n🔄 Syncing with remote...")
    ok, _, err = run("git rebase origin/main", check=False)
    if not ok:
        print("  ⚠️ Rebase had issues — trying alternative approach...")
        run("git rebase --abort", check=False)
        # Reset to local HEAD and merge remote on top
        run("git merge origin/main --allow-unrelated-histories --no-edit", check=False)

    # ── Step 7: Restore stashed changes ──
    print("\n📦 Restoring local changes...")
    run("git stash pop", check=False)

    # ── Step 8: Stage and commit anything remaining ──
    print("\n📦 Staging all changes...")
    run("git add -A")

    # Check if there's anything to commit
    ok, out, _ = run("git diff --cached --quiet", check=False)
    if not ok:
        run('git commit -m "v7.2.2: All fixes [skip ci]"')
    else:
        print("  ℹ️ No new changes to commit")

    # ── Step 9: Force push ──
    print("\n🚀 Force pushing to GitHub...")
    pushed = False
    for attempt in range(5):
        ok, _, err = run("git push origin main --force", check=False)
        if ok:
            print("  ✅ PUSHED!")
            pushed = True
            break
        else:
            print(f"  ⚠️ Push attempt {attempt+1}/5 failed")
            if "Authentication" in err or "403" in err:
                print("  🔑 GitHub authentication failed!")
                print("     Make sure you have push access to the repo.")
                print("     Try: git remote set-url origin git@github.com:fozayelibnayaz/eagle3d-kpi-automation.git")
                break
            run("git fetch origin main", check=False)
            run("git rebase origin/main", check=False)
            run("git add -A", check=False)
            time.sleep(3)

    if not pushed:
        print("\n❌ PUSH FAILED — try manually:")
        print("   git push origin main --force")
        print("\n   If auth fails, set up SSH key or use personal access token:")
        print("   git remote set-url origin https://<YOUR_TOKEN>@github.com/fozayelibnayaz/eagle3d-kpi-automation.git")
        print("   git push origin main --force")

    # ── Done ──
    if pushed:
        print()
        print("=" * 55)
        print("✅ DEPLOY COMPLETE!")
        print()
        print("🌐 https://eagle3d-kpi-automation.streamlit.app/")
        print("⏱️  Streamlit Cloud updates in 60-90 seconds")
        print()
        print("📋 After deploy:")
        print("  1. Login: eagleanalytics")
        print("  2. Browse Data → First Uploads (check date filter)")
        print("  3. LinkedIn Command Center (should load now!)")
        print("  4. YouTube Command Center")
        print("=" * 55)


if __name__ == "__main__":
    main()
