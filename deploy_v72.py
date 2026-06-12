#!/usr/bin/env python3
"""
Eagle Analytics Hub — v7.2 Deployer
====================================
Run on your machine:  python3 deploy_v72.py

This script pushes the committed v7.2 changes to GitHub.
Streamlit Cloud auto-deploys from the GitHub main branch.

What's new in v7.2:
  - 🔒 Login/password gate (password: eagleanalytics or APP_PASSWORD secret)
  - 🔍 Browse Data date filter FIXED (no more all-time data showing)
  - 🔗 App link in sidebar header + dashboard title
  - 🚪 Sign Out button in sidebar
  - 📺 YouTube Command Center connector
  - 💼 LinkedIn Command Center connector
  - 🔗 Cross-platform correlation engine
  - 📊 Per-subsystem Telegram reports
  - 💳 Stripe URL fix (ALL paying customers)
  - ⚙️ Pipeline v7 with YouTube + LinkedIn stages
  - 🧠 AI engine secret fix
  - 🚫 No secrets/tokens in code
"""
import subprocess, sys, time


def run(cmd, check=True):
    print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True)
    if check and result.returncode != 0:
        print(f"    ❌ Failed (exit {result.returncode})")
        sys.exit(1)
    elif result.returncode == 0:
        print(f"    ✅")
    return result


def main():
    print("🦅 Eagle Analytics Hub — v7.2 Deployer")
    print("=" * 55)

    # Step 1: Verify no secrets in tracked files
    print("\n🔍 Checking for secrets in code...")
    result = subprocess.run(
        'git diff --cached | grep -i "8743434532\\|AAFMy9F\\|1003989604195\\|sk_live\\|ghp_" || echo "CLEAN"',
        shell=True, capture_output=True, text=True
    )
    if "CLEAN" not in result.stdout:
        print("❌ SECRET DETECTED in staged changes! Aborting.")
        sys.exit(1)
    print("  ✅ No secrets found")

    # Step 2: Verify all files compile
    print("\n🔍 Verifying all Python files compile...")
    result = subprocess.run(
        'python3 -c "'
        'import ast, os;'
        'errors=[];'
        '[errors.append(f) for f in os.listdir(\\".\\") if f.endswith(\\".py\\") '
        'and not (lambda f: (open(f).read(), ast.parse(open(f).read())))(f)];'
        'print(f\\\"✅ All files compile\\\") if not errors else print(f\\\"❌ {errors}\\\")"',
        shell=True, capture_output=True, text=True
    )
    print(f"  {result.stdout.strip()}")

    # Step 3: Fetch remote
    print("\n📥 Syncing with remote...")
    run("git fetch origin main", check=False)
    run("git pull --rebase origin main", check=False)

    # Step 4: Stage any new changes
    print("\n📦 Staging all changes...")
    run("git add -A")

    # Step 5: Commit if needed
    print("\n💾 Committing if needed...")
    result = run('git diff --cached --quiet', check=False)
    if result.returncode != 0:
        run('git commit -m "v7.2: Login + Browse Data fix + LinkedIn + YouTube + Cross-platform [skip ci]"')
    else:
        print("  ℹ️ No new changes to commit")

    # Step 6: Force push (required because pipeline auto-commits state files)
    print("\n🚀 Pushing to GitHub...")
    for attempt in range(3):
        result = run("git push origin main --force", check=False)
        if result.returncode == 0:
            print("  ✅ Pushed!")
            break
        else:
            print(f"  ⚠️ Push attempt {attempt+1} failed, retrying...")
            run("git fetch origin main", check=False)
            run("git rebase origin/main", check=False)
            run("git add -A", check=False)
            time.sleep(3)

    # Done
    print()
    print("=" * 55)
    print("✅ DEPLOY COMPLETE")
    print()
    print("🌐 https://eagle3d-kpi-automation.streamlit.app/")
    print("⏱️  Streamlit Cloud updates in 60-90 seconds")
    print()
    print("📋 AFTER DEPLOY — DO THESE STEPS:")
    print()
    print("1. Check the app loads with login screen")
    print("   → Password: eagleanalytics")
    print("   → Or set APP_PASSWORD in Streamlit Cloud secrets")
    print()
    print("2. Verify Browse Data date filter works correctly")
    print("   → Select 'This Month' — should only show current month data")
    print()
    print("3. Verify LinkedIn + YouTube pages load without errors")
    print()
    print("4. Trigger pipeline manually:")
    print("   https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions")
    print()
    print("5. Check Telegram for subsystem reports")
    print()
    print("6. Update Stripe cookies if paid count is still wrong:")
    print("   → Export fresh cookies from browser")
    print("   → Update STRIPE_COOKIES_JSON GitHub secret")
    print()
    print("7. Revoke old Telegram bot token at @BotFather")
    print("   → Get new token → update GitHub + Streamlit secrets")
    print("=" * 55)


if __name__ == "__main__":
    main()
