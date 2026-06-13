#!/usr/bin/env python3
"""
Eagle Analytics Hub — Deploy Script v7.7
=========================================
Run this on your machine:
    python3 deploy_v77.py

All fixes through v7.5 + v7.6 + v7.7:
  - LinkedIn NameError fix
  - Login uses real Eagle logo
  - Dark mode only (no broken light mode)
  - Telegram alerts use HTML (fixes ❌ Failed)
  - Alert buttons in each main page (KPI, GA4, YouTube, LinkedIn)
  - LinkedIn 3-tier authenticated scraping (Playwright → cookies → public)
  - Pipeline restores LinkedIn cookies from secrets
  - Comprehensive Telegram alerts (all subsystems + performance + monthly goals)
  - KPI stats fallback to local JSON
  - Workspace cleaned

Handles: stash → fetch → rebase → force push → Streamlit Cloud auto-deploys
"""
import subprocess, sys, time, os


def run(cmd, check=True):
    """Run shell command."""
    print(f"  $ {cmd}")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    ok = r.returncode == 0
    if check and not ok:
        print(f"    ❌ Exit {r.returncode}")
        if r.stderr:
            for line in r.stderr.strip().split('\n')[:5]:
                print(f"    {line}")
    elif ok:
        print(f"    ✅")
    else:
        print(f"    ⚠️ (non-fatal)")
    return ok, r.stdout.strip(), r.stderr.strip()


def main():
    print("🦅 Eagle Analytics Hub — Deploy v7.7")
    print("=" * 55)

    if not os.path.exists("app.py"):
        print("❌ Run this from the eagle3d-kpi-automation directory")
        sys.exit(1)

    # 1. No secrets check
    print("\n🔍 Checking for secrets...")
    bad = False
    for f in sorted(os.listdir(".")):
        if f.endswith(".py") and not f.startswith("deploy_"):
            with open(f) as fh:
                c = fh.read()
            for pat in ["8743434532", "AAFMy9F", "1003989604195", "sk_live_", "ghp_"]:
                if pat in c:
                    print(f"  🚨 {f}: contains {pat}!")
                    bad = True
    if bad:
        sys.exit(1)
    print("  ✅ No secrets found")

    # 2. Compile check
    print("\n🔍 Verifying files compile...")
    import ast
    errs = []
    for f in sorted(os.listdir(".")):
        if f.endswith(".py") and not f.startswith("deploy_"):
            try:
                with open(f) as fh:
                    ast.parse(fh.read())
            except SyntaxError as e:
                errs.append(f"{f}: L{e.lineno}: {e.msg}")
    if errs:
        for e in errs:
            print(f"  ❌ {e}")
        sys.exit(1)
    print("  ✅ All files compile")

    # 3. Ensure git remote is set
    print("\n🔍 Checking git remote...")
    ok, out, _ = run("git remote get-url origin", check=False)
    if not ok or "eagle3d-kpi-automation" not in out:
        print("  Setting remote...")
        run("git remote add origin https://github.com/fozayelibnayaz/eagle3d-kpi-automation.git", check=False)
        run("git remote set-url origin https://github.com/fozayelibnayaz/eagle3d-kpi-automation.git", check=False)

    # 4. Stash
    print("\n📦 Stashing local changes...")
    run("git stash --include-untracked", check=False)

    # 5. Fetch
    print("\n📥 Fetching from GitHub...")
    run("git fetch origin main", check=False)

    # 6. Rebase
    print("\n🔄 Rebasing onto origin/main...")
    ok, _, err = run("git rebase origin/main", check=False)
    if not ok:
        print("  ⚠️ Rebase conflict — resolving...")
        run("git rebase --abort", check=False)
        run("git merge origin/main --allow-unrelated-histories --no-edit", check=False)

    # 7. Pop stash
    print("\n📦 Restoring changes...")
    run("git stash pop", check=False)

    # 8. Stage + commit
    print("\n📦 Committing all changes...")
    run("git add -A")
    ok, _, _ = run("git diff --cached --quiet", check=False)
    if not ok:
        run('git commit -m "v7.7: LinkedIn auth scrape, Telegram HTML, per-page alerts, dark-only [skip ci]"')
    else:
        print("  ℹ️ No changes to commit")

    # 9. Force push
    print("\n🚀 Pushing to GitHub...")
    pushed = False
    for i in range(5):
        ok, out, err = run("git push origin main --force", check=False)
        if ok:
            print("  ✅ PUSHED!")
            pushed = True
            break
        print(f"  ⚠️ Attempt {i+1}/5 failed")
        if "403" in err or "Authentication" in err or "Permission" in err:
            print("\n  🔑 AUTH FAILED — Fix with:")
            print("     Option A (SSH):")
            print("       git remote set-url origin git@github.com:fozayelibnayaz/eagle3d-kpi-automation.git")
            print("")
            print("     Option B (Personal Access Token):")
            print("       git remote set-url origin https://<YOUR_PAT>@github.com/fozayelibnayaz/eagle3d-kpi-automation.git")
            print("")
            print("     Then run: python3 deploy_v77.py")
            break
        run("git fetch origin main", check=False)
        run("git rebase origin/main", check=False)
        run("git add -A", check=False)
        time.sleep(3)

    if pushed:
        print("\n" + "=" * 55)
        print("✅ DEPLOY COMPLETE!")
        print()
        print("🌐 https://eagle3d-kpi-automation.streamlit.app/")
        print("⏱️  Wait 60-90 seconds for Streamlit Cloud to rebuild")
        print()
        print("📋 After deploy, verify:")
        print("   1. Login: eagleanalytics")
        print("   2. Click '📤 Send KPI Report' on Dashboard")
        print("   3. Check Telegram for the report (should be HTML now)")
        print("   4. LinkedIn: click 'Trigger LinkedIn Scrape' (uses cookies)")
        print("   5. Check LinkedIn Command Center shows posts & analytics")
        print()
        print("🔑 Streamlit Cloud Secrets to set/update:")
        print("   TELEGRAM_BOT_TOKEN = '<your-new-token-from-@BotFather>'")
        print("   TELEGRAM_CHAT_ID = '-1003989604195'")
        print("   LINKEDIN_COMPANY_PAGE = 'https://www.linkedin.com/company/eagle-3d-streaming/'")
        print("   LINKEDIN_COOKIES_JSON = '<your-cookie-json>'")
        print()
        print("🔑 GitHub Secrets to update:")
        print("   TELEGRAM_BOT_TOKEN")
        print("   LINKEDIN_COMPANY_PAGE")
        print("   LINKEDIN_COOKIES_JSON")
        print("=" * 55)
    else:
        print("\n❌ Push failed — see errors above")


if __name__ == "__main__":
    main()
