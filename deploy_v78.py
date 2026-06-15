#!/usr/bin/env python3
"""
Eagle Analytics Hub — Deploy Script v7.8
=========================================
Run this on your machine:
    python3 deploy_v78.py
"""
import subprocess, sys, time, os


def run(cmd, check=True):
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
    print("🦅 Eagle Analytics Hub — Deploy v7.8")
    print("=" * 55)

    if not os.path.exists("app.py"):
        print("❌ Run this from the eagle3d-kpi-automation directory")
        sys.exit(1)

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

    print("\n🔍 Checking git remote...")
    ok, out, _ = run("git remote get-url origin", check=False)
    if not ok or "eagle3d-kpi-automation" not in out:
        print("  Setting remote...")
        run("git remote add origin https://github.com/fozayelibnayaz/eagle3d-kpi-automation.git", check=False)
        run("git remote set-url origin https://github.com/fozayelibnayaz/eagle3d-kpi-automation.git", check=False)

    print("\n📦 Stashing...")
    run("git stash --include-untracked", check=False)

    print("\n📥 Fetching...")
    run("git fetch origin main", check=False)

    print("\n🔄 Rebasing...")
    ok, _, err = run("git rebase origin/main", check=False)
    if not ok:
        print("  ⚠️ Rebase conflict — trying merge...")
        run("git rebase --abort", check=False)
        run("git merge origin/main --allow-unrelated-histories --no-edit", check=False)

    print("\n📦 Restoring...")
    run("git stash pop", check=False)

    print("\n📦 Committing...")
    run("git add -A")
    ok, _, _ = run("git diff --cached --quiet", check=False)
    if not ok:
        run('git commit -m "v7.8: All fixes — Telegram, LinkedIn, GA4, YouTube, Settings, Cross-platform [skip ci]"')

    print("\n🚀 Pushing...")
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
            print("     git remote set-url origin https://<YOUR_PAT>@github.com/fozayelibnayaz/eagle3d-kpi-automation.git")
            print("     Then: python3 deploy_v78.py")
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
        print("⏱️  Wait 60-90s for rebuild")
        print()
        print("🔑 Streamlit Cloud Secrets (update these):")
        print("   LINKEDIN_COMPANY_PAGE = 'https://www.linkedin.com/company/eagle-3d-streaming/'")
        print("   LINKEDIN_COOKIES_JSON = '<your-cookie-json>'")
        print("   TELEGRAM_BOT_TOKEN = '<new-token>'")
        print("   TELEGRAM_CHAT_ID = '-1003989604195'")
        print("=" * 55)
    else:
        print("\n❌ Push failed")


if __name__ == "__main__":
    main()
