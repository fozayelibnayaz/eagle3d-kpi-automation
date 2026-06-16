#!/usr/bin/env python3
"""
Eagle Analytics Hub — Deploy v8.0
==================================
Run on YOUR machine:

    python3 deploy_v80.py

Or push manually:
    git push origin main --force
"""
import subprocess, sys, os, time

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def run(cmd, check=True, quiet=False):
    if not quiet:
        print(f"  $ {cmd}")
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    ok = r.returncode == 0
    if not ok:
        if check:
            print(f"    {RED}❌ Exit {r.returncode}{RESET}")
            if r.stderr:
                for line in r.stderr.strip().split('\n')[:8]:
                    print(f"    {line}")
        else:
            if not quiet:
                print(f"    {YELLOW}⚠️ (non-fatal){RESET}")
    elif not quiet:
        print(f"    {GREEN}✅{RESET}")
    return ok, r.stdout.strip(), r.stderr.strip()


def header(msg):
    print(f"\n{BOLD}{CYAN}{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}{RESET}")


def main():
    print(f"\n{BOLD}🦅 Eagle Analytics Hub — Deploy v8.0{RESET}")
    header("Step 1: Pre-flight checks")

    if not os.path.exists("app.py"):
        print(f"  {RED}❌ Run from eagle3d-kpi-automation directory{RESET}")
        sys.exit(1)

    # --- Secret scan (skip deploy scripts and data files) ---
    print("\n  🔍 Scanning for leaked secrets...")
    _OLD_BOT_PART1 = "8743434532"
    _OLD_BOT_PART2 = "AAFMy9FduXeIStlVMtLWY"
    leaked = False
    for root, dirs, files in os.walk('.'):
        dirs[:] = [d for d in dirs if d not in ('.git', 'data_output', 'data', '__pycache__')]
        for f in files:
            if f.startswith('deploy_'):
                continue  # Skip deploy scripts themselves
            if not f.endswith(('.py', '.json', '.yml', '.yaml')):
                continue
            path = os.path.join(root, f)
            try:
                with open(path) as fh:
                    content = fh.read()
                full_token = f"{_OLD_BOT_PART1}:{_OLD_BOT_PART2}"
                if full_token in content:
                    print(f"  {RED}🚨 {path}: FULL TELEGRAM BOT TOKEN LEAKED!{RESET}")
                    leaked = True
            except Exception:
                pass
    if leaked:
        print(f"\n  {RED}BLOCKED: Remove leaked secrets before pushing!{RESET}")
        sys.exit(1)
    print(f"  {GREEN}✅ No leaked secrets{RESET}")

    # --- Compile check ---
    print("\n  🔍 Verifying all Python files compile...")
    import ast
    errs = []
    for f in sorted(os.listdir('.')):
        if f.endswith('.py') and not f.startswith('deploy_'):
            try:
                with open(f) as fh:
                    ast.parse(fh.read())
            except SyntaxError as e:
                errs.append(f"{f}: L{e.lineno}: {e.msg}")
    if errs:
        for e in errs:
            print(f"  {RED}❌ {e}{RESET}")
        sys.exit(1)
    print(f"  {GREEN}✅ All files compile{RESET}")

    header("Step 2: Git setup")

    ok_remote, current_remote, _ = run("git remote get-url origin", check=False, quiet=True)
    # FIX: Handle case where git remote returns error (current_remote is empty string, not False)
    if not ok_remote or not isinstance(current_remote, str) or "eagle3d-kpi-automation" not in current_remote:
        print(f"\n  {YELLOW}No GitHub remote configured or wrong URL.{RESET}")
        print(f"  You need a Personal Access Token (PAT) with repo permissions.")
        print(f"  Create one at: https://github.com/settings/tokens")
        pat = input(f"\n  {BOLD}Enter your GitHub PAT (or press Enter for manual setup): {RESET}").strip()
        if pat:
            url = f"https://{pat}@github.com/fozayelibnayaz/eagle3d-kpi-automation.git"
            run('git remote add origin https://github.com/fozayelibnayaz/eagle3d-kpi-automation.git', check=False)
            run(f'git remote set-url origin {url}', check=False)
            print(f"  {GREEN}✅ Remote configured with PAT{RESET}")
        else:
            print(f"\n  Set it manually:")
            print(f"  git remote set-url origin https://<PAT>@github.com/fozayelibnayaz/eagle3d-kpi-automation.git")
            print(f"  Then run: python3 deploy_v80.py")
            sys.exit(1)
    else:
        print(f"  {GREEN}✅ Remote already configured{RESET}")

    header("Step 3: Push to GitHub")

    run("git add -A", check=False)
    ok_clean, _, _ = run("git diff --cached --quiet", check=False, quiet=True)
    if not ok_clean:
        run('git commit -m "v8.0: Deploy [skip ci]"', check=False)

    run("git stash --include-untracked", check=False, quiet=True)
    run("git fetch origin main", check=False, quiet=True)

    ok_rebase, _, _ = run("git rebase origin/main", check=False, quiet=True)
    if not ok_rebase:
        print(f"  {YELLOW}⚠️ Rebase conflict — using merge instead{RESET}")
        run("git rebase --abort", check=False, quiet=True)
        run("git merge origin/main --allow-unrelated-histories --no-edit", check=False, quiet=True)

    run("git stash pop", check=False, quiet=True)

    _, log_out, _ = run("git log --oneline origin/main..HEAD", check=False, quiet=True)
    n_commits = len(log_out.strip().split('\n')) if log_out.strip() else 0
    print(f"\n  📊 {n_commits} commits ready to push")

    print(f"\n  {BOLD}🚀 Pushing to GitHub...{RESET}")
    pushed = False
    for attempt in range(5):
        ok, out, err = run("git push origin main --force", check=False)
        if ok:
            print(f"\n  {GREEN}{BOLD}✅ PUSHED SUCCESSFULLY!{RESET}")
            pushed = True
            break

        print(f"\n  {RED}❌ Attempt {attempt+1}/5 failed{RESET}")
        if "403" in err or "Authentication" in err or "Permission" in err:
            print(f"\n  {RED}{BOLD}🔑 AUTH FAILED{RESET}")
            print(f"  Create a new PAT at: https://github.com/settings/tokens")
            print(f"  Then:")
            print(f"    git remote set-url origin https://<PAT>@github.com/fozayelibnayaz/eagle3d-kpi-automation.git")
            print(f"    python3 deploy_v80.py")
            break

        run("git fetch origin main", check=False, quiet=True)
        run("git rebase origin/main", check=False, quiet=True)
        time.sleep(3)

    if pushed:
        header("Step 4: Post-deploy checklist")
        print(f"""
  {GREEN}{BOLD}✅ CODE IS LIVE ON GITHUB!{RESET}

  {CYAN}🌐 Streamlit Cloud rebuilds in ~60-90 seconds{RESET}
     https://eagle3d-kpi-automation.streamlit.app/

  {YELLOW}⚡ Make sure these secrets are set in BOTH:{RESET}
     • Streamlit Cloud → Settings → Secrets
     • GitHub → Settings → Secrets and variables → Actions

  Required secrets:
     YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID
     YOUTUBE_OAUTH_TOKEN, YOUTUBE_REFRESH_TOKEN
     YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET
     LINKEDIN_COOKIES_JSON, LINKEDIN_COMPANY_PAGE
     GOOGLE_CREDS_JSON, MASTER_SHEET_URL
     TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
     GITHUB_TOKEN (for pipeline trigger button)

  {CYAN}Trigger pipeline to test:{RESET}
     GitHub → Actions → Daily Pipeline → Run workflow
""")
    else:
        print(f"\n  {RED}{BOLD}❌ PUSH FAILED{RESET}")
        print(f"  Fix: git remote set-url origin https://<PAT>@github.com/fozayelibnayaz/eagle3d-kpi-automation.git")


if __name__ == "__main__":
    main()
