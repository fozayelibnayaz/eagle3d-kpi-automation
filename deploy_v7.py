#!/usr/bin/env python3
"""
Eagle3D KPI System — v7 Deployer
=================================
Run on Mac:  python3 deploy_v7.py
This script writes all updated files and pushes to GitHub.
"""
import os, sys, json, base64, subprocess

PAYLOAD_FILE = "update_v7.b64"

def main():
    print("🦅 Eagle3D KPI v7 Deployer")
    print("=" * 50)

    if not os.path.exists(PAYLOAD_FILE):
        print(f"❌ {PAYLOAD_FILE} not found!")
        print("Make sure update_v7.b64 is in the same directory.")
        sys.exit(1)

    # Read and decode
    with open(PAYLOAD_FILE, 'r') as f:
        b64 = f.read()
    try:
        files = json.loads(base64.b64decode(b64).decode())
    except Exception as e:
        print(f"❌ Failed to decode payload: {e}")
        sys.exit(1)

    print(f"📦 Loaded {len(files)} files")

    # Write files
    for path, content in files.items():
        dirname = os.path.dirname(path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(path, 'w') as f:
            f.write(content)
        print(f"  ✅ {path} ({len(content):,} bytes)")

    print()
    print("✅ All files written!")
    print()

    # Git commit and push
    print("🚀 Deploying to GitHub...")
    subprocess.run(["git", "add", "-A"], check=False)
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        capture_output=True
    )
    if result.returncode == 0:
        print("⚠️ No changes detected — files may be identical")
    else:
        subprocess.run([
            "git", "commit", "-m",
            "v7.0: YouTube + LinkedIn + Cross-Platform + Pipeline update"
        ], check=False)
        print("✅ Committed")

    # Push
    print("⚠️ Force pushing (overrides pipeline auto-commits)...")
    result = subprocess.run(
        ["git", "push", "origin", "main", "--force"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print("✅ Force pushed to GitHub!")
    else:
        print(f"❌ Push failed: {result.stderr}")
        print()
        print("Run manually:")
        print("  git pull --rebase origin main || true")
        print("  git push origin main --force")

    print()
    print("=" * 50)
    print("🌐 Live at: https://eagle3d-kpi-automation.streamlit.app/")
    print("⏱️  Streamlit Cloud updates in 60-90 seconds")
    print()
    print("=" * 50)
    print("📋 SECRETS TO ADD:")
    print()
    print("── Streamlit Cloud (share.streamlit.io -> Settings -> Secrets) ──")
    print("Add these lines (in addition to existing secrets):")
    print()
    print("# YouTube")
    print('YOUTUBE_API_KEY = "YOUR_YOUTUBE_API_KEY"')
    print('YOUTUBE_CHANNEL_ID = "YOUR_CHANNEL_ID"')
    print("# YOUTUBE_OAUTH_TOKEN = optional, for Analytics API")
    print()
    print("# LinkedIn")
    print('LINKEDIN_COMPANY_PAGE = "https://www.linkedin.com/company/YOUR_COMPANY/"')
    print("# LINKEDIN_COOKIES_JSON = optional, for deep scraping")
    print()
    print("── GitHub Secrets (repo -> Settings -> Secrets -> Actions) ──")
    print("Add: YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID, YOUTUBE_OAUTH_TOKEN (optional)")
    print("Add: LINKEDIN_COMPANY_PAGE, LINKEDIN_COOKIES_JSON (optional)")
    print()
    print("── Telegram (if not already added) ──")
    print('TELEGRAM_BOT_TOKEN = "8743434532:AAFMy9FduXeIStlVMtLWY-NX_Bln-IRYDes"')
    print('TELEGRAM_CHAT_ID = "-1003989604195"')

if __name__ == "__main__":
    main()
