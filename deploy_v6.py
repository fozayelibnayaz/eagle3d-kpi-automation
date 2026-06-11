#!/usr/bin/env python3
"""
Eagle3D KPI System — v6 Deployer
=================================
Run on Mac:  python3 deploy_v6.py
This script writes all updated files and pushes to GitHub.
"""
import os, sys, json, base64, subprocess

# ══════════════════════════════════════════════════════════════
# PAYLOAD — base64 encoded JSON of all files
# Generated: 2026-06-11
# ══════════════════════════════════════════════════════════════
PAYLOAD_FILE = "update_v6.b64"

def main():
    print("🦅 Eagle3D KPI v6 Deployer")
    print("=" * 50)

    if not os.path.exists(PAYLOAD_FILE):
        print(f"❌ {PAYLOAD_FILE} not found!")
        print("Make sure update_v6.b64 is in the same directory.")
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
            "v6.0: Complete update — all fixes + Telegram + Browse Data + AI + Traffic Intel"
        ], check=False)
        print("✅ Committed")

    # Push
    result = subprocess.run(["git", "push", "origin", "main"], capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ Pushed to GitHub!")
    else:
        print("⚠️ Push failed, trying force push...")
        result2 = subprocess.run(
            ["git", "push", "--force-with-lease", "origin", "main"],
            capture_output=True, text=True
        )
        if result2.returncode == 0:
            print("✅ Force pushed!")
        else:
            print(f"❌ Push failed: {result2.stderr}")
            print()
            print("Run manually:")
            print("  git push origin main --force")

    print()
    print("=" * 50)
    print("🌐 Live at: https://eagle3d-kpi-automation.streamlit.app/")
    print("⏱️  Streamlit Cloud updates in 60-90 seconds")
    print()
    print("IMPORTANT — Add Telegram credentials to Streamlit Cloud secrets:")
    print("  1. Go to https://share.streamlit.io")
    print("  2. Your app → Settings → Secrets")
    print("  3. Add these lines:")
    print()
    print('TELEGRAM_BOT_TOKEN = "your-bot-token-here"')
    print('TELEGRAM_CHAT_ID = "your-chat-id-here"')
    print()
    print("  Get the values from: GitHub → repo → Settings → Secrets → Actions")
    print("  Copy TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from there")

if __name__ == "__main__":
    main()
