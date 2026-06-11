"""
YouTube OAuth Token Helper
===========================
This script helps you get YouTube Analytics API OAuth tokens.

Step 1: Create OAuth 2.0 Client ID in Google Cloud Console
  - Go to https://console.cloud.google.com/apis/credentials
  - Create OAuth 2.0 Client ID (Desktop app)
  - Note down CLIENT_ID and CLIENT_SECRET

Step 2: Run this script
  python3 youtube_oauth_helper.py

It will give you a URL to visit in your browser.
After authorizing, paste the authorization code back.
The script will exchange it for tokens.

Step 3: Add tokens to your secrets
  YOUTUBE_OAUTH_TOKEN = "ya29..."
  YOUTUBE_REFRESH_TOKEN = "1//..."
  YOUTUBE_CLIENT_ID = "xxxx.apps.googleusercontent.com"
  YOUTUBE_CLIENT_SECRET = "GOCSPX-..."
"""

import urllib.request
import urllib.parse
import json
import sys

def main():
    print("🦅 YouTube OAuth Token Helper")
    print("=" * 50)
    print()

    client_id = input("Enter your Google OAuth CLIENT_ID: ").strip()
    client_secret = input("Enter your Google OAuth CLIENT_SECRET: ").strip()

    if not client_id or not client_secret:
        print("❌ Both CLIENT_ID and CLIENT_SECRET are required.")
        sys.exit(1)

    redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
    scopes = [
        "https://www.googleapis.com/auth/youtube.readonly",
        "https://www.googleapis.com/auth/yt-analytics.readonly",
        "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    ]

    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        + urllib.parse.urlencode({
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": " ".join(scopes),
            "access_type": "offline",
            "prompt": "consent",
        })
    )

    print()
    print("🔗 Open this URL in your browser:")
    print()
    print(auth_url)
    print()
    print("After authorizing, Google will show you a code.")
    print()

    code = input("Paste the authorization code here: ").strip()

    if not code:
        print("❌ No code provided.")
        sys.exit(1)

    # Exchange code for tokens
    print()
    print("🔄 Exchanging code for tokens...")

    data = urllib.parse.urlencode({
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }).encode()

    req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"❌ Token exchange failed: {e.read().decode()}")
        sys.exit(1)

    access_token = result.get("access_token", "")
    refresh_token = result.get("refresh_token", "")

    print()
    print("✅ Success!")
    print()
    print("=" * 50)
    print("ADD THESE TO YOUR SECRETS:")
    print("=" * 50)
    print()
    print("── Streamlit Cloud (share.streamlit.io → Settings → Secrets) ──")
    print()
    print(f'YOUTUBE_OAUTH_TOKEN = "{access_token}"')
    print(f'YOUTUBE_REFRESH_TOKEN = "{refresh_token}"')
    print(f'YOUTUBE_CLIENT_ID = "{client_id}"')
    print(f'YOUTUBE_CLIENT_SECRET = "{client_secret}"')
    print()
    print("── GitHub Secrets (repo → Settings → Secrets → Actions) ──")
    print("Add: YOUTUBE_OAUTH_TOKEN")
    print("Add: YOUTUBE_REFRESH_TOKEN")
    print("Add: YOUTUBE_CLIENT_ID")
    print("Add: YOUTUBE_CLIENT_SECRET")
    print()
    print("⚠️ Access tokens expire in ~1 hour.")
    print("   The refresh token (if provided) will auto-refresh on Streamlit Cloud.")
    print("   If no refresh token, you'll need to re-run this script periodically.")
    print()

    # Test the token
    if access_token:
        print("🧪 Testing token...")
        try:
            test_req = urllib.request.Request(
                "https://www.googleapis.com/youtube/v3/channels?part=snippet&mine=true",
            )
            test_req.add_header("Authorization", f"Bearer {access_token}")
            with urllib.request.urlopen(test_req, timeout=15) as resp:
                test_data = json.loads(resp.read().decode())
                if test_data.get("items"):
                    ch = test_data["items"][0]
                    print(f"✅ Connected to channel: {ch.get('snippet', {}).get('title', 'Unknown')}")
                else:
                    print("⚠️ Token works but no channels found.")
        except Exception as e:
            print(f"⚠️ Token test failed: {e}")


if __name__ == "__main__":
    main()
