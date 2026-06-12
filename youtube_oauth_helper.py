"""
YouTube OAuth Token Helper v2 — Fixed redirect_uri_mismatch
=============================================================
The previous version used 'urn:ietf:wg:oauth:2.0:oob' which Google deprecated.
This version uses a loopback redirect that works on all Google OAuth setups.

HOW TO USE:
1. Create OAuth 2.0 Client ID at https://console.cloud.google.com/apis/credentials
   - Application type: **Web application** (NOT Desktop app)
   - Add Authorized redirect URI: http://localhost:8089
2. Run this script: python3 youtube_oauth_helper.py
3. Open the URL it gives you in your browser
4. After authorizing, the browser will redirect to localhost:8089
   — this script catches the code automatically
5. It prints your tokens to add to secrets

If port 8089 is busy, it tries 8090, 8091, etc.
"""

import http.server
import urllib.parse
import urllib.request
import json
import sys
import threading

# You can hardcode these or enter them when prompted
CLIENT_ID = ""
CLIENT_SECRET = ""

REDIRECT_PORT = 8089
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
]


def main():
    print("🦅 YouTube OAuth Token Helper v2 (Fixed)")
    print("=" * 55)
    print()

    client_id = CLIENT_ID or input("Enter your Google OAuth CLIENT_ID: ").strip()
    client_secret = CLIENT_SECRET or input("Enter your Google OAuth CLIENT_SECRET: ").strip()

    if not client_id or not client_secret:
        print("❌ Both CLIENT_ID and CLIENT_SECRET are required.")
        sys.exit(1)

    redirect_uri = f"http://localhost:{REDIRECT_PORT}"

    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        + urllib.parse.urlencode({
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": " ".join(SCOPES),
            "access_type": "offline",
            "prompt": "consent",
        })
    )

    print()
    print("⚠️  IMPORTANT: Make sure your OAuth client has this redirect URI:")
    print(f"    {redirect_uri}")
    print()
    print("  Add it at: https://console.cloud.google.com/apis/credentials")
    print("  → Click your OAuth client → Authorized redirect URIs → Add URI")
    print()
    print("🔗 Open this URL in your browser:")
    print()
    print(auth_url)
    print()

    # Start local server to catch the redirect
    auth_code = [None]
    auth_error = [None]

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)

            if "code" in params:
                auth_code[0] = params["code"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h1>&#9989; Success! You can close this tab.</h1><p>Return to the terminal.</p></body></html>")
            elif "error" in params:
                auth_error[0] = params.get("error", ["unknown"])[0]
                self.send_response(400)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h1>&#10060; Authorization denied.</h1></body></html>")
            else:
                self.send_response(400)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Suppress server logs

    # Try ports
    server = None
    port = REDIRECT_PORT
    for attempt in range(5):
        try:
            server = http.server.HTTPServer(("localhost", port), Handler)
            redirect_uri = f"http://localhost:{port}"
            break
        except OSError:
            port += 1
            continue

    if not server:
        print("❌ Could not start local server. Try closing other apps using ports 8089-8093.")
        sys.exit(1)

    print(f"⏳ Waiting for authorization (listening on localhost:{port})...")
    print()

    # Handle one request in a thread
    def serve():
        server.handle_request()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    t.join(timeout=120)

    server.server_close()

    if auth_error[0]:
        print(f"❌ Authorization error: {auth_error[0]}")
        sys.exit(1)

    if not auth_code[0]:
        print("❌ No authorization code received (timeout). Try again.")
        sys.exit(1)

    code = auth_code[0]
    print(f"✅ Got authorization code!")
    print()

    # Exchange code for tokens
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
        body = e.read().decode()
        print(f"❌ Token exchange failed: {body}")
        if "redirect_uri_mismatch" in body:
            print()
            print("💡 FIX: Add this URI to your OAuth client:")
            print(f"   {redirect_uri}")
            print("   at https://console.cloud.google.com/apis/credentials")
            print("   → Your OAuth client → Authorized redirect URIs → Add the URI above")
        sys.exit(1)

    access_token = result.get("access_token", "")
    refresh_token = result.get("refresh_token", "")

    print()
    print("✅ Success!")
    print()
    print("=" * 55)
    print("ADD THESE TO YOUR STREAMLIT CLOUD SECRETS:")
    print("=" * 55)
    print()
    print('YOUTUBE_OAUTH_TOKEN = "{}"'.format(access_token))
    print('YOUTUBE_REFRESH_TOKEN = "{}"'.format(refresh_token))
    print('YOUTUBE_CLIENT_ID = "{}"'.format(client_id))
    print('YOUTUBE_CLIENT_SECRET = "{}"'.format(client_secret))
    print()
    print("=" * 55)
    print("ALSO ADD TO GITHUB SECRETS (repo → Settings → Secrets):")
    print("=" * 55)
    print("  YOUTUBE_OAUTH_TOKEN")
    print("  YOUTUBE_REFRESH_TOKEN")
    print("  YOUTUBE_CLIENT_ID")
    print("  YOUTUBE_CLIENT_SECRET")
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

    print()
    print("⚠️ Access tokens expire in ~1 hour. The refresh token auto-renews on Streamlit Cloud.")


if __name__ == "__main__":
    main()
