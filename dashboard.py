"""
DASHBOARD - Eagle3D KPI v7
- Clean UI with collapsible sections
- Built-in diagnostics page (find issues fast)
- Run Now button with proper feedback
- Polling for in-progress runs
- Per-day dedup (fixes doubling)
"""
import json
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
import requests
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Eagle3D KPI Dashboard", page_icon="🦅",
                   layout="wide", initial_sidebar_state="collapsed")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
METRIC_COLS = ["SignUps_Accepted", "FirstUploads_Accepted", "PaidSubscribers_Accepted"]


# ════════════════════════════════════════════════════════════════
#  SECRET / CONFIG LOADING
# ════════════════════════════════════════════════════════════════

def get_creds_path():
    if os.path.exists("google_creds.json"):
        return "google_creds.json"
    try:
        if "GOOGLE_CREDS" in st.secrets:
            creds = dict(st.secrets["GOOGLE_CREDS"])
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(creds, tmp)
            tmp.close()
            return tmp.name
    except Exception:
        pass
    try:
        raw = st.secrets["GOOGLE_CREDS_JSON"]
        creds = json.loads(raw) if isinstance(raw, str) else dict(raw)
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(creds, tmp)
        tmp.close()
        return tmp.name
    except Exception:
        pass
    st.error("Google credentials not found in Streamlit secrets.")
    st.stop()


def get_secret(key, default=None):
    """Safely get a secret value."""
    try:
        if key in st.secrets:
            val = st.secrets[key]
            return val
    except Exception:
        pass
    return default


def secret_exists(key):
    """Check if a secret key actually exists (without erroring)."""
    try:
        return key in st.secrets
    except Exception:
        return False


CREDS_PATH = get_creds_path()
MASTER_SHEET_URL = get_secret("MASTER_SHEET_URL")
GITHUB_TOKEN = get_secret("GITHUB_TOKEN")
GITHUB_REPO = get_secret("GITHUB_REPO", "fozayelibnayaz/eagle3d-kpi-automation")
WORKFLOW_FILE = get_secret("WORKFLOW_FILE", "daily.yml")


# ════════════════════════════════════════════════════════════════
#  DATA LOADING
# ════════════════════════════════════════════════════════════════

@st.cache_data(ttl=180)
def load_sheet(tab):
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(MASTER_SHEET_URL)
    try:
        ws = sh.worksheet(tab)
        data = ws.get_all_values()
        if len(data) < 2:
            return pd.DataFrame()
        headers = data[0]
        seen = {}
        clean = []
        for h in headers:
            h = h.strip() if h else "unknown"
            if h in seen:
                seen[h] += 1
                clean.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                clean.append(h)
        return pd.DataFrame(data[1:], columns=clean)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()


# ════════════════════════════════════════════════════════════════
#  GITHUB API
# ════════════════════════════════════════════════════════════════

def github_headers():
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def trigger_workflow():
    """Returns (success: bool, message: str, debug: dict)"""
    debug = {
        "token_present": bool(GITHUB_TOKEN),
        "token_prefix": (GITHUB_TOKEN[:12] + "...") if GITHUB_TOKEN else None,
        "repo": GITHUB_REPO,
        "workflow": WORKFLOW_FILE,
    }

    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN not in secrets. Run Diagnostics tab.", debug

    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    debug["url"] = url

    try:
        r = requests.post(url, headers=github_headers(),
                         json={"ref": "main"}, timeout=15)
        debug["status_code"] = r.status_code
        debug["response_body"] = r.text[:500] if r.text else ""

        if r.status_code == 204:
            return True, "Workflow triggered successfully", debug
        elif r.status_code == 401:
            return False, "401 Unauthorized — GITHUB_TOKEN is invalid or expired", debug
        elif r.status_code == 403:
            return False, "403 Forbidden — token lacks 'Actions: write' permission", debug
        elif r.status_code == 404:
            return False, f"404 Not Found — check GITHUB_REPO ({GITHUB_REPO}) and WORKFLOW_FILE ({WORKFLOW_FILE})", debug
        else:
            return False, f"GitHub API returned {r.status_code}: {r.text[:200]}", debug
    except Exception as e:
        debug["exception"] = str(e)
        return False, f"Request failed: {e}", debug


def get_workflow_runs(limit=5):
    """Returns (list of runs, debug info)"""
    debug = {
        "token_present": bool(GITHUB_TOKEN),
        "repo": GITHUB_REPO,
        "workflow": WORKFLOW_FILE,
    }

    if not GITHUB_TOKEN:
        return [], debug

    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/runs?per_page={limit}"
    debug["url"] = url

    try:
        r = requests.get(url, headers=github_headers(), timeout=15)
        debug["status_code"] = r.status_code

        if r.status_code == 200:
            runs = r.json().get("workflow_runs", [])
            debug["runs_count"] = len(runs)
            return runs, debug
        else:
            debug["response_body"] = r.text[:500]
            return [], debug
    except Exception as e:
        debug["exception"] = str(e)
        return [], debug


def list_workflows():
    """List all workflows in the repo (for diagnostics)."""
    if not GITHUB_TOKEN:
        return []
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows"
    try:
        r = requests.get(url, headers=github_headers(), timeout=15)
        if r.status_code == 200:
            return r.json().get("workflows", [])
    except Exception:
        pass
    return []


def format_age(iso_str):
    if not iso_str:
        return "?"
    try:
        ts = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - ts
        s = age.total_seconds()
        if s < 60:
            return f"{int(s)}s ago"
        if s < 3600:
            return f"{int(s//60)}m ago"
        if s < 86400:
            return f"{int(s//3600)}h ago"
        return f"{int(s//86400)}d ago"
    except Exception:
        return iso_str


# ════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════

def card(label, value, color, sub=None):
    sub_html = f'<div style="font-size:13px;opacity:0.85;margin-top:4px;">{sub}</div>' if sub else ""
    html = f'<div style="background:{color};color:white;padding:25px;border-radius:12px;text-align:center;">'
    html += f'<div style="font-size:14px;opacity:0.9;">{label}</div>'
    html += f'<div style="font-size:48px;font-weight:bold;margin:8px 0;">{value}</div>'
    html += sub_html + "</div>"
    st.markdown(html, unsafe_allow_html=True)


def n_accepted(df):
    if df.empty or "final_status" not in df.columns:
        return 0
    return int((df["final_status"].str.upper() == "ACCEPTED").sum())


def n_rejected_for(df, reason):
    if df.empty or "verdict_reason" not in df.columns:
        return 0
    return int(df["verdict_reason"].str.contains(reason, case=False, na=False).sum())


def safe_int_col(df, col):
    if col not in df.columns:
        return pd.Series([0] * len(df))
    return pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)


def get_date_range(preset):
    today = datetime.now().date()
    if preset == "Today":
        return (today, today)
    if preset == "This Week":
        return (today - timedelta(days=today.weekday()), today)
    if preset == "Last Week":
        end = today - timedelta(days=today.weekday() + 1)
        return (end - timedelta(days=6), end)
    if preset == "Last 7 Days":
        return (today - timedelta(days=6), today)
    if preset == "Last 15 Days":
        return (today - timedelta(days=14), today)
    if preset == "Last 28 Days":
        return (today - timedelta(days=27), today)
    if preset == "This Month":
        return (today.replace(day=1), today)
    if preset == "Last Month":
        first = today.replace(day=1)
        last_prev = first - timedelta(days=1)
        return (last_prev.replace(day=1), last_prev)
    if preset == "Last 3 Months":
        return (today - timedelta(days=90), today)
    if preset == "Last 6 Months":
        return (today - timedelta(days=180), today)
    if preset == "This Year":
        return (today.replace(month=1, day=1), today)
    if preset == "Last Year":
        ly = today.year - 1
        return (datetime(ly, 1, 1).date(), datetime(ly, 12, 31).date())
    return None


def filter_daily(df, dr):
    if df.empty:
        return df
    df = df.copy()
    if "Date" in df.columns:
        df["_dt"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    elif "Timestamp" in df.columns:
        df["_dt"] = pd.to_datetime(df["Timestamp"], errors="coerce").dt.date
    else:
        return df.iloc[0:0]
    df = df.dropna(subset=["_dt"])
    if dr is None:
        return df
    return df[(df["_dt"] >= dr[0]) & (df["_dt"] <= dr[1])]


def collapse_to_latest_per_day(df):
    """Keep only the LATEST row per day (fixes doubling)."""
    if df.empty:
        return df
    df = df.copy()
    df["SignUps_Accepted"] = safe_int_col(df, "SignUps_Accepted")
    df["FirstUploads_Accepted"] = safe_int_col(df, "FirstUploads_Accepted")
    df["PaidSubscribers_Accepted"] = safe_int_col(df, "PaidSubscribers_Accepted")
    if "Timestamp" in df.columns:
        df["_ts"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.sort_values("_ts").groupby("_dt", as_index=False).tail(1)
    else:
        df = df.groupby("_dt", as_index=False)[METRIC_COLS].max()
    return df


# ════════════════════════════════════════════════════════════════
#  HEADER + NAVIGATION
# ════════════════════════════════════════════════════════════════

st.markdown("""
<style>
    .stButton > button { width: 100%; }
    div[data-testid="metric-container"] { background: #f0f2f6; padding: 10px; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

st.title("🦅 Eagle3D Streaming — KPI Dashboard")
st.caption(f"Loaded at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ─── PAGE TABS ──────────────────────────────────────────────────
page = st.radio(
    "Navigate",
    ["📊 Dashboard", "🔍 Browse Data", "⚙️ Pipeline Control", "🛠 Diagnostics"],
    horizontal=True,
    label_visibility="collapsed",
)

st.divider()


# ════════════════════════════════════════════════════════════════
#  PAGE: DIAGNOSTICS
# ════════════════════════════════════════════════════════════════

if page == "🛠 Diagnostics":
    st.header("System Diagnostics")
    st.caption("Use this to identify why something isn't working.")

    # ── Section 1: Secrets check
    st.subheader("1. Secrets Configuration")
    expected = ["MASTER_SHEET_URL", "GOOGLE_CREDS",
                "GITHUB_TOKEN", "GITHUB_REPO", "WORKFLOW_FILE"]
    rows = []
    for k in expected:
        present = secret_exists(k)
        if present:
            val = get_secret(k)
            if k == "GITHUB_TOKEN" and val:
                shown = val[:12] + "..." + val[-4:]
            elif k == "GOOGLE_CREDS":
                shown = "✓ (TOML section present)"
            elif isinstance(val, str) and len(val) > 60:
                shown = val[:50] + "..."
            else:
                shown = str(val)[:80]
        else:
            shown = "❌ MISSING"
        rows.append({"Secret": k, "Status": "✅" if present else "❌", "Value": shown})
    st.table(pd.DataFrame(rows))

    if not GITHUB_TOKEN:
        st.error("**GITHUB_TOKEN is missing.** This is why 'Run Now' doesn't work.")
        with st.expander("How to fix"):
            st.markdown("""
            **Step 1:** Go to https://github.com/settings/tokens?type=beta
            **Step 2:** Click "Generate new token" → Fine-grained personal access token
            **Step 3:** Set:
            - Name: `Eagle3D Dashboard Trigger`
            - Expiration: 1 year
            - Repository access: select `eagle3d-kpi-automation`
            - Repository permissions:
              - **Actions: Read and write** ← critical!
              - Metadata: Read-only (auto)
            **Step 4:** Generate, then COPY the token (starts with `github_pat_`)
            **Step 5:** In Streamlit Cloud → app Settings → Secrets, ADD AT THE TOP:
            ```
            GITHUB_TOKEN = "github_pat_paste_here"
            GITHUB_REPO = "fozayelibnayaz/eagle3d-kpi-automation"
            WORKFLOW_FILE = "daily.yml"
            ```
            **Step 6:** Save → Reboot app.
            """)

    # ── Section 2: GitHub API connectivity
    st.subheader("2. GitHub API Connection Test")
    if st.button("Run GitHub API tests"):
        with st.spinner("Testing..."):
            # Test 1: Auth
            st.write("**Test 1: Authentication**")
            try:
                r = requests.get("https://api.github.com/user",
                               headers=github_headers(), timeout=10)
                if r.status_code == 200:
                    user = r.json().get("login", "?")
                    st.success(f"✅ Authenticated as: `{user}`")
                else:
                    st.error(f"❌ Status {r.status_code}: {r.text[:300]}")
            except Exception as e:
                st.error(f"❌ Exception: {e}")

            # Test 2: Repo access
            st.write(f"**Test 2: Repo access ({GITHUB_REPO})**")
            try:
                r = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}",
                               headers=github_headers(), timeout=10)
                if r.status_code == 200:
                    st.success(f"✅ Can access repo")
                else:
                    st.error(f"❌ Status {r.status_code}: {r.text[:300]}")
            except Exception as e:
                st.error(f"❌ Exception: {e}")

            # Test 3: List workflows
            st.write("**Test 3: List workflows in repo**")
            workflows = list_workflows()
            if workflows:
                wf_data = [{"Name": w.get("name"), "File": w.get("path", "").split("/")[-1],
                           "State": w.get("state")} for w in workflows]
                st.table(pd.DataFrame(wf_data))
                names = [w.get("path", "").split("/")[-1] for w in workflows]
                if WORKFLOW_FILE in names:
                    st.success(f"✅ `{WORKFLOW_FILE}` exists in repo")
                else:
                    st.error(f"❌ `{WORKFLOW_FILE}` NOT FOUND. Available: {names}")
            else:
                st.warning("No workflows found or token lacks permission")

            # Test 4: Get recent runs
            st.write(f"**Test 4: Get recent runs of {WORKFLOW_FILE}**")
            runs, debug = get_workflow_runs(limit=5)
            st.json(debug)
            if runs:
                run_data = []
                for r in runs[:5]:
                    run_data.append({
                        "Run #": r.get("run_number"),
                        "Status": r.get("status"),
                        "Conclusion": r.get("conclusion"),
                        "Created": format_age(r.get("created_at")),
                        "Trigger": r.get("event"),
                    })
                st.table(pd.DataFrame(run_data))

    # ── Section 3: Sheet connection
    st.subheader("3. Google Sheet Connection Test")
    if st.button("Test Sheet Read"):
        try:
            df = load_sheet("Daily_Report")
            if df.empty:
                st.warning("Daily_Report tab is empty (no rows)")
            else:
                st.success(f"✅ Read {len(df)} rows from Daily_Report")
                st.dataframe(df.head(), use_container_width=True)
        except Exception as e:
            st.error(f"❌ Failed: {e}")
            st.info("Make sure Master Sheet is shared with: " +
                   get_secret("GOOGLE_CREDS", {}).get("client_email", "your service account"))


# ════════════════════════════════════════════════════════════════
#  PAGE: PIPELINE CONTROL
# ════════════════════════════════════════════════════════════════

elif page == "⚙️ Pipeline Control":
    st.header("Pipeline Control")
    st.caption("Trigger the data scrape & processing pipeline manually.")

    # Status card
    runs, _ = get_workflow_runs(limit=5)
    if runs:
        latest = runs[0]
        status = latest.get("status", "?")
        conclusion = latest.get("conclusion", "?")
        ago = format_age(latest.get("created_at"))
        url = latest.get("html_url", "")
        run_number = latest.get("run_number")

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            if status == "in_progress" or status == "queued":
                st.info(f"🔄 **Running now**\nRun #{run_number} — {ago}")
            elif conclusion == "success":
                st.success(f"✅ **Last run: SUCCESS**\nRun #{run_number} — {ago}")
            elif conclusion == "failure":
                st.error(f"❌ **Last run: FAILED**\nRun #{run_number} — {ago}")
            else:
                st.warning(f"⚠️ **Last run: {conclusion or status}**\nRun #{run_number} — {ago}")
        with col_b:
            st.markdown(f"**Workflow:** `{WORKFLOW_FILE}`")
            st.markdown(f"**Repo:** `{GITHUB_REPO}`")
        with col_c:
            if url:
                st.markdown(f"[View on GitHub →]({url})")
    else:
        st.warning("No workflow runs found yet (or GitHub token issue — check Diagnostics).")

    st.divider()

    # Trigger button
    st.subheader("Trigger Pipeline Now")
    st.caption("Runs the full scrape & processing pipeline. Takes ~5-10 minutes.")

    if st.button("🚀 Run Pipeline Now", type="primary", use_container_width=False):
        with st.spinner("Sending trigger to GitHub..."):
            ok, msg, debug = trigger_workflow()

        if ok:
            st.success(f"✅ {msg}")
            st.info("Pipeline is now running on GitHub. Refresh this page in 5-10 minutes to see results.")
            st.json({"debug": debug})
        else:
            st.error(f"❌ {msg}")
            st.warning("Go to **🛠 Diagnostics** tab to find the root cause.")
            with st.expander("Debug info"):
                st.json(debug)

    st.divider()

    # Recent runs table
    st.subheader("Recent Pipeline Runs")
    if runs:
        run_data = []
        for r in runs:
            run_data.append({
                "Run": r.get("run_number"),
                "Status": r.get("status"),
                "Result": r.get("conclusion") or "—",
                "When": format_age(r.get("created_at")),
                "Trigger": r.get("event"),
                "URL": r.get("html_url"),
            })
        st.dataframe(pd.DataFrame(run_data), use_container_width=True, hide_index=True)
    else:
        st.info("No runs yet — trigger one above.")

    st.divider()

    st.subheader("Session/Cookie Health")
    st.caption("If the pipeline starts failing, your scraper login sessions probably expired.")
    col_x, col_y = st.columns(2)
    with col_x:
        st.info("**KPI Dashboard cookies** (~30 days lifetime)\n\nTo refresh:\n1. Run `python kpi_login.py` locally\n2. Update GitHub secret `KPI_COOKIES_JSON`")
    with col_y:
        st.info("**Stripe cookies** (~14 days lifetime)\n\nTo refresh:\n1. Login Stripe in normal Chrome\n2. Cookie-Editor → Export JSON\n3. Update GitHub secret `STRIPE_COOKIES_JSON`")


# ════════════════════════════════════════════════════════════════
#  PAGE: BROWSE DATA
# ════════════════════════════════════════════════════════════════

elif page == "🔍 Browse Data":
    st.header("Browse Customer Data")
    st.caption("Search and filter through emails, usernames, plans, dates.")

    if st.button("🔄 Refresh from sheet"):
        st.cache_data.clear()
        st.rerun()

    free = load_sheet("Verified_FREE")
    upload = load_sheet("Verified_FIRST_UPLOAD")
    stripe = load_sheet("Verified_STRIPE")

    browse_tabs = st.tabs(["📥 Sign-ups", "📦 First Uploads", "💳 Paid Subscribers"])

    for tab, (label, df) in zip(browse_tabs, [
        ("Sign-ups", free), ("First Uploads", upload), ("Paid Subscribers", stripe)
    ]):
        with tab:
            if df.empty:
                st.warning(f"No {label} data yet. Run pipeline first.")
                continue

            f1, f2, f3 = st.columns([2, 2, 4])
            with f1:
                status_filter = st.selectbox("Status:", ["All", "ACCEPTED only", "REJECTED only"],
                                             key=f"s_{label}")
            with f2:
                verdict_options = ["All"]
                if "email_verdict" in df.columns:
                    verdict_options += sorted(df["email_verdict"].dropna().unique().tolist())
                verdict_filter = st.selectbox("Email Verdict:", verdict_options, key=f"v_{label}")
            with f3:
                search_term = st.text_input("🔎 Search any field:", key=f"q_{label}",
                                           placeholder="email, name, anything...")

            filtered = df.copy()
            if status_filter == "ACCEPTED only":
                filtered = filtered[filtered["final_status"].str.upper() == "ACCEPTED"]
            elif status_filter == "REJECTED only":
                filtered = filtered[filtered["final_status"].str.upper() == "REJECTED"]
            if verdict_filter != "All" and "email_verdict" in filtered.columns:
                filtered = filtered[filtered["email_verdict"] == verdict_filter]
            if search_term:
                mask = pd.Series([False] * len(filtered))
                for col in filtered.columns:
                    mask = mask | filtered[col].astype(str).str.contains(search_term, case=False, na=False)
                filtered = filtered[mask]

            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Showing", len(filtered))
            m2.metric("Accepted", n_accepted(filtered))
            m3.metric("Disposable", n_rejected_for(filtered, "isposable"))
            m4.metric("Fake/No MX", n_rejected_for(filtered, "MX"))
            m5.metric("Duplicate", n_rejected_for(filtered, "database"))

            priority_cols = []
            for col in filtered.columns:
                cl = col.lower()
                if any(k in cl for k in ("email", "name", "user", "customer", "phone",
                                          "source", "lead", "created", "spend", "plan",
                                          "country", "verdict", "status", "tier", "score",
                                          "row_date", "history_dates")):
                    priority_cols.append(col)
            other_cols = [c for c in filtered.columns if c not in priority_cols]
            ordered = priority_cols + other_cols
            seen, final_cols = set(), []
            for c in ordered:
                if c not in seen:
                    seen.add(c)
                    final_cols.append(c)

            st.dataframe(filtered[final_cols], use_container_width=True, height=500, hide_index=True)

            csv = filtered[final_cols].to_csv(index=False).encode("utf-8")
            st.download_button(f"⬇️ Download filtered {label} as CSV", data=csv,
                              file_name=f"{label.lower().replace(' ', '_')}_export.csv",
                              mime="text/csv", key=f"dl_{label}")


# ════════════════════════════════════════════════════════════════
#  PAGE: DASHBOARD (default)
# ════════════════════════════════════════════════════════════════

else:  # 📊 Dashboard
    # Status pill at top
    runs, _ = get_workflow_runs(limit=1)
    if runs:
        latest = runs[0]
        status = latest.get("status", "?")
        conclusion = latest.get("conclusion", "?")
        ago = format_age(latest.get("created_at"))
        if status == "in_progress" or status == "queued":
            st.info(f"🔄 Pipeline currently running ({ago}). Numbers will update when it finishes.")
        elif conclusion == "success":
            st.success(f"✅ Last pipeline run: SUCCESS — {ago}")
        elif conclusion == "failure":
            st.error(f"❌ Last pipeline run: FAILED — {ago}. Check Pipeline Control tab.")

    # Refresh buttons
    rc1, rc2 = st.columns([1, 5])
    with rc1:
        if st.button("🔄 Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # Date filter
    st.markdown("### 📅 Date Range")
    PRESETS = ["Today", "This Week", "Last Week", "Last 7 Days", "Last 15 Days",
               "Last 28 Days", "This Month", "Last Month", "Last 3 Months",
               "Last 6 Months", "This Year", "Last Year", "All Time", "Custom"]
    preset = st.selectbox("Select period:", PRESETS, index=6)

    if preset == "Custom":
        c1, c2 = st.columns(2)
        custom_start = c1.date_input("Start", value=datetime.now().date() - timedelta(days=30))
        custom_end = c2.date_input("End", value=datetime.now().date())
        date_range = (custom_start, custom_end)
    else:
        date_range = get_date_range(preset)

    if date_range:
        st.info(f"Showing data for: **{date_range[0]}** to **{date_range[1]}**")
    else:
        st.info("Showing **all-time** data")

    # Load data
    free = load_sheet("Verified_FREE")
    upload = load_sheet("Verified_FIRST_UPLOAD")
    stripe = load_sheet("Verified_STRIPE")
    daily = load_sheet("Daily_Report")

    daily_filtered = filter_daily(daily, date_range)
    daily_collapsed = collapse_to_latest_per_day(daily_filtered)

    sum_signups = int(daily_collapsed["SignUps_Accepted"].sum()) if not daily_collapsed.empty else 0
    sum_uploads = int(daily_collapsed["FirstUploads_Accepted"].sum()) if not daily_collapsed.empty else 0
    sum_paid = int(daily_collapsed["PaidSubscribers_Accepted"].sum()) if not daily_collapsed.empty else 0

    st.markdown(f"### 📊 Totals for: {preset}")
    c1, c2, c3 = st.columns(3)
    with c1:
        card("New Sign-ups", sum_signups, "#2563eb", "verified · deduped · first-time")
    with c2:
        card("First Uploads", sum_uploads, "#16a34a", "verified · deduped · first-time")
    with c3:
        card("Paid Subscribers", sum_paid, "#ea580c", "active subscribers")

    st.divider()

    st.markdown("### 🔴 Live Right Now (current month from sheet)")
    l1, l2, l3 = st.columns(3)
    with l1:
        card("Sign-ups", n_accepted(free), "#3b82f6")
    with l2:
        card("First Uploads", n_accepted(upload), "#22c55e")
    with l3:
        card("Paid Subscribers", n_accepted(stripe), "#f97316")

    st.divider()

    # Trend chart
    st.markdown("### 📈 Trend Over Selected Period")
    if daily_collapsed.empty:
        st.info("No daily report data in this range yet.")
    else:
        chart_df = daily_collapsed.copy()
        chart_df["_dt"] = pd.to_datetime(chart_df["_dt"])
        chart = chart_df.set_index("_dt")[METRIC_COLS]
        st.plotly_chart(px.line(chart, title=f"Daily Trend — {preset}", markers=True),
                        use_container_width=True)

        funnel_df = pd.DataFrame({
            "Stage": ["Sign-ups", "First Upload", "Paid"],
            "Count": [sum_signups, sum_uploads, sum_paid],
        })
        st.plotly_chart(px.funnel(funnel_df, x="Count", y="Stage",
                                  title=f"Funnel — {preset}"),
                        use_container_width=True)

    st.divider()

    # Group by
    st.markdown("### 🗓️ Grouped Analysis")
    group_by = st.radio("Group by:", ["Day", "Week", "Month", "Year"], horizontal=True)

    if not daily_collapsed.empty:
        df = daily_collapsed.copy()
        df["_dt"] = pd.to_datetime(df["_dt"])
        if group_by == "Day":
            df["bucket"] = df["_dt"].dt.strftime("%Y-%m-%d")
        elif group_by == "Week":
            df["bucket"] = df["_dt"].dt.strftime("%Y-W%U")
        elif group_by == "Month":
            df["bucket"] = df["_dt"].dt.strftime("%Y-%m")
        else:
            df["bucket"] = df["_dt"].dt.strftime("%Y")

        grouped = df.groupby("bucket")[METRIC_COLS].sum().reset_index()
        st.plotly_chart(px.bar(grouped, x="bucket", y=METRIC_COLS,
                              title=f"Grouped by {group_by}", barmode="group"),
                        use_container_width=True)
        st.dataframe(grouped, use_container_width=True)
    else:
        st.info("No history yet for grouping.")

st.divider()
st.caption("💡 Pipeline runs daily 8/9/10 AM UTC + on-demand. Use Pipeline Control tab to trigger manually.")
