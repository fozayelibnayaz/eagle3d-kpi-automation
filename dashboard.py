"""
DASHBOARD v8 - Eagle3D KPI
Complete edition: all controls restored, daily delta, sidebar settings.
"""
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
import requests
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Eagle3D KPI Dashboard", page_icon="🦅",
                   layout="wide", initial_sidebar_state="expanded")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
METRIC_COLS = ["SignUps", "FirstUploads", "PaidSubscribers"]
LEGACY_METRIC_COLS = ["SignUps_Accepted", "FirstUploads_Accepted", "PaidSubscribers_Accepted"]


# ─── SECRETS ───────────────────────────────────────────────────
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
    st.error("Google credentials not found.")
    st.stop()


def get_secret(key, default=None):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return default


def secret_exists(key):
    try:
        return key in st.secrets
    except Exception:
        return False


CREDS_PATH = get_creds_path()
MASTER_SHEET_URL = get_secret("MASTER_SHEET_URL")
GITHUB_TOKEN = get_secret("GITHUB_TOKEN")
GITHUB_REPO = get_secret("GITHUB_REPO", "fozayelibnayaz/eagle3d-kpi-automation")
WORKFLOW_FILE = get_secret("WORKFLOW_FILE", "daily.yml")


# ─── DATA LOADING ──────────────────────────────────────────────
@st.cache_data(ttl=120)
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


# ─── GITHUB API ────────────────────────────────────────────────
def github_headers():
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def trigger_workflow():
    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN missing in secrets"
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    try:
        r = requests.post(url, headers=github_headers(), json={"ref": "main"}, timeout=15)
        if r.status_code == 204:
            return True, "Pipeline triggered. Refresh in 5-10 minutes."
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, str(e)


def get_workflow_runs(limit=10):
    if not GITHUB_TOKEN:
        return []
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/runs?per_page={limit}"
    try:
        r = requests.get(url, headers=github_headers(), timeout=15)
        if r.status_code == 200:
            return r.json().get("workflow_runs", [])
    except Exception:
        pass
    return []


def list_workflows():
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


# ─── HELPERS ───────────────────────────────────────────────────
def card(label, value, color, sub=None):
    sub_html = f"<div style=\"font-size:13px;opacity:0.85;margin-top:4px;\">{sub}</div>" if sub else ""
    html = f"<div style=\"background:{color};color:white;padding:25px;border-radius:12px;text-align:center;\">"
    html += f"<div style=\"font-size:14px;opacity:0.9;\">{label}</div>"
    html += f"<div style=\"font-size:48px;font-weight:bold;margin:8px 0;\">{value}</div>"
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
    """For Daily_Counts: true daily counts, no collapsing needed (one row per date)."""
    if df.empty or "_dt" not in df.columns:
        return df
    df = df.copy()
    # Detect schema
    if "SignUps" in df.columns:
        # New schema (Daily_Counts) - already one row per date
        df["SignUps"] = safe_int_col(df, "SignUps")
        df["FirstUploads"] = safe_int_col(df, "FirstUploads")
        df["PaidSubscribers"] = safe_int_col(df, "PaidSubscribers")
        return df.groupby("_dt", as_index=False).agg({
            "SignUps": "sum",
            "FirstUploads": "sum",
            "PaidSubscribers": "sum",
        })
    else:
        # Legacy (Daily_Report) - take max per day
        df["SignUps_Accepted"] = safe_int_col(df, "SignUps_Accepted")
        df["FirstUploads_Accepted"] = safe_int_col(df, "FirstUploads_Accepted")
        df["PaidSubscribers_Accepted"] = safe_int_col(df, "PaidSubscribers_Accepted")
        return df.groupby("_dt", as_index=False).agg({
            "SignUps_Accepted": "max",
            "FirstUploads_Accepted": "max",
            "PaidSubscribers_Accepted": "max",
        })


# ════════════════════════════════════════════════════════════════
#  SIDEBAR - controls
# ════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("🦅 Eagle3D KPIs")

    st.divider()
    st.subheader("⚙️ Pipeline Control")

    # Status
    runs = get_workflow_runs(limit=5)
    if runs:
        latest = runs[0]
        status = latest.get("status", "?")
        conclusion = latest.get("conclusion", "?")
        ago = format_age(latest.get("created_at"))
        if status in ("in_progress", "queued"):
            st.info(f"🔄 Running ({ago})")
        elif conclusion == "success":
            st.success(f"✅ Last: SUCCESS ({ago})")
        elif conclusion == "failure":
            st.error(f"❌ Last: FAILED ({ago})")
        else:
            st.warning(f"⚠️ Last: {conclusion} ({ago})")
    else:
        st.warning("No runs found")

    # Run Now button
    if st.button("🚀 Run Pipeline Now", type="primary", use_container_width=True):
        with st.spinner("Triggering..."):
            ok, msg = trigger_workflow()
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    # Refresh button
    if st.button("🔄 Refresh Dashboard", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.subheader("🗓 Date Filter")
    PRESETS = ["Today", "This Week", "Last Week", "Last 7 Days", "Last 15 Days",
               "Last 28 Days", "This Month", "Last Month", "Last 3 Months",
               "Last 6 Months", "This Year", "Last Year", "All Time", "Custom"]
    preset = st.selectbox("View Range", PRESETS, index=6)

    if preset == "Custom":
        custom_start = st.date_input("Start", value=datetime.now().date() - timedelta(days=30))
        custom_end = st.date_input("End", value=datetime.now().date())
        date_range = (custom_start, custom_end)
    else:
        date_range = get_date_range(preset)

    st.divider()
    st.subheader("📍 Navigation")
    page = st.radio("Page", ["📊 Dashboard", "🔍 Browse Data", "🛠 Diagnostics"],
                    label_visibility="collapsed")

    st.divider()
    st.caption("Pipeline auto-runs daily 10:00 AM Bangladesh time.")
    st.caption("v8 • Built with Streamlit")


# ════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════

st.title("🦅 Eagle3D Streaming — KPI Dashboard")

if date_range:
    st.caption(f"Showing: **{date_range[0]}** to **{date_range[1]}** ({preset})")
else:
    st.caption(f"Showing: **All Time**")


# ─── DIAGNOSTICS PAGE ──────────────────────────────────────────
if page == "🛠 Diagnostics":
    st.header("System Diagnostics")

    st.subheader("1. Secrets Configuration")
    expected = ["MASTER_SHEET_URL", "GOOGLE_CREDS", "GITHUB_TOKEN", "GITHUB_REPO", "WORKFLOW_FILE"]
    rows = []
    for k in expected:
        present = secret_exists(k)
        if present:
            val = get_secret(k)
            if k == "GITHUB_TOKEN" and val:
                shown = val[:12] + "..." + val[-4:]
            elif k == "GOOGLE_CREDS":
                shown = "✓ TOML section present"
            elif isinstance(val, str) and len(val) > 60:
                shown = val[:50] + "..."
            else:
                shown = str(val)[:80]
            status = "✅"
        else:
            shown = "❌ MISSING"
            status = "❌"
        rows.append({"Secret": k, "Status": status, "Value": shown})
    st.table(pd.DataFrame(rows))

    st.subheader("2. GitHub API Tests")
    if st.button("🧪 Run API Tests"):
        with st.spinner("Running tests..."):
            try:
                r = requests.get("https://api.github.com/user", headers=github_headers(), timeout=10)
                if r.status_code == 200:
                    st.success(f"✅ Auth: logged in as {r.json().get('login')}")
                else:
                    st.error(f"❌ Auth failed: {r.status_code}")
            except Exception as e:
                st.error(f"❌ Auth: {e}")

            try:
                r = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}", headers=github_headers(), timeout=10)
                if r.status_code == 200:
                    st.success(f"✅ Can access repo {GITHUB_REPO}")
                else:
                    st.error(f"❌ Repo access: {r.status_code}")
            except Exception as e:
                st.error(f"❌ Repo: {e}")

            workflows = list_workflows()
            if workflows:
                names = [w.get("path", "").split("/")[-1] for w in workflows]
                if WORKFLOW_FILE in names:
                    st.success(f"✅ Workflow {WORKFLOW_FILE} exists")
                else:
                    st.error(f"❌ Workflow not found. Available: {names}")

            runs = get_workflow_runs(limit=5)
            if runs:
                st.success(f"✅ Found {len(runs)} recent runs")
                run_data = [{
                    "Run": r.get("run_number"),
                    "Status": r.get("status"),
                    "Result": r.get("conclusion") or "-",
                    "When": format_age(r.get("created_at")),
                    "URL": r.get("html_url"),
                } for r in runs]
                st.dataframe(pd.DataFrame(run_data), use_container_width=True, hide_index=True)

    st.subheader("3. Sheet Read Test")
    if st.button("📄 Test Sheet Read"):
        df = load_sheet("Daily_Report")
        if df.empty:
            st.warning("Daily_Report is empty")
        else:
            st.success(f"✅ Read {len(df)} rows from Daily_Report")
            st.dataframe(df, use_container_width=True)


# ─── BROWSE DATA PAGE ──────────────────────────────────────────
elif page == "🔍 Browse Data":
    st.header("Browse Customer Data")
    free = load_sheet("Verified_FREE")
    upload = load_sheet("Verified_FIRST_UPLOAD")
    stripe = load_sheet("Verified_STRIPE")

    browse_tabs = st.tabs(["📥 Sign-ups", "📦 First Uploads", "💳 Paid Subscribers"])

    for tab, (label, df) in zip(browse_tabs, [
        ("Sign-ups", free), ("First Uploads", upload), ("Paid Subscribers", stripe)
    ]):
        with tab:
            if df.empty:
                st.warning(f"No {label} data yet.")
                continue

            f1, f2, f3 = st.columns([2, 2, 4])
            with f1:
                status_filter = st.selectbox("Status:", ["All", "ACCEPTED only", "REJECTED only"], key=f"s_{label}")
            with f2:
                vo = ["All"]
                if "email_verdict" in df.columns:
                    vo += sorted(df["email_verdict"].dropna().unique().tolist())
                verdict_filter = st.selectbox("Verdict:", vo, key=f"v_{label}")
            with f3:
                search = st.text_input("Search:", key=f"q_{label}", placeholder="any text...")

            filt = df.copy()
            if status_filter == "ACCEPTED only":
                filt = filt[filt["final_status"].str.upper() == "ACCEPTED"]
            elif status_filter == "REJECTED only":
                filt = filt[filt["final_status"].str.upper() == "REJECTED"]
            if verdict_filter != "All" and "email_verdict" in filt.columns:
                filt = filt[filt["email_verdict"] == verdict_filter]
            if search:
                mask = pd.Series([False] * len(filt))
                for col in filt.columns:
                    mask = mask | filt[col].astype(str).str.contains(search, case=False, na=False)
                filt = filt[mask]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Showing", len(filt))
            m2.metric("Accepted", n_accepted(filt))
            m3.metric("Disposable", n_rejected_for(filt, "isposable"))
            m4.metric("Duplicate", n_rejected_for(filt, "database"))

            st.dataframe(filt, use_container_width=True, height=500, hide_index=True)
            csv = filt.to_csv(index=False).encode("utf-8")
            st.download_button(f"⬇️ Download {label}", data=csv,
                              file_name=f"{label.lower().replace(' ', '_')}.csv",
                              mime="text/csv", key=f"dl_{label}")


# ─── DASHBOARD PAGE ────────────────────────────────────────────
else:
    free = load_sheet("Verified_FREE")
    upload = load_sheet("Verified_FIRST_UPLOAD")
    stripe = load_sheet("Verified_STRIPE")
    # Load TRUE daily counts (grouped by actual sign-up date)
    daily = load_sheet("Daily_Counts")
    if daily.empty:
        # Fallback to old format
        daily = load_sheet("Daily_Report")

    daily_filtered = filter_daily(daily, date_range)
    daily_collapsed = collapse_to_latest_per_day(daily_filtered)

    # Daily_Counts has TRUE per-day counts grouped by sign-up date
    # So we just SUM them (no max-per-month needed - they're already daily totals)
    if daily_collapsed.empty:
        sum_signups = sum_uploads = sum_paid = 0
    else:
        # Detect column names (new vs legacy)
        sc = "SignUps" if "SignUps" in daily_collapsed.columns else "SignUps_Accepted"
        uc = "FirstUploads" if "FirstUploads" in daily_collapsed.columns else "FirstUploads_Accepted"
        pc = "PaidSubscribers" if "PaidSubscribers" in daily_collapsed.columns else "PaidSubscribers_Accepted"
        sum_signups = int(safe_int_col(daily_collapsed, sc).sum())
        sum_uploads = int(safe_int_col(daily_collapsed, uc).sum())
        sum_paid = int(safe_int_col(daily_collapsed, pc).sum())

    st.markdown(f"### 📊 Totals for: {preset}")
    c1, c2, c3 = st.columns(3)
    with c1:
        card("Sign-ups", sum_signups, "#2563eb", "verified · deduped")
    with c2:
        card("First Uploads", sum_uploads, "#16a34a", "verified · deduped")
    with c3:
        card("Paid Subscribers", sum_paid, "#ea580c", "active")

    st.divider()

    st.markdown("### 🔴 Live (current month from sheet)")
    l1, l2, l3 = st.columns(3)
    with l1:
        card("Sign-ups", n_accepted(free), "#3b82f6")
    with l2:
        card("First Uploads", n_accepted(upload), "#22c55e")
    with l3:
        card("Paid", n_accepted(stripe), "#f97316")

    st.divider()

    st.markdown("### 🆕 Daily New (delta vs previous day)")
    if not daily_collapsed.empty and len(daily_collapsed) > 0:
        delta = daily_collapsed.copy()
        delta["_dt"] = pd.to_datetime(delta["_dt"])
        delta = delta.sort_values("_dt").reset_index(drop=True)
        delta["NewSignUps"] = delta["SignUps_Accepted"].diff().fillna(delta["SignUps_Accepted"]).clip(lower=0).astype(int)
        delta["NewUploads"] = delta["FirstUploads_Accepted"].diff().fillna(delta["FirstUploads_Accepted"]).clip(lower=0).astype(int)
        delta["NewPaid"] = delta["PaidSubscribers_Accepted"].diff().fillna(delta["PaidSubscribers_Accepted"]).clip(lower=0).astype(int)

        latest = delta.iloc[-1]
        d1, d2, d3 = st.columns(3)
        with d1:
            card("New Sign-ups Today", int(latest["NewSignUps"]), "#1d4ed8")
        with d2:
            card("New Uploads Today", int(latest["NewUploads"]), "#15803d")
        with d3:
            card("New Paid Today", int(latest["NewPaid"]), "#c2410c")

        if len(delta) > 1:
            chart_df = delta.set_index("_dt")[["NewSignUps", "NewUploads", "NewPaid"]]
            st.plotly_chart(px.bar(chart_df, title="Daily New Counts", barmode="group"),
                          use_container_width=True)

        with st.expander("📋 Daily details"):
            show = delta[["_dt", "NewSignUps", "NewUploads", "NewPaid",
                         "SignUps_Accepted", "FirstUploads_Accepted",
                         "PaidSubscribers_Accepted"]].copy()
            show.columns = ["Date", "New SignUps", "New Uploads", "New Paid",
                           "Cumulative SignUps", "Cumulative Uploads", "Cumulative Paid"]
            st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("Need at least 2 days of data for daily-new view.")

    st.divider()

    st.markdown("### 📈 Trend Over Period")
    if not daily_collapsed.empty:
        chart_df = daily_collapsed.copy()
        chart_df["_dt"] = pd.to_datetime(chart_df["_dt"])
        chart = chart_df.set_index("_dt")[METRIC_COLS]
        st.plotly_chart(px.line(chart, title="Cumulative Daily Snapshots", markers=True),
                       use_container_width=True)

        funnel_df = pd.DataFrame({
            "Stage": ["Sign-ups", "First Upload", "Paid"],
            "Count": [sum_signups, sum_uploads, sum_paid],
        })
        st.plotly_chart(px.funnel(funnel_df, x="Count", y="Stage", title="Funnel"),
                       use_container_width=True)
