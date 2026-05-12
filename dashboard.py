"""
DASHBOARD - Eagle3D KPI v6
- Per-day dedup (fixes doubling bug)
- Run Now button (triggers GitHub Action)
- Pipeline status widget
- Cookie freshness warnings
"""
import json
import os
import tempfile
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
import requests
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Eagle3D KPI Dashboard", page_icon="E", layout="wide")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

METRIC_COLS = ["SignUps_Accepted", "FirstUploads_Accepted", "PaidSubscribers_Accepted"]


# ─── CREDENTIAL & SECRETS LOADING ────────────────────────────────
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
    st.error("Google credentials not found.")
    st.stop()


def get_secret(key, default=None):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return default


CREDS_PATH = get_creds_path()
MASTER_SHEET_URL = get_secret("MASTER_SHEET_URL")
GITHUB_TOKEN = get_secret("GITHUB_TOKEN")
GITHUB_REPO = get_secret("GITHUB_REPO", "fozayelibnayaz/eagle3d-kpi-automation")
WORKFLOW_FILE = get_secret("WORKFLOW_FILE", "daily.yml")


# ─── DATA LOADING ────────────────────────────────────────────────
@st.cache_data(ttl=180)
def load(tab):
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


# ─── GITHUB ACTIONS API ──────────────────────────────────────────
def trigger_github_workflow():
    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN not configured in secrets"
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {"ref": "main"}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code == 204:
            return True, "Workflow triggered successfully"
        return False, f"GitHub API returned {r.status_code}: {r.text}"
    except Exception as e:
        return False, f"Request failed: {e}"


def get_last_workflow_run():
    if not GITHUB_TOKEN:
        return None
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/runs?per_page=1"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            runs = r.json().get("workflow_runs", [])
            if runs:
                return runs[0]
    except Exception:
        pass
    return None


# ─── HELPERS ─────────────────────────────────────────────────────
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


# ─── HEADER ──────────────────────────────────────────────────────
st.title("Eagle3D Streaming - KPI Dashboard")
st.caption(f"Loaded at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ─── TOP CONTROL BAR ─────────────────────────────────────────────
ctrl1, ctrl2, ctrl3, ctrl4 = st.columns([1.2, 1.2, 1.5, 4])

with ctrl1:
    if st.button("Refresh View", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

with ctrl2:
    if st.button("Run Pipeline Now", use_container_width=True, type="primary"):
        with st.spinner("Triggering pipeline..."):
            ok, msg = trigger_github_workflow()
        if ok:
            st.success(f"Pipeline triggered. Takes ~10 minutes. {msg}")
        else:
            st.error(f"Could not trigger: {msg}")

with ctrl3:
    last_run = get_last_workflow_run()
    if last_run:
        status = last_run.get("status", "?")
        conclusion = last_run.get("conclusion", "?")
        created = last_run.get("created_at", "")
        try:
            ts = datetime.fromisoformat(created.replace("Z", "+00:00"))
            age = datetime.now(ts.tzinfo) - ts
            ago = f"{int(age.total_seconds()//3600)}h ago" if age.total_seconds() > 3600 else f"{int(age.total_seconds()//60)}m ago"
        except Exception:
            ago = "?"
        if status == "completed" and conclusion == "success":
            st.success(f"Last run: SUCCESS ({ago})")
        elif status == "in_progress":
            st.info(f"Pipeline running now... ({ago})")
        else:
            st.warning(f"Last run: {conclusion} ({ago})")
    else:
        st.info("No workflow runs yet")

with ctrl4:
    pass  # reserved

st.divider()

# ─── DATE FILTER ─────────────────────────────────────────────────
st.markdown("### Date Range")
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
    st.info(f"Showing data for: {date_range[0]} to {date_range[1]}")
else:
    st.info("Showing all-time data")

# ─── LOAD DATA ───────────────────────────────────────────────────
free = load("Verified_FREE")
upload = load("Verified_FIRST_UPLOAD")
stripe = load("Verified_STRIPE")
daily = load("Daily_Report")


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


daily_filtered = filter_daily(daily, date_range)

# ── BUG FIX: collapse multiple runs per day to LATEST entry only ──
sum_signups = sum_uploads = sum_paid = 0
if not daily_filtered.empty:
    df_calc = daily_filtered.copy()
    df_calc["SignUps_Accepted"] = safe_int_col(df_calc, "SignUps_Accepted")
    df_calc["FirstUploads_Accepted"] = safe_int_col(df_calc, "FirstUploads_Accepted")
    df_calc["PaidSubscribers_Accepted"] = safe_int_col(df_calc, "PaidSubscribers_Accepted")

    # If there's a Timestamp column, sort by it and keep last per day (latest run wins)
    if "Timestamp" in df_calc.columns:
        df_calc["_ts"] = pd.to_datetime(df_calc["Timestamp"], errors="coerce")
        df_calc = df_calc.sort_values("_ts").groupby("_dt").tail(1)
    else:
        df_calc = df_calc.groupby("_dt").max(numeric_only=True).reset_index()

    sum_signups = int(df_calc["SignUps_Accepted"].sum())
    sum_uploads = int(df_calc["FirstUploads_Accepted"].sum())
    sum_paid = int(df_calc["PaidSubscribers_Accepted"].sum())

# ─── TOP CARDS ───────────────────────────────────────────────────
st.markdown(f"### Totals for: {preset}")
c1, c2, c3 = st.columns(3)
with c1:
    card("New Sign-ups", sum_signups, "#2563eb", "verified, deduped, first-time")
with c2:
    card("First Uploads", sum_uploads, "#16a34a", "verified, deduped, first-time")
with c3:
    card("Paid Subscribers", sum_paid, "#ea580c", "active subscribers")

st.divider()

st.markdown("### Live Right Now (current month from sheet)")
l1, l2, l3 = st.columns(3)
with l1:
    card("Sign-ups", n_accepted(free), "#3b82f6")
with l2:
    card("First Uploads", n_accepted(upload), "#22c55e")
with l3:
    card("Paid Subscribers", n_accepted(stripe), "#f97316")

st.divider()

# ─── TREND ───────────────────────────────────────────────────────
st.markdown("### Trend Over Selected Period")
if daily_filtered.empty:
    st.info("No daily report data in this range yet.")
else:
    df_chart = daily_filtered.copy()
    df_chart["SignUps_Accepted"] = safe_int_col(df_chart, "SignUps_Accepted")
    df_chart["FirstUploads_Accepted"] = safe_int_col(df_chart, "FirstUploads_Accepted")
    df_chart["PaidSubscribers_Accepted"] = safe_int_col(df_chart, "PaidSubscribers_Accepted")
    if "Timestamp" in df_chart.columns:
        df_chart["_ts"] = pd.to_datetime(df_chart["Timestamp"], errors="coerce")
        df_chart = df_chart.sort_values("_ts").groupby("_dt").tail(1)
    else:
        df_chart = df_chart.groupby("_dt").max(numeric_only=True).reset_index()
    df_chart["_dt"] = pd.to_datetime(df_chart["_dt"])
    chart = df_chart.set_index("_dt")[METRIC_COLS]
    st.plotly_chart(px.line(chart, title=f"Daily Trend - {preset}", markers=True),
                    use_container_width=True)

    funnel_df = pd.DataFrame({
        "Stage": ["Sign-ups", "First Upload", "Paid"],
        "Count": [sum_signups, sum_uploads, sum_paid],
    })
    st.plotly_chart(px.funnel(funnel_df, x="Count", y="Stage",
                              title=f"Funnel - {preset}"),
                    use_container_width=True)

st.divider()

# ─── GROUP BY ────────────────────────────────────────────────────
st.markdown("### Grouped Analysis")
group_by = st.radio("Group by:", ["Day", "Week", "Month", "Year"], horizontal=True)

if not daily_filtered.empty:
    df = daily_filtered.copy()
    df["_dt"] = pd.to_datetime(df["_dt"])
    df["SignUps_Accepted"] = safe_int_col(df, "SignUps_Accepted")
    df["FirstUploads_Accepted"] = safe_int_col(df, "FirstUploads_Accepted")
    df["PaidSubscribers_Accepted"] = safe_int_col(df, "PaidSubscribers_Accepted")

    # Latest per day first
    if "Timestamp" in df.columns:
        df["_ts"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df = df.sort_values("_ts").groupby(df["_dt"].dt.date).tail(1)
    else:
        df = df.groupby(df["_dt"].dt.date).max(numeric_only=True).reset_index()
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

st.divider()

# ─── BROWSE ──────────────────────────────────────────────────────
st.markdown("## Browse Customer Data")
st.caption("Search and filter through emails, usernames, plans, dates.")

browse_tabs = st.tabs(["Sign-ups", "First Uploads", "Paid Subscribers"])

for tab, (label, df) in zip(browse_tabs, [
    ("Sign-ups", free), ("First Uploads", upload), ("Paid Subscribers", stripe)
]):
    with tab:
        if df.empty:
            st.warning(f"No {label} data yet.")
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
            search_term = st.text_input("Search any field:", key=f"q_{label}",
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
        st.download_button(f"Download filtered {label} as CSV", data=csv,
                          file_name=f"{label.lower().replace(' ', '_')}_export.csv",
                          mime="text/csv", key=f"dl_{label}")

st.divider()

# ─── COOKIE / SESSION HEALTH ─────────────────────────────────────
st.markdown("### Session Health")
st.caption("If runs start failing, your KPI or Stripe cookies probably expired. Re-upload them as GitHub secrets.")
sh1, sh2 = st.columns(2)
with sh1:
    st.info("**KPI Dashboard cookies** typically last ~30 days. To refresh: run `python kpi_login.py` locally, then update the GitHub secret.")
with sh2:
    st.info("**Stripe cookies** typically last ~14 days. To refresh: re-export via Cookie-Editor extension, then update the GitHub secret.")

st.divider()
st.caption("Pipeline runs daily 8/9/10 AM UTC + on-demand. Click 'Run Pipeline Now' for instant update.")
