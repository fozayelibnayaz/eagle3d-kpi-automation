"""
DASHBOARD v9 - Eagle3D KPI
- Fixed AttrDict bug (st.secrets returns dict, not JSON string)
- Uses Daily_Counts (true per-day counts by sign-up date)
- Browse Data tab shows actual emails/usernames/etc
- Sidebar with Run Now, Refresh, Diagnostics
- Works whether Daily_Counts exists or not
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


# ─── SECRETS ─────────────────────────────────────────────────────
def get_creds_path():
    """Resolve Google credentials from local file OR Streamlit secrets."""
    if os.path.exists("google_creds.json"):
        return "google_creds.json"

    # Try [GOOGLE_CREDS] TOML section
    try:
        if "GOOGLE_CREDS" in st.secrets:
            raw = st.secrets["GOOGLE_CREDS"]
            # st.secrets returns AttrDict (dict-like), so just convert it
            if isinstance(raw, str):
                creds = json.loads(raw)
            else:
                creds = dict(raw)
            tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(creds, tmp)
            tmp.close()
            return tmp.name
    except Exception as e:
        st.error(f"Failed to read GOOGLE_CREDS: {e}")
        st.stop()

    # Try GOOGLE_CREDS_JSON (legacy)
    try:
        raw = st.secrets["GOOGLE_CREDS_JSON"]
        creds = json.loads(raw) if isinstance(raw, str) else dict(raw)
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(creds, tmp)
        tmp.close()
        return tmp.name
    except Exception:
        pass

    st.error("Google credentials not found in any expected location.")
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


# ─── DATA LOADING ────────────────────────────────────────────────
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


# ─── GITHUB API ──────────────────────────────────────────────────
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


def format_age(iso_str):
    if not iso_str:
        return "?"
    try:
        ts = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - ts
        s = age.total_seconds()
        if s < 60: return f"{int(s)}s ago"
        if s < 3600: return f"{int(s//60)}m ago"
        if s < 86400: return f"{int(s//3600)}h ago"
        return f"{int(s//86400)}d ago"
    except Exception:
        return iso_str


# ─── HELPERS ─────────────────────────────────────────────────────
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


def n_accepted_current_month(df, date_field):
    """Count rows where final_status=ACCEPTED AND date is in current month."""
    if df.empty:
        return 0
    if "final_status" not in df.columns:
        return 0
    
    from datetime import datetime
    current_month = datetime.now().strftime("%Y-%m")
    
    # Filter to ACCEPTED
    accepted = df[df["final_status"].astype(str).str.upper() == "ACCEPTED"]
    if accepted.empty:
        return 0
    
    # Filter to current month using the date field
    if date_field not in accepted.columns:
        return 0
    
    # Parse dates and check if they're in current month
    def in_current_month(val):
        if not val or str(val).strip() in ("", "—", "-", "nan"):
            return False
        s = str(val).strip()
        # Try YYYY-MM-DD format first
        if s.startswith(current_month):
            return True
        # Try MM/DD/YY or MM/DD/YYYY (Stripe)
        try:
            from datetime import datetime as _dt
            for fmt in ["%m/%d/%y, %I:%M %p", "%m/%d/%Y, %I:%M %p",
                        "%m/%d/%y", "%m/%d/%Y", "%a %b %d %Y",
                        "%a %b %d %Y %H:%M:%S"]:
                try:
                    parsed = _dt.strptime(s, fmt)
                    if parsed.strftime("%Y-%m") == current_month:
                        return True
                    return False
                except Exception:
                    continue
        except Exception:
            pass
        # Try RFC 2822 (KPI dashboard)
        try:
            from email.utils import parsedate_to_datetime
            parsed = parsedate_to_datetime(s)
            if parsed and parsed.strftime("%Y-%m") == current_month:
                return True
        except Exception:
            pass
        return False
    
    in_month_mask = accepted[date_field].apply(in_current_month)
    return int(in_month_mask.sum())



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


def detect_metric_columns(df):
    """Return tuple (signups_col, uploads_col, paid_col) detecting which schema."""
    if "SignUps" in df.columns:
        return ("SignUps", "FirstUploads", "PaidSubscribers")
    return ("SignUps_Accepted", "FirstUploads_Accepted", "PaidSubscribers_Accepted")


# ════════════════════════════════════════════════════════════════
#  SIDEBAR
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.title("🦅 Eagle3D KPIs")

    st.divider()
    st.subheader("⚙️ Pipeline Control")

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

    if st.button("🚀 Run Pipeline Now", type="primary"):
        with st.spinner("Triggering..."):
            ok, msg = trigger_workflow()
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    if st.button("🔄 Refresh Dashboard"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.subheader("🗓 Date Filter")
    PRESETS = ["Today", "This Week", "Last Week", "Last 7 Days", "Last 15 Days",
               "Last 28 Days", "This Month", "Last Month", "Last 3 Months",
               "Last 6 Months", "This Year", "Last Year", "All Time", "Custom"]
    preset = st.selectbox("View Range", PRESETS, index=12)  # default All Time

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
    st.caption("Pipeline auto-runs daily at 10 AM Bangladesh.")


# ════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════
st.title("🦅 Eagle3D Streaming — KPI Dashboard")

if date_range:
    st.caption(f"Showing: **{date_range[0]}** to **{date_range[1]}** ({preset})")
else:
    st.caption("Showing: **All Time**")


# ─── DIAGNOSTICS PAGE ────────────────────────────────────────────
if page == "🛠 Diagnostics":
    st.header("System Diagnostics")
    expected = ["MASTER_SHEET_URL", "GOOGLE_CREDS", "GITHUB_TOKEN", "GITHUB_REPO", "WORKFLOW_FILE"]
    rows = []
    for k in expected:
        present = secret_exists(k)
        if present:
            val = get_secret(k)
            if k == "GITHUB_TOKEN" and val:
                shown = val[:12] + "..." + val[-4:]
            elif k == "GOOGLE_CREDS":
                shown = "✓ TOML section"
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

    if st.button("🧪 Test Sheet Read"):
        for tab_name in ["Daily_Counts", "Daily_Report", "Verified_FREE",
                        "Verified_FIRST_UPLOAD", "Verified_STRIPE"]:
            df = load_sheet(tab_name)
            if df.empty:
                st.warning(f"❌ {tab_name}: empty or not found")
            else:
                st.success(f"✅ {tab_name}: {len(df)} rows")


# ─── BROWSE DATA PAGE ────────────────────────────────────────────
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

            st.dataframe(filt, height=500, hide_index=True)
            csv = filt.to_csv(index=False).encode("utf-8")
            st.download_button(f"⬇️ Download {label}", data=csv,
                              file_name=f"{label.lower().replace(' ', '_')}.csv",
                              mime="text/csv", key=f"dl_{label}")


# ─── DASHBOARD PAGE ──────────────────────────────────────────────
else:
    free = load_sheet("Verified_FREE")
    upload = load_sheet("Verified_FIRST_UPLOAD")
    stripe = load_sheet("Verified_STRIPE")

    # PRIMARY source: Daily_Counts (true per-day from sign-up dates)
    daily = load_sheet("Daily_Counts")
    if daily.empty:
        st.warning("Daily_Counts not yet populated. Run pipeline once to generate it.")
        daily = load_sheet("Daily_Report")  # fallback

    if daily.empty:
        st.error("No daily data available. Trigger the pipeline first.")
        st.stop()

    sc, uc, pc = detect_metric_columns(daily)
    st.caption(f"Data source columns: {sc}, {uc}, {pc}")

    # Build _dt
    if "Date" in daily.columns:
        daily["_dt"] = pd.to_datetime(daily["Date"], errors="coerce").dt.date
    elif "Timestamp" in daily.columns:
        daily["_dt"] = pd.to_datetime(daily["Timestamp"], errors="coerce").dt.date
    daily = daily.dropna(subset=["_dt"])

    # Filter by date range
    if date_range:
        daily_filtered = daily[(daily["_dt"] >= date_range[0]) & (daily["_dt"] <= date_range[1])]
    else:
        daily_filtered = daily

    # Convert to int
    daily_filtered = daily_filtered.copy()
    daily_filtered["_signups"] = safe_int_col(daily_filtered, sc)
    daily_filtered["_uploads"] = safe_int_col(daily_filtered, uc)
    daily_filtered["_paid"] = safe_int_col(daily_filtered, pc)

    # Compute totals based on schema
    if "SignUps" in daily.columns:
        # Daily_Counts: each row = real daily count → SUM
        sum_signups = int(daily_filtered["_signups"].sum())
        sum_uploads = int(daily_filtered["_uploads"].sum())
        sum_paid = int(daily_filtered["_paid"].sum())
    else:
        # Daily_Report (legacy): cumulative snapshot → MAX per day, then sum across months
        per_day = daily_filtered.groupby("_dt", as_index=False).agg({
            "_signups": "max", "_uploads": "max", "_paid": "max"
        })
        per_day["_dt"] = pd.to_datetime(per_day["_dt"])
        per_day["_ym"] = per_day["_dt"].dt.strftime("%Y-%m")
        per_month = per_day.groupby("_ym").agg({
            "_signups": "max", "_uploads": "max", "_paid": "max"
        })
        sum_signups = int(per_month["_signups"].sum())
        sum_uploads = int(per_month["_uploads"].sum())
        sum_paid = int(per_month["_paid"].sum())

    st.markdown(f"### 📊 Totals for: {preset}")
    c1, c2, c3 = st.columns(3)
    with c1:
        card("Sign-ups", sum_signups, "#2563eb", "verified · deduped")
    with c2:
        card("First Uploads", sum_uploads, "#16a34a", "verified · deduped")
    with c3:
        card("Paid Subscribers", sum_paid, "#ea580c", "active")

    st.divider()

    from datetime import datetime as _dt_now
    _current_month_label = _dt_now.now().strftime("%B %Y")
    st.markdown(f"### 🔴 Live Snapshot ({_current_month_label})")
    l1, l2, l3 = st.columns(3)
    with l1:
        card("Sign-ups",
             n_accepted_current_month(free, "Account Created On"),
             "#3b82f6", "this month, accepted")
    with l2:
        card("First Uploads",
             n_accepted_current_month(upload, "Upload Date"),
             "#22c55e", "this month, accepted")
    with l3:
        # Stripe: try First payment first, fall back to Created
        if not stripe.empty and "First payment" in stripe.columns:
            paid_count = n_accepted_current_month(stripe, "First payment")
        elif not stripe.empty and "Created" in stripe.columns:
            paid_count = n_accepted_current_month(stripe, "Created")
        else:
            paid_count = 0
        card("Paid", paid_count, "#f97316", "this month, paid")

    st.divider()

    # Trend chart
    st.markdown("### 📈 Daily Trend")
    if not daily_filtered.empty:
        chart_df = daily_filtered[["_dt", "_signups", "_uploads", "_paid"]].copy()
        chart_df.columns = ["Date", "Sign-ups", "First Uploads", "Paid"]
        chart_df["Date"] = pd.to_datetime(chart_df["Date"])
        chart_df = chart_df.sort_values("Date")
        chart = chart_df.set_index("Date")
        st.plotly_chart(px.line(chart, title=f"Daily Trend - {preset}", markers=True))

        funnel_df = pd.DataFrame({
            "Stage": ["Sign-ups", "First Upload", "Paid"],
            "Count": [sum_signups, sum_uploads, sum_paid],
        })
        st.plotly_chart(px.funnel(funnel_df, x="Count", y="Stage", title=f"Funnel - {preset}"))

    # Daily breakdown table
    st.markdown("### 📋 Daily Breakdown")
    if not daily_filtered.empty:
        show_df = daily_filtered[["_dt", "_signups", "_uploads", "_paid"]].copy()
        show_df.columns = ["Date", "Sign-ups", "First Uploads", "Paid Subscribers"]
        show_df = show_df.sort_values("Date", ascending=False)
        st.dataframe(show_df, hide_index=True, height=400)

    st.divider()

    # Group by
    st.markdown("### 🗓️ Grouped Analysis")
    group_by = st.radio("Group by:", ["Day", "Week", "Month", "Year"], horizontal=True)

    if not daily_filtered.empty:
        gdf = daily_filtered[["_dt", "_signups", "_uploads", "_paid"]].copy()
        gdf["_dt"] = pd.to_datetime(gdf["_dt"])
        if group_by == "Day":
            gdf["bucket"] = gdf["_dt"].dt.strftime("%Y-%m-%d")
        elif group_by == "Week":
            gdf["bucket"] = gdf["_dt"].dt.strftime("%Y-W%U")
        elif group_by == "Month":
            gdf["bucket"] = gdf["_dt"].dt.strftime("%Y-%m")
        else:
            gdf["bucket"] = gdf["_dt"].dt.strftime("%Y")

        grouped = gdf.groupby("bucket").agg({
            "_signups": "sum", "_uploads": "sum", "_paid": "sum"
        }).reset_index()
        grouped.columns = ["Period", "Sign-ups", "First Uploads", "Paid"]
        st.plotly_chart(px.bar(grouped, x="Period", y=["Sign-ups", "First Uploads", "Paid"],
                              title=f"Grouped by {group_by}", barmode="group"))
        st.dataframe(grouped, hide_index=True)
