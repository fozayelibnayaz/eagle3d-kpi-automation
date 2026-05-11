"""
DASHBOARD v5
- Bulletproof column handling (no Year/Month leaking into metrics)
- Strict numeric conversion only on metric columns
- Browseable / searchable customer data
- Full date filters
- Day/Week/Month/Year grouping
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

from config import GOOGLE_CREDS_FILE, MASTER_SHEET_URL

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

# CANONICAL metric columns — these are the ONLY ones we treat as numeric metrics
METRIC_COLS = ["SignUps_Accepted", "FirstUploads_Accepted", "PaidSubscribers_Accepted"]

st.set_page_config(page_title="Eagle3D KPI Dashboard", page_icon="E", layout="wide")


@st.cache_data(ttl=300)
def load(tab):
    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(MASTER_SHEET_URL)
    try:
        ws = sh.worksheet(tab)
        data = ws.get_all_values()
        if len(data) < 2:
            return pd.DataFrame()
        # De-duplicate column names
        headers = data[0]
        seen = {}
        clean_headers = []
        for h in headers:
            h = h.strip() if h else "unknown"
            if h in seen:
                seen[h] += 1
                clean_headers.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                clean_headers.append(h)
        return pd.DataFrame(data[1:], columns=clean_headers)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()


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
    """Get a column as int safely, even if missing/non-numeric."""
    if col not in df.columns:
        return pd.Series([0] * len(df))
    return pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)


def get_date_range(preset):
    today = datetime.now().date()
    if preset == "Today":
        return (today, today)
    if preset == "This Week":
        start = today - timedelta(days=today.weekday())
        return (start, today)
    if preset == "Last Week":
        end = today - timedelta(days=today.weekday() + 1)
        start = end - timedelta(days=6)
        return (start, end)
    if preset == "Last 7 Days":
        return (today - timedelta(days=6), today)
    if preset == "Last 15 Days":
        return (today - timedelta(days=14), today)
    if preset == "Last 28 Days":
        return (today - timedelta(days=27), today)
    if preset == "This Month":
        return (today.replace(day=1), today)
    if preset == "Last Month":
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        return (last_prev.replace(day=1), last_prev)
    if preset == "Last 3 Months":
        return (today - timedelta(days=90), today)
    if preset == "Last 6 Months":
        return (today - timedelta(days=180), today)
    if preset == "This Year":
        return (today.replace(month=1, day=1), today)
    if preset == "Last Year":
        last = today.year - 1
        return (datetime(last, 1, 1).date(), datetime(last, 12, 31).date())
    return None


# ─── HEADER ─────────────────────────────────────────
st.title("Eagle3D Streaming - KPI Dashboard")
st.caption(f"Loaded at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | cache 5 min")

cols = st.columns([1, 5])
with cols[0]:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()

# ─── DATE FILTER ─────────────────────────────────────
st.markdown("### 📅 Date Range")
PRESETS = [
    "Today", "This Week", "Last Week",
    "Last 7 Days", "Last 15 Days", "Last 28 Days",
    "This Month", "Last Month",
    "Last 3 Months", "Last 6 Months",
    "This Year", "Last Year", "All Time", "Custom",
]
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

# ─── LOAD DATA ─────────────────────────────────────
free = load("Verified_FREE")
upload = load("Verified_FIRST_UPLOAD")
stripe = load("Verified_STRIPE")
daily = load("Daily_Report")


def filter_daily(df, dr):
    if df.empty:
        return df
    df = df.copy()

    # Build date column from Date or Timestamp
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

# Compute period totals - max per day, then sum
sum_signups = sum_uploads = sum_paid = 0

if not daily_filtered.empty:
    # Pull only the metric columns (avoid Year leaking in)
    metric_data = pd.DataFrame({
        "_dt": daily_filtered["_dt"],
        "SignUps_Accepted": safe_int_col(daily_filtered, "SignUps_Accepted"),
        "FirstUploads_Accepted": safe_int_col(daily_filtered, "FirstUploads_Accepted"),
        "PaidSubscribers_Accepted": safe_int_col(daily_filtered, "PaidSubscribers_Accepted"),
    })
    per_day = metric_data.groupby("_dt").max()
    sum_signups = int(per_day["SignUps_Accepted"].sum())
    sum_uploads = int(per_day["FirstUploads_Accepted"].sum())
    sum_paid = int(per_day["PaidSubscribers_Accepted"].sum())

# ─── TOP CARDS ────────────────────────────────────
st.markdown(f"### 📊 Totals for: {preset}")
c1, c2, c3 = st.columns(3)
with c1:
    card("New Sign-ups", sum_signups, "#2563eb", "verified, deduped, first-time")
with c2:
    card("First Uploads", sum_uploads, "#16a34a", "verified, deduped, first-time")
with c3:
    card("Paid Subscribers", sum_paid, "#ea580c", "active subscribers")

st.divider()

# ─── LIVE NOW ─────────────────────────────────────
st.markdown("### 🔴 Live Right Now (current month from sheet)")
l1, l2, l3 = st.columns(3)
with l1:
    card("Sign-ups", n_accepted(free), "#3b82f6")
with l2:
    card("First Uploads", n_accepted(upload), "#22c55e")
with l3:
    card("Paid Subscribers", n_accepted(stripe), "#f97316")

st.divider()

# ─── TREND CHART ───────────────────────────────────
st.markdown("### 📈 Trend Over Selected Period")
if daily_filtered.empty:
    st.info("No daily report data in this range yet.")
else:
    metric_data = pd.DataFrame({
        "_dt": daily_filtered["_dt"],
        "SignUps_Accepted": safe_int_col(daily_filtered, "SignUps_Accepted"),
        "FirstUploads_Accepted": safe_int_col(daily_filtered, "FirstUploads_Accepted"),
        "PaidSubscribers_Accepted": safe_int_col(daily_filtered, "PaidSubscribers_Accepted"),
    })
    per_day = metric_data.groupby("_dt").max().reset_index()
    per_day["_dt"] = pd.to_datetime(per_day["_dt"])
    chart = per_day.set_index("_dt")
    st.plotly_chart(px.line(chart, title=f"Daily Trend - {preset}", markers=True),
                    use_container_width=True)

    st.markdown("### 🎯 Conversion Funnel")
    funnel_df = pd.DataFrame({
        "Stage": ["Sign-ups", "First Upload", "Paid"],
        "Count": [sum_signups, sum_uploads, sum_paid],
    })
    st.plotly_chart(px.funnel(funnel_df, x="Count", y="Stage",
                              title=f"Funnel - {preset}"),
                    use_container_width=True)

st.divider()

# ─── GROUP BY ─────────────────────────────────────
st.markdown("### 🗓️ Grouped Analysis")
group_by = st.radio("Group by:", ["Day", "Week", "Month", "Year"], horizontal=True)

if not daily_filtered.empty:
    df = pd.DataFrame({
        "_dt": pd.to_datetime(daily_filtered["_dt"]),
        "SignUps_Accepted": safe_int_col(daily_filtered, "SignUps_Accepted"),
        "FirstUploads_Accepted": safe_int_col(daily_filtered, "FirstUploads_Accepted"),
        "PaidSubscribers_Accepted": safe_int_col(daily_filtered, "PaidSubscribers_Accepted"),
    })
    if group_by == "Day":
        df["bucket"] = df["_dt"].dt.strftime("%Y-%m-%d")
    elif group_by == "Week":
        df["bucket"] = df["_dt"].dt.strftime("%Y-W%U")
    elif group_by == "Month":
        df["bucket"] = df["_dt"].dt.strftime("%Y-%m")
    else:
        df["bucket"] = df["_dt"].dt.strftime("%Y")

    # Max per day per bucket, then sum across days
    per_day_per_bucket = df.groupby(["bucket", df["_dt"].dt.date])[METRIC_COLS].max().reset_index()
    grouped = per_day_per_bucket.groupby("bucket")[METRIC_COLS].sum().reset_index()

    st.plotly_chart(px.bar(grouped, x="bucket", y=METRIC_COLS,
                          title=f"Grouped by {group_by}", barmode="group"),
                    use_container_width=True)
    st.dataframe(grouped, use_container_width=True)

st.divider()

# ─── 🔍 BROWSE DATA ────────────────────────────────
st.markdown("## 🔍 Browse Customer Data")
st.caption("Search and filter through actual emails, usernames, plans, etc.")

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
            status_filter = st.selectbox("Status:", ["All", "ACCEPTED only", "REJECTED only"], key=f"s_{label}")
        with f2:
            verdict_options = ["All"] + (sorted(df["email_verdict"].dropna().unique().tolist())
                                         if "email_verdict" in df.columns else [])
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

st.divider()
st.caption("💡 Pipeline runs daily 9 AM. Click 🔄 Refresh after a manual run.")
