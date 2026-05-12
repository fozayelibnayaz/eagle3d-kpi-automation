# dashboard.py — Eagle3D KPI Dashboard v3.0
# Full-featured production dashboard with all analytics
# FIX: secrets type handling, iloc[-1] for live, comprehensive UI

import streamlit as st
import gspread
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import json

# ── Page Config ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Eagle3D KPI Dashboard",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ────────────────────────────────────────────────────
st.markdown("""
<style>
/* Main background */
.stApp {
    background: linear-gradient(135deg, #0a0e1a 0%, #1a1f35 50%, #0d1117 100%);
}

/* Header styling */
h1 {
    background: linear-gradient(90deg, #00d4ff, #7b2ff7, #ff6b6b);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 2.5rem !important;
    font-weight: 800 !important;
    letter-spacing: -0.5px;
    padding-bottom: 0.3rem;
}

h2 {
    color: #e2e8f0 !important;
    font-weight: 700 !important;
    border-bottom: 2px solid rgba(123, 47, 247, 0.3);
    padding-bottom: 8px;
    margin-top: 1.5rem !important;
}

h3 {
    color: #cbd5e1 !important;
    font-weight: 600 !important;
}

/* Metric cards */
[data-testid="stMetric"] {
    background: linear-gradient(145deg, rgba(30, 41, 82, 0.8), rgba(20, 27, 55, 0.9));
    border: 1px solid rgba(123, 47, 247, 0.25);
    border-radius: 16px;
    padding: 20px 24px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3), inset 0 1px 0 rgba(255,255,255,0.05);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}

[data-testid="stMetric"]:hover {
    transform: translateY(-2px);
    box-shadow: 0 12px 40px rgba(123, 47, 247, 0.2), inset 0 1px 0 rgba(255,255,255,0.08);
    border-color: rgba(123, 47, 247, 0.5);
}

[data-testid="stMetricLabel"] {
    color: #94a3b8 !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

[data-testid="stMetricValue"] {
    color: #f1f5f9 !important;
    font-size: 2.2rem !important;
    font-weight: 800 !important;
}

[data-testid="stMetricDelta"] {
    font-size: 0.85rem !important;
    font-weight: 600 !important;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f1629 0%, #1a1f35 100%);
    border-right: 1px solid rgba(123, 47, 247, 0.15);
}

[data-testid="stSidebar"] .stMarkdown p {
    color: #94a3b8;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: rgba(15, 22, 41, 0.5);
    border-radius: 12px;
    padding: 4px;
}

.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    color: #94a3b8;
    font-weight: 600;
    padding: 10px 20px;
}

.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, rgba(123, 47, 247, 0.3), rgba(0, 212, 255, 0.2));
    color: #f1f5f9 !important;
    border: 1px solid rgba(123, 47, 247, 0.4);
}

/* Expander */
.streamlit-expanderHeader {
    background: rgba(30, 41, 82, 0.5);
    border-radius: 12px;
    color: #e2e8f0 !important;
    font-weight: 600;
}

/* Dataframe */
[data-testid="stDataFrame"] {
    border-radius: 12px;
    overflow: hidden;
}

/* Dividers */
hr {
    border-color: rgba(123, 47, 247, 0.15) !important;
    margin: 2rem 0 !important;
}

/* Status indicator dot */
.status-dot {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    margin-right: 6px;
    animation: pulse 2s infinite;
}
.status-dot.live { background: #22c55e; }
.status-dot.stale { background: #f59e0b; }
.status-dot.offline { background: #ef4444; }

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

/* Cards container */
.kpi-section {
    background: rgba(15, 22, 41, 0.4);
    border: 1px solid rgba(123, 47, 247, 0.1);
    border-radius: 20px;
    padding: 24px;
    margin: 12px 0;
}

/* Info boxes */
.stAlert {
    border-radius: 12px !important;
}

/* Button styling */
.stButton > button {
    background: linear-gradient(135deg, #7b2ff7, #00d4ff);
    color: white;
    border: none;
    border-radius: 10px;
    font-weight: 600;
    padding: 8px 20px;
    transition: all 0.3s ease;
}

.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 20px rgba(123, 47, 247, 0.4);
}

/* Selectbox / date input */
[data-testid="stSelectbox"], [data-testid="stDateInput"] {
    background: rgba(30, 41, 82, 0.3);
    border-radius: 10px;
}

/* Footer */
.footer-text {
    text-align: center;
    color: #475569;
    font-size: 0.75rem;
    padding: 2rem 0 1rem 0;
    border-top: 1px solid rgba(123, 47, 247, 0.1);
    margin-top: 3rem;
}
</style>
""", unsafe_allow_html=True)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

MASTER_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1E5PI3-m7mTMKRQ4Cy-WqpVCo5dQjbICcA2EnrC9ORE4/edit"
)

KPI_COLS = ["SignUps_Accepted", "FirstUploads_Accepted", "PaidSubscribers_Accepted"]
KPI_LABELS = {
    "SignUps_Accepted": "Sign-Ups",
    "FirstUploads_Accepted": "First Uploads",
    "PaidSubscribers_Accepted": "Paid Subscribers"
}
KPI_ICONS = {
    "SignUps_Accepted": "✅",
    "FirstUploads_Accepted": "📤",
    "PaidSubscribers_Accepted": "💳"
}
KPI_COLORS = {
    "SignUps_Accepted": "#22c55e",
    "FirstUploads_Accepted": "#3b82f6",
    "PaidSubscribers_Accepted": "#f59e0b"
}


# ═════════════════════════════════════════════════════════════════
#  DATA LAYER
# ═════════════════════════════════════════════════════════════════

@st.cache_resource(ttl=300)
def get_sheet_client():
    """Handles dict / AttrDict / str for Streamlit Cloud secrets."""
    if "GOOGLE_CREDS" in st.secrets:
        raw = st.secrets["GOOGLE_CREDS"]
        if hasattr(raw, "to_dict"):
            creds_dict = raw.to_dict()
        elif isinstance(raw, dict):
            creds_dict = dict(raw)
        elif isinstance(raw, str):
            creds_dict = json.loads(raw)
        else:
            st.error(f"GOOGLE_CREDS unexpected type: {type(raw)}")
            st.stop()
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("google_creds.json", scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_url(MASTER_SHEET_URL)


@st.cache_data(ttl=120)
def load_daily_report() -> pd.DataFrame:
    """Load Daily_Report tab into a clean DataFrame."""
    sh = get_sheet_client()
    try:
        ws = sh.worksheet("Daily_Report")
    except gspread.WorksheetNotFound:
        return pd.DataFrame()

    data = ws.get_all_values()
    if len(data) < 2:
        return pd.DataFrame()

    df = pd.DataFrame(data[1:], columns=data[0])

    for col in KPI_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])
        df = df.sort_values("Date").reset_index(drop=True)

    if "Year" in df.columns:
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").fillna(0).astype(int)

    return df


@st.cache_data(ttl=120)
def load_verified_tab(tab_name: str) -> pd.DataFrame:
    """Load a Verified_ tab for detailed drill-down."""
    sh = get_sheet_client()
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()

    data = ws.get_all_values()
    if len(data) < 2:
        return pd.DataFrame()

    return pd.DataFrame(data[1:], columns=data[0])


def safe_delta(current: int, previous: int) -> tuple:
    """Calculate delta and direction string."""
    diff = current - previous
    if previous == 0:
        pct = "N/A"
    else:
        pct = f"{(diff / previous) * 100:+.1f}%"
    return diff, pct


# ═════════════════════════════════════════════════════════════════
#  SIDEBAR
# ═════════════════════════════════════════════════════════════════

def render_sidebar(df: pd.DataFrame):
    with st.sidebar:
        st.markdown("## 🦅 Eagle3D KPIs")
        st.markdown("---")

        # Refresh
        if st.button("🔄 Refresh All Data", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

        st.markdown("---")

        # Pipeline status
        st.markdown("### ⚙️ Pipeline Status")
        if not df.empty and "Timestamp" in df.columns:
            last_ts = df.iloc[-1]["Timestamp"]
            try:
                last_dt = pd.to_datetime(last_ts)
                hours_ago = (datetime.now() - last_dt).total_seconds() / 3600
                if hours_ago < 2:
                    status_class = "live"
                    status_text = "🟢 Live — just ran"
                elif hours_ago < 26:
                    status_class = "live"
                    status_text = f"🟢 Active — {hours_ago:.0f}h ago"
                elif hours_ago < 50:
                    status_class = "stale"
                    status_text = f"🟡 Stale — {hours_ago:.0f}h ago"
                else:
                    status_class = "offline"
                    status_text = f"🔴 Offline — {hours_ago:.0f}h ago"
                st.markdown(f"{status_text}")
                st.caption(f"Last run: {last_ts}")
            except Exception:
                st.markdown("⚪ Unknown")
                st.caption(f"Raw timestamp: {last_ts}")
        else:
            st.markdown("⚪ No data")

        st.markdown("---")

        # Quick stats
        st.markdown("### 📊 Quick Stats")
        if not df.empty:
            total_days = df["Date"].nunique() if "Date" in df.columns else len(df)
            total_runs = len(df)
            first_date = df["Date"].min().strftime("%d %b %Y") if "Date" in df.columns else "—"

            st.metric("Days Tracked", total_days)
            st.metric("Total Pipeline Runs", total_runs)
            st.caption(f"Since: {first_date}")

        st.markdown("---")

        # Date filter
        st.markdown("### 🗓 Date Filter")
        date_range = st.selectbox(
            "View Range",
            ["All Time", "Last 7 Days", "Last 14 Days", "Last 30 Days",
             "This Month", "Last Month", "Custom Range"],
            index=0
        )

        custom_start = None
        custom_end = None

        if date_range == "Custom Range" and not df.empty and "Date" in df.columns:
            min_date = df["Date"].min().date()
            max_date = df["Date"].max().date()
            custom_start = st.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date)
            custom_end = st.date_input("End Date", value=max_date, min_value=min_date, max_value=max_date)

        st.markdown("---")

        # Info
        st.markdown("### ℹ️ About")
        st.caption(
            "Automated KPI tracking for Eagle3D Streaming. "
            "Data scraped daily from KPI Dashboard + Stripe. "
            "Pipeline runs via GitHub Actions."
        )
        st.caption("v3.0 • Built with Streamlit")

    return date_range, custom_start, custom_end


def filter_df_by_range(df: pd.DataFrame, date_range: str,
                       custom_start=None, custom_end=None) -> pd.DataFrame:
    """Filter dataframe based on sidebar date selection."""
    if df.empty or "Date" not in df.columns:
        return df

    now = datetime.now()

    if date_range == "Last 7 Days":
        cutoff = now - timedelta(days=7)
        return df[df["Date"] >= cutoff].reset_index(drop=True)
    elif date_range == "Last 14 Days":
        cutoff = now - timedelta(days=14)
        return df[df["Date"] >= cutoff].reset_index(drop=True)
    elif date_range == "Last 30 Days":
        cutoff = now - timedelta(days=30)
        return df[df["Date"] >= cutoff].reset_index(drop=True)
    elif date_range == "This Month":
        return df[
            (df["Date"].dt.month == now.month) &
            (df["Date"].dt.year == now.year)
        ].reset_index(drop=True)
    elif date_range == "Last Month":
        last_month = now.replace(day=1) - timedelta(days=1)
        return df[
            (df["Date"].dt.month == last_month.month) &
            (df["Date"].dt.year == last_month.year)
        ].reset_index(drop=True)
    elif date_range == "Custom Range" and custom_start and custom_end:
        return df[
            (df["Date"].dt.date >= custom_start) &
            (df["Date"].dt.date <= custom_end)
        ].reset_index(drop=True)

    return df  # "All Time"


# ═════════════════════════════════════════════════════════════════
#  SECTION: LIVE RIGHT NOW
# ═════════════════════════════════════════════════════════════════

def render_live_now(df: pd.DataFrame):
    """Live Right Now — reads ONLY the latest row."""
    st.markdown("## 📡 Live Right Now")

    latest = df.iloc[-1]

    # Date header
    date_label = "Unknown"
    if "Date" in latest.index and pd.notna(latest["Date"]):
        date_label = pd.Timestamp(latest["Date"]).strftime("%A, %d %B %Y")

    if "Timestamp" in latest.index:
        st.caption(f"🕐 Pipeline last ran: **{latest['Timestamp']}**  •  📅 {date_label}")

    # Calculate deltas vs previous day
    has_previous = len(df) >= 2
    prev = df.iloc[-2] if has_previous else None

    col1, col2, col3 = st.columns(3)

    for col_container, kpi_col in zip([col1, col2, col3], KPI_COLS):
        current_val = int(latest.get(kpi_col, 0))
        icon = KPI_ICONS[kpi_col]
        label = KPI_LABELS[kpi_col]

        if has_previous and prev is not None:
            prev_val = int(prev.get(kpi_col, 0))
            diff, pct = safe_delta(current_val, prev_val)
            delta_str = f"{diff:+d} ({pct})"
        else:
            delta_str = None

        with col_container:
            st.metric(
                label=f"{icon} {label} Today",
                value=f"{current_val:,}",
                delta=delta_str,
                help=f"Accepted {label.lower()} from today's pipeline run. "
                     f"Delta compared to previous day."
            )

    # Conversion funnel
    signups = int(latest.get("SignUps_Accepted", 0))
    uploads = int(latest.get("FirstUploads_Accepted", 0))
    paid = int(latest.get("PaidSubscribers_Accepted", 0))

    if signups > 0:
        st.markdown("---")
        st.markdown("##### 🔀 Today's Conversion Funnel")
        fcol1, fcol2, fcol3, fcol4, fcol5 = st.columns(5)

        with fcol1:
            st.markdown(f"**{signups:,}**")
            st.caption("Sign-Ups")
        with fcol2:
            rate1 = (uploads / signups * 100) if signups > 0 else 0
            st.markdown(f"**→ {rate1:.1f}%**")
            st.caption("Upload Rate")
        with fcol3:
            st.markdown(f"**{uploads:,}**")
            st.caption("First Uploads")
        with fcol4:
            rate2 = (paid / uploads * 100) if uploads > 0 else 0
            st.markdown(f"**→ {rate2:.1f}%**")
            st.caption("Paid Rate")
        with fcol5:
            st.markdown(f"**{paid:,}**")
            st.caption("Paid")


# ═════════════════════════════════════════════════════════════════
#  SECTION: ALL-TIME TOTALS
# ═════════════════════════════════════════════════════════════════

def render_all_time(df_full: pd.DataFrame):
    """Cumulative all-time totals — sum of ALL rows in Daily_Report."""
    st.markdown("## 🏆 All-Time Cumulative Totals")

    total_signups = int(df_full["SignUps_Accepted"].sum())
    total_uploads = int(df_full["FirstUploads_Accepted"].sum())
    total_paid    = int(df_full["PaidSubscribers_Accepted"].sum())

    total_days = df_full["Date"].nunique() if "Date" in df_full.columns else len(df_full)
    first_date = df_full["Date"].min().strftime("%d %b %Y") if "Date" in df_full.columns else "—"
    last_date  = df_full["Date"].max().strftime("%d %b %Y") if "Date" in df_full.columns else "—"

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("✅ Total Sign-Ups", f"{total_signups:,}",
                  help="Sum of all accepted sign-ups across all pipeline runs")
    with col2:
        st.metric("📤 Total First Uploads", f"{total_uploads:,}",
                  help="Sum of all first uploads across all pipeline runs")
    with col3:
        st.metric("💳 Total Paid Subscribers", f"{total_paid:,}",
                  help="Sum of all paid subscriber validations across all pipeline runs")

    # All-time funnel
    st.markdown("---")
    acol1, acol2, acol3 = st.columns(3)

    with acol1:
        if total_signups > 0:
            upload_rate = total_uploads / total_signups * 100
            st.metric("📊 Upload Conversion Rate", f"{upload_rate:.1f}%",
                      help="% of sign-ups who uploaded at least once (all time)")
        else:
            st.metric("📊 Upload Conversion Rate", "N/A")

    with acol2:
        if total_uploads > 0:
            paid_rate = total_paid / total_uploads * 100
            st.metric("💰 Paid Conversion Rate", f"{paid_rate:.1f}%",
                      help="% of uploaders who became paid subscribers (all time)")
        else:
            st.metric("💰 Paid Conversion Rate", "N/A")

    with acol3:
        if total_days > 0:
            avg_signups = total_signups / total_days
            st.metric("📈 Avg Sign-Ups / Day", f"{avg_signups:.1f}",
                      help="Average accepted sign-ups per day across all tracked days")
        else:
            st.metric("📈 Avg Sign-Ups / Day", "N/A")

    st.caption(
        f"📅 Tracking **{total_days}** days  •  "
        f"First: {first_date}  •  Latest: {last_date}"
    )


# ═════════════════════════════════════════════════════════════════
#  SECTION: FILTERED PERIOD STATS
# ═════════════════════════════════════════════════════════════════

def render_period_stats(df_filtered: pd.DataFrame, date_range: str):
    """Stats for the currently selected date range."""
    if df_filtered.empty:
        st.info(f"No data for selected range: {date_range}")
        return

    st.markdown(f"## 📅 Period Stats — {date_range}")

    period_signups = int(df_filtered["SignUps_Accepted"].sum())
    period_uploads = int(df_filtered["FirstUploads_Accepted"].sum())
    period_paid    = int(df_filtered["PaidSubscribers_Accepted"].sum())
    period_days    = df_filtered["Date"].nunique() if "Date" in df_filtered.columns else len(df_filtered)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(f"Sign-Ups ({date_range})", f"{period_signups:,}")
    with col2:
        st.metric(f"First Uploads ({date_range})", f"{period_uploads:,}")
    with col3:
        st.metric(f"Paid ({date_range})", f"{period_paid:,}")
    with col4:
        st.metric("Days in Range", period_days)

    # Daily averages for period
    if period_days > 0:
        dcol1, dcol2, dcol3 = st.columns(3)
        with dcol1:
            st.metric("Avg Sign-Ups/Day", f"{period_signups / period_days:.1f}")
        with dcol2:
            st.metric("Avg Uploads/Day", f"{period_uploads / period_days:.1f}")
        with dcol3:
            st.metric("Avg Paid/Day", f"{period_paid / period_days:.1f}")


# ═════════════════════════════════════════════════════════════════
#  SECTION: CHARTS
# ═════════════════════════════════════════════════════════════════

def render_charts(df_filtered: pd.DataFrame, date_range: str):
    """All chart visualizations."""
    st.markdown(f"## 📈 Trends & Charts — {date_range}")

    if df_filtered.empty or "Date" not in df_filtered.columns:
        st.info("No data to chart.")
        return

    tab_trend, tab_individual, tab_cumulative, tab_funnel = st.tabs([
        "📊 Combined Trend",
        "📉 Individual KPIs",
        "📈 Cumulative Growth",
        "🔀 Conversion Funnel"
    ])

    # ── Combined Trend ────────────────────────────────────────────
    with tab_trend:
        st.markdown("##### Daily KPI Trend — All Metrics")

        chart_df = df_filtered[["Date"] + KPI_COLS].copy().set_index("Date")
        chart_df.columns = [KPI_LABELS[c] for c in KPI_COLS]
        st.line_chart(chart_df, use_container_width=True)

        # Bar chart below
        st.markdown("##### Daily Volume (Bar Chart)")
        st.bar_chart(chart_df, use_container_width=True)

    # ── Individual KPIs ───────────────────────────────────────────
    with tab_individual:
        for kpi_col in KPI_COLS:
            label = KPI_LABELS[kpi_col]
            icon  = KPI_ICONS[kpi_col]
            st.markdown(f"##### {icon} {label}")

            ind_df = df_filtered[["Date", kpi_col]].copy().set_index("Date")
            ind_df.columns = [label]
            st.area_chart(ind_df, use_container_width=True)

            # Stats row
            vals = df_filtered[kpi_col]
            scol1, scol2, scol3, scol4 = st.columns(4)
            with scol1:
                st.caption(f"Min: {int(vals.min()):,}")
            with scol2:
                st.caption(f"Max: {int(vals.max()):,}")
            with scol3:
                st.caption(f"Mean: {vals.mean():.1f}")
            with scol4:
                st.caption(f"Total: {int(vals.sum()):,}")

            st.markdown("---")

    # ── Cumulative Growth ─────────────────────────────────────────
    with tab_cumulative:
        st.markdown("##### Running Total Over Time")
        cum_df = df_filtered[["Date"] + KPI_COLS].copy().set_index("Date")
        cum_df = cum_df.cumsum()
        cum_df.columns = [f"{KPI_LABELS[c]} (Cumulative)" for c in KPI_COLS]
        st.area_chart(cum_df, use_container_width=True)

    # ── Conversion Funnel ─────────────────────────────────────────
    with tab_funnel:
        st.markdown("##### Daily Conversion Rates Over Time")

        funnel_df = df_filtered[["Date"] + KPI_COLS].copy()

        funnel_df["Upload Rate %"] = funnel_df.apply(
            lambda r: (r["FirstUploads_Accepted"] / r["SignUps_Accepted"] * 100)
            if r["SignUps_Accepted"] > 0 else 0, axis=1
        )
        funnel_df["Paid Rate %"] = funnel_df.apply(
            lambda r: (r["PaidSubscribers_Accepted"] / r["FirstUploads_Accepted"] * 100)
            if r["FirstUploads_Accepted"] > 0 else 0, axis=1
        )
        funnel_df["Overall Conv %"] = funnel_df.apply(
            lambda r: (r["PaidSubscribers_Accepted"] / r["SignUps_Accepted"] * 100)
            if r["SignUps_Accepted"] > 0 else 0, axis=1
        )

        rate_df = funnel_df[["Date", "Upload Rate %", "Paid Rate %", "Overall Conv %"]].set_index("Date")
        st.line_chart(rate_df, use_container_width=True)

        # Period average rates
        avg_upload = rate_df["Upload Rate %"].mean()
        avg_paid   = rate_df["Paid Rate %"].mean()
        avg_overall = rate_df["Overall Conv %"].mean()

        rcol1, rcol2, rcol3 = st.columns(3)
        with rcol1:
            st.metric("Avg Upload Rate", f"{avg_upload:.1f}%")
        with rcol2:
            st.metric("Avg Paid Rate", f"{avg_paid:.1f}%")
        with rcol3:
            st.metric("Avg Overall Conv", f"{avg_overall:.1f}%")


# ═════════════════════════════════════════════════════════════════
#  SECTION: DAY-OVER-DAY COMPARISON
# ═════════════════════════════════════════════════════════════════

def render_day_comparison(df: pd.DataFrame):
    """Compare any two days side by side."""
    st.markdown("## 🔄 Day-over-Day Comparison")

    if len(df) < 2 or "Date" not in df.columns:
        st.info("Need at least 2 days of data for comparison.")
        return

    dates = df["Date"].dt.date.unique().tolist()
    dates_sorted = sorted(dates, reverse=True)

    col1, col2 = st.columns(2)
    with col1:
        day_a = st.selectbox("Day A (newer)", dates_sorted, index=0)
    with col2:
        day_b = st.selectbox("Day B (older)", dates_sorted, index=min(1, len(dates_sorted)-1))

    row_a = df[df["Date"].dt.date == day_a].iloc[-1] if not df[df["Date"].dt.date == day_a].empty else None
    row_b = df[df["Date"].dt.date == day_b].iloc[-1] if not df[df["Date"].dt.date == day_b].empty else None

    if row_a is None or row_b is None:
        st.warning("Could not find data for selected dates.")
        return

    mcol1, mcol2, mcol3 = st.columns(3)

    for container, kpi_col in zip([mcol1, mcol2, mcol3], KPI_COLS):
        val_a = int(row_a.get(kpi_col, 0))
        val_b = int(row_b.get(kpi_col, 0))
        diff, pct = safe_delta(val_a, val_b)

        with container:
            st.metric(
                label=f"{KPI_ICONS[kpi_col]} {KPI_LABELS[kpi_col]}",
                value=f"{val_a:,}",
                delta=f"{diff:+d} vs {day_b} ({pct})"
            )


# ═════════════════════════════════════════════════════════════════
#  SECTION: RECORDS & MILESTONES
# ═════════════════════════════════════════════════════════════════

def render_records(df_full: pd.DataFrame):
    """Best days, worst days, milestones."""
    st.markdown("## 🥇 Records & Milestones")

    if df_full.empty:
        st.info("No data yet.")
        return

    tab_best, tab_worst, tab_milestones = st.tabs([
        "🏅 Best Days", "📉 Lowest Days", "🎯 Milestones"
    ])

    with tab_best:
        for kpi_col in KPI_COLS:
            icon  = KPI_ICONS[kpi_col]
            label = KPI_LABELS[kpi_col]
            idx   = df_full[kpi_col].idxmax()
            row   = df_full.loc[idx]
            val   = int(row[kpi_col])
            date  = row["Date"].strftime("%d %b %Y") if "Date" in row.index else "—"
            st.markdown(f"**{icon} Best {label}:** {val:,} on {date}")

    with tab_worst:
        for kpi_col in KPI_COLS:
            icon  = KPI_ICONS[kpi_col]
            label = KPI_LABELS[kpi_col]
            idx   = df_full[kpi_col].idxmin()
            row   = df_full.loc[idx]
            val   = int(row[kpi_col])
            date  = row["Date"].strftime("%d %b %Y") if "Date" in row.index else "—"
            st.markdown(f"**{icon} Lowest {label}:** {val:,} on {date}")

    with tab_milestones:
        total_signups = int(df_full["SignUps_Accepted"].sum())
        total_uploads = int(df_full["FirstUploads_Accepted"].sum())
        total_paid    = int(df_full["PaidSubscribers_Accepted"].sum())

        milestones = [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

        for metric_name, total, icon in [
            ("Sign-Ups", total_signups, "✅"),
            ("First Uploads", total_uploads, "📤"),
            ("Paid Subscribers", total_paid, "💳"),
        ]:
            reached = [m for m in milestones if total >= m]
            upcoming = [m for m in milestones if total < m]
            next_goal = upcoming[0] if upcoming else "∞"
            remaining = (upcoming[0] - total) if upcoming else 0

            reached_str = ", ".join(str(m) for m in reached) if reached else "None yet"
            st.markdown(
                f"**{icon} {metric_name}:** Reached [{reached_str}] • "
                f"Next: **{next_goal:,}** ({remaining:,} to go)"
            )


# ═════════════════════════════════════════════════════════════════
#  SECTION: VERIFIED DATA DRILL-DOWN
# ═════════════════════════════════════════════════════════════════

def render_verified_drilldown():
    """Drill into Verified_ tabs for detail on each record."""
    st.markdown("## 🔍 Verified Data Drill-Down")

    tab_free, tab_upload, tab_stripe = st.tabs([
        "✅ Verified Sign-Ups",
        "📤 Verified First Uploads",
        "💳 Verified Stripe"
    ])

    for tab_container, tab_name, display_name in [
        (tab_free,   "Verified_FREE",         "Sign-Ups"),
        (tab_upload, "Verified_FIRST_UPLOAD",  "First Uploads"),
        (tab_stripe, "Verified_STRIPE",        "Stripe Subscribers"),
    ]:
        with tab_container:
            vdf = load_verified_tab(tab_name)
            if vdf.empty:
                st.info(f"No verified data in {tab_name}")
                continue

            # Summary stats
            total = len(vdf)
            accepted = len(vdf[vdf.get("final_status", pd.Series()) == "ACCEPTED"]) if "final_status" in vdf.columns else "—"
            rejected = len(vdf[vdf.get("final_status", pd.Series()) == "REJECTED"]) if "final_status" in vdf.columns else "—"

            scol1, scol2, scol3 = st.columns(3)
            with scol1:
                st.metric(f"Total {display_name} Processed", total)
            with scol2:
                st.metric("Accepted", accepted)
            with scol3:
                st.metric("Rejected", rejected)

            # Status breakdown
            if "final_status" in vdf.columns:
                status_counts = vdf["final_status"].value_counts()
                st.bar_chart(status_counts)

            # Rejection reasons
            if "verdict_reason" in vdf.columns and "final_status" in vdf.columns:
                rejected_df = vdf[vdf["final_status"] == "REJECTED"]
                if not rejected_df.empty:
                    with st.expander(f"❌ Rejection Reasons ({len(rejected_df)} records)"):
                        reason_counts = rejected_df["verdict_reason"].value_counts()
                        st.dataframe(reason_counts.reset_index().rename(
                            columns={"index": "Reason", "verdict_reason": "Count"}
                        ), use_container_width=True)

            # Quality tier breakdown
            if "ml_quality_tier" in vdf.columns:
                with st.expander("🧠 ML Quality Tier Breakdown"):
                    tier_counts = vdf["ml_quality_tier"].value_counts()
                    st.bar_chart(tier_counts)

            # Full data table
            with st.expander(f"📋 Full {display_name} Data ({total} rows)"):
                # Select key columns to display
                display_cols = [c for c in [
                    "Email", "email", "normalized_email", "final_status",
                    "email_verdict", "deduplication_status", "ml_quality_tier",
                    "legitimacy_score", "row_date_used", "processed_at"
                ] if c in vdf.columns]

                if display_cols:
                    st.dataframe(vdf[display_cols], use_container_width=True)
                else:
                    st.dataframe(vdf, use_container_width=True)


# ═════════════════════════════════════════════════════════════════
#  SECTION: RAW DATA TABLE
# ═════════════════════════════════════════════════════════════════

def render_raw_table(df_full: pd.DataFrame, df_filtered: pd.DataFrame, date_range: str):
    """Raw Daily_Report data table with download."""
    st.markdown("## 🗂 Raw Data")

    tab_filtered, tab_all = st.tabs([
        f"📅 Filtered ({date_range})",
        "📋 All Data"
    ])

    with tab_filtered:
        if df_filtered.empty:
            st.info("No data for selected range.")
        else:
            st.dataframe(df_filtered, use_container_width=True)
            csv = df_filtered.to_csv(index=False)
            st.download_button(
                label="⬇️ Download Filtered CSV",
                data=csv,
                file_name=f"eagle3d_kpi_{date_range.lower().replace(' ', '_')}.csv",
                mime="text/csv"
            )

    with tab_all:
        if df_full.empty:
            st.info("No data.")
        else:
            st.dataframe(df_full, use_container_width=True)
            csv = df_full.to_csv(index=False)
            st.download_button(
                label="⬇️ Download All Data CSV",
                data=csv,
                file_name="eagle3d_kpi_all_time.csv",
                mime="text/csv"
            )


# ═════════════════════════════════════════════════════════════════
#  SECTION: FOOTER
# ═════════════════════════════════════════════════════════════════

def render_footer():
    st.markdown(
        '<div class="footer-text">'
        '🦅 Eagle3D KPI Dashboard v3.0 • Automated by GitHub Actions • '
        'Data: KPI Dashboard + Stripe • Built with Streamlit<br>'
        f'Dashboard loaded: {datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}'
        '</div>',
        unsafe_allow_html=True
    )


# ═════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════

def main():
    st.title("🦅 Eagle3D KPI Dashboard")

    # Load data
    df_full = load_daily_report()

    if df_full.empty:
        st.warning(
            "⚠️ No data in Daily_Report yet. "
            "Run the pipeline at least once to populate the sheet."
        )
        st.markdown("**To populate data:**")
        st.code("python daily_pipeline.py", language="bash")
        render_footer()
        return

    # Sidebar — returns filter choices
    date_range, custom_start, custom_end = render_sidebar(df_full)

    # Filter
    df_filtered = filter_df_by_range(df_full, date_range, custom_start, custom_end)

    # ── All sections ──────────────────────────────────────────────
    render_live_now(df_full)                          # Latest row only
    st.divider()
    render_all_time(df_full)                          # Sum of all rows
    st.divider()
    if date_range != "All Time":
        render_period_stats(df_filtered, date_range)  # Filtered period stats
        st.divider()
    render_charts(df_filtered, date_range)            # All charts
    st.divider()
    render_day_comparison(df_full)                    # Day vs Day
    st.divider()
    render_records(df_full)                           # Best/worst/milestones
    st.divider()
    render_verified_drilldown()                       # Verified tabs detail
    st.divider()
    render_raw_table(df_full, df_filtered, date_range)  # Raw data + download
    render_footer()                                   # Footer


if __name__ == "__main__":
    main()
