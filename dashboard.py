# dashboard.py — Eagle3D KPI Dashboard
# FIX: Live Right Now reads latest row only, not sum of all rows
# FIX2: st.secrets["GOOGLE_CREDS"] is already a dict — no json.loads() needed

import streamlit as st
import gspread
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
import json

st.set_page_config(
    page_title="Eagle3D KPI Dashboard",
    page_icon="🦅",
    layout="wide"
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

MASTER_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1E5PI3-m7mTMKRQ4Cy-WqpVCo5dQjbICcA2EnrC9ORE4/edit"
)


@st.cache_resource(ttl=300)
def get_sheet_client():
    """
    Streamlit Cloud: st.secrets["GOOGLE_CREDS"] is already parsed
    as a dict by Streamlit — json.loads() must NOT be called on it.

    Two supported formats in secrets.toml:
      Option A — nested TOML (recommended):
        [GOOGLE_CREDS]
        type = "service_account"
        project_id = "..."
        private_key_id = "..."
        private_key = "..."
        client_email = "..."
        ...

      Option B — raw JSON string:
        GOOGLE_CREDS = '{"type": "service_account", ...}'

    This function handles both automatically.
    Local dev: falls back to google_creds.json file on disk.
    """
    if "GOOGLE_CREDS" in st.secrets:
        raw = st.secrets["GOOGLE_CREDS"]

        # Streamlit parsed TOML → already a dict-like object
        if hasattr(raw, "to_dict"):
            creds_dict = raw.to_dict()
        elif isinstance(raw, dict):
            creds_dict = dict(raw)
        elif isinstance(raw, str):
            # stored as a raw JSON string in secrets
            creds_dict = json.loads(raw)
        else:
            st.error(
                f"GOOGLE_CREDS secret has unexpected type: {type(raw)}. "
                f"Expected dict or JSON string."
            )
            st.stop()

        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    else:
        # Local development — use file on disk
        creds = Credentials.from_service_account_file(
            "google_creds.json", scopes=SCOPES
        )

    gc = gspread.authorize(creds)
    return gc.open_by_url(MASTER_SHEET_URL)


@st.cache_data(ttl=300)
def load_daily_report() -> pd.DataFrame:
    """Load full Daily_Report tab as a clean sorted DataFrame."""
    sh = get_sheet_client()

    try:
        ws = sh.worksheet("Daily_Report")
    except gspread.WorksheetNotFound:
        st.error("Daily_Report tab not found in Google Sheet.")
        return pd.DataFrame()

    data = ws.get_all_values()

    if len(data) < 2:
        return pd.DataFrame()

    headers = data[0]
    rows    = data[1:]
    df      = pd.DataFrame(rows, columns=headers)

    for col in ["SignUps_Accepted", "FirstUploads_Accepted", "PaidSubscribers_Accepted"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.sort_values("Date").reset_index(drop=True)

    return df


def get_latest_row(df: pd.DataFrame):
    """
    Returns the single most recent row from Daily_Report.
    FIX: dashboard previously summed ALL rows — inflated cumulative
    number shown as today's live figure.
    Now returns df.iloc[-1] — last row = most recent pipeline run.
    """
    if df.empty:
        return None
    return df.iloc[-1]


def render_live_now(latest: pd.Series):
    """Live Right Now section — latest row only."""
    st.header("📡 Live Right Now")

    date_label = "Unknown"
    if "Date" in latest.index and pd.notna(latest["Date"]):
        date_label = pd.Timestamp(latest["Date"]).strftime("%A, %d %b %Y")

    if "Timestamp" in latest.index:
        st.caption(
            f"Pipeline last ran: {latest['Timestamp']}  |  Date: {date_label}"
        )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="✅ Sign-Ups Accepted Today",
            value=int(latest.get("SignUps_Accepted", 0)),
            help="New valid sign-ups accepted today by the pipeline"
        )
    with col2:
        st.metric(
            label="📤 First Uploads Accepted Today",
            value=int(latest.get("FirstUploads_Accepted", 0)),
            help="Users who completed their first upload today"
        )
    with col3:
        st.metric(
            label="💳 Paid Subscribers Today",
            value=int(latest.get("PaidSubscribers_Accepted", 0)),
            help="Active paid subscribers validated today from Stripe"
        )


def render_summary_all_time(df: pd.DataFrame):
    """
    All-time cumulative totals across every row in Daily_Report.
    Clearly labelled as ALL-TIME so never confused with today's numbers.
    """
    st.header("🏆 All-Time Totals")

    if df.empty:
        st.info("No data yet.")
        return

    total_signups = int(df["SignUps_Accepted"].sum())
    total_uploads = int(df["FirstUploads_Accepted"].sum())
    total_paid    = int(df["PaidSubscribers_Accepted"].sum())
    total_days    = df["Date"].nunique() if "Date" in df.columns else len(df)
    first_date    = df["Date"].min().strftime("%d %b %Y") if "Date" in df.columns else "—"
    last_date     = df["Date"].max().strftime("%d %b %Y") if "Date" in df.columns else "—"

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Sign-Ups (all time)",        total_signups)
    with col2:
        st.metric("Total First Uploads (all time)",   total_uploads)
    with col3:
        st.metric("Total Paid Subscribers (all time)", total_paid)

    st.caption(
        f"Tracking {total_days} days of data  |  "
        f"First record: {first_date}  |  Latest: {last_date}"
    )


def render_trend_chart(df: pd.DataFrame):
    """Day-by-day trend chart — uses full history."""
    st.header("📈 Day-by-Day Trend")

    if df.empty or "Date" not in df.columns:
        st.info("No historical data yet.")
        return

    chart_df = df[
        ["Date", "SignUps_Accepted",
         "FirstUploads_Accepted", "PaidSubscribers_Accepted"]
    ].copy().set_index("Date")

    st.line_chart(chart_df)


def render_data_table(df: pd.DataFrame):
    """Full raw table for debugging."""
    with st.expander("🗂 Full Daily_Report Data (all rows)"):
        if df.empty:
            st.info("No data.")
        else:
            st.dataframe(df, use_container_width=True)


def main():
    st.title("🦅 Eagle3D KPI Dashboard")

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    df = load_daily_report()

    if df.empty:
        st.warning(
            "No data in Daily_Report yet. "
            "Run the pipeline at least once to populate the sheet."
        )
        return

    latest = get_latest_row(df)

    if latest is None:
        st.warning("Could not read latest row.")
        return

    render_live_now(latest)         # today only — latest row
    st.divider()
    render_summary_all_time(df)     # all-time totals — sum of all rows
    st.divider()
    render_trend_chart(df)          # full history line chart
    st.divider()
    render_data_table(df)           # raw table


if __name__ == "__main__":
    main()
