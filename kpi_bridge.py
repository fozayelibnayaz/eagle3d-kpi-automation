"""
KPI Bridge v3 - Direct connection to Eagle3D Daily_Counts tab
Reads pre-aggregated daily KPIs from your existing Master Sheet
"""

import pandas as pd
import os
from datetime import datetime


SHEET_TAB_DAILY        = "Daily_Counts"
SHEET_TAB_MONTHLY      = "Monthly_Counts"
SHEET_TAB_SIGNUPS_RAW  = "Verified_FREE"
SHEET_TAB_UPLOADS_RAW  = "Verified_FIRST_UPLOAD"
SHEET_TAB_PAID_RAW     = "Verified_STRIPE"


def _get_sheet_client():
    """Build authenticated Google Sheets client from secrets or local file."""
    try:
        # import gspread  # disabled - using Supabase
        from google.oauth2 import service_account

        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]

        # Try Streamlit secrets first (Cloud) — check GOOGLE_CREDS
        try:
            import streamlit as st
            if "GOOGLE_CREDS" in st.secrets:
                d = dict(st.secrets["GOOGLE_CREDS"])
                if "private_key" in d:
                    d["private_key"] = d["private_key"].replace("\\n", "\n")
                creds = service_account.Credentials.from_service_account_info(d, scopes=SCOPES)
                return gspread.authorize(creds)
        except Exception:
            pass

        # Also try ga4_service_account (same SA can access both GA4 and Sheets)
        try:
            import streamlit as st
            if "ga4_service_account" in st.secrets:
                d = dict(st.secrets["ga4_service_account"])
                if "private_key" in d:
                    d["private_key"] = d["private_key"].replace("\\n", "\n")
                creds = service_account.Credentials.from_service_account_info(d, scopes=SCOPES)
                return gspread.authorize(creds)
        except Exception:
            pass

        # Local file fallback
        if os.path.exists("google_creds.json"):
            creds = service_account.Credentials.from_service_account_file(
                "google_creds.json", scopes=SCOPES
            )
            return gspread.authorize(creds)
        return None
    except Exception as e:
        print(f"Sheet client error: {e}")
        return None


def _get_master_sheet_url():
    """Get Master Sheet URL from secrets or config."""
    try:
        import streamlit as st
        url = st.secrets.get("MASTER_SHEET_URL", "")
        if url:
            return url
    except Exception:
        pass

    try:
        import config
        return getattr(config, "MASTER_SHEET_URL", "")
    except Exception:
        return ""


def _open_sheet():
    """Open the Master Sheet."""
    client = _get_sheet_client()
    if not client:
        return None
    url = _get_master_sheet_url()
    if not url:
        return None
    try:
        return client.open_by_url(url)
    except Exception as e:
        print(f"Sheet open error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
#  MAIN: Fetch Daily KPIs from Daily_Counts tab
# ─────────────────────────────────────────────────────────────────

def fetch_daily_kpis(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Read Daily_Counts tab from Master Sheet.
    Returns DataFrame: date, signups, first_uploads, paid_customers
    """
    sheet = _open_sheet()
    if not sheet:
        return _empty_kpi_df()

    try:
        ws = sheet.worksheet(SHEET_TAB_DAILY)
        records = ws.get_all_records()
    except Exception as e:
        print(f"Could not read {SHEET_TAB_DAILY}: {e}")
        return _empty_kpi_df()

    if not records:
        return _empty_kpi_df()

    rows = []
    for r in records:
        date_val = r.get("Date", "")
        if not date_val:
            continue

        try:
            # Daily_Counts uses YYYY-MM-DD format
            dt = pd.to_datetime(str(date_val))
            date_str = dt.strftime("%Y-%m-%d")
        except Exception:
            continue

        rows.append({
            "date":            date_str,
            "signups":         _safe_int(r.get("SignUps_Accepted", 0)),
            "first_uploads":   _safe_int(r.get("FirstUploads_Accepted", 0)),
            "paid_customers":  _safe_int(r.get("PaidSubscribers_Accepted", 0)),
            "signup_details":  r.get("SignUp_Details", ""),
            "upload_details":  r.get("Upload_Details", ""),
            "paid_details":    r.get("Paid_Details", ""),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Date filter
    if start_date:
        df = df[df["date"] >= start_date]
    if end_date:
        df = df[df["date"] <= end_date]

    return df.sort_values("date", ascending=False).reset_index(drop=True)


def _safe_int(val) -> int:
    try:
        if pd.isna(val) or val == "":
            return 0
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def _empty_kpi_df():
    return pd.DataFrame(columns=[
        "date", "signups", "first_uploads", "paid_customers",
        "signup_details", "upload_details", "paid_details",
    ])


# ─────────────────────────────────────────────────────────────────
#  Diagnostic — Inspect sheet structure
# ─────────────────────────────────────────────────────────────────

def diagnose_sheet() -> dict:
    """Inspect Master Sheet and return structure info."""
    info = {"connected": False, "tabs": [], "error": None}

    sheet = _open_sheet()
    if not sheet:
        info["error"] = "Cannot connect to sheet. Check MASTER_SHEET_URL + credentials."
        return info

    info["connected"] = True
    info["title"] = sheet.title

    for ws in sheet.worksheets():
        try:
            headers = ws.row_values(1)
            info["tabs"].append({
                "name":    ws.title,
                "rows":    ws.row_count,
                "cols":    ws.col_count,
                "headers": headers[:15],
            })
        except Exception as e:
            info["tabs"].append({"name": ws.title, "error": str(e)})

    return info


# ─────────────────────────────────────────────────────────────────
#  Raw Data Access — for source attribution
# ─────────────────────────────────────────────────────────────────

def fetch_signup_details(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Read individual signup records from Verified_FREE.
    Has: Account Created On, Email, Lead Source, Phone, Username
    """
    sheet = _open_sheet()
    if not sheet:
        return pd.DataFrame()

    try:
        ws = sheet.worksheet(SHEET_TAB_SIGNUPS_RAW)
        records = ws.get_all_records()
    except Exception:
        return pd.DataFrame()

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Normalize date column
    if "Account Created On" in df.columns:
        df["date"] = pd.to_datetime(df["Account Created On"], errors="coerce").dt.strftime("%Y-%m-%d")

    if start_date and "date" in df.columns:
        df = df[df["date"] >= start_date]
    if end_date and "date" in df.columns:
        df = df[df["date"] <= end_date]

    return df.reset_index(drop=True)


def fetch_upload_details(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Read individual upload records."""
    sheet = _open_sheet()
    if not sheet:
        return pd.DataFrame()

    try:
        ws = sheet.worksheet(SHEET_TAB_UPLOADS_RAW)
        records = ws.get_all_records()
    except Exception:
        return pd.DataFrame()

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    if "Upload Date" in df.columns:
        df["date"] = pd.to_datetime(df["Upload Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    if start_date and "date" in df.columns:
        df = df[df["date"] >= start_date]
    if end_date and "date" in df.columns:
        df = df[df["date"] <= end_date]

    return df.reset_index(drop=True)


def fetch_paid_details(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Read individual paid customer records from Verified_STRIPE."""
    sheet = _open_sheet()
    if not sheet:
        return pd.DataFrame()

    try:
        ws = sheet.worksheet(SHEET_TAB_PAID_RAW)
        records = ws.get_all_records()
    except Exception:
        return pd.DataFrame()

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    if "First payment" in df.columns:
        df["date"] = pd.to_datetime(df["First payment"], errors="coerce").dt.strftime("%Y-%m-%d")
    elif "row_date_used" in df.columns:
        df["date"] = pd.to_datetime(df["row_date_used"], errors="coerce").dt.strftime("%Y-%m-%d")
    elif "Created" in df.columns:
        df["date"] = pd.to_datetime(df["Created"], errors="coerce").dt.strftime("%Y-%m-%d")

    if start_date and "date" in df.columns:
        df = df[df["date"] >= start_date]
    if end_date and "date" in df.columns:
        df = df[df["date"] <= end_date]

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────
#  Source Attribution — links sign-ups to traffic sources
# ─────────────────────────────────────────────────────────────────

def attribute_signups_by_lead_source(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Returns sign-ups grouped by 'Lead Source' (the field they filled at signup).
    Shows: Source | Count | Percentage
    
    Uses intelligent source normalization to deduplicate:
      Google/Google Search/google → "Google"
      LinkedIn/linkedin/Linkedin → "LinkedIn"
      etc.
    """
    signups = fetch_signup_details(start_date, end_date)
    if signups.empty or "Lead Source" not in signups.columns:
        return pd.DataFrame()

    # Use intelligent source normalization
    try:
        from source_normalizer import aggregate_normalized_sources
        grouped = aggregate_normalized_sources(signups, "Lead Source")
        if grouped is not None and not grouped.empty:
            return grouped
    except ImportError:
        pass

    # Fallback to basic grouping
    signups["Lead Source"] = signups["Lead Source"].fillna("(Not Specified)").replace("", "(Not Specified)")

    grouped = (
        signups.groupby("Lead Source").size()
        .reset_index(name="Signups")
        .sort_values("Signups", ascending=False)
    )
    total = grouped["Signups"].sum()
    grouped["% of Total"] = (grouped["Signups"] / total * 100).round(2) if total > 0 else 0

    return grouped


def attribute_signups_to_ga4_sources(kpi_df, utm_df) -> pd.DataFrame:
    """
    Distributes daily sign-ups proportionally by GA4 source share.
    Example: 10 signups on May 15 + 50% Google traffic = ~5 signups attributed to Google
    """
    if kpi_df.empty or utm_df.empty:
        return pd.DataFrame()

    utm = utm_df.copy()
    utm["date_norm"] = pd.to_datetime(utm["date"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")

    kpi = kpi_df.copy()
    kpi["date_norm"] = pd.to_datetime(kpi["date"]).dt.strftime("%Y-%m-%d")

    result = []
    for date in kpi["date_norm"].unique():
        kpi_row = kpi[kpi["date_norm"] == date].iloc[0]
        utm_day = utm[utm["date_norm"] == date]

        if utm_day.empty:
            continue

        total_sess = float(utm_day["sessions"].sum())
        if total_sess == 0:
            continue

        for _, src_row in utm_day.iterrows():
            src_share = float(src_row["sessions"]) / total_sess
            result.append({
                "date":              date,
                "source":            src_row["sessionSource"],
                "medium":            src_row["sessionMedium"],
                "sessions":          int(src_row["sessions"]),
                "signups_est":       round(kpi_row["signups"] * src_share, 1),
                "uploads_est":       round(kpi_row["first_uploads"] * src_share, 1),
                "paid_est":          round(kpi_row["paid_customers"] * src_share, 1),
            })

    df = pd.DataFrame(result)
    if not df.empty:
        df["est_signup_rate_%"] = (df["signups_est"] / df["sessions"].replace(0,1) * 100).round(2)

    return df


def get_source_attribution_summary(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Aggregate Lead Source from CRM with totals.
    The Lead Source field is what users typed when signing up.
    """
    signups = fetch_signup_details(start_date, end_date)
    if signups.empty:
        return pd.DataFrame()

    if "Lead Source" not in signups.columns:
        return pd.DataFrame()

    # Count by source
    by_source = (
        signups.groupby("Lead Source").size().reset_index(name="signup_count")
        .sort_values("signup_count", ascending=False)
    )

    total = by_source["signup_count"].sum()
    by_source["share_%"] = (by_source["signup_count"] / total * 100).round(2) if total > 0 else 0

    return by_source


# ─────────────────────────────────────────────────────────────────
#  Funnel & Merging
# ─────────────────────────────────────────────────────────────────

def merge_ga4_with_kpis(ga4_df: pd.DataFrame, kpi_df: pd.DataFrame) -> pd.DataFrame:
    """JOIN GA4 traffic data with internal KPI data by date."""
    if ga4_df.empty:
        return kpi_df.copy() if not kpi_df.empty else pd.DataFrame()
    if kpi_df.empty:
        return ga4_df.copy()

    ga4_copy = ga4_df.copy()
    if "date" in ga4_copy.columns:
        ga4_copy["date_normalized"] = pd.to_datetime(
            ga4_copy["date"], format="%Y%m%d", errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    else:
        return ga4_copy

    kpi_copy = kpi_df.copy()
    kpi_copy["date_normalized"] = pd.to_datetime(kpi_copy["date"]).dt.strftime("%Y-%m-%d")

    merged = ga4_copy.merge(
        kpi_copy[["date_normalized","signups","first_uploads","paid_customers"]],
        on="date_normalized", how="left",
    )

    for col in ["signups","first_uploads","paid_customers"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0).astype(int)

    return merged


def calculate_funnel_metrics(kpi_df: pd.DataFrame) -> dict:
    """Calculate funnel conversion rates."""
    if kpi_df.empty:
        return {"signups": 0, "uploads": 0, "paid": 0,
                "signup_to_upload": 0, "upload_to_paid": 0, "signup_to_paid": 0}

    totals = {
        "signups": int(kpi_df["signups"].sum()),
        "uploads": int(kpi_df["first_uploads"].sum()),
        "paid":    int(kpi_df["paid_customers"].sum()),
    }
    totals["signup_to_upload"] = round((totals["uploads"]/totals["signups"]*100), 2) if totals["signups"] > 0 else 0
    totals["upload_to_paid"]   = round((totals["paid"]/totals["uploads"]*100), 2)   if totals["uploads"] > 0 else 0
    totals["signup_to_paid"]   = round((totals["paid"]/totals["signups"]*100), 2)   if totals["signups"] > 0 else 0

    return totals

