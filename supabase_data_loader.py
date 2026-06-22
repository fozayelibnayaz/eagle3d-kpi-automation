#!/usr/bin/env python3
# SUPABASE DATA LOADER
# Replaces load_sheet() in app.py
# Primary: Supabase PostgreSQL
# Fallback: Google Sheets then local JSON

import json
import os
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

DATA_DIR = Path("data_output")


def log(m):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Loader] {m}", flush=True)


def _get_supabase():
    url = ""
    key = ""
    try:
        import streamlit as st
        url = str(st.secrets.get("SUPABASE_URL", "")).strip()
        key = str(st.secrets.get("SUPABASE_SERVICE_KEY", "")).strip()
    except Exception:
        pass
    if not url:
        url = os.environ.get("SUPABASE_URL", "").strip()
    if not key:
        key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        return None
    try:
        from supabase import create_client
        return create_client(url, key)
    except Exception as e:
        log(f"Supabase client error: {e}")
        return None


def _apply_overrides(df, sb=None):
    overrides = {}
    if sb:
        try:
            resp = sb.table("manual_overrides").select("*").eq("is_active", True).execute()
            if resp.data:
                for ov in resp.data:
                    overrides[ov["email_normalized"]] = ov
        except Exception:
            pass
    if not overrides:
        try:
            from override_engine import load_overrides
            overrides = load_overrides()
        except Exception:
            pass
    if not overrides or df.empty:
        return df
    email_col = None
    for c in ("__email_normalized__", "Email", "email"):
        if c in df.columns:
            email_col = c
            break
    if not email_col:
        return df
    status_map = {"accept": "ACCEPTED", "reject": "REJECTED", "pending": "PENDING"}
    applied = 0
    for idx, row in df.iterrows():
        email = str(row.get(email_col, "")).strip().lower()
        if email in overrides:
            ov = overrides[email]
            new_status = status_map.get(ov.get("action", ""), "")
            if new_status:
                df.at[idx, "final_status"] = new_status
                df.at[idx, "__override_applied__"] = True
                applied += 1
    if applied:
        log(f"Applied {applied} overrides")
    return df


def load_daily_counts():
    sb = _get_supabase()
    if sb:
        try:
            resp = sb.table("daily_kpis").select("*").order("date").execute()
            if resp.data:
                df = pd.DataFrame(resp.data)
                rename = {
                    "date": "Date", "year": "Year", "month": "Month",
                    "signups_accepted": "SignUps_Accepted",
                    "uploads_accepted": "FirstUploads_Accepted",
                    "paid_accepted": "PaidSubscribers_Accepted",
                    "signup_details": "SignUp_Details",
                    "upload_details": "Upload_Details",
                    "paid_details": "Paid_Details",
                    "last_updated": "LastUpdated",
                }
                df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
                log(f"Supabase daily_kpis: {len(df)} rows")
                return df
        except Exception as e:
            log(f"Supabase daily_kpis error: {e}")
    cache = DATA_DIR / "daily_counts.json"
    if cache.exists():
        try:
            data = json.loads(cache.read_text())
            log(f"Fallback daily_counts.json: {len(data)} rows")
            return pd.DataFrame(data)
        except Exception as e:
            log(f"daily_counts.json error: {e}")
    return pd.DataFrame()


def load_signups():
    sb = _get_supabase()
    if sb:
        try:
            resp = sb.table("signups").select("*").execute()
            if resp.data:
                df = pd.DataFrame(resp.data)
                rename = {
                    "email": "Email",
                    "email_normalized": "__email_normalized__",
                    "signup_date": "Account Created On",
                    "lead_source": "Lead Source",
                    "final_status": "final_status",
                    "category": "category",
                    "rejection_reason": "__rejection_reason__",
                    "verify_score": "__verify_score__",
                    "scraped_date": "__scraped_date__",
                    "override_status": "override_status",
                    "is_overridden": "__is_overridden__",
                }
                df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
                df = _apply_overrides(df, sb)
                log(f"Supabase signups: {len(df)} rows")
                return df
        except Exception as e:
            log(f"Supabase signups error: {e}")
    try:
        from sheets_writer import read_tab_data
        rows = read_tab_data("Verified_FREE")
        if rows:
            log(f"Fallback Sheets Verified_FREE: {len(rows)} rows")
            return pd.DataFrame(rows)
    except Exception as e:
        log(f"Sheets Verified_FREE error: {e}")
    return pd.DataFrame()


def load_uploads():
    sb = _get_supabase()
    if sb:
        try:
            resp = sb.table("uploads").select("*").execute()
            if resp.data:
                df = pd.DataFrame(resp.data)
                rename = {
                    "email": "Email",
                    "email_normalized": "__email_normalized__",
                    "upload_date": "Upload Date",
                    "final_status": "final_status",
                    "category": "category",
                    "rejection_reason": "__rejection_reason__",
                    "scraped_date": "__scraped_date__",
                    "override_status": "override_status",
                    "is_overridden": "__is_overridden__",
                }
                df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
                df = _apply_overrides(df, sb)
                log(f"Supabase uploads: {len(df)} rows")
                return df
        except Exception as e:
            log(f"Supabase uploads error: {e}")
    try:
        from sheets_writer import read_tab_data
        rows = read_tab_data("Verified_FIRST_UPLOAD")
        if rows:
            log(f"Fallback Sheets Verified_FIRST_UPLOAD: {len(rows)} rows")
            return pd.DataFrame(rows)
    except Exception as e:
        log(f"Sheets error: {e}")
    return pd.DataFrame()


def load_payments():
    sb = _get_supabase()
    if sb:
        try:
            resp = sb.table("payments").select("*").execute()
            if resp.data:
                df = pd.DataFrame(resp.data)
                rename = {
                    "email": "Email",
                    "email_normalized": "__email_normalized__",
                    "first_payment_date": "First payment",
                    "total_spend": "Amount",
                    "payment_count": "Payment Count",
                    "final_status": "final_status",
                    "category": "category",
                    "stripe_customer_id": "stripe_customer_id",
                    "override_status": "override_status",
                    "is_overridden": "__is_overridden__",
                }
                df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
                if "row_date_used" not in df.columns and "First payment" in df.columns:
                    df["row_date_used"] = df["First payment"]
                df = _apply_overrides(df, sb)
                log(f"Supabase payments: {len(df)} rows")
                return df
        except Exception as e:
            log(f"Supabase payments error: {e}")
    try:
        from sheets_writer import read_tab_data
        rows = read_tab_data("Verified_STRIPE")
        if rows:
            log(f"Fallback Sheets Verified_STRIPE: {len(rows)} rows")
            return pd.DataFrame(rows)
    except Exception as e:
        log(f"Sheets error: {e}")
    return pd.DataFrame()


def load_tab(tab_name):
    tab_map = {
        "Daily_Counts": load_daily_counts,
        "Verified_FREE": load_signups,
        "Verified_FIRST_UPLOAD": load_uploads,
        "Verified_STRIPE": load_payments,
    }
    loader = tab_map.get(tab_name)
    if loader:
        return loader()
    try:
        from sheets_writer import read_tab_data
        rows = read_tab_data(tab_name)
        if rows:
            return pd.DataFrame(rows)
    except Exception:
        pass
    return pd.DataFrame()


def sync_sheets_to_supabase():
    sb = _get_supabase()
    if not sb:
        print("Supabase not configured")
        return False
    try:
        from sheets_writer import read_tab_data
    except ImportError:
        print("sheets_writer not available")
        return False

    log("Starting Sheets -> Supabase sync...")

    # Daily_Counts -> daily_kpis
    try:
        rows = read_tab_data("Daily_Counts")
        if rows:
            upsert_rows = []
            for r in rows:
                date_val = r.get("Date", "")
                if not date_val:
                    continue
                upsert_rows.append({
                    "date": str(date_val)[:10],
                    "year": str(r.get("Year", str(date_val)[:4])),
                    "month": str(r.get("Month", str(date_val)[:7])),
                    "signups_accepted": int(r.get("SignUps_Accepted", 0) or 0),
                    "uploads_accepted": int(r.get("FirstUploads_Accepted", 0) or 0),
                    "paid_accepted": int(r.get("PaidSubscribers_Accepted", 0) or 0),
                    "signup_details": str(r.get("SignUp_Details", "") or ""),
                    "upload_details": str(r.get("Upload_Details", "") or ""),
                    "paid_details": str(r.get("Paid_Details", "") or ""),
                    "last_updated": str(r.get("LastUpdated", datetime.utcnow().isoformat())),
                })
            for i in range(0, len(upsert_rows), 100):
                sb.table("daily_kpis").upsert(upsert_rows[i:i+100], on_conflict="date").execute()
            log(f"Synced {len(upsert_rows)} daily_kpis rows")
    except Exception as e:
        log(f"daily_kpis sync error: {e}")

    # Verified_FREE -> signups
    try:
        rows = read_tab_data("Verified_FREE")
        if rows:
            upsert_rows = []
            for r in rows:
                email = str(r.get("Email", r.get("email", "")) or "").strip().lower()
                if not email or "@" not in email:
                    continue
                date_val = None
                for df in ("Account Created On", "row_date_used", "__scraped_date__"):
                    v = str(r.get(df, "") or "").strip()
                    if v and v not in ("nan", "None", ""):
                        date_val = v[:10]
                        break
                upsert_rows.append({
                    "email": email,
                    "email_normalized": email,
                    "signup_date": date_val,
                    "lead_source": str(r.get("Lead Source", "") or ""),
                    "final_status": str(r.get("final_status", "PENDING") or "PENDING").upper(),
                    "category": str(r.get("category", "") or ""),
                    "rejection_reason": str(r.get("__rejection_reason__", "") or ""),
                    "scraped_date": date_val,
                })
            for i in range(0, len(upsert_rows), 100):
                sb.table("signups").upsert(upsert_rows[i:i+100], on_conflict="email_normalized").execute()
            log(f"Synced {len(upsert_rows)} signups rows")
    except Exception as e:
        log(f"signups sync error: {e}")

    # Verified_FIRST_UPLOAD -> uploads
    try:
        rows = read_tab_data("Verified_FIRST_UPLOAD")
        if rows:
            upsert_rows = []
            for r in rows:
                email = str(r.get("Email", r.get("email", "")) or "").strip().lower()
                if not email or "@" not in email:
                    continue
                date_val = None
                for df in ("Upload Date", "row_date_used", "__scraped_date__"):
                    v = str(r.get(df, "") or "").strip()
                    if v and v not in ("nan", "None", ""):
                        date_val = v[:10]
                        break
                upsert_rows.append({
                    "email": email,
                    "email_normalized": email,
                    "upload_date": date_val,
                    "final_status": str(r.get("final_status", "PENDING") or "PENDING").upper(),
                    "category": str(r.get("category", "") or ""),
                    "rejection_reason": str(r.get("__rejection_reason__", "") or ""),
                    "scraped_date": date_val,
                })
            for i in range(0, len(upsert_rows), 100):
                sb.table("uploads").upsert(upsert_rows[i:i+100], on_conflict="email_normalized").execute()
            log(f"Synced {len(upsert_rows)} uploads rows")
    except Exception as e:
        log(f"uploads sync error: {e}")

    # Verified_STRIPE -> payments
    try:
        rows = read_tab_data("Verified_STRIPE")
        if rows:
            upsert_rows = []
            for r in rows:
                email = str(r.get("Email", r.get("email", "")) or "").strip().lower()
                if not email or "@" not in email:
                    continue
                date_val = None
                for df in ("First payment", "row_date_used", "Created"):
                    v = str(r.get(df, "") or "").strip()
                    if v and v not in ("nan", "None", ""):
                        date_val = v[:10]
                        break
                amt = 0.0
                for af in ("Amount", "Total spend", "Total Spend", "__amount__"):
                    v = str(r.get(af, "") or "").strip()
                    v = re.sub(r"[$,EUR\s\n\r]", "", v)
                    v = re.sub(r"[A-Za-z]+$", "", v).strip()
                    if v:
                        try:
                            amt = float(v)
                            if amt > 0:
                                break
                        except Exception:
                            pass
                upsert_rows.append({
                    "email": email,
                    "email_normalized": email,
                    "first_payment_date": date_val,
                    "total_spend": amt,
                    "payment_count": int(r.get("Payment Count", r.get("payment_count", 0)) or 0),
                    "final_status": str(r.get("final_status", "PENDING") or "PENDING").upper(),
                    "category": str(r.get("category", "") or ""),
                    "stripe_customer_id": str(r.get("stripe_customer_id", "") or ""),
                })
            for i in range(0, len(upsert_rows), 100):
                sb.table("payments").upsert(upsert_rows[i:i+100], on_conflict="email_normalized").execute()
            log(f"Synced {len(upsert_rows)} payments rows")
    except Exception as e:
        log(f"payments sync error: {e}")

    log("Sync complete")
    return True


def get_connection_status():
    sb = _get_supabase()
    if not sb:
        return {
            "connected": False,
            "source": "none",
            "message": "Supabase not configured. Add SUPABASE_URL and SUPABASE_SERVICE_KEY to secrets.toml",
        }
    try:
        resp = sb.table("daily_kpis").select("count", count="exact").execute()
        count = resp.count or 0
        return {
            "connected": True,
            "source": "supabase",
            "daily_kpis_rows": count,
            "message": f"Supabase connected - {count} daily KPI rows",
        }
    except Exception as e:
        return {"connected": False, "source": "error", "message": f"Supabase error: {e}"}


if __name__ == "__main__":
    status = get_connection_status()
    print(json.dumps(status, indent=2))
    if status.get("connected") and status.get("daily_kpis_rows", 0) == 0:
        print("No data in Supabase - syncing from Google Sheets...")
        sync_sheets_to_supabase()
    elif status.get("connected"):
        print("Testing data load...")
        df = load_daily_counts()
        print(f"Daily counts: {len(df)} rows")
        df2 = load_signups()
        print(f"Signups: {len(df2)} rows")
        df3 = load_payments()
        print(f"Payments: {len(df3)} rows")
