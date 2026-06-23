#!/usr/bin/env python3
"""
sheets_writer.py - SUPABASE ONLY MODE
Google Sheets disabled. All read/write goes to Supabase.
Sheets functions kept as compatibility shims.
"""
import os
import time
from datetime import datetime


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Sheets-shim] {msg}", flush=True)


def _get_sb():
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        try:
            import streamlit as st
            url = str(st.secrets.get("SUPABASE_URL", "")).strip()
            key = str(st.secrets.get("SUPABASE_SERVICE_KEY", "")).strip()
        except Exception:
            pass
    if not url or not key:
        return None
    try:
        from supabase import create_client
        return create_client(url, key)
    except Exception:
        return None


# Tab to Supabase mapping
TAB_TO_TABLE = {
    "Verified_FREE":          ("signups",   "signup_date",        "email"),
    "Verified_FIRST_UPLOAD":  ("uploads",   "upload_date",        "email"),
    "Verified_STRIPE":        ("payments",  "first_payment_date", "email"),
    "Daily_Counts":           ("daily_kpis", "date",              None),
}


def test_connection():
    sb = _get_sb()
    return sb is not None


def _get_client():
    return None, None


def _rate_limit():
    pass


def _retry_on_quota(func, *args, **kwargs):
    return func(*args, **kwargs)


def _get_or_create_ws(*args, **kwargs):
    return None


def ensure_tabs_exist(*args, **kwargs):
    return True


def write_run_summary(*args, **kwargs):
    return True


def _fetch_all(sb, table, cols="*"):
    """Paginate Supabase fetch."""
    rows = []
    offset = 0
    while True:
        try:
            r = sb.table(table).select(cols).range(offset, offset + 999).execute()
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
        except Exception as e:
            log(f"Fetch error {table}: {e}")
            break
    return rows


def read_tab_data(tab_name):
    """Read from Supabase only. Returns list of dicts in Sheet-compatible format."""
    sb = _get_sb()
    if not sb:
        log(f"Supabase unavailable - returning empty for {tab_name}")
        return []

    if tab_name == "Verified_FREE":
        rows = _fetch_all(sb, "signups")
        # Convert to Sheet format
        return [{
            "Email":               r.get("email", ""),
            "__email_normalized__": r.get("email_normalized", ""),
            "Account Created On":  r.get("signup_date", ""),
            "row_date_used":       r.get("signup_date", ""),
            "Lead Source":         r.get("lead_source", ""),
            "final_status":        r.get("final_status", ""),
            "category":            r.get("category", ""),
            "__rejection_reason__": r.get("rejection_reason", ""),
        } for r in rows]

    elif tab_name == "Verified_FIRST_UPLOAD":
        rows = _fetch_all(sb, "uploads")
        return [{
            "Email":               r.get("email", ""),
            "__email_normalized__": r.get("email_normalized", ""),
            "Upload Date":         r.get("upload_date", ""),
            "row_date_used":       r.get("upload_date", ""),
            "final_status":        r.get("final_status", ""),
            "category":            r.get("category", ""),
            "__rejection_reason__": r.get("rejection_reason", ""),
        } for r in rows]

    elif tab_name == "Verified_STRIPE":
        rows = _fetch_all(sb, "payments")
        return [{
            "Email":               r.get("email", ""),
            "__email_normalized__": r.get("email_normalized", ""),
            "First payment":       r.get("first_payment_date", ""),
            "row_date_used":       r.get("first_payment_date", ""),
            "Amount":              r.get("total_spend", 0),
            "Total spend":         r.get("total_spend", 0),
            "__amount__":          r.get("total_spend", 0),
            "Payment Count":       r.get("payment_count", 0),
            "final_status":        r.get("final_status", ""),
            "category":            r.get("category", ""),
        } for r in rows]

    elif tab_name == "Daily_Counts":
        rows = _fetch_all(sb, "daily_kpis")
        return [{
            "Date":                       r.get("date", ""),
            "Year":                       r.get("year", ""),
            "Month":                      r.get("month", ""),
            "SignUps_Accepted":           r.get("signups_accepted", 0),
            "FirstUploads_Accepted":      r.get("uploads_accepted", 0),
            "PaidSubscribers_Accepted":   r.get("paid_accepted", 0),
            "SignUp_Details":             r.get("signup_details", ""),
            "Upload_Details":             r.get("upload_details", ""),
            "Paid_Details":               r.get("paid_details", ""),
            "LastUpdated":                r.get("last_updated", ""),
        } for r in rows]

    else:
        log(f"Unknown tab '{tab_name}' - returning empty")
        return []


def write_tab_data(tab_name, rows):
    """Write to Supabase only. No Sheets writes."""
    if not rows:
        log(f"No rows to write for {tab_name}")
        return True

    sb = _get_sb()
    if not sb:
        log(f"Supabase unavailable - cannot write {tab_name}")
        return False

    import re as _re
    from datetime import datetime as _dt

    def _parse_date(v):
        if not v: return None
        s = str(v).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d","%m/%d/%y, %I:%M %p","%m/%d/%Y, %I:%M %p",
                    "%a, %d %b %Y %H:%M:%S %Z","%a, %d %b %Y %H:%M:%S GMT","%b %d, %Y"):
            try:
                return _dt.strptime(s.split("+")[0], fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        m = _re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return None

    def _parse_amount(v):
        if not v: return 0.0
        s = _re.sub(r"[$,\s\n\r]", "", str(v))
        s = _re.sub(r"[A-Za-z]+$", "", s).strip()
        try: return float(s)
        except: return 0.0

    def _norm_email(r):
        em = str(r.get("Email","") or r.get("email","") or r.get("__email_normalized__","")).strip().lower()
        return em if em and "@" in em else ""

    if tab_name == "Verified_FREE":
        upsert = []
        for r in rows:
            em = _norm_email(r)
            if not em: continue
            d = None
            for f in ("Account Created On","row_date_used","__scraped_date__"):
                v = r.get(f)
                if v:
                    d = _parse_date(v)
                    if d: break
            upsert.append({
                "email": em, "email_normalized": em,
                "signup_date": d,
                "lead_source": str(r.get("Lead Source","") or "")[:200],
                "final_status": str(r.get("final_status","PENDING") or "PENDING").upper()[:20],
                "category": str(r.get("category","") or "")[:50],
                "rejection_reason": str(r.get("__rejection_reason__", r.get("verdict_reason","")) or "")[:300],
            })
        return _upsert(sb, "signups", upsert)

    elif tab_name == "Verified_FIRST_UPLOAD":
        upsert = []
        for r in rows:
            em = _norm_email(r)
            if not em: continue
            d = None
            for f in ("Upload Date","row_date_used","__scraped_date__"):
                v = r.get(f)
                if v:
                    d = _parse_date(v)
                    if d: break
            upsert.append({
                "email": em, "email_normalized": em,
                "upload_date": d,
                "final_status": str(r.get("final_status","PENDING") or "PENDING").upper()[:20],
                "category": str(r.get("category","") or "")[:50],
                "rejection_reason": str(r.get("__rejection_reason__","") or "")[:300],
            })
        return _upsert(sb, "uploads", upsert)

    elif tab_name == "Verified_STRIPE":
        upsert = []
        for r in rows:
            em = _norm_email(r)
            if not em: continue
            d = None
            for f in ("First payment","row_date_used","Created","Created (UTC)","__scraped_date__"):
                v = r.get(f)
                if v:
                    d = _parse_date(v)
                    if d: break
            amt = 0.0
            for af in ("Amount","Total spend","Total Spend","__amount__"):
                v = r.get(af)
                if v:
                    a = _parse_amount(v)
                    if a > 0: amt = a; break
            try:
                pc = int(r.get("Payment Count", r.get("payment_count",0)) or 0)
            except: pc = 0
            upsert.append({
                "email": em, "email_normalized": em,
                "first_payment_date": d,
                "total_spend": amt,
                "payment_count": pc,
                "final_status": str(r.get("final_status","PENDING") or "PENDING").upper()[:20],
                "category": str(r.get("category","") or "")[:50],
            })
        return _upsert(sb, "payments", upsert)

    elif tab_name == "Daily_Counts":
        upsert = []
        for r in rows:
            d = str(r.get("Date","") or "")[:10]
            if not d: continue
            upsert.append({
                "date": d,
                "year": str(r.get("Year", d[:4])),
                "month": str(r.get("Month", d[:7])),
                "signups_accepted": int(r.get("SignUps_Accepted",0) or 0),
                "uploads_accepted": int(r.get("FirstUploads_Accepted",0) or 0),
                "paid_accepted": int(r.get("PaidSubscribers_Accepted",0) or 0),
                "signup_details": str(r.get("SignUp_Details","") or "")[:5000],
                "upload_details": str(r.get("Upload_Details","") or "")[:5000],
                "paid_details": str(r.get("Paid_Details","") or "")[:5000],
                "last_updated": str(r.get("LastUpdated", datetime.utcnow().isoformat())),
            })
        return _upsert(sb, "daily_kpis", upsert, on_conflict="date")

    else:
        log(f"Unknown tab '{tab_name}' - skipping write")
        return True


def _upsert(sb, table, rows, on_conflict="email_normalized"):
    if not rows:
        return True
    log(f"Upserting {len(rows)} rows to {table}")
    errors = 0
    for i in range(0, len(rows), 50):
        try:
            sb.table(table).upsert(rows[i:i+50], on_conflict=on_conflict).execute()
        except Exception as e:
            errors += 1
            if errors <= 3:
                log(f"  Chunk {i} err: {e}")
    if errors == 0:
        log(f"  {table} OK")
    return errors == 0
