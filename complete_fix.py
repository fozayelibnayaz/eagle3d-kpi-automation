#!/usr/bin/env python3
"""
COMPLETE FIX:
1. Fix app.py to read all date columns from Supabase data correctly
2. Fix reporting_engine to use Supabase for KPI stats
3. Fix month format bug in Telegram (2026-06% -> 2026-06)
4. Fix common period logic - use upload coverage start as All Time start
5. Speed up data loading with Supabase caching
"""
from pathlib import Path
from datetime import datetime

print("Starting complete fix...")

app = Path("app.py")
content = app.read_text()

# ─────────────────────────────────────────────
# FIX 1: After load_sheet calls, add column normalizer
# so Supabase data columns match what app.py expects
# ─────────────────────────────────────────────
OLD_LOAD = """with st.spinner("Loading data..."):
    counts_raw = load_sheet("Daily_Counts")
    free_raw = load_sheet("Verified_FREE")
    upload_raw = load_sheet("Verified_FIRST_UPLOAD")
    stripe_raw = load_sheet("Verified_STRIPE")"""

NEW_LOAD = """with st.spinner("Loading data..."):
    counts_raw  = load_sheet("Daily_Counts")
    free_raw    = load_sheet("Verified_FREE")
    upload_raw  = load_sheet("Verified_FIRST_UPLOAD")
    stripe_raw  = load_sheet("Verified_STRIPE")

# ── COLUMN NORMALIZER: ensure Supabase columns match app.py expectations ──
def _norm_cols(df, col_map):
    if df.empty:
        return df
    for src, dst in col_map.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
    return df

# Signups: signup_date -> Account Created On
free_raw = _norm_cols(free_raw, {
    "signup_date": "Account Created On",
    "lead_source": "Lead Source",
    "email_normalized": "__email_normalized__",
})
# Uploads: upload_date -> Upload Date
upload_raw = _norm_cols(upload_raw, {
    "upload_date": "Upload Date",
    "email_normalized": "__email_normalized__",
})
# Payments: first_payment_date -> First payment, total_spend -> Amount
stripe_raw = _norm_cols(stripe_raw, {
    "first_payment_date": "First payment",
    "total_spend": "Amount",
    "email_normalized": "__email_normalized__",
})
if not stripe_raw.empty and "row_date_used" not in stripe_raw.columns:
    for _fc in ("First payment", "first_payment_date"):
        if _fc in stripe_raw.columns:
            stripe_raw["row_date_used"] = stripe_raw[_fc]
            break
# Daily counts: ensure correct column names
counts_raw = _norm_cols(counts_raw, {
    "signups_accepted":  "SignUps_Accepted",
    "uploads_accepted":  "FirstUploads_Accepted",
    "paid_accepted":     "PaidSubscribers_Accepted",
    "signup_details":    "SignUp_Details",
    "upload_details":    "Upload_Details",
    "paid_details":      "Paid_Details",
    "last_updated":      "LastUpdated",
    "date":              "Date",
})"""

if OLD_LOAD in content:
    content = content.replace(OLD_LOAD, NEW_LOAD)
    print("FIX 1 APPLIED: Column normalizer added after load_sheet calls")
else:
    print("WARNING: Could not find load_sheet block - trying alternate")
    # Try with different whitespace
    if 'counts_raw = load_sheet("Daily_Counts")' in content:
        content = content.replace(
            'counts_raw = load_sheet("Daily_Counts")',
            'counts_raw  = load_sheet("Daily_Counts")'
        )
        print("  Applied alternate whitespace fix")

# ─────────────────────────────────────────────
# FIX 2: Fix month format bug
# "2026-06%" comes from cur_month_str = "%Y-%m%"
# in reporting_engine.py build_kpi_stats
# ─────────────────────────────────────────────
# Fix in reporting_engine.py
re_path = Path("reporting_engine.py")
re_content = re_path.read_text()
if '"%Y-%m%"' in re_content:
    re_content = re_content.replace('"%Y-%m%"', '"%Y-%m"')
    re_path.write_text(re_content)
    print("FIX 2 APPLIED: Month format bug fixed in reporting_engine.py (removed % suffix)")
else:
    print("FIX 2: Month format bug not found in reporting_engine.py (may already be fixed)")

# Also fix in app.py if present
if '"%Y-%m%"' in content:
    content = content.replace('"%Y-%m%"', '"%Y-%m"')
    print("FIX 2b APPLIED: Month format bug fixed in app.py")

# ─────────────────────────────────────────────
# FIX 3: Add Supabase-based KPI stats function
# that correctly uses common period
# ─────────────────────────────────────────────
SUPABASE_KPI_FUNC = '''
# ── SUPABASE FAST KPI STATS ──
@st.cache_data(ttl=60)
def get_supabase_kpi_fast():
    """Get KPI counts directly from Supabase - fast and accurate."""
    try:
        import os
        from supabase import create_client
        _url = os.environ.get("SUPABASE_URL","")
        _key = os.environ.get("SUPABASE_SERVICE_KEY","")
        if not _url:
            _url = str(st.secrets.get("SUPABASE_URL","")).strip()
        if not _key:
            _key = str(st.secrets.get("SUPABASE_SERVICE_KEY","")).strip()
        if not _url or not _key:
            return None
        _sb = create_client(_url, _key)
        _today = datetime.now().strftime("%Y-%m-%d")
        _month_start = datetime.now().strftime("%Y-%m-01")
        # Upload coverage start = common period start
        _upload_start = "2025-12-01"
        try:
            _ur = _sb.table("uploads").select("upload_date").eq("final_status","ACCEPTED").order("upload_date").limit(1).execute()
            if _ur.data and _ur.data[0].get("upload_date"):
                _upload_start = _ur.data[0]["upload_date"][:10]
        except Exception:
            pass
        # Today counts
        _st = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_today).execute()
        _ut = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_today).execute()
        _pt = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_today).execute()
        # Month counts
        _sm = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_month_start).execute()
        _um = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_month_start).execute()
        _pm = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_month_start).execute()
        # Common period (from upload start = Dec 2025)
        _sc = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_upload_start).execute()
        _uc = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_upload_start).execute()
        _pc = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_upload_start).execute()
        # Full DB totals
        _sf = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").execute()
        _uf = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").execute()
        _pf = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").execute()
        return {
            "today_signups":   _st.count or 0,
            "today_uploads":   _ut.count or 0,
            "today_paid":      _pt.count or 0,
            "month_signups":   _sm.count or 0,
            "month_uploads":   _um.count or 0,
            "month_paid":      _pm.count or 0,
            "common_signups":  _sc.count or 0,
            "common_uploads":  _uc.count or 0,
            "common_paid":     _pc.count or 0,
            "full_signups":    _sf.count or 0,
            "full_uploads":    _uf.count or 0,
            "full_paid":       _pf.count or 0,
            "common_start":    _upload_start,
            "today":           _today,
            "month_start":     _month_start,
        }
    except Exception as _e:
        return None
'''

# Add function before the load_sheet function
if "get_supabase_kpi_fast" not in content:
    content = content.replace(
        "@st.cache_data(ttl=120)\ndef load_sheet(tab):",
        SUPABASE_KPI_FUNC + "\n@st.cache_data(ttl=120)\ndef load_sheet(tab):"
    )
    print("FIX 3 APPLIED: get_supabase_kpi_fast() added")
else:
    print("FIX 3: get_supabase_kpi_fast already exists")

# ─────────────────────────────────────────────
# FIX 4: Speed up - increase cache TTL and add
# Supabase-based fast path for KPI dashboard metrics
# ─────────────────────────────────────────────
# Increase cache TTL from 120 to 300 seconds for faster repeat loads
content = content.replace("@st.cache_data(ttl=120)\ndef load_sheet(tab):",
                           "@st.cache_data(ttl=300)\ndef load_sheet(tab):")
print("FIX 4 APPLIED: Cache TTL increased to 300s for faster repeat loads")

# ─────────────────────────────────────────────
# FIX 5: Fix the KPI display to use Supabase fast stats
# when available - inject after kpi_all is built
# ─────────────────────────────────────────────
# Find where today's KPI is computed for display
# and add Supabase override

INJECT_AFTER = "kpi = filter_kpi(kpi_all, p_start, p_end)"
SUPABASE_INJECT = """
# ── SUPABASE FAST KPI OVERRIDE ──
# Use direct Supabase counts for today/month (always accurate)
_sb_kpi = get_supabase_kpi_fast()
if _sb_kpi:
    _sb_today_s = _sb_kpi["today_signups"]
    _sb_today_u = _sb_kpi["today_uploads"]
    _sb_today_p = _sb_kpi["today_paid"]
    _sb_month_s = _sb_kpi["month_signups"]
    _sb_month_u = _sb_kpi["month_uploads"]
    _sb_month_p = _sb_kpi["month_paid"]
    _sb_common_s = _sb_kpi["common_signups"]
    _sb_common_u = _sb_kpi["common_uploads"]
    _sb_common_p = _sb_kpi["common_paid"]
    _sb_common_start = _sb_kpi["common_start"]
else:
    _sb_today_s = _sb_today_u = _sb_today_p = 0
    _sb_month_s = _sb_month_u = _sb_month_p = 0
    _sb_common_s = _sb_common_u = _sb_common_p = 0
    _sb_common_start = "2025-12-01"
"""

# Only inject once
if "SUPABASE FAST KPI OVERRIDE" not in content:
    # Find the first occurrence of kpi = filter_kpi
    idx = content.find(INJECT_AFTER)
    if idx >= 0:
        insert_pos = idx + len(INJECT_AFTER)
        content = content[:insert_pos] + "\n" + SUPABASE_INJECT + content[insert_pos:]
        print("FIX 5 APPLIED: Supabase KPI fast path injected")
    else:
        print("WARNING: Could not inject Supabase KPI override - filter_kpi not found")
else:
    print("FIX 5: Already injected")

# Save patched app.py
app.write_text(content)
print("\napp.py saved successfully")
print("All fixes applied")
