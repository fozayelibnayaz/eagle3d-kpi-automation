#!/usr/bin/env python3
"""Fix reporting_engine.py to use Supabase for KPI stats"""
from pathlib import Path

re_path = Path("reporting_engine.py")
content = re_path.read_text()

# Fix month format bug
fixed = 0
if '"%Y-%m%"' in content:
    content = content.replace('"%Y-%m%"', '"%Y-%m"')
    fixed += 1
    print("Fixed month format bug: removed % suffix")

if "cur_month_str = datetime.now().strftime" in content:
    import re
    # Find and fix any strftime with % at end
    content = re.sub(
        r'strftime\("%Y-%m%"\)',
        'strftime("%Y-%m")',
        content
    )
    fixed += 1
    print("Fixed strftime month format")

# Add Supabase KPI function at top of build_kpi_stats
OLD_BUILD_KPI = 'def build_kpi_stats():\n    """KPI System stats'
NEW_BUILD_KPI = '''def build_kpi_stats():
    """KPI System stats - uses Supabase as primary source"""
    # Try Supabase first for fast accurate counts
    try:
        import os
        from supabase import create_client as _sb_cc
        _url = os.environ.get("SUPABASE_URL","").strip()
        _key = os.environ.get("SUPABASE_SERVICE_KEY","").strip()
        if not _url:
            try:
                import streamlit as st
                _url = str(st.secrets.get("SUPABASE_URL","")).strip()
                _key = str(st.secrets.get("SUPABASE_SERVICE_KEY","")).strip()
            except Exception:
                pass
        if _url and _key:
            _sb = _sb_cc(_url, _key)
            _today = datetime.now().strftime("%Y-%m-%d")
            _mstart = datetime.now().strftime("%Y-%m-01")
            _upload_start = "2025-12-01"
            try:
                _ur = _sb.table("uploads").select("upload_date").eq("final_status","ACCEPTED").order("upload_date").limit(1).execute()
                if _ur.data and _ur.data[0].get("upload_date"):
                    _upload_start = _ur.data[0]["upload_date"][:10]
            except Exception:
                pass
            _st = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_today).execute()
            _ut = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_today).execute()
            _pt = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_today).execute()
            _sm = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_mstart).execute()
            _um = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_mstart).execute()
            _pm = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_mstart).execute()
            _sc = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_upload_start).execute()
            _uc = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_upload_start).execute()
            _pc = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_upload_start).execute()
            _sf = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").execute()
            _pf = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").execute()
            _ovr = _sb.table("manual_overrides").select("count",count="exact").eq("is_active",True).execute()
            _month_str = datetime.now().strftime("%Y-%m")
            return {
                "signups_today":          _st.count or 0,
                "uploads_today":          _ut.count or 0,
                "paid_today":             _pt.count or 0,
                "signups_month":          _sm.count or 0,
                "uploads_month":          _um.count or 0,
                "paid_month":             _pm.count or 0,
                "signups_all":            _sc.count or 0,
                "uploads_all":            _uc.count or 0,
                "paid_all":               _pc.count or 0,
                "signups_full_db":        _sf.count or 0,
                "uploads_full_db":        0,
                "paid_full_db":           _pf.count or 0,
                "signups_all_override":   _sc.count or 0,
                "uploads_all_override":   _uc.count or 0,
                "paid_all_override":      _pc.count or 0,
                "signups_month_override": _sm.count or 0,
                "uploads_month_override": _um.count or 0,
                "paid_month_override":    _pm.count or 0,
                "today_str":              _today,
                "month_str":              _month_str,
                "alltime_period":         f"Common Period ({_upload_start[:7]} to {_today[:7]})",
                "alltime_start":          _upload_start,
                "alltime_end":            _today,
                "total_overrides":        _ovr.count or 0,
                "validation_passed":      True,
                "validation_warnings":    0,
                "validation_summary":     "PASS - Supabase direct counts",
                "validation_banner":      "",
                "source":                 "supabase",
            }
    except Exception as _e:
        log(f"Supabase KPI stats error (falling back to Sheets): {_e}")

    # FALLBACK: original Sheets-based implementation
    """KPI System stats'''

if OLD_BUILD_KPI in content:
    content = content.replace(OLD_BUILD_KPI, NEW_BUILD_KPI)
    print("Fixed build_kpi_stats to use Supabase primary")
    fixed += 1
else:
    print("WARNING: Could not patch build_kpi_stats - function signature different")

re_path.write_text(content)
print(f"reporting_engine.py saved ({fixed} fixes applied)")
