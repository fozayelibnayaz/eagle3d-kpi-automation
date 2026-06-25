#!/usr/bin/env python3
"""
Executive Dashboard - Core Business Metrics
Shows ONLY what matters: Revenue, Signups, Uploads, Marketing, Growth
Data accuracy first. No vanity metrics.
"""

import os
from datetime import datetime, date, timedelta
from collections import Counter, defaultdict


def _get_sb():
    """Get Supabase via sheets_writer shim (100% proven on Streamlit Cloud)."""
    try:
        from sheets_writer import _get_sb as _sw_get_sb
        return _sw_get_sb()
    except Exception:
        pass
    try:
        from supabase_data_loader import _get_supabase
        return _get_supabase()
    except Exception:
        pass
    import os
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        try:
            import streamlit as st
            try: url = str(st.secrets["SUPABASE_URL"]).strip()
            except: pass
            try: key = str(st.secrets["SUPABASE_SERVICE_KEY"]).strip()
            except: pass
        except: pass
    if not url or not key:
        return None
    try:
        from supabase import create_client
        return create_client(url, key)
    except:
        return None


def _fetch_all(sb, table, cols, filters=None):
    rows = []
    offset = 0
    while True:
        try:
            q = sb.table(table).select(cols)
            if filters:
                for k, v in filters.items():
                    q = q.eq(k, v)
            r = q.range(offset, offset + 999).execute()
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
        except Exception:
            break
    return rows


def _period_range(period_name):
    """Returns (start_date, end_date, prev_start, prev_end, label)."""
    today = date.today()
    if period_name == "this_month":
        start = today.replace(day=1)
        end = today
        prev_end = start - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        return start, end, prev_start, prev_end, f"{start.strftime('%B %Y')}"
    elif period_name == "last_month":
        end = today.replace(day=1) - timedelta(days=1)
        start = end.replace(day=1)
        prev_end = start - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        return start, end, prev_start, prev_end, f"{start.strftime('%B %Y')}"
    elif period_name == "this_quarter":
        q = (today.month - 1) // 3
        start = date(today.year, q * 3 + 1, 1)
        end = today
        prev_start = date(today.year if q > 0 else today.year - 1, (q - 1) * 3 + 1 if q > 0 else 10, 1)
        prev_end = start - timedelta(days=1)
        return start, end, prev_start, prev_end, f"Q{q+1} {today.year}"
    elif period_name == "this_year":
        start = date(today.year, 1, 1)
        end = today
        prev_start = date(today.year - 1, 1, 1)
        prev_end = date(today.year - 1, 12, 31)
        return start, end, prev_start, prev_end, str(today.year)
    elif period_name == "last_year":
        start = date(today.year - 1, 1, 1)
        end = date(today.year - 1, 12, 31)
        prev_start = date(today.year - 2, 1, 1)
        prev_end = date(today.year - 2, 12, 31)
        return start, end, prev_start, prev_end, str(today.year - 1)
    else:
        start = today.replace(day=1)
        end = today
        prev_end = start - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        return start, end, prev_start, prev_end, "Current"


def get_core_metrics(period="this_month"):
    """Get ALL core business metrics for the executive dashboard."""
    sb = _get_sb()
    if not sb:
        return {"error": "Supabase not configured"}

    start, end, prev_start, prev_end, label = _period_range(period)
    s, e = start.isoformat(), end.isoformat()
    ps, pe = prev_start.isoformat(), prev_end.isoformat()
    today = date.today()

    upload_start = "2025-12-01"
    try:
        ur = sb.table("uploads").select("upload_date").eq("final_status","ACCEPTED").not_.is_("upload_date","null").order("upload_date").limit(1).execute()
        if ur.data and ur.data[0].get("upload_date"):
            upload_start = str(ur.data[0]["upload_date"])[:10]
    except Exception:
        pass

    def cnt(table, col, start_d, end_d, status="ACCEPTED"):
        try:
            return sb.table(table).select("count",count="exact").eq("final_status",status).gte(col,start_d).lte(col,end_d).execute().count or 0
        except Exception:
            return 0

    def rev(start_d, end_d):
        try:
            r = sb.table("payments").select("total_spend").eq("final_status","ACCEPTED").gte("first_payment_date",start_d).lte("first_payment_date",end_d).execute().data or []
            return round(sum(float(x.get("total_spend") or 0) for x in r), 2)
        except Exception:
            return 0.0

    def delta(curr, prev_val):
        if prev_val == 0:
            return curr, None
        d = curr - prev_val
        pct = round(d / prev_val * 100, 1)
        return d, pct

    # Revenue
    curr_rev = rev(s, e)
    prev_rev = rev(ps, pe)
    rev_delta, rev_pct = delta(curr_rev, prev_rev)
    yoy_start = date(start.year-1, start.month, start.day).isoformat()
    yoy_end = date(end.year-1, end.month, min(end.day,28)).isoformat()
    yoy_rev = rev(yoy_start, yoy_end)
    _, yoy_rev_pct = delta(curr_rev, yoy_rev)

    # Signups
    curr_signups = cnt("signups","signup_date",s,e)
    prev_signups = cnt("signups","signup_date",ps,pe)
    signup_delta, signup_pct = delta(curr_signups, prev_signups)
    yoy_signups = cnt("signups","signup_date",yoy_start,yoy_end)
    _, yoy_signup_pct = delta(curr_signups, yoy_signups)

    # Uploads
    curr_uploads = cnt("uploads","upload_date",s,e)
    prev_uploads = cnt("uploads","upload_date",ps,pe)
    upload_delta, upload_pct = delta(curr_uploads, prev_uploads)

    # Paid
    curr_paid = cnt("payments","first_payment_date",s,e)
    prev_paid = cnt("payments","first_payment_date",ps,pe)
    paid_delta, paid_pct = delta(curr_paid, prev_paid)

    # Conversion
    s2u = round(curr_uploads/curr_signups*100,1) if curr_signups > 0 else 0
    s2p = round(curr_paid/curr_signups*100,1) if curr_signups > 0 else 0

    # Totals
    total_rev = rev("2020-01-01", today.isoformat())
    total_paid = cnt("payments","first_payment_date","2020-01-01",today.isoformat())
    avg_sub = round(total_rev/total_paid,2) if total_paid > 0 else 0

    # ── MONTHLY TREND ──
    monthly_trend = []
    for i in range(12):
        m_date = date(today.year, today.month, 1) - timedelta(days=30 * i)
        ms = date(m_date.year, m_date.month, 1).isoformat()
        if m_date.month == 12:
            me = date(m_date.year, 12, 31).isoformat()
        else:
            me = (date(m_date.year, m_date.month + 1, 1) - timedelta(days=1)).isoformat()
        monthly_trend.append({
            "month": m_date.strftime("%Y-%m"),
            "signups": cnt("signups", "signup_date", ms, me),
            "uploads": cnt("uploads", "upload_date", ms, me),
            "paid": cnt("payments", "first_payment_date", ms, me),
            "revenue": rev(ms, me),
        })
    monthly_trend.reverse()

    # ── DATA QUALITY ──
    total_signups_raw = sb.table("signups").select("count", count="exact").execute().count or 0
    total_accepted = sb.table("signups").select("count", count="exact").eq("final_status", "ACCEPTED").execute().count or 0
    total_rejected = total_signups_raw - total_accepted
    spam_rate = round(total_rejected / total_signups_raw * 100, 1) if total_signups_raw > 0 else 0
    rej_data = sb.table("signups").select("category").eq("final_status", "REJECTED").execute().data or []
    rej_reasons = Counter(r.get("category", "") for r in rej_data)
    internal_count = sb.table("signups").select("count", count="exact").ilike("email_normalized", "%eagle3d%").execute().count or 0

    # ── LEAD SOURCES ──
    leads = _fetch_all(sb, "signups", "lead_source", {"final_status": "ACCEPTED"})
    lead_sources = Counter(r.get("lead_source", "") or "(unspecified)" for r in leads)
    period_leads = sb.table("signups").select("lead_source").eq("final_status", "ACCEPTED").gte("signup_date", s).lte("signup_date", e).execute().data or []
    period_lead_sources = Counter(r.get("lead_source", "") or "(unspecified)" for r in period_leads)

    return {
        "period": label,
        "period_start": s,
        "period_end": e,
        "prev_period": prev_start.strftime("%B %Y"),
        "common_start": upload_start,
        "revenue": curr_rev,
        "prev_revenue": prev_rev,
        "revenue_delta": rev_delta,
        "revenue_pct": rev_pct,
        "yoy_revenue": yoy_rev,
        "yoy_revenue_pct": yoy_rev_pct,
        "total_revenue": total_rev,
        "avg_subscription": avg_sub,
        "signups": curr_signups,
        "prev_signups": prev_signups,
        "signup_delta": signup_delta,
        "signup_pct": signup_pct,
        "yoy_signups": yoy_signups,
        "yoy_signup_pct": yoy_signup_pct,
        "uploads": curr_uploads,
        "prev_uploads": prev_uploads,
        "upload_delta": upload_delta,
        "upload_pct": upload_pct,
        "paid": curr_paid,
        "prev_paid": prev_paid,
        "paid_delta": paid_delta,
        "paid_pct": paid_pct,
        "total_paid": total_paid,
        "s2u_rate": s2u,
        "s2p_rate": s2p,
        "total_raw": total_signups_raw,
        "total_accepted": total_accepted,
        "total_rejected": total_rejected,
        "spam_rate": spam_rate,
        "internal_count": internal_count,
        "rejection_reasons": dict(rej_reasons.most_common(10)),
        "lead_sources_all": dict(lead_sources.most_common(20)),
        "lead_sources_period": dict(period_lead_sources.most_common(15)),
        "monthly_trend": monthly_trend,
        "content_volume": {},
        "channel_growth": {},
        "generated_at": datetime.utcnow().isoformat(),
    }


def get_signup_definition():
    """Clear definition of what counts as a signup."""
    return {
        "definition": "A signup is a UNIQUE, VERIFIED, NON-INTERNAL email that registered on the Eagle3D platform.",
        "accepted_criteria": [
            "Email has valid format",
            "Email has valid MX record (mail server exists)",
            "Email is NOT from a disposable domain",
            "Email is NOT from eagle3dstreaming.com (internal)",
            "Email is NOT a duplicate of an existing signup",
            "Email passed SMTP verification (when available)",
        ],
        "rejected_reasons": [
            "NOT_DETERMINED: SMTP check inconclusive (conservative reject)",
            "DISPOSABLE: Email from known disposable domain",
            "INTERNAL: eagle3dstreaming.com domain",
            "DUPLICATE_IN_BATCH: Same email appeared twice in one scrape",
            "NO_MX: Domain has no mail server",
            "DUPLICATE_DIFFERENT_DATE: Already signed up on different date",
            "INVALID_FORMAT: Not a valid email address",
            "SUSPICIOUS: Pattern matches known spam",
        ],
        "note": "This definition scrubs spam and duplicates. Only ACCEPTED signups are counted in KPIs.",
    }


if __name__ == "__main__":
    import json
    m = get_core_metrics("this_month")
    print(json.dumps({k: v for k, v in m.items() if k != "monthly_trend"}, indent=2, default=str))
