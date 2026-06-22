#!/usr/bin/env python3
"""
Customer Success Analytics - COMPREHENSIVE
Covers all 9 tabs from the CS sheet with deep analysis.
"""

import os
import json
import re
from datetime import datetime, date, timedelta
from collections import Counter, defaultdict


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
    from supabase import create_client
    return create_client(url, key)


def _parse_date(v):
    if not v:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("none","nan","null"):
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%m/%d/%y",
                "%d/%m/%Y", "%b %d, %Y", "%Y-%m-%dT%H:%M:%S", "%B %d, %Y"):
        try:
            return datetime.strptime(s.split(".")[0].split("+")[0], fmt).date()
        except Exception:
            pass
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass
    return None


def _parse_int(v):
    if v is None or v == "":
        return 0
    try:
        s = str(v).replace(",","").replace("$","").strip()
        return int(float(s)) if s and s.lower() not in ("none","nan","null") else 0
    except Exception:
        return 0


def _parse_float(v):
    if v is None or v == "":
        return 0.0
    try:
        s = str(v).replace(",","").replace("$","").strip()
        return float(s) if s and s.lower() not in ("none","nan","null") else 0.0
    except Exception:
        return 0.0


def get_tab_data(tab_slug):
    sb = _get_sb()
    if not sb:
        return []
    try:
        rows = sb.table("customer_success_master").select("row_data").eq("tab_slug", tab_slug).execute().data or []
        return [r.get("row_data", {}) for r in rows]
    except Exception:
        return []


def get_unified_customers():
    """Merge all tabs by email."""
    sb = _get_sb()
    if not sb:
        return []
    try:
        rows = sb.table("customer_success_master").select("row_data,tab_name,email").execute().data or []
    except Exception:
        return []

    by_email = defaultdict(dict)
    for r in rows:
        rd = r.get("row_data", {}) or {}
        email = (rd.get("Email") or rd.get("E-mail") or r.get("email", "")).strip().lower()
        if not email or "@" not in email:
            continue
        for k, v in rd.items():
            if v not in (None, "", "—") and str(v).strip():
                by_email[email][k] = v
        by_email[email]["_source_tabs"] = by_email[email].get("_source_tabs", "") + r.get("tab_name", "") + ";"
    return list(by_email.values())


# ════════════════════════════════════════════════
# CHURN ANALYSIS
# ════════════════════════════════════════════════
def churn_by_month():
    churned = get_tab_data("churned_user")
    by_month = Counter()
    customers = []
    total_revenue_lost = 0
    for r in churned:
        d = _parse_date(r.get("Date") or r.get("date"))
        if d:
            by_month[d.strftime("%Y-%m")] += 1
        rev = _parse_float(r.get("Total (Rev/Mon)", r.get("Revenue", 0)))
        total_revenue_lost += rev
        customers.append({
            "date":      str(d) if d else str(r.get("Date","")),
            "customer":  r.get("Customer", ""),
            "revenue":   rev,
            "note":      r.get("Note", ""),
        })
    return {
        "by_month":         dict(sorted(by_month.items())),
        "total_churned":    len(churned),
        "monthly_revenue_lost": round(total_revenue_lost, 2),
        "annual_revenue_lost":  round(total_revenue_lost * 12, 2),
        "customers":        customers,
    }


# ════════════════════════════════════════════════
# CUSTOMER HEALTH INDEX
# ════════════════════════════════════════════════
def health_index_analysis():
    """All Customer Health Index data analysis."""
    rows = get_tab_data("customer_health_index")
    if not rows:
        rows = get_tab_data("sheet53")
    
    plans = Counter()
    statuses = Counter()
    company_sizes = Counter()
    perfect_fit = Counter()
    recurring_dist = []
    no_recurring = 0

    for r in rows:
        plans[str(r.get("Plan", "Unknown")).strip() or "Unknown"] += 1
        statuses[str(r.get("Status", "Unknown")).strip() or "Unknown"] += 1
        company_sizes[str(r.get("Company Size", "Unknown")).strip() or "Unknown"] += 1
        perfect_fit[str(r.get("Perfect Fit customer", "Unknown")).strip() or "Unknown"] += 1
        rc = _parse_int(r.get("Recurring Payment Count", 0))
        if rc > 0:
            recurring_dist.append(rc)
        else:
            no_recurring += 1

    avg_recurring = sum(recurring_dist) / len(recurring_dist) if recurring_dist else 0
    return {
        "total_customers":   len(rows),
        "plans":             dict(plans.most_common()),
        "statuses":          dict(statuses.most_common()),
        "company_sizes":     dict(company_sizes.most_common()),
        "perfect_fit":       dict(perfect_fit.most_common()),
        "avg_recurring":     round(avg_recurring, 1),
        "max_recurring":     max(recurring_dist) if recurring_dist else 0,
        "no_recurring_count": no_recurring,
        "recurring_distribution": recurring_dist,
    }


# ════════════════════════════════════════════════
# STREAMING TIME (across all bi-weekly periods)
# ════════════════════════════════════════════════
def streaming_time_breakdown():
    """Analyze ALL stream time periods (76+ columns of bi-weekly data)."""
    rows = get_tab_data("customer_health_index")
    if not rows:
        rows = get_tab_data("sheet53")
    
    period_totals = defaultdict(float)
    per_customer = []
    
    for r in rows:
        email = (r.get("Email") or "").strip().lower()
        total_stream = 0
        for k, v in r.items():
            kl = str(k).lower()
            if "stream time" in kl or "tst" in kl or "stream_time" in kl:
                val = _parse_float(v)
                period_totals[k] += val
                total_stream += val
        if total_stream > 0 and email:
            per_customer.append({
                "email":         email,
                "name":          f"{r.get('First Name','')} {r.get('Last Name','')}".strip() or r.get('User Name',''),
                "total_stream":  round(total_stream, 1),
                "plan":          r.get("Plan", ""),
                "status":        r.get("Status", ""),
                "company_size":  r.get("Company Size", ""),
                "perfect_fit":   r.get("Perfect Fit customer", ""),
                "recurring":     _parse_int(r.get("Recurring Payment Count", 0)),
            })
    per_customer.sort(key=lambda x: -x["total_stream"])
    
    return {
        "period_totals":      {k: round(v,1) for k,v in sorted(period_totals.items())},
        "total_stream_hours": round(sum(period_totals.values()), 1),
        "top_50_streamers":   per_customer[:50],
        "bottom_streamers":   per_customer[-30:] if len(per_customer) > 30 else [],
        "active_streamers":   len(per_customer),
        "non_streamers":      len(rows) - len(per_customer),
    }


# ════════════════════════════════════════════════
# SESSION ANALYSIS (Sheet40 + Sheet54)
# ════════════════════════════════════════════════
def session_analysis():
    """Detailed session/connection success/failure rates."""
    rows = get_tab_data("sheet40")
    if not rows:
        rows = get_tab_data("sheet54")
    
    per_client = []
    total_sessions = 0
    total_video_min = 0
    total_channel_min = 0
    total_connected_success = 0
    total_connected_failure = 0
    total_streamed_success = 0
    total_streamed_failure = 0
    
    for r in rows:
        sessions = _parse_int(r.get("Number of Sessions", 0))
        video_min = _parse_float(r.get("Video Streaming Time in min(s)", 0))
        channel_min = _parse_float(r.get("Channel Streaming Time in min(s)", 0))
        connected_succ = _parse_int(r.get("Connected Success", 0))
        connected_fail = _parse_int(r.get("Connected Failure", 0))
        streamed_succ = _parse_int(r.get("Streamed Success", 0))
        streamed_fail = _parse_int(r.get("Streamed Failure", 0))
        
        total_sessions += sessions
        total_video_min += video_min
        total_channel_min += channel_min
        total_connected_success += connected_succ
        total_connected_failure += connected_fail
        total_streamed_success += streamed_succ
        total_streamed_failure += streamed_fail
        
        if sessions > 0 or video_min > 0:
            per_client.append({
                "client":           r.get("Client Name", r.get("User Name", "")),
                "sessions":         sessions,
                "video_min":        video_min,
                "channel_min":      channel_min,
                "connected_success": connected_succ,
                "connected_failure": connected_fail,
                "streamed_success": streamed_succ,
                "streamed_failure": streamed_fail,
                "success_rate":     round(streamed_succ / (streamed_succ + streamed_fail) * 100, 1) if (streamed_succ + streamed_fail) else 0,
            })
    
    per_client.sort(key=lambda x: -x["sessions"])
    
    total_conn = total_connected_success + total_connected_failure
    total_stream = total_streamed_success + total_streamed_failure
    
    return {
        "total_clients":          len(per_client),
        "total_sessions":         total_sessions,
        "total_video_minutes":    round(total_video_min, 1),
        "total_channel_minutes":  round(total_channel_min, 1),
        "total_video_hours":      round(total_video_min / 60, 1),
        "connected_success_rate": round(total_connected_success / total_conn * 100, 2) if total_conn else 0,
        "streamed_success_rate":  round(total_streamed_success / total_stream * 100, 2) if total_stream else 0,
        "total_connected_success": total_connected_success,
        "total_connected_failure": total_connected_failure,
        "total_streamed_success":  total_streamed_success,
        "total_streamed_failure":  total_streamed_failure,
        "top_30_clients":         per_client[:30],
        "high_failure_clients":   sorted([c for c in per_client if c["success_rate"] < 80 and c["sessions"] > 5], key=lambda x: x["success_rate"])[:20],
    }


# ════════════════════════════════════════════════
# SHEET1 - LAST STREAMED + MONTHLY STREAMING
# ════════════════════════════════════════════════
def sheet1_analysis():
    """Last streamed, monthly TST, CHI score, color classification."""
    rows = get_tab_data("sheet1")
    
    customers = []
    color_dist = Counter()
    chi_dist = Counter()
    plan_dist = Counter()
    today = date.today()
    
    for r in rows:
        email = (r.get("Email") or "").strip().lower()
        if not email:
            continue
        last_streamed = _parse_date(r.get("Last Streamed"))
        days_since = (today - last_streamed).days if last_streamed else None
        
        chi = str(r.get("CHI", "")).strip()
        color = str(r.get("Color", "")).strip()
        plan = str(r.get("Plan", "")).strip() or "Unknown"
        
        chi_dist[chi] += 1
        color_dist[color] += 1
        plan_dist[plan] += 1
        
        customers.append({
            "email":           email,
            "name":            r.get("User Name", ""),
            "plan":            plan,
            "status":          r.get("Status", ""),
            "last_streamed":   str(last_streamed) if last_streamed else str(r.get("Last Streamed","")),
            "days_since":      days_since,
            "tst_april":       _parse_float(r.get("Total Streaming Time (April)", 0)),
            "tst_march":       _parse_float(r.get("TST : March", 0)),
            "tst_feb":         _parse_float(r.get("TST : February", 0)),
            "tst_jan":         _parse_float(r.get("TST: January", 0)),
            "tst_2025":        _parse_float(r.get("TST (2025)", 0)),
            "total_sessions":  _parse_int(r.get("Total Session", 0)),
            "last_month_sessions": _parse_int(r.get("Last Month Sessions", 0)),
            "chi":             chi,
            "color":           color,
        })
    
    customers.sort(key=lambda x: -x.get("tst_2025", 0))
    
    danger = [c for c in customers if c["days_since"] and c["days_since"] > 30 and c.get("tst_2025", 0) > 0]
    
    return {
        "total_customers":  len(customers),
        "color_dist":       dict(color_dist.most_common()),
        "chi_dist":         dict(chi_dist.most_common()),
        "plan_dist":        dict(plan_dist.most_common()),
        "top_2025_streamers": customers[:30],
        "dormant_paying":    sorted(danger, key=lambda x: -x.get("tst_2025", 0))[:30],
        "never_streamed":    [c for c in customers if not c["days_since"]][:30],
    }


# ════════════════════════════════════════════════
# SUBSCRIPTION LIFECYCLE
# ════════════════════════════════════════════════
def subscription_lifecycle():
    rows = get_tab_data("customer_health_index")
    if not rows:
        rows = get_tab_data("sheet53")
    
    today = date.today()
    month_start = today.replace(day=1)
    
    new_this_month = []
    ending_soon = []
    expired = []
    long_term = []  # 12+ months
    
    for r in rows:
        email = (r.get("Email") or "").strip().lower()
        if not email:
            continue
        start = _parse_date(r.get("Subscription Create Date"))
        cur_end = _parse_date(r.get("Current Period End Date"))
        cur_start = _parse_date(r.get("Current Period Start Date"))
        recurring = _parse_int(r.get("Recurring Payment Count", 0))
        plan = r.get("Plan", "")
        
        record = {
            "email":           email,
            "name":            f"{r.get('First Name','')} {r.get('Last Name','')}".strip(),
            "plan":            plan,
            "status":          r.get("Status", ""),
            "subscription_start": str(start) if start else "",
            "current_start":   str(cur_start) if cur_start else "",
            "current_end":     str(cur_end) if cur_end else "",
            "recurring":       recurring,
            "company_size":    r.get("Company Size", ""),
            "perfect_fit":     r.get("Perfect Fit customer", ""),
        }
        
        if start and start >= month_start:
            new_this_month.append(record)
        
        if cur_end:
            days_to_end = (cur_end - today).days
            if 0 <= days_to_end <= 30:
                ending_soon.append({**record, "days_left": days_to_end})
            elif days_to_end < 0:
                expired.append({**record, "days_ago": abs(days_to_end)})
        
        if recurring >= 12:
            long_term.append(record)
    
    return {
        "new_this_month":   new_this_month,
        "ending_in_30d":    sorted(ending_soon, key=lambda x: x["days_left"]),
        "expired":          sorted(expired, key=lambda x: x["days_ago"])[:50],
        "long_term_customers": sorted(long_term, key=lambda x: -x["recurring"]),
        "count_new":        len(new_this_month),
        "count_ending":     len(ending_soon),
        "count_expired":    len(expired),
        "count_long_term":  len(long_term),
    }


# ════════════════════════════════════════════════
# REVENUE & NET LOSS
# ════════════════════════════════════════════════
def revenue_analysis():
    """Revenue from Net loss tab + Churned + Recurring."""
    net_loss = get_tab_data("net_loss_due_to_conversion")
    losses = []
    total_prev = 0
    total_curr = 0
    total_dev = 0
    for r in net_loss:
        prev = _parse_float(r.get("previous spent", 0))
        curr = _parse_float(r.get("current", 0))
        dev = _parse_float(r.get("Deviation in USD", curr - prev))
        total_prev += prev
        total_curr += curr
        total_dev += dev
        losses.append({
            "email":    r.get("E-mail", ""),
            "previous": prev,
            "current":  curr,
            "deviation": dev,
        })
    
    return {
        "conversion_losses":      sorted(losses, key=lambda x: x["deviation"]),
        "total_previous":         round(total_prev, 2),
        "total_current":          round(total_curr, 2),
        "total_deviation":        round(total_dev, 2),
        "loss_customers":         len([l for l in losses if l["deviation"] < 0]),
        "gain_customers":         len([l for l in losses if l["deviation"] > 0]),
    }


# ════════════════════════════════════════════════
# STREAM TIME GRAPH (period trend)
# ════════════════════════════════════════════════
def stream_time_trend():
    rows = get_tab_data("stream_time_graph")
    return [
        {"period": r.get("Period", ""), "stream_time": _parse_float(r.get("Stream Time", 0))}
        for r in rows
    ]


# ════════════════════════════════════════════════
# NO SUB USERS
# ════════════════════════════════════════════════
def no_sub_users_analysis():
    rows = get_tab_data("no_sub_users")
    plans = Counter()
    company_sizes = Counter()
    perfect_fit = Counter()
    users = []
    for r in rows:
        plans[str(r.get("Plan","Unknown")).strip() or "Unknown"] += 1
        company_sizes[str(r.get("Company Size","Unknown")).strip() or "Unknown"] += 1
        perfect_fit[str(r.get("Perfect Fit customer","Unknown")).strip() or "Unknown"] += 1
        users.append({
            "email":         (r.get("Email") or "").strip().lower(),
            "name":          f"{r.get('First Name','')} {r.get('Last Name','')}".strip(),
            "plan":          r.get("Plan",""),
            "company_size":  r.get("Company Size",""),
            "perfect_fit":   r.get("Perfect Fit customer",""),
        })
    return {
        "total":         len(rows),
        "by_plan":       dict(plans.most_common()),
        "by_size":       dict(company_sizes.most_common()),
        "by_fit":        dict(perfect_fit.most_common()),
        "users":         users[:50],
    }


# ════════════════════════════════════════════════
# CORRELATIONS
# ════════════════════════════════════════════════
def cross_correlations():
    """Cross-tab insights: company size vs churn, plan vs streaming, etc."""
    rows_health = get_tab_data("customer_health_index") or get_tab_data("sheet53")
    rows_churn = get_tab_data("churned_user")
    
    churned_customers = set()
    for r in rows_churn:
        c = str(r.get("Customer","")).lower().strip()
        if c:
            churned_customers.add(c)
    
    # Company size vs churn
    size_total = Counter()
    size_churned = Counter()
    plan_total = Counter()
    plan_churned = Counter()
    fit_total = Counter()
    fit_churned = Counter()
    
    for r in rows_health:
        size = str(r.get("Company Size","Unknown")).strip() or "Unknown"
        plan = str(r.get("Plan","Unknown")).strip() or "Unknown"
        fit = str(r.get("Perfect Fit customer","Unknown")).strip() or "Unknown"
        email = (r.get("Email") or "").strip().lower()
        status = str(r.get("Status","")).lower()
        
        size_total[size] += 1
        plan_total[plan] += 1
        fit_total[fit] += 1
        
        is_churned = "cancel" in status or "ended" in status or email in churned_customers
        if is_churned:
            size_churned[size] += 1
            plan_churned[plan] += 1
            fit_churned[fit] += 1
    
    def _churn_rate(totals, churned):
        return {k: round(churned.get(k, 0) / v * 100, 1) for k, v in totals.items() if v > 0}
    
    return {
        "company_size_churn_rate": _churn_rate(size_total, size_churned),
        "plan_churn_rate":         _churn_rate(plan_total, plan_churned),
        "perfect_fit_churn_rate":  _churn_rate(fit_total, fit_churned),
        "size_totals":             dict(size_total),
        "plan_totals":             dict(plan_total),
        "fit_totals":              dict(fit_total),
    }


def get_all_insights():
    return {
        "churn":              churn_by_month(),
        "health":             health_index_analysis(),
        "streaming":          streaming_time_breakdown(),
        "sessions":           session_analysis(),
        "sheet1":             sheet1_analysis(),
        "subscriptions":      subscription_lifecycle(),
        "revenue":            revenue_analysis(),
        "stream_trend":       stream_time_trend(),
        "no_sub":             no_sub_users_analysis(),
        "correlations":       cross_correlations(),
        "generated_at":       datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    r = get_all_insights()
    for k, v in r.items():
        if isinstance(v, dict):
            print(f"\n=== {k} ===")
            for kk, vv in v.items():
                if isinstance(vv, (list, dict)):
                    print(f"  {kk}: {len(vv)} items")
                else:
                    print(f"  {kk}: {vv}")
