#!/usr/bin/env python3
"""
Customer Success Analytics
Deep analysis of customer data: streaming time, churn, plan distribution,
company sizes, recurring payments, last streamed, perfect fit customers.
"""

import os
import json
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
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%m/%d/%y",
                "%d/%m/%Y", "%b %d, %Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s.split(".")[0].split("+")[0], fmt).date()
        except Exception:
            pass
    return None


def _parse_int(v):
    try:
        return int(float(str(v).replace(",","").replace("$","").strip() or 0))
    except Exception:
        return 0


def _parse_float(v):
    try:
        return float(str(v).replace(",","").replace("$","").strip() or 0)
    except Exception:
        return 0.0


def get_all_cs_data():
    """Get all customer success master rows flattened by email."""
    sb = _get_sb()
    if not sb:
        return []
    try:
        rows = sb.table("customer_success_master").select("*").execute().data or []
        # Flatten row_data
        flat = []
        for r in rows:
            rd = r.get("row_data", {}) or {}
            flat.append({**rd, "_tab": r.get("tab_name"), "_email": r.get("email")})
        return flat
    except Exception as e:
        print(f"Error: {e}")
        return []


def get_unique_customers():
    """Merge data from all tabs by email to get unified customer view."""
    rows = get_all_cs_data()
    by_email = defaultdict(dict)
    for r in rows:
        email = (r.get("Email") or r.get("E-mail") or r.get("_email") or "").strip().lower()
        if not email or "@" not in email:
            continue
        # Merge all fields, preserving non-empty values
        for k, v in r.items():
            if v and str(v).strip() and k not in by_email[email]:
                by_email[email][k] = v
            elif v and str(v).strip():
                # Keep latest value
                by_email[email][k] = v
    return list(by_email.values())


def churn_by_month():
    """Count churned customers per month based on subscription end / canceled status."""
    customers = get_unique_customers()
    by_month = Counter()
    for c in customers:
        status = str(c.get("Status", "")).lower()
        if "cancel" in status or "churn" in status or "ended" in status:
            end_date = (
                _parse_date(c.get("Current Period End Date"))
                or _parse_date(c.get("Canceled At"))
                or _parse_date(c.get("Subscription End Date"))
            )
            if end_date:
                by_month[end_date.strftime("%Y-%m")] += 1

    return {
        "by_month":      dict(sorted(by_month.items())),
        "total_churned": sum(by_month.values()),
    }


def plan_distribution():
    """Plan popularity + which plans churn the most."""
    customers = get_unique_customers()
    plans = Counter()
    plans_churned = Counter()
    plans_active = Counter()
    revenue_per_plan = defaultdict(float)

    for c in customers:
        plan = str(c.get("Plan", "")).strip() or "Unknown"
        status = str(c.get("Status", "")).lower()
        plans[plan] += 1
        if "cancel" in status or "churn" in status or "ended" in status:
            plans_churned[plan] += 1
        elif "active" in status or "trial" in status:
            plans_active[plan] += 1

    # Churn rate per plan
    churn_rate = {}
    for plan, total in plans.items():
        churned = plans_churned.get(plan, 0)
        churn_rate[plan] = round(churned / total * 100, 1) if total else 0

    return {
        "total_by_plan":      dict(plans.most_common()),
        "active_by_plan":     dict(plans_active.most_common()),
        "churned_by_plan":    dict(plans_churned.most_common()),
        "churn_rate_by_plan": churn_rate,
    }


def streaming_time_analysis():
    """Analyze streaming time data."""
    customers = get_unique_customers()
    stream_data = []
    for c in customers:
        # Look for streaming time fields across all tabs
        total_stream = 0
        for k, v in c.items():
            if "stream" in str(k).lower() and "time" in str(k).lower():
                total_stream += _parse_float(v)
        if total_stream > 0:
            stream_data.append({
                "email":         c.get("Email", c.get("_email", "")),
                "stream_time":   total_stream,
                "plan":          c.get("Plan", ""),
                "status":        c.get("Status", ""),
                "last_streamed": str(c.get("Last Streamed", "")),
            })
    stream_data.sort(key=lambda x: -x["stream_time"])
    total = sum(s["stream_time"] for s in stream_data)
    return {
        "total_stream_time":     round(total, 2),
        "users_with_streaming":  len(stream_data),
        "avg_stream_time":       round(total / len(stream_data), 2) if stream_data else 0,
        "top_streamers":         stream_data[:20],
        "bottom_streamers":      stream_data[-20:] if len(stream_data) > 20 else [],
    }


def recurring_payment_analysis():
    """Who has the most recurring payments, who has high payment count."""
    customers = get_unique_customers()
    paying = []
    for c in customers:
        count = _parse_int(c.get("Recurring Payment Count", c.get("Payment Count", 0)))
        if count > 0:
            paying.append({
                "email":            c.get("Email", c.get("_email", "")),
                "recurring_count":  count,
                "plan":             c.get("Plan", ""),
                "status":           c.get("Status", ""),
                "subscription_start": str(c.get("Subscription Create Date", "")),
                "current_period_start": str(c.get("Current Period Start Date", "")),
                "current_period_end":   str(c.get("Current Period End Date", "")),
            })
    paying.sort(key=lambda x: -x["recurring_count"])
    return {
        "total_paying_customers": len(paying),
        "total_recurring_payments": sum(p["recurring_count"] for p in paying),
        "avg_recurring":          round(sum(p["recurring_count"] for p in paying) / len(paying), 1) if paying else 0,
        "top_recurring":          paying[:30],
        "loyal_customers":        [p for p in paying if p["recurring_count"] >= 6],
        "one_time_customers":     [p for p in paying if p["recurring_count"] == 1],
    }


def perfect_fit_analysis():
    """Analyze 'Perfect Fit Customer' field + company size."""
    customers = get_unique_customers()
    fit_distribution = Counter()
    company_size = Counter()
    industries = Counter()

    for c in customers:
        # Perfect Fit
        for k, v in c.items():
            kl = str(k).lower()
            if "perfect fit" in kl or "fit customer" in kl:
                val = str(v).strip()
                if val:
                    fit_distribution[val] += 1
            if "company size" in kl or "employees" in kl:
                val = str(v).strip()
                if val:
                    company_size[val] += 1
            if "industry" in kl:
                val = str(v).strip()
                if val:
                    industries[val] += 1

    return {
        "perfect_fit":  dict(fit_distribution.most_common(20)),
        "company_size": dict(company_size.most_common(20)),
        "industries":   dict(industries.most_common(20)),
    }


def last_streamed_analysis():
    """Who last streamed and when - identify inactive paying customers."""
    customers = get_unique_customers()
    streamed = []
    for c in customers:
        ls = c.get("Last Streamed", "")
        if ls and str(ls).strip():
            d = _parse_date(ls)
            days_ago = (date.today() - d).days if d else None
            streamed.append({
                "email":       c.get("Email", c.get("_email", "")),
                "last_streamed": str(ls),
                "days_ago":    days_ago,
                "plan":        c.get("Plan", ""),
                "status":      c.get("Status", ""),
                "recurring":   _parse_int(c.get("Recurring Payment Count", 0)),
            })
    streamed.sort(key=lambda x: x["days_ago"] if x["days_ago"] is not None else 9999)

    # Identify danger zone: paying but not streaming recently
    danger = [s for s in streamed if s.get("days_ago") and s["days_ago"] > 30 and s["recurring"] > 0]
    return {
        "total_streamers":    len(streamed),
        "most_recent":        streamed[:20],
        "inactive_paying":    danger[:30],
        "never_streamed":     [s for s in streamed if not s.get("days_ago")][:30],
    }


def subscription_lifecycle():
    """Subscription dates analysis - start, end, current periods."""
    customers = get_unique_customers()
    new_this_month = []
    ending_soon = []
    expired = []
    today = date.today()
    month_start = today.replace(day=1)

    for c in customers:
        start = _parse_date(c.get("Subscription Create Date"))
        cur_end = _parse_date(c.get("Current Period End Date"))
        email = c.get("Email", c.get("_email", ""))

        if start and start >= month_start:
            new_this_month.append({
                "email": email,
                "start": str(start),
                "plan":  c.get("Plan", ""),
            })

        if cur_end:
            days_to_end = (cur_end - today).days
            if 0 <= days_to_end <= 30:
                ending_soon.append({
                    "email":       email,
                    "end_date":    str(cur_end),
                    "days_left":   days_to_end,
                    "plan":        c.get("Plan", ""),
                    "recurring":   _parse_int(c.get("Recurring Payment Count", 0)),
                })
            elif days_to_end < 0:
                expired.append({
                    "email":      email,
                    "expired_on": str(cur_end),
                    "days_ago":   abs(days_to_end),
                    "plan":       c.get("Plan", ""),
                })

    return {
        "new_this_month":  new_this_month,
        "ending_in_30d":   sorted(ending_soon, key=lambda x: x["days_left"]),
        "expired":         sorted(expired, key=lambda x: x["days_ago"])[:50],
        "count_new":       len(new_this_month),
        "count_ending":    len(ending_soon),
        "count_expired":   len(expired),
    }


def get_all_insights():
    """One-call get everything."""
    return {
        "churn":        churn_by_month(),
        "plans":        plan_distribution(),
        "streaming":    streaming_time_analysis(),
        "recurring":    recurring_payment_analysis(),
        "perfect_fit":  perfect_fit_analysis(),
        "last_streamed": last_streamed_analysis(),
        "subscriptions": subscription_lifecycle(),
        "generated_at": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    import json
    r = get_all_insights()
    print(json.dumps({k: {kk: vv if not isinstance(vv, list) else f"{len(vv)} items" for kk, vv in v.items()} if isinstance(v, dict) else v for k, v in r.items()}, indent=2, default=str))
