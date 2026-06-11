"""
Cross-Platform Correlation Engine — Eagle 3D KPI System
=======================================================
Correlates data across all platforms:
  - KPI (sign-ups, uploads, paid customers)
  - GA4 (web traffic, sources, user behavior)
  - YouTube (views, subscribers, watch time)
  - LinkedIn (followers, engagement, posts)
  - Stripe (revenue, payments)

Provides:
  - Unified timeline view
  - Correlation analysis (which platform drives what?)
  - Attribution modeling
  - Cross-platform funnel analysis
  - ROI per channel
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)


def _safe_corr(a: pd.Series, b: pd.Series) -> float:
    """Safely compute correlation, return 0 if impossible."""
    try:
        if len(a) < 3 or len(b) < 3:
            return 0.0
        r = a.corr(b)
        return round(float(r), 3) if not np.isnan(r) else 0.0
    except Exception:
        return 0.0


def _normalize_date_series(df: pd.DataFrame, date_col: str, value_col: str) -> pd.Series:
    """Normalize a date-indexed series for correlation."""
    if df.empty or date_col not in df.columns or value_col not in df.columns:
        return pd.Series(dtype=float)
    s = df[[date_col, value_col]].copy()
    s[date_col] = pd.to_datetime(s[date_col], errors="coerce")
    s[value_col] = pd.to_numeric(s[value_col], errors="coerce").fillna(0)
    s = s.dropna(subset=[date_col]).set_index(date_col)[value_col]
    return s


def build_unified_timeline(
    kpi_df: pd.DataFrame,
    ga4_df: pd.DataFrame = None,
    youtube_daily: pd.DataFrame = None,
    linkedin_daily: pd.DataFrame = None,
    start_date: str = "",
    end_date: str = "",
) -> pd.DataFrame:
    """
    Build a unified timeline DataFrame with all platform metrics aligned by date.
    
    Each input should have a 'date' or 'day' column and metric columns.
    Returns DataFrame indexed by date with prefixed columns.
    """
    # Determine date range
    if start_date and end_date:
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
    else:
        dates = pd.date_range(
            start=datetime.now() - timedelta(days=90),
            end=datetime.now(),
            freq="D",
        )

    unified = pd.DataFrame({"date": dates})
    unified["date"] = pd.to_datetime(unified["date"])

    # ── KPI data ──
    if not kpi_df.empty:
        kpi = kpi_df.copy()
        dc = next((c for c in kpi.columns if "date" in c.lower()), None)
        if dc:
            kpi[dc] = pd.to_datetime(kpi[dc], errors="coerce")
            for col in kpi.columns:
                if col != dc and kpi[col].dtype in [np.float64, np.int64, float, int]:
                    kpi = kpi.rename(columns={col: f"kpi_{col}"})
            kpi = kpi.rename(columns={dc: "date"})
            unified = unified.merge(kpi, on="date", how="left")

    # ── GA4 data ──
    if ga4_df is not None and not ga4_df.empty:
        ga4 = ga4_df.copy()
        dc = next((c for c in ga4.columns if "date" in c.lower() or c == "day"), None)
        if dc:
            ga4[dc] = pd.to_datetime(ga4[dc], errors="coerce")
            numeric_cols = [c for c in ga4.columns if c != dc and ga4[c].dtype in [np.float64, np.int64, float, int]]
            rename_map = {c: f"ga4_{c}" for c in numeric_cols}
            ga4 = ga4.rename(columns={**rename_map, dc: "date"})
            if len(ga4.columns) > 1:
                unified = unified.merge(ga4, on="date", how="left")

    # ── YouTube data ──
    if youtube_daily is not None and not youtube_daily.empty:
        yt = youtube_daily.copy()
        dc = next((c for c in yt.columns if c == "day" or "date" in c.lower()), None)
        if dc:
            yt[dc] = pd.to_datetime(yt[dc], errors="coerce")
            numeric_cols = [c for c in yt.columns if c != dc]
            rename_map = {c: f"yt_{c}" for c in numeric_cols}
            yt = yt.rename(columns={**rename_map, dc: "date"})
            unified = unified.merge(yt, on="date", how="left")

    # ── LinkedIn data ──
    if linkedin_daily is not None and not linkedin_daily.empty:
        li = linkedin_daily.copy()
        dc = next((c for c in li.columns if "date" in c.lower() or c == "day"), None)
        if dc:
            li[dc] = pd.to_datetime(li[dc], errors="coerce")
            numeric_cols = [c for c in li.columns if c != dc and li[c].dtype in [np.float64, np.int64, float, int]]
            rename_map = {c: f"li_{c}" for c in numeric_cols}
            li = li.rename(columns={**rename_map, dc: "date"})
            unified = unified.merge(li, on="date", how="left")

    # Fill NaN with 0 for numeric columns
    for col in unified.columns:
        if col != "date":
            unified[col] = unified[col].fillna(0)

    return unified


def compute_correlations(unified: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute cross-platform correlations from unified timeline.
    Returns matrix of correlations between all metric pairs.
    """
    if unified.empty or len(unified) < 3:
        return {"error": "Need at least 3 days of data for correlation"}

    numeric_cols = [c for c in unified.columns if c != "date" and unified[c].dtype in [np.float64, np.int64, float, int]]
    if len(numeric_cols) < 2:
        return {"error": "Need at least 2 metrics for correlation"}

    # Compute correlation matrix
    corr_matrix = unified[numeric_cols].corr().round(3)

    # Find strongest correlations (excluding self-correlation)
    strong_corrs = []
    for i, col1 in enumerate(numeric_cols):
        for j, col2 in enumerate(numeric_cols):
            if i < j:
                r = corr_matrix.loc[col1, col2]
                if abs(r) > 0.3:  # Only meaningful correlations
                    strong_corrs.append({
                        "metric_a": col1,
                        "metric_b": col2,
                        "correlation": r,
                        "strength": "strong" if abs(r) > 0.7 else "moderate",
                        "direction": "positive" if r > 0 else "negative",
                    })

    strong_corrs.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    return {
        "matrix": corr_matrix.to_dict(),
        "strong_correlations": strong_corrs[:20],
        "metric_count": len(numeric_cols),
        "day_count": len(unified),
    }


def compute_attribution(unified: pd.DataFrame) -> Dict[str, Any]:
    """
    Simple attribution model: which platform metrics best predict KPI conversions.
    Uses lagged correlations to detect leading indicators.
    """
    if unified.empty or len(unified) < 7:
        return {"error": "Need at least 7 days of data"}

    kpi_cols = [c for c in unified.columns if c.startswith("kpi_")]
    traffic_cols = [c for c in unified.columns if c.startswith(("ga4_", "yt_", "li_"))]

    if not kpi_cols or not traffic_cols:
        return {"error": "Need both KPI and traffic metrics"}

    attributions = {}

    for kpi_col in kpi_cols:
        kpi_series = unified[kpi_col]
        if kpi_series.sum() == 0:
            continue

        predictors = []
        for traffic_col in traffic_cols:
            traffic_series = unified[traffic_col]
            if traffic_series.sum() == 0:
                continue

            # Same-day correlation
            r0 = _safe_corr(kpi_series, traffic_series)

            # Lagged correlations (1-7 days)
            best_lag = 0
            best_r = abs(r0)
            for lag in range(1, 8):
                if len(kpi_series) > lag:
                    r_lag = _safe_corr(kpi_series.iloc[lag:].reset_index(drop=True),
                                       traffic_series.iloc[:-lag].reset_index(drop=True))
                    if abs(r_lag) > best_r:
                        best_r = abs(r_lag)
                        best_lag = lag

            if best_r > 0.2:
                # Determine source platform
                if traffic_col.startswith("ga4_"):
                    platform = "GA4/Web"
                elif traffic_col.startswith("yt_"):
                    platform = "YouTube"
                elif traffic_col.startswith("li_"):
                    platform = "LinkedIn"
                else:
                    platform = "Other"

                predictors.append({
                    "metric": traffic_col,
                    "platform": platform,
                    "correlation": round(best_r, 3),
                    "lag_days": best_lag,
                    "interpretation": f"{'Leads' if best_lag > 0 else 'Same-day'}: {traffic_col.replace('_', ' ').title()} → {best_lag}d later → {kpi_col.replace('_', ' ').title()}",
                })

        predictors.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        attributions[kpi_col] = predictors[:10]

    return attributions


def compute_cross_platform_funnel(
    kpi_df: pd.DataFrame,
    ga4_sessions: int = 0,
    yt_views: int = 0,
    li_impressions: int = 0,
    period_label: str = "",
) -> Dict[str, Any]:
    """
    Build cross-platform funnel:
    Awareness (YT views + LI impressions) → Interest (GA4 sessions) → 
    Consideration (sign-ups) → Action (uploads) → Revenue (paid)
    """
    total_signups = int(kpi_df["signups"].sum()) if not kpi_df.empty and "signups" in kpi_df.columns else 0
    total_uploads = int(kpi_df["first_uploads"].sum()) if not kpi_df.empty and "first_uploads" in kpi_df.columns else 0
    total_paid = int(kpi_df["paid_customers"].sum()) if not kpi_df.empty and "paid_customers" in kpi_df.columns else 0

    awareness = yt_views + li_impressions

    funnel = {
        "period": period_label or "Selected Period",
        "stages": [
            {"stage": "Awareness", "value": awareness, "icon": "👁️",
             "detail": f"YouTube: {yt_views:,} views + LinkedIn: {li_impressions:,} impressions"},
            {"stage": "Interest", "value": ga4_sessions, "icon": "🌐",
             "detail": f"GA4: {ga4_sessions:,} web sessions"},
            {"stage": "Consideration", "value": total_signups, "icon": "👥",
             "detail": f"KPI: {total_signups:,} sign-ups"},
            {"stage": "Action", "value": total_uploads, "icon": "📤",
             "detail": f"KPI: {total_uploads:,} first uploads"},
            {"stage": "Revenue", "value": total_paid, "icon": "💳",
             "detail": f"KPI: {total_paid:,} paid customers"},
        ],
        "conversion_rates": {},
    }

    # Compute conversion rates between stages
    stages = funnel["stages"]
    for i in range(1, len(stages)):
        prev = stages[i - 1]["value"]
        curr = stages[i]["value"]
        rate = round(curr / prev * 100, 2) if prev > 0 else 0
        funnel["conversion_rates"][f"{stages[i-1]['stage']}_to_{stages[i]['stage']}"] = rate

    # Overall conversion
    if awareness > 0:
        funnel["conversion_rates"]["awareness_to_revenue"] = round(total_paid / awareness * 100, 4)
    if ga4_sessions > 0:
        funnel["conversion_rates"]["interest_to_revenue"] = round(total_paid / ga4_sessions * 100, 4)

    return funnel


def compute_platform_comparison(
    ga4_data: Dict = None,
    youtube_data: Dict = None,
    linkedin_data: Dict = None,
    period_label: str = "",
) -> Dict[str, Any]:
    """
    Compare metrics across platforms side-by-side.
    """
    comparison = {
        "period": period_label,
        "platforms": {},
    }

    if ga4_data:
        comparison["platforms"]["GA4/Web"] = {
            "sessions": ga4_data.get("sessions", 0),
            "users": ga4_data.get("users", 0),
            "page_views": ga4_data.get("page_views", 0),
            "avg_session_duration": ga4_data.get("avg_session_duration", 0),
            "bounce_rate": ga4_data.get("bounce_rate", 0),
        }

    if youtube_data:
        comparison["platforms"]["YouTube"] = {
            "views": youtube_data.get("views", 0),
            "subscribers": youtube_data.get("subscribers", 0),
            "watch_time_hours": round(youtube_data.get("watch_time_minutes", 0) / 60, 1),
            "engagement_rate": youtube_data.get("engagement_rate", 0),
            "videos_published": youtube_data.get("video_count", 0),
        }

    if linkedin_data:
        comparison["platforms"]["LinkedIn"] = {
            "followers": linkedin_data.get("followers", 0),
            "impressions": linkedin_data.get("impressions", 0),
            "engagement_rate": linkedin_data.get("engagement_rate", 0),
            "posts": linkedin_data.get("post_count", 0),
        }

    return comparison


def compute_growth_analysis(
    unified: pd.DataFrame,
    lookback_days: int = 28,
) -> Dict[str, Any]:
    """
    Analyze growth trends across platforms.
    Compare recent period vs previous period.
    """
    if unified.empty:
        return {"error": "No data available"}

    # Split into current and previous periods
    dates = unified["date"]
    if len(dates) < lookback_days * 2:
        lookback_days = len(dates) // 2

    if lookback_days < 3:
        return {"error": "Not enough data for growth analysis"}

    current = unified.tail(lookback_days)
    previous = unified.iloc[-(lookback_days * 2):-lookback_days]

    growth = {}
    numeric_cols = [c for c in unified.columns if c != "date"]

    for col in numeric_cols:
        curr_sum = float(current[col].sum())
        prev_sum = float(previous[col].sum())

        change = curr_sum - prev_sum
        pct_change = round(change / prev_sum * 100, 1) if prev_sum > 0 else 0

        # Determine trend
        if abs(pct_change) < 5:
            trend = "stable"
        elif pct_change > 0:
            trend = "growing"
        else:
            trend = "declining"

        growth[col] = {
            "current_period": round(curr_sum, 1),
            "previous_period": round(prev_sum, 1),
            "change": round(change, 1),
            "pct_change": pct_change,
            "trend": trend,
        }

    return {
        "lookback_days": lookback_days,
        "metrics": growth,
    }


def generate_cross_insights(
    correlations: Dict,
    attributions: Dict,
    growth: Dict,
    funnel: Dict,
) -> List[str]:
    """
    Generate AI-like insights from cross-platform analysis.
    Returns list of insight strings.
    """
    insights = []

    # Correlation insights
    strong = correlations.get("strong_correlations", [])
    if strong:
        for corr in strong[:3]:
            direction = "increases" if corr["correlation"] > 0 else "decreases"
            insights.append(
                f"🔗 <b>Correlation ({corr['strength']})</b>: When <code>{corr['metric_a']}</code> goes up, "
                f"<code>{corr['metric_b']}</code> also {direction} (r={corr['correlation']})"
            )

    # Attribution insights
    for kpi, predictors in attributions.items():
        if isinstance(predictors, list) and predictors:
            top = predictors[0]
            if top["lag_days"] > 0:
                insights.append(
                    f"🎯 <b>Attribution</b>: <code>{top['metric']}</code> ({top['platform']}) "
                    f"leads <code>{kpi}</code> by <b>{top['lag_days']} days</b> (r={top['correlation']})"
                )
            else:
                insights.append(
                    f"🎯 <b>Attribution</b>: <code>{top['metric']}</code> ({top['platform']}) "
                    f"directly correlates with <code>{kpi}</code> (r={top['correlation']})"
                )

    # Growth insights
    metrics = growth.get("metrics", {})
    for metric, data in metrics.items():
        if data["trend"] == "growing" and data["pct_change"] > 20:
            insights.append(
                f"📈 <b>Growth</b>: <code>{metric}</code> is up <b>{data['pct_change']}%</b> vs previous period"
            )
        elif data["trend"] == "declining" and data["pct_change"] < -20:
            insights.append(
                f"📉 <b>Alert</b>: <code>{metric}</code> is down <b>{abs(data['pct_change'])}%</b> vs previous period"
            )

    # Funnel insights
    conv_rates = funnel.get("conversion_rates", {})
    awareness_to_revenue = conv_rates.get("awareness_to_revenue", 0)
    if awareness_to_revenue > 0:
        insights.append(
            f"🔄 <b>Full Funnel</b>: Awareness → Revenue conversion rate is <b>{awareness_to_revenue}%</b>"
        )

    interest_to_revenue = conv_rates.get("interest_to_revenue", 0)
    if interest_to_revenue > 0:
        insights.append(
            f"💰 <b>Web Conversion</b>: Web sessions → Paid customer rate is <b>{interest_to_revenue}%</b>"
        )

    if not insights:
        insights.append("📊 Cross-platform analysis requires data from multiple sources. Configure YouTube, LinkedIn, and GA4 connectors for deeper insights.")

    return insights
