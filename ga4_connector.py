"""
GA4 Connector - Eagle 3D KPI System
Reads from google_creds.json locally OR Streamlit secrets on cloud
100% Free - No paid APIs needed
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    OrderBy,
)
from google.oauth2 import service_account

GA4_PROPERTY_ID = "374525971"
SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def _get_credentials():
    # Try Streamlit secrets first (cloud)
    try:
        import streamlit as st
        if "ga4_service_account" in st.secrets:
            d = dict(st.secrets["ga4_service_account"])
            if "private_key" in d:
                d["private_key"] = d["private_key"].replace("\\n", "\n")
            return service_account.Credentials.from_service_account_info(
                d, scopes=SCOPES
            )
    except Exception:
        pass

    # Fall back to local file
    try:
        return service_account.Credentials.from_service_account_file(
            "google_creds.json", scopes=SCOPES
        )
    except Exception as e:
        raise RuntimeError(f"No credentials found: {e}")


def _get_client():
    return BetaAnalyticsDataClient(credentials=_get_credentials())


def _run_report(body):
    try:
        client = _get_client()
        response = client.run_report(RunReportRequest(**body))
        rows = []
        dim_h = [h.name for h in response.dimension_headers]
        met_h = [h.name for h in response.metric_headers]
        for row in response.rows:
            r = {}
            for i, d in enumerate(row.dimension_values):
                r[dim_h[i]] = d.value
            for i, m in enumerate(row.metric_values):
                r[met_h[i]] = m.value
            rows.append(r)
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        for col in met_h:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        print(f"GA4 Error: {e}")
        return pd.DataFrame()


def fetch_utm_traffic(start_date, end_date):
    return _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [
            Dimension(name="date"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="sessionDefaultChannelGroup"),
        ],
        "metrics": [
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="conversions"),
        ],
        "order_bys": [OrderBy(
            dimension=OrderBy.DimensionOrderBy(dimension_name="date"),
            desc=True
        )],
        "limit": 10000,
    })


def fetch_daily_traffic_summary(start_date, end_date):
    return _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [
            Dimension(name="date"),
            Dimension(name="sessionDefaultChannelGroup"),
        ],
        "metrics": [
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="conversions"),
        ],
        "order_bys": [OrderBy(
            dimension=OrderBy.DimensionOrderBy(dimension_name="date")
        )],
        "limit": 10000,
    })


def fetch_page_performance(start_date, end_date):
    return _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [
            Dimension(name="date"),
            Dimension(name="pagePath"),
            Dimension(name="pageTitle"),
        ],
        "metrics": [
            Metric(name="screenPageViews"),
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="engagementRate"),
            Metric(name="conversions"),
        ],
        "order_bys": [OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"),
            desc=True
        )],
        "limit": 5000,
    })


def fetch_event_performance(start_date, end_date):
    return _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [
            Dimension(name="date"),
            Dimension(name="eventName"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
        ],
        "metrics": [
            Metric(name="eventCount"),
            Metric(name="totalUsers"),
            Metric(name="conversions"),
        ],
        "order_bys": [OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="eventCount"),
            desc=True
        )],
        "limit": 5000,
    })


def fetch_geo_traffic(start_date, end_date):
    return _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [
            Dimension(name="country"),
            Dimension(name="city"),
            Dimension(name="sessionSource"),
        ],
        "metrics": [
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="conversions"),
        ],
        "order_bys": [OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="sessions"),
            desc=True
        )],
        "limit": 2000,
    })


def fetch_device_traffic(start_date, end_date):
    return _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [
            Dimension(name="deviceCategory"),
            Dimension(name="operatingSystem"),
            Dimension(name="sessionSource"),
        ],
        "metrics": [
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="conversions"),
        ],
        "order_bys": [OrderBy(
            metric=OrderBy.MetricOrderBy(metric_name="sessions"),
            desc=True
        )],
        "limit": 500,
    })


def fetch_signup_source_correlation(start_date, end_date):
    return _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [
            Dimension(name="date"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="firstUserSource"),
            Dimension(name="firstUserMedium"),
            Dimension(name="firstUserCampaignName"),
        ],
        "metrics": [
            Metric(name="conversions"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
            Metric(name="sessions"),
        ],
        "order_bys": [
            OrderBy(
                dimension=OrderBy.DimensionOrderBy(dimension_name="date"),
                desc=True
            ),
            OrderBy(
                metric=OrderBy.MetricOrderBy(metric_name="conversions"),
                desc=True
            ),
        ],
        "limit": 5000,
    })


def fetch_event_attribution(start_date, end_date, event_name=None):
    """
    Fetch event data with traffic attribution (9 dimensions max - GA4 limit).

    Returns:
    - eventName, eventCount, totalUsers
    - sessionSource, sessionMedium, sessionCampaignName (last-touch)
    - firstUserSource, firstUserMedium (first-touch — fewer to fit limit)
    - country, deviceCategory

    Use fetch_event_attribution_extended() for landing page + campaign first-touch
    """
    from google.analytics.data_v1beta.types import FilterExpression, Filter

    dimensions = [
        Dimension(name="date"),
        Dimension(name="eventName"),
        Dimension(name="sessionSource"),
        Dimension(name="sessionMedium"),
        Dimension(name="sessionCampaignName"),
        Dimension(name="firstUserSource"),
        Dimension(name="firstUserMedium"),
        Dimension(name="country"),
        Dimension(name="deviceCategory"),
    ]

    metrics = [
        Metric(name="eventCount"),
        Metric(name="totalUsers"),
        Metric(name="sessions"),
        Metric(name="conversions"),
    ]

    request_body = {
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": dimensions,
        "metrics": metrics,
        "order_bys": [
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True),
        ],
        "limit": 10000,
    }

    if event_name:
        request_body["dimension_filter"] = FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(
                    value=event_name,
                    match_type=Filter.StringFilter.MatchType.EXACT,
                ),
            )
        )

    return _run_report(request_body)


def fetch_event_attribution_extended(start_date, end_date, event_name=None):
    """
    Extended view with landing page details (9 dimensions max).
    Use this as supplementary query for landing pages + campaigns.
    """
    from google.analytics.data_v1beta.types import FilterExpression, Filter

    dimensions = [
        Dimension(name="date"),
        Dimension(name="eventName"),
        Dimension(name="sessionSource"),
        Dimension(name="sessionCampaignName"),
        Dimension(name="firstUserSource"),
        Dimension(name="firstUserCampaignName"),
        Dimension(name="landingPage"),
        Dimension(name="country"),
        Dimension(name="deviceCategory"),
    ]

    metrics = [
        Metric(name="eventCount"),
        Metric(name="totalUsers"),
        Metric(name="sessions"),
    ]

    request_body = {
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": dimensions,
        "metrics": metrics,
        "order_bys": [
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True),
        ],
        "limit": 10000,
    }

    if event_name:
        request_body["dimension_filter"] = FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(
                    value=event_name,
                    match_type=Filter.StringFilter.MatchType.EXACT,
                ),
            )
        )

    return _run_report(request_body)


def fetch_available_events(start_date, end_date):
    """Get list of all unique events in the period - for dropdown selector."""
    df = _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [Dimension(name="eventName")],
        "metrics": [Metric(name="eventCount"), Metric(name="totalUsers")],
        "order_bys": [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)],
        "limit": 200,
    })
    return df


def fetch_available_events(start_date, end_date):
    """Get list of all unique events in the period - for dropdown selector."""
    df = _run_report({
        "property": f"properties/{GA4_PROPERTY_ID}",
        "date_ranges": [DateRange(start_date=start_date, end_date=end_date)],
        "dimensions": [Dimension(name="eventName")],
        "metrics": [Metric(name="eventCount"), Metric(name="totalUsers")],
        "order_bys": [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)],
        "limit": 200,
    })
    return df

