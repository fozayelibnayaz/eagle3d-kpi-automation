#!/usr/bin/env python3
# SUPABASE ADAPTER
# Primary data store adapter - replaces Google Sheets for production
# Falls back to local JSON cache if Supabase not configured

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Optional

DATA_DIR = Path("data_output")
log = lambda m: print(f"[{datetime.now().strftime('%H:%M:%S')}] [Supabase] {m}", flush=True)


def _get_client():
    url = os.environ.get("SUPABASE_URL","").strip()
    key = os.environ.get("SUPABASE_SERVICE_KEY","").strip()

    if not url or not key:
        try:
            import streamlit as st
            url = str(st.secrets.get("SUPABASE_URL","")).strip()
            key = str(st.secrets.get("SUPABASE_SERVICE_KEY","")).strip()
        except Exception:
            pass

    if not url or not key:
        return None

    try:
        from supabase import create_client
        return create_client(url, key)
    except ImportError:
        log("supabase package not installed. Run: pip install supabase")
        return None
    except Exception as e:
        log(f"Supabase client error: {e}")
        return None


def is_configured() -> bool:
    return _get_client() is not None


def read_table(table: str, filters: dict = None) -> list:
    sb = _get_client()
    if not sb:
        log(f"Supabase not configured — reading from local cache for {table}")
        return _read_local_cache(table)

    try:
        q = sb.table(table).select("*")
        if filters:
            for k, v in filters.items():
                q = q.eq(k, v)
        resp = q.execute()
        return resp.data or []
    except Exception as e:
        log(f"Read error [{table}]: {e}")
        return _read_local_cache(table)


def upsert_rows(table: str, rows: list, on_conflict: str = "id") -> bool:
    sb = _get_client()
    if not sb:
        log(f"Supabase not configured — saving to local cache for {table}")
        _save_local_cache(table, rows)
        return False

    try:
        if not rows:
            return True
        # Batch upsert in chunks of 100
        for i in range(0, len(rows), 100):
            chunk = rows[i:i+100]
            sb.table(table).upsert(chunk, on_conflict=on_conflict).execute()
        log(f"Upserted {len(rows)} rows to {table}")
        return True
    except Exception as e:
        log(f"Upsert error [{table}]: {e}")
        _save_local_cache(table, rows)
        return False


def get_analytics_cache(source: str, period_type: str = "365days") -> Optional[dict]:
    sb = _get_client()
    if not sb:
        cache_file = DATA_DIR / f"{source}_analytics_full.json"
        if cache_file.exists():
            return json.loads(cache_file.read_text())
        return None

    try:
        resp = sb.table("analytics_cache") \
            .select("*") \
            .eq("source", source) \
            .eq("period_type", period_type) \
            .order("fetched_at", desc=True) \
            .limit(1) \
            .execute()
        if resp.data:
            return resp.data[0].get("data", {})
        return None
    except Exception as e:
        log(f"Cache read error: {e}")
        return None


def save_analytics_cache(source: str, data: dict, period_type: str = "365days",
                          period_start: str = "", period_end: str = "") -> bool:
    sb = _get_client()
    cache_file = DATA_DIR / f"{source}_analytics_full.json"
    cache_file.write_text(json.dumps(data, indent=2))

    if not sb:
        return False

    try:
        sb.table("analytics_cache").upsert({
            "source":       source,
            "metric_date":  datetime.now().strftime("%Y-%m-%d"),
            "period_type":  period_type,
            "period_start": period_start,
            "period_end":   period_end,
            "data":         data,
            "fetched_at":   datetime.utcnow().isoformat(),
            "is_valid":     True,
        }, on_conflict="source,metric_date").execute()
        log(f"Saved analytics cache: {source}")
        return True
    except Exception as e:
        log(f"Save cache error: {e}")
        return False


def migrate_daily_counts_to_supabase() -> bool:
    daily_file = DATA_DIR / "daily_counts.json"
    if not daily_file.exists():
        log("daily_counts.json not found")
        return False

    daily = json.loads(daily_file.read_text())
    rows  = []
    for r in daily:
        date = r.get("Date","")
        if not date:
            continue
        rows.append({
            "date":             date,
            "year":             r.get("Year", date[:4]),
            "month":            r.get("Month", date[:7]),
            "signups_accepted": int(r.get("SignUps_Accepted", 0) or 0),
            "uploads_accepted": int(r.get("FirstUploads_Accepted", 0) or 0),
            "paid_accepted":    int(r.get("PaidSubscribers_Accepted", 0) or 0),
            "signup_details":   r.get("SignUp_Details",""),
            "upload_details":   r.get("Upload_Details",""),
            "paid_details":     r.get("Paid_Details",""),
            "last_updated":     r.get("LastUpdated", datetime.utcnow().isoformat()),
        })

    return upsert_rows("daily_kpis", rows, on_conflict="date")


def migrate_overrides_to_supabase() -> bool:
    ov_file  = DATA_DIR / "manual_overrides.json"
    log_file = DATA_DIR / "override_audit_log.json"

    if ov_file.exists():
        overrides = json.loads(ov_file.read_text())
        rows = []
        for email, ov in overrides.items():
            rows.append({
                "email_normalized":   email,
                "action":             ov.get("action",""),
                "final_status":       ov.get("final_status",""),
                "category":           ov.get("category",""),
                "reason":             ov.get("reason",""),
                "override_user":      ov.get("override_user",""),
                "override_timestamp": ov.get("override_timestamp",""),
                "source_tab":         ov.get("source_tab",""),
                "is_active":          True,
                "extra_data":         ov.get("extra",{}),
            })
        if rows:
            upsert_rows("manual_overrides", rows, on_conflict="email_normalized")
            log(f"Migrated {len(rows)} overrides to Supabase")

    if log_file.exists():
        audit_log = json.loads(log_file.read_text())
        rows = []
        for entry in audit_log:
            rows.append({
                "timestamp":        entry.get("timestamp",""),
                "email_normalized": entry.get("email",""),
                "action":           entry.get("action",""),
                "previous_action":  entry.get("previous_action",""),
                "previous_status":  entry.get("previous_status",""),
                "new_status":       entry.get("new_status",""),
                "reason":           entry.get("reason",""),
                "override_user":    entry.get("user",""),
                "source_tab":       entry.get("source_tab",""),
                "change_type":      entry.get("change_type",""),
            })
        if rows:
            upsert_rows("override_audit_log", rows, on_conflict="id")
            log(f"Migrated {len(rows)} audit log entries to Supabase")

    return True


def _read_local_cache(table: str) -> list:
    cache_map = {
        "daily_kpis":       DATA_DIR / "daily_counts.json",
        "analytics_cache":  DATA_DIR / "linkedin_analytics_full.json",
    }
    cache_file = cache_map.get(table)
    if cache_file and cache_file.exists():
        try:
            data = json.loads(cache_file.read_text())
            return data if isinstance(data, list) else [data]
        except Exception:
            pass
    return []


def _save_local_cache(table: str, rows: list):
    cache_file = DATA_DIR / f"{table}_local_cache.json"
    cache_file.write_text(json.dumps(rows, indent=2))


def get_supabase_status() -> dict:
    sb = _get_client()
    if not sb:
        return {
            "connected": False,
            "message": "SUPABASE_URL and SUPABASE_SERVICE_KEY not set in environment or secrets",
            "setup_steps": [
                "1. Go to https://supabase.com and create project: eagle3d-kpi",
                "2. Run data_output/migration/schema.sql in SQL Editor",
                "3. Add SUPABASE_URL to .streamlit/secrets.toml",
                "4. Add SUPABASE_SERVICE_KEY to .streamlit/secrets.toml",
                "5. Run: python3 supabase_adapter.py to migrate existing data",
            ]
        }
    try:
        resp = sb.table("daily_kpis").select("count", count="exact").execute()
        return {
            "connected": True,
            "message": "Supabase connected",
            "daily_kpis_count": resp.count or 0,
        }
    except Exception as e:
        return {"connected": False, "message": f"Connected but error: {e}"}


if __name__ == "__main__":
    status = get_supabase_status()
    print(json.dumps(status, indent=2))
    if status["connected"]:
        print("Migrating daily_counts.json to Supabase...")
        migrate_daily_counts_to_supabase()
        print("Migrating overrides to Supabase...")
        migrate_overrides_to_supabase()
        print("Migration complete")
    else:
        print("Supabase not configured - see setup steps above")
        print("Schema file: data_output/migration/schema.sql")
