#!/usr/bin/env python3
"""
DATA GAP FILLER
Detects missing dates in Supabase and backfills from all available sources.
Ensures NO DATA IS EVER LOST even if pipeline fails for days.

Runs at start of every pipeline execution.
Checks: signups, uploads, payments, daily_kpis
Sources: Google Sheets (CS sheet), Supabase existing, KPI Dashboard live
"""

import os
import re
from datetime import datetime, date, timedelta
from collections import defaultdict


def log(m):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [GapFiller] {m}", flush=True)


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


def _fetch_all(sb, table, cols):
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
        except Exception:
            break
    return rows


def detect_gaps():
    """Detect missing dates in daily_kpis vs what should exist."""
    sb = _get_sb()
    if not sb:
        log("Supabase not configured")
        return {"gaps": [], "error": "Supabase not configured"}

    log("Detecting date gaps...")

    # Get all dates in daily_kpis
    rows = _fetch_all(sb, "daily_kpis", "date")
    existing_dates = set(str(r.get("date", ""))[:10] for r in rows if r.get("date"))
    log(f"  daily_kpis has {len(existing_dates)} dates")

    if not existing_dates:
        return {"gaps": [], "first_date": None, "last_date": None}

    first = min(existing_dates)
    last = max(existing_dates)
    today = date.today().isoformat()

    # Expected dates: from first to today (every day)
    expected = set()
    d = date.fromisoformat(first)
    end_d = date.today()
    while d <= end_d:
        expected.add(d.isoformat())
        d += timedelta(days=1)

    gaps = sorted(expected - existing_dates)
    log(f"  Expected {len(expected)} dates, found {len(existing_dates)}, gaps: {len(gaps)}")

    if gaps:
        log(f"  Gap dates: {gaps[:20]}{'...' if len(gaps) > 20 else ''}")

    return {
        "gaps":       gaps,
        "first_date": first,
        "last_date":  last,
        "today":      today,
        "expected":   len(expected),
        "existing":   len(existing_dates),
        "gap_count":  len(gaps),
    }


def fill_gaps_from_source_tables():
    """
    Rebuild daily_kpis for ALL dates from signups/uploads/payments tables.
    This catches any gaps because source tables have the raw data.
    """
    sb = _get_sb()
    if not sb:
        return {"error": "Supabase not configured"}

    log("Rebuilding daily_kpis from source tables (full rebuild)...")

    sign_rows = _fetch_all(sb, "signups", "signup_date,email")
    upl_rows = _fetch_all(sb, "uploads", "upload_date,email")
    pay_rows = _fetch_all(sb, "payments", "first_payment_date,email")

    log(f"  Source: {len(sign_rows)} signups, {len(upl_rows)} uploads, {len(pay_rows)} payments")

    # Filter ACCEPTED only
    sign_acc = [r for r in sign_rows if str(r.get("final_status", "")).upper() == "ACCEPTED"] if "final_status" in (sign_rows[0] if sign_rows else {}) else sign_rows
    # Actually need to fetch with final_status
    sign_acc = _fetch_all_accepted(sb, "signups", "signup_date,email,final_status")
    upl_acc = _fetch_all_accepted(sb, "uploads", "upload_date,email,final_status")
    pay_acc = _fetch_all_accepted(sb, "payments", "first_payment_date,email,final_status")

    log(f"  Accepted: {len(sign_acc)} signups, {len(upl_acc)} uploads, {len(pay_acc)} payments")

    daily = defaultdict(lambda: {"s": 0, "u": 0, "p": 0, "se": [], "ue": [], "pe": []})
    for r in sign_acc:
        d = str(r.get("signup_date") or "")[:10]
        if d:
            daily[d]["s"] += 1
            daily[d]["se"].append(r.get("email", ""))
    for r in upl_acc:
        d = str(r.get("upload_date") or "")[:10]
        if d:
            daily[d]["u"] += 1
            daily[d]["ue"].append(r.get("email", ""))
    for r in pay_acc:
        d = str(r.get("first_payment_date") or "")[:10]
        if d:
            daily[d]["p"] += 1
            daily[d]["pe"].append(r.get("email", ""))

    # Also fill in zero-data dates between first and today
    if daily:
        first_d = date.fromisoformat(min(daily.keys()))
        today_d = date.today()
        d = first_d
        while d <= today_d:
            ds = d.isoformat()
            if ds not in daily:
                daily[ds] = {"s": 0, "u": 0, "p": 0, "se": [], "ue": [], "pe": []}
            d += timedelta(days=1)

    upsert = []
    for d, v in daily.items():
        upsert.append({
            "date":             d,
            "year":             str(d[:4]),
            "month":            str(d[:7]),
            "signups_accepted": v["s"],
            "uploads_accepted": v["u"],
            "paid_accepted":    v["p"],
            "signup_details":   "; ".join(sorted(set(v["se"])))[:5000],
            "upload_details":   "; ".join(sorted(set(v["ue"])))[:5000],
            "paid_details":     "; ".join(sorted(set(v["pe"])))[:5000],
            "last_updated":     datetime.utcnow().isoformat(),
        })

    upsert.sort(key=lambda x: x["date"])
    log(f"  Upserting {len(upsert)} daily_kpis rows...")
    for i in range(0, len(upsert), 100):
        sb.table("daily_kpis").upsert(upsert[i:i + 100], on_conflict="date").execute()

    log(f"  daily_kpis rebuilt: {len(upsert)} days (no gaps)")
    return {"success": True, "days": len(upsert)}


def _fetch_all_accepted(sb, table, cols):
    rows = []
    offset = 0
    while True:
        try:
            r = sb.table(table).select(cols).eq("final_status", "ACCEPTED").range(offset, offset + 999).execute()
            batch = r.data or []
            rows.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
        except Exception:
            break
    return rows


def sync_new_data_from_sheets():
    """
    Pull latest data from Google Sheets (which the KPI scraper writes to)
    and push to Supabase. This catches any rows added by the scraper
    that haven't been synced yet.
    """
    log("Syncing latest data from scraper output to Supabase...")

    # The sheets_writer is now a Supabase shim, so this sync is handled
    # automatically when the scraper writes via write_tab_data.
    # But if the scraper wrote to Sheets directly (before our shim),
    # we need to pull from Sheets.

    # Check if there's a google_creds.json for direct sheet read
    if not os.path.exists("google_creds.json"):
        log("  No google_creds.json - relying on scraper's Supabase writes")
        return {"synced": 0}

    try:
        import gspread
        from google.oauth2 import service_account

        SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly",
                   "https://www.googleapis.com/auth/drive.readonly"]
        creds = service_account.Credentials.from_service_account_file("google_creds.json", scopes=SCOPES)
        gc = gspread.authorize(creds)

        sheet_url = os.environ.get("MASTER_SHEET_URL", "")
        if not sheet_url:
            log("  No MASTER_SHEET_URL - skipping Sheets sync")
            return {"synced": 0}

        sheet = gc.open_by_url(sheet_url)
        sb = _get_sb()
        if not sb:
            return {"synced": 0}

        total_synced = 0

        # Sync each verified tab
        for tab_name, table, date_col, date_fields in [
            ("Verified_FREE", "signups", "signup_date", ["Account Created On", "row_date_used"]),
            ("Verified_FIRST_UPLOAD", "uploads", "upload_date", ["Upload Date", "row_date_used"]),
            ("Verified_STRIPE", "payments", "first_payment_date", ["First payment", "row_date_used", "Created"]),
        ]:
            try:
                ws = sheet.worksheet(tab_name)
                all_vals = ws.get_all_values()
                if len(all_vals) < 2:
                    continue
                headers = all_vals[0]
                rows = []
                for row in all_vals[1:]:
                    r = dict(zip(headers, row))
                    email = str(r.get("Email", r.get("email", "")) or "").strip().lower()
                    if not email or "@" not in email:
                        continue

                    d = None
                    for f in date_fields:
                        v = r.get(f, "")
                        if v:
                            d = _parse_date(v)
                            if d:
                                break

                    row_data = {
                        "email": email,
                        "email_normalized": email,
                        date_col: d,
                        "final_status": str(r.get("final_status", "PENDING") or "PENDING").upper()[:20],
                        "category": str(r.get("category", "") or "")[:50],
                    }

                    if table == "signups":
                        row_data["lead_source"] = str(r.get("Lead Source", "") or "")[:200]
                    elif table == "payments":
                        amt = 0.0
                        for af in ("Amount", "Total spend", "Total Spend"):
                            v = r.get(af, "")
                            if v:
                                a = _parse_amount(v)
                                if a > 0:
                                    amt = a
                                    break
                        row_data["total_spend"] = amt
                        try:
                            row_data["payment_count"] = int(r.get("Payment Count", 0) or 0)
                        except Exception:
                            row_data["payment_count"] = 0

                    rows.append(row_data)

                if rows:
                    for i in range(0, len(rows), 50):
                        try:
                            sb.table(table).upsert(rows[i:i + 50], on_conflict="email_normalized").execute()
                        except Exception:
                            pass
                    total_synced += len(rows)
                    log(f"  Synced {len(rows)} rows from {tab_name} -> {table}")
            except Exception as e:
                log(f"  {tab_name} sync error: {e}")

        return {"synced": total_synced}
    except Exception as e:
        log(f"  Sheets sync error: {e}")
        return {"synced": 0, "error": str(e)}


def _parse_date(v):
    if not v:
        return None
    s = str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%y, %I:%M %p", "%m/%d/%Y, %I:%M %p",
                "%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S GMT", "%b %d, %Y"):
        try:
            return datetime.strptime(s.split("+")[0], fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None


def _parse_amount(v):
    if not v:
        return 0.0
    s = re.sub(r"[$,\s\n\r]", "", str(v))
    s = re.sub(r"[A-Za-z]+$", "", s).strip()
    try:
        return float(s)
    except Exception:
        return 0.0


def run():
    """Main: detect gaps + sync + rebuild."""
    log("=" * 60)
    log("DATA GAP FILLER - ensuring zero missing dates")
    log("=" * 60)

    # Step 1: Detect current gaps
    gaps = detect_gaps()
    if gaps.get("error"):
        log(f"Gap detection error: {gaps['error']}")
        return gaps

    log(f"Gaps found: {gaps['gap_count']}")

    # Step 2: Sync latest from Sheets (if available)
    sync_result = sync_new_data_from_sheets()
    log(f"Sheets sync: {sync_result}")

    # Step 3: Rebuild daily_kpis from source tables
    rebuild_result = fill_gaps_from_source_tables()
    log(f"Rebuild: {rebuild_result}")

    # Step 4: Verify no gaps remain
    final_gaps = detect_gaps()
    log(f"Final gap count: {final_gaps['gap_count']}")

    if final_gaps["gap_count"] > 0:
        log(f"WARNING: {final_gaps['gap_count']} gaps remain: {final_gaps['gaps'][:10]}")
    else:
        log("SUCCESS: Zero gaps - all dates covered")

    return {
        "initial_gaps":  gaps["gap_count"],
        "synced":        sync_result.get("synced", 0),
        "rebuilt":       rebuild_result.get("days", 0),
        "final_gaps":    final_gaps["gap_count"],
        "remaining":     final_gaps["gaps"][:20],
    }


if __name__ == "__main__":
    result = run()
    print("\n" + "=" * 40)
    print(f"Initial gaps: {result.get('initial_gaps', 'N/A')}")
    print(f"Synced rows:  {result.get('synced', 0)}")
    print(f"Rebuilt days: {result.get('rebuilt', 0)}")
    print(f"Final gaps:   {result.get('final_gaps', 'N/A')}")
