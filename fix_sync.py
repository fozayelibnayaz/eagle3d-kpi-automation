#!/usr/bin/env python3
# Fix sync errors and re-sync all data to Supabase
import re
import os
import json
from datetime import datetime
from pathlib import Path

SUPABASE_URL = "https://nqxmlvftcoakgeqtojwy.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5xeG1sdmZ0Y29ha2dlcXRvand5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4MjEwNjU0NSwiZXhwIjoyMDk3NjgyNTQ1fQ.mDTqE7TMVkv1Gi1RriC3PXfrTqPBqb5PgCpygBUl0yk"

from supabase import create_client
sb = create_client(SUPABASE_URL, SUPABASE_KEY)

def log(m):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)


def clean_date(val):
    if not val or str(val).strip() in ("", "nan", "None", "-", "--"):
        return None
    s = str(val).strip()
    # Try YYYY-MM-DD first (already clean)
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # Try common formats
    for fmt in (
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
        "%m/%d/%y, %I:%M %p", "%m/%d/%Y, %I:%M %p",
        "%m/%d/%y, %I:%M:%S %p", "%m/%d/%Y, %I:%M:%S %p",
        "%m/%d/%y %I:%M %p", "%m/%d/%Y %I:%M %p",
        "%m/%d/%y", "%m/%d/%Y",
        "%d/%m/%Y", "%d/%m/%y",
        "%b %d, %Y", "%d %b %Y",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    # Regex YYYY-MM-DD anywhere in string
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # Regex M/D/YY or M/D/YYYY
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if m:
        mo, day, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if yr < 100:
            yr += 2000
        try:
            return datetime(yr, mo, day).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def clean_amount(val):
    if not val and val != 0:
        return 0.0
    s = str(val).strip()
    if s in ("", "nan", "None", "-"):
        return 0.0
    s = re.sub(r"[$,€£\s\n\r]", "", s)
    s = re.sub(r"[A-Za-z]+$", "", s).strip()
    if not s:
        return 0.0
    try:
        return round(float(s), 2)
    except Exception:
        return 0.0


from sheets_writer import read_tab_data

# ── SYNC signups ──
log("Syncing Verified_FREE -> signups...")
rows = read_tab_data("Verified_FREE")
log(f"Read {len(rows)} rows from Verified_FREE")
upsert_rows = []
skipped = 0
for r in rows:
    email = str(r.get("Email", r.get("email", "")) or "").strip().lower()
    if not email or "@" not in email:
        skipped += 1
        continue
    date_val = None
    for field in ("Account Created On", "row_date_used", "__scraped_date__", "__processed_at__"):
        v = str(r.get(field, "") or "").strip()
        if v and v not in ("nan", "None", ""):
            date_val = clean_date(v)
            if date_val:
                break
    upsert_rows.append({
        "email":            email,
        "email_normalized": email,
        "signup_date":      date_val,
        "lead_source":      str(r.get("Lead Source", "") or "")[:200],
        "final_status":     str(r.get("final_status", "PENDING") or "PENDING").upper()[:20],
        "category":         str(r.get("category", "") or "")[:50],
        "rejection_reason": str(r.get("__rejection_reason__", r.get("verdict_reason", "")) or "")[:200],
        "is_overridden":    False,
    })

log(f"Upserting {len(upsert_rows)} signups (skipped {skipped} no-email rows)...")
errors = 0
for i in range(0, len(upsert_rows), 50):
    chunk = upsert_rows[i:i+50]
    try:
        sb.table("signups").upsert(chunk, on_conflict="email_normalized").execute()
    except Exception as e:
        errors += 1
        if errors <= 3:
            log(f"Chunk {i}-{i+50} error: {e}")
log(f"Signups done: {len(upsert_rows)} rows, {errors} chunk errors")


# ── SYNC uploads ──
log("Syncing Verified_FIRST_UPLOAD -> uploads...")
rows = read_tab_data("Verified_FIRST_UPLOAD")
log(f"Read {len(rows)} rows from Verified_FIRST_UPLOAD")
upsert_rows = []
skipped = 0
for r in rows:
    email = str(r.get("Email", r.get("email", "")) or "").strip().lower()
    if not email or "@" not in email:
        skipped += 1
        continue
    date_val = None
    for field in ("Upload Date", "row_date_used", "__scraped_date__", "__processed_at__"):
        v = str(r.get(field, "") or "").strip()
        if v and v not in ("nan", "None", ""):
            date_val = clean_date(v)
            if date_val:
                break
    upsert_rows.append({
        "email":            email,
        "email_normalized": email,
        "upload_date":      date_val,
        "final_status":     str(r.get("final_status", "PENDING") or "PENDING").upper()[:20],
        "category":         str(r.get("category", "") or "")[:50],
        "rejection_reason": str(r.get("__rejection_reason__", r.get("verdict_reason", "")) or "")[:200],
        "is_overridden":    False,
    })

log(f"Upserting {len(upsert_rows)} uploads (skipped {skipped})...")
errors = 0
for i in range(0, len(upsert_rows), 50):
    chunk = upsert_rows[i:i+50]
    try:
        sb.table("uploads").upsert(chunk, on_conflict="email_normalized").execute()
    except Exception as e:
        errors += 1
        if errors <= 3:
            log(f"Chunk {i}-{i+50} error: {e}")
log(f"Uploads done: {len(upsert_rows)} rows, {errors} chunk errors")


# ── SYNC payments ──
log("Syncing Verified_STRIPE -> payments...")
rows = read_tab_data("Verified_STRIPE")
log(f"Read {len(rows)} rows from Verified_STRIPE")
upsert_rows = []
skipped = 0
for r in rows:
    email = str(r.get("Email", r.get("email", "")) or "").strip().lower()
    if not email or "@" not in email:
        skipped += 1
        continue
    date_val = None
    for field in ("First payment", "row_date_used", "Created", "Created (UTC)", "__scraped_date__"):
        v = str(r.get(field, "") or "").strip()
        if v and v not in ("nan", "None", ""):
            date_val = clean_date(v)
            if date_val:
                break
    amt = clean_amount(
        r.get("Amount") or r.get("Total spend") or r.get("Total Spend") or r.get("__amount__") or 0
    )
    try:
        pc = int(r.get("Payment Count", r.get("payment_count", 0)) or 0)
    except Exception:
        pc = 0
    upsert_rows.append({
        "email":              email,
        "email_normalized":   email,
        "first_payment_date": date_val,
        "total_spend":        amt,
        "payment_count":      pc,
        "final_status":       str(r.get("final_status", "PENDING") or "PENDING").upper()[:20],
        "category":           str(r.get("category", "") or "")[:50],
        "stripe_customer_id": str(r.get("stripe_customer_id", "") or "")[:100],
        "is_overridden":      False,
    })

log(f"Upserting {len(upsert_rows)} payments (skipped {skipped})...")
errors = 0
for i in range(0, len(upsert_rows), 50):
    chunk = upsert_rows[i:i+50]
    try:
        sb.table("payments").upsert(chunk, on_conflict="email_normalized").execute()
    except Exception as e:
        errors += 1
        if errors <= 3:
            log(f"Chunk {i}-{i+50} error: {e}")
log(f"Payments done: {len(upsert_rows)} rows, {errors} chunk errors")


# ── VERIFY COUNTS ──
log("Verifying Supabase counts...")
for table in ("daily_kpis", "signups", "uploads", "payments"):
    try:
        resp = sb.table(table).select("count", count="exact").execute()
        log(f"  {table}: {resp.count} rows")
    except Exception as e:
        log(f"  {table}: error {e}")

log("ALL SYNC COMPLETE")
