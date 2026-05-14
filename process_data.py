"""
process_data.py
Validates ALL rows. Date-aware duplicate detection.
SPECIAL CASE for STRIPE: only check total spend > $0 (no email validation needed).
"""
import csv
import json
import os
import re
from pathlib import Path
from datetime import datetime
from collections import Counter

from sheets_writer import write_tab_data, read_tab_data
from email_validator_engine import (
    check_syntax, check_disposable, check_skip, check_mx,
    DISPOSABLE_DOMAINS, INTERNAL_DOMAINS, INTERNAL_KEYWORDS
)
from dedup_engine import (
    load_old_database_with_dates, is_duplicate_different_date,
    normalize_email, parse_date
)
from ml_intelligence import score_rows, needs_retrain, train_models

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)
PROCESS_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [Process] {msg}", flush=True)


def get_email(row):
    for k in ("Email", "email", "EMAIL"):
        if k in row and row[k] and "@" in str(row[k]):
            return str(row[k]).strip().lower()
    return ""


def get_scraped_date(row, source_type):
    """Get the actual date for this row based on source type."""
    if source_type == "FREE":
        return parse_date(row.get("Account Created On", ""))
    elif source_type == "FIRST_UPLOAD":
        return parse_date(row.get("Upload Date", ""))
    elif source_type == "STRIPE":
        for f in ("First payment", "Created"):
            if f in row and row[f]:
                d = parse_date(row[f])
                if d:
                    return d
        return ""
    return ""


def parse_amount(val):
    """Parse '$29.00' or '$1,234.56' to float."""
    if not val:
        return 0.0
    s = str(val).strip()
    if not s or s in ("—", "-", "N/A"):
        return 0.0
    # Remove currency symbol and commas
    s = re.sub(r"[$,€£¥\s]", "", s)
    try:
        return float(s)
    except Exception:
        return 0.0


def categorize_stripe_row(row):
    """
    SPECIAL: Stripe only checks total spend > 0.
    No email validation, no dedup against old DB (all are real customers).
    
    Categories:
      ACCEPTED      - paid customer (total spend > 0)
      ZERO_SPEND    - in Stripe but never paid ($0)
    """
    email = get_email(row)
    
    spend_raw = row.get("Total spend", "") or row.get("total_spend", "")
    amount = parse_amount(spend_raw)
    
    if amount > 0:
        return {
            "final_status": "ACCEPTED",
            "category": "ACCEPTED",
            "reason": f"paid ${amount:.2f}",
            "email": email,
            "amount": amount,
        }
    else:
        return {
            "final_status": "REJECTED",
            "category": "ZERO_SPEND",
            "reason": "total spend is $0 (not actually paid)",
            "email": email,
            "amount": 0.0,
        }


def categorize_row(row, source_type, check_dns=False, old_db=None, seen_in_batch=None):
    """
    For FREE and FIRST_UPLOAD only. STRIPE uses categorize_stripe_row.
    """
    if old_db is None:
        old_db = {}
    if seen_in_batch is None:
        seen_in_batch = {}
    
    email = get_email(row)
    if not email:
        return {"final_status": "REJECTED", "category": "NO_EMAIL",
                "reason": "no email", "email": ""}
    
    # Syntax check
    ok, reason = check_syntax(email)
    if not ok:
        return {"final_status": "REJECTED", "category": "INVALID_FORMAT",
                "reason": reason, "email": email}
    
    # Internal/test/suspicious check
    ok, reason = check_skip(email)
    if not ok:
        if "internal" in reason.lower():
            return {"final_status": "REJECTED", "category": "INTERNAL",
                    "reason": reason, "email": email}
        return {"final_status": "REJECTED", "category": "SUSPICIOUS",
                "reason": reason, "email": email}
    
    # Disposable check
    ok, reason = check_disposable(email)
    if not ok:
        return {"final_status": "REJECTED", "category": "DISPOSABLE",
                "reason": reason, "email": email}
    
    # Lead source check
    lead_source = ""
    for k, v in row.items():
        if "lead" in k.lower() or "source" in k.lower():
            if v:
                lead_source = str(v).lower()
                break
    
    INTERNAL_LEAD_KEYWORDS = ["internal", "test", "demo", "fake", "sample"]
    for kw in INTERNAL_LEAD_KEYWORDS:
        if kw in lead_source:
            return {"final_status": "REJECTED", "category": "INTERNAL",
                    "reason": f"lead_source contains '{kw}'", "email": email}
    
    # MX check
    if check_dns:
        domain = email.split("@")[-1]
        ok, reason = check_mx(domain)
        if not ok:
            return {"final_status": "REJECTED", "category": "INVALID_DOMAIN",
                    "reason": reason, "email": email}
    
    scraped_date = get_scraped_date(row, source_type)
    normalized = normalize_email(email)
    
    # Batch dup check
    if normalized in seen_in_batch:
        existing_dates = seen_in_batch[normalized]
        if scraped_date and scraped_date in existing_dates:
            return {"final_status": "REJECTED", "category": "DUPLICATE_IN_BATCH",
                    "reason": "same email + same date in batch", "email": email}
        return {"final_status": "REJECTED", "category": "DUPLICATE_IN_BATCH",
                "reason": f"same email different date in batch", "email": email}
    
    # Date-aware old DB check
    is_dup, dup_reason = is_duplicate_different_date(normalized, scraped_date, old_db)
    if is_dup:
        return {"final_status": "REJECTED", "category": "DUPLICATE_DIFFERENT_DATE",
                "reason": dup_reason, "email": email,
                "scraped_date": scraped_date}
    
    in_old_db = "yes" if normalized in old_db else "no"
    return {"final_status": "ACCEPTED", "category": "ACCEPTED",
            "reason": dup_reason, "email": email,
            "in_old_db": in_old_db, "scraped_date": scraped_date}


def process_tab(raw_tab, verified_tab, source_type, old_db, check_dns=True):
    log("-" * 60)
    log(f"PROCESSING: {raw_tab} -> {verified_tab} (source={source_type})")
    
    source = read_tab_data(raw_tab)
    log(f"  Source rows: {len(source)}")
    
    if not source:
        return {"raw_tab": raw_tab, "source": 0, "accepted": 0, "rejected": 0}
    
    seen_in_batch = {}
    categorized = []
    
    if source_type == "STRIPE":
        log(f"  STRIPE mode: only checking total spend > $0 (no email validation)")
        for row in source:
            cat = categorize_stripe_row(row)
            
            enriched = dict(row)
            enriched["final_status"]          = cat["final_status"]
            enriched["category"]              = cat["category"]
            enriched["email_verdict"]         = cat["category"]
            enriched["__rejection_reason__"]  = cat.get("reason", "")
            enriched["__email_normalized__"]  = cat.get("email", "")
            enriched["__amount__"]            = cat.get("amount", 0.0)
            enriched["__scraped_date__"]      = parse_date(
                row.get("First payment", "") or row.get("Created", "")
            )
            enriched["__processed_at__"]      = PROCESS_TS
            enriched["__validation_status__"] = (
                "verified" if cat["final_status"] == "ACCEPTED" else "rejected"
            )
            
            categorized.append(enriched)
    else:
        log(f"  Categorizing {len(source)} rows (DNS check: {check_dns})...")
        
        for row in source:
            cat = categorize_row(
                row, source_type,
                check_dns=check_dns,
                old_db=old_db,
                seen_in_batch=seen_in_batch
            )
            
            if cat["email"] and cat["final_status"] == "ACCEPTED":
                normalized = normalize_email(cat["email"])
                scraped_date = cat.get("scraped_date", "")
                if normalized not in seen_in_batch:
                    seen_in_batch[normalized] = set()
                if scraped_date:
                    seen_in_batch[normalized].add(scraped_date)
            
            enriched = dict(row)
            enriched["final_status"]          = cat["final_status"]
            enriched["category"]              = cat["category"]
            enriched["email_verdict"]         = cat["category"]
            enriched["__rejection_reason__"]  = cat.get("reason", "")
            enriched["__email_normalized__"]  = cat.get("email", "")
            enriched["__in_old_db__"]         = cat.get("in_old_db", "")
            enriched["__scraped_date__"]      = cat.get("scraped_date", "")
            enriched["__processed_at__"]      = PROCESS_TS
            enriched["__validation_status__"] = (
                "verified" if cat["final_status"] == "ACCEPTED" else "rejected"
            )
            
            categorized.append(enriched)
    
    cats = Counter(r["category"] for r in categorized)
    log(f"  Categories breakdown:")
    for c, n in cats.most_common():
        log(f"    {c:30s}: {n}")
    
    accepted = [r for r in categorized if r["final_status"] == "ACCEPTED"]
    rejected = [r for r in categorized if r["final_status"] == "REJECTED"]
    
    log(f"  ACCEPTED: {len(accepted)}, REJECTED: {len(rejected)}")
    
    if accepted:
        log(f"  ML scoring {len(accepted)} accepted rows...")
        try:
            accepted = score_rows(accepted)
        except Exception as e:
            log(f"  ML scoring error (continuing): {e}")
    
    all_rows = accepted + rejected
    
    if rejected:
        rejected_csv = DATA_DIR / f"Rejected_{verified_tab}.csv"
        try:
            fields = sorted({k for r in rejected for k in r.keys()})
            with open(rejected_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                w.writeheader()
                w.writerows(rejected)
        except Exception as e:
            log(f"  Rejected CSV save: {e}")
    
    log(f"  Writing {len(all_rows)} rows to {verified_tab}...")
    ok = write_tab_data(verified_tab, all_rows)
    log(f"  Sheets write: {'OK' if ok else 'FAILED'}")
    
    return {
        "raw_tab":      raw_tab,
        "verified_tab": verified_tab,
        "source":       len(source),
        "accepted":     len(accepted),
        "rejected":     len(rejected),
        "categories":   dict(cats),
    }


def main():
    log("=" * 60)
    log(f"PROCESS DATA - {PROCESS_TS}")
    log("=" * 60)
    
    if needs_retrain():
        log("Models need retraining...")
        try:
            train_models()
        except Exception as e:
            log(f"Training failed: {e}")
    
    log("Loading old database with dates...")
    old_db = load_old_database_with_dates()
    log(f"Old DB: {len(old_db)} unique emails")
    
    TAB_MAP = [
        ("Raw_FREE",         "Verified_FREE",          "FREE"),
        ("Raw_FIRST_UPLOAD", "Verified_FIRST_UPLOAD",  "FIRST_UPLOAD"),
        ("Raw_STRIPE",       "Verified_STRIPE",        "STRIPE"),
    ]
    
    check_mx_for = {
        "Raw_FREE":         True,
        "Raw_FIRST_UPLOAD": False,
        "Raw_STRIPE":       False,
    }
    
    results = []
    for raw_tab, verified_tab, source_type in TAB_MAP:
        try:
            r = process_tab(
                raw_tab, verified_tab, source_type, old_db,
                check_dns=check_mx_for.get(raw_tab, False)
            )
            results.append(r)
        except Exception as e:
            log(f"Error processing {raw_tab}: {e}")
            import traceback
            traceback.print_exc()
    
    log("=" * 60)
    log("FINAL SUMMARY")
    log("=" * 60)
    for r in results:
        raw = r.get("raw_tab", "?")
        src = r.get("source", 0)
        acc = r.get("accepted", 0)
        rej = r.get("rejected", 0)
        log(f"  {raw:25s}: {src:>4} src -> {acc:>4} ACCEPTED, {rej:>4} REJECTED")
        for cat, n in (r.get("categories") or {}).items():
            log(f"      {cat:30s}: {n}")
    
    return results


if __name__ == "__main__":
    main()
