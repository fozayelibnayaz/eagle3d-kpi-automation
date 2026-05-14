"""
process_data.py
Validates ALL rows. Tags each with category instead of skipping.
ACCEPTED rows go to dashboard counts.
REJECTED rows still saved (categorized) for transparency.
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
from dedup_engine import load_old_database_emails, deduplicate
from ml_intelligence import score_rows, needs_retrain, train_models

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)
PROCESS_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [Process] {msg}", flush=True)


def categorize_row(row, check_dns=False, old_db_emails=None, seen_in_batch=None):
    """
    Assign single category to a row.
    Returns dict with: final_status, category, reason, email, in_old_db
    """
    if old_db_emails is None:
        old_db_emails = {}
    if seen_in_batch is None:
        seen_in_batch = set()
    
    # Find email
    email = ""
    for k in ("Email", "email", "EMAIL"):
        if k in row and row[k] and "@" in str(row[k]):
            email = str(row[k]).strip().lower()
            break
    
    if not email:
        return {"final_status": "REJECTED", "category": "NO_EMAIL",
                "reason": "no email", "email": ""}
    
    # Syntax check
    ok, reason = check_syntax(email)
    if not ok:
        return {"final_status": "REJECTED", "category": "INVALID_FORMAT",
                "reason": reason, "email": email}
    
    # Internal/test/suspicious check (uses email_validator_engine.check_skip)
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
    
    # Lead source check (catches internal-testing and similar)
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
    
    # MX check (optional, slow)
    if check_dns:
        domain = email.split("@")[-1]
        ok, reason = check_mx(domain)
        if not ok:
            return {"final_status": "REJECTED", "category": "INVALID_DOMAIN",
                    "reason": reason, "email": email}
    
    # Duplicate within this batch?
    if email in seen_in_batch:
        return {"final_status": "REJECTED", "category": "DUPLICATE_IN_BATCH",
                "reason": "seen earlier in this scrape", "email": email}
    
    # Check old DB
    in_old_db = "yes" if email in old_db_emails else "no"
    
    return {"final_status": "ACCEPTED", "category": "ACCEPTED",
            "reason": "passed all checks", "email": email,
            "in_old_db": in_old_db}


def process_tab(raw_tab, verified_tab, old_db_emails, check_dns=True):
    log("-" * 60)
    log(f"PROCESSING: {raw_tab} -> {verified_tab}")
    
    source = read_tab_data(raw_tab)
    log(f"  Source rows: {len(source)}")
    
    if not source:
        return {"raw_tab": raw_tab, "source": 0, "accepted": 0, "rejected": 0}
    
    log(f"  Categorizing {len(source)} rows (DNS check: {check_dns})...")
    
    seen_in_batch = set()
    categorized = []
    
    for row in source:
        cat = categorize_row(row,
                              check_dns=check_dns,
                              old_db_emails=old_db_emails,
                              seen_in_batch=seen_in_batch)
        
        if cat["email"] and cat["final_status"] == "ACCEPTED":
            seen_in_batch.add(cat["email"])
        
        # Build enriched row
        enriched = dict(row)
        enriched["final_status"]          = cat["final_status"]
        enriched["category"]              = cat["category"]
        enriched["email_verdict"]         = cat["category"]
        enriched["__rejection_reason__"]  = cat.get("reason", "")
        enriched["__email_normalized__"]  = cat.get("email", "")
        enriched["__in_old_db__"]         = cat.get("in_old_db", "")
        enriched["__processed_at__"]      = PROCESS_TS
        enriched["__validation_status__"] = (
            "verified" if cat["final_status"] == "ACCEPTED" else "rejected"
        )
        
        categorized.append(enriched)
    
    # Stats
    cats = Counter(r["category"] for r in categorized)
    log(f"  Categories breakdown:")
    for c, n in cats.most_common():
        log(f"    {c:20s}: {n}")
    
    accepted = [r for r in categorized if r["final_status"] == "ACCEPTED"]
    rejected = [r for r in categorized if r["final_status"] == "REJECTED"]
    
    log(f"  ACCEPTED: {len(accepted)}, REJECTED: {len(rejected)}")
    
    # ML scoring on accepted only
    if accepted:
        log(f"  ML scoring {len(accepted)} accepted rows...")
        try:
            accepted = score_rows(accepted)
        except Exception as e:
            log(f"  ML scoring error (continuing): {e}")
    
    # Combine all rows
    all_rows = accepted + rejected
    
    # Save rejected to local CSV for inspection
    if rejected:
        rejected_csv = DATA_DIR / f"Rejected_{verified_tab}.csv"
        try:
            fields = sorted({k for r in rejected for k in r.keys()})
            with open(rejected_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                w.writeheader()
                w.writerows(rejected)
            log(f"  Rejected CSV: {rejected_csv}")
        except Exception as e:
            log(f"  Rejected CSV save error: {e}")
    
    # Write all categorized rows to Sheets
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
    
    # Train ML if needed
    if needs_retrain():
        log("Models need retraining...")
        try:
            train_models()
        except Exception as e:
            log(f"Training failed (will use heuristics): {e}")
    
    # Load old DB emails for cross-reference
    old_db_emails = load_old_database_emails()
    log(f"Old DB emails loaded: {len(old_db_emails)}")
    
    TAB_MAP = {
        "Raw_FREE":         "Verified_FREE",
        "Raw_FIRST_UPLOAD": "Verified_FIRST_UPLOAD",
        "Raw_STRIPE":       "Verified_STRIPE",
    }
    
    # MX check is slow - only do it for FREE signups
    check_mx_for = {
        "Raw_FREE":         True,
        "Raw_FIRST_UPLOAD": False,
        "Raw_STRIPE":       False,
    }
    
    results = []
    for raw_tab, verified_tab in TAB_MAP.items():
        try:
            r = process_tab(raw_tab, verified_tab, old_db_emails,
                            check_dns=check_mx_for.get(raw_tab, False))
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
            log(f"      {cat:20s}: {n}")
    
    return results


if __name__ == "__main__":
    main()
