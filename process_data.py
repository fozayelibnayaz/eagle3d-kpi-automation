"""
process_data.py
ORCHESTRATOR: runs Layers 3, 4, 5 in sequence.
  Layer 3: Email validation
  Layer 4: Deduplication (vs old DB)
  Layer 5: ML scoring

Reads:  Raw_FREE, Raw_FIRST_UPLOAD, Raw_STRIPE
Writes: Verified_FREE, Verified_FIRST_UPLOAD, Verified_STRIPE
"""
import csv
import json
import os
from pathlib import Path
from datetime import datetime
from collections import Counter

from sheets_writer import write_tab_data, read_tab_data
from email_validator_engine import validate_batch
from dedup_engine import load_old_database_emails, deduplicate
from ml_intelligence import score_rows, needs_retrain, train_models

DATA_DIR   = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)
PROCESS_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Process] {msg}", flush=True)


# ─── APPEND MODE ───────────────────────────────────────
def merge_with_existing(tab_name: str, new_rows: list) -> list:
    try:
        existing = read_tab_data(tab_name)
        log(f"  Existing in {tab_name}: {len(existing)} rows")
    except Exception:
        existing = []

    by_email = {}
    for r in existing:
        for k in ("__normalized_email__","__email_normalized__","Email","email"):
            if k in r and r[k] and "@" in str(r[k]):
                by_email[str(r[k]).strip().lower()] = r
                break

    new_count = updated_count = 0
    for r in new_rows:
        email = ""
        for k in ("__normalized_email__","__email_normalized__","Email"):
            if k in r and r[k]:
                email = str(r[k]).strip().lower()
                break
        if email:
            if email in by_email:
                old = by_email[email]
                merged = {**old, **r}
                if "__first_processed_at__" not in merged:
                    merged["__first_processed_at__"] = old.get("__processed_at__", PROCESS_TS)
                else:
                    merged["__first_processed_at__"] = old.get("__first_processed_at__", PROCESS_TS)
                by_email[email] = merged
                updated_count += 1
            else:
                r["__first_processed_at__"] = PROCESS_TS
                by_email[email] = r
                new_count += 1

    log(f"  After merge: {len(by_email)} (+{new_count} new, {updated_count} updated)")
    return list(by_email.values())


def process_tab(raw_tab: str, verified_tab: str, old_db_emails: dict) -> dict:
    log(f"{'─'*60}")
    log(f"PROCESSING: {raw_tab} → {verified_tab}")
    log(f"{'─'*60}")

    source = read_tab_data(raw_tab)
    log(f"  [Layer 1] Source rows: {len(source)}")

    if not source:
        log(f"  NO DATA - skipping")
        return {"raw_tab": raw_tab, "source": 0, "verified": 0,
                "validated": 0, "deduplicated": 0, "skipped": 0}

    # ─── Layer 3: Email Validation ───
    log(f"  [Layer 3] Running email validation...")
    check_dns = os.environ.get("CHECK_MX","1") == "1"
    log(f"    MX check: {'enabled' if check_dns else 'disabled'}")
    validated, val_skipped = validate_batch(source, check_dns=check_dns)
    log(f"    Validated: {len(validated)}, Skipped: {len(val_skipped)}")

    # Log skip reasons
    if val_skipped:
        skip_reasons = Counter(r.get("__skip_reason__","unknown") for r in val_skipped)
        for reason, count in skip_reasons.most_common(10):
            log(f"      {reason:30s}: {count}")

    # ─── Layer 4: Deduplication ───
    log(f"  [Layer 4] Running deduplication...")
    unique, dups = deduplicate(validated, old_db_emails)
    log(f"    Unique: {len(unique)}, Duplicates: {len(dups)}")

    # ─── Layer 5: ML Scoring ───
    log(f"  [Layer 5] Running ML scoring...")
    scored = score_rows(unique)
    log(f"    Scored: {len(scored)}")

    # Add processing metadata
    for r in scored:
        r["__processed_at__"] = PROCESS_TS
        r["__pipeline_version__"] = "v3"

    # Save skipped/dup logs
    for label, data in [("Skipped", val_skipped), ("Duplicates", dups)]:
        if data:
            sp = DATA_DIR / f"{label}_{verified_tab}.csv"
            try:
                fields = sorted({k for r in data for k in r.keys()})
                with open(sp,"w",newline="",encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                    w.writeheader()
                    w.writerows(data)
                log(f"  {label} log: {sp} ({len(data)} rows)")
            except Exception as e:
                log(f"  {label} log error: {e}")

    # ─── Layer 6: Write to Sheets (APPEND mode) ───
    if scored:
        log(f"  [Layer 6] Writing to Sheets...")
        merged = merge_with_existing(verified_tab, scored)
        ok = write_tab_data(verified_tab, merged)
        log(f"    Sheets write: {'OK' if ok else 'FAILED/CSV'}")

    return {
        "raw_tab":      raw_tab,
        "verified_tab": verified_tab,
        "source":       len(source),
        "validated":    len(validated),
        "deduplicated": len(unique),
        "verified":     len(scored),
        "skipped":      len(val_skipped),
        "duplicates":   len(dups),
    }


def main():
    log("=" * 60)
    log("PROCESS DATA - Full pipeline (Layers 3, 4, 5)")
    log(f"Time: {PROCESS_TS}")
    log("=" * 60)

    # Retrain models if needed (weekly)
    if needs_retrain():
        log("Models need retraining...")
        try:
            train_models()
        except Exception as e:
            log(f"Training failed (will use heuristics): {e}")

    # Load old DB for dedup
    old_db_emails = load_old_database_emails()
    log(f"Old DB emails loaded: {len(old_db_emails)}")

    TAB_MAP = {
        "Raw_FREE":         "Verified_FREE",
        "Raw_FIRST_UPLOAD": "Verified_FIRST_UPLOAD",
        "Raw_STRIPE":       "Verified_STRIPE",
    }

    results = []
    for raw_tab, verified_tab in TAB_MAP.items():
        try:
            r = process_tab(raw_tab, verified_tab, old_db_emails)
            results.append(r)
        except Exception as e:
            log(f"Error processing {raw_tab}: {e}")
            import traceback
            traceback.print_exc()
            results.append({"raw_tab": raw_tab, "error": str(e)})

    log("=" * 60)
    log("FINAL SUMMARY")
    log("=" * 60)
    for r in results:
        if "error" in r:
            log(f"  {r['raw_tab']:25s} ERROR: {r['error']}")
        else:
            log(f"  {r['raw_tab']:25s}: "
                f"{r['source']:>4} src → "
                f"{r['validated']:>4} valid → "
                f"{r['deduplicated']:>4} unique → "
                f"{r['verified']:>4} scored")

    try:
        with open(DATA_DIR/"processing_report.json","w") as f:
            json.dump({
                "processed_at": PROCESS_TS,
                "old_db_count": len(old_db_emails),
                "results": results,
            }, f, indent=2, default=str)
    except Exception as e:
        log(f"Report save: {e}")

    return results


if __name__ == "__main__":
    main()
