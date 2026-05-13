"""
dedup_engine.py
LAYER 4 - DEDUPLICATION ENGINE
- Pulls full history from old Google Sheet (read-only)
- Normalizes emails (lowercase, strip +aliases, gmail dots)
- Flags: NEW / DUPLICATE / SUSPICIOUS
"""
import os
import json
from pathlib import Path
from datetime import datetime
from email_validator_engine import normalize_email

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

OLD_DB_CACHE = DATA_DIR / "old_db_emails.json"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Dedup] {msg}", flush=True)


def load_old_database_emails() -> dict:
    """
    Load all emails from old database sheet for dedup.
    Returns dict of {normalized_email: original_data}
    Cached to disk for performance.
    """
    old_db_url = os.environ.get(
        "OLD_DATABASE_SHEET_URL",
        "https://docs.google.com/spreadsheets/d/1tEaUA2hGxuHw3E9n0TzyaEUz9MIlpQGoZy-WQz0NwSc"
    )
    
    if not old_db_url:
        log("No OLD_DATABASE_SHEET_URL set - skipping old DB dedup")
        return {}
    
    # Check cache (refresh daily)
    if OLD_DB_CACHE.exists():
        age_hours = (datetime.now().timestamp() - OLD_DB_CACHE.stat().st_mtime) / 3600
        if age_hours < 24:
            try:
                cache = json.load(open(OLD_DB_CACHE))
                log(f"Using cached old DB: {len(cache)} emails")
                return cache
            except Exception:
                pass
    
    # Fetch from old sheet
    log(f"Fetching old DB from {old_db_url[:60]}...")
    emails = {}
    
    try:
        from sheets_writer import _get_client
        gc, _ = _get_client()
        old_ss = gc.open_by_url(old_db_url)
        
        for ws in old_ss.worksheets():
            try:
                rows = ws.get_all_records()
                for r in rows:
                    # Find email field
                    email = ""
                    for k in ("Email","email","EMAIL","Email Address","email_address"):
                        if k in r and r[k] and "@" in str(r[k]):
                            email = str(r[k]).strip()
                            break
                    if not email:
                        # Scan all values
                        for v in r.values():
                            if isinstance(v, str) and "@" in v and "." in v:
                                email = v.strip()
                                break
                    
                    if email:
                        norm = normalize_email(email)
                        if norm not in emails:
                            emails[norm] = {
                                "original_email": email,
                                "tab": ws.title,
                                "found_in_old_db": True,
                            }
                log(f"  {ws.title}: {len(rows)} rows scanned")
            except Exception as e:
                log(f"  {ws.title}: error - {e}")
        
        # Cache
        try:
            json.dump(emails, open(OLD_DB_CACHE,"w"), indent=2, default=str)
        except Exception as e:
            log(f"Cache save failed: {e}")
        
    except Exception as e:
        log(f"Old DB fetch failed: {e}")
    
    log(f"Old DB total unique emails: {len(emails)}")
    return emails


def deduplicate(rows: list, old_db_emails: dict = None) -> tuple:
    """
    Deduplicate rows.
    Returns (unique_rows, duplicate_rows).
    
    Each row gets flagged:
      __dedup_status__: NEW | DUPLICATE_IN_BATCH | DUPLICATE_OLD_DB
      __normalized_email__: normalized version
    """
    if old_db_emails is None:
        old_db_emails = {}
    
    seen_in_batch = set()
    unique     = []
    duplicates = []
    
    new_count = 0
    in_old_db_count = 0
    dup_in_batch_count = 0
    
    for row in rows:
        email = ""
        for k in ("Email","email","__email_normalized__"):
            if k in row and row[k] and "@" in str(row[k]):
                email = str(row[k]).strip()
                break
        
        if not email:
            duplicates.append({**row, "__dedup_status__":"NO_EMAIL"})
            continue
        
        normalized = normalize_email(email)
        
        # Check batch dup
        if normalized in seen_in_batch:
            duplicates.append({
                **row,
                "__normalized_email__": normalized,
                "__dedup_status__": "DUPLICATE_IN_BATCH",
            })
            dup_in_batch_count += 1
            continue
        
        seen_in_batch.add(normalized)
        
        # Check old DB
        in_old_db = normalized in old_db_emails
        if in_old_db:
            in_old_db_count += 1
        else:
            new_count += 1
        
        unique.append({
            **row,
            "__normalized_email__": normalized,
            "__dedup_status__": "DUPLICATE_OLD_DB" if in_old_db else "NEW",
            "__in_old_db__": "yes" if in_old_db else "no",
        })
    
    log(f"Dedup result: {len(unique)} unique ({new_count} NEW, {in_old_db_count} in old DB), {dup_in_batch_count} batch dups")
    return unique, duplicates


if __name__ == "__main__":
    # Test
    emails_db = load_old_database_emails()
    test = [
        {"Email":"test@gmail.com"},
        {"Email":"Test@Gmail.com"},  # dup of above (case)
        {"Email":"test+tag@gmail.com"},  # dup of above (alias)
        {"Email":"new@example.com"},
    ]
    unique, dups = deduplicate(test, emails_db)
    print(f"Unique: {len(unique)}, Dups: {len(dups)}")
    for r in unique:
        print(f"  UNIQUE: {r}")
    for r in dups:
        print(f"  DUP: {r}")
