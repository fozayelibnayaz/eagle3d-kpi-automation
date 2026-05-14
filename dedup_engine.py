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
        if age_hours < 24*7:  # 7 days cache to survive API outages
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
    Deduplicate rows using DATE-AWARE rule:
    - Same email + same Account Created date = NOT duplicate (just re-scrape)
    - Same email + different dates = DUPLICATE (real duplicate)
    - New email = NEW
    
    Returns (unique_rows, duplicate_rows).
    """
    if old_db_emails is None:
        old_db_emails = {}
    
    # Track (email, date) seen in this batch
    seen_in_batch = {}  # {email: set of dates}
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
        
        # Get the Account Created date
        date_str = ""
        for k in ("Account Created On","account_created","Created","Date","Upload Date","__scrape_date__"):
            if k in row and row[k]:
                date_str = str(row[k]).strip()[:25]  # first 25 chars (handles "Tue, 28 Apr 2026 21:30:14 GMT")
                break
        
        # Check batch dup using (email, date) pair
        if normalized in seen_in_batch:
            existing_dates = seen_in_batch[normalized]
            if date_str in existing_dates:
                # SAME email + SAME date = same record, just re-scraped
                # This is NOT a duplicate - skip silently
                continue
            else:
                # SAME email + DIFFERENT date = real duplicate (e.g. user signed up twice)
                duplicates.append({
                    **row,
                    "__normalized_email__": normalized,
                    "__dedup_status__": "DUPLICATE_DIFFERENT_DATE",
                    "__previous_dates__": "; ".join(existing_dates),
                })
                dup_in_batch_count += 1
                seen_in_batch[normalized].add(date_str)
                continue
        
        seen_in_batch[normalized] = {date_str}
        
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
    
    log(f"Dedup: {len(unique)} unique ({new_count} NEW, {in_old_db_count} in old DB), {dup_in_batch_count} real dups (different dates)")
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
