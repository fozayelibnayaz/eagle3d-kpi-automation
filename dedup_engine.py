"""
dedup_engine.py
Date-aware deduplication against old database.

RULE:
  - Email NOT in old DB              -> NEW
  - Email in old DB, SAME date       -> SAME RECORD (not duplicate)
  - Email in old DB, DIFFERENT date  -> DUPLICATE (existing user, new action)
"""
import os
import re
import json
from pathlib import Path
from datetime import datetime
from email.utils import parsedate_to_datetime
from collections import defaultdict

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

OLD_DB_CACHE = DATA_DIR / "old_db_with_dates.json"
OLD_DB_URL = os.environ.get(
    "OLD_DATABASE_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/1tEaUA2hGxuHw3E9n0TzyaEUz9MIlpQGoZy-WQz0NwSc"
)


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [Dedup] {msg}", flush=True)


def normalize_email(email):
    if not email:
        return ""
    e = str(email).strip().lower()
    if "@" not in e:
        return ""
    local, domain = e.split("@", 1)
    if "+" in local:
        local = local.split("+")[0]
    if domain in ("gmail.com", "googlemail.com"):
        local = local.replace(".", "")
    return f"{local}@{domain}"


DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%m/%d/%y, %I:%M %p",
    "%m/%d/%Y, %I:%M %p",
    "%a %b %d %Y",
    "%a %b %d %Y %H:%M:%S",
    "%b %d, %Y",
    "%d %b %Y",
    "%d/%m/%Y",
]


def parse_date(raw):
    """Parse any date format. Returns YYYY-MM-DD or empty."""
    if not raw or not str(raw).strip():
        return ""
    raw = str(raw).strip()
    if raw in ("—", "-", "N/A", "n/a"):
        return ""
    
    # Try RFC 2822
    try:
        dt = parsedate_to_datetime(raw)
        if dt:
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    
    # Try common formats
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    
    # Regex fallback
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%Y-%m-%d")
        except Exception:
            pass
    
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", raw)
    if m:
        try:
            month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if year < 100:
                year += 2000
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except Exception:
            pass
    
    return ""


def load_old_database_with_dates(force_refresh=False):
    """
    Load old database. Returns {email: [list of YYYY-MM-DD dates]}.
    Cached for 7 days.
    """
    # Check cache
    if not force_refresh and OLD_DB_CACHE.exists():
        try:
            age_hours = (datetime.now().timestamp() - OLD_DB_CACHE.stat().st_mtime) / 3600
            if age_hours < 24 * 7:
                with open(OLD_DB_CACHE) as f:
                    cache = json.load(f)
                log(f"Using cached old DB: {len(cache)} unique emails")
                return cache
        except Exception as e:
            log(f"Cache read error: {e}")
    
    log(f"Fetching old DB from {OLD_DB_URL[:60]}...")
    
    email_dates = defaultdict(set)  # {email: set of dates}
    
    try:
        from sheets_writer import _get_client
        gc, _ = _get_client()
        ss = gc.open_by_url(OLD_DB_URL)
        
        for ws in ss.worksheets():
            log(f"  Reading tab: {ws.title}")
            try:
                # Use get_all_values to handle duplicate headers
                all_values = ws.get_all_values()
                if not all_values:
                    log(f"    Empty")
                    continue
                
                headers = all_values[0]
                data_rows = all_values[1:]
                
                # Find email column index
                email_col_idx = None
                date_col_idx = None
                
                for i, h in enumerate(headers):
                    h_lower = (h or "").lower().strip()
                    if email_col_idx is None and "email" == h_lower:
                        email_col_idx = i
                    if date_col_idx is None:
                        # Look for "Created on", "Date", "Created", etc
                        if any(k in h_lower for k in ["created on", "created", "date", "signup date", "joined"]):
                            if "called" not in h_lower:  # skip "Date Called (Attempt N)"
                                date_col_idx = i
                
                if email_col_idx is None:
                    log(f"    No email column found in headers")
                    continue
                
                log(f"    Email col: {email_col_idx} ('{headers[email_col_idx]}')")
                if date_col_idx is not None:
                    log(f"    Date col: {date_col_idx} ('{headers[date_col_idx]}')")
                else:
                    log(f"    No date column found")
                
                tab_emails = 0
                for row in data_rows:
                    if email_col_idx >= len(row):
                        continue
                    raw_email = row[email_col_idx]
                    if not raw_email or "@" not in str(raw_email):
                        continue
                    
                    email = normalize_email(raw_email)
                    if not email:
                        continue
                    
                    # Get date for this row
                    date_str = ""
                    if date_col_idx is not None and date_col_idx < len(row):
                        raw_date = row[date_col_idx]
                        date_str = parse_date(raw_date)
                    
                    if date_str:
                        email_dates[email].add(date_str)
                    else:
                        # Add a placeholder so we know email exists even without date
                        email_dates[email].add("__no_date__")
                    
                    tab_emails += 1
                
                log(f"    Found {tab_emails} email entries")
            
            except Exception as e:
                log(f"    Tab error: {e}")
        
        # Convert sets to lists for JSON serialization
        result = {email: sorted(list(dates)) for email, dates in email_dates.items()}
        
        # Save cache
        try:
            with open(OLD_DB_CACHE, "w") as f:
                json.dump(result, f, indent=2)
            log(f"Cached to {OLD_DB_CACHE}")
        except Exception as e:
            log(f"Cache save error: {e}")
        
        log(f"Old DB loaded: {len(result)} unique emails, "
            f"{sum(len(d) for d in result.values())} total date entries")
        return result
    
    except Exception as e:
        log(f"Old DB fetch failed: {e}")
        # Return cached data if available, even if stale
        if OLD_DB_CACHE.exists():
            try:
                with open(OLD_DB_CACHE) as f:
                    return json.load(f)
            except Exception:
                pass
        return {}


def is_duplicate_different_date(email, scraped_date, old_db):
    """
    Check if email exists in old DB with a DIFFERENT date.
    
    Returns:
        (is_duplicate, reason)
    """
    norm = normalize_email(email)
    if not norm:
        return False, "no_email"
    
    if norm not in old_db:
        return False, "new_email"
    
    old_dates = old_db[norm]
    
    # If we don't have a scraped date, can't compare
    if not scraped_date:
        return False, "no_scrape_date"
    
    # If exact date match (or we don't have date in old DB), not duplicate
    if scraped_date in old_dates:
        return False, "same_date_match"
    
    # If old DB has only "__no_date__" placeholder, treat as not duplicate
    # (we can't prove different date)
    if old_dates == ["__no_date__"]:
        return False, "old_db_has_no_dates"
    
    # Email exists in old DB with different dates
    real_dates = [d for d in old_dates if d != "__no_date__"]
    if real_dates:
        return True, f"existed_on:{','.join(real_dates[:3])}"
    
    return False, "no_real_dates_in_old_db"


def deduplicate(rows, old_db_emails=None, scraped_date_field=None):
    """
    Date-aware deduplication.
    
    Args:
        rows: list of row dicts
        old_db_emails: dict {email: [dates]}
        scraped_date_field: field name to use for scraped date
                           (e.g., "Account Created On" for FREE)
    
    Returns:
        (unique_rows, duplicate_rows) - both with dedup_status field
    """
    if old_db_emails is None:
        old_db_emails = {}
    
    seen_in_batch = {}
    unique = []
    duplicates = []
    
    new_count = 0
    same_date_count = 0
    different_date_count = 0
    batch_dup_count = 0
    
    for row in rows:
        # Find email
        email = ""
        for k in ("Email", "email", "EMAIL", "__email_normalized__"):
            if k in row and row[k] and "@" in str(row[k]):
                email = str(row[k]).strip()
                break
        
        if not email:
            duplicates.append({**row, "__dedup_status__": "NO_EMAIL"})
            continue
        
        normalized = normalize_email(email)
        
        # Get scraped date for this row
        scraped_date = ""
        if scraped_date_field and scraped_date_field in row:
            scraped_date = parse_date(row[scraped_date_field])
        
        # Check batch dup
        if normalized in seen_in_batch:
            existing = seen_in_batch[normalized]
            if scraped_date in existing["dates"]:
                # Same date in same batch = same record, skip silently
                continue
            else:
                duplicates.append({
                    **row,
                    "__normalized_email__": normalized,
                    "__dedup_status__": "DUPLICATE_IN_BATCH",
                    "__batch_dates__": ",".join(existing["dates"]),
                })
                batch_dup_count += 1
                seen_in_batch[normalized]["dates"].add(scraped_date)
                continue
        
        seen_in_batch[normalized] = {"dates": {scraped_date} if scraped_date else set()}
        
        # Check old DB with date awareness
        is_dup, reason = is_duplicate_different_date(normalized, scraped_date, old_db_emails)
        
        if is_dup:
            duplicates.append({
                **row,
                "__normalized_email__": normalized,
                "__dedup_status__": "DUPLICATE_DIFFERENT_DATE",
                "__dup_reason__": reason,
                "__scraped_date__": scraped_date,
            })
            different_date_count += 1
        else:
            in_old_db = normalized in old_db_emails
            
            unique.append({
                **row,
                "__normalized_email__": normalized,
                "__dedup_status__": "DUPLICATE_OLD_DB" if (in_old_db and reason == "same_date_match") else "NEW",
                "__in_old_db__": "yes" if in_old_db else "no",
                "__dedup_reason__": reason,
                "__scraped_date__": scraped_date,
            })
            
            if reason == "same_date_match":
                same_date_count += 1
            else:
                new_count += 1
    
    log(f"Dedup result: {len(unique)} unique "
        f"({new_count} NEW, {same_date_count} same-date matches), "
        f"{different_date_count} REAL DUPLICATES (different dates), "
        f"{batch_dup_count} batch dups")
    
    return unique, duplicates


# Backward compatibility aliases
def load_old_database_emails():
    """Alias for old code. Returns {email: True} for compatibility."""
    full = load_old_database_with_dates()
    return {email: True for email in full.keys()}


if __name__ == "__main__":
    log("Loading old DB with dates...")
    db = load_old_database_with_dates(force_refresh=True)
    log(f"Total: {len(db)} emails")
    
    # Show samples with multiple dates
    log("\nEmails with multiple dates (first 10):")
    multi = [(e, d) for e, d in db.items() if len(d) > 1]
    for email, dates in multi[:10]:
        log(f"  {email}: {dates}")
    log(f"\nTotal emails with multiple dates: {len(multi)}")
