"""
upload_registry.py
Determines TRUE first upload using account-creation vs upload-date comparison.

Logic:
  Get upload_date from current scrape
  Look up email in old DB (Stripe All Time Data)
  Get account_created_date
  
  If email not in old DB                              -> TRUE FIRST (new user)
  If email in old DB, signup recent (<= 60 days ago) -> TRUE FIRST (just signed up + uploaded)
  If email in old DB, signup old (> 60 days)         -> REPEAT (uploaded historically, deleted, re-uploaded)

Threshold: 60 days = free tier usage period.
Adjust SIGNUP_TO_UPLOAD_THRESHOLD_DAYS if needed.
"""
import json
from pathlib import Path
from datetime import datetime, timedelta
from dedup_engine import normalize_email, parse_date

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

REGISTRY_FILE = DATA_DIR / "upload_registry.json"

# Configurable: how recently must signup be to count upload as legitimate first?
# 60 days = matches free tier window
SIGNUP_TO_UPLOAD_THRESHOLD_DAYS = 60


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [Registry] {msg}", flush=True)


def load_registry():
    """Local registry of emails we have already counted as first upload."""
    if not REGISTRY_FILE.exists():
        return {}
    try:
        with open(REGISTRY_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_registry(registry):
    try:
        with open(REGISTRY_FILE, "w") as f:
            json.dump(registry, f, indent=2, sort_keys=True)
    except Exception as e:
        log(f"Save error: {e}")


def is_truly_first_upload(email, upload_date, old_db):
    """
    THE definitive check using account-creation comparison.
    
    Args:
        email: the email being checked
        upload_date: when they uploaded (YYYY-MM-DD string)
        old_db: dict {email: {"earliest": date, "dates": [...]}}
    
    Returns:
        (is_first_upload, reason)
    """
    norm = normalize_email(email)
    if not norm:
        return True, "no_email_skip_check"
    
    # Already counted in our local registry? (prevents double-counting same scrape)
    registry = load_registry()
    if norm in registry:
        first_seen = registry[norm].get("first_seen", "?")
        return False, f"already_counted_in_registry_on_{first_seen}"
    
    if not upload_date:
        # Conservative: if we don't have upload date, accept and add to registry
        return True, "no_upload_date_assume_new"
    
    # Look up in old DB
    if norm not in old_db:
        # Not in old DB = brand new user we have no history of = TRUE FIRST UPLOAD
        return True, "new_user_not_in_old_db"
    
    # Email IS in old DB - check account creation date
    entry = old_db[norm]
    earliest_signup = entry.get("earliest", "")
    
    if not earliest_signup or earliest_signup == "__no_date__":
        # In old DB but we don't know when they signed up
        # Conservative: assume new
        return True, "in_old_db_but_no_signup_date"
    
    # Calculate gap between signup and upload
    try:
        signup_dt = datetime.strptime(earliest_signup, "%Y-%m-%d")
        upload_dt = datetime.strptime(upload_date, "%Y-%m-%d")
        days_between = (upload_dt - signup_dt).days
        
        if days_between < 0:
            # Upload BEFORE signup? Data quality issue - accept conservatively
            return True, f"upload_before_signup_data_issue_({days_between}d)"
        
        if days_between <= SIGNUP_TO_UPLOAD_THRESHOLD_DAYS:
            # Recently signed up + uploaded = legitimate first upload
            return True, f"signed_up_{days_between}d_ago_within_{SIGNUP_TO_UPLOAD_THRESHOLD_DAYS}d_window"
        else:
            # Signed up long ago, just uploaded now = re-upload
            return False, f"signed_up_{days_between}d_ago_>{SIGNUP_TO_UPLOAD_THRESHOLD_DAYS}d_threshold"
    
    except ValueError as e:
        return True, f"date_parse_error:{e}"


def record_first_upload(email, upload_date, reason=""):
    """Record this email as having been counted as first upload."""
    norm = normalize_email(email)
    if not norm:
        return False
    
    registry = load_registry()
    
    if norm in registry:
        # Already there - just track this date
        if upload_date and upload_date not in registry[norm].get("all_dates_observed", []):
            registry[norm].setdefault("all_dates_observed", []).append(upload_date)
            registry[norm]["all_dates_observed"].sort()
            save_registry(registry)
        return False
    
    registry[norm] = {
        "first_seen": upload_date or datetime.now().strftime("%Y-%m-%d"),
        "added_at": datetime.now().isoformat(),
        "all_dates_observed": [upload_date] if upload_date else [],
        "reason_accepted": reason,
        "source": "live_scrape",
    }
    save_registry(registry)
    return True


def is_in_registry(email):
    norm = normalize_email(email)
    if not norm:
        return False
    return norm in load_registry()


def add_to_registry(email, source, date_str=None, notes=""):
    """Manual add to registry."""
    norm = normalize_email(email)
    if not norm:
        return False
    
    registry = load_registry()
    if norm in registry:
        return False
    
    registry[norm] = {
        "first_seen": date_str or datetime.now().strftime("%Y-%m-%d"),
        "source": source,
        "added_at": datetime.now().isoformat(),
        "all_dates_observed": [date_str] if date_str else [],
        "notes": notes,
    }
    save_registry(registry)
    return True


def bootstrap_registry(force=False):
    """
    NO bootstrap from old DB - we use live old DB lookup instead.
    Registry only tracks what we have ALREADY counted in current scrapes,
    to prevent double-counting same upload across multiple daily runs.
    """
    registry = load_registry()
    
    if registry and not force:
        log(f"Registry has {len(registry)} entries (no bootstrap needed)")
        return registry
    
    log("Initializing empty registry (will populate as uploads are detected)")
    
    # Optionally bootstrap from current Verified_FIRST_UPLOAD ACCEPTED rows
    # to avoid re-counting on next run
    try:
        from sheets_writer import read_tab_data
        rows = read_tab_data("Verified_FIRST_UPLOAD")
        added = 0
        for r in rows:
            if r.get("category") != "ACCEPTED":
                continue
            email = ""
            for k in ("Email", "email"):
                if k in r and r[k] and "@" in str(r[k]):
                    email = str(r[k]).strip()
                    break
            if not email:
                continue
            
            norm = normalize_email(email)
            if not norm:
                continue
            
            upload_date = parse_date(r.get("Upload Date", ""))
            
            if norm not in registry:
                registry[norm] = {
                    "first_seen": upload_date or "",
                    "source": "bootstrap_current_accepted",
                    "added_at": datetime.now().isoformat(),
                    "all_dates_observed": [upload_date] if upload_date else [],
                    "notes": "Already in current Verified_FIRST_UPLOAD as ACCEPTED",
                }
                added += 1
        
        save_registry(registry)
        log(f"Bootstrap added {added} from currently-accepted uploads")
    except Exception as e:
        log(f"Bootstrap from current verified failed: {e}")
    
    return registry


if __name__ == "__main__":
    log("=" * 60)
    log("UPLOAD REGISTRY TEST")
    log("=" * 60)
    
    # Load old DB
    from dedup_engine import load_old_database_with_dates
    old_db = load_old_database_with_dates()
    log(f"Old DB: {len(old_db)} emails")
    
    # Test cases
    test_cases = [
        ("wolfgang.bernecker@tridonic.com", "2026-05-10"),
        ("eirik.murbraech@ramboll.no", "2026-05-13"),
        ("brand.new.user@nowhere.com", "2026-05-14"),
    ]
    
    print()
    for email, upload_date in test_cases:
        norm = normalize_email(email)
        in_db = norm in old_db
        signup = old_db.get(norm, {}).get("earliest", "?") if in_db else "N/A"
        
        is_first, reason = is_truly_first_upload(email, upload_date, old_db)
        marker = "FIRST" if is_first else "REPEAT"
        
        print(f"  [{marker}] {email}")
        print(f"          Upload: {upload_date}, Signup: {signup}")
        print(f"          {reason}")
        print()
