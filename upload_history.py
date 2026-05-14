"""
upload_history.py
Maintains a permanent record of every email we have EVER seen uploading.
This is the source of truth for "is this their first ever upload?"

Logic:
  - First time we see email upload (in any scrape) -> add to history -> ACCEPT
  - Email already in history -> REJECT as REPEAT_UPLOAD
  - File grows monotonically; once added, email is permanently "has uploaded"

Bootstrap:
  - On first run, load ALL existing Verified_FIRST_UPLOAD entries into history
  - This sets the baseline of "users we already counted"
"""
import json
from pathlib import Path
from datetime import datetime
from dedup_engine import normalize_email, parse_date

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

HISTORY_FILE = DATA_DIR / "upload_history.json"


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [UploadHistory] {msg}", flush=True)


def load_history():
    """Load upload history. Returns {email: {"first_seen": date, "all_dates": [list]}}."""
    if not HISTORY_FILE.exists():
        return {}
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except Exception as e:
        log(f"Load error: {e}")
        return {}


def save_history(history):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2, sort_keys=True)
        log(f"Saved {len(history)} entries to {HISTORY_FILE}")
    except Exception as e:
        log(f"Save error: {e}")


def has_uploaded_before(email, history):
    """Check if this email has ever been recorded as uploading."""
    norm = normalize_email(email)
    if not norm:
        return False
    return norm in history


def record_upload(email, upload_date, history):
    """Record an upload. Returns True if this is the FIRST upload, False if repeat."""
    norm = normalize_email(email)
    if not norm:
        return False
    
    parsed_date = parse_date(upload_date) if upload_date else ""
    
    if norm in history:
        # Already in history - this is a repeat
        if parsed_date:
            entry = history[norm]
            if parsed_date not in entry.get("all_dates", []):
                entry["all_dates"].append(parsed_date)
                entry["all_dates"].sort()
        return False
    
    # First time we see this upload
    history[norm] = {
        "first_seen": parsed_date or datetime.now().strftime("%Y-%m-%d"),
        "first_recorded_at": datetime.now().isoformat(),
        "all_dates": [parsed_date] if parsed_date else [],
    }
    return True


def bootstrap_from_existing(force=False):
    """
    One-time bootstrap: load all existing Verified_FIRST_UPLOAD into history.
    Won't re-bootstrap if history already exists (unless force=True).
    """
    history = load_history()
    
    if history and not force:
        log(f"History already has {len(history)} entries - skipping bootstrap")
        return history
    
    log("BOOTSTRAPPING upload history from existing data...")
    
    try:
        from sheets_writer import read_tab_data
        rows = read_tab_data("Verified_FIRST_UPLOAD")
        log(f"  Loading {len(rows)} existing upload records")
        
        added = 0
        for r in rows:
            email = ""
            for k in ("Email", "email"):
                if k in r and r[k] and "@" in str(r[k]):
                    email = str(r[k]).strip()
                    break
            if not email:
                continue
            
            upload_date = r.get("Upload Date", "")
            
            norm = normalize_email(email)
            if norm and norm not in history:
                parsed = parse_date(upload_date)
                history[norm] = {
                    "first_seen": parsed or "",
                    "first_recorded_at": "BOOTSTRAP",
                    "all_dates": [parsed] if parsed else [],
                    "source": "bootstrap_from_verified_first_upload",
                }
                added += 1
        
        log(f"  Bootstrap added {added} emails")
        
        # Also bootstrap from old DB Stripe customers (they likely uploaded too)
        # This catches the wolfgang case - he's in Stripe, so he uploaded historically
        try:
            from dedup_engine import load_old_database_with_dates
            old_db = load_old_database_with_dates()
            log(f"  Cross-referencing {len(old_db)} old DB emails...")
            
            old_added = 0
            for email, entry in old_db.items():
                if email not in history:
                    earliest = entry.get("earliest", "")
                    if earliest:
                        # If they're in old DB (Stripe customers), assume they uploaded historically
                        # We don't know exact upload date, so use signup date as proxy
                        history[email] = {
                            "first_seen": earliest,
                            "first_recorded_at": "BOOTSTRAP_FROM_OLD_DB",
                            "all_dates": [earliest],
                            "source": "old_db_stripe_customer",
                            "note": "Estimated - email exists in Stripe historical data, likely uploaded before",
                        }
                        old_added += 1
            
            log(f"  Added {old_added} from old DB (Stripe customers)")
        except Exception as e:
            log(f"  Old DB bootstrap skipped: {e}")
        
        save_history(history)
        log(f"Total history: {len(history)} emails")
        return history
    
    except Exception as e:
        log(f"Bootstrap error: {e}")
        return history


def is_truly_first_upload(email, upload_date, history):
    """
    Returns (is_first, reason).
    
    Rules:
      - Email NEVER seen uploading before -> TRUE FIRST UPLOAD
      - Email seen before -> NOT FIRST (repeat upload)
    """
    norm = normalize_email(email)
    if not norm:
        return True, "no_email_to_check"
    
    if norm not in history:
        return True, "first_ever_upload"
    
    entry = history[norm]
    first_seen = entry.get("first_seen", "")
    source = entry.get("source", "previous_scrape")
    
    return False, f"already_uploaded_on_{first_seen}_source:{source}"


if __name__ == "__main__":
    log("Bootstrapping upload history...")
    history = bootstrap_from_existing(force=True)
    log(f"\nTotal: {len(history)} emails in history")
    
    # Test cases
    print()
    test_cases = [
        ("wolfgang.bernecker@tridonic.com", "2026-05-10"),
        ("brand.new.user@example.com", "2026-05-14"),
    ]
    for email, date in test_cases:
        is_first, reason = is_truly_first_upload(email, date, history)
        marker = "FIRST" if is_first else "REPEAT"
        print(f"  {email}: {marker} ({reason})")
