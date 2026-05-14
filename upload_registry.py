"""
upload_registry.py
PERMANENT registry of every email that has EVER uploaded a project.
Once added, never counted as "first upload" again.
"""
import json
from pathlib import Path
from datetime import datetime
from dedup_engine import normalize_email, parse_date

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

REGISTRY_FILE = DATA_DIR / "upload_registry.json"


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [Registry] {msg}", flush=True)


def load_registry():
    if not REGISTRY_FILE.exists():
        return {}
    try:
        with open(REGISTRY_FILE) as f:
            return json.load(f)
    except Exception as e:
        log(f"Load error: {e}")
        return {}


def save_registry(registry):
    try:
        with open(REGISTRY_FILE, "w") as f:
            json.dump(registry, f, indent=2, sort_keys=True)
    except Exception as e:
        log(f"Save error: {e}")


def is_in_registry(email):
    norm = normalize_email(email)
    if not norm:
        return False
    return norm in load_registry()


def add_to_registry(email, source, date_str=None, notes=""):
    norm = normalize_email(email)
    if not norm:
        return False
    
    registry = load_registry()
    
    if norm in registry:
        if date_str and date_str not in registry[norm].get("all_dates_observed", []):
            registry[norm].setdefault("all_dates_observed", []).append(date_str)
            registry[norm]["all_dates_observed"].sort()
            save_registry(registry)
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
    registry = load_registry()
    
    if registry and not force:
        log(f"Registry has {len(registry)} entries. Skipping (use force=True).")
        return registry
    
    log("=" * 60)
    log("BOOTSTRAPPING upload registry")
    log("=" * 60)
    
    bootstrap_sources = []
    
    # SOURCE 1: Stripe All Time Data
    log("")
    log("SOURCE 1: Stripe All Time Data (paid customers)")
    try:
        from sheets_writer import _get_client
        gc, _ = _get_client()
        ss = gc.open_by_url(
            "https://docs.google.com/spreadsheets/d/1tEaUA2hGxuHw3E9n0TzyaEUz9MIlpQGoZy-WQz0NwSc"
        )
        
        for ws in ss.worksheets():
            if ws.title != "All Time Data":
                continue
            
            log(f"  Reading tab: {ws.title}")
            all_values = ws.get_all_values()
            if not all_values:
                continue
            
            headers = all_values[0]
            email_col = None
            date_col = None
            
            for i, h in enumerate(headers):
                h_lower = (h or "").lower().strip()
                if email_col is None and h_lower == "email":
                    email_col = i
                if date_col is None and "created" in h_lower:
                    date_col = i
            
            log(f"    Email col: {email_col}, Date col: {date_col}")
            
            added = 0
            for row in all_values[1:]:
                if email_col is None or email_col >= len(row):
                    continue
                raw_email = row[email_col]
                if not raw_email or "@" not in str(raw_email):
                    continue
                
                norm = normalize_email(raw_email)
                if not norm:
                    continue
                
                date_str = ""
                if date_col is not None and date_col < len(row):
                    date_str = parse_date(row[date_col])
                
                if norm not in registry:
                    registry[norm] = {
                        "first_seen": date_str or "",
                        "source": "bootstrap_stripe_all_time_data",
                        "added_at": datetime.now().isoformat(),
                        "all_dates_observed": [date_str] if date_str else [],
                        "notes": "Stripe customer",
                    }
                    added += 1
            
            log(f"  Added {added} from Stripe All Time Data")
            bootstrap_sources.append(("stripe_all_time", added))
            break
    except Exception as e:
        log(f"  Stripe source error: {e}")
    
    # SOURCE 2: Enterprise-Inbound-Leads
    log("")
    log("SOURCE 2: Enterprise-Inbound-Leads")
    try:
        from sheets_writer import _get_client
        gc, _ = _get_client()
        ss = gc.open_by_url(
            "https://docs.google.com/spreadsheets/d/1tEaUA2hGxuHw3E9n0TzyaEUz9MIlpQGoZy-WQz0NwSc"
        )
        
        for ws in ss.worksheets():
            if ws.title != "Enterprise-Inbound-Leads":
                continue
            
            all_values = ws.get_all_values()
            if not all_values:
                continue
            
            headers = all_values[0]
            email_col = None
            date_col = None
            
            for i, h in enumerate(headers):
                h_lower = (h or "").lower().strip()
                if email_col is None and h_lower == "email":
                    email_col = i
                if date_col is None and "created on" in h_lower:
                    date_col = i
            
            added = 0
            for row in all_values[1:]:
                if email_col is None or email_col >= len(row):
                    continue
                raw_email = row[email_col]
                if not raw_email or "@" not in str(raw_email):
                    continue
                
                norm = normalize_email(raw_email)
                if not norm:
                    continue
                
                date_str = ""
                if date_col is not None and date_col < len(row):
                    date_str = parse_date(row[date_col])
                
                if norm not in registry:
                    registry[norm] = {
                        "first_seen": date_str or "",
                        "source": "bootstrap_enterprise_leads",
                        "added_at": datetime.now().isoformat(),
                        "all_dates_observed": [date_str] if date_str else [],
                        "notes": "Enterprise lead",
                    }
                    added += 1
            
            log(f"  Added {added} from Enterprise leads")
            bootstrap_sources.append(("enterprise_leads", added))
            break
    except Exception as e:
        log(f"  Enterprise source error: {e}")
    
    # SOURCE 3: Current Verified_FIRST_UPLOAD
    log("")
    log("SOURCE 3: Current Verified_FIRST_UPLOAD")
    try:
        from sheets_writer import read_tab_data
        rows = read_tab_data("Verified_FIRST_UPLOAD")
        
        added = 0
        for r in rows:
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
                    "source": "bootstrap_current_verified",
                    "added_at": datetime.now().isoformat(),
                    "all_dates_observed": [upload_date] if upload_date else [],
                    "notes": "Already in current Verified_FIRST_UPLOAD",
                }
                added += 1
        
        log(f"  Added {added} from current Verified_FIRST_UPLOAD")
        bootstrap_sources.append(("current_verified", added))
    except Exception as e:
        log(f"  Current verified source error: {e}")
    
    save_registry(registry)
    
    log("")
    log("=" * 60)
    log("BOOTSTRAP COMPLETE")
    log("=" * 60)
    log(f"Total registry size: {len(registry)} unique emails")
    for source, count in bootstrap_sources:
        log(f"  {source}: +{count}")
    
    return registry


def is_truly_first_upload(email, upload_date):
    norm = normalize_email(email)
    if not norm:
        return True, "no_email_skip_check"
    
    registry = load_registry()
    
    if norm in registry:
        entry = registry[norm]
        first = entry.get("first_seen", "?")
        source = entry.get("source", "previous_scrape")
        return False, f"already_in_registry_since_{first}_via_{source}"
    
    return True, "first_ever_upload"


def record_first_upload(email, upload_date):
    return add_to_registry(email,
                           source="live_scrape",
                           date_str=upload_date,
                           notes="Recorded as first ever upload")


if __name__ == "__main__":
    log("UPLOAD REGISTRY TOOL")
    
    registry = load_registry()
    if not registry:
        log("Registry empty - bootstrapping...")
        registry = bootstrap_registry(force=True)
    else:
        log(f"Registry exists with {len(registry)} entries")
    
    print()
    print("=" * 60)
    print("TEST CASES")
    print("=" * 60)
    
    test_emails = [
        ("wolfgang.bernecker@tridonic.com", "2026-05-10"),
        ("eirik.murbraech@ramboll.no", "2026-05-13"),
        ("brand.new.user@nowhere.com", "2026-05-14"),
    ]
    
    for email, date in test_emails:
        is_first, reason = is_truly_first_upload(email, date)
        marker = "FIRST" if is_first else "REPEAT"
        print(f"  [{marker}] {email}: {reason}")
