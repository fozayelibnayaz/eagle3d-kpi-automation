# storage_adapter.py
"""
Storage adapter - tries Google Sheets FIRST, falls back to CSV/JSON files.
Data is ALWAYS captured regardless of Sheets availability.
"""
import csv
import json
import logging
from pathlib import Path
from datetime import datetime

log = logging.getLogger(__name__)

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

SHEETS_AVAILABLE = False

# --- Try Google Sheets connection at import time ---
try:
    from sheets_writer import write_tab_data as _sheets_write_tab
    from sheets_writer import write_run_summary as _sheets_write_summary
    from sheets_writer import test_connection
    result = test_connection()
    if result:
        SHEETS_AVAILABLE = True
        print("[StorageAdapter] Google Sheets: AVAILABLE - will use as primary storage", flush=True)
    else:
        print("[StorageAdapter] Google Sheets: connection test failed - using CSV fallback", flush=True)
except Exception as e:
    print(f"[StorageAdapter] Google Sheets: unavailable ({e}) - using CSV fallback", flush=True)


def _extract_email(row: dict):
    for k in ("Email", "email", "EMAIL"):
        if k in row and row[k] and "@" in str(row[k]):
            return str(row[k]).strip().lower()
    for v in row.values():
        if isinstance(v, str) and "@" in v and "." in v:
            return v.strip().lower()
    return None


def _append_to_archive(archive_path: Path, new_rows: list, fields: list, tab_name: str):
    existing_rows = []
    if archive_path.exists():
        try:
            with open(archive_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing_rows = list(reader)
                if reader.fieldnames:
                    for fn in reader.fieldnames:
                        if fn not in fields:
                            fields.append(fn)
        except Exception as e:
            print(f"[StorageAdapter] Archive read error: {e}", flush=True)

    existing_keys = set()
    for row in existing_rows:
        email = _extract_email(row)
        date_part = str(row.get("__scraped_at__", ""))[:10]
        if email:
            existing_keys.add(f"{email}|{date_part}")

    today = datetime.now().strftime("%Y-%m-%d")
    added = 0
    for row in new_rows:
        email = _extract_email(row)
        date_part = str(row.get("__scraped_at__", today))[:10]
        key = f"{email}|{date_part}" if email else None
        if key and key in existing_keys:
            continue
        if key:
            existing_keys.add(key)
        existing_rows.append(row)
        added += 1

    with open(archive_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(existing_rows)

    print(f"[StorageAdapter] {tab_name} archive: +{added} new rows, {len(existing_rows)} total", flush=True)


def write_tab_data(tab_name: str, rows: list) -> bool:
    """
    PRIMARY: Google Sheets
    FALLBACK: Local CSV + JSON
    ALWAYS: Both written when Sheets is available
    """
    if not rows:
        print(f"[StorageAdapter] {tab_name}: no rows to write", flush=True)
        return False

    sheets_ok = False
    csv_ok = False

    # === STEP 1: Try Google Sheets FIRST ===
    if SHEETS_AVAILABLE:
        try:
            _sheets_write_tab(tab_name, rows)
            sheets_ok = True
            print(f"[StorageAdapter] {tab_name}: written to Google Sheets ({len(rows)} rows)", flush=True)
        except Exception as e:
            err_str = str(e).lower()
            if any(x in err_str for x in [
                "storage", "quota", "insufficient", "limit exceeded",
                "unable to save", "drive", "diskquota", "storageQuotaExceeded"
            ]):
                print(f"[StorageAdapter] {tab_name}: Sheets STORAGE FULL - falling back to CSV", flush=True)
            else:
                print(f"[StorageAdapter] {tab_name}: Sheets write failed ({e}) - falling back to CSV", flush=True)

    # === STEP 2: Always write local CSV (backup or primary if Sheets failed) ===
    try:
        all_fields = set()
        for row in rows:
            all_fields.update(row.keys())
        fields = sorted(all_fields)

        # Current snapshot (mirrors Raw_* sheet)
        csv_path = DATA_DIR / f"Raw_{tab_name}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        # Historical archive (never lost)
        archive_path = DATA_DIR / f"Archive_{tab_name}.csv"
        _append_to_archive(archive_path, rows, fields, tab_name)

        # JSON snapshot
        json_path = DATA_DIR / f"Raw_{tab_name}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({
                "tab": tab_name,
                "scraped_at": datetime.now().isoformat(),
                "row_count": len(rows),
                "sheets_written": sheets_ok,
                "rows": rows
            }, f, indent=2, default=str)

        csv_ok = True
        status = "backup copy" if sheets_ok else "PRIMARY (Sheets unavailable)"
        print(f"[StorageAdapter] {tab_name}: CSV written as {status} -> {csv_path}", flush=True)

    except Exception as e:
        print(f"[StorageAdapter] {tab_name}: CSV write ALSO failed: {e}", flush=True)

    return sheets_ok or csv_ok


def write_run_summary(summary: dict) -> bool:
    """Write pipeline run summary - Sheets first, always local JSON."""
    sheets_ok = False

    # Try Sheets first
    if SHEETS_AVAILABLE:
        try:
            _sheets_write_summary(summary)
            sheets_ok = True
            print(f"[StorageAdapter] Run summary written to Sheets", flush=True)
        except Exception as e:
            print(f"[StorageAdapter] Sheets summary failed ({e}) - saving locally", flush=True)

    # Always write local JSON
    try:
        summary_path = DATA_DIR / "run_summaries.json"
        existing = []
        if summary_path.exists():
            try:
                with open(summary_path, "r") as f:
                    existing = json.load(f)
            except Exception:
                existing = []

        existing.append({
            "timestamp": datetime.now().isoformat(),
            "sheets_written": sheets_ok,
            "summary": summary
        })
        existing = existing[-100:]

        with open(summary_path, "w") as f:
            json.dump(existing, f, indent=2, default=str)

        print(f"[StorageAdapter] Run summary saved locally: {summary}", flush=True)

    except Exception as e:
        print(f"[StorageAdapter] Local summary write failed: {e}", flush=True)

    return True


def read_tab_data(tab_name: str) -> list:
    """Read data - local CSV first (fastest), nothing else needed."""
    csv_path = DATA_DIR / f"Raw_{tab_name}.csv"
    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            with open(csv_path, "r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            print(f"[StorageAdapter] {tab_name}: read {len(rows)} rows from local CSV", flush=True)
            return rows
        except Exception as e:
            print(f"[StorageAdapter] {tab_name}: CSV read failed: {e}", flush=True)

    json_path = DATA_DIR / f"Raw_{tab_name}.json"
    if json_path.exists():
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
            rows = data.get("rows", [])
            print(f"[StorageAdapter] {tab_name}: read {len(rows)} rows from JSON", flush=True)
            return rows
        except Exception as e:
            print(f"[StorageAdapter] {tab_name}: JSON read failed: {e}", flush=True)

    print(f"[StorageAdapter] {tab_name}: no local data found", flush=True)
    return []


def read_archive(tab_name: str) -> list:
    """Read full historical archive for a tab."""
    archive_path = DATA_DIR / f"Archive_{tab_name}.csv"
    if not archive_path.exists():
        return []
    try:
        with open(archive_path, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        print(f"[StorageAdapter] {tab_name} archive: {len(rows)} total rows", flush=True)
        return rows
    except Exception as e:
        print(f"[StorageAdapter] {tab_name} archive read failed: {e}", flush=True)
        return []


def get_storage_status() -> dict:
    """Return summary of current storage state."""
    status = {
        "sheets_available": SHEETS_AVAILABLE,
        "data_dir": str(DATA_DIR.absolute()),
        "files": {}
    }
    for f in sorted(DATA_DIR.glob("*.csv")):
        try:
            with open(f, "r") as fh:
                row_count = sum(1 for _ in fh) - 1
            status["files"][f.name] = {
                "rows": max(0, row_count),
                "size_kb": round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat()
            }
        except Exception:
            status["files"][f.name] = {"error": "unreadable"}
    return status
