"""
storage_adapter.py
Thin wrapper around sheets_writer.
Sheets is PRIMARY. CSV is fallback ONLY if Sheets fails.
Google Drive storage is fixed.
"""
from sheets_writer import (
    write_tab_data,
    append_tab_rows,
    read_tab_data,
    write_run_summary,
    test_connection,
    _get_client,
)
from pathlib import Path
from datetime import datetime
import csv

DATA_DIR = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)

# Check at import
try:
    SHEETS_AVAILABLE = test_connection()
    if SHEETS_AVAILABLE:
        print("[StorageAdapter] Google Sheets: AVAILABLE (primary storage)", flush=True)
    else:
        print("[StorageAdapter] Google Sheets: unavailable - CSV fallback active", flush=True)
except Exception:
    SHEETS_AVAILABLE = False
    print("[StorageAdapter] Google Sheets: unavailable - CSV fallback active", flush=True)


def _get_sheets_client():
    """Expose internal client for direct access when needed."""
    try:
        return _get_client()
    except Exception:
        return None, None


def read_archive(tab_name: str) -> list:
    """Read from ARCHIVE CSV (permanent historical record)."""
    archive_path = DATA_DIR / f"ARCHIVE_{tab_name}.csv"
    if not archive_path.exists():
        return []
    try:
        with open(archive_path, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        print(f"[StorageAdapter] Archive {tab_name}: {len(rows)} rows", flush=True)
        return rows
    except Exception as e:
        print(f"[StorageAdapter] Archive read error: {e}", flush=True)
        return []


def get_storage_status() -> dict:
    status = {
        "sheets_available": SHEETS_AVAILABLE,
        "data_dir": str(DATA_DIR.absolute()),
        "files": {},
    }
    for f in sorted(DATA_DIR.glob("*.csv")):
        try:
            with open(f) as fh:
                count = sum(1 for _ in fh) - 1
            status["files"][f.name] = {
                "rows":     max(0, count),
                "size_kb":  round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(
                    f.stat().st_mtime
                ).strftime("%Y-%m-%d %H:%M:%S"),
            }
        except Exception:
            status["files"][f.name] = {"error": "unreadable"}
    return status
