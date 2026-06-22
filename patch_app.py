#!/usr/bin/env python3
# Patch app.py to use Supabase as primary data source
# Replaces load_sheet() calls with supabase_data_loader

from pathlib import Path
from datetime import datetime

app_path = Path("app.py")
content = app_path.read_text()

MARKER = "# SUPABASE_PATCH_APPLIED"

if MARKER in content:
    print("Already patched")
else:
    # Insert supabase loader import right after the existing imports block
    # Find the load_sheet function and add supabase_data_loader before it
    insert_after = "def get_secret(k, d=None):"
    
    supabase_patch = '''
# SUPABASE_PATCH_APPLIED
# Load Supabase credentials into environment so supabase_data_loader can find them
import os as _os
try:
    _sb_url = st.secrets.get("SUPABASE_URL", "")
    _sb_key = st.secrets.get("SUPABASE_SERVICE_KEY", "")
    if _sb_url:
        _os.environ["SUPABASE_URL"] = str(_sb_url).strip()
    if _sb_key:
        _os.environ["SUPABASE_SERVICE_KEY"] = str(_sb_key).strip()
except Exception:
    pass

# Import Supabase data loader
try:
    from supabase_data_loader import load_tab as _sb_load_tab, get_connection_status as _sb_status
    _SUPABASE_ACTIVE = _sb_status().get("connected", False)
except Exception as _e:
    _sb_load_tab = None
    _SUPABASE_ACTIVE = False

'''

    # Add after get_secret definition
    content = content.replace(
        "def get_secret(k, d=None):",
        supabase_patch + "def get_secret(k, d=None):"
    )

    # Now patch the load_sheet function to try Supabase first
    old_load_sheet = '''@st.cache_data(ttl=120)
def load_sheet(tab):
    if not MASTER_SHEET_URL:
        return pd.DataFrame()'''

    new_load_sheet = '''@st.cache_data(ttl=120)
def load_sheet(tab):
    # PRIMARY: Try Supabase first
    if _SUPABASE_ACTIVE and _sb_load_tab is not None:
        try:
            _df = _sb_load_tab(tab)
            if not _df.empty:
                return _df
        except Exception as _e:
            pass  # Fall through to Google Sheets
    # FALLBACK: Google Sheets
    if not MASTER_SHEET_URL:
        return pd.DataFrame()'''

    if old_load_sheet in content:
        content = content.replace(old_load_sheet, new_load_sheet)
        print("load_sheet patched to use Supabase first")
    else:
        print("WARNING: Could not find exact load_sheet signature - trying alternate patch")
        # Try finding just the cache decorator + function
        alt_old = "@st.cache_data(ttl=120)\ndef load_sheet(tab):"
        alt_new = """@st.cache_data(ttl=120)
def load_sheet(tab):
    # PRIMARY: Try Supabase first
    if _SUPABASE_ACTIVE and _sb_load_tab is not None:
        try:
            _df = _sb_load_tab(tab)
            if not _df.empty:
                return _df
        except Exception:
            pass"""
        if alt_old in content:
            content = content.replace(alt_old, alt_new)
            print("load_sheet patched (alternate method)")
        else:
            print("ERROR: Could not patch load_sheet - manual edit required")

    app_path.write_text(content)
    print("app.py patched successfully")

print("Done")
