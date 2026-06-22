#!/usr/bin/env python3
"""
Access Control System
- Only emails on the allowlist can log in
- Default rule: @eagle3dstreaming.com domain allowed
- Admin can add specific emails (any domain) via Settings page
- Stored in Supabase: access_control table
"""

import os
import json
from datetime import datetime
from pathlib import Path


def _get_sb():
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        try:
            import streamlit as st
            url = str(st.secrets.get("SUPABASE_URL", "")).strip()
            key = str(st.secrets.get("SUPABASE_SERVICE_KEY", "")).strip()
        except Exception:
            pass
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def is_allowed(email: str) -> tuple:
    """Returns (allowed: bool, role: str, reason: str)."""
    if not email or "@" not in email:
        return False, "none", "Invalid email"

    email = email.strip().lower()
    domain = email.split("@")[-1]

    # Rule 1: @eagle3dstreaming.com always allowed
    if domain == "eagle3dstreaming.com":
        # Check Supabase for role
        sb = _get_sb()
        if sb:
            try:
                r = sb.table("access_control").select("*").eq("email", email).eq("is_active", True).execute()
                if r.data:
                    return True, r.data[0].get("role", "viewer"), "eagle3d domain + DB record"
            except Exception:
                pass
        return True, "admin", "eagle3dstreaming.com domain (auto-allow)"

    # Rule 2: Specific email on allowlist
    sb = _get_sb()
    if sb:
        try:
            r = sb.table("access_control").select("*").eq("email", email).eq("is_active", True).execute()
            if r.data:
                row = r.data[0]
                return True, row.get("role", "viewer"), f"Allowlisted by {row.get('added_by', 'admin')}"
        except Exception as e:
            return False, "none", f"DB error: {e}"

    return False, "none", "Email not on allowlist"


def add_email(email: str, role: str = "viewer", added_by: str = "admin", notes: str = "") -> dict:
    """Add email to allowlist."""
    email = email.strip().lower()
    if "@" not in email:
        return {"success": False, "message": "Invalid email"}

    sb = _get_sb()
    if not sb:
        return {"success": False, "message": "Supabase not connected"}

    try:
        sb.table("access_control").upsert({
            "email":      email,
            "role":       role,
            "added_by":   added_by,
            "notes":      notes,
            "is_active":  True,
            "added_at":   datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }, on_conflict="email").execute()
        return {"success": True, "message": f"{email} added with role {role}"}
    except Exception as e:
        return {"success": False, "message": str(e)}


def remove_email(email: str, removed_by: str = "admin") -> dict:
    email = email.strip().lower()
    sb = _get_sb()
    if not sb:
        return {"success": False, "message": "Supabase not connected"}
    try:
        sb.table("access_control").update({
            "is_active":  False,
            "removed_by": removed_by,
            "removed_at": datetime.utcnow().isoformat(),
        }).eq("email", email).execute()
        return {"success": True, "message": f"{email} access revoked"}
    except Exception as e:
        return {"success": False, "message": str(e)}


def list_users() -> list:
    sb = _get_sb()
    if not sb:
        return []
    try:
        r = sb.table("access_control").select("*").order("added_at", desc=True).execute()
        return r.data or []
    except Exception:
        return []


def log_access(email: str, action: str = "login", success: bool = True, ip: str = ""):
    """Log access attempts for audit."""
    sb = _get_sb()
    if not sb:
        return
    try:
        sb.table("access_log").insert({
            "email":     email.strip().lower(),
            "action":    action,
            "success":   success,
            "ip":        ip,
            "timestamp": datetime.utcnow().isoformat(),
        }).execute()
    except Exception:
        pass


def get_access_logs(limit: int = 100) -> list:
    sb = _get_sb()
    if not sb:
        return []
    try:
        r = sb.table("access_log").select("*").order("timestamp", desc=True).limit(limit).execute()
        return r.data or []
    except Exception:
        return []


if __name__ == "__main__":
    print("Test allowlist:")
    print(is_allowed("ayaz@eagle3dstreaming.com"))
    print(is_allowed("random@gmail.com"))
