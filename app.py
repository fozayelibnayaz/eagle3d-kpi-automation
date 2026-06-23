"""
Eagle Analytics Hub — Unified KPI & Analytics Dashboard v7
=================================================
All-in-one: KPI, GA4, YouTube, LinkedIn, Cross-Platform Correlation
Dark/light mode, AI-powered analytics, Telegram alerts.
Pages: Dashboard, Google Analytics, YouTube, LinkedIn, Cross-Platform, Ask AI, Predictions,
Reports, Alerts, EDA Lab, Browse Data, Settings.
"""

import streamlit as st
# ── SUPABASE ENV VAR INJECTION (must run before any imports that use Supabase) ──
import os as _early_os
try:
    import streamlit as _early_st
    _early_su = str(_early_st.secrets.get("SUPABASE_URL", "")).strip()
    _early_sk = str(_early_st.secrets.get("SUPABASE_SERVICE_KEY", "")).strip()
    if _early_su:
        _early_os.environ["SUPABASE_URL"] = _early_su
    if _early_sk:
        _early_os.environ["SUPABASE_SERVICE_KEY"] = _early_sk
except Exception:
    pass

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import tempfile
import re
import base64
import sys
from pathlib import Path
from email.utils import parsedate_to_datetime

# ═══════════════════════════════════════════════════════════════
# LOGO & FAVICON
# ═══════════════════════════════════════════════════════════════
LOGO_B64 = None
try:
    _root = os.path.dirname(os.path.abspath(__file__))
    for _lf in ["static/eagle3d_logo2.png", "static/eagle3d_logo.png"]:
        _lp = os.path.join(_root, _lf)
        if os.path.exists(_lp):
            with open(_lp, "rb") as _f:
                LOGO_B64 = base64.b64encode(_f.read()).decode()
            break
except Exception:
    pass

if LOGO_B64:
    _icon = f"data:image/png;base64,{LOGO_B64}"
else:
    _icon = "🦅"

st.set_page_config(
    page_title="Eagle Analytics Hub",
    page_icon=_icon,
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── ACCESS CONTROL GATE ──
def _get_client_ip():
    """Best-effort client IP from Streamlit context."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        from streamlit.runtime import get_instance
        ctx = get_script_run_ctx()
        if ctx:
            session_info = get_instance()._session_mgr.get_session_info(ctx.session_id)
            if session_info and session_info.client:
                # Try multiple attributes
                req = getattr(session_info.client, "request", None)
                if req:
                    ip = req.headers.get("X-Forwarded-For", "").split(",")[0].strip()
                    if not ip:
                        ip = req.headers.get("X-Real-IP", "")
                    if not ip:
                        ip = getattr(req, "remote_ip", "")
                    return ip or "unknown"
    except Exception:
        pass
    # Fallback: external service
    try:
        import urllib.request, json as _j
        with urllib.request.urlopen("https://api.ipify.org?format=json", timeout=3) as r:
            return _j.loads(r.read()).get("ip", "unknown")
    except Exception:
        return "unknown"


def _enforce_access_control():
    try:
        from access_control import is_allowed, log_access
    except ImportError:
        return
    if st.session_state.get("_access_checked"):
        return

    user_email = st.session_state.get("user_email", "") or st.session_state.get("auth_email", "")

    if not user_email:
        # ── CLEAN LOGIN SCREEN: hide sidebar + center content ──
        st.markdown("""
        <style>
            [data-testid="stSidebar"] {display: none !important;}
            [data-testid="stSidebarNav"] {display: none !important;}
            [data-testid="collapsedControl"] {display: none !important;}
            section[data-testid="stSidebar"] {display: none !important;}
            .main .block-container {
                max-width: 480px !important;
                padding-top: 5rem !important;
                margin: 0 auto !important;
            }
            header {visibility: hidden;}
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
        </style>
        """, unsafe_allow_html=True)

        st.markdown("# 🔒 Eagle3D KPI Hub")
        st.markdown("### Login Required")
        st.caption("Only authorized emails can access this dashboard.")
        st.markdown("---")

        with st.form("access_login", clear_on_submit=False):
            entered = st.text_input("Email address", placeholder="you@eagle3dstreaming.com")
            submit = st.form_submit_button("🔓 Verify Access", use_container_width=True)
            if submit:
                client_ip = _get_client_ip()
                if not entered or "@" not in entered:
                    st.error("Valid email required")
                    log_access(entered or "empty", "login_attempt", False, client_ip)
                    st.stop()
                allowed, role, reason = is_allowed(entered)
                log_access(entered, "login", allowed, client_ip)
                if allowed:
                    st.session_state["user_email"] = entered.strip().lower()
                    st.session_state["user_role"] = role
                    st.session_state["user_ip"] = client_ip
                    st.session_state["_access_checked"] = True
                    st.success(f"Welcome! Role: {role} | IP: {client_ip}")
                    st.rerun()
                else:
                    st.error(f"Access denied: {reason}")
                    st.caption(f"Attempt logged from IP: {client_ip}")
                    st.stop()

        st.markdown("---")
        st.caption("Need access? Contact your administrator.")
        st.stop()
    else:
        allowed, role, reason = is_allowed(user_email)
        if not allowed:
            st.error(f"Access revoked: {reason}")
            if st.button("Sign out"):
                for k in ("user_email", "user_role", "_access_checked", "user_ip"):
                    st.session_state.pop(k, None)
                st.rerun()
            st.stop()
        st.session_state["user_role"] = role
        st.session_state["_access_checked"] = True

_enforce_access_control()

# ═══════════════════════════════════════════════════════════════
# THEME ENGINE
# ═══════════════════════════════════════════════════════════════
# Theme: Dark mode only (no light mode switcher)
IS_DARK = True


def _theme_colors():
    if IS_DARK:
        return {
            "bg": "#060D1A", "surface": "#0D1829", "card": "#111D32",
            "card_alt": "#152240", "border": "#1B3054",
            "accent": "#00D4FF", "accent2": "#6C5CE7",
            "text": "#E8EDF5", "text_sec": "#94A3C1", "muted": "#5B6B85",
            "green": "#00E676", "yellow": "#FFD600", "red": "#FF5252",
            "plot_bg": "rgba(0,0,0,0)", "paper_bg": "rgba(0,0,0,0)",
            "font_c": "#94A3C1",
            "input_bg": "#0D1829", "input_text": "#E8EDF5",
            "sidebar_bg": "#0A1222", "df_header_bg": "#152240",
            "df_cell_bg": "#111D32",
        }
    else:
        return {
            "bg": "#F0F4F8", "surface": "#FFFFFF", "card": "#FFFFFF",
            "card_alt": "#EDF2F7", "border": "#CBD5E1",
            "accent": "#0077B6", "accent2": "#5B4FCF",
            "text": "#0F172A", "text_sec": "#334155", "muted": "#64748B",
            "green": "#16A34A", "yellow": "#CA8A04", "red": "#DC2626",
            "plot_bg": "#FFFFFF", "paper_bg": "#FFFFFF",
            "font_c": "#334155",
            "input_bg": "#FFFFFF", "input_text": "#0F172A",
            "sidebar_bg": "#F8FAFC", "df_header_bg": "#F1F5F9",
            "df_cell_bg": "#FFFFFF",
        }


T = _theme_colors()


def _css():
    """Inject comprehensive CSS for dark mode."""

    st.markdown(f"""
    <style>
    /* ═══ ROOT VARIABLES ═══ */
    :root {{
        --bg:{T['bg']}; --surface:{T['surface']}; --card:{T['card']};
        --card-alt:{T['card_alt']}; --border:{T['border']};
        --accent:{T['accent']}; --accent2:{T['accent2']};
        --text:{T['text']}; --text-sec:{T['text_sec']}; --muted:{T['muted']};
        --green:{T['green']}; --yellow:{T['yellow']}; --red:{T['red']};
    }}

    /* ═══ GLOBAL ═══ */
    .stApp {{ background: var(--bg) !important; }}
    .stMarkdown, .stText {{ color: var(--text); }}
    html {{ scroll-behavior: smooth; }}

    /* ═══ FADE-IN ANIMATION ═══ */
    @keyframes fadeInUp {{
        from {{ opacity: 0; transform: translateY(18px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    @keyframes glowPulse {{
        0%, 100% {{ box-shadow: 0 0 8px {T['accent']}10; }}
        50% {{ box-shadow: 0 0 20px {T['accent']}25; }}
    }}
    @keyframes shimmer {{
        0% {{ background-position: -200% 0; }}
        100% {{ background-position: 200% 0; }}
    }}
    .kpi, .sec-head, .stDataFrame, .alert-card, .comp-box {{
        animation: fadeInUp 0.45s ease-out both;
    }}
    .kpi:nth-child(1) {{ animation-delay: 0.02s; }}
    .kpi:nth-child(2) {{ animation-delay: 0.06s; }}
    .kpi:nth-child(3) {{ animation-delay: 0.10s; }}
    .kpi:nth-child(4) {{ animation-delay: 0.14s; }}
    .kpi:nth-child(5) {{ animation-delay: 0.18s; }}
    .kpi:nth-child(6) {{ animation-delay: 0.22s; }}

    /* ═══ SIDEBAR ═══ */
    [data-testid="stSidebar"] {{
        background: {T['sidebar_bg']} !important;
        border-right: 1px solid var(--border) !important;
    }}
    [data-testid="stSidebarNav"] {{ display: none !important; }}
    [data-testid="stSidebar"] .stButton button {{
        color: var(--text-sec); font-size: 0.82rem; font-weight: 500;
        padding: 8px 14px; border-radius: 10px;
        border: 1px solid transparent; margin: 1px 0;
        transition: all 0.2s ease; background: transparent;
    }}
    [data-testid="stSidebar"] .stButton button:hover {{
        background: var(--card-alt) !important;
        border-color: var(--border) !important;
        transform: translateX(3px);
        color: var(--text) !important;
    }}
    [data-testid="stSidebar"] .stButton button:active {{
        background: linear-gradient(135deg, {T['accent']}22, {T['accent2']}18) !important;
        border-color: var(--accent) !important;
        color: var(--accent) !important;
    }}
    [data-testid="stSidebar"] .streamlit-expanderHeader {{
        font-size: 0.75rem; font-weight: 700; letter-spacing: 0.5px;
        color: var(--muted); text-transform: uppercase;
    }}

    /* ═══ KPI GRID ═══ */
    .kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
        gap: 12px; margin: 12px 0 20px;
    }}
    .kpi {{
        background: linear-gradient(145deg, var(--card), var(--card-alt));
        border: 1px solid var(--border); border-radius: 14px;
        padding: 18px 14px; text-align: center;
        transition: all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
        position: relative; overflow: hidden;
    }}
    .kpi::before {{
        content: ''; position: absolute; top: 0; left: -100%;
        width: 100%; height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.03), transparent);
        transition: left 0.6s;
    }}
    .kpi:hover::before {{ left: 100%; }}
    .kpi:hover {{
        transform: translateY(-3px) scale(1.02);
        box-shadow: 0 8px 32px {T['accent']}20;
        border-color: var(--accent);
    }}
    .kpi-val {{
        font-size: 1.8rem; font-weight: 800;
        color: var(--text); line-height: 1.15;
    }}
    .kpi-lbl {{
        font-size: 0.68rem; color: var(--muted);
        text-transform: uppercase; letter-spacing: 1px;
        margin-top: 5px; font-weight: 600;
    }}
    .kpi-delta {{ font-size: 0.78rem; font-weight: 700; margin-top: 4px; }}
    .d-up {{ color: var(--green); }} .d-dn {{ color: var(--red); }}
    .d-fl {{ color: var(--muted); }}

    /* ═══ SECTION HEADERS ═══ */
    .sec-head {{
        font-size: 1.15rem; font-weight: 700; color: var(--accent);
        border-bottom: 2px solid var(--border);
        padding-bottom: 8px; margin: 22px 0 14px;
    }}

    /* ═══ BADGES ═══ */
    .badge {{
        padding: 3px 10px; border-radius: 20px;
        font-size: 0.7rem; font-weight: 600;
        display: inline-block;
    }}
    .badge-ok {{ background: {T['green']}20; color: var(--green); }}
    .badge-warn {{ background: {T['yellow']}20; color: var(--yellow); }}
    .badge-err {{ background: {T['red']}20; color: var(--red); }}
    .badge-info {{ background: {T['accent']}18; color: var(--accent); }}

    /* ═══ ALERT CARDS ═══ */
    .alert-card {{
        border-radius: 12px; padding: 14px 18px;
        margin: 8px 0; border-left: 4px solid;
    }}
    .al-crit {{ background: {T['red']}15; border-color: var(--red); }}
    .al-warn {{ background: {T['yellow']}15; border-color: var(--yellow); }}
    .al-ok {{ background: {T['green']}15; border-color: var(--green); }}

    /* ═══ CHAT ═══ */
    .chat-msg {{
        padding: 10px 14px; border-radius: 12px;
        margin: 6px 0; max-width: 92%;
        font-size: 0.9rem; line-height: 1.5;
    }}
    .chat-user {{
        background: {T['accent']}15;
        border: 1px solid var(--border); margin-left: auto;
    }}
    .chat-ai {{
        background: var(--card);
        border: 1px solid var(--border);
    }}

    /* ═══ INFO BOX ═══ */
    .comp-box {{
        background: var(--card); border: 1px solid var(--border);
        border-radius: 10px; padding: 10px 14px;
        margin: 8px 0; font-size: 0.82rem;
    }}

    /* ═══ DATAFRAME ═══ */
    .stDataFrame {{
        border: 1px solid var(--border);
        border-radius: 10px; overflow: hidden;
    }}

    /* ═══ BUTTONS & INPUTS ═══ */
    .stButton button {{
        border-radius: 10px !important;
        font-weight: 600 !important;
        transition: all 0.2s ease !important;
    }}
    .stButton button:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 16px rgba(0,0,0,0.3);
    }}
    .stTextInput input, .stSelectbox, .stDateInput, .stNumberInput {{
        border-radius: 8px !important;
    }}

    /* ═══ TABS ═══ */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 2px; border-radius: 10px;
        background: var(--card-alt);
        padding: 4px;
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 8px !important;
        padding: 6px 16px !important;
        font-size: 0.82rem !important;
        transition: all 0.2s;
    }}
    .stTabs [aria-selected="true"] {{
        background: var(--card) !important;
        border-color: var(--accent) !important;
    }}

    /* ═══ METRIC CARDS ═══ */
    [data-testid="stMetric"] {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 12px 16px;
        transition: all 0.2s;
    }}
    [data-testid="stMetric"]:hover {{
        border-color: var(--accent);
        box-shadow: 0 4px 16px {T['accent']}12;
    }}

    /* ═══ RESPONSIVE ═══ */
    @media (max-width: 1024px) {{
        .kpi-grid {{
            grid-template-columns: repeat(3, 1fr); gap: 10px;
        }}
    }}
    @media (max-width: 768px) {{
        .kpi-grid {{
            grid-template-columns: repeat(2, 1fr); gap: 8px;
        }}
        .kpi-val {{ font-size: 1.4rem; }}
        [data-testid="stMetric"] {{ padding: 8px 10px; }}
    }}
    @media (max-width: 480px) {{
        .kpi-grid {{
            grid-template-columns: 1fr 1fr; gap: 6px;
        }}
        .kpi-val {{ font-size: 1.1rem; }}
        .kpi {{ padding: 10px 8px; }}
        .stTabs [data-baseweb="tab"] {{ padding: 4px 10px !important; font-size: 0.7rem !important; }}
        [data-testid="column"] {{ min-width: auto !important; }}
    }}
    @media (max-width: 360px) {{
        .kpi-grid {{ grid-template-columns: 1fr; }}
    }}

    /* ═══ DESIGN SYSTEM: Typography & Spacing ═══ */
    body, .stApp {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; }}
    h1, h2, h3, h4, h5, h6 {{ font-weight: 700; letter-spacing: -0.02em; }}
    h1 {{ font-size: 1.65rem; }} h2 {{ font-size: 1.35rem; }}
    h3 {{ font-size: 1.1rem; }} h4 {{ font-size: 1rem; }}

    /* ═══ CARD COMPONENT ═══ */
    .card {{
        background: var(--card); border: 1px solid var(--border);
        border-radius: 14px; padding: 20px; margin: 12px 0;
        transition: all 0.3s ease;
    }}
    .card:hover {{ border-color: var(--accent); box-shadow: 0 4px 24px {T['accent']}15; }}

    /* ═══ EXPANDER REFINEMENT ═══ */
    .streamlit-expanderHeader {{
        font-weight: 700 !important; font-size: 0.85rem !important;
        border-radius: 10px !important; padding: 8px 12px !important;
        transition: background 0.2s;
    }}
    .streamlit-expanderHeader:hover {{
        background: var(--card-alt) !important;
    }}

    /* ═══ SELECT/SLIDER/INPUT REFINEMENT ═══ */
    div[data-baseweb="select"] > div, div[data-baseweb="input"] > div {{
        border-radius: 8px !important;
        border-color: var(--border) !important;
    }}
    div[data-baseweb="slider"] > div {{
        background: var(--border) !important;
    }}

    /* ═══ PROGRESS BAR ═══ */
    .stProgress > div > div > div {{
        background: linear-gradient(90deg, var(--accent), var(--accent2)) !important;
        border-radius: 4px !important;
    }}

    /* ═══ DIVIDER ═══ */
    hr {{
        border-color: var(--border) !important;
        opacity: 0.5;
    }}

    /* ═══ TOAST / SUCCESS / ERROR ═══ */
    .stAlert {{
        border-radius: 10px !important;
        border: 1px solid var(--border) !important;
    }}

    /* ═══ CLEANUP ═══ */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    </style>
    """, unsafe_allow_html=True)


_css()

# ═══════════════════════════════════════════════════════════════
# AUTHENTICATION GATE
# ═══════════════════════════════════════════════════════════════


def _get_app_password():
    """Get password from secrets first, then fallback to default."""
    try:
        if "APP_PASSWORD" in st.secrets:
            val = st.secrets["APP_PASSWORD"]
            if val and str(val).strip():
                return str(val).strip()
    except Exception:
        pass
    return os.environ.get("APP_PASSWORD", "eagleanalytics")


_APP_PASSWORD = _get_app_password()


def _check_auth():
    """Password-gate for the dashboard with proper centered login UI."""
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
    if st.session_state["authenticated"]:
        return True
    # Login screen — hide sidebar, centered panel
    st.markdown("""
    <style>
    [data-testid="stSidebar"] {display: none !important;}
    [data-testid="stSidebarNav"] {display: none !important;}
    .block-container {padding-top: 8rem !important;}
    </style>
    """, unsafe_allow_html=True)

    # Centered login card
    _login_logo = ""
    if LOGO_B64:
        _login_logo = f'<img src="data:image/png;base64,{LOGO_B64}" style="width:72px;height:auto;border-radius:14px;margin-bottom:0.8rem;">'
    else:
        _login_logo = '<div style="font-size:2.8rem;margin-bottom:0.5rem;">🦅</div>'
    st.markdown(f"""
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
                min-height:50vh;text-align:center;">
        {_login_logo}
        <div style="font-size:1.8rem;font-weight:800;color:#00D4FF;margin-bottom:0.3rem;">
            Eagle Analytics Hub</div>
        <div style="font-size:0.85rem;color:#94A3C1;margin-bottom:0.2rem;">
            Eagle 3D Streaming — Unified Command Center</div>
        <div style="font-size:0.75rem;color:#5B6B85;margin-bottom:2rem;">
            Secure Dashboard — Authentication Required</div>
    </div>
    """, unsafe_allow_html=True)
    _cols = st.columns([3, 2, 3])
    with _cols[1]:
        _pwd = st.text_input("Password", type="password", key="_auth_pwd",
                             placeholder="Enter password...")
        _login_btn = st.button("🔓 Sign In", use_container_width=True, key="_auth_btn")
        if _login_btn:
            if _pwd == _APP_PASSWORD:
                st.session_state["authenticated"] = True
                st.toast("✅ Access granted", icon="🦅")
                st.rerun()
            else:
                st.error("❌ Incorrect password. Try again.")
        st.caption("🔒 This dashboard is private and password-protected.")
    return False


if not _check_auth():
    st.stop()


# ═══════════════════════════════════════════════════════════════
# COMPATIBILITY HELPERS
# ═══════════════════════════════════════════════════════════════


def _pc(fig, **kwargs):
    """Plot chart — handles both old and new Streamlit API."""
    try:
        st.plotly_chart(fig, width="stretch", **kwargs)
    except TypeError:
        try:
            st.plotly_chart(fig, use_container_width=True, **kwargs)
        except TypeError:
            st.plotly_chart(fig, **kwargs)


def _df(df, height=400, **kwargs):
    """Display dataframe — handles both old and new API."""
    try:
        st.dataframe(df, width="stretch", height=height,
                     hide_index=True, **kwargs)
    except TypeError:
        try:
            st.dataframe(df, use_container_width=True, height=height,
                         hide_index=True, **kwargs)
        except TypeError:
            st.dataframe(df, height=height, hide_index=True, **kwargs)


# ═══════════════════════════════════════════════════════════════
# MODULE LOADING
# ═══════════════════════════════════════════════════════════════
MOD = {}


def _imp(name, frm=None):
    try:
        if frm:
            m = __import__(frm)
            for p in frm.split(".")[1:]:
                m = getattr(m, p)
        else:
            m = __import__(name)
        MOD[name] = m
        return True
    except Exception as e:
        MOD[name + "_err"] = str(e)
        return False


for _n, _f in [
    ("kpi_bridge", None), ("ga4_connector", None),
    ("source_intel", "ga4_source_intel"),
    ("smart_qa", "ga4_smart_qa"), ("strategic", "ga4_strategic"),
    ("notifications", "ga4_notifications"),
    ("intelligence", "ga4_intelligence"),
    ("ai_engine", None), ("prediction_engine", None),
    ("report_generator", None), ("source_normalizer", None),
    ("manual_override_engine", None),
]:
    _imp(_n, _f)

# ═══════════════════════════════════════════════════════════════
# CREDENTIALS
# ═══════════════════════════════════════════════════════════════
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_creds_path():
    if os.path.exists("google_creds.json"):
        return "google_creds.json"
    for k in ["GOOGLE_CREDS", "GOOGLE_CREDS_JSON"]:
        try:
            r = st.secrets[k]
            c = json.loads(r) if isinstance(r, str) else dict(r)
            t = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(c, t)
            t.close()
            return t.name
        except Exception:
            pass
    return None



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

def get_secret(k, d=None):
    try:
        if k in st.secrets:
            return st.secrets[k]
    except Exception:
        pass
    return d


CREDS_PATH = get_creds_path()
MASTER_SHEET_URL = get_secret("MASTER_SHEET_URL")

# Fallback: try config.py for MASTER_SHEET_URL when not in secrets
if not MASTER_SHEET_URL:
    try:
        import config as _cfg
        MASTER_SHEET_URL = getattr(_cfg, "MASTER_SHEET_URL", "")
    except Exception:
        pass

# Fallback: try ga4_service_account for Google Sheets credentials
if not CREDS_PATH:
    try:
        _sa = dict(st.secrets["ga4_service_account"])
        if "private_key" in _sa:
            _sa["private_key"] = _sa["private_key"].replace("\\n", "\n")
        _tf = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(_sa, _tf)
        _tf.close()
        CREDS_PATH = _tf.name
    except Exception:
        pass



# ── SUPABASE FAST KPI STATS ──
@st.cache_data(ttl=60)
def get_supabase_kpi_fast():
    """Get KPI counts directly from Supabase - fast and accurate."""
    try:
        import os
        from supabase import create_client
        _url = os.environ.get("SUPABASE_URL","")
        _key = os.environ.get("SUPABASE_SERVICE_KEY","")
        if not _url:
            _url = str(st.secrets.get("SUPABASE_URL","")).strip()
        if not _key:
            _key = str(st.secrets.get("SUPABASE_SERVICE_KEY","")).strip()
        if not _url or not _key:
            return None
        _sb = create_client(_url, _key)
        _today = datetime.now().strftime("%Y-%m-%d")
        _month_start = datetime.now().strftime("%Y-%m-01")
        # Upload coverage start = common period start
        _upload_start = "2025-12-01"
        try:
            _ur = _sb.table("uploads").select("upload_date").eq("final_status","ACCEPTED").order("upload_date").limit(1).execute()
            if _ur.data and _ur.data[0].get("upload_date"):
                _upload_start = _ur.data[0]["upload_date"][:10]
        except Exception:
            pass
        # Today counts
        _st = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_today).execute()
        _ut = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_today).execute()
        _pt = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_today).execute()
        # Month counts
        _sm = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_month_start).execute()
        _um = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_month_start).execute()
        _pm = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_month_start).execute()
        # Common period (from upload start = Dec 2025)
        _sc = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",_upload_start).execute()
        _uc = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",_upload_start).execute()
        _pc = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",_upload_start).execute()
        # Full DB totals
        _sf = _sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").execute()
        _uf = _sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").execute()
        _pf = _sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").execute()
        return {
            "today_signups":   _st.count or 0,
            "today_uploads":   _ut.count or 0,
            "today_paid":      _pt.count or 0,
            "month_signups":   _sm.count or 0,
            "month_uploads":   _um.count or 0,
            "month_paid":      _pm.count or 0,
            "common_signups":  _sc.count or 0,
            "common_uploads":  _uc.count or 0,
            "common_paid":     _pc.count or 0,
            "full_signups":    _sf.count or 0,
            "full_uploads":    _uf.count or 0,
            "full_paid":       _pf.count or 0,
            "common_start":    _upload_start,
            "today":           _today,
            "month_start":     _month_start,
        }
    except Exception as _e:
        return None


# ── FAST BROWSE DATA: server-side filtered from Supabase ──
@st.cache_data(ttl=60)
def _browse_supabase(table_key, status_filter, search_val, date_start, date_end, limit=1000):
    """Server-side filtered browse data from Supabase - much faster than client-side."""
    try:
        import os
        from supabase import create_client as _sb_cc
        _url = os.environ.get("SUPABASE_URL","")
        _key = os.environ.get("SUPABASE_SERVICE_KEY","")
        if not _url:
            try:
                _url = str(st.secrets.get("SUPABASE_URL","")).strip()
                _key = str(st.secrets.get("SUPABASE_SERVICE_KEY","")).strip()
            except Exception:
                pass
        if not _url or not _key:
            return None, {}

        _sb = _sb_cc(_url, _key)

        _table_map = {
            "Sign-ups":     ("signups",  "signup_date",        "email"),
            "First Uploads":("uploads",  "upload_date",        "email"),
            "Stripe":       ("payments", "first_payment_date", "email"),
        }
        if table_key not in _table_map:
            return None, {}

        _tbl, _date_col, _email_col = _table_map[table_key]

        # Build query
        _q = _sb.table(_tbl).select("*")

        # Date filter
        if date_start:
            _q = _q.gte(_date_col, str(date_start))
        if date_end:
            _q = _q.lte(_date_col, str(date_end))

        # Status filter
        if status_filter and status_filter != "All":
            _q = _q.eq("final_status", status_filter.upper())

        # Search (email only for server-side)
        if search_val and "@" in search_val:
            _q = _q.ilike(_email_col, f"%{search_val}%")

        _q = _q.order(_date_col, desc=True).limit(limit)
        _resp = _q.execute()
        _data = _resp.data or []

        _total = len(_data)
        _accepted = sum(1 for r in _data if str(r.get("final_status","")).upper() == "ACCEPTED")
        _rejected = _total - _accepted

        _diag = {
            "raw_total": _total,
            "after_date_filter": _total,
            "after_status_filter": _total,
            "after_search_filter": _total,
            "source": "supabase",
            "table": _tbl,
            "date_col": _date_col,
            "accepted": _accepted,
            "rejected": _rejected,
        }

        return pd.DataFrame(_data), _diag
    except Exception as _e:
        return None, {"error": str(_e)}



@st.cache_data(ttl=300)
def load_sheet(tab):
    # PRIMARY: Supabase - fast, no cold start, no quota limits
    if _SUPABASE_ACTIVE and _sb_load_tab is not None:
        try:
            _df = _sb_load_tab(tab)
            if _df is not None and not _df.empty:
                return _df
        except Exception:
            pass
    # SECONDARY: local JSON cache (instant, no network)
    _cache_map = {
        "Daily_Counts":        "data_output/daily_counts.json",
        "Verified_FREE":       None,
        "Verified_FIRST_UPLOAD": None,
        "Verified_STRIPE":     None,
    }
    if tab in _cache_map and _cache_map[tab]:
        try:
            import json as _json
            _p = Path(_cache_map[tab])
            if _p.exists():
                _data = _json.loads(_p.read_text())
                if _data:
                    return pd.DataFrame(_data)
        except Exception:
            pass
    # TERTIARY: Google Sheets (slow fallback only)
    if not MASTER_SHEET_URL:
        return pd.DataFrame()
    _creds = CREDS_PATH
    # Try to build credentials if we don't have them yet
    if not _creds:
        for _sk in ["GOOGLE_CREDS_JSON", "GOOGLE_CREDS"]:
            try:
                _r = st.secrets[_sk]
                _c = json.loads(_r) if isinstance(_r, str) else dict(_r)
                if "private_key" in _c:
                    _c["private_key"] = _c["private_key"].replace("\\n", "\n")
                _t = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
                json.dump(_c, _t)
                _t.close()
                _creds = _t.name
                break
            except Exception:
                pass
    if not _creds:
        try:
            _sa = dict(st.secrets["ga4_service_account"])
            if "private_key" in _sa:
                _sa["private_key"] = _sa["private_key"].replace("\\n", "\n")
            _t = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(_sa, _t)
            _t.close()
            _creds = _t.name
        except Exception:
            pass
    if not _creds:
        return pd.DataFrame()
    try:
        # import gspread  # disabled - using Supabase
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(_creds, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(MASTER_SHEET_URL)
        ws = sh.worksheet(tab)
        data = ws.get_all_values()
        if len(data) < 2:
            return pd.DataFrame()
        hdrs = data[0]
        seen = {}
        clean = []
        for h in hdrs:
            h = h.strip() if h else "unknown"
            if h in seen:
                seen[h] += 1
                clean.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                clean.append(h)
        return pd.DataFrame(data[1:], columns=clean)
    except Exception:
        return pd.DataFrame()


def parse_to_date(raw):
    if not raw or str(raw).strip() in ("", "-", "—", "nan", "None", "N/A"):
        return None
    s = str(raw).strip()
    # Try email.utils RFC 2822 parser (handles many web formats)
    try:
        return parsedate_to_datetime(s).date()
    except Exception:
        pass
    # Exhaustive format list — covers KPI Dashboard, Stripe, Google Sheets, etc.
    for fmt in [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
        "%m/%d/%y, %I:%M %p", "%m/%d/%Y, %I:%M %p",
        "%m/%d/%y, %I:%M:%S %p", "%m/%d/%Y, %I:%M:%S %p",
        "%m/%d/%y %I:%M %p", "%m/%d/%Y %I:%M %p",
        "%m/%d/%y", "%m/%d/%Y",
        "%d/%m/%Y", "%d/%m/%y",
        "%b %d, %Y", "%d %b %Y",
        "%a %b %d %Y %H:%M:%S", "%a %b %d %Y",
        "%Y/%m/%d", "%Y.%m.%d",
    ]:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    # Regex fallback: extract YYYY-MM-DD
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
        except Exception:
            pass
    # Regex fallback: extract MM/DD/YY or MM/DD/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if m:
        try:
            y = int(m.group(3))
            if y < 100:
                y += 2000
            return datetime(y, int(m.group(1)), int(m.group(2))).date()
        except Exception:
            pass
    return None


# ═══════════════════════════════════════════════════════════════
# DATE ENGINE
# ═══════════════════════════════════════════════════════════════
DATE_PRESETS = [
    "Today", "Yesterday", "This Week", "Last Week", "Last 7 Days",
    "Last 14 Days", "Last 28 Days", "Last 30 Days", "This Month",
    "Last Month", "Last 3 Months", "Last 6 Months", "Last 12 Months",
    "This Year", "Last Year", "All Time", "Custom Range",
]


def get_date_range(preset, cs=None, ce=None):
    t = datetime.now().date()
    y = t - timedelta(days=1)
    R = {
        "Today": (t, t),
        "Yesterday": (y, y),
        "This Week": (t - timedelta(days=t.weekday()), t),
        "Last 7 Days": (t - timedelta(days=6), t),
        "Last 14 Days": (t - timedelta(days=13), t),
        "Last 28 Days": (t - timedelta(days=27), t),
        "Last 30 Days": (t - timedelta(days=29), t),
        "This Month": (t.replace(day=1), t),
        "This Year": (t.replace(month=1, day=1), t),
        "All Time": (datetime(2024, 1, 1).date(), t),
    }
    lwe = t - timedelta(days=t.weekday() + 1)
    R["Last Week"] = (lwe - timedelta(days=6), lwe)
    fp = t.replace(day=1)
    lp = fp - timedelta(days=1)
    R["Last Month"] = (lp.replace(day=1), lp)
    R["Last 3 Months"] = (t - timedelta(days=90), t)
    R["Last 6 Months"] = (t - timedelta(days=180), t)
    R["Last 12 Months"] = (t - timedelta(days=365), t)
    R["Last Year"] = (datetime(t.year - 1, 1, 1).date(),
                      datetime(t.year - 1, 12, 31).date())
    if preset == "Custom Range" and cs and ce:
        return cs, ce
    return R.get(preset, (t - timedelta(days=27), t))


def get_comp_range(ps, pe, mode, cs=None, ce=None):
    if mode == "Previous Period":
        span = (pe - ps).days + 1
        e = ps - timedelta(days=1)
        return e - timedelta(days=span - 1), e
    if mode == "Same Period Last Year":
        try:
            return ps.replace(year=ps.year - 1), pe.replace(year=pe.year - 1)
        except ValueError:
            return None, None
    if mode == "Custom Comparison" and cs and ce:
        return cs, ce
    return None, None


# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def CT():
    """Common Plotly theme."""
    return {
        "paper_bgcolor": T["paper_bg"],
        "plot_bgcolor": T["plot_bg"],
        "font_color": T["font_c"],
        "title_font_color": T["text"],
        "legend_bgcolor": T["card"],
    }


CC = [
    "#00D4FF", "#6C5CE7", "#00E676", "#FFD600",
    "#FF5252", "#B388FF", "#FF9100", "#00BFA5",
]


def filter_kpi(df, s, e):
    if df is None or df.empty or "date" not in df.columns:
        return pd.DataFrame()
    d = df.copy()
    d["date"] = d["date"].astype(str)
    # Try multiple parse strategies
    _parsed = pd.to_datetime(d["date"], errors="coerce")
    if _parsed.isna().all() and d["date"].str.match(r"^\d{5}$").any():
        _parsed = pd.to_datetime(d["date"].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:8]}"), errors="coerce")
    d["_d"] = _parsed.dt.date
    d = d.dropna(subset=["_d"])
    return d[(d["_d"] >= s) & (d["_d"] <= e)].drop(columns=["_d"])


def km(df, col):
    """Sum of numeric column, safely."""
    if df is None or df.empty or col not in df.columns:
        return 0
    return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def fmt_val(v):
    """Format value for KPI card — handles int, float, and string."""
    if isinstance(v, (int, float)):
        try:
            if isinstance(v, float):
                return f"{v:,.1f}"
            return f"{v:,}"
        except (ValueError, TypeError):
            return str(v)
    return str(v)


def delta_h(c, p, inv=False):
    """Delta HTML for KPI card."""
    try:
        c = float(c)
        p = float(p)
    except (ValueError, TypeError):
        return '<span class="kpi-delta d-fl">→ —</span>'
    if p == 0:
        if c > 0:
            return f'<span class="kpi-delta d-up">{int(c):,} total</span>'
        return '<span class="kpi-delta d-fl">→ —</span>'
    pct = (c - p) / p * 100
    pos = pct > 0 if not inv else pct < 0
    if abs(pct) < 0.5:
        return '<span class="kpi-delta d-fl">→ flat</span>'
    if pos:
        return f'<span class="kpi-delta d-up">▲ {pct:+.1f}%</span>'
    return f'<span class="kpi-delta d-dn">▼ {abs(pct):.1f}%</span>'


def kpi_h(label, value, delta, icon=""):
    """KPI card HTML — uses fmt_val to handle any value type."""
    fv = fmt_val(value)
    return (
        f'<div class="kpi">'
        f'<div class="kpi-val">{icon} {fv}</div>'
        f'<div class="kpi-lbl">{label}</div>'
        f'{delta}'
        f'</div>'
    )


def fd(d):
    return d.strftime("%Y-%m-%d") if d else "?"


# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    # Logo
    if LOGO_B64:
        st.markdown(
            f'<div style="text-align:center;padding:6px 0;">'
            f'<img src="data:image/png;base64,{LOGO_B64}" '
            f'style="width:72px;height:auto;border-radius:10px;">'
            f"</div>",
            unsafe_allow_html=True,
        )
    st.markdown(
        '<div style="text-align:center;padding:2px 0 6px 0;">'
        '<a href="https://eagle3d-kpi-automation.streamlit.app/" target="_blank" '
        'style="font-size:0.9rem;font-weight:800;color:var(--accent);'
        'letter-spacing:0.5px;text-decoration:none;">Eagle Analytics Hub 🦅</a>'
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # Navigation — organized into expandable category sections
    if "nav_page" not in st.session_state:
        st.session_state["nav_page"] = "📊 Dashboard"

    _SYS_PAGES = {"📈 Google Analytics", "📺 YouTube", "💼 LinkedIn", "🎯 Customer Success", "🔗 Cross-Platform"}
    _KPI_PAGES = {"📊 Dashboard", "🔍 Browse Data", "🔬 EDA Lab", "✏️ Manual Override", "📋 Reports", "🔔 Alerts"}
    _AI_PAGES = {"🤖 Ask AI", "🤖 AI KPI", "🤖 AI YouTube", "🤖 AI LinkedIn", "🤖 AI GA4", "🤖 AI CS", "🔮 Predictions", "🧠 AI Tools"}

    page = st.session_state["nav_page"]

    with st.expander("📊 KPI", expanded=page in _KPI_PAGES):
        for _label, _key in [("Dashboard", "📊 Dashboard"), ("Browse Data", "🔍 Browse Data"),
                             ("EDA Lab", "🔬 EDA Lab"), ("Manual Override", "✏️ Manual Override"),
                             ("Reports", "📋 Reports"), ("Alerts", "🔔 Alerts")]:
            if st.button(_label, use_container_width=True, key=f"nav_{_key}"):
                st.session_state["nav_page"] = _key
                st.rerun()

    with st.expander("📈 Google Analytics", expanded=page == "📈 Google Analytics"):
        if st.button("📈 Google Analytics", use_container_width=True, key="nav_ga"):
            st.session_state["nav_page"] = "📈 Google Analytics"
            st.rerun()

    with st.expander("📺 YouTube", expanded=page == "📺 YouTube"):
        if st.button("📺 YouTube", use_container_width=True, key="nav_yt"):
            st.session_state["nav_page"] = "📺 YouTube"
            st.rerun()

    with st.expander("💼 LinkedIn", expanded=page == "💼 LinkedIn"):
        if st.button("💼 LinkedIn", use_container_width=True, key="nav_li"):
            st.session_state["nav_page"] = "💼 LinkedIn"
            st.rerun()

    with st.expander("🎯 Customer Success", expanded=page == "🎯 Customer Success"):
        if st.button("🎯 Customer Success", use_container_width=True, key="nav_cs"):
            st.session_state["nav_page"] = "🎯 Customer Success"
            st.rerun()

    with st.expander("🔗 Cross-Platform", expanded=page == "🔗 Cross-Platform"):
        if st.button("🔗 Cross-Platform", use_container_width=True, key="nav_cp"):
            st.session_state["nav_page"] = "🔗 Cross-Platform"
            st.rerun()

    with st.expander("🤖 AI & Insights", expanded=page in _AI_PAGES):
        for _label, _key in [("Ask AI (All)", "🤖 Ask AI"),
                              ("AI - KPI", "🤖 AI KPI"),
                              ("AI - YouTube", "🤖 AI YouTube"),
                              ("AI - LinkedIn", "🤖 AI LinkedIn"),
                              ("AI - GA4", "🤖 AI GA4"),
                              ("AI - Customer Success", "🤖 AI CS"),
                              ("AI Tools", "🧠 AI Tools"),
                              ("Predictions", "🔮 Predictions")]:
            if st.button(_label, use_container_width=True, key=f"nav_{_key}"):
                st.session_state["nav_page"] = _key
                st.rerun()

    st.markdown("---")

    # Refresh data button
    if st.button("🔄 Refresh Data", use_container_width=True, key="nav_refresh"):
        st.cache_data.clear()
        st.session_state["_last_refresh"] = datetime.now().strftime("%H:%M:%S")
        st.rerun()
    # Pipeline trigger button (delegated to GitHub Actions)
    _gh_tok = get_secret("GITHUB_TOKEN", "")
    if _gh_tok:
        _repo = "fozayelibnayaz/eagle3d-kpi-automation"
        if st.button("🚀 Run Pipeline", use_container_width=True, key="nav_pipeline"):
            try:
                import urllib.request
                _url = (
                    f"https://api.github.com/repos/{_repo}"
                    "/actions/workflows/daily_pipeline.yml/dispatches"
                )
                _data = json.dumps({"ref": "main"}).encode()
                _req = urllib.request.Request(
                    _url, data=_data, method="POST",
                    headers={
                        "Authorization": f"token {_gh_tok}",
                        "Accept": "application/vnd.github+json"
                    }
                )
                with urllib.request.urlopen(_req, timeout=10) as _r:
                    if _r.status in (200, 204):
                        st.toast("🚀 Pipeline triggered! Refresh in ~5 min")
            except Exception:
                st.error("Pipeline trigger failed")

    st.markdown("---")

    # Universal Settings page (always accessible)
    if st.button("⚙️ Settings", use_container_width=True, key="nav_settings"):
        st.session_state["_go_settings"] = True
        st.rerun()

    # Show last refresh time
    _last_ref = st.session_state.get("_last_refresh", "")
    if _last_ref:
        st.caption(f"🔄 Refreshed at {_last_ref}")

    st.markdown("---")

    # Date range
    date_preset = st.selectbox("Period", DATE_PRESETS, index=8,
                               label_visibility="collapsed")
    custom_s = custom_e = None
    if date_preset == "Custom Range":
        _a, _b = st.columns(2)
        with _a:
            custom_s = st.date_input("Start",
                                     value=datetime.now().date() - timedelta(days=30))
        with _b:
            custom_e = st.date_input("End",
                                     value=datetime.now().date())
    p_start, p_end = get_date_range(date_preset, custom_s, custom_e)

    st.markdown("---")

    # Comparison toggle
    enable_comp = st.toggle("🔄 Enable Comparison", value=False)
    comp_mode = "None"
    comp_s = comp_e = None
    if enable_comp:
        comp_mode = st.selectbox(
            "Compare To",
            ["Previous Period", "Same Period Last Year", "Custom Comparison"],
        )
        if comp_mode == "Custom Comparison":
            _a2, _b2 = st.columns(2)
            with _a2:
                comp_s = st.date_input("Comp Start",
                                       value=p_start - timedelta(days=28))
            with _b2:
                comp_e = st.date_input("Comp End",
                                       value=p_start - timedelta(days=1))
        else:
            comp_s, comp_e = get_comp_range(p_start, p_end, comp_mode)

    st.markdown("---")
    st.markdown(
        f'<div class="comp-box"><b>📅 Current:</b><br>{fd(p_start)} → {fd(p_end)}</div>',
        unsafe_allow_html=True,
    )
    if enable_comp and comp_s and comp_e:
        st.markdown(
            f'<div class="comp-box"><b>🔄 Compare:</b><br>'
            f'{fd(comp_s)} → {fd(comp_e)}</div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")
    _ai_mod = MOD.get("ai_engine")
    if _ai_mod:
        _pr, _dbg = _ai_mod._get_provider_debug()
        _ic = {"groq": "⚡", "gemini": "💎", "rule_based": "🧠"}
        _nm = {"groq": "Groq", "gemini": "Gemini", "rule_based": "Rules"}
        st.caption(f"{_ic.get(_pr, '🤖')} AI: {_nm.get(_pr, _pr)}")
        if _pr == "rule_based":
            lines = []
            if _dbg["secret_groq"]:
                lines.append(f"⚠️ GROQ key found ({_dbg['groq_len']} chars) but invalid")
            if _dbg["secret_gemini"]:
                lines.append(f"⚠️ GEMINI key found ({_dbg['gemini_len']} chars) but invalid")
            if not lines:
                lines.append("⚠️ No AI keys in secrets — using rule-based")
            for line in lines:
                st.caption(line)
    
    # ── Connection Status (fast check) ──
    try:
        if _SUPABASE_ACTIVE:
            st.sidebar.success("🟢 SUPABASE CONNECTED")
            st.sidebar.caption("Fast mode • Live data")
        else:
            st.sidebar.error("🔴 SUPABASE OFFLINE")
            st.sidebar.caption("Using Google Sheets fallback (slow)")
            st.sidebar.caption("Add SUPABASE_URL + SUPABASE_SERVICE_KEY to secrets")
    except Exception:
        pass
    st.caption(f"🦅 Eagle Analytics Hub v7.2 | {datetime.now().strftime('%H:%M')}")
    # Logout button
    if st.button("🔒 Sign Out", key="_auth_logout", use_container_width=True):
        st.session_state["authenticated"] = False
        st.rerun()

# ═══════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════

# ── Auto-trigger pipeline if data is stale ──
# After data loads, check if Daily_Counts has today's date.
# If not and GITHUB_TOKEN is available, automatically trigger the GitHub Actions pipeline once per day.
_today_str = datetime.now().strftime("%Y-%m-%d")
_auto_trigger_key = f"_auto_triggered_{_today_str}"

with st.spinner("Loading data..."):
    counts_raw  = load_sheet("Daily_Counts")
    free_raw    = load_sheet("Verified_FREE")
    upload_raw  = load_sheet("Verified_FIRST_UPLOAD")
    stripe_raw  = load_sheet("Verified_STRIPE")

# ── COLUMN NORMALIZER: ensure Supabase columns match app.py expectations ──
def _norm_cols(df, col_map):
    if df.empty:
        return df
    for src, dst in col_map.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
    return df

# Signups: signup_date -> Account Created On
free_raw = _norm_cols(free_raw, {
    "signup_date": "Account Created On",
    "lead_source": "Lead Source",
    "email_normalized": "__email_normalized__",
})
# Uploads: upload_date -> Upload Date
upload_raw = _norm_cols(upload_raw, {
    "upload_date": "Upload Date",
    "email_normalized": "__email_normalized__",
})
# Payments: first_payment_date -> First payment, total_spend -> Amount
stripe_raw = _norm_cols(stripe_raw, {
    "first_payment_date": "First payment",
    "total_spend": "Amount",
    "email_normalized": "__email_normalized__",
})
if not stripe_raw.empty and "row_date_used" not in stripe_raw.columns:
    for _fc in ("First payment", "first_payment_date"):
        if _fc in stripe_raw.columns:
            stripe_raw["row_date_used"] = stripe_raw[_fc]
            break
# Daily counts: ensure correct column names
counts_raw = _norm_cols(counts_raw, {
    "signups_accepted":  "SignUps_Accepted",
    "uploads_accepted":  "FirstUploads_Accepted",
    "paid_accepted":     "PaidSubscribers_Accepted",
    "signup_details":    "SignUp_Details",
    "upload_details":    "Upload_Details",
    "paid_details":      "Paid_Details",
    "last_updated":      "LastUpdated",
    "date":              "Date",
})

# ── DATA SOURCE BANNER (visible on every page) ──
try:
    _sb_chk = _sb_status()
    if _sb_chk.get("connected"):
        _rows = _sb_chk.get("daily_kpis_rows", 0)
        st.success(f"🟢 LIVE: Supabase | daily_kpis={_rows} rows | Fast queries enabled")
    else:
        _msg = _sb_chk.get("message", "Unknown")
        st.error(f"🔴 SUPABASE NOT CONNECTED: {_msg}")
        st.warning("⚠️ Add SUPABASE_URL and SUPABASE_SERVICE_KEY to Streamlit Cloud Secrets, then Reboot app")
except Exception as _e:
    st.error(f"🔴 Supabase check failed: {_e}")



# Auto-trigger check (runs once per day per session)
if not st.session_state.get(_auto_trigger_key):
    _has_today = False
    if not counts_raw.empty:
        _dc = next((c for c in counts_raw.columns if "date" in c.lower()), None)
        if _dc:
            _has_today = _today_str in counts_raw[_dc].astype(str).values
    if not _has_today:
        _gh_tok = get_secret("GITHUB_TOKEN", "")
        if _gh_tok:
            try:
                import urllib.request
                _repo = "fozayelibnayaz/eagle3d-kpi-automation"
                _url = (
                    f"https://api.github.com/repos/{_repo}"
                    "/actions/workflows/daily_pipeline.yml/dispatches"
                )
                _data = json.dumps({"ref": "main"}).encode()
                _req = urllib.request.Request(
                    _url, data=_data, method="POST",
                    headers={
                        "Authorization": f"token {_gh_tok}",
                        "Accept": "application/vnd.github+json"
                    }
                )
                with urllib.request.urlopen(_req, timeout=10) as _r:
                    if _r.status in (200, 204):
                        st.toast(
                            "🚀 Auto-triggered daily pipeline (data was stale) "
                            "— refresh in ~5 min for updated numbers",
                            icon="🔄"
                        )
            except Exception:
                pass  # Silently fail — user can trigger manually
    st.session_state[_auto_trigger_key] = True

kpi_all = pd.DataFrame()

# ── METHOD 1: Compute directly from Verified tabs (MOST RELIABLE — always correct) ──
if not free_raw.empty:
    from collections import defaultdict as _dd
    _daily = _dd(lambda: {"signups": 0, "first_uploads": 0, "paid_customers": 0})
    # Count ACCEPTED signups by date
    for _, _row in free_raw.iterrows():
        _st = str(_row.get("final_status", "")).upper()
        if _st == "ACCEPTED":
            _d = parse_to_date(_row.get("Account Created On", ""))
            if _d:
                _daily[_d.strftime("%Y-%m-%d")]["signups"] += 1
    # Count ACCEPTED uploads by date
    if not upload_raw.empty:
        for _, _row in upload_raw.iterrows():
            _st = str(_row.get("final_status", "")).upper()
            if _st == "ACCEPTED":
                _d = parse_to_date(_row.get("Upload Date", ""))
                if _d:
                    _daily[_d.strftime("%Y-%m-%d")]["first_uploads"] += 1
    # Count ACCEPTED paid by date
    if not stripe_raw.empty:
        for _, _row in stripe_raw.iterrows():
            _st = str(_row.get("final_status", "")).upper()
            if _st == "ACCEPTED":
                _d = parse_to_date(_row.get("First payment", "") or _row.get("row_date_used", "") or _row.get("Created", ""))
                if _d:
                    _daily[_d.strftime("%Y-%m-%d")]["paid_customers"] += 1
    _rows = [{"date": _d, **_daily[_d]} for _d in sorted(_daily.keys())]
    if _rows:
        kpi_all = pd.DataFrame(_rows)

# ── METHOD 2: Read pre-aggregated Daily_Counts (PRIMARY source — most reliable) ──
if not counts_raw.empty:
    df = counts_raw.copy()
    dc = next((c for c in df.columns if "date" in c.lower()), None)
    if dc:
        df = df.rename(columns={dc: "date"})
        _col_map = {}
        for c in df.columns:
            cl = c.lower()
            if cl in ("signups_accepted", "signup_accepted", "signups"):
                _col_map[c] = "signups"
            elif cl in ("firstuploads_accepted", "first_upload_accepted", "first_uploads"):
                _col_map[c] = "first_uploads"
            elif cl in ("paidsubscribers_accepted", "paid_accepted", "paid_customers"):
                _col_map[c] = "paid_customers"
        for col, pats in [
            ("signups", ["signups_accepted", "signup", "free", "sign_up"]),
            ("first_uploads", ["firstuploads_accepted", "first_upload", "upload_accepted"]),
            ("paid_customers", ["paidsubscribers_accepted", "paid", "stripe", "customer"]),
        ]:
            if col not in _col_map.values():
                for c in df.columns:
                    if c in _col_map:
                        continue
                    if any(p in c.lower() for p in pats):
                        if "detail" in c.lower() or "email" in c.lower():
                            continue
                        _col_map[c] = col
                        break
        for old, new in _col_map.items():
            df = df.rename(columns={old: new})
        nc = [c for c in ["signups", "first_uploads", "paid_customers"] if c in df.columns]
        for c in nc:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        if all(c in df.columns for c in ["date", "signups"]):
            _dc_kpi = df[["date"] + nc].copy()
            # If kpi_all is empty (Method 1 failed), use Daily_Counts as-is
            if kpi_all.empty:
                kpi_all = _dc_kpi
            else:
                # Merge: Daily_Counts overwrites Method 1 for matching dates, adds new dates
                _dc_kpi["date"] = _dc_kpi["date"].astype(str)
                kpi_all["date"] = kpi_all["date"].astype(str)
                _existing_dates = set(kpi_all["date"])
                _new_rows = _dc_kpi[~_dc_kpi["date"].isin(_existing_dates)]
                for _ds in _existing_dates.intersection(set(_dc_kpi["date"])):
                    _mask = kpi_all["date"] == _ds
                    for _c in nc:
                        _dc_val = _dc_kpi.loc[_dc_kpi["date"] == _ds, _c].values
                        if len(_dc_val) > 0:
                            kpi_all.loc[_mask, _c] = _dc_val[0]
                if not _new_rows.empty:
                    kpi_all = pd.concat([kpi_all, _new_rows], ignore_index=True)

# ── METHOD 3: Historical JSON fallback ──
if kpi_all.empty:
    _hist_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_output", "historical_accounts.json")
    _paid_path = os.path.join(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_output", "historical_paid.json"))
    if os.path.exists(_hist_path):
        try:
            from collections import defaultdict as _dd
            _daily = _dd(lambda: {"signups": 0, "first_uploads": 0, "paid_customers": 0})
            with open(_hist_path, "r") as _f:
                _accounts = json.load(_f)
            for _email, _info in _accounts.items():
                for _date, _details in _info.get("rows_by_date", {}).items():
                    _daily[_date]["signups"] += 1
                    if _details.get("has_upload_yes", False):
                        _daily[_date]["first_uploads"] += 1
            if os.path.exists(_paid_path):
                with open(_paid_path, "r") as _f:
                    _paid = json.load(_f)
                for _email, _info in _paid.items():
                    if _info.get("has_paid", False):
                        for _d in _info.get("all_created_utc", []):
                            _daily[_d]["paid_customers"] += 1
            _rows = [{"date": _d, **_daily[_d]} for _d in sorted(_daily.keys())]
            if _rows:
                kpi_all = pd.DataFrame(_rows)
        except Exception:
            pass

kpi = filter_kpi(kpi_all, p_start, p_end)

# ── SUPABASE FAST KPI OVERRIDE ──
# Use direct Supabase counts for today/month (always accurate)
_sb_kpi = get_supabase_kpi_fast()
if _sb_kpi:
    _sb_today_s = _sb_kpi["today_signups"]
    _sb_today_u = _sb_kpi["today_uploads"]
    _sb_today_p = _sb_kpi["today_paid"]
    _sb_month_s = _sb_kpi["month_signups"]
    _sb_month_u = _sb_kpi["month_uploads"]
    _sb_month_p = _sb_kpi["month_paid"]
    _sb_common_s = _sb_kpi["common_signups"]
    _sb_common_u = _sb_kpi["common_uploads"]
    _sb_common_p = _sb_kpi["common_paid"]
    _sb_common_start = _sb_kpi["common_start"]
else:
    _sb_today_s = _sb_today_u = _sb_today_p = 0
    _sb_month_s = _sb_month_u = _sb_month_p = 0
    _sb_common_s = _sb_common_u = _sb_common_p = 0
    _sb_common_start = "2025-12-01"

prev_kpi = (
    filter_kpi(kpi_all, comp_s, comp_e)
    if enable_comp and comp_s
    else pd.DataFrame()
)

# ── HARD OVERRIDE: Always count paid from Verified_STRIPE ACCEPTED rows directly ──
# This ensures the paid count is ALWAYS correct regardless of Daily_Counts column mismatches
# FIX: Never zeros existing paid data — only supplements/overwrites for dates with new ACCEPTED rows
if not stripe_raw.empty:
    _hard_direct_paid = sum(
        1 for _, r in stripe_raw.iterrows()
        if str(r.get("final_status", str(r.get("Final_Status", "")))).upper() == "ACCEPTED"
    )
    _kpi_paid_sum = int(kpi_all["paid_customers"].sum()) if not kpi_all.empty and "paid_customers" in kpi_all.columns else 0
    if _kpi_paid_sum != _hard_direct_paid:
        _paid_by_date = {}
        for _, r in stripe_raw.iterrows():
            _fs = str(r.get("final_status", str(r.get("Final_Status", "")))).upper()
            if _fs == "ACCEPTED":
                _pd = parse_to_date(r.get("First payment", "") or r.get("row_date_used", "") or r.get("Created", ""))
                if _pd:
                    _ds = _pd.strftime("%Y-%m-%d")
                    _paid_by_date[_ds] = _paid_by_date.get(_ds, 0) + 1
        _dated_count = sum(_paid_by_date.values())
        _undated = _hard_direct_paid - _dated_count
        if _undated > 0:
            _today_ds = datetime.now().strftime("%Y-%m-%d")
            _paid_by_date[_today_ds] = _paid_by_date.get(_today_ds, 0) + _undated
        # Apply paid counts per date — only update dates that have ACCEPTED rows
        # Never zero out existing data for dates without ACCEPTED rows
        if not kpi_all.empty and "paid_customers" in kpi_all.columns:
            for _ds, _cnt in _paid_by_date.items():
                _mask = kpi_all["date"] == _ds if "date" in kpi_all.columns else pd.Series([False])
                if _mask.any():
                    kpi_all.loc[_mask, "paid_customers"] = _cnt
                else:
                    _new = {"date": _ds, "signups": 0, "first_uploads": 0, "paid_customers": _cnt}
                    kpi_all = pd.concat([kpi_all, pd.DataFrame([_new])], ignore_index=True)
            # Re-filter
            kpi = filter_kpi(kpi_all, p_start, p_end)
            prev_kpi = filter_kpi(kpi_all, comp_s, comp_e) if enable_comp and comp_s else pd.DataFrame()

free_rows = free_raw.copy()
upload_rows = upload_raw.copy()

# ── Apply manual overrides to in-memory data ──
# Override flow:
#   1. Load overrides from data_output/manual_overrides.json
#   2. Apply to free_rows, upload_rows, stripe_raw (change final_status)
#   3. Compute DELTA: how many rows changed status (ACCEPTED→REJECTED or REJECTED→ACCEPTED)
#   4. Adjust kpi_all by the delta per date so counts reflect overrides
#   5. This avoids rebuilding kpi_all from scratch (which can lose non-parseable-date rows)
_ov_engine = MOD.get("manual_override_engine")
_ov_applied_total = 0
_ov_changed_total = 0  # How many rows actually changed status


def _apply_overrides_to_df(df, tab_name, overrides, norm_fn, mapping):
    """Apply overrides to a DataFrame in-place. Returns (applied, changed) counts."""
    if df.empty or "final_status" not in df.columns or not overrides:
        return 0, 0
    applied = 0
    changed = 0
    for _idx, _row in df.iterrows():
        _em = ""
        for _k in ("Email", "email", "__email_normalized__"):
            if _k in df.columns and pd.notna(_row.get(_k)) and "@" in str(_row[_k]):
                _em = str(_row[_k]).strip().lower()
                break
        _norm = norm_fn(_em) if _em else ""
        if _norm and _norm in overrides:
            _ov = overrides[_norm]
            _ov_tab = _ov.get("target_tab", "ALL")
            if _ov_tab in ("ALL", tab_name):
                _action = _ov.get("action", "")
                _m = mapping.get(_action, {})
                if _m:
                    _old_status = str(df.at[_idx, "final_status"]).upper()
                    _new_status = _m["final_status"]
                    df.at[_idx, "final_status"] = _new_status
                    df.at[_idx, "category"] = _m.get("category", "")
                    applied += 1
                    # Track actual changes (status flipped)
                    if _old_status != _new_status.upper():
                        changed += 1
    return applied, changed


_ovs = {}  # Initialize globally for delta calculation
if _ov_engine:
    try:
        from manual_override_engine import normalize_email, ACTION_TO_STATUS
        _ovs = _ov_engine.load_overrides()
        if _ovs:
            _a1, _c1 = _apply_overrides_to_df(
                free_rows, "Verified_FREE", _ovs, normalize_email, ACTION_TO_STATUS
            )
            _a2, _c2 = _apply_overrides_to_df(
                upload_rows, "Verified_FIRST_UPLOAD", _ovs, normalize_email, ACTION_TO_STATUS
            )
            _a3, _c3 = _apply_overrides_to_df(
                stripe_raw, "Verified_STRIPE", _ovs, normalize_email, ACTION_TO_STATUS
            )
            _ov_applied_total = _a1 + _a2 + _a3
            _ov_changed_total = _c1 + _c2 + _c3

            # Show override toast ONLY once per session (not on every rerun)
            if _ov_changed_total > 0 and not st.session_state.get("_ov_toast_shown"):
                st.toast(f"🔄 {_ov_applied_total} overrides applied ({_ov_changed_total} status changes)", icon="✏️")
                st.session_state["_ov_toast_shown"] = True
            elif _ov_applied_total > 0 and not st.session_state.get("_ov_toast_shown"):
                st.toast(f"ℹ️ {_ov_applied_total} overrides active (no new changes)", icon="📋")
                st.session_state["_ov_toast_shown"] = True
    except Exception:
        pass

# ── Adjust kpi_all by override delta ──
# Instead of rebuilding from verified tabs (risky: may lose rows without parseable dates),
# we compute the delta per date and adjust kpi_all.
if _ov_changed_total > 0 and not kpi_all.empty:
    # Build delta from free_rows (overridden) vs what the status was before override
    _delta = {}  # date -> {"signups_delta": N, "first_uploads_delta": N, "paid_customers_delta": N}
    if "final_status" in free_rows.columns:
        for _, _r in free_rows.iterrows():
            _st = str(_r.get("final_status", "")).upper()
            _norm_email = ""
            for _k in ("Email", "email", "__email_normalized__"):
                if _k in free_rows.columns and pd.notna(_r.get(_k)) and "@" in str(_r[_k]):
                    _norm_email = normalize_email(str(_r[_k]).strip().lower()) if _ov_engine else ""
                    break
            if not _norm_email or _norm_email not in _ovs:
                continue
            _ov = _ovs[_norm_email]
            _orig_cat = str(_ov.get("original_category", "")).upper()
            _new_st = _st
            _d = parse_to_date(_r.get("Account Created On", ""))
            if not _d:
                continue
            _ds = _d.strftime("%Y-%m-%d")
            if _ds not in _delta:
                _delta[_ds] = {"signups_delta": 0, "first_uploads_delta": 0, "paid_customers_delta": 0}
            # If original was ACCEPTED and now REJECTED → -1
            # If original was REJECTED and now ACCEPTED → +1
            if _orig_cat == "ACCEPTED" and _new_st != "ACCEPTED":
                _delta[_ds]["signups_delta"] -= 1
            elif _orig_cat != "ACCEPTED" and _new_st == "ACCEPTED":
                _delta[_ds]["signups_delta"] += 1

    if "final_status" in upload_rows.columns:
        for _, _r in upload_rows.iterrows():
            _st = str(_r.get("final_status", "")).upper()
            _norm_email = ""
            for _k in ("Email", "email", "__email_normalized__"):
                if _k in upload_rows.columns and pd.notna(_r.get(_k)) and "@" in str(_r[_k]):
                    _norm_email = normalize_email(str(_r[_k]).strip().lower()) if _ov_engine else ""
                    break
            if not _norm_email or _norm_email not in _ovs:
                continue
            _ov = _ovs[_norm_email]
            _orig_cat = str(_ov.get("original_category", "")).upper()
            _new_st = _st
            _d = parse_to_date(_r.get("Upload Date", ""))
            if not _d:
                continue
            _ds = _d.strftime("%Y-%m-%d")
            if _ds not in _delta:
                _delta[_ds] = {"signups_delta": 0, "first_uploads_delta": 0, "paid_customers_delta": 0}
            if _orig_cat == "ACCEPTED" and _new_st != "ACCEPTED":
                _delta[_ds]["first_uploads_delta"] -= 1
            elif _orig_cat != "ACCEPTED" and _new_st == "ACCEPTED":
                _delta[_ds]["first_uploads_delta"] += 1

    if "final_status" in stripe_raw.columns:
        for _, _r in stripe_raw.iterrows():
            _st = str(_r.get("final_status", "")).upper()
            _norm_email = ""
            for _k in ("Email", "email", "__email_normalized__"):
                if _k in stripe_raw.columns and pd.notna(_r.get(_k)) and "@" in str(_r[_k]):
                    _norm_email = normalize_email(str(_r[_k]).strip().lower()) if _ov_engine else ""
                    break
            if not _norm_email or _norm_email not in _ovs:
                continue
            _ov = _ovs[_norm_email]
            _orig_cat = str(_ov.get("original_category", "")).upper()
            _new_st = _st
            # Prefer First payment date, then row_date_used, then Created
            _d = parse_to_date(_r.get("First payment", "") or _r.get("row_date_used", "") or _r.get("Created", ""))
            if not _d:
                continue
            _ds = _d.strftime("%Y-%m-%d")
            if _ds not in _delta:
                _delta[_ds] = {"signups_delta": 0, "first_uploads_delta": 0, "paid_customers_delta": 0}
            if _orig_cat == "ACCEPTED" and _new_st != "ACCEPTED":
                _delta[_ds]["paid_customers_delta"] -= 1
            elif _orig_cat != "ACCEPTED" and _new_st == "ACCEPTED":
                _delta[_ds]["paid_customers_delta"] += 1

    # Apply delta to kpi_all
    if _delta and "date" in kpi_all.columns:
        kpi_all = kpi_all.copy()
        for _ds, _deltas in _delta.items():
            _mask = kpi_all["date"] == _ds
            if _mask.any():
                for _col, _key in [("signups", "signups_delta"), ("first_uploads", "first_uploads_delta"), ("paid_customers", "paid_customers_delta")]:
                    if _col in kpi_all.columns and _deltas[_key] != 0:
                        kpi_all.loc[_mask, _col] = kpi_all.loc[_mask, _col] + _deltas[_key]
            else:
                # Date not in kpi_all — add a new row for this date
                _new_row = {"date": _ds, "signups": 0, "first_uploads": 0, "paid_customers": 0}
                for _col, _key in [("signups", "signups_delta"), ("first_uploads", "first_uploads_delta"), ("paid_customers", "paid_customers_delta")]:
                    _new_row[_col] = max(0, _deltas[_key])
                kpi_all = pd.concat([kpi_all, pd.DataFrame([_new_row])], ignore_index=True)

        # Ensure no negative counts
        for _col in ["signups", "first_uploads", "paid_customers"]:
            if _col in kpi_all.columns:
                kpi_all[_col] = kpi_all[_col].clip(lower=0)

        # Re-apply date filter
        kpi = filter_kpi(kpi_all, p_start, p_end)
        prev_kpi = (
            filter_kpi(kpi_all, comp_s, comp_e)
            if enable_comp and comp_s
            else pd.DataFrame()
        )

# ── FINAL PAID OVERRIDE: Force paid count to match Verified_STRIPE ACCEPTED for this period ──
_correct_paid = None
if not stripe_raw.empty and "final_status" in stripe_raw.columns:
    try:
        _fs_s = stripe_raw["final_status"].astype(str).str.upper()
        _stripe_acc = stripe_raw[_fs_s == "ACCEPTED"].copy()
        if not _stripe_acc.empty:
            _stripe_acc["_paid_date"] = _stripe_acc.apply(
                lambda r: parse_to_date(r.get("First payment", "") or r.get("row_date_used", "") or r.get("Created", "")),
                axis=1
            )
            _stripe_acc = _stripe_acc.dropna(subset=["_paid_date"])
            _s_acc_period = _stripe_acc[(_stripe_acc["_paid_date"] >= p_start) & (_stripe_acc["_paid_date"] <= p_end)]
            _correct_paid = len(_s_acc_period)
    except Exception:
        pass

leads_df = pd.DataFrame()
if "kpi_bridge" in MOD and not free_rows.empty:
    try:
        leads_df = MOD["kpi_bridge"].attribute_signups_by_lead_source(
            p_start.strftime("%Y-%m-%d"), p_end.strftime("%Y-%m-%d")
        )
    except Exception:
        pass
if leads_df.empty and "source_normalizer" in MOD and not free_rows.empty:
    try:
        # Apply date filter to free_rows before aggregating sources
        _fr_filtered = free_rows.copy()
        if "Account Created On" in _fr_filtered.columns:
            _fr_filtered["_src_date"] = _fr_filtered["Account Created On"].apply(
                lambda x: parse_to_date(x)
            )
            _fr_filtered = _fr_filtered[
                (_fr_filtered["_src_date"] >= p_start)
                & (_fr_filtered["_src_date"] <= p_end)
            ].drop(columns=["_src_date"])
        if not _fr_filtered.empty:
            leads_df = MOD["source_normalizer"].aggregate_normalized_sources(_fr_filtered)
    except Exception:
        pass

utm_df = pages_df = events_df = geo_df = pd.DataFrame()
prev_utm_df = pd.DataFrame()
if "ga4_connector" in MOD:
    for attr, tgt in [
        ("fetch_utm_traffic", "utm_df"),
        ("fetch_page_performance", "pages_df"),
        ("fetch_event_performance", "events_df"),
        ("fetch_geo_traffic", "geo_df"),
    ]:
        try:
            globals()[tgt] = getattr(MOD["ga4_connector"], attr)(
                p_start.strftime("%Y-%m-%d"), p_end.strftime("%Y-%m-%d")
            )
        except Exception:
            pass
    if enable_comp and comp_s:
        try:
            prev_utm_df = MOD["ga4_connector"].fetch_utm_traffic(
                comp_s.strftime("%Y-%m-%d"), comp_e.strftime("%Y-%m-%d")
            )
        except Exception:
            pass


# ── Time-to-Conversion Analytics ──
def _build_time_to_conversion(free_df, upload_df, stripe_df=None, mode="signup_to_upload", ledger_path=None):
    """Time-to-conversion via Supabase direct query (bypasses Sheets shim column drift)."""
    _start_label = {"signup_to_upload": "Sign-up", "signup_to_paid": "Sign-up", "upload_to_paid": "First Upload"}
    _end_label = {"signup_to_upload": "First Upload", "signup_to_paid": "Paid", "upload_to_paid": "Paid"}
    _st_label = _start_label.get(mode, "Start")
    _en_label = _end_label.get(mode, "End")

    # Direct Supabase query - source of truth
    _su = os.environ.get("SUPABASE_URL","")
    _sk = os.environ.get("SUPABASE_SERVICE_KEY","")
    if not _su or not _sk:
        try:
            _su = str(st.secrets.get("SUPABASE_URL","")).strip()
            _sk = str(st.secrets.get("SUPABASE_SERVICE_KEY","")).strip()
        except Exception:
            pass
    if not _su or not _sk:
        return {}, pd.DataFrame(), pd.DataFrame(), [], _st_label, _en_label

    try:
        from supabase import create_client as _cc
        _sb = _cc(_su, _sk)
    except Exception:
        return {}, pd.DataFrame(), pd.DataFrame(), [], _st_label, _en_label

    def _fetch_all(table, cols):
        rows = []
        offset = 0
        while True:
            try:
                r = _sb.table(table).select(cols).eq("final_status","ACCEPTED").range(offset, offset+999).execute()
                batch = r.data or []
                rows.extend(batch)
                if len(batch) < 1000: break
                offset += 1000
            except Exception:
                break
        return rows

    # Map mode to tables
    if mode == "signup_to_upload":
        src_rows = _fetch_all("signups", "email_normalized,signup_date")
        tgt_rows = _fetch_all("uploads", "email_normalized,upload_date")
        src_date_field = "signup_date"
        tgt_date_field = "upload_date"
    elif mode == "signup_to_paid":
        src_rows = _fetch_all("signups", "email_normalized,signup_date")
        tgt_rows = _fetch_all("payments", "email_normalized,first_payment_date")
        src_date_field = "signup_date"
        tgt_date_field = "first_payment_date"
    elif mode == "upload_to_paid":
        src_rows = _fetch_all("uploads", "email_normalized,upload_date")
        tgt_rows = _fetch_all("payments", "email_normalized,first_payment_date")
        src_date_field = "upload_date"
        tgt_date_field = "first_payment_date"
    else:
        return {}, pd.DataFrame(), pd.DataFrame(), [], _st_label, _en_label

    from datetime import datetime as _dt
    def _to_date(s):
        if not s: return None
        try:
            return _dt.fromisoformat(str(s)[:10]).date()
        except Exception:
            return None

    src_map = {}
    for r in src_rows:
        em = (r.get("email_normalized") or "").strip().lower()
        d  = _to_date(r.get(src_date_field))
        if em and d:
            if em not in src_map or d < src_map[em]:
                src_map[em] = d

    tgt_map = {}
    for r in tgt_rows:
        em = (r.get("email_normalized") or "").strip().lower()
        d  = _to_date(r.get(tgt_date_field))
        if em and d:
            if em not in tgt_map or d < tgt_map[em]:
                tgt_map[em] = d

    # Compute gaps
    rows = []
    for em in (set(src_map) & set(tgt_map)):
        gap = (tgt_map[em] - src_map[em]).days
        if gap < 0:
            continue  # target before source = data quality issue
        rows.append({"email": em, "gap_days": gap,
                     "src_date": src_map[em].isoformat(),
                     "tgt_date": tgt_map[em].isoformat()})

    if not rows:
        return {"matched_users":0,"median_days":0,"mean_days":0,"min_days":0,"max_days":0,"pct_7d":0,"pct_30d":0},                pd.DataFrame(), pd.DataFrame(), [], _st_label, _en_label

    import statistics as _st_mod
    gaps = [r["gap_days"] for r in rows]
    stats = {
        "matched_users": len(rows),
        "median_days":   round(_st_mod.median(gaps), 1),
        "mean_days":     round(_st_mod.mean(gaps), 1),
        "min_days":      min(gaps),
        "max_days":      max(gaps),
        "pct_7d":        round(sum(1 for g in gaps if g <= 7) / len(gaps) * 100, 1),
        "pct_30d":       round(sum(1 for g in gaps if g <= 30) / len(gaps) * 100, 1),
    }

    # Histogram bins
    bins = [(0,1,"0-1d"),(2,3,"2-3d"),(4,7,"4-7d"),(8,14,"8-14d"),(15,30,"15-30d"),
            (31,60,"31-60d"),(61,90,"61-90d"),(91,180,"91-180d"),(181,365,"181-365d"),(366,9999,"365+d")]
    hist_data = []
    for lo, hi, label in bins:
        cnt = sum(1 for g in gaps if lo <= g <= hi)
        if cnt > 0:
            hist_data.append({"range": label, "count": cnt})
    hist_df = pd.DataFrame(hist_data)

    # Monthly cohort
    from collections import defaultdict
    by_month = defaultdict(list)
    for r in rows:
        by_month[r["src_date"][:7]].append(r["gap_days"])
    monthly_data = [{"month": m, "median_days": round(_st_mod.median(g),1), "count": len(g)}
                     for m, g in sorted(by_month.items())]
    monthly_df = pd.DataFrame(monthly_data)

    return stats, hist_df, monthly_df, rows, _st_label, _en_label



