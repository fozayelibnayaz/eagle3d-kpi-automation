"""
Eagle Analytics Hub — Unified KPI & Analytics Dashboard v7
=================================================
All-in-one: KPI, GA4, YouTube, LinkedIn, Cross-Platform Correlation
Dark/light mode, AI-powered analytics, Telegram alerts.
Pages: Dashboard, Traffic Intel, Ask AI, Predictions,
Reports, Alerts, EDA Lab, Browse Data, Settings.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json, os, tempfile, re, base64, sys
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

# ═══════════════════════════════════════════════════════════════
# THEME ENGINE
# ═══════════════════════════════════════════════════════════════
if "theme_mode" not in st.session_state:
    st.session_state["theme_mode"] = "dark"

IS_DARK = st.session_state.get("theme_mode", "dark") == "dark"


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
    """Inject comprehensive CSS for both dark and light modes."""
    light_fixes = ""
    if not IS_DARK:
        light_fixes = f"""
    /* ═══ LIGHT MODE: Force visibility on ALL native Streamlit widgets ═══ */
    .stApp > div > div > div > div p,
    .stApp > div > div > div > div span,
    .stApp > div > div > div > div label,
    .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6 {{
        color: {T["text"]} !important;
    }}
    [data-testid="stMetricLabel"] {{ color: {T["muted"]} !important; }}
    [data-testid="stMetricValue"] {{ color: {T["text"]} !important; }}
    [data-testid="stMetricDelta"] {{ color: {T["text_sec"]} !important; }}
    [data-testid="stMetric"] {{
        background: {T["card"]}; border: 1px solid {T["border"]};
        border-radius: 10px; padding: 10px 14px;
    }}
    .stApp [data-testid="stCaption"] {{ color: {T["muted"]} !important; }}
    .stApp div[data-testid="stText"] {{ color: {T["text"]} !important; }}
    .stApp div[data-testid="stMarkdownContainer"] {{ color: {T["text"]} !important; }}

    /* Inputs */
    .stApp input[type="text"],
    .stApp input[type="number"],
    .stApp input[type="date"],
    .stApp textarea,
    .stApp div[data-baseweb="select"] > div > div,
    .stApp div[data-baseweb="input"] > div > div {{
        background: {T["input_bg"]} !important;
        color: {T["input_text"]} !important;
        border-color: {T["border"]} !important;
    }}

    /* Buttons */
    .stApp button[kind="header"] {{ color: {T["text"]} !important; }}
    .stApp button:not([kind="primary"]) {{
        color: {T["text"]} !important;
        border-color: {T["border"]} !important;
        background: {T["card"]} !important;
    }}
    .stApp button[kind="primary"] {{
        color: #FFFFFF !important;
        background: {T["accent"]} !important;
        border-color: {T["accent"]} !important;
    }}

    /* Tabs */
    .stApp .stTabs [role="tab"] {{ color: {T["muted"]} !important; }}
    .stApp .stTabs [role="tab"][aria-selected="true"],
    .stApp .stTabs [role="tab"][aria-selected="true"] p {{
        color: {T["accent"]} !important;
    }}
    .stApp .stTabs [role="tablist"] {{
        border-bottom: 2px solid {T["border"]};
    }}

    /* Dataframes */
    .stApp .stDataFrame {{
        background: {T["card"]} !important;
    }}
    .stApp [data-testid="stDataFrameResizable"] {{
        background: {T["card"]} !important;
    }}

    /* Alerts */
    .stApp [data-testid="stAlert"],
    .stApp [data-testid="stAlert"] p,
    .stApp [data-testid="stAlert"] span,
    .stApp [data-testid="stAlert"] div {{
        color: {T["text"]} !important;
    }}

    /* Sidebar */
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span:not([class*="badge"]),
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stCaption,
    [data-testid="stSidebar"] .stMarkdown {{
        color: {T["text"]} !important;
    }}
    [data-testid="stSidebar"] input,
    [data-testid="stSidebar"] div[data-baseweb="select"] > div > div {{
        background: {T["card_alt"]} !important;
        color: {T["text"]} !important;
        border-color: {T["border"]} !important;
    }}

    /* Code blocks */
    .stApp code, .stApp pre {{
        color: {T["text"]} !important;
        background: {T["card_alt"]} !important;
    }}

    /* Expander */
    .stApp .streamlit-expanderHeader {{
        color: {T["text"]} !important;
    }}
    """

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

    /* ═══ APP BACKGROUND ═══ */
    .stApp {{ background: var(--bg) !important; }}
    .stMarkdown, .stText {{ color: var(--text); }}

    /* ═══ SIDEBAR ═══ */
    [data-testid="stSidebar"] {{
        background: {T['sidebar_bg']} !important;
        border-right: 1px solid var(--border) !important;
    }}
    [data-testid="stSidebarNav"] {{ display: none !important; }}
    [data-testid="stSidebar"] label[data-baseweb="radio"] {{
        color: var(--text-sec); font-size: 0.85rem; font-weight: 500;
        padding: 9px 14px; border-radius: 10px;
        border: 1px solid transparent; margin: 2px 0;
        transition: all 0.15s;
    }}
    [data-testid="stSidebar"] label[data-baseweb="radio"]:hover {{
        background: var(--card-alt); border-color: var(--border);
    }}
    [data-testid="stSidebar"] [aria-checked="true"] {{
        background: linear-gradient(135deg, {T['accent']}22, {T['accent2']}18) !important;
        border-color: var(--accent) !important;
    }}
    [data-testid="stSidebar"] [aria-checked="true"] + div {{
        color: var(--accent) !important; font-weight: 700 !important;
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
        transition: all 0.2s; position: relative; overflow: hidden;
    }}
    .kpi:hover {{
        transform: translateY(-2px);
        box-shadow: 0 6px 24px {T['accent']}18;
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

    /* ═══ RESPONSIVE ═══ */
    @media (max-width: 768px) {{
        .kpi-grid {{
            grid-template-columns: repeat(2, 1fr); gap: 8px;
        }}
        .kpi-val {{ font-size: 1.3rem; }}
    }}
    @media (max-width: 480px) {{
        .kpi-grid {{
            grid-template-columns: 1fr 1fr; gap: 6px;
        }}
        .kpi-val {{ font-size: 1.1rem; }}
    }}

    /* ═══ CLEANUP ═══ */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}

    /* ═══ LIGHT MODE OVERRIDES ═══ */
    {light_fixes}
    </style>
    """, unsafe_allow_html=True)


_css()

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


@st.cache_data(ttl=120)
def load_sheet(tab):
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
        import gspread
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
    if not raw or str(raw).strip() in ("", "-", "—", "nan", "None"):
        return None
    s = str(raw).strip()
    try:
        return parsedate_to_datetime(s).date()
    except Exception:
        pass
    for fmt in [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
        "%m/%d/%y, %I:%M %p", "%m/%d/%Y, %I:%M %p",
    ]:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
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
    d["_d"] = pd.to_datetime(d["date"], errors="coerce").dt.date
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
            return '<span class="kpi-delta d-up">▲ NEW</span>'
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
        '<div style="font-size:0.9rem;font-weight:800;color:var(--accent);'
        'letter-spacing:0.5px;">Eagle Analytics Hub</div>'
        "</div>",
        unsafe_allow_html=True,
    )

    # Theme toggle
    _dark = st.toggle(
        "🌙 Dark Mode",
        value=(st.session_state.get("theme_mode", "dark") == "dark"),
    )
    _new_mode = "dark" if _dark else "light"
    if _new_mode != st.session_state.get("theme_mode", "dark"):
        st.session_state["theme_mode"] = _new_mode
        st.rerun()

    st.markdown("---")

    # Navigation — 4 primary platform groups
    _nav_section = st.radio(
        "Platform",
        ["📊 KPI System", "🌐 Analytics", "📺 YouTube", "💼 LinkedIn"],
        label_visibility="collapsed",
        key="nav_section",
    )
    st.markdown("---")
    if _nav_section == "📊 KPI System":
        page = st.radio("KPI", [
            "📊 Dashboard", "🔍 Browse Data", "✏️ Manual Override", "⚙️ Settings",
        ], label_visibility="collapsed", key="nav_kpi")
    elif _nav_section == "🌐 Analytics":
        page = st.radio("GA", [
            "🚦 Traffic Intel", "🔬 EDA Lab", "🤖 Ask AI",
            "🔮 Predictions", "📋 Reports", "🔔 Alerts",
        ], label_visibility="collapsed", key="nav_ga")
    elif _nav_section == "📺 YouTube":
        page = st.radio("YT", [
            "📺 YouTube", "🔗 Cross-Platform", "🤖 Ask AI",
            "🔮 Predictions", "📋 Reports", "🔔 Alerts",
        ], label_visibility="collapsed", key="nav_yt")
    else:
        page = st.radio("LI", [
            "💼 LinkedIn", "🔗 Cross-Platform", "🤖 Ask AI",
            "🔮 Predictions", "📋 Reports", "🔔 Alerts",
        ], label_visibility="collapsed", key="nav_li")

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
            for l in lines:
                st.caption(l)
    st.caption(f"🦅 Eagle Analytics Hub v7.1 | {datetime.now().strftime('%H:%M')}")

# ═══════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════

# ── Auto-trigger pipeline if data is stale ──
# After data loads, check if Daily_Counts has today's date.
# If not and GITHUB_TOKEN is available, automatically trigger the GitHub Actions pipeline once per day.
_today_str = datetime.now().strftime("%Y-%m-%d")
_auto_trigger_key = f"_auto_triggered_{_today_str}"

with st.spinner("Loading data..."):
    counts_raw = load_sheet("Daily_Counts")
    free_raw = load_sheet("Verified_FREE")
    upload_raw = load_sheet("Verified_FIRST_UPLOAD")
    stripe_raw = load_sheet("Verified_STRIPE")

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
                _url = f"https://api.github.com/repos/{_repo}/actions/workflows/daily_pipeline.yml/dispatches"
                _data = json.dumps({"ref": "main"}).encode()
                _req = urllib.request.Request(_url, data=_data, method="POST",
                    headers={"Authorization": f"token {_gh_tok}", "Accept": "application/vnd.github+json"})
                with urllib.request.urlopen(_req, timeout=10) as _r:
                    if _r.status in (200, 204):
                        st.toast("🚀 Auto-triggered daily pipeline (data was stale) — refresh in ~5 min for updated numbers", icon="🔄")
            except Exception:
                pass  # Silently fail — user can trigger manually
    st.session_state[_auto_trigger_key] = True

kpi_all = pd.DataFrame()

# ── METHOD 1: Read pre-aggregated Daily_Counts ──
if not counts_raw.empty:
    df = counts_raw.copy()
    # Find date column
    dc = next((c for c in df.columns if "date" in c.lower()), None)
    if dc:
        df = df.rename(columns={dc: "date"})
        # Robust column matching: prefer exact matches, avoid detail columns
        _col_map = {}
        for c in df.columns:
            cl = c.lower()
            if cl in ("signups_accepted", "signup_accepted"):
                _col_map[c] = "signups"
            elif cl in ("firstuploads_accepted", "first_upload_accepted"):
                _col_map[c] = "first_uploads"
            elif cl in ("paidsubscribers_accepted", "paid_accepted"):
                _col_map[c] = "paid_customers"
        # Fallback fuzzy matching only for unmatched target columns
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
                        # Skip detail/text columns
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
            kpi_all = df[["date"] + nc].copy()

# ── METHOD 2: Compute directly from Verified tabs (most reliable) ──
if kpi_all.empty and not free_raw.empty:
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
                # Prefer First payment date (most reliable for paid customers), then row_date_used, then Created
                _d = parse_to_date(_row.get("First payment", "") or _row.get("row_date_used", "") or _row.get("Created", ""))
                if _d:
                    _daily[_d.strftime("%Y-%m-%d")]["paid_customers"] += 1
    _rows = [{"date": _d, **_daily[_d]} for _d in sorted(_daily.keys())]
    if _rows:
        kpi_all = pd.DataFrame(_rows)

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
prev_kpi = (
    filter_kpi(kpi_all, comp_s, comp_e)
    if enable_comp and comp_s
    else pd.DataFrame()
)

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

period_label = f"{fd(p_start)} to {fd(p_end)}"

# ═══════════════════════════════════════════════════════════════
# PAGE: 📊 DASHBOARD
# ═══════════════════════════════════════════════════════════════
if page == "📊 Dashboard":
    st.markdown(
        '<div class="sec-head">📊 Executive Dashboard</div>',
        unsafe_allow_html=True,
    )

    # Data diagnostics
    _total_all = int(kpi_all["signups"].sum()) if not kpi_all.empty and "signups" in kpi_all.columns else 0
    _total_u_all = int(kpi_all["first_uploads"].sum()) if not kpi_all.empty and "first_uploads" in kpi_all.columns else 0
    _total_p_all = int(kpi_all["paid_customers"].sum()) if not kpi_all.empty and "paid_customers" in kpi_all.columns else 0
    if _total_all > 0 or _total_u_all > 0 or _total_p_all > 0:
        _src = "Daily_Counts" if not counts_raw.empty else "Verified tabs" if not free_raw.empty else "Historical JSON"
        _ov_note = f" | ✏️ {_ov_applied_total} overrides active" if _ov_applied_total > 0 else ""
        st.caption(f"📡 Source: {_src} | All-time: {_total_all:,} sign-ups, {_total_u_all:,} uploads, {_total_p_all:,} paid{_ov_note}")
    else:
        st.warning("⚠️ No KPI data loaded. Check Settings → Secrets Status.")

    cs = km(kpi, "signups")
    cu = km(kpi, "first_uploads")
    cp = km(kpi, "paid_customers")
    ps = km(prev_kpi, "signups")
    pu = km(prev_kpi, "first_uploads")
    pp = km(prev_kpi, "paid_customers")

    s2u = (cu / cs * 100) if cs > 0 else 0
    u2p = (cp / cu * 100) if cu > 0 else 0
    s2p = (cp / cs * 100) if cs > 0 else 0
    ps2u = (pu / ps * 100) if ps > 0 else 0
    pu2p = (pp / pu * 100) if pu > 0 else 0
    ps2p = (pp / ps * 100) if ps > 0 else 0

    h = '<div class="kpi-grid">'
    h += kpi_h("Sign-ups", cs, delta_h(cs, ps), "👥")
    h += kpi_h("First Uploads", cu, delta_h(cu, pu), "📤")
    h += kpi_h("Paid Customers", cp, delta_h(cp, pp), "💳")
    h += kpi_h("S→U Rate", f"{s2u:.1f}%", delta_h(s2u, ps2u), "🔄")
    h += kpi_h("U→P Rate", f"{u2p:.1f}%", delta_h(u2p, pu2p), "💰")
    h += kpi_h("S→P Rate", f"{s2p:.1f}%", delta_h(s2p, ps2p), "🎯")
    h += "</div>"
    st.markdown(h, unsafe_allow_html=True)

    # ── Monthly Goals Tracker ──
    _cur_month = datetime.now().strftime("%Y-%m")
    _month_kpi = kpi_all.copy() if not kpi_all.empty else pd.DataFrame()
    if not _month_kpi.empty and "date" in _month_kpi.columns:
        _month_kpi["_d"] = pd.to_datetime(_month_kpi["date"], errors="coerce").dt.strftime("%Y-%m")
        _month_kpi = _month_kpi[_month_kpi["_d"] == _cur_month].drop(columns=["_d"])
    _month_s = int(_month_kpi["signups"].sum()) if not _month_kpi.empty and "signups" in _month_kpi.columns else 0
    _month_u = int(_month_kpi["first_uploads"].sum()) if not _month_kpi.empty and "first_uploads" in _month_kpi.columns else 0
    _month_p = int(_month_kpi["paid_customers"].sum()) if not _month_kpi.empty and "paid_customers" in _month_kpi.columns else 0

    _goals_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_output", "monthly_goals.json")
    def _load_goals():
        if os.path.exists(_goals_file):
            try:
                with open(_goals_file, "r") as _gf:
                    return json.load(_gf)
            except Exception:
                pass
        return {}
    def _save_goals(data):
        os.makedirs(os.path.dirname(_goals_file), exist_ok=True)
        with open(_goals_file, "w") as _gf:
            json.dump(data, _gf, indent=2)

    _goals = _load_goals()
    _goal_s = _goals.get("signups", 100)
    _goal_u = _goals.get("uploads", 30)
    _goal_p = _goals.get("paid", 5)

    with st.expander("🎯 Monthly Goals — " + datetime.now().strftime("%B %Y"), expanded=True):
        _gc1, _gc2, _gc3 = st.columns(3)
        with _gc1:
            _pct_s = (_month_s / _goal_s * 100) if _goal_s > 0 else 0
            st.metric("👥 Sign-ups", f"{_month_s} / {_goal_s}", f"{_pct_s:.0f}% of goal",
                      delta_color="normal" if _pct_s >= 50 else "inverse")
            st.progress(min(_pct_s / 100, 1.0))
        with _gc2:
            _pct_u = (_month_u / _goal_u * 100) if _goal_u > 0 else 0
            st.metric("📤 Uploads", f"{_month_u} / {_goal_u}", f"{_pct_u:.0f}% of goal",
                      delta_color="normal" if _pct_u >= 50 else "inverse")
            st.progress(min(_pct_u / 100, 1.0))
        with _gc3:
            _pct_p = (_month_p / _goal_p * 100) if _goal_p > 0 else 0
            st.metric("💳 Paid", f"{_month_p} / {_goal_p}", f"{_pct_p:.0f}% of goal",
                      delta_color="normal" if _pct_p >= 50 else "inverse")
            st.progress(min(_pct_p / 100, 1.0))

        _g_e1, _g_e2, _g_e3 = st.columns(3)
        with _g_e1:
            _new_gs = st.number_input("Sign-up Goal", value=_goal_s, min_value=1, key="goal_s")
        with _g_e2:
            _new_gu = st.number_input("Upload Goal", value=_goal_u, min_value=1, key="goal_u")
        with _g_e3:
            _new_gp = st.number_input("Paid Goal", value=_goal_p, min_value=1, key="goal_p")
        if st.button("💾 Save Goals", use_container_width=True):
            _save_goals({"signups": _new_gs, "uploads": _new_gu, "paid": _new_gp})
            st.success(f"✅ Goals saved: {_new_gs} sign-ups, {_new_gu} uploads, {_new_gp} paid")
            st.rerun()

    c1, c2 = st.columns([1, 2])
    with c1:
        # Build full funnel: Sessions → Sign-ups → Uploads → Paid
        _sess = int(utm_df["sessions"].sum()) if not utm_df.empty and "sessions" in utm_df.columns else 0
        _funnel_y = ["Sessions"]
        _funnel_x = [_sess if _sess > 0 else cs]
        if _sess > 0:
            _funnel_y += ["Sign-ups", "Uploads", "Paid"]
            _funnel_x += [cs, cu, cp]
        else:
            _funnel_y += ["Sign-ups", "Uploads", "Paid"]
            _funnel_x += [cs, cu, cp]

        fig = go.Figure(
            go.Funnel(
                y=_funnel_y,
                x=_funnel_x,
                textposition="inside",
                textinfo="value+percent initial",
                marker=dict(color=[T["muted"], T["accent"], T["accent2"], T["green"]][:len(_funnel_y)]),
            )
        )
        fig.update_layout(
            title="Conversion Funnel", height=340,
            **CT(), margin=dict(l=0, r=0, t=40, b=0),
        )
        _pc(fig)

    with c2:
        if not kpi.empty and "date" in kpi.columns:
            dk = kpi.copy()
            dk["date"] = pd.to_datetime(dk["date"])
            dk = dk.sort_values("date")
            fig = go.Figure()
            if "signups" in dk.columns:
                fig.add_trace(go.Scatter(
                    x=dk["date"], y=dk["signups"], name="Sign-ups",
                    line=dict(color=T["accent"], width=2),
                ))
            if "first_uploads" in dk.columns:
                fig.add_trace(go.Scatter(
                    x=dk["date"], y=dk["first_uploads"], name="Uploads",
                    line=dict(color=T["accent2"], width=2),
                ))
            if "paid_customers" in dk.columns:
                fig.add_trace(go.Scatter(
                    x=dk["date"], y=dk["paid_customers"], name="Paid",
                    line=dict(color=T["green"], width=2),
                ))
            fig.update_layout(
                title="Daily Trends", height=340,
                **CT(), margin=dict(l=0, r=0, t=40, b=0),
                hovermode="x unified",
            )
            _pc(fig)

    if enable_comp and not prev_kpi.empty:
        st.markdown(
            '<div class="sec-head">📊 Period Comparison</div>',
            unsafe_allow_html=True,
        )
        comp_data = {
            "Metric": ["Sign-ups", "Uploads", "Paid", "S→U", "U→P", "S→P"],
            "Current": [
                cs, cu, cp,
                f"{s2u:.1f}%", f"{u2p:.1f}%", f"{s2p:.1f}%",
            ],
            "Previous": [
                ps, pu, pp,
                f"{ps2u:.1f}%", f"{pu2p:.1f}%", f"{ps2p:.1f}%",
            ],
            "Δ %": [
                f"{((cs - ps) / ps * 100):+.1f}%" if ps > 0 else "—",
                f"{((cu - pu) / pu * 100):+.1f}%" if pu > 0 else "—",
                f"{((cp - pp) / pp * 100):+.1f}%" if pp > 0 else "—",
                f"{(s2u - ps2u):+.1f}pp",
                f"{(u2p - pu2p):+.1f}pp",
                f"{(s2p - ps2p):+.1f}pp",
            ],
        }
        _df(pd.DataFrame(comp_data), height=250)

    if not leads_df.empty:
        st.markdown(
            '<div class="sec-head">🎯 Lead Sources</div>',
            unsafe_allow_html=True,
        )
        c1, c2 = st.columns([2, 1])
        with c1:
            _df(leads_df, height=300)
        with c2:
            if "Signups" in leads_df.columns:
                fig = px.pie(
                    leads_df.head(8), values="Signups",
                    names="Lead Source", hole=0.5,
                    color_discrete_sequence=CC,
                )
                fig.update_layout(
                    height=300, **CT(),
                    margin=dict(l=0, r=0, t=0, b=0),
                    showlegend=False,
                )
                _pc(fig)

    # ── COMBINED SOURCES: GA4 + CRM (Smart Dedup) ──
    if not utm_df.empty or not leads_df.empty:
        st.markdown(
            '<div class="sec-head">🌐📋 Combined Sources — GA4 Traffic + CRM Sign-ups</div>',
            unsafe_allow_html=True,
        )
        # Use source_normalizer for intelligent deduplication
        _sn_mod = MOD.get("source_normalizer")
        _norm_fn = _sn_mod.normalize_source if _sn_mod else None

        # Accumulate by canonical source name
        _combined = {}  # canonical_name -> {sessions, conversions, signups, type}

        # GA4 traffic sources — normalize each source
        if not utm_df.empty:
            src_col = "source_normalized" if "source_normalized" in utm_df.columns else "sessionSource"
            sess_col = "sessions" if "sessions" in utm_df.columns else src_col
            for src_val, grp in utm_df.groupby(src_col):
                canonical = src_val
                if _norm_fn:
                    try:
                        canonical, _ = _norm_fn(str(src_val))
                    except Exception:
                        canonical = str(src_val).strip()
                else:
                    canonical = str(src_val).strip()
                sessions = int(grp[sess_col].sum()) if sess_col in grp.columns else 0
                conversions = int(grp["conversions"].sum()) if "conversions" in grp.columns else 0
                if canonical not in _combined:
                    _combined[canonical] = {"sessions": 0, "conversions": 0, "signups": 0, "type": "traffic"}
                _combined[canonical]["sessions"] += sessions
                _combined[canonical]["conversions"] += conversions

        # CRM lead sources — normalize and merge with GA4
        if not leads_df.empty:
            ls_col = "Lead Source" if "Lead Source" in leads_df.columns else None
            su_col = "Signups" if "Signups" in leads_df.columns else None
            if ls_col and su_col:
                # First, aggregate CRM by raw source
                _crm_agg = {}
                for _, row in leads_df.iterrows():
                    raw_src = str(row[ls_col]).strip()
                    su = int(row.get(su_col, 0)) if pd.notna(row.get(su_col)) else 0
                    # Normalize
                    canonical = raw_src
                    if _norm_fn:
                        try:
                            canonical, _ = _norm_fn(raw_src)
                        except Exception:
                            canonical = raw_src
                    if canonical not in _crm_agg:
                        _crm_agg[canonical] = 0
                    _crm_agg[canonical] += su
                # Merge into combined
                for canonical, signups in _crm_agg.items():
                    if canonical not in _combined:
                        _combined[canonical] = {"sessions": 0, "conversions": 0, "signups": 0, "type": "signup"}
                    _combined[canonical]["signups"] += signups
                    # Update type
                    if _combined[canonical]["sessions"] > 0 and signups > 0:
                        _combined[canonical]["type"] = "both"
                    elif signups > 0:
                        _combined[canonical]["type"] = _combined[canonical]["type"] if _combined[canonical]["sessions"] > 0 else "signup"

        if _combined:
            combined_rows = []
            for canonical, vals in _combined.items():
                combined_rows.append({
                    "Source": canonical,
                    "GA4 Sessions": vals["sessions"],
                    "GA4 Conversions": vals["conversions"],
                    "CRM Signups": vals["signups"],
                    "Type": vals["type"],
                })
            combined_df = pd.DataFrame(combined_rows)
            combined_df["Total Interactions"] = combined_df["GA4 Sessions"] + combined_df["CRM Signups"]
            combined_df = combined_df.sort_values("Total Interactions", ascending=False)
            combined_df["Conv %"] = (combined_df["GA4 Conversions"] / combined_df["GA4 Sessions"].replace(0, 1) * 100).round(1)
            # Reorder columns
            show_cols = [c for c in ["Source", "GA4 Sessions", "GA4 Conversions", "Conv %", "CRM Signups", "Total Interactions", "Type"] if c in combined_df.columns]
            _df(combined_df[show_cols].head(30), height=400)

            # Pie chart of combined
            fig = px.pie(combined_df.head(10), values="Total Interactions", names="Source", hole=0.5,
                         color_discrete_sequence=CC)
            fig.update_layout(height=350, **CT(), margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
            _pc(fig)

    _ai_mod = MOD.get("ai_engine")
    if _ai_mod:
        st.markdown("---")
        if st.button("⚡ Generate AI Insight"):
            with st.spinner("AI analyzing..."):
                result = _ai_mod.ask_ai(
                    "Give 3 bullet-point insights with specific numbers.",
                    kpi_df=kpi, leads_df=leads_df, utm_df=utm_df,
                    geo_df=geo_df, period_label=period_label,
                    prev_kpi_df=prev_kpi if enable_comp else None,
                    prev_utm_df=prev_utm_df if enable_comp else None,
                )
                st.markdown(result["answer"])

# ═══════════════════════════════════════════════════════════════
# PAGE: 🚦 TRAFFIC INTEL
# ═══════════════════════════════════════════════════════════════
elif page == "🚦 Traffic Intel":
    st.markdown(
        '<div class="sec-head">🚦 Traffic Intelligence Hub</div>',
        unsafe_allow_html=True,
    )
    try:
        pd_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "pages"
        )
        if pd_dir not in sys.path:
            sys.path.insert(0, pd_dir)
        ti_loaded = False
        for f in os.listdir(pd_dir):
            if "Traffic_Intelligence" in f and f.endswith(".py"):
                # Force reimport to pick up latest code changes
                import importlib
                ti = __import__(f[:-3])
                importlib.reload(ti)
                # Pass current date range via session_state + cache buster
                st.session_state["ti_start"] = p_start.strftime("%Y-%m-%d")
                st.session_state["ti_end"] = p_end.strftime("%Y-%m-%d")
                st.session_state["ti_cache_key"] = f"{p_start.strftime('%Y-%m-%d')}_{p_end.strftime('%Y-%m-%d')}"
                ti.main()
                ti_loaded = True
                break
        if not ti_loaded:
            raise ImportError("Traffic page not found")
    except Exception as e:
        st.warning(f"Traffic Intel module: {e}")
        if not utm_df.empty:
            sc = (
                "source_normalized"
                if "source_normalized" in utm_df.columns
                else "sessionSource"
            )
            agg = utm_df.groupby(sc).agg(
                Sessions=("sessions", "sum")
                if "sessions" in utm_df.columns
                else (sc, "count"),
            ).reset_index().sort_values("Sessions", ascending=False)
            fig = px.bar(
                agg.head(15), x="Sessions", y=sc,
                orientation="h", color_discrete_sequence=[T["accent"]],
            )
            fig.update_layout(
                height=400, **CT(),
                margin=dict(l=0, r=0, t=20, b=0),
            )
            _pc(fig)
            _df(agg)
        else:
            st.info("No GA4 traffic data available. Configure GA4 connector in Settings.")

# ═══════════════════════════════════════════════════════════════
# PAGE: 🤖 ASK AI — Chat System
# ═══════════════════════════════════════════════════════════════
elif page == "🤖 Ask AI":
    st.markdown(
        '<div class="sec-head">🤖 AI Analytics Assistant</div>',
        unsafe_allow_html=True,
    )
    _ai_mod = MOD.get("ai_engine")
    if not _ai_mod:
        st.error("AI Engine not loaded")
        st.stop()

    _provider = _ai_mod._get_provider()
    _pi = {
        "groq": ("⚡ Groq Cloud", "Ultra-fast"),
        "gemini": ("💎 Google Gemini", "Advanced reasoning"),
        "rule_based": ("🧠 Rule-Based", "No API key"),
    }
    _pn, _pd = _pi.get(_provider, ("Unknown", ""))
    st.markdown(
        f'<span class="badge badge-info">{_pn}</span> '
        f'<span style="color:var(--muted);font-size:0.8rem;">{_pd}</span>',
        unsafe_allow_html=True,
    )

    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    st.markdown("#### 🛠 AI Tools")
    _tools = _ai_mod.get_available_tools()
    _tc = st.columns(4)
    for _i, _tool in enumerate(_tools):
        with _tc[_i % 4]:
            if st.button(
                _tool["name"],
                key=f"t_{_tool['id']}",
                use_container_width=True,
                help=_tool["description"],
            ):
                with st.spinner("AI analyzing..."):
                    _result = _ai_mod.run_tool(
                        _tool["id"],
                        kpi_df=kpi, utm_df=utm_df, pages_df=pages_df,
                        events_df=events_df, geo_df=geo_df,
                        leads_df=leads_df, period_label=period_label,
                        prev_kpi_df=prev_kpi if enable_comp else None,
                        prev_utm_df=prev_utm_df if enable_comp else None,
                    )
                    st.session_state["chat_history"].append({
                        "role": "tool",
                        "content": _result["answer"],
                        "name": _tool["name"],
                        "time": datetime.now().strftime("%H:%M"),
                        "provider": _result.get("provider", "rule_based"),
                    })

    st.markdown("---")
    st.markdown("#### 💬 Chat")
    st.caption("Ask anything about your data in plain English")

    for _msg in st.session_state["chat_history"]:
        if _msg["role"] == "user":
            st.markdown(
                f'<div class="chat-msg chat-user">'
                f'<b style="color:var(--accent);">You:</b> {_msg["content"]}'
                f'<br><span style="font-size:0.7rem;color:var(--muted);">'
                f'{_msg["time"]}</span></div>',
                unsafe_allow_html=True,
            )
        else:
            _pl = {
                "groq": "⚡ Groq", "gemini": "💎 Gemini",
                "rule_based": "🧠 Rules",
            }.get(_msg.get("provider", ""), "🤖")
            _lb = (
                _msg.get("name", f"AI ({_pl})")
                if _msg["role"] == "tool"
                else f"AI ({_pl})"
            )
            st.markdown(
                f'<div class="chat-msg chat-ai">'
                f'<b style="color:var(--green);">{_lb}</b><br>'
                f'<span style="font-size:0.7rem;color:var(--muted);">'
                f'{_msg["time"]}</span><br><br>'
                f'{_msg["content"]}</div>',
                unsafe_allow_html=True,
            )

    _question = st.text_input(
        "💬 Ask your question:",
        placeholder="e.g., How many signups came from Google this month?",
        key="chat_input",
    )
    _bc1, _bc2 = st.columns([1, 6])
    with _bc1:
        if st.button("🚀 Send", type="primary", use_container_width=True):
            if _question:
                with st.spinner("AI thinking..."):
                    _result = _ai_mod.ask_ai(
                        _question,
                        kpi_df=kpi, utm_df=utm_df, pages_df=pages_df,
                        events_df=events_df, geo_df=geo_df,
                        leads_df=leads_df, period_label=period_label,
                        prev_kpi_df=prev_kpi if enable_comp else None,
                        prev_utm_df=prev_utm_df if enable_comp else None,
                    )
                    st.session_state["chat_history"].append({
                        "role": "user",
                        "content": _question,
                        "time": datetime.now().strftime("%H:%M"),
                    })
                    st.session_state["chat_history"].append({
                        "role": "assistant",
                        "content": _result["answer"],
                        "time": datetime.now().strftime("%H:%M"),
                        "provider": _result.get("provider", "rule_based"),
                    })
                    st.rerun()
    with _bc2:
        if st.session_state["chat_history"]:
            if st.button("🗑️ Clear Chat", use_container_width=True):
                st.session_state["chat_history"] = []
                st.rerun()

# ═══════════════════════════════════════════════════════════════
# PAGE: 🔮 PREDICTIONS
# ═══════════════════════════════════════════════════════════════
elif page == "🔮 Predictions":
    st.markdown(
        '<div class="sec-head">🔮 ML Forecasting & Predictions</div>',
        unsafe_allow_html=True,
    )
    _pred = MOD.get("prediction_engine")
    if not _pred:
        st.error("Prediction Engine not loaded")
        st.stop()

    st.markdown(
        f'<div class="comp-box">'
        f'<b>ℹ️</b> <b style="color:var(--green);">Current Total = REAL DATA</b> '
        f'from pipeline. '
        f'<b style="color:var(--accent);">Predicted = ML FORECASTS</b>.</div>',
        unsafe_allow_html=True,
    )

    horizon = st.selectbox(
        "Horizon", [7, 14, 21, 30, 60, 90], index=1,
        format_func=lambda x: f"{x} days",
    )
    if st.button("🔮 Generate Forecast", type="primary"):
        with st.spinner("Running ML ensemble..."):
            st.session_state["forecast"] = _pred.generate_forecast_report(
                kpi, horizon=horizon,
                prev_kpi_df=prev_kpi if enable_comp else None,
            )

    if st.session_state.get("forecast"):
        _rpt = st.session_state["forecast"]
        st.markdown(
            f"**Generated:** {_rpt['generated_at']} · "
            f"**Horizon:** {_rpt['horizon_days']} days"
        )
        for _key, _data in _rpt["metrics"].items():
            if _data.get("status") == "insufficient_data":
                st.warning(
                    f"⚠️ {_data['name']}: {_data.get('message', '')}"
                )
                continue
            st.markdown(f"#### {_data['name']}")
            _pk = f"predicted_next_{horizon}d"
            _fh = '<div class="kpi-grid">'
            _fh += kpi_h(
                "Current (Real)", _data["current_total"],
                '<span class="badge badge-ok">REAL DATA</span>',
            )
            _fh += kpi_h(
                "Avg/Day", f"{_data['avg_daily']:.1f}",
                f'<span class="badge badge-info">{_data["confidence"]}</span>',
            )
            _fh += kpi_h(
                f"Next {horizon}d", _data.get(_pk, 0),
                f'<span class="kpi-delta">{_data["trend"]}</span>',
            )
            _fh += kpi_h(
                "Best Case", _data.get("best_case_7d", 0),
                '<span class="kpi-delta d-up">+20%</span>',
            )
            _fh += kpi_h(
                "Worst Case", _data.get("worst_case_7d", 0),
                '<span class="kpi-delta d-dn">−20%</span>',
            )
            _fh += "</div>"
            st.markdown(_fh, unsafe_allow_html=True)

            _dp = _data.get("daily_predictions", [])
            if _dp:
                _ds = [d["date"] for d in _dp[:horizon]]
                _vs = [d["predicted"] for d in _dp[:horizon]]
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=_ds, y=[v * 1.2 for v in _vs],
                    fill=None, mode="lines", line=dict(width=0),
                    showlegend=False,
                ))
                fig.add_trace(go.Scatter(
                    x=_ds, y=[v * 0.8 for v in _vs],
                    fill="tonexty", mode="lines", line=dict(width=0),
                    fillcolor="rgba(0,212,255,0.09)", showlegend=False,
                ))
                fig.add_trace(go.Scatter(
                    x=_ds, y=_vs, mode="lines+markers",
                    name="Forecast",
                    line=dict(color=T["accent"], width=2.5),
                    marker=dict(size=4),
                ))
                fig.update_layout(
                    title=f"{_data['name']} Forecast", height=350,
                    **CT(), margin=dict(l=0, r=0, t=40, b=0),
                    hovermode="x unified",
                )
                _pc(fig)
            st.caption(f"Method: {_data['method']}")

# ═══════════════════════════════════════════════════════════════
# PAGE: 📋 REPORTS
# ═══════════════════════════════════════════════════════════════
elif page == "📋 Reports":
    st.markdown(
        '<div class="sec-head">📋 Automated Reports</div>',
        unsafe_allow_html=True,
    )
    _rep = MOD.get("report_generator")
    if not _rep:
        st.error("Report Generator not loaded")
        st.stop()

    rep_type = st.selectbox(
        "Report Type",
        [
            "weekly", "biweekly", "monthly", "quarterly",
            "business", "marketing", "data_analysis", "executive",
        ],
    )
    _rc1, _rc2, _rc3 = st.columns(3)
    with _rc1:
        if st.button("📝 Generate Report", type="primary",
                     use_container_width=True):
            with st.spinner("Generating..."):
                _report = _rep.generate_report(
                    kpi_df=kpi,
                    prev_kpi_df=prev_kpi if enable_comp else None,
                    leads_df=leads_df, utm_df=utm_df,
                    period_type=rep_type, period_label=period_label,
                )
                st.session_state["report"] = _report
                _fp = _rep.save_report(_report)
                st.success(f"✅ Saved: {_fp}")
    with _rc2:
        if (st.button("📨 Telegram Format", use_container_width=True)
                and st.session_state.get("report")):
            st.code(
                _rep.generate_telegram_report(st.session_state["report"]),
                language="markdown",
            )
    with _rc3:
        if (st.button("📤 Send to Telegram", type="primary", use_container_width=True)
                and st.session_state.get("report")):
            with st.spinner("Sending..."):
                try:
                    from telegram_alerts import _send
                    _tg_msg = _rep.generate_telegram_report(st.session_state["report"])
                    _result = _send(_tg_msg)
                    if _result["ok"]:
                        st.success("✅ Sent to Telegram!")
                    else:
                        st.error(f"❌ Failed: {_result.get('error', 'unknown')}")
                except Exception as _e:
                    st.error(f"❌ Error: {_e}")

    if st.session_state.get("report"):
        st.markdown(st.session_state["report"]["markdown"])
        st.download_button(
            "⬇️ Download",
            st.session_state["report"]["markdown"],
            f"eagle3d_{rep_type}_{datetime.now().strftime('%Y%m%d')}.md",
            "text/markdown",
            use_container_width=True,
        )

    # ── SMART TELEGRAM ALERTS ──
    st.markdown("---")
    st.markdown("### 📨 Smart Telegram Alerts")

    try:
        from telegram_alerts import (
            daily_kpi_report, period_report, anomaly_alerts,
            marketing_performance, top_performer_alert, send,
        )
        _tg_ok = True
    except ImportError:
        _tg_ok = False
        st.warning("telegram_alerts module not found")

    if _tg_ok:
        _tg_c1, _tg_c2 = st.columns(2)
        with _tg_c1:
            st.markdown("#### 📊 Reports")
            if st.button("📤 Send Daily Report", use_container_width=True, key="tg_daily"):
                with st.spinner("Sending daily report..."):
                    _msg = daily_kpi_report(kpi, prev_kpi if enable_comp else None, utm_df, leads_df)
                    _r = send(_msg)
                    st.success("✅ Sent!") if _r["ok"] else st.error(f"❌ {_r.get('error')}")

            if st.button("📤 Send Period Report", use_container_width=True, key="tg_period"):
                with st.spinner("Sending period report..."):
                    _msg = period_report(kpi, prev_kpi if enable_comp else None, utm_df, leads_df, period_type=rep_type)
                    _r = send(_msg)
                    st.success("✅ Sent!") if _r["ok"] else st.error(f"❌ {_r.get('error')}")

            if st.button("📤 Marketing Scorecard", use_container_width=True, key="tg_marketing"):
                with st.spinner("Sending marketing scorecard..."):
                    _msg = marketing_performance(kpi, prev_kpi if enable_comp else None, utm_df, leads_df)
                    _r = send(_msg)
                    st.success("✅ Sent!") if _r["ok"] else st.error(f"❌ {_r.get('error')}")

            if st.button("📤 Top Performer Alert", use_container_width=True, key="tg_top"):
                with st.spinner("Sending..."):
                    _msg = top_performer_alert(kpi, utm_df)
                    _r = send(_msg)
                    st.success("✅ Sent!") if _r["ok"] else st.error(f"❌ {_r.get('error')}")

        with _tg_c2:
            st.markdown("#### 🚨 Anomaly Alerts")
            if st.button("📤 Send Anomaly Scan", use_container_width=True, key="tg_anomaly"):
                with st.spinner("Scanning anomalies..."):
                    _alerts = anomaly_alerts(kpi, prev_kpi if enable_comp else None)
                    for _a in _alerts[:3]:
                        _r = send(_a["msg"])
                        if not _r["ok"]:
                            st.error(f"❌ {_r.get('error')}")
                            break
                    else:
                        st.success(f"✅ Sent {len(_alerts[:3])} alert(s)!")

            # Show current anomalies
            st.markdown("#### Current Anomalies")
            _cur_alerts = anomaly_alerts(kpi, prev_kpi if enable_comp else None)
            for _a in _cur_alerts:
                _colors = {"critical": "#FF5252", "warning": "#FFD600", "positive": "#00E676", "info": "#00D4FF"}
                _c = _colors.get(_a["type"], "#94A3B1")
                st.markdown(f'<div style="border-left:3px solid {_c};padding:8px 12px;margin:4px 0;background:#111D32;border-radius:6px;">{_a["msg"][:200]}</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("#### 📁 Report Archive")
    _saved = _rep.list_saved_reports()
    if _saved:
        for _r in _saved[:20]:
            _rf_path = os.path.join("data_output", "reports", _r['filename'])
            with st.expander(f"📄 {_r['filename']} — {_r['modified']} ({_r['size'] / 1024:.1f} KB)"):
                try:
                    _rf_content = Path(_rf_path).read_text() if os.path.exists(_rf_path) else ""
                    if _rf_content:
                        st.markdown(_rf_content[:5000])
                        if len(_rf_content) > 5000:
                            st.caption(f"... ({len(_rf_content):,} total characters)")
                        st.download_button(
                            "⬇️ Download", _rf_content,
                            file_name=_r['filename'], mime="text/markdown",
                            key=f"dl_{_r['filename']}", use_container_width=True,
                        )
                    else:
                        st.info("Report file not found")
                except Exception as _e:
                    st.error(f"Error reading: {_e}")
    else:
        st.info("No saved reports yet. Generate a report first.")

# ═══════════════════════════════════════════════════════════════
# PAGE: 🔔 ALERTS
# ═══════════════════════════════════════════════════════════════
elif page == "🔔 Alerts":
    st.markdown(
        '<div class="sec-head">🔔 Alerts & Anomaly Detection</div>',
        unsafe_allow_html=True,
    )
    cs = km(kpi, "signups")
    cu = km(kpi, "first_uploads")
    cp = km(kpi, "paid_customers")

    _t1, _t2, _t3 = st.columns(3)
    with _t1:
        wt = st.slider("⚠️ Warning %", 10, 50, 20)
    with _t2:
        ct_val = st.slider("🚨 Critical %", 20, 80, 40)
    with _t3:
        mv = st.number_input("Min value", value=1, min_value=0)

    alerts = []
    if enable_comp and not prev_kpi.empty:
        ps = km(prev_kpi, "signups")
        pu = km(prev_kpi, "first_uploads")
        pp = km(prev_kpi, "paid_customers")
        for nm, c, p in [
            ("Sign-ups", cs, ps), ("Uploads", cu, pu), ("Paid", cp, pp)
        ]:
            if p == 0 or c < mv:
                continue
            ch = (c - p) / p * 100
            if abs(ch) >= ct_val:
                sev = "critical"
            elif abs(ch) >= wt:
                sev = "warning"
            else:
                continue
            em = "🚨" if ch < 0 else "📈"
            dr = "dropped" if ch < 0 else "surged"
            alerts.append({
                "sev": sev, "em": em,
                "t": f"{nm} {dr} {abs(ch):.0f}%",
                "m": f"{p} → {c} ({ch:+.1f}%)",
                "a": f"Investigate {nm.lower()}",
            })
    elif not kpi.empty and "signups" in kpi.columns and len(kpi) >= 5:
        vs = kpi["signups"].astype(float)
        mn = vs.mean()
        sd = vs.std()
        if sd > 0:
            for _, row in kpi.iterrows():
                v = float(row.get("signups", 0))
                z = (v - mn) / sd
                if abs(z) >= 2.0:
                    alerts.append({
                        "sev": "critical" if abs(z) >= 3 else "warning",
                        "em": "🚨" if z < 0 else "📈",
                        "t": f"Anomaly {row.get('date', '?')}: {v:.0f}",
                        "m": f"Z-score: {z:.1f}",
                        "a": "Check data or spike",
                    })

    if not alerts:
        st.markdown(
            '<div class="alert-card al-ok" style="text-align:center;padding:30px;">'
            '<div style="font-size:2.5rem;">✅</div>'
            '<div style="font-size:1.1rem;font-weight:700;color:var(--green);">'
            'All KPIs Normal</div></div>',
            unsafe_allow_html=True,
        )
    else:
        for a in alerts:
            cl = "al-crit" if a["sev"] == "critical" else "al-warn"
            st.markdown(
                f'<div class="alert-card {cl}">'
                f'<div style="font-weight:700;">{a["em"]} {a["t"]}</div>'
                f'<div style="color:var(--text);">{a["m"]}</div>'
                f'<div style="color:var(--muted);font-size:0.8rem;margin-top:6px;">'
                f'💡 {a["a"]}</div></div>',
                unsafe_allow_html=True,
            )

    # ── TELEGRAM ALERTS ──
    st.markdown("---")
    st.markdown("### 📨 Send Alerts to Telegram (Per Subsystem)")

    _tg_bot = get_secret("TELEGRAM_BOT_TOKEN", "")
    _tg_chat = get_secret("TELEGRAM_CHAT_ID", "")
    if not _tg_bot or not _tg_chat:
        st.warning("⚠️ Add `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` to secrets to enable Telegram alerts.")
    else:
        _sub_c1, _sub_c2 = st.columns(2)

        with _sub_c1:
            st.markdown("#### 📊 KPI System Alert")
            if st.button("📤 Send KPI Report", use_container_width=True, key="tg_kpi"):
                with st.spinner("Sending KPI report..."):
                    try:
                        from reporting_engine import build_kpi_stats, build_telegram_kpi_section
                        _kpi_data = build_kpi_stats()
                        _msg = build_telegram_kpi_section(_kpi_data)
                        from reporting_engine import send_telegram
                        _ok = send_telegram(_msg)
                        st.success("✅ KPI report sent!") if _ok else st.error("❌ Failed")
                    except Exception as _e:
                        st.error(f"❌ {_e}")

            st.markdown("#### 🌐 GA4 Analytics Alert")
            if st.button("📤 Send GA4 Report", use_container_width=True, key="tg_ga4"):
                with st.spinner("Sending GA4 report..."):
                    try:
                        from reporting_engine import build_ga4_stats, build_telegram_ga4_section, send_telegram
                        _ga4_data = build_ga4_stats()
                        _msg = build_telegram_ga4_section(_ga4_data)
                        send_telegram(_msg)
                        st.success("✅ GA4 report sent!")
                    except Exception as _e:
                        st.error(f"❌ {_e}")

            st.markdown("#### 📺 YouTube Alert")
            if st.button("📤 Send YouTube Report", use_container_width=True, key="tg_yt"):
                with st.spinner("Sending YouTube report..."):
                    try:
                        from reporting_engine import build_youtube_stats, build_telegram_youtube_section, send_telegram
                        _yt_data = build_youtube_stats()
                        _msg = build_telegram_youtube_section(_yt_data)
                        send_telegram(_msg)
                        st.success("✅ YouTube report sent!")
                    except Exception as _e:
                        st.error(f"❌ {_e}")

        with _sub_c2:
            st.markdown("#### 💼 LinkedIn Alert")
            if st.button("📤 Send LinkedIn Report", use_container_width=True, key="tg_li"):
                with st.spinner("Sending LinkedIn report..."):
                    try:
                        from reporting_engine import build_linkedin_stats, build_telegram_linkedin_section, send_telegram
                        _li_data = build_linkedin_stats()
                        _msg = build_telegram_linkedin_section(_li_data)
                        send_telegram(_msg)
                        st.success("✅ LinkedIn report sent!")
                    except Exception as _e:
                        st.error(f"❌ {_e}")

            st.markdown("#### 💳 Stripe Alert")
            if st.button("📤 Send Stripe Report", use_container_width=True, key="tg_stripe"):
                with st.spinner("Sending Stripe report..."):
                    try:
                        from reporting_engine import build_stripe_stats, build_telegram_stripe_section, send_telegram
                        _stripe_data = build_stripe_stats()
                        _msg = build_telegram_stripe_section(_stripe_data)
                        send_telegram(_msg)
                        st.success("✅ Stripe report sent!")
                    except Exception as _e:
                        st.error(f"❌ {_e}")

            st.markdown("#### 🦅 Full System Report")
            if st.button("📤 Send ALL Reports", type="primary", use_container_width=True, key="tg_all"):
                with st.spinner("Sending full system report..."):
                    try:
                        from reporting_engine import main as _rep_main
                        _rep_main()
                        st.success("✅ All subsystem reports sent to Telegram!")
                    except Exception as _e:
                        st.error(f"❌ {_e}")

# ═══════════════════════════════════════════════════════════════
# PAGE: 🔬 EDA LAB
# ═══════════════════════════════════════════════════════════════
elif page == "🔬 EDA Lab":
    st.markdown(
        '<div class="sec-head">🔬 Exploratory Data Analysis</div>',
        unsafe_allow_html=True,
    )
    tabs = st.tabs([
        "📊 Distributions", "📈 Correlations", "🔥 Heatmap",
        "📅 Time Series", "🎯 Cohort",
    ])

    with tabs[0]:
        if not kpi.empty:
            mt = st.selectbox(
                "Metric",
                ["signups", "first_uploads", "paid_customers"],
                key="edm",
            )
            if mt in kpi.columns:
                vs = pd.to_numeric(kpi[mt], errors="coerce").dropna()
                if len(vs) > 0:
                    c1, c2 = st.columns(2)
                    with c1:
                        fig = px.histogram(
                            x=vs, nbins=20,
                            color_discrete_sequence=[T["accent"]],
                        )
                        fig.update_layout(
                            title=f"{mt} Distribution", **CT(),
                            margin=dict(l=0, r=0, t=40, b=0),
                        )
                        _pc(fig)
                    with c2:
                        fig = px.box(
                            y=vs,
                            color_discrete_sequence=[T["accent2"]],
                        )
                        fig.update_layout(
                            title=f"{mt} Box Plot", **CT(),
                            margin=dict(l=0, r=0, t=40, b=0),
                        )
                        _pc(fig)
                    st.markdown("#### Statistics")
                    _sc = st.columns(4)
                    _stats = {
                        "Mean": vs.mean(), "Median": vs.median(),
                        "Std": vs.std(), "Min": vs.min(),
                        "Max": vs.max(), "Skew": vs.skew(),
                        "CV%": (vs.std() / vs.mean() * 100
                                if vs.mean() > 0 else 0),
                    }
                    for idx, (k, v) in enumerate(_stats.items()):
                        with _sc[idx % 4]:
                            st.metric(k, f"{v:.2f}" if isinstance(v, float)
                                      else str(v))
        else:
            st.info("No KPI data available.")

    with tabs[1]:
        if not kpi.empty:
            _nc = [c for c in [
                "signups", "first_uploads", "paid_customers"
            ] if c in kpi.columns]
            if len(_nc) >= 2:
                cr = kpi[_nc].astype(float).corr()
                fig = px.imshow(
                    cr, text_auto=".2f",
                    color_continuous_scale="RdBu_r",
                    aspect="auto", zmin=-1, zmax=1,
                )
                fig.update_layout(
                    title="Correlations", **CT(),
                    margin=dict(l=0, r=0, t=40, b=0),
                )
                _pc(fig)
                xc = st.selectbox("X", _nc, key="ex")
                yc = st.selectbox("Y", [c for c in _nc if c != xc], key="ey")
                fig = px.scatter(
                    kpi, x=xc, y=yc, trendline="ols",
                    color_discrete_sequence=[T["accent"]],
                )
                fig.update_layout(
                    **CT(), margin=dict(l=0, r=0, t=20, b=0),
                )
                _pc(fig)

    with tabs[2]:
        if (not kpi.empty and "date" in kpi.columns
                and "signups" in kpi.columns):
            _h = kpi.copy()
            _h["date"] = pd.to_datetime(_h["date"])
            _h["wd"] = _h["date"].dt.day_name()
            _h["wk"] = _h["date"].dt.isocalendar().week.astype(int)
            _h["signups"] = pd.to_numeric(_h["signups"], errors="coerce")
            pv = _h.pivot_table(
                values="signups", index="wd",
                columns="wk", aggfunc="sum",
            )
            dn = [
                "Monday", "Tuesday", "Wednesday",
                "Thursday", "Friday", "Saturday", "Sunday",
            ]
            pv = pv.reindex([d for d in dn if d in pv.index])
            fig = px.imshow(
                pv, color_continuous_scale="Viridis", aspect="auto",
            )
            fig.update_layout(
                title="Heatmap", height=350, **CT(),
                margin=dict(l=0, r=0, t=40, b=0),
            )
            _pc(fig)

    with tabs[3]:
        if (not kpi.empty and "date" in kpi.columns
                and "signups" in kpi.columns):
            _ts = kpi.copy()
            _ts["date"] = pd.to_datetime(_ts["date"])
            _ts = _ts.sort_values("date")
            _ts["signups"] = pd.to_numeric(
                _ts["signups"], errors="coerce"
            )
            _ts = _ts.dropna(subset=["signups"])
            if len(_ts) >= 7:
                _ts["MA7"] = _ts["signups"].rolling(7, min_periods=1).mean()
                _ts["MA14"] = _ts["signups"].rolling(
                    14, min_periods=1
                ).mean()
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=_ts["date"], y=_ts["signups"], name="Actual",
                    line=dict(color=T["accent"], width=1), opacity=0.6,
                ))
                fig.add_trace(go.Scatter(
                    x=_ts["date"], y=_ts["MA7"], name="7d MA",
                    line=dict(color=T["accent2"], width=2.5),
                ))
                fig.add_trace(go.Scatter(
                    x=_ts["date"], y=_ts["MA14"], name="14d MA",
                    line=dict(color="#FF9100", width=2.5),
                ))
                fig.update_layout(
                    title="Moving Averages", height=400,
                    **CT(), margin=dict(l=0, r=0, t=40, b=0),
                    hovermode="x unified",
                )
                _pc(fig)

    with tabs[4]:
        st.markdown("#### Cohort Analysis")
        if not free_rows.empty and not upload_rows.empty:
            ecf = next(
                (c for c in free_rows.columns if "email" in c.lower()),
                None,
            )
            dcf = next(
                (c for c in free_rows.columns if "date" in c.lower()),
                None,
            )
            ecu = next(
                (c for c in upload_rows.columns if "email" in c.lower()),
                None,
            )
            dcu = next(
                (c for c in upload_rows.columns if "date" in c.lower()),
                None,
            )
            if ecf and dcf and ecu and dcu:
                fc = free_rows.copy()
                uc = upload_rows.copy()
                fc["_e"] = fc[ecf].astype(str).str.lower().str.strip()
                fc["_d"] = fc[dcf].apply(parse_to_date)
                uc["_e"] = uc[ecu].astype(str).str.lower().str.strip()
                uc["_d"] = uc[dcu].apply(parse_to_date)
                fc = fc.dropna(subset=["_e", "_d"])
                uc = uc.dropna(subset=["_e", "_d"])
                fc["month"] = fc["_d"].apply(lambda d: d.replace(day=1))
                mg = fc[["_e", "_d", "month"]].merge(
                    uc[["_e", "_d"]].rename(columns={"_d": "ud"}),
                    on="_e", how="left",
                )
                mg["conv"] = mg["ud"].notna()
                co = mg.groupby("month").agg(
                    s=("_e", "count"), c=("conv", "sum"),
                ).reset_index()
                co["rate"] = (co["c"] / co["s"] * 100).round(1)
                co["month"] = co["month"].astype(str)
                if not co.empty:
                    fig = px.bar(
                        co, x="month", y=["s", "c"],
                        barmode="group",
                        color_discrete_sequence=[T["accent"], T["green"]],
                    )
                    fig.update_layout(
                        title="Signup Cohort", height=350,
                        **CT(), margin=dict(l=0, r=0, t=40, b=0),
                    )
                    _pc(fig)
                    _df(co)

# ═══════════════════════════════════════════════════════════════
# PAGE: 🔍 BROWSE DATA
# ═══════════════════════════════════════════════════════════════
elif page == "🔍 Browse Data":
    st.markdown(
        '<div class="sec-head">🔍 Browse Customer Data</div>',
        unsafe_allow_html=True,
    )

    # Global date filter for Browse Data
    _bd_c1, _bd_c2, _bd_c3, _bd_c4 = st.columns(4)
    with _bd_c1:
        _bd_preset = st.selectbox("📅 Date filter:", [
            "This Month", "Last Month", "All Time", "Today", "Yesterday", "This Week", "Last Week",
            "Last 7 Days", "Last 14 Days", "Last 28 Days", "Last 30 Days",
            "Last 3 Months", "Last 6 Months", "This Year", "Custom Range", "Custom Month",
        ], index=0, key="bd_preset")
    with _bd_c2:
        if _bd_preset == "Custom Month":
            _bd_month = st.selectbox("Month:", [
                f"{y}-{m:02d}" for y in range(2026, 2023, -1) for m in range(12, 0, -1)
            ], index=0, key="bd_month_sel")
        else:
            _bd_month = None
    with _bd_c3:
        _bd_status = st.selectbox("Status:", ["All", "ACCEPTED", "REJECTED", "REPEAT", "REPEAT_UPLOAD", "NOT_DETERMINED", "DISPOSABLE", "INTERNAL", "SMTP_REJECTED", "NO_MX", "ZERO_SPEND"], key="bd_status")
    with _bd_c4:
        _bd_search = st.text_input("🔍 Search", placeholder="email, name...", key="bd_search")

    # Compute date range from preset
    _bd_today = datetime.now().date()
    _bd_date_map = {
        "All Time": (datetime(2020, 1, 1).date(), _bd_today),
        "Today": (_bd_today, _bd_today),
        "Yesterday": (_bd_today - timedelta(days=1), _bd_today - timedelta(days=1)),
        "This Week": (_bd_today - timedelta(days=_bd_today.weekday()), _bd_today),
        "Last Week": (_bd_today - timedelta(days=_bd_today.weekday() + 7), _bd_today - timedelta(days=_bd_today.weekday() + 1)),
        "This Month": (_bd_today.replace(day=1), _bd_today),
        "Last Month": ((_bd_today.replace(day=1) - timedelta(days=1)).replace(day=1), _bd_today.replace(day=1) - timedelta(days=1)),
        "Last 7 Days": (_bd_today - timedelta(days=6), _bd_today),
        "Last 14 Days": (_bd_today - timedelta(days=13), _bd_today),
        "Last 28 Days": (_bd_today - timedelta(days=27), _bd_today),
        "Last 30 Days": (_bd_today - timedelta(days=29), _bd_today),
        "Last 3 Months": (_bd_today - timedelta(days=90), _bd_today),
        "Last 6 Months": (_bd_today - timedelta(days=180), _bd_today),
        "This Year": (_bd_today.replace(month=1, day=1), _bd_today),
    }
    if _bd_preset == "Custom Range":
        with _bd_c2:
            _bd_cr_start = st.date_input("Start", value=_bd_today - timedelta(days=30), key="bd_cr_start")
        with _bd_c3:
            _bd_cr_end = st.date_input("End", value=_bd_today, key="bd_cr_end")
        _bd_ds, _bd_de = _bd_cr_start, _bd_cr_end
    elif _bd_preset == "Custom Month" and _bd_month:
        _m_parts = _bd_month.split("-")
        _m_y, _m_m = int(_m_parts[0]), int(_m_parts[1])
        _m_start = datetime(_m_y, _m_m, 1).date()
        if _m_m == 12:
            _m_end = datetime(_m_y + 1, 1, 1).date() - timedelta(days=1)
        else:
            _m_end = datetime(_m_y, _m_m + 1, 1).date() - timedelta(days=1)
        _bd_ds, _bd_de = _m_start, _m_end
    else:
        _bd_ds, _bd_de = _bd_date_map.get(_bd_preset, (datetime(2020, 1, 1).date(), _bd_today))

    st.caption(f"📅 Showing: **{_bd_ds}** → **{_bd_de}**")

    def _apply_browse_filters(df, label):
        """Apply date, status, and search filters to a dataframe."""
        fl = df.copy()
        # Date filter: find the date column
        _date_col = None
        for _cand in fl.columns:
            _cl = _cand.lower()
            if any(k in _cl for k in ["created", "upload date", "date"]):
                if "detail" not in _cl and "last" not in _cl:
                    _date_col = _cand
                    break
        if _date_col and _bd_preset != "All Time":
            fl["_parsed_date"] = fl[_date_col].apply(parse_to_date)
            fl = fl[fl["_parsed_date"].between(_bd_ds, _bd_de)]
            fl = fl.drop(columns=["_parsed_date"])
        # Status filter
        if _bd_status != "All" and "final_status" in fl.columns:
            fl = fl[fl["final_status"].astype(str).str.upper() == _bd_status]
        # Search
        if _bd_search:
            msk = pd.Series([False] * len(fl), index=fl.index)
            for c in fl.columns:
                msk = msk | fl[c].astype(str).str.contains(_bd_search, case=False, na=False)
            fl = fl[msk]
        return fl

    # ── INLINE OVERRIDE ENGINE ──
    _mo_engine = MOD.get("manual_override_engine")

    tabs = st.tabs(["📥 Sign-ups", "📦 First Uploads", "💳 Stripe"])
    for _tab, (_lb, _df_data) in zip(
        tabs,
        [
            ("Sign-ups", free_rows),
            ("First Uploads", upload_rows),
            ("Stripe", stripe_raw),
        ],
    ):
        with _tab:
            if _df_data.empty:
                st.warning(f"No {_lb} data — check Google Sheets connection")
                continue

            # Apply all filters
            fl = _apply_browse_filters(_df_data, _lb)

            # Sort + display controls
            c1, c2, c3 = st.columns(3)
            with c1:
                _sort_cols = ["—"] + [c for c in fl.columns if c]
                _sort_sel = st.selectbox("Sort by:", _sort_cols, key=f"sort_{_lb}")
            with c2:
                _sort_asc = st.radio("Direction", ["↓ Desc", "↑ Asc"], key=f"sortd_{_lb}", horizontal=True)
            with c3:
                mr = st.number_input("Rows", value=500, min_value=10, max_value=10000, key=f"r_{_lb}")

            # Apply sorting
            if _sort_sel != "—" and _sort_sel in fl.columns:
                _ascending = (_sort_asc == "↑ Asc")
                try:
                    fl[_sort_sel] = pd.to_numeric(fl[_sort_sel], errors="ignore")
                except Exception:
                    pass
                fl = fl.sort_values(by=_sort_sel, ascending=_ascending, na_position="last")

            st.metric("Showing", f"{len(fl)} rows")

            # ── INLINE OVERRIDE SECTION ──
            if _mo_engine and "final_status" in fl.columns:
                with st.expander("✏️ Override Labels", expanded=False):
                    st.caption("Select emails to change their status. Changes apply to the override system.")
                    _email_col = None
                    for _ec in fl.columns:
                        if "email" in _ec.lower():
                            _email_col = _ec
                            break
                    if _email_col:
                        _avail_emails = fl[_email_col].unique().tolist()
                        _sel_emails = st.multiselect(
                            f"Select emails from {_lb}:",
                            _avail_emails[:200],
                            key=f"ov_sel_{_lb}",
                        )
                        _ov_action = st.selectbox(
                            "Change to:",
                            ["ACCEPTED", "REJECTED", "REPEAT_UPLOAD", "DISPOSABLE", "NOT_DETERMINED"],
                            key=f"ov_act_{_lb}",
                        )
                        _ov_reason = st.text_input("Reason (optional):", key=f"ov_reason_{_lb}")
                        if _sel_emails and st.button(f"✅ Apply Override ({len(_sel_emails)} emails)", type="primary", key=f"ov_btn_{_lb}", use_container_width=True):
                            _ov_count = 0
                            for _se in _sel_emails:
                                # Get the ORIGINAL status from raw data (before any overrides)
                                _orig_df = {"Sign-ups": free_raw, "First Uploads": upload_raw, "Stripe": stripe_raw}.get(_lb, free_raw)
                                _orig = _orig_df.loc[_orig_df[_email_col] == _se, "final_status"].values if _email_col in _orig_df.columns else []
                                _orig_status = str(_orig[0]) if len(_orig) > 0 else "UNKNOWN"
                                _action_map = {
                                    "ACCEPTED": "accept", "REJECTED": "reject",
                                    "REPEAT_UPLOAD": "repeat_upload", "DISPOSABLE": "disposable",
                                    "NOT_DETERMINED": "not_determined",
                                }
                                _mo_engine.apply_override(
                                    email=_se,
                                    action=_action_map.get(_ov_action, "not_determined"),
                                    target_tab="ALL",
                                    reason=_ov_reason or f"Changed to {_ov_action} via Browse Data",
                                    original_category=_orig_status,
                                )
                                _ov_count += 1
                            st.success(f"✅ Overrode {_ov_count} emails → {_ov_action}. Counts updated everywhere!")
                            st.session_state["_ov_toast_shown"] = False  # Reset so delta toast shows
                            st.cache_data.clear()
                            st.rerun()
                    else:
                        st.info("No email column found for override.")
            elif not _mo_engine:
                st.caption("⚠️ Manual override engine not loaded")

            # Show project links for First Uploads tab
            if _lb == "First Uploads" and not fl.empty:
                _proj_cols = [c for c in fl.columns if any(k in c.lower() for k in ["project", "url", "link", "scene", "upload_url"])]
                _email_col_bd = next((c for c in fl.columns if "email" in c.lower()), None)
                if _proj_cols or _email_col_bd:
                    with st.expander("🔗 Project Links", expanded=False):
                        _display_rows = fl.head(50)
                        for _, _pr in _display_rows.iterrows():
                            _em = str(_pr.get(_email_col_bd, "")) if _email_col_bd else ""
                            _links = []
                            for _pc_col in _proj_cols:
                                _pv = str(_pr.get(_pc_col, ""))
                                if _pv and _pv not in ("nan", "None", ""):
                                    if _pv.startswith("http"):
                                        _links.append(f"([{_pc_col}]({_pv}))")
                                    else:
                                        _links.append(f"{_pc_col}: {_pv}")
                            _date_col = next((c for c in fl.columns if "date" in c.lower() and "upload" in c.lower()), None)
                            _dt = str(_pr.get(_date_col, "")) if _date_col else ""
                            if _links:
                                st.markdown(f"📤 **{_em}** ({_dt}) — {' | '.join(_links)}")
                            elif _em:
                                st.markdown(f"📤 **{_em}** ({_dt})")

            _df(fl.head(mr), height=450)
            st.download_button(
                "⬇️ Download",
                data=fl.to_csv(index=False).encode("utf-8"),
                file_name=f"{_lb.lower().replace(' ', '_')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

            # Stripe data check
            if _lb == "Stripe" and fl.empty:
                st.warning("⚠️ No Stripe data loaded. The Stripe scraper may be timing out. Check pipeline logs.")

# ═══════════════════════════════════════════════════════════════
# PAGE: ✏️ MANUAL OVERRIDE
# ═══════════════════════════════════════════════════════════════
elif page == "✏️ Manual Override":
    st.markdown(
        '<div class="sec-head">✏️ Manual Data Entry & Override</div>',
        unsafe_allow_html=True,
    )
    _mo_tab1, _mo_tab2, _mo_tab3, _mo_tab4 = st.tabs([
        "📝 Add Daily Entry", "📂 Bulk CSV Import", "📋 Entries Log", "🔧 Override Manager",
    ])
    MANUAL_DATA_FILE = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "data_output", "manual_kpi_data.json",
    )
    def _load_manual():
        if os.path.exists(MANUAL_DATA_FILE):
            try:
                with open(MANUAL_DATA_FILE, "r") as _f:
                    return json.load(_f)
            except Exception:
                pass
        return {"daily": []}
    def _save_manual(data):
        os.makedirs(os.path.dirname(MANUAL_DATA_FILE), exist_ok=True)
        with open(MANUAL_DATA_FILE, "w") as _f:
            json.dump(data, _f, indent=2)

    with _mo_tab1:
        st.markdown("#### 📝 Add a Daily KPI Entry")
        _oc1, _oc2 = st.columns(2)
        with _oc1:
            _entry_date = st.date_input("Date", value=datetime.now().date(), key="mo_date")
        with _oc2:
            _entry_signups = st.number_input("Sign-ups", min_value=0, value=0, key="mo_s")
        _oc3, _oc4 = st.columns(2)
        with _oc3:
            _entry_uploads = st.number_input("First Uploads", min_value=0, value=0, key="mo_u")
        with _oc4:
            _entry_paid = st.number_input("Paid Customers", min_value=0, value=0, key="mo_p")
        if st.button("💾 Save Entry", type="primary", use_container_width=True):
            _md = _load_manual()
            _date_str = _entry_date.strftime("%Y-%m-%d")
            _found = False
            for _row in _md["daily"]:
                if _row.get("date") == _date_str:
                    _row["signups"] = _entry_signups
                    _row["first_uploads"] = _entry_uploads
                    _row["paid_customers"] = _entry_paid
                    _found = True
                    break
            if not _found:
                _md["daily"].append({
                    "date": _date_str,
                    "signups": _entry_signups,
                    "first_uploads": _entry_uploads,
                    "paid_customers": _entry_paid,
                })
            try:
                _save_manual(_md)
                st.success(f"✅ Saved: {_date_str} — {_entry_signups} sign-ups, {_entry_uploads} uploads, {_entry_paid} paid")
            except OSError:
                st.warning("⚠️ Cannot write on Streamlit Cloud (read-only). Works locally only.")

    with _mo_tab2:
        st.markdown("#### 📂 Bulk CSV Import")
        st.caption("Upload a CSV with columns: date, signups, first_uploads, paid_customers")
        _csv_file = st.file_uploader("Choose CSV", type=["csv"], key="mo_csv")
        if _csv_file is not None:
            try:
                _csv_df = pd.read_csv(_csv_file)
                st.dataframe(_csv_df.head(10), use_container_width=True)
                if st.button("📥 Import All Rows", type="primary"):
                    _md = _load_manual()
                    _existing = {r["date"] for r in _md["daily"]}
                    _added = 0
                    for _, _row in _csv_df.iterrows():
                        _d = str(_row.get("date", ""))
                        if _d and _d not in _existing:
                            _md["daily"].append({
                                "date": _d,
                                "signups": int(pd.to_numeric(_row.get("signups", 0), errors="coerce") or 0),
                                "first_uploads": int(pd.to_numeric(_row.get("first_uploads", 0), errors="coerce") or 0),
                                "paid_customers": int(pd.to_numeric(_row.get("paid_customers", 0), errors="coerce") or 0),
                            })
                            _added += 1
                    try:
                        _save_manual(_md)
                        st.success(f"✅ Imported {_added} new entries")
                    except OSError:
                        st.warning("⚠️ Cannot write on Streamlit Cloud.")
            except Exception as _e:
                st.error(f"CSV error: {_e}")

    with _mo_tab3:
        st.markdown("#### 📋 Current Manual Entries")
        _md = _load_manual()
        if _md["daily"]:
            _log_df = pd.DataFrame(_md["daily"])
            _df(_log_df, height=400)
            if st.button("🗑️ Clear All Manual Entries", type="secondary"):
                try:
                    _save_manual({"daily": []})
                    st.success("Cleared!")
                    st.rerun()
                except OSError:
                    st.warning("⚠️ Cannot write on Streamlit Cloud.")
        else:
            st.info("No manual entries yet.")

    # ── OVERRIDE MANAGER TAB ──
    with _mo_tab4:
        st.markdown("#### 🔧 Override Manager")
        st.caption("View, manage, and remove label overrides. Changes update counts everywhere instantly.")
        _mo_engine4 = MOD.get("manual_override_engine")
        if _mo_engine4:
            _ov_summary = _mo_engine4.get_override_summary()
            if _ov_summary["total"] > 0:
                # Show impact summary
                st.metric("Active Overrides", _ov_summary["total"])
                c_a1, c_a2 = st.columns(2)
                with c_a1:
                    for act, cnt in _ov_summary.get("by_action", {}).items():
                        _act_label = {
                            "accept": "✅ Accepted", "reject": "❌ Rejected",
                            "disposable": "🗑️ Disposable", "duplicate": "📋 Duplicate",
                            "repeat_upload": "🔄 Repeat Upload", "not_determined": "❓ Not Determined",
                        }.get(act, act)
                        st.metric(_act_label, cnt)
                with c_a2:
                    st.metric("Impact", f"{_ov_summary['total']} emails affected")

                if st.button("🗑️ Clear All Overrides", type="secondary", use_container_width=True):
                    _mo_engine4.save_overrides({})
                    st.session_state["_ov_toast_shown"] = False
                    st.success("✅ All overrides cleared! Counts restored to original.")
                    st.cache_data.clear()
                    st.rerun()

                st.markdown("---")
                st.markdown("##### Override Details")
                st.caption("Click ❌ to remove an override. The email will revert to its original status.")
                _ovs = _mo_engine4.load_overrides()
                for _ov_em, _ov_data in _ovs.items():
                    c_r1, c_r2, c_r3, c_r4 = st.columns([3, 2, 2, 1])
                    with c_r1:
                        st.text(f"📧 {_ov_em}")
                    with c_r2:
                        _act_nice = {
                            "accept": "→ ACCEPTED", "reject": "→ REJECTED",
                            "disposable": "→ DISPOSABLE", "duplicate": "→ DUPLICATE",
                            "repeat_upload": "→ REPEAT_UPLOAD", "not_determined": "→ NOT_DETERMINED",
                        }.get(_ov_data.get("action", "?"), f"→ {_ov_data.get('action', '?')}")
                        st.text(_act_nice)
                    with c_r3:
                        _orig = _ov_data.get("original_category", "Unknown")
                        st.text(f"was: {_orig}")
                    with c_r4:
                        if st.button("❌", key=f"rm_ov_{_ov_em}", help=f"Remove override for {_ov_em}"):
                            _mo_engine4.remove_override(_ov_em)
                            st.session_state["_ov_toast_shown"] = False
                            st.cache_data.clear()
                            st.rerun()
            else:
                st.info("No active overrides. Use Browse Data → Override Labels to add them.")
        else:
            st.warning("⚠️ Override engine not loaded")


# ═══════════════════════════════════════════════════════════════
# PAGE: 📺 YOUTUBE ANALYTICS (Full Command Center)
# ═══════════════════════════════════════════════════════════════
elif page == "📺 YouTube":
    st.markdown(
        '<div class="sec-head">📺 YouTube Command Center</div>',
        unsafe_allow_html=True,
    )
    try:
        from youtube_connector import (
            get_channel_info, get_channel_videos, get_daily_analytics,
            get_subscriber_growth, get_traffic_sources, get_demographics,
            get_revenue, get_revenue_daily, get_top_videos, get_search_terms,
            get_views_by_playback, get_sharing_services, get_playlist_analytics,
            get_video_analytics_batch, get_retention_curve,
            calculate_performance_score, get_engagement_rating, get_retention_rating,
            diagnose_video, get_score_label, format_number,
            is_configured, has_analytics_access, get_status, _parse_duration, _format_duration,
        )
    except Exception as e:
        st.error(f"YouTube connector not loaded: {e}")
        st.stop()

    _yt_status = get_status()
    _ch_info = get_channel_info()
    _has_oauth = has_analytics_access()

    # ── Status bar ──
    _s1, _s2, _s3, _s4 = st.columns(4)
    with _s1:
        st.metric("Subscribers", f"{_ch_info.get('subscribers', 0):,}")
    with _s2:
        st.metric("Total Views", f"{_ch_info.get('total_views', 0):,}")
    with _s3:
        st.metric("Videos", _ch_info.get('video_count', 0))
    with _s4:
        # Verify OAuth actually works by checking if we can get analytics
        _oauth_works = False
        if _has_oauth:
            try:
                _test_daily = get_daily_analytics(
                    (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
                    datetime.now().strftime("%Y-%m-%d")
                )
                _oauth_works = not _test_daily.empty
            except Exception:
                _oauth_works = False
        _data_src = "✅ OAuth" if _oauth_works else "📋 Public API"
        st.metric("Data", _data_src)

    if not _yt_status["configured"]:
        st.warning("⚠️ YouTube not configured. Add `YOUTUBE_API_KEY` and `YOUTUBE_CHANNEL_ID` to secrets.")
        with st.expander("📖 Setup Guide"):
            st.markdown("""
            **To connect YouTube:**
            1. Go to [Google Cloud Console](https://console.cloud.google.com/)
            2. Enable **YouTube Data API v3**
            3. Create credentials → API Key
            4. Find your Channel ID from [youtube.com/account](https://www.youtube.com/account)
            5. Add to **Streamlit Cloud Secrets**:
            ```toml
            YOUTUBE_API_KEY = "AIza..."
            YOUTUBE_CHANNEL_ID = "UC..."
            ```

            **For full Analytics (CTR, retention, watch time, revenue):**
            6. Enable **YouTube Analytics API** in Google Cloud Console
            7. Create **OAuth 2.0 Client ID** credentials (Desktop app)
            8. Run the OAuth helper script (see YouTube tab in Settings)
            9. Add `YOUTUBE_OAUTH_TOKEN` and optionally `YOUTUBE_REFRESH_TOKEN`, `YOUTUBE_CLIENT_ID`, `YOUTUBE_CLIENT_SECRET` to secrets
            """)
        st.stop()

    st.markdown("---")

    # ── Date range for analytics ──
    _yt_period = st.selectbox("📅 Period", ["Last 7 Days", "Last 14 Days", "Last 28 Days", "Last 30 Days", "Last 90 Days", "Last 180 Days", "Last 365 Days", "All Time"], index=2, key="yt_period")
    _yt_days_map = {"Last 7 Days": 7, "Last 14 Days": 14, "Last 28 Days": 28, "Last 30 Days": 30, "Last 90 Days": 90, "Last 180 Days": 180, "Last 365 Days": 365, "All Time": 3650}
    _yt_days = _yt_days_map.get(_yt_period, 28)
    _yt_start = (datetime.now() - timedelta(days=_yt_days)).strftime("%Y-%m-%d")
    _yt_end = datetime.now().strftime("%Y-%m-%d")

    # ── Tabs matching Command Center ──
    _yt_tabs = st.tabs([
        "📊 Dashboard", "🎬 All Videos", "📈 Analytics", "👥 Audience",
        "💰 Revenue", "🔍 Traffic", "🎵 Playlists", "💡 Ask AI",
    ])

    # ════════════════════════════════════════════════════════════
    # TAB 0: DASHBOARD (Overview + Best/Worst + Scored Videos)
    # ════════════════════════════════════════════════════════════
    with _yt_tabs[0]:
        st.markdown("#### 📊 Channel Dashboard")

        # Load videos
        if "yt_videos" not in st.session_state:
            with st.spinner("Loading video library..."):
                st.session_state["yt_videos"] = get_channel_videos(max_videos=300)

        _vids = st.session_state.get("yt_videos", [])
        _subs = _ch_info.get("subscribers", 1000)

        # Aggregate stats
        _total_views = sum(v["views"] for v in _vids)
        _total_likes = sum(v["likes"] for v in _vids)
        _total_comments = sum(v["comments"] for v in _vids)
        _avg_eng = np.mean([v["engagement_rate"] for v in _vids if v["views"] > 0]) if _vids else 0

        # OAuth analytics for the period
        _daily_data = pd.DataFrame()
        _period_views = 0
        _period_watch_min = 0
        _period_avg_dur = 0
        _period_subs_gained = 0
        _period_subs_lost = 0

        if _has_oauth:
            try:
                _daily_data = get_daily_analytics(_yt_start, _yt_end)
            except Exception:
                _daily_data = pd.DataFrame()
            if not _daily_data.empty:
                _period_views = int(_daily_data["views"].sum())
                _period_watch_min = float(_daily_data.get("estimatedMinutesWatched", pd.Series([0])).sum())
                _period_avg_dur = float(_daily_data.get("averageViewDuration", pd.Series([0])).mean())
                _period_subs_gained = int(_daily_data.get("subscribersGained", pd.Series([0])).sum())
                _period_subs_lost = int(_daily_data.get("subscribersLost", pd.Series([0])).sum())

        # KPI row
        _k1, _k2, _k3, _k4, _k5, _k6 = st.columns(6)
        with _k1:
            _views_display = _period_views if _has_oauth and _period_views > 0 else _total_views
            st.metric("Total Views", format_number(_views_display))
        with _k2:
            st.metric("Subscribers", f"{_ch_info.get('subscribers', 0):,}")
        with _k3:
            st.metric("Total Likes", format_number(_total_likes))
        with _k4:
            st.metric("Comments", format_number(_total_comments))
        with _k5:
            st.metric("Avg Engagement", f"{_avg_eng:.1f}%")
        with _k6:
            if _has_oauth and _period_watch_min > 0:
                st.metric("Watch Hours", f"{_period_watch_min/60:.0f}")
            else:
                st.metric("Videos", f"{len(_vids)}")

        # Daily views chart
        if _has_oauth and not _daily_data.empty:
            st.markdown("##### 📈 Daily Views Trend")
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=_daily_data["day"], y=_daily_data["views"],
                fill="tozeroy", line=dict(color=T["accent"], width=2),
                fillcolor="rgba(0,212,255,0.08)",
            ))
            fig.update_layout(height=300, **CT(), margin=dict(l=40, r=20, t=20, b=20))
            _pc(fig)
        elif not _has_oauth:
            st.info("💡 Connect YouTube Analytics API (OAuth) for daily trends, CTR, watch time & revenue data.")

        st.markdown("---")

        # ── Scored videos ──
        if _vids:
            _scored = []
            for v in _vids:
                score = calculate_performance_score(
                    views=v["views"], likes=v["likes"], comments=v["comments"],
                    published_at=v.get("published_at", ""), subscribers=_subs,
                )
                _scored.append({**v, "score": score})

            _scored.sort(key=lambda x: x["score"], reverse=True)

            # Best performing
            if _scored:
                _best = _scored[0]
                _worst = _scored[-1]

                _bc, _wc = st.columns(2)
                with _bc:
                    st.markdown("##### 🏆 Best Performing")
                    _b_eng = get_engagement_rating(_best["engagement_rate"])
                    st.markdown(f"**{_best['title'][:80]}**")
                    st.caption(f"Views: {format_number(_best['views'])} | Eng: {_best['engagement_rate']:.1f}% | Likes: {_best['likes']} | Score: {_best['score']}/100 {_b_eng['color']}")
                    if st.button("🔍 Analyze Best", key="analyze_best"):
                        st.session_state["yt_analyze_vid"] = _best["video_id"]

                with _wc:
                    st.markdown("##### 💀 Worst Performing")
                    _w_eng = get_engagement_rating(_worst["engagement_rate"])
                    st.markdown(f"**{_worst['title'][:80]}**")
                    st.caption(f"Views: {format_number(_worst['views'])} | Eng: {_worst['engagement_rate']:.1f}% | Likes: {_worst['likes']} | Score: {_worst['score']}/100 {_w_eng['color']}")
                    if st.button("🔍 Analyze Worst", key="analyze_worst"):
                        st.session_state["yt_analyze_vid"] = _worst["video_id"]

            st.markdown("---")

            # ── All videos ranked ──
            st.markdown(f"##### 📋 All Videos ({len(_scored)} videos, ranked by score)")
            _rank_data = []
            for i, v in enumerate(_scored[:50]):
                _eng_r = get_engagement_rating(v["engagement_rate"])
                days_pub = ""
                if v.get("published_at"):
                    try:
                        pub = datetime.fromisoformat(v["published_at"].replace("Z", "+00:00"))
                        days_pub = f"{(datetime.now(pub.tzinfo) - pub).days}d ago"
                    except Exception:
                        pass
                _rank_data.append({
                    "#": i + 1,
                    "Title": v["title"][:50],
                    "Score": f"{v['score']}/100",
                    "Views": v["views"],
                    "Likes": v["likes"],
                    "Eng%": f"{v['engagement_rate']:.1f}%",
                    "Rating": f"{_eng_r['color']} {_eng_r['label']}",
                    "Duration": v["duration_label"],
                    "Age": days_pub,
                })
            _df(pd.DataFrame(_rank_data))

    # ════════════════════════════════════════════════════════════
    # TAB 1: ALL VIDEOS (Detailed video cards)
    # ════════════════════════════════════════════════════════════
    with _yt_tabs[1]:
        st.markdown("#### 🎬 Video Library")

        _vids = st.session_state.get("yt_videos", [])
        if not _vids:
            if st.button("🔄 Load All Videos", use_container_width=True):
                with st.spinner("Loading..."):
                    st.session_state["yt_videos"] = get_channel_videos(max_videos=300)
                    st.rerun()
        else:
            # Filters
            _fc1, _fc2, _fc3 = st.columns(3)
            with _fc1:
                _sort_by = st.selectbox("Sort by", ["Score", "Views", "Engagement", "Date", "Duration"], index=0)
            with _fc2:
                _vid_search = st.text_input("🔍 Search videos", "")
            with _fc3:
                if st.button("🔄 Reload"):
                    st.session_state.pop("yt_videos", None)
                    st.rerun()

            _subs = _ch_info.get("subscribers", 1000)

            # Score and sort
            _scored = []
            for v in _vids:
                score = calculate_performance_score(
                    views=v["views"], likes=v["likes"], comments=v["comments"],
                    published_at=v.get("published_at", ""), subscribers=_subs,
                )
                _scored.append({**v, "score": score})

            # Filter
            if _vid_search:
                _scored = [v for v in _scored if _vid_search.lower() in v["title"].lower()]

            # Sort
            if _sort_by == "Views":
                _scored.sort(key=lambda x: x["views"], reverse=True)
            elif _sort_by == "Engagement":
                _scored.sort(key=lambda x: x["engagement_rate"], reverse=True)
            elif _sort_by == "Date":
                _scored.sort(key=lambda x: x.get("published_at", ""), reverse=True)
            elif _sort_by == "Duration":
                _scored.sort(key=lambda x: x["duration_seconds"], reverse=True)
            else:
                _scored.sort(key=lambda x: x["score"], reverse=True)

            st.caption(f"Showing {len(_scored)} videos")

            # Display as table with expandable details
            for i, v in enumerate(_scored[:30]):
                _eng_r = get_engagement_rating(v["engagement_rate"])
                _score_lbl = get_score_label(v["score"])

                with st.expander(f"{'🔥' if v['score'] >= 50 else '⚠️' if v['score'] >= 30 else '❌'} {i+1}. {v['title'][:70]} — {v['score']}/100 | {v['views']:,} views | {_eng_r['color']} {_eng_r['label']}"):
                    _vc1, _vc2, _vc3, _vc4 = st.columns(4)
                    with _vc1:
                        st.metric("Views", f"{v['views']:,}")
                    with _vc2:
                        st.metric("Likes", f"{v['likes']:,}")
                    with _vc3:
                        st.metric("Comments", f"{v['comments']:,}")
                    with _vc4:
                        st.metric("Score", f"{v['score']}/100")

                    _vc5, _vc6, _vc7 = st.columns(3)
                    with _vc5:
                        st.metric("Engagement", f"{v['engagement_rate']:.1f}%")
                    with _vc6:
                        st.metric("Duration", v["duration_label"])
                    with _vc7:
                        st.metric("Published", v.get("published_at", "")[:10])

                    # Diagnose
                    _diagnosis = diagnose_video(
                        views=v["views"], likes=v["likes"], comments=v["comments"],
                        published_at=v.get("published_at", ""), subscribers=_subs,
                    )
                    st.markdown("**Diagnosis:**")
                    for d in _diagnosis:
                        _icon = {"critical": "🔴", "warning": "🟡", "minor": "🔵", "good": "🟢"}.get(d["severity"], "⚪")
                        st.markdown(f"{_icon} **{d['issue']}** — {d['fix']}")

                    # Links
                    st.markdown(f"[▶ Watch](https://youtube.com/watch?v={v['video_id']}) | [📊 Studio](https://studio.youtube.com/video/{v['video_id']}/analytics)")

    # ════════════════════════════════════════════════════════════
    # TAB 2: ANALYTICS (Daily trends, subscriber growth)
    # ════════════════════════════════════════════════════════════
    with _yt_tabs[2]:
        st.markdown("#### 📈 Channel Analytics")

        _daily = pd.DataFrame()
        if _has_oauth:
            try:
                _daily = get_daily_analytics(_yt_start, _yt_end)
            except Exception:
                _daily = pd.DataFrame()

        if _daily.empty:
            # Fallback: build analytics from public video data
            st.caption("📋 OAuth analytics unavailable — showing aggregated video data from Data API")
            try:
                _pub_vids = st.session_state.get("yt_videos", [])
                if not _pub_vids:
                    _pub_vids = get_channel_videos(max_videos=300)
                    st.session_state["yt_videos"] = _pub_vids
                if _pub_vids:
                    from collections import defaultdict as _dd_ya
                    _ya = _dd_ya(lambda: {"views": 0, "likes": 0, "comments": 0})
                    for _v in _pub_vids:
                        _vd = _v.get("published_at", "")
                        if _vd:
                            _vdate = _vd[:10]
                            if _yt_start <= _vdate <= _yt_end:
                                _ya[_vdate]["views"] += _v.get("views", 0)
                                _ya[_vdate]["likes"] += _v.get("likes", 0)
                                _ya[_vdate]["comments"] += _v.get("comments", 0)
                    if _ya:
                        _daily = pd.DataFrame([
                            {"day": d, **_ya[d]} for d in sorted(_ya.keys())
                        ])
            except Exception:
                pass

        if _daily.empty:
            st.warning("⚠️ No analytics data available. Connect YouTube OAuth for full analytics, or ensure Data API key is configured.")
            st.stop()

        # Summary metrics
        _a1, _a2, _a3, _a4, _a5, _a6 = st.columns(6)
        with _a1:
            st.metric("Views", format_number(int(_daily["views"].sum())))
        with _a2:
            _wh = float(_daily.get("estimatedMinutesWatched", pd.Series([0])).sum()) / 60
            st.metric("Watch Hours", f"{_wh:.0f}")
        with _a3:
            _ad = float(_daily.get("averageViewDuration", pd.Series([0])).mean())
            st.metric("Avg Duration", _format_duration(int(_ad)))
        with _a4:
            st.metric("Likes", format_number(int(_daily.get("likes", pd.Series([0])).sum())))
        with _a5:
            st.metric("Comments", format_number(int(_daily.get("comments", pd.Series([0])).sum())))
        with _a6:
            _sg = int(_daily.get("subscribersGained", pd.Series([0])).sum())
            _sl = int(_daily.get("subscribersLost", pd.Series([0])).sum())
            st.metric("Net Subs", f"+{_sg}/-{_sl}")

        # Engagement rate
        _total_v = int(_daily["views"].sum())
        _total_l = int(_daily.get("likes", pd.Series([0])).sum())
        _total_c = int(_daily.get("comments", pd.Series([0])).sum())
        _total_s = int(_daily.get("shares", pd.Series([0])).sum())
        _eng_rate = ((_total_l + _total_c) / _total_v * 100) if _total_v > 0 else 0
        st.metric("Engagement Rate", f"{_eng_rate:.2f}%")

        # Daily views chart
        st.markdown("##### 📊 Daily Views")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=_daily["day"], y=_daily["views"], name="Views", marker_color=T["accent"]))
        fig.update_layout(height=350, **CT(), margin=dict(l=40, r=20, t=20, b=20))
        _pc(fig)

        # Watch time chart
        if "estimatedMinutesWatched" in _daily.columns:
            st.markdown("##### ⏱️ Watch Time (minutes)")
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(
                x=_daily["day"], y=_daily["estimatedMinutesWatched"],
                fill="tozeroy", line=dict(color=T["green"], width=2),
                fillcolor="rgba(0,230,118,0.08)",
            ))
            fig2.update_layout(height=300, **CT(), margin=dict(l=40, r=20, t=20, b=20))
            _pc(fig2)

        # Subscriber activity
        if "subscribersGained" in _daily.columns:
            st.markdown("##### 👥 Subscriber Activity")
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(x=_daily["day"], y=_daily["subscribersGained"], name="Gained", marker_color=T["green"]))
            fig3.add_trace(go.Bar(x=_daily["day"], y=_daily["subscribersLost"], name="Lost", marker_color=T["red"]))
            fig3.update_layout(height=300, **CT(), barmode="group", margin=dict(l=40, r=20, t=20, b=20))
            _pc(fig3)

        # CTR chart
        if "ctr" in _daily.columns and _daily["ctr"].sum() > 0:
            st.markdown("##### 📌 Click-Through Rate (CTR)")
            fig4 = go.Figure()
            fig4.add_trace(go.Scatter(x=_daily["day"], y=_daily["ctr"], line=dict(color=T["accent2"], width=2)))
            fig4.update_layout(height=250, **CT(), margin=dict(l=40, r=20, t=20, b=20), yaxis=dict(ticksuffix="%"))
            _pc(fig4)

        # Top videos by views and watch time
        _t1, _t2 = st.columns(2)
        with _t1:
            st.markdown("##### 🏆 Top by Views")
            _top_v = get_top_videos("views", _yt_start, _yt_end, 5)
            if not _top_v.empty:
                _df(_top_v)
            else:
                st.info("No data")

        with _t2:
            st.markdown("##### ⏱️ Top by Watch Time")
            _top_w = get_top_videos("estimatedMinutesWatched", _yt_start, _yt_end, 5)
            if not _top_w.empty:
                _df(_top_w)
            else:
                st.info("No data")

        # Raw data
        with st.expander("📋 Raw Daily Data"):
            _df(_daily)

    # ════════════════════════════════════════════════════════════
    # TAB 3: AUDIENCE (Demographics)
    # ════════════════════════════════════════════════════════════
    with _yt_tabs[3]:
        st.markdown("#### 👥 Audience Demographics")
        if not _has_oauth:
            st.warning("⚠️ Demographics require YouTube Analytics API (OAuth). Current data is from public Data API only.")
            # Show what we can from public data
            if not _vids:
                st.info("No video data available.")
            else:
                st.markdown("##### 📊 Video Engagement Distribution")
                _eng_data = pd.DataFrame([
                    {"Title": v["title"][:50], "Views": v["views"], "Likes": v["likes"],
                     "Comments": v["comments"], "Engagement %": f"{v['engagement_rate']:.1f}%"}
                    for v in sorted(_vids, key=lambda x: x.get("views", 0), reverse=True)[:20]
                ])
                _df(_eng_data, height=400)
        else:
            _demo = get_demographics(_yt_start, _yt_end)
        _dt1, _dt2 = st.columns(2)

        with _dt1:
            st.markdown("##### 🌍 Top Countries")
            _geo = _demo.get("geography", pd.DataFrame())
            if not _geo.empty:
                fig = px.bar(_geo, x="views", y="country", orientation="h", color_discrete_sequence=[T["accent"]])
                fig.update_layout(height=400, **CT(), margin=dict(l=0, r=0, t=20, b=0))
                _pc(fig)
            else:
                st.info("No country data.")

        with _dt2:
            st.markdown("##### 📱 Devices")
            _dev = _demo.get("devices", pd.DataFrame())
            if not _dev.empty:
                fig = px.pie(_dev, values="views", names="deviceType", color_discrete_sequence=px.colors.qualitative.Pastel)
                fig.update_layout(height=400, **CT())
                _pc(fig)
            else:
                st.info("No device data.")

        _dt3, _dt4 = st.columns(2)
        with _dt3:
            st.markdown("##### 🖥️ Operating Systems")
            _os = _demo.get("os", pd.DataFrame())
            if not _os.empty:
                fig = px.bar(_os, x="views", y="operatingSystem", orientation="h", color_discrete_sequence=[T["accent2"]])
                fig.update_layout(height=350, **CT(), margin=dict(l=0, r=0, t=20, b=0))
                _pc(fig)
            else:
                st.info("No OS data.")

        with _dt4:
            st.markdown("##### 👤 Age & Gender")
            _ag = _demo.get("age_gender", pd.DataFrame())
            if not _ag.empty:
                _df(_ag)
            else:
                st.info("No age/gender data.")

        _sub_status = _demo.get("subscribed_status", pd.DataFrame())
        if not _sub_status.empty:
            st.markdown("##### 🔔 Subscriber vs Non-Subscriber Views")
            fig = px.pie(_sub_status, values="views", names="subscribedStatus", color_discrete_sequence=[T["green"], T["accent"]])
            fig.update_layout(height=300, **CT())
            _pc(fig)

    # ════════════════════════════════════════════════════════════
    # TAB 4: REVENUE
    # ════════════════════════════════════════════════════════════
    with _yt_tabs[4]:
        st.markdown("#### 💰 Revenue")
        if not _has_oauth:
            st.warning("⚠️ Revenue data requires YouTube Analytics API (OAuth). Connect OAuth to see revenue, CPM, and ad metrics.")
            st.info("💡 YouTube revenue is only available through the YouTube Analytics API with an authorized OAuth token. See **Settings → YouTube OAuth Setup** for instructions.")

        _rev = get_revenue(_yt_start, _yt_end)
        if _rev:
            _r1, _r2, _r3, _r4 = st.columns(4)
            with _r1:
                st.metric("Est. Revenue", f"${float(_rev.get('estimatedRevenue', 0)):,.2f}")
            with _r2:
                st.metric("CPM", f"${float(_rev.get('cpm', 0)):,.2f}")
            with _r3:
                st.metric("Ad Impressions", f"{int(float(_rev.get('adImpressions', 0))):,}")
            with _r4:
                st.metric("Monetized Playbacks", f"{int(float(_rev.get('monetizedPlaybacks', 0))):,}")
        else:
            st.info("No revenue data for this period. (Monetization may not be enabled)")

        _rev_daily = get_revenue_daily(_yt_start, _yt_end)
        if not _rev_daily.empty:
            st.markdown("##### 📊 Daily Revenue")
            fig = go.Figure()
            if "estimatedRevenue" in _rev_daily.columns:
                fig.add_trace(go.Scatter(
                    x=_rev_daily["day"], y=_rev_daily["estimatedRevenue"],
                    fill="tozeroy", line=dict(color=T["green"], width=2),
                    fillcolor="rgba(0,230,118,0.08)",
                ))
            fig.update_layout(height=350, **CT(), margin=dict(l=50, r=20, t=20, b=20))
            _pc(fig)
            with st.expander("📋 Revenue Data"):
                _df(_rev_daily)

    # ════════════════════════════════════════════════════════════
    # TAB 5: TRAFFIC SOURCES
    # ════════════════════════════════════════════════════════════
    with _yt_tabs[5]:
        st.markdown("#### 🔍 Traffic & Discovery")
        if not _has_oauth:
            st.warning("⚠️ Traffic sources require YouTube Analytics API (OAuth). Showing search terms from public data instead.")
            # Show what we can from public video data
            if _vids:
                st.markdown("##### 🔥 Top Videos by Views")
                _top_vids = sorted(_vids, key=lambda x: x.get("views", 0), reverse=True)[:10]
                for i, v in enumerate(_top_vids, 1):
                    st.markdown(f"**{i}.** {v['title'][:60]} — {v['views']:,} views, {v['likes']:,} likes")
            else:
                st.info("No video data available.")

        _tc1, _tc2 = st.columns(2)
        with _tc1:
            st.markdown("##### 📊 Traffic Sources")
            _traffic = get_traffic_sources(_yt_start, _yt_end)
            if not _traffic.empty:
                fig = px.pie(_traffic, values="views", names="insightTrafficSourceType", color_discrete_sequence=px.colors.qualitative.Set3)
                fig.update_layout(height=400, **CT())
                _pc(fig)
                _df(_traffic)
            else:
                st.info("No traffic source data.")

        with _tc2:
            st.markdown("##### 📍 Where Videos Are Watched")
            _playback = get_views_by_playback(_yt_start, _yt_end)
            if not _playback.empty:
                fig = px.pie(_playback, values="views", names="insightPlaybackLocationType", color_discrete_sequence=px.colors.qualitative.Pastel)
                fig.update_layout(height=400, **CT())
                _pc(fig)
            else:
                st.info("No playback location data.")

        _sc1, _sc2 = st.columns(2)
        with _sc1:
            st.markdown("##### 🔎 Search Terms")
            _search = get_search_terms(_yt_start, _yt_end)
            if not _search.empty:
                fig = px.treemap(_search, path=["insightTrafficSourceDetail"], values="views", color="views", color_continuous_scale="Blues")
                fig.update_layout(height=450, **CT())
                _pc(fig)
                with st.expander("📋 Search Term Data"):
                    _df(_search.head(30))
            else:
                st.info("No search term data.")

        with _sc2:
            st.markdown("##### 🔗 Shared To")
            _sharing = get_sharing_services(_yt_start, _yt_end)
            if not _sharing.empty:
                fig = px.bar(_sharing, x="shares", y="sharingService", orientation="h", color_discrete_sequence=[T["accent2"]])
                fig.update_layout(height=400, **CT(), margin=dict(l=0, r=0, t=20, b=0))
                _pc(fig)
            else:
                st.info("No sharing data.")

    # ════════════════════════════════════════════════════════════
    # TAB 6: PLAYLISTS
    # ════════════════════════════════════════════════════════════
    with _yt_tabs[6]:
        st.markdown("#### 🎵 Playlist Performance")
        if not _has_oauth:
            st.warning("⚠️ Requires YouTube Analytics API (OAuth).")
            st.stop()

        _pl = get_playlist_analytics(_yt_start, _yt_end)
        if not _pl.empty:
            # Format for display
            _pl_display = _pl.copy()
            for col in _pl_display.columns:
                if col not in ("playlist", "playlist_title"):
                    _pl_display[col] = pd.to_numeric(_pl_display[col], errors="coerce").fillna(0)
            if "estimatedMinutesWatched" in _pl_display.columns:
                _pl_display["Watch Hours"] = (_pl_display["estimatedMinutesWatched"] / 60).round(1)
            if "averageViewDuration" in _pl_display.columns:
                _pl_display["Avg Time"] = _pl_display["averageViewDuration"].apply(lambda x: _format_duration(int(x)))

            _df(_pl_display)

            if "views" in _pl.columns:
                fig = px.bar(
                    _pl_display, x="views",
                    y=_pl_display.get("playlist_title", _pl_display["playlist"]),
                    orientation="h", color_discrete_sequence=[T["accent"]],
                )
                fig.update_layout(height=max(300, len(_pl_display) * 40), **CT(), margin=dict(l=0, r=0, t=20, b=0))
                _pc(fig)
        else:
            st.info("No playlist data for this period.")

    # ════════════════════════════════════════════════════════════
    # TAB 7: ASK AI (YouTube-specific AI chat)
    # ════════════════════════════════════════════════════════════
    with _yt_tabs[7]:
        st.markdown("#### 💡 Ask AI About Your Channel")
        _ai_mod = MOD.get("ai_engine")
        if not _ai_mod:
            st.warning("AI Engine not loaded.")
            st.stop()

        # Pre-built questions
        _yt_questions = [
            "How is my channel doing overall?",
            "What's my best performing video and why?",
            "Which videos have the worst retention?",
            "What content should I create next based on my data?",
            "Show me engagement breakdown",
            "What's my average watch time?",
            "Compare my top 5 vs bottom 5 videos",
            "What patterns do you see in my viral videos?",
            "Why are some videos getting 0 views?",
            "Give me a 30-day action plan",
            "What times should I upload?",
            "Which videos should I make Part 2 of?",
        ]

        _sel_q = st.selectbox("Quick questions", ["Custom question..."] + _yt_questions, key="yt_ai_q")

        if _sel_q != "Custom question...":
            _yt_prompt = _sel_q
        else:
            _yt_prompt = st.text_input("Ask anything about your YouTube channel:", key="yt_custom_q")

        if st.button("🤖 Ask AI", type="primary", use_container_width=True) and _yt_prompt:
            # Build context from video data
            _vids = st.session_state.get("yt_videos", [])
            _context_parts = [
                f"Channel: {_ch_info.get('title', 'Unknown')}",
                f"Subscribers: {_ch_info.get('subscribers', 0):,}",
                f"Total Views: {_ch_info.get('total_views', 0):,}",
                f"Video Count: {len(_vids)}",
            ]
            if _vids:
                _top5 = sorted(_vids, key=lambda x: x["views"], reverse=True)[:5]
                _bot5 = sorted(_vids, key=lambda x: x["views"])[:5]
                _context_parts.append("\nTop 5 Videos:")
                for v in _top5:
                    _context_parts.append(f"  - {v['title'][:60]}: {v['views']:,} views, {v['likes']} likes, {v['engagement_rate']:.1f}% eng, {v['duration_label']}")
                _context_parts.append("\nBottom 5 Videos:")
                for v in _bot5:
                    _context_parts.append(f"  - {v['title'][:60]}: {v['views']:,} views, {v['likes']} likes, {v['engagement_rate']:.1f}% eng")
            _context = "\n".join(_context_parts)

            _full_prompt = f"""You are a YouTube analytics expert analyzing the Eagle 3D Streaming YouTube channel data.

Channel Data:
{_context}

User Question: {_yt_prompt}

Provide specific, actionable advice based on the data. Include numbers and specific video titles when relevant."""

            with st.spinner("Thinking..."):
                _resp = _ai_mod.ask(_full_prompt)
                st.markdown(_resp)



# ═══════════════════════════════════════════════════════════════
# PAGE: 💼 LINKEDIN ANALYTICS
# ═══════════════════════════════════════════════════════════════
elif page == "💼 LinkedIn":
    st.markdown(
        '<div class="sec-head">💼 LinkedIn Analytics Center</div>',
        unsafe_allow_html=True,
    )
    try:
        from linkedin_connector import (
            scrape_public_metrics, get_cached_metrics, get_manual_history,
            save_manual_entry, import_csv_data, get_status,
            scrape_with_playwright,
        )
    except Exception as e:
        st.error(f"LinkedIn connector not loaded: {e}")
        st.stop()

    _li_status = get_status()

    # Status indicators
    _ls1, _ls2, _ls3 = st.columns(3)
    with _ls1:
        st.metric("Company Page", "✅" if _li_status["company_page"] else "❌ Not Set")
    with _ls2:
        st.metric("Auth Cookies", "✅ Deep Scrape" if _li_status["cookies"] else "⚠️ Public Only")
    with _ls3:
        st.metric("Cached Data", "✅" if _li_status["cached_data"] else "—")

    if not _li_status["configured"]:
        st.warning("⚠️ LinkedIn not configured. Add `LINKEDIN_COMPANY_PAGE` to secrets.")
        with st.expander("📖 Setup Guide"):
            st.markdown("""
            **LinkedIn Integration — 3 Methods:**

            **Method 1: Public Page Scrape (Free, No Auth)**
            - Just provide your company page URL
            - Gets: follower count, company name, industry
            - Add to secrets: `LINKEDIN_COMPANY_PAGE = "https://www.linkedin.com/company/eagle3d/"`

            **Method 2: Authenticated Scrape (Recommended)**
            - Export cookies from your logged-in LinkedIn session
            - Gets: analytics, post engagement, follower demographics
            - Add to secrets: `LINKEDIN_COOKIES_JSON = '[{"name":"li_at","value":"...","domain":".linkedin.com"}]'`
            - **How to get cookies:** Use browser extension "EditThisCookie" or "Cookie-Editor"

            **Method 3: Manual Entry**
            - Enter metrics directly in the dashboard
            - Good for filling gaps or historical data
            """)
        st.stop()

    st.markdown("---")

    _li_tabs = st.tabs([
        "📊 Overview", "📝 Manual Entry", "📥 Import CSV", "🔧 Scrape Settings",
    ])

    with _li_tabs[0]:  # Overview
        st.markdown("#### 📊 LinkedIn Metrics")

        # Try cached data first
        _li_cached = get_cached_metrics()
        if _li_cached and not _li_cached.get("error"):
            _lc1, _lc2, _lc3, _lc4 = st.columns(4)
            with _lc1:
                st.metric("👥 Followers", f"{_li_cached.get('followers', 0):,}")
            with _lc2:
                st.metric("🏢 Company", _li_cached.get('company_name', 'N/A'))
            with _lc3:
                st.metric("🏭 Industry", _li_cached.get('industry', 'N/A'))
            with _lc4:
                st.metric("👔 Employees", _li_cached.get('employees', 'N/A'))
            if _li_cached.get('scraped_at'):
                st.caption(f"🕐 Last scraped: {_li_cached.get('scraped_at', '')[:19]}")
            if _li_cached.get('description'):
                with st.expander("📝 Company Description"):
                    st.write(_li_cached['description'][:500])

            # Historical data charts
            _li_hist = get_manual_history()
            if not _li_hist.empty and "date" in _li_hist.columns:
                st.markdown("---")
                st.markdown("#### 📈 Historical Trends")

                _li_metric = st.selectbox(
                    "Select metric", 
                    ["followers", "impressions", "engagement_rate", "unique_visitors", "posts", "likes", "comments"],
                    key="li_metric_sel"
                )
                if _li_metric in _li_hist.columns:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=_li_hist["date"], y=_li_hist[_li_metric],
                        mode="lines+markers", name=_li_metric,
                        line=dict(color=T["accent"], width=2),
                        fill="tozeroy", fillcolor=f"rgba(100,180,255,0.1)",
                    ))
                    fig.update_layout(
                        title=f"LinkedIn {_li_metric.title()} Over Time",
                        height=350, **CT(),
                        margin=dict(l=0, r=0, t=40, b=0),
                    )
                    _pc(fig)

                # Growth rate
                if len(_li_hist) >= 2 and "followers" in _li_hist.columns:
                    _latest_f = _li_hist["followers"].iloc[-1]
                    _prev_f = _li_hist["followers"].iloc[-2]
                    if _prev_f > 0:
                        _growth_rate = ((_latest_f - _prev_f) / _prev_f * 100)
                        st.metric("Follower Growth", f"{_growth_rate:+.1f}%",
                                  f"{_latest_f - _prev_f:+d} followers")

                # Engagement chart
                if "likes" in _li_hist.columns and "comments" in _li_hist.columns:
                    _ec1, _ec2 = st.columns(2)
                    with _ec1:
                        fig2 = go.Figure()
                        fig2.add_trace(go.Bar(x=_li_hist["date"], y=_li_hist["likes"], name="Likes", marker_color=T["accent"]))
                        fig2.add_trace(go.Bar(x=_li_hist["date"], y=_li_hist["comments"], name="Comments", marker_color=T["accent2"]))
                        fig2.update_layout(title="Engagement", height=300, **CT(), barmode="stack", margin=dict(l=0, r=0, t=40, b=0))
                        _pc(fig2)
                    with _ec2:
                        if "impressions" in _li_hist.columns:
                            fig3 = go.Figure()
                            fig3.add_trace(go.Scatter(x=_li_hist["date"], y=_li_hist["impressions"], mode="lines+markers", name="Impressions", line=dict(color=T["green"])))
                            fig3.update_layout(title="Impressions", height=300, **CT(), margin=dict(l=0, r=0, t=40, b=0))
                            _pc(fig3)

                # Data table
                with st.expander("📋 Raw Data"):
                    _df(_li_hist.tail(30))
            else:
                st.info("📊 No historical data yet. Add data via Manual Entry or Scrape to see trends.")
        else:
            # Auto-scrape public page if no cached data
            if st.button("🌐 Scrape LinkedIn Now", type="primary", use_container_width=True):
                with st.spinner("Scraping LinkedIn public page..."):
                    _result = scrape_public_metrics()
                    if _result.get("error"):
                        st.error(f"Scrape failed: {_result['error']}")
                    else:
                        st.success("✅ Metrics scraped!")
                        st.rerun()

        # Scrape buttons
        _sbc1, _sbc2 = st.columns(2)
        with _sbc1:
            if st.button("🌐 Scrape Public Page", use_container_width=True):
                with st.spinner("Scraping LinkedIn public page..."):
                    _result = scrape_public_metrics()
                    if _result.get("error"):
                        st.error(f"Scrape failed: {_result['error']}")
                    else:
                        st.success("✅ Public metrics scraped!")
                        st.rerun()

        with _sbc2:
            if _li_status["cookies"]:
                if st.button("🔐 Deep Scrape (Authenticated)", use_container_width=True):
                    with st.spinner("Running authenticated scrape..."):
                        _result = scrape_with_playwright()
                        if _result.get("error"):
                            st.error(f"Scrape failed: {_result['error']}")
                        else:
                            st.success("✅ Deep metrics scraped!")
                            st.rerun()
            else:
                st.info("💡 Add `LINKEDIN_COOKIES_JSON` to secrets for deep scraping.")

        # Manual history chart
        _li_hist = get_manual_history()
        if not _li_hist.empty:
            st.markdown("#### 📈 Historical Data")
            _df(_li_hist.tail(30))

    with _li_tabs[1]:  # Manual Entry
        st.markdown("#### 📝 Enter LinkedIn Metrics Manually")
        st.caption("Enter current metric values. Each entry is timestamped and saved to history.")

        _me1, _me2, _me3, _me4 = st.columns(4)
        with _me1:
            _m_followers = st.number_input("Followers", min_value=0, value=0, step=1)
        with _me2:
            _m_impressions = st.number_input("Impressions (period)", min_value=0, value=0, step=1)
        with _me3:
            _m_engagement = st.number_input("Engagement Rate (%)", min_value=0.0, value=0.0, step=0.1)
        with _me4:
            _m_posts = st.number_input("Posts (period)", min_value=0, value=0, step=1)

        _me5, _me6, _me7 = st.columns(3)
        with _me5:
            _m_visitors = st.number_input("Unique Visitors", min_value=0, value=0, step=1)
        with _me6:
            _m_likes = st.number_input("Total Likes", min_value=0, value=0, step=1)
        with _me7:
            _m_comments = st.number_input("Total Comments", min_value=0, value=0, step=1)

        if st.button("💾 Save Manual Entry", use_container_width=True):
            _entry = {
                "followers": _m_followers,
                "impressions": _m_impressions,
                "engagement_rate": _m_engagement,
                "posts": _m_posts,
                "unique_visitors": _m_visitors,
                "likes": _m_likes,
                "comments": _m_comments,
            }
            if save_manual_entry(_entry):
                st.success("✅ Entry saved!")
                st.rerun()
            else:
                st.error("Failed to save entry.")

    with _li_tabs[2]:  # Import CSV
        st.markdown("#### 📥 Import LinkedIn Data from CSV")
        st.caption("Paste CSV content exported from LinkedIn Analytics or enter manually.")

        _csv_text = st.text_area(
            "CSV Content",
            value="date,followers,impressions,engagement_rate,unique_visitors,posts,likes,comments\n",
            height=200,
        )

        if st.button("📥 Import CSV", use_container_width=True):
            if import_csv_data(_csv_text):
                st.success("✅ CSV data imported!")
            else:
                st.error("Failed to import CSV. Check format.")

        with st.expander("📋 Expected CSV Format"):
            st.code("""date,followers,impressions,engagement_rate,unique_visitors,posts,likes,comments
2026-06-01,450,12000,3.5,800,5,350,42
2026-06-02,452,11500,3.2,750,3,280,35""")

    with _li_tabs[3]:  # Scrape Settings
        st.markdown("#### 🔧 LinkedIn Scraping Configuration")
        st.caption("Configure automated daily scraping via GitHub Actions pipeline.")

        _co_url = st.text_input(
            "Company Page URL",
            value=os.environ.get("LINKEDIN_COMPANY_PAGE", ""),
            help="e.g., https://www.linkedin.com/company/eagle3d/",
        )

        with st.expander("🍪 Cookie Instructions"):
            st.markdown("""
            **How to export LinkedIn cookies:**
            1. Install [Cookie-Editor](https://cookie-editor.cgagnier.ca/) browser extension
            2. Log in to LinkedIn in your browser
            3. Click the Cookie-Editor extension icon
            4. Click "Export" → "Export as JSON"
            5. Copy the JSON and paste it in your secrets

            **Required cookies:** `li_at`, `JSESSIONID`

            **Add to GitHub Secrets:**
            - `LINKEDIN_COOKIES_JSON` — the full JSON array
            - `LINKEDIN_COMPANY_PAGE` — your company page URL

            **Add to Streamlit Cloud Secrets:**
            ```toml
            LINKEDIN_COMPANY_PAGE = "https://www.linkedin.com/company/eagle3d/"
            LINKEDIN_COOKIES_JSON = '[{"name":"li_at","value":"...","domain":".linkedin.com"}]'
            ```

            ⚠️ **Important:** LinkedIn cookies expire every ~90 days. You'll need to re-export periodically.
            """)


# ═══════════════════════════════════════════════════════════════
# PAGE: 🔗 CROSS-PLATFORM CORRELATION
# ═══════════════════════════════════════════════════════════════
elif page == "🔗 Cross-Platform":
    st.markdown(
        '<div class="sec-head">🔗 Cross-Platform Intelligence Hub</div>',
        unsafe_allow_html=True,
    )
    st.caption("Correlate data across KPI, GA4, YouTube, LinkedIn & Stripe — measure what matters.")

    try:
        from cross_platform_engine import (
            build_unified_timeline, compute_correlations,
            compute_attribution, compute_cross_platform_funnel,
            compute_platform_comparison, compute_growth_analysis,
            generate_cross_insights,
        )
        from youtube_connector import get_channel_info, get_daily_analytics, is_configured as yt_ok, get_status as yt_st
        from linkedin_connector import get_cached_metrics, get_manual_history, is_configured as li_ok, get_status as li_st
    except Exception as e:
        st.error(f"Cross-platform engine not loaded: {e}")
        st.stop()

    # ── Platform Status ──
    _yt_s = yt_st()
    _li_s = li_st()
    _ga4_ok = bool(os.environ.get("GA4_PROPERTY_ID") or True)  # GA4 is already configured

    _ps1, _ps2, _ps3, _ps4 = st.columns(4)
    with _ps1:
        _kpi_status = "✅" if not kpi_all.empty else "⚠️"
        st.metric("KPI Data", f"{_kpi_status} ({len(kpi_all)} days)")
    with _ps2:
        st.metric("GA4 Traffic", "✅" if _ga4_ok else "❌")
    with _ps3:
        st.metric("YouTube", "✅" if _yt_s["configured"] else "❌")
    with _ps4:
        st.metric("LinkedIn", "✅" if _li_s["configured"] else "❌")

    st.markdown("---")

    # Period selection
    _cp_start = p_start.strftime("%Y-%m-%d")
    _cp_end = p_end.strftime("%Y-%m-%d")

    # ── Build unified timeline ──
    _yt_daily = pd.DataFrame()
    _li_daily = pd.DataFrame()
    _ga4_daily = pd.DataFrame()

    # Get GA4 data
    try:
        _ga4_mod = MOD.get("ga4_connector")
        if _ga4_mod and hasattr(_ga4_mod, "fetch_utm_traffic"):
            _ga4_daily = _ga4_mod.fetch_utm_traffic(_cp_start, _cp_end)
    except Exception:
        pass

    # Get YouTube data
    if _yt_s["configured"]:
        try:
            _yt_daily = get_daily_analytics(_cp_start, _cp_end)
        except Exception:
            _yt_daily = pd.DataFrame()
        # Fallback 1: build YouTube daily from public video data if no OAuth analytics
        if _yt_daily.empty:
            try:
                _yt_vids = get_channel_videos(max_videos=200)
                if _yt_vids:
                    from collections import defaultdict as _dd3
                    _yt_dd = _dd3(lambda: {"youtube_views": 0, "youtube_likes": 0, "youtube_comments": 0})
                    for _v in _yt_vids:
                        _vd = _v.get("published_at", "")
                        if _vd:
                            _vdate = _vd[:10]
                            if _cp_start <= _vdate <= _cp_end:
                                _yt_dd[_vdate]["youtube_views"] += _v.get("views", 0)
                                _yt_dd[_vdate]["youtube_likes"] += _v.get("likes", 0)
                                _yt_dd[_vdate]["youtube_comments"] += _v.get("comments", 0)
                    if _yt_dd:
                        _yt_daily = pd.DataFrame([
                            {"date": d, **_yt_dd[d]} for d in sorted(_yt_dd.keys())
                        ])
            except Exception:
                pass
        # Fallback 2: use saved youtube_daily.json from pipeline
        if _yt_daily.empty:
            try:
                _yt_daily_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_output", "youtube_daily.json")
                if os.path.exists(_yt_daily_path):
                    with open(_yt_daily_path, "r") as _yf:
                        _yt_daily_data = json.load(_yf)
                    if _yt_daily_data:
                        _yt_rows = []
                        for _d, _vals in _yt_daily_data.items():
                            if _cp_start <= _d <= _cp_end:
                                _yt_rows.append({"date": _d, **_vals})
                        if _yt_rows:
                            _yt_daily = pd.DataFrame(_yt_rows)
            except Exception:
                pass

    # Get LinkedIn data
    _li_hist = get_manual_history()
    if not _li_hist.empty:
        _li_daily = _li_hist
    else:
        # Fallback: use cached metrics as single-day entry
        _li_cached = get_cached_metrics()
        if _li_cached and not _li_cached.get("error"):
            _li_daily = pd.DataFrame([{
                "date": datetime.now().strftime("%Y-%m-%d"),
                "followers": _li_cached.get("followers", 0),
                "company_name": _li_cached.get("company_name", ""),
            }])
        else:
            _li_daily = pd.DataFrame()

    # Build unified
    _unified = build_unified_timeline(
        kpi_df=kpi_all,
        ga4_df=_ga4_daily,
        youtube_daily=_yt_daily,
        linkedin_daily=_li_daily,
        start_date=_cp_start,
        end_date=_cp_end,
    )

    _cp_tabs = st.tabs([
        "🔄 Unified Timeline", "🔗 Correlations", "🎯 Attribution",
        "📊 Funnel", "📈 Growth", "💡 Insights",
    ])

    with _cp_tabs[0]:  # Unified Timeline
        st.markdown("#### 🔄 Unified Cross-Platform Timeline")
        if not _unified.empty:
            _avail_cols = []
            for c in _unified.columns:
                if c != "date":
                    try:
                        if pd.api.types.is_numeric_dtype(_unified[c]) and _unified[c].sum() > 0:
                            _avail_cols.append(c)
                    except Exception:
                        pass
            if _avail_cols:
                # Let user pick metrics to chart
                _sel_metrics = st.multiselect(
                    "Select metrics to chart",
                    _avail_cols,
                    default=_avail_cols[:5] if len(_avail_cols) >= 5 else _avail_cols,
                )
                if _sel_metrics:
                    fig = go.Figure()
                    colors = [T["accent"], T["accent2"], T["green"], T["yellow"], T["red"], "#FF6B6B", "#4ECDC4"]
                    for i, m in enumerate(_sel_metrics):
                        fig.add_trace(go.Scatter(
                            x=_unified["date"], y=_unified[m],
                            name=m.replace("_", " ").title(),
                            line=dict(color=colors[i % len(colors)], width=2),
                        ))
                    fig.update_layout(
                        height=500, **CT(),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        margin=dict(l=50, r=20, t=50, b=30),
                    )
                    _pc(fig)

                _df(_unified.tail(30))
            else:
                st.info("No data with non-zero values found in the selected period.")
        else:
            st.warning("⚠️ No data available from any platform for the selected period.")

    with _cp_tabs[1]:  # Correlations
        st.markdown("#### 🔗 Cross-Platform Correlations")
        if not _unified.empty and len(_unified) >= 3:
            _corr = compute_correlations(_unified)
            if _corr.get("error"):
                st.info(_corr["error"])
            else:
                _strong = _corr.get("strong_correlations", [])
                if _strong:
                    st.markdown("##### Strongest Correlations")
                    for c in _strong[:10]:
                        _dir = "📈" if c["direction"] == "positive" else "📉"
                        _str = "🔥" if c["strength"] == "strong" else "🔸"
                        st.markdown(
                            f"{_str} {_dir} <code>{c['metric_a']}</code> ↔ <code>{c['metric_b']}</code> "
                            f"— r = <b>{c['correlation']}</b> ({c['strength']})",
                            unsafe_allow_html=True,
                        )

                    # Correlation heatmap
                    _mat = _corr.get("matrix", {})
                    if _mat:
                        _mat_df = pd.DataFrame(_mat)
                        fig = go.Figure(data=go.Heatmap(
                            z=_mat_df.values,
                            x=_mat_df.columns,
                            y=_mat_df.index,
                            colorscale="RdBu",
                            zmin=-1, zmax=1,
                        ))
                        fig.update_layout(
                            height=max(400, len(_mat_df) * 30),
                            **CT(),
                            margin=dict(l=150, r=20, t=30, b=100),
                            xaxis=dict(tickangle=45),
                        )
                        _pc(fig)
                else:
                    st.info("No significant correlations found (r > 0.3). Need more data or different metrics.")

                st.caption(f"Analyzed {_corr.get('metric_count', 0)} metrics over {_corr.get('day_count', 0)} days")
        else:
            st.info("Need at least 3 days of data from multiple platforms for correlation analysis.")

    with _cp_tabs[2]:  # Attribution
        st.markdown("#### 🎯 Channel Attribution")
        st.caption("Which platform metrics best predict your KPI conversions?")
        if not _unified.empty and len(_unified) >= 7:
            _attr = compute_attribution(_unified)
            if isinstance(_attr, dict) and not _attr.get("error"):
                for _kpi_name, _predictors in _attr.items():
                    if _predictors:
                        st.markdown(f"##### {_kpi_name.replace('_', ' ').title()}")
                        for p in _predictors[:5]:
                            _lag = f" (leads by {p['lag_days']}d)" if p["lag_days"] > 0 else " (same-day)"
                            st.markdown(
                                f"- 🎯 <code>{p['metric']}</code> ({p['platform']}) — "
                                f"r={p['correlation']}{_lag}",
                                unsafe_allow_html=True,
                            )
            else:
                st.info(_attr.get("error", "Not enough data for attribution analysis."))
        else:
            st.info("Need at least 7 days of data for attribution analysis.")

    with _cp_tabs[3]:  # Funnel
        st.markdown("#### 📊 Cross-Platform Funnel")
        _yt_info = get_channel_info() if _yt_s["configured"] else {}
        _li_metrics = get_cached_metrics()

        _yt_views = int(_unified.filter(like="yt_views").sum().sum()) if not _unified.empty else 0
        _li_impressions = int(_unified.filter(like="li_impressions").sum().sum()) if not _unified.empty else 0
        _ga4_sessions = int(_unified.filter(like="ga4_sessions").sum().sum()) if not _unified.empty else 0

        _funnel = compute_cross_platform_funnel(
            kpi_df=kpi_all if not kpi_all.empty else pd.DataFrame({"signups": [0], "first_uploads": [0], "paid_customers": [0]}),
            ga4_sessions=_ga4_sessions,
            yt_views=_yt_views,
            li_impressions=_li_impressions,
            period_label=f"{_cp_start} to {_cp_end}",
        )

        # Draw funnel
        _stages = _funnel.get("stages", [])
        if _stages:
            fig = go.Figure(go.Funnel(
                y=[s["stage"] for s in _stages],
                x=[max(s["value"], 1) for s in _stages],
                textinfo="value+percent initial+percent previous",
                marker={"color": [T["accent"], T["accent2"], T["green"], T["yellow"], T["red"]]},
            ))
            fig.update_layout(height=450, **CT())
            _pc(fig)

            # Conversion rates
            _conv = _funnel.get("conversion_rates", {})
            if _conv:
                st.markdown("##### Conversion Rates")
                for k, v in _conv.items():
                    if v > 0:
                        st.markdown(f"- <code>{k.replace('_', ' ').title()}</code>: <b>{v}%</b>", unsafe_allow_html=True)

    with _cp_tabs[4]:  # Growth
        st.markdown("#### 📈 Cross-Platform Growth")
        if not _unified.empty and len(_unified) >= 6:
            _growth = compute_growth_analysis(_unified)
            if _growth.get("error"):
                st.info(_growth["error"])
            else:
                _g_metrics = _growth.get("metrics", {})
                _g_rows = []
                for m, d in _g_metrics.items():
                    _g_rows.append({
                        "Metric": m.replace("_", " ").title(),
                        "Current": d["current_period"],
                        "Previous": d["previous_period"],
                        "Change": d["change"],
                        "% Change": f"{d['pct_change']}%",
                        "Trend": {"growing": "📈", "declining": "📉", "stable": "➡️"}[d["trend"]],
                    })
                if _g_rows:
                    _df(pd.DataFrame(_g_rows))
        else:
            st.info("Need more data for growth analysis.")

    with _cp_tabs[5]:  # Insights
        st.markdown("#### 💡 Cross-Platform Insights")
        _corr_data = {}
        _attr_data = {}
        _growth_data = {}
        _funnel_data = {}

        if not _unified.empty and len(_unified) >= 3:
            _corr_data = compute_correlations(_unified)
        if not _unified.empty and len(_unified) >= 7:
            _attr_data = compute_attribution(_unified)
        if not _unified.empty and len(_unified) >= 6:
            _growth_data = compute_growth_analysis(_unified)
        _funnel_data = compute_cross_platform_funnel(
            kpi_df=kpi_all if not kpi_all.empty else pd.DataFrame({"signups": [0]}),
            ga4_sessions=0, yt_views=0, li_impressions=0,
        )

        _insights = generate_cross_insights(_corr_data, _attr_data, _growth_data, _funnel_data)
        for insight in _insights:
            st.markdown(f"<div style='padding:8px 12px;margin:4px 0;background:{T['card']};border-radius:8px;border-left:3px solid {T['accent']};'>{insight}</div>", unsafe_allow_html=True)

        if not _insights:
            st.info("Connect more platforms (YouTube, LinkedIn) for richer cross-platform insights.")


# ═══════════════════════════════════════════════════════════════
# PAGE: ⚙️ SETTINGS (with Run Pipeline + Secrets Editor + Cache Clear)
# ═══════════════════════════════════════════════════════════════
elif page == "⚙️ Settings":
    st.markdown(
        '<div class="sec-head">⚙️ System Settings & Diagnostics</div>',
        unsafe_allow_html=True,
    )

    st.markdown("#### 🏥 Module Status")
    _mods = {
        "KPI Bridge": "kpi_bridge",
        "GA4 Connector": "ga4_connector",
        "Source Intel": "source_intel",
        "Smart Q&A": "smart_qa",
        "Strategic": "strategic",
        "Notifications": "notifications",
        "Intelligence": "intelligence",
        "AI Engine": "ai_engine",
        "Predictions": "prediction_engine",
        "Reports": "report_generator",
        "Source Normalizer": "source_normalizer",
    }
    _mc = st.columns(4)
    _act = 0
    for _i, (_mname, _ky) in enumerate(_mods.items()):
        with _mc[_i % 4]:
            _ok = _ky in MOD
            if _ok:
                _act += 1
            _bd = (
                '<span class="badge badge-ok">✅</span>'
                if _ok
                else '<span class="badge badge-err">❌</span>'
            )
            st.markdown(f"**{_mname}** {_bd}", unsafe_allow_html=True)
    st.metric("Active", f"{_act}/{len(_mods)}")

    st.markdown("#### 🤖 AI Provider")
    _ai_mod = MOD.get("ai_engine")
    if _ai_mod:
        _pr = _ai_mod._get_provider()
        _pi2 = {
            "groq": ("⚡ Groq Cloud", "Ultra-fast"),
            "gemini": ("💎 Gemini", "Reasoning"),
            "rule_based": ("🧠 Rule-Based", "No key"),
        }
        _n2, _d2 = _pi2.get(_pr, ("?", ""))
        st.info(f"{_n2} — {_d2}")
        if _pr == "rule_based":
            _has_grok = False
            _has_gem = False
            try:
                if st.secrets.get("GROQ_API_KEY", ""):
                    _has_grok = True
                if st.secrets.get("GEMINI_API_KEY", ""):
                    _has_gem = True
            except Exception:
                pass
            if not _has_grok and not _has_gem:
                st.warning(
                    "💡 Add API keys in **Streamlit Cloud → Settings → Secrets**:\n\n"
                    "```toml\n"
                    'GROQ_API_KEY = "your-key"\n'
                    'GEMINI_API_KEY = "your-key"\n'
                    "```\n\n"
                    "Get free keys: [Groq](https://console.groq.com) · "
                    "[Gemini](https://aistudio.google.com)"
                )
            else:
                st.error(f"⚠️ Keys found in secrets but detection failed! Groq={_has_grok}, Gemini={_has_gem}")
    else:
        st.error("AI Engine not loaded")

    st.markdown("#### 🔐 Secrets Status")
    for _ky, _ds in [
        ("MASTER_SHEET_URL", "Google Sheets URL"),
        ("GOOGLE_CREDS", "Google Credentials"),
        ("GROQ_API_KEY", "Groq AI"),
        ("GEMINI_API_KEY", "Gemini AI"),
        ("TELEGRAM_BOT_TOKEN", "Telegram Bot"),
        ("TELEGRAM_CHAT_ID", "Telegram Chat ID"),
        ("YOUTUBE_API_KEY", "YouTube Data API"),
        ("YOUTUBE_CHANNEL_ID", "YouTube Channel ID"),
        ("YOUTUBE_OAUTH_TOKEN", "YouTube Analytics (OAuth)"),
        ("LINKEDIN_COMPANY_PAGE", "LinkedIn Company Page"),
        ("LINKEDIN_COOKIES_JSON", "LinkedIn Cookies"),
    ]:
        _vl = get_secret(_ky)
        if _vl:
            st.success(f"✅ {_ds}")
        else:
            st.warning(f"⚠️ {_ds}: Not set")

    st.markdown("---")
    st.markdown("#### 📊 Data Summary")
    _total_s = int(kpi_all["signups"].sum()) if not kpi_all.empty and "signups" in kpi_all.columns else 0
    _total_u = int(kpi_all["first_uploads"].sum()) if not kpi_all.empty and "first_uploads" in kpi_all.columns else 0
    _total_p = int(kpi_all["paid_customers"].sum()) if not kpi_all.empty and "paid_customers" in kpi_all.columns else 0
    _data_src = "Google Sheets (live)" if not counts_raw.empty else "Historical JSON (offline)"
    _di = {
        "Data Source": _data_src,
        "KPI Rows": f"{len(kpi_all):,}",
        "Total Sign-ups": f"{_total_s:,}",
        "Total Uploads": f"{_total_u:,}",
        "Total Paid": f"{_total_p:,}",
        "Period Rows": f"{len(kpi):,}",
        "Sheet Sign-ups": f"{len(free_rows):,}",
        "Sheet Uploads": f"{len(upload_rows):,}",
    }
    _ic = st.columns(3)
    for _idx, (_l, _c) in enumerate(_di.items()):
        with _ic[_idx % 3]:
            st.metric(_l, _c)

    st.markdown("---")
    st.markdown("#### ⚡ Pipeline & Auto-Refresh")
    st.caption("The pipeline fetches fresh data from all sources. Auto-trigger runs daily when data is stale.")

    # Auto-trigger status
    _gh_token = get_secret("GITHUB_TOKEN", "")
    _auto_enabled = st.toggle("🔄 Auto-Trigger Pipeline (when data is stale)", value=True, key="auto_pipeline_toggle")
    if _auto_enabled and not _gh_token:
        st.warning("⚠️ Add `GITHUB_TOKEN` (GitHub PAT) to secrets to enable auto-trigger.")

    # Show data freshness
    _today_str2 = datetime.now().strftime("%Y-%m-%d")
    _has_today_data = False
    if not counts_raw.empty:
        _dc2 = next((c for c in counts_raw.columns if "date" in c.lower()), None)
        if _dc2:
            _has_today_data = _today_str2 in counts_raw[_dc2].astype(str).values
    if _has_today_data:
        st.success(f"✅ Data is fresh — today ({_today_str2}) data exists")
    else:
        st.warning(f"⚠️ Data is stale — no data for today ({_today_str2})")

    # Manual trigger buttons
    _trig_c1, _trig_c2 = st.columns(2)
    with _trig_c1:
        if st.button("🚀 Run Pipeline (Local)", type="secondary", use_container_width=True):
            with st.spinner("Running pipeline..."):
                try:
                    import subprocess
                    _root_dir = os.path.dirname(os.path.abspath(__file__))
                    _result = subprocess.run(
                        ["python3", "daily_pipeline.py"],
                        capture_output=True, text=True, timeout=180,
                        cwd=_root_dir,
                    )
                    if _result.returncode == 0:
                        st.success("✅ Pipeline completed! Refresh page to see updated data.")
                        if _result.stdout:
                            st.code(_result.stdout[-800:])
                        st.cache_data.clear()
                    else:
                        st.warning("Pipeline finished with warnings")
                        if _result.stderr:
                            st.code(_result.stderr[-800:])
                except FileNotFoundError:
                    st.info("Pipeline scripts not available on Streamlit Cloud. Use Remote trigger.")
                except Exception as _e:
                    st.info(f"Run manually: `python3 daily_pipeline.py` ({_e})")

    with _trig_c2:
        if _gh_token:
            if st.button("⚡ Trigger Remote Pipeline", type="primary", use_container_width=True):
                try:
                    import urllib.request
                    import json as _json
                    _repo = "fozayelibnayaz/eagle3d-kpi-automation"
                    _url = f"https://api.github.com/repos/{_repo}/actions/workflows/daily_pipeline.yml/dispatches"
                    _data = _json.dumps({"ref": "main"}).encode()
                    _req = urllib.request.Request(_url, data=_data, method="POST",
                        headers={"Authorization": f"token {_gh_token}", "Accept": "application/vnd.github+json"})
                    with urllib.request.urlopen(_req, timeout=15) as _r:
                        if _r.status in (200, 204):
                            st.success("✅ Pipeline triggered! Data updates in ~5 min. Refresh then.")
                except Exception as _e:
                    st.error(f"Failed: {_e}")
        else:
            st.info("Add `GITHUB_TOKEN` to secrets for remote trigger.")
            st.markdown("[GitHub Actions → Run manually](https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions)")

    st.markdown("---")
    st.markdown("#### 🔧 Add / Update Secrets (Local Only)")
    st.caption("For Streamlit Cloud, add secrets at **share.streamlit.io → Settings → Secrets**.")
    with st.expander("📝 Edit local secrets.toml"):
        _secret_text = st.text_area("secrets.toml content", value="""# API Keys
GROQ_API_KEY = "your-groq-key"
GEMINI_API_KEY = "your-gemini-key"
""", height=180)
        if st.button("💾 Save secrets.toml"):
            try:
                os.makedirs(".streamlit", exist_ok=True)
                with open(".streamlit/secrets.toml", "w") as _f:
                    _f.write(_secret_text)
                st.success("✅ Saved! Refresh page to apply.")
            except OSError:
                st.warning("⚠️ Cannot write here (read-only). Add secrets in share.streamlit.io → Settings → Secrets.")

    st.markdown("---")
    st.markdown("#### 🔄 Clear Cache & Refresh Data")
    if st.button("🔄 Clear All Cache", use_container_width=True):
        st.cache_data.clear()
        st.success("✅ Cache cleared! Refresh page to reload all data.")

# ═══════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════
st.divider()
_fc1, _fc2, _fc3 = st.columns(3)
with _fc1:
    st.caption(
        f"🦅 Eagle Analytics Hub v7.1 | "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
with _fc2:
    st.caption("Pipeline: Daily 12:00 UTC")
with _fc3:
    _ai_mod = MOD.get("ai_engine")
    if _ai_mod:
        _pr = _ai_mod._get_provider()
        _prl = (
            "⚡ Groq" if _pr == "groq"
            else "💎 Gemini" if _pr == "gemini"
            else "🧠 Rules"
        )
        st.caption(f"AI: {_prl}")
