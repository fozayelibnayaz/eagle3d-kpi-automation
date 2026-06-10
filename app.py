"""
Eagle 3D Streaming — KPI Analytics Dashboard v4
=================================================
Complete rewrite — error-free, responsive, dark/light mode.
All 9 pages: Dashboard, Traffic Intel, Ask AI, Predictions,
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
    page_title="Eagle 3D Streaming — KPI",
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
    if not CREDS_PATH or not MASTER_SHEET_URL:
        return pd.DataFrame()
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
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
        'letter-spacing:0.5px;">Eagle 3D Streaming</div>'
        '<div style="font-size:0.58rem;color:var(--muted);'
        'text-transform:uppercase;letter-spacing:2px;">KPI Analytics v4</div>'
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

    # Navigation
    page = st.radio(
        "Navigate",
        [
            "📊 Dashboard", "🚦 Traffic Intel", "🤖 Ask AI",
            "🔮 Predictions", "📋 Reports", "🔔 Alerts",
            "🔬 EDA Lab", "🔍 Browse Data", "⚙️ Settings",
        ],
        label_visibility="collapsed",
    )

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
        _pr = _ai_mod._get_provider()
        _ic = {"groq": "⚡", "gemini": "💎", "rule_based": "🧠"}
        _nm = {"groq": "Groq", "gemini": "Gemini", "rule_based": "Rules"}
        st.caption(f"{_ic.get(_pr, '🤖')} AI: {_nm.get(_pr, _pr)}")
    st.caption(f"🦅 v5.1 | {datetime.now().strftime('%H:%M')}")

# ═══════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════
with st.spinner("Loading data..."):
    counts_raw = load_sheet("Daily_Counts")
    free_raw = load_sheet("Verified_FREE")
    upload_raw = load_sheet("Verified_FIRST_UPLOAD")
    stripe_raw = load_sheet("Verified_STRIPE")

kpi_all = pd.DataFrame()
if not counts_raw.empty:
    df = counts_raw.copy()
    dc = next((c for c in df.columns if "date" in c.lower()), None)
    if dc:
        df = df.rename(columns={dc: "date"})
        for col, pats in [
            ("signups", ["signup", "free", "sign_up"]),
            ("first_uploads", ["first_upload", "upload"]),
            ("paid_customers", ["paid", "stripe", "customer"]),
        ]:
            m = next(
                (c for c in df.columns if any(p in c.lower() for p in pats)),
                None,
            )
            if m:
                df = df.rename(columns={m: col})
        nc = [c for c in ["signups", "first_uploads", "paid_customers"]
              if c in df.columns]
        for c in nc:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        if all(c in df.columns for c in ["date", "signups"]):
            kpi_all = df

# Historical fallback: only if Google Sheets didn't load
if kpi_all.empty:
    _hist_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_output", "historical_accounts.json")
    _paid_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_output", "historical_paid.json")
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
        leads_df = MOD["source_normalizer"].aggregate_normalized_sources(free_rows)
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

    c1, c2 = st.columns([1, 2])
    with c1:
        fig = go.Figure(
            go.Funnel(
                y=["Sign-ups", "Uploads", "Paid"],
                x=[cs, cu, cp],
                textposition="inside",
                textinfo="value+percent initial",
                marker=dict(color=[T["accent"], T["accent2"], T["green"]]),
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
                ti = __import__(f[:-3])
                # Pass current date range via session_state so Traffic Intel uses sidebar dates
                st.session_state["ti_start"] = p_start.strftime("%Y-%m-%d")
                st.session_state["ti_end"] = p_end.strftime("%Y-%m-%d")
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
                    fillcolor=f"{T['accent']}18", showlegend=False,
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
    _rc1, _rc2 = st.columns(2)
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

    if st.session_state.get("report"):
        st.markdown(st.session_state["report"]["markdown"])
        st.download_button(
            "⬇️ Download",
            st.session_state["report"]["markdown"],
            f"eagle3d_{rep_type}_{datetime.now().strftime('%Y%m%d')}.md",
            "text/markdown",
            use_container_width=True,
        )

    st.markdown("---")
    st.markdown("#### 📁 Archive")
    _saved = _rep.list_saved_reports()
    if _saved:
        for _r in _saved[:15]:
            st.markdown(
                f"- **{_r['filename']}** — {_r['modified']} "
                f"({_r['size'] / 1024:.1f} KB)"
            )
    else:
        st.info("No saved reports yet.")

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
    tabs = st.tabs(["📥 Sign-ups", "📦 First Uploads", "💳 Stripe"])
    _stripe = load_sheet("Verified_STRIPE")
    for _tab, (_lb, _df_data) in zip(
        tabs,
        [
            ("Sign-ups", free_rows),
            ("First Uploads", upload_rows),
            ("Stripe", _stripe),
        ],
    ):
        with _tab:
            if _df_data.empty:
                st.warning(f"No {_lb} data")
                continue
            c1, c2, c3 = st.columns(3)
            with c1:
                sf = st.selectbox(
                    "Status:",
                    ["All", "ACCEPTED", "REJECTED", "NOT_DETERMINED"],
                    key=f"s_{_lb}",
                )
            with c2:
                sr = st.text_input("🔍", key=f"q_{_lb}",
                                   placeholder="search...")
            with c3:
                mr = st.number_input(
                    "Rows", value=200, min_value=10,
                    max_value=5000, key=f"r_{_lb}",
                )
            fl = _df_data.copy()
            if sf != "All" and "final_status" in fl.columns:
                fl = fl[
                    fl["final_status"].astype(str).str.upper() == sf
                ]
            if sr:
                msk = pd.Series([False] * len(fl), index=fl.index)
                for c in fl.columns:
                    msk = msk | fl[c].astype(str).str.contains(
                        sr, case=False, na=False
                    )
                fl = fl[msk]
            st.metric("Showing", f"{len(fl)} rows")
            _df(fl.head(mr), height=450)
            st.download_button(
                "⬇️ Download",
                data=fl.to_csv(index=False).encode("utf-8"),
                file_name=f"{_lb.lower().replace(' ', '_')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

# ═══════════════════════════════════════════════════════════════
# PAGE: ⚙️ SETTINGS
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
    for _i, (_nm, _ky) in enumerate(_mods.items()):
        with _mc[_i % 4]:
            _ok = _ky in MOD
            if _ok:
                _act += 1
            _bd = (
                '<span class="badge badge-ok">✅</span>'
                if _ok
                else '<span class="badge badge-err">❌</span>'
            )
            st.markdown(f"**{_nm}** {_bd}", unsafe_allow_html=True)
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
            st.warning(
                "💡 Get free keys: [Groq](https://console.groq.com) · "
                "[Gemini](https://aistudio.google.com)"
            )
    else:
        st.error("AI Engine not loaded")

    st.markdown("#### 🔐 Secrets")
    for _ky, _ds in [
        ("MASTER_SHEET_URL", "Sheets URL"),
        ("GOOGLE_CREDS", "Service Acct"),
        ("GROQ_API_KEY", "Groq AI"),
        ("GEMINI_API_KEY", "Gemini AI"),
        ("TELEGRAM_BOT_TOKEN", "Telegram Bot"),
    ]:
        _vl = get_secret(_ky)
        if _vl:
            st.success(f"✅ {_ds}")
        else:
            st.warning(f"⚠️ {_ds}: Not set")

    st.markdown("#### 📊 Data")
    _di = {
        "KPI": len(kpi_all),
        "Period": len(kpi),
        "Sign-ups": len(free_rows),
        "Uploads": len(upload_rows),
        "Stripe": len(stripe_raw),
        "Sources": len(leads_df),
    }
    _ic = st.columns(3)
    for _idx, (_l, _c) in enumerate(_di.items()):
        with _ic[_idx % 3]:
            st.metric(_l, f"{_c:,}")

# ═══════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════
st.divider()
_fc1, _fc2, _fc3 = st.columns(3)
with _fc1:
    st.caption(
        f"🦅 Eagle 3D Streaming KPI v5.1 | "
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
