"""
Email Intelligence — Mixpanel-style Analytics UI
Dark sidebar · Dense KPIs · Clean white canvas
"""

import io
import logging
import os
import streamlit as st

logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="Email Intelligence",
    page_icon="✉",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── GLOBAL CSS ────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  /* Sidebar — dark */
  --sb-bg:      #0f1117;
  --sb-border:  #1e2130;
  --sb-text:    #8b90a0;
  --sb-text-hi: #e8eaf0;
  --sb-active:  #1e2130;
  --sb-accent:  #7c5cfc;

  /* Main canvas — light */
  --bg:         #f7f8fa;
  --surface:    #ffffff;
  --surface2:   #f1f3f7;
  --border:     #e4e7ef;
  --border2:    #cdd0dc;
  --text:       #0d0f1a;
  --text2:      #4b5068;
  --text3:      #8b90a0;

  /* Accents */
  --purple:     #7c5cfc;
  --purple-bg:  #f0ecff;
  --purple-dim: rgba(124,92,252,.12);
  --green:      #22c55e;
  --green-bg:   #f0fdf4;
  --green-dim:  rgba(34,197,94,.12);
  --red:        #ef4444;
  --red-bg:     #fef2f2;
  --amber:      #f59e0b;
  --amber-bg:   #fffbeb;
  --blue:       #3b82f6;
  --blue-bg:    #eff6ff;

  --sans:   'Inter', system-ui, sans-serif;
  --mono:   'JetBrains Mono', 'Courier New', monospace;
  --r:      8px;
  --r-sm:   6px;
  --r-lg:   12px;

  --sidebar-w: 220px;
}

/* ── STREAMLIT RESETS ─────────────────────────── */
#MainMenu, footer, header,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"],
.stDeployButton { display: none !important; }

.stApp { background: var(--sb-bg) !important; font-family: var(--sans) !important; }
.main .block-container { padding: 0 !important; max-width: 100% !important; }
[data-testid="stVerticalBlock"],
[data-testid="stVerticalBlockBorderWrapper"] { gap: 0 !important; padding: 0 !important; }
[data-testid="element-container"] { margin: 0 !important; }
.stSelectbox label, .stSlider label { display: none !important; }

/* ── LAYOUT SHELL ─────────────────────────────── */
.ei-shell {
  display: flex;
  min-height: 100vh;
}

/* ── SIDEBAR ──────────────────────────────────── */
.ei-sidebar {
  width: var(--sidebar-w);
  flex-shrink: 0;
  background: var(--sb-bg);
  border-right: 1px solid var(--sb-border);
  display: flex;
  flex-direction: column;
  position: fixed;
  top: 0; left: 0; bottom: 0;
  z-index: 100;
  overflow-y: auto;
}

.sb-logo-area {
  padding: 18px 16px 14px;
  border-bottom: 1px solid var(--sb-border);
  display: flex; align-items: center; gap: 10px;
}
.sb-logo-icon {
  width: 28px; height: 28px; border-radius: 7px;
  background: var(--sb-accent);
  display: flex; align-items: center; justify-content: center;
  font-size: 13px; flex-shrink: 0;
  box-shadow: 0 2px 8px rgba(124,92,252,.4);
}
.sb-logo-text {
  font-size: 13px; font-weight: 600;
  color: var(--sb-text-hi); letter-spacing: -.01em;
}
.sb-logo-text span { color: var(--sb-accent); }

.sb-section { padding: 16px 10px 8px; }
.sb-section-label {
  font-size: 9px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .1em; color: var(--sb-text);
  padding: 0 6px; margin-bottom: 4px;
}
.sb-nav-item {
  display: flex; align-items: center; gap: 8px;
  padding: 7px 8px; border-radius: var(--r-sm);
  font-size: 12px; font-weight: 500; color: var(--sb-text);
  cursor: pointer; transition: all .12s; margin-bottom: 1px;
}
.sb-nav-item:hover { background: var(--sb-active); color: var(--sb-text-hi); }
.sb-nav-item.active { background: var(--sb-active); color: var(--sb-text-hi); }
.sb-nav-item.active::before {
  content: ''; position: absolute; left: 0;
  width: 3px; height: 20px; border-radius: 0 2px 2px 0;
  background: var(--sb-accent);
}
.sb-nav-item { position: relative; }
.sb-nav-icon { font-size: 13px; opacity: .8; }

.sb-divider { height: 1px; background: var(--sb-border); margin: 8px 10px; }

.sb-data-badge {
  margin: 0 10px 10px;
  background: #141620;
  border: 1px solid var(--sb-border);
  border-radius: var(--r-sm);
  padding: 10px 10px;
}
.sb-data-title { font-size: 10px; font-weight: 600; color: var(--sb-text); margin-bottom: 6px; text-transform: uppercase; letter-spacing: .07em; }
.sb-data-row {
  display: flex; align-items: center; gap: 5px;
  font-size: 10px; color: var(--sb-text); padding: 2px 0;
}
.sb-dot { width: 5px; height: 5px; border-radius: 50%; flex-shrink: 0; }
.sb-dot-green  { background: var(--green); }
.sb-dot-red    { background: var(--red); }
.sb-dot-purple { background: var(--sb-accent); }

.sb-status {
  margin: auto 10px 12px;
  display: flex; align-items: center; gap: 6px;
  font-size: 10px; color: var(--sb-text);
  padding: 8px 8px;
  background: #141620; border: 1px solid var(--sb-border);
  border-radius: var(--r-sm);
}
.sb-live-dot {
  width: 6px; height: 6px; border-radius: 50%;
  background: var(--green); flex-shrink: 0;
  box-shadow: 0 0 6px var(--green);
  animation: livepulse 2s ease-in-out infinite;
}
@keyframes livepulse { 0%,100%{opacity:1} 50%{opacity:.4} }

/* ── MAIN CANVAS ──────────────────────────────── */
.ei-main {
  margin-left: var(--sidebar-w);
  flex: 1;
  background: var(--bg);
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

/* ── TOP BAR ──────────────────────────────────── */
.ei-topbar {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  padding: 0 24px;
  height: 48px;
  display: flex; align-items: center; justify-content: space-between;
  position: sticky; top: 0; z-index: 50;
}
.tb-breadcrumb {
  display: flex; align-items: center; gap: 6px;
  font-size: 12px; color: var(--text3);
}
.tb-breadcrumb strong { color: var(--text); font-weight: 600; }
.tb-sep { color: var(--border2); }
.tb-actions { display: flex; align-items: center; gap: 8px; }
.tb-tag {
  font-size: 10px; font-weight: 600; padding: 3px 8px;
  border-radius: 20px; letter-spacing: .02em;
  display: flex; align-items: center; gap: 4px;
}
.tag-source { background: var(--purple-bg); color: var(--purple); border: 1px solid rgba(124,92,252,.2); }
.tag-live   { background: var(--green-bg);  color: #16a34a;        border: 1px solid rgba(34,197,94,.2); }

/* ── PAGE CONTENT ─────────────────────────────── */
.ei-content { padding: 20px 24px 100px; }

/* ── KPI ROW ──────────────────────────────────── */
.kpi-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
  margin-bottom: 16px;
}
.kpi-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--r-lg);
  padding: 16px 18px;
}
.kpi-label {
  font-size: 10px; font-weight: 600; text-transform: uppercase;
  letter-spacing: .08em; color: var(--text3); margin-bottom: 8px;
}
.kpi-value {
  font-size: 28px; font-weight: 700; color: var(--text);
  letter-spacing: -.03em; line-height: 1; margin-bottom: 8px;
  font-variant-numeric: tabular-nums;
}
.kpi-delta {
  display: inline-flex; align-items: center; gap: 4px;
  font-size: 11px; font-weight: 500; padding: 3px 8px;
  border-radius: 20px;
}
.delta-up   { background: var(--green-dim); color: #16a34a; }
.delta-info { background: var(--purple-dim); color: var(--purple); }
.delta-neu  { background: var(--surface2); color: var(--text3); }

/* ── CHARTS ROW ───────────────────────────────── */
.charts-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
  margin-bottom: 16px;
}
.chart-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--r-lg);
  padding: 16px 18px;
}
.chart-title { font-size: 13px; font-weight: 600; color: var(--text); margin-bottom: 2px; }
.chart-sub   { font-size: 11px; color: var(--text3); margin-bottom: 12px; }

/* ── DATA CONTEXT ─────────────────────────────── */
.ctx-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--r-lg);
  margin-bottom: 16px;
  overflow: hidden;
}
.ctx-card-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 12px 18px;
  cursor: pointer; user-select: none;
  border-bottom: 1px solid transparent;
  transition: background .1s;
}
.ctx-card-header:hover { background: var(--surface2); }
.ctx-card-header.open { border-bottom-color: var(--border); }
.ctx-hdr-left { display: flex; align-items: center; gap: 10px; }
.ctx-pill {
  font-size: 10px; font-weight: 600;
  background: var(--purple-dim); color: var(--purple);
  border: 1px solid rgba(124,92,252,.2);
  padding: 2px 8px; border-radius: 20px;
}
.ctx-hdr-title { font-size: 13px; font-weight: 600; color: var(--text); }
.ctx-hdr-sub   { font-size: 11px; color: var(--text3); margin-left: 8px; }
.ctx-chevron   { font-size: 11px; color: var(--text3); transition: transform .2s; }
.ctx-chevron.open { transform: rotate(180deg); }

.ctx-body { display: none; padding: 16px 18px; }
.ctx-body.open { display: block; }
.ctx-grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }

.ctx-col-title {
  font-size: 10px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .08em; margin-bottom: 8px;
}
.ctx-col-title.green  { color: #16a34a; }
.ctx-col-title.red    { color: var(--red); }
.ctx-col-title.purple { color: var(--purple); }

.ctx-row {
  display: flex; align-items: flex-start; gap: 7px;
  font-size: 11px; color: var(--text2); padding: 3px 0; line-height: 1.4;
}
.ctx-dot-sm { width: 5px; height: 5px; border-radius: 50%; margin-top: 4px; flex-shrink: 0; }
.cdot-g { background: var(--green); }
.cdot-r { background: var(--red); }
.cdot-p { background: var(--purple); }

.hypo-wrap { display: flex; flex-wrap: wrap; gap: 5px; }
.hypo-btn {
  font-size: 10px; font-weight: 500; color: var(--purple);
  background: var(--purple-bg); border: 1px solid rgba(124,92,252,.2);
  border-radius: var(--r-sm); padding: 4px 10px;
  cursor: pointer; transition: all .12s; font-family: var(--sans);
}
.hypo-btn:hover { background: var(--purple-dim); border-color: var(--purple); }

/* ── FILTERS BAR ──────────────────────────────── */
.filters-bar {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--r-lg);
  padding: 10px 14px;
  display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  margin-bottom: 16px;
}
.filter-label {
  font-size: 10px; font-weight: 600; text-transform: uppercase;
  letter-spacing: .08em; color: var(--text3); white-space: nowrap;
  margin-right: 2px;
}
.filter-sep { width: 1px; height: 24px; background: var(--border); margin: 0 4px; }

/* Streamlit widget overrides inside filters-bar */
.filters-bar .stSelectbox { min-width: 110px !important; }
.filters-bar .stSelectbox > div > div {
  background: var(--surface2) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important;
  font-size: 12px !important; color: var(--text) !important;
  min-height: 30px !important; box-shadow: none !important;
  font-family: var(--sans) !important;
}
.filters-bar .stSlider { min-width: 90px !important; }
.filters-bar [data-testid="stSliderThumb"] { background: var(--purple) !important; }
.filters-bar .stButton > button {
  background: var(--surface2) !important; border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important; color: var(--text3) !important;
  font-size: 11px !important; padding: 4px 10px !important;
  white-space: nowrap !important; box-shadow: none !important; height: 30px !important;
  font-family: var(--sans) !important;
}
.filters-bar .stButton > button:hover {
  border-color: var(--red) !important; color: var(--red) !important;
  background: var(--red-bg) !important;
}

/* ── DOWNLOADS ────────────────────────────────── */
.dl-bar { display: flex; gap: 6px; margin-bottom: 16px; }
.dl-bar .stDownloadButton > button {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important; color: var(--text3) !important;
  font-size: 11px !important; padding: 4px 12px !important;
  box-shadow: none !important; font-family: var(--sans) !important;
}
.dl-bar .stDownloadButton > button:hover {
  border-color: var(--purple) !important; color: var(--purple) !important;
  background: var(--purple-bg) !important;
}

/* ── CHAT ─────────────────────────────────────── */
.chat-panel {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--r-lg);
  overflow: hidden;
}
.chat-panel-header {
  padding: 12px 18px;
  border-bottom: 1px solid var(--border);
  display: flex; align-items: center; justify-content: space-between;
}
.chat-panel-title { font-size: 13px; font-weight: 600; color: var(--text); }
.chat-panel-sub   { font-size: 11px; color: var(--text3); }

/* Suggestion chips */
.sug-chips { display: flex; flex-wrap: wrap; gap: 6px; padding: 14px 18px; border-bottom: 1px solid var(--border); }
.sug-chips .stButton > button {
  background: var(--surface2) !important; border: 1px solid var(--border) !important;
  border-radius: 20px !important; color: var(--text2) !important;
  font-size: 11px !important; padding: 5px 12px !important;
  white-space: nowrap !important; box-shadow: none !important;
  font-weight: 500 !important; font-family: var(--sans) !important;
}
.sug-chips .stButton > button:hover {
  border-color: var(--purple) !important; color: var(--purple) !important;
  background: var(--purple-bg) !important;
}

/* Messages */
.msgs-area { padding: 12px 18px; display: flex; flex-direction: column; gap: 10px; min-height: 40px; }
.msg-user {
  align-self: flex-end;
  background: var(--text); color: #f7f8fa;
  border-radius: 10px 10px 3px 10px;
  padding: 9px 14px; font-size: 13px; line-height: 1.5;
  max-width: min(500px, 85%); word-wrap: break-word;
}
.msg-ai-row { display: flex; gap: 8px; align-items: flex-start; }
.msg-ai-avatar {
  width: 22px; height: 22px; border-radius: 6px; flex-shrink: 0;
  background: var(--purple); display: flex; align-items: center;
  justify-content: center; font-size: 10px; margin-top: 1px;
  box-shadow: 0 2px 6px rgba(124,92,252,.35);
}
.msg-ai-bubble {
  background: var(--surface2); border: 1px solid var(--border);
  border-radius: 3px 10px 10px 10px;
  padding: 9px 14px; font-size: 13px; line-height: 1.6; color: var(--text);
  flex: 1; min-width: 0;
}

/* Pills */
.tpills { display: flex; gap: 4px; margin-bottom: 6px; }
.pill {
  font-family: var(--mono); font-size: 9px; font-weight: 500;
  padding: 2px 6px; border-radius: 4px; letter-spacing: .03em;
}
.p-sql { background: var(--blue-bg); color: var(--blue); border: 1px solid rgba(59,130,246,.2); }
.p-rag { background: var(--purple-bg); color: var(--purple); border: 1px solid rgba(124,92,252,.2); }

/* Tables inside messages */
.msg-ai-bubble table { width: 100%; border-collapse: collapse; font-size: 12px; margin-top: 8px; }
.msg-ai-bubble thead tr { background: var(--bg); }
.msg-ai-bubble th {
  text-align: left; padding: 6px 10px;
  font-size: 10px; font-weight: 600; color: var(--text3);
  text-transform: uppercase; letter-spacing: .06em;
  border-bottom: 1px solid var(--border);
}
.msg-ai-bubble td { padding: 6px 10px; border-bottom: 1px solid var(--surface2); }
.msg-ai-bubble tr:last-child td { border-bottom: none; }

/* Thinking */
.thinking { display: flex; align-items: center; gap: 6px; color: var(--text3); font-size: 12px; padding: 4px 0; }
.tdot { width: 4px; height: 4px; border-radius: 50%; background: var(--border2); animation: bdot 1.3s ease-in-out infinite; }
.tdot:nth-child(2){ animation-delay:.15s; } .tdot:nth-child(3){ animation-delay:.3s; }
@keyframes bdot { 0%,80%,100%{opacity:.2;transform:scale(.7)} 40%{opacity:1;transform:scale(1)} }

/* Fixed chat input */
.stChatInputContainer {
  background: var(--surface) !important;
  border-top: 1px solid var(--border) !important;
  padding: 10px 24px 12px !important;
  position: fixed !important; bottom: 0 !important;
  left: var(--sidebar-w) !important; right: 0 !important;
  z-index: 150 !important;
  box-shadow: 0 -4px 24px rgba(0,0,0,.06) !important;
}
textarea[data-testid="stChatInputTextArea"] {
  background: var(--bg) !important; border: 1px solid var(--border2) !important;
  border-radius: var(--r) !important; color: var(--text) !important;
  font-family: var(--sans) !important; font-size: 13px !important;
  padding: 10px 14px !important; max-width: 800px !important;
  margin: 0 auto !important; display: block !important;
}
textarea[data-testid="stChatInputTextArea"]:focus {
  border-color: var(--purple) !important;
  box-shadow: 0 0 0 3px rgba(124,92,252,.1) !important; outline: none !important;
}
[data-testid="stChatInputSubmitButton"] > button {
  background: var(--purple) !important; border: none !important;
  color: white !important; border-radius: 7px !important;
}

.bottom-pad { height: 80px; }
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }
::-webkit-scrollbar-track { background: transparent; }
</style>
""", unsafe_allow_html=True)


# ─── STATE ─────────────────────────────────────────────────────────────────
if "history"  not in st.session_state: st.session_state.history  = []
if "messages" not in st.session_state: st.session_state.messages = []
if "last_df"  not in st.session_state: st.session_state.last_df  = None
if "filters"  not in st.session_state: st.session_state.filters  = {}

REQUIRED = ["OPENAI_API_KEY", "QDRANT_URL", "QDRANT_API_KEY"]
missing  = [v for v in REQUIRED if not os.environ.get(v)]


# ─── HELPERS ───────────────────────────────────────────────────────────────
def parse_md_table(raw):
    if not raw or "|" not in raw: return []
    rows = [r for r in raw.split("\n") if r.startswith("|") and "---" not in r]
    if len(rows) < 2: return []
    headers = [h.strip() for h in rows[0].split("|")[1:-1]]
    out = []
    for row in rows[1:]:
        vals = [v.strip() for v in row.split("|")[1:-1]]
        if len(vals) == len(headers):
            out.append(dict(zip(headers, vals)))
    return out

@st.cache_data(ttl=300, show_spinner=False)
def load_stats():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT COUNT(*) as total,
              ROUND(AVG(k.open_rate_percent),1) as avg_open,
              ROUND(AVG(k.ctr_percent),2) as avg_ctr,
              COUNT(DISTINCT e.hook_type) as hook_types
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
        """, max_rows=1)
    except: return None

@st.cache_data(ttl=300, show_spinner=False)
def load_hook_data():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT e.hook_type, COUNT(*) as campaigns,
              ROUND(AVG(k.open_rate_percent),1) as avg_open,
              ROUND(AVG(k.ctr_percent),2) as avg_ctr
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
            WHERE e.hook_type IS NOT NULL AND e.hook_type != ''
            GROUP BY e.hook_type ORDER BY avg_open DESC
        """, max_rows=20)
    except: return None

@st.cache_data(ttl=300, show_spinner=False)
def load_tone_data():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT e.tone, COUNT(*) as campaigns,
              ROUND(AVG(k.open_rate_percent),1) as avg_open
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
            WHERE e.tone IS NOT NULL AND e.tone != ''
            GROUP BY e.tone ORDER BY campaigns DESC
        """, max_rows=10)
    except: return None


# ─── KPI VALUES ────────────────────────────────────────────────────────────
stat_vals = ["—", "—", "—", "—"]
if not missing:
    parsed = parse_md_table(load_stats())
    if parsed:
        stat_vals = list(parsed[0].values())

KPI = [
    ("Campaigns",     "",  "All time",           "delta-neu"),
    ("Avg Open Rate", "%", "↑ vs 21% industry",  "delta-up"),
    ("Avg CTR",       "%", "↑ vs 2.6% industry", "delta-up"),
    ("Hook Types",    "",  "GPT classified",      "delta-info"),
]


# ─── SIDEBAR ───────────────────────────────────────────────────────────────
live_badge = "🟢 Live" if not missing else "🔴 Setup needed"
sb_status_cls = "sb-dot-green" if not missing else "sb-dot-red"

st.markdown(f"""
<div class="ei-sidebar">
  <div class="sb-logo-area">
    <div class="sb-logo-icon">✉</div>
    <div class="sb-logo-text">Email <span>Intelligence</span></div>
  </div>

  <div class="sb-section">
    <div class="sb-section-label">Analytics</div>
    <div class="sb-nav-item active"><span class="sb-nav-icon">📊</span> Overview</div>
    <div class="sb-nav-item"><span class="sb-nav-icon">🪝</span> Hook Analysis</div>
    <div class="sb-nav-item"><span class="sb-nav-icon">🎭</span> Tone Performance</div>
    <div class="sb-nav-item"><span class="sb-nav-icon">🌍</span> By Language</div>
  </div>

  <div class="sb-divider"></div>

  <div class="sb-section">
    <div class="sb-section-label">Tools</div>
    <div class="sb-nav-item"><span class="sb-nav-icon">✦</span> AI Assistant</div>
    <div class="sb-nav-item"><span class="sb-nav-icon">↓</span> Export</div>
  </div>

  <div class="sb-divider"></div>

  <div class="sb-section">
    <div class="sb-section-label">Data source</div>
    <div class="sb-data-badge">
      <div class="sb-data-title">Mailchimp → BigQuery</div>
      <div class="sb-data-row"><div class="sb-dot sb-dot-green"></div>Open rate · CTR · Hook · Tone</div>
      <div class="sb-data-row"><div class="sb-dot sb-dot-green"></div>Subject line · Language · Date</div>
      <div class="sb-data-row"><div class="sb-dot sb-dot-red"></div>No revenue / affiliate data</div>
      <div class="sb-data-row"><div class="sb-dot sb-dot-red"></div>No unsubscribes / bounces</div>
    </div>
  </div>

  <div class="sb-status">
    <div class="sb-live-dot"></div>
    <span>{"Connected · Live data" if not missing else "Setup needed"}</span>
  </div>
</div>
""", unsafe_allow_html=True)


# ─── MAIN CANVAS ───────────────────────────────────────────────────────────
st.markdown("""
<div class="ei-main">
  <div class="ei-topbar">
    <div class="tb-breadcrumb">
      <span>Email Intelligence</span>
      <span class="tb-sep">›</span>
      <strong>Overview</strong>
    </div>
    <div class="tb-actions">
      <span class="tb-tag tag-source">Mailchimp · BigQuery</span>
      <span class="tb-tag tag-live">● Live</span>
    </div>
  </div>
  <div class="ei-content">
""", unsafe_allow_html=True)

# KPI cards
kpi_html = '<div class="kpi-row">'
for i, (lbl, sfx, hint, dcls) in enumerate(KPI):
    v = stat_vals[i] if i < len(stat_vals) else "—"
    kpi_html += f"""
    <div class="kpi-card">
      <div class="kpi-label">{lbl}</div>
      <div class="kpi-value">{v}{sfx}</div>
      <span class="kpi-delta {dcls}">{hint}</span>
    </div>"""
kpi_html += '</div>'
st.markdown(kpi_html, unsafe_allow_html=True)

# Charts
if not missing:
    try:
        import plotly.graph_objects as go
        hook_data = parse_md_table(load_hook_data())
        tone_data = parse_md_table(load_tone_data())

        st.markdown('<div class="charts-row">', unsafe_allow_html=True)
        c1, c2 = st.columns(2, gap="small")

        with c1:
            st.markdown('<div class="chart-card"><div class="chart-title">Open Rate by Hook Type</div><div class="chart-sub">avg % · sorted by performance</div>', unsafe_allow_html=True)
            if hook_data:
                hooks  = [d["hook_type"].replace("-", " ").title() for d in hook_data]
                opens  = [float(d["avg_open"]) for d in hook_data]
                counts = [int(d["campaigns"]) for d in hook_data]
                mx = max(opens)
                PURPLE_PALETTE = ["#7c5cfc" if o == mx else "#c4b5fd" for o in opens]
                fig = go.Figure(go.Bar(
                    x=opens, y=hooks, orientation="h",
                    marker_color=PURPLE_PALETTE,
                    marker_line_width=0,
                    text=[f"{o}%" for o in opens], textposition="outside",
                    textfont=dict(size=10, color="#8b90a0"),
                    customdata=counts,
                    hovertemplate="<b>%{y}</b><br>%{x}% · %{customdata} campaigns<extra></extra>",
                ))
                fig.update_layout(
                    height=200, margin=dict(l=0, r=44, t=2, b=0),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="Inter,sans-serif", size=11, color="#8b90a0"),
                    xaxis=dict(showgrid=True, gridcolor="#f1f3f7", zeroline=False,
                               showticklabels=False, range=[0, mx * 1.3]),
                    yaxis=dict(showgrid=False),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                st.markdown('<div style="color:#8b90a0;font-size:12px;padding:20px 0;text-align:center">Enrichment in progress…</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        with c2:
            st.markdown('<div class="chart-card"><div class="chart-title">Tone Distribution</div><div class="chart-sub">campaigns by communication style</div>', unsafe_allow_html=True)
            if tone_data:
                tones  = [d["tone"].title() for d in tone_data]
                counts = [int(d["campaigns"]) for d in tone_data]
                PALETTE = ["#7c5cfc", "#a78bfa", "#c4b5fd", "#ddd6fe", "#6d28d9", "#8b5cf6", "#4c1d95", "#7c3aed"]
                fig2 = go.Figure(go.Pie(
                    labels=tones, values=counts, hole=0.58,
                    marker_colors=PALETTE[:len(tones)],
                    marker=dict(line=dict(color="#fff", width=2)),
                    textinfo="percent", textfont_size=10,
                    hovertemplate="<b>%{label}</b><br>%{value} · %{percent}<extra></extra>",
                ))
                fig2.update_layout(
                    height=200, margin=dict(l=0, r=0, t=2, b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="Inter,sans-serif", size=10, color="#8b90a0"),
                    legend=dict(orientation="v", x=1.02, y=0.5, font=dict(size=10), bgcolor="rgba(0,0,0,0)"),
                )
                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
            else:
                st.markdown('<div style="color:#8b90a0;font-size:12px;padding:20px 0;text-align:center">Enrichment in progress…</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    except ImportError:
        st.markdown("""<div style="margin-bottom:16px;padding:10px 14px;background:#fff7ed;
            border:1px solid #fed7aa;border-radius:8px;font-size:12px;color:#c2410c;">
            ⚠ Charts need plotly — <code>pip install plotly</code></div>""", unsafe_allow_html=True)


# ─── FILTERS BAR ───────────────────────────────────────────────────────────
st.markdown('<div class="filters-bar">', unsafe_allow_html=True)

st.markdown('<span class="filter-label">Filter</span>', unsafe_allow_html=True)

ctl = st.columns([1.1, 1, 1, 1, 1.2, .7])

with ctl[0]:
    model = st.selectbox("model", ["gpt-4o-mini", "gpt-4o"], index=0,
                         label_visibility="collapsed", key="mdl")
    os.environ["AGENT_MODEL"] = model

with ctl[1]:
    fh = st.selectbox("hook", ["Any hook","curiosity","urgency","social-proof",
                                "fear-of-missing-out","story","discount","question"],
                      label_visibility="collapsed", key="fh")

with ctl[2]:
    ft = st.selectbox("tone", ["Any tone","casual","formal","playful",
                                "urgent","inspirational","informational"],
                      label_visibility="collapsed", key="ft")

with ctl[3]:
    fl = st.selectbox("lang", ["Any lang","en","lt","ru","es","pl"],
                      label_visibility="collapsed", key="fl")

with ctl[4]:
    mo = st.slider("min_open", 0, 100, 0, label_visibility="collapsed", key="mo")

with ctl[5]:
    if st.button("✕ Clear", key="clr"):
        st.session_state.history  = []
        st.session_state.messages = []
        st.session_state.last_df  = None
        st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

st.session_state.filters = {k: v for k, v in {
    "hook_type":     None if fh.startswith("Any") else fh,
    "tone":          None if ft.startswith("Any") else ft,
    "language":      None if fl.startswith("Any") else fl,
    "min_open_rate": mo if mo > 0 else None,
}.items() if v is not None}

# Downloads
if st.session_state.last_df is not None:
    df = st.session_state.last_df
    st.markdown('<div class="dl-bar">', unsafe_allow_html=True)
    dl = st.columns([.6, .6, 8])
    with dl[0]:
        st.download_button("↓ CSV", df.to_csv(index=False).encode(),
                           "export.csv", "text/csv", key="dl_csv")
    with dl[1]:
        try:
            import openpyxl; buf = io.BytesIO()
            df.to_excel(buf, index=False, engine="openpyxl")
            st.download_button("↓ Excel", buf.getvalue(), "export.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_xlsx")
        except ImportError: pass
    st.markdown('</div>', unsafe_allow_html=True)


# ─── DATA CONTEXT PANEL ────────────────────────────────────────────────────
st.markdown("""
<div class="ctx-card">
  <div class="ctx-card-header" id="ctx-hdr" onclick="toggleCtx()">
    <div class="ctx-hdr-left">
      <span class="ctx-pill">Data schema</span>
      <span class="ctx-hdr-title">What's in this dataset?</span>
      <span class="ctx-hdr-sub">Mailchimp → BigQuery · no revenue data</span>
    </div>
    <span class="ctx-chevron" id="ctx-arrow">▾</span>
  </div>
  <div class="ctx-body" id="ctx-body">
    <div class="ctx-grid3">

      <div>
        <div class="ctx-col-title green">✓ Available</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-g"></div>1,652 email campaigns from Mailchimp</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-g"></div>Open rate &amp; CTR per campaign</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-g"></div>Hook type — GPT-classified (7 types)</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-g"></div>Tone (Casual, Informational, Playful…)</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-g"></div>Language (EN, RU, LT, ES, PL)</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-g"></div>Subject line text &amp; preview</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-g"></div>Send date &amp; campaign name</div>
      </div>

      <div>
        <div class="ctx-col-title red">✗ Not available</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-r"></div>Revenue / GMV — connect affiliate data separately</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-r"></div>Unsubscribe &amp; bounce rates</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-r"></div>Individual recipient behaviour</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-r"></div>A/B test variants</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-r"></div>Audience segment breakdown</div>
        <div class="ctx-row"><div class="ctx-dot-sm cdot-r"></div>Send-time optimisation data</div>
      </div>

      <div>
        <div class="ctx-col-title purple">⚡ Hypotheses to explore</div>
        <div class="hypo-wrap">
          <button class="hypo-btn" onclick="sendHypo('Does curiosity hook outperform urgency across all languages?')">Curiosity vs Urgency by language</button>
          <button class="hypo-btn" onclick="sendHypo('Which tone has the highest CTR — casual or informational?')">Tone → CTR</button>
          <button class="hypo-btn" onclick="sendHypo('Show open rate trend over time — are results improving?')">Open rate trend</button>
          <button class="hypo-btn" onclick="sendHypo('What subject line patterns appear in top 50 campaigns by open rate?')">Top subject patterns</button>
          <button class="hypo-btn" onclick="sendHypo('Compare performance of discount vs gift hook types')">Discount vs Gift</button>
          <button class="hypo-btn" onclick="sendHypo('Which language audience responds best to urgency emails?')">Language × Hook</button>
          <button class="hypo-btn" onclick="sendHypo('Show me campaigns with open rate above 50%')">50%+ open rate</button>
          <button class="hypo-btn" onclick="sendHypo('Is there a correlation between subject line length and open rate?')">Subject length → open</button>
        </div>
      </div>

    </div>
  </div>
</div>

<script>
function toggleCtx() {
  var body  = document.getElementById('ctx-body');
  var arrow = document.getElementById('ctx-arrow');
  var hdr   = document.getElementById('ctx-hdr');
  var open  = body.classList.contains('open');
  body.classList.toggle('open', !open);
  arrow.classList.toggle('open', !open);
  hdr.classList.toggle('open', !open);
}
function sendHypo(text) {
  var inputs = window.parent.document.querySelectorAll('textarea[data-testid="stChatInputTextArea"]');
  if (inputs.length > 0) {
    var inp = inputs[0];
    var setter = Object.getOwnPropertyDescriptor(window.parent.HTMLTextAreaElement.prototype, 'value').set;
    setter.call(inp, text);
    inp.dispatchEvent(new Event('input', { bubbles: true }));
    inp.focus();
  }
}
</script>
""", unsafe_allow_html=True)


# ─── CHAT PANEL ────────────────────────────────────────────────────────────
st.markdown("""
<div class="chat-panel">
  <div class="chat-panel-header">
    <div class="chat-panel-title">AI Assistant</div>
    <div class="chat-panel-sub">Ask anything about your campaigns</div>
  </div>
""", unsafe_allow_html=True)

SUGGESTIONS = [
    ("📈", "Top 10 by open rate",  "Top 10 campaigns by open rate"),
    ("🪝", "Best hook type?",      "Which hook type works best?"),
    ("⚡", "Urgency > 30% open",   "Urgency emails with open rate > 30%"),
    ("🎭", "CTR by tone",          "CTR by tone comparison"),
    ("🌍", "By language",          "Campaigns by language breakdown"),
    ("💸", "Best discount emails", "Best discount campaigns"),
]

if not st.session_state.messages:
    st.markdown('<div class="sug-chips">', unsafe_allow_html=True)
    chip_cols = st.columns(6)
    for i, (icon, label, full_q) in enumerate(SUGGESTIONS):
        with chip_cols[i % 6]:
            if st.button(f"{icon} {label}", key=f"c{i}"):
                st.session_state.pending_question = full_q
                st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="msgs-area">', unsafe_allow_html=True)
for msg in st.session_state.messages:
    if msg["role"] == "user":
        st.markdown(f'<div class="msg-user">{msg["content"]}</div>', unsafe_allow_html=True)
    else:
        content = msg["content"]
        pills = ""
        if any(k in content.lower() for k in ["open rate", "%", "avg", "count", "ctr", "campaigns"]):
            pills += '<span class="pill p-sql">SQL</span>'
        if any(k in content.lower() for k in ["similar", "score:", "preview:"]):
            pills += '<span class="pill p-rag">RAG</span>'
        ph = f'<div class="tpills">{pills}</div>' if pills else ""
        st.markdown(f'<div class="msg-ai-row"><div class="msg-ai-avatar">✦</div><div class="msg-ai-bubble">{ph}', unsafe_allow_html=True)
        st.markdown(content)
        st.markdown('</div></div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)  # /msgs-area
st.markdown('</div>', unsafe_allow_html=True)  # /chat-panel

st.markdown('</div>', unsafe_allow_html=True)  # /ei-content
st.markdown('</div>', unsafe_allow_html=True)  # /ei-main

st.markdown('<div class="bottom-pad"></div>', unsafe_allow_html=True)


# ─── AGENT ─────────────────────────────────────────────────────────────────
def extract_df(reply):
    try:
        import pandas as pd
        rows = parse_md_table(reply)
        if rows: return pd.DataFrame(rows)
    except: pass
    return None

def run_question(question: str):
    st.session_state.messages.append({"role": "user", "content": question})
    st.markdown(f'<div class="msg-user">{question}</div>', unsafe_allow_html=True)
    ph = st.empty()
    ph.markdown('<div class="thinking"><div class="tdot"></div><div class="tdot"></div><div class="tdot"></div><span>Analysing…</span></div>', unsafe_allow_html=True)
    try:
        from agent import run_agent
        aug = question
        if st.session_state.filters:
            aug = f"{question}\n[Filters: {', '.join(f'{k}={v}' for k, v in st.session_state.filters.items())}]"
        reply, hist = run_agent(aug, st.session_state.history)
        st.session_state.history = hist
        ph.empty()
        df = extract_df(reply)
        if df is not None: st.session_state.last_df = df
        pills = ""
        if any(k in reply.lower() for k in ["open rate", "%", "avg", "count", "ctr", "campaigns"]):
            pills += '<span class="pill p-sql">SQL</span>'
        if any(k in reply.lower() for k in ["similar", "score:", "preview:"]):
            pills += '<span class="pill p-rag">RAG</span>'
        ph2 = f'<div class="tpills">{pills}</div>' if pills else ""
        st.markdown(f'<div class="msg-ai-row"><div class="msg-ai-avatar">✦</div><div class="msg-ai-bubble">{ph2}', unsafe_allow_html=True)
        st.markdown(reply)
        st.markdown('</div></div>', unsafe_allow_html=True)
        st.session_state.messages.append({"role": "assistant", "content": reply})
    except Exception as e:
        ph.empty()
        err = f"**Error:** {e}"
        st.markdown(f'<div class="msg-ai-row"><div class="msg-ai-avatar">✦</div><div class="msg-ai-bubble" style="color:var(--red)">{err}</div></div>', unsafe_allow_html=True)
        st.session_state.messages.append({"role": "assistant", "content": err})

if "pending_question" in st.session_state:
    q = st.session_state.pop("pending_question")
    run_question(q)
    st.rerun()

if prompt := st.chat_input("Ask about your campaigns…"):
    run_question(prompt)
