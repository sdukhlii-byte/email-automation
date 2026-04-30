"""
Email Intelligence — Mixpanel-style UI
Uses native st.sidebar — no CSS position hacks
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
    initial_sidebar_state="expanded",
)

# ─── CSS ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:         #f7f8fa;
  --surface:    #ffffff;
  --surface2:   #f1f3f7;
  --border:     #e4e7ef;
  --border2:    #cdd0dc;
  --text:       #0d0f1a;
  --text2:      #4b5068;
  --text3:      #8b90a0;
  --purple:     #7c5cfc;
  --purple-bg:  #f0ecff;
  --purple-dim: rgba(124,92,252,.12);
  --green:      #22c55e;
  --green-bg:   #f0fdf4;
  --green-dim:  rgba(34,197,94,.12);
  --red:        #ef4444;
  --red-bg:     #fef2f2;
  --blue:       #3b82f6;
  --blue-bg:    #eff6ff;
  --sans:   'Inter', system-ui, sans-serif;
  --mono:   'JetBrains Mono', monospace;
  --r:      8px;
  --r-sm:   6px;
  --r-lg:   12px;
}

/* ── STREAMLIT SHELL ───────────────────────── */
#MainMenu, footer, header,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
.stDeployButton { display: none !important; }

[data-testid="stSidebarCollapseButton"] { display: none !important; }

.stApp { background: var(--bg) !important; font-family: var(--sans) !important; }
.main .block-container {
  padding: 0 !important;
  max-width: 100% !important;
}
[data-testid="stVerticalBlock"],
[data-testid="stVerticalBlockBorderWrapper"] { gap: 0 !important; padding: 0 !important; }
[data-testid="element-container"] { margin: 0 !important; }
.stSelectbox label, .stSlider label { display: none !important; }

/* ── NATIVE SIDEBAR → DARK ─────────────────── */
[data-testid="stSidebar"] {
  background: #0f1117 !important;
  border-right: 1px solid #1e2130 !important;
  min-width: 220px !important;
  max-width: 220px !important;
}
[data-testid="stSidebar"] > div:first-child {
  padding: 0 !important;
  background: #0f1117 !important;
}
/* kill streamlit's own sidebar padding */
[data-testid="stSidebarContent"] {
  padding: 0 !important;
  background: #0f1117 !important;
}

/* ── SIDEBAR COMPONENTS ────────────────────── */
.sb-logo {
  display: flex; align-items: center; gap: 10px;
  padding: 18px 16px 16px;
  border-bottom: 1px solid #1e2130;
}
.sb-logo-icon {
  width: 28px; height: 28px; border-radius: 7px;
  background: #7c5cfc; display: flex; align-items: center;
  justify-content: center; font-size: 13px; flex-shrink: 0;
  box-shadow: 0 2px 8px rgba(124,92,252,.4);
}
.sb-logo-text { font-size: 13px; font-weight: 600; color: #e8eaf0; letter-spacing: -.01em; }
.sb-logo-text em { color: #7c5cfc; font-style: normal; }

.sb-section { padding: 14px 10px 6px; }
.sb-section-lbl {
  font-size: 9px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .1em; color: #4b5068; padding: 0 6px; margin-bottom: 4px;
}
.sb-item {
  display: flex; align-items: center; gap: 8px;
  padding: 7px 8px; border-radius: 6px;
  font-size: 12px; font-weight: 500; color: #8b90a0;
  margin-bottom: 1px; transition: background .1s, color .1s;
}
.sb-item:hover { background: #1e2130; color: #e8eaf0; }
.sb-item.active { background: #1e2130; color: #e8eaf0; }
.sb-item.active { position: relative; }
.sb-item-icon { font-size: 12px; }

.sb-divider { height: 1px; background: #1e2130; margin: 8px 10px; }

.sb-schema {
  margin: 0 10px 10px;
  background: #141620;
  border: 1px solid #1e2130;
  border-radius: 6px;
  padding: 10px 10px;
}
.sb-schema-title {
  font-size: 9px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .08em; color: #4b5068; margin-bottom: 7px;
}
.sb-schema-row {
  display: flex; align-items: flex-start; gap: 6px;
  font-size: 10px; color: #8b90a0; padding: 2px 0; line-height: 1.4;
}
.sb-dot { width: 5px; height: 5px; border-radius: 50%; flex-shrink: 0; margin-top: 4px; }
.d-green { background: #22c55e; }
.d-red   { background: #ef4444; }

.sb-status {
  margin: 0 10px 14px;
  display: flex; align-items: center; gap: 6px;
  font-size: 10px; color: #8b90a0;
  padding: 8px 10px;
  background: #141620;
  border: 1px solid #1e2130;
  border-radius: 6px;
}
.sb-live { width: 6px; height: 6px; border-radius: 50%; background: #22c55e; flex-shrink: 0; box-shadow: 0 0 6px #22c55e; animation: lp 2s ease-in-out infinite; }
@keyframes lp { 0%,100%{opacity:1}50%{opacity:.3} }

/* ── TOPBAR ────────────────────────────────── */
.ei-topbar {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  padding: 0 24px;
  height: 48px;
  display: flex; align-items: center; justify-content: space-between;
  position: sticky; top: 0; z-index: 50;
}
.tb-crumb { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text3); }
.tb-crumb strong { color: var(--text); font-weight: 600; }
.tb-tags { display: flex; gap: 6px; }
.tb-tag {
  font-size: 10px; font-weight: 600; padding: 3px 9px;
  border-radius: 20px; display: flex; align-items: center; gap: 4px;
}
.tag-src  { background: var(--purple-bg); color: var(--purple); border: 1px solid rgba(124,92,252,.2); }
.tag-live { background: var(--green-bg);  color: #16a34a; border: 1px solid rgba(34,197,94,.2); }

/* ── PAGE BODY ─────────────────────────────── */
.ei-body { padding: 20px 24px 100px; background: var(--bg); }

/* ── KPI CARDS ─────────────────────────────── */
.kpi-grid {
  display: grid; grid-template-columns: repeat(4,1fr);
  gap: 12px; margin-bottom: 16px;
}
.kpi-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--r-lg); padding: 16px 18px;
}
.kpi-lbl {
  font-size: 10px; font-weight: 600; text-transform: uppercase;
  letter-spacing: .08em; color: var(--text3); margin-bottom: 8px;
}
.kpi-val {
  font-size: 28px; font-weight: 700; color: var(--text);
  letter-spacing: -.03em; line-height: 1; margin-bottom: 8px;
  font-variant-numeric: tabular-nums;
}
.kpi-badge {
  display: inline-flex; align-items: center; gap: 4px;
  font-size: 11px; font-weight: 500; padding: 3px 8px; border-radius: 20px;
}
.b-up   { background: var(--green-dim); color: #16a34a; }
.b-info { background: var(--purple-dim); color: var(--purple); }
.b-neu  { background: var(--surface2); color: var(--text3); }

/* ── CHART CARDS ───────────────────────────── */
.charts-grid {
  display: grid; grid-template-columns: 1fr 1fr;
  gap: 12px; margin-bottom: 16px;
}
.chart-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--r-lg); padding: 16px 18px;
}
.chart-title { font-size: 13px; font-weight: 600; color: var(--text); margin-bottom: 2px; }
.chart-sub   { font-size: 11px; color: var(--text3); margin-bottom: 12px; }
.chart-empty { font-size: 12px; color: var(--text3); padding: 24px 0; text-align: center; }

/* ── DATA CONTEXT ──────────────────────────── */
.ctx-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--r-lg); margin-bottom: 16px; overflow: hidden;
}
.ctx-hdr {
  display: flex; align-items: center; justify-content: space-between;
  padding: 11px 18px; cursor: pointer; user-select: none;
  transition: background .1s;
}
.ctx-hdr:hover { background: var(--surface2); }
.ctx-hdr.open  { border-bottom: 1px solid var(--border); }
.ctx-hdr-l { display: flex; align-items: center; gap: 9px; }
.ctx-pill {
  font-size: 10px; font-weight: 600; padding: 2px 8px; border-radius: 20px;
  background: var(--purple-dim); color: var(--purple); border: 1px solid rgba(124,92,252,.2);
}
.ctx-title { font-size: 13px; font-weight: 600; color: var(--text); }
.ctx-hint  { font-size: 11px; color: var(--text3); }
.ctx-arrow { font-size: 10px; color: var(--text3); transition: transform .2s; }
.ctx-arrow.open { transform: rotate(180deg); }

.ctx-body { display: none; padding: 16px 18px; }
.ctx-body.open { display: block; }
.ctx-cols { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }

.ctx-col-title {
  font-size: 10px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .08em; margin-bottom: 8px;
}
.ctx-col-title.g { color: #16a34a; }
.ctx-col-title.r { color: var(--red); }
.ctx-col-title.p { color: var(--purple); }

.ctx-row {
  display: flex; align-items: flex-start; gap: 7px;
  font-size: 11px; color: var(--text2); padding: 3px 0; line-height: 1.4;
}
.cd { width: 5px; height: 5px; border-radius: 50%; margin-top: 4px; flex-shrink: 0; }
.cd-g { background: var(--green); }
.cd-r { background: var(--red); }

.hypo-wrap { display: flex; flex-wrap: wrap; gap: 5px; }
.hypo-btn {
  font-size: 10px; font-weight: 500; color: var(--purple);
  background: var(--purple-bg); border: 1px solid rgba(124,92,252,.2);
  border-radius: var(--r-sm); padding: 4px 10px;
  cursor: pointer; transition: all .12s; font-family: var(--sans);
}
.hypo-btn:hover { background: var(--purple-dim); border-color: var(--purple); }

/* ── FILTER BAR ────────────────────────────── */
.filter-bar {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--r-lg); padding: 10px 16px;
  margin-bottom: 16px;
  display: flex; align-items: center; gap: 4px; flex-wrap: nowrap;
}
.filter-lbl {
  font-size: 10px; font-weight: 700; text-transform: uppercase;
  letter-spacing: .08em; color: var(--text3); white-space: nowrap;
  padding-right: 8px;
}
.filter-sep { width: 1px; height: 22px; background: var(--border); margin: 0 6px; flex-shrink: 0; }

/* Override Streamlit widgets inside filter bar */
.filter-bar .stSelectbox > div > div {
  background: var(--surface2) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important;
  font-size: 12px !important; color: var(--text) !important;
  min-height: 30px !important; box-shadow: none !important;
  font-family: var(--sans) !important; padding: 0 8px !important;
}
.filter-bar .stSelectbox svg { color: var(--text3) !important; }
.filter-bar [data-testid="stSliderThumb"] { background: var(--purple) !important; }
.filter-bar [data-testid="stSlider"] { padding-top: 12px !important; }

.filter-bar .stButton > button {
  background: var(--surface2) !important; border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important; color: var(--text3) !important;
  font-size: 11px !important; padding: 5px 10px !important;
  white-space: nowrap !important; box-shadow: none !important;
  font-family: var(--sans) !important; line-height: 1 !important;
}
.filter-bar .stButton > button:hover {
  border-color: var(--red) !important; color: var(--red) !important;
  background: var(--red-bg) !important;
}

/* ── DOWNLOAD STRIP ────────────────────────── */
.dl-strip { display: flex; gap: 6px; margin-bottom: 14px; }
.dl-strip .stDownloadButton > button {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important; color: var(--text3) !important;
  font-size: 11px !important; padding: 4px 12px !important; box-shadow: none !important;
}
.dl-strip .stDownloadButton > button:hover {
  border-color: var(--purple) !important; color: var(--purple) !important;
  background: var(--purple-bg) !important;
}

/* ── CHAT PANEL ────────────────────────────── */
.chat-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--r-lg); overflow: hidden;
}
.chat-hdr {
  padding: 12px 18px; border-bottom: 1px solid var(--border);
  display: flex; align-items: center; justify-content: space-between;
}
.chat-title { font-size: 13px; font-weight: 600; color: var(--text); }
.chat-sub   { font-size: 11px; color: var(--text3); }

.sug-row {
  display: flex; flex-wrap: wrap; gap: 6px;
  padding: 12px 18px; border-bottom: 1px solid var(--border);
}
.sug-row .stButton > button {
  background: var(--surface2) !important; border: 1px solid var(--border) !important;
  border-radius: 20px !important; color: var(--text2) !important;
  font-size: 11px !important; padding: 5px 13px !important;
  white-space: nowrap !important; box-shadow: none !important;
  font-weight: 500 !important; font-family: var(--sans) !important;
}
.sug-row .stButton > button:hover {
  border-color: var(--purple) !important; color: var(--purple) !important;
  background: var(--purple-bg) !important;
}

.msgs {
  padding: 12px 18px;
  display: flex; flex-direction: column; gap: 10px;
  min-height: 20px;
}
.msg-u {
  align-self: flex-end;
  background: var(--text); color: #f7f8fa;
  border-radius: 10px 10px 3px 10px;
  padding: 9px 14px; font-size: 13px; line-height: 1.5;
  max-width: min(500px,85%); word-wrap: break-word;
}
.msg-a-row { display: flex; gap: 8px; align-items: flex-start; }
.msg-av {
  width: 22px; height: 22px; border-radius: 6px; flex-shrink: 0;
  background: var(--purple); display: flex; align-items: center;
  justify-content: center; font-size: 10px; margin-top: 1px;
  box-shadow: 0 2px 6px rgba(124,92,252,.35); color: #fff;
}
.msg-a {
  background: var(--surface2); border: 1px solid var(--border);
  border-radius: 3px 10px 10px 10px;
  padding: 9px 14px; font-size: 13px; line-height: 1.6; color: var(--text);
  flex: 1; min-width: 0;
}
.tpills { display: flex; gap: 4px; margin-bottom: 6px; }
.pill {
  font-family: var(--mono); font-size: 9px; font-weight: 500;
  padding: 2px 6px; border-radius: 4px; letter-spacing: .03em;
}
.p-sql { background: var(--blue-bg); color: var(--blue); border: 1px solid rgba(59,130,246,.2); }
.p-rag { background: var(--purple-bg); color: var(--purple); border: 1px solid rgba(124,92,252,.2); }

.msg-a table { width:100%; border-collapse:collapse; font-size:12px; margin-top:8px; }
.msg-a thead tr { background: var(--bg); }
.msg-a th { text-align:left; padding:6px 10px; font-size:10px; font-weight:600; color:var(--text3); text-transform:uppercase; letter-spacing:.06em; border-bottom:1px solid var(--border); }
.msg-a td { padding:6px 10px; border-bottom:1px solid var(--surface2); }
.msg-a tr:last-child td { border-bottom:none; }

.thinking { display:flex; align-items:center; gap:6px; color:var(--text3); font-size:12px; padding:4px 0; }
.dot { width:4px; height:4px; border-radius:50%; background:var(--border2); animation:bd 1.3s ease-in-out infinite; }
.dot:nth-child(2){animation-delay:.15s} .dot:nth-child(3){animation-delay:.3s}
@keyframes bd { 0%,80%,100%{opacity:.2;transform:scale(.7)} 40%{opacity:1;transform:scale(1)} }

/* ── FIXED CHAT INPUT ──────────────────────── */
.stChatInputContainer {
  background: var(--surface) !important;
  border-top: 1px solid var(--border) !important;
  padding: 10px 24px 12px !important;
  box-shadow: 0 -4px 24px rgba(0,0,0,.06) !important;
}
textarea[data-testid="stChatInputTextArea"] {
  background: var(--bg) !important; border: 1px solid var(--border2) !important;
  border-radius: var(--r) !important; color: var(--text) !important;
  font-family: var(--sans) !important; font-size: 13px !important;
  padding: 10px 14px !important;
}
textarea[data-testid="stChatInputTextArea"]:focus {
  border-color: var(--purple) !important;
  box-shadow: 0 0 0 3px rgba(124,92,252,.1) !important; outline:none !important;
}
[data-testid="stChatInputSubmitButton"] > button {
  background: var(--purple) !important; border: none !important;
  color: #fff !important; border-radius: 7px !important;
}

.bottom-pad { height: 80px; }
::-webkit-scrollbar { width:4px; }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius:2px; }
</style>
""", unsafe_allow_html=True)


# ─── STATE ──────────────────────────────────────────────────────────────────
if "history"  not in st.session_state: st.session_state.history  = []
if "messages" not in st.session_state: st.session_state.messages = []
if "last_df"  not in st.session_state: st.session_state.last_df  = None
if "filters"  not in st.session_state: st.session_state.filters  = {}

REQUIRED = ["OPENAI_API_KEY", "QDRANT_URL", "QDRANT_API_KEY"]
missing  = [v for v in REQUIRED if not os.environ.get(v)]


# ─── HELPERS ────────────────────────────────────────────────────────────────
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


# ─── SIDEBAR (NATIVE) ────────────────────────────────────────────────────────
with st.sidebar:
    connected = not missing
    st.markdown(f"""
    <div class="sb-logo">
      <div class="sb-logo-icon">✉</div>
      <div class="sb-logo-text">Email <em>Intelligence</em></div>
    </div>

    <div class="sb-section">
      <div class="sb-section-lbl">Analytics</div>
      <div class="sb-item active"><span class="sb-item-icon">▣</span> Overview</div>
      <div class="sb-item"><span class="sb-item-icon">🪝</span> Hook Analysis</div>
      <div class="sb-item"><span class="sb-item-icon">🎭</span> Tone Performance</div>
      <div class="sb-item"><span class="sb-item-icon">🌍</span> By Language</div>
    </div>

    <div class="sb-divider"></div>

    <div class="sb-section">
      <div class="sb-section-lbl">Tools</div>
      <div class="sb-item"><span class="sb-item-icon">✦</span> AI Assistant</div>
      <div class="sb-item"><span class="sb-item-icon">↓</span> Export</div>
    </div>

    <div class="sb-divider"></div>

    <div class="sb-section">
      <div class="sb-section-lbl">Data source</div>
    </div>
    <div class="sb-schema">
      <div class="sb-schema-title">Mailchimp → BigQuery</div>
      <div class="sb-schema-row"><div class="sb-dot d-green"></div>Open rate · CTR · Hook · Tone</div>
      <div class="sb-schema-row"><div class="sb-dot d-green"></div>Subject line · Language · Date</div>
      <div class="sb-schema-row"><div class="sb-dot d-red"></div>No revenue / affiliate data</div>
      <div class="sb-schema-row"><div class="sb-dot d-red"></div>No unsubscribes / bounces</div>
    </div>

    <div class="sb-divider"></div>

    <div class="sb-status">
      <div class="sb-live"></div>
      <span>{"Connected · Live" if connected else "Setup needed"}</span>
    </div>
    """, unsafe_allow_html=True)


# ─── MAIN AREA ───────────────────────────────────────────────────────────────

# Top bar
st.markdown("""
<div class="ei-topbar">
  <div class="tb-crumb">
    <span>Email Intelligence</span>
    <span style="color:var(--border2);margin:0 2px">›</span>
    <strong>Overview</strong>
  </div>
  <div class="tb-tags">
    <span class="tb-tag tag-src">Mailchimp · BigQuery</span>
    <span class="tb-tag tag-live">● Live</span>
  </div>
</div>
<div class="ei-body">
""", unsafe_allow_html=True)

# ── KPIs ──────────────────────────────────────────────────────────────────────
stat_vals = ["—", "—", "—", "—"]
if not missing:
    parsed = parse_md_table(load_stats())
    if parsed:
        stat_vals = list(parsed[0].values())

KPI = [
    ("Campaigns",     "",  "All time",           "b-neu"),
    ("Avg Open Rate", "%", "↑ vs 21% industry",  "b-up"),
    ("Avg CTR",       "%", "↑ vs 2.6% industry", "b-up"),
    ("Hook Types",    "",  "GPT classified",      "b-info"),
]
kpi_html = '<div class="kpi-grid">'
for i, (lbl, sfx, hint, bc) in enumerate(KPI):
    v = stat_vals[i] if i < len(stat_vals) else "—"
    kpi_html += f'<div class="kpi-card"><div class="kpi-lbl">{lbl}</div><div class="kpi-val">{v}{sfx}</div><span class="kpi-badge {bc}">{hint}</span></div>'
kpi_html += '</div>'
st.markdown(kpi_html, unsafe_allow_html=True)

# ── CHARTS ────────────────────────────────────────────────────────────────────
if not missing:
    try:
        import plotly.graph_objects as go
        hook_data = parse_md_table(load_hook_data())
        tone_data = parse_md_table(load_tone_data())

        st.markdown('<div class="charts-grid">', unsafe_allow_html=True)
        c1, c2 = st.columns(2, gap="small")

        with c1:
            st.markdown('<div class="chart-card"><div class="chart-title">Open Rate by Hook Type</div><div class="chart-sub">avg % · sorted by performance</div>', unsafe_allow_html=True)
            if hook_data:
                hooks  = [d["hook_type"].replace("-", " ").title() for d in hook_data]
                opens  = [float(d["avg_open"]) for d in hook_data]
                counts = [int(d["campaigns"]) for d in hook_data]
                mx = max(opens)
                fig = go.Figure(go.Bar(
                    x=opens, y=hooks, orientation="h",
                    marker_color=["#7c5cfc" if o == mx else "#c4b5fd" for o in opens],
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
                st.markdown('<div class="chart-empty">Enrichment in progress…</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        with c2:
            st.markdown('<div class="chart-card"><div class="chart-title">Tone Distribution</div><div class="chart-sub">campaigns by communication style</div>', unsafe_allow_html=True)
            if tone_data:
                tones  = [d["tone"].title() for d in tone_data]
                counts = [int(d["campaigns"]) for d in tone_data]
                PAL = ["#7c5cfc","#a78bfa","#c4b5fd","#ddd6fe","#6d28d9","#8b5cf6","#4c1d95","#7c3aed"]
                fig2 = go.Figure(go.Pie(
                    labels=tones, values=counts, hole=0.58,
                    marker_colors=PAL[:len(tones)],
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
                st.markdown('<div class="chart-empty">Enrichment in progress…</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)  # /charts-grid

    except ImportError:
        st.markdown('<div style="padding:12px 16px;background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;font-size:12px;color:#c2410c;margin-bottom:16px;">⚠ Charts need plotly — <code>pip install plotly</code></div>', unsafe_allow_html=True)

# ── FILTER BAR ────────────────────────────────────────────────────────────────
st.markdown('<div class="filter-bar">', unsafe_allow_html=True)
st.markdown('<span class="filter-lbl">Filters</span>', unsafe_allow_html=True)

cols = st.columns([1.1, 1.1, 1.1, 1.1, 1.4, 0.8])
with cols[0]:
    model = st.selectbox("Model", ["gpt-4o-mini", "gpt-4o"], index=0,
                         label_visibility="collapsed", key="mdl")
    os.environ["AGENT_MODEL"] = model
with cols[1]:
    fh = st.selectbox("Hook", ["Any hook","curiosity","urgency","social-proof",
                                "fear-of-missing-out","story","discount","question"],
                      label_visibility="collapsed", key="fh")
with cols[2]:
    ft = st.selectbox("Tone", ["Any tone","casual","formal","playful",
                                "urgent","inspirational","informational"],
                      label_visibility="collapsed", key="ft")
with cols[3]:
    fl = st.selectbox("Lang", ["Any lang","en","lt","ru","es","pl"],
                      label_visibility="collapsed", key="fl")
with cols[4]:
    mo = st.slider("Min open %", 0, 100, 0, label_visibility="collapsed", key="mo")
with cols[5]:
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
    st.markdown('<div class="dl-strip">', unsafe_allow_html=True)
    dl = st.columns([0.6, 0.6, 8])
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


# ── DATA CONTEXT ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="ctx-card">
  <div class="ctx-hdr" id="ctx-hdr" onclick="toggleCtx()">
    <div class="ctx-hdr-l">
      <span class="ctx-pill">Data schema</span>
      <span class="ctx-title">What's in this dataset?</span>
      <span class="ctx-hint" style="margin-left:8px">Mailchimp → BigQuery · no revenue data</span>
    </div>
    <span class="ctx-arrow" id="ctx-arr">▾</span>
  </div>
  <div class="ctx-body" id="ctx-body">
    <div class="ctx-cols">
      <div>
        <div class="ctx-col-title g">✓ Available</div>
        <div class="ctx-row"><div class="cd cd-g"></div>1,652 email campaigns from Mailchimp</div>
        <div class="ctx-row"><div class="cd cd-g"></div>Open rate &amp; CTR per campaign</div>
        <div class="ctx-row"><div class="cd cd-g"></div>Hook type — GPT-classified (7 types)</div>
        <div class="ctx-row"><div class="cd cd-g"></div>Tone (Casual, Informational, Playful…)</div>
        <div class="ctx-row"><div class="cd cd-g"></div>Language (EN, RU, LT, ES, PL)</div>
        <div class="ctx-row"><div class="cd cd-g"></div>Subject line text &amp; preview</div>
        <div class="ctx-row"><div class="cd cd-g"></div>Send date &amp; campaign name</div>
      </div>
      <div>
        <div class="ctx-col-title r">✗ Not available</div>
        <div class="ctx-row"><div class="cd cd-r"></div>Revenue / GMV — connect affiliate data separately</div>
        <div class="ctx-row"><div class="cd cd-r"></div>Unsubscribe &amp; bounce rates</div>
        <div class="ctx-row"><div class="cd cd-r"></div>Individual recipient behaviour</div>
        <div class="ctx-row"><div class="cd cd-r"></div>A/B test variants</div>
        <div class="ctx-row"><div class="cd cd-r"></div>Audience segment breakdown</div>
        <div class="ctx-row"><div class="cd cd-r"></div>Send-time optimisation data</div>
      </div>
      <div>
        <div class="ctx-col-title p">⚡ Hypotheses to explore</div>
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
  var b = document.getElementById('ctx-body');
  var a = document.getElementById('ctx-arr');
  var h = document.getElementById('ctx-hdr');
  var open = b.classList.contains('open');
  b.classList.toggle('open', !open);
  a.classList.toggle('open', !open);
  h.classList.toggle('open', !open);
}
function sendHypo(text) {
  var el = window.parent.document.querySelector('textarea[data-testid="stChatInputTextArea"]');
  if (el) {
    var s = Object.getOwnPropertyDescriptor(window.parent.HTMLTextAreaElement.prototype,'value').set;
    s.call(el, text);
    el.dispatchEvent(new Event('input',{bubbles:true}));
    el.focus();
  }
}
</script>
""", unsafe_allow_html=True)


# ── CHAT ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chat-card">
  <div class="chat-hdr">
    <div class="chat-title">AI Assistant</div>
    <div class="chat-sub">Ask anything about your campaigns</div>
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
    st.markdown('<div class="sug-row">', unsafe_allow_html=True)
    sug_cols = st.columns(6)
    for i, (icon, label, full_q) in enumerate(SUGGESTIONS):
        with sug_cols[i % 6]:
            if st.button(f"{icon} {label}", key=f"c{i}"):
                st.session_state.pending_question = full_q
                st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="msgs">', unsafe_allow_html=True)
for msg in st.session_state.messages:
    if msg["role"] == "user":
        st.markdown(f'<div class="msg-u">{msg["content"]}</div>', unsafe_allow_html=True)
    else:
        content = msg["content"]
        pills = ""
        if any(k in content.lower() for k in ["open rate","%","avg","count","ctr","campaigns"]):
            pills += '<span class="pill p-sql">SQL</span>'
        if any(k in content.lower() for k in ["similar","score:","preview:"]):
            pills += '<span class="pill p-rag">RAG</span>'
        ph = f'<div class="tpills">{pills}</div>' if pills else ""
        st.markdown(f'<div class="msg-a-row"><div class="msg-av">✦</div><div class="msg-a">{ph}', unsafe_allow_html=True)
        st.markdown(content)
        st.markdown('</div></div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)  # /msgs
st.markdown('</div>', unsafe_allow_html=True)  # /chat-card

st.markdown('</div>', unsafe_allow_html=True)  # /ei-body
st.markdown('<div class="bottom-pad"></div>', unsafe_allow_html=True)


# ─── AGENT ───────────────────────────────────────────────────────────────────
def extract_df(reply):
    try:
        import pandas as pd
        rows = parse_md_table(reply)
        if rows: return pd.DataFrame(rows)
    except: pass
    return None

def run_question(question: str):
    st.session_state.messages.append({"role":"user","content":question})
    st.markdown(f'<div class="msg-u">{question}</div>', unsafe_allow_html=True)
    ph = st.empty()
    ph.markdown('<div class="thinking"><div class="dot"></div><div class="dot"></div><div class="dot"></div><span>Analysing…</span></div>', unsafe_allow_html=True)
    try:
        from agent import run_agent
        aug = question
        if st.session_state.filters:
            aug = f"{question}\n[Filters: {', '.join(f'{k}={v}' for k,v in st.session_state.filters.items())}]"
        reply, hist = run_agent(aug, st.session_state.history)
        st.session_state.history = hist
        ph.empty()
        df = extract_df(reply)
        if df is not None: st.session_state.last_df = df
        pills = ""
        if any(k in reply.lower() for k in ["open rate","%","avg","count","ctr","campaigns"]):
            pills += '<span class="pill p-sql">SQL</span>'
        if any(k in reply.lower() for k in ["similar","score:","preview:"]):
            pills += '<span class="pill p-rag">RAG</span>'
        ph2 = f'<div class="tpills">{pills}</div>' if pills else ""
        st.markdown(f'<div class="msg-a-row"><div class="msg-av">✦</div><div class="msg-a">{ph2}', unsafe_allow_html=True)
        st.markdown(reply)
        st.markdown('</div></div>', unsafe_allow_html=True)
        st.session_state.messages.append({"role":"assistant","content":reply})
    except Exception as e:
        ph.empty()
        err = f"**Error:** {e}"
        st.markdown(f'<div class="msg-a-row"><div class="msg-av">✦</div><div class="msg-a" style="color:var(--red)">{err}</div></div>', unsafe_allow_html=True)
        st.session_state.messages.append({"role":"assistant","content":err})

if "pending_question" in st.session_state:
    q = st.session_state.pop("pending_question")
    run_question(q)
    st.rerun()

if prompt := st.chat_input("Ask about your campaigns…"):
    run_question(prompt)
