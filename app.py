"""
Email Intelligence — Pro Dashboard
CMO/CRO-grade email marketing analytics for multi-country Mailchimp campaigns.
Single-file Streamlit app. Stateless agent, BigQuery + Qdrant RAG.
"""

import io
import logging
import os
import streamlit as st

logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="Email Intelligence",
    page_icon="✦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ═══════════════════════════════════════════════════════════════════════════
# GLOBAL CSS — Dark editorial theme, sharp typography
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=DM+Mono:wght@400;500&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:        #0e0f12;
  --bg2:       #13151a;
  --bg3:       #1a1d25;
  --surface:   #1e2130;
  --surface2:  #252a3a;
  --border:    rgba(255,255,255,.07);
  --border2:   rgba(255,255,255,.13);
  --text:      #eceef2;
  --text2:     #8b90a0;
  --text3:     #555c70;
  --green:     #00d97e;
  --green2:    #00b869;
  --green-dim: rgba(0,217,126,.12);
  --green-glow:rgba(0,217,126,.25);
  --amber:     #f59e0b;
  --amber-dim: rgba(245,158,11,.12);
  --red:       #f87171;
  --red-dim:   rgba(248,113,113,.12);
  --blue:      #60a5fa;
  --blue-dim:  rgba(96,165,250,.12);
  --purple:    #a78bfa;
  --purple-dim:rgba(167,139,250,.12);
  --sans:      'DM Sans', system-ui, sans-serif;
  --serif:     'DM Serif Display', Georgia, serif;
  --mono:      'DM Mono', 'Courier New', monospace;
  --r:         10px;
  --r-sm:      7px;
  --r-lg:      14px;
}

/* ── Streamlit chrome removal ── */
#MainMenu, footer, header,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"],
.stDeployButton { display: none !important; }

.stApp { background: var(--bg) !important; font-family: var(--sans) !important; color: var(--text) !important; }
.main .block-container { padding: 0 !important; max-width: 100% !important; }
[data-testid="stVerticalBlock"],
[data-testid="stVerticalBlockBorderWrapper"] { gap: 0 !important; padding: 0 !important; }
[data-testid="element-container"] { margin: 0 !important; }
.stSelectbox label, .stSlider label, .stTextInput label { display: none !important; }

/* ── NAV BAR ── */
.ei-nav {
  position: sticky; top: 0; z-index: 300;
  background: rgba(14,15,18,.92);
  backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border);
  display: flex; align-items: center; justify-content: space-between;
  padding: 0 20px; height: 52px;
}
.ei-brand { display: flex; align-items: center; gap: 12px; }
.ei-logo {
  width: 28px; height: 28px;
  background: linear-gradient(135deg, var(--green), var(--green2));
  border-radius: 7px; display: flex; align-items: center; justify-content: center;
  font-size: 13px; box-shadow: 0 0 16px var(--green-glow);
}
.ei-name { font-family: var(--serif) !important; font-size: 16px; color: var(--text); letter-spacing: .01em; }
.ei-name em { color: var(--green); font-style: italic; }
.ei-nav-right { display: flex; align-items: center; gap: 14px; }
.ei-badge {
  display: flex; align-items: center; gap: 5px;
  font-size: 11px; font-weight: 500; padding: 4px 10px;
  border-radius: 20px; letter-spacing: .02em;
}
.b-live { background: rgba(0,217,126,.15); color: var(--green); border: 1px solid rgba(0,217,126,.3); }
.b-warn { background: var(--red-dim); color: var(--red); border: 1px solid rgba(248,113,113,.3); }
.sdot { width: 5px; height: 5px; border-radius: 50%; background: currentColor; animation: blink 2s ease-in-out infinite; }
@keyframes blink { 0%,100%{opacity:.9} 50%{opacity:.3} }
.ei-refresh {
  font-size: 11px; color: var(--text3); cursor: pointer; padding: 4px 10px;
  border: 1px solid var(--border); border-radius: 6px;
  transition: all .15s;
}
.ei-refresh:hover { color: var(--text2); border-color: var(--border2); }

/* ── SECTION HEADERS ── */
.section-hd {
  display: flex; align-items: baseline; justify-content: space-between;
  padding: 18px 20px 10px;
}
.section-title { font-family: var(--serif) !important; font-size: 18px; color: var(--text); font-style: italic; }
.section-sub { font-size: 11px; color: var(--text3); }

/* ── KPI STRIP ── */
.kpi-strip {
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  gap: 10px; padding: 12px 20px 0;
}
@media(max-width:900px){ .kpi-strip { grid-template-columns: repeat(3,1fr); } }
@media(max-width:500px){ .kpi-strip { grid-template-columns: repeat(2,1fr); } }

.kpi {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--r); padding: 14px 16px 12px;
  position: relative; overflow: hidden;
  transition: border-color .2s, box-shadow .2s;
}
.kpi:hover { border-color: var(--border2); box-shadow: 0 4px 24px rgba(0,0,0,.3); }
.kpi::before {
  content:''; position:absolute; bottom:0; left:0; right:0; height:2px;
  background: var(--kc, var(--green)); opacity:.6;
}
.kpi-l { font-size: 9px; font-weight: 600; color: var(--text3); text-transform: uppercase; letter-spacing: .1em; margin-bottom: 6px; }
.kpi-v { font-family: var(--serif) !important; font-size: 28px; color: var(--text); line-height: 1; letter-spacing: -.02em; margin-bottom: 6px; font-style: italic; }
.kpi-tag {
  display: inline-flex; align-items: center; gap: 3px;
  font-size: 10px; font-weight: 500; padding: 2px 8px;
  border-radius: 20px; letter-spacing: .01em;
}
.tg { background: var(--green-dim); color: var(--green); }
.ta { background: var(--amber-dim); color: var(--amber); }
.tb { background: var(--blue-dim); color: var(--blue); }
.tp { background: var(--purple-dim); color: var(--purple); }
.tr { background: var(--red-dim); color: var(--red); }

/* ── CHART GRID ── */
.charts-2 {
  display: grid; grid-template-columns: 1fr 1fr;
  gap: 10px; padding: 10px 20px 0;
}
.charts-3 {
  display: grid; grid-template-columns: 1fr 1fr 1fr;
  gap: 10px; padding: 10px 20px 0;
}
.charts-wide {
  padding: 10px 20px 0;
}
@media(max-width:800px){
  .charts-2, .charts-3 { grid-template-columns: 1fr; }
}
.chart-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--r-lg); padding: 14px 16px 10px;
  overflow: hidden;
}
.chart-card:hover { border-color: var(--border2); }
.ct { font-size: 12px; font-weight: 600; color: var(--text); margin-bottom: 2px; }
.cs { font-size: 10px; color: var(--text3); margin-bottom: 10px; }
.empty-chart { color: var(--text3); font-size: 12px; padding: 24px 0; text-align: center; }

/* ── INSIGHT CARDS ── */
.insights-row {
  display: grid; grid-template-columns: repeat(3, 1fr);
  gap: 10px; padding: 10px 20px 0;
}
@media(max-width:800px){ .insights-row { grid-template-columns: 1fr; } }
.insight-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--r-lg); padding: 14px 16px;
}
.ins-icon { font-size: 16px; margin-bottom: 8px; }
.ins-title { font-size: 11px; font-weight: 600; color: var(--text3); text-transform: uppercase; letter-spacing: .08em; margin-bottom: 4px; }
.ins-value { font-family: var(--serif) !important; font-size: 22px; color: var(--text); font-style: italic; line-height: 1.1; margin-bottom: 4px; }
.ins-sub { font-size: 11px; color: var(--text3); line-height: 1.4; }

/* ── DIVIDER ── */
.ei-div { border:none; border-top: 1px solid var(--border); margin: 14px 20px 0; }

/* ── FILTER STRIP ── */
.filter-strip {
  padding: 10px 20px 0;
  display: flex; align-items: flex-end; gap: 10px; flex-wrap: wrap;
}
.f-group { display: flex; flex-direction: column; gap: 4px; }
.f-lbl { font-size: 9px; font-weight: 600; color: var(--text3); text-transform: uppercase; letter-spacing: .1em; }

.filter-strip .stSelectbox { width: 130px !important; }
.filter-strip .stSelectbox > div > div {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important; font-size: 12px !important;
  color: var(--text) !important; min-height: 30px !important; box-shadow: none !important;
}
.filter-strip .stSelectbox > div > div:hover { border-color: var(--border2) !important; }
.filter-strip .stSlider { width: 110px !important; }

.filter-strip .stButton > button {
  background: transparent !important; border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important; color: var(--text3) !important;
  font-size: 11px !important; padding: 4px 12px !important;
  height: 30px !important; white-space: nowrap !important; box-shadow: none !important;
}
.filter-strip .stButton > button:hover {
  border-color: rgba(248,113,113,.4) !important; color: var(--red) !important;
}

/* ── DOWNLOAD STRIP ── */
.dl-strip { padding: 8px 20px 0; display: flex; gap: 6px; }
.dl-strip .stDownloadButton > button {
  background: transparent !important; border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important; color: var(--text3) !important;
  font-size: 10px !important; padding: 3px 10px !important; box-shadow: none !important;
}
.dl-strip .stDownloadButton > button:hover {
  border-color: var(--green) !important; color: var(--green) !important;
}

/* ── CHAT ── */
.chat-wrap { padding: 10px 20px 0; }
.chat-hd { display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 10px; }
.chat-title { font-family: var(--serif) !important; font-size: 17px; color: var(--text); font-style: italic; }
.chat-sub { font-size: 11px; color: var(--text3); }

/* Quick chips */
.chips { display: grid; grid-template-columns: repeat(3,1fr); gap: 6px; margin-bottom: 10px; }
@media(max-width:480px){ .chips{ grid-template-columns: repeat(2,1fr); } }
.chips .stButton > button {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-radius: var(--r-sm) !important; color: var(--text2) !important;
  font-size: 11px !important; padding: 9px 11px !important; line-height: 1.3 !important;
  text-align: left !important; width: 100% !important; height: auto !important;
  white-space: normal !important; box-shadow: none !important; font-weight: 400 !important;
}
.chips .stButton > button:hover {
  border-color: rgba(0,217,126,.35) !important; color: var(--green) !important;
  background: var(--green-dim) !important;
}

/* Messages */
.msgs { display: flex; flex-direction: column; gap: 10px; margin-bottom: 6px; }
.msg-u {
  align-self: flex-end; background: var(--surface2);
  color: var(--text); border: 1px solid var(--border);
  border-radius: var(--r) var(--r) 3px var(--r);
  padding: 10px 14px; font-size: 13px; line-height: 1.55;
  max-width: min(520px, 85%); word-wrap: break-word;
}
.msg-a-row { display: flex; gap: 10px; align-items: flex-start; }
.msg-av {
  width: 26px; height: 26px; flex-shrink: 0;
  background: linear-gradient(135deg, var(--green), var(--green2));
  border-radius: 7px; display: flex; align-items: center; justify-content: center;
  font-size: 12px; margin-top: 2px; box-shadow: 0 0 10px var(--green-glow);
}
.msg-a {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 3px var(--r) var(--r) var(--r);
  padding: 12px 16px; font-size: 13px; line-height: 1.65; color: var(--text);
  flex: 1; min-width: 0;
}
.tpills { display: flex; gap: 5px; margin-bottom: 7px; flex-wrap: wrap; }
.pill {
  font-family: var(--mono) !important; font-size: 9px; font-weight: 500;
  padding: 2px 7px; border-radius: 3px; letter-spacing: .04em;
}
.p-sql { background: var(--blue-dim); color: var(--blue); border: 1px solid rgba(96,165,250,.2); }
.p-rag { background: var(--green-dim); color: var(--green); border: 1px solid rgba(0,217,126,.2); }

/* Tables inside messages */
.msg-a table { width: 100%; border-collapse: collapse; font-size: 12px; margin-top: 8px; }
.msg-a thead tr { background: var(--bg2); }
.msg-a th { text-align: left; padding: 7px 10px; font-size: 9px; font-weight: 600; color: var(--text3); text-transform: uppercase; letter-spacing: .07em; border-bottom: 1px solid var(--border); }
.msg-a td { padding: 7px 10px; border-bottom: 1px solid var(--border); color: var(--text2); }
.msg-a tr:last-child td { border-bottom: none; }
.msg-a tr:hover td { background: var(--surface2); color: var(--text); }
.msg-a strong { color: var(--text); }

/* Thinking */
.thinking { display: flex; align-items: center; gap: 7px; color: var(--text3); font-size: 12px; padding: 4px 0; }
.dot { width: 4px; height: 4px; border-radius: 50%; background: var(--green); animation: bdot 1.3s ease-in-out infinite; }
.dot:nth-child(2){ animation-delay:.15s; } .dot:nth-child(3){ animation-delay:.3s; }
@keyframes bdot { 0%,80%,100%{opacity:.2;transform:scale(.7)} 40%{opacity:1;transform:scale(1)} }

/* Fixed input */
.stChatInputContainer {
  background: var(--bg2) !important; border-top: 1px solid var(--border) !important;
  padding: 10px 20px 14px !important; position: fixed !important;
  bottom: 0 !important; left: 0 !important; right: 0 !important;
  z-index: 200 !important; box-shadow: 0 -8px 30px rgba(0,0,0,.4) !important;
}
textarea[data-testid="stChatInputTextArea"] {
  background: var(--surface) !important; border: 1px solid var(--border2) !important;
  border-radius: var(--r) !important; color: var(--text) !important;
  font-family: var(--sans) !important; font-size: 14px !important;
  padding: 10px 14px !important; max-width: 800px !important;
  margin: 0 auto !important; display: block !important;
}
textarea[data-testid="stChatInputTextArea"]:focus {
  border-color: var(--green) !important;
  box-shadow: 0 0 0 3px rgba(0,217,126,.12) !important; outline: none !important;
}
[data-testid="stChatInputSubmitButton"] > button {
  background: var(--green) !important; border: none !important;
  color: #0e0f12 !important; border-radius: 8px !important; font-weight: 600 !important;
}

.bottom-pad { height: 80px; }

/* Plotly inside dark theme */
.js-plotly-plot .plotly { background: transparent !important; }

::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-thumb { background: var(--surface2); border-radius: 2px; }
::-webkit-scrollbar-track { background: transparent; }

/* Streamlit slider track */
[data-testid="stSlider"] [data-baseweb="slider"] [data-testid="stSliderThumb"] {
  background: var(--green) !important;
}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════════════════════
for k, v in [("history", []), ("messages", []), ("last_df", None), ("filters", {})]:
    if k not in st.session_state:
        st.session_state[k] = v

REQUIRED = ["OPENAI_API_KEY", "QDRANT_URL", "QDRANT_API_KEY"]
missing  = [v for v in REQUIRED if not os.environ.get(v)]


# ═══════════════════════════════════════════════════════════════════════════
# DATA HELPERS
# ═══════════════════════════════════════════════════════════════════════════
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

def safe_float(s, default=0.0):
    try: return float(str(s).replace("%","").strip())
    except: return default

def safe_int(s, default=0):
    try: return int(str(s).replace(",","").strip())
    except: return default


@st.cache_data(ttl=300, show_spinner=False)
def load_overview():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT
              COUNT(*) as total_campaigns,
              ROUND(AVG(k.open_rate_percent),1) as avg_open_rate,
              ROUND(AVG(k.ctr_percent),2) as avg_ctr,
              ROUND(AVG(k.unsub_rate_percent),3) as avg_unsub,
              COUNT(DISTINCT e.hook_type) as hook_types,
              COUNT(DISTINCT e.language) as languages
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
        """, max_rows=1)
    except: return None


@st.cache_data(ttl=300, show_spinner=False)
def load_hook_perf():
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
def load_tone_perf():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT e.tone, COUNT(*) as campaigns,
              ROUND(AVG(k.open_rate_percent),1) as avg_open,
              ROUND(AVG(k.ctr_percent),2) as avg_ctr
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
            WHERE e.tone IS NOT NULL AND e.tone != ''
            GROUP BY e.tone ORDER BY avg_open DESC
        """, max_rows=15)
    except: return None


@st.cache_data(ttl=300, show_spinner=False)
def load_geo_perf():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT e.geo, e.language, COUNT(*) as campaigns,
              ROUND(AVG(k.open_rate_percent),1) as avg_open,
              ROUND(AVG(k.ctr_percent),2) as avg_ctr
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
            WHERE e.geo IS NOT NULL AND e.geo != '' AND e.geo != 'global'
            GROUP BY e.geo, e.language
            ORDER BY avg_open DESC
            LIMIT 15
        """, max_rows=15)
    except: return None


@st.cache_data(ttl=300, show_spinner=False)
def load_send_time_perf():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT
              EXTRACT(HOUR FROM k.SendTime) as send_hour,
              COUNT(*) as campaigns,
              ROUND(AVG(k.open_rate_percent),1) as avg_open,
              ROUND(AVG(k.ctr_percent),2) as avg_ctr
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            WHERE k.SendTime IS NOT NULL
            GROUP BY send_hour ORDER BY send_hour
        """, max_rows=24)
    except: return None


@st.cache_data(ttl=300, show_spinner=False)
def load_offer_perf():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT e.offer_type, COUNT(*) as campaigns,
              ROUND(AVG(k.open_rate_percent),1) as avg_open,
              ROUND(AVG(k.ctr_percent),2) as avg_ctr,
              ROUND(AVG(k.unsub_rate_percent),3) as avg_unsub
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
            WHERE e.offer_type IS NOT NULL AND e.offer_type != ''
            GROUP BY e.offer_type ORDER BY avg_open DESC
        """, max_rows=12)
    except: return None


@st.cache_data(ttl=300, show_spinner=False)
def load_top_campaigns():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT
              k.SubjectLine as subject,
              e.hook_type as hook,
              e.tone,
              e.geo,
              e.language as lang,
              k.open_rate_percent as open_rate,
              k.ctr_percent as ctr,
              k.unsub_rate_percent as unsub,
              k.EmailsSent as sent
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
            WHERE k.SubjectLine IS NOT NULL AND k.open_rate_percent IS NOT NULL
            ORDER BY k.open_rate_percent DESC
            LIMIT 10
        """, max_rows=10)
    except: return None


@st.cache_data(ttl=300, show_spinner=False)
def load_language_trend():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT e.language, COUNT(*) as campaigns,
              ROUND(AVG(k.open_rate_percent),1) as avg_open,
              ROUND(AVG(k.ctr_percent),2) as avg_ctr,
              ROUND(AVG(k.unsub_rate_percent),3) as avg_unsub
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              ON k.campaign_id = e.campaign_id
            WHERE e.language IS NOT NULL AND e.language != ''
            GROUP BY e.language ORDER BY campaigns DESC
            LIMIT 10
        """, max_rows=10)
    except: return None


# ═══════════════════════════════════════════════════════════════════════════
# PLOTLY THEME
# ═══════════════════════════════════════════════════════════════════════════
PLOT_DEFAULTS = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans,sans-serif", size=11, color="#8b90a0"),
    margin=dict(l=0, r=40, t=4, b=0),
)
GRID_COLOR = "rgba(255,255,255,.05)"
GREEN_PALETTE = ["#00d97e","#00b869","#33e8a0","#006640","#66efc0","#003d26","#99f5da","#00ffaa"]
MULTI_PALETTE = ["#00d97e","#60a5fa","#f59e0b","#a78bfa","#f87171","#34d399","#fb923c","#38bdf8"]


# ═══════════════════════════════════════════════════════════════════════════
# NAV
# ═══════════════════════════════════════════════════════════════════════════
status_html = (
    '<div class="ei-badge b-live"><div class="sdot"></div>Live</div>'
    if not missing else
    '<div class="ei-badge b-warn"><div class="sdot"></div>Setup needed</div>'
)
st.markdown(f"""
<div class="ei-nav">
  <div class="ei-brand">
    <div class="ei-logo">✦</div>
    <div class="ei-name">Email <em>Intelligence</em></div>
  </div>
  <div class="ei-nav-right">
    <span class="ei-refresh" title="Cache refreshes every 5 min">⟳ Live data</span>
    {status_html}
  </div>
</div>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — KPI OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════
ov = parse_md_table(load_overview()) if not missing else []
ov = ov[0] if ov else {}

total     = safe_int(ov.get("total_campaigns","—"))
avg_open  = safe_float(ov.get("avg_open_rate","—"))
avg_ctr   = safe_float(ov.get("avg_ctr","—"))
avg_unsub = safe_float(ov.get("avg_unsub","—"))
hook_types = safe_int(ov.get("hook_types","—"))
languages = safe_int(ov.get("languages","—"))

# Benchmark comparisons (industry averages)
IND_OPEN, IND_CTR, IND_UNSUB = 21.0, 2.6, 0.10

open_delta = avg_open - IND_OPEN
ctr_delta  = avg_ctr  - IND_CTR

def fmt_v(v, suffix=""):
    return f"{v}{suffix}" if v not in (0, 0.0) else "—"

KPI_DEFS = [
    ("Campaigns",      str(total) if total else "—", "All time",                    "ta", "#f59e0b"),
    ("Avg Open Rate",  fmt_v(avg_open,"%"), f"{'↑' if open_delta>=0 else '↓'} {abs(open_delta):.1f}pp vs industry", "tg" if open_delta>=0 else "tr", "#00d97e" if open_delta>=0 else "#f87171"),
    ("Avg CTR",        fmt_v(avg_ctr,"%"),  f"{'↑' if ctr_delta>=0 else '↓'} {abs(ctr_delta):.2f}pp vs industry",  "tg" if ctr_delta>=0 else "tr",  "#00d97e" if ctr_delta>=0 else "#f87171"),
    ("Avg Unsub",      fmt_v(avg_unsub,"%"), "Rate — lower is better",              "tg" if avg_unsub <= IND_UNSUB else "tr", "#00d97e"),
    ("Hook Types",     str(hook_types) if hook_types else "—", "GPT-classified angles", "tb", "#60a5fa"),
    ("Languages",      str(languages) if languages else "—",  "Geo coverage",           "tp", "#a78bfa"),
]

kpi_html = '<div class="kpi-strip">'
for lbl, val, hint, tcls, color in KPI_DEFS:
    kpi_html += f'''<div class="kpi" style="--kc:{color}">
      <div class="kpi-l">{lbl}</div>
      <div class="kpi-v">{val}</div>
      <span class="kpi-tag {tcls}">{hint}</span>
    </div>'''
kpi_html += "</div>"
st.markdown(kpi_html, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — HOOK × TONE PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════
if not missing:
    try:
        import plotly.graph_objects as go

        hook_data = parse_md_table(load_hook_perf())
        tone_data = parse_md_table(load_tone_perf())

        st.markdown('<div class="charts-2">', unsafe_allow_html=True)
        c1, c2 = st.columns(2, gap="small")

        with c1:
            st.markdown('<div class="chart-card"><div class="ct">Open Rate by Hook Type</div><div class="cs">avg % · best performers highlighted</div>', unsafe_allow_html=True)
            if hook_data:
                hooks  = [d["hook_type"].replace("-"," ").title() for d in hook_data]
                opens  = [safe_float(d["avg_open"]) for d in hook_data]
                ctrs   = [safe_float(d["avg_ctr"]) for d in hook_data]
                counts = [safe_int(d["campaigns"]) for d in hook_data]
                mx = max(opens) if opens else 1
                colors = ["#00d97e" if o == mx else ("#00b869" if o >= mx*0.85 else "#2a3a30") for o in opens]

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=opens, y=hooks, orientation="h",
                    marker_color=colors, marker_line_width=0,
                    text=[f"{o}%" for o in opens], textposition="outside",
                    textfont=dict(size=10, color="#8b90a0"),
                    customdata=list(zip(counts, ctrs)),
                    hovertemplate="<b>%{y}</b><br>Open: %{x}% · CTR: %{customdata[1]}%<br>%{customdata[0]} campaigns<extra></extra>",
                ))
                fig.update_layout(
                    height=200, **PLOT_DEFAULTS,
                    xaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False,
                               showticklabels=False, range=[0, mx*1.35]),
                    yaxis=dict(showgrid=False, tickfont=dict(size=10)),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                st.markdown('<div class="empty-chart">Enrichment in progress…</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with c2:
            st.markdown('<div class="chart-card"><div class="ct">CTR vs Open Rate by Tone</div><div class="cs">bubble size = campaign count</div>', unsafe_allow_html=True)
            if tone_data:
                tones   = [d["tone"].title() for d in tone_data]
                t_opens = [safe_float(d["avg_open"]) for d in tone_data]
                t_ctrs  = [safe_float(d["avg_ctr"]) for d in tone_data]
                t_cnts  = [safe_int(d["campaigns"]) for d in tone_data]
                fig2 = go.Figure(go.Scatter(
                    x=t_opens, y=t_ctrs,
                    mode="markers+text",
                    marker=dict(
                        size=[max(12, min(42, c//4)) for c in t_cnts],
                        color=MULTI_PALETTE[:len(tones)],
                        opacity=0.85, line=dict(width=0),
                    ),
                    text=tones,
                    textposition="top center",
                    textfont=dict(size=9, color="#8b90a0"),
                    customdata=t_cnts,
                    hovertemplate="<b>%{text}</b><br>Open: %{x}% · CTR: %{y}%<br>%{customdata} campaigns<extra></extra>",
                ))
                fig2.update_layout(
                    height=200, **PLOT_DEFAULTS,
                    xaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False,
                               title=dict(text="Open Rate %", font=dict(size=10)), tickfont=dict(size=9)),
                    yaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False,
                               title=dict(text="CTR %", font=dict(size=10)), tickfont=dict(size=9)),
                    showlegend=False,
                )
                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
            else:
                st.markdown('<div class="empty-chart">Enrichment in progress…</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)  # /charts-2


        # ── ROW 2: Offer Performance + Send Hour Heatmap + Language Breakdown ──
        offer_data   = parse_md_table(load_offer_perf())
        send_data    = parse_md_table(load_send_time_perf())
        lang_data    = parse_md_table(load_language_trend())

        st.markdown('<div class="charts-3">', unsafe_allow_html=True)
        c3, c4, c5 = st.columns(3, gap="small")

        with c3:
            st.markdown('<div class="chart-card"><div class="ct">Offer Type Performance</div><div class="cs">open rate by offer strategy</div>', unsafe_allow_html=True)
            if offer_data:
                o_types = [d["offer_type"].replace("-"," ").title() for d in offer_data]
                o_opens = [safe_float(d["avg_open"]) for d in offer_data]
                o_ctrs  = [safe_float(d["avg_ctr"]) for d in offer_data]
                o_cnts  = [safe_int(d["campaigns"]) for d in offer_data]
                o_mx    = max(o_opens) if o_opens else 1
                fig3 = go.Figure(go.Bar(
                    x=o_opens, y=o_types, orientation="h",
                    marker_color=["#60a5fa" if o == o_mx else "#1e2f4a" for o in o_opens],
                    marker_line_width=0,
                    text=[f"{o}%" for o in o_opens], textposition="outside",
                    textfont=dict(size=9, color="#8b90a0"),
                    customdata=list(zip(o_cnts, o_ctrs)),
                    hovertemplate="<b>%{y}</b><br>Open: %{x}% · CTR: %{customdata[1]}%<br>%{customdata[0]} campaigns<extra></extra>",
                ))
                fig3.update_layout(
                    height=190, **PLOT_DEFAULTS,
                    margin=dict(l=0, r=44, t=4, b=0),
                    xaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False,
                               showticklabels=False, range=[0, o_mx*1.35]),
                    yaxis=dict(showgrid=False, tickfont=dict(size=9)),
                    showlegend=False,
                )
                st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})
            else:
                st.markdown('<div class="empty-chart">No offer data yet</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with c4:
            st.markdown('<div class="chart-card"><div class="ct">Best Send Hours</div><div class="cs">avg open rate by hour (UTC)</div>', unsafe_allow_html=True)
            if send_data:
                hours  = [safe_int(d["send_hour"]) for d in send_data]
                h_open = [safe_float(d["avg_open"]) for d in send_data]
                h_cnt  = [safe_int(d["campaigns"]) for d in send_data]
                h_mx   = max(h_open) if h_open else 1
                fig4 = go.Figure(go.Bar(
                    x=hours, y=h_open,
                    marker_color=["#00d97e" if o >= h_mx*0.9 else ("#f59e0b" if o >= h_mx*0.75 else "#2a2e3d") for o in h_open],
                    marker_line_width=0,
                    customdata=h_cnt,
                    hovertemplate="<b>%{x}:00 UTC</b><br>Open: %{y}%<br>%{customdata} campaigns<extra></extra>",
                ))
                fig4.update_layout(
                    height=190, **PLOT_DEFAULTS,
                    margin=dict(l=0, r=10, t=4, b=0),
                    xaxis=dict(showgrid=False, tickfont=dict(size=9),
                               tickvals=list(range(0, 24, 3)),
                               ticktext=[f"{h}:00" for h in range(0, 24, 3)]),
                    yaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False, tickfont=dict(size=9)),
                    showlegend=False,
                )
                st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})
            else:
                st.markdown('<div class="empty-chart">No timing data</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with c5:
            st.markdown('<div class="chart-card"><div class="ct">Performance by Language</div><div class="cs">open rate · dot size = volume</div>', unsafe_allow_html=True)
            if lang_data:
                langs   = [d["language"].upper() for d in lang_data]
                l_opens = [safe_float(d["avg_open"]) for d in lang_data]
                l_ctrs  = [safe_float(d["avg_ctr"]) for d in lang_data]
                l_cnts  = [safe_int(d["campaigns"]) for d in lang_data]
                fig5 = go.Figure(go.Scatter(
                    x=l_opens, y=l_ctrs,
                    mode="markers+text",
                    marker=dict(
                        size=[max(14, min(50, c//3)) for c in l_cnts],
                        color=GREEN_PALETTE[:len(langs)],
                        opacity=0.85, line=dict(width=0),
                    ),
                    text=langs, textposition="top center",
                    textfont=dict(size=9, color="#8b90a0"),
                    customdata=l_cnts,
                    hovertemplate="<b>%{text}</b><br>Open: %{x}% · CTR: %{y}%<br>%{customdata} campaigns<extra></extra>",
                ))
                fig5.update_layout(
                    height=190, **PLOT_DEFAULTS,
                    margin=dict(l=0, r=10, t=4, b=0),
                    xaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False,
                               title=dict(text="Open %", font=dict(size=9)), tickfont=dict(size=9)),
                    yaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False,
                               title=dict(text="CTR %", font=dict(size=9)), tickfont=dict(size=9)),
                    showlegend=False,
                )
                st.plotly_chart(fig5, use_container_width=True, config={"displayModeBar": False})
            else:
                st.markdown('<div class="empty-chart">No language data</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)  # /charts-3


        # ── ROW 3: Geo Map + Top 10 Campaigns Table ──
        geo_data = parse_md_table(load_geo_perf())
        top_data = parse_md_table(load_top_campaigns())

        st.markdown('<div class="charts-2">', unsafe_allow_html=True)
        c6, c7 = st.columns([1, 1.1], gap="small")

        with c6:
            st.markdown('<div class="chart-card"><div class="ct">Open Rate by Geography</div><div class="cs">avg open % per country/region</div>', unsafe_allow_html=True)
            if geo_data:
                # Choropleth-style scatter geo
                g_geos  = [d["geo"] for d in geo_data]
                g_opens = [safe_float(d["avg_open"]) for d in geo_data]
                g_ctrs  = [safe_float(d["avg_ctr"]) for d in geo_data]
                g_cnts  = [safe_int(d["campaigns"]) for d in geo_data]
                g_langs = [d.get("language","").upper() for d in geo_data]

                fig6 = go.Figure(go.Bar(
                    x=g_geos, y=g_opens,
                    marker=dict(
                        color=g_opens,
                        colorscale=[[0,"#1e2130"],[0.5,"#00b869"],[1,"#00d97e"]],
                        showscale=True,
                        colorbar=dict(thickness=8, outlinewidth=0, tickfont=dict(size=9, color="#555c70")),
                    ),
                    marker_line_width=0,
                    customdata=list(zip(g_cnts, g_ctrs, g_langs)),
                    hovertemplate="<b>%{x}</b> (%{customdata[2]})<br>Open: %{y}% · CTR: %{customdata[1]}%<br>%{customdata[0]} campaigns<extra></extra>",
                ))
                fig6.update_layout(
                    height=210, **PLOT_DEFAULTS,
                    margin=dict(l=0, r=60, t=4, b=0),
                    xaxis=dict(showgrid=False, tickfont=dict(size=9), tickangle=-30),
                    yaxis=dict(showgrid=True, gridcolor=GRID_COLOR, zeroline=False, tickfont=dict(size=9)),
                    showlegend=False,
                )
                st.plotly_chart(fig6, use_container_width=True, config={"displayModeBar": False})
            else:
                st.markdown('<div class="empty-chart">No geo data</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with c7:
            st.markdown('<div class="chart-card"><div class="ct">Top 10 Campaigns · Open Rate</div><div class="cs">ranked by open rate %</div>', unsafe_allow_html=True)
            if top_data:
                import pandas as pd
                df_top = pd.DataFrame(top_data)
                # Render compact table
                rows_html = ""
                for i, row in df_top.iterrows():
                    subj = str(row.get("subject",""))[:45] + ("…" if len(str(row.get("subject",""))) > 45 else "")
                    op   = str(row.get("open_rate",""))
                    ctr  = str(row.get("ctr",""))
                    hook = str(row.get("hook","")).replace("-"," ").title()
                    geo  = str(row.get("geo",""))
                    lang = str(row.get("lang","")).upper()
                    rows_html += f"""<tr>
                      <td style="color:var(--text);font-size:11px">{subj}</td>
                      <td style="color:var(--green);font-weight:500;font-size:11px;white-space:nowrap">{op}%</td>
                      <td style="color:var(--text2);font-size:10px;white-space:nowrap">{ctr}%</td>
                      <td style="color:var(--text3);font-size:10px">{hook}</td>
                      <td style="color:var(--text3);font-size:10px">{geo} {lang}</td>
                    </tr>"""

                table_html = f"""<table style="width:100%;border-collapse:collapse;margin-top:4px">
                  <thead>
                    <tr style="background:rgba(255,255,255,.03)">
                      <th style="text-align:left;padding:6px 8px;font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.08em;border-bottom:1px solid var(--border)">Subject</th>
                      <th style="text-align:left;padding:6px 8px;font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.08em;border-bottom:1px solid var(--border)">Open</th>
                      <th style="text-align:left;padding:6px 8px;font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.08em;border-bottom:1px solid var(--border)">CTR</th>
                      <th style="text-align:left;padding:6px 8px;font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.08em;border-bottom:1px solid var(--border)">Hook</th>
                      <th style="text-align:left;padding:6px 8px;font-size:9px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.08em;border-bottom:1px solid var(--border)">Geo</th>
                    </tr>
                  </thead>
                  <tbody>{rows_html}</tbody>
                </table>"""
                st.markdown(table_html, unsafe_allow_html=True)
            else:
                st.markdown('<div class="empty-chart">No campaigns yet</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)  # /charts-2


        # ── ROW 4: AI-derived Insights ──
        st.markdown("""
        <div class="section-hd" style="padding-top:14px">
          <div class="section-title">Strategic Signals</div>
          <div class="section-sub">Derived from GPT enrichment · refreshes with data</div>
        </div>""", unsafe_allow_html=True)

        # Build insights dynamically from data
        best_hook = hook_data[0] if hook_data else {}
        worst_hook = hook_data[-1] if len(hook_data) > 1 else {}
        best_tone = tone_data[0] if tone_data else {}
        best_offer = offer_data[0] if offer_data else {}
        best_lang = lang_data[0] if lang_data else {}

        def peak_hours(sd):
            if not sd: return "—"
            sorted_sd = sorted(sd, key=lambda x: safe_float(x["avg_open"]), reverse=True)
            top = sorted_sd[:3]
            return ", ".join(f"{safe_int(d['send_hour']):02d}:00" for d in top) + " UTC"

        ins = [
            ("🎯", "Best Hook", best_hook.get("hook_type","—").replace("-"," ").title() if best_hook else "—",
             f"{best_hook.get('avg_open','—')}% open · {best_hook.get('campaigns','—')} campaigns" if best_hook else "No data"),
            ("⚡", "Top Tone", best_tone.get("tone","—").title() if best_tone else "—",
             f"{best_tone.get('avg_open','—')}% open · {best_tone.get('avg_ctr','—')}% CTR" if best_tone else "No data"),
            ("🎁", "Top Offer", best_offer.get("offer_type","—").replace("-"," ").title() if best_offer else "—",
             f"{best_offer.get('avg_open','—')}% open · {best_offer.get('avg_ctr','—')}% CTR" if best_offer else "No data"),
            ("⏰", "Peak Hours", peak_hours(send_data),
             "Highest average open rate time windows"),
            ("🌍", "Top Market", best_lang.get("language","—").upper() if best_lang else "—",
             f"{best_lang.get('campaigns','—')} campaigns · {best_lang.get('avg_open','—')}% open" if best_lang else "No data"),
            ("📉", "Avoid Hook", worst_hook.get("hook_type","—").replace("-"," ").title() if worst_hook else "—",
             f"Lowest open: {worst_hook.get('avg_open','—')}%" if worst_hook else "No data"),
        ]

        st.markdown('<div class="insights-row">', unsafe_allow_html=True)
        cols_ins = st.columns(3, gap="small")
        for i, (icon, title, val, sub) in enumerate(ins):
            with cols_ins[i % 3]:
                st.markdown(f"""<div class="insight-card">
                  <div class="ins-icon">{icon}</div>
                  <div class="ins-title">{title}</div>
                  <div class="ins-value">{val}</div>
                  <div class="ins-sub">{sub}</div>
                </div>""", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    except ImportError:
        st.markdown("""<div style="margin:10px 20px 0;padding:12px 16px;background:var(--amber-dim);
            border:1px solid rgba(245,158,11,.3);border-radius:8px;font-size:12px;color:var(--amber);">
            ⚠ Charts require <code>pip install plotly</code></div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — FILTER + EXPORT CONTROLS
# ═══════════════════════════════════════════════════════════════════════════
st.markdown('<hr class="ei-div">', unsafe_allow_html=True)
st.markdown("""<div style="padding:10px 20px 2px">
  <span style="font-size:11px;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.08em">
    Filter agent responses
  </span>
</div>""", unsafe_allow_html=True)

st.markdown('<div class="filter-strip">', unsafe_allow_html=True)
ctl = st.columns([1, 1, 1, 1, 1, 1, .7])

with ctl[0]:
    st.markdown('<div class="f-lbl">Model</div>', unsafe_allow_html=True)
    model = st.selectbox("model", ["gpt-4o-mini","gpt-4o"], index=0, label_visibility="collapsed", key="mdl")
    os.environ["AGENT_MODEL"] = model

with ctl[1]:
    st.markdown('<div class="f-lbl">Hook</div>', unsafe_allow_html=True)
    fh = st.selectbox("hook", ["Any","curiosity","urgency","social-proof","fear-of-missing-out","story","discount","question"], label_visibility="collapsed", key="fh")

with ctl[2]:
    st.markdown('<div class="f-lbl">Tone</div>', unsafe_allow_html=True)
    ft = st.selectbox("tone", ["Any","casual","formal","playful","urgent","inspirational","informational"], label_visibility="collapsed", key="ft")

with ctl[3]:
    st.markdown('<div class="f-lbl">Language</div>', unsafe_allow_html=True)
    fl = st.selectbox("lang", ["Any","en","lt","ru","es","pl","de","fr","pt","lv"], label_visibility="collapsed", key="fl")

with ctl[4]:
    st.markdown('<div class="f-lbl">Geo</div>', unsafe_allow_html=True)
    fg = st.selectbox("geo", ["Any","global","US","EU","LT","LV","EE","PL","RU","DE","UK"], label_visibility="collapsed", key="fg")

with ctl[5]:
    st.markdown('<div class="f-lbl">Min Open %</div>', unsafe_allow_html=True)
    mo = st.slider("min_open", 0, 80, 0, label_visibility="collapsed", key="mo")

with ctl[6]:
    st.markdown('<div style="height:19px"></div>', unsafe_allow_html=True)
    if st.button("✕ Clear", key="clr"):
        st.session_state.history  = []
        st.session_state.messages = []
        st.session_state.last_df  = None
        st.rerun()

st.markdown("</div>", unsafe_allow_html=True)

st.session_state.filters = {k: v for k, v in {
    "hook_type":     None if fh == "Any" else fh,
    "tone":          None if ft == "Any" else ft,
    "language":      None if fl == "Any" else fl,
    "geo":           None if fg == "Any" else fg,
    "min_open_rate": mo if mo > 0 else None,
}.items() if v is not None}

# Export
if st.session_state.last_df is not None:
    df_export = st.session_state.last_df
    st.markdown('<div class="dl-strip">', unsafe_allow_html=True)
    dl = st.columns([.5, .6, 10])
    with dl[0]:
        st.download_button("↓ CSV", df_export.to_csv(index=False).encode(), "export.csv", "text/csv", key="dl_csv")
    with dl[1]:
        try:
            import openpyxl; buf = io.BytesIO()
            df_export.to_excel(buf, index=False, engine="openpyxl")
            st.download_button("↓ Excel", buf.getvalue(), "export.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_xlsx")
        except ImportError: pass
    st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — AI CHAT ASSISTANT
# ═══════════════════════════════════════════════════════════════════════════
st.markdown('<hr class="ei-div">', unsafe_allow_html=True)
st.markdown("""
<div class="chat-wrap">
  <div class="chat-hd">
    <div class="chat-title">Campaign Analyst</div>
    <div class="chat-sub">SQL · Vector search · Multi-turn memory</div>
  </div>
""", unsafe_allow_html=True)

# CMO/CRO-focused quick prompts
CHIPS = [
    ("📊", "Weekly trend",        "Show open rate trend over the last 8 weeks"),
    ("🏆", "Win formula",         "What combination of hook type, tone, and send time produces the highest CTR?"),
    ("🌍", "Country comparison",  "Compare campaign performance by country — open rate, CTR, unsub rate"),
    ("💡", "Subject line ideas",  "Find the 5 highest open rate subject lines and analyse what makes them work"),
    ("⚡", "Quick win",           "Which hook and tone combination is underused but high performing?"),
    ("📉", "Revenue leaks",       "Which campaign segments have high opens but low CTR — conversion gap?"),
    ("🔁", "Re-engagement",       "Find campaigns with low unsub rate and high CTR — best for re-engagement"),
    ("🧪", "A/B insights",        "Compare urgency vs curiosity hooks — performance breakdown by language"),
    ("🎯", "Lookalike",           "Find campaigns similar to our best performers using semantic search"),
]

if not st.session_state.messages:
    st.markdown('<div class="chips">', unsafe_allow_html=True)
    chip_cols = st.columns(3)
    for i, (icon, label, full_q) in enumerate(CHIPS):
        with chip_cols[i % 3]:
            if st.button(f"{icon} {label}", key=f"c{i}"):
                st.session_state.pending_question = full_q
                st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

# Render conversation
st.markdown('<div class="msgs">', unsafe_allow_html=True)
for msg in st.session_state.messages:
    if msg["role"] == "user":
        st.markdown(f'<div class="msg-u">{msg["content"]}</div>', unsafe_allow_html=True)
    else:
        content = msg["content"]
        pills = ""
        if any(k in content.lower() for k in ["open rate","%","avg","count","ctr","campaigns","rows"]):
            pills += '<span class="pill p-sql">SQL</span>'
        if any(k in content.lower() for k in ["similar","score:","preview:","semantic"]):
            pills += '<span class="pill p-rag">RAG</span>'
        ph = f'<div class="tpills">{pills}</div>' if pills else ""
        st.markdown(f'<div class="msg-a-row"><div class="msg-av">✦</div><div class="msg-a">{ph}', unsafe_allow_html=True)
        st.markdown(content)
        st.markdown("</div></div>", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)  # /msgs
st.markdown("</div>", unsafe_allow_html=True)  # /chat-wrap
st.markdown('<div class="bottom-pad"></div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# AGENT
# ═══════════════════════════════════════════════════════════════════════════
def extract_df(reply):
    try:
        import pandas as pd
        rows = parse_md_table(reply)
        if rows: return pd.DataFrame(rows)
    except: pass
    return None


def run_question(question: str):
    st.session_state.messages.append({"role": "user", "content": question})
    st.markdown(f'<div class="msg-u">{question}</div>', unsafe_allow_html=True)
    ph = st.empty()
    ph.markdown("""<div class="thinking">
      <div class="dot"></div><div class="dot"></div><div class="dot"></div>
      <span style="font-size:11px;color:var(--text3)">Querying data…</span>
    </div>""", unsafe_allow_html=True)
    try:
        from agent import run_agent
        aug = question
        if st.session_state.filters:
            fstr = ", ".join(f"{k}={v}" for k, v in st.session_state.filters.items())
            aug = f"{question}\n[Active filters: {fstr}]"
        reply, hist = run_agent(aug, st.session_state.history)
        st.session_state.history = hist
        ph.empty()
        df = extract_df(reply)
        if df is not None:
            st.session_state.last_df = df
        pills = ""
        if any(k in reply.lower() for k in ["open rate","%","avg","count","ctr","campaigns"]):
            pills += '<span class="pill p-sql">SQL</span>'
        if any(k in reply.lower() for k in ["similar","score:","preview:"]):
            pills += '<span class="pill p-rag">RAG</span>'
        ph2 = f'<div class="tpills">{pills}</div>' if pills else ""
        st.markdown(f'<div class="msg-a-row"><div class="msg-av">✦</div><div class="msg-a">{ph2}', unsafe_allow_html=True)
        st.markdown(reply)
        st.markdown("</div></div>", unsafe_allow_html=True)
        st.session_state.messages.append({"role": "assistant", "content": reply})
    except Exception as e:
        ph.empty()
        err = f"**Error:** {e}"
        st.markdown(f'<div class="msg-a-row"><div class="msg-av">✦</div><div class="msg-a" style="border-color:rgba(248,113,113,.3);color:var(--red)">{err}</div></div>', unsafe_allow_html=True)
        st.session_state.messages.append({"role": "assistant", "content": err})


if "pending_question" in st.session_state:
    q = st.session_state.pop("pending_question")
    run_question(q)
    st.rerun()

if prompt := st.chat_input("Ask about your campaigns…"):
    run_question(prompt)
