"""
Email Intelligence — Compact SaaS UI
Single-screen layout · Mobile-first · No wasted space
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
@import url('https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=Geist:wght@400;500;600&family=Geist+Mono:wght@400;500&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:        #f4f2ee;
  --surface:   #ffffff;
  --surface2:  #f9f8f6;
  --border:    #e6e1d8;
  --border2:   #cdc8bc;
  --text:      #1c1917;
  --text2:     #6b6560;
  --text3:     #a8a29e;
  --green:     #1a6b47;
  --green2:    #27a765;
  --green-bg:  #e8f8ef;
  --green-pale:#f2fbf5;
  --warm:      #b06020;
  --warm-bg:   #fef3e8;
  --blue:      #2f54c0;
  --blue-bg:   #edf0fb;
  --sans:      'Geist', system-ui, sans-serif;
  --serif:     'Instrument Serif', Georgia, serif;
  --mono:      'Geist Mono', 'Courier New', monospace;
  --r:         10px;
  --r-sm:      7px;
}

#MainMenu, footer, header,
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"],
.stDeployButton { display: none !important; }

.stApp { background: var(--bg) !important; font-family: var(--sans) !important; }
.main .block-container { padding: 0 !important; max-width: 100% !important; }
[data-testid="stVerticalBlock"],
[data-testid="stVerticalBlockBorderWrapper"] { gap: 0 !important; padding: 0 !important; }
[data-testid="element-container"] { margin: 0 !important; }

.stSelectbox label, .stSlider label, .stTextInput label { display: none !important; }

/* NAV */
.ei-nav {
  position: sticky; top: 0; z-index: 200;
  background: var(--surface); border-bottom: 1px solid var(--border);
  display: flex; align-items: center; justify-content: space-between;
  padding: 0 16px; height: 52px;
}
.ei-brand { display: flex; align-items: center; gap: 10px; }
.ei-logo {
  width: 30px; height: 30px; flex-shrink: 0;
  background: linear-gradient(135deg, var(--green), var(--green2));
  border-radius: 8px; display: flex; align-items: center; justify-content: center;
  font-size: 14px; box-shadow: 0 2px 6px rgba(26,107,71,.25);
}
.ei-name {
  font-family: var(--serif) !important;
  font-size: 16px; color: var(--text); letter-spacing: -.01em;
}
.ei-name em { color: var(--green); font-style: normal; }
.ei-badge {
  display: flex; align-items: center; gap: 5px;
  font-size: 11px; font-weight: 500; padding: 4px 10px;
  border-radius: 20px; letter-spacing: .01em;
}
.b-live { background: #dcfce7; color: #15803d; border: 1px solid #bbf7d0; }
.b-warn { background: #fef2f2; color: #b91c1c; border: 1px solid #fecaca; }
.sdot { width: 5px; height: 5px; border-radius: 50%; background: currentColor; animation: blink 2s ease-in-out infinite; }
@keyframes blink { 0%,100%{opacity:.9} 50%{opacity:.3} }

/* KPI */
.kpi-row {
  display: grid; grid-template-columns: repeat(4,1fr);
  gap: 10px; padding: 12px 16px 0;
}
@media(max-width:640px){ .kpi-row{ grid-template-columns:repeat(2,1fr); } }
.kpi-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--r); padding: 12px 14px 10px;
  position: relative; overflow: hidden;
}
.kpi-card::after {
  content:''; position:absolute; bottom:0; left:0; right:0; height:2px;
  background: var(--kc, var(--green)); opacity:.5;
}
.kpi-l { font-size:10px; font-weight:600; color:var(--text3); text-transform:uppercase; letter-spacing:.07em; margin-bottom:4px; }
.kpi-n { font-family:var(--serif) !important; font-size:26px; color:var(--text); line-height:1; letter-spacing:-.02em; margin-bottom:5px; }
.kpi-t { display:inline-flex; align-items:center; gap:3px; font-size:10px; font-weight:500; padding:2px 7px; border-radius:20px; }
.tg { background:var(--green-bg); color:var(--green); }
.tw { background:var(--warm-bg);  color:var(--warm); }
.tb { background:var(--blue-bg);  color:var(--blue); }

/* CHARTS */
.charts-row {
  display: grid; grid-template-columns: 1fr 1fr;
  gap: 10px; padding: 10px 16px 0;
}
@media(max-width:640px){ .charts-row{ grid-template-columns:1fr; } }
.chart-card {
  background:var(--surface); border:1px solid var(--border);
  border-radius:var(--r); padding:12px 14px 8px; overflow:hidden;
}
.ct { font-size:12px; font-weight:600; color:var(--text); margin-bottom:1px; }
.cs { font-size:11px; color:var(--text3); margin-bottom:8px; }
.empty-chart { color:var(--text3); font-size:12px; padding:20px 0; text-align:center; }

/* DIVIDER */
.ei-div { border:none; border-top:1px solid var(--border); margin:10px 16px 0; }

/* CONTROLS STRIP — purely CSS flex, Streamlit widgets float inside */
.ctrl-area { padding: 8px 16px 0; display: flex; align-items: flex-end; gap: 10px; flex-wrap: wrap; }
.ctrl-group { display: flex; flex-direction: column; gap: 3px; }
.ctrl-lbl { font-size:10px; font-weight:600; color:var(--text3); text-transform:uppercase; letter-spacing:.08em; }
.ctrl-sep { width:1px; height:28px; background:var(--border); align-self:flex-end; margin-bottom:2px; }

.ctrl-area .stSelectbox { width:120px !important; }
.ctrl-area .stSelectbox > div > div {
  background:var(--surface) !important; border:1px solid var(--border) !important;
  border-radius:var(--r-sm) !important; font-size:12px !important;
  color:var(--text) !important; min-height:32px !important; box-shadow:none !important;
}
.ctrl-area .stSlider { width:100px !important; }
.ctrl-area [data-testid="stSliderThumb"] { background:var(--green) !important; }

.ctrl-area .stButton > button {
  background:var(--surface) !important; border:1px solid var(--border) !important;
  border-radius:var(--r-sm) !important; color:var(--text2) !important;
  font-size:12px !important; padding:5px 12px !important;
  white-space:nowrap !important; box-shadow:none !important; height:32px !important;
}
.ctrl-area .stButton > button:hover {
  border-color:#fca5a5 !important; color:#b91c1c !important; background:#fef2f2 !important;
}

/* DOWNLOAD STRIP */
.dl-strip { padding: 6px 16px 0; display: flex; gap: 6px; }
.dl-strip .stDownloadButton > button {
  background:var(--surface) !important; border:1px solid var(--border) !important;
  border-radius:var(--r-sm) !important; color:var(--text2) !important;
  font-size:11px !important; padding:4px 12px !important; box-shadow:none !important;
}
.dl-strip .stDownloadButton > button:hover {
  border-color:var(--green) !important; color:var(--green) !important; background:var(--green-pale) !important;
}

/* CHAT */
.chat-area { padding: 10px 16px 0; }
.chat-hd { display:flex; align-items:baseline; justify-content:space-between; margin-bottom:8px; }
.chat-title { font-family:var(--serif) !important; font-size:17px; color:var(--text); }
.chat-sub   { font-size:11px; color:var(--text3); }

/* Chips */
.chips { display:grid; grid-template-columns:repeat(3,1fr); gap:6px; margin-bottom:10px; }
@media(max-width:480px){ .chips{ grid-template-columns:repeat(2,1fr); } }
.chips .stButton > button {
  background:var(--surface) !important; border:1px solid var(--border) !important;
  border-radius:var(--r-sm) !important; color:var(--text2) !important;
  font-size:11px !important; padding:8px 10px !important; line-height:1.3 !important;
  text-align:left !important; width:100% !important; height:auto !important;
  white-space:normal !important; box-shadow:none !important; font-weight:400 !important;
}
.chips .stButton > button:hover {
  border-color:var(--green) !important; color:var(--green) !important; background:var(--green-pale) !important;
}

/* Messages */
.msgs { display:flex; flex-direction:column; gap:8px; margin-bottom:4px; }
.msg-u {
  align-self:flex-end; background:var(--text); color:#f4f2ee;
  border-radius:var(--r) var(--r) 3px var(--r);
  padding:10px 14px; font-size:13px; line-height:1.55;
  max-width:min(480px,85%); word-wrap:break-word;
}
.msg-a-row { display:flex; gap:8px; align-items:flex-start; }
.msg-av {
  width:24px; height:24px; flex-shrink:0;
  background:linear-gradient(135deg,var(--green),var(--green2));
  border-radius:6px; display:flex; align-items:center; justify-content:center;
  font-size:11px; margin-top:2px; box-shadow:0 1px 4px rgba(26,107,71,.2);
}
.msg-a {
  background:var(--surface); border:1px solid var(--border);
  border-radius:3px var(--r) var(--r) var(--r);
  padding:10px 14px; font-size:13px; line-height:1.65; color:var(--text);
  flex:1; min-width:0; box-shadow:0 1px 4px rgba(0,0,0,.04);
}
.tpills { display:flex; gap:5px; margin-bottom:6px; flex-wrap:wrap; }
.pill {
  font-family:var(--mono) !important; font-size:9px; font-weight:500;
  padding:2px 7px; border-radius:3px; letter-spacing:.03em;
}
.p-sql { background:var(--blue-bg); color:var(--blue); border:1px solid #c7d2fe; }
.p-rag { background:var(--green-bg); color:var(--green); border:1px solid #a7f3d0; }

/* Tables inside messages */
.msg-a table { width:100%; border-collapse:collapse; font-size:12px; margin-top:6px; }
.msg-a thead tr { background:var(--bg); }
.msg-a th { text-align:left; padding:6px 10px; font-size:10px; font-weight:600; color:var(--text3); text-transform:uppercase; letter-spacing:.06em; border-bottom:1px solid var(--border); }
.msg-a td { padding:6px 10px; border-bottom:1px solid var(--bg); }
.msg-a tr:last-child td { border-bottom:none; }

/* Thinking */
.thinking { display:flex; align-items:center; gap:6px; color:var(--text3); font-size:12px; padding:4px 0; }
.dot { width:4px; height:4px; border-radius:50%; background:var(--border2); animation:bdot 1.3s ease-in-out infinite; }
.dot:nth-child(2){ animation-delay:.15s; }
.dot:nth-child(3){ animation-delay:.3s; }
@keyframes bdot { 0%,80%,100%{opacity:.2;transform:scale(.7)} 40%{opacity:1;transform:scale(1)} }

/* Fixed chat input */
.stChatInputContainer {
  background:var(--surface) !important; border-top:1px solid var(--border) !important;
  padding:10px 16px 12px !important; position:fixed !important;
  bottom:0 !important; left:0 !important; right:0 !important;
  z-index:150 !important; box-shadow:0 -4px 20px rgba(0,0,0,.07) !important;
}
textarea[data-testid="stChatInputTextArea"] {
  background:var(--bg) !important; border:1px solid var(--border2) !important;
  border-radius:var(--r) !important; color:var(--text) !important;
  font-family:var(--sans) !important; font-size:14px !important;
  padding:10px 14px !important; max-width:760px !important;
  margin:0 auto !important; display:block !important;
}
textarea[data-testid="stChatInputTextArea"]:focus {
  border-color:var(--green) !important; box-shadow:0 0 0 3px rgba(26,107,71,.1) !important; outline:none !important;
}
[data-testid="stChatInputSubmitButton"] > button {
  background:var(--green) !important; border:none !important;
  color:white !important; border-radius:7px !important;
}

.bottom-pad { height: 76px; }
::-webkit-scrollbar { width:4px; height:4px; }
::-webkit-scrollbar-thumb { background:var(--border2); border-radius:2px; }
::-webkit-scrollbar-track { background:transparent; }

/* DATA CONTEXT PANEL */
.ctx-panel {
  margin: 10px 16px 0;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--r);
  overflow: hidden;
}
.ctx-header {
  display: flex; align-items: center; justify-content: space-between;
  padding: 10px 14px; cursor: pointer; user-select: none;
  background: var(--surface2);
  border-bottom: 1px solid transparent;
  transition: border-color .15s;
}
.ctx-header:hover { background: #f3f1ed; }
.ctx-header.open { border-bottom-color: var(--border); }
.ctx-header-left { display: flex; align-items: center; gap: 8px; }
.ctx-icon {
  width: 22px; height: 22px;
  background: var(--blue-bg); border-radius: 6px;
  display: flex; align-items: center; justify-content: center;
  font-size: 11px; color: var(--blue); border: 1px solid #c7d2fe;
}
.ctx-title { font-size: 12px; font-weight: 600; color: var(--text); }
.ctx-hint { font-size: 11px; color: var(--text3); }
.ctx-arrow { font-size: 10px; color: var(--text3); transition: transform .2s; }
.ctx-arrow.open { transform: rotate(180deg); }

.ctx-body { padding: 12px 14px; display: none; }
.ctx-body.open { display: block; }

.ctx-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; }
@media(max-width:640px){ .ctx-grid{ grid-template-columns:1fr; } }

.ctx-section { }
.ctx-section-title {
  font-size: 10px; font-weight: 600; text-transform: uppercase;
  letter-spacing: .07em; margin-bottom: 7px;
}
.ctx-section-title.green { color: var(--green); }
.ctx-section-title.red   { color: #dc2626; }
.ctx-section-title.blue  { color: var(--blue); }

.ctx-item {
  display: flex; align-items: flex-start; gap: 6px;
  font-size: 11px; color: var(--text2); line-height: 1.45;
  padding: 3px 0;
}
.ctx-dot { width: 5px; height: 5px; border-radius: 50%; flex-shrink: 0; margin-top: 5px; }
.dot-green { background: var(--green2); }
.dot-red   { background: #ef4444; }
.dot-blue  { background: var(--blue); }

.ctx-sep { height: 1px; background: var(--border); margin: 10px 0; }

.hypo-chips { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 2px; }
.hypo-chip {
  font-size: 10px; color: var(--blue); background: var(--blue-bg);
  border: 1px solid #c7d2fe; border-radius: 20px;
  padding: 3px 9px; cursor: pointer; transition: all .15s;
  font-weight: 500;
}
.hypo-chip:hover { background: #dde5fb; border-color: var(--blue); }
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


# ─── NAV ───────────────────────────────────────────────────────────────────
status = (
    '<div class="ei-badge b-live"><div class="sdot"></div>Live</div>'
    if not missing else
    '<div class="ei-badge b-warn"><div class="sdot"></div>Setup needed</div>'
)
st.markdown(f"""
<div class="ei-nav">
  <div class="ei-brand">
    <div class="ei-logo">✉</div>
    <div class="ei-name">Email <em>Intelligence</em></div>
  </div>
  {status}
</div>
""", unsafe_allow_html=True)


# ─── KPI ───────────────────────────────────────────────────────────────────
stat_vals = ["—","—","—","—"]
if not missing:
    parsed = parse_md_table(load_stats())
    if parsed:
        stat_vals = list(parsed[0].values())

KPI = [
    ("Campaigns",    "", "All time",          "tw", "#b06020"),
    ("Avg Open Rate","%","↑ vs 21% industry", "tg", "#1a6b47"),
    ("Avg CTR",      "%","↑ vs 2.6% industry","tg", "#1a6b47"),
    ("Hook Types",   "", "GPT classified",    "tb", "#2f54c0"),
]
kpi_html = '<div class="kpi-row">'
for i,(lbl,sfx,hint,tcls,color) in enumerate(KPI):
    v = stat_vals[i] if i < len(stat_vals) else "—"
    kpi_html += f'<div class="kpi-card" style="--kc:{color}"><div class="kpi-l">{lbl}</div><div class="kpi-n">{v}{sfx}</div><span class="kpi-t {tcls}">{hint}</span></div>'
kpi_html += '</div>'
st.markdown(kpi_html, unsafe_allow_html=True)


# ─── CHARTS ────────────────────────────────────────────────────────────────
if not missing:
    try:
        import plotly.graph_objects as go
        hook_data = parse_md_table(load_hook_data())
        tone_data = parse_md_table(load_tone_data())

        st.markdown('<div class="charts-row">', unsafe_allow_html=True)
        c1, c2 = st.columns(2, gap="small")

        with c1:
            st.markdown('<div class="chart-card"><div class="ct">Open Rate by Hook Type</div><div class="cs">avg % · sorted by performance</div>', unsafe_allow_html=True)
            if hook_data:
                hooks  = [d["hook_type"].replace("-"," ").title() for d in hook_data]
                opens  = [float(d["avg_open"]) for d in hook_data]
                counts = [int(d["campaigns"]) for d in hook_data]
                mx = max(opens)
                fig = go.Figure(go.Bar(
                    x=opens, y=hooks, orientation="h",
                    marker_color=["#1a6b47" if o==mx else "#a7d7ba" for o in opens],
                    marker_line_width=0,
                    text=[f"{o}%" for o in opens], textposition="outside",
                    textfont=dict(size=10, color="#6b6560"),
                    customdata=counts,
                    hovertemplate="<b>%{y}</b><br>%{x}% · %{customdata} campaigns<extra></extra>",
                ))
                fig.update_layout(
                    height=190, margin=dict(l=0,r=44,t=2,b=0),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="Geist,sans-serif",size=11,color="#6b6560"),
                    xaxis=dict(showgrid=True,gridcolor="#ede9e1",zeroline=False,
                               showticklabels=False,range=[0,mx*1.3]),
                    yaxis=dict(showgrid=False), showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})
            else:
                st.markdown('<div class="empty-chart">Enrichment in progress…</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        with c2:
            st.markdown('<div class="chart-card"><div class="ct">Tone Distribution</div><div class="cs">campaigns by communication style</div>', unsafe_allow_html=True)
            if tone_data:
                tones  = [d["tone"].title() for d in tone_data]
                counts = [int(d["campaigns"]) for d in tone_data]
                PALETTE= ["#1a6b47","#27a765","#74c69d","#b7e4c7","#52b788","#95d5b2","#1b4332","#40916c"]
                fig2 = go.Figure(go.Pie(
                    labels=tones, values=counts, hole=0.55,
                    marker_colors=PALETTE[:len(tones)],
                    marker=dict(line=dict(color="#fff",width=2)),
                    textinfo="percent", textfont_size=10,
                    hovertemplate="<b>%{label}</b><br>%{value} · %{percent}<extra></extra>",
                ))
                fig2.update_layout(
                    height=190, margin=dict(l=0,r=0,t=2,b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="Geist,sans-serif",size=10,color="#6b6560"),
                    legend=dict(orientation="v",x=1.02,y=0.5,font=dict(size=10),bgcolor="rgba(0,0,0,0)"),
                )
                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar":False})
            else:
                st.markdown('<div class="empty-chart">Enrichment in progress…</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    except ImportError:
        st.markdown("""<div style="margin:10px 16px 0;padding:10px 14px;background:#fef3e8;
            border:1px solid #f0d5b5;border-radius:8px;font-size:12px;color:#b06020;">
            ⚠ Charts need plotly — <code>pip install plotly</code></div>""", unsafe_allow_html=True)


# ─── CONTROLS ──────────────────────────────────────────────────────────────
st.markdown('<hr class="ei-div">', unsafe_allow_html=True)
st.markdown('<div class="ctrl-area">', unsafe_allow_html=True)

ctl = st.columns([1.1, 1, 1, 1, 1, .8])

with ctl[0]:
    st.markdown('<div class="ctrl-lbl">Model</div>', unsafe_allow_html=True)
    model = st.selectbox("model", ["gpt-4o-mini","gpt-4o"], index=0, label_visibility="collapsed", key="mdl")
    os.environ["AGENT_MODEL"] = model

with ctl[1]:
    st.markdown('<div class="ctrl-lbl">Hook</div>', unsafe_allow_html=True)
    fh = st.selectbox("hook", ["Any","curiosity","urgency","social-proof","fear-of-missing-out","story","discount","question"], label_visibility="collapsed", key="fh")

with ctl[2]:
    st.markdown('<div class="ctrl-lbl">Tone</div>', unsafe_allow_html=True)
    ft = st.selectbox("tone", ["Any","casual","formal","playful","urgent","inspirational","informational"], label_visibility="collapsed", key="ft")

with ctl[3]:
    st.markdown('<div class="ctrl-lbl">Language</div>', unsafe_allow_html=True)
    fl = st.selectbox("lang", ["Any","en","lt","ru","es","pl"], label_visibility="collapsed", key="fl")

with ctl[4]:
    st.markdown('<div class="ctrl-lbl">Min open %</div>', unsafe_allow_html=True)
    mo = st.slider("min_open", 0, 100, 0, label_visibility="collapsed", key="mo")

with ctl[5]:
    st.markdown('<div style="height:19px"></div>', unsafe_allow_html=True)
    if st.button("✕ Clear chat", key="clr"):
        st.session_state.history  = []
        st.session_state.messages = []
        st.session_state.last_df  = None
        st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

st.session_state.filters = {k: v for k,v in {
    "hook_type":     None if fh=="Any" else fh,
    "tone":          None if ft=="Any" else ft,
    "language":      None if fl=="Any" else fl,
    "min_open_rate": mo if mo>0 else None,
}.items() if v is not None}

# Downloads
if st.session_state.last_df is not None:
    df = st.session_state.last_df
    st.markdown('<div class="dl-strip">', unsafe_allow_html=True)
    dl = st.columns([.7,.7,6])
    with dl[0]:
        st.download_button("↓ CSV", df.to_csv(index=False).encode(), "export.csv", "text/csv", key="dl_csv")
    with dl[1]:
        try:
            import openpyxl; buf = io.BytesIO()
            df.to_excel(buf, index=False, engine="openpyxl")
            st.download_button("↓ Excel", buf.getvalue(), "export.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_xlsx")
        except ImportError: pass
    st.markdown('</div>', unsafe_allow_html=True)


# ─── DATA CONTEXT PANEL ────────────────────────────────────────────────────
st.markdown('<hr class="ei-div">', unsafe_allow_html=True)
st.markdown("""
<div class="ctx-panel">
  <div class="ctx-header" id="ctx-hdr" onclick="toggleCtx()" >
    <div class="ctx-header-left">
      <div class="ctx-icon">𝌞</div>
      <div>
        <div class="ctx-title">What's in this data?</div>
      </div>
    </div>
    <div style="display:flex;align-items:center;gap:10px;">
      <span class="ctx-hint">Mailchimp → BigQuery · no revenue data</span>
      <span class="ctx-arrow" id="ctx-arrow">▼</span>
    </div>
  </div>
  <div class="ctx-body" id="ctx-body">
    <div class="ctx-grid">

      <div class="ctx-section">
        <div class="ctx-section-title green">✓ Available data</div>
        <div class="ctx-item"><div class="ctx-dot dot-green"></div>1 652 email campaigns from Mailchimp</div>
        <div class="ctx-item"><div class="ctx-dot dot-green"></div>Open rate &amp; CTR per campaign</div>
        <div class="ctx-item"><div class="ctx-dot dot-green"></div>Hook type (Curiosity, Urgency, Gift…) — GPT-classified</div>
        <div class="ctx-item"><div class="ctx-dot dot-green"></div>Tone (Casual, Informational, Playful…)</div>
        <div class="ctx-item"><div class="ctx-dot dot-green"></div>Language (EN, RU, LT, ES, PL)</div>
        <div class="ctx-item"><div class="ctx-dot dot-green"></div>Subject line text &amp; preview</div>
        <div class="ctx-item"><div class="ctx-dot dot-green"></div>Send date &amp; campaign name</div>
      </div>

      <div class="ctx-section">
        <div class="ctx-section-title red">✗ Not available</div>
        <div class="ctx-item"><div class="ctx-dot dot-red"></div>Revenue / GMV — needs affiliate data separately</div>
        <div class="ctx-item"><div class="ctx-dot dot-red"></div>Unsubscribe &amp; bounce rates</div>
        <div class="ctx-item"><div class="ctx-dot dot-red"></div>Individual recipient behaviour</div>
        <div class="ctx-item"><div class="ctx-dot dot-red"></div>A/B test variants</div>
        <div class="ctx-item"><div class="ctx-dot dot-red"></div>Segment / audience breakdown</div>
        <div class="ctx-item"><div class="ctx-dot dot-red"></div>Send-time optimisation data</div>
      </div>

      <div class="ctx-section">
        <div class="ctx-section-title blue">⚡ Hypotheses to explore</div>
        <div class="hypo-chips">
          <span class="hypo-chip" onclick="sendHypo('Does curiosity hook outperform urgency across all languages?')">Curiosity vs Urgency by language</span>
          <span class="hypo-chip" onclick="sendHypo('Which tone has the highest CTR — casual or informational?')">Tone → CTR</span>
          <span class="hypo-chip" onclick="sendHypo('Show open rate trend over time — are results improving?')">Open rate trend</span>
          <span class="hypo-chip" onclick="sendHypo('What subject line patterns appear in top 50 campaigns by open rate?')">Top subject patterns</span>
          <span class="hypo-chip" onclick="sendHypo('Compare performance of discount vs gift hook types')">Discount vs Gift</span>
          <span class="hypo-chip" onclick="sendHypo('Which language audience responds best to urgency emails?')">Language × Hook</span>
          <span class="hypo-chip" onclick="sendHypo('Show me campaigns with open rate above 50% — what do they have in common?')">50%+ open rate</span>
          <span class="hypo-chip" onclick="sendHypo('Is there a correlation between subject line length and open rate?')">Subject length → open</span>
        </div>
      </div>

    </div>
  </div>
</div>

<script>
function toggleCtx() {
  var body = document.getElementById('ctx-body');
  var arrow = document.getElementById('ctx-arrow');
  var hdr = document.getElementById('ctx-hdr');
  var isOpen = body.classList.contains('open');
  body.classList.toggle('open', !isOpen);
  arrow.classList.toggle('open', !isOpen);
  hdr.classList.toggle('open', !isOpen);
}
function sendHypo(text) {
  // Find Streamlit chat input and inject text
  var inputs = window.parent.document.querySelectorAll('textarea[data-testid="stChatInputTextArea"]');
  if (inputs.length > 0) {
    var inp = inputs[0];
    var nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.parent.HTMLTextAreaElement.prototype, 'value').set;
    nativeInputValueSetter.call(inp, text);
    inp.dispatchEvent(new Event('input', { bubbles: true }));
    inp.focus();
  }
}
</script>
""", unsafe_allow_html=True)


# ─── CHAT ──────────────────────────────────────────────────────────────────
st.markdown('<hr class="ei-div">', unsafe_allow_html=True)
st.markdown("""
<div class="chat-area">
  <div class="chat-hd">
    <div class="chat-title">AI Assistant</div>
    <div class="chat-sub">Ask anything about your campaigns</div>
  </div>
""", unsafe_allow_html=True)

SUGGESTIONS = [
    ("📈","Top 10 by open rate",   "Top 10 campaigns by open rate"),
    ("🪝","Best hook type?",       "Which hook type works best?"),
    ("⚡","Urgency > 30% open",    "Urgency emails with open rate > 30%"),
    ("🎭","CTR by tone",           "CTR by tone comparison"),
    ("🌍","By language",           "Campaigns by language breakdown"),
    ("💸","Best discount emails",  "Best discount campaigns"),
]

if not st.session_state.messages:
    st.markdown('<div class="chips">', unsafe_allow_html=True)
    chip_cols = st.columns(3)
    for i,(icon,label,full_q) in enumerate(SUGGESTIONS):
        with chip_cols[i%3]:
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
st.markdown('</div>', unsafe_allow_html=True)  # /chat-area
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
        st.markdown(f'<div class="msg-a-row"><div class="msg-av">✦</div><div class="msg-a" style="color:#b91c1c">{err}</div></div>', unsafe_allow_html=True)
        st.session_state.messages.append({"role":"assistant","content":err})

if "pending_question" in st.session_state:
    q = st.session_state.pop("pending_question")
    run_question(q)
    st.rerun()

if prompt := st.chat_input("Ask about your campaigns…"):
    run_question(prompt)
