"""
Email Marketing Intelligence — Professional Dashboard
Warm editorial design · Dashboard + AI Chat · Export
"""

import io
import json
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

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=DM+Mono:wght@400;500&display=swap');

:root {
  --bg:        #f7f6f3;
  --surface:   #ffffff;
  --border:    #e8e4dc;
  --border2:   #d4cfc4;
  --text:      #1c1917;
  --text2:     #78716c;
  --text3:     #a8a29e;
  --accent:    #4f46e5;
  --accent-bg: #eef2ff;
  --green:     #16a34a;
  --green-bg:  #f0fdf4;
  --amber:     #d97706;
  --amber-bg:  #fffbeb;
  --red:       #dc2626;
  --mono:      'DM Mono', monospace;
  --sans:      'DM Sans', sans-serif;
  --radius:    10px;
  --shadow:    0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04);
  --shadow-md: 0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
}

*, html, body, [class*="css"] {
  font-family: var(--sans) !important;
  -webkit-font-smoothing: antialiased;
}
.stApp { background: var(--bg) !important; }
#MainMenu, footer, header, [data-testid="stToolbar"],
[data-testid="collapsedControl"] { visibility: hidden; height: 0; }
.main .block-container { padding: 0 !important; max-width: 100% !important; }

.topnav {
  background: var(--surface); border-bottom: 1px solid var(--border);
  padding: 0 32px; height: 56px;
  display: flex; align-items: center; justify-content: space-between;
  box-shadow: var(--shadow);
}
.nav-brand { display: flex; align-items: center; gap: 10px; }
.nav-logo {
  width: 32px; height: 32px; background: var(--accent);
  border-radius: 8px; display: flex; align-items: center;
  justify-content: center; color: white; font-size: 16px;
}
.nav-title { font-size: 15px; font-weight: 600; color: var(--text); letter-spacing: -0.02em; }
.nav-subtitle { font-size: 12px; color: var(--text3); }
.nav-pill {
  display: flex; align-items: center; gap: 6px;
  font-size: 12px; padding: 4px 12px; border-radius: 20px;
  border: 1px solid #bbf7d0; background: var(--green-bg); color: var(--green);
}
.nav-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--green); }

.page-body { padding: 28px 32px 120px 32px; max-width: 1200px; margin: 0 auto; }

.kpi-grid { display: grid; grid-template-columns: repeat(4,1fr); gap: 12px; margin-bottom: 28px; }
.kpi-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 20px 22px; box-shadow: var(--shadow);
  transition: box-shadow 0.15s;
}
.kpi-card:hover { box-shadow: var(--shadow-md); }
.kpi-val { font-family: var(--mono); font-size: 26px; font-weight: 500; color: var(--text); letter-spacing: -0.03em; line-height: 1; margin-bottom: 6px; }
.kpi-lbl { font-size: 11px; font-weight: 600; color: var(--text3); text-transform: uppercase; letter-spacing: 0.07em; }
.kpi-delta { display: inline-block; font-size: 11px; font-weight: 500; margin-top: 6px; padding: 2px 7px; border-radius: 4px; }
.d-up  { background: var(--green-bg); color: var(--green); }
.d-neu { background: var(--amber-bg); color: var(--amber); }

.chart-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 20px 22px;
  box-shadow: var(--shadow); margin-bottom: 12px;
}
.sec-hd { display: flex; align-items: baseline; gap: 8px; margin-bottom: 14px; }
.sec-title { font-size: 13px; font-weight: 600; color: var(--text); }
.sec-sub   { font-size: 12px; color: var(--text3); }

.divider { border: none; border-top: 1px solid var(--border); margin: 28px 0; }

.msg-user {
  background: var(--accent-bg); border: 1px solid #c7d2fe;
  border-radius: var(--radius); padding: 12px 16px;
  font-size: 14px; color: var(--text); line-height: 1.6; margin-bottom: 6px;
}
.msg-bot {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 16px 20px;
  font-size: 14px; color: var(--text); line-height: 1.7;
  margin-bottom: 6px; box-shadow: var(--shadow);
}
.badge {
  display: inline-flex; align-items: center; gap: 4px;
  font-family: var(--mono); font-size: 10px; font-weight: 500;
  padding: 2px 8px; border-radius: 4px; margin-right: 6px; margin-bottom: 8px;
}
.b-sql { background: var(--accent-bg); color: var(--accent); border: 1px solid #c7d2fe; }
.b-rag { background: var(--green-bg);  color: var(--green);  border: 1px solid #bbf7d0; }

.thinking { display: flex; align-items: center; gap: 8px; color: var(--text3); font-size: 13px; padding: 8px 0; }
.dot { width: 5px; height: 5px; border-radius: 50%; background: var(--border2); animation: blink 1.2s ease-in-out infinite; }
.dot:nth-child(2) { animation-delay: 0.2s; }
.dot:nth-child(3) { animation-delay: 0.4s; }
@keyframes blink { 0%,80%,100%{opacity:.3;transform:scale(.8);} 40%{opacity:1;transform:scale(1);} }

.stButton > button {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-radius: 20px !important; color: var(--text2) !important;
  font-size: 13px !important; font-weight: 400 !important;
  padding: 5px 14px !important; transition: all 0.12s !important;
  white-space: nowrap !important; width: auto !important; box-shadow: none !important;
}
.stButton > button:hover {
  border-color: var(--accent) !important; color: var(--accent) !important;
  background: var(--accent-bg) !important;
}
.btn-flat > button {
  border-radius: 8px !important; font-size: 12px !important; color: var(--text2) !important;
}

.stChatInputContainer {
  background: var(--surface) !important; border-top: 1px solid var(--border) !important;
  padding: 12px 32px !important; position: fixed !important;
  bottom: 0 !important; left: 0 !important; right: 0 !important;
  z-index: 99 !important; box-shadow: 0 -4px 20px rgba(0,0,0,0.06) !important;
}
textarea[data-testid="stChatInputTextArea"] {
  background: var(--bg) !important; border: 1px solid var(--border) !important;
  border-radius: var(--radius) !important; color: var(--text) !important;
  font-family: var(--sans) !important; font-size: 14px !important; padding: 10px 14px !important;
  max-width: 860px !important; margin: 0 auto !important; display: block !important;
}
textarea[data-testid="stChatInputTextArea"]:focus {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 3px rgba(79,70,229,0.1) !important; outline: none !important;
}

table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 4px; }
thead tr { background: var(--bg); }
th { text-align: left; padding: 9px 14px; font-size: 11px; font-weight: 600; color: var(--text3); text-transform: uppercase; letter-spacing: 0.06em; border-bottom: 1px solid var(--border); }
td { padding: 9px 14px; border-bottom: 1px solid var(--bg); color: var(--text); vertical-align: top; }
tr:hover td { background: var(--bg); }
tr:last-child td { border-bottom: none; }

.stSelectbox > div > div {
  background: var(--surface) !important; border: 1px solid var(--border) !important;
  border-radius: 8px !important; font-size: 13px !important; color: var(--text) !important;
}
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }
</style>
""", unsafe_allow_html=True)


# ── State ──────────────────────────────────────────────────────────────────
if "history"  not in st.session_state: st.session_state.history  = []
if "messages" not in st.session_state: st.session_state.messages = []
if "last_df"  not in st.session_state: st.session_state.last_df  = None

REQUIRED = ["OPENAI_API_KEY", "QDRANT_URL", "QDRANT_API_KEY"]
missing  = [v for v in REQUIRED if not os.environ.get(v)]


# ── Nav ────────────────────────────────────────────────────────────────────
pill = ('<div class="nav-pill"><div class="nav-dot"></div>Live</div>' if not missing
        else '<div class="nav-pill" style="background:#fef2f2;border-color:#fecaca;color:#dc2626;"><div class="nav-dot" style="background:#dc2626"></div>Setup needed</div>')

st.markdown(f"""
<div class="topnav">
  <div class="nav-brand">
    <div class="nav-logo">✉</div>
    <div>
      <div class="nav-title">Email Intelligence</div>
      <div class="nav-subtitle">Mailchimp · BigQuery · AI</div>
    </div>
  </div>
  {pill}
</div>
<div class="page-body">
""", unsafe_allow_html=True)


# ── Data loaders ───────────────────────────────────────────────────────────
def parse_md_table(raw):
    if not raw or "|" not in raw: return []
    rows = [r for r in raw.split("\n") if r.startswith("|") and "---" not in r]
    if len(rows) < 2: return []
    headers = [h.strip() for h in rows[0].split("|")[1:-1]]
    result  = []
    for row in rows[1:]:
        vals = [v.strip() for v in row.split("|")[1:-1]]
        if len(vals) == len(headers):
            result.append(dict(zip(headers, vals)))
    return result

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
            SELECT e.hook_type,
              COUNT(*) as campaigns,
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


# ── KPIs ───────────────────────────────────────────────────────────────────
stat_vals = ["—","—","—","—"]
if not missing:
    parsed = parse_md_table(load_stats())
    if parsed:
        stat_vals = list(parsed[0].values())

KPI = [
    ("Campaigns",    "",  "All time",          "d-neu"),
    ("Avg Open Rate","%", "vs industry 21%",   "d-up"),
    ("Avg CTR",      "%", "vs industry 2.6%",  "d-up"),
    ("Hook Types",   "",  "GPT classified",     "d-neu"),
]
cards_html = "".join(
    f'<div class="kpi-card"><div class="kpi-val">{stat_vals[i]}{s}</div>'
    f'<div class="kpi-lbl">{l}</div>'
    f'<div class="kpi-delta {cls}">{hint}</div></div>'
    for i,(l,s,hint,cls) in enumerate(KPI)
)
st.markdown(f'<div class="kpi-grid">{cards_html}</div>', unsafe_allow_html=True)


# ── Charts ─────────────────────────────────────────────────────────────────
if not missing:
    try:
        import plotly.graph_objects as go

        hook_data = parse_md_table(load_hook_data())
        tone_data = parse_md_table(load_tone_data())

        c1, c2 = st.columns([3, 2], gap="medium")

        with c1:
            st.markdown('<div class="chart-card"><div class="sec-hd"><span class="sec-title">Open Rate by Hook Type</span><span class="sec-sub">avg %</span></div>', unsafe_allow_html=True)
            if hook_data:
                hooks  = [d["hook_type"].title() for d in hook_data]
                opens  = [float(d["avg_open"]) for d in hook_data]
                counts = [int(d["campaigns"]) for d in hook_data]
                colors = ["#4f46e5" if o == max(opens) else "#c7d2fe" for o in opens]
                fig = go.Figure(go.Bar(
                    x=opens, y=hooks, orientation="h",
                    marker_color=colors,
                    text=[f"{o}%" for o in opens], textposition="outside",
                    customdata=counts,
                    hovertemplate="<b>%{y}</b><br>Open rate: %{x}%<br>Campaigns: %{customdata}<extra></extra>",
                ))
                fig.update_layout(
                    height=260, margin=dict(l=0,r=40,t=4,b=0),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="DM Sans",size=12,color="#78716c"),
                    xaxis=dict(showgrid=True, gridcolor="#f0ece4", zeroline=False,
                               ticksuffix="%", range=[0, max(opens)*1.25]),
                    yaxis=dict(showgrid=False), showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                st.caption("Enrichment in progress…")
            st.markdown('</div>', unsafe_allow_html=True)

        with c2:
            st.markdown('<div class="chart-card"><div class="sec-hd"><span class="sec-title">Tone Distribution</span><span class="sec-sub">campaigns</span></div>', unsafe_allow_html=True)
            if tone_data:
                tones  = [d["tone"].title() for d in tone_data]
                counts = [int(d["campaigns"]) for d in tone_data]
                PALETTE = ["#4f46e5","#818cf8","#c7d2fe","#e0e7ff","#6366f1","#a5b4fc","#3730a3","#312e81"]
                fig2 = go.Figure(go.Pie(
                    labels=tones, values=counts, hole=0.55,
                    marker_colors=PALETTE[:len(tones)],
                    textinfo="percent", textfont_size=11,
                    hovertemplate="<b>%{label}</b><br>%{value} campaigns<br>%{percent}<extra></extra>",
                ))
                fig2.update_layout(
                    height=260, margin=dict(l=0,r=0,t=4,b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="DM Sans",size=12,color="#78716c"),
                    legend=dict(orientation="v",x=1.0,y=0.5,font=dict(size=11),bgcolor="rgba(0,0,0,0)"),
                )
                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
            else:
                st.caption("Enrichment in progress…")
            st.markdown('</div>', unsafe_allow_html=True)

    except ImportError:
        st.info("Install plotly: `pip install plotly`")


# ── Divider ────────────────────────────────────────────────────────────────
st.markdown('<div class="divider"></div>', unsafe_allow_html=True)


# ── Chat + Controls ────────────────────────────────────────────────────────
chat_col, ctrl_col = st.columns([5, 1], gap="medium")

with ctrl_col:
    model = st.selectbox("Model", ["gpt-4o-mini","gpt-4o"], index=0, label_visibility="collapsed")
    os.environ["AGENT_MODEL"] = model
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    st.markdown('<div class="btn-flat">', unsafe_allow_html=True)
    if st.button("🗑 Clear", use_container_width=True):
        st.session_state.history  = []
        st.session_state.messages = []
        st.session_state.last_df  = None
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    if st.session_state.last_df is not None:
        df = st.session_state.last_df
        st.download_button("⬇ CSV", df.to_csv(index=False).encode(),
            file_name="export.csv", mime="text/csv", use_container_width=True)
        try:
            import openpyxl
            buf = io.BytesIO()
            df.to_excel(buf, index=False, engine="openpyxl")
            st.download_button("⬇ Excel", buf.getvalue(),
                file_name="export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)
        except ImportError:
            pass

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown('<div style="font-size:11px;font-weight:600;color:#a8a29e;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px;">Filters</div>', unsafe_allow_html=True)
    fh = st.selectbox("Hook", ["Any","curiosity","urgency","social-proof","fear-of-missing-out","story","discount","question"], index=0)
    ft = st.selectbox("Tone", ["Any","casual","formal","playful","urgent","inspirational","informational"], index=0)
    fl = st.selectbox("Lang", ["Any","en","lt","ru","es","pl"], index=0)
    mo = st.slider("Min open %", 0, 100, 0)
    st.session_state.sidebar_filters = {k:v for k,v in {
        "hook_type": None if fh=="Any" else fh,
        "tone":      None if ft=="Any" else ft,
        "language":  None if fl=="Any" else fl,
        "min_open_rate": mo if mo>0 else None,
    }.items() if v is not None}


with chat_col:
    st.markdown('<div class="sec-hd"><span class="sec-title">AI Assistant</span><span class="sec-sub">Ask anything about your campaigns</span></div>', unsafe_allow_html=True)

    SUGGESTIONS = [
        "Top 10 campaigns by open rate",
        "Which hook type works best?",
        "Urgency emails open rate > 30%",
        "CTR by tone comparison",
        "Campaigns by language",
        "Best discount campaigns",
    ]
    if not st.session_state.messages:
        cols = st.columns(3)
        for i, s in enumerate(SUGGESTIONS):
            with cols[i % 3]:
                if st.button(s, key=f"s{i}"):
                    st.session_state.pending_question = s
                    st.rerun()
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    for msg in st.session_state.messages:
        if msg["role"] == "user":
            st.markdown(f'<div class="msg-user">{msg["content"]}</div>', unsafe_allow_html=True)
        else:
            content = msg["content"]
            badges = ""
            if any(k in content.lower() for k in ["open rate","%","avg","count","campaigns","ctr"]):
                badges += '<span class="badge b-sql">⬡ SQL</span>'
            if any(k in content.lower() for k in ["similar","score:","preview:"]):
                badges += '<span class="badge b-rag">◈ RAG</span>'
            if badges: st.markdown(badges, unsafe_allow_html=True)
            st.markdown('<div class="msg-bot">', unsafe_allow_html=True)
            st.markdown(content)
            st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)


# ── Agent ──────────────────────────────────────────────────────────────────
def extract_df(reply):
    try:
        import pandas as pd
        rows = parse_md_table(reply)
        if rows: return pd.DataFrame(rows)
    except: pass
    return None

def run_question(question):
    with chat_col:
        st.markdown(f'<div class="msg-user">{question}</div>', unsafe_allow_html=True)
    st.session_state.messages.append({"role":"user","content":question})

    with chat_col:
        ph = st.empty()
        ph.markdown('<div class="thinking"><div class="dot"></div><div class="dot"></div><div class="dot"></div><span>Analysing…</span></div>', unsafe_allow_html=True)
        try:
            from agent import run_agent
            filters = st.session_state.get("sidebar_filters", {})
            aug = question
            if filters:
                aug = f"{question}\n[Filters: {', '.join(f'{k}={v}' for k,v in filters.items())}]"
            reply, hist = run_agent(aug, st.session_state.history)
            st.session_state.history = hist
            ph.empty()

            df = extract_df(reply)
            if df is not None: st.session_state.last_df = df

            badges = ""
            if any(k in reply.lower() for k in ["open rate","%","avg","count","ctr"]):
                badges += '<span class="badge b-sql">⬡ SQL</span>'
            if any(k in reply.lower() for k in ["similar","score:","preview:"]):
                badges += '<span class="badge b-rag">◈ RAG</span>'
            if badges: st.markdown(badges, unsafe_allow_html=True)
            st.markdown('<div class="msg-bot">', unsafe_allow_html=True)
            st.markdown(reply)
            st.markdown('</div>', unsafe_allow_html=True)
            st.session_state.messages.append({"role":"assistant","content":reply})
        except Exception as e:
            ph.empty()
            err = f"**Error:** {e}"
            st.markdown(f'<div class="msg-bot">{err}</div>', unsafe_allow_html=True)
            st.session_state.messages.append({"role":"assistant","content":err})

if "pending_question" in st.session_state:
    q = st.session_state.pop("pending_question")
    run_question(q)
    st.rerun()

if prompt := st.chat_input("Ask about your campaigns…"):
    run_question(prompt)

st.markdown('</div>', unsafe_allow_html=True)
