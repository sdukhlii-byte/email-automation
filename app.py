"""
Email Marketing Intelligence — Streamlit UI
Clean minimalist design, Notion/Linear aesthetic
"""

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

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Geist:wght@300;400;500;600&family=Geist+Mono:wght@400;500&display=swap');

*, html, body, [class*="css"] {
    font-family: 'Geist', -apple-system, sans-serif !important;
    -webkit-font-smoothing: antialiased;
}

/* ── Base ── */
.stApp { background: #ffffff; color: #1a1a1a; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #fafafa !important;
    border-right: 1px solid #e5e5e5 !important;
    padding-top: 0 !important;
}
section[data-testid="stSidebar"] > div { padding: 20px 16px; }

/* ── Sidebar text ── */
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span {
    color: #6b7280 !important;
    font-size: 12px !important;
    font-weight: 500 !important;
    letter-spacing: 0.02em;
    text-transform: uppercase;
}
section[data-testid="stSidebar"] .stSelectbox > div > div {
    background: #fff !important;
    border: 1px solid #e5e5e5 !important;
    border-radius: 8px !important;
    color: #1a1a1a !important;
    font-size: 13px !important;
    font-weight: 400 !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
}
section[data-testid="stSidebar"] .stSlider { margin-top: 4px; }

/* ── Main area padding ── */
.main .block-container { padding: 32px 40px 80px 40px; max-width: 900px; }

/* ── Chat messages ── */
.stChatMessage {
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
    margin-bottom: 24px !important;
}

/* ── User bubble ── */
[data-testid="stChatMessageContent"] {
    font-size: 14px !important;
    line-height: 1.7 !important;
    color: #1a1a1a !important;
}

/* ── Chat input ── */
.stChatInputContainer {
    background: #fff !important;
    border-top: 1px solid #e5e5e5 !important;
    padding: 12px 40px !important;
}
textarea[data-testid="stChatInputTextArea"] {
    background: #fafafa !important;
    border: 1px solid #e5e5e5 !important;
    border-radius: 10px !important;
    color: #1a1a1a !important;
    font-family: 'Geist', sans-serif !important;
    font-size: 14px !important;
    padding: 10px 14px !important;
}
textarea[data-testid="stChatInputTextArea"]:focus {
    border-color: #1a1a1a !important;
    box-shadow: none !important;
    outline: none !important;
}

/* ── Metric cards ── */
.metric-row { display: flex; gap: 12px; margin-bottom: 32px; }
.metric-card {
    flex: 1;
    background: #fafafa;
    border: 1px solid #e5e5e5;
    border-radius: 10px;
    padding: 16px 20px;
}
.metric-val {
    font-family: 'Geist Mono', monospace;
    font-size: 22px;
    font-weight: 500;
    color: #1a1a1a;
    letter-spacing: -0.02em;
    line-height: 1;
}
.metric-lbl {
    font-size: 11px;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-top: 6px;
    font-weight: 500;
}

/* ── Section label ── */
.section-label {
    font-size: 11px;
    font-weight: 600;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 10px;
}

/* ── Suggestion chips ── */
.chip-row { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 32px; }
.chip {
    background: #fff;
    border: 1px solid #e5e5e5;
    border-radius: 100px;
    padding: 6px 14px;
    font-size: 13px;
    color: #374151;
    cursor: pointer;
    transition: all 0.12s;
    white-space: nowrap;
}
.chip:hover { border-color: #1a1a1a; color: #1a1a1a; background: #f9f9f9; }

/* ── Tool badge ── */
.tool-badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-family: 'Geist Mono', monospace;
    font-size: 10px;
    font-weight: 500;
    padding: 2px 8px;
    border-radius: 4px;
    margin-right: 6px;
    margin-bottom: 10px;
    vertical-align: middle;
}
.badge-sql { background: #eff6ff; color: #3b82f6; border: 1px solid #dbeafe; }
.badge-rag { background: #f0fdf4; color: #16a34a; border: 1px solid #dcfce7; }

/* ── Thinking indicator ── */
.thinking {
    display: flex;
    align-items: center;
    gap: 8px;
    color: #9ca3af;
    font-size: 13px;
}
.dot {
    width: 5px; height: 5px; border-radius: 50%;
    background: #d1d5db;
    animation: pulse 1.2s ease-in-out infinite;
}
.dot:nth-child(2) { animation-delay: 0.2s; }
.dot:nth-child(3) { animation-delay: 0.4s; }
@keyframes pulse {
    0%, 80%, 100% { opacity: 0.3; transform: scale(0.8); }
    40% { opacity: 1; transform: scale(1); }
}

/* ── Divider ── */
hr { border: none; border-top: 1px solid #f3f4f6; margin: 20px 0; }

/* ── Streamlit button override for suggestion chips ── */
.stButton > button {
    background: #fff !important;
    border: 1px solid #e5e5e5 !important;
    border-radius: 100px !important;
    color: #374151 !important;
    font-size: 13px !important;
    font-weight: 400 !important;
    padding: 5px 16px !important;
    transition: all 0.12s !important;
    white-space: nowrap !important;
    width: auto !important;
}
.stButton > button:hover {
    border-color: #1a1a1a !important;
    color: #1a1a1a !important;
    background: #fafafa !important;
}

/* ── Clear button ── */
.clear-btn > button {
    background: transparent !important;
    border: 1px solid #e5e5e5 !important;
    border-radius: 8px !important;
    color: #9ca3af !important;
    font-size: 12px !important;
    width: 100% !important;
}
.clear-btn > button:hover {
    border-color: #ef4444 !important;
    color: #ef4444 !important;
}

/* ── Markdown tables ── */
table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    margin-top: 8px;
    border-radius: 8px;
    overflow: hidden;
    border: 1px solid #e5e5e5;
}
thead tr { background: #fafafa; }
th {
    text-align: left;
    padding: 10px 14px;
    font-size: 11px;
    font-weight: 600;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 1px solid #e5e5e5;
}
td {
    padding: 10px 14px;
    border-bottom: 1px solid #f3f4f6;
    color: #374151;
    vertical-align: top;
    max-width: 280px;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: #fafafa; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #e5e5e5; border-radius: 2px; }
::-webkit-scrollbar-thumb:hover { background: #d1d5db; }

/* ── Status dot ── */
.status { display: flex; align-items: center; gap: 6px; font-size: 12px; color: #6b7280; }
.status-dot-green {
    width: 6px; height: 6px; border-radius: 50%;
    background: #22c55e; flex-shrink: 0;
}
.status-dot-red {
    width: 6px; height: 6px; border-radius: 50%;
    background: #ef4444; flex-shrink: 0;
}

/* ── User message box ── */
.user-msg {
    background: #f9fafb;
    border: 1px solid #e5e5e5;
    border-radius: 10px;
    padding: 12px 16px;
    font-size: 14px;
    color: #1a1a1a;
    line-height: 1.6;
    margin-bottom: 4px;
}

/* hide streamlit branding */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Session state ──────────────────────────────────────────────────────────
if "history" not in st.session_state:
    st.session_state.history = []
if "messages" not in st.session_state:
    st.session_state.messages = []


# ── Env check ─────────────────────────────────────────────────────────────
REQUIRED = ["OPENAI_API_KEY", "QDRANT_URL", "QDRANT_API_KEY"]
missing = [v for v in REQUIRED if not os.environ.get(v)]


# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    # Logo / title
    st.markdown("""
    <div style="padding: 8px 0 20px 0; border-bottom: 1px solid #e5e5e5; margin-bottom: 20px;">
        <div style="font-size: 15px; font-weight: 600; color: #1a1a1a; letter-spacing: -0.02em;">
            ✉ Email Intelligence
        </div>
        <div style="font-size: 12px; color: #9ca3af; margin-top: 3px;">
            Mailchimp campaign analysis
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Connection status
    if missing:
        st.markdown(f"""
        <div class="status">
            <div class="status-dot-red"></div>
            Missing: {', '.join(missing)}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="status">
            <div class="status-dot-green"></div>
            BigQuery · Qdrant · OpenAI
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    # Model
    st.markdown('<div class="section-label">Model</div>', unsafe_allow_html=True)
    model = st.selectbox(
        "model", ["gpt-4o-mini", "gpt-4o"],
        index=0, label_visibility="collapsed"
    )
    os.environ["AGENT_MODEL"] = model

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown('<div style="border-top:1px solid #e5e5e5; margin-bottom:16px;"></div>', unsafe_allow_html=True)

    # Filters
    st.markdown('<div class="section-label">Filters for semantic search</div>', unsafe_allow_html=True)

    filter_hook = st.selectbox(
        "Hook type", ["Any", "curiosity", "urgency", "social-proof",
                      "fear-of-missing-out", "story", "discount", "question"],
        index=0, label_visibility="visible"
    )
    filter_tone = st.selectbox(
        "Tone", ["Any", "casual", "formal", "playful", "urgent",
                 "inspirational", "informational"],
        index=0, label_visibility="visible"
    )
    filter_lang = st.selectbox(
        "Language", ["Any", "en", "lt", "ru", "es", "pl"],
        index=0, label_visibility="visible"
    )
    min_open = st.slider("Min open rate %", 0, 100, 0)

    st.session_state.sidebar_filters = {
        k: v for k, v in {
            "hook_type": None if filter_hook == "Any" else filter_hook,
            "tone": None if filter_tone == "Any" else filter_tone,
            "language": None if filter_lang == "Any" else filter_lang,
            "min_open_rate": min_open if min_open > 0 else None,
        }.items() if v is not None
    }

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown('<div style="border-top:1px solid #e5e5e5; margin-bottom:16px;"></div>', unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="clear-btn">', unsafe_allow_html=True)
        if st.button("Clear conversation", use_container_width=True):
            st.session_state.history = []
            st.session_state.messages = []
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)


# ── Stats row ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_stats():
    try:
        from bigquery_tools import run_sql
        return run_sql("""
            SELECT
              COUNT(*) as total,
              ROUND(AVG(k.open_rate_percent), 1) as avg_open,
              ROUND(AVG(k.ctr_percent), 2) as avg_ctr,
              COUNT(DISTINCT e.hook_type) as hook_types
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              USING (campaign_id)
        """, max_rows=1)
    except Exception:
        return None

if not missing:
    stat_vals = ["—", "—", "—", "—"]
    stats_raw = load_stats()
    if stats_raw and "|" in stats_raw:
        rows = [r for r in stats_raw.split("\n") if r.startswith("|") and "---" not in r]
        if len(rows) >= 2:
            vals = [v.strip() for v in rows[1].split("|")[1:-1]]
            if len(vals) == 4:
                stat_vals = vals

    labels   = ["Campaigns", "Avg Open Rate", "Avg CTR", "Hook Types"]
    suffixes = ["", "%", "%", ""]

    cards_html = '<div class="metric-row">'
    for val, label, suffix in zip(stat_vals, labels, suffixes):
        cards_html += f"""
        <div class="metric-card">
            <div class="metric-val">{val}{suffix}</div>
            <div class="metric-lbl">{label}</div>
        </div>"""
    cards_html += '</div>'
    st.markdown(cards_html, unsafe_allow_html=True)


# ── Suggestions (empty state) ──────────────────────────────────────────────
SUGGESTIONS = [
    "Top 10 campaigns by open rate",
    "Compare hook types — which works best?",
    "Urgency emails with open rate > 30%",
    "Subject line patterns for high CTR",
    "Lithuanian campaigns with curiosity hooks",
    "Highest unsubscribe rate campaigns",
    "Best performing discount campaigns",
    "Campaigns by language breakdown",
]

if not st.session_state.messages:
    st.markdown('<div class="section-label">Suggested questions</div>', unsafe_allow_html=True)

    # 4 chips per row
    row1 = st.columns(4)
    row2 = st.columns(4)
    for i, suggestion in enumerate(SUGGESTIONS):
        col = row1[i] if i < 4 else row2[i - 4]
        with col:
            if st.button(suggestion, key=f"sug_{i}"):
                st.session_state.pending_question = suggestion
                st.rerun()

    st.markdown("<div style='height:32px'></div>", unsafe_allow_html=True)


# ── Chat history ───────────────────────────────────────────────────────────
for msg in st.session_state.messages:
    role = msg["role"]
    content = msg["content"]
    if role == "user":
        st.markdown(f'<div class="user-msg">{content}</div>', unsafe_allow_html=True)
    else:
        # Detect tool usage hints in content and show badges
        badges = ""
        if any(kw in content.lower() for kw in ["open rate", "ctr", "campaigns", "avg", "%", "hook"]):
            badges += '<span class="tool-badge badge-sql">⬡ SQL</span>'
        if any(kw in content.lower() for kw in ["similar", "semantic", "found", "score", "preview"]):
            badges += '<span class="tool-badge badge-rag">◈ RAG</span>'

        with st.chat_message("assistant", avatar="✉"):
            if badges:
                st.markdown(badges, unsafe_allow_html=True)
            st.markdown(content)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)


# ── Agent runner ───────────────────────────────────────────────────────────
def run_question(question: str):
    st.markdown(f'<div class="user-msg">{question}</div>', unsafe_allow_html=True)
    st.session_state.messages.append({"role": "user", "content": question})

    with st.chat_message("assistant", avatar="✉"):
        thinking = st.empty()
        thinking.markdown("""
        <div class="thinking">
            <div class="dot"></div><div class="dot"></div><div class="dot"></div>
            <span>Thinking…</span>
        </div>
        """, unsafe_allow_html=True)

        try:
            from agent import run_agent
            filters = st.session_state.get("sidebar_filters", {})
            augmented = question
            if filters:
                filter_str = ", ".join(f"{k}={v}" for k, v in filters.items())
                augmented = f"{question}\n[Active filters: {filter_str}]"

            reply, updated_history = run_agent(augmented, st.session_state.history)
            st.session_state.history = updated_history

            thinking.empty()

            # Tool badges
            badges = ""
            if any(kw in reply.lower() for kw in ["open rate", "ctr", "%", "avg", "hook_type", "campaigns"]):
                badges += '<span class="tool-badge badge-sql">⬡ SQL</span>'
            if any(kw in reply.lower() for kw in ["similar", "semantic", "score:", "preview:"]):
                badges += '<span class="tool-badge badge-rag">◈ RAG</span>'
            if badges:
                st.markdown(badges, unsafe_allow_html=True)

            st.markdown(reply)
            st.session_state.messages.append({"role": "assistant", "content": reply})

        except Exception as e:
            thinking.empty()
            err = f"**Error:** {e}"
            st.markdown(err)
            st.session_state.messages.append({"role": "assistant", "content": err})


# ── Pending question from chip ─────────────────────────────────────────────
if "pending_question" in st.session_state:
    question = st.session_state.pop("pending_question")
    run_question(question)
    st.rerun()


# ── Chat input ─────────────────────────────────────────────────────────────
if prompt := st.chat_input("Ask about your campaigns…"):
    run_question(prompt)
