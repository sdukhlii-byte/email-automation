"""
Email Marketing Intelligence — Streamlit UI
"""

import logging
import os
import streamlit as st

logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Email Intelligence",
    page_icon="📬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS — dark analytical aesthetic
# ---------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* Main background */
.stApp {
    background-color: #0d0f14;
    color: #e2e8f0;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #111318;
    border-right: 1px solid #1e2330;
}

/* Chat messages */
.stChatMessage {
    background-color: #151820 !important;
    border: 1px solid #1e2330 !important;
    border-radius: 8px !important;
    margin-bottom: 8px !important;
}

/* User message */
.stChatMessage[data-testid="stChatMessageContent"] {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 14px;
    line-height: 1.6;
}

/* Input box */
.stChatInputContainer {
    background-color: #111318 !important;
    border-top: 1px solid #1e2330 !important;
}

textarea[data-testid="stChatInputTextArea"] {
    background-color: #151820 !important;
    border: 1px solid #2a3045 !important;
    color: #e2e8f0 !important;
    font-family: 'IBM Plex Sans', sans-serif !important;
    font-size: 14px !important;
}

/* Metric cards */
.metric-card {
    background: #151820;
    border: 1px solid #1e2330;
    border-radius: 8px;
    padding: 16px 20px;
    text-align: center;
}
.metric-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 28px;
    font-weight: 500;
    color: #60a5fa;
    line-height: 1;
}
.metric-label {
    font-size: 11px;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 6px;
}

/* Header */
.app-header {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 4px 0 20px 0;
    border-bottom: 1px solid #1e2330;
    margin-bottom: 20px;
}
.app-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 18px;
    font-weight: 500;
    color: #e2e8f0;
    letter-spacing: -0.02em;
}
.app-subtitle {
    font-size: 12px;
    color: #475569;
    margin-top: 2px;
}

/* Suggested questions */
.suggestion-btn {
    background: #151820;
    border: 1px solid #1e2330;
    border-radius: 6px;
    padding: 10px 14px;
    font-size: 13px;
    color: #94a3b8;
    cursor: pointer;
    width: 100%;
    text-align: left;
    transition: all 0.15s;
    font-family: 'IBM Plex Sans', sans-serif;
}
.suggestion-btn:hover {
    border-color: #3b82f6;
    color: #e2e8f0;
    background: #1a1f2e;
}

/* Tool indicator */
.tool-badge {
    display: inline-block;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    padding: 2px 8px;
    border-radius: 4px;
    margin-right: 6px;
}
.tool-sql { background: #1e3a5f; color: #60a5fa; border: 1px solid #2563eb33; }
.tool-rag { background: #1a3a2a; color: #4ade80; border: 1px solid #16a34a33; }

/* Scrollbar */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #0d0f14; }
::-webkit-scrollbar-thumb { background: #2a3045; border-radius: 2px; }

/* Status bar */
.status-bar {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: #475569;
    padding: 6px 12px;
    background: #111318;
    border: 1px solid #1e2330;
    border-radius: 6px;
}
.status-dot {
    display: inline-block;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #4ade80;
    margin-right: 6px;
    vertical-align: middle;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Session state init
# ---------------------------------------------------------------------------
if "history" not in st.session_state:
    st.session_state.history = []
if "messages" not in st.session_state:
    st.session_state.messages = []  # display messages
if "agent_ready" not in st.session_state:
    st.session_state.agent_ready = False


# ---------------------------------------------------------------------------
# Check env vars
# ---------------------------------------------------------------------------
REQUIRED_VARS = ["OPENAI_API_KEY", "QDRANT_URL", "QDRANT_API_KEY"]
missing = [v for v in REQUIRED_VARS if not os.environ.get(v)]


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style="padding: 4px 0 16px 0;">
        <div style="font-family: 'IBM Plex Mono', monospace; font-size: 13px; color: #60a5fa; font-weight: 500;">
            📬 EMAIL INTEL
        </div>
        <div style="font-size: 11px; color: #475569; margin-top: 4px;">
            AI-powered campaign analysis
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # Status
    if missing:
        st.error(f"Missing env vars: {', '.join(missing)}")
    else:
        st.markdown("""
        <div class="status-bar">
            <span class="status-dot"></span>Connected to BQ + Qdrant
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Model selector
    model = st.selectbox(
        "Model",
        ["gpt-4o-mini", "gpt-4o"],
        index=0,
        help="gpt-4o-mini is faster and cheaper; gpt-4o for complex analysis"
    )
    os.environ["AGENT_MODEL"] = model

    st.markdown("---")

    # Quick filters for RAG
    st.markdown("**Quick filters**")
    st.caption("Applied to semantic search")

    filter_hook = st.selectbox(
        "Hook type", ["Any", "curiosity", "urgency", "social-proof",
                      "fear-of-missing-out", "story", "discount", "question"],
        index=0
    )
    filter_tone = st.selectbox(
        "Tone", ["Any", "casual", "formal", "playful", "urgent",
                 "inspirational", "informational"],
        index=0
    )
    filter_lang = st.selectbox("Language", ["Any", "en", "lt", "ru", "es", "pl"], index=0)
    min_open = st.slider("Min open rate %", 0, 100, 0)

    st.session_state.sidebar_filters = {
        k: v for k, v in {
            "hook_type": None if filter_hook == "Any" else filter_hook,
            "tone": None if filter_tone == "Any" else filter_tone,
            "language": None if filter_lang == "Any" else filter_lang,
            "min_open_rate": min_open if min_open > 0 else None,
        }.items() if v is not None
    }

    st.markdown("---")

    if st.button("🗑 Clear conversation", use_container_width=True):
        st.session_state.history = []
        st.session_state.messages = []
        st.rerun()


# ---------------------------------------------------------------------------
# Main area — header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="app-header">
    <div>
        <div class="app-title">Email Intelligence</div>
        <div class="app-subtitle">Ask anything about your Mailchimp campaigns</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Quick stats row (lazy load)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def load_stats():
    try:
        from bigquery_tools import run_sql
        result = run_sql("""
            SELECT
              COUNT(*) as total,
              ROUND(AVG(open_rate_percent), 1) as avg_open,
              ROUND(AVG(ctr_percent), 2) as avg_ctr,
              COUNT(DISTINCT e.hook_type) as hook_types
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e USING (campaign_id)
        """, max_rows=1)
        return result
    except Exception:
        return None

if not missing:
    with st.container():
        cols = st.columns(4)
        stats_raw = load_stats()

        # Parse first data row from markdown table
        stat_vals = ["—", "—", "—", "—"]
        if stats_raw and "|" in stats_raw:
            rows = [r for r in stats_raw.split("\n") if r.startswith("|") and "---" not in r]
            if len(rows) >= 2:
                vals = [v.strip() for v in rows[1].split("|")[1:-1]]
                stat_vals = vals if len(vals) == 4 else stat_vals

        labels = ["Campaigns", "Avg Open Rate", "Avg CTR", "Hook Types"]
        suffixes = ["", "%", "%", ""]
        for col, val, label, suffix in zip(cols, stat_vals, labels, suffixes):
            with col:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{val}{suffix}</div>
                    <div class="metric-label">{label}</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Suggested questions (shown when history is empty)
# ---------------------------------------------------------------------------
SUGGESTIONS = [
    "What are the top 10 campaigns by open rate?",
    "Compare performance by hook type — which works best?",
    "Find me urgency-based email examples with open rate > 30%",
    "What subject line patterns correlate with high CTR?",
    "Show campaigns in Lithuanian with curiosity hooks",
    "Which campaigns had the highest unsubscribe rate and why?",
]

if not st.session_state.messages:
    st.markdown("**Suggested questions**")
    cols = st.columns(2)
    for i, suggestion in enumerate(SUGGESTIONS):
        with cols[i % 2]:
            if st.button(suggestion, key=f"sug_{i}", use_container_width=True):
                st.session_state.pending_question = suggestion
                st.rerun()


# ---------------------------------------------------------------------------
# Chat history display
# ---------------------------------------------------------------------------
for msg in st.session_state.messages:
    role = msg["role"]
    content = msg["content"]
    with st.chat_message(role, avatar="👤" if role == "user" else "📬"):
        st.markdown(content)


# ---------------------------------------------------------------------------
# Handle pending question from suggestion buttons
# ---------------------------------------------------------------------------
if "pending_question" in st.session_state:
    question = st.session_state.pop("pending_question")
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user", avatar="👤"):
        st.markdown(question)

    with st.chat_message("assistant", avatar="📬"):
        placeholder = st.empty()
        placeholder.markdown("_Thinking..._")

        try:
            from agent import run_agent
            # Inject sidebar filters into question context if set
            filters = st.session_state.get("sidebar_filters", {})
            augmented = question
            if filters:
                filter_str = ", ".join(f"{k}={v}" for k, v in filters.items())
                augmented = f"{question}\n[Active filters: {filter_str}]"

            reply, updated_history = run_agent(augmented, st.session_state.history)
            st.session_state.history = updated_history
            placeholder.markdown(reply)
            st.session_state.messages.append({"role": "assistant", "content": reply})
        except Exception as e:
            err = f"❌ Agent error: {e}"
            placeholder.markdown(err)
            st.session_state.messages.append({"role": "assistant", "content": err})

    st.rerun()


# ---------------------------------------------------------------------------
# Chat input
# ---------------------------------------------------------------------------
if prompt := st.chat_input("Ask about your campaigns..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user", avatar="👤"):
        st.markdown(prompt)

    with st.chat_message("assistant", avatar="📬"):
        placeholder = st.empty()
        placeholder.markdown("_Thinking..._")

        try:
            from agent import run_agent
            filters = st.session_state.get("sidebar_filters", {})
            augmented = prompt
            if filters:
                filter_str = ", ".join(f"{k}={v}" for k, v in filters.items())
                augmented = f"{prompt}\n[Active filters: {filter_str}]"

            reply, updated_history = run_agent(augmented, st.session_state.history)
            st.session_state.history = updated_history
            placeholder.markdown(reply)
            st.session_state.messages.append({"role": "assistant", "content": reply})
        except Exception as e:
            err = f"❌ Agent error: {e}"
            placeholder.markdown(err)
            st.session_state.messages.append({"role": "assistant", "content": err})
