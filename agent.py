"""
Email Marketing Agent
Orchestrates sql_tool and rag_tool via OpenAI function calling.
Stateless — caller manages conversation history.

v5 (deterministic refactor):
- CHANGE 1: temperature=0 for all LLM calls that generate SQL
- CHANGE 2: Question normalisation (_normalise_question) before LLM dispatch
- CHANGE 3: In-memory response cache with TTL=1h (_ResponseCache)
- CHANGE 4: SQL generation and interpretation split into two separate LLM calls
            _chat_sql  → temperature=0,   only produces SQL (JSON output)
            _chat_interp → temperature=0.7, only interprets BQ results
- CHANGE 5: Strict SQL-generation system prompt with JSON-only output format,
            schema embedded, error JSON when params unknown

Original logic preserved everywhere else:
  - Tool dispatch (run_sql / rag_search)
  - History trimming (_trim_history)
  - Streaming generator (run_agent_stream)
  - HISTORY_SENTINEL pass-through
  - MAX_TOOL_ROUNDS guard
  - Chart / Period markers in interpretation prompt
"""

import hashlib
import json
import logging
import os
import re
import time
from typing import Generator

import requests

from bigquery_tools import SQL_TOOL_SPEC, get_schema, run_sql
from rag_tools import RAG_TOOL_SPEC, rag_search

log = logging.getLogger(__name__)

OPENAI_API_KEY      = os.environ["OPENAI_API_KEY"]
AGENT_MODEL         = os.getenv("AGENT_MODEL", "gpt-4o-mini")
MAX_TOOL_ROUNDS     = int(os.getenv("MAX_TOOL_ROUNDS", "6"))
MAX_HISTORY_MSGS    = int(os.getenv("MAX_HISTORY_MSGS", "20"))
_HISTORY_CHAR_LIMIT = int(os.getenv("HISTORY_CHAR_LIMIT", "60000"))

# Sentinel prefix used to pass history through the streaming generator.
# Never visible to the end user.
_HISTORY_SENTINEL = "\x00HISTORY\x00"


# ===========================================================================
# CHANGE 3 — In-memory response cache (TTL configurable, default 1 h)
# ===========================================================================
class _ResponseCache:
    """
    Thread-unsafe single-process cache.
    Key   = SHA-256 of normalised question (first 16 hex chars)
    Value = (timestamp_float, reply_str, history_list)
    TTL   = 3600 s by default; override with env CACHE_TTL
    """
    TTL = int(os.getenv("CACHE_TTL", "3600"))

    def __init__(self) -> None:
        self._store: dict[str, tuple[float, str, list]] = {}

    def _key(self, normalised: str) -> str:
        return hashlib.sha256(normalised.encode()).hexdigest()[:16]

    def get(self, normalised: str) -> tuple[str, list] | None:
        k = self._key(normalised)
        entry = self._store.get(k)
        if entry and time.time() - entry[0] < self.TTL:
            log.info("Cache HIT key=%s", k)
            return entry[1], entry[2]
        return None

    def set(self, normalised: str, reply: str, history: list) -> None:
        k = self._key(normalised)
        self._store[k] = (time.time(), reply, history)
        log.info("Cache SET key=%s", k)

    def invalidate(self, normalised: str) -> None:
        self._store.pop(self._key(normalised), None)


_cache = _ResponseCache()


# ===========================================================================
# CHANGE 2 — Question normalisation
# ===========================================================================

# Synonym map: alias → canonical form (both lowercase)
_SYNONYMS: dict[str, str] = {
    # open rate
    "openrate":           "open_rate",
    "open rate":          "open_rate",
    "открываемость":      "open_rate",
    "open_rate_percent":  "open_rate",
    "open rates":         "open_rate",
    # ctr
    "click rate":         "ctr",
    "click-through rate": "ctr",
    "кликабельность":     "ctr",
    "ctr_percent":        "ctr",
    # campaign
    "кампания":           "campaign",
    "кампании":           "campaigns",
    "рассылка":           "campaign",
    "рассылки":           "campaigns",
    # subject
    "тема письма":        "subject_line",
    "subject":            "subject_line",
    # hook / tone
    "hook":               "hook_type",
    "тон":                "tone",
}

# Filler words/phrases to remove before LLM dispatch
_FILLER = re.compile(
    r"\b(пожалуйста|please|покажи\s+мне|можешь\s+показать|можешь|"
    r"скажи|tell\s+me|show\s+me|can\s+you|could\s+you|"
    r"would\s+you|give\s+me|get\s+me|i\s+want\s+to\s+know|"
    r"i\s+need|мне\s+нужно|мне\s+нужны)\b",
    re.IGNORECASE,
)


def _normalise_question(question: str) -> str:
    """
    1. Strip + lowercase
    2. Remove filler phrases
    3. Substitute synonyms
    4. Collapse whitespace
    """
    q = question.strip().lower()
    q = _FILLER.sub("", q)
    for alias, canonical in _SYNONYMS.items():
        q = q.replace(alias, canonical)
    q = re.sub(r"\s{2,}", " ", q).strip()
    return q


# ===========================================================================
# History trimming (unchanged from v4)
# ===========================================================================
def _trim_history(history: list[dict]) -> list[dict]:
    if not history:
        return history
    if len(history) > MAX_HISTORY_MSGS:
        history = [history[0]] + history[-(MAX_HISTORY_MSGS - 1):]
    while len(history) > 2:
        if sum(len(json.dumps(m)) for m in history) <= _HISTORY_CHAR_LIMIT:
            break
        history = [history[0]] + history[2:]
    return history


# ===========================================================================
# CHANGE 5 — Strict SQL-generation system prompt
# ===========================================================================
def _build_sql_system_prompt() -> str:
    return f"""You are a BigQuery SQL generator for an email marketing database.

YOUR ONLY JOB: produce a valid BigQuery SELECT query that answers the user question.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATABASE SCHEMA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{get_schema()}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT RULES — read carefully
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Respond ONLY with a single JSON object. No markdown. No prose. No explanation.

2. SQL success:
   {{"sql": "<complete SELECT statement>", "params": {{}}}}

3. Missing parameter (e.g. a campaign name you were not given):
   {{"error": "missing_parameter", "needs": "<describe what is missing>"}}

4. Semantic / similarity question (find emails like X, similar style, etc.):
   {{"rag": true, "question": "<rephrase for vector search>"}}

5. NEVER guess or hallucinate table/column names. Use only the schema above.
6. ALWAYS add LIMIT N when the user asks for top-N or a list (default LIMIT 10).
7. ALWAYS use fully qualified table names:
   `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase`
8. ALWAYS alias tables (k, e, r) and prefix every column: k.SubjectLine, e.hook_type.
9. Use LEFT JOIN for EmailEnrichment so NULL-enriched rows appear.
10. NEVER add WHERE e.hook_type IS NOT NULL unless the user explicitly asks.

Output JSON only. No preamble."""


# ===========================================================================
# CHANGE 4 — Interpretation-only system prompt
# ===========================================================================
def _build_interp_system_prompt() -> str:
    return """You are an expert email marketing analyst.
A SQL query has already been run. You receive the raw results.
Your job: interpret and present them clearly.

RULES:
- Do NOT generate SQL. Do NOT show SQL. Do NOT mention SQL.
- Be numbers-first and concise.
- Include subject line, open rate, CTR, hook type, tone when relevant.

RESPONSE FORMAT — pick ONE:

1. CONVERSATIONAL (greetings, single facts):
   Plain prose, max 3 sentences. No tables.

2. ANALYTICAL (rankings, trends, top-N):
   - One headline insight sentence.
   - ONE artifact: markdown table (≤10 rows, ≤5 cols) OR chart marker (not both).

PERIOD HONESTY:
- Say "all-time" if no date filter was applied.
- Append on its own line when relevant:
  <<<PERIOD{"from":"YYYY-MM-DD","to":"YYYY-MM-DD","rows":N,"label":"last 7 days"}>>>

CHART MARKER (optional):
<<<CHART{"type":"bar","title":"...","data":[...],"x_key":"...","y_key":"..."}>>>
- type: "bar" | "line" | "pie". Max 20 points.

Reply in the user's language."""


# ===========================================================================
# General agent system prompt (multi-tool fallback: RAG + complex queries)
# ===========================================================================
def _build_system_prompt() -> str:
    return f"""You are an expert email marketing analyst with access to a Mailchimp \
campaigns database (content, performance metrics, AI classifications).

Tools available:
1. sql_tool — aggregations, rankings, trends, metric comparisons
2. rag_tool — semantic similarity search by topic or style

Always ground answers in real data. Show subject line, open rate, CTR, hook type, tone.
Be concise and numbers-first.

Database schema:
{get_schema()}

SQL QUERY RULES:
1. ALWAYS add LIMIT N for top-N or ranked lists.
2. NEVER filter WHERE e.hook_type IS NOT NULL unless user asks.
3. Prefix every column: k.SubjectLine, e.hook_type etc.
4. Fully qualified table names: `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k

RESPONSE FORMAT — one of:
1. CONVERSATIONAL: plain prose, max 3 sentences.
2. ANALYTICAL: headline + ONE table OR chart marker.
   Tables: GitHub markdown, ≤10 rows, ≤5 cols.

PERIOD HONESTY: append <<<PERIOD{{"from":"...","to":"...","rows":N,"label":"..."}}>>>
CHART MARKER:   <<<CHART{{"type":"bar","title":"...","data":[...],"x_key":"...","y_key":"..."}}>>>

Reply in the user's language."""


TOOLS = [SQL_TOOL_SPEC, RAG_TOOL_SPEC]


# ===========================================================================
# CHANGE 1 + 4 — Three focused LLM call functions
# ===========================================================================

def _chat_sql(messages: list[dict]) -> requests.Response:
    """
    CHANGE 1 + 4 — SQL generation only.
    temperature=0: fully deterministic, JSON-only output, no tools attached.
    """
    return requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": AGENT_MODEL,
            "messages": messages,
            "temperature": 0,       # CHANGE 1: deterministic SQL
            "max_tokens": 1000,
            "stream": False,
        },
        timeout=60,
    )


def _chat_interp(messages: list[dict], stream: bool = False) -> requests.Response:
    """
    CHANGE 4 — Interpretation only.
    temperature=0.7: natural language variety, no SQL hallucination risk.
    Receives BQ results, NOT raw SQL.
    """
    return requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": AGENT_MODEL,
            "messages": messages,
            "temperature": 0.7,     # CHANGE 4: readable interpretation
            "max_tokens": 2000,
            "stream": stream,
        },
        timeout=120,
        stream=stream,
    )


def _chat(messages: list[dict], stream: bool = False) -> requests.Response:
    """
    Multi-tool orchestration (RAG fallback).
    CHANGE 1: temperature lowered to 0 (was 0.2) for deterministic tool selection.
    """
    return requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": AGENT_MODEL,
            "messages": messages,
            "tools": TOOLS,
            "tool_choice": "auto",
            "temperature": 0,       # CHANGE 1: was 0.2
            "max_tokens": 2000,
            "stream": stream,
        },
        timeout=120,
        stream=stream,
    )


# ===========================================================================
# Tool dispatch (unchanged from v4)
# ===========================================================================
def _dispatch_tool(name: str, arguments: str) -> str:
    try:
        args = json.loads(arguments)
    except json.JSONDecodeError:
        return f"ERROR: Invalid JSON arguments: {arguments[:200]}"

    if name == "sql_tool":
        query = args.get("query", "")
        log.info("sql_tool: %s", query[:120])
        return run_sql(query)

    if name == "rag_tool":
        question = args.get("question", "")
        filters  = args.get("filters")
        top_k    = int(args.get("top_k", 5))
        log.info("rag_tool: %s | filters=%s", question[:80], filters)
        return rag_search(question, filters=filters, top_k=top_k)

    return f"ERROR: Unknown tool '{name}'"


# ===========================================================================
# CHANGE 4 — Two-phase SQL→Interpret pipeline
# ===========================================================================
def _run_sql_then_interpret(normalised_q: str, original_message: str) -> str | None:
    """
    Phase 1: SQL-gen model (temp=0) → {"sql": "...", "params": {}}
    Phase 2: BigQuery execution
    Phase 3: Interpretation model (temp=0.7) → natural language answer

    Returns the interpretation string, or None when the question is
    not SQL-shaped (RAG signal or non-JSON output) so caller falls through
    to the multi-tool agent loop.
    """
    # ---- Phase 1: SQL generation ----
    sql_messages = [
        {"role": "system", "content": _build_sql_system_prompt()},
        {"role": "user",   "content": normalised_q},
    ]
    log.info("SQL-gen: %s", normalised_q[:80])
    resp = _chat_sql(sql_messages)
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"].strip()
    log.debug("SQL-gen raw: %s", raw[:300])

    # Strip accidental markdown fences
    raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("SQL-gen returned non-JSON — falling through to agent loop")
        return None

    # Model signalled missing parameter
    if "error" in parsed:
        log.info("SQL-gen error: %s", parsed)
        return f"I need more information to answer that: {parsed.get('needs', 'unknown parameter')}"

    # Model signalled RAG question — let the agent loop handle it
    if parsed.get("rag"):
        log.info("SQL-gen flagged as RAG — falling through to agent loop")
        return None

    sql = parsed.get("sql", "").strip()
    if not sql:
        log.warning("SQL-gen: empty sql field — falling through")
        return None

    log.info("SQL-gen produced: %s", sql[:120])

    # ---- Phase 2: BigQuery execution ----
    bq_result = run_sql(sql)
    log.info("BQ result: %d chars", len(bq_result))

    # ---- Phase 3: Interpretation ----
    interp_messages = [
        {"role": "system", "content": _build_interp_system_prompt()},
        {
            "role": "user",
            "content": (
                f"User question: {original_message}\n\n"
                f"Query results:\n{bq_result}"
            ),
        },
    ]
    log.info("Interp call (BQ result %d chars)", len(bq_result))
    resp2 = _chat_interp(interp_messages, stream=False)
    resp2.raise_for_status()
    return resp2.json()["choices"][0]["message"]["content"]


# ===========================================================================
# Non-streaming agent — POST /chat
# ===========================================================================
def run_agent(
    user_message: str,
    history: list[dict] | None = None,
) -> tuple[str, list[dict]]:
    """
    Run the agent for one user turn.
    Returns (reply_text, updated_history_without_system_prompt).

    CHANGE 2: normalises the question.
    CHANGE 3: checks cache before any LLM call.
    CHANGE 4: tries two-phase SQL→Interpret pipeline; falls back to multi-tool loop.
    """
    # CHANGE 2
    normalised = _normalise_question(user_message)
    log.info("Normalised: %s", normalised)

    # CHANGE 3: cache lookup (skip when there is conversation history, as
    # context changes the answer)
    if not history:
        cached = _cache.get(normalised)
        if cached:
            return cached  # (reply, history)

    # CHANGE 4: two-phase pipeline
    try:
        two_phase = _run_sql_then_interpret(normalised, user_message)
    except Exception as exc:
        log.warning("Two-phase pipeline error (%s) — falling through", exc)
        two_phase = None

    if two_phase is not None:
        history_out = list(history or []) + [
            {"role": "user",      "content": user_message},
            {"role": "assistant", "content": two_phase},
        ]
        if not history:
            _cache.set(normalised, two_phase, history_out)
        return two_phase, history_out

    # ------------------------------------------------------------------
    # Fall-through: original multi-tool agent loop (RAG / complex queries)
    # ------------------------------------------------------------------
    messages = [{"role": "system", "content": _build_system_prompt()}]
    messages.extend(_trim_history(list(history or [])))
    messages.append({"role": "user", "content": user_message})

    last_content = ""

    for round_num in range(1, MAX_TOOL_ROUNDS + 1):
        log.info("Agent round %d/%d", round_num, MAX_TOOL_ROUNDS)
        resp = _chat(messages)
        resp.raise_for_status()
        data    = resp.json()
        choice  = data["choices"][0]
        message = choice["message"]
        messages.append(message)

        content = message.get("content") or ""
        if content:
            last_content = content

        if choice["finish_reason"] != "tool_calls":
            result_history = messages[1:]
            if not history:
                _cache.set(normalised, content, result_history)
            return content, result_history

        tool_calls = message.get("tool_calls", [])
        log.info("Round %d: %d tool call(s): %s", round_num, len(tool_calls),
                 [tc["function"]["name"] for tc in tool_calls])

        for tc in tool_calls:
            result = _dispatch_tool(tc["function"]["name"], tc["function"]["arguments"])
            log.info("  → %s: %d chars", tc["function"]["name"], len(result))
            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result,
            })

    log.warning("Hit MAX_TOOL_ROUNDS=%d", MAX_TOOL_ROUNDS)
    return last_content or "Reached maximum tool call rounds. Please rephrase.", messages[1:]


# ===========================================================================
# Streaming agent — POST /chat/stream (SSE)
# ===========================================================================
def run_agent_stream(
    user_message: str,
    history: list[dict] | None = None,
) -> Generator[str, None, None]:
    """
    Generator yielding text chunks, then a single sentinel with updated history.

    CHANGE 2+3: normalises question, serves from cache when possible.
    CHANGE 4:   tries two-phase pipeline; if it returns a reply, streams it
                in small chunks so the UI still feels live.

    Sentinel format (never forwarded to client):
        "\x00HISTORY\x00" + json.dumps(updated_history)
    """
    normalised = _normalise_question(user_message)

    # CHANGE 3: stream cached reply directly
    if not history:
        cached = _cache.get(normalised)
        if cached:
            reply, hist = cached
            log.info("Cache HIT — streaming cached reply")
            chunk_size = 50
            for i in range(0, len(reply), chunk_size):
                yield reply[i:i + chunk_size]
            yield _HISTORY_SENTINEL + json.dumps(hist, ensure_ascii=False)
            return

    # CHANGE 4: two-phase pipeline (non-streaming internally)
    try:
        two_phase = _run_sql_then_interpret(normalised, user_message)
    except Exception as exc:
        log.warning("Two-phase pipeline error (%s) — falling through to stream agent", exc)
        two_phase = None

    if two_phase is not None:
        history_out = list(history or []) + [
            {"role": "user",      "content": user_message},
            {"role": "assistant", "content": two_phase},
        ]
        if not history:
            _cache.set(normalised, two_phase, history_out)
        chunk_size = 50
        for i in range(0, len(two_phase), chunk_size):
            yield two_phase[i:i + chunk_size]
        yield _HISTORY_SENTINEL + json.dumps(history_out, ensure_ascii=False)
        return

    # ------------------------------------------------------------------
    # Fall-through: original streaming multi-tool loop
    # ------------------------------------------------------------------
    messages = [{"role": "system", "content": _build_system_prompt()}]
    messages.extend(_trim_history(list(history or [])))
    messages.append({"role": "user", "content": user_message})

    for round_num in range(1, MAX_TOOL_ROUNDS + 1):
        resp = _chat(messages, stream=False)
        resp.raise_for_status()
        data   = resp.json()
        choice = data["choices"][0]
        msg    = choice["message"]
        messages.append(msg)

        if choice["finish_reason"] != "tool_calls":
            # Remove the non-streamed assistant turn, re-request with streaming
            messages.pop()
            # Use interpretation call for the final streamed answer
            stream_resp = _chat_interp(messages, stream=True)
            stream_resp.raise_for_status()

            streamed_content = ""
            for line in stream_resp.iter_lines():
                if not line:
                    continue
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                if line.startswith("data: "):
                    line = line[6:]
                if line == "[DONE]":
                    break
                try:
                    delta = json.loads(line)["choices"][0].get("delta", {})
                    text = delta.get("content")
                    if text:
                        streamed_content += text
                        yield text
                except (json.JSONDecodeError, KeyError, IndexError):
                    continue

            messages.append({"role": "assistant", "content": streamed_content})
            updated_history = messages[1:]
            if not history:
                _cache.set(normalised, streamed_content, updated_history)
            yield _HISTORY_SENTINEL + json.dumps(updated_history, ensure_ascii=False)
            return

        for tc in msg.get("tool_calls", []):
            result = _dispatch_tool(tc["function"]["name"], tc["function"]["arguments"])
            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result,
            })

    fallback = "\n\n_(Reached maximum tool call rounds — please rephrase.)_"
    yield fallback
    messages.append({"role": "assistant", "content": fallback})
    yield _HISTORY_SENTINEL + json.dumps(messages[1:], ensure_ascii=False)


# ===========================================================================
# CLI test
# ===========================================================================
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    question = " ".join(sys.argv[1:]) or "Top 5 campaigns by open rate?"
    print(f"\nQ: {question}\n")
    reply, _ = run_agent(question)
    print(f"A:\n{reply}")
