"""
Email Marketing Agent
Orchestrates sql_tool and rag_tool via OpenAI function calling.
Stateless — caller manages conversation history.

v3: Streamlit dependency removed. Only run_agent (sync) is kept.
    run_agent_stream is now a true line-by-line SSE generator for FastAPI.
"""

import json
import logging
import os
from typing import Generator

import requests

from bigquery_tools import SQL_TOOL_SPEC, get_schema, run_sql
from rag_tools import RAG_TOOL_SPEC, rag_search

log = logging.getLogger(__name__)

OPENAI_API_KEY   = os.environ["OPENAI_API_KEY"]
AGENT_MODEL      = os.getenv("AGENT_MODEL", "gpt-4o-mini")
MAX_TOOL_ROUNDS  = int(os.getenv("MAX_TOOL_ROUNDS", "6"))
MAX_HISTORY_MSGS = int(os.getenv("MAX_HISTORY_MSGS", "20"))
_HISTORY_CHAR_LIMIT = int(os.getenv("HISTORY_CHAR_LIMIT", "80000"))  # ~20k tokens


# ---------------------------------------------------------------------------
# History trimming
# ---------------------------------------------------------------------------
def _trim_history(history: list[dict]) -> list[dict]:
    """Keep conversation within token budget.
    Always preserves the first message, drops oldest pairs when over limit."""
    if not history:
        return history
    if len(history) > MAX_HISTORY_MSGS:
        history = [history[0]] + history[-(MAX_HISTORY_MSGS - 1):]
    while len(history) > 2:
        if sum(len(json.dumps(m)) for m in history) <= _HISTORY_CHAR_LIMIT:
            break
        history = [history[0]] + history[3:]
    return history


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
def _build_system_prompt() -> str:
    return f"""You are an expert email marketing analyst with access to a Mailchimp \
campaigns database including content, performance metrics, and AI classifications \
(hook type, tone, angle, language, geo).

You have two tools:
1. sql_tool — for aggregations, rankings, trends, metric comparisons
2. rag_tool — for finding semantically similar campaigns by topic or style

Always ground your answers in real data. When showing campaigns, include subject line, \
open rate, CTR, hook type, and tone. Be concise and numbers-first.

Database schema:
{get_schema()}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE FORMAT — choose ONE:

1. CONVERSATIONAL (greetings, clarifications, single facts, opinions):
   - Plain prose, max 3 short sentences. No tables, no chart marker.

2. ANALYTICAL (rankings, comparisons, distributions, top-N, trends):
   - One headline insight sentence.
   - Then ONE artifact: markdown table OR chart marker (never both unless asked).
   - Tables: GitHub markdown, ≤10 rows, ≤5 columns.

PERIOD HONESTY:
- If you mention a timeframe, the SQL MUST have a matching WHERE SendTime >= TIMESTAMP_SUB(...).
- If no date filter: say "all-time", never imply a period.
- Append on its own line:
  <<<PERIOD{{"from":"YYYY-MM-DD","to":"YYYY-MM-DD","rows":N,"label":"last 7 days"}}>>>
  (Use null values when no date filter applied.)

CHART MARKER (analytical only, optional):
<<<CHART{{"type":"bar","title":"...","data":[...],"x_key":"...","y_key":"..."}}>>>
- type: "bar" | "line" | "pie". Max 20 data points.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Reply in the user's language."""


TOOLS = [SQL_TOOL_SPEC, RAG_TOOL_SPEC]


# ---------------------------------------------------------------------------
# OpenAI chat call
# ---------------------------------------------------------------------------
def _chat(messages: list[dict], stream: bool = False) -> requests.Response:
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
            "temperature": 0.2,
            "max_tokens": 2000,
            "stream": stream,
        },
        timeout=120,
        stream=stream,
    )


# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Non-streaming agent — used by POST /chat
# ---------------------------------------------------------------------------
def run_agent(
    user_message: str,
    history: list[dict] | None = None,
) -> tuple[str, list[dict]]:
    """
    Run the agent for one user turn.
    Returns (reply_text, updated_history_without_system_prompt).
    """
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
            return content, messages[1:]  # strip system prompt from history

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


# ---------------------------------------------------------------------------
# Streaming agent — used by POST /chat/stream (SSE)
# Runs tool loop synchronously, then streams final answer token-by-token.
# ---------------------------------------------------------------------------
def run_agent_stream(
    user_message: str,
    history: list[dict] | None = None,
) -> Generator[str, None, None]:
    """
    Generator yielding raw text chunks from the final LLM response.
    Tool calls are executed silently before streaming begins.
    The caller (FastAPI) wraps chunks in SSE format.
    """
    messages = [{"role": "system", "content": _build_system_prompt()}]
    messages.extend(_trim_history(list(history or [])))
    messages.append({"role": "user", "content": user_message})

    # Tool loop (non-streaming)
    for round_num in range(1, MAX_TOOL_ROUNDS + 1):
        resp = _chat(messages, stream=False)
        resp.raise_for_status()
        data   = resp.json()
        choice = data["choices"][0]
        msg    = choice["message"]
        messages.append(msg)

        if choice["finish_reason"] != "tool_calls":
            # Re-request the same conversation with streaming for the final answer
            messages.pop()  # remove non-streamed assistant turn
            stream_resp = _chat(messages, stream=True)
            stream_resp.raise_for_status()
            for line in stream_resp.iter_lines():
                if not line:
                    continue
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                if line.startswith("data: "):
                    line = line[6:]
                if line == "[DONE]":
                    return
                try:
                    text = json.loads(line)["choices"][0].get("delta", {}).get("content")
                    if text:
                        yield text
                except (json.JSONDecodeError, KeyError, IndexError):
                    continue
            return

        for tc in msg.get("tool_calls", []):
            result = _dispatch_tool(tc["function"]["name"], tc["function"]["arguments"])
            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": result,
            })

    yield "\n\n_(Reached maximum tool call rounds — please rephrase.)_"


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    question = " ".join(sys.argv[1:]) or "Top 5 campaigns by open rate?"
    print(f"\nQ: {question}\n")
    reply, _ = run_agent(question)
    print(f"A:\n{reply}")
