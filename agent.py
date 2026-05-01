"""
Email Marketing Agent
Orchestrates sql_tool and rag_tool via OpenAI function calling.
Stateless — caller manages conversation history.
"""

import json
import logging
import os
from typing import Generator

import requests

from bigquery_tools import SQL_TOOL_SPEC, get_schema, run_sql
from rag_tools import RAG_TOOL_SPEC, rag_search

log = logging.getLogger(__name__)

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
AGENT_MODEL    = os.getenv("AGENT_MODEL", "gpt-4o-mini")
MAX_TOOL_ROUNDS = 5  # prevent infinite loops

SYSTEM_PROMPT = f"""You are an expert affiliate & email marketing analyst with access to a database \
of Mailchimp email campaigns including their content, performance metrics, and AI-generated \
classifications (hook type, tone, angle, language, geo).

You have two tools:
1. sql_tool — for aggregations, rankings, trends, metric comparisons
2. rag_tool — for finding semantically similar campaigns, examples by topic or style

Always ground your answers in real data from the tools. When showing campaigns, \
include subject line, open rate, CTR, hook type, and tone. \
Be concise and numbers-first. If a question requires both tools, use both.

GEO NORMALIZATION RULE — always apply when grouping or filtering by country/geo:
The `geo` column in EmailEnrichment contains inconsistent values that must be normalized \
before grouping. Always wrap `geo` with this CASE expression:
  CASE UPPER(TRIM(geo))
    WHEN 'LT'        THEN 'Lithuania'
    WHEN 'LITHUANIA' THEN 'Lithuania'
    WHEN 'ES'        THEN 'Spain'
    WHEN 'SPAIN'     THEN 'Spain'
    WHEN 'GB'        THEN 'United Kingdom'
    WHEN 'UNITED KINGDOM' THEN 'United Kingdom'
    WHEN 'GLOBAL'    THEN 'Global'
    ELSE INITCAP(TRIM(geo))
  END AS geo_normalized
Then GROUP BY geo_normalized, never by raw `geo`.

Database schema:
{get_schema()}
"""

TOOLS = [SQL_TOOL_SPEC, RAG_TOOL_SPEC]


# ---------------------------------------------------------------------------
# OpenAI chat call
# ---------------------------------------------------------------------------
def _chat(messages: list[dict], stream: bool = False) -> requests.Response:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": AGENT_MODEL,
        "messages": messages,
        "tools": TOOLS,
        "tool_choice": "auto",
        "temperature": 0.2,
        "max_tokens": 2000,
        "stream": stream,
    }
    return requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=payload,
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
        return f"ERROR: Invalid tool arguments JSON: {arguments[:200]}"

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
# Agent run — single turn, returns final text
# ---------------------------------------------------------------------------
def run_agent(
    user_message: str,
    history: list[dict] | None = None,
) -> tuple[str, list[dict]]:
    """
    Runs the agent for one user turn.

    Args:
        user_message: The user's question.
        history: Previous conversation messages (excluding system prompt).

    Returns:
        (assistant_reply, updated_history)
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        messages.extend(history)
    messages.append({"role": "user", "content": user_message})

    for round_num in range(MAX_TOOL_ROUNDS):
        resp = _chat(messages)
        resp.raise_for_status()
        data = resp.json()

        choice  = data["choices"][0]
        message = choice["message"]
        messages.append(message)

        # No tool calls → final answer
        if choice["finish_reason"] != "tool_calls":
            reply = message.get("content") or ""
            # Return history without system prompt
            updated_history = messages[1:]
            return reply, updated_history

        # Execute each tool call
        for tool_call in message.get("tool_calls", []):
            tool_name   = tool_call["function"]["name"]
            tool_args   = tool_call["function"]["arguments"]
            tool_result = _dispatch_tool(tool_name, tool_args)
            log.info("Tool %s → %d chars result", tool_name, len(tool_result))

            messages.append({
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "content": tool_result,
            })

    # Fallback if max rounds hit
    return "Reached maximum tool call rounds. Please rephrase your question.", messages[1:]


# ---------------------------------------------------------------------------
# Streaming variant (for Streamlit)
# ---------------------------------------------------------------------------
def run_agent_stream(
    user_message: str,
    history: list[dict] | None = None,
) -> Generator[str, None, list[dict]]:
    """
    Generator that yields text chunks as they stream, then returns updated history.
    Tool calls are executed silently before streaming the final answer.

    Usage in Streamlit:
        reply = ""
        for chunk in run_agent_stream(question, history):
            reply += chunk
            placeholder.markdown(reply)
    """
    # First run tools (non-streaming) to get final messages
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        messages.extend(history)
    messages.append({"role": "user", "content": user_message})

    # Tool loop (non-streaming)
    for _ in range(MAX_TOOL_ROUNDS):
        resp = _chat(messages, stream=False)
        resp.raise_for_status()
        data   = resp.json()
        choice = data["choices"][0]
        msg    = choice["message"]
        messages.append(msg)

        if choice["finish_reason"] != "tool_calls":
            # Stream the final answer
            final_text = msg.get("content") or ""
            # Yield in ~50-char chunks to simulate streaming
            chunk_size = 50
            for i in range(0, len(final_text), chunk_size):
                yield final_text[i : i + chunk_size]
            return

        for tool_call in msg.get("tool_calls", []):
            result = _dispatch_tool(
                tool_call["function"]["name"],
                tool_call["function"]["arguments"],
            )
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "content": result,
            })

    yield "Reached maximum tool call rounds."


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    question = " ".join(sys.argv[1:]) or "What are the top 5 campaigns by open rate?"
    print(f"\nQuestion: {question}\n")

    reply, _ = run_agent(question)
    print(f"Answer:\n{reply}")
