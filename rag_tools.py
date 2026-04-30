"""
RAG tool — semantic search over email campaigns via Qdrant + OpenAI embeddings.
"""

import logging
import os
import time
from typing import Any

import requests
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

log = logging.getLogger(__name__)

COLLECTION_NAME  = os.getenv("QDRANT_COLLECTION", "email_campaigns")
EMBED_MODEL      = os.getenv("EMBED_MODEL", "text-embedding-3-small")
OPENAI_API_KEY   = os.environ["OPENAI_API_KEY"]

# ---------------------------------------------------------------------------
# Clients (singletons)
# ---------------------------------------------------------------------------
_qdrant_client: QdrantClient | None = None

def get_qdrant_client() -> QdrantClient:
    global _qdrant_client
    if _qdrant_client is None:
        _qdrant_client = QdrantClient(
            url=os.environ["QDRANT_URL"],
            api_key=os.environ["QDRANT_API_KEY"],
        )
    return _qdrant_client


# ---------------------------------------------------------------------------
# Embed query text
# ---------------------------------------------------------------------------
def embed_text(text: str, retries: int = 3) -> list[float]:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/embeddings",
                headers=headers,
                json={"model": EMBED_MODEL, "input": text},
                timeout=30,
            )
            if resp.status_code in (429, 500, 502, 503):
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json()["data"][0]["embedding"]
        except Exception as e:
            if attempt == retries:
                raise RuntimeError(f"Embedding failed: {e}")
            time.sleep(2 ** attempt)


# ---------------------------------------------------------------------------
# Build Qdrant filter from dict
# ---------------------------------------------------------------------------
def build_filter(filters: dict[str, Any] | None) -> Filter | None:
    """
    Supported filter keys:
      hook_type, tone, language, geo, offer_type — exact string match
      min_open_rate, max_open_rate               — numeric range
      min_ctr, max_ctr                           — numeric range
    """
    if not filters:
        return None

    conditions = []

    STRING_FIELDS = {"hook_type", "tone", "language", "geo", "offer_type"}
    for field in STRING_FIELDS:
        if field in filters and filters[field]:
            conditions.append(
                FieldCondition(key=field, match=MatchValue(value=filters[field]))
            )

    # Numeric ranges
    open_range = {}
    if "min_open_rate" in filters:
        open_range["gte"] = float(filters["min_open_rate"])
    if "max_open_rate" in filters:
        open_range["lte"] = float(filters["max_open_rate"])
    if open_range:
        conditions.append(FieldCondition(key="open_rate_percent", range=Range(**open_range)))

    ctr_range = {}
    if "min_ctr" in filters:
        ctr_range["gte"] = float(filters["min_ctr"])
    if "max_ctr" in filters:
        ctr_range["lte"] = float(filters["max_ctr"])
    if ctr_range:
        conditions.append(FieldCondition(key="ctr_percent", range=Range(**ctr_range)))

    return Filter(must=conditions) if conditions else None


# ---------------------------------------------------------------------------
# Semantic search
# ---------------------------------------------------------------------------
def rag_search(
    question: str,
    filters: dict[str, Any] | None = None,
    top_k: int = 5,
) -> str:
    """
    Semantic search over email campaigns.
    Returns formatted string with top matches including metadata.
    """
    try:
        vector = embed_text(question)
    except RuntimeError as e:
        return f"ERROR: Could not embed query — {e}"

    qdrant = get_qdrant_client()
    qdrant_filter = build_filter(filters)

    try:
        results = qdrant.search(
            collection_name=COLLECTION_NAME,
            query_vector=vector,
            query_filter=qdrant_filter,
            limit=top_k,
            with_payload=True,
        )
    except Exception as e:
        log.error("Qdrant search error: %s", e)
        return f"ERROR: Qdrant search failed — {e}"

    if not results:
        return "No similar campaigns found."

    lines = [f"Found {len(results)} semantically similar campaigns:\n"]
    for i, hit in enumerate(results, 1):
        p = hit.payload
        lines.append(
            f"**{i}. {p.get('subject_line', 'No subject')}**\n"
            f"   Score: {hit.score:.3f} | "
            f"Open: {p.get('open_rate_percent', '?')}% | "
            f"CTR: {p.get('ctr_percent', '?')}%\n"
            f"   Hook: {p.get('hook_type', '?')} | "
            f"Tone: {p.get('tone', '?')} | "
            f"Geo: {p.get('geo', '?')} | "
            f"Language: {p.get('language', '?')}\n"
            f"   Preview: {p.get('preview_text', '')[:120]}\n"
            f"   campaign_id: `{p.get('campaign_id', '')}`\n"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool definition for OpenAI function calling
# ---------------------------------------------------------------------------
RAG_TOOL_SPEC = {
    "type": "function",
    "function": {
        "name": "rag_tool",
        "description": (
            "Semantic search over email campaigns using vector similarity. "
            "Use when the question is about finding similar emails, examples of a style, "
            "campaigns about a specific topic, or 'show me emails like X'. "
            "Supports metadata filters: hook_type, tone, language, geo, offer_type, "
            "min_open_rate, max_open_rate, min_ctr, max_ctr."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "Natural language search query describing the kind of campaign to find.",
                },
                "filters": {
                    "type": "object",
                    "description": (
                        "Optional metadata filters. Keys: hook_type, tone, language, geo, "
                        "offer_type (strings), min_open_rate, max_open_rate, min_ctr, max_ctr (numbers)."
                    ),
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results to return (default 5, max 10).",
                    "default": 5,
                },
            },
            "required": ["question"],
        },
    },
}
