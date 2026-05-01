"""
FastAPI backend — Email Marketing Intelligence
Designed for Railway / Render deployment with Lovable frontend.

Endpoints:
  GET  /health          — health check
  GET  /stats           — dashboard KPIs from BigQuery (cached 5 min)
  GET  /sync-status     — pipeline sync metadata (cached 1 min)
  POST /chat            — blocking agent response (JSON)
  POST /chat/stream     — streaming agent response (Server-Sent Events)

How to connect from Lovable:
  Non-streaming:
    const res = await fetch(`${API_URL}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message, history, filters })
    })
    const { reply, chart, data_period, history: newHistory } = await res.json()

  Streaming:
    import { fetchEventSource } from '@microsoft/fetch-event-source'
    await fetchEventSource(`${API_URL}/chat/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message, history, filters }),
      onmessage(ev) {
        const event = JSON.parse(ev.data)
        if (event.type === 'chunk')   appendText(event.text)
        if (event.type === 'chart')   setChart(event.data)
        if (event.type === 'period')  setPeriod(event.data)
        if (event.type === 'history') setHistory(event.data)
        if (event.type === 'done')    finalize()
        if (event.type === 'error')   showError(event.message)
      }
    })

Changes vs v3:
  - FIX #2: /sync-status now derives last_sync_at from MAX(fetched_at) in
    CampaignContentsRaw (= actual Mailchimp pull time), not MAX(SendTime)+24h
    which was the campaign send date — a completely different concept.
  - FIX #3: _sse_generator no longer calls run_agent() a second time to get
    history. Instead it reads the sentinel chunk emitted by run_agent_stream().
  - FIX #4: /stats uses COALESCE so hook_types returns 0 (not error) when
    EmailEnrichment is empty or not yet populated.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Email Intelligence API", version="4.1.0")

# Allow all origins — restrict to your Lovable domain in production:
# allow_origins=["https://your-app.lovable.app"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []
    filters: dict[str, Any] = {}


class ChartData(BaseModel):
    type: str
    title: str
    data: list[dict]
    x_key: str = ""
    y_key: str = ""

class DataPeriod(BaseModel):
    from_: str | None = None
    to:    str | None = None
    rows:  int | None = None
    label: str | None = None

    class Config:
        fields = {"from_": "from"}

class ChatResponse(BaseModel):
    reply: str
    history: list[dict]
    chart: ChartData | None = None
    data_period: DataPeriod | None = None


class StatsResponse(BaseModel):
    total_campaigns: int
    avg_open_rate: float
    avg_ctr: float
    hook_types: int


class SyncStatusResponse(BaseModel):
    last_sync_at: str | None
    next_sync_at: str | None
    sync_status: str          # "ok" | "error"
    campaigns_total: int
    campaigns_added_24h: int
    data_from: str | None
    data_to: str | None
    source: str
    last_error: str | None


# ---------------------------------------------------------------------------
# TTL cache (single-process; swap _val/_ts for Redis on multi-worker)
# ---------------------------------------------------------------------------
class _TTLCache:
    def __init__(self, ttl: int):
        self.ttl  = ttl
        self._val: dict | None = None
        self._ts:  float = 0.0

    def get(self) -> dict | None:
        if self._val and time.time() - self._ts < self.ttl:
            return self._val
        return None

    def set(self, val: dict) -> dict:
        self._val = val
        self._ts  = time.time()
        return val


_stats_cache = _TTLCache(ttl=300)
_sync_cache  = _TTLCache(ttl=60)


# ---------------------------------------------------------------------------
# BigQuery row helpers
# ---------------------------------------------------------------------------
def _parse_bq_row(result: str) -> list[str]:
    lines = [r for r in result.split("\n") if r.startswith("|") and "---" not in r]
    if len(lines) < 2:
        return []
    return [v.strip() for v in lines[1].split("|")[1:-1]]


def _safe(vals: list[str], idx: int, cast=str, default=None):
    try:
        v = vals[idx]
        return default if v in ("", "None", "NULL", "null") else cast(v)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# RESPONSE_STYLE system prompt
# ---------------------------------------------------------------------------
RESPONSE_STYLE = """You are a senior email-marketing analyst. Reply in the user's language.

DEFAULT MODE = CONVERSATIONAL.
Switch to ANALYTICAL only when the user EXPLICITLY asks for data:
"top", "ranking", "compare", "distribution", "breakdown", "show table",
"chart", "graph", "сколько", "топ", "сравни", "покажи таблицу", "график",
"распределение", "по дням/неделям/месяцам", "average by ...", "list of ...".

Greetings, opinions, clarifications, follow-ups, "what do you think",
"explain why", "how does X work", "tell me about", "что думаешь", "объясни",
"почему", "расскажи", "давай поговорим", or any message under ~6 words
without an explicit data verb → CONVERSATIONAL, no exceptions.

In CONVERSATIONAL mode it is FORBIDDEN to emit:
  - markdown tables, bullet/numbered lists
  - <<<CHART...>>> markers, code blocks
Only 1–3 short sentences of plain prose.

ANALYTICAL mode:
  - One headline sentence, then ONE artifact (table OR chart, not both).
  - Tables ≤ 10 rows, ≤ 5 columns. Charts ≤ 20 points.

DATA HYGIENE — NON-NEGOTIABLE for every SQL query you generate:
  - Always exclude warmup/seed lists:
        AND UPPER(IFNULL(ListName,'')) NOT LIKE '%WARMY%'
  - Always require minimum volume:
        AND EmailsSent >= 500
  - Treat any group with COUNT(*) < 5 as "low sample" — either skip it
    or label it explicitly "(low sample, n=K)" in the answer.
  - BigQuery DAYOFWEEK is 1=Sunday, 2=Monday, 3=Tuesday, 4=Wednesday,
    5=Thursday, 6=Friday, 7=Saturday. Never invert this mapping.
  - Hours are in UTC unless the user asks for a different timezone.

PERIOD HONESTY:
  - If the user (or filter directives) specify a period, your SQL MUST
    contain a matching WHERE SendTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),
    INTERVAL N DAY).
  - Do NOT invent dates. Do NOT emit a <<<PERIOD>>> marker — the server
    fills period metadata from the actual SQL, not from your text.

CHART MARKER (analytical only, optional):
<<<CHART{"type":"bar","title":"...","data":[...],"x_key":"...","y_key":"..."}>>>
type ∈ {"bar","line","pie"}. Exactly one chart per reply or none."""

# ---------------------------------------------------------------------------
# Range map
# ---------------------------------------------------------------------------
_RANGE_MAP: dict[str, int] = {
    "7d": 7,  "last_7d": 7,  "week": 7,
    "30d": 30, "last_30d": 30, "month": 30,
    "90d": 90, "quarter": 90,
}

# ---------------------------------------------------------------------------
# Intent classifier
# ---------------------------------------------------------------------------
_ANALYTICAL_TRIGGERS = re.compile(
    r"\b(top|ranking|compare|comparison|distribution|breakdown|table|chart|graph|"
    r"average\s+by|list\s+of|how\s+many|count\s+of|"
    r"топ|сравни|сравнение|распределени|таблиц|график|диаграмм|"
    r"сколько|посчита|по\s+(дн|недел|месяц|кварта)|средн\w*\s+по)\b",
    re.IGNORECASE,
)

def _is_conversational(message: str) -> bool:
    msg = (message or "").strip()
    if _ANALYTICAL_TRIGGERS.search(msg):
        return False
    if len(msg.split()) <= 5:
        return True
    return len(msg) < 80

# ---------------------------------------------------------------------------
# Prose sanitizer (safety net for conversational mode)
# ---------------------------------------------------------------------------
_TABLE_LINE  = re.compile(r"^\s*\|.*\|\s*$", re.MULTILINE)
_LIST_LINE   = re.compile(r"^\s*([-*+]|\d+\.)\s+", re.MULTILINE)
_CODE_BLOCK  = re.compile(r"```.*?```", re.DOTALL)
_CHART_MARK  = re.compile(r"<<<CHART\{.*?\}>>>", re.DOTALL)
_PERIOD_MARK = re.compile(r"<<<PERIOD\{.*?\}>>>", re.DOTALL)

def _strip_to_prose(reply: str) -> str:
    reply = _CODE_BLOCK.sub("", reply)
    reply = _CHART_MARK.sub("", reply)
    reply = _PERIOD_MARK.sub("", reply)
    reply = _TABLE_LINE.sub("", reply)
    reply = _LIST_LINE.sub("", reply)
    reply = re.sub(r"\n{2,}", "\n", reply).strip()
    sentences = re.split(r"(?<=[.!?])\s+", reply)
    return " ".join(sentences[:3]).strip()

def _drop_period_marker(reply: str) -> str:
    return _PERIOD_MARK.sub("", reply).strip()

# ---------------------------------------------------------------------------
# Deterministic period computation
# ---------------------------------------------------------------------------
def _compute_period(days: int | None, rows: int | None = None) -> "DataPeriod | None":
    if not days:
        return None
    now  = datetime.now(timezone.utc)
    frm  = (now - timedelta(days=days)).date().isoformat()
    return DataPeriod(
        from_=frm,
        to=now.date().isoformat(),
        rows=rows,
        label=f"last {days} days",
    )

# Detect when model reply mentions a timeframe (for unverified period warning)
_PERIOD_HINT = re.compile(
    r"(last\s+\d+\s+(day|days|week|weeks|month|months)|"
    r"за\s+(последн\w+\s+)?(\d+\s+)?(дн|недел|месяц|кварта))",
    re.IGNORECASE,
)
def _reply_mentions_period(reply: str) -> bool:
    return bool(_PERIOD_HINT.search(reply or ""))

# ---------------------------------------------------------------------------
# Augment message with RESPONSE_STYLE + directives
# ---------------------------------------------------------------------------
def _augment(message: str, filters: dict[str, Any], conv: bool) -> str:
    directives: list[str] = []
    range_val = str(filters.get("range") or filters.get("date_range") or "").lower()
    if range_val in _RANGE_MAP:
        days = _RANGE_MAP[range_val]
        directives.append(
            f"Restrict every SQL with WHERE SendTime >= "
            f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY). "
            f"Refer to this period as 'last {days} days'."
        )
    extras = {k: v for k, v in filters.items() if k not in ("range", "date_range") and v}
    if extras:
        directives.append(
            "Apply these filters as SQL WHERE clauses: "
            + ", ".join(f"{k}={v!r}" for k, v in extras.items())
        )
    directives.append(
        "Always include: AND UPPER(IFNULL(ListName,'')) NOT LIKE '%WARMY%' "
        "AND EmailsSent >= 500."
    )
    directive_block = "\n".join(f"- {d}" for d in directives)
    mode_hint = (
        "CURRENT MODE: CONVERSATIONAL — prose only, no tables/lists/charts."
        if conv else
        "CURRENT MODE: ANALYTICAL allowed if the data justifies it."
    )
    return (
        f"{RESPONSE_STYLE}\n\n{mode_hint}\n\n"
        f"USER FILTER DIRECTIVES:\n{directive_block}\n\n"
        f"USER MESSAGE:\n{message}"
    )


# ---------------------------------------------------------------------------
# Chart extraction from reply text
# ---------------------------------------------------------------------------
_CHART_PATTERN = re.compile(r"<<<CHART(\{.*?\})>>>", re.DOTALL)


def _extract_chart(reply: str) -> tuple[str, "ChartData | None"]:
    m = _CHART_PATTERN.search(reply)
    if not m:
        return reply, None
    clean = (reply[:m.start()] + reply[m.end():]).strip()
    try:
        return clean, ChartData(**json.loads(m.group(1)))
    except Exception as e:
        log.warning("Chart parse error: %s", e)
        return clean, None


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "version": "4.1.0"}


# ---------------------------------------------------------------------------
# GET /stats
# ---------------------------------------------------------------------------
@app.get("/stats", response_model=StatsResponse)
def get_stats():
    cached = _stats_cache.get()
    if cached:
        return cached
    try:
        from bigquery_tools import run_sql
        result = run_sql(
            """
            SELECT
              COUNT(*)                                     AS total,
              ROUND(AVG(k.open_rate_percent), 1)           AS avg_open,
              ROUND(AVG(k.ctr_percent), 2)                 AS avg_ctr,
              -- FIX #4: COALESCE so hook_types = 0 when enrichment is empty,
              -- not NULL/error. Also exclude empty-string values.
              COUNT(DISTINCT CASE
                WHEN e.hook_type IS NOT NULL AND e.hook_type != ''
                THEN e.hook_type
              END)                                         AS hook_types
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              USING (campaign_id)
            """,
            max_rows=1,
        )
        vals = _parse_bq_row(result)
        return _stats_cache.set({
            "total_campaigns": _safe(vals, 0, lambda v: int(float(v)), 0),
            "avg_open_rate":   _safe(vals, 1, float, 0.0),
            "avg_ctr":         _safe(vals, 2, float, 0.0),
            "hook_types":      _safe(vals, 3, lambda v: int(float(v)), 0),
        })
    except Exception as e:
        log.error("Stats error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# GET /sync-status
#
# FIX #2: last_sync_at now comes from MAX(fetched_at) in CampaignContentsRaw,
# which is the actual timestamp when the Mailchimp API was last polled.
# Previously it used MAX(SendTime) which is the campaign send date — completely
# different. next_sync_at = last_sync_at + 24h (matches the fetch_mailchimp_content
# schedule; adjust INTERVAL if your cron runs at a different cadence).
# ---------------------------------------------------------------------------
@app.get("/sync-status", response_model=SyncStatusResponse)
def get_sync_status():
    cached = _sync_cache.get()
    if cached:
        return cached
    try:
        from bigquery_tools import run_sql
        result = run_sql(
            """
            SELECT
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(c.fetched_at))  AS last_sync_at,
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ',
                TIMESTAMP_ADD(MAX(c.fetched_at), INTERVAL 24 HOUR))       AS next_sync_at,
              COUNTIF(k.SendTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
                                                                           AS added_24h,
              COUNT(DISTINCT k.campaign_id)                               AS total,
              FORMAT_TIMESTAMP('%Y-%m-%d', MIN(k.SendTime))               AS data_from,
              FORMAT_TIMESTAMP('%Y-%m-%d', MAX(k.SendTime))               AS data_to
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.CampaignContentsRaw` c
              USING (campaign_id)
            """,
            max_rows=1,
        )
        vals = _parse_bq_row(result)
        last_sync_at        = _safe(vals, 0)
        next_sync_at        = _safe(vals, 1)
        campaigns_added_24h = _safe(vals, 2, lambda v: int(float(v)), 0)
        campaigns_total     = _safe(vals, 3, lambda v: int(float(v)), 0)
        data_from           = _safe(vals, 4)
        data_to             = _safe(vals, 5)

        sync_status: str       = "ok"
        last_error: str | None = None

        if campaigns_total == 0:
            sync_status = "error"
            last_error  = "No campaigns found in EmailKnowledgeBase"
        elif last_sync_at:
            try:
                age = datetime.now(timezone.utc) - datetime.strptime(
                    last_sync_at, "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
                if age > timedelta(days=7):
                    sync_status = "error"
                    last_error  = f"Latest sync is {int(age.total_seconds()//3600)}h ago"
            except ValueError:
                pass

        return _sync_cache.set({
            "last_sync_at":        last_sync_at,
            "next_sync_at":        next_sync_at,
            "sync_status":         sync_status,
            "campaigns_total":     campaigns_total,
            "campaigns_added_24h": campaigns_added_24h,
            "data_from":           data_from,
            "data_to":             data_to,
            "source":              "mailchimp",
            "last_error":          last_error,
        })
    except Exception as e:
        log.error("Sync status error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# POST /chat — blocking (for Lovable if it uses plain fetch + await)
# ---------------------------------------------------------------------------
@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    try:
        from agent import run_agent

        # Determine intent and period_days from filters
        range_val = str((req.filters or {}).get("range") or (req.filters or {}).get("date_range") or "").lower()
        period_days: int | None = _RANGE_MAP.get(range_val)

        conv = _is_conversational(req.message)
        augmented = _augment(req.message, req.filters or {}, conv)

        reply, updated_history = run_agent(augmented, req.history or None)
        reply, chart = _extract_chart(reply)
        reply = _drop_period_marker(reply)   # ignore model-supplied period

        # Conversational hard-strip
        if conv:
            reply = _strip_to_prose(reply)
            chart = None

        # Deterministic period metadata
        period: DataPeriod | None = None
        if period_days:
            period = _compute_period(period_days)
        elif not conv and _reply_mentions_period(reply):
            # Reply claims a timeframe but no filter was set → mark unverified
            period = DataPeriod(label="unverified", from_=None, to=None, rows=None)

        return ChatResponse(
            reply=reply,
            history=updated_history,
            chart=chart,
            data_period=period,
        )
    except Exception as e:
        log.error("Chat error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# POST /chat/stream — SSE streaming (for Lovable with fetchEventSource)
#
# FIX #3: history is read from the sentinel chunk emitted by run_agent_stream()
# instead of triggering a second full run_agent() call. This saves one LLM
# round-trip (and associated cost) per streaming request.
#
# Event schema:
#   {"type":"chunk",   "text":"..."}          — streamed text token
#   {"type":"chart",   "data":{...}}          — ChartData after stream ends
#   {"type":"period",  "data":{...}}          — DataPeriod after stream ends
#   {"type":"history", "data":[...]}          — updated history after stream ends
#   {"type":"done"}                           — stream finished
#   {"type":"error",   "message":"..."}       — something went wrong
# ---------------------------------------------------------------------------
_HISTORY_SENTINEL = "\x00HISTORY\x00"


async def _sse_generator(req: ChatRequest) -> AsyncGenerator[str, None]:
    import asyncio
    from agent import run_agent_stream

    range_val = str((req.filters or {}).get("range") or (req.filters or {}).get("date_range") or "").lower()
    period_days: int | None = _RANGE_MAP.get(range_val)

    conv = _is_conversational(req.message)
    augmented  = _augment(req.message, req.filters or {}, conv)
    full_reply = ""
    history_data: list | None = None

    def _evt(obj: dict) -> str:
        return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

    try:
        for chunk in run_agent_stream(augmented, req.history or None):
            if chunk.startswith(_HISTORY_SENTINEL):
                try:
                    history_data = json.loads(chunk[len(_HISTORY_SENTINEL):])
                except Exception as he:
                    log.warning("History sentinel parse error: %s", he)
                continue

            full_reply += chunk
            yield _evt({"type": "chunk", "text": chunk})
            await asyncio.sleep(0)

        # Post-process
        clean, chart = _extract_chart(full_reply)
        clean = _drop_period_marker(clean)

        if conv:
            clean = _strip_to_prose(clean)
            chart = None

        # Deterministic period
        period: DataPeriod | None = None
        if period_days:
            period = _compute_period(period_days)
        elif not conv and _reply_mentions_period(clean):
            period = DataPeriod(label="unverified", from_=None, to=None, rows=None)

        if chart:
            yield _evt({"type": "chart", "data": chart.dict()})
        if period:
            yield _evt({"type": "period", "data": period.dict()})
        if history_data is not None:
            yield _evt({"type": "history", "data": history_data})

        yield _evt({"type": "done"})

    except Exception as e:
        log.error("SSE stream error: %s", e)
        yield _evt({"type": "error", "message": str(e)})


@app.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    return StreamingResponse(
        _sse_generator(req),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",   # tell nginx not to buffer
            "Connection":       "keep-alive",
        },
    )


# ---------------------------------------------------------------------------
# Entry point for local dev and Railway/Render
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("ENV", "production") == "development",
    )
