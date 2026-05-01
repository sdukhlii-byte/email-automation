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
app = FastAPI(title="Email Intelligence API", version="3.0.0")

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
    type: str        # "bar" | "line" | "pie"
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
# Filter → directive block
# ---------------------------------------------------------------------------
_RANGE_MAP: dict[str, int] = {
    "7d": 7,  "last_7d": 7,  "week": 7,
    "30d": 30, "last_30d": 30, "month": 30,
    "90d": 90, "quarter": 90,
}


def _build_filter_directive(filters: dict[str, Any]) -> str:
    if not filters:
        return ""
    lines: list[str] = []
    range_val = str(filters.get("range") or filters.get("date_range") or "").lower()
    if range_val in _RANGE_MAP:
        days = _RANGE_MAP[range_val]
        lines.append(
            f"Restrict every SQL query: WHERE SendTime >= "
            f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY). "
            f"Refer to this period as 'last {days} days'."
        )
    extras = {k: v for k, v in filters.items() if k not in ("range", "date_range") and v}
    if extras:
        lines.append(
            "Also apply these filters as SQL WHERE clauses: "
            + ", ".join(f"{k}={v!r}" for k, v in extras.items())
        )
    return "\n".join(f"- {l}" for l in lines)


def _augment(message: str, filters: dict[str, Any]) -> str:
    directive = _build_filter_directive(filters)
    if not directive:
        return message
    return (
        f"[ANALYST DIRECTIVES — apply to all SQL in this turn]\n"
        f"{directive}\n\n"
        f"[USER MESSAGE]\n{message}"
    )


# ---------------------------------------------------------------------------
# Reply post-processing
# ---------------------------------------------------------------------------
_CHART_PATTERN  = re.compile(r"<<<CHART(\{.*?\})>>>",  re.DOTALL)
_PERIOD_PATTERN = re.compile(r"<<<PERIOD(\{.*?\})>>>", re.DOTALL)


def _extract_chart(reply: str) -> tuple[str, ChartData | None]:
    m = _CHART_PATTERN.search(reply)
    if not m:
        return reply, None
    clean = (reply[:m.start()] + reply[m.end():]).strip()
    try:
        return clean, ChartData(**json.loads(m.group(1)))
    except Exception as e:
        log.warning("Chart parse error: %s", e)
        return clean, None


def _extract_period(reply: str) -> tuple[str, DataPeriod | None]:
    m = _PERIOD_PATTERN.search(reply)
    if not m:
        return reply, None
    clean = (reply[:m.start()] + reply[m.end():]).strip()
    try:
        raw = json.loads(m.group(1))
        if "from" in raw:
            raw["from_"] = raw.pop("from")
        return clean, DataPeriod(**raw)
    except Exception as e:
        log.warning("Period parse error: %s", e)
        return clean, None


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "version": "3.0.0"}


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
              COUNT(*)                               AS total,
              ROUND(AVG(k.open_rate_percent), 1)     AS avg_open,
              ROUND(AVG(k.ctr_percent), 2)           AS avg_ctr,
              COUNT(DISTINCT e.hook_type)            AS hook_types
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
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(SendTime)) AS last_sync_at,
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ',
                TIMESTAMP_ADD(MAX(SendTime), INTERVAL 24 HOUR))      AS next_sync_at,
              COUNTIF(SendTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
                AS added_24h,
              COUNT(*) AS total,
              FORMAT_TIMESTAMP('%Y-%m-%d', MIN(SendTime)) AS data_from,
              FORMAT_TIMESTAMP('%Y-%m-%d', MAX(SendTime)) AS data_to
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase`
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
                    last_error  = f"Latest SendTime is {int(age.total_seconds()//3600)}h ago"
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
        augmented = _augment(req.message, req.filters)
        reply, history = run_agent(augmented, req.history or None)
        reply, chart   = _extract_chart(reply)
        reply, period  = _extract_period(reply)
        return ChatResponse(reply=reply, history=history, chart=chart, data_period=period)
    except Exception as e:
        log.error("Chat error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# POST /chat/stream — SSE streaming (for Lovable with fetchEventSource)
#
# Event schema:
#   {"type":"chunk",   "text":"..."}          — streamed text token
#   {"type":"chart",   "data":{...}}          — ChartData after stream ends
#   {"type":"period",  "data":{...}}          — DataPeriod after stream ends
#   {"type":"history", "data":[...]}          — updated history after stream ends
#   {"type":"done"}                           — stream finished
#   {"type":"error",   "message":"..."}       — something went wrong
# ---------------------------------------------------------------------------
async def _sse_generator(req: ChatRequest) -> AsyncGenerator[str, None]:
    import asyncio
    from agent import run_agent_stream, run_agent

    augmented = _augment(req.message, req.filters)
    full_reply = ""

    def _evt(obj: dict) -> str:
        return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

    try:
        for chunk in run_agent_stream(augmented, req.history or None):
            full_reply += chunk
            yield _evt({"type": "chunk", "text": chunk})
            await asyncio.sleep(0)

        # Post-process extracted metadata
        clean, chart  = _extract_chart(full_reply)
        clean, period = _extract_period(clean)

        if chart:
            yield _evt({"type": "chart", "data": chart.dict()})
        if period:
            yield _evt({"type": "period", "data": period.dict()})

        # Return updated history (non-streaming re-run — tools are already cached
        # by OpenAI so this is usually a single fast call)
        try:
            _, updated_history = run_agent(augmented, req.history or None)
            yield _evt({"type": "history", "data": updated_history})
        except Exception as hist_err:
            log.warning("History re-run failed: %s", hist_err)

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
