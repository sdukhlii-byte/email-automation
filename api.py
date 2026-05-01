"""
FastAPI backend — Email Marketing Intelligence
Endpoints:
  GET  /health       — health check
  GET  /stats        — dashboard metrics from BigQuery
  GET  /sync-status  — last sync metadata from BigQuery
  POST /chat         — agent chat with tool calls
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Email Intelligence API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your Lovable domain in production
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


class ChatResponse(BaseModel):
    reply: str
    history: list[dict]
    chart: ChartData | None = None


class StatsResponse(BaseModel):
    total_campaigns: int
    avg_open_rate: float
    avg_ctr: float
    hook_types: int


class SyncStatusResponse(BaseModel):
    last_sync_at: str | None
    next_sync_at: str | None
    sync_status: str          # "ok" | "running" | "error"
    campaigns_total: int
    campaigns_added_24h: int
    data_from: str | None
    data_to: str | None
    source: str
    last_error: str | None


# ---------------------------------------------------------------------------
# Shared BigQuery helper — parses the markdown table returned by run_sql
# ---------------------------------------------------------------------------
def _parse_bq_row(result: str) -> list[str]:
    """Return a list of cell values from the first data row of a run_sql result."""
    lines = [r for r in result.split("\n") if r.startswith("|") and "---" not in r]
    if len(lines) < 2:
        return []
    return [v.strip() for v in lines[1].split("|")[1:-1]]


def _safe(vals: list[str], index: int, cast=str, default=None):
    """Safely cast a cell value; return default on missing / null-like values."""
    try:
        v = vals[index]
        return default if v in ("", "None", "NULL", "null") else cast(v)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# /stats — dashboard metrics (cached 5 min)
# ---------------------------------------------------------------------------
_stats_cache: dict = {}
_stats_ts: float = 0.0
STATS_TTL = 300  # seconds


@app.get("/stats", response_model=StatsResponse)
def get_stats():
    global _stats_cache, _stats_ts

    if _stats_cache and time.time() - _stats_ts < STATS_TTL:
        return _stats_cache

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
        _stats_cache = {
            "total_campaigns": _safe(vals, 0, lambda v: int(float(v)), 0),
            "avg_open_rate":   _safe(vals, 1, float, 0.0),
            "avg_ctr":         _safe(vals, 2, float, 0.0),
            "hook_types":      _safe(vals, 3, lambda v: int(float(v)), 0),
        }
        _stats_ts = time.time()
        return _stats_cache

    except Exception as e:
        log.error("Stats error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# /sync-status — last sync metadata (cached 60 s)
# Column is SendTime (TIMESTAMP) — used directly, no cast needed
# ---------------------------------------------------------------------------
_sync_cache: dict = {}
_sync_ts: float = 0.0
SYNC_TTL = 60  # seconds


@app.get("/sync-status", response_model=SyncStatusResponse)
def get_sync_status():
    global _sync_cache, _sync_ts

    if _sync_cache and time.time() - _sync_ts < SYNC_TTL:
        return _sync_cache

    try:
        from bigquery_tools import run_sql

        result = run_sql(
            """
            SELECT
              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(SendTime))
                AS last_sync_at,

              FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ',
                TIMESTAMP_ADD(MAX(SendTime), INTERVAL 24 HOUR))
                AS next_sync_at,

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

        # ---------------------------------------------------------------
        # Derive sync_status
        #   "ok"    — data exists and MAX(SendTime) is reasonably recent
        #   "error" — table empty, or real exception (caught below)
        # ---------------------------------------------------------------
        sync_status: str = "ok"
        last_error: str | None = None

        if campaigns_total == 0:
            sync_status = "error"
            last_error  = "No campaigns found in EmailKnowledgeBase"
        elif last_sync_at:
            try:
                last_dt = datetime.strptime(last_sync_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
                age = datetime.now(timezone.utc) - last_dt
                if age > timedelta(hours=26):
                    sync_status = "error"
                    last_error  = (
                        f"Latest SendTime is {int(age.total_seconds() // 3600)} hours ago"
                    )
            except ValueError as exc:
                log.warning("Could not parse last_sync_at '%s': %s", last_sync_at, exc)
        else:
            log.warning("campaigns_total=%d but last_sync_at is None", campaigns_total)

        _sync_cache = {
            "last_sync_at":        last_sync_at,
            "next_sync_at":        next_sync_at,
            "sync_status":         sync_status,
            "campaigns_total":     campaigns_total,
            "campaigns_added_24h": campaigns_added_24h,
            "data_from":           data_from,
            "data_to":             data_to,
            "source":              "mailchimp",
            "last_error":          last_error,
        }
        _sync_ts = time.time()
        return _sync_cache

    except Exception as e:
        log.error("Sync status error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Chart extraction — agent signals charts via a special marker in its reply
# ---------------------------------------------------------------------------
_CHART_PATTERN = re.compile(r"<<<CHART(\{.*?\})>>>", re.DOTALL)


def extract_chart(reply: str) -> tuple[str, ChartData | None]:
    """
    Strip the <<<CHART{...}>>> marker from the agent reply and parse it.
    Returns (clean_reply, ChartData | None).
    """
    match = _CHART_PATTERN.search(reply)
    if not match:
        return reply, None

    clean_reply = (reply[: match.start()] + reply[match.end() :]).strip()
    try:
        return clean_reply, ChartData(**json.loads(match.group(1)))
    except Exception as e:
        log.warning("Chart parse error: %s", e)
        return clean_reply, None


# ---------------------------------------------------------------------------
# Chart instructions injected into every agent turn
# ---------------------------------------------------------------------------
CHART_INSTRUCTIONS = """
When your answer involves ranking, comparison, or time-series data that would be
clearer as a chart, append ONE chart marker AFTER your text reply in this exact format:

<<<CHART{"type":"bar","title":"Open Rate by Hook Type","data":[{"hook":"curiosity","value":34.2},{"hook":"urgency","value":28.1}],"x_key":"hook","y_key":"value"}>>>

Chart types: "bar" for comparisons, "line" for trends, "pie" for distributions.
Keep data arrays under 20 items. Only emit ONE chart per response.
If no chart is needed, omit the marker entirely.
"""


# ---------------------------------------------------------------------------
# /chat — agent endpoint
# ---------------------------------------------------------------------------
@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    try:
        from agent import run_agent

        message = req.message
        if req.filters:
            filter_str = ", ".join(f"{k}={v}" for k, v in req.filters.items())
            message = f"{message}\n[Active filters: {filter_str}]"

        augmented = f"{message}\n\n{CHART_INSTRUCTIONS}"
        reply, updated_history = run_agent(augmented, req.history or None)
        clean_reply, chart = extract_chart(reply)

        return ChatResponse(reply=clean_reply, history=updated_history, chart=chart)

    except Exception as e:
        log.error("Chat error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Run locally
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )
