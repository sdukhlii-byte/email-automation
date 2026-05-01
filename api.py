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
    from_: str | None = None   # ISO date, inclusive
    to:    str | None = None   # ISO date, inclusive
    rows:  int | None = None   # rows used to compute the answer
    label: str | None = None   # "last 7 days", "all time", etc.

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
# SendTime is TIMESTAMP — used directly, no cast needed
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
                if age > timedelta(days=7):
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
# Chart extraction
# ---------------------------------------------------------------------------
_CHART_PATTERN = re.compile(r"<<<CHART(\{.*?\})>>>", re.DOTALL)


def extract_chart(reply: str) -> tuple[str, ChartData | None]:
    match = _CHART_PATTERN.search(reply)
    if not match:
        return reply, None
    clean_reply = (reply[: match.start()] + reply[match.end():]).strip()
    try:
        return clean_reply, ChartData(**json.loads(match.group(1)))
    except Exception as e:
        log.warning("Chart parse error: %s", e)
        return clean_reply, None


# ---------------------------------------------------------------------------
# Period extraction
# ---------------------------------------------------------------------------
_PERIOD_PATTERN = re.compile(r"<<<PERIOD(\{.*?\})>>>", re.DOTALL)


def extract_period(reply: str) -> tuple[str, DataPeriod | None]:
    m = _PERIOD_PATTERN.search(reply)
    if not m:
        return reply, None
    clean = (reply[: m.start()] + reply[m.end():]).strip()
    try:
        raw = json.loads(m.group(1))
        if "from" in raw:
            raw["from_"] = raw.pop("from")
        return clean, DataPeriod(**raw)
    except Exception as e:
        log.warning("Period parse error: %s", e)
        return clean, None


# ---------------------------------------------------------------------------
# Response style — injected into every /chat call instead of CHART_INSTRUCTIONS
# ---------------------------------------------------------------------------
RESPONSE_STYLE = """
You are a senior email-marketing analyst. Reply in the user's language.

RESPONSE FORMAT — choose ONE based on the question:

1. CONVERSATIONAL (greetings, clarifications, single-fact lookups, opinions,
   follow-ups, "what do you think", "explain", "почему", "как"):
   - Plain prose, max 3 short sentences.
   - NO tables, NO bullet lists, NO chart marker.

2. ANALYTICAL (ranking, comparison, distribution, trend, top-N, breakdowns
   with 3+ rows of real data):
   - One concise paragraph (1-2 sentences) with the headline insight.
   - Then ONE artifact: either a markdown table OR a chart marker — never both
     unless the user explicitly asked for both.
   - Tables: standard GitHub markdown, ≤ 10 rows, ≤ 5 columns.

PERIOD HONESTY — non-negotiable:
- If your reply mentions any timeframe ("last week", "this month", "за неделю",
  "за месяц", "за 30 дней", etc.) the SQL you ran MUST contain a matching
  WHERE SendTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL N DAY).
- If no such WHERE was applied, the reply must say "за всё время" / "all-time" —
  never invent or imply a period.
- After your answer, append on its own line:
  <<<PERIOD{"from":"YYYY-MM-DD","to":"YYYY-MM-DD","rows":N,"label":"last 7 days"}>>>
  Use "from":null,"to":null,"label":"all time" when no date filter was applied.

CHART MARKER (optional, analytical only):
<<<CHART{"type":"bar","title":"...","data":[...],"x_key":"...","y_key":"..."}>>>
- type: "bar" (comparisons), "line" (trends), "pie" (distributions).
- ≤ 20 data points, exactly ONE chart per reply, omit entirely if not needed.
"""

# Map of filter range keys → number of days
_RANGE_MAP: dict[str, int] = {
    "7d": 7,  "last_7d": 7,  "week": 7,
    "30d": 30, "last_30d": 30, "month": 30,
    "90d": 90, "quarter": 90,
}


# ---------------------------------------------------------------------------
# /chat — agent endpoint
# ---------------------------------------------------------------------------
@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    try:
        from agent import run_agent

        # Translate filters into explicit model directives
        directives: list[str] = []
        if req.filters:
            f = req.filters
            range_val = str(f.get("range") or f.get("date_range") or "").lower()
            if range_val in _RANGE_MAP:
                days = _RANGE_MAP[range_val]
                directives.append(
                    f"Restrict every SQL query with "
                    f"WHERE SendTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY). "
                    f"Refer to this period as 'last {days} days'."
                )
            extras = {k: v for k, v in f.items() if k not in ("range", "date_range")}
            if extras:
                directives.append(
                    "Also apply these filters as SQL WHERE clauses: "
                    + ", ".join(f"{k}={v!r}" for k, v in extras.items())
                )

        directive_block = ("\n".join(f"- {d}" for d in directives)) if directives else "- none"

        augmented = (
            f"{RESPONSE_STYLE}\n\n"
            f"USER FILTER DIRECTIVES:\n{directive_block}\n\n"
            f"USER MESSAGE:\n{req.message}"
        )

        reply, updated_history = run_agent(augmented, req.history or None)
        reply, chart  = extract_chart(reply)
        reply, period = extract_period(reply)

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
