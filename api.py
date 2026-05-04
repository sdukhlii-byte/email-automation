"""
FastAPI backend — Email Marketing Intelligence
Designed for Railway / Render deployment with Lovable frontend.

Endpoints:
  GET  /health          — health check
  GET  /stats           — dashboard KPIs from BigQuery (cached 5 min per period)
  GET  /stats?period=30d — same, scoped to last N days
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

  Stats with period filter (for dashboard KPI cards):
    fetch(`${API_URL}/stats?period=30d`)   // "7d" | "30d" | "90d" | omit for all-time

Changes vs v4.1 (v4.2):
  - FIX A: /stats total_campaigns now counts ALL campaigns (no EmailsSent/WARMY
    filter) so it matches the true Mailchimp library size (~1500+, not ~800).
    avg_open_rate / avg_ctr still use quality filters (noise exclusion).
  - FIX B: /stats accepts ?period=Nd query param; all KPIs including
    total_campaigns are scoped to that window. Frontend should call
    /stats?period=30d when the user selects "Last 30 days" in the filter.
  - FIX C: stats cache is keyed per period (no cross-contamination between
    all-time and period-specific results).
  - FIX D: /sync-status added_24h now counts by fetched_at (actual sync
    time) not SendTime (campaign send date).

Changes vs v3 (v4.1):
  - FIX #2: /sync-status last_sync_at from MAX(fetched_at) not MAX(SendTime)+24h
  - FIX #3: streaming history via sentinel, no second LLM call
  - FIX #4: hook_types COALESCE to 0 when enrichment empty
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
app = FastAPI(title="Email Intelligence API", version="4.2.0")

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
    period_days: int | None = None   # echoed back so frontend knows which window was used


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
# Campaign Analysis models
# ---------------------------------------------------------------------------
class CampaignAnalysisRequest(BaseModel):
    campaign_id: str
    language: str = "en"   # "en" | "ru" | "lt"


class CampaignAnalysisResponse(BaseModel):
    found: bool
    campaign_id: str
    analysis: dict | None = None       # full LLM audit JSON
    raw_data: dict | None = None       # BQ row for supplementary display
    benchmark: dict | None = None      # segment benchmark stats
    error: str | None = None


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
RESPONSE_STYLE = """You are a senior email-marketing analyst.

CRITICAL — LANGUAGE DETECTION (highest priority rule, overrides everything):
1. Read the language of the USER MESSAGE below.
2. Your entire response MUST be in that exact language.
3. English message → English response.
4. Russian message → Russian response.
5. Lithuanian message → Lithuanian response.
6. Do NOT default to Russian. Do NOT mix languages.
7. If unsure — match the script (Latin vs Cyrillic).
---

Always answer with real data. For any question about performance, timing, rankings,
or patterns — immediately run the appropriate SQL or RAG query and respond with
concrete numbers. Never ask clarifying questions before querying. Never give generic
advice without data. If the user says "да" or "yes" after a question, run the query now.

When presenting results:
  - One headline insight sentence, then ONE artifact: markdown table OR chart marker.
  - Tables ≤ 10 rows, ≤ 5 columns. Charts ≤ 20 points.
  - Never produce both a table and a chart in the same reply.

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

CHART MARKER (optional):
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
# Period marker cleanup
# ---------------------------------------------------------------------------
_PERIOD_MARK = re.compile(r"<<<PERIOD\{.*?\}>>>", re.DOTALL)

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
def _augment(message: str, filters: dict[str, Any]) -> str:
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
    import langdetect  # pip install langdetect

    try:
        detected_lang = langdetect.detect(message)
    except Exception:
        detected_lang = "unknown"

    lang_reminder = f"REMINDER: The user wrote in language code '{detected_lang}'. Reply in THAT language only."

    return (
        f"{RESPONSE_STYLE}\n\n"
        f"USER FILTER DIRECTIVES:\n{directive_block}\n\n"
        f"{lang_reminder}\n\n"
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
    return {"status": "ok", "version": "4.2.0"}


# ---------------------------------------------------------------------------
# GET /stats?period=30d
#
# Three fixes vs v4.1:
#
# FIX A — total_campaigns counts ALL campaigns from EmailKnowledgeBase with no
#   EmailsSent/WARMY filter, so it matches the true Mailchimp library size.
#   The CTE-wrapper in bigquery_tools would normally inject those filters, but
#   we bypass it here by querying a raw COUNT(*) without the CTE alias.
#   avg_open_rate / avg_ctr still use the quality filters (EmailsSent>=500,
#   no warmup) because those are performance metrics that should exclude noise.
#
# FIX B — period query parameter (7, 30, 90) now filters SendTime for the
#   performance KPIs (avg_open, avg_ctr) AND for total_campaigns so the
#   number on screen reflects what the user selected.
#
# FIX C — result is cached per period (keyed by period_days) so selecting
#   "Last 30 days" in the UI fetches fresh numbers without poisoning the
#   "all-time" cache entry.
# ---------------------------------------------------------------------------
_stats_cache_by_period: dict[str | None, _TTLCache] = {}

def _get_stats_cache(period_key: str | None) -> _TTLCache:
    if period_key not in _stats_cache_by_period:
        _stats_cache_by_period[period_key] = _TTLCache(ttl=300)
    return _stats_cache_by_period[period_key]


@app.get("/stats", response_model=StatsResponse)
def get_stats(period: str | None = None):
    """
    period: optional, one of "7d" | "30d" | "90d" (matches _RANGE_MAP keys).
    When supplied, all KPIs are scoped to that time window.
    """
    period_days: int | None = _RANGE_MAP.get((period or "").lower())
    cache_key   = str(period_days) if period_days else None
    cache       = _get_stats_cache(cache_key)

    cached = cache.get()
    if cached:
        return cached

    try:
        from bigquery_tools import run_sql, get_bq_client
        from google.cloud import bigquery as _bq

        period_filter = (
            f"AND k.SendTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {period_days} DAY)"
            if period_days else ""
        )

        # ── Total campaigns: raw count (no quality filter) scoped to period ──
        total_sql = f"""
            SELECT COUNT(*) AS total
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            WHERE 1=1
              {period_filter}
        """
        # We run this directly (not through run_sql) to bypass the CTE wrapper
        client = get_bq_client()
        job_cfg = _bq.QueryJobConfig(
            default_dataset="x-fabric-494718-d1.datasetmailchimp",
            maximum_bytes_billed=50 * 1024 * 1024,
        )
        total_rows = list(client.query(total_sql, job_config=job_cfg).result())
        total_campaigns = int(dict(total_rows[0]).get("total", 0)) if total_rows else 0

        # ── Performance KPIs: quality-filtered, period-scoped ──
        # run_sql goes through _wrap_with_cte which injects the WARMY/EmailsSent guard.
        # We just add the period filter in the WHERE so it lands inside the CTE.
        perf_sql = f"""
            SELECT
              ROUND(AVG(k.open_rate_percent), 1)  AS avg_open,
              ROUND(AVG(k.ctr_percent), 2)         AS avg_ctr,
              COUNT(DISTINCT CASE
                WHEN e.hook_type IS NOT NULL AND e.hook_type != ''
                THEN e.hook_type END)              AS hook_types
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              USING (campaign_id)
            WHERE 1=1
              {period_filter}
        """
        perf_result = run_sql(perf_sql, max_rows=1)
        vals = _parse_bq_row(perf_result)

        return cache.set({
            "total_campaigns": total_campaigns,
            "avg_open_rate":   _safe(vals, 0, float, 0.0),
            "avg_ctr":         _safe(vals, 1, float, 0.0),
            "hook_types":      _safe(vals, 2, lambda v: int(float(v)), 0),
            "period_days":     period_days,
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
              COUNTIF(c.fetched_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
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

        augmented = _augment(req.message, req.filters or {})

        reply, updated_history = run_agent(augmented, req.history or None)
        reply, chart = _extract_chart(reply)
        reply = _drop_period_marker(reply)

        # Deterministic period metadata
        period: DataPeriod | None = None
        if period_days:
            period = _compute_period(period_days)
        elif _reply_mentions_period(reply):
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

    augmented  = _augment(req.message, req.filters or {})
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

        # Deterministic period
        period: DataPeriod | None = None
        if period_days:
            period = _compute_period(period_days)
        elif _reply_mentions_period(clean):
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
# POST /campaign/analyze
#
# Deep single-campaign audit powered by campaign_analyst.py.
#
# Request:
#   { "campaign_id": "abc123", "language": "en" }
#
# Response: CampaignAnalysisResponse
#   {
#     "found": true,
#     "campaign_id": "abc123",
#     "analysis": { ... full LLM audit ... },
#     "raw_data":  { ... BQ row ... },
#     "benchmark": { ... segment stats ... },
#     "error": null
#   }
#
# Errors:
#   404 — campaign not found in database
#   422 — validation error (bad request body)
#   500 — LLM or BQ failure (error field populated in body too)
#
# No caching intentionally — analysis is expensive and should always
# be fresh. Add a Redis TTL cache here if you hit latency issues.
# ---------------------------------------------------------------------------
@app.post("/campaign/analyze", response_model=CampaignAnalysisResponse)
def analyze_campaign_endpoint(req: CampaignAnalysisRequest):
    """
    Full deep audit of a single email campaign.

    Runs 4 steps internally:
      1. BQ fetch — all campaign metadata + enrichment + body excerpt
      2. BQ benchmark — segment avg open/CTR for same list ±45 days
      3. Qdrant RAG — 5 semantically similar peer campaigns
      4. LLM audit — senior email marketer persona, structured JSON output

    Typical latency: 8–15s (dominated by LLM call).
    Set a 30s timeout on the client side.
    """
    cid = req.campaign_id.strip()
    if not cid:
        raise HTTPException(status_code=422, detail="campaign_id must not be empty")

    log.info("POST /campaign/analyze campaign_id=%s lang=%s", cid, req.language)

    try:
        from campaign_analyst import analyze_campaign
        result = analyze_campaign(campaign_id=cid, language=req.language)
    except Exception as e:
        log.error("analyze_campaign raised: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    if not result.get("found"):
        raise HTTPException(
            status_code=404,
            detail=result.get("error") or f"Campaign '{cid}' not found",
        )

    # If LLM analysis itself failed (found=True but analysis=None), still
    # return 200 with error field populated so the frontend can show
    # the raw_data section even without the AI audit.
    if result.get("found") and result.get("analysis") is None and result.get("error"):
        log.warning("Campaign found but LLM audit failed: %s", result["error"])

    return CampaignAnalysisResponse(
        found=result["found"],
        campaign_id=result["campaign_id"],
        analysis=result.get("analysis"),
        raw_data=result.get("raw_data"),
        benchmark=result.get("benchmark"),
        error=result.get("error"),
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
