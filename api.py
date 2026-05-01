"""
FastAPI backend — Email Marketing Intelligence
Endpoints:
  GET  /stats       — dashboard metrics from BigQuery
  POST /chat        — agent chat with tool calls
  GET  /health      — health check
"""

import json
import logging
import os
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Email Intelligence API", version="1.0.0")

# ---------------------------------------------------------------------------
# CORS — allow Lovable frontend
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your Lovable domain in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []
    filters: dict[str, Any] = {}


class ChartData(BaseModel):
    type: str           # "bar" | "line" | "pie"
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


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Stats endpoint — cached in memory for 5 min
# ---------------------------------------------------------------------------
import time
_stats_cache: dict = {}
_stats_ts: float = 0
STATS_TTL = 300  # seconds


@app.get("/stats", response_model=StatsResponse)
def get_stats():
    global _stats_cache, _stats_ts

    if _stats_cache and time.time() - _stats_ts < STATS_TTL:
        return _stats_cache

    try:
        from bigquery_tools import run_sql
        result = run_sql("""
            SELECT
              COUNT(*) as total,
              ROUND(AVG(k.open_rate_percent), 1) as avg_open,
              ROUND(AVG(k.ctr_percent), 2) as avg_ctr,
              COUNT(DISTINCT e.hook_type) as hook_types
            FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
            LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
              USING (campaign_id)
        """, max_rows=1)

        # Parse markdown table from run_sql output
        lines = [r for r in result.split("\n") if r.startswith("|") and "---" not in r]
        vals = [v.strip() for v in lines[1].split("|")[1:-1]] if len(lines) >= 2 else []

        _stats_cache = {
            "total_campaigns": int(float(vals[0])) if vals else 0,
            "avg_open_rate":   float(vals[1]) if len(vals) > 1 else 0.0,
            "avg_ctr":         float(vals[2]) if len(vals) > 2 else 0.0,
            "hook_types":      int(float(vals[3])) if len(vals) > 3 else 0,
        }
        _stats_ts = time.time()
        return _stats_cache

    except Exception as e:
        log.error("Stats error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Chart extraction — agent can signal charts via special marker in reply
# ---------------------------------------------------------------------------
import re

def extract_chart(reply: str) -> tuple[str, ChartData | None]:
    """
    Agent can embed chart JSON in reply using marker:
    <<<CHART{"type":"bar","title":"...","data":[...],"x_key":"...","y_key":"..."}>>>
    This strips it from text and returns (clean_reply, ChartData|None)
    """
    pattern = r"<<<CHART(\{.*?\})>>>"
    match = re.search(pattern, reply, re.DOTALL)
    if not match:
        return reply, None

    clean_reply = reply[:match.start()].strip() + reply[match.end():].strip()
    try:
        chart_dict = json.loads(match.group(1))
        return clean_reply, ChartData(**chart_dict)
    except Exception as e:
        log.warning("Chart parse error: %s", e)
        return clean_reply, None


# ---------------------------------------------------------------------------
# System prompt addon — teaches agent to emit charts
# ---------------------------------------------------------------------------
CHART_INSTRUCTIONS = """
When your answer involves ranking, comparison, or time-series data that would be 
clearer as a chart, append a chart marker AFTER your text reply in this exact format:

<<<CHART{"type":"bar","title":"Open Rate by Hook Type","data":[{"hook":"curiosity","value":34.2},{"hook":"urgency","value":28.1}],"x_key":"hook","y_key":"value"}>>>

Chart types: "bar" for comparisons, "line" for trends, "pie" for distributions.
Keep data arrays under 20 items. Only emit ONE chart per response.
If no chart is needed, do not include the marker.
"""


# ---------------------------------------------------------------------------
# Chat endpoint
# ---------------------------------------------------------------------------
@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    try:
        from agent import run_agent

        # Augment message with filters if set
        message = req.message
        if req.filters:
            filter_str = ", ".join(f"{k}={v}" for k, v in req.filters.items())
            message = f"{message}\n[Active filters: {filter_str}]"

        # Inject chart instructions into first user message
        augmented = f"{message}\n\n{CHART_INSTRUCTIONS}"

        reply, updated_history = run_agent(augmented, req.history or None)

        # Extract chart if present
        clean_reply, chart = extract_chart(reply)

        return ChatResponse(
            reply=clean_reply,
            history=updated_history,
            chart=chart,
        )

    except Exception as e:
        log.error("Chat error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Run locally
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
