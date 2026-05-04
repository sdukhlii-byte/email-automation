"""
campaign_analyst.py
====================
Deep single-campaign analysis engine.

Given a campaign_id this module:
  1. Fetches ALL available data from BigQuery (EmailKnowledgeBase +
     EmailEnrichment + CampaignContentsRaw) in one query.
  2. Fetches the 5 most semantically similar campaigns from Qdrant (RAG)
     to build a benchmark peer group.
  3. Runs a BigQuery benchmark query to get segment-level averages
     (same ListName, same period ±30 days) for comparison.
  4. Passes everything to a senior email-marketing analyst LLM prompt
     that produces a structured audit report.

Endpoint: POST /campaign/analyze
Request : { "campaign_id": "abc123", "language": "ru" }
Response: CampaignAnalysisResponse (see models below)

The analysis covers:
  - Performance verdict (open rate / CTR vs. benchmark)
  - Subject line dissection (hook, curiosity gap, personalisation)
  - Body copy dissection (CTA strength, readability, offer clarity)
  - Audience fit (list, geo, language match)
  - Deliverability signals (unsub rate, EmailsSent)
  - 3–5 concrete improvement recommendations with expected impact
  - Overall score 1–10 with reasoning
"""

import json
import logging
import os
import re
from typing import Any

import requests

from bigquery_tools import run_sql, get_bq_client
from rag_tools import rag_search

log = logging.getLogger(__name__)

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ANALYST_MODEL  = os.getenv("ANALYST_MODEL", os.getenv("AGENT_MODEL", "gpt-4o-mini"))

# ---------------------------------------------------------------------------
# Analyst system prompt — senior email marketer persona
# ---------------------------------------------------------------------------
_ANALYST_SYSTEM = """\
You are a world-class senior email marketing strategist with 15+ years of \
hands-on experience managing high-volume B2C and affiliate email programs \
in Eastern European markets (Lithuania, Croatia). You have managed lists \
with 500k+ subscribers and personally written or reviewed thousands of \
campaigns across casino, sports betting, poker, lifestyle, and tech verticals.

YOUR TASK: Perform a deep, honest audit of a single email campaign.
You have been given:
  • All campaign metadata and performance metrics
  • Full subject line and preview text
  • Body copy excerpt (clean text)
  • AI-generated classifications (hook type, tone, offer type, angle, CTA)
  • Benchmark data: performance of similar campaigns in the same segment
  • Peer campaigns: 5 semantically similar campaigns with their metrics

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUDIT STRUCTURE — produce EXACTLY this JSON (no markdown fences, pure JSON):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{
  "campaign_id": "<id>",
  "overall_score": <1-10 integer>,
  "score_reasoning": "<2-3 sentences why this score>",
  "performance_verdict": {
    "open_rate": <float>,
    "open_rate_vs_benchmark": "<e.g. +8.3pp above segment avg>",
    "open_rate_status": "excellent|good|average|below_avg|poor",
    "ctr": <float>,
    "ctr_vs_benchmark": "<e.g. -1.2pp below segment avg>",
    "ctr_status": "excellent|good|average|below_avg|poor",
    "unsub_rate": <float>,
    "unsub_status": "healthy|elevated|high|critical",
    "verdict_summary": "<1 sentence overall performance verdict>"
  },
  "subject_line_audit": {
    "text": "<full subject line>",
    "length_chars": <int>,
    "length_verdict": "too_short|optimal|too_long",
    "hook_type": "<curiosity|urgency|social_proof|fear|story|discount|question|none>",
    "hook_strength": "strong|moderate|weak|missing",
    "personalisation": true|false,
    "emoji_used": true|false,
    "power_words": ["<word1>", "<word2>"],
    "weaknesses": ["<specific weakness 1>", "<specific weakness 2>"],
    "rewrite_suggestion": "<concrete rewritten subject line that would perform better>",
    "rewrite_rationale": "<why the rewrite would improve open rate>"
  },
  "preview_text_audit": {
    "text": "<preview text or null>",
    "complements_subject": true|false,
    "verdict": "<short assessment>",
    "rewrite_suggestion": "<rewritten preview text or null if already good>"
  },
  "body_copy_audit": {
    "cta_text": "<primary CTA>",
    "cta_strength": "strong|moderate|weak|missing",
    "cta_placement": "early|middle|late|missing",
    "readability": "excellent|good|average|poor",
    "tone_match": true|false,
    "offer_clarity": "crystal_clear|clear|vague|confusing",
    "key_weaknesses": ["<weakness 1>", "<weakness 2>"],
    "key_strengths": ["<strength 1>", "<strength 2>"]
  },
  "audience_fit": {
    "list_name": "<list name>",
    "geo": "<detected geo>",
    "language": "<email language>",
    "audience_match": "excellent|good|mismatch|unclear",
    "notes": "<any audience fit observations>"
  },
  "benchmark_comparison": {
    "segment": "<ListName / period>",
    "segment_avg_open_rate": <float|null>,
    "segment_avg_ctr": <float|null>,
    "peer_campaigns_count": <int>,
    "this_campaign_percentile": "<e.g. top 15% by open rate in this segment>",
    "key_differentiators": ["<what makes this campaign different from peers>"]
  },
  "recommendations": [
    {
      "priority": 1,
      "area": "subject_line|preview_text|cta|timing|segmentation|copy|offer|frequency",
      "problem": "<specific problem>",
      "action": "<concrete action to take>",
      "expected_impact": "<e.g. +3–5pp open rate>"
    }
  ],
  "red_flags": ["<any critical issues — high unsub, spam triggers, etc.>"],
  "what_worked": ["<what this campaign did right>"]
}

RULES:
- overall_score: 1=disaster, 4=below avg, 5=avg, 7=good, 9=excellent, 10=rare best-in-class.
- Be brutally honest. Do not soften verdicts to be polite.
- If data is missing for a field, use null (not empty string).
- recommendations: minimum 3, maximum 5, ordered by expected impact (highest first).
- red_flags: leave empty array [] if none. Do NOT invent red flags.
- Respond in the LANGUAGE specified in the request (ru/en/lt). All text fields follow that language.
- Output pure JSON only. Zero prose before or after the JSON object.
"""


# ---------------------------------------------------------------------------
# Fetch campaign data from BigQuery
# ---------------------------------------------------------------------------
def _fetch_campaign_data(campaign_id: str) -> dict[str, Any] | None:
    """
    Pulls everything we have about the campaign in one query.
    Returns a flat dict or None if campaign not found.
    """
    sql = f"""
    SELECT
      k.campaign_id,
      k.CampaignTitle,
      k.SubjectLine,
      k.PreviewText,
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', k.SendTime) AS send_time,
      k.ListName,
      k.EmailsSent,
      ROUND(k.open_rate_percent, 2)   AS open_rate_percent,
      ROUND(k.ctr_percent, 2)         AS ctr_percent,
      ROUND(k.unsub_rate_percent, 3)  AS unsub_rate_percent,
      k.Opens_UniqueOpens,
      k.Clicks_UniqueClicks,
      k.Unsubscribed,
      e.hook_type,
      e.offer_type,
      e.angle,
      e.language,
      e.geo,
      e.cta,
      e.tone,
      e.summary           AS ai_summary,
      e.reasoning         AS ai_reasoning,
      LEFT(k.clean_text, 4000)        AS body_excerpt
    FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
    LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
      USING (campaign_id)
    WHERE k.campaign_id = '{campaign_id.replace("'", "")}'
    LIMIT 1
    """
    result = run_sql(sql, max_rows=1)

    if result.startswith("ERROR") or result == "Query returned 0 rows.":
        log.warning("Campaign not found: %s | BQ: %s", campaign_id, result[:200])
        return None

    # Parse the markdown table returned by run_sql
    lines = [r for r in result.split("\n") if r.startswith("|") and "---" not in r]
    if len(lines) < 2:
        return None

    headers = [v.strip() for v in lines[0].split("|")[1:-1]]
    values  = [v.strip() for v in lines[1].split("|")[1:-1]]

    row: dict[str, Any] = {}
    for h, v in zip(headers, values):
        row[h] = None if v in ("", "None", "NULL", "null") else v

    # Cast numeric fields
    for field in ("open_rate_percent", "ctr_percent", "unsub_rate_percent"):
        if row.get(field):
            try:
                row[field] = float(row[field])
            except (TypeError, ValueError):
                row[field] = None

    for field in ("EmailsSent", "Opens_UniqueOpens", "Clicks_UniqueClicks", "Unsubscribed"):
        if row.get(field):
            try:
                row[field] = int(float(row[field]))
            except (TypeError, ValueError):
                row[field] = None

    return row


# ---------------------------------------------------------------------------
# Fetch benchmark: segment averages for comparison
# ---------------------------------------------------------------------------
def _fetch_benchmark(list_name: str, send_time: str) -> dict[str, Any]:
    """
    Returns avg open_rate and ctr for campaigns in the same list
    within ±45 days of this campaign's send time.
    Falls back to all-time list average if the window has < 10 campaigns.
    """
    if not list_name or not send_time:
        return {}

    ln_safe = list_name.replace("'", "")
    # send_time is already formatted as 'YYYY-MM-DD HH:MM'
    date_part = send_time[:10]  # 'YYYY-MM-DD'

    sql = f"""
    SELECT
      COUNT(*) AS n,
      ROUND(AVG(k.open_rate_percent), 2) AS avg_open_rate,
      ROUND(AVG(k.ctr_percent), 2)       AS avg_ctr,
      ROUND(MIN(k.open_rate_percent), 2) AS min_open_rate,
      ROUND(MAX(k.open_rate_percent), 2) AS max_open_rate
    FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
    WHERE k.ListName = '{ln_safe}'
      AND k.open_rate_percent < 60
      AND k.ctr_percent < 50
      AND k.EmailsSent >= 200
      AND k.SendTime BETWEEN
            TIMESTAMP_SUB(TIMESTAMP('{date_part}'), INTERVAL 45 DAY) AND
            TIMESTAMP_ADD(TIMESTAMP('{date_part}'), INTERVAL 45 DAY)
    """
    result = run_sql(sql, max_rows=1)

    lines = [r for r in result.split("\n") if r.startswith("|") and "---" not in r]
    if len(lines) < 2:
        return {}

    headers = [v.strip() for v in lines[0].split("|")[1:-1]]
    values  = [v.strip() for v in lines[1].split("|")[1:-1]]
    bm: dict[str, Any] = {}
    for h, v in zip(headers, values):
        bm[h] = None if v in ("", "None", "NULL", "null") else v

    # If window has < 10 campaigns, fall back to all-time
    n = int(float(bm.get("n") or 0))
    if n < 10:
        log.info("Benchmark window too small (n=%d), using all-time for list %s", n, list_name)
        sql_alltime = f"""
        SELECT
          COUNT(*) AS n,
          ROUND(AVG(k.open_rate_percent), 2) AS avg_open_rate,
          ROUND(AVG(k.ctr_percent), 2)       AS avg_ctr,
          ROUND(MIN(k.open_rate_percent), 2) AS min_open_rate,
          ROUND(MAX(k.open_rate_percent), 2) AS max_open_rate
        FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
        WHERE k.ListName = '{ln_safe}'
          AND k.open_rate_percent < 60
          AND k.ctr_percent < 50
          AND k.EmailsSent >= 200
        """
        result2 = run_sql(sql_alltime, max_rows=1)
        lines2  = [r for r in result2.split("\n") if r.startswith("|") and "---" not in r]
        if len(lines2) >= 2:
            headers2 = [v.strip() for v in lines2[0].split("|")[1:-1]]
            values2  = [v.strip() for v in lines2[1].split("|")[1:-1]]
            bm = {h: (None if v in ("", "None", "NULL", "null") else v)
                  for h, v in zip(headers2, values2)}

    # Cast to float
    for field in ("avg_open_rate", "avg_ctr", "min_open_rate", "max_open_rate"):
        try:
            bm[field] = float(bm[field]) if bm.get(field) else None
        except (TypeError, ValueError):
            bm[field] = None
    try:
        bm["n"] = int(float(bm.get("n") or 0))
    except Exception:
        bm["n"] = 0

    log.info("Benchmark for '%s': n=%s avg_open=%.1f avg_ctr=%.2f",
             list_name, bm.get("n"), bm.get("avg_open_rate") or 0, bm.get("avg_ctr") or 0)
    return bm


# ---------------------------------------------------------------------------
# Fetch peer campaigns via RAG
# ---------------------------------------------------------------------------
def _fetch_peers(campaign: dict[str, Any]) -> str:
    """
    Build a descriptive query from the campaign and search Qdrant for
    5 semantically similar campaigns. Returns formatted string for LLM context.
    """
    parts = []
    if campaign.get("SubjectLine"):
        parts.append(campaign["SubjectLine"])
    if campaign.get("hook_type"):
        parts.append(f"{campaign['hook_type']} hook")
    if campaign.get("angle"):
        parts.append(campaign["angle"])
    if campaign.get("ListName"):
        parts.append(campaign["ListName"])

    query = " ".join(parts) if parts else "email campaign"
    log.info("RAG peer query: %s", query[:120])

    try:
        result = rag_search(query, top_k=6)
        # Remove the current campaign from peers
        cid = campaign.get("campaign_id", "")
        if cid and cid in result:
            # Filter out lines containing this campaign_id
            filtered = "\n".join(
                line for line in result.split("\n")
                if cid not in line
            )
            return filtered
        return result
    except Exception as e:
        log.warning("RAG peer search failed: %s", e)
        return "(peer search unavailable)"


# ---------------------------------------------------------------------------
# LLM analysis call
# ---------------------------------------------------------------------------
def _run_analysis(
    campaign: dict[str, Any],
    benchmark: dict[str, Any],
    peers: str,
    language: str,
) -> dict[str, Any]:
    """
    Calls the LLM with all assembled context. Returns parsed JSON dict.
    Raises RuntimeError on failure.
    """
    # ---- Build user message ----
    bm_text = (
        f"  Segment avg open rate : {benchmark.get('avg_open_rate')}%\n"
        f"  Segment avg CTR       : {benchmark.get('avg_ctr')}%\n"
        f"  Open rate range       : {benchmark.get('min_open_rate')}% – {benchmark.get('max_open_rate')}%\n"
        f"  Sample size           : {benchmark.get('n')} campaigns"
    ) if benchmark else "  No benchmark data available."

    user_msg = f"""LANGUAGE FOR RESPONSE: {language}

━━━━━━━━━━━━━ CAMPAIGN DATA ━━━━━━━━━━━━━

Campaign ID    : {campaign.get('campaign_id')}
Title          : {campaign.get('CampaignTitle')}
List           : {campaign.get('ListName')}
Send time      : {campaign.get('send_time')}
Emails sent    : {campaign.get('EmailsSent')}

Subject line   : {campaign.get('SubjectLine')}
Preview text   : {campaign.get('PreviewText')}

PERFORMANCE METRICS:
  Open rate    : {campaign.get('open_rate_percent')}%  (unique opens: {campaign.get('Opens_UniqueOpens')})
  CTR          : {campaign.get('ctr_percent')}%  (unique clicks: {campaign.get('Clicks_UniqueClicks')})
  Unsub rate   : {campaign.get('unsub_rate_percent')}%  (unsubscribes: {campaign.get('Unsubscribed')})

AI CLASSIFICATIONS (auto-generated, may have errors):
  Hook type    : {campaign.get('hook_type')}
  Offer type   : {campaign.get('offer_type')}
  Angle        : {campaign.get('angle')}
  Tone         : {campaign.get('tone')}
  Primary CTA  : {campaign.get('cta')}
  Language     : {campaign.get('language')}
  Geo target   : {campaign.get('geo')}

AI SUMMARY: {campaign.get('ai_summary')}

BODY COPY EXCERPT (first 4000 chars):
---
{campaign.get('body_excerpt') or '(not available)'}
---

━━━━━━━━━━━━━ BENCHMARK (same list ±45 days) ━━━━━━━━━━━━━

{bm_text}

━━━━━━━━━━━━━ PEER CAMPAIGNS (semantically similar) ━━━━━━━━━━━━━

{peers}

━━━━━━━━━━━━━

Now produce the full audit JSON as specified in your instructions.
Be specific, reference actual numbers, and give concrete rewrite suggestions."""

    messages = [
        {"role": "system", "content": _ANALYST_SYSTEM},
        {"role": "user",   "content": user_msg},
    ]

    log.info("Analyst LLM call: campaign_id=%s model=%s",
             campaign.get("campaign_id"), ANALYST_MODEL)

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model":       ANALYST_MODEL,
            "messages":    messages,
            "temperature": 0.3,    # slight creativity for recommendations, but grounded
            "max_tokens":  3000,
            "stream":      False,
        },
        timeout=120,
    )
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"].strip()
    log.info("Analyst LLM responded: %d chars", len(raw))

    # Strip accidental markdown fences
    raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        log.error("Analyst JSON parse error: %s\nRaw: %s", exc, raw[:500])
        raise RuntimeError(f"LLM returned invalid JSON: {exc}") from exc


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def analyze_campaign(
    campaign_id: str,
    language: str = "en",
) -> dict[str, Any]:
    """
    Full analysis pipeline for a single campaign.

    Returns a dict with keys:
      found        : bool — whether the campaign exists
      campaign_id  : str
      analysis     : dict — parsed LLM audit (when found=True)
      raw_data     : dict — BQ row (for frontend supplementary display)
      benchmark    : dict — segment benchmark stats
      error        : str | None — error message if something failed

    Never raises — always returns a dict.
    """
    log.info("analyze_campaign: %s lang=%s", campaign_id, language)

    # Step 1: Fetch campaign data
    campaign = _fetch_campaign_data(campaign_id)
    if campaign is None:
        return {
            "found":       False,
            "campaign_id": campaign_id,
            "analysis":    None,
            "raw_data":    None,
            "benchmark":   None,
            "error":       f"Campaign '{campaign_id}' not found in database.",
        }

    # Step 2: Benchmark
    benchmark = {}
    try:
        benchmark = _fetch_benchmark(
            campaign.get("ListName", ""),
            campaign.get("send_time", ""),
        )
    except Exception as e:
        log.warning("Benchmark fetch failed: %s", e)

    # Step 3: RAG peers
    peers = "(peer search skipped)"
    try:
        peers = _fetch_peers(campaign)
    except Exception as e:
        log.warning("Peer fetch failed: %s", e)

    # Step 4: LLM analysis
    analysis = None
    error = None
    try:
        analysis = _run_analysis(campaign, benchmark, peers, language)
    except Exception as e:
        log.error("LLM analysis failed: %s", e)
        error = str(e)

    return {
        "found":       True,
        "campaign_id": campaign_id,
        "analysis":    analysis,
        "raw_data":    campaign,
        "benchmark":   benchmark,
        "error":       error,
    }
