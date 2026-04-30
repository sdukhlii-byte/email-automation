"""
Mailchimp Campaign Enrichment Worker
Reads campaigns from BigQuery, classifies them via LLM, writes results back.
Designed to run as a Railway batch worker (no web server).
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_TABLE = "x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase"
TARGET_TABLE = "x-fabric-494718-d1.datasetmailchimp.EmailEnrichment"

LLM_API_KEY   = os.environ["LLM_API_KEY"]
LLM_BASE_URL  = os.environ["LLM_BASE_URL"].rstrip("/")
LLM_MODEL     = os.environ["LLM_MODEL"]
ENRICH_LIMIT  = int(os.getenv("ENRICH_LIMIT", "100"))
RATE_LIMIT_S  = float(os.getenv("RATE_LIMIT_SECONDS", "0.5"))
BQ_BATCH_SIZE = int(os.getenv("BQ_BATCH_SIZE", "50"))
CLEAN_TEXT_MAX = 8000

LLM_CHAT_URL = f"{LLM_BASE_URL}/chat/completions"

EXPECTED_FIELDS = {
    "hook_type", "offer_type", "angle", "language",
    "geo", "cta", "tone", "summary", "reasoning",
}

SYSTEM_PROMPT = """\
You are a senior email-marketing analyst. Classify the email campaign below and \
return ONLY a single valid JSON object — no markdown fences, no extra text — with \
exactly these keys:

{
  "hook_type":   "<e.g. curiosity | urgency | social-proof | fear-of-missing-out | story | discount | question>",
  "offer_type":  "<e.g. discount | free-shipping | free-trial | bundle | no-offer | webinar | content>",
  "angle":       "<main persuasion angle in 3-7 words>",
  "language":    "<ISO 639-1 language code, e.g. en | es | fr>",
  "geo":         "<target geography or 'global' if unclear>",
  "cta":         "<primary call-to-action text or intent, ≤10 words>",
  "tone":        "<e.g. formal | casual | playful | urgent | inspirational | informational>",
  "summary":     "<one-sentence campaign summary ≤25 words>",
  "reasoning":   "<2-3 sentences explaining your classification choices>"
}

Output JSON only. No preamble. No trailing commentary."""

USER_PROMPT_TEMPLATE = """\
Subject line   : {subject}
Preview text   : {preview}
Body (excerpt) : {body}

Performance metrics:
  Open rate      : {open_rate}%
  CTR            : {ctr}%
  Unsubscribe rate: {unsub}%"""


# ---------------------------------------------------------------------------
# BigQuery client
# ---------------------------------------------------------------------------
def build_bq_client() -> bigquery.Client:
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        project = info.get("project_id")
        log.info("Authenticated via GOOGLE_APPLICATION_CREDENTIALS_JSON (project=%s)", project)
        return bigquery.Client(credentials=creds, project=project)

    # Fall back to ADC (useful for local dev with gcloud auth)
    log.info("GOOGLE_APPLICATION_CREDENTIALS_JSON not set — using Application Default Credentials")
    return bigquery.Client()


# ---------------------------------------------------------------------------
# Fetch campaigns to enrich
# ---------------------------------------------------------------------------
def fetch_campaigns(client: bigquery.Client) -> list[dict]:
    query = f"""
        SELECT
            campaign_id,
            SubjectLine,
            PreviewText,
            clean_text,
            open_rate_percent,
            ctr_percent,
            unsub_rate_percent
        FROM `{SOURCE_TABLE}`
        WHERE campaign_id NOT IN (
            SELECT campaign_id FROM `{TARGET_TABLE}`
        )
        LIMIT {ENRICH_LIMIT}
    """
    log.info("Querying BigQuery for campaigns not yet enriched (limit=%d)…", ENRICH_LIMIT)
    rows = list(client.query(query).result())
    log.info("Found %d campaign(s) to enrich.", len(rows))
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# LLM call with retry
# ---------------------------------------------------------------------------
def call_llm(prompt: str, *, retries: int = 3, backoff: float = 2.0) -> str:
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 800,
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(LLM_CHAT_URL, headers=headers, json=payload, timeout=60)
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = backoff ** attempt
                log.warning("LLM returned %d on attempt %d/%d — retrying in %.1fs…",
                            resp.status_code, attempt, retries, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except requests.RequestException as exc:
            wait = backoff ** attempt
            log.warning("LLM request error on attempt %d/%d: %s — retrying in %.1fs…",
                        attempt, retries, exc, wait)
            time.sleep(wait)

    raise RuntimeError(f"LLM call failed after {retries} retries.")


# ---------------------------------------------------------------------------
# JSON parsing (robust)
# ---------------------------------------------------------------------------
def parse_llm_json(raw: str, campaign_id: str) -> Optional[dict]:
    # Strip markdown code fences if present
    cleaned = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()

    # Attempt to extract the first {...} block
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if not match:
        log.error("[%s] LLM response contains no JSON object. Raw: %r", campaign_id, raw[:300])
        return None

    try:
        data = json.loads(match.group())
    except json.JSONDecodeError as exc:
        log.error("[%s] JSON parse error: %s. Raw: %r", campaign_id, exc, raw[:300])
        return None

    missing = EXPECTED_FIELDS - data.keys()
    if missing:
        log.warning("[%s] LLM JSON missing fields: %s — filling with empty string.", campaign_id, missing)
        for f in missing:
            data[f] = ""

    return data


# ---------------------------------------------------------------------------
# Enrich a single campaign
# ---------------------------------------------------------------------------
def enrich_campaign(row: dict) -> Optional[dict]:
    cid = row["campaign_id"]
    body = (row.get("clean_text") or "")[:CLEAN_TEXT_MAX]

    prompt = USER_PROMPT_TEMPLATE.format(
        subject  = row.get("SubjectLine") or "",
        preview  = row.get("PreviewText") or "",
        body     = body,
        open_rate= row.get("open_rate_percent") or 0,
        ctr      = row.get("ctr_percent") or 0,
        unsub    = row.get("unsub_rate_percent") or 0,
    )

    try:
        raw = call_llm(prompt)
    except RuntimeError as exc:
        log.error("[%s] Skipping — LLM call failed: %s", cid, exc)
        return None

    parsed = parse_llm_json(raw, cid)
    if parsed is None:
        return None

    return {
        "campaign_id": cid,
        "hook_type"  : str(parsed.get("hook_type",  "")),
        "offer_type" : str(parsed.get("offer_type", "")),
        "angle"      : str(parsed.get("angle",      "")),
        "language"   : str(parsed.get("language",   "")),
        "geo"        : str(parsed.get("geo",         "")),
        "cta"        : str(parsed.get("cta",         "")),
        "tone"       : str(parsed.get("tone",        "")),
        "summary"    : str(parsed.get("summary",     "")),
        "reasoning"  : str(parsed.get("reasoning",  "")),
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# BigQuery insert
# ---------------------------------------------------------------------------
def insert_rows(client: bigquery.Client, rows: list[dict]) -> None:
    if not rows:
        return
    errors = client.insert_rows_json(TARGET_TABLE, rows)
    if errors:
        log.error("BigQuery insert errors: %s", errors)
    else:
        log.info("✓ Inserted %d row(s) into %s", len(rows), TARGET_TABLE)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("=== Mailchimp Enrichment Worker starting ===")
    log.info("Source : %s", SOURCE_TABLE)
    log.info("Target : %s", TARGET_TABLE)
    log.info("Model  : %s | Limit: %d | Rate limit: %.1fs", LLM_MODEL, ENRICH_LIMIT, RATE_LIMIT_S)

    client    = build_bq_client()
    campaigns = fetch_campaigns(client)

    if not campaigns:
        log.info("Nothing to enrich — exiting.")
        return

    total     = len(campaigns)
    succeeded = 0
    failed    = 0
    batch: list[dict] = []

    for idx, row in enumerate(campaigns, start=1):
        cid = row["campaign_id"]
        log.info("[%d/%d] Enriching campaign_id=%s …", idx, total, cid)

        result = enrich_campaign(row)

        if result is None:
            failed += 1
            log.warning("[%d/%d] ✗ Failed campaign_id=%s", idx, total, cid)
        else:
            succeeded += 1
            batch.append(result)
            log.info("[%d/%d] ✓ Classified campaign_id=%s (hook=%s, tone=%s)",
                     idx, total, cid, result["hook_type"], result["tone"])

        # Flush batch
        if len(batch) >= BQ_BATCH_SIZE:
            insert_rows(client, batch)
            batch = []

        # Rate limit between LLM calls (skip after last item)
        if idx < total:
            time.sleep(RATE_LIMIT_S)

    # Flush remainder
    insert_rows(client, batch)

    log.info("=== Enrichment complete: %d succeeded, %d failed (of %d) ===",
             succeeded, failed, total)


if __name__ == "__main__":
    main()
