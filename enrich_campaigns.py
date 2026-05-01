"""
Mailchimp Enrichment Worker
Fetches un-enriched campaigns from EmailKnowledgeBase, classifies them via LLM,
writes results into EmailEnrichment.

Changes vs v1:
- Replaced streaming insert (insert_rows_json) with load_table_from_json → MERGE
  to avoid BigQuery streaming buffer race condition (was causing re-enrichment
  of already-processed campaigns on the next run)
- Added DRY_RUN=1 support for safe testing
- Exponential backoff on BQ write failures (same pattern as clean_campaign_text_v2)
- Progress logging: rows/sec, ETA
- Graceful Ctrl-C via signal handler
"""

import json
import logging
import os
import re
import signal
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
SOURCE_TABLE  = "x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase"
TARGET_TABLE  = "x-fabric-494718-d1.datasetmailchimp.EmailEnrichment"

LLM_API_KEY   = os.environ["LLM_API_KEY"]
LLM_BASE_URL  = os.environ["LLM_BASE_URL"].rstrip("/")
LLM_MODEL     = os.environ["LLM_MODEL"]
ENRICH_LIMIT  = int(os.getenv("ENRICH_LIMIT", "100"))
RATE_LIMIT_S  = float(os.getenv("RATE_LIMIT_SECONDS", "0.5"))
BQ_BATCH_SIZE = int(os.getenv("BQ_BATCH_SIZE", "50"))
MAX_RETRIES   = int(os.getenv("ENRICH_MAX_RETRIES", "3"))
DRY_RUN       = os.getenv("DRY_RUN", "0") == "1"
CLEAN_TEXT_MAX = 8000

LLM_CHAT_URL = f"{LLM_BASE_URL}/chat/completions"

EXPECTED_FIELDS = {
    "hook_type", "offer_type", "angle", "language",
    "geo", "cta", "tone", "summary", "reasoning",
}

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown = False

def _handle_sigint(sig, frame):
    global _shutdown
    log.warning("Ctrl-C received — will stop after current batch.")
    _shutdown = True

signal.signal(signal.SIGINT, _handle_sigint)

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------
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
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        project = info.get("project_id")
        log.info("Authenticated via GOOGLE_APPLICATION_CREDENTIALS_JSON (project=%s)", project)
        return bigquery.Client(credentials=creds, project=project)
    log.info("Using Application Default Credentials")
    return bigquery.Client()


# ---------------------------------------------------------------------------
# Fetch campaigns to enrich
# ---------------------------------------------------------------------------
def fetch_campaigns(client: bigquery.Client) -> list[dict]:
    query = f"""
        SELECT
            k.campaign_id,
            k.SubjectLine,
            k.PreviewText,
            k.clean_text,
            k.open_rate_percent,
            k.ctr_percent,
            k.unsub_rate_percent
        FROM `{SOURCE_TABLE}` k
        WHERE k.campaign_id NOT IN (
            SELECT campaign_id FROM `{TARGET_TABLE}`
        )
        AND k.clean_text IS NOT NULL
        LIMIT {ENRICH_LIMIT}
    """
    log.info("Querying BigQuery for un-enriched campaigns (limit=%d)…", ENRICH_LIMIT)
    rows = list(client.query(query).result())
    log.info("Found %d campaign(s) to enrich.", len(rows))
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# LLM call with retry
# ---------------------------------------------------------------------------
def call_llm(prompt: str, retries: int = 3, backoff: float = 2.0) -> str:
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
# JSON parsing
# ---------------------------------------------------------------------------
def parse_llm_json(raw: str, campaign_id: str) -> Optional[dict]:
    cleaned = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()
    match   = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if not match:
        log.error("[%s] No JSON object in LLM response. Raw: %r", campaign_id, raw[:300])
        return None
    try:
        data = json.loads(match.group())
    except json.JSONDecodeError as exc:
        log.error("[%s] JSON parse error: %s. Raw: %r", campaign_id, exc, raw[:300])
        return None

    missing = EXPECTED_FIELDS - data.keys()
    if missing:
        log.warning("[%s] Missing fields: %s — filling with empty string.", campaign_id, missing)
        for f in missing:
            data[f] = ""
    return data


# ---------------------------------------------------------------------------
# Enrich a single campaign
# ---------------------------------------------------------------------------
def enrich_campaign(row: dict) -> Optional[dict]:
    cid  = row["campaign_id"]
    body = (row.get("clean_text") or "")[:CLEAN_TEXT_MAX]

    prompt = USER_PROMPT_TEMPLATE.format(
        subject   = row.get("SubjectLine") or "",
        preview   = row.get("PreviewText") or "",
        body      = body,
        open_rate = row.get("open_rate_percent") or 0,
        ctr       = row.get("ctr_percent") or 0,
        unsub     = row.get("unsub_rate_percent") or 0,
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
        "hook_type":   str(parsed.get("hook_type",  "")),
        "offer_type":  str(parsed.get("offer_type", "")),
        "angle":       str(parsed.get("angle",      "")),
        "language":    str(parsed.get("language",   "")),
        "geo":         str(parsed.get("geo",        "")),
        "cta":         str(parsed.get("cta",        "")),
        "tone":        str(parsed.get("tone",       "")),
        "summary":     str(parsed.get("summary",    "")),
        "reasoning":   str(parsed.get("reasoning",  "")),
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# BigQuery write via load_table_from_json → MERGE
# Avoids streaming buffer race condition — results immediately queryable.
# ---------------------------------------------------------------------------
_ENRICH_SCHEMA = [
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("hook_type",   "STRING"),
    bigquery.SchemaField("offer_type",  "STRING"),
    bigquery.SchemaField("angle",       "STRING"),
    bigquery.SchemaField("language",    "STRING"),
    bigquery.SchemaField("geo",         "STRING"),
    bigquery.SchemaField("cta",         "STRING"),
    bigquery.SchemaField("tone",        "STRING"),
    bigquery.SchemaField("summary",     "STRING"),
    bigquery.SchemaField("reasoning",   "STRING"),
    bigquery.SchemaField("enriched_at", "STRING"),
]


def _write_via_merge(client: bigquery.Client, rows: list[dict]) -> None:
    """
    Loads rows into a staging table via storage API (no streaming buffer),
    then MERGEs into EmailEnrichment. Only inserts rows not already present.
    """
    project = client.project
    dataset  = TARGET_TABLE.split(".")[1]
    staging  = f"{project}.{dataset}._enrichment_staging"

    client.delete_table(staging, not_found_ok=True)
    client.create_table(bigquery.Table(staging, schema=_ENRICH_SCHEMA))

    job_config = bigquery.LoadJobConfig(
        schema=_ENRICH_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(rows, staging, job_config=job_config)
    job.result()

    merge_sql = f"""
        MERGE `{TARGET_TABLE}` T
        USING `{staging}` S
        ON T.campaign_id = S.campaign_id
        WHEN NOT MATCHED THEN
            INSERT (campaign_id, hook_type, offer_type, angle, language,
                    geo, cta, tone, summary, reasoning, enriched_at)
            VALUES (S.campaign_id, S.hook_type, S.offer_type, S.angle, S.language,
                    S.geo, S.cta, S.tone, S.summary, S.reasoning, S.enriched_at)
    """
    client.query(merge_sql).result()
    client.delete_table(staging, not_found_ok=True)


def write_with_retry(client: bigquery.Client, rows: list[dict]) -> int:
    if not rows:
        return 0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _write_via_merge(client, rows)
            return len(rows)
        except Exception as e:
            wait = 2 ** attempt
            log.warning("Write attempt %d/%d failed: %s — retrying in %ds",
                        attempt, MAX_RETRIES, e, wait)
            time.sleep(wait)
    log.error("All %d retries exhausted for batch of %d rows", MAX_RETRIES, len(rows))
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("=== Mailchimp Enrichment Worker starting ===")
    log.info("Source: %s | Target: %s", SOURCE_TABLE, TARGET_TABLE)
    log.info("Model: %s | Limit: %d | Rate limit: %.1fs | DRY_RUN: %s",
             LLM_MODEL, ENRICH_LIMIT, RATE_LIMIT_S, DRY_RUN)

    client    = build_bq_client()
    campaigns = fetch_campaigns(client)

    if not campaigns:
        log.info("Nothing to enrich — exiting.")
        return

    total     = len(campaigns)
    succeeded = 0
    failed    = 0
    batch: list[dict] = []
    start_ts  = time.time()

    for idx, row in enumerate(campaigns, start=1):
        if _shutdown:
            log.warning("Shutdown requested — stopping before campaign %d.", idx)
            break

        cid = row["campaign_id"]
        log.info("[%d/%d] Enriching campaign_id=%s…", idx, total, cid)

        result = enrich_campaign(row)

        if result is None:
            failed += 1
            log.warning("[%d/%d] ✗ Failed campaign_id=%s", idx, total, cid)
        else:
            batch.append(result)
            log.info("[%d/%d] ✓ campaign_id=%s (hook=%s, tone=%s)",
                     idx, total, cid, result["hook_type"], result["tone"])

        # Flush batch
        if len(batch) >= BQ_BATCH_SIZE:
            if DRY_RUN:
                log.info("DRY_RUN — would write %d rows", len(batch))
                succeeded += len(batch)
            else:
                written = write_with_retry(client, batch)
                succeeded += written
                failed    += len(batch) - written
                log.info("✓ Wrote %d rows to %s", written, TARGET_TABLE)
            batch = []

        # Progress ETA
        elapsed     = time.time() - start_ts
        rps         = idx / elapsed if elapsed else 0
        remaining   = (total - idx) / rps if rps else 0
        log.info("Progress: %d/%d | %.1f rows/s | ETA ~%ds", idx, total, rps, int(remaining))

        if idx < total:
            time.sleep(RATE_LIMIT_S)

    # Flush remainder
    if batch:
        if DRY_RUN:
            log.info("DRY_RUN — would write %d rows", len(batch))
            succeeded += len(batch)
        else:
            written = write_with_retry(client, batch)
            succeeded += written
            failed    += len(batch) - written
            log.info("✓ Wrote %d rows to %s", written, TARGET_TABLE)

    log.info("=== Done: %d succeeded, %d failed (of %d) | %.1fs ===",
             succeeded, failed, total, time.time() - start_ts)


if __name__ == "__main__":
    main()
