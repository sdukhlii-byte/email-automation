"""
Campaign HTML Cleaner v2
Reads raw HTML from CampaignContentsRaw where clean_text is NULL,
strips HTML tags to readable plain text, writes back to clean_text.

Fixes vs v1:
- Removed dead code in update_batch (broken UNNEST/VALUES MERGE)
- Replaced streaming insert + sleep(2) with load_table_from_json (avoids
  streaming buffer race condition that caused silent missed updates)
- Added retry logic with exponential backoff for transient BQ errors
- Dry-run mode via DRY_RUN=1 env var for safe testing
- Progress stats: rows/sec, ETA
- Graceful Ctrl-C: flushes current batch before exit
"""

import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime
from html.parser import HTMLParser

from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config (override via env vars)
# ---------------------------------------------------------------------------
TABLE         = os.getenv("CLEAN_TABLE", "x-fabric-494718-d1.datasetmailchimp.CampaignContentsRaw")
BATCH_SIZE    = int(os.getenv("CLEAN_BATCH_SIZE", "200"))
CLEAN_TEXT_MAX = int(os.getenv("CLEAN_TEXT_MAX", "12000"))
DRY_RUN       = os.getenv("DRY_RUN", "0") == "1"
MAX_RETRIES   = int(os.getenv("CLEAN_MAX_RETRIES", "3"))

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
# BigQuery client
# ---------------------------------------------------------------------------
def build_bq_client() -> bigquery.Client:
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        log.info("BigQuery: service account auth (project=%s)", info.get("project_id"))
        return bigquery.Client(credentials=creds, project=info.get("project_id"))
    log.info("BigQuery: Application Default Credentials")
    return bigquery.Client()

# ---------------------------------------------------------------------------
# HTML → plain text
# ---------------------------------------------------------------------------
SKIP_TAGS  = {"script", "style", "head", "meta", "link", "noscript", "img"}
BLOCK_TAGS = {"p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td"}


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in SKIP_TAGS:
            self._skip += 1
        if tag in BLOCK_TAGS and not self._skip:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in SKIP_TAGS:
            self._skip = max(0, self._skip - 1)

    def handle_data(self, data):
        if not self._skip:
            self.parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self.parts)
        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        return "\n".join(lines)


def html_to_text(html: str) -> str:
    if not html:
        return ""
    try:
        parser = TextExtractor()
        parser.feed(html)
        text = parser.get_text()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text).strip()

    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Z|a-z]{2,}\b", "", text)
    text = re.sub(r"[^\S\n]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:CLEAN_TEXT_MAX].strip()

# ---------------------------------------------------------------------------
# Fetch rows where clean_text is NULL
# ---------------------------------------------------------------------------
def fetch_uncleaned(client: bigquery.Client) -> list[dict]:
    query = f"""
        SELECT campaign_id, html_content, plain_text_content
        FROM `{TABLE}`
        WHERE clean_text IS NULL
          AND (html_content IS NOT NULL OR plain_text_content IS NOT NULL)
        ORDER BY fetched_at DESC
    """
    log.info("Fetching campaigns with NULL clean_text ...")
    rows = [dict(r) for r in client.query(query).result()]
    log.info("Found %d campaigns to process", len(rows))
    return rows

# ---------------------------------------------------------------------------
# Write batch via load_table_from_json → MERGE
# Avoids streaming buffer race condition (no sleep needed)
# ---------------------------------------------------------------------------
def _update_via_load(client: bigquery.Client, updates: list[dict]) -> None:
    """
    Writes updates (list of {campaign_id, clean_text}) to a staging table
    using load_table_from_json (storage API, not streaming), then MERGEs
    into the main table. Staging table is dropped after.
    """
    project = client.project
    dataset  = TABLE.split(".")[1]          # datasetmailchimp
    staging  = f"{project}.{dataset}._clean_text_staging"

    schema = [
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("clean_text",  "STRING"),
    ]

    # Drop & recreate staging
    client.delete_table(staging, not_found_ok=True)
    client.create_table(bigquery.Table(staging, schema=schema))

    # Load via storage API (no streaming buffer, immediately queryable)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(updates, staging, job_config=job_config)
    job.result()  # blocks until done

    # MERGE
    merge_sql = f"""
        MERGE `{TABLE}` T
        USING `{staging}` S
        ON T.campaign_id = S.campaign_id
        WHEN MATCHED AND T.clean_text IS NULL THEN
            UPDATE SET T.clean_text = S.clean_text
    """
    client.query(merge_sql).result()

    # Cleanup
    client.delete_table(staging, not_found_ok=True)


def update_with_retry(client: bigquery.Client, updates: list[dict]) -> int:
    """Returns number of successfully written rows."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _update_via_load(client, updates)
            return len(updates)
        except Exception as e:
            wait = 2 ** attempt
            log.warning("Attempt %d/%d failed: %s — retrying in %ds",
                        attempt, MAX_RETRIES, e, wait)
            time.sleep(wait)
    log.error("All %d retries exhausted for batch of %d rows", MAX_RETRIES, len(updates))
    return 0

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("=== Campaign HTML Cleaner v2 starting ===")
    log.info("Table: %s | Batch: %d | DRY_RUN: %s", TABLE, BATCH_SIZE, DRY_RUN)

    client = build_bq_client()
    rows   = fetch_uncleaned(client)

    if not rows:
        log.info("Nothing to clean — exiting.")
        return

    total     = len(rows)
    succeeded = 0
    failed    = 0
    start_ts  = time.time()

    for batch_start in range(0, total, BATCH_SIZE):
        if _shutdown:
            log.warning("Shutdown requested — stopping before batch %d.", batch_start + 1)
            break

        batch     = rows[batch_start: batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1

        log.info("[batch %d] rows %d–%d of %d ...",
                 batch_num, batch_start + 1, batch_start + len(batch), total)

        updates: list[dict] = []
        for row in batch:
            cid = row["campaign_id"]
            if row.get("plain_text_content"):
                text = row["plain_text_content"][:CLEAN_TEXT_MAX]
            elif row.get("html_content"):
                text = html_to_text(row["html_content"])
            else:
                continue

            if not text.strip():
                log.warning("[%s] empty after cleaning — skipping", cid)
                failed += 1
                continue

            updates.append({"campaign_id": cid, "clean_text": text})

        if not updates:
            log.info("[batch %d] no valid rows — skipping write", batch_num)
            continue

        if DRY_RUN:
            log.info("[batch %d] DRY_RUN — would write %d rows (sample: %s…)",
                     batch_num, len(updates), updates[0]["clean_text"][:80])
            succeeded += len(updates)
        else:
            written = update_with_retry(client, updates)
            succeeded += written
            failed    += len(updates) - written
            log.info("[batch %d] ✓ wrote %d rows", batch_num, written)

        # ETA
        elapsed  = time.time() - start_ts
        done_so_far = batch_start + len(batch)
        rps      = done_so_far / elapsed if elapsed else 0
        remaining = (total - done_so_far) / rps if rps else 0
        log.info("Progress: %d/%d | %.1f rows/s | ETA ~%ds",
                 done_so_far, total, rps, int(remaining))

    log.info("=== Done: %d cleaned, %d failed (of %d total) | %.1fs ===",
             succeeded, failed, total, time.time() - start_ts)


if __name__ == "__main__":
    main()
