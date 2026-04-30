"""
Email Embeddings Builder
Reads campaigns from BigQuery, creates embeddings via OpenAI,
upserts into Qdrant Cloud. Safe to re-run — skips already-indexed campaigns.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    VectorParams,
    Filter,
    FieldCondition,
    MatchValue,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SOURCE_TABLE      = "x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase"
ENRICHMENT_TABLE  = "x-fabric-494718-d1.datasetmailchimp.EmailEnrichment"

QDRANT_URL        = os.environ["QDRANT_URL"]           # https://xxx.qdrant.io:6333
QDRANT_API_KEY    = os.environ["QDRANT_API_KEY"]
COLLECTION_NAME   = os.getenv("QDRANT_COLLECTION", "email_campaigns")

OPENAI_API_KEY    = os.environ["OPENAI_API_KEY"]
EMBED_MODEL       = os.getenv("EMBED_MODEL", "text-embedding-3-small")
EMBED_DIMENSIONS  = 1536                               # text-embedding-3-small default

BATCH_SIZE        = int(os.getenv("EMBED_BATCH_SIZE", "100"))   # campaigns per API call
RATE_LIMIT_S      = float(os.getenv("RATE_LIMIT_SECONDS", "0.3"))
CLEAN_TEXT_MAX    = 6000


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------
def build_bq_client() -> bigquery.Client:
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        log.info("BigQuery: authenticated via service account (project=%s)", info.get("project_id"))
        return bigquery.Client(credentials=creds, project=info.get("project_id"))
    log.info("BigQuery: using Application Default Credentials")
    return bigquery.Client()


def build_qdrant_client() -> QdrantClient:
    client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    log.info("Qdrant: connected to %s", QDRANT_URL)
    return client


# ---------------------------------------------------------------------------
# Qdrant collection setup
# ---------------------------------------------------------------------------
def ensure_collection(qdrant: QdrantClient) -> None:
    existing = [c.name for c in qdrant.get_collections().collections]
    if COLLECTION_NAME not in existing:
        qdrant.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=EMBED_DIMENSIONS, distance=Distance.COSINE),
        )
        log.info("Qdrant: created collection '%s'", COLLECTION_NAME)
    else:
        log.info("Qdrant: collection '%s' already exists", COLLECTION_NAME)


# ---------------------------------------------------------------------------
# Fetch campaigns from BigQuery
# ---------------------------------------------------------------------------
def fetch_campaigns(bq: bigquery.Client) -> list[dict]:
    """
    Fetch all campaigns from EmailKnowledgeBase joined with enrichment data.
    Only returns campaigns NOT yet in Qdrant (checked by campaign_id scroll).
    We first get all IDs from Qdrant, then filter in BigQuery.
    """
    query = f"""
        SELECT
            k.campaign_id,
            k.SubjectLine,
            k.PreviewText,
            k.clean_text,
            k.open_rate_percent,
            k.ctr_percent,
            k.unsub_rate_percent,
            e.hook_type,
            e.offer_type,
            e.angle,
            e.language,
            e.geo,
            e.tone
        FROM `{SOURCE_TABLE}` k
        LEFT JOIN `{ENRICHMENT_TABLE}` e USING (campaign_id)
    """
    log.info("BigQuery: fetching all campaigns with enrichment...")
    rows = [dict(r) for r in bq.query(query).result()]
    log.info("BigQuery: got %d campaigns", len(rows))
    return rows


def get_indexed_ids(qdrant: QdrantClient) -> set[str]:
    """Scroll through Qdrant collection and collect all campaign_ids already indexed."""
    indexed = set()
    offset = None
    while True:
        result, offset = qdrant.scroll(
            collection_name=COLLECTION_NAME,
            limit=1000,
            offset=offset,
            with_payload=["campaign_id"],
            with_vectors=False,
        )
        for point in result:
            if point.payload and "campaign_id" in point.payload:
                indexed.add(point.payload["campaign_id"])
        if offset is None:
            break
    log.info("Qdrant: %d campaigns already indexed", len(indexed))
    return indexed


# ---------------------------------------------------------------------------
# Build text for embedding
# ---------------------------------------------------------------------------
def build_embed_text(row: dict) -> str:
    parts = []
    if row.get("SubjectLine"):
        parts.append(f"Subject: {row['SubjectLine']}")
    if row.get("PreviewText"):
        parts.append(f"Preview: {row['PreviewText']}")
    if row.get("hook_type"):
        parts.append(f"Hook: {row['hook_type']}")
    if row.get("angle"):
        parts.append(f"Angle: {row['angle']}")
    if row.get("clean_text"):
        parts.append(f"Body: {row['clean_text'][:CLEAN_TEXT_MAX]}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# OpenAI embeddings API
# ---------------------------------------------------------------------------
def embed_batch(texts: list[str], retries: int = 3, backoff: float = 2.0) -> list[list[float]]:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"model": EMBED_MODEL, "input": texts}

    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/embeddings",
                headers=headers,
                json=payload,
                timeout=60,
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = backoff ** attempt
                log.warning("OpenAI returned %d (attempt %d/%d) — retrying in %.1fs",
                            resp.status_code, attempt, retries, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            # Sort by index to guarantee order
            items = sorted(data["data"], key=lambda x: x["index"])
            return [item["embedding"] for item in items]
        except requests.RequestException as e:
            wait = backoff ** attempt
            log.warning("OpenAI request error (attempt %d/%d): %s — retrying in %.1fs",
                        attempt, retries, e, wait)
            time.sleep(wait)

    raise RuntimeError(f"Embedding API failed after {retries} retries")


# ---------------------------------------------------------------------------
# Build Qdrant payload (metadata for filtering)
# ---------------------------------------------------------------------------
def build_payload(row: dict) -> dict:
    def safe_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    return {
        "campaign_id":       str(row.get("campaign_id", "")),
        "subject_line":      str(row.get("SubjectLine") or ""),
        "preview_text":      str(row.get("PreviewText") or ""),
        "open_rate_percent": safe_float(row.get("open_rate_percent")),
        "ctr_percent":       safe_float(row.get("ctr_percent")),
        "unsub_rate_percent":safe_float(row.get("unsub_rate_percent")),
        "hook_type":         str(row.get("hook_type") or ""),
        "offer_type":        str(row.get("offer_type") or ""),
        "angle":             str(row.get("angle") or ""),
        "language":          str(row.get("language") or ""),
        "geo":               str(row.get("geo") or ""),
        "tone":              str(row.get("tone") or ""),
        "indexed_at":        datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("=== Email Embeddings Builder starting ===")
    log.info("Collection: %s | Batch size: %d | Model: %s",
             COLLECTION_NAME, BATCH_SIZE, EMBED_MODEL)

    bq     = build_bq_client()
    qdrant = build_qdrant_client()

    ensure_collection(qdrant)

    all_campaigns = fetch_campaigns(bq)
    indexed_ids   = get_indexed_ids(qdrant)

    to_index = [r for r in all_campaigns if r["campaign_id"] not in indexed_ids]
    log.info("Campaigns to index: %d (skipping %d already indexed)",
             len(to_index), len(indexed_ids))

    if not to_index:
        log.info("Nothing to index — exiting.")
        return

    total     = len(to_index)
    succeeded = 0
    failed    = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = to_index[batch_start : batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        log.info("[batch %d] Processing campaigns %d–%d of %d...",
                 batch_num, batch_start + 1, batch_start + len(batch), total)

        texts = [build_embed_text(r) for r in batch]

        try:
            vectors = embed_batch(texts)
        except RuntimeError as e:
            log.error("[batch %d] Embedding failed: %s — skipping batch", batch_num, e)
            failed += len(batch)
            continue

        points = []
        for row, vector in zip(batch, vectors):
            # Use campaign_id as a stable integer hash for Qdrant point ID
            # Qdrant requires unsigned int or UUID — we use abs hash
            point_id = abs(hash(row["campaign_id"])) % (2**63)
            points.append(PointStruct(
                id=point_id,
                vector=vector,
                payload=build_payload(row),
            ))

        qdrant.upsert(collection_name=COLLECTION_NAME, points=points)
        succeeded += len(batch)
        log.info("[batch %d] ✓ Upserted %d points into Qdrant", batch_num, len(batch))

        if batch_start + BATCH_SIZE < total:
            time.sleep(RATE_LIMIT_S)

    log.info("=== Done: %d indexed, %d failed (of %d total) ===",
             succeeded, failed, total)


if __name__ == "__main__":
    main()
