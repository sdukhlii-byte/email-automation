# backfill_subject_lines.py
# One-time job to backfill SubjectLine and PreviewText
# for all existing campaigns where SubjectLine IS NULL.
# Safe to re-run — only touches NULL rows.
# Expected runtime: ~10 min for 1650 campaigns (0.3s per API call)

import os
import json
import requests
import time
from datetime import datetime, timezone

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "x-fabric-494718-d1"
DATASET = "datasetmailchimp"
KNOWLEDGE_TABLE = f"{PROJECT_ID}.{DATASET}.EmailKnowledgeBase"

MAILCHIMP_API_KEY = os.environ["MAILCHIMP_API_KEY"]
SERVER_PREFIX = MAILCHIMP_API_KEY.split("-")[-1]

credentials_info = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
credentials = service_account.Credentials.from_service_account_info(credentials_info)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)


def get_campaigns_without_subject():
    query = f"""
    SELECT campaign_id
    FROM `{KNOWLEDGE_TABLE}`
    WHERE SubjectLine IS NULL
      AND campaign_id IS NOT NULL
    ORDER BY SendTime DESC
    """
    return [row.campaign_id for row in client.query(query).result()]


def fetch_subject_line(campaign_id, retries=3):
    url = f"https://{SERVER_PREFIX}.api.mailchimp.com/3.0/campaigns/{campaign_id}"

    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                auth=("anystring", MAILCHIMP_API_KEY),
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "campaign_id":  campaign_id,
                    "subject_line": data.get("settings", {}).get("subject_line"),
                    "preview_text": data.get("settings", {}).get("preview_text"),
                }

            if response.status_code == 404:
                print(f"Not found: {campaign_id}")
                return None

            if response.status_code == 429:
                time.sleep(2)
                continue

            print(f"Error {response.status_code} for {campaign_id}: {response.text[:200]}")
            return None

        except Exception as e:
            print(f"Exception for {campaign_id}: {e}")
            time.sleep(2)

    return None


def update_batch(rows):
    if not rows:
        return

    value_rows = []
    for r in rows:
        if r.get("subject_line"):
            cid  = r["campaign_id"].replace("'", "")
            subj = (r["subject_line"] or "").replace("'", "\\'")
            prev = (r["preview_text"] or "").replace("'", "\\'")
            value_rows.append(f"('{cid}', '{subj}', '{prev}')")

    if not value_rows:
        print("No subject lines to update")
        return

    values_sql = ",\n      ".join(value_rows)

    merge_sql = f"""
    MERGE `{KNOWLEDGE_TABLE}` AS target
    USING (
      SELECT * FROM UNNEST([
        STRUCT<campaign_id STRING, subject_line STRING, preview_text STRING>
        {values_sql}
      ])
    ) AS source
    ON target.campaign_id = source.campaign_id
    WHEN MATCHED THEN UPDATE SET
      target.SubjectLine = source.subject_line,
      target.PreviewText = source.preview_text
    """

    try:
        client.query(merge_sql).result()
        print(f"Updated {len(value_rows)} campaigns")
    except Exception as e:
        print(f"MERGE error: {e}")


def main():
    campaign_ids = get_campaigns_without_subject()
    print(f"Found {len(campaign_ids)} campaigns without SubjectLine")

    if not campaign_ids:
        print("Nothing to do")
        return

    total      = len(campaign_ids)
    done       = 0
    updated    = 0
    failed     = 0
    batch      = []
    BATCH_SIZE = 50

    for campaign_id in campaign_ids:
        result = fetch_subject_line(campaign_id)

        if result:
            batch.append(result)
        else:
            failed += 1

        done += 1
        time.sleep(0.3)

        if len(batch) >= BATCH_SIZE:
            update_batch(batch)
            updated += len(batch)
            batch = []

        if done % 50 == 0:
            print(f"Progress: {done}/{total} | updated: {updated} | failed: {failed}")

    # Flush remainder
    update_batch(batch)
    updated += len(batch)

    print(f"Backfill complete: {updated} updated, {failed} failed")


if __name__ == "__main__":
    main()
