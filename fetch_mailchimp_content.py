import os
import json
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime, timezone

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "x-fabric-494718-d1"
DATASET = "datasetmailchimp"

REPORTS_TABLE = f"{PROJECT_ID}.{DATASET}.Reports"
CONTENT_TABLE = f"{PROJECT_ID}.{DATASET}.CampaignContentsRaw"
KNOWLEDGE_TABLE = f"{PROJECT_ID}.{DATASET}.EmailKnowledgeBase"

MAILCHIMP_API_KEY = os.environ["MAILCHIMP_API_KEY"]
SERVER_PREFIX = MAILCHIMP_API_KEY.split("-")[-1]

credentials_info = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
credentials = service_account.Credentials.from_service_account_info(credentials_info)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)

def clean_html(html):
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style"]):
        tag.decompose()

    text = soup.get_text(separator=" ")
    text = " ".join(text.split())

    return text

def get_campaign_ids():
    query = f"""
    SELECT DISTINCT Id
    FROM `{REPORTS_TABLE}`
    WHERE Id IS NOT NULL
      AND Id NOT IN (
        SELECT campaign_id
        FROM `{CONTENT_TABLE}`
      )
    """
    return [row.Id for row in client.query(query).result()]

def fetch_campaign_content(campaign_id, retries=3):
    url = f"https://{SERVER_PREFIX}.api.mailchimp.com/3.0/campaigns/{campaign_id}/content"
    meta_url = f"https://{SERVER_PREFIX}.api.mailchimp.com/3.0/campaigns/{campaign_id}"

    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                auth=("anystring", MAILCHIMP_API_KEY),
                timeout=30
            )

            if response.status_code == 429:
                print(f"429 rate limit for {campaign_id}, retrying...")
                time.sleep(2)
                continue

            if response.status_code != 200:
                print(f"Error {response.status_code} for {campaign_id}: {response.text[:200]}")
                return None

            data = response.json()
            html = data.get("html")

            # Fetch campaign metadata (subject line, preview text)
            subject_line = None
            preview_text = None
            try:
                meta_response = requests.get(
                    meta_url,
                    auth=("anystring", MAILCHIMP_API_KEY),
                    timeout=30
                )
                if meta_response.status_code == 200:
                    meta = meta_response.json()
                    subject_line = meta.get("settings", {}).get("subject_line")
                    preview_text = meta.get("settings", {}).get("preview_text")
            except Exception as me:
                print(f"Meta fetch error for {campaign_id}: {me}")

            return {
                "campaign_id":        campaign_id,
                "html_content":       html,
                "plain_text_content": data.get("plain_text"),
                "clean_text":         clean_html(html),
                "archive_html":       data.get("archive_html"),
                "subject_line":       subject_line,
                "preview_text":       preview_text,
                "fetched_at":         datetime.now(timezone.utc).isoformat()
            }

        except Exception as e:
            print(f"Exception for {campaign_id}: {str(e)}")
            time.sleep(2)

    return None

def insert_rows(rows):
    if not rows:
        return

    errors = client.insert_rows_json(CONTENT_TABLE, rows)

    if errors:
        print("BigQuery insert errors:", errors)
    else:
        print(f"Inserted {len(rows)} rows")

def update_subject_lines(rows):
    """
    After inserting content rows, backfill SubjectLine and
    PreviewText into EmailKnowledgeBase using a MERGE statement.
    """
    if not rows:
        return

    # Build temp values for MERGE
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

    values_sql = ",\n  ".join(value_rows)

    merge_sql = f"""
    MERGE `{KNOWLEDGE_TABLE}` AS target
    USING (
      SELECT campaign_id, subject_line, preview_text
      FROM UNNEST([
        STRUCT<campaign_id STRING,
               subject_line STRING,
               preview_text STRING>
        {values_sql}
      ])
    ) AS source
    ON target.campaign_id = source.campaign_id
    WHEN MATCHED THEN UPDATE SET
      target.SubjectLine  = source.subject_line,
      target.PreviewText  = source.preview_text
    """

    try:
        client.query(merge_sql).result()
        print(f"Updated SubjectLine for {len(value_rows)} campaigns")
    except Exception as e:
        print(f"MERGE error: {e}")

def main():
    campaign_ids = get_campaign_ids()
    print(f"Campaigns to fetch: {len(campaign_ids)}")

    rows = []

    for i, campaign_id in enumerate(campaign_ids, start=1):
        content = fetch_campaign_content(campaign_id)

        if content:
            rows.append(content)

        time.sleep(0.2)

        if len(rows) >= 100:
            insert_rows(rows)
            update_subject_lines(rows)
            rows = []

        print(f"{i}/{len(campaign_ids)} done")

    insert_rows(rows)
    update_subject_lines(rows)
    print("Done")

if __name__ == "__main__":
    main()
