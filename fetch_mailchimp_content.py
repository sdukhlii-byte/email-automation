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

            return {
                "campaign_id": campaign_id,
                "html_content": html,
                "plain_text_content": data.get("plain_text"),
                "clean_text": clean_html(html),
                "archive_html": data.get("archive_html"),
                "fetched_at": datetime.now(timezone.utc).isoformat()
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
            rows = []

        print(f"{i}/{len(campaign_ids)} done")

    insert_rows(rows)
    print("Done")

if __name__ == "__main__":
    main()
