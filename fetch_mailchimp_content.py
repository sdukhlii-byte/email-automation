import os
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT_ID = "x-fabric-494718-d1"
DATASET = "datasetmailchimp"
REPORTS_TABLE = f"{PROJECT_ID}.{DATASET}.Reports"
CONTENT_TABLE = f"{PROJECT_ID}.{DATASET}.CampaignContentsRaw"

MAILCHIMP_API_KEY = os.environ["MAILCHIMP_API_KEY"]
SERVER_PREFIX = MAILCHIMP_API_KEY.split("-")[-1]  # например us10

client = bigquery.Client(project=PROJECT_ID)

def get_campaign_ids():
    query = f"""
    SELECT DISTINCT Id
    FROM `{REPORTS_TABLE}`
    WHERE Id IS NOT NULL
      AND Id NOT IN (
        SELECT campaign_id FROM `{CONTENT_TABLE}`
      )
    """
    return [row.Id for row in client.query(query).result()]

def fetch_campaign_content(campaign_id):
    url = f"https://{SERVER_PREFIX}.api.mailchimp.com/3.0/campaigns/{campaign_id}/content"

    response = requests.get(
        url,
        auth=("anystring", MAILCHIMP_API_KEY),
        timeout=30
    )

    if response.status_code != 200:
        print(f"Error {response.status_code} for {campaign_id}: {response.text[:300]}")
        return None

    data = response.json()

    return {
        "campaign_id": campaign_id,
        "html_content": data.get("html"),
        "plain_text_content": data.get("plain_text"),
        "archive_html": data.get("archive_html"),
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }

def main():
    campaign_ids = get_campaign_ids()
    print(f"Campaigns to fetch: {len(campaign_ids)}")

    rows = []

    for i, campaign_id in enumerate(campaign_ids, start=1):
        content = fetch_campaign_content(campaign_id)

        if content:
            rows.append(content)

        if len(rows) >= 100:
            errors = client.insert_rows_json(CONTENT_TABLE, rows)
            if errors:
                print(errors)
            else:
                print(f"Inserted {len(rows)} rows")
            rows = []

        print(f"{i}/{len(campaign_ids)} done")

    if rows:
        errors = client.insert_rows_json(CONTENT_TABLE, rows)
        if errors:
            print(errors)
        else:
            print(f"Inserted final {len(rows)} rows")

if __name__ == "__main__":
    main()
