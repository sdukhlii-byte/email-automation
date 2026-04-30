"""
BigQuery SQL tool for the email marketing agent.
Executes read-only queries against the datasetmailchimp dataset.
"""

import json
import logging
import os
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

log = logging.getLogger(__name__)

PROJECT   = "x-fabric-494718-d1"
DATASET   = "datasetmailchimp"

# Tables the agent is allowed to query
ALLOWED_TABLES = {
    "EmailKnowledgeBase",
    "EmailEnrichment",
    "Reports",
    "CampaignContentsRaw",
    "Lists",
}

# ---------------------------------------------------------------------------
# Client (singleton)
# ---------------------------------------------------------------------------
_bq_client: bigquery.Client | None = None

def get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if creds_json:
            info = json.loads(creds_json)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            _bq_client = bigquery.Client(credentials=creds, project=info["project_id"])
        else:
            _bq_client = bigquery.Client()
    return _bq_client


# ---------------------------------------------------------------------------
# Schema helper — agent can call this to understand table structure
# ---------------------------------------------------------------------------
def get_schema() -> str:
    """Returns a compact schema description for the agent's context."""
    return """
Available tables in x-fabric-494718-d1.datasetmailchimp:

EmailKnowledgeBase (main table):
  campaign_id, CampaignTitle, SubjectLine, PreviewText, SendTime,
  ListName, EmailsSent, Opens_UniqueOpens, Clicks_UniqueClicks,
  Unsubscribed, open_rate_percent, ctr_percent, unsub_rate_percent, clean_text

EmailEnrichment (GPT classifications, JOIN on campaign_id):
  campaign_id, hook_type, offer_type, angle, language, geo,
  cta, tone, summary, reasoning, enriched_at

Reports (raw Mailchimp metrics, JOIN on Id = campaign_id):
  Id, CampaignTitle, SubjectLine, SendTime, EmailsSent,
  Opens_OpenRate, Clicks_ClickRate, ListId

Lists:
  Id, Name

Example useful queries:
  -- Top campaigns by open rate
  SELECT SubjectLine, open_rate_percent, ctr_percent, hook_type, tone
  FROM EmailKnowledgeBase k
  JOIN EmailEnrichment e USING (campaign_id)
  ORDER BY open_rate_percent DESC LIMIT 10

  -- Performance by hook type
  SELECT e.hook_type,
    COUNT(*) as campaigns,
    ROUND(AVG(k.open_rate_percent),2) as avg_open_rate,
    ROUND(AVG(k.ctr_percent),2) as avg_ctr
  FROM EmailKnowledgeBase k
  JOIN EmailEnrichment e USING (campaign_id)
  GROUP BY 1 ORDER BY avg_open_rate DESC
""".strip()


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------
def run_sql(query: str, max_rows: int = 50) -> str:
    """
    Executes a BigQuery SQL query and returns results as a formatted string.
    Only SELECT statements allowed. Max 50 rows returned to keep context tight.
    """
    q = query.strip().upper()
    if not q.startswith("SELECT") and not q.startswith("WITH"):
        return "ERROR: Only SELECT queries are allowed."

    # Basic injection guard — no DML
    for keyword in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "MERGE", "TRUNCATE"):
        if keyword in q:
            return f"ERROR: {keyword} statements are not allowed."

    try:
        client = get_bq_client()
        job_config = bigquery.QueryJobConfig(
            default_dataset=f"{PROJECT}.{DATASET}",
            maximum_bytes_billed=100 * 1024 * 1024,  # 100 MB cap
        )
        rows = list(client.query(query, job_config=job_config).result())

        if not rows:
            return "Query returned 0 rows."

        # Format as markdown table
        fields = [f.name for f in rows[0].__class__._meta.fields] if hasattr(rows[0].__class__, '_meta') else list(rows[0].keys())
        
        # Get column names from first row
        cols = list(dict(rows[0]).keys())
        lines = ["| " + " | ".join(cols) + " |"]
        lines.append("| " + " | ".join(["---"] * len(cols)) + " |")

        for row in rows[:max_rows]:
            d = dict(row)
            lines.append("| " + " | ".join(str(d.get(c, "")) for c in cols) + " |")

        result = "\n".join(lines)
        if len(rows) > max_rows:
            result += f"\n\n_(showing {max_rows} of {len(rows)} rows)_"

        return result

    except Exception as e:
        log.error("BigQuery error: %s", e)
        return f"ERROR: {e}"


# ---------------------------------------------------------------------------
# Tool definition for OpenAI function calling
# ---------------------------------------------------------------------------
SQL_TOOL_SPEC = {
    "type": "function",
    "function": {
        "name": "sql_tool",
        "description": (
            "Runs a BigQuery SQL SELECT query against the Mailchimp email marketing database. "
            "Use for aggregations, rankings, trend analysis, filtering by metrics. "
            "Always use fully qualified table names like `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` "
            "or rely on the default dataset. JOIN EmailEnrichment for hook_type, tone, geo, language."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Valid BigQuery SQL SELECT statement.",
                }
            },
            "required": ["query"],
        },
    },
}
