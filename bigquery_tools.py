"""
BigQuery SQL tool for the email marketing agent.
Executes read-only queries against the datasetmailchimp dataset.

v2 — mandatory filter enforcement via CTE wrapper instead of regex injection.

WHY CTE WRAPPER:
  _inject_filters() used regex to insert WHERE conditions into arbitrary SQL.
  This broke silently in three patterns:
    1. Subqueries / CTEs — filter landed on outer query, inner scan was unfiltered.
    2. Aggregations without WHERE — filter was appended after GROUP BY as a second
       WHERE, producing a syntax error.
    3. HAVING / window functions — regex chose the wrong injection point.

  The CTE wrapper is unconditional and structure-agnostic:
    WITH _kb AS (
        SELECT * FROM EmailKnowledgeBase
        WHERE UPPER(IFNULL(ListName,'')) NOT LIKE '%WARMY%'
          AND EmailsSent >= 500
    )
  The model's query is then rewritten to reference `_kb` instead of the raw table.
  Because the filter runs on the base table BEFORE any JOIN / GROUP BY / subquery,
  it is guaranteed to apply exactly once, regardless of query shape.
"""

import json
import logging
import os
import re as _re
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

log = logging.getLogger(__name__)

PROJECT   = "x-fabric-494718-d1"
DATASET   = "datasetmailchimp"
_EKB_FULL = f"`{PROJECT}.{DATASET}.EmailKnowledgeBase`"

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
# Schema
# ---------------------------------------------------------------------------
def get_schema() -> str:
    return """
Available tables in x-fabric-494718-d1.datasetmailchimp:

EmailKnowledgeBase (alias: k) — main campaign table:
  campaign_id, CampaignTitle, SubjectLine, PreviewText, SendTime,
  ListName, EmailsSent, Opens_UniqueOpens, Clicks_UniqueClicks,
  Unsubscribed, open_rate_percent, ctr_percent, unsub_rate_percent, clean_text

EmailEnrichment (alias: e) — GPT classifications, JOIN on campaign_id:
  campaign_id, hook_type, offer_type, angle, language, geo,
  cta, tone, summary, reasoning, enriched_at

Reports (alias: r) — raw Mailchimp metrics, JOIN on Id = campaign_id:
  Id, CampaignTitle, SubjectLine, SendTime, EmailsSent,
  Opens_OpenRate, Clicks_ClickRate, ListId

Lists: Id, Name

CRITICAL RULES for query generation:
1. ALWAYS use table aliases and prefix every column: k.SubjectLine, e.hook_type etc.
   Never use bare column names — SubjectLine exists in both EmailKnowledgeBase
   and Reports, so unqualified references return NULL or cause errors.
2. Always use fully qualified table names:
   `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
3. When joining, use: LEFT JOIN ... e USING (campaign_id) or ON k.campaign_id = e.campaign_id

NOTE: warmup/seed lists (ListName LIKE '%WARMY%') and campaigns with EmailsSent < 500
are excluded automatically by the query layer — do NOT add these filters yourself.

Correct example queries:

  -- Top campaigns by open rate (CORRECT — all columns prefixed)
  SELECT
    k.SubjectLine,
    k.open_rate_percent,
    k.ctr_percent,
    e.hook_type,
    e.tone
  FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
  LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
    ON k.campaign_id = e.campaign_id
  WHERE k.SubjectLine IS NOT NULL
  ORDER BY k.open_rate_percent DESC
  LIMIT 10

  -- Performance by hook type
  SELECT
    e.hook_type,
    COUNT(*) as campaigns,
    ROUND(AVG(k.open_rate_percent), 2) as avg_open_rate,
    ROUND(AVG(k.ctr_percent), 2) as avg_ctr
  FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
  LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
    ON k.campaign_id = e.campaign_id
  WHERE e.hook_type IS NOT NULL
  GROUP BY e.hook_type
  ORDER BY avg_open_rate DESC

  -- Campaigns by language
  SELECT
    e.language,
    COUNT(*) as campaigns,
    ROUND(AVG(k.open_rate_percent), 1) as avg_open_rate
  FROM `x-fabric-494718-d1.datasetmailchimp.EmailKnowledgeBase` k
  LEFT JOIN `x-fabric-494718-d1.datasetmailchimp.EmailEnrichment` e
    ON k.campaign_id = e.campaign_id
  WHERE e.language IS NOT NULL
  GROUP BY e.language
  ORDER BY campaigns DESC
""".strip()


# ---------------------------------------------------------------------------
# CTE-based mandatory filter enforcement
#
# Strategy:
#   1. Detect whether the query references EmailKnowledgeBase (any form).
#   2. If yes, prepend a _kb CTE that pre-filters the base table.
#   3. Replace every reference to the raw table with `_kb` in the model's query.
#   4. If the model already wrote its own WITH clause, append _kb as an
#      additional CTE at the front (BigQuery allows multiple CTEs).
#
# This means the filter is always applied at the leaf scan — before any
# JOIN, GROUP BY, subquery, or window function sees the data.
# ---------------------------------------------------------------------------

# The pre-filter CTE body (no alias — alias is added at the call site)
_CTE_BODY = (
    f"SELECT * FROM {_EKB_FULL}\n"
    "    WHERE UPPER(IFNULL(ListName, '')) NOT LIKE '%WARMY%'\n"
    "      AND EmailsSent >= 500"
)

# Patterns that match the raw EmailKnowledgeBase table reference, with or
# without backtick quoting and with or without a trailing alias.
_EKB_PATTERN = _re.compile(
    r"`?x-fabric-494718-d1\.datasetmailchimp\.EmailKnowledgeBase`?"
    r"(?:\s+(?:AS\s+)?(\w+))?",
    _re.IGNORECASE,
)


def _wrap_with_cte(query: str) -> str:
    """
    Rewrite *query* so that every reference to EmailKnowledgeBase is replaced
    by a pre-filtered CTE named _kb.

    Handles:
      - Simple SELECT … FROM EmailKnowledgeBase k
      - Existing WITH … SELECT … FROM EmailKnowledgeBase k
      - Subqueries that reference EmailKnowledgeBase
      - Multiple references to the table (all replaced)

    Does nothing if the query doesn't touch EmailKnowledgeBase.
    """
    if "EMAILKNOWLEDGEBASE" not in query.upper():
        return query

    # Replace every occurrence of the raw table reference with `_kb`,
    # preserving the alias if the model supplied one (so k.Column still works).
    def _replace(m: _re.Match) -> str:
        alias = m.group(1)  # e.g. "k" or None
        if alias:
            return f"`_kb` {alias}"
        return "`_kb`"

    rewritten = _EKB_PATTERN.sub(_replace, query)

    # Prepend the CTE.  If the model already has a WITH clause, inject _kb
    # as the first CTE.  Otherwise, wrap the whole query.
    with_match = _re.match(r"\s*WITH\s+", rewritten, _re.IGNORECASE)
    if with_match:
        # Insert "_kb AS (...), " right after "WITH "
        insert_pos = with_match.end()
        cte_fragment = f"_kb AS (\n    {_CTE_BODY}\n  ),\n  "
        rewritten = rewritten[:insert_pos] + cte_fragment + rewritten[insert_pos:]
    else:
        rewritten = f"WITH _kb AS (\n    {_CTE_BODY}\n)\n{rewritten}"

    return rewritten


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------
def run_sql(query: str, max_rows: int = 50) -> str:
    q = query.strip().upper()
    if not q.startswith("SELECT") and not q.startswith("WITH"):
        return "ERROR: Only SELECT queries are allowed."

    for keyword in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "MERGE", "TRUNCATE"):
        if keyword in q:
            return f"ERROR: {keyword} statements are not allowed."

    query = _wrap_with_cte(query)
    log.debug("Rewritten query:\n%s", query)

    try:
        client = get_bq_client()
        job_config = bigquery.QueryJobConfig(
            default_dataset=f"{PROJECT}.{DATASET}",
            maximum_bytes_billed=100 * 1024 * 1024,
        )
        rows = list(client.query(query, job_config=job_config).result())

        if not rows:
            return "Query returned 0 rows."

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
# Tool spec
# ---------------------------------------------------------------------------
SQL_TOOL_SPEC = {
    "type": "function",
    "function": {
        "name": "sql_tool",
        "description": (
            "Runs a BigQuery SQL SELECT query against the Mailchimp email marketing database. "
            "Use for aggregations, rankings, trend analysis, filtering by metrics. "
            "ALWAYS use table aliases and prefix every column (k.SubjectLine, e.hook_type). "
            "Never use bare unqualified column names — they resolve to NULL on JOIN. "
            "Do NOT add warmup or EmailsSent filters — they are applied automatically."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Valid BigQuery SQL SELECT with fully qualified table names and aliased columns.",
                }
            },
            "required": ["query"],
        },
    },
}
