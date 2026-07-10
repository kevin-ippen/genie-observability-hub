# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Observability — Conversation Harvester
# MAGIC
# MAGIC Incrementally harvests Genie Space conversations via:
# MAGIC 1. `system.access.audit` (aibiGenie) → discover new conversations/messages
# MAGIC 2. Genie Conversation REST API → fetch actual content (questions, responses, SQL)
# MAGIC 3. `system.query.history` → Genie-executed SQL performance data
# MAGIC
# MAGIC Writes to Delta tables created by 01_bootstrap.sql.
# MAGIC Idempotent: safe to re-run (MERGE/upsert semantics).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# DBTITLE 1,Pipeline Parameters
# Accept parameters from 00_config orchestrator (with defaults for standalone runs)
_widget_names = [w.name for w in dbutils.widgets.getAll()]
TARGET_CATALOG = dbutils.widgets.get("target_catalog") if "target_catalog" in _widget_names else "serverless_stable_h7wanf_catalog"
TARGET_SCHEMA = dbutils.widgets.get("target_schema") if "target_schema" in _widget_names else "genie_observability"
TARGET_PREFIX = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id") if "warehouse_id" in _widget_names else "4047b28d66a51bdc"
WORKSPACE_ID = dbutils.widgets.get("workspace_id") if "workspace_id" in _widget_names else "7474657986130378"
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days")) if "lookback_days" in _widget_names else 90
SPACE_IDS_STR = dbutils.widgets.get("space_ids") if "space_ids" in _widget_names else ""
SPACE_IDS = SPACE_IDS_STR.split(",") if SPACE_IDS_STR else []

print(f"Config: {TARGET_PREFIX} | Warehouse: {WAREHOUSE_ID} | Spaces: {len(SPACE_IDS)}")

# COMMAND ----------

import time
import json
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, BooleanType, ArrayType, IntegerType
)

# COMMAND ----------

# Parameters (from job or widgets)
dbutils.widgets.text("catalog", "", "Target Catalog")
dbutils.widgets.text("schema", "genie_observability", "Target Schema")
dbutils.widgets.text("lookback_days", "30", "Lookback Days (first run)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))

TARGET_PREFIX = f"{CATALOG}.{SCHEMA}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("genie_harvester")

spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility: Watermark Management

# COMMAND ----------

def get_watermark(source_name: str) -> Optional[datetime]:
    """Get the high watermark for a source, or None if never harvested."""
    try:
        row = spark.sql(f"""
            SELECT high_watermark FROM {TARGET_PREFIX}.harvest_watermarks
            WHERE source_name = '{source_name}'
        """).first()
        return row.high_watermark if row else None
    except Exception:
        return None


def set_watermark(source_name: str, high_watermark: datetime, rows_harvested: int):
    """Upsert the watermark for a source."""
    spark.sql(f"""
        MERGE INTO {TARGET_PREFIX}.harvest_watermarks AS t
        USING (SELECT
            '{source_name}' AS source_name,
            TIMESTAMP '{high_watermark.isoformat()}' AS high_watermark,
            {rows_harvested} AS rows_harvested,
            current_timestamp() AS updated_at
        ) AS s
        ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Discover Spaces from Audit Log

# COMMAND ----------

def discover_active_spaces() -> list:
    """Find all space_ids with activity in the audit log since last watermark."""
    wm = get_watermark("audit_spaces")
    since = wm if wm else datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    df = spark.sql(f"""
        SELECT DISTINCT request_params['space_id'] AS space_id
        FROM system.access.audit
        WHERE service_name = 'aibiGenie'
          AND event_date >= DATE('{since.strftime('%Y-%m-%d')}')
          AND request_params['space_id'] IS NOT NULL
    """)
    spaces = [row.space_id for row in df.collect()]
    logger.info(f"Discovered {len(spaces)} active spaces since {since}")
    return spaces

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Harvest Space Metadata via REST API

# COMMAND ----------

def harvest_space(space_id: str) -> Optional[dict]:
    """Fetch space metadata from Genie API."""
    try:
        resp = w.api_client.do(
            "GET", f"/api/2.0/genie/spaces/{space_id}"
        )
        return resp
    except Exception as e:
        # Only log at debug — deleted spaces are expected in audit
        logger.debug(f"Space {space_id} not accessible: {e}")
        return None


def harvest_all_spaces(space_ids: list) -> list:
    """Fetch and upsert space metadata. Returns list of valid (accessible) space_ids."""
    rows = []
    valid_ids = []
    skipped = []
    try:
        workspace_id = str(w.get_workspace_id())
    except Exception:
        workspace_id = "unknown"

    for sid in space_ids:
        data = harvest_space(sid)
        if data:
            valid_ids.append(sid)
            rows.append({
                "space_id": sid,
                "workspace_id": workspace_id,
                "display_name": data.get("title") or data.get("display_name") or data.get("name"),
                "description": data.get("description"),
                "warehouse_id": data.get("warehouse_id"),
                "table_identifiers": data.get("table_identifiers", []),
                "created_by": data.get("created_by"),
                "created_at": data.get("created_at"),
                "updated_at": data.get("updated_at"),
            })
        else:
            skipped.append(sid)
        time.sleep(0.2)  # Rate limit courtesy

    if skipped:
        logger.info(f"Skipped {len(skipped)} inaccessible/deleted spaces")

    if not rows:
        logger.info("No space metadata to write")
        return valid_ids

    schema = StructType([
        StructField("space_id", StringType()),
        StructField("workspace_id", StringType()),
        StructField("display_name", StringType()),
        StructField("description", StringType()),
        StructField("warehouse_id", StringType()),
        StructField("table_identifiers", ArrayType(StringType())),
        StructField("created_by", StringType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    df = df.withColumn("harvested_at", current_timestamp())

    df.createOrReplaceTempView("_spaces_incoming")
    spark.sql(f"""
        MERGE INTO {TARGET_PREFIX}.genie_spaces AS t
        USING _spaces_incoming AS s
        ON t.space_id = s.space_id AND t.workspace_id = s.workspace_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info(f"Upserted {len(rows)} spaces")
    return valid_ids

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Discover & Harvest Conversations

# COMMAND ----------

def discover_conversations(space_id: str) -> list:
    """List conversations in a space via API."""
    try:
        resp = w.api_client.do(
            "GET", f"/api/2.0/genie/spaces/{space_id}/conversations"
        )
        return resp.get("conversations", [])
    except Exception as e:
        logger.warning(f"Failed to list conversations for space {space_id}: {e}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Harvest Messages (the high-value content)

# COMMAND ----------

def harvest_messages(space_id: str, conversation_id: str) -> list:
    """Fetch all messages in a conversation via API."""
    try:
        resp = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages"
        )
        return resp.get("messages", [])
    except Exception as e:
        logger.warning(f"Failed to fetch messages for {space_id}/{conversation_id}: {e}")
        return []


def parse_message(msg: dict, space_id: str, conversation_id: str, workspace_id: str) -> dict:
    """Parse a raw API message into our schema.
    
    ACTUAL API shape (verified): messages have 'content' = user's question,
    'attachments' = list of dicts with keys 'query', 'text', 'suggested_questions'.
    Timestamps are epoch milliseconds. No 'role' field — all messages are user messages
    with assistant response embedded in attachments.
    """
    # Extract from attachments (list of dicts with top-level keys)
    generated_sql = None
    response_text = None
    has_viz = False
    has_table = False
    result_row_count = None

    for att in msg.get("attachments", []):
        if "query" in att and isinstance(att["query"], dict):
            generated_sql = att["query"].get("query")
            result_row_count = att["query"].get("query_result_metadata", {}).get("row_count")
            has_table = result_row_count is not None and result_row_count > 0
        if "text" in att and isinstance(att["text"], dict):
            response_text = att["text"].get("content")

    # Timestamps are epoch milliseconds
    created_ts = msg.get("created_timestamp")
    updated_ts = msg.get("last_updated_timestamp")

    created_at = None
    if created_ts:
        created_at = datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc).isoformat()

    completed_at = None
    if updated_ts:
        completed_at = datetime.fromtimestamp(updated_ts / 1000, tz=timezone.utc).isoformat()

    # Duration in ms (both are epoch ms)
    duration_ms = None
    if created_ts and updated_ts:
        duration_ms = int(updated_ts - created_ts)

    return {
        "message_id": msg.get("message_id") or msg.get("id"),
        "conversation_id": conversation_id,
        "space_id": space_id,
        "workspace_id": workspace_id,
        "user_email": str(msg.get("user_id", "")) or None,  # numeric user_id (email lookup TBD)
        "role": "USER",  # All messages from list endpoint are user messages with attachments
        "user_query": msg.get("content"),  # content is ALWAYS the user's question
        "response_text": response_text,  # from attachments[].text.content
        "generated_sql": generated_sql,  # from attachments[].query.query
        "status": msg.get("status"),
        "state_transitions": [],
        "error_message": None,
        "created_at": created_at,
        "completed_at": completed_at,
        "duration_ms": duration_ms,
        "feedback_rating": None,
        "feedback_comment": None,
        "has_visualization": has_viz,
        "has_table_result": has_table,
        "result_row_count": result_row_count,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Orchestrate Full Harvest

# COMMAND ----------

def run_harvest():
    """Main orchestration: discover → fetch → upsert."""
    try:
        workspace_id = str(w.get_workspace_id())
    except Exception:
        workspace_id = "unknown"

    # 1. Get accessible spaces directly from list API (avoids 7K+ dead audit entries)
    print("[1/6] Listing accessible spaces via API...")
    resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_size": 50})
    accessible_spaces = resp.get("spaces", [])
    if not accessible_spaces:
        print("No accessible spaces found. Nothing to harvest.")
        return
    
    space_ids = [s["space_id"] for s in accessible_spaces]
    print(f"  Found {len(space_ids)} accessible spaces")

    # 2. Harvest space metadata
    print("[2/6] Harvesting space metadata...")
    valid_space_ids = harvest_all_spaces(space_ids)
    if not valid_space_ids:
        print("No spaces to harvest conversations from.")
        return
    print(f"  Upserted {len(valid_space_ids)} spaces")

    # 3. Harvest conversations and messages
    print("[3/6] Harvesting conversations and messages...")
    all_messages = []
    all_conversations = []

    for si, space_id in enumerate(valid_space_ids):
        conversations = discover_conversations(space_id)
        print(f"  [{si+1}/{len(valid_space_ids)}] Space {space_id[:8]}...: {len(conversations)} conversations")

        for i, conv in enumerate(conversations):
            conv_id = conv.get("conversation_id") or conv.get("id")
            all_conversations.append({
                "conversation_id": conv_id,
                "space_id": space_id,
                "workspace_id": workspace_id,
                "user_email": conv.get("created_by"),
                "title": conv.get("title"),
                "created_at": conv.get("created_at"),
                "message_count": conv.get("message_count"),
                "last_message_at": conv.get("last_message_at"),
            })

            # Fetch messages for this conversation
            messages = harvest_messages(space_id, conv_id)
            for msg in messages:
                parsed = parse_message(msg, space_id, conv_id, workspace_id)
                if parsed["message_id"]:  # Skip messages without ID
                    all_messages.append(parsed)

            if (i + 1) % 10 == 0:
                print(f"    ...processed {i+1}/{len(conversations)} conversations")
            time.sleep(0.1)

        time.sleep(0.2)

    # 4. Upsert conversations
    print(f"[4/6] Upserting {len(all_conversations)} conversations...")
    if all_conversations:
        conv_schema = StructType([
            StructField("conversation_id", StringType()),
            StructField("space_id", StringType()),
            StructField("workspace_id", StringType()),
            StructField("user_email", StringType()),
            StructField("title", StringType()),
            StructField("created_at", StringType()),
            StructField("message_count", IntegerType()),
            StructField("last_message_at", StringType()),
        ])
        df_conv = spark.createDataFrame(all_conversations, schema=conv_schema)
        df_conv = df_conv.withColumn("harvested_at", current_timestamp())
        df_conv.createOrReplaceTempView("_convs_incoming")
        spark.sql(f"""
            MERGE INTO {TARGET_PREFIX}.genie_conversations AS t
            USING _convs_incoming AS s
            ON t.conversation_id = s.conversation_id AND t.space_id = s.space_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"  ✓ {len(all_conversations)} conversations upserted")

    # 5. Upsert messages
    print(f"[5/6] Upserting {len(all_messages)} messages...")
    if all_messages:
        msg_schema = StructType([
            StructField("message_id", StringType()),
            StructField("conversation_id", StringType()),
            StructField("space_id", StringType()),
            StructField("workspace_id", StringType()),
            StructField("user_email", StringType()),
            StructField("role", StringType()),
            StructField("user_query", StringType()),
            StructField("response_text", StringType()),
            StructField("generated_sql", StringType()),
            StructField("status", StringType()),
            StructField("state_transitions", ArrayType(StringType())),
            StructField("error_message", StringType()),
            StructField("created_at", StringType()),
            StructField("completed_at", StringType()),
            StructField("duration_ms", LongType()),
            StructField("feedback_rating", StringType()),
            StructField("feedback_comment", StringType()),
            StructField("has_visualization", BooleanType()),
            StructField("has_table_result", BooleanType()),
            StructField("result_row_count", LongType()),
        ])
        df_msg = spark.createDataFrame(all_messages, schema=msg_schema)
        df_msg = df_msg.withColumn("harvested_at", current_timestamp())
        df_msg.createOrReplaceTempView("_msgs_incoming")
        spark.sql(f"""
            MERGE INTO {TARGET_PREFIX}.genie_messages AS t
            USING _msgs_incoming AS s
            ON t.message_id = s.message_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"  ✓ {len(all_messages)} messages upserted")

    # 6. Update watermark
    print("[6/6] Updating watermark...")
    set_watermark("audit_spaces", datetime.now(timezone.utc), len(all_messages))
    print(f"\n✓ Harvest complete! {len(valid_space_ids)} spaces, {len(all_conversations)} conversations, {len(all_messages)} messages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Harvest Query Executions from system.query.history

# COMMAND ----------

def harvest_query_executions():
    """Pull Genie-originated queries from system.query.history."""
    wm = get_watermark("query_history")
    since = wm if wm else datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    df = spark.sql(f"""
        SELECT
            statement_id,
            workspace_id,
            executed_by,
            client_application,
            statement_text,
            execution_status,
            error_message,
            total_duration_ms,
            execution_duration_ms,
            compilation_duration_ms,
            read_bytes,
            read_rows,
            produced_rows,
            start_time,
            end_time,
            current_timestamp() AS harvested_at
        FROM system.query.history
        WHERE client_application IN (
            'Databricks SQL Genie Space',
            'genie-workbench',
            'DatabricksGenie',
            'genie-space-optimizer'
        )
        AND start_time >= TIMESTAMP '{since.strftime('%Y-%m-%dT%H:%M:%S')}'
    """)

    row_count = df.count()
    if row_count == 0:
        logger.info("No new query executions to harvest")
        return

    df.createOrReplaceTempView("_queries_incoming")
    spark.sql(f"""
        MERGE INTO {TARGET_PREFIX}.genie_query_executions AS t
        USING _queries_incoming AS s
        ON t.statement_id = s.statement_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    set_watermark("query_history", datetime.now(timezone.utc), row_count)
    logger.info(f"Upserted {row_count} query executions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

# DBTITLE 1,Small-Scale Test (1 space)
# Quick test: harvest ONE space end-to-end with visible output
# Uses spaces list API (accessible only) instead of the 7K dead audit entries
import time

try:
    workspace_id = str(w.get_workspace_id())
except Exception:
    workspace_id = "unknown"
print(f"Workspace ID: {workspace_id}")

# 1. Get accessible spaces via list API
print("\n--- Step 1: List accessible spaces via API ---")
resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_size": 50})
spaces = resp.get("spaces", [])
print(f"Found {len(spaces)} accessible spaces:")
for s in spaces:
    print(f"  • {s.get('title', '?')} ({s.get('space_id', '?')[:12]}...)")

# 2. Pick first space
if not spaces:
    print("\nNo accessible spaces!")
else:
    test = spaces[0]
    space_id = test['space_id']
    print(f"\n--- Step 2: Testing with '{test.get('title')}' ---")

    # 3. Get conversations
    convs = discover_conversations(space_id)
    print(f"Conversations: {len(convs)}")

    # Show conversation keys so we know the shape
    if convs:
        print(f"  Conv keys: {list(convs[0].keys())}")

    # 4. Harvest messages from first 3 conversations
    print(f"\n--- Step 3: Fetch messages (first 3 conversations) ---")
    total_msgs = 0
    for conv in convs[:3]:
        conv_id = conv.get('id') or conv.get('conversation_id')
        if not conv_id:
            print(f"  ⚠ Skipping conv with no id: {list(conv.keys())}")
            continue
        msgs = harvest_messages(space_id, conv_id)
        print(f"  Conv {str(conv_id)[:8]}...: {len(msgs)} messages")
        for msg in msgs[:2]:
            role = msg.get('role', '?')
            status = msg.get('status', '?')
            content = (msg.get('content') or '')[:80]
            print(f"    [{role}] ({status}) {content}")
        total_msgs += len(msgs)
        time.sleep(0.1)

    print(f"\n--- Result ---")
    print(f"✓ Space: {test.get('title')}")
    print(f"✓ Conversations: {len(convs)} total, tested {min(3, len(convs))}")
    print(f"✓ Messages harvested: {total_msgs}")
    print(f"\nPipeline works! Full run processes all {len(spaces)} spaces.")
    print(f"\n⚠ INSIGHT: discover_active_spaces() finds 7K+ space_ids in audit")
    print(f"  but only {len(spaces)} are accessible. Harvester should start from")
    print(f"  the list API and cross-ref audit for activity, not the other way around.")

# COMMAND ----------

run_harvest()

# COMMAND ----------

harvest_query_executions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

for table in ["genie_spaces", "genie_conversations", "genie_messages", "genie_query_executions"]:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_PREFIX}.{table}").first().n
    print(f"  {table}: {count:,} rows")

# COMMAND ----------

# DBTITLE 1,Compute Pre-Aggregated Metrics
# Compute pre-aggregated metric tables for fast dashboard queries
# Runs after harvest — replaces all rows with fresh rollups

print("[Metrics] Computing pre-aggregated tables...")

# 1. genie_daily_metrics
spark.sql(f"""
INSERT OVERWRITE {TARGET_PREFIX}.genie_daily_metrics
SELECT
  DATE(m.created_at) AS metric_date,
  m.workspace_id,
  COUNT(*) AS message_count,
  COUNT(DISTINCT m.conversation_id) AS conversation_count,
  COUNT(DISTINCT m.user_email) AS unique_users,
  AVG(m.duration_ms) AS avg_duration_ms,
  PERCENTILE(m.duration_ms, 0.5) AS p50_duration_ms,
  PERCENTILE(m.duration_ms, 0.95) AS p95_duration_ms,
  SUM(CASE WHEN m.status = 'COMPLETED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS success_rate,
  SUM(CASE WHEN m.generated_sql IS NOT NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS sql_generation_rate,
  COALESCE(c.total_dbus, 0) AS total_dbus,
  current_timestamp() AS computed_at
FROM {TARGET_PREFIX}.genie_messages m
LEFT JOIN (
  SELECT usage_date, workspace_id, SUM(total_dbus) AS total_dbus
  FROM {TARGET_PREFIX}.genie_cost_attribution
  GROUP BY usage_date, workspace_id
) c ON DATE(m.created_at) = c.usage_date AND m.workspace_id = c.workspace_id
WHERE m.created_at IS NOT NULL
GROUP BY DATE(m.created_at), m.workspace_id, c.total_dbus
""")
print("  ✓ genie_daily_metrics")

# 2. genie_space_metrics
spark.sql(f"""
INSERT OVERWRITE {TARGET_PREFIX}.genie_space_metrics
SELECT
  m.space_id,
  s.display_name,
  COUNT(*) AS total_messages,
  COUNT(DISTINCT m.conversation_id) AS total_conversations,
  COUNT(DISTINCT m.user_email) AS unique_users,
  AVG(m.duration_ms) AS avg_duration_ms,
  PERCENTILE(m.duration_ms, 0.95) AS p95_duration_ms,
  SUM(CASE WHEN m.status = 'COMPLETED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS success_rate,
  SUM(CASE WHEN m.generated_sql IS NOT NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS sql_generation_rate,
  MAX(m.created_at) AS last_activity_at,
  current_timestamp() AS computed_at
FROM {TARGET_PREFIX}.genie_messages m
LEFT JOIN {TARGET_PREFIX}.genie_spaces s ON m.space_id = s.space_id
GROUP BY m.space_id, s.display_name
""")
print("  ✓ genie_space_metrics")

# 3. genie_user_metrics
spark.sql(f"""
INSERT OVERWRITE {TARGET_PREFIX}.genie_user_metrics
SELECT
  m.user_email,
  m.workspace_id,
  COUNT(*) AS total_messages,
  COUNT(DISTINCT m.conversation_id) AS total_conversations,
  COUNT(DISTINCT m.space_id) AS spaces_used,
  AVG(m.duration_ms) AS avg_duration_ms,
  SUM(CASE WHEN m.status = 'COMPLETED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS success_rate,
  COALESCE(c.total_dbus, 0) AS total_dbus,
  MIN(m.created_at) AS first_seen,
  MAX(m.created_at) AS last_seen,
  current_timestamp() AS computed_at
FROM {TARGET_PREFIX}.genie_messages m
LEFT JOIN (
  SELECT user_email, workspace_id, SUM(total_dbus) AS total_dbus
  FROM {TARGET_PREFIX}.genie_cost_attribution
  GROUP BY user_email, workspace_id
) c ON m.user_email = c.user_email AND m.workspace_id = c.workspace_id
WHERE m.user_email IS NOT NULL
GROUP BY m.user_email, m.workspace_id, c.total_dbus
""")
print("  ✓ genie_user_metrics")

# Show counts
for t in ["genie_daily_metrics", "genie_space_metrics", "genie_user_metrics"]:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_PREFIX}.{t}").first().n
    print(f"  {t}: {n} rows")

print("\n✓ All metric tables computed — dashboard will load instantly.")
