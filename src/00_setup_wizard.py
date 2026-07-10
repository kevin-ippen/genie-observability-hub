# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Observability Hub — Setup Wizard
# MAGIC
# MAGIC Interactive setup for first-time deployment. Run this notebook to:
# MAGIC 1. Validate prerequisites (permissions, system tables, warehouse)
# MAGIC 2. Configure space tags and workspace filters
# MAGIC 3. Run initial bootstrap + first harvest
# MAGIC 4. Verify data landed correctly

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Target Catalog")
dbutils.widgets.text("schema", "genie_observability", "Target Schema")
dbutils.widgets.text("workspace_filter", "", "Workspace IDs to monitor (comma-separated, blank=all)")
dbutils.widgets.text("space_tags", "", "JSON: {space_id: [tag1, tag2], ...}")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WORKSPACE_FILTER = dbutils.widgets.get("workspace_filter")
SPACE_TAGS = dbutils.widgets.get("space_tags")

assert CATALOG, "catalog is required"
PREFIX = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prerequisite Checks

# COMMAND ----------

import json

checks = []

# Check system tables access
for table in ["system.access.audit", "system.billing.usage", "system.query.history"]:
    try:
        spark.sql(f"SELECT 1 FROM {table} LIMIT 1").collect()
        checks.append(("✅", table, "accessible"))
    except Exception as e:
        checks.append(("❌", table, str(e)[:100]))

# Check catalog permissions
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PREFIX}")
    checks.append(("✅", f"{PREFIX}", "schema created/exists"))
except Exception as e:
    checks.append(("❌", f"{PREFIX}", f"Cannot create schema: {str(e)[:100]}"))

# Check Genie API access
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
try:
    # List spaces to verify API access
    resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_size": 1})
    space_count = len(resp.get("spaces", []))
    checks.append(("✅", "Genie API", f"accessible ({space_count}+ spaces)"))
except Exception as e:
    checks.append(("⚠️", "Genie API", f"Limited access: {str(e)[:100]}"))

for status, resource, detail in checks:
    print(f"  {status} {resource}: {detail}")

failed = [c for c in checks if c[0] == "❌"]
if failed:
    raise Exception(f"Prerequisites not met: {[c[1] for c in failed]}")

print("\n✅ All prerequisites passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configure Space Tags & Workspace Filter
# MAGIC
# MAGIC Space tags let you group spaces for filtering (e.g., "production", "sales-team", "pilot").
# MAGIC Workspace filter restricts harvesting to specific workspace IDs (multi-workspace deployments).

# COMMAND ----------

# Create configuration table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PREFIX}.harvest_config (
  config_key STRING NOT NULL COMMENT 'Configuration key',
  config_value STRING COMMENT 'JSON or plain value',
  updated_at TIMESTAMP NOT NULL,
  updated_by STRING,
  CONSTRAINT pk_config PRIMARY KEY (config_key)
) USING DELTA
COMMENT 'Harvester configuration: workspace filters, space tags, retention, alerts'
""")

# Upsert workspace filter
if WORKSPACE_FILTER.strip():
    workspace_ids = json.dumps([w.strip() for w in WORKSPACE_FILTER.split(",") if w.strip()])
    spark.sql(f"""
        MERGE INTO {PREFIX}.harvest_config AS t
        USING (SELECT 'workspace_filter' AS config_key, '{workspace_ids}' AS config_value,
               current_timestamp() AS updated_at, current_user() AS updated_by) AS s
        ON t.config_key = s.config_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Set workspace filter: {workspace_ids}")

# Upsert space tags
if SPACE_TAGS.strip():
    spark.sql(f"""
        MERGE INTO {PREFIX}.harvest_config AS t
        USING (SELECT 'space_tags' AS config_key, '{SPACE_TAGS}' AS config_value,
               current_timestamp() AS updated_at, current_user() AS updated_by) AS s
        ON t.config_key = s.config_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Set space tags: {SPACE_TAGS}")

# COMMAND ----------

# Create space_tags lookup table (denormalized for fast filtering)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PREFIX}.space_tags (
  space_id STRING NOT NULL COMMENT 'Genie Space UUID',
  tag STRING NOT NULL COMMENT 'Tag label (e.g., production, sales-team, pilot)',
  assigned_by STRING COMMENT 'Who assigned this tag',
  assigned_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_space_tags PRIMARY KEY (space_id, tag)
) USING DELTA
COMMENT 'User-defined tags for Genie Spaces — used for dashboard filtering'
""")

# Populate from JSON config if provided
if SPACE_TAGS.strip():
    try:
        tags_dict = json.loads(SPACE_TAGS)
        rows = []
        for space_id, tags in tags_dict.items():
            for tag in tags:
                rows.append({"space_id": space_id, "tag": tag})
        if rows:
            df = spark.createDataFrame(rows)
            df.createOrReplaceTempView("_new_tags")
            spark.sql(f"""
                MERGE INTO {PREFIX}.space_tags AS t
                USING (SELECT *, current_user() AS assigned_by, current_timestamp() AS assigned_at FROM _new_tags) AS s
                ON t.space_id = s.space_id AND t.tag = s.tag
                WHEN NOT MATCHED THEN INSERT (space_id, tag, assigned_by, assigned_at)
                VALUES (s.space_id, s.tag, s.assigned_by, s.assigned_at)
            """)
            print(f"Inserted {len(rows)} space tags")
    except json.JSONDecodeError as e:
        print(f"⚠️ Could not parse space_tags JSON: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Set Default Retention & Alert Thresholds

# COMMAND ----------

defaults = {
    "retention_days": "90",
    "alert_failure_rate_threshold": "0.15",
    "alert_cost_spike_threshold": "2.0",
    "alert_latency_p95_threshold_ms": "30000",
    "alert_recipients": json.dumps([]),
}

for key, value in defaults.items():
    spark.sql(f"""
        MERGE INTO {PREFIX}.harvest_config AS t
        USING (SELECT '{key}' AS config_key, '{value}' AS config_value,
               current_timestamp() AS updated_at, current_user() AS updated_by) AS s
        ON t.config_key = s.config_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print("Default configuration set:")
display(spark.sql(f"SELECT * FROM {PREFIX}.harvest_config ORDER BY config_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify: Run Quick Discovery

# COMMAND ----------

# Show what spaces are visible
spaces_resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_size": 50})
spaces = spaces_resp.get("spaces", [])

# Inspect actual field names from first space
if spaces:
    print("API response keys:", list(spaces[0].keys()))
    print()

print(f"Found {len(spaces)} Genie Spaces accessible to current user:\n")
for s in spaces[:20]:
    # Try common field names for display
    name = s.get('title') or s.get('display_name') or s.get('name') or s.get('space_name') or 'unnamed'
    space_id = s.get('id') or s.get('space_id') or '?'
    print(f"  • {name} ({space_id[:8]}...)")

if len(spaces) > 20:
    print(f"  ... and {len(spaces) - 20} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Next Steps
# MAGIC
# MAGIC **Done so far** (this wizard): schema created, `harvest_config` + `space_tags` tables populated, API access verified.
# MAGIC
# MAGIC **Remaining**:
# MAGIC 1. Run `01_bootstrap` — creates the 6 core Delta tables (`genie_spaces`, `genie_conversations`, `genie_messages`, `genie_query_executions`, `genie_cost_attribution`, `harvest_watermarks`)
# MAGIC 2. Run `02_harvester` — first data pull (audit log → API → Delta)
# MAGIC 3. Run `03_cost_attribution` — backfill per-user cost from billing.usage
# MAGIC 4. Deploy the bundle:
# MAGIC    ```bash
# MAGIC    databricks bundle deploy -t dev \
# MAGIC      --var catalog=serverless_stable_h7wanf_catalog \
# MAGIC      --var warehouse_id=4047b28d66a51bdc
# MAGIC    ```
# MAGIC 5. (Optional) Tag spaces for dashboard filtering:
# MAGIC    ```sql
# MAGIC    INSERT INTO serverless_stable_h7wanf_catalog.genie_observability.space_tags VALUES
# MAGIC      ('01f17ba95e361317972969baab2bdf45', 'production', current_user(), current_timestamp()),
# MAGIC      ('01f16ff6...', 'fleet-ops', current_user(), current_timestamp());
# MAGIC    ```
