-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Genie Observability — Cost Attribution
-- MAGIC
-- MAGIC Refreshes per-user daily Genie token cost from `system.billing.usage`.
-- MAGIC Joins with list prices for estimated USD cost.
-- MAGIC Idempotent: MERGE upsert.

-- COMMAND ----------

-- DBTITLE 1,Pipeline Parameters
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

# Bridge for SQL cells that use :catalog / :schema parameter syntax
dbutils.widgets.text("catalog", TARGET_CATALOG, "Catalog")
dbutils.widgets.text("schema", TARGET_SCHEMA, "Schema")

print(f"Config: {TARGET_PREFIX} | Warehouse: {WAREHOUSE_ID} | Spaces: {len(SPACE_IDS)}")

-- COMMAND ----------

MERGE INTO ${catalog}.${schema}.genie_cost_attribution AS t
USING (
  SELECT
    u.usage_date,
    u.workspace_id,
    COALESCE(u.custom_tags['Owner'], 'unknown') AS user_email,
    u.identity_metadata.run_as AS user_id,
    u.sku_name,
    SUM(u.usage_quantity) AS total_dbus,
    SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) AS estimated_cost_usd,
    current_timestamp() AS harvested_at
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.usage_date >= DATE(p.price_start_time)
    AND (p.price_end_time IS NULL OR u.usage_date < DATE(p.price_end_time))
  WHERE u.billing_origin_product = 'GENIE'
    AND u.usage_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY
    u.usage_date,
    u.workspace_id,
    COALESCE(u.custom_tags['Owner'], 'unknown'),
    u.identity_metadata.run_as,
    u.sku_name
) AS s
ON t.usage_date = s.usage_date
  AND t.workspace_id = s.workspace_id
  AND t.user_email = s.user_email
  AND t.sku_name = s.sku_name
WHEN MATCHED THEN UPDATE SET
  t.total_dbus = s.total_dbus,
  t.estimated_cost_usd = s.estimated_cost_usd,
  t.user_id = s.user_id,
  t.harvested_at = s.harvested_at
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- Update watermark
MERGE INTO IDENTIFIER(:catalog || '.' || :schema || '.harvest_watermarks') AS t
USING (SELECT
  'billing_cost' AS source_name,
  current_timestamp() AS high_watermark,
  (SELECT COUNT(*) FROM IDENTIFIER(:catalog || '.' || :schema || '.genie_cost_attribution')
   WHERE harvested_at >= current_timestamp() - INTERVAL 1 MINUTE) AS rows_harvested,
  current_timestamp() AS updated_at
) AS s
ON t.source_name = s.source_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- Summary
SELECT
  user_email,
  SUM(total_dbus) AS total_dbus,
  SUM(estimated_cost_usd) AS estimated_cost_usd,
  COUNT(DISTINCT usage_date) AS active_days
FROM IDENTIFIER(:catalog || '.' || :schema || '.genie_cost_attribution')
GROUP BY user_email
ORDER BY total_dbus DESC
LIMIT 20;
