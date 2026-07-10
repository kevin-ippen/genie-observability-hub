# Genie Observability Hub

End-to-end observability for Databricks Genie Spaces and Genie One queries. Goes beyond native audit logs by harvesting actual conversation content via the Genie REST API and storing it in governed Delta tables.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Sources                                   │
├─────────────────┬──────────────────┬────────────────────────────┤
│ system.access   │ Genie REST API   │ system.billing.usage        │
│ .audit          │ /conversations/  │ (GENIE product)             │
│ (event metadata)│ /messages/       │ (token cost attribution)    │
│                 │ (actual content) │                             │
└────────┬────────┴────────┬─────────┴──────────────┬─────────────┘
         │                 │                         │
         ▼                 ▼                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Delta Lakehouse                                │
├─────────────────┬──────────────────┬────────────────────────────┤
│ genie_spaces    │ genie_           │ genie_message_details       │
│ (space metadata)│ conversations    │ (user Q, response, SQL,     │
│                 │ (thread-level)   │  status, duration, tokens)  │
└────────┬────────┴────────┬─────────┴──────────────┬─────────────┘
         │                 │                         │
         ▼                 ▼                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Observability Layer                                  │
├──────────────────────┬──────────────────────────────────────────┤
│ Lakeview Dashboard   │ MLflow Traces (optional)                  │
│ (6-page admin view)  │ (step-level Genie debugging)              │
└──────────────────────┴──────────────────────────────────────────┘
```

## What This Solves

System tables (`system.access.audit`) only capture **event metadata** — space/conversation/message IDs, user emails, and HTTP status codes. They do NOT contain:
- The actual user question text
- Genie's natural-language response
- The generated SQL
- Step-level thinking/reasoning traces
- Token consumption per message

This project fills those gaps by calling the Genie Conversation API to extract full message content and persisting it in queryable Delta tables.

## Components

| Component | Path | Purpose |
|-----------|------|---------|
| Harvester | `src/01_harvester.py` | Scheduled job that scans audit logs, calls Genie API, upserts to Delta |
| DDL | `src/00_ddl.py` | Creates/migrates the 3 Delta tables + cost view |
| Tracing | `src/lib/tracing.py` | MLflow trace wrapper for step-level Genie debugging |
| Config | `src/lib/config.py` | Shared constants (catalog, schema, API paths) |
| Dashboard | `dashboards/genie_observability.lvdash.json` | 6-page Lakeview dashboard |
| Bundle | `databricks.yml` | DABs deployment config |

## Deployment

```bash
# Validate
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Run harvester manually
databricks bundle run -t dev genie_harvester
```

## Delta Tables

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `genie_spaces` | 1 row per space | space_id, display_name, warehouse_id, tables, created_by |
| `genie_conversations` | 1 row per conversation | conversation_id, space_id, user_email, created_at, message_count |
| `genie_message_details` | 1 row per message | message_id, conversation_id, user_query, genie_response, generated_sql, status, duration_ms, token_count |

## Observability Tiers

1. **Native** — Monitor tab + audit logs (what you get for free)
2. **This project** — API harvester → Delta → Lakeview (workspace-wide admin observability)
3. **MLflow tracing** — Step-level debugging for custom/embedded Genie integrations
4. **Custom app logging** — Wrap the Conversation API yourself for non-UI use cases

## Requirements

- Databricks workspace with Genie Spaces enabled
- `CAN MANAGE` permission on target Genie Spaces (for API access)
- SQL Warehouse (serverless recommended)
- Unity Catalog with a target schema for Delta tables
# Genie Observability Hub

End-to-end observability for Databricks Genie Spaces and Genie One — harvests user questions, responses, generated SQL, thinking traces, and per-user cost attribution into governed Delta tables, surfaced via a Lakeview dashboard.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  system.access.audit (aibiGenie)                                │
│  + Genie Conversation REST API                                  │
│  + system.billing.usage (GENIE)                                 │
│  + system.query.history (Genie client_applications)             │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Harvester Job (daily, incremental)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Delta Tables (your catalog.schema)                             │
│  ├── genie_spaces          (space metadata + config)            │
│  ├── genie_conversations   (conversation threads)               │
│  ├── genie_messages        (full Q&A: question, response, SQL)  │
│  ├── genie_query_executions (Genie-generated SQL + perf)        │
│  └── genie_cost_attribution (per-user daily token DBU cost)     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Lakeview Dashboard (6 pages)                                   │
│  ├── Executive Summary (KPIs, trends)                           │
│  ├── Space Performance (per-space metrics)                      │
│  ├── User Activity (per-user drilldown)                         │
│  ├── FAQs & Patterns (top questions, common failures)           │
│  ├── Cost & ROI (token spend, per-user attribution)             │
│  └── Quality & Feedback (ratings, review requests)              │
└─────────────────────────────────────────────────────────────────┘

Optional: MLflow Tracing (pattern 4)
┌─────────────────────────────────────────────────────────────────┐
│  genie_traced_client.py — bypasses SDK, calls REST directly,    │
│  logs each state transition to MLflow with full payloads        │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# 1. Configure your target catalog/schema
#    Edit databricks.yml variables or pass at deploy time

# 2. Deploy
databricks bundle deploy -t dev

# 3. Run bootstrap (creates tables)
databricks bundle run bootstrap_job -t dev

# 4. Run initial harvest
databricks bundle run harvester_job -t dev

# 5. Dashboard auto-deploys with the bundle
```

## Bundle Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog` | Target UC catalog | _(required)_ |
| `schema` | Target schema for Delta tables | `genie_observability` |
| `warehouse_id` | SQL warehouse for dashboard/jobs | _(required)_ |
| `harvest_lookback_days` | How far back to scan on first run | `30` |

## Filtering & Multi-Workspace

Filters are stored in the `harvest_config` table and respected by the harvester:

| Config Key | Type | Effect |
|------------|------|--------|
| `workspace_filter` | JSON array | Only harvest from these workspace IDs |
| `space_include_tags` | JSON array | Only harvest spaces with at least one of these tags |
| `space_exclude_tags` | JSON array | Skip spaces with any of these tags |
| `space_include_ids` | JSON array | Always include these space IDs (overrides exclude tags) |
| `space_exclude_ids` | JSON array | Always exclude these space IDs |

**Tag management:**
```sql
-- Tag a space
INSERT INTO <catalog>.genie_observability.space_tags VALUES
  ('space-id', 'production', current_user(), current_timestamp());

-- Filter by tag in the dashboard
SELECT m.* FROM genie_messages m
JOIN space_tags t ON m.space_id = t.space_id
WHERE t.tag = 'production';
```

**Workspace filter (multi-workspace):**
```sql
-- Only harvest from specific workspaces
MERGE INTO harvest_config ...
VALUES ('workspace_filter', '["12345", "67890"]', ...);
```

## Alerting & SLA

The `06_alerts_and_retention` job (runs daily at 7am after harvester) provides:

| Feature | Description |
|---------|-------------|
| **Failure rate spike** | Alert when a space's 24h failure rate exceeds threshold AND is 1.5x its 7-day baseline |
| **Cost spike** | Alert when a user's daily DBUs exceed 2x their 7-day average |
| **Latency degradation** | Alert when p95 response time exceeds threshold |
| **FAQ detection** | Auto-identifies most common questions per space (for certified SQL candidates) |
| **SLA metrics** | Daily p50/p95/p99 latency, success rate, unique users per space |
| **Data retention** | Auto-purges records older than configurable threshold (default 90d) |

Configure thresholds via `harvest_config`:
```sql
-- Adjust alert sensitivity
MERGE INTO harvest_config ... VALUES ('alert_failure_rate_threshold', '0.20', ...);
MERGE INTO harvest_config ... VALUES ('alert_latency_p95_threshold_ms', '45000', ...);
MERGE INTO harvest_config ... VALUES ('retention_days', '180', ...);
```

## What Gets Captured

| Data Point | Source | Available |
|------------|--------|-----------|
| User's natural-language question | Genie Conversation API | ✅ |
| Genie's text response | Genie Conversation API | ✅ |
| Generated SQL | Genie API + query.history | ✅ |
| Query execution time & status | system.query.history | ✅ |
| Thinking/state transitions | Genie API (message status polling) | ✅ |
| User feedback (thumbs up/down) | system.access.audit | ✅ |
| Per-user token cost (DBUs) | system.billing.usage | ✅ |
| Space configuration & tables | Genie API (getSpace) | ✅ |
| Conversation threading | Genie API | ✅ |

## Prerequisites

- Databricks workspace with system tables enabled
- `CAN MANAGE` on at least one Genie Space (for API access), OR workspace admin
- SQL warehouse (serverless recommended)
- Target catalog/schema with CREATE TABLE permissions

## File Structure

```
├── databricks.yml                 # Bundle config (jobs, variables, targets)
├── .gitignore
├── README.md
└── src/
    ├── 01_bootstrap.sql           # DDL: creates schema + 6 Delta tables
    ├── 02_harvester.py            # Main harvester: audit → API → Delta
    ├── 03_cost_attribution.sql    # Per-user cost rollup from billing
    ├── 04_tracing_example.py      # Pattern 4: MLflow-traced Genie client usage
    └── tracing_client.py          # Reusable traced client module
```

### Legacy files (pre-DAB prototype, can be removed)
```
    ├── 00_ddl.py                  # Superseded by 01_bootstrap.sql
    ├── 01_harvester.py            # Superseded by 02_harvester.py
    ├── 02_tracing.py              # Superseded by tracing_client.py
    ├── 03_backfill_feedback.py    # Migrate to harvester feedback extraction
    ├── 04_enrich_query_duration.py # Merged into query_executions table
    └── config.py                  # Superseded by DAB variables
```

## Limitations & Notes

- **Message content** is only available via the Genie Conversation API (not in system tables)
- The harvester requires API access to each space — it uses the deploying user's credentials
- First run may take several minutes depending on conversation volume
- The Genie API has rate limits — harvester includes backoff/retry logic
- For custom/embedded Genie apps, use `tracing_client.py` to get step-level MLflow traces
