# Databricks notebook source
# DBTITLE 1,Genie Observability Hub — Master Config & Orchestrator
# MAGIC %md
# MAGIC # Genie Observability Hub — Master Config & Orchestrator
# MAGIC
# MAGIC Centralized configuration and feature flags for the observability pipeline. Run this notebook to execute the full pipeline with your chosen settings.
# MAGIC
# MAGIC **Pipeline stages:**
# MAGIC 1. Bootstrap (DDL/schema creation)
# MAGIC 2. Harvest (Genie Conversation API → Delta)
# MAGIC 3. AI Enrichment (classify, extract, FAQ, sentiment)
# MAGIC 4. Cost Attribution (billing rollup)
# MAGIC 5. Alerts & Retention
# MAGIC
# MAGIC **Usage:** Toggle feature flags in Cell 3, then Run All.

# COMMAND ----------

# DBTITLE 1,Core Parameters
# === CORE PARAMETERS ===
TARGET_CATALOG = "serverless_stable_h7wanf_catalog"
TARGET_SCHEMA = "genie_observability"
TARGET_PREFIX = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"
WAREHOUSE_ID = "4047b28d66a51bdc"
WORKSPACE_ID = "7474657986130378"
LOOKBACK_DAYS = 90

# Space IDs to harvest (all 13 known spaces)
SPACE_IDS = [
    "01f17ba95e361317972969baab2bdf45",  # QSR Technology Procurement Intelligence
    "01f16ff6927f1db7a309d80e3ae5e15c",  # DPZ Fleet Ops
    "01f16e73bf171aee850bea3a4b4dbd83",  # Retail Store Profile
    "01f164675569132db4b84e7a20a8eb4f",  # Agent Harness Quality Dashboard
    "01f15c8c50cd10009efc2d8e8e55ec2e",  # SOA Agent Ops & Evaluation
    "01f14401cd6e1a248535d22e00b5af56",  # DPZ Fleet Operations Intelligence
    "01f14d9ae182136a8c3a6f21ea1f6d1a",  # Joe Jordan Executive Simulator
    "01f134251c5f11e891e9a5ddc7d2e143",  # PizzaTwin What-If Explorer
    "01f134250620167f8a6bd1a92e71aeb2",  # DPG What-If Franchise Analytics
    "01f12a0e5074155ab15e36aa5312edac",  # Domino's Store Performance Agent
    "01f12a0e503516a68d6f5e18d35a54a2",  # Domino's Customer Insights Agent
    "01f12a0e4fe71a7691d6fba4b8e3f7cd",  # Domino's Sales & Revenue Agent
    "01f12a0e4ec617d0a06e6e8e08b4c6df",  # Domino's Operations Agent
]

# COMMAND ----------

# DBTITLE 1,Feature Flags
# === FEATURE FLAGS ===
# Toggle each pipeline stage on/off

# Core pipeline
ENABLE_BOOTSTRAP = False          # Run DDL/schema creation (01_bootstrap) — only needed first time
ENABLE_HARVEST = True             # Run conversation harvester (02_harvester)
ENABLE_COST_ATTRIBUTION = True    # Run cost attribution rollup (03_cost_attribution)
ENABLE_ALERTS = False             # Run alerting rules (04_alerts_and_retention)

# AI Enrichment (batch inference on harvested messages)
ENABLE_AI_CLASSIFICATION = True   # ai_classify: question category, complexity, intent
ENABLE_AI_ENTITY_EXTRACTION = False  # ai_extract: table names, metrics, time ranges from queries
ENABLE_AI_FAQ_DETECTION = False   # ai_similarity: cluster similar questions for FAQ detection
ENABLE_AI_SENTIMENT = False       # ai_analyze_sentiment: satisfaction proxy from response text

# AI Configuration
AI_BATCH_SIZE = 10                # Process N messages per batch (rate limit friendly)
AI_RATE_LIMIT_RPS = 1.0           # Max requests per second to FMAPI
AI_ONLY_NEW = True                # Only classify messages not yet classified (incremental)

# COMMAND ----------

# DBTITLE 1,Pipeline Orchestration
# === PIPELINE ORCHESTRATION ===
print("=" * 60)
print("🚀 GENIE OBSERVABILITY PIPELINE")
print("=" * 60)
print(f"  Target: {TARGET_PREFIX}")
print(f"  Workspace: {WORKSPACE_ID}")
print(f"  Spaces: {len(SPACE_IDS)}")
print(f"  Lookback: {LOOKBACK_DAYS} days")
print()
print("Feature Flags:")
print(f"  Bootstrap:         {'✅' if ENABLE_BOOTSTRAP else '⏭️'}")
print(f"  Harvest:           {'✅' if ENABLE_HARVEST else '⏭️'}")
print(f"  Cost Attribution:  {'✅' if ENABLE_COST_ATTRIBUTION else '⏭️'}")
print(f"  Alerts:            {'✅' if ENABLE_ALERTS else '⏭️'}")
print(f"  AI Classification: {'✅' if ENABLE_AI_CLASSIFICATION else '⏭️'}")
print(f"  AI Extraction:     {'✅' if ENABLE_AI_ENTITY_EXTRACTION else '⏭️'}")
print(f"  AI FAQ Detection:  {'✅' if ENABLE_AI_FAQ_DETECTION else '⏭️'}")
print(f"  AI Sentiment:      {'✅' if ENABLE_AI_SENTIMENT else '⏭️'}")
print("=" * 60)

# Build shared params to pass to child notebooks
shared_params = {
    "target_catalog": TARGET_CATALOG,
    "target_schema": TARGET_SCHEMA,
    "warehouse_id": WAREHOUSE_ID,
    "workspace_id": WORKSPACE_ID,
    "lookback_days": str(LOOKBACK_DAYS),
    "space_ids": ",".join(SPACE_IDS),
}

notebook_base = "/Users/kevin.ippen@databricks.com/genie-observability-hub/src"

# Step 1: Bootstrap (schema/DDL)
if ENABLE_BOOTSTRAP:
    print("\n📐 Step 1: Running bootstrap (DDL)...")
    result = dbutils.notebook.run(f"{notebook_base}/01_bootstrap", timeout_seconds=300, arguments=shared_params)
    print(f"  Bootstrap result: {result}")
else:
    print("\n⏭️ Step 1: Bootstrap skipped")

# Step 2: Harvest conversations
if ENABLE_HARVEST:
    print("\n🌾 Step 2: Running harvester...")
    result = dbutils.notebook.run(f"{notebook_base}/02_harvester", timeout_seconds=1800, arguments=shared_params)
    print(f"  Harvest result: {result}")
else:
    print("\n⏭️ Step 2: Harvest skipped")

# Step 3: AI Enrichment (runs in this notebook — cells above)
print("\n🧠 Step 3: AI Enrichment (runs in cells below — continue Run All)")

# Step 4: Cost Attribution
if ENABLE_COST_ATTRIBUTION:
    print("\n💰 Step 4: Running cost attribution...")
    result = dbutils.notebook.run(f"{notebook_base}/03_cost_attribution", timeout_seconds=600, arguments=shared_params)
    print(f"  Cost attribution result: {result}")
else:
    print("\n⏭️ Step 4: Cost attribution skipped")

# Step 5: Alerts & Retention
if ENABLE_ALERTS:
    print("\n🚨 Step 5: Running alerts...")
    result = dbutils.notebook.run(f"{notebook_base}/04_alerts_and_retention", timeout_seconds=600, arguments=shared_params)
    print(f"  Alerts result: {result}")
else:
    print("\n⏭️ Step 5: Alerts skipped")

print("\n" + "=" * 60)
print("✅ PIPELINE COMPLETE")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,AI Enrichment — Classification
# === AI ENRICHMENT: CLASSIFICATION ===
import time

if ENABLE_AI_CLASSIFICATION:
    print("🧠 Running AI classification on harvested messages...")

    # Ensure enrichment columns exist
    spark.sql(f"""
        ALTER TABLE {TARGET_PREFIX}.genie_messages
        ADD COLUMNS IF NOT EXISTS (
            question_category STRING COMMENT 'AI-classified question type',
            question_complexity STRING COMMENT 'AI-classified complexity level',
            question_intent STRING COMMENT 'AI-classified intent type',
            ai_classified_at TIMESTAMP COMMENT 'When AI classification was run'
        )
    """)

    # Get unclassified messages (incremental)
    if AI_ONLY_NEW:
        unclassified_filter = "AND question_category IS NULL"
    else:
        unclassified_filter = ""

    unclassified = spark.sql(f"""
        SELECT message_id, user_query
        FROM {TARGET_PREFIX}.genie_messages
        WHERE user_query IS NOT NULL
          AND LENGTH(TRIM(user_query)) > 0
          AND created_at IS NOT NULL
          {unclassified_filter}
    """).collect()

    print(f"  Found {len(unclassified)} messages to classify")

    if len(unclassified) > 0:
        classified_count = 0
        for i in range(0, len(unclassified), AI_BATCH_SIZE):
            batch = unclassified[i:i + AI_BATCH_SIZE]
            batch_ids = [row.message_id for row in batch]
            ids_str = ",".join(f"'{mid}'" for mid in batch_ids)

            spark.sql(f"""
                MERGE INTO {TARGET_PREFIX}.genie_messages AS target
                USING (
                    SELECT
                        message_id,
                        ai_classify(user_query, ARRAY('data_lookup', 'aggregation', 'trend_analysis', 'comparison', 'filtering', 'exploration')) AS question_category,
                        ai_classify(user_query, ARRAY('simple', 'moderate', 'complex')) AS question_complexity,
                        ai_classify(user_query, ARRAY('operational', 'strategic', 'diagnostic', 'predictive')) AS question_intent,
                        current_timestamp() AS ai_classified_at
                    FROM {TARGET_PREFIX}.genie_messages
                    WHERE message_id IN ({ids_str})
                ) AS source
                ON target.message_id = source.message_id
                WHEN MATCHED THEN UPDATE SET
                    target.question_category = source.question_category,
                    target.question_complexity = source.question_complexity,
                    target.question_intent = source.question_intent,
                    target.ai_classified_at = source.ai_classified_at
            """)

            classified_count += len(batch)
            print(f"  Classified {classified_count}/{len(unclassified)} messages")

            if i + AI_BATCH_SIZE < len(unclassified):
                time.sleep(AI_BATCH_SIZE / AI_RATE_LIMIT_RPS)

        print(f"✅ AI classification complete: {classified_count} messages enriched")
    else:
        print("  All messages already classified (AI_ONLY_NEW=True)")
else:
    print("⏭️ AI classification disabled (ENABLE_AI_CLASSIFICATION=False)")

# COMMAND ----------

# DBTITLE 1,AI Enrichment — Entity Extraction
# === AI ENRICHMENT: ENTITY EXTRACTION ===
import time

if ENABLE_AI_ENTITY_EXTRACTION:
    print("🔍 Running AI entity extraction on harvested messages...")

    # Ensure enrichment columns exist
    spark.sql(f"""
        ALTER TABLE {TARGET_PREFIX}.genie_messages
        ADD COLUMNS IF NOT EXISTS (
            extracted_entities STRING COMMENT 'JSON of extracted entities (tables, metrics, time ranges)',
            ai_extracted_at TIMESTAMP COMMENT 'When entity extraction was run'
        )
    """)

    if AI_ONLY_NEW:
        unclassified_filter = "AND extracted_entities IS NULL"
    else:
        unclassified_filter = ""

    unclassified = spark.sql(f"""
        SELECT message_id, user_query
        FROM {TARGET_PREFIX}.genie_messages
        WHERE user_query IS NOT NULL
          AND LENGTH(TRIM(user_query)) > 0
          AND created_at IS NOT NULL
          {unclassified_filter}
    """).collect()

    print(f"  Found {len(unclassified)} messages to extract entities from")

    if len(unclassified) > 0:
        for i in range(0, len(unclassified), AI_BATCH_SIZE):
            batch = unclassified[i:i + AI_BATCH_SIZE]
            batch_ids = [row.message_id for row in batch]
            ids_str = ",".join(f"'{mid}'" for mid in batch_ids)

            spark.sql(f"""
                MERGE INTO {TARGET_PREFIX}.genie_messages AS target
                USING (
                    SELECT
                        message_id,
                        TO_JSON(ai_extract(user_query, ARRAY('metric_name', 'table_name', 'time_range', 'dimension'))) AS extracted_entities,
                        current_timestamp() AS ai_extracted_at
                    FROM {TARGET_PREFIX}.genie_messages
                    WHERE message_id IN ({ids_str})
                ) AS source
                ON target.message_id = source.message_id
                WHEN MATCHED THEN UPDATE SET
                    target.extracted_entities = source.extracted_entities,
                    target.ai_extracted_at = source.ai_extracted_at
            """)

            if i + AI_BATCH_SIZE < len(unclassified):
                time.sleep(AI_BATCH_SIZE / AI_RATE_LIMIT_RPS)

        print(f"✅ Entity extraction complete")
    else:
        print("  All messages already extracted")
else:
    print("⏭️ Entity extraction disabled (ENABLE_AI_ENTITY_EXTRACTION=False)")

# COMMAND ----------

# DBTITLE 1,AI Enrichment — FAQ Detection
# === AI ENRICHMENT: FAQ DETECTION ===
import time
from collections import defaultdict
import uuid

if ENABLE_AI_FAQ_DETECTION:
    print("🔁 Running FAQ detection (similarity clustering)...")

    # Ensure enrichment columns exist
    spark.sql(f"""
        ALTER TABLE {TARGET_PREFIX}.genie_messages
        ADD COLUMNS IF NOT EXISTS (
            faq_cluster_id STRING COMMENT 'Cluster ID for similar questions',
            faq_canonical_question STRING COMMENT 'Representative question for this cluster'
        )
    """)

    # Get all messages for similarity comparison
    messages = spark.sql(f"""
        SELECT message_id, user_query
        FROM {TARGET_PREFIX}.genie_messages
        WHERE user_query IS NOT NULL
          AND LENGTH(TRIM(user_query)) > 0
          AND created_at IS NOT NULL
        ORDER BY created_at DESC
    """)

    messages_df = messages.toPandas()

    if len(messages_df) > 1:
        clusters = {}  # message_id -> cluster_id
        cluster_canonical = {}  # cluster_id -> canonical question

        # Compare each pair (O(n^2) but n is small ~50-100)
        # For larger datasets, use Vector Search index instead
        for i in range(len(messages_df)):
            if messages_df.iloc[i]['message_id'] in clusters:
                continue

            cluster_id = str(uuid.uuid4())[:8]
            clusters[messages_df.iloc[i]['message_id']] = cluster_id
            cluster_canonical[cluster_id] = messages_df.iloc[i]['user_query']

            for j in range(i + 1, len(messages_df)):
                if messages_df.iloc[j]['message_id'] in clusters:
                    continue

                query_i = messages_df.iloc[i]['user_query'].replace("'", "''")
                query_j = messages_df.iloc[j]['user_query'].replace("'", "''")
                sim_result = spark.sql(f"""
                    SELECT ai_similarity('{query_i}', '{query_j}') AS sim_score
                """).collect()[0].sim_score

                if sim_result and sim_result > 0.8:
                    clusters[messages_df.iloc[j]['message_id']] = cluster_id

                time.sleep(1.0 / AI_RATE_LIMIT_RPS)

        # Update table with cluster assignments
        for msg_id, cid in clusters.items():
            canonical = cluster_canonical[cid].replace("'", "''")
            spark.sql(f"""
                UPDATE {TARGET_PREFIX}.genie_messages
                SET faq_cluster_id = '{cid}',
                    faq_canonical_question = '{canonical}'
                WHERE message_id = '{msg_id}'
            """)

        # Count FAQs (clusters with >1 member)
        cluster_counts = defaultdict(int)
        for cid in clusters.values():
            cluster_counts[cid] += 1
        faq_count = sum(1 for c in cluster_counts.values() if c > 1)
        print(f"✅ FAQ detection complete: {faq_count} FAQ clusters found ({sum(cluster_counts.values())} messages clustered)")
    else:
        print("  Not enough messages for FAQ detection")
else:
    print("⏭️ FAQ detection disabled (ENABLE_AI_FAQ_DETECTION=False)")

# COMMAND ----------

# DBTITLE 1,AI Enrichment — Sentiment Analysis
# === AI ENRICHMENT: SENTIMENT ANALYSIS ===
import time

if ENABLE_AI_SENTIMENT:
    print("💬 Running sentiment analysis on responses...")

    spark.sql(f"""
        ALTER TABLE {TARGET_PREFIX}.genie_messages
        ADD COLUMNS IF NOT EXISTS (
            response_sentiment STRING COMMENT 'Sentiment of Genie response (positive/neutral/negative/uncertain)',
            ai_sentiment_at TIMESTAMP COMMENT 'When sentiment analysis was run'
        )
    """)

    if AI_ONLY_NEW:
        unclassified_filter = "AND response_sentiment IS NULL"
    else:
        unclassified_filter = ""

    unclassified = spark.sql(f"""
        SELECT message_id, response_text
        FROM {TARGET_PREFIX}.genie_messages
        WHERE response_text IS NOT NULL
          AND LENGTH(TRIM(response_text)) > 0
          AND created_at IS NOT NULL
          {unclassified_filter}
    """).collect()

    print(f"  Found {len(unclassified)} responses to analyze")

    if len(unclassified) > 0:
        for i in range(0, len(unclassified), AI_BATCH_SIZE):
            batch = unclassified[i:i + AI_BATCH_SIZE]
            batch_ids = [row.message_id for row in batch]
            ids_str = ",".join(f"'{mid}'" for mid in batch_ids)

            spark.sql(f"""
                MERGE INTO {TARGET_PREFIX}.genie_messages AS target
                USING (
                    SELECT
                        message_id,
                        ai_analyze_sentiment(response_text) AS response_sentiment,
                        current_timestamp() AS ai_sentiment_at
                    FROM {TARGET_PREFIX}.genie_messages
                    WHERE message_id IN ({ids_str})
                ) AS source
                ON target.message_id = source.message_id
                WHEN MATCHED THEN UPDATE SET
                    target.response_sentiment = source.response_sentiment,
                    target.ai_sentiment_at = source.ai_sentiment_at
            """)

            if i + AI_BATCH_SIZE < len(unclassified):
                time.sleep(AI_BATCH_SIZE / AI_RATE_LIMIT_RPS)

        print(f"✅ Sentiment analysis complete")
    else:
        print("  All responses already analyzed")
else:
    print("⏭️ Sentiment analysis disabled (ENABLE_AI_SENTIMENT=False)")

# COMMAND ----------

# DBTITLE 1,Post-Pipeline: Dashboard Dataset Update
# MAGIC %md
# MAGIC ## Post-Pipeline: Dashboard Dataset Update
# MAGIC
# MAGIC Once AI enrichment columns are populated in `genie_messages`, update the dashboard's **Message Insights** dataset to read directly from the pre-computed columns instead of the rule-based CASE WHEN fallback:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT message_id, user_query, space_name, status, duration_s, has_sql, message_date,
# MAGIC        question_category, question_complexity, question_intent,
# MAGIC        response_sentiment, faq_cluster_id, faq_canonical_question
# MAGIC FROM genie_messages m
# MAGIC LEFT JOIN genie_spaces s ON m.space_id = s.space_id
# MAGIC WHERE created_at IS NOT NULL AND user_query IS NOT NULL
# MAGIC ```
# MAGIC
# MAGIC The dashboard's AI Insights page will then show true AI classifications rather than keyword heuristics.
