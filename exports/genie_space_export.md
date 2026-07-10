# Genie Observability Hub — Portable Space Export

## 1. Overview

| Field | Value |
| --- | --- |
| Space name | Genie Observability Hub |
| Space ID | 01f17c7b3b4b1fc0a0f89246122d4777 |
| Run-as type | VIEWER |
| Warehouse ID | 4047b28d66a51bdc |
| Source workspace ID | 7474657986130378 |
| Purpose | Natural language access to Genie Spaces usage data, response quality, costs, and qualitative insights |

---

## 2. Attached Tables

* `serverless_stable_h7wanf_catalog.genie_observability.genie_messages`
* `serverless_stable_h7wanf_catalog.genie_observability.genie_conversations`
* `serverless_stable_h7wanf_catalog.genie_observability.genie_spaces`
* `serverless_stable_h7wanf_catalog.genie_observability.genie_cost_attribution`

---

## 3. Space Description

# Genie Observability Hub

This space provides natural language access to Genie Spaces usage data — who's asking what, how well Genie answers, response times, costs, and AI-classified question insights.

## Data Model

### genie_messages (core fact table)
Each row = one user message sent to a Genie Space. Key columns:
- message_id (STRING, PK), conversation_id, space_id, workspace_id
- user_email (STRING) — who asked
- user_query (STRING) — the actual question text
- response_text (STRING) — Genie's natural language answer (truncated to 300 chars in some views)
- generated_sql (STRING) — the SQL Genie wrote to answer the question
- status (STRING) — COMPLETED or FAILED
- duration_ms (BIGINT) — end-to-end response time in milliseconds
- created_at (TIMESTAMP) — when the message was sent
- feedback_rating (STRING) — thumbs_up/thumbs_down (often NULL)
- AI enrichment columns (populated by batch pipeline): question_category, question_complexity, question_intent, response_sentiment, faq_cluster_id

### genie_conversations
Each row = one conversation thread. Columns: conversation_id, space_id, user_email, title, message_count, created_at, last_message_at.

### genie_spaces
Each row = one Genie Space. Columns: space_id, display_name, description, warehouse_id, table_identifiers (ARRAY), created_by, created_at.

### genie_cost_attribution
Daily per-user cost. Columns: usage_date (DATE), workspace_id, user_email, sku_name, total_dbus (DOUBLE), estimated_cost_usd (DOUBLE).

## Rules
- Date columns use TIMESTAMP or DATE format. Use INTERVAL syntax for date math: `current_date() - INTERVAL 7 DAYS`
- Always include GROUP BY when using aggregate functions
- For response time analysis, use `duration_ms / 1000.0` to get seconds
- success_rate = COUNT messages WHERE status = 'COMPLETED' / total COUNT
- sql_generation_rate = COUNT messages WHERE generated_sql IS NOT NULL / total COUNT
- Filter out bad data: always include `WHERE created_at IS NOT NULL AND user_query IS NOT NULL AND LENGTH(TRIM(user_query)) > 0`
- The question_category, question_complexity, question_intent columns may be NULL if AI enrichment hasn't run yet
- Cost is estimated at $0.07/DBU for serverless Genie compute

---

## 4. General Text Instructions

### Qualitative analysis guidance

For qualitative questions about what users are asking:
1. Use the pre-computed AI columns (question_category, question_complexity, question_intent) for structured analysis — these are the fastest and most reliable.
2. For keyword/topic searches, use LOWER(user_query) LIKE '%keyword%' patterns.
3. For error analysis, group by error_message or use LIKE patterns on error_message.
4. For 'summarize' or 'what are people asking about' questions, GROUP BY question_category or show the top N distinct user_query values.
5. For trend analysis of qualitative data, combine DATE(created_at) with question_category or question_complexity for time-series breakdowns.
6. The faq_canonical_question column shows the representative question for clusters of similar questions — use this for FAQ/pattern detection.

---

## 5. Table-Level Overrides and Metadata

### Table Descriptions

| Table | Description |
| --- | --- |
| genie_messages | Core fact table — every user message sent to a Genie Space. Contains the original question, Genie's generated SQL, response text, success/failure status, response time, user feedback, and AI-classified question metadata (category, complexity, intent). One row per message. Join to genie_spaces on space_id for space names, to genie_conversations on conversation_id for thread context. |
| genie_conversations | Conversation threads across Genie Spaces. Each row is a multi-turn conversation with message count, start/end timestamps, and the conversation title. Join to genie_spaces on space_id for space names. |
| genie_spaces | Metadata for all monitored Genie Spaces — display name, description, configured tables, warehouse binding, and creator. This is the dimension table for resolving space_id to human-readable names. |
| genie_cost_attribution | Daily per-user Genie compute cost, sourced from system.billing.usage. Shows DBUs consumed and estimated USD cost (DBUs × list price) broken down by user, workspace, and SKU. Use for cost trending, per-user spend analysis, and ROI calculations. |

### Hidden Columns

| Table | Hidden |
| --- | --- |
| genie_messages | role, harvested_at, state_transitions |
| genie_conversations | harvested_at |
| genie_spaces | harvested_at |
| genie_cost_attribution | harvested_at, user_id |

### Entity Matching Enabled

| Table | Columns |
| --- | --- |
| genie_messages | status, question_category, question_complexity, question_intent, feedback_rating |
| genie_cost_attribution | sku_name |

### Synonyms

| Column | Synonyms |
| --- | --- |
| genie_messages.user_query | question, prompt, ask, request |
| genie_messages.generated_sql | SQL, query, code, SQL code |
| genie_messages.duration_ms | response time, latency, time to respond, how long |
| genie_messages.response_text | answer, response, reply |
| genie_messages.user_email | user, who, person, asker |
| genie_messages.status | outcome, result, success or failure |
| genie_spaces.display_name | space name, name, space |
| genie_cost_attribution.estimated_cost_usd | cost, spend, price, dollars |
| genie_cost_attribution.total_dbus | DBUs, tokens, compute, usage |

---

## 6. Reusable Knowledge Snippets

### Joins

| Title | Condition | Relationship |
| --- | --- | --- |
| Messages to Spaces | genie_messages.space_id = genie_spaces.space_id | many_to_one |
| Messages to Conversations | genie_messages.conversation_id = genie_conversations.conversation_id | many_to_one |
| Conversations to Spaces | genie_conversations.space_id = genie_spaces.space_id | many_to_one |

### Filters

| Title | Expression |
| --- | --- |
| Valid messages only | genie_messages.created_at IS NOT NULL AND genie_messages.user_query IS NOT NULL AND LENGTH(TRIM(genie_messages.user_query)) > 0 |
| Failed messages only | genie_messages.status = 'FAILED' |
| Messages with SQL generated | genie_messages.generated_sql IS NOT NULL |
| Topic keyword search | LOWER(genie_messages.user_query) LIKE '%keyword%' |
| AI-classified messages only | genie_messages.question_category IS NOT NULL |
| Negative feedback only | genie_messages.feedback_rating = 'thumbs_down' |

### Derived Columns

| Title | Expression |
| --- | --- |
| Response time in seconds | genie_messages.duration_ms / 1000.0 |
| Message date | DATE(genie_messages.created_at) |

### Measures

| Title | Expression |
| --- | --- |
| Success rate | ROUND(SUM(CASE WHEN genie_messages.status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(\*), 1) |
| SQL generation rate | ROUND(SUM(CASE WHEN genie_messages.generated_sql IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(\*), 1) |
| Total cost (USD) | ROUND(SUM(genie_cost_attribution.estimated_cost_usd), 2) |
| Avg response time (seconds) | ROUND(AVG(genie_messages.duration_ms) / 1000.0, 1) |
| Unique users | COUNT(DISTINCT genie_messages.user_email) |
| Total conversations | COUNT(DISTINCT genie_messages.conversation_id) |
| Total DBUs consumed | SUM(genie_cost_attribution.total_dbus) |
| User satisfaction rate (% thumbs up of rated) | ROUND(SUM(CASE WHEN genie_messages.feedback_rating = 'thumbs_up' THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN genie_messages.feedback_rating IS NOT NULL THEN 1 ELSE 0 END), 0), 1) |
| Distinct FAQ clusters | COUNT(DISTINCT genie_messages.faq_cluster_id) |

---

## 7. Starter Questions

* What are the most active spaces?
* What's the success rate by space?
* Show me the top users by volume
* What's the daily cost trend?
* What types of questions are users asking?
* Show me recent failed messages
* What are the most common questions (FAQs)?

---

## 8. SQL Examples

* What are the most active spaces?
* What's the success rate by space?
* Show me the top users by volume
* What's the daily cost trend?
* What types of questions are users asking?
* Show me recent failed messages
* What are the most common questions (FAQs)?
* Search for questions about a specific topic
* What errors are users hitting?
* Are questions getting more complex over time?
* Which spaces get the most strategic questions?

---

## 9. Benchmarks (14)

* What are the most active spaces?
* What's the success rate by space?
* Show me the top users by volume
* What's the daily cost trend?
* What types of questions are users asking?
* Show me recent failed messages
* What are the most common questions (FAQs)?
* What types of questions are users asking? Break down by category.
* Are questions getting more complex over time?
* Which spaces get the most strategic questions?
* What errors are users hitting?
* Search for questions about revenue
* What are the most frequently asked questions?
* What's the user satisfaction rate?

---

## 10. Portability Notes

* Replace the catalog/schema prefixes when moving to a new workspace.
* Recreate tables first, then apply table metadata, then instructions/snippets, then starter questions, then benchmarks.
* AI enrichment columns must be populated by running the repo pipeline before qualitative analysis works well.
* If raw instruction export ever shows table descriptions as FROM_SNIPPET entries, treat the table-level overrides in this file as the correct source of truth.
