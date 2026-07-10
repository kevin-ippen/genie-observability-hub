# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Tracing — Example Usage
# MAGIC
# MAGIC Demonstrates Pattern 4: bypassing the Genie SDK to get full visibility
# MAGIC into what Genie is thinking at each step, logged to MLflow.
# MAGIC
# MAGIC **Use cases:**
# MAGIC - Debugging wrong/unexpected Genie answers
# MAGIC - Performance bottleneck diagnosis
# MAGIC - Demo and incident investigation
# MAGIC - Quality evaluation harnesses

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install --upgrade databricks-sdk>=0.40.0 mlflow>=2.18.0

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Client
import os
import sys
import mlflow

cwd = os.getcwd()
if cwd not in sys.path:
    sys.path.append(cwd)

from tracing_client import GenieTracedClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Point at your Genie Space and MLflow experiment.

# COMMAND ----------

# DBTITLE 1,Configure Tracing
dbutils.widgets.text("space_id", "01f17ba95e361317972969baab2bdf45", "Genie Space ID")
dbutils.widgets.text("experiment_name", "/Users/kevin.ippen@databricks.com/genie-tracing", "MLflow Experiment")

SPACE_ID = dbutils.widgets.get("space_id")
EXPERIMENT_NAME = dbutils.widgets.get("experiment_name")

client = GenieTracedClient(
    space_id=SPACE_ID,
    experiment_name=EXPERIMENT_NAME,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ask a Question (fully traced)

# COMMAND ----------

# DBTITLE 1,Ask a Question (fully traced)
result = client.ask("How many active contracts do we have?")

print(f"Status: {result.status}")
print(f"Duration: {result.duration_ms}ms")
print(f"States observed: {result.state_transitions}")
print(f"\nResponse:\n{result.content[:500] if result.content else 'No content'}")
print(f"\nGenerated SQL:\n{result.generated_sql or 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continue the Conversation

# COMMAND ----------

# DBTITLE 1,Follow-up Question
result2 = client.ask(
    "Break that down by supplier",
    conversation_id=result.conversation_id,
)

print(f"Follow-up status: {result2.status}")
print(f"Duration: {result2.duration_ms}ms")
print(f"States: {result2.state_transitions}")
print(f"\nResponse:\n{result2.content[:300] if result2.content else 'No content'}")
print(f"\nFollow-up SQL:\n{result2.generated_sql or 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Traces in MLflow
# MAGIC
# MAGIC Navigate to the MLflow experiment to see the full trace tree:
# MAGIC ```
# MAGIC genie_conversation (parent)
# MAGIC ├── send_message
# MAGIC ├── poll_completion
# MAGIC │   ├── state:asking_ai
# MAGIC │   ├── state:fetching_metadata
# MAGIC │   ├── state:filtering_context
# MAGIC │   └── state:executing_query
# MAGIC ```
# MAGIC
# MAGIC Each span includes timing, status, and the full API payload.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Evaluation Pattern
# MAGIC
# MAGIC Use this for systematic quality testing across many questions:

# COMMAND ----------

# DBTITLE 1,Batch Evaluation
questions = [
    "What is our total procurement spend this quarter?",
    "Which suppliers have the most contracts?",
    "Show me contracts expiring in the next 30 days",
    "What's our average days to expiry across all active contracts?",
]

results = []
for q in questions:
    r = client.ask(q)
    results.append({
        "question": q,
        "status": r.status,
        "duration_ms": r.duration_ms,
        "has_sql": r.generated_sql is not None,
        "states": r.state_transitions,
        "error": r.error,
    })
    print(f"  [{r.status}] {r.duration_ms}ms — {q[:50]}")

# COMMAND ----------

# Convert to DataFrame for analysis
import pandas as pd
df = pd.DataFrame(results)
display(df)
