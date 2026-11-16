# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · DQ experiment config
# MAGIC Loads the main project config and keeps the DQ experiment in the main
# MAGIC `workspace.wildfire` schema using DQ-prefixed state/log tables. The
# MAGIC production Bronze, Silver, and Gold tables are not modified; only
# MAGIC `silver_pm25_daily_test` is used for experiment changes.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

DQ_SCHEMA  = "wildfire"
SOURCE_TABLE = t("silver_pm25_daily")     # real Silver table, read once to make the copy

def dq(name):
    """Full name of a table in the DQ namespace."""
    return f"{CATALOG}.{DQ_SCHEMA}.{name}"

TEST_TABLE = dq("silver_pm25_daily_test")  # the copy every experiment notebook works on
DQ_LOG_TABLE = dq("dq_ops_step_log")      # do not mix DQ runs with the production ops log

def log_step(step, layer, rows, started, extra=None):
    """Same shape as the project's log_step, but uses a DQ-prefixed log table."""
    elapsed = time.time() - started
    row = [(step, layer, int(rows), float(elapsed), dt.datetime.utcnow(), json.dumps(extra or {}))]
    (spark.createDataFrame(row, "step string, layer string, rows long, seconds double, logged_at_utc timestamp, extra string")
          .write.mode("append").saveAsTable(DQ_LOG_TABLE))
    source_file = globals().get("SOURCE_FILE", "dq")
    store_log(
        spark,
        "INFO",
        source_file,
        f"{step} completed: {int(rows):,} rows in {elapsed:.1f}s",
    )

# COMMAND ----------

