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

# The production Silver table that must pass DQ before Gold is allowed to run.
# The experiment notebooks can override this through their table widget.
DQ_GOLD_SOURCE_TABLE = t("silver_pm25_daily")
DQ_CDF_VALUE_STATE = dq("dq_cdf_value_state")
DQ_CDF_METRIC_STATE = dq("dq_cdf_metric_state")
DQ_METRIC_RUNS = dq("dq_metric_runs")
DQ_GOLD_GATE = dq("dq_gold_gate")

def log_step(step, layer, rows, started, extra=None):
    """Same shape as the project's log_step, but uses a DQ-prefixed log table."""
    elapsed = time.time() - started
    row = [(step, layer, int(rows), float(elapsed), dt.datetime.utcnow(), json.dumps(extra or {}))]
    (spark.createDataFrame(row, "step string, layer string, rows long, seconds double, logged_at_utc timestamp, extra string")
          .write.mode("append").saveAsTable(DQ_LOG_TABLE))
    print(f"[{layer}] {step}: {rows:,} rows in {elapsed:.1f}s")

print(f"DQ experiment: working on {TEST_TABLE}")

# COMMAND ----------

