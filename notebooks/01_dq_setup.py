# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · DQ experiment setup (run once, or with recreate=true to start over)
# MAGIC Uses the main `wildfire` schema and copies the real `silver_pm25_daily`
# MAGIC into `silver_pm25_daily_test`. All tests change only this copy.

# COMMAND ----------

# MAGIC %run ./00_dq_config

# COMMAND ----------

dbutils.widgets.dropdown("recreate", "false", ["false", "true"], "Recreate the test copy and clear all experiment state")
RECREATE = dbutils.widgets.get("recreate") == "true"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DQ_SCHEMA}")

if RECREATE or not spark.catalog.tableExists(TEST_TABLE):
    spark.sql(f"CREATE OR REPLACE TABLE {TEST_TABLE} AS SELECT * FROM {SOURCE_TABLE}")
    # State from an earlier copy would be wrong for the new one
    for name in [
        "dq_deequ_state", "dq_cdf_state", "dq_cdf_value_state",
        "dq_cdf_metric_state", "dq_metric_runs", "dq_gold_gate", "dq_test_log",
        "dq_pm25_test_log",
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {dq(name)}")
    print(f"Created {TEST_TABLE} from {SOURCE_TABLE} and cleared experiment state")
else:
    print(f"{TEST_TABLE} already exists (set recreate=true to start over)")

display(spark.sql(f"""
  SELECT count(*) AS rows,
         sum(CAST(pm25_ugm3 IS NULL AS INT)) AS pm25_ugm3_nulls
  FROM {TEST_TABLE}
"""))

# COMMAND ----------

