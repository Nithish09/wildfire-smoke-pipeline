# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · DQ experiment setup (run once, or with recreate=true to start over)
# MAGIC Uses the main `wildfire` schema and copies the real `silver_pm25_daily`
# MAGIC into `silver_pm25_daily_test`. All tests change only this copy.

# COMMAND ----------

# MAGIC %run ./00_dq_config

# COMMAND ----------

SOURCE_FILE = "01_dq_setup"

def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)

dbutils.widgets.dropdown("recreate", "false", ["false", "true"], "Recreate the test copy and clear all experiment state")
RECREATE = dbutils.widgets.get("recreate") == "true"
write_notebook_log("INFO", f"Preparing DQ test copy {TEST_TABLE}; recreate={RECREATE}.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DQ_SCHEMA}")

if RECREATE or not spark.catalog.tableExists(TEST_TABLE):
    spark.sql(f"CREATE OR REPLACE TABLE {TEST_TABLE} AS SELECT * FROM {SOURCE_TABLE}")
    # State from an earlier copy would be wrong for the new one
    for name in ["dq_deequ_state", "dq_cdf_state", "dq_test_log"]:
        spark.sql(f"DROP TABLE IF EXISTS {dq(name)}")
    write_notebook_log("INFO", f"Created {TEST_TABLE} from {SOURCE_TABLE} and cleared experiment state")
else:
    write_notebook_log("INFO", f"{TEST_TABLE} already exists; no DQ state was reset")

summary = spark.sql(f"""
  SELECT count(*) AS rows, sum(CAST(aqi IS NULL AS INT)) AS aqi_nulls FROM {TEST_TABLE}
""").first()
write_notebook_log(
    "INFO",
    f"DQ test copy ready: rows={summary['rows']:,}, aqi_nulls={summary['aqi_nulls'] or 0:,}",
)

# COMMAND ----------

