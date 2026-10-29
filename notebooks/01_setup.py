# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup (run once)
# MAGIC Creates the schema, the raw-file volume and the landing folders. Then upload the files from `tools/download_sources.py`
# MAGIC (or let `02_bronze` fetch them from the APIs if your workspace allows outbound calls).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
for sub in ["firms", "epa", "noaa", "reference", "epa_bulk"]:
    dbutils.fs.mkdirs(f"{RAW_ROOT}/{sub}")

# Batch manifest: one row per raw file ingested. Lets reruns skip files already loaded,
# and detects a revised file (same name, different content hash) so it is loaded again.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {t('ops_ingest_manifest')} (
  source STRING, file_path STRING, file_name STRING, data_date DATE,
  content_hash STRING, rows_loaded LONG, ingested_at_utc TIMESTAMP, batch_id STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {t('ops_step_log')} (
  step STRING, layer STRING, rows LONG, seconds DOUBLE, logged_at_utc TIMESTAMP, extra STRING
) USING DELTA
""")

display(dbutils.fs.ls(RAW_ROOT))
