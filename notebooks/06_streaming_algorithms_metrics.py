# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 - Streaming algorithm evaluation
# MAGIC
# MAGIC `03s_silver_stream.py` is the execution stage for all three Bronze sources.
# MAGIC This notebook is read-only: it summarizes Bloom and DGIM results written by
# MAGIC the Silver streams and does not run another stream or perform another MERGE.
# MAGIC The view returns the latest available batch independently for each source
# MAGIC and algorithm, so timestamps can differ when only one Bronze source changed.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from logging_utils import store_log

SOURCE_FILE = "06_streaming_algorithms_metrics"
METRICS = t("v_ops_stream_metrics_latest")
STATE = t("ops_stream_state")

def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)

if not spark.catalog.tableExists(METRICS):
    raise ValueError(
        "v_ops_stream_metrics_latest does not exist; run "
        "08_streaming_algorithm_tables.sql after 03s_silver_stream.py"
    )

# COMMAND ----------

# MAGIC %md ## Bloom filter results for all three Bronze sources

# COMMAND ----------

bloom_summary = spark.sql(f"""
SELECT source, batch_id,
       max(metric_ts_utc) AS latest_metric_ts_utc,
       max(CASE WHEN metric_name = 'incoming_rows' THEN metric_value END) AS incoming_rows,
       max(CASE WHEN metric_name = 'existing_silver_keys' THEN metric_value END) AS existing_silver_keys,
       max(CASE WHEN metric_name = 'candidate_duplicate_hits' THEN metric_value END) AS candidate_duplicate_hits,
       max(CASE WHEN metric_name = 'exact_existing_key_hits' THEN metric_value END) AS exact_existing_key_hits,
       max(CASE WHEN metric_name = 'exact_new_rows' THEN metric_value END) AS exact_new_rows,
       max(CASE WHEN metric_name = 'false_positive_count' THEN metric_value END) AS false_positive_count,
       max(CASE WHEN metric_name = 'false_negative_count' THEN metric_value END) AS false_negative_count,
       max(CASE WHEN metric_name = 'measured_false_positive_rate' THEN metric_value END) AS measured_false_positive_rate,
       CASE WHEN max(CASE WHEN metric_name = 'false_negative_count' THEN metric_value END) = 0
            THEN 'OK' ELSE 'CHECK' END AS bloom_status
FROM {METRICS}
WHERE algorithm = 'bloom_filter'
GROUP BY source, batch_id
ORDER BY source, batch_id DESC
""")
write_notebook_log("INFO", f"Summarized Bloom metrics for {bloom_summary.count():,} source-batch group(s).")

# COMMAND ----------

# MAGIC %md ## DGIM results for FIRMS and EPA

# COMMAND ----------

dgim_summary = spark.sql(f"""
SELECT source, batch_id,
       max(metric_ts_utc) AS latest_metric_ts_utc,
       max(CASE WHEN metric_name = 'estimated_alerts_24h' THEN metric_value END) AS estimated_alerts_24h,
       max(CASE WHEN metric_name = 'exact_alerts_24h' THEN metric_value END) AS exact_alerts_24h,
       max(CASE WHEN metric_name = 'absolute_error' THEN metric_value END) AS absolute_error,
       max(CASE WHEN metric_name = 'relative_error' THEN metric_value END) AS relative_error,
       max(CASE WHEN metric_name = 'bucket_count' THEN metric_value END) AS bucket_count,
       max(CASE WHEN metric_name = 'late_events_in_batch' THEN metric_value END) AS late_events_in_batch,
       max(CASE WHEN metric_name = 'state_rebuilt' THEN metric_value END) AS state_rebuilt,
       CASE WHEN max(CASE WHEN metric_name = 'absolute_error' THEN metric_value END) = 0
            THEN 'OK' ELSE 'APPROXIMATE' END AS dgim_status
FROM {METRICS}
WHERE algorithm = 'dgim'
GROUP BY source, batch_id
ORDER BY source, batch_id DESC
""")
write_notebook_log("INFO", f"Summarized DGIM metrics for {dgim_summary.count():,} source-batch group(s).")

# COMMAND ----------

# MAGIC %md ## Persisted DGIM state

# COMMAND ----------

if spark.catalog.tableExists(STATE):
    state_summary = spark.table(STATE).select(
        "state_key", "algorithm", "last_event_ts", "window_seconds",
        "max_buckets", "updated_at_utc",
        F.length("buckets_json").alias("state_characters"),
    )
    write_notebook_log("INFO", f"Verified {state_summary.count():,} persisted streaming state row(s).")

# COMMAND ----------

# MAGIC %md ## Rubric evidence summary

# COMMAND ----------

rubric_summary = spark.sql(f"""
SELECT algorithm, source,
       count(DISTINCT batch_id) AS batches_evaluated,
       max(metric_ts_utc) AS latest_metric_ts_utc,
       round(avg(CASE WHEN metric_name = 'relative_error' THEN metric_value END), 4) AS avg_relative_error,
       max(CASE WHEN metric_name = 'false_negative_count' THEN metric_value END) AS max_bloom_false_negatives,
       round(avg(CASE WHEN metric_name = 'measured_false_positive_rate' THEN metric_value END), 4) AS avg_bloom_fpr
FROM {METRICS}
WHERE algorithm IN ('bloom_filter', 'dgim')
GROUP BY algorithm, source
ORDER BY algorithm, source
""")
write_notebook_log("INFO", f"Saved streaming-algorithm evidence summary with {rubric_summary.count():,} row(s).")
