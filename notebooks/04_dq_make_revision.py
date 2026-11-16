# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Simulate an EPA revision on the TEST copy
# MAGIC Updates **one** row of `silver_pm25_daily_test` in place with MERGE, the same way `03_transform_silver`
# MAGIC applies a newer EPA revision: new `aqi`, newer `date_of_last_change`, new `_ingested_at_utc`.
# MAGIC
# MAGIC * **round 1:** pick a row whose `aqi` is NOT null and set it to NULL
# MAGIC * **round 2:** set the same row back to its original value
# MAGIC
# MAGIC Only the test copy changes. The real pipeline tables are untouched.

# COMMAND ----------

# MAGIC %run ./00_dq_config

# COMMAND ----------

SOURCE_FILE = "04_dq_make_revision"

dbutils.widgets.dropdown("round", "1", ["1", "2"], "1 = aqi to NULL, 2 = back to original")
ROUND = dbutils.widgets.get("round")
LOG = dq("dq_test_log")
store_log(spark, "INFO", SOURCE_FILE, f"Applying DQ revision round {ROUND} to {TEST_TABLE}.")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {LOG} (
    round STRING, site_id STRING, poc INT, date_local DATE, sample_duration STRING,
    old_aqi INT, new_aqi INT, applied_at_utc TIMESTAMP
  ) USING DELTA
""")

KEY = ["site_id", "poc", "date_local", "sample_duration"]

if ROUND == "1":
    pick = (spark.table(TEST_TABLE).where(F.col("aqi").isNotNull())
                 .orderBy(*KEY).select(*KEY, "aqi").first())
    old_aqi, new_aqi, new_dlc = pick.aqi, None, "2099-12-30"
else:
    last = spark.table(LOG).where("round = '1'").orderBy(F.desc("applied_at_utc")).first()
    if last is None:
        raise ValueError("Run round 1 first.")
    pick = last
    old_aqi, new_aqi, new_dlc = None, last.old_aqi, "2099-12-31"

# COMMAND ----------

# Apply the revision with MERGE (updates the row in place, like the Silver notebook)
spark.createDataFrame(
    [(pick.site_id, pick.poc, pick.date_local, pick.sample_duration, new_aqi)],
    "site_id string, poc int, date_local date, sample_duration string, aqi int"
).createOrReplaceTempView("revision")

spark.sql(f"""
  MERGE INTO {TEST_TABLE} s
  USING revision n
  ON  s.site_id = n.site_id AND s.poc = n.poc
  AND s.date_local = n.date_local AND s.sample_duration <=> n.sample_duration
  WHEN MATCHED THEN UPDATE SET
    s.aqi = n.aqi,
    s.date_of_last_change = DATE'{new_dlc}',
    s._ingested_at_utc = current_timestamp()
""")

spark.createDataFrame(
    [(ROUND, pick.site_id, pick.poc, pick.date_local, pick.sample_duration, old_aqi, new_aqi)],
    "round string, site_id string, poc int, date_local date, sample_duration string, old_aqi int, new_aqi int"
).withColumn("applied_at_utc", F.current_timestamp()).write.mode("append").saveAsTable(LOG)

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    f"Round {ROUND} applied: site_id={pick.site_id}, poc={pick.poc}, "
    f"date={pick.date_local}, sample_duration={pick.sample_duration}, "
    f"aqi {old_aqi} -> {new_aqi}.",
)
updated = spark.table(TEST_TABLE).where(
    (F.col("site_id") == pick.site_id)
    & (F.col("poc") == pick.poc)
    & (F.col("date_local") == pick.date_local)
).count()
store_log(spark, "INFO", SOURCE_FILE, f"Verified {updated} matching revised test row(s).")
