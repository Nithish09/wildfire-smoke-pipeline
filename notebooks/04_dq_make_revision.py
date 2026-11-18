# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 · Simulate an EPA revision on the TEST copy
# MAGIC Updates **one** row of `silver_pm25_daily_test` in place with MERGE, the same way `03_transform_silver`
# MAGIC applies a newer EPA revision: new `pm25_ugm3`, newer `date_of_last_change`, new `_ingested_at_utc`.
# MAGIC
# MAGIC * **round 1:** pick a row whose `pm25_ugm3` is NOT null and set it to NULL
# MAGIC * **round 2:** set the same row back to its original value
# MAGIC
# MAGIC Only the test copy changes. The real pipeline tables are untouched.

# COMMAND ----------

# MAGIC %run ./00_dq_config

# COMMAND ----------

SOURCE_FILE = "04_dq_make_revision"

dbutils.widgets.dropdown("round", "1", ["1", "2"], "1 = PM2.5 to NULL, 2 = back to original")
ROUND = dbutils.widgets.get("round")
LOG = dq("dq_pm25_test_log")
store_log(spark, "INFO", SOURCE_FILE, f"Applying DQ revision round {ROUND} to {TEST_TABLE}.")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {LOG} (
    round STRING, site_id STRING, poc INT, date_local DATE, sample_duration STRING,
    old_pm25_ugm3 DOUBLE, new_pm25_ugm3 DOUBLE, applied_at_utc TIMESTAMP
  ) USING DELTA
""")

KEY = ["site_id", "poc", "date_local", "sample_duration"]

if ROUND == "1":
    pick = (spark.table(TEST_TABLE).where(F.col("pm25_ugm3").isNotNull())
                 .orderBy(*KEY).select(*KEY, "pm25_ugm3").first())
    old_pm25, new_pm25, new_dlc = pick.pm25_ugm3, None, "2099-12-30"
else:
    last = spark.table(LOG).where("round = '1'").orderBy(F.desc("applied_at_utc")).first()
    if last is None:
        raise ValueError("Run round 1 first.")
    pick = last
    old_pm25, new_pm25, new_dlc = None, last.old_pm25_ugm3, "2099-12-31"

before = (spark.table(TEST_TABLE)
    .where((F.col("site_id") == pick.site_id)
           & (F.col("poc") == pick.poc)
           & (F.col("date_local") == pick.date_local)
           & F.col("sample_duration").eqNullSafe(F.lit(pick.sample_duration)))
    .select(*KEY, "pm25_ugm3", "date_of_last_change", "_ingested_at_utc"))

store_log(spark, "INFO", SOURCE_FILE, f"Before round {ROUND}: expected pm25_ugm3 {old_pm25}")
display(before)

# COMMAND ----------

# Apply the revision with MERGE (updates the row in place, like the Silver notebook)
spark.createDataFrame(
    [(pick.site_id, pick.poc, pick.date_local, pick.sample_duration, new_pm25)],
    "site_id string, poc int, date_local date, sample_duration string, pm25_ugm3 double"
).createOrReplaceTempView("revision")

spark.sql(f"""
  MERGE INTO {TEST_TABLE} s
  USING revision n
  ON  s.site_id = n.site_id AND s.poc = n.poc
  AND s.date_local = n.date_local AND s.sample_duration <=> n.sample_duration
  WHEN MATCHED THEN UPDATE SET
    s.pm25_ugm3 = n.pm25_ugm3,
    s.date_of_last_change = DATE'{new_dlc}',
    s._ingested_at_utc = current_timestamp()
""")

after = (spark.table(TEST_TABLE)
    .where((F.col("site_id") == pick.site_id)
           & (F.col("poc") == pick.poc)
           & (F.col("date_local") == pick.date_local)
           & F.col("sample_duration").eqNullSafe(F.lit(pick.sample_duration)))
    .select(*KEY, "pm25_ugm3", "date_of_last_change", "_ingested_at_utc"))

updated = after.count()
if updated != 1:
    raise ValueError(f"Expected exactly one revised row, found {updated}")

actual = after.first()
if actual.pm25_ugm3 != new_pm25:
    raise ValueError(f"Expected pm25_ugm3={new_pm25}, found {actual.pm25_ugm3}")

spark.createDataFrame(
    [(ROUND, pick.site_id, pick.poc, pick.date_local, pick.sample_duration, old_pm25, new_pm25)],
    "round string, site_id string, poc int, date_local date, sample_duration string, "
    "old_pm25_ugm3 double, new_pm25_ugm3 double"
).withColumn("applied_at_utc", F.current_timestamp()).write.mode("append").saveAsTable(LOG)

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    f"Round {ROUND} applied: site_id={pick.site_id}, poc={pick.poc}, "
    f"date={pick.date_local}, sample_duration={pick.sample_duration}, "
    f"pm25_ugm3 {old_pm25} -> {new_pm25}.",
)
# updated = spark.table(TEST_TABLE).where(
#     (F.col("site_id") == pick.site_id)
#     & (F.col("poc") == pick.poc)
#     & (F.col("date_local") == pick.date_local)
# ).count()

display(after)
display(
    spark.table(LOG)
    .where(
        (F.col("round") == ROUND)
        & (F.col("site_id") == pick.site_id)
        & (F.col("poc") == pick.poc)
        & (F.col("date_local") == pick.date_local)
    )
    .orderBy(F.col("applied_at_utc").desc())
)

store_log(spark, "INFO", SOURCE_FILE, f"Verified exactly one revised row; actual pm25_ugm3={actual.pm25_ugm3}")