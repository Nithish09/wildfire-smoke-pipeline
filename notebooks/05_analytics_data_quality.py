# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Data-quality report
# MAGIC Produces the numbers for `docs/data_quality_report.md`: pass rates, quarantine reasons, duplicates removed,
# MAGIC null rates, unmatched identifiers and late (revised) records. Results are saved to `dq_summary` with a run timestamp.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

SOURCE_FILE = "05_analytics_data_quality"

def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)

write_notebook_log("INFO", "Starting analytical data-quality summary.")

run_ts = dt.datetime.utcnow()
rows = []

def add(metric, source, value, note=""):
    rows.append((run_ts, source, metric, float(value), note))

# 1) Completeness: share of Bronze rows that reached Silver (after dedup) or were quarantined
pairs = [("nasa_firms", "bronze_firms", "silver_fire_detections"),
         ("epa_aqs", "bronze_epa_pm25", "silver_pm25_daily"),
         ("noaa_cdo", "bronze_noaa_daily", "silver_weather_obs")]
q = spark.table(t("silver_quarantine"))
for src, bt, st in pairs:
    nb = spark.table(t(bt)).count()
    ns = spark.table(t(st)).count()
    nq = q.where(F.col("source") == src).count()
    add("bronze_rows", src, nb); add("silver_rows", src, ns); add("quarantined_rows", src, nq)
    add("pct_passing_validation", src, 100.0 * (nb - nq) / nb if nb else 0)
    add("duplicate_or_superseded_rows", src, nb - nq - ns, "Bronze rows that passed but collapsed onto an existing key")

# 2) Late / revised records
m = spark.table(t("ops_ingest_manifest"))
revised = m.groupBy("source", "file_name").agg(F.countDistinct("content_hash").alias("v")).where("v > 1")
for r in revised.groupBy("source").count().collect():
    add("files_revised_at_source", r.source, r["count"], "same file name received with new content")
late = spark.table(t("silver_pm25_daily")).where(F.datediff("date_of_last_change", "date_local") > 30).count()
add("epa_values_changed_30d_after_measurement", "epa_aqs", late)

# 3) Unmatched identifiers / spatial assignment quality
fires = spark.table(t("silver_fire_detections"))
add("fires_far_from_assigned_county_centre_gt_60km", "nasa_firms",
    fires.where("county_distance_km > 60").count(), "candidates for border misassignment")
epa_counties = spark.table(t("silver_pm25_daily")).select("county_fips").distinct()
unmatched = epa_counties.join(spark.table(t("silver_dim_county")), "county_fips", "left_anti").count()
add("epa_county_codes_not_in_dimension", "epa_aqs", unmatched)

# 4) Gold coverage
g = spark.table(t("gold_fact_county_day"))
ng = g.count()
add("gold_rows", "gold", ng)
add("pct_county_days_with_pm25", "gold", 100.0 * g.where("has_pm25").count() / ng if ng else 0)
add("pct_county_days_with_weather", "gold", 100.0 * g.where("has_weather").count() / ng if ng else 0)
add("duplicate_gold_keys", "gold", ng - g.select("county_fips", "date_local").distinct().count())

summary = spark.createDataFrame(rows, "run_at_utc timestamp, source string, metric string, value double, note string")
summary.write.mode("append").saveAsTable(t("dq_summary"))
write_notebook_log("INFO", f"Saved {len(rows):,} data-quality metrics to {t('dq_summary')}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine reasons

# COMMAND ----------

quarantine_summary = spark.sql(f"""SELECT source, reason, count(*) AS records,
  round(100 * count(*) / sum(count(*)) OVER (PARTITION BY source), 1) AS pct_of_source_quarantine
  FROM {t('silver_quarantine')} GROUP BY source, reason ORDER BY source, records DESC""")
write_notebook_log("INFO", f"Summarized {quarantine_summary.count():,} quarantine reason group(s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null rates in Silver (key analytical columns)

# COMMAND ----------

def null_rates(table, cols):
    df = spark.table(t(table)); n = df.count()
    exprs = [F.round(100 * F.sum(F.col(c).isNull().cast("int")) / F.lit(n), 2).alias(c) for c in cols]
    return df.agg(*exprs).withColumn("table", F.lit(table))

null_rate_tables = [
    ("silver_fire_detections", ["frp_mw", "brightness_k", "confidence_class", "county_fips"]),
    ("silver_pm25_daily", ["pm25_ugm3", "aqi", "observation_percent", "latitude"]),
    ("silver_weather_daily", ["tmax_c", "tmin_c", "prcp_mm", "awnd_ms", "wdf2_deg"]),
]
for table_name, columns in null_rate_tables:
    null_rates(table_name, columns).count()
write_notebook_log("INFO", f"Calculated null-rate summaries for {len(null_rate_tables)} Silver table(s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample-based correctness check
# MAGIC Pick 5 random county-days and compare Gold against a manual recount from Silver. Paste the output into the report.

# COMMAND ----------

sample = g.where("fire_detections > 0").orderBy(F.rand(42)).limit(5).select("county_fips", "date_local", "fire_detections", "pm25_mean_ugm3")
recount = (spark.table(t("silver_fire_detections")).groupBy("county_fips", "date_local").agg(F.count("*").alias("recount_fires")))
sample_check = (sample.join(recount, ["county_fips", "date_local"], "left")
                .withColumn("match", F.col("fire_detections") == F.col("recount_fires")))
sample_count = sample_check.count()
mismatch_count = sample_check.where(~F.col("match")).count()
write_notebook_log(
    "INFO" if mismatch_count == 0 else "WARNING",
    f"Gold recount check completed: sampled_rows={sample_count}, mismatches={mismatch_count}.",
)
