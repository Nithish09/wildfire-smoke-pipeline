# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Benchmark (RQ2) and rerun / late-data tests (RQ3)
# MAGIC **Part A – where time goes:** summary of `ops_step_log` by step.
# MAGIC **Part B – CSV vs API (JSON):** the same EPA PM2.5 data read from the bulk CSV file and from the API JSON responses.
# MAGIC **Part C – rerun test:** fingerprint Gold, rerun Bronze → Silver → Gold on the same files, fingerprint again.
# MAGIC **Part D – late-data test:** revise one EPA value in a copy of a raw file, ingest it, confirm only that county-day changes.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## A · Step timings

# COMMAND ----------

display(spark.sql(f"""
SELECT layer, step, count(*) AS runs, sum(rows) AS rows, round(avg(seconds),1) AS avg_s,
       round(percentile(seconds, 0.5),1) AS median_s, round(max(seconds),1) AS max_s
FROM {t('ops_step_log')} GROUP BY layer, step ORDER BY avg_s DESC"""))

# COMMAND ----------

# MAGIC %md ## B · CSV vs API JSON (same EPA data)
# MAGIC Put the EPA pre-generated file `daily_88101_2020.csv` (from https://aqs.epa.gov/aqsweb/airdata/download_files.html)
# MAGIC in `epa_bulk/`. The bulk file covers the whole US for 2020, so we filter to California and the study window
# MAGIC so both inputs hold the same records. Each test is repeated `REPS` times; report the median.

# COMMAND ----------

import statistics
REPS = 3
bulk_path = f"{LANDING['epa_csv_bulk']}/daily_88101_2020.csv"
json_glob = f"{LANDING['epa']}/*.json"

def timed(fn):
    s = time.time(); n = fn(); return time.time() - s, n

def read_csv():
    df = (spark.read.option("header", True).csv(bulk_path)
          .where((F.col("State Code") == STATE_FIPS) &
                 F.col("Date Local").between(str(START_DATE), str(END_DATE))))
    return df.select(F.avg(F.col("Arithmetic Mean").cast("double"))).first()[0] is not None and df.count()

def read_json():
    df = spark.read.option("multiLine", True).json(json_glob).select(F.explode("Data").alias("r")).select("r.*")
    return df.select(F.avg(F.col("arithmetic_mean").cast("double"))).first()[0] is not None and df.count()

results = []
for name, fn in [("raw_csv_bulk", read_csv), ("raw_json_api", read_json)]:
    times = []
    for _ in range(REPS):
        s, n = timed(fn); times.append(s)
    results.append((name, n, statistics.median(times), min(times), max(times)))

# Query behaviour once the data is in Delta: same aggregate on each landed copy
(spark.read.option("header", True).csv(bulk_path).where(F.col("State Code") == STATE_FIPS)
      .write.mode("overwrite").saveAsTable(t("bench_epa_from_csv")))
(spark.read.option("multiLine", True).json(json_glob).select(F.explode("Data").alias("r")).select("r.*")
      .write.mode("overwrite").saveAsTable(t("bench_epa_from_json")))
for name, sql in [("delta_query_from_csv", f"SELECT `County Code`, avg(CAST(`Arithmetic Mean` AS DOUBLE)) FROM {t('bench_epa_from_csv')} GROUP BY 1"),
                  ("delta_query_from_json", f"SELECT county_code, avg(CAST(arithmetic_mean AS DOUBLE)) FROM {t('bench_epa_from_json')} GROUP BY 1")]:
    times = []
    for _ in range(REPS):
        s, n = timed(lambda: spark.sql(sql).count()); times.append(s)
    results.append((name, n, statistics.median(times), min(times), max(times)))

bench = spark.createDataFrame(results, "test string, rows long, median_s double, min_s double, max_s double") \
             .withColumn("run_at_utc", F.current_timestamp())
bench.write.mode("append").saveAsTable(t("bench_results"))
display(bench)
# Also note file sizes for the report:
for f in dbutils.fs.ls(LANDING["epa_csv_bulk"]): print(f.name, round(f.size / 1e6, 1), "MB")
print("JSON total MB:", round(sum(f.size for f in dbutils.fs.ls(LANDING["epa"])) / 1e6, 1))

# COMMAND ----------

# MAGIC %md ## C · Rerun test: Gold must not change when nothing changed at the source

# COMMAND ----------

def fingerprint(table):
    df = spark.table(t(table)).drop("_gold_built_at_utc", "_ingested_at_utc", "_batch_id", "_source_file")
    return df.count(), df.select(F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)"))).first()[0]

before = {tb: fingerprint(tb) for tb in ["silver_fire_detections", "silver_pm25_daily", "silver_weather_obs", "gold_fact_county_day"]}
v_before = spark.sql(f"DESCRIBE HISTORY {t('gold_fact_county_day')} LIMIT 1").first().version

dbutils.notebook.run("./02_bronze", 3600)
dbutils.notebook.run("./03_silver", 3600)
dbutils.notebook.run("./04_gold", 3600, {"rebuild_all": "true"})   # force a rebuild of every date: the strictest test

after = {tb: fingerprint(tb) for tb in before}
for tb in before:
    print(f"{tb:28s} before={before[tb]}  after={after[tb]}  unchanged={before[tb] == after[tb]}")
display(spark.sql(f"DESCRIBE HISTORY {t('gold_fact_county_day')}").where(f"version > {v_before}")
        .select("version", "operation", "operationMetrics"))
# Expect: MERGE with numTargetRowsUpdated = 0 and numTargetRowsInserted = 0

# COMMAND ----------

# MAGIC %md ## D · Late-data test: a revised EPA file changes only the affected county-day

# COMMAND ----------

import os, json as _json
test_day = "2020-09-10"
src = f"{LANDING['epa']}/epa_{test_day}.json"
doc = _json.load(open(src))
target = doc["Data"][0]
old_val = target["arithmetic_mean"]
target["arithmetic_mean"] = round(float(old_val) + 10.0, 1)
target["date_of_last_change"] = "2021-05-01"          # simulates EPA publishing a correction months later
_json.dump(doc, open(src, "w"))                        # same file name, new content -> new hash
print(f"Revised site {target['site_number']} county {target['county_code']}: {old_val} -> {target['arithmetic_mean']}")

g_before = spark.table(t("gold_fact_county_day")).select("county_fips", "date_local", "pm25_mean_ugm3")
g_before.cache().count()
dbutils.notebook.run("./02_bronze", 3600)
dbutils.notebook.run("./03_silver", 3600)
dbutils.notebook.run("./04_gold", 3600)
g_after = spark.table(t("gold_fact_county_day")).select("county_fips", "date_local", F.col("pm25_mean_ugm3").alias("after"))
diff = g_before.join(g_after, ["county_fips", "date_local"]).where(~F.col("pm25_mean_ugm3").eqNullSafe(F.col("after")))
display(diff)   # expect exactly one row: that county on that date
# To undo: restore the original file (old_val, original date_of_last_change) and rerun 02-04.
