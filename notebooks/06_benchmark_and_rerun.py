# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 06 · Benchmark, reproducibility, and late-data tests
# MAGIC
# MAGIC This notebook is an evaluation notebook, not part of the daily ingestion chain.
# MAGIC
# MAGIC * **A — Step timings:** summarizes the timings already written by the pipeline.
# MAGIC * **B — CSV versus API JSON:** an optional benchmark using EPA's pre-generated bulk CSV.
# MAGIC   The CSV is benchmark-only; it is not required by the operational daily pipeline.
# MAGIC * **C — Reproducibility:** reruns the current Bronze → Silver → Gold notebooks and checks
# MAGIC   that analytical values do not change when the source files have not changed.
# MAGIC * **D — Late data:** optionally adds a temporary revised EPA file and checks that the
# MAGIC   affected county-day is rebuilt. It is disabled by default because it intentionally
# MAGIC   changes the current analytical result while the test is running.
# MAGIC
# MAGIC The operational notebooks currently used by the job are:
# MAGIC `01_extract_epa`, `01_extract_firms_v2`, `01_extract_noaa`, `02_load_bronze_v2`,
# MAGIC `03_transform_silver`, `04_kpi_gold`, and `05_analytics_data_quality`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from logging_utils import store_log

SOURCE_FILE = "06_benchmark_and_rerun"

def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)

# MAGIC %md
# MAGIC ## Test controls
# MAGIC
# MAGIC `run_csv_benchmark=auto` runs the CSV comparison only when
# MAGIC `raw/epa_bulk/daily_88101_2020.csv` exists. It does not fail when the file has not
# MAGIC been uploaded. Set it to `true` to require the file, or `false` to skip the section.
# MAGIC
# MAGIC The late-data test is opt-in. It creates a temporary file in `raw/epa`, runs the
# MAGIC current ingestion notebooks, reports the changed county-days, and removes the
# MAGIC temporary file. The test is intended for a controlled run because the resulting
# MAGIC revised record remains the latest source version in the Delta tables.

# COMMAND ----------

dbutils.widgets.dropdown(
    "run_csv_benchmark", "auto", ["auto", "true", "false"],
    "Run optional EPA bulk CSV benchmark"
)
dbutils.widgets.dropdown(
    "run_reproducibility_test", "true", ["true", "false"],
    "Run Bronze-Silver-Gold rerun test"
)
dbutils.widgets.dropdown(
    "run_late_data_test", "false", ["true", "false"],
    "Run temporary late-data correction test"
)
dbutils.widgets.text("late_test_day", "2020-09-10", "EPA day for late-data test")

RUN_CSV = dbutils.widgets.get("run_csv_benchmark")
RUN_REPRO = dbutils.widgets.get("run_reproducibility_test") == "true"
RUN_LATE = dbutils.widgets.get("run_late_data_test") == "true"
LATE_TEST_DAY = dbutils.widgets.get("late_test_day").strip()
REPS = 3
NOTEBOOK_TIMEOUT_SECONDS = 3600

# These names match the notebooks currently used by the job. Do not rename the files
# just to make this notebook work.
BRONZE_NOTEBOOK = "./02_load_bronze_v2"
SILVER_NOTEBOOK = "./03_transform_silver"
GOLD_NOTEBOOK = "./04_kpi_gold"

write_notebook_log(
    "INFO",
    f"Benchmark controls: csv={RUN_CSV}, reproducibility={RUN_REPRO}, "
    f"late_data={RUN_LATE}, late_test_day={LATE_TEST_DAY}.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · Pipeline step timings

# COMMAND ----------

if spark.catalog.tableExists(t("ops_step_log")):
    timing_summary = spark.sql(f"""
        SELECT layer, step,
               count(*) AS runs,
               sum(rows) AS rows,
               round(avg(seconds), 1) AS avg_s,
               round(percentile(seconds, 0.5), 1) AS median_s,
               round(max(seconds), 1) AS max_s
        FROM {t('ops_step_log')}
        GROUP BY layer, step
        ORDER BY avg_s DESC
    """)
    write_notebook_log("INFO", f"Summarized timings for {timing_summary.count():,} pipeline step(s).")
else:
    write_notebook_log("WARNING", "ops_step_log does not exist yet; timing summary was skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Optional EPA bulk CSV versus API JSON
# MAGIC
# MAGIC This section preserves the proposal's CSV-versus-API comparison without making a
# MAGIC bulk load part of the daily job. The CSV is filtered to California and the 2020 study
# MAGIC window so it can be compared with the API JSON files already in `raw/epa`.
# MAGIC
# MAGIC If the CSV is not present, `bench_epa_from_csv` is deliberately **not** created.
# MAGIC `bench_epa_from_json` can still be created from the operational API output.

# COMMAND ----------

import os
import json as _json
import re
import statistics
import uuid

bulk_path = f"{LANDING['epa_csv_bulk']}/daily_88101_2020.csv"
json_glob = f"{LANDING['epa']}/*.json"

def path_exists(path):
    try:
        return any(f.name == os.path.basename(path) for f in dbutils.fs.ls(os.path.dirname(path)))
    except Exception as exc:
        write_notebook_log("WARNING", f"Could not inspect {path}: {exc}")
        return False

bulk_available = path_exists(bulk_path)
if RUN_CSV == "true" and not bulk_available:
    raise FileNotFoundError(
        f"Required benchmark input is missing: {bulk_path}. "
        "Upload daily_88101_2020.csv or set run_csv_benchmark=false/auto."
    )

run_csv_section = RUN_CSV == "true" or (RUN_CSV == "auto" and bulk_available)
csv_note = "CSV benchmark completed." if run_csv_section else (
    f"CSV benchmark skipped; {bulk_path} is not present."
)

def api_epa_df():
    """Return API rows in the same California/study-window scope as the CSV test."""
    return (
        spark.read.option("multiLine", True).json(json_glob)
        .select(F.explode("Data").alias("r"))
        .select("r.*")
        .where(
            (F.lpad(F.col("state_code").cast("string"), 2, "0") == STATE_FIPS)
            & F.to_date("date_local").between(F.lit(START_DATE), F.lit(END_DATE))
            & (F.col("parameter_code").cast("string") == F.lit("88101"))
        )
    )

def bulk_epa_df():
    """Return bulk CSV rows in the same California/study-window scope."""
    state = F.lpad(F.regexp_replace(F.col("State Code"), "[^0-9]", ""), 2, "0")
    return (
        spark.read.option("header", True).option("inferSchema", False).csv(bulk_path)
        .where(
            (state == STATE_FIPS)
            & F.to_date("Date Local").between(F.lit(START_DATE), F.lit(END_DATE))
            & (F.col("Parameter Code").cast("string") == F.lit("88101"))
        )
    )

def timed_count(df, value_col):
    started = time.time()
    row = df.agg(
        F.count("*").alias("rows"),
        F.avg(F.col(value_col).cast("double")).alias("avg_value"),
    ).first()
    return time.time() - started, int(row["rows"] or 0)

benchmark_rows = []

def add_timing_result(test, status, times, rows, note=""):
    benchmark_rows.append((
        test,
        status,
        int(rows),
        float(statistics.median(times)) if times else None,
        float(min(times)) if times else None,
        float(max(times)) if times else None,
        note,
    ))

if run_csv_section:
    for test, make_df, value_col in [
        ("raw_csv_bulk", bulk_epa_df, "Arithmetic Mean"),
        ("raw_json_api", api_epa_df, "arithmetic_mean"),
    ]:
        times = []
        rows = 0
        for _ in range(REPS):
            elapsed, rows = timed_count(make_df(), value_col)
            times.append(elapsed)
        add_timing_result(test, "completed", times, rows)

    # Materialise the two scoped inputs as Delta tables for a second comparison.
    # The CSV table exists only when the optional file exists.
    bulk_epa_df().write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        t("bench_epa_from_csv")
    )
    api_epa_df().write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        t("bench_epa_from_json")
    )

    delta_queries = [
        (
            "delta_query_from_csv",
            f"""SELECT `County Code`, avg(CAST(`Arithmetic Mean` AS DOUBLE)) AS avg_pm25
                FROM {t('bench_epa_from_csv')} GROUP BY 1""",
        ),
        (
            "delta_query_from_json",
            f"""SELECT county_code, avg(CAST(arithmetic_mean AS DOUBLE)) AS avg_pm25
                FROM {t('bench_epa_from_json')} GROUP BY 1""",
        ),
    ]
    for test, sql in delta_queries:
        times = []
        rows = 0
        for _ in range(REPS):
            started = time.time()
            rows = spark.sql(sql).count()
            times.append(time.time() - started)
        add_timing_result(test, "completed", times, rows)
else:
    add_timing_result(
        "raw_csv_bulk", "skipped_missing_input", [], 0,
        f"Upload {bulk_path} for the proposal's CSV-versus-API comparison.",
    )
    # The API side is still useful and uses only operationally landed data.
    times = []
    rows = 0
    for _ in range(REPS):
        elapsed, rows = timed_count(api_epa_df(), "arithmetic_mean")
        times.append(elapsed)
    add_timing_result("raw_json_api", "completed", times, rows)
    api_epa_df().write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        t("bench_epa_from_json")
    )

if benchmark_rows:
    bench_schema = """
        test string,
        status string,
        rows long,
        median_s double,
        min_s double,
        max_s double,
        note string
    """
    bench = (
        spark.createDataFrame(benchmark_rows, bench_schema)
        .withColumn("run_at_utc", F.current_timestamp())
    )
    (
        bench.write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(t("bench_results"))
    )
    write_notebook_log(
        "INFO",
        f"{csv_note} Saved {bench.count():,} benchmark result row(s) to {t('bench_results')}.",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Reproducibility: rerun unchanged inputs
# MAGIC
# MAGIC This uses the current notebook names. Gold is forced to rebuild every study date,
# MAGIC which makes the test stricter than a normal incremental run. The fingerprint excludes
# MAGIC ingestion/build timestamps and compares the analytical values only.

# COMMAND ----------

def fingerprint(table):
    df = spark.table(t(table))
    ignored = {"_gold_built_at_utc", "_ingested_at_utc", "_batch_id", "_source_file"}
    cols = [c for c in df.columns if c not in ignored]
    if not cols:
        return df.count(), None
    return df.count(), df.select(
        F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("fingerprint")
    ).first()["fingerprint"]

def run_current_pipeline(rebuild_all=False):
    """Run the current Bronze → Silver → Gold notebooks in dependency order."""
    dbutils.notebook.run(BRONZE_NOTEBOOK, NOTEBOOK_TIMEOUT_SECONDS)
    dbutils.notebook.run(SILVER_NOTEBOOK, NOTEBOOK_TIMEOUT_SECONDS)
    params = {"rebuild_all": "true"} if rebuild_all else {}
    dbutils.notebook.run(GOLD_NOTEBOOK, NOTEBOOK_TIMEOUT_SECONDS, params)

if RUN_REPRO:
    fingerprint_tables = [
        "silver_fire_detections",
        "silver_pm25_daily",
        "silver_weather_obs",
        "gold_fact_county_day",
    ]
    before = {tb: fingerprint(tb) for tb in fingerprint_tables}
    gold_version_before = spark.sql(
        f"DESCRIBE HISTORY {t('gold_fact_county_day')} LIMIT 1"
    ).first().version

    run_current_pipeline(rebuild_all=True)

    after = {tb: fingerprint(tb) for tb in fingerprint_tables}
    repro_rows = [
        (tb, before[tb][0], after[tb][0], before[tb][1], after[tb][1], before[tb] == after[tb])
        for tb in fingerprint_tables
    ]
    repro_schema = "table string, rows_before long, rows_after long, fingerprint_before decimal(38,0), fingerprint_after decimal(38,0), unchanged boolean"
    unchanged_count = sum(1 for row in repro_rows if row[-1])
    history_rows = (
        spark.sql(f"DESCRIBE HISTORY {t('gold_fact_county_day')}")
        .where(F.col("version") > F.lit(gold_version_before))
        .select("version", "operation", "operationMetrics")
        .count()
    )
    write_notebook_log(
        "INFO" if unchanged_count == len(repro_rows) else "WARNING",
        f"Reproducibility check: unchanged={unchanged_count}/{len(repro_rows)}, "
        f"new_gold_history_rows={history_rows}.",
    )
else:
    write_notebook_log("INFO", "Reproducibility test skipped by widget.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · Late-data correction test (opt-in)
# MAGIC
# MAGIC A temporary copy of one non-empty EPA JSON file is created with a changed PM2.5 value
# MAGIC and a later `date_of_last_change`. The file name contains the same measurement date, so
# MAGIC the existing incremental Bronze logic sees it as a new source version. After Bronze →
# MAGIC Silver → Gold, the notebook reports the county-days whose Gold PM2.5 value changed.
# MAGIC
# MAGIC The temporary file is removed afterward, but the revised record remains in the Delta
# MAGIC tables as the latest ingested version. Run this only when that state is acceptable, or
# MAGIC restore the tables from Delta history afterward.

# COMMAND ----------

def select_late_test_file(day):
    candidates = [
        f for f in dbutils.fs.ls(LANDING["epa"])
        if f.name.startswith(f"epa_{day}") and f.name.endswith(".json")
    ]
    if not candidates:
        raise FileNotFoundError(f"No EPA JSON file found for {day} in {LANDING['epa']}")
    # Build a local /Volumes path instead of relying on FileInfo.path, which may
    # be returned as dbfs:/... and cannot be passed to Python open().
    return f"{LANDING['epa']}/{sorted(candidates, key=lambda x: x.name)[0].name}"

def choose_valid_epa_record(data):
    for record in data:
        try:
            value = float(record.get("arithmetic_mean"))
            completeness = float(record.get("observation_percent"))
            if (
                str(record.get("state_code")).zfill(2) == STATE_FIPS
                and str(record.get("parameter_code")) == "88101"
                and PM25_MIN <= value <= PM25_MAX
                and completeness >= 75
            ):
                return record
        except (TypeError, ValueError):
            continue
    raise ValueError("No valid EPA PM2.5 record was found for the late-data test.")

if RUN_LATE:
    original_path = select_late_test_file(LATE_TEST_DAY)
    temp_name = f"epa_{LATE_TEST_DAY}_late_test_{uuid.uuid4().hex[:8]}.json"
    temp_path = f"{LANDING['epa']}/{temp_name}"

    # Volume paths are available to the driver in the same way as the extractor and
    # Bronze notebook already use them.
    with open(original_path, "r") as fh:
        doc = _json.load(fh)
    target = choose_valid_epa_record(doc.get("Data", []))
    old_value = float(target["arithmetic_mean"])
    new_value = (
        round(old_value + 10.0, 1)
        if old_value <= PM25_MAX - 10.0
        else round(old_value - 10.0, 1)
    )
    target["arithmetic_mean"] = str(new_value)
    target["date_of_last_change"] = "2099-12-31"
    with open(temp_path, "w") as fh:
        _json.dump(doc, fh)

    target_county = str(target.get("state_code")).zfill(2) + str(target.get("county_code")).zfill(3)
    write_notebook_log(
        "INFO",
        f"Late-data test file prepared: {temp_name}; county={target_county}; "
        f"date={LATE_TEST_DAY}; PM2.5 {old_value} -> {new_value}.",
    )

    try:
        before_gold = (
            spark.table(t("gold_fact_county_day"))
            .select("county_fips", "date_local", "pm25_mean_ugm3")
            .cache()
        )
        before_gold.count()
        run_current_pipeline(rebuild_all=False)
        after_gold = (
            spark.table(t("gold_fact_county_day"))
            .select("county_fips", "date_local", F.col("pm25_mean_ugm3").alias("pm25_after"))
        )
        diff = (
            before_gold.join(after_gold, ["county_fips", "date_local"])
            .where(~F.col("pm25_mean_ugm3").eqNullSafe(F.col("pm25_after")))
        )
        changed_count = diff.count()
        write_notebook_log("INFO", f"Late-data test changed {changed_count:,} county-day value(s).")
    finally:
        try:
            os.remove(temp_path)
        except FileNotFoundError:
            pass
else:
    write_notebook_log("INFO", "Late-data test skipped by widget.")
