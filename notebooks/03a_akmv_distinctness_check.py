# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03a - AKMV composite-key distinctness check (Silver to Gold)
# MAGIC
# MAGIC This check uses the Gold-preserved EPA site-day grain:
# MAGIC `(county_fips, site_id, date_local)`.
# MAGIC
# MAGIC `poc` and `sample_duration` are intentionally excluded from this
# MAGIC cross-layer key. Silver can contain multiple observations for one site-day
# MAGIC , while Gold averages those observations into one site-day value. Therefore
# MAGIC `distinctness_pct` is an informational site-day collapse ratio, not a
# MAGIC default pass/fail rule.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import uuid

from pyspark.sql import functions as F

from logging_utils import store_log


SOURCE_FILE = "03a_akmv_distinctness_check"
# This test table is deliberately different from the production Silver table.
SILVER_TABLE = "workspace.wildfire.silver_pm25_daily_test"
GOLD_TABLE = t("gold_fact_county_day")
RESULT_TABLE = t("dq_akmv_silver_gold")
KEY = ["county_fips", "site_id", "date_local"]

dbutils.widgets.text("k", "2048", "AKMV retained hashes")
dbutils.widgets.text(
    "min_distinctness_pct",
    "0.0",
    "Optional minimum site-day distinctness; used only when enabled",
)
dbutils.widgets.dropdown(
    "enforce_distinctness",
    "false",
    ["false", "true"],
    "Enforce the informational distinctness threshold",
)

K = int(dbutils.widgets.get("k"))
MIN_DISTINCTNESS_PCT = float(dbutils.widgets.get("min_distinctness_pct"))
ENFORCE_DISTINCTNESS = dbutils.widgets.get("enforce_distinctness") == "true"

if K < 2:
    raise ValueError("k must be at least 2")
if not 0 <= MIN_DISTINCTNESS_PCT <= 100:
    raise ValueError("min_distinctness_pct must be between 0 and 100")
if not spark.catalog.tableExists(SILVER_TABLE):
    raise ValueError(f"Silver source does not exist: {SILVER_TABLE}")

silver_all = spark.table(SILVER_TABLE)
missing = sorted(set(KEY) - set(silver_all.columns))
if missing:
    raise ValueError(f"{SILVER_TABLE} is missing required columns: {missing}")

# Null key values cannot be reconciled reliably with Gold. Keep them visible as
# a separate failure metric rather than silently treating them as one key.
valid_key_condition = (
    F.col("county_fips").isNotNull()
    & F.col("site_id").isNotNull()
    & F.col("date_local").isNotNull()
)
source_row_count = silver_all.count()
invalid_key_rows = silver_all.where(~valid_key_condition).count()
silver = silver_all.where(valid_key_condition)
valid_row_count = silver.count()

# Use explicit null/value tags and length prefixes. This avoids collisions such
# as a SQL NULL being serialized the same way as a literal "<NULL>" string.
def canonical_component(column):
    value = F.col(column).cast("string")
    return F.when(
        value.isNull(),
        F.lit("N"),
    ).otherwise(
        F.concat(F.lit("V"), F.length(value).cast("string"), F.lit(":"), value)
    )


keyed = silver.select(
    *KEY,
    F.concat_ws(
        "|",
        F.lit("epa_aqs"),
        *[canonical_component(column) for column in KEY],
    ).alias("canonical_key"),
)
hashed = keyed.withColumn(
    "hash_hex",
    F.substring(F.sha2("canonical_key", 256), 1, 16),
)

# The driver receives at most K+1 grouped hashes, not the full Silver table.
bottom_plus_one = (
    hashed.groupBy("hash_hex")
    .agg(F.count("*").alias("occurrences"))
    .orderBy("hash_hex")
    .limit(K + 1)
    .collect()
)

is_exact = len(bottom_plus_one) <= K
bottom = bottom_plus_one[:K]
retained = len(bottom)

if not retained:
    akmv_distinct_count = 0
elif is_exact:
    akmv_distinct_count = retained
else:
    kth_hash = int(bottom[-1]["hash_hex"], 16)
    threshold = max(kth_hash / float(1 << 64), 1.0 / float(1 << 64))
    akmv_distinct_count = max(retained, round((K - 1) / threshold))

# An estimate cannot exceed the number of valid input rows.
akmv_distinct_count = min(valid_row_count, akmv_distinct_count)
distinctness_pct = (
    min(100.0, 100.0 * akmv_distinct_count / valid_row_count)
    if valid_row_count
    else 0.0
)
# This is only the duplicate count among retained hashes, not a full duplicate
# count. It is retained as a bounded-memory diagnostic.
sampled_duplicate_rows = sum(int(row.occurrences) - 1 for row in bottom)

# Compute an exact site-day count for the cross-layer reconciliation. Gold's
# pm25_sites is also a site-day count, so this is the authoritative comparison;
# AKMV remains the scalable estimate and monitoring metric.
silver_sites = (
    silver.groupBy("county_fips", "date_local")
    .agg(F.countDistinct("site_id").alias("expected_pm25_sites"))
)
silver_rollup = silver_sites.agg(
    F.sum("expected_pm25_sites").cast("long").alias("site_days"),
    F.count("*").cast("long").alias("county_days"),
).first()
exact_distinct_count = int(silver_rollup.site_days or 0)
exact_distinctness_pct = (
    100.0 * exact_distinct_count / valid_row_count
    if valid_row_count
    else 0.0
)
akmv_error_pct = (
    100.0 * (akmv_distinct_count - exact_distinct_count) / exact_distinct_count
    if exact_distinct_count
    else 0.0
)

gold_available = spark.catalog.tableExists(GOLD_TABLE)
gold_mismatch_groups = None
gold_duplicate_groups = None
silver_site_days = exact_distinct_count
gold_site_days = None
site_day_retention_pct = None
silver_pm25_county_days = int(silver_rollup.county_days or 0)
gold_pm25_county_days = None
county_day_coverage_pct = None
gold_version = None

if gold_available:
    gold = spark.table(GOLD_TABLE)
    required_gold_columns = {"county_fips", "date_local", "pm25_sites"}
    missing_gold = sorted(required_gold_columns - set(gold.columns))
    if missing_gold:
        raise ValueError(f"{GOLD_TABLE} is missing required columns: {missing_gold}")

    bounds = silver.agg(
        F.min("date_local").alias("min_date"),
        F.max("date_local").alias("max_date"),
    ).first()
    if bounds.min_date is None:
        gold_scope = gold.limit(0)
    else:
        # Compare the same date scope. Gold may contain additional dates from
        # other Silver runs, which should not affect this test-table result.
        gold_scope = gold.where(
            F.col("date_local").between(bounds.min_date, bounds.max_date)
        )

    gold_sites = gold_scope.select(
        "county_fips",
        "date_local",
        F.coalesce(F.col("pm25_sites"), F.lit(0)).alias("gold_pm25_sites"),
    )
    gold_duplicate_groups = (
        gold_sites.groupBy("county_fips", "date_local")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    gold_mismatch_groups = (
        silver_sites.join(gold_sites, ["county_fips", "date_local"], "full")
        .where(
            F.coalesce(F.col("expected_pm25_sites"), F.lit(0))
            != F.coalesce(F.col("gold_pm25_sites"), F.lit(0))
        )
        .count()
    )

    gold_rollup = gold_scope.agg(
        F.sum(F.coalesce(F.col("pm25_sites"), F.lit(0)))
        .cast("long")
        .alias("site_days"),
        # A PM2.5 site-day exists only when at least one valid site is counted.
        # This is safer than relying on has_pm25 if a malformed Gold row has
        # has_pm25=true but pm25_sites=0.
        F.sum(
            (F.coalesce(F.col("pm25_sites"), F.lit(0)) > 0).cast("long")
        ).cast("long").alias("county_days"),
    ).first()
    gold_site_days = int(gold_rollup.site_days or 0)
    gold_pm25_county_days = int(gold_rollup.county_days or 0)
    site_day_retention_pct = (
        100.0 * gold_site_days / silver_site_days
        if silver_site_days
        else 100.0
    )
    county_day_coverage_pct = (
        100.0 * gold_pm25_county_days / silver_pm25_county_days
        if silver_pm25_county_days
        else 100.0
    )
    gold_version = int(
        spark.sql(f"DESCRIBE HISTORY {GOLD_TABLE} LIMIT 1").first()["version"]
    )

distinctness_check_applied = ENFORCE_DISTINCTNESS and MIN_DISTINCTNESS_PCT > 0
distinctness_check_passed = (
    not distinctness_check_applied
    or distinctness_pct >= MIN_DISTINCTNESS_PCT
)
gold_check_passed = (
    gold_available
    and invalid_key_rows == 0
    and gold_mismatch_groups == 0
    and gold_duplicate_groups == 0
)
passed = bool(gold_check_passed and distinctness_check_passed)

failure_reasons = []
if not gold_available:
    failure_reasons.append("gold_table_missing")
if invalid_key_rows:
    failure_reasons.append("silver_null_key_rows")
if gold_mismatch_groups not in (None, 0):
    failure_reasons.append("gold_site_day_count_mismatch")
if gold_duplicate_groups not in (None, 0):
    failure_reasons.append("gold_duplicate_county_day_groups")
if distinctness_check_applied and not distinctness_check_passed:
    failure_reasons.append("distinctness_below_threshold")

silver_version = int(
    spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE} LIMIT 1").first()["version"]
)
run_id = str(uuid.uuid4())
result = spark.createDataFrame(
    [
        (
            run_id,
            SILVER_TABLE,
            GOLD_TABLE,
            K,
            source_row_count,
            valid_row_count,
            invalid_key_rows,
            akmv_distinct_count,
            exact_distinct_count,
            distinctness_pct,
            exact_distinctness_pct,
            akmv_error_pct,
            sampled_duplicate_rows,
            gold_available,
            silver_site_days,
            gold_site_days,
            site_day_retention_pct,
            silver_pm25_county_days,
            gold_pm25_county_days,
            county_day_coverage_pct,
            gold_mismatch_groups,
            gold_duplicate_groups,
            MIN_DISTINCTNESS_PCT,
            distinctness_check_applied,
            passed,
            ",".join(failure_reasons),
            silver_version,
            gold_version,
        )
    ],
    "run_id string, silver_table string, gold_table string, k int, "
    "source_row_count long, row_count long, invalid_key_rows long, "
    "distinct_count long, exact_distinct_count long, distinctness_pct double, "
    "exact_distinctness_pct double, akmv_error_pct double, "
    "sampled_duplicate_rows long, gold_available boolean, "
    "silver_site_days long, gold_site_days long, site_day_retention_pct double, "
    "silver_pm25_county_days long, gold_pm25_county_days long, "
    "county_day_coverage_pct double, gold_site_count_mismatch_groups long, "
    "gold_duplicate_county_day_groups long, min_distinctness_pct double, "
    "distinctness_check_applied boolean, passed boolean, failure_reasons string, "
    "silver_version long, gold_version long",
).withColumn("evaluated_at_utc", F.current_timestamp())

result.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
    RESULT_TABLE
)
store_log(
    spark,
    "INFO" if passed else "WARNING",
    SOURCE_FILE,
    f"AKMV Silver/Gold check {'passed' if passed else 'failed'}: "
    f"AKMV distinct={akmv_distinct_count:,}, exact distinct={exact_distinct_count:,}, "
    f"site-day distinctness={distinctness_pct:.2f}%, "
    f"Gold mismatch groups={gold_mismatch_groups}, "
    f"failures={','.join(failure_reasons) or 'none'}",
)
display(result)