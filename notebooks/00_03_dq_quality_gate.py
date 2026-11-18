# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Overwrite-aware Silver DQ gate
# MAGIC
# MAGIC This is the production-facing extension of the Deequ-style experiment. It
# MAGIC keeps a signed frequency state for every observed value. Delta Change Data
# MAGIC Feed contributes `-1` for an old row image and `+1` for its new image, so an
# MAGIC overwrite replaces the old contribution instead of being counted twice.
# MAGIC
# MAGIC Metrics produced for each configured column:
# MAGIC * `null_count` and `null_pct` (completeness)
# MAGIC * `distinct_count` and `distinctness_pct` (the paper's consistency metric)
# MAGIC * `row_count`
# MAGIC
# MAGIC The same run also computes an AKMV estimate for the composite EPA
# MAGIC site-day key `(county_fips, site_id, date_local)`. AKMV is kept separate
# MAGIC from the CDF-backed per-column metrics because it is an approximate,
# MAGIC bounded-memory algorithm. This notebook intentionally validates Silver
# MAGIC only; Gold reconciliation is not part of this DQ gate.
# MAGIC
# MAGIC The notebook appends an auditable pass/fail decision. Gold can consume this
# MAGIC gate later, but this notebook validates Silver only.

# COMMAND ----------

# MAGIC %run ./00_dq_config

# COMMAND ----------

import uuid

dbutils.widgets.text("table", TEST_TABLE, "Silver table to validate")
dbutils.widgets.text("null_columns", "pm25_ugm3", "Columns to null-check (comma separated)")
dbutils.widgets.text("distinct_columns", "pm25_ugm3", "Columns to distinct-count (comma separated)")
dbutils.widgets.text(
    "rules_json",
    '{"pm25_ugm3":{"max_null_pct":10.0,"min_distinctness_pct":0.0}}',
    "Per-column thresholds as JSON",
)
dbutils.widgets.dropdown("reinit", "false", ["false", "true"], "Rebuild CDF state with a full scan")
dbutils.widgets.dropdown("verify", "true", ["true", "false"], "Compare state with a full scan")
dbutils.widgets.text(
    "akmv_key_columns",
    "county_fips,site_id,date_local",
    "AKMV composite key columns",
)
dbutils.widgets.text("akmv_k", "2048", "AKMV retained hashes")
dbutils.widgets.text(
    "akmv_min_distinctness_pct",
    "40.0",
    "Minimum AKMV site-day distinctness",
)

TABLE = dbutils.widgets.get("table")
NULL_COLS = [c.strip() for c in dbutils.widgets.get("null_columns").split(",") if c.strip()]
DISTINCT_COLS = [c.strip() for c in dbutils.widgets.get("distinct_columns").split(",") if c.strip()]
COLS = list(dict.fromkeys(NULL_COLS + DISTINCT_COLS))
RULES = json.loads(dbutils.widgets.get("rules_json"))
REINIT = dbutils.widgets.get("reinit") == "true"
VERIFY = dbutils.widgets.get("verify") == "true"
AKMV_KEY_COLS = [c.strip() for c in dbutils.widgets.get("akmv_key_columns").split(",") if c.strip()]
AKMV_K = int(dbutils.widgets.get("akmv_k"))
AKMV_MIN_DISTINCTNESS_PCT = float(dbutils.widgets.get("akmv_min_distinctness_pct"))

if not COLS:
    raise ValueError("At least one column is required")
if len(AKMV_KEY_COLS) < 1:
    raise ValueError("akmv_key_columns must contain at least one column")
if AKMV_K < 2:
    raise ValueError("akmv_k must be at least 2")
if not 0 <= AKMV_MIN_DISTINCTNESS_PCT <= 100:
    raise ValueError("akmv_min_distinctness_pct must be between 0 and 100")
if not spark.catalog.tableExists(TABLE):
    raise ValueError(f"DQ source table does not exist: {TABLE}")
table_columns = set(spark.table(TABLE).columns)
missing = sorted((set(COLS) | set(AKMV_KEY_COLS)) - table_columns)
if missing:
    raise ValueError(f"DQ/AKMV columns are missing from {TABLE}: {missing}")
for column in NULL_COLS:
    rule = RULES.get(column, {})
    if "max_null_pct" not in rule:
        raise ValueError(f"rules_json must define max_null_pct for {column}")
    if not 0 <= float(rule["max_null_pct"]) <= 100:
        raise ValueError(f"max_null_pct for {column} must be between 0 and 100")
for column in DISTINCT_COLS:
    rule = RULES.get(column, {})
    if not 0 <= float(rule.get("min_distinctness_pct", 0)) <= 100:
        raise ValueError(f"min_distinctness_pct for {column} must be between 0 and 100")

VALUE_STATE = DQ_CDF_VALUE_STATE
METRIC_STATE = DQ_CDF_METRIC_STATE
RUNS = DQ_METRIC_RUNS
GATE = DQ_GOLD_GATE
AKMV_RUNS = dq("dq_akmv_runs")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DQ_SCHEMA}")

# CDF is enabled here as a safe migration. If it was not enabled when Silver
# was first created, the first run reinitialises from the current snapshot.
props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {TABLE}").collect()}
if props.get("delta.enableChangeDataFeed", "false").lower() != "true":
    spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    REINIT = True

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {VALUE_STATE} (
  table_name STRING, column_name STRING, value_hash STRING, value_repr STRING,
  is_null BOOLEAN, value_count BIGINT, last_version BIGINT, updated_at_utc TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METRIC_STATE} (
  table_name STRING, column_name STRING, row_count BIGINT, null_count BIGINT,
  non_null_count BIGINT, distinct_count BIGINT, last_version BIGINT,
  updated_at_utc TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RUNS} (
  run_id STRING, table_name STRING, column_name STRING, row_count BIGINT,
  null_count BIGINT, null_pct DOUBLE, distinct_count BIGINT,
  distinctness_pct DOUBLE, last_version BIGINT, evaluated_at_utc TIMESTAMP
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GATE} (
  run_id STRING, table_name STRING, evaluated_at_utc TIMESTAMP, passed BOOLEAN,
  failed_columns STRING, rules_json STRING, metrics_json STRING, last_version BIGINT
) USING DELTA
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AKMV_RUNS} (
  run_id STRING, table_name STRING, key_columns STRING, k INT,
  row_count BIGINT, invalid_key_rows BIGINT, distinct_count BIGINT,
  distinctness_pct DOUBLE,
  sampled_duplicate_rows BIGINT,
  min_distinctness_pct DOUBLE, distinctness_check_applied BOOLEAN,
  passed BOOLEAN, failure_reasons STRING,
  last_version BIGINT, evaluated_at_utc TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

def current_version():
    return int(spark.sql(f"DESCRIBE HISTORY {TABLE} LIMIT 1").first()["version"])


def value_columns(df, column):
    # Include the null marker in the hash so a literal string such as '__NULL__'
    # cannot collide with an actual null.
    value = F.col(column).cast("string")
    tagged = F.struct(F.col(column).isNull().alias("is_null"), value.alias("value"))
    return (df.withColumn("_is_null", F.col(column).isNull())
              .withColumn("_value_repr", value)
              .withColumn("_value_hash", F.sha2(F.to_json(tagged), 256)))


def save_metrics(version):
    values = spark.table(VALUE_STATE).where(
        (F.col("table_name") == TABLE) & F.col("column_name").isin(COLS)
    )
    metrics = (values.where(F.col("value_count") > 0)
        .groupBy("table_name", "column_name")
        .agg(
            F.sum("value_count").cast("long").alias("row_count"),
            F.sum(F.when(F.col("is_null"), F.col("value_count")).otherwise(0)).cast("long").alias("null_count"),
            F.sum(F.when(~F.col("is_null"), F.col("value_count")).otherwise(0)).cast("long").alias("non_null_count"),
            F.sum(F.when(~F.col("is_null"), 1).otherwise(0)).cast("long").alias("distinct_count"),
        )
        .withColumn("last_version", F.lit(version).cast("long"))
        .withColumn("updated_at_utc", F.current_timestamp()))
    # A zero-row table has no value-state groups; explicitly preserve a metric row.
    existing = {(r.column_name) for r in metrics.select("column_name").collect()}
    missing_rows = [(TABLE, c, 0, 0, 0, 0, version) for c in COLS if c not in existing]
    if missing_rows:
        metrics = metrics.unionByName(
            spark.createDataFrame(missing_rows,
                "table_name string, column_name string, row_count long, null_count long, "
                "non_null_count long, distinct_count long, last_version long")
                .withColumn("updated_at_utc", F.current_timestamp())
        )
    (DeltaTable.forName(spark, METRIC_STATE).alias("s")
        .merge(metrics.alias("n"), "s.table_name = n.table_name AND s.column_name = n.column_name")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    return {r.column_name: r.asDict() for r in spark.table(METRIC_STATE)
            .where((F.col("table_name") == TABLE) & F.col("column_name").isin(COLS)).collect()}


def init_state(version):
    spark.sql(f"DELETE FROM {VALUE_STATE} WHERE table_name = '{TABLE}'")
    spark.sql(f"DELETE FROM {METRIC_STATE} WHERE table_name = '{TABLE}'")
    source = spark.table(TABLE)
    for column in COLS:
        grouped = (value_columns(source, column)
            .groupBy("_value_hash", "_value_repr", "_is_null")
            .agg(F.count("*").cast("long").alias("value_count"))
            .select(F.lit(TABLE).alias("table_name"), F.lit(column).alias("column_name"),
                    F.col("_value_hash").alias("value_hash"), F.col("_value_repr").alias("value_repr"),
                    F.col("_is_null").alias("is_null"), "value_count")
            .withColumn("last_version", F.lit(version).cast("long"))
            .withColumn("updated_at_utc", F.current_timestamp()))
        grouped.write.mode("append").saveAsTable(VALUE_STATE)
    save_metrics(version)
    print(f"Initialised overwrite-aware state at Delta version {version}")


def load_metric_state():
    rows = (spark.table(METRIC_STATE)
        .where((F.col("table_name") == TABLE) & F.col("column_name").isin(COLS)).collect())
    return {r.column_name: r for r in rows}


def apply_cdf(version, state):
    start = min(int(r.last_version) for r in state.values()) + 1
    if start > version:
        return 0
    changes = (spark.read.option("readChangeFeed", "true")
        .option("startingVersion", start).option("endingVersion", version).table(TABLE))
    sign = (F.when(F.col("_change_type").isin("insert", "update_postimage"), 1)
        .when(F.col("_change_type").isin("update_preimage", "delete"), -1).otherwise(0))
    change_count = changes.count()
    for column in COLS:
        delta = (value_columns(changes, column)
            .withColumn("_delta", sign)
            .groupBy("_value_hash", "_value_repr", "_is_null")
            .agg(F.sum("_delta").cast("long").alias("delta"))
            .where(F.col("delta") != 0)
            .select(F.lit(TABLE).alias("table_name"), F.lit(column).alias("column_name"),
                    F.col("_value_hash").alias("value_hash"), F.col("_value_repr").alias("value_repr"),
                    F.col("_is_null").alias("is_null"), "delta")
            .withColumn("last_version", F.lit(version).cast("long"))
            .withColumn("updated_at_utc", F.current_timestamp()))
        (DeltaTable.forName(spark, VALUE_STATE).alias("s")
            .merge(delta.alias("n"), "s.table_name = n.table_name AND s.column_name = n.column_name AND s.value_hash = n.value_hash")
            .whenMatchedUpdate(set={"value_count": "s.value_count + n.delta", "last_version": "n.last_version", "updated_at_utc": "n.updated_at_utc"})
            .whenNotMatchedInsert(values={"table_name": "n.table_name", "column_name": "n.column_name", "value_hash": "n.value_hash", "value_repr": "n.value_repr", "is_null": "n.is_null", "value_count": "n.delta", "last_version": "n.last_version", "updated_at_utc": "n.updated_at_utc"})
            .execute())
    save_metrics(version)
    return change_count


def compute_akmv(version, run_id):
    """Compute the bounded-memory AKMV composite-key metric for this DQ run."""
    source_all = spark.table(TABLE)
    valid_condition = None
    for column in AKMV_KEY_COLS:
        condition = F.col(column).isNotNull()
        valid_condition = condition if valid_condition is None else valid_condition & condition

    invalid_key_rows = source_all.where(~valid_condition).count()
    source = source_all.where(valid_condition)
    row_count = source.count()

    def canonical_component(column):
        value = F.col(column).cast("string")
        return F.when(
            value.isNull(),
            F.lit("N"),
        ).otherwise(
            F.concat(F.lit("V"), F.length(value).cast("string"), F.lit(":"), value)
        )

    keyed = source.select(
        *AKMV_KEY_COLS,
        F.concat_ws(
            "|",
            F.lit("akmv"),
            *[canonical_component(column) for column in AKMV_KEY_COLS],
        ).alias("_akmv_key"),
    )
    hashed = keyed.withColumn(
        "_akmv_hash_hex",
        F.substring(F.sha2("_akmv_key", 256), 1, 16),
    )
    bottom_plus_one = (
        hashed.groupBy("_akmv_hash_hex")
        .agg(F.count("*").alias("occurrences"))
        .orderBy("_akmv_hash_hex")
        .limit(AKMV_K + 1)
        .collect()
    )
    is_exact = len(bottom_plus_one) <= AKMV_K
    bottom = bottom_plus_one[:AKMV_K]
    retained = len(bottom)
    if not retained:
        akmv_distinct_count = 0
    elif is_exact:
        akmv_distinct_count = retained
    else:
        kth_hash = int(bottom[-1]["_akmv_hash_hex"], 16)
        threshold = max(kth_hash / float(1 << 64), 1.0 / float(1 << 64))
        akmv_distinct_count = max(retained, round((AKMV_K - 1) / threshold))
    akmv_distinct_count = min(row_count, akmv_distinct_count)
    distinctness_pct = 100.0 * akmv_distinct_count / row_count if row_count else 0.0
    sampled_duplicate_rows = sum(int(row.occurrences) - 1 for row in bottom)

    # AKMV is deliberately the only composite-key distinctness algorithm in
    # this notebook. No Gold table is read or required here.
    distinctness_check_applied = True
    distinctness_check_passed = (
        distinctness_pct >= AKMV_MIN_DISTINCTNESS_PCT
    )
    passed = bool(
        invalid_key_rows == 0
        and distinctness_check_passed
    )
    failure_reasons = []
    if invalid_key_rows:
        failure_reasons.append("akmv_null_key_rows")
    if distinctness_check_applied and not distinctness_check_passed:
        failure_reasons.append("akmv_distinctness_below_threshold")

    akmv_result = {
        "key_columns": AKMV_KEY_COLS,
        "k": AKMV_K,
        "row_count": row_count,
        "invalid_key_rows": invalid_key_rows,
        "distinct_count": akmv_distinct_count,
        "distinctness_pct": distinctness_pct,
        "sampled_duplicate_rows": sampled_duplicate_rows,
        "min_distinctness_pct": AKMV_MIN_DISTINCTNESS_PCT,
        "distinctness_check_applied": distinctness_check_applied,
        "passed": passed,
        "failure_reasons": failure_reasons,
        "last_version": version,
    }
    spark.createDataFrame([(
        run_id, TABLE, ",".join(AKMV_KEY_COLS), AKMV_K, row_count,
        invalid_key_rows, akmv_distinct_count, distinctness_pct,
        sampled_duplicate_rows, AKMV_MIN_DISTINCTNESS_PCT,
        distinctness_check_applied, passed,
        ",".join(failure_reasons), version,
    )], "run_id string, table_name string, key_columns string, k int, "
       "row_count long, invalid_key_rows long, distinct_count long, "
       "distinctness_pct double, "
       "sampled_duplicate_rows long, min_distinctness_pct double, "
       "distinctness_check_applied boolean, "
       "passed boolean, failure_reasons string, last_version long") \
        .withColumn("evaluated_at_utc", F.current_timestamp()) \
        .write.mode("append").option("mergeSchema", "true").saveAsTable(AKMV_RUNS)
    return akmv_result


version = current_version()
state = load_metric_state()
if REINIT or set(state) != set(COLS) or any(int(r.last_version) > version for r in state.values()):
    init_state(version)
else:
    changed_rows = apply_cdf(version, state)
    print(f"Processed {changed_rows:,} CDF change rows through version {version}")

metrics = load_metric_state()
run_id = str(uuid.uuid4())
akmv_result = compute_akmv(version, run_id)
results = []
for column in COLS:
    row = metrics[column]
    n = int(row.row_count)
    nulls = int(row.null_count)
    distinct = int(row.distinct_count)
    null_pct = 100.0 * nulls / n if n else 0.0
    distinctness_pct = 100.0 * distinct / n if n else 0.0
    rule = RULES.get(column, {})
    max_null_pct = float(rule["max_null_pct"]) if column in NULL_COLS else None
    min_distinctness_pct = float(rule.get("min_distinctness_pct", 0)) if column in DISTINCT_COLS else None
    null_passed = max_null_pct is None or null_pct <= max_null_pct
    distinct_passed = min_distinctness_pct is None or distinctness_pct >= min_distinctness_pct
    results.append({
        "column": column, "row_count": n, "null_count": nulls,
        "null_pct": null_pct, "distinct_count": distinct,
        "distinctness_pct": distinctness_pct,
        "max_null_pct": max_null_pct,
        "min_distinctness_pct": min_distinctness_pct,
        "null_check_applied": column in NULL_COLS,
        "distinct_check_applied": column in DISTINCT_COLS,
        "passed": null_passed and distinct_passed,
    })

spark.createDataFrame([
    (run_id, TABLE, r["column"], r["row_count"], r["null_count"], r["null_pct"],
     r["distinct_count"], r["distinctness_pct"], version)
    for r in results
], "run_id string, table_name string, column_name string, row_count long, null_count long, "
   "null_pct double, distinct_count long, distinctness_pct double, last_version long") \
 .withColumn("evaluated_at_utc", F.current_timestamp()).write.mode("append").saveAsTable(RUNS)

failed = [r["column"] for r in results if not r["passed"]]
if not akmv_result["passed"]:
    failed.append("__akmv_composite_key__")
metrics_json = json.dumps({"columns": results, "akmv": akmv_result}, sort_keys=True)
spark.createDataFrame([(
    run_id, TABLE, bool(not failed), ",".join(failed), dbutils.widgets.get("rules_json"), metrics_json, version
)], "run_id string, table_name string, passed boolean, failed_columns string, rules_json string, metrics_json string, last_version long") \
 .withColumn("evaluated_at_utc", F.current_timestamp()).write.mode("append").saveAsTable(GATE)

print("DQ: ")
display(spark.createDataFrame(results))

akmv_display = dict(akmv_result)
akmv_display["key_columns"] = ",".join(akmv_result["key_columns"])
akmv_display["failure_reasons"] = ",".join(akmv_result["failure_reasons"])

print("AKMV: ")
display(spark.createDataFrame([akmv_display]))
print("DQ gate: PASS" if not failed else f"DQ gate: FAIL ({', '.join(failed)})")
print(
    "AKMV: "
    f"distinct={akmv_result['distinct_count']:,}, "
    f"distinctness={akmv_result['distinctness_pct']:.2f}%, "
    f"passed={akmv_result['passed']}"
)

# Optional exact verification and the paper-vs-CDF comparison used in the demo.
if VERIFY:
    full = spark.table(TABLE).agg(
        F.count("*").alias("row_count"),
        *[F.sum(F.col(c).isNull().cast("long")).alias(f"{c}__null_count") for c in COLS],
        *[F.countDistinct(F.col(c)).alias(f"{c}__distinct_count") for c in COLS],
    ).first()
    comparison = []
    old_state_exists = spark.catalog.tableExists(dq("dq_deequ_state"))
    old = (spark.table(dq("dq_deequ_state")).where(F.col("table_name") == TABLE)
           if old_state_exists else None)
    old_map = {r.column_name: r for r in old.collect()} if old is not None else {}
    for r in results:
        c = r["column"]
        comparison.append((c, int(old_map[c].null_count) if c in old_map else None,
            int(full[f"{c}__null_count"]), r["null_count"],
            int(full[f"{c}__distinct_count"]), r["distinct_count"],
            int(full["row_count"]), r["row_count"]))
    display(spark.createDataFrame(comparison,
        "column string, paper_null_count long, full_scan_null_count long, cdf_null_count long, "
        "full_scan_distinct_count long, cdf_distinct_count long, full_scan_row_count long, cdf_row_count long"))


# COMMAND ----------

display(
    spark.table(dq("dq_deequ_state"))
    .where(
        (F.col("table_name") == TEST_TABLE)
        & (F.col("column_name") == "pm25_ugm3")
    )
)