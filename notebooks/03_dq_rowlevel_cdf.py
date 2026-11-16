# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Part 2: row-level incremental null count + row count (Change Data Feed)
# MAGIC When a row is updated in place by MERGE, Change Data Feed (CDF) records its old and new version:
# MAGIC
# MAGIC | `_change_type` | sign |
# MAGIC |---|---|
# MAGIC | `insert`, `update_postimage` | +1 |
# MAGIC | `update_preimage`, `delete` | −1 |
# MAGIC
# MAGIC Each run reads only the change records since the last processed version:
# MAGIC * change in rows  = Σ sign
# MAGIC * change in nulls = Σ sign × isNull(col)
# MAGIC
# MAGIC These are added to the stored state, and the version bookmark moves forward in the same MERGE.
# MAGIC The result stays exact under updates, and the cost depends on the number of changed rows.

# COMMAND ----------

# MAGIC %run ./00_dq_config

# COMMAND ----------

SOURCE_FILE = "03_dq_rowlevel_cdf"

dbutils.widgets.text("columns", "aqi", "Columns to null-count (comma separated)")
dbutils.widgets.dropdown("reinit", "false", ["false", "true"], "Rebuild state with a full scan")
dbutils.widgets.dropdown("verify", "true", ["true", "false"], "Assert against a full scan")

COLS   = [c.strip() for c in dbutils.widgets.get("columns").split(",") if c.strip()]
REINIT = dbutils.widgets.get("reinit") == "true"
VERIFY = dbutils.widgets.get("verify") == "true"
TABLE  = TEST_TABLE
STATE  = dq("dq_cdf_state")

# COMMAND ----------

# 1) Enable CDF once on the TEST copy (checking first avoids a new version every run)
props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {TABLE}").collect()}
if props.get("delta.enableChangeDataFeed", "false").lower() != "true":
    spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    store_log(spark, "INFO", SOURCE_FILE, f"Enabled Change Data Feed on {TABLE}.")
    REINIT = True   # nothing before this point was recorded

# 2) State table
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {STATE} (
    table_name STRING, column_name STRING,
    null_count BIGINT, row_count BIGINT,
    last_version BIGINT, updated_at_utc TIMESTAMP
  ) USING DELTA
""")

# COMMAND ----------

def current_version(table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]

def is_null(c):
    return F.col(c).isNull().cast("int")

def load_state():
    rows = (spark.table(STATE)
                 .where((F.col("table_name") == TABLE) & F.col("column_name").isin(COLS))
                 .collect())
    return {r.column_name: r for r in rows}

def save_state(rows):
    """Counts and bookmark for all columns in ONE merge, so they always move together."""
    df = (spark.createDataFrame(
              rows, "table_name string, column_name string, null_count long, row_count long, last_version long")
          .withColumn("updated_at_utc", F.current_timestamp()))
    (DeltaTable.forName(spark, STATE).alias("s")
        .merge(df.alias("n"), "s.table_name = n.table_name AND s.column_name = n.column_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

def init_state():
    """One full scan pinned to a table version."""
    started = time.time()
    v = current_version(TABLE)
    agg = (spark.read.option("versionAsOf", v).table(TABLE)
           .agg(F.count("*").alias("_rows"),
                *[F.coalesce(F.sum(is_null(c)), F.lit(0)).alias(c) for c in COLS]).first())
    save_state([(TABLE, c, int(agg[c]), int(agg["_rows"]), v) for c in COLS])
    log_step("dq_cdf_init_full_scan", "dq", agg["_rows"], started, {"version": v, "columns": COLS})

def update_incremental():
    started = time.time()
    state = load_state()
    end = current_version(TABLE)

    # Missing state, or table recreated (its version is behind our bookmark): full scan
    if set(state) != set(COLS) or any(r.last_version > end for r in state.values()):
        store_log(spark, "WARNING", SOURCE_FILE, "CDF state is missing or out of step; rebuilding with a full scan.")
        return init_state()

    start = min(r.last_version for r in state.values()) + 1
    if start > end:
        store_log(spark, "INFO", SOURCE_FILE, f"No new table versions since {start - 1}; nothing to do.")
        log_step("dq_cdf_incremental", "dq", 0, started, {"from_version": start, "to_version": end})
        return

    try:
        changes = (spark.read.option("readChangeFeed", "true")
                        .option("startingVersion", start)
                        .option("endingVersion", end)
                        .table(TABLE))
        sign = (F.when(F.col("_change_type").isin("insert", "update_postimage"), 1)
                 .when(F.col("_change_type").isin("update_preimage", "delete"), -1)
                 .otherwise(0))
        d = changes.agg(
            F.count("*").alias("_cdf_rows"),
            F.coalesce(F.sum(sign), F.lit(0)).alias("_d_rows"),
            *[F.coalesce(F.sum(sign * is_null(c)), F.lit(0)).alias(c) for c in COLS]
        ).first()
    except Exception as exc:   # e.g. change files removed by VACUUM
        store_log(
            spark,
            "WARNING",
            SOURCE_FILE,
            f"Could not read CDF versions {start}..{end}; rebuilding with a full scan: {exc}",
        )
        return init_state()

    save_state([(TABLE, c,
                 int(state[c].null_count) + int(d[c]),
                 int(state[c].row_count) + int(d["_d_rows"]),
                 end) for c in COLS])
    log_step("dq_cdf_incremental", "dq", d["_cdf_rows"], started,
             {"from_version": start, "to_version": end, "d_rows": int(d["_d_rows"]),
              "d_nulls": {c: int(d[c]) for c in COLS}})

# COMMAND ----------

# 3) Run
if REINIT:
    init_state()
else:
    update_incremental()

state_rows = spark.table(STATE).where(F.col("table_name") == TABLE).count()
store_log(spark, "INFO", SOURCE_FILE, f"Stored CDF state for {state_rows} column(s).")

# COMMAND ----------

# 4) Must match a full scan exactly
if VERIFY:
    started = time.time()
    full = spark.table(TABLE).agg(
        F.count("*").alias("_rows"),
        *[F.coalesce(F.sum(is_null(c)), F.lit(0)).alias(c) for c in COLS]).first()
    log_step("dq_full_scan", "dq", full["_rows"], started, {"columns": COLS})

    state = load_state()
    result = [(c, int(state[c].row_count), int(full["_rows"]),
                  int(state[c].null_count), int(full[c]),
                  int(state[c].row_count) == int(full["_rows"]) and int(state[c].null_count) == int(full[c]))
              for c in COLS]
    mismatch_count = sum(1 for row in result if not row[-1])
    store_log(
        spark,
        "INFO" if mismatch_count == 0 else "ERROR",
        SOURCE_FILE,
        f"Full-scan verification completed: columns={len(result)}, mismatches={mismatch_count}.",
    )
    assert all(r[-1] for r in result), "Row-level counts do not match the full scan"

# COMMAND ----------

