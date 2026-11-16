# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Part 1: Deequ-style incremental null count + row count
# MAGIC Mimics the incremental logic in Schelter et al. (VLDB 2018), the paper behind Deequ / PyDeequ:
# MAGIC
# MAGIC * Keep a small **state** per metric: `null_count`, `row_count`.
# MAGIC * Each run, read only rows that arrived since the last run (watermark on `_ingested_at_utc`).
# MAGIC * Compute the same state for those rows and **add** it to the stored state. Old rows are never reread.
# MAGIC
# MAGIC This is correct for **append-only** data. When a row is **updated in place**, it comes back with a newer
# MAGIC `_ingested_at_utc` and is counted **again**, while its old version is never subtracted.
# MAGIC The verify cell shows that drift against a full scan.

# COMMAND ----------

# MAGIC %run ./00_dq_config

# COMMAND ----------

SOURCE_FILE = "02_dq_deequ_style"

dbutils.widgets.text("columns", "aqi", "Columns to null-count (comma separated)")
dbutils.widgets.dropdown("reset", "false", ["false", "true"], "Clear state and start again")
dbutils.widgets.dropdown("verify", "true", ["true", "false"], "Compare with a full scan")

COLS   = [c.strip() for c in dbutils.widgets.get("columns").split(",") if c.strip()]
RESET  = dbutils.widgets.get("reset") == "true"
VERIFY = dbutils.widgets.get("verify") == "true"
TABLE  = TEST_TABLE
STATE  = dq("dq_deequ_state")
WATERMARK_COL = "_ingested_at_utc"

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {STATE} (
    table_name STRING, column_name STRING,
    null_count BIGINT, row_count BIGINT,
    last_ingested_at_utc TIMESTAMP, updated_at_utc TIMESTAMP
  ) USING DELTA
""")
if RESET:
    spark.sql(f"DELETE FROM {STATE} WHERE table_name = '{TABLE}'")

# COMMAND ----------

def is_null(c):
    return F.col(c).isNull().cast("int")

def load_state():
    rows = (spark.table(STATE)
                 .where((F.col("table_name") == TABLE) & F.col("column_name").isin(COLS))
                 .collect())
    return {r.column_name: r for r in rows}

def save_state(rows):
    """Counts and watermark for all columns in ONE merge, so they always move together."""
    df = (spark.createDataFrame(
              rows, "table_name string, column_name string, null_count long, row_count long, last_ingested_at_utc timestamp")
          .withColumn("updated_at_utc", F.current_timestamp()))
    (DeltaTable.forName(spark, STATE).alias("s")
        .merge(df.alias("n"), "s.table_name = n.table_name AND s.column_name = n.column_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

started = time.time()
state = load_state()
if set(state) != set(COLS):
    state = {}   # column list changed: start every column from zero together

wm = min((r.last_ingested_at_utc for r in state.values()), default=None)

# 1) Only the new increment
increment = spark.table(TABLE)
if wm is not None:
    increment = increment.where(F.col(WATERMARK_COL) > F.lit(wm))

# 2) State of the increment
inc = increment.agg(
    F.count("*").alias("_rows"),
    F.max(WATERMARK_COL).alias("_max_wm"),
    *[F.coalesce(F.sum(is_null(c)), F.lit(0)).alias(c) for c in COLS]
).first()

# 3) Merge = add increment state to stored state
if inc["_rows"] == 0:
    store_log(spark, "INFO", SOURCE_FILE, "No new rows since the last run.")
else:
    save_state([
        (TABLE, c,
         (int(state[c].null_count) if c in state else 0) + int(inc[c]),
         (int(state[c].row_count) if c in state else 0) + int(inc["_rows"]),
         inc["_max_wm"])
        for c in COLS
    ])
    store_log(
        spark,
        "INFO",
        SOURCE_FILE,
        f"Added increment of {inc['_rows']:,} rows: "
        + ", ".join(f"{c} nulls +{inc[c]}" for c in COLS),
    )

log_step("dq_deequ_incremental", "dq", inc["_rows"], started, {"columns": COLS})
state_rows = spark.table(STATE).where(F.col("table_name") == TABLE).count()
store_log(spark, "INFO", SOURCE_FILE, f"Stored incremental DQ state for {state_rows} column(s).")

# COMMAND ----------

# 4) Compare with a full scan (expect drift after in-place updates)
if VERIFY:
    started = time.time()
    full = spark.table(TABLE).agg(
        F.count("*").alias("_rows"),
        *[F.coalesce(F.sum(is_null(c)), F.lit(0)).alias(c) for c in COLS]).first()
    log_step("dq_full_scan", "dq", full["_rows"], started, {"columns": COLS})

    state = load_state()
    result = [(c,
               int(state[c].row_count), int(full["_rows"]), int(state[c].row_count) - int(full["_rows"]),
               int(state[c].null_count), int(full[c]), int(state[c].null_count) - int(full[c]))
              for c in COLS]
    drift_count = sum(1 for row in result if row[3] != 0 or row[6] != 0)
    store_log(
        spark,
        "INFO" if drift_count == 0 else "WARNING",
        SOURCE_FILE,
        f"Full-scan verification completed: columns={len(result)}, drifted_columns={drift_count}.",
    )
