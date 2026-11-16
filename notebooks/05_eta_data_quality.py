# Databricks notebook source
# MAGIC %md
# MAGIC # Silver EPA data-quality table
# MAGIC
# MAGIC This notebook creates or updates `workspace.wildfire.silver_qa_epa`.
# MAGIC
# MAGIC The target contains one row per `date_local` and includes:
# MAGIC
# MAGIC - source row count
# MAGIC - total null count across EPA data columns
# MAGIC - one `null_<column>` count for each EPA data column
# MAGIC - null rate and completeness
# MAGIC - source-change metadata used for incremental updates
# MAGIC
# MAGIC The notebook recomputes only new or changed `date_local` values. A change to `date_of_last_change`, `_content_hash`, or the number of rows causes that date to be merged into the Silver table again.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from logging_utils import store_log

SOURCE_FILE = "05_eta_data_quality"
SOURCE_TABLE = "workspace.wildfire.bronze_epa_pm25"
TARGET_TABLE = "workspace.wildfire.silver_qa_epa"

GROUPING_COLUMN = "date_local"
CHANGE_COLUMN = "date_of_last_change"
ROW_HASH_COLUMN = "_content_hash"

# The default is to check EPA data columns only and exclude ingestion metadata.
INCLUDE_METADATA_COLUMNS = False

# Keep this False to count SQL NULL values only. Set it to True if empty strings
# should also be treated as missing values.
COUNT_EMPTY_STRINGS_AS_NULL = False

# COMMAND ----------

source_df = spark.table(SOURCE_TABLE)

required_columns = [GROUPING_COLUMN, CHANGE_COLUMN, ROW_HASH_COLUMN]
missing_columns = [
    column_name
    for column_name in required_columns
    if column_name not in source_df.columns
]

if missing_columns:
    raise ValueError(
        f"The source table is missing required columns: {missing_columns}"
    )

source_types = dict(source_df.dtypes)

qa_columns = [
    column_name
    for column_name in source_df.columns
    if (INCLUDE_METADATA_COLUMNS or not column_name.startswith("_"))
    and column_name != GROUPING_COLUMN
]

if not qa_columns:
    raise ValueError("No columns were selected for the null analysis.")

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    f"Analyzing {SOURCE_TABLE} into {TARGET_TABLE}; columns_checked={len(qa_columns)}.",
)

# COMMAND ----------

def missing_condition(column_name):
    """Return a Spark expression identifying missing values."""
    condition = F.col(column_name).isNull()

    if (
        COUNT_EMPTY_STRINGS_AS_NULL
        and source_types[column_name] == "string"
    ):
        condition = condition | (F.trim(F.col(column_name)) == "")

    return condition


# Normalize the grouping key while retaining it as a string.
# Empty date_local values are excluded because they cannot represent a valid
# per-date QA record.
working_df = (
    source_df
    .withColumn(
        "__date_local",
        F.when(
            F.col(GROUPING_COLUMN).isNull()
            | (F.trim(F.col(GROUPING_COLUMN).cast("string")) == ""),
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col(GROUPING_COLUMN).cast("string"))
        )
    )
)

missing_date_count = (
    working_df
    .filter(F.col("__date_local").isNull())
    .limit(1)
    .count()
)

if missing_date_count:
    store_log(spark, "WARNING", SOURCE_FILE, "Rows with null or empty date_local will be excluded.")

working_df = working_df.filter(F.col("__date_local").isNotNull())

# This token changes if date_of_last_change or the row content hash changes.
# Sorting all tokens within each date makes the aggregate deterministic.
working_df = working_df.withColumn(
    "__change_token",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col(CHANGE_COLUMN).cast("string"), F.lit("<NULL>")),
            F.coalesce(F.col(ROW_HASH_COLUMN).cast("string"), F.lit("<NULL>"))
        ),
        256
    )
)

# COMMAND ----------

aggregation_expressions = [
    F.count(F.lit(1)).cast("long").alias("row_count"),
    F.max(F.col(CHANGE_COLUMN).cast("string")).alias(
        "source_max_date_of_last_change"
    ),
    F.sha2(
        F.array_join(
            F.sort_array(F.collect_list(F.col("__change_token"))),
            "||"
        ),
        256
    ).alias("source_change_hash")
]

null_count_aliases = []
total_missing_expression = None

for index, column_name in enumerate(qa_columns):
    condition = missing_condition(column_name)
    missing_indicator = F.when(condition, 1).otherwise(0)
    null_alias = f"__null_count_{index}"

    aggregation_expressions.append(
        F.sum(missing_indicator)
        .cast("long")
        .alias(null_alias)
    )
    null_count_aliases.append((column_name, null_alias))

    if total_missing_expression is None:
        total_missing_expression = missing_indicator
    else:
        total_missing_expression = (
            total_missing_expression + missing_indicator
        )

aggregation_expressions.append(
    F.sum(total_missing_expression)
    .cast("long")
    .alias("total_null_count")
)

aggregated_df = (
    working_df
    .groupBy("__date_local")
    .agg(*aggregation_expressions)
)

output_expressions = [
    F.col("__date_local").alias("date_local"),
    F.col("row_count"),
    F.col("total_null_count"),
    F.col("source_max_date_of_last_change"),
    F.col("source_change_hash")
]

for column_name, null_alias in null_count_aliases:
    output_expressions.append(
        F.col(null_alias).alias(f"null_{column_name}")
    )

source_aggregate_df = aggregated_df.select(*output_expressions)

# COMMAND ----------

target_exists = spark.catalog.tableExists(TARGET_TABLE)

if target_exists:
    target_keys_df = (
        spark.table(TARGET_TABLE)
        .select(
            "date_local",
            "row_count",
            "source_change_hash"
        )
    )

    comparison_df = (
        source_aggregate_df.alias("s")
        .join(
            target_keys_df.alias("t"),
            F.col("s.date_local") == F.col("t.date_local"),
            "left"
        )
    )

    affected_dates_df = (
        comparison_df
        .filter(
            F.col("t.date_local").isNull()
            | (
                F.coalesce(F.col("s.source_change_hash"), F.lit(""))
                != F.coalesce(F.col("t.source_change_hash"), F.lit(""))
            )
            | (
                F.coalesce(F.col("s.row_count"), F.lit(-1))
                != F.coalesce(F.col("t.row_count"), F.lit(-1))
            )
        )
        .select(F.col("s.date_local").alias("date_local"))
        .distinct()
    )
else:
    affected_dates_df = source_aggregate_df.select("date_local")

affected_aggregate_df = (
    source_aggregate_df
    .join(affected_dates_df, on="date_local", how="inner")
    .withColumn("updated_at_utc", F.current_timestamp())
)

affected_count = affected_dates_df.count()
store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    f"Identified {affected_count} new or changed date_local value(s); target_exists={target_exists}.",
)

# COMMAND ----------

if affected_count == 0:
    if not target_exists:
        (
            source_aggregate_df
            .withColumn("updated_at_utc", F.current_timestamp())
            .limit(0)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(TARGET_TABLE)
        )
        store_log(spark, "INFO", SOURCE_FILE, f"Created empty {TARGET_TABLE}; no source rows were available.")
    else:
        store_log(spark, "INFO", SOURCE_FILE, "No new or changed date_local values; target table is current.")
else:
    if not target_exists:
        (
            affected_aggregate_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(TARGET_TABLE)
        )
        store_log(spark, "INFO", SOURCE_FILE, f"Created {TARGET_TABLE} with {affected_count} affected date(s).")
    else:
        delta_target = DeltaTable.forName(spark, TARGET_TABLE)

        (
            delta_target.alias("t")
            .merge(
                affected_aggregate_df.alias("s"),
                "t.date_local = s.date_local"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        store_log(spark, "INFO", SOURCE_FILE, f"Updated {TARGET_TABLE} for {affected_count} affected date(s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily execution
# MAGIC
# MAGIC Run this notebook after the Bronze ingestion task. On the first run, it creates `silver_qa_epa`. On later runs, it compares each source date with the stored `source_change_hash` and `row_count`.
# MAGIC
# MAGIC The hash includes both `date_of_last_change` and `_content_hash`, so a changed EPA row is recomputed even when its date is not the newest date in the table. Unchanged dates are not rewritten.
