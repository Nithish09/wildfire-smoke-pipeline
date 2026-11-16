"""Reusable Silver-layer helpers for batch and Structured Streaming notebooks.

The helpers accept a Spark session and fully-qualified table names so they work
from a `%run` notebook as well as from a normal Python import. They contain no
actions until the caller invokes them.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable


def split_valid(df, rules, source, key_cols):
    """Return (valid_rows, quarantined_rows), preserving the first failed rule."""
    reason = F.lit(None).cast("string")
    for name, condition in reversed(rules):
        reason = F.when(~F.coalesce(condition, F.lit(False)), F.lit(name)).otherwise(reason)
    tagged = df.withColumn("_dq_reason", reason)
    bad = (
        tagged.where(F.col("_dq_reason").isNotNull())
        .select(
            F.lit(source).alias("source"),
            F.col("_dq_reason").alias("reason"),
            F.concat_ws("|", *[
                F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_cols
            ]).alias("record_key"),
            F.to_json(F.struct(*[
                c for c in df.columns if not c.startswith("_")
            ])).alias("record_json"),
            "_source_file", "_ingested_at_utc", "_batch_id",
        )
    )
    return tagged.where(F.col("_dq_reason").isNull()).drop("_dq_reason"), bad


def write_quarantine(spark, quarantine_table, bad):
    """Idempotently merge failed rows into the quarantine table."""
    if bad.limit(1).count() == 0:
        return
    target = DeltaTable.forName(spark, quarantine_table)
    source = bad.dropDuplicates(["source", "record_key", "reason", "_source_file"])
    condition = (
        "q.source = b.source AND q.record_key = b.record_key "
        "AND q.reason = b.reason AND q._source_file = b._source_file"
    )
    target.alias("q").merge(source.alias("b"), condition).whenNotMatchedInsertAll().execute()


def latest_per_key(df, key_cols, order_cols):
    order = [F.col(c).desc_nulls_last() if isinstance(c, str) else c for c in order_cols]
    window = Window.partitionBy(*key_cols).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def merge_into(spark, table_name, df, key_cols, newer_condition):
    """Create or MERGE a Delta table using a null-safe business key."""
    if df.limit(1).count() == 0:
        return
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").saveAsTable(table_name)
        return
    condition = " AND ".join([f"s.{c} <=> n.{c}" for c in key_cols])
    (
        DeltaTable.forName(spark, table_name).alias("s")
        .merge(df.alias("n"), condition)
        .whenMatchedUpdateAll(condition=newer_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def queue_dates(spark, pending_table, df, source):
    if df.limit(1).count() == 0:
        return
    (
        df.select("date_local").where(F.col("date_local").isNotNull()).distinct()
        .withColumn("source", F.lit(source))
        .withColumn("queued_at_utc", F.current_timestamp())
        .write.mode("append").saveAsTable(pending_table)
    )


def haversine_km(lat1, lon1, lat2, lon2):
    radius = F.lit(6371.0)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.pow(F.sin(dlat / 2), 2)
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    )
    return 2 * radius * F.asin(F.sqrt(a))


def assign_county(spark, counties_df, df, lat_col, lon_col, key_cols):
    """Assign the nearest Census internal point, matching the batch pipeline."""
    joined = (
        df.crossJoin(counties_df.select("county_fips", "c_lat", "c_lon"))
        .withColumn("_distance", haversine_km(
            F.col(lat_col), F.col(lon_col), F.col("c_lat"), F.col("c_lon")
        ))
    )
    window = Window.partitionBy(*key_cols).orderBy("_distance")
    return (
        joined.withColumn("_rn", F.row_number().over(window)).where("_rn = 1")
        .withColumnRenamed("_distance", "county_distance_km")
        .drop("_rn", "c_lat", "c_lon")
    )
