# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03s - Silver Structured Streaming with rolling monthly Bloom filters
# MAGIC
# MAGIC Reads the three Bronze Delta tables, applies the existing Silver cleaning
# MAGIC and MERGE rules, and routes incoming rows through persisted monthly Bloom
# MAGIC filters. Bloom-negative rows are treated as definitely new and appended;
# MAGIC Bloom-positive rows go through the exact existing-key check and the original
# MAGIC source-specific Delta MERGE rules.

# COMMAND ----------

"""Complete replacement for 03s_silver_stream.py.

The source-specific cleaning, validation, quarantine, DGIM, queueing, and Delta
MERGE rules are preserved from the original notebook. The Bloom implementation
uses the existing BloomFilter class from streaming_algorithms.py, which uses
SHA-256 double hashing.
"""

# COMMAND ----------

from datetime import datetime, timezone
import base64
import json
import os
import pickle
from zoneinfo import ZoneInfo

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T
from logging_utils import store_log

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from lib_silver import (
    assign_county,
    latest_per_key,
    merge_into,
    queue_dates,
    split_valid,
    write_quarantine,
)
from streaming_algorithms import (
    BloomFilter,
    canonical_key,
    dgim_expire,
    dgim_query,
    dgim_update,
)

SOURCE_FILE = "03s_silver_stream"


def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)


# COMMAND ----------

dbutils.widgets.text(
    "checkpoint_root",
    f"{RAW_ROOT}/_checkpoints/silver_stream_all_sources_v2",
    "Silver checkpoint root",
)
dbutils.widgets.text("bloom_false_positive_rate", "0.005", "Bloom target FPR")
dbutils.widgets.text("bloom_retention_months", "3", "Bloom rolling retention months")
dbutils.widgets.text("bloom_monthly_capacity", "500000", "Bloom capacity per source-month")
dbutils.widgets.text("dgim_window_hours", "24", "DGIM event-time window")
dbutils.widgets.text("dgim_max_buckets", "4", "DGIM buckets per size")
dbutils.widgets.text("epa_pm25_alert_threshold", "35", "EPA PM2.5 alert threshold")

CHECKPOINT_ROOT = dbutils.widgets.get("checkpoint_root").rstrip("/")
BLOOM_FPR = float(dbutils.widgets.get("bloom_false_positive_rate"))
BLOOM_RETENTION_MONTHS = int(dbutils.widgets.get("bloom_retention_months"))
BLOOM_MONTHLY_CAPACITY = int(dbutils.widgets.get("bloom_monthly_capacity"))
DGIM_WINDOW_SECONDS = int(float(dbutils.widgets.get("dgim_window_hours")) * 3600)
DGIM_MAX_BUCKETS = int(dbutils.widgets.get("dgim_max_buckets"))
EPA_PM25_ALERT_THRESHOLD = float(dbutils.widgets.get("epa_pm25_alert_threshold"))

if not 0 < BLOOM_FPR < 1:
    raise ValueError("bloom_false_positive_rate must be between 0 and 1")
if BLOOM_RETENTION_MONTHS < 1:
    raise ValueError("bloom_retention_months must be at least 1")
if BLOOM_MONTHLY_CAPACITY < 1:
    raise ValueError("bloom_monthly_capacity must be at least 1")
if DGIM_WINDOW_SECONDS < 1 or DGIM_MAX_BUCKETS < 2:
    raise ValueError("invalid DGIM settings")

FIRE_KEY = ["latitude", "longitude", "acq_ts_utc", "satellite"]
EPA_KEY = ["site_id", "poc", "date_local", "sample_duration"]
NOAA_KEY = ["station", "date_local", "element"]

write_notebook_log(
    "INFO",
    f"Starting Silver streams: bloom_fpr={BLOOM_FPR}, "
    f"bloom_retention_months={BLOOM_RETENTION_MONTHS}, "
    f"bloom_monthly_capacity={BLOOM_MONTHLY_CAPACITY}, "
    f"dgim_window_hours={DGIM_WINDOW_SECONDS / 3600:g}, "
    f"dgim_max_buckets={DGIM_MAX_BUCKETS}.",
)

# COMMAND ----------

# Required state/metrics tables are created by 08_streaming_algorithm_tables.sql.
for required_table in [
    "ops_stream_metrics",
    "ops_stream_state",
    "v_ops_stream_metrics_latest",
]:
    if not spark.catalog.tableExists(t(required_table)):
        raise ValueError(
            f"{t(required_table)} is missing. "
            "Run 08_streaming_algorithm_tables.sql before starting Silver streaming."
        )

for sql in [
    f"""CREATE TABLE IF NOT EXISTS {t('silver_quarantine')} (
      source STRING, reason STRING, record_key STRING, record_json STRING,
      _source_file STRING, _ingested_at_utc TIMESTAMP, _batch_id STRING)
      USING DELTA""",
    f"""CREATE TABLE IF NOT EXISTS {t('ops_gold_pending_dates')} (
      date_local DATE, source STRING, queued_at_utc TIMESTAMP)
      USING DELTA""",
]:
    spark.sql(sql)


def write_metrics(source, batch_id, algorithm, values, details=None):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    details_json = json.dumps(details or {}, sort_keys=True)
    rows = [
        (
            now,
            source,
            str(batch_id),
            algorithm,
            name,
            float(value) if value is not None else None,
            details_json,
        )
        for name, value in values.items()
    ]
    (spark.createDataFrame(
        rows,
        "metric_ts_utc timestamp, source string, batch_id string, algorithm string, "
        "metric_name string, metric_value double, details_json string",
    ).write.mode("append").saveAsTable(t("ops_stream_metrics")))


def ensure_county_dimension():
    if spark.catalog.tableExists(t("silver_dim_county")):
        return
    counties = (spark.table(t("bronze_ref_counties"))
        .where(F.col("USPS") == "CA")
        .select(
            F.col("GEOID").alias("county_fips"),
            F.regexp_replace("NAME", " County$", "").alias("county_name"),
            F.col("INTPTLAT").cast("double").alias("c_lat"),
            F.col("INTPTLONG").cast("double").alias("c_lon"),
            F.col("ALAND_SQMI").cast("double").alias("land_sqmi"),
        ))
    counties.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_dim_county"))


ensure_county_dimension()

if (spark.catalog.tableExists(t("silver_fire_detections"))
        and "_firms_product" not in spark.table(t("silver_fire_detections")).columns):
    spark.sql(f"ALTER TABLE {t('silver_fire_detections')} ADD COLUMNS (_firms_product STRING)")

# COMMAND ----------

# Rolling monthly Bloom state.
BLOOM_ALGORITHM = "bloom_filter"


def month_shift(month_text, delta):
    year, month = map(int, month_text.split("-"))
    absolute = year * 12 + month - 1 + delta
    return f"{absolute // 12:04d}-{absolute % 12 + 1:02d}"


def active_months():
    current = datetime.now(ZoneInfo(LOCAL_TZ)).strftime("%Y-%m")
    return [month_shift(current, -i) for i in range(BLOOM_RETENTION_MONTHS)]


def bloom_state_key(source, month_text):
    return f"bloom_{source.replace('-', '_')}_{month_text.replace('-', '_')}"


def encode_bloom_state(source, month_text, bloom, key_count):
    return json.dumps({
        "format": 1,
        "source": source,
        "month": month_text,
        "capacity": BLOOM_MONTHLY_CAPACITY,
        "false_positive_rate": BLOOM_FPR,
        "key_count": int(key_count),
        "pickle_b64": base64.b64encode(
            pickle.dumps(bloom, protocol=pickle.HIGHEST_PROTOCOL)
        ).decode("ascii"),
    }, sort_keys=True)


def decode_bloom_state(payload):
    value = json.loads(payload)
    if value.get("format") != 1:
        raise ValueError("Unsupported Bloom state format")
    bloom = pickle.loads(base64.b64decode(value["pickle_b64"]))
    return bloom, int(value.get("key_count", 0))


def load_bloom_states(source, months):
    keys = {bloom_state_key(source, month): month for month in months}
    rows = (spark.table(t("ops_stream_state"))
        .where(F.col("algorithm") == BLOOM_ALGORITHM)
        .where(F.col("state_key").isin(list(keys)))
        .orderBy(F.col("updated_at_utc").desc())
        .collect())

    states = {}
    for row in rows:
        month_text = keys[row.state_key]
        if month_text in states:
            continue
        bloom, key_count = decode_bloom_state(row.buckets_json)
        states[month_text] = {
            "state_key": row.state_key,
            "bloom": bloom,
            "key_count": key_count,
        }
    return states


def bootstrap_bloom_state(source, month_text, silver_table, key_columns, key_source):
    bloom = BloomFilter(BLOOM_MONTHLY_CAPACITY, BLOOM_FPR)
    key_count = 0

    if spark.catalog.tableExists(silver_table):
        existing = (spark.table(silver_table)
            .where(F.date_format(F.col("date_local"), "yyyy-MM") == month_text)
            .select(*key_columns)
            .dropDuplicates(key_columns))
        for row in existing.toLocalIterator():
            bloom.add(canonical_key(row.asDict(recursive=True), key_source))
            key_count += 1

    return {
        "state_key": bloom_state_key(source, month_text),
        "bloom": bloom,
        "key_count": key_count,
    }


def save_bloom_states(source, states):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for month_text, state in states.items():
        rows.append((
            state["state_key"],
            BLOOM_ALGORITHM,
            encode_bloom_state(
                source,
                month_text,
                state["bloom"],
                state["key_count"],
            ),
            0,
            0,
            0,
            now,
        ))

    if not rows:
        return

    incoming = spark.createDataFrame(rows, (
        "state_key string, algorithm string, buckets_json string, "
        "last_event_ts long, window_seconds long, max_buckets int, "
        "updated_at_utc timestamp"
    ))
    (DeltaTable.forName(spark, t("ops_stream_state")).alias("target")
        .merge(incoming.alias("source"), "target.state_key = source.state_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def delete_old_bloom_states(source, months):
    keep = [bloom_state_key(source, month) for month in months]
    keep_sql = ",".join(repr(value) for value in keep)
    source_prefix = f"bloom_{source.replace('-', '_')}_%"
    spark.sql(
        f"DELETE FROM {t('ops_stream_state')} "
        f"WHERE algorithm = '{BLOOM_ALGORITHM}' "
        f"AND state_key LIKE '{source_prefix}' "
        f"AND state_key NOT IN ({keep_sql})"
    )


def null_safe_key_condition(left_alias, right_alias, key_columns):
    condition = F.lit(True)
    for column in key_columns:
        condition = condition & F.col(f"{left_alias}.{column}").eqNullSafe(
            F.col(f"{right_alias}.{column}")
        )
    return condition


def route_bloom(incoming_df, source, key_source, silver_table, key_columns, batch_id):
    """Split a deduplicated normalized batch into Bloom-new and Bloom-maybe rows."""
    months = active_months()
    states = load_bloom_states(source, months)
    for month_text in months:
        if month_text not in states:
            states[month_text] = bootstrap_bloom_state(
                source, month_text, silver_table, key_columns, key_source
            )

    key_udf = F.udf(
        lambda *values: canonical_key(
            dict(zip(key_columns, values)), key_source
        ),
        T.StringType(),
    )

    routed = (incoming_df
        .withColumn("__bloom_key", key_udf(*[F.col(c) for c in key_columns]))
        .withColumn("__bloom_month", F.date_format(F.col("date_local"), "yyyy-MM")))

    # The incoming batch is small relative to Silver, so membership is checked
    # in Python while Spark retains the actual rows in DataFrames.
    route_pairs = {}
    incoming_pairs = []
    for row in routed.select("__bloom_key", "__bloom_month").distinct().collect():
        key = row["__bloom_key"]
        month_text = row["__bloom_month"]
        definitely_new = (
            month_text in states
            and key not in states[month_text]["bloom"]
        )
        route_pairs[(key, month_text)] = "definitely_new" if definitely_new else "maybe_existing"
        incoming_pairs.append((key, month_text))

    route_schema = T.StructType([
        T.StructField("__bloom_key", T.StringType(), False),
        T.StructField("__bloom_month", T.StringType(), True),
        T.StructField("__bloom_route", T.StringType(), False),
    ])
    route_rows = [
        (key, month_text, route_pairs[(key, month_text)])
        for key, month_text in route_pairs
    ]
    route_df = spark.createDataFrame(route_rows, route_schema)
    routed = routed.join(route_df, ["__bloom_key", "__bloom_month"], "left")

    definitely_new_df = (routed
        .where(F.col("__bloom_route") == "definitely_new")
        .drop("__bloom_key", "__bloom_month", "__bloom_route"))
    maybe_existing_df = (routed
        .where(F.col("__bloom_route") == "maybe_existing")
        .drop("__bloom_key", "__bloom_month", "__bloom_route"))

    incoming_rows = incoming_df.count()
    candidate_duplicate_hits = len({
        pair[0] for pair, route in route_pairs.items()
        if route == "maybe_existing"
    })

    if spark.catalog.tableExists(silver_table) and candidate_duplicate_hits:
        existing = spark.table(silver_table).select(*key_columns)
        condition = null_safe_key_condition("n", "s", key_columns)
        exact_existing = (maybe_existing_df.alias("n")
            .join(existing.alias("s"), condition, "inner")
            .select("n.*"))
        exact_new_from_maybe = (maybe_existing_df.alias("n")
            .join(existing.alias("s"), condition, "left_anti")
            .select("n.*"))
    else:
        exact_existing = maybe_existing_df.limit(0)
        exact_new_from_maybe = maybe_existing_df

    # This is the exact same semantic input previously passed to DGIM:
    # rows whose business key was not already present in Silver.
    # Materialize before the append/MERGE actions. Otherwise Spark could
    # reevaluate the lazy anti-join after the candidate MERGE and see those
    # rows as existing, producing an empty DGIM input.
    new_rows = definitely_new_df.unionByName(exact_new_from_maybe).cache()
    new_rows.count()

    exact_existing_keys = {
        canonical_key(row.asDict(recursive=True), key_source)
        for row in exact_existing.select(*key_columns).toLocalIterator()
    }
    incoming_keys = {
        canonical_key(row.asDict(recursive=True), key_source)
        for row in incoming_df.select(*key_columns).toLocalIterator()
    }
    candidate_keys = {
        key for (key, _month), route in route_pairs.items()
        if route == "maybe_existing"
    }
    exact_new_count = len(incoming_keys - exact_existing_keys)
    false_positive_count = len(candidate_keys - exact_existing_keys)

    metrics = {
        "incoming_rows": incoming_rows,
        "existing_silver_keys": sum(state["key_count"] for state in states.values()),
        "candidate_duplicate_hits": candidate_duplicate_hits,
        "exact_existing_key_hits": len(exact_existing_keys),
        "exact_new_rows": exact_new_count,
        "false_positive_count": false_positive_count,
        # A correct Bloom filter has no false negatives. This value is an
        # invariant because Bloom-negative rows are not exact-scanned.
        "false_negative_count": 0,
        "measured_false_positive_rate": false_positive_count / max(exact_new_count, 1),
    }

    # Keep key/month pairs for state updates after the source writes succeed.
    new_key_month_pairs = [
        (row["__bloom_key"], row["__bloom_month"])
        for row in (routed
            .where(F.col("__bloom_route") == "definitely_new")
            .select("__bloom_key", "__bloom_month")
            .distinct()
            .collect())
    ]
    new_key_month_pairs.extend([
        (row["__bloom_key"], row["__bloom_month"])
        for row in (routed
            .where(F.col("__bloom_route") == "maybe_existing")
            .join(
                exact_new_from_maybe.select(*key_columns).dropDuplicates(key_columns),
                key_columns,
                "inner",
            )
            .select("__bloom_key", "__bloom_month")
            .distinct()
            .collect())
    ])

    return {
        "definitely_new_df": definitely_new_df,
        "maybe_existing_df": maybe_existing_df,
        "new_rows": new_rows,
        "states": states,
        "months": months,
        "new_key_month_pairs": list(set(new_key_month_pairs)),
        "incoming_pairs": list(set(incoming_pairs)),
        "metrics": metrics,
    }


def finish_bloom_batch(source, route, batch_id):
    """Add all batch keys to monthly filters and persist them."""
    new_pairs = set(route["new_key_month_pairs"])
    for key, month_text in route["incoming_pairs"]:
        if month_text in route["states"]:
            state = route["states"][month_text]
            was_present = key in state["bloom"]
            state["bloom"].add(key)
            if (key, month_text) in new_pairs and not was_present:
                state["key_count"] += 1

    delete_old_bloom_states(source, route["months"])
    save_bloom_states(source, route["states"])
    write_metrics(
        source,
        batch_id,
        "bloom_filter",
        route["metrics"],
        {
            "configured_false_positive_rate": BLOOM_FPR,
            "monthly_capacity": BLOOM_MONTHLY_CAPACITY,
            "retention_months": BLOOM_RETENTION_MONTHS,
            "hash_algorithm": "SHA-256 double hashing",
            "role": "candidate routing; exact Delta MERGE remains authoritative",
        },
    )


# COMMAND ----------

def load_dgim_state(state_key):
    rows = (spark.table(t("ops_stream_state"))
        .where(F.col("state_key") == state_key)
        .orderBy(F.col("updated_at_utc").desc())
        .limit(1).collect())
    if not rows:
        return None
    row = rows[0]
    return {
        "buckets": [tuple(bucket) for bucket in json.loads(row.buckets_json)],
        "last_event_ts": int(row.last_event_ts),
        "window_seconds": int(row.window_seconds),
        "max_buckets": int(row.max_buckets),
    }


def save_dgim_state(state_key, buckets, last_event_ts):
    state = spark.createDataFrame([(
        state_key,
        "dgim",
        json.dumps(buckets),
        int(last_event_ts),
        int(DGIM_WINDOW_SECONDS),
        int(DGIM_MAX_BUCKETS),
        datetime.utcnow(),
    )], (
        "state_key string, algorithm string, buckets_json string, last_event_ts long, "
        "window_seconds long, max_buckets int, updated_at_utc timestamp"
    ))
    (DeltaTable.forName(spark, t("ops_stream_state")).alias("target")
        .merge(state.alias("source"), "target.state_key = source.state_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def dgim_event_rows(df, timestamp_column, bit_expression, cutoff=None):
    result = (df.select(
        F.unix_timestamp(F.col(timestamp_column).cast("timestamp")).cast("long").alias("event_ts"),
        bit_expression.cast("int").alias("bit"),
    ).where(F.col("event_ts").isNotNull()))
    if cutoff is not None:
        result = result.where(F.col("event_ts") >= F.lit(cutoff))
    return result.orderBy("event_ts").collect()


def update_dgim(new_rows, source, silver_table, state_key, timestamp_column, bit_expression, batch_id):
    if not spark.catalog.tableExists(silver_table):
        return
    target = spark.table(silver_table)
    latest_event_ts = target.select(
        F.max(F.unix_timestamp(F.col(timestamp_column).cast("timestamp"))).alias("latest")
    ).first().latest
    if latest_event_ts is None:
        return
    latest_event_ts = int(latest_event_ts)
    cutoff = latest_event_ts - DGIM_WINDOW_SECONDS

    incoming_events = dgim_event_rows(new_rows, timestamp_column, bit_expression)
    state = load_dgim_state(state_key)
    late_event_count = 0 if state is None else sum(
        int(row.event_ts) < state["last_event_ts"] for row in incoming_events
    )
    config_changed = state is not None and (
        state["window_seconds"] != DGIM_WINDOW_SECONDS
        or state["max_buckets"] != DGIM_MAX_BUCKETS
    )
    state_stale = state is not None and latest_event_ts > state["last_event_ts"] and not incoming_events
    rebuild = state is None or late_event_count > 0 or config_changed or state_stale

    if rebuild:
        buckets = []
        rows_to_apply = dgim_event_rows(target, timestamp_column, bit_expression, cutoff)
    else:
        buckets = list(state["buckets"])
        rows_to_apply = incoming_events

    for row in rows_to_apply:
        dgim_update(buckets, int(row.bit), int(row.event_ts), max_buckets=DGIM_MAX_BUCKETS)
    dgim_expire(buckets, cutoff)

    estimate = dgim_query(buckets, cutoff)
    exact = (target
        .where(F.unix_timestamp(F.col(timestamp_column).cast("timestamp")) >= F.lit(cutoff))
        .where(bit_expression)
        .count())
    relative_error = abs(estimate - exact) / max(exact, 1)

    save_dgim_state(state_key, buckets, latest_event_ts)
    write_metrics(source, batch_id, "dgim", {
        "estimated_alerts_24h": estimate,
        "exact_alerts_24h": exact,
        "absolute_error": abs(estimate - exact),
        "relative_error": relative_error,
        "bucket_count": len(buckets),
        "late_events_in_batch": late_event_count,
        "state_rebuilt": int(rebuild),
    }, {
        "state_key": state_key,
        "window_seconds": DGIM_WINDOW_SECONDS,
        "max_buckets_per_size": DGIM_MAX_BUCKETS,
        "window_ends_at_epoch": latest_event_ts,
    })


# COMMAND ----------

def write_split_and_merge(route, silver_table, key_columns, newer_condition):
    """Append definite-new rows and MERGE only Bloom-positive candidates."""
    if route["definitely_new_df"].limit(1).count() > 0:
        route["definitely_new_df"].write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(silver_table)
    merge_into(
        spark,
        silver_table,
        route["maybe_existing_df"],
        key_columns,
        newer_condition,
    )


def normalize_firms(batch_df):
    columns = set(batch_df.columns)
    brightness = F.col("bright_ti4") if "bright_ti4" in columns else F.col("brightness")
    product = F.col("_firms_product") if "_firms_product" in columns else F.lit("SP")
    confidence_raw = F.lower(F.trim(F.col("confidence")))
    confidence_number = confidence_raw.cast("double")
    fire = (batch_df
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("acq_time_hhmm", F.lpad(F.trim(F.col("acq_time")), 4, "0"))
        .withColumn("acq_ts_utc", F.to_timestamp(
            F.concat_ws(" ", F.col("acq_date"), F.col("acq_time_hhmm")),
            "yyyy-MM-dd HHmm",
        ))
        .withColumn("date_local", F.to_date(F.from_utc_timestamp("acq_ts_utc", LOCAL_TZ)))
        .withColumn("brightness_k", brightness.cast("double"))
        .withColumn("frp_mw", F.col("frp").cast("double"))
        .withColumn("confidence_class",
            F.when(confidence_raw.isin("l", "low"), "low")
             .when(confidence_raw.isin("n", "nominal"), "nominal")
             .when(confidence_raw.isin("h", "high"), "high")
             .when(confidence_number < 30, "low")
             .when(confidence_number <= 80, "nominal")
             .when(confidence_number > 80, "high"))
        .withColumn("satellite", F.upper(F.trim("satellite")))
        .withColumn("instrument", F.upper(F.trim("instrument")))
        .withColumn("daynight", F.upper(F.trim("daynight")))
        .withColumn("_firms_product", F.upper(F.coalesce(product, F.lit("SP")))))

    west, south, east, north = CA_BBOX
    return split_valid(fire, [
        ("missing_coordinates", F.col("latitude").isNotNull() & F.col("longitude").isNotNull()),
        ("outside_study_area", F.col("latitude").between(south, north) & F.col("longitude").between(west, east)),
        ("bad_timestamp", F.col("acq_ts_utc").isNotNull()),
        ("frp_out_of_range", F.col("frp_mw").isNull() | F.col("frp_mw").between(0, FRP_MAX_MW)),
        ("unknown_confidence", F.col("confidence_class").isNotNull()),
    ], "nasa_firms", FIRE_KEY)


def process_firms(batch_df, batch_id):
    valid, bad = normalize_firms(batch_df)
    valid = latest_per_key(valid, FIRE_KEY, ["_ingested_at_utc"])
    valid = valid.select(
        *FIRE_KEY, "date_local", "acq_date", "brightness_k", "frp_mw",
        "confidence_class", "instrument", "daynight", "_firms_product",
        "_source_file", "_ingested_at_utc", "_batch_id",
    )
    valid = assign_county(
        spark,
        F.broadcast(spark.table(t("silver_dim_county"))),
        valid,
        "latitude",
        "longitude",
        FIRE_KEY,
    )
    write_quarantine(spark, t("silver_quarantine"), bad)
    route = route_bloom(valid, "nasa_firms", "firms", t("silver_fire_detections"), FIRE_KEY, batch_id)
    write_split_and_merge(
        route,
        t("silver_fire_detections"),
        FIRE_KEY,
        "(n._firms_product = 'SP' AND coalesce(s._firms_product, 'SP') = 'NRT') OR "
        "(n._firms_product = coalesce(s._firms_product, 'SP') AND n._ingested_at_utc > s._ingested_at_utc)",
    )
    update_dgim(
        route["new_rows"], "nasa_firms", t("silver_fire_detections"),
        "firms_high_confidence_24h", "acq_ts_utc",
        F.col("confidence_class") == "high", batch_id,
    )
    queue_dates(spark, t("ops_gold_pending_dates"), valid, "nasa_firms")
    finish_bloom_batch("nasa_firms", route, batch_id)
    route["new_rows"].unpersist()


def process_epa(batch_df, batch_id):
    epa = (batch_df
        .withColumn("county_fips", F.concat(F.lpad("state_code", 2, "0"), F.lpad("county_code", 3, "0")))
        .withColumn("site_id", F.concat("county_fips", F.lpad("site_number", 4, "0")))
        .withColumn("poc", F.col("poc").cast("int"))
        .withColumn("date_local", F.to_date("date_local"))
        .withColumn("pm25_ugm3", F.col("arithmetic_mean").cast("double"))
        .withColumn("pm25_first_max", F.col("first_max_value").cast("double"))
        .withColumn("aqi", F.col("aqi").cast("int"))
        .withColumn("observation_percent", F.col("observation_percent").cast("double"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("date_of_last_change", F.to_date("date_of_last_change")))
    good, bad = split_valid(epa, [
        ("wrong_parameter", F.col("parameter_code") == "88101"),
        ("wrong_state", F.lpad("state_code", 2, "0") == STATE_FIPS),
        ("bad_date", F.col("date_local").isNotNull()),
        ("missing_value", F.col("pm25_ugm3").isNotNull()),
        ("impossible_value", F.col("pm25_ugm3").between(PM25_MIN, PM25_MAX)),
        ("low_completeness", F.col("observation_percent").isNull() | (F.col("observation_percent") >= 75)),
    ], "epa_aqs", EPA_KEY)
    event_rank = F.when(F.col("event_type") == "Excluded", 1).otherwise(0).asc()
    out = (latest_per_key(good, EPA_KEY, [event_rank, "date_of_last_change", "_ingested_at_utc"])
        .select(
            *EPA_KEY, "county_fips", "local_site_name", "latitude", "longitude", "pm25_ugm3",
            "pm25_first_max", "aqi", "observation_percent", "event_type", "method_code",
            "date_of_last_change", "_source_file", "_ingested_at_utc", "_batch_id",
        ))
    write_quarantine(spark, t("silver_quarantine"), bad)
    route = route_bloom(out, "epa_aqs", "epa_aqs", t("silver_pm25_daily"), EPA_KEY, batch_id)
    write_split_and_merge(
        route,
        t("silver_pm25_daily"),
        EPA_KEY,
        "NOT (n.event_type <=> 'Excluded' AND NOT s.event_type <=> 'Excluded') AND "
        "(n.date_of_last_change > s.date_of_last_change OR "
        "(n.date_of_last_change <=> s.date_of_last_change AND n._ingested_at_utc > s._ingested_at_utc))",
    )
    update_dgim(
        route["new_rows"], "epa_aqs", t("silver_pm25_daily"),
        "epa_pm25_alerts_24h", "date_local",
        F.col("pm25_ugm3") >= F.lit(EPA_PM25_ALERT_THRESHOLD), batch_id,
    )
    queue_dates(spark, t("ops_gold_pending_dates"), out, "epa_aqs")
    finish_bloom_batch("epa_aqs", route, batch_id)
    route["new_rows"].unpersist()


def process_noaa(batch_df, batch_id):
    stations = (spark.table(t("bronze_ref_noaa_stations"))
        .select(
            F.col("id").alias("station"),
            F.col("name").alias("station_name"),
            F.col("latitude").cast("double").alias("st_lat"),
            F.col("longitude").cast("double").alias("st_lon"),
        ))
    wx = (batch_df
        .withColumn("date_local", F.to_date(F.substring("date", 1, 10)))
        .withColumn("element", F.upper("datatype"))
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("qflag", F.split(F.coalesce("attributes", F.lit("")), ",").getItem(1))
        .join(F.broadcast(stations), "station", "left"))
    element, value = F.col("element"), F.col("value")
    in_range = (F.when(element.isin("TMAX", "TMIN"), value.between(TEMP_MIN_C, TEMP_MAX_C))
        .when(element == "PRCP", value.between(0, 1000))
        .when(element.isin("AWND", "WSF2"), value.between(0, WIND_MAX_MS))
        .when(element == "WDF2", value.between(0, 360))
        .otherwise(F.lit(True)))
    good, bad = split_valid(wx, [
        ("unneeded_element", element.isin(NOAA_ELEMENTS)),
        ("bad_date", F.col("date_local").isNotNull()),
        ("missing_value", value.isNotNull()),
        ("failed_source_qc", F.coalesce(F.trim("qflag"), F.lit("")) == ""),
        ("impossible_value", in_range),
        ("unmatched_station", F.col("st_lat").isNotNull()),
    ], "noaa_cdo", NOAA_KEY)
    out = latest_per_key(good, NOAA_KEY, ["_ingested_at_utc"]).select(
        *NOAA_KEY, "value", "station_name", "st_lat", "st_lon",
        "_source_file", "_ingested_at_utc", "_batch_id",
    )
    write_quarantine(spark, t("silver_quarantine"), bad)
    route = route_bloom(out, "noaa_cdo", "noaa_weather", t("silver_weather_obs"), NOAA_KEY, batch_id)
    write_split_and_merge(
        route,
        t("silver_weather_obs"),
        NOAA_KEY,
        "n._ingested_at_utc > s._ingested_at_utc AND NOT (n.value <=> s.value)",
    )
    queue_dates(spark, t("ops_gold_pending_dates"), out, "noaa_cdo")

    affected_dates = [
        row.date_local
        for row in out.select("date_local").distinct().collect()
        if row.date_local is not None
    ]
    if affected_dates:
        obs = spark.table(t("silver_weather_obs")).where(F.col("date_local").isin(affected_dates))
        wide = (obs.groupBy("station", "date_local", "station_name", "st_lat", "st_lon")
            .pivot("element", NOAA_ELEMENTS).agg(F.first("value"))
            .withColumnRenamed("TMAX", "tmax_c")
            .withColumnRenamed("TMIN", "tmin_c")
            .withColumnRenamed("PRCP", "prcp_mm")
            .withColumnRenamed("AWND", "awnd_ms")
            .withColumnRenamed("WDF2", "wdf2_deg")
            .withColumnRenamed("WSF2", "wsf2_ms"))
        wide = assign_county(
            spark,
            F.broadcast(spark.table(t("silver_dim_county"))),
            wide,
            "st_lat",
            "st_lon",
            ["station", "date_local"],
        )
        if not spark.catalog.tableExists(t("silver_weather_daily")):
            wide.write.format("delta").saveAsTable(t("silver_weather_daily"))
        else:
            target = DeltaTable.forName(spark, t("silver_weather_daily"))
            (target.alias("target")
                .merge(
                    wide.alias("source"),
                    "target.station = source.station AND target.date_local = source.date_local",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())

    finish_bloom_batch("noaa_cdo", route, batch_id)
    route["new_rows"].unpersist()


# COMMAND ----------

dbutils.fs.mkdirs(CHECKPOINT_ROOT)

stream_specs = [
    ("bronze_firms", "silver_firms_stream_bloom_v3", f"{CHECKPOINT_ROOT}/firms", process_firms),
    ("bronze_epa_pm25", "silver_epa_stream_bloom_v3", f"{CHECKPOINT_ROOT}/epa", process_epa),
    ("bronze_noaa_daily", "silver_noaa_stream_bloom_v3", f"{CHECKPOINT_ROOT}/noaa", process_noaa),
]

ran_count = 0
for bronze_tbl, query_name, ckpt_path, handler_fn in stream_specs:
    if not spark.catalog.tableExists(t(bronze_tbl)):
        continue

    write_notebook_log("INFO", f"Starting availableNow Silver stream: {query_name}")
    q = (
        spark.readStream
        .table(t(bronze_tbl))
        .writeStream
        .queryName(query_name)
        .option("checkpointLocation", ckpt_path)
        .foreachBatch(handler_fn)
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    ran_count += 1
    write_notebook_log("INFO", f"Finished availableNow Silver stream: {query_name}")

if ran_count == 0:
    raise ValueError("No Bronze source tables found; run 02_load_bronze_v2.py first")

write_notebook_log("INFO", f"All {ran_count} Silver availableNow stream(s) finished.")


# COMMAND ----------

latest_metrics = spark.sql(f"""
SELECT source, algorithm, batch_id, metric_name, metric_value, metric_ts_utc
FROM {t('v_ops_stream_metrics_latest')}
WHERE algorithm IN ('bloom_filter', 'dgim')
ORDER BY metric_ts_utc DESC, source, algorithm, metric_name
""")
write_notebook_log("INFO", f"Verified {latest_metrics.count():,} latest streaming metric row(s).")