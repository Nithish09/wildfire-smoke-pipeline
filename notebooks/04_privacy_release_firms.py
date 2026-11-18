# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # FIRMS privacy release: spatial/temporal generalization + k-threshold suppression
# MAGIC Run after `03_transform_silver` (or `03s_silver_stream` after its batch completes).
# MAGIC This standalone notebook reads `silver_fire_detections`; it does not require Gold.
# MAGIC
# MAGIC **Release contract:** publish county × grid-cell × California local-day groups
# MAGIC only when they contain at least `k` distinct Silver detection keys. County-day
# MAGIC totals are derived exclusively from released groups. Missing groups are withheld
# MAGIC or absent; they must not be interpreted as zero. Do not combine these releases
# MAGIC with exact totals to reconstruct suppressed cells.
# MAGIC
# MAGIC This demonstrates disclosure reduction on already-public environmental data.
# MAGIC It is **k-threshold suppression**, not a guarantee of person-level k-anonymity
# MAGIC or differential privacy. Five detections can represent one fire/location.
# MAGIC Public FIRMS records remain available and can be linked back to these summaries.
# MAGIC
# MAGIC The grid uses a local equirectangular projection centred on California, with
# MAGIC standard latitude 37.25 degrees. `grid_km` is a nominal size: east-west ground
# MAGIC width varies with latitude (roughly 4.7–5.3 km for a nominal 5 km cell in CA).
# MAGIC Cells are fixed across runs. County is part of the suppression key because it
# MAGIC is also released; a cell split across counties must pass k separately in each.
# MAGIC County assignment is inherited from Silver's nearest-county-centre method,
# MAGIC rather than a county-boundary point-in-polygon calculation.
# MAGIC
# MAGIC Only the two `privacy_firms_*_day` tables are intended as release candidates.
# MAGIC `ops_privacy_firms_results` contains exact internal comparison statistics.
# MAGIC Table names do not enforce access control: configure grants separately before
# MAGIC sharing. Repeated snapshots, different grid/k settings, or exact Gold exports
# MAGIC can permit differencing; this notebook does not provide composition protection.

# COMMAND ----------

import math
import re
from datetime import date
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "wildfire", "Schema")
dbutils.widgets.text("grid_km", "5", "Nominal grid size (km)")
dbutils.widgets.text("k", "5", "Minimum distinct detections per released group")
dbutils.widgets.text("start_date", "", "First local day (blank = all)")
dbutils.widgets.text("end_date", "", "Last local day (blank = all)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
for identifier in (CATALOG, SCHEMA):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError("Catalog/schema must be simple SQL identifiers")
GRID_KM = float(dbutils.widgets.get("grid_km"))
K = int(dbutils.widgets.get("k"))
if not math.isfinite(GRID_KM) or not 0.1 <= GRID_KM <= 100:
    raise ValueError("grid_km must be finite and between 0.1 and 100")
if K < 2:
    raise ValueError("k must be at least 2")

def optional_date(name):
    value = dbutils.widgets.get(name).strip()
    return date.fromisoformat(value) if value else None

START_DATE, END_DATE = optional_date("start_date"), optional_date("end_date")
if START_DATE and END_DATE and START_DATE > END_DATE:
    raise ValueError("start_date must not be after end_date")

PREFIX = f"{CATALOG}.{SCHEMA}"
SOURCE = f"{PREFIX}.silver_fire_detections"
GRID_TARGET = f"{PREFIX}.privacy_firms_grid_day"
COUNTY_TARGET = f"{PREFIX}.privacy_firms_county_day"
RESULTS_TARGET = f"{PREFIX}.ops_privacy_firms_results"
DETECTION_KEY = ["latitude", "longitude", "acq_ts_utc", "satellite"]
GROUP_KEY = ["county_fips", "grid_x", "grid_y", "date_local"]
R_KM, ORIGIN_LAT, ORIGIN_LON = 6371.0088, 37.25, -120.0
KM_PER_DEG = R_KM * math.pi / 180.0
LON_KM_PER_DEG = KM_PER_DEG * math.cos(math.radians(ORIGIN_LAT))

def aggregate_grid(detections, grid_km):
    """Native Spark expressions; no Python UDF or extra packages required."""
    return (
        detections
        .withColumn("grid_x", F.floor((F.col("longitude") - ORIGIN_LON) * LON_KM_PER_DEG / grid_km).cast("long"))
        .withColumn("grid_y", F.floor((F.col("latitude") - ORIGIN_LAT) * KM_PER_DEG / grid_km).cast("long"))
        .groupBy(*GROUP_KEY)
        .agg(F.count("*").alias("detection_count"),
             F.sum("frp_mw").alias("frp_total_mw"),
             F.count("frp_mw").alias("frp_observation_count"))
    )

def county_from_released(released):
    return released.groupBy("county_fips", "date_local").agg(
        F.sum("detection_count").alias("detection_count"),
        F.sum("frp_total_mw").alias("frp_total_mw"),
        F.sum("frp_observation_count").alias("frp_observation_count"),
        F.count("*").alias("released_cell_count"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthetic checks (run before any writes)
# MAGIC Cover k−1/k boundaries, county/day splits, missing FRP, and negative grid
# MAGIC indices. County totals must include only released groups.

# COMMAND ----------

test_rows = []
for county, day, lon, n, power in [
    ("06001", date(2020, 8, 20), -119.99, 5, 10.0),
    ("06001", date(2020, 8, 20), -119.90, 4, 10.0),
    ("06003", date(2020, 8, 20), -119.99, 4, 10.0),
    ("06001", date(2020, 8, 21), -119.99, 4, 10.0),
    ("06005", date(2020, 8, 20), -120.01, 5, None),
]:
    for i in range(n):
        test_rows.append((county, day, 37.26 + i * 0.00001, lon, power))
test_df = spark.createDataFrame(test_rows,
    "county_fips string, date_local date, latitude double, longitude double, frp_mw double")
test_released = aggregate_grid(test_df, 5.0).where(F.col("detection_count") >= 5)
assert test_released.count() == 2, "Suppression must use the complete released key"
test_counties = {r.county_fips: r for r in county_from_released(test_released).collect()}
assert test_counties["06001"].detection_count == 5, "County total leaked suppressed cells"
assert test_counties["06001"].frp_total_mw == 50.0
assert test_counties["06005"].frp_total_mw is None, "Missing FRP must not become zero"
assert test_released.where("grid_x = -1").count() == 1, "Grid requires floor, not truncation"
print("Synthetic privacy-release checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate and generalize the current Silver snapshot
# MAGIC Reject duplicate business keys instead of allowing duplicates to inflate k.
# MAGIC Dates use Silver's California local day. Null/invalid release keys fail closed.

# COMMAND ----------

if not spark.catalog.tableExists(SOURCE):
    raise ValueError(f"Run Silver first: missing {SOURCE}")
source = spark.table(SOURCE)
required = set(DETECTION_KEY + ["date_local", "county_fips", "frp_mw"])
if required - set(source.columns):
    raise ValueError(f"Missing Silver fields: {sorted(required - set(source.columns))}")
source = source.select(*sorted(required))
if START_DATE:
    source = source.where(F.col("date_local") >= F.lit(START_DATE))
if END_DATE:
    source = source.where(F.col("date_local") <= F.lit(END_DATE))
source = source.cache()
source_count = source.count()
if not source_count:
    raise ValueError("No source rows in the selected window; existing releases were not replaced")
valid = (
    F.col("latitude").between(32.5, 42.0)
    & F.col("longitude").between(-124.5, -114.1)
    & F.col("date_local").isNotNull()
    & F.col("acq_ts_utc").isNotNull()
    & F.col("county_fips").rlike(r"^06[0-9]{3}$")
    & (F.col("frp_mw").isNull() | F.col("frp_mw").between(0.0, 20000.0))
)
if source.where(~F.coalesce(valid, F.lit(False))).limit(1).count():
    raise ValueError("Invalid Silver release keys or FRP values; fix Silver before releasing")
if source.groupBy(*DETECTION_KEY).count().where("count > 1").limit(1).count():
    raise ValueError("Duplicate Silver detection keys would inflate k; repair Silver first")

all_cells = aggregate_grid(source, GRID_KM).cache()
released = all_cells.where(F.col("detection_count") >= K).cache()
county_release = county_from_released(released).cache()
baseline_county = source.groupBy("county_fips", "date_local").agg(
    F.count("*").alias("detection_count"), F.sum("frp_mw").alias("frp_total_mw"))

def release_metadata(frame):
    return (frame.withColumn("grid_km_nominal", F.lit(GRID_KM))
        .withColumn("k_min_detections", F.lit(K))
        .withColumn("release_method", F.lit("spatial_temporal_generalization_k_threshold"))
        .withColumn("released_at_utc", F.current_timestamp()))

# Explicit allowlist: no exact coordinates, times, row hashes, or source metadata.
grid_output = release_metadata(released.select(
    *GROUP_KEY, "detection_count", F.round("frp_total_mw", 2).alias("frp_total_mw"),
    "frp_observation_count"))
county_output = release_metadata(county_release.select(
    "county_fips", "date_local", "detection_count",
    F.round("frp_total_mw", 2).alias("frp_total_mw"),
    "frp_observation_count", "released_cell_count"
).withColumn("coverage", F.lit("retained_cells_only_not_complete_county_total")))
assert released.where(F.col("detection_count") < K).limit(1).count() == 0
retained_count = released.agg(F.sum("detection_count")).first()[0] or 0
assert (county_release.agg(F.sum("detection_count")).first()[0] or 0) == retained_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Internal results: suppression, retained observations/FRP, county ranking
# MAGIC These are measured results, not privacy guarantees. Do not publish the exact
# MAGIC baseline totals alongside the release. FRP retention is NULL if baseline FRP
# MAGIC is missing or zero. Sparse counties can disappear entirely.

# COMMAND ----------

def totals(frame):
    row = frame.agg(F.count("*").alias("groups"),
        F.sum("detection_count").alias("detections"),
        F.sum("frp_total_mw").alias("frp")).first()
    return row.groups, row.detections or 0, row.frp

def top_counties(frame):
    return [r.county_fips for r in frame.groupBy("county_fips")
        .agg(F.sum("detection_count").alias("n"))
        .orderBy(F.desc("n"), "county_fips").limit(5).collect()]

top_before, top_after = top_counties(baseline_county), top_counties(county_release)
metric_rows = []
for level, before, after in [
    ("county_grid_day", all_cells, released),
    ("county_day", baseline_county, county_release),
]:
    bg, bn, bf = totals(before)
    ag, an, af = totals(after)
    metric_rows.append((level, bg, ag, bg - ag, 100.0 * (bg - ag) / bg,
        bn, an, 100.0 * an / bn, bf, af,
        (100.0 * (af or 0.0) / bf) if bf else None))
results = spark.createDataFrame(metric_rows,
    "level string, groups_before long, groups_released long, groups_withheld long, "
    "groups_withheld_pct double, detections_before long, detections_released long, "
    "detections_retained_pct double, frp_before_mw double, frp_released_mw double, "
    "frp_retained_pct double")
results = release_metadata(results).withColumn("top5_counties_before", F.lit(",".join(top_before))) \
    .withColumn("top5_counties_after", F.lit(",".join(top_after))) \
    .withColumn("top5_counties_overlap", F.lit(len(set(top_before) & set(top_after)))) \
    .withColumn("source_table", F.lit(SOURCE)) \
    .withColumn("start_date_filter", F.lit(str(START_DATE) if START_DATE else "all")) \
    .withColumn("end_date_filter", F.lit(str(END_DATE) if END_DATE else "all"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the current release and display results
# MAGIC Each successful run replaces these three dedicated tables for the selected
# MAGIC date range; it never appends old release settings. Delta commits are atomic per
# MAGIC table, not across tables. Share only after the whole notebook succeeds. Older
# MAGIC Delta versions may remain accessible to privileged users through time travel.
# MAGIC Run this job after Silver finishes, without concurrent release writers.

# COMMAND ----------

for frame, target in [(grid_output, GRID_TARGET), (county_output, COUNTY_TARGET),
                      (results, RESULTS_TARGET)]:
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
display(spark.table(RESULTS_TARGET))
print(f"Release candidates: {GRID_TARGET}, {COUNTY_TARGET}")
print(f"Internal evaluation: {RESULTS_TARGET}")
for frame in (source, all_cells, released, county_release):
    frame.unpersist()