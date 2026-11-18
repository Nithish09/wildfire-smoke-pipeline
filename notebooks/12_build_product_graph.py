# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Build product graph tables
# MAGIC
# MAGIC This notebook materializes the fire-and-smoke graph as Delta tables for a
# MAGIC Databricks dashboard. It does not render Plotly output.
# MAGIC
# MAGIC Run it after `09_fire_events.py`. The fire progression links are written by
# MAGIC notebook 09, while MinHash/LSH movement links come from
# MAGIC `gold_fire_lsh_detection_edges` and smoke-impact links come from
# MAGIC `gold_fire_event_county_impact`.
# MAGIC The tables are rebuilt with overwrite because notebook 09 recomputes its
# MAGIC graph outputs from the complete Silver detection history.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import time

from pyspark.sql import Window
from pyspark.sql import functions as F

SOURCE_FILE = "12_build_product_graph"
started = time.time()

EVENT_DAY_TABLE = t("gold_fire_event_day")
IMPACT_TABLE = t("gold_fire_event_county_impact")
COUNTY_TABLE = t("gold_dim_county")
DETECTIONS_TABLE = t("silver_fire_detections")
MEMBERS_TABLE = t("silver_fire_event_members")
FIRE_EDGES_TABLE = t("gold_fire_detection_edges")
LSH_FIRE_EDGES_TABLE = t("gold_fire_lsh_detection_edges")

NODES_TABLE = t("gold_product_graph_nodes")
EDGES_TABLE = t("gold_product_graph_edges")
PATHS_TABLE = t("gold_product_graph_paths")

required_tables = (
    EVENT_DAY_TABLE,
    IMPACT_TABLE,
    COUNTY_TABLE,
    DETECTIONS_TABLE,
    MEMBERS_TABLE,
    FIRE_EDGES_TABLE,
    LSH_FIRE_EDGES_TABLE,
)
missing_tables = [name for name in required_tables if not spark.catalog.tableExists(name)]
if missing_tables:
    raise ValueError(
        "Run 04_kpi_gold.py and the MinHash/LSH version of 09_fire_events.py first. "
        f"Missing tables: {missing_tables}"
    )

if "detection_id" not in spark.table(MEMBERS_TABLE).columns:
    raise ValueError(
        "silver_fire_event_members does not contain detection_id. "
        "Run the updated 09_fire_events.py before building the product graph."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fire-detection and event-centroid nodes

# COMMAND ----------

KEY = ["latitude", "longitude", "acq_ts_utc", "satellite"]

members = spark.table(MEMBERS_TABLE).select("detection_id", *KEY, "event_id")
detections = (
    spark.table(DETECTIONS_TABLE)
    .withColumn("satellite", F.coalesce(F.col("satellite"), F.lit("")))
    .join(members, KEY, "inner")
    .select(
        "detection_id",
        "event_id",
        "latitude",
        "longitude",
        "acq_ts_utc",
        "date_local",
        "frp_mw",
        "county_fips",
        "confidence_class",
        "satellite",
    )
    .where(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("acq_ts_utc").isNotNull()
    )
)

event_order_window = Window.partitionBy("event_id").orderBy(
    "acq_ts_utc", "latitude", "longitude", "satellite"
)

fire_nodes = (
    detections
    .withColumn("node_id", F.concat(F.lit("fire:"), F.col("detection_id")))
    .withColumn("node_type", F.lit("fire"))
    .withColumn("node_shape", F.lit("circle"))
    .withColumn("event_order", F.row_number().over(event_order_window))
    .withColumn("size_value", F.coalesce(F.col("frp_mw"), F.lit(0.0)))
    .withColumn("county_name", F.lit(None).cast("string"))
    .withColumn("distance_km", F.lit(None).cast("double"))
    .withColumn("days_in_radius", F.lit(None).cast("long"))
    .withColumn("upwind_days", F.lit(None).cast("long"))
    .withColumn("upwind_alignment", F.lit(None).cast("double"))
    .withColumn("event_upwind", F.lit(None).cast("boolean"))
    .withColumn("wind_dir_deg", F.lit(None).cast("double"))
    .withColumn("wind_mean_ms", F.lit(None).cast("double"))
    .withColumn("pm25_mean_ugm3", F.lit(None).cast("double"))
    .withColumn(
        "display_label",
        F.concat(F.lit("Fire detection "), F.col("event_order").cast("string")),
    )
    .select(
        "node_id",
        "event_id",
        "node_type",
        "node_shape",
        "date_local",
        F.col("latitude"),
        F.col("longitude"),
        "size_value",
        "frp_mw",
        "event_order",
        "county_fips",
        "county_name",
        "distance_km",
        "days_in_radius",
        "upwind_days",
        "upwind_alignment",
        "event_upwind",
        "wind_dir_deg",
        "wind_mean_ms",
        "pm25_mean_ugm3",
        "display_label",
    )
)

event_day_nodes = (
    spark.table(EVENT_DAY_TABLE)
    .select("event_id", "date_local", "centroid_lat", "centroid_lon", "frp_total_mw", "detections")
    .where(F.col("centroid_lat").isNotNull() & F.col("centroid_lon").isNotNull())
    .withColumn(
        "node_id",
        F.concat_ws(":", F.lit("event_day"), F.col("event_id"), F.col("date_local").cast("string")),
    )
    .withColumn("node_type", F.lit("event_centroid"))
    .withColumn("node_shape", F.lit("circle"))
    .withColumn("size_value", F.coalesce(F.col("frp_total_mw"), F.lit(0.0)))
    .withColumn("frp_mw", F.col("frp_total_mw"))
    .withColumn("event_order", F.lit(None).cast("long"))
    .withColumn("county_fips", F.lit(None).cast("string"))
    .withColumn("county_name", F.lit(None).cast("string"))
    .withColumn("distance_km", F.lit(None).cast("double"))
    .withColumn("days_in_radius", F.lit(None).cast("long"))
    .withColumn("upwind_days", F.lit(None).cast("long"))
    .withColumn("upwind_alignment", F.lit(None).cast("double"))
    .withColumn("event_upwind", F.lit(None).cast("boolean"))
    .withColumn("wind_dir_deg", F.lit(None).cast("double"))
    .withColumn("wind_mean_ms", F.lit(None).cast("double"))
    .withColumn("pm25_mean_ugm3", F.lit(None).cast("double"))
    .withColumn("display_label", F.concat_ws(" ", F.lit("Event centroid"), F.col("date_local")))
    .select(
        "node_id",
        "event_id",
        "node_type",
        "node_shape",
        "date_local",
        F.col("centroid_lat").alias("latitude"),
        F.col("centroid_lon").alias("longitude"),
        "size_value",
        "frp_mw",
        "event_order",
        "county_fips",
        "county_name",
        "distance_km",
        "days_in_radius",
        "upwind_days",
        "upwind_alignment",
        "event_upwind",
        "wind_dir_deg",
        "wind_mean_ms",
        "pm25_mean_ugm3",
        "display_label",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## County smoke-impact nodes

# COMMAND ----------

county_dim = spark.table(COUNTY_TABLE).select("county_fips", "c_lat", "c_lon")
impact = (
    spark.table(IMPACT_TABLE)
    .join(county_dim, "county_fips", "left")
    .where(F.col("c_lat").isNotNull() & F.col("c_lon").isNotNull())
)

county_nodes = (
    impact.groupBy("event_id", "county_fips", "county_name", "c_lat", "c_lon")
    .agg(
        F.countDistinct("date_local").cast("long").alias("days_in_radius"),
        F.min("distance_km").alias("distance_km"),
        F.sum(F.when(F.col("event_upwind") == True, 1).otherwise(0)).cast("long").alias("upwind_days"),
        F.count("event_upwind").cast("long").alias("wind_observation_days"),
        F.avg("upwind_alignment").alias("upwind_alignment"),
        F.first("wind_dir_deg", ignorenulls=True).alias("wind_dir_deg"),
        F.avg("wind_mean_ms").alias("wind_mean_ms"),
        F.avg("pm25_mean_ugm3").alias("pm25_mean_ugm3"),
    )
    .withColumn(
        "node_id",
        F.concat_ws(":", F.lit("county"), F.col("event_id"), F.col("county_fips")),
    )
    .withColumn("node_type", F.lit("county"))
    .withColumn("node_shape", F.lit("square"))
    .withColumn("date_local", F.lit(None).cast("date"))
    .withColumn("size_value", F.col("days_in_radius").cast("double"))
    .withColumn("frp_mw", F.lit(None).cast("double"))
    .withColumn("event_order", F.lit(None).cast("long"))
    .withColumn(
        "event_upwind",
        F.when(F.col("wind_observation_days") == 0, F.lit(None).cast("boolean"))
        .otherwise(F.col("upwind_days") > 0),
    )
    .withColumn(
        "display_label",
        F.concat_ws(" ", F.col("county_name"), F.lit("smoke-impact county")),
    )
    .select(
        "node_id",
        "event_id",
        "node_type",
        "node_shape",
        "date_local",
        F.col("c_lat").alias("latitude"),
        F.col("c_lon").alias("longitude"),
        "size_value",
        "frp_mw",
        "event_order",
        "county_fips",
        "county_name",
        "distance_km",
        "days_in_radius",
        "upwind_days",
        "upwind_alignment",
        "event_upwind",
        "wind_dir_deg",
        "wind_mean_ms",
        "pm25_mean_ugm3",
        "display_label",
    )
)

node_columns = [
    "node_id",
    "event_id",
    "node_type",
    "node_shape",
    "date_local",
    "latitude",
    "longitude",
    "size_value",
    "frp_mw",
    "event_order",
    "county_fips",
    "county_name",
    "distance_km",
    "days_in_radius",
    "upwind_days",
    "upwind_alignment",
    "event_upwind",
    "wind_dir_deg",
    "wind_mean_ms",
    "pm25_mean_ugm3",
    "display_label",
]

nodes = (
    fire_nodes.select(node_columns)
    .unionByName(event_day_nodes.select(node_columns))
    .unionByName(county_nodes.select(node_columns))
    .withColumn("_built_at_utc", F.current_timestamp())
)
nodes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(NODES_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fire and smoke edges

# COMMAND ----------

fire_edges = (
    spark.table(FIRE_EDGES_TABLE)
    .select(
        "edge_id",
        "event_id",
        "source_detection_id",
        "target_detection_id",
        "source_latitude",
        "source_longitude",
        "target_latitude",
        "target_longitude",
        "source_time",
        "target_time",
        F.col("source_date_local").alias("date_local"),
        "distance_km",
        "elapsed_hours",
    )
    .withColumn("edge_type", F.lit("fire_progression"))
    .withColumn("source_node_id", F.concat(F.lit("fire:"), F.col("source_detection_id")))
    .withColumn("target_node_id", F.concat(F.lit("fire:"), F.col("target_detection_id")))
    .withColumn("event_day_frp_mw", F.lit(None).cast("double"))
    .withColumn("event_day_detections", F.lit(None).cast("long"))
    .withColumn("wind_dir_deg", F.lit(None).cast("double"))
    .withColumn("wind_mean_ms", F.lit(None).cast("double"))
    .withColumn("upwind_alignment", F.lit(None).cast("double"))
    .withColumn("event_upwind", F.lit(None).cast("boolean"))
    .withColumn("pm25_mean_ugm3", F.lit(None).cast("double"))
    .withColumn("pm25_next_day_ugm3", F.lit(None).cast("double"))
    .withColumn("minhash_estimate", F.lit(None).cast("double"))
    .withColumn("shared_lsh_bands", F.lit(None).cast("int"))
    .withColumn("lsh_detection_links", F.lit(None).cast("long"))
    .withColumn("edge_color_group", F.lit("fire_progression"))
)

# The LSH table contains detection-level links. Aggregate them by event and local day so the
# dashboard receives one readable line between event-day centroids instead of a dense hairball.
lsh_day_edges = (
    spark.table(LSH_FIRE_EDGES_TABLE)
    .where(F.col("source_date_local") != F.col("target_date_local"))
    .withColumn(
        "from_date",
        F.when(
            F.col("source_date_local") < F.col("target_date_local"),
            F.col("source_date_local"),
        ).otherwise(F.col("target_date_local")),
    )
    .withColumn(
        "to_date",
        F.when(
            F.col("source_date_local") < F.col("target_date_local"),
            F.col("target_date_local"),
        ).otherwise(F.col("source_date_local")),
    )
    .groupBy("event_id", "from_date", "to_date")
    .agg(
        F.count("*").cast("long").alias("lsh_detection_links"),
        F.min("distance_km").alias("distance_km"),
        F.avg("elapsed_hours").alias("elapsed_hours"),
        F.avg("minhash_estimate").alias("minhash_estimate"),
        F.max("shared_lsh_bands").cast("int").alias("shared_lsh_bands"),
    )
)

source_event_days = (
    spark.table(EVENT_DAY_TABLE)
    .select(
        "event_id",
        F.col("date_local").alias("from_date"),
        F.col("centroid_lat").alias("source_latitude"),
        F.col("centroid_lon").alias("source_longitude"),
        F.col("frp_total_mw").alias("source_frp_mw"),
        F.col("detections").cast("long").alias("source_detections"),
    )
)
target_event_days = (
    spark.table(EVENT_DAY_TABLE)
    .select(
        "event_id",
        F.col("date_local").alias("to_date"),
        F.col("centroid_lat").alias("target_latitude"),
        F.col("centroid_lon").alias("target_longitude"),
    )
)

lsh_edges = (
    lsh_day_edges.join(source_event_days, ["event_id", "from_date"], "inner")
    .join(target_event_days, ["event_id", "to_date"], "inner")
    .select(
        F.concat_ws(":", F.lit("L"), F.col("event_id"), F.col("from_date"), F.col("to_date")).alias("edge_id"),
        F.col("event_id"),
        F.concat_ws(":", F.lit("event_day"), F.col("event_id"), F.col("from_date")).alias("source_node_id"),
        F.concat_ws(":", F.lit("event_day"), F.col("event_id"), F.col("to_date")).alias("target_node_id"),
        F.col("source_latitude"),
        F.col("source_longitude"),
        F.col("target_latitude"),
        F.col("target_longitude"),
        F.lit(None).cast("timestamp").alias("source_time"),
        F.lit(None).cast("timestamp").alias("target_time"),
        F.col("from_date").alias("date_local"),
        F.col("distance_km").cast("double"),
        F.col("elapsed_hours").cast("double"),
        F.lit("fire_movement_lsh").alias("edge_type"),
        F.col("source_frp_mw").cast("double").alias("event_day_frp_mw"),
        F.col("source_detections").alias("event_day_detections"),
        F.lit(None).cast("double").alias("wind_dir_deg"),
        F.lit(None).cast("double").alias("wind_mean_ms"),
        F.lit(None).cast("double").alias("upwind_alignment"),
        F.lit(None).cast("boolean").alias("event_upwind"),
        F.lit(None).cast("double").alias("pm25_mean_ugm3"),
        F.lit(None).cast("double").alias("pm25_next_day_ugm3"),
        F.col("minhash_estimate").cast("double"),
        F.col("shared_lsh_bands").cast("int"),
        F.col("lsh_detection_links").cast("long"),
        F.lit("fire_movement_lsh").alias("edge_color_group"),
    )
)

smoke_edges = (
    impact.alias("i")
    .join(
        spark.table(EVENT_DAY_TABLE)
        .select("event_id", "date_local", "centroid_lat", "centroid_lon")
        .alias("d"),
        ["event_id", "date_local"],
        "inner",
    )
    .select(
        F.concat_ws(":", F.lit("S"), F.col("event_id"), F.col("date_local"), F.col("county_fips")).alias("edge_id"),
        F.col("event_id"),
        F.concat_ws(":", F.lit("event_day"), F.col("event_id"), F.col("date_local")).alias("source_node_id"),
        F.concat_ws(":", F.lit("county"), F.col("event_id"), F.col("county_fips")).alias("target_node_id"),
        F.col("d.centroid_lat").alias("source_latitude"),
        F.col("d.centroid_lon").alias("source_longitude"),
        F.col("c_lat").alias("target_latitude"),
        F.col("c_lon").alias("target_longitude"),
        F.lit(None).cast("timestamp").alias("source_time"),
        F.lit(None).cast("timestamp").alias("target_time"),
        F.col("date_local"),
        F.col("distance_km"),
        F.lit(None).cast("double").alias("elapsed_hours"),
        F.lit("smoke_impact").alias("edge_type"),
        F.col("event_day_frp_mw"),
        F.col("event_day_detections").cast("long"),
        F.col("wind_dir_deg").cast("double"),
        F.col("wind_mean_ms").cast("double"),
        F.col("upwind_alignment").cast("double"),
        F.col("event_upwind").cast("boolean"),
        F.col("pm25_mean_ugm3").cast("double"),
        F.col("pm25_next_day_ugm3").cast("double"),
        F.lit(None).cast("double").alias("minhash_estimate"),
        F.lit(None).cast("int").alias("shared_lsh_bands"),
        F.lit(None).cast("long").alias("lsh_detection_links"),
        F.when(F.col("event_upwind") == True, "smoke_upwind")
        .when(F.col("event_upwind").isNull(), "smoke_unknown")
        .otherwise("smoke_not_upwind")
        .alias("edge_color_group"),
    )
)

edge_columns = [
    "edge_id",
    "event_id",
    "source_node_id",
    "target_node_id",
    "source_latitude",
    "source_longitude",
    "target_latitude",
    "target_longitude",
    "source_time",
    "target_time",
    "date_local",
    "distance_km",
    "elapsed_hours",
    "edge_type",
    "event_day_frp_mw",
    "event_day_detections",
    "wind_dir_deg",
    "wind_mean_ms",
    "upwind_alignment",
    "event_upwind",
    "pm25_mean_ugm3",
    "pm25_next_day_ugm3",
    "minhash_estimate",
    "shared_lsh_bands",
    "lsh_detection_links",
    "edge_color_group",
]

edges = (
    fire_edges.select(edge_columns)
    .unionByName(lsh_edges.select(edge_columns))
    .unionByName(smoke_edges.select(edge_columns))
    .withColumn("_built_at_utc", F.current_timestamp())
)
edges.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(EDGES_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ordered path points for the Databricks path map
# MAGIC
# MAGIC Each edge becomes two rows: point 1 is the source and point 2 is the target.
# MAGIC Use `edge_id` as the path grouping field and `point_order` as the sequence.

# COMMAND ----------

path_start = edges.select(
    F.col("edge_id").alias("path_id"),
    "edge_id",
    "event_id",
    "edge_type",
    "date_local",
    F.lit(1).cast("int").alias("point_order"),
    F.col("source_node_id").alias("node_id"),
    F.col("source_latitude").alias("latitude"),
    F.col("source_longitude").alias("longitude"),
    "edge_color_group",
    "event_upwind",
    "upwind_alignment",
    "wind_dir_deg",
    "wind_mean_ms",
    "distance_km",
    "elapsed_hours",
    "minhash_estimate",
    "shared_lsh_bands",
    "lsh_detection_links",
)

path_end = edges.select(
    F.col("edge_id").alias("path_id"),
    "edge_id",
    "event_id",
    "edge_type",
    "date_local",
    F.lit(2).cast("int").alias("point_order"),
    F.col("target_node_id").alias("node_id"),
    F.col("target_latitude").alias("latitude"),
    F.col("target_longitude").alias("longitude"),
    "edge_color_group",
    "event_upwind",
    "upwind_alignment",
    "wind_dir_deg",
    "wind_mean_ms",
    "distance_km",
    "elapsed_hours",
    "minhash_estimate",
    "shared_lsh_bands",
    "lsh_detection_links",
)

paths = (
    path_start.unionByName(path_end)
    .where(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    .withColumn("_built_at_utc", F.current_timestamp())
)
paths.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PATHS_TABLE)

log_step("gold_product_graph_nodes", "gold", nodes.count(), started, {"table": NODES_TABLE})
log_step("gold_product_graph_edges", "gold", edges.count(), started, {"table": EDGES_TABLE})
log_step("gold_product_graph_paths", "gold", paths.count(), started, {"table": PATHS_TABLE})

print(f"Created {NODES_TABLE}")
print(f"Created {EDGES_TABLE}")
print(f"Created {PATHS_TABLE}")

