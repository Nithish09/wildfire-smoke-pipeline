# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Fire events (graph-stream algorithm)
# MAGIC Turns individual FIRMS detections into **fire events** and links each event to the counties around it.
# MAGIC
# MAGIC **Algorithm (streaming connected components).** Detections are processed once, in time order. Each new detection is
# MAGIC linked to every earlier detection within **2 km and 24 hours** (only nearby grid cells and a sliding time window are
# MAGIC checked). A union–find structure merges the connected groups as these edges stream in; each final group is one event.
# MAGIC
# MAGIC **Outputs**
# MAGIC | Table | Grain | Use |
# MAGIC | --- | --- | --- |
# MAGIC | `silver_fire_event_members` | detection → event_id | which event every detection belongs to |
# MAGIC | `gold_fire_events` | event | start/end, size, fire power, centroid, counties burned |
# MAGIC | `gold_fire_event_day` | event × local date | daily growth of each event |
# MAGIC | `gold_fire_event_county_impact` | event × day × nearby county | which counties each event could smoke, with wind and PM2.5 |
# MAGIC
# MAGIC Run after `04_kpi_gold` (it reads `gold_dim_county` and `gold_fact_county_day`). Each run recomputes events from all of
# MAGIC Silver; results are deterministic, so a rerun on unchanged data gives identical tables and event IDs.
# MAGIC Event IDs come from each event's earliest detection; if two events later merge, the merged event keeps the earlier ID.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./lib_fire_events

# COMMAND ----------

dbutils.widgets.text("link_km", "2", "Link distance (km)")
dbutils.widgets.text("link_hours", "24", "Link time window (hours)")
dbutils.widgets.text("impact_km", "150", "Max distance from event to county centre for impact (km)")
LINK_KM = float(dbutils.widgets.get("link_km"))
LINK_HOURS = float(dbutils.widgets.get("link_hours"))
IMPACT_KM = float(dbutils.widgets.get("impact_km"))
print(f"link {LINK_KM} km / {LINK_HOURS} h, impact radius {IMPACT_KM} km")

# COMMAND ----------

# MAGIC %md ## 1 · Stream detections through the graph algorithm

# COMMAND ----------

started = time.time()
KEY = ["latitude", "longitude", "acq_ts_utc", "satellite"]
# satellite is part of the key; coalesce NULL to '' so the joins below never drop a detection
det = (spark.table(t("silver_fire_detections"))
       .withColumn("satellite", F.coalesce("satellite", F.lit("")))
       .select(*KEY, "date_local", "frp_mw", "county_fips")
       .where(F.col("latitude").isNotNull() & F.col("longitude").isNotNull() & F.col("acq_ts_utc").isNotNull()))

# One pass in time order on the driver (Free Edition serverless has no RDD API; ~200k detections is a few MB).
rows = (det.select(*KEY, F.unix_timestamp("acq_ts_utc").alias("epoch_s"))
           .orderBy("acq_ts_utc", "latitude", "longitude", "satellite").collect())   # time order = stream order
detections = [(r.latitude, r.longitude, r.epoch_s / 3600.0, f"{r.latitude}|{r.longitude}|{r.acq_ts_utc}|{r.satellite}")
              for r in rows]
labels, stats = cluster_detections(detections, LINK_KM, LINK_HOURS)

first_index = {}
for k, e in enumerate(labels):
    first_index.setdefault(e, k)
ids = {e: event_id(detections[k][3], rows[k].acq_ts_utc.date()) for e, k in first_index.items()}
print(stats, f"in {time.time() - started:.1f}s")

members = spark.createDataFrame(
    [(r.latitude, r.longitude, r.acq_ts_utc, r.satellite, ids[labels[k]]) for k, r in enumerate(rows)],
    "latitude double, longitude double, acq_ts_utc timestamp, satellite string, event_id string")
(members.withColumn("_built_at_utc", F.current_timestamp())
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_fire_event_members")))
log_step("silver_fire_event_members", "silver", len(rows), started, {**stats, "link_km": LINK_KM, "link_hours": LINK_HOURS})

# COMMAND ----------

# MAGIC %md ## 2 · Event summaries and daily growth

# COMMAND ----------

started = time.time()
m = spark.table(t("silver_fire_event_members"))
d = (spark.table(t("silver_fire_detections")).withColumn("satellite", F.coalesce("satellite", F.lit("")))
     .select(*KEY, "date_local", "frp_mw", "county_fips", "confidence_class")
     .join(m.select(*KEY, "event_id"), KEY, "inner"))
w = F.greatest(F.coalesce("frp_mw", F.lit(0.0)), F.lit(0.1))          # FRP weight for the centroid

daily = (d.groupBy("event_id", "date_local").agg(
            F.count("*").alias("detections"), F.round(F.sum("frp_mw"), 1).alias("frp_total_mw"),
            F.round(F.max("frp_mw"), 1).alias("frp_max_mw"),
            (F.sum(F.col("latitude") * w) / F.sum(w)).alias("centroid_lat"),
            (F.sum(F.col("longitude") * w) / F.sum(w)).alias("centroid_lon")))
daily.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_fire_event_day"))

peak = (daily.withColumn("_r", F.row_number().over(Window.partitionBy("event_id").orderBy(F.desc("frp_total_mw"))))
             .where("_r = 1").select("event_id", F.col("date_local").alias("peak_day"), F.col("frp_total_mw").alias("peak_day_frp_mw")))
main_county = (d.groupBy("event_id", "county_fips").count()
                .withColumn("_r", F.row_number().over(Window.partitionBy("event_id").orderBy(F.desc("count"), "county_fips")))
                .where("_r = 1").select("event_id", F.col("county_fips").alias("primary_county_fips")))
events = (d.groupBy("event_id").agg(
            F.min("acq_ts_utc").alias("first_seen_utc"), F.max("acq_ts_utc").alias("last_seen_utc"),
            F.min("date_local").alias("first_day"), F.max("date_local").alias("last_day"),
            F.countDistinct("date_local").alias("active_days"), F.count("*").alias("detections"),
            F.sum(F.when(F.col("confidence_class") == "high", 1).otherwise(0)).alias("high_conf_detections"),
            F.round(F.sum("frp_mw"), 1).alias("frp_total_mw"), F.round(F.max("frp_mw"), 1).alias("frp_max_mw"),
            F.round(F.sum(F.col("latitude") * w) / F.sum(w), 4).alias("centroid_lat"),
            F.round(F.sum(F.col("longitude") * w) / F.sum(w), 4).alias("centroid_lon"),
            F.min("latitude").alias("min_lat"), F.max("latitude").alias("max_lat"),
            F.min("longitude").alias("min_lon"), F.max("longitude").alias("max_lon"),
            F.array_sort(F.collect_set("county_fips")).alias("counties_burned"))
          .withColumn("duration_hours", F.round((F.unix_timestamp("last_seen_utc") - F.unix_timestamp("first_seen_utc")) / 3600, 1))
          .withColumn("size_class", F.when(F.col("detections") >= 500, "large").when(F.col("detections") >= 10, "medium")
                                     .when(F.col("detections") >= 2, "small").otherwise("single"))
          .join(peak, "event_id").join(main_county, "event_id")
          .join(spark.table(t("gold_dim_county")).select(F.col("county_fips").alias("primary_county_fips"),
                                                          F.col("county_name").alias("primary_county")), "primary_county_fips", "left")
          .withColumn("event_label", F.concat_ws(" ", F.coalesce("primary_county", F.lit("Unknown")), F.lit("fire, from"),
                                                 F.date_format("first_day", "d MMM yyyy")))
          .withColumn("_built_at_utc", F.current_timestamp()))
events.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_fire_events"))
log_step("gold_fire_events", "gold", spark.table(t("gold_fire_events")).count(), started)

# COMMAND ----------

# MAGIC %md ## 3 · Which counties could each event smoke?
# MAGIC For each event-day, every county whose centre lies within `impact_km` of the event's daily centroid.
# MAGIC `upwind_alignment` = cos(bearing from county to event − wind-from direction at the county): +1 means the event is
# MAGIC directly upwind (its smoke blows toward the county), −1 directly downwind. NULL when the county had no wind data
# MAGIC that day (station wind coverage is sparse; see the data-quality notebook).

# COMMAND ----------

started = time.time()
cty = spark.table(t("gold_dim_county")).select("county_fips", "county_name", "c_lat", "c_lon")
fact = spark.table(t("gold_fact_county_day")).select("county_fips", "date_local", "wind_dir_deg", "pm25_mean_ugm3")
nxt = fact.select("county_fips", F.date_sub("date_local", 1).alias("date_local"), F.col("pm25_mean_ugm3").alias("pm25_next_day_ugm3"))

def hav_km(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1); dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    return 2 * 6371.0 * F.asin(F.sqrt(a))

def bearing_deg(lat1, lon1, lat2, lon2):
    y = F.sin(F.radians(lon2 - lon1)) * F.cos(F.radians(lat2))
    x = F.cos(F.radians(lat1)) * F.sin(F.radians(lat2)) - F.sin(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.cos(F.radians(lon2 - lon1))
    return (F.degrees(F.atan2(y, x)) + 360) % 360

impact = (spark.table(t("gold_fire_event_day")).alias("e")
          .crossJoin(F.broadcast(cty).alias("c"))
          .withColumn("distance_km", F.round(hav_km(F.col("c.c_lat"), F.col("c.c_lon"), F.col("e.centroid_lat"), F.col("e.centroid_lon")), 1))
          .where(F.col("distance_km") <= IMPACT_KM)
          .withColumn("bearing_county_to_event", F.round(bearing_deg(F.col("c.c_lat"), F.col("c.c_lon"), F.col("e.centroid_lat"), F.col("e.centroid_lon")), 0))
          .select("e.event_id", "e.date_local", "c.county_fips", "c.county_name", "distance_km", "bearing_county_to_event",
                  F.col("e.frp_total_mw").alias("event_day_frp_mw"), F.col("e.detections").alias("event_day_detections"))
          .join(fact, ["county_fips", "date_local"], "left").join(nxt, ["county_fips", "date_local"], "left")
          .withColumn("upwind_alignment", F.round(F.cos(F.radians(F.col("bearing_county_to_event") - F.col("wind_dir_deg"))), 2))
          .withColumn("event_upwind", F.when(F.col("upwind_alignment").isNull(), F.lit(None).cast("boolean"))
                                       .otherwise(F.col("upwind_alignment") >= 0.5))
          .withColumn("_built_at_utc", F.current_timestamp()))
impact.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_fire_event_county_impact"))
log_step("gold_fire_event_county_impact", "gold", spark.table(t("gold_fire_event_county_impact")).count(), started)

# COMMAND ----------

# MAGIC %md ## 4 · Checks

# COMMAND ----------

n_det = spark.table(t("silver_fire_detections")).where(F.col("acq_ts_utc").isNotNull()).count()
n_mem = spark.table(t("silver_fire_event_members")).count()
n_dup = n_mem - spark.table(t("silver_fire_event_members")).select(*KEY).distinct().count()
ev = spark.table(t("gold_fire_events"))
print(f"detections {n_det:,} | assigned {n_mem:,} | assigned twice {n_dup} | events {ev.count():,}")
print("size classes:", {r.size_class: r["count"] for r in ev.groupBy("size_class").count().collect()})
assert n_det == n_mem and n_dup == 0, "every detection must belong to exactly one event"

# COMMAND ----------

# MAGIC %md ## 5 · Results (dashboard tiles)

# COMMAND ----------

# Largest events
display(spark.sql(f"""
SELECT event_id, event_label, first_day, last_day, active_days, detections, frp_total_mw, frp_max_mw,
       centroid_lat, centroid_lon, peak_day, size(counties_burned) AS counties_burned
FROM {t('gold_fire_events')} ORDER BY detections DESC LIMIT 15"""))

# COMMAND ----------

# Counties around the largest event: distance, wind alignment, PM2.5 that day and the next
display(spark.sql(f"""
WITH top AS (SELECT event_id FROM {t('gold_fire_events')} ORDER BY detections DESC LIMIT 1)
SELECT i.county_name, count(*) AS days_within_radius, round(min(i.distance_km), 0) AS closest_km,
       sum(CASE WHEN i.event_upwind THEN 1 ELSE 0 END) AS days_event_upwind,
       round(avg(i.pm25_mean_ugm3), 1) AS avg_pm25_same_day, round(avg(i.pm25_next_day_ugm3), 1) AS avg_pm25_next_day
FROM {t('gold_fire_event_county_impact')} i JOIN top USING (event_id)
GROUP BY i.county_name ORDER BY days_within_radius DESC, closest_km"""))

# COMMAND ----------

# Daily number of active events and total fire power (timeline)
display(spark.sql(f"""
SELECT date_local, count(DISTINCT event_id) AS active_events,
       sum(CASE WHEN detections >= 50 THEN 1 ELSE 0 END) AS events_with_50plus_detections,
       round(sum(frp_total_mw), 0) AS total_frp_mw
FROM {t('gold_fire_event_day')} GROUP BY date_local ORDER BY date_local"""))

# COMMAND ----------

# Does PM2.5 depend on whether an event is upwind? (county-days with wind data only)
display(spark.sql(f"""
SELECT CASE WHEN event_upwind THEN 'event upwind' ELSE 'event not upwind' END AS wind_relation,
       count(*) AS county_event_days, round(avg(pm25_mean_ugm3), 1) AS avg_pm25_same_day,
       round(avg(pm25_next_day_ugm3), 1) AS avg_pm25_next_day
FROM {t('gold_fire_event_county_impact')}
WHERE event_upwind IS NOT NULL AND event_day_frp_mw >= 1000
GROUP BY 1"""))
