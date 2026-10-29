# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: star schema + county-day fact table
# MAGIC * `gold_dim_county`, `gold_dim_date` – dimensions
# MAGIC * `gold_fact_county_day` – one row per California county per local date, with fire activity, PM2.5, weather
# MAGIC   and data-quality flags. Every county gets a row every day, so "no fires" is an explicit 0 and missing PM2.5 is NULL.
# MAGIC * `gold_kpi_state_day` – statewide daily KPIs for the dashboard.
# MAGIC
# MAGIC Only dates queued by Silver (`ops_gold_pending_dates`) are rebuilt, then `MERGE`d on (county_fips, date_local).
# MAGIC The smoke indicator is descriptive, **not** a causal or regulatory estimate of wildfire-attributed PM2.5.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("rebuild_all", "false", ["false", "true"], "Rebuild every date in the study window")
REBUILD_ALL = dbutils.widgets.get("rebuild_all") == "true"
NEAR_KM = 100.0     # fire detections within this distance of a county's centre count as "nearby"
PM25_DAILY_STANDARD = 35.0   # EPA 24-hour PM2.5 standard, ug/m3

started = time.time()
if REBUILD_ALL:
    days = [START_DATE + dt.timedelta(d) for d in range((END_DATE - START_DATE).days + 1)]
else:
    days = [r.date_local for r in spark.table(t("ops_gold_pending_dates")).select("date_local").distinct().collect()]
days = sorted(d for d in days if START_DATE <= d <= END_DATE)
print(f"Rebuilding {len(days)} date(s)")

# COMMAND ----------

# MAGIC %md ## Dimensions

# COMMAND ----------

spark.table(t("silver_dim_county")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_dim_county"))

(spark.sql(f"SELECT explode(sequence(DATE'{START_DATE}', DATE'{END_DATE}', INTERVAL 1 DAY)) AS date_local")
    .withColumn("date_key", F.date_format("date_local", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date_local")).withColumn("month", F.month("date_local"))
    .withColumn("day_of_week", F.date_format("date_local", "E"))
    .withColumn("is_weekend", F.dayofweek("date_local").isin(1, 7))
    .withColumn("study_week", (F.datediff("date_local", F.lit(START_DATE)) / 7).cast("int") + 1)
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_dim_date")))

# COMMAND ----------

# MAGIC %md ## Fact table for the pending dates

# COMMAND ----------

if days:
    county = spark.table(t("gold_dim_county"))
    grid = county.select("county_fips", "c_lat", "c_lon").crossJoin(
        spark.createDataFrame([(d,) for d in days], "date_local date"))

    fires = spark.table(t("silver_fire_detections")).where(F.col("date_local").isin(days))

    # fires assigned to the county itself
    fire_in = (fires.groupBy("county_fips", "date_local").agg(
        F.count("*").alias("fire_detections"),
        F.sum(F.when(F.col("confidence_class") == "high", 1).otherwise(0)).alias("fire_detections_high_conf"),
        F.round(F.sum("frp_mw"), 1).alias("fire_frp_total_mw"),
        F.round(F.max("frp_mw"), 1).alias("fire_frp_max_mw")))

    # fires within NEAR_KM of the county centre (smoke can travel; this captures neighbours too)
    def hav(lat1, lon1, lat2, lon2):
        dlat = F.radians(lat2 - lat1); dlon = F.radians(lon2 - lon1)
        a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
        return 2 * 6371.0 * F.asin(F.sqrt(a))
    fire_near = (fires.select("date_local", "latitude", "longitude", "frp_mw").alias("f")
        .crossJoin(F.broadcast(county.select("county_fips", "c_lat", "c_lon")).alias("c"))
        .where(hav(F.col("f.latitude"), F.col("f.longitude"), F.col("c.c_lat"), F.col("c.c_lon")) <= NEAR_KM)
        .groupBy("county_fips", "date_local").agg(
            F.count("*").alias("fire_detections_within_100km"),
            F.round(F.sum("frp_mw"), 1).alias("fire_frp_within_100km_mw")))

    # PM2.5: average POCs/durations per site first, then across sites, so a site with 3 monitors is not triple-weighted
    pm_site = (spark.table(t("silver_pm25_daily")).where(F.col("date_local").isin(days))
        .groupBy("county_fips", "site_id", "date_local")
        .agg(F.avg("pm25_ugm3").alias("site_pm25"), F.max("aqi").alias("site_aqi")))
    pm = pm_site.groupBy("county_fips", "date_local").agg(
        F.round(F.avg("site_pm25"), 2).alias("pm25_mean_ugm3"),
        F.round(F.max("site_pm25"), 2).alias("pm25_max_site_ugm3"),
        F.max("site_aqi").alias("aqi_max"),
        F.countDistinct("site_id").alias("pm25_sites"))

    # Weather: average stations in the county. Wind direction is averaged as a vector, not arithmetically.
    wx = (spark.table(t("silver_weather_daily")).where(F.col("date_local").isin(days))
        .groupBy("county_fips", "date_local").agg(
            F.round(F.avg("tmax_c"), 1).alias("tmax_c"), F.round(F.avg("tmin_c"), 1).alias("tmin_c"),
            F.round(F.avg("prcp_mm"), 1).alias("prcp_mm"), F.round(F.avg("awnd_ms"), 1).alias("wind_mean_ms"),
            F.avg(F.sin(F.radians("wdf2_deg"))).alias("_s"), F.avg(F.cos(F.radians("wdf2_deg"))).alias("_c"),
            F.countDistinct("station").alias("weather_stations"))
        .withColumn("wind_dir_deg", F.round((F.degrees(F.atan2("_s", "_c")) + 360) % 360, 0))
        .drop("_s", "_c"))

    keys = ["county_fips", "date_local"]
    fact = (grid.drop("c_lat", "c_lon")
        .join(fire_in, keys, "left").join(fire_near, keys, "left").join(pm, keys, "left").join(wx, keys, "left")
        .fillna(0, subset=["fire_detections", "fire_detections_high_conf", "fire_frp_total_mw",
                           "fire_detections_within_100km", "fire_frp_within_100km_mw"])
        .withColumn("date_key", F.date_format("date_local", "yyyyMMdd").cast("int"))
        .withColumn("pm25_exceeds_daily_standard", F.col("pm25_mean_ugm3") > PM25_DAILY_STANDARD)
        .withColumn("smoke_indicator",
            F.when(F.col("pm25_mean_ugm3").isNull(), "no_pm25_data")
             .when((F.col("pm25_mean_ugm3") > PM25_DAILY_STANDARD) & (F.col("fire_detections_within_100km") > 0), "likely_smoke")
             .when(F.col("pm25_mean_ugm3") > PM25_DAILY_STANDARD, "high_pm25_no_nearby_fire")
             .when(F.col("fire_detections_within_100km") > 0, "nearby_fire_pm25_ok")
             .otherwise("clear"))
        .withColumn("has_pm25", F.col("pm25_sites").isNotNull())
        .withColumn("has_weather", F.col("weather_stations").isNotNull())
        .withColumn("sources_present", 1 + F.col("has_pm25").cast("int") + F.col("has_weather").cast("int"))
        .withColumn("_gold_built_at_utc", F.current_timestamp()))

    if not spark.catalog.tableExists(t("gold_fact_county_day")):
        fact.write.format("delta").saveAsTable(t("gold_fact_county_day"))
    else:
        # update only when a measure actually changed, so reruns leave the table untouched
        measures = [c for c in fact.columns if c not in keys + ["_gold_built_at_utc"]]
        changed = " OR ".join([f"NOT (g.{c} <=> n.{c})" for c in measures])
        (DeltaTable.forName(spark, t("gold_fact_county_day")).alias("g")
            .merge(fact.alias("n"), "g.county_fips = n.county_fips AND g.date_local = n.date_local")
            .whenMatchedUpdateAll(condition=changed)
            .whenNotMatchedInsertAll().execute())

    # statewide KPIs
    (spark.table(t("gold_fact_county_day")).groupBy("date_local").agg(
        F.sum("fire_detections").alias("ca_fire_detections"),
        F.round(F.sum("fire_frp_total_mw"), 0).alias("ca_fire_frp_mw"),
        F.round(F.avg("pm25_mean_ugm3"), 2).alias("avg_county_pm25_ugm3"),
        F.sum(F.col("pm25_exceeds_daily_standard").cast("int")).alias("counties_over_standard"),
        F.sum(F.col("has_pm25").cast("int")).alias("counties_with_pm25"),
        F.sum((F.col("smoke_indicator") == "likely_smoke").cast("int")).alias("counties_likely_smoke"))
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_kpi_state_day")))

    spark.sql(f"DELETE FROM {t('ops_gold_pending_dates')} WHERE date_local IN ({','.join(repr(str(d)) for d in days)})")
    spark.sql(f"OPTIMIZE {t('gold_fact_county_day')} ZORDER BY (date_local, county_fips)")

log_step("gold_fact_county_day", "gold", spark.table(t("gold_fact_county_day")).count() if days else 0, started,
         {"dates_rebuilt": len(days)})

# COMMAND ----------

display(spark.sql(f"""
SELECT f.date_local, c.county_name, f.fire_detections_within_100km, f.pm25_mean_ugm3, f.wind_mean_ms, f.smoke_indicator
FROM {t('gold_fact_county_day')} f JOIN {t('gold_dim_county')} c USING (county_fips)
ORDER BY f.pm25_mean_ugm3 DESC NULLS LAST LIMIT 20"""))
