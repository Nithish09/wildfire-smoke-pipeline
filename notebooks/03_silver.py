# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: cast, validate, quarantine, deduplicate, MERGE
# MAGIC For each source:
# MAGIC 1. Take only the Bronze rows that arrived since the last Silver run (watermark on `_ingested_at_utc`).
# MAGIC 2. Cast types, standardise units and times (UTC timestamp + local California date).
# MAGIC 3. Apply validation rules. Failing rows go to `silver_quarantine` with a reason, not to Silver.
# MAGIC 4. Deduplicate inside the batch on the business key (latest version wins).
# MAGIC 5. `MERGE` into the Silver table on the same key. A rerun of the same data changes nothing;
# MAGIC    a revised record (newer version) replaces the old one.
# MAGIC 6. Record which local dates changed, so Gold only rebuilds those days.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Reprocess all Bronze rows")
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('ops_watermark')}
              (source STRING, last_ingested_at_utc TIMESTAMP, updated_at_utc TIMESTAMP) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('ops_gold_pending_dates')}
              (date_local DATE, source STRING, queued_at_utc TIMESTAMP) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('silver_quarantine')} (
              source STRING, reason STRING, record_key STRING, record_json STRING,
              _source_file STRING, _ingested_at_utc TIMESTAMP, _batch_id STRING) USING DELTA""")

def incoming(bronze_table, source):
    """Bronze rows newer than the watermark for this source."""
    df = spark.table(t(bronze_table))
    if FULL_REFRESH:
        return df
    wm = (spark.table(t("ops_watermark")).where(F.col("source") == source)
               .agg(F.max("last_ingested_at_utc")).first()[0])
    return df if wm is None else df.where(F.col("_ingested_at_utc") > F.lit(wm))

def set_watermark(df, source):
    mx = df.agg(F.max("_ingested_at_utc")).first()[0]
    if mx is None:
        return
    spark.sql(f"DELETE FROM {t('ops_watermark')} WHERE source = '{source}'")
    spark.createDataFrame([(source, mx, dt.datetime.utcnow())],
                          "source string, last_ingested_at_utc timestamp, updated_at_utc timestamp") \
         .write.mode("append").saveAsTable(t("ops_watermark"))

def split_valid(df, rules, source, key_cols):
    """rules = list of (reason, condition_that_must_be_true). First failing rule is the reason."""
    reason = F.lit(None).cast("string")
    for name, ok in reversed(rules):
        reason = F.when(~F.coalesce(ok, F.lit(False)), F.lit(name)).otherwise(reason)
    tagged = df.withColumn("_dq_reason", reason)
    bad = (tagged.where(F.col("_dq_reason").isNotNull())
                 .select(F.lit(source).alias("source"), F.col("_dq_reason").alias("reason"),
                         F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_cols]).alias("record_key"),
                         F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("record_json"),
                         "_source_file", "_ingested_at_utc", "_batch_id"))
    good = tagged.where(F.col("_dq_reason").isNull()).drop("_dq_reason")
    return good, bad

def write_quarantine(bad):
    # MERGE so a rerun does not add the same bad record twice
    tgt = DeltaTable.forName(spark, t("silver_quarantine"))
    (tgt.alias("q").merge(bad.dropDuplicates(["source", "record_key", "reason", "_source_file"]).alias("b"),
        "q.source = b.source AND q.record_key = b.record_key AND q.reason = b.reason AND q._source_file = b._source_file")
        .whenNotMatchedInsertAll().execute())

def latest_per_key(df, key_cols, order_cols):
    """Keep one row per key. order_cols: column names (sorted newest first) or ready-made sort Columns."""
    order = [F.col(c).desc_nulls_last() if isinstance(c, str) else c for c in order_cols]
    w = Window.partitionBy(*key_cols).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

def merge_into(df, table, key_cols, newer_condition):
    if not spark.catalog.tableExists(t(table)):
        df.write.format("delta").saveAsTable(t(table))
        return
    cond = " AND ".join([f"s.{c} <=> n.{c}" for c in key_cols])
    (DeltaTable.forName(spark, t(table)).alias("s")
        .merge(df.alias("n"), cond)
        .whenMatchedUpdateAll(condition=newer_condition)
        .whenNotMatchedInsertAll()
        .execute())

def queue_dates(df, source):
    (df.select("date_local").distinct()
       .withColumn("source", F.lit(source)).withColumn("queued_at_utc", F.current_timestamp())
       .write.mode("append").saveAsTable(t("ops_gold_pending_dates")))

# COMMAND ----------

# MAGIC %md ## County dimension + nearest-county lookup
# MAGIC FIRMS points and NOAA stations carry only coordinates. Each is assigned to the county whose internal point
# MAGIC (Census gazetteer `INTPTLAT/INTPTLONG`) is nearest by great-circle distance. This is an approximation near borders
# MAGIC (see limitations); a polygon join would be exact but needs a geometry library.

# COMMAND ----------

dim_county = (spark.table(t("bronze_ref_counties"))
    .where(F.col("USPS") == "CA")
    .select(F.col("GEOID").alias("county_fips"),
            F.regexp_replace("NAME", " County$", "").alias("county_name"),
            F.col("INTPTLAT").cast("double").alias("c_lat"),
            F.col("INTPTLONG").cast("double").alias("c_lon"),
            F.col("ALAND_SQMI").cast("double").alias("land_sqmi")))
dim_county.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_dim_county"))
counties_b = F.broadcast(spark.table(t("silver_dim_county")))

def haversine_km(lat1, lon1, lat2, lon2):
    r = F.lit(6371.0)
    dlat = F.radians(lat2 - lat1); dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    return 2 * r * F.asin(F.sqrt(a))

def assign_county(df, lat_col, lon_col, key_cols):
    j = (df.crossJoin(counties_b.select("county_fips", "c_lat", "c_lon"))
           .withColumn("_d", haversine_km(F.col(lat_col), F.col(lon_col), F.col("c_lat"), F.col("c_lon"))))
    w = Window.partitionBy(*key_cols).orderBy("_d")
    return (j.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
             .withColumnRenamed("_d", "county_distance_km").drop("_rn", "c_lat", "c_lon"))

# COMMAND ----------

# MAGIC %md ## FIRMS → `silver_fire_detections`

# COMMAND ----------

started = time.time()
b = incoming("bronze_firms", "nasa_firms")
cols = set(b.columns)
bright = F.col("bright_ti4") if "bright_ti4" in cols else F.col("brightness")   # VIIRS vs MODIS naming
conf_raw = F.lower(F.trim(F.col("confidence")))
conf_num = conf_raw.cast("double")
fire = (b
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("acq_time_hhmm", F.lpad(F.trim(F.col("acq_time")), 4, "0"))
    .withColumn("acq_ts_utc", F.to_timestamp(F.concat_ws(" ", F.col("acq_date"), F.col("acq_time_hhmm")), "yyyy-MM-dd HHmm"))
    .withColumn("date_local", F.to_date(F.from_utc_timestamp("acq_ts_utc", LOCAL_TZ)))
    .withColumn("brightness_k", bright.cast("double"))
    .withColumn("frp_mw", F.col("frp").cast("double"))
    .withColumn("confidence_class",
        F.when(conf_raw.isin("l", "low"), "low").when(conf_raw.isin("n", "nominal"), "nominal")
         .when(conf_raw.isin("h", "high"), "high")
         .when(conf_num < 30, "low").when(conf_num <= 80, "nominal").when(conf_num > 80, "high"))
    .withColumn("satellite", F.upper(F.trim("satellite")))
    .withColumn("instrument", F.upper(F.trim("instrument")))
    .withColumn("daynight", F.upper(F.trim("daynight"))))

W, S, E, N = CA_BBOX
fire_key = ["latitude", "longitude", "acq_ts_utc", "satellite"]
fire_good, fire_bad = split_valid(fire, [
    ("missing_coordinates", F.col("latitude").isNotNull() & F.col("longitude").isNotNull()),
    ("outside_study_area", F.col("latitude").between(S, N) & F.col("longitude").between(W, E)),
    ("bad_timestamp", F.col("acq_ts_utc").isNotNull()),
    ("frp_out_of_range", F.col("frp_mw").isNull() | F.col("frp_mw").between(0, FRP_MAX_MW)),
    ("unknown_confidence", F.col("confidence_class").isNotNull()),
], "nasa_firms", fire_key)

fire_out = (latest_per_key(fire_good, fire_key, ["_ingested_at_utc"])
    .select(*fire_key, "date_local", "acq_date", "brightness_k", "frp_mw", "confidence_class",
            "instrument", "daynight", "_source_file", "_ingested_at_utc", "_batch_id"))
fire_out = assign_county(fire_out, "latitude", "longitude", fire_key)

write_quarantine(fire_bad)
merge_into(fire_out, "silver_fire_detections", fire_key, "n._ingested_at_utc > s._ingested_at_utc")
queue_dates(fire_out, "nasa_firms")
set_watermark(b, "nasa_firms")
log_step("silver_firms", "silver", fire_out.count(), started)

# COMMAND ----------

# MAGIC %md ## EPA → `silver_pm25_daily`
# MAGIC AQS returns one row per site-day **per pollutant standard**, so the same measurement can appear 2–3 times.
# MAGIC The key below drops `pollutant_standard` (and `event_type`) to remove those copies. `date_of_last_change` decides which revision wins.

# COMMAND ----------

started = time.time()
b = incoming("bronze_epa_pm25", "epa_aqs")
epa = (b
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

epa_key = ["site_id", "poc", "date_local", "sample_duration"]
epa_good, epa_bad = split_valid(epa, [
    ("wrong_parameter", F.col("parameter_code") == "88101"),
    ("wrong_state", F.lpad("state_code", 2, "0") == STATE_FIPS),
    ("bad_date", F.col("date_local").isNotNull()),
    ("missing_value", F.col("pm25_ugm3").isNotNull()),
    ("impossible_value", F.col("pm25_ugm3").between(PM25_MIN, PM25_MAX)),
    ("low_completeness", F.col("observation_percent").isNull() | (F.col("observation_percent") >= 75)),
], "epa_aqs", epa_key)

# On exceptional-event days (wildfires!) AQS sends both an "Included" and an "Excluded" row.
# We want smoke included, so prefer None/Included over Excluded, then the newest revision.
event_rank = F.when(F.col("event_type") == "Excluded", 1).otherwise(0).asc()
epa_out = (latest_per_key(epa_good, epa_key, [event_rank, "date_of_last_change", "_ingested_at_utc"])
    .select(*epa_key, "county_fips", "local_site_name", "latitude", "longitude", "pm25_ugm3", "pm25_first_max",
            "aqi", "observation_percent", "event_type", "method_code", "date_of_last_change",
            "_source_file", "_ingested_at_utc", "_batch_id"))

write_quarantine(epa_bad)
merge_into(epa_out, "silver_pm25_daily", epa_key,
           "NOT (n.event_type <=> 'Excluded' AND NOT s.event_type <=> 'Excluded') AND ("
           " n.date_of_last_change > s.date_of_last_change OR "
           " (n.date_of_last_change <=> s.date_of_last_change AND n._ingested_at_utc > s._ingested_at_utc "
           "  AND NOT (n.pm25_ugm3 <=> s.pm25_ugm3 AND n.aqi <=> s.aqi)))")
queue_dates(epa_out, "epa_aqs")
set_watermark(b, "epa_aqs")
log_step("silver_epa", "silver", epa_out.count(), started)

# COMMAND ----------

# MAGIC %md ## NOAA → `silver_weather_obs` (long) → `silver_weather_daily` (one row per station-day)
# MAGIC `attributes` is `mflag,qflag,sflag,time`. A non-empty **qflag** means the value failed NOAA's own QC, so it is quarantined.
# MAGIC Values are requested with `units=metric`: °C, mm, m/s, degrees.

# COMMAND ----------

started = time.time()
b = incoming("bronze_noaa_daily", "noaa_cdo")
stations = (spark.table(t("bronze_ref_noaa_stations"))
    .select(F.col("id").alias("station"), F.col("name").alias("station_name"),
            F.col("latitude").cast("double").alias("st_lat"), F.col("longitude").cast("double").alias("st_lon")))

wx = (b
    .withColumn("date_local", F.to_date(F.substring("date", 1, 10)))
    .withColumn("element", F.upper("datatype"))
    .withColumn("value", F.col("value").cast("double"))
    .withColumn("qflag", F.split(F.coalesce("attributes", F.lit("")), ",").getItem(1))
    .join(F.broadcast(stations), "station", "left"))

el = F.col("element"); v = F.col("value")
in_range = (F.when(el.isin("TMAX", "TMIN"), v.between(TEMP_MIN_C, TEMP_MAX_C))
             .when(el == "PRCP", v.between(0, 1000))
             .when(el.isin("AWND", "WSF2"), v.between(0, WIND_MAX_MS))
             .when(el == "WDF2", v.between(0, 360))
             .otherwise(F.lit(True)))
wx_key = ["station", "date_local", "element"]
wx_good, wx_bad = split_valid(wx, [
    ("unneeded_element", el.isin(NOAA_ELEMENTS)),
    ("bad_date", F.col("date_local").isNotNull()),
    ("missing_value", v.isNotNull()),
    ("failed_source_qc", F.coalesce(F.trim("qflag"), F.lit("")) == ""),
    ("impossible_value", in_range),
    ("unmatched_station", F.col("st_lat").isNotNull()),
], "noaa_cdo", wx_key)

wx_out = latest_per_key(wx_good, wx_key, ["_ingested_at_utc"]).select(
    *wx_key, "value", "station_name", "st_lat", "st_lon", "_source_file", "_ingested_at_utc", "_batch_id")
write_quarantine(wx_bad)
merge_into(wx_out, "silver_weather_obs", wx_key,
           "n._ingested_at_utc > s._ingested_at_utc AND NOT (n.value <=> s.value)")
queue_dates(wx_out, "noaa_cdo")
set_watermark(b, "noaa_cdo")

# Rebuild the wide station-day table for the affected dates only
dates = [r.date_local for r in wx_out.select("date_local").distinct().collect()]
if dates:
    obs = spark.table(t("silver_weather_obs")).where(F.col("date_local").isin(dates))
    wide = (obs.groupBy("station", "date_local", "station_name", "st_lat", "st_lon")
               .pivot("element", NOAA_ELEMENTS).agg(F.first("value"))
               .withColumnRenamed("TMAX", "tmax_c").withColumnRenamed("TMIN", "tmin_c")
               .withColumnRenamed("PRCP", "prcp_mm").withColumnRenamed("AWND", "awnd_ms")
               .withColumnRenamed("WDF2", "wdf2_deg").withColumnRenamed("WSF2", "wsf2_ms"))
    wide = assign_county(wide, "st_lat", "st_lon", ["station", "date_local"])
    if spark.catalog.tableExists(t("silver_weather_daily")):
        spark.sql(f"DELETE FROM {t('silver_weather_daily')} WHERE date_local IN ({','.join(repr(str(d)) for d in dates)})")
        wide.write.mode("append").saveAsTable(t("silver_weather_daily"))
    else:
        wide.write.saveAsTable(t("silver_weather_daily"))
log_step("silver_noaa", "silver", wx_out.count(), started)

# COMMAND ----------

display(spark.sql(f"SELECT source, reason, count(*) n FROM {t('silver_quarantine')} GROUP BY 1,2 ORDER BY 1,3 DESC"))
