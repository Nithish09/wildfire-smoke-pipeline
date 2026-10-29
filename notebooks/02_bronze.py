# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: land raw files as Delta
# MAGIC * Reads every file in the landing folders (`firms/*.csv`, `epa/*.json`, `noaa/*.json`).
# MAGIC * Keeps every source value **as a string** (no cleaning here), and adds metadata:
# MAGIC   `_source`, `_source_file`, `_data_date` (the day the data is for), `_ingested_at_utc`, `_batch_id`, `_content_hash`.
# MAGIC * **Incremental:** a file whose name and content hash are already in `ops_ingest_manifest` is skipped.
# MAGIC   A file with the same name but a new hash (the source revised that day) is loaded again as a new version;
# MAGIC   Silver then keeps the latest version.
# MAGIC * Bronze is append-only, so the full history of what was received stays auditable.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import hashlib, os, re, uuid

dbutils.widgets.text("only_date", "", "Only ingest files for this date (YYYY-MM-DD, blank = all)")
ONLY_DATE = dbutils.widgets.get("only_date").strip()
BATCH_ID = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def already_loaded():
    if not spark.catalog.tableExists(t("ops_ingest_manifest")):
        return set()
    return {(r.file_name, r.content_hash) for r in spark.table(t("ops_ingest_manifest")).select("file_name", "content_hash").collect()}

def new_files(source, ext):
    folder = LANDING[source]
    seen = already_loaded()
    out = []
    for name in sorted(os.listdir(folder)):
        if not name.endswith(ext):
            continue
        m = DATE_RE.search(name)
        if not m or (ONLY_DATE and m.group(1) != ONLY_DATE):
            continue
        path = f"{folder}/{name}"
        h = file_hash(path)
        if (name, h) in seen:
            print(f"skip (already loaded): {name}")
            continue
        out.append((path, name, m.group(1), h))
    return out

def add_metadata(df, source, path, data_date, h):
    return (df.select([F.col(c).cast("string").alias(c) for c in df.columns])
              .withColumn("_source", F.lit(source))
              .withColumn("_source_file", F.lit(path))
              .withColumn("_data_date", F.to_date(F.lit(data_date)))
              .withColumn("_ingested_at_utc", F.current_timestamp())
              .withColumn("_batch_id", F.lit(BATCH_ID))
              .withColumn("_content_hash", F.lit(h)))

def record_manifest(source, path, name, data_date, h, rows):
    spark.createDataFrame(
        [(source, path, name, dt.date.fromisoformat(data_date), h, int(rows), dt.datetime.utcnow(), BATCH_ID)],
        "source string, file_path string, file_name string, data_date date, content_hash string, rows_loaded long, ingested_at_utc timestamp, batch_id string",
    ).write.mode("append").saveAsTable(t("ops_ingest_manifest"))

def append_bronze(df, table):
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(t(table))

# COMMAND ----------

# MAGIC %md ## NASA FIRMS (CSV)

# COMMAND ----------

started = time.time(); total = 0
for path, name, d, h in new_files("firms", ".csv"):
    raw = spark.read.option("header", True).option("inferSchema", False).csv(path)
    df = add_metadata(raw, "nasa_firms", path, d, h)
    n = df.count()
    append_bronze(df, "bronze_firms")
    record_manifest("nasa_firms", path, name, d, h, n)
    total += n
log_step("bronze_firms", "bronze", total, started, {"batch_id": BATCH_ID})

# COMMAND ----------

# MAGIC %md ## EPA AQS daily PM2.5 (JSON API response: `{"Header": [...], "Data": [...]}`)

# COMMAND ----------

started = time.time(); total = 0
for path, name, d, h in new_files("epa", ".json"):
    raw = spark.read.option("multiLine", True).json(path)
    from pyspark.sql.types import ArrayType, StructType
    dtype = raw.schema["Data"].dataType if "Data" in raw.columns else None
    if not (isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType)):
        # AQS answers "No data matched your selection" with an empty Data array
        print(f"no records in {name}; recorded with 0 rows"); record_manifest("epa_aqs", path, name, d, h, 0); continue
    rows = raw.select(F.explode("Data").alias("r")).select("r.*")
    df = add_metadata(rows, "epa_aqs", path, d, h)
    n = df.count()
    append_bronze(df, "bronze_epa_pm25")
    record_manifest("epa_aqs", path, name, d, h, n)
    total += n
log_step("bronze_epa", "bronze", total, started, {"batch_id": BATCH_ID})

# COMMAND ----------

# MAGIC %md ## NOAA GHCN-Daily (JSON: `{"results": [...]}`, pages already combined by the downloader)

# COMMAND ----------

started = time.time(); total = 0
for path, name, d, h in new_files("noaa", ".json"):
    raw = spark.read.option("multiLine", True).json(path)
    from pyspark.sql.types import ArrayType, StructType
    dtype = raw.schema["results"].dataType if "results" in raw.columns else None
    if not (isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType)):
        record_manifest("noaa_cdo", path, name, d, h, 0); continue
    rows = raw.select(F.explode("results").alias("r")).select("r.*")
    df = add_metadata(rows, "noaa_cdo", path, d, h)
    n = df.count()
    append_bronze(df, "bronze_noaa_daily")
    record_manifest("noaa_cdo", path, name, d, h, n)
    total += n
log_step("bronze_noaa", "bronze", total, started, {"batch_id": BATCH_ID})

# COMMAND ----------

# MAGIC %md ## Reference data (overwritten each run: small, slowly changing)

# COMMAND ----------

counties = spark.read.option("header", True).csv(LANDING["counties"])
counties.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("bronze_ref_counties"))

stations = (spark.read.option("multiLine", True).json(LANDING["noaa_stations"])
                 .select(F.explode("results").alias("r")).select("r.*"))
stations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("bronze_ref_noaa_stations"))

display(spark.table(t("ops_ingest_manifest")).orderBy(F.desc("ingested_at_utc")).limit(20))
