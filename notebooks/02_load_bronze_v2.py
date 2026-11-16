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
from logging_utils import store_log
from pyspark import StorageLevel
from pyspark.sql.types import ArrayType, StructType

SOURCE_FILE = "02_load_bronze_v2"

def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)

dbutils.widgets.text("only_date", "", "Only ingest files for this date (YYYY-MM-DD, blank = all)")
ONLY_DATE = dbutils.widgets.get("only_date").strip()
BATCH_INGESTED_AT = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
BATCH_ID = BATCH_INGESTED_AT.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
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

SEEN_MANIFEST = already_loaded()

def new_files(source, ext):
    folder = LANDING[source]
    out = []
    for name in sorted(os.listdir(folder)):
        if not name.endswith(ext):
            continue
        m = DATE_RE.search(name)
        if not m or (ONLY_DATE and m.group(1) != ONLY_DATE):
            continue
        path = f"{folder}/{name}"
        h = file_hash(path)
        if (name, h) in SEEN_MANIFEST:
            continue
        out.append((path, name, m.group(1), h))
    return out

def file_metadata_df(files):
    return spark.createDataFrame(
        [
            (path, name, dt.date.fromisoformat(data_date), content_hash)
            for path, name, data_date, content_hash in files
        ],
        "file_path string, file_name string, data_date date, content_hash string",
    ).select(
        F.col("file_name").alias("_meta_file_name"),
        F.col("file_path").alias("_meta_file_path"),
        F.col("data_date").alias("_meta_data_date"),
        F.col("content_hash").alias("_meta_content_hash"),
    )

def add_batch_metadata(df, source):
    metadata_columns = {
        "_meta_file_name",
        "_meta_file_path",
        "_meta_data_date",
        "_meta_content_hash",
    }
    payload_columns = [c for c in df.columns if c not in metadata_columns]
    return df.select(
        *[F.col(c).cast("string").alias(c) for c in payload_columns],
        F.lit(source).alias("_source"),
        F.col("_meta_file_path").alias("_source_file"),
        F.col("_meta_data_date").alias("_data_date"),
        F.current_timestamp().alias("_ingested_at_utc"),
        F.lit(BATCH_ID).alias("_batch_id"),
        F.col("_meta_content_hash").alias("_content_hash"),
    )

def record_manifest_batch(source, count_rows):
    if not count_rows:
        return
    manifest_rows = [
        (
            source,
            row["_meta_file_path"],
            row["_meta_file_name"],
            row["_meta_data_date"],
            row["_meta_content_hash"],
            int(row["rows_loaded"]),
            BATCH_INGESTED_AT,
            BATCH_ID,
        )
        for row in count_rows
    ]
    spark.createDataFrame(
        manifest_rows,
        "source string, file_path string, file_name string, data_date date, content_hash string, rows_loaded long, ingested_at_utc timestamp, batch_id string",
    ).write.mode("append").saveAsTable(t("ops_ingest_manifest"))

def append_bronze(df, table):
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(t(table))

def load_csv_batch(source_key, extension, manifest_source, bronze_source, bronze_table):
    files = new_files(source_key, extension)
    if not files:
        return 0, 0

    metadata = file_metadata_df(files)
    raw = (
        spark.read.option("header", True).option("inferSchema", False)
        .csv([path for path, _, _, _ in files])
        .withColumn("_meta_file_name", F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    counts = (
        raw.groupBy("_meta_file_name").count()
        .withColumnRenamed("count", "rows_loaded")
        .withColumn("rows_loaded", F.col("rows_loaded").cast("long"))
    )
    count_df = (
        metadata.join(counts, "_meta_file_name", "left")
        .fillna(0, subset=["rows_loaded"])
    )
    count_rows = count_df.collect()
    total = sum(int(row["rows_loaded"]) for row in count_rows)
    empty_files = [row["_meta_file_name"] for row in count_rows if int(row["rows_loaded"]) == 0]
    if empty_files:
        write_notebook_log("WARNING", f"{len(empty_files):,} {source_key} file(s) contained no records: {empty_files[:20]}")

    if total:
        enriched = raw.join(F.broadcast(metadata), "_meta_file_name", "inner")
        append_bronze(add_batch_metadata(enriched, bronze_source), bronze_table)

    record_manifest_batch(manifest_source, count_rows)
    raw.unpersist()
    return total, len(files)

def load_json_array_batch(source_key, extension, array_field, manifest_source, bronze_source, bronze_table):
    files = new_files(source_key, extension)
    if not files:
        return 0, 0

    metadata = file_metadata_df(files)
    raw = (
        spark.read.option("multiLine", True)
        .json([path for path, _, _, _ in files])
        .withColumn("_meta_file_name", F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    data_type = raw.schema[array_field].dataType if array_field in raw.columns else None
    valid_array = isinstance(data_type, ArrayType) and isinstance(data_type.elementType, StructType)

    if valid_array:
        counts = raw.select(
            "_meta_file_name",
            F.coalesce(F.size(F.col(array_field)), F.lit(0)).cast("long").alias("rows_loaded"),
        )
        count_df = (
            metadata.join(counts, "_meta_file_name", "left")
            .fillna(0, subset=["rows_loaded"])
        )
    else:
        count_df = metadata.withColumn("rows_loaded", F.lit(0).cast("long"))

    count_rows = count_df.collect()
    total = sum(int(row["rows_loaded"]) for row in count_rows)
    empty_files = [row["_meta_file_name"] for row in count_rows if int(row["rows_loaded"]) == 0]
    if empty_files:
        severity = "WARNING" if valid_array else "ERROR"
        write_notebook_log(severity, f"{len(empty_files):,} {source_key} file(s) contained no records or had an invalid {array_field} array: {empty_files[:20]}")

    if valid_array and total:
        data_rows = (
            raw.where(F.size(F.col(array_field)) > 0)
            .select("_meta_file_name", F.explode(F.col(array_field)).alias("r"))
            .select("_meta_file_name", "r.*")
        )
        enriched = data_rows.join(F.broadcast(metadata), "_meta_file_name", "inner")
        append_bronze(add_batch_metadata(enriched, bronze_source), bronze_table)

    record_manifest_batch(manifest_source, count_rows)
    raw.unpersist()
    return total, len(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## NASA FIRMS (CSV)

# COMMAND ----------

started = time.time()
total, files_loaded = load_csv_batch("firms", ".csv", "nasa_firms", "nasa_firms", "bronze_firms")
log_step("bronze_firms", "bronze", total, started, {"batch_id": BATCH_ID, "files_loaded": files_loaded})

# COMMAND ----------

# MAGIC %md
# MAGIC ## EPA AQS daily PM2.5 (JSON API response: `{"Header": [...], "Data": [...]}`)

# COMMAND ----------

started = time.time()
total, files_loaded = load_json_array_batch("epa", ".json", "Data", "epa_aqs", "epa_aqs", "bronze_epa_pm25")
log_step("bronze_epa", "bronze", total, started, {"batch_id": BATCH_ID, "files_loaded": files_loaded})

# COMMAND ----------

# MAGIC %md
# MAGIC ## NOAA GHCN-Daily (JSON: `{"results": [...]}`, pages already combined by the downloader)

# COMMAND ----------

started = time.time()
total, files_loaded = load_json_array_batch("noaa", ".json", "results", "noaa_cdo", "noaa_cdo", "bronze_noaa_daily")
log_step("bronze_noaa", "bronze", total, started, {"batch_id": BATCH_ID, "files_loaded": files_loaded})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data (overwritten each run: small, slowly changing)

# COMMAND ----------

counties = spark.read.option("header", True).csv(LANDING["counties"])
counties.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("bronze_ref_counties"))

stations = (spark.read.option("multiLine", True).json(LANDING["noaa_stations"])
                 .select(F.explode("results").alias("r")).select("r.*"))
stations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("bronze_ref_noaa_stations"))

# COMMAND ----------

write_notebook_log("INFO", f"Bronze ingestion completed for batch {BATCH_ID}; per-source counts are recorded in the operational tables")
