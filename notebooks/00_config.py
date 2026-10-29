# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config
# MAGIC Shared settings for every notebook. Every other notebook loads this one with `%run ./00_config`.
# MAGIC
# MAGIC **Before the first run:** change `CATALOG` / `SCHEMA` if needed, then run `01_setup` once to create the schema and volume.

# COMMAND ----------

import datetime as dt
import time
import json
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

# --- Where things live (Databricks Free Edition uses the "workspace" catalog) ---
CATALOG = "workspace"
SCHEMA = "wildfire"
VOLUME = "raw"
RAW_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"   # landing zone for downloaded files

# Landing folders, one per source and format. Files are named <source>_<YYYY-MM-DD>.<ext>
LANDING = {
    "firms": f"{RAW_ROOT}/firms",          # CSV from the FIRMS area API
    "epa":   f"{RAW_ROOT}/epa",            # JSON from the AQS dailyData API
    "noaa":  f"{RAW_ROOT}/noaa",           # JSON from the NOAA CDO v2 data API
    "noaa_stations": f"{RAW_ROOT}/reference/noaa_stations.json",
    "counties":      f"{RAW_ROOT}/reference/ca_counties.csv",   # Census gazetteer, CA rows
    "epa_csv_bulk":  f"{RAW_ROOT}/epa_bulk",  # EPA pre-generated CSV (for the CSV vs API benchmark)
}

# --- Study scope (historical replay of the 2020 fire season) ---
STATE_FIPS = "06"                          # California
CA_BBOX = (-124.5, 32.5, -114.1, 42.0)     # west, south, east, north
START_DATE = dt.date(2020, 8, 15)
END_DATE = dt.date(2020, 9, 30)
LOCAL_TZ = "America/Los_Angeles"

# --- Validation thresholds (documented in docs/data_contract.md) ---
PM25_MIN, PM25_MAX = 0.0, 1000.0           # ug/m3; negative or > 1000 is treated as impossible
TEMP_MIN_C, TEMP_MAX_C = -40.0, 60.0
WIND_MAX_MS = 75.0
FRP_MAX_MW = 20000.0
NOAA_ELEMENTS = ["TMAX", "TMIN", "PRCP", "AWND", "WDF2", "WSF2"]

def t(name):
    """Fully qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"

# --- Step timing log, used for the benchmark (RQ2) ---
def log_step(step, layer, rows, started, extra=None):
    elapsed = time.time() - started
    row = [(step, layer, int(rows), float(elapsed), dt.datetime.utcnow(), json.dumps(extra or {}))]
    (spark.createDataFrame(row, "step string, layer string, rows long, seconds double, logged_at_utc timestamp, extra string")
          .write.mode("append").saveAsTable(t("ops_step_log")))
    print(f"[{layer}] {step}: {rows:,} rows in {elapsed:.1f}s")

print(f"Config loaded: {CATALOG}.{SCHEMA}, raw files in {RAW_ROOT}")
