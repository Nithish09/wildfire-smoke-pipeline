# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extract NASA FIRMS
# MAGIC
# MAGIC Downloads California fire detections from NASA FIRMS using the
# MAGIC VIIRS_SNPP_NRT product and writes one CSV file per acquisition date:
# MAGIC
# MAGIC `/Volumes/workspace/wildfire/raw/firms/firms_YYYY-MM-DD.csv`
# MAGIC
# MAGIC By default, the notebook continues after the newest existing nonempty
# MAGIC FIRMS file. An optional start_date widget can override that behavior for
# MAGIC a backfill or restart; leave it blank, null, or none to use the default.
# MAGIC It requests at most 90 days per run, but never requests dates newer
# MAGIC than five days before the current date in California time.
# MAGIC
# MAGIC A NASA FIRMS MAP_KEY is required. Enter it as a widget or Job parameter;
# MAGIC do not hard-code it in this notebook.

# COMMAND ----------

from datetime import date, datetime, timedelta
import csv
import hashlib
import io
import json
import os
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

from pyspark.sql import types as T
from logging_utils import store_log

SOURCE_FILE = "01_extract_firms_v2"

def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)

CATALOG = "workspace"
SCHEMA = "wildfire"
RAW_FIRMS_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw/firms"

FIRMS_ENDPOINT = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
FIRMS_SOURCE = "VIIRS_SNPP_NRT"
CA_BBOX = (-124.5, 32.5, -114.1, 42.0)  # west, south, east, north
LOCAL_TZ = ZoneInfo("America/Los_Angeles")
MAX_API_DAYS = 5  # FIRMS Area API limit

# COMMAND ----------

dbutils.widgets.text("firms_map_key", "", "NASA FIRMS MAP_KEY")
dbutils.widgets.text("initial_date", "2020-08-15", "Initial date if no valid files exist")
dbutils.widgets.text("start_date", "", "Optional start-date override (YYYY-MM-DD)")
dbutils.widgets.text("max_days", "90", "Maximum number of new days to request")
dbutils.widgets.text("request_timeout_seconds", "120", "HTTP timeout")

MAP_KEY = dbutils.widgets.get("firms_map_key").strip()
INITIAL_DATE_TEXT = dbutils.widgets.get("initial_date").strip()
START_DATE_TEXT = dbutils.widgets.get("start_date").strip()
MAX_DAYS = int(dbutils.widgets.get("max_days").strip())
TIMEOUT = int(dbutils.widgets.get("request_timeout_seconds").strip())

if not MAP_KEY:
    raise ValueError(
        "firms_map_key is required. Request a free NASA FIRMS MAP_KEY and "
        "enter it as a widget or Job parameter."
    )
if not INITIAL_DATE_TEXT:
    raise ValueError("initial_date cannot be blank when no valid FIRMS files exist")

def parse_optional_date(value, name):
    normalized = value.strip().lower()
    if normalized in {"", "null", "none"}:
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, blank, null, or none") from exc

START_DATE_OVERRIDE = parse_optional_date(START_DATE_TEXT, "start_date")
if MAX_DAYS < 1 or MAX_DAYS > 90:
    raise ValueError("max_days must be between 1 and 90")

local_today = datetime.now(LOCAL_TZ).date()
max_available_date = local_today - timedelta(days=5)

# COMMAND ----------

def sha256_bytes(payload):
    return hashlib.sha256(payload).hexdigest()


def latest_nonempty_firms_date(folder):
    pattern = re.compile(r"^firms_(\d{4}-\d{2}-\d{2})\.csv$")
    candidates = []
    if not os.path.isdir(folder):
        return None

    for name in os.listdir(folder):
        match = pattern.match(name)
        if not match:
            continue
        path = os.path.join(folder, name)
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as file:
                rows = list(csv.DictReader(file))
            if rows:
                candidates.append(date.fromisoformat(match.group(1)))
        except (OSError, ValueError, csv.Error):
            write_notebook_log("WARNING", f"Ignoring unreadable or invalid FIRMS file: {path}")
    return max(candidates) if candidates else None


def fetch_firms_chunk(start_day, day_count):
    west, south, east, north = CA_BBOX
    bbox = ",".join(str(x) for x in (west, south, east, north))
    url = (
        f"{FIRMS_ENDPOINT}/{urllib.parse.quote(MAP_KEY, safe='')}/"
        f"{FIRMS_SOURCE}/{bbox}/{day_count}/{start_day.isoformat()}"
    )
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Databricks-Free-Edition-wildfire-project/1.0"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        http_code = response.getcode()
        raw = response.read()
    return raw, http_code, url


def serialize_daily_rows(fieldnames, rows):
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fieldnames})
    return output.getvalue().encode("utf-8")


def write_if_changed(path, raw):
    new_hash = sha256_bytes(raw)
    if os.path.exists(path):
        with open(path, "rb") as existing:
            if sha256_bytes(existing.read()) == new_hash:
                return False

    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix="firms_", suffix=".csv", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "wb") as temp_file:
            temp_file.write(raw)
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    return True


# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
dbutils.fs.mkdirs(RAW_FIRMS_ROOT)

latest_date = latest_nonempty_firms_date(RAW_FIRMS_ROOT)
default_start_date = (
    latest_date + timedelta(days=1)
    if latest_date is not None
    else date.fromisoformat(INITIAL_DATE_TEXT)
)
start_date = START_DATE_OVERRIDE or default_start_date
end_date = min(start_date + timedelta(days=MAX_DAYS - 1), max_available_date)

write_notebook_log(
    "INFO",
    f"FIRMS extraction window starts {start_date}; latest existing nonempty date={latest_date or 'none'}",
)
if start_date <= end_date:
    write_notebook_log("INFO", f"Requested dates: {start_date} through {end_date}")
else:
    write_notebook_log("INFO", f"No request needed: next date {start_date} exceeds {max_available_date}")

results = []
current = start_date
while current <= end_date:
    chunk_days = min(MAX_API_DAYS, (end_date - current).days + 1)
    try:
        raw, http_code, url = fetch_firms_chunk(current, chunk_days)
        text = raw.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        fieldnames = reader.fieldnames or []
        rows = list(reader)

        if not fieldnames or not rows:
            write_notebook_log("WARNING", f"FIRMS response body: {text[:4000]}")
            results.append((str(current), chunk_days, http_code, 0, 0, "no_data", ""))
        else:
            rows_by_date = {}
            for row in rows:
                row_date = (row.get("acq_date") or "").strip()
                if row_date:
                    rows_by_date.setdefault(row_date, []).append(row)

            for day_text, day_rows in sorted(rows_by_date.items()):
                day_rows.sort(
                    key=lambda row: (
                        row.get("acq_time", ""),
                        row.get("latitude", ""),
                        row.get("longitude", ""),
                        row.get("satellite", ""),
                    )
                )
                destination = f"{RAW_FIRMS_ROOT}/firms_{day_text}.csv"
                daily_raw = serialize_daily_rows(fieldnames, day_rows)
                changed = write_if_changed(destination, daily_raw)
                results.append(
                    (day_text, chunk_days, http_code, len(day_rows), int(changed), "success", "")
                )
        current += timedelta(days=chunk_days)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:4000]
        write_notebook_log("ERROR", f"FIRMS HTTP {exc.code}: {body}")
        results.append((str(current), chunk_days, exc.code, 0, 0, "http_error", body))
        current += timedelta(days=chunk_days)
    except urllib.error.URLError as exc:
        message = str(exc.reason)
        write_notebook_log("ERROR", f"FIRMS network error: {message}")
        results.append((str(current), chunk_days, 0, 0, 0, "network_error", message))
        current += timedelta(days=chunk_days)
    except Exception as exc:
        write_notebook_log("ERROR", f"FIRMS error: {exc}")
        results.append((str(current), chunk_days, 0, 0, 0, "error", str(exc)))
        current += timedelta(days=chunk_days)

# COMMAND ----------

schema = T.StructType([
    T.StructField("date", T.StringType(), False),
    T.StructField("requested_days", T.IntegerType(), False),
    T.StructField("http_code", T.IntegerType(), False),
    T.StructField("records", T.IntegerType(), False),
    T.StructField("changed", T.IntegerType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("error", T.StringType(), False),
])
summary = spark.createDataFrame(results, schema)
errors = summary.where(summary.status.isin("http_error", "network_error", "error")).count()
if errors:
    error_message = f"{errors} FIRMS request chunk(s) failed; inspect the log table."
    write_notebook_log("ERROR", error_message)
    raise RuntimeError(error_message)
else:
    write_notebook_log("INFO", f"FIRMS extraction completed with {len(results)} result row(s).")
