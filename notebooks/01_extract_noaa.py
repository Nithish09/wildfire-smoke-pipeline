# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extract NOAA GHCND daily weather
# MAGIC
# MAGIC Downloads NOAA Climate Data Online daily summaries for California and
# MAGIC writes one JSON file per observation date:
# MAGIC
# MAGIC `/Volumes/workspace/wildfire/raw/noaa/noaa_YYYY-MM-DD.json`
# MAGIC
# MAGIC The notebook uses the GHCND dataset and the weather elements already
# MAGIC expected by the Silver notebook:
# MAGIC TMAX, TMIN, PRCP, AWND, WDF2 and WSF2.
# MAGIC
# MAGIC It continues after the newest existing nonempty NOAA file. It requests at
# MAGIC most 90 days per run and allows requests through the current date in
# MAGIC California time. Dates not yet published by NOAA are not written and are
# MAGIC retried on a later run.
# MAGIC
# MAGIC A NOAA CDO API token is required. Enter it as a widget or Job parameter;
# MAGIC do not hard-code it in this notebook.

# COMMAND ----------

from datetime import date, datetime, timedelta
import hashlib
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

SOURCE_FILE = "01_extract_noaa"

def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)

CATALOG = "workspace"
SCHEMA = "wildfire"
RAW_NOAA_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw/noaa"

NOAA_ENDPOINT = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
DATASET_ID = "GHCND"
STATE_FIPS = "06"
DATATYPES = ["TMAX", "TMIN", "PRCP", "AWND", "WDF2", "WSF2"]
LOCAL_TZ = ZoneInfo("America/Los_Angeles")
PAGE_SIZE = 1000

# COMMAND ----------

dbutils.widgets.text("noaa_token", "", "NOAA CDO API token")
dbutils.widgets.text("initial_date", "2020-08-15", "Initial date if no valid files exist")
dbutils.widgets.text("max_days", "90", "Maximum number of new days to request")
dbutils.widgets.text("request_timeout_seconds", "120", "HTTP timeout")

TOKEN = dbutils.widgets.get("noaa_token").strip()
INITIAL_DATE_TEXT = dbutils.widgets.get("initial_date").strip()
MAX_DAYS = int(dbutils.widgets.get("max_days").strip())
TIMEOUT = int(dbutils.widgets.get("request_timeout_seconds").strip())

if not TOKEN:
    raise ValueError(
        "noaa_token is required. Request a NOAA CDO token and enter it "
        "as a widget or Job parameter."
    )
if not INITIAL_DATE_TEXT:
    raise ValueError("initial_date cannot be blank when no valid NOAA files exist")
if MAX_DAYS < 1 or MAX_DAYS > 90:
    raise ValueError("max_days must be between 1 and 90")

local_today = datetime.now(LOCAL_TZ).date()
max_available_date = local_today

# COMMAND ----------

def sha256_bytes(payload):
    return hashlib.sha256(payload).hexdigest()


def latest_nonempty_noaa_date(folder):
    pattern = re.compile(r"^noaa_(\d{4}-\d{2}-\d{2})\.json$")
    candidates = []
    if not os.path.isdir(folder):
        return None

    for name in os.listdir(folder):
        match = pattern.match(name)
        if not match:
            continue
        path = os.path.join(folder, name)
        try:
            with open(path, "r", encoding="utf-8") as file:
                payload = json.load(file)
            if isinstance(payload.get("results"), list) and payload["results"]:
                candidates.append(date.fromisoformat(match.group(1)))
        except (OSError, ValueError, json.JSONDecodeError, AttributeError):
            write_notebook_log("WARNING", f"Ignoring unreadable or invalid NOAA file: {path}")
    return max(candidates) if candidates else None


def fetch_noaa_range(start_day, end_day):
    all_rows = []
    offset = 1
    first_payload = None

    while True:
        query = [
            ("datasetid", DATASET_ID),
            ("locationid", f"FIPS:{STATE_FIPS}"),
            ("startdate", start_day.isoformat()),
            ("enddate", end_day.isoformat()),
            ("units", "metric"),
            ("limit", str(PAGE_SIZE)),
            ("offset", str(offset)),
            ("includemetadata", "true"),
        ]
        for datatype in DATATYPES:
            query.append(("datatypeid", datatype))

        url = f"{NOAA_ENDPOINT}?{urllib.parse.urlencode(query)}"
        request = urllib.request.Request(
            url,
            headers={
                "token": TOKEN,
                "User-Agent": "Databricks-Free-Edition-wildfire-project/1.0",
            },
        )

        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            http_code = response.getcode()
            raw = response.read()
        payload = json.loads(raw.decode("utf-8"))
        if first_payload is None:
            first_payload = payload

        page_rows = payload.get("results") or []
        all_rows.extend(page_rows)

        resultset = (payload.get("metadata") or {}).get("resultset") or {}
        total = resultset.get("count")
        if not page_rows or total is None or offset + len(page_rows) >= int(total):
            return all_rows, http_code, first_payload
        offset += len(page_rows)


def write_if_changed(path, raw):
    new_hash = sha256_bytes(raw)
    if os.path.exists(path):
        with open(path, "rb") as existing:
            if sha256_bytes(existing.read()) == new_hash:
                return False

    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix="noaa_", suffix=".json", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "wb") as temp_file:
            temp_file.write(raw)
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    return True


def serialize_daily_payload(day_text, rows):
    payload = {
        "metadata": {
            "datasetid": DATASET_ID,
            "locationid": f"FIPS:{STATE_FIPS}",
            "datatypes": DATATYPES,
            "date": day_text,
            "units": "metric",
        },
        "results": rows,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
dbutils.fs.mkdirs(RAW_NOAA_ROOT)

latest_date = latest_nonempty_noaa_date(RAW_NOAA_ROOT)
start_date = (
    latest_date + timedelta(days=1)
    if latest_date is not None
    else date.fromisoformat(INITIAL_DATE_TEXT)
)
end_date = min(start_date + timedelta(days=MAX_DAYS - 1), max_available_date)

write_notebook_log("INFO", f"Latest existing nonempty NOAA date: {latest_date or 'none'}")
if start_date <= end_date:
    write_notebook_log("INFO", f"Requested dates: {start_date} through {end_date}")
else:
    write_notebook_log("INFO", f"No request needed: next date {start_date} exceeds {max_available_date}")

results = []
if start_date <= end_date:
    try:
        rows, http_code, first_payload = fetch_noaa_range(start_date, end_date)
        rows_by_date = {}
        for row in rows:
            row_date = str(row.get("date", ""))[:10]
            if row_date:
                rows_by_date.setdefault(row_date, []).append(row)

        if not rows_by_date:
            write_notebook_log("WARNING", f"NOAA response body: {json.dumps(first_payload, indent=2, ensure_ascii=False)[:8000]}")
            results.append((str(start_date), str(end_date), http_code, 0, 0, "no_data", ""))
        else:
            for day_text, day_rows in sorted(rows_by_date.items()):
                day_rows.sort(
                    key=lambda row: (
                        str(row.get("station", "")),
                        str(row.get("date", "")),
                        str(row.get("datatype", "")),
                    )
                )
                destination = f"{RAW_NOAA_ROOT}/noaa_{day_text}.json"
                daily_raw = serialize_daily_payload(day_text, day_rows)
                changed = write_if_changed(destination, daily_raw)
                results.append(
                    (day_text, day_text, http_code, len(day_rows), int(changed), "success", "")
                )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:8000]
        write_notebook_log("ERROR", f"NOAA HTTP {exc.code}: {body}")
        results.append((str(start_date), str(end_date), exc.code, 0, 0, "http_error", body))
    except urllib.error.URLError as exc:
        message = str(exc.reason)
        write_notebook_log("ERROR", f"NOAA network error: {message}")
        results.append((str(start_date), str(end_date), 0, 0, 0, "network_error", message))
    except Exception as exc:
        write_notebook_log("ERROR", f"NOAA error: {exc}")
        results.append((str(start_date), str(end_date), 0, 0, 0, "error", str(exc)))

# COMMAND ----------

schema = T.StructType([
    T.StructField("start_date", T.StringType(), False),
    T.StructField("end_date", T.StringType(), False),
    T.StructField("http_code", T.IntegerType(), False),
    T.StructField("records", T.IntegerType(), False),
    T.StructField("changed", T.IntegerType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("error", T.StringType(), False),
])
summary = spark.createDataFrame(results, schema)
errors = summary.where(summary.status.isin("http_error", "network_error", "error")).count()
if errors:
    error_message = f"{errors} NOAA request(s) failed; inspect the log table."
    write_notebook_log("ERROR", error_message)
    raise RuntimeError(error_message)
else:
    write_notebook_log("INFO", f"NOAA extraction completed with {len(results)} result row(s).")
