# Databricks notebook source
# MAGIC %md
# MAGIC # EPA AQS daily PM2.5 downloader
# MAGIC
# MAGIC Downloads EPA Air Quality System daily-summary JSON files into the Unity
# MAGIC Catalog volume used by `02_bronze`.
# MAGIC
# MAGIC The output files are named `epa_YYYY-MM-DD.json` and contain the EPA
# MAGIC response structure `{\"Header\": [...], \"Data\": [...]}`.
# MAGIC
# MAGIC This notebook uses only Python's standard library. It does not require
# MAGIC `pip install`, a JAR, or a custom cluster. Free Edition may still block
# MAGIC the external EPA connection; the log table records the exact error.
# MAGIC
# MAGIC EPA AQS API credentials:
# MAGIC - For a quick test, the defaults are `test@aqs.api` / `test`.
# MAGIC - For regular use, request an EPA AQS key and enter it in the widgets.
# MAGIC - Do not hard-code a personal API key in this notebook.

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

from pyspark.sql import functions as F

from logging_utils import store_log

SOURCE_FILE = "01_extract_epa"

def write_notebook_log(severity, message):
    store_log(spark, severity, SOURCE_FILE, message)

# Keep these values aligned with 00_config.py.
CATALOG = "workspace"
SCHEMA = "wildfire"
RAW_EPA_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw/epa"

AQS_ENDPOINT = "https://aqs.epa.gov/data/api/dailyData/byState"
STATE_FIPS = "06"       # California
PARAMETER = "88101"     # PM2.5 FRM/FEM mass, local conditions
LOCAL_TZ = ZoneInfo("America/Los_Angeles")

# COMMAND ----------

# Widgets make the same notebook usable interactively and as a scheduled Job task.
dbutils.widgets.text("aqs_email", "test@aqs.api", "EPA AQS email")
dbutils.widgets.text("aqs_key", "test", "EPA AQS key")
dbutils.widgets.text("initial_date", "2020-08-15", "Initial date if no valid EPA files exist")
dbutils.widgets.text("max_days", "90", "Maximum number of new days to request")
dbutils.widgets.text("state_fips", STATE_FIPS, "State FIPS")
dbutils.widgets.text("parameter", PARAMETER, "AQS parameter")
dbutils.widgets.text("request_timeout_seconds", "120", "HTTP timeout")

AQSEMAIL = dbutils.widgets.get("aqs_email").strip()
AQSKEY = dbutils.widgets.get("aqs_key").strip()
INITIAL_DATE_TEXT = dbutils.widgets.get("initial_date").strip()
MAX_DAYS = int(dbutils.widgets.get("max_days").strip())
STATE = dbutils.widgets.get("state_fips").strip().zfill(2)
PARAM = dbutils.widgets.get("parameter").strip()
TIMEOUT = int(dbutils.widgets.get("request_timeout_seconds").strip())

if MAX_DAYS < 1 or MAX_DAYS > 90:
    raise ValueError("max_days must be between 1 and 90")
if not AQSEMAIL or not AQSKEY:
    raise ValueError("aqs_email and aqs_key cannot be blank")
if not INITIAL_DATE_TEXT:
    raise ValueError("initial_date cannot be blank when no valid EPA files exist")

local_today = datetime.now(LOCAL_TZ).date()
MAX_AVAILABLE_DATE = local_today - timedelta(days=5)

# COMMAND ----------

def sha256_bytes(payload):
    return hashlib.sha256(payload).hexdigest()


def fetch_epa_day(day):
    """Fetch one day from EPA and return the decoded JSON response."""
    day_text = day.strftime("%Y%m%d")
    query = urllib.parse.urlencode({
        "email": AQSEMAIL,
        "key": AQSKEY,
        "param": PARAM,
        "bdate": day_text,
        "edate": day_text,
        "state": STATE,
    })
    url = f"{AQS_ENDPOINT}?{query}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Databricks-Free-Edition-wildfire-project/1.0"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        http_code = response.getcode()
        raw = response.read()
    payload = json.loads(raw.decode("utf-8"))
    return payload, raw, http_code


def response_status(payload):
    headers = payload.get("Header") or []
    if isinstance(headers, list) and headers:
        return str(headers[0].get("status", "")).lower()
    return ""


def write_if_changed(path, raw):
    """Atomically write the response only if its bytes changed."""
    new_hash = sha256_bytes(raw)
    if os.path.exists(path):
        with open(path, "rb") as existing:
            old_hash = sha256_bytes(existing.read())
        if old_hash == new_hash:
            return False, new_hash

    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix="epa_", suffix=".json", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "wb") as temp_file:
            temp_file.write(raw)
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    return True, new_hash


def latest_nonempty_epa_date(folder):
    """Return the newest date whose existing JSON file contains records."""
    pattern = re.compile(r"^epa_(\d{4}-\d{2}-\d{2})\.json$")
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
            if isinstance(payload.get("Data"), list) and len(payload["Data"]) > 0:
                candidates.append(date.fromisoformat(match.group(1)))
        except (OSError, ValueError, json.JSONDecodeError, AttributeError):
            write_notebook_log("WARNING", f"Ignoring unreadable or invalid EPA file: {path}")

    return max(candidates) if candidates else None


# COMMAND ----------

# Make sure the managed volume directory exists. The volume itself should be
# created by 01_setup, but this is harmless if the notebook is run independently.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
dbutils.fs.mkdirs(RAW_EPA_ROOT)

latest_date = latest_nonempty_epa_date(RAW_EPA_ROOT)
start_date = (
    latest_date + timedelta(days=1)
    if latest_date is not None
    else date.fromisoformat(INITIAL_DATE_TEXT)
)
end_date = min(start_date + timedelta(days=MAX_DAYS - 1), MAX_AVAILABLE_DATE)

write_notebook_log("INFO", f"Latest existing nonempty EPA date: {latest_date or 'none'}")
if start_date <= end_date:
    write_notebook_log("INFO", f"Requested dates: {start_date} through {end_date}")
else:
    write_notebook_log("INFO", f"No request needed: next date would be {start_date}, beyond {MAX_AVAILABLE_DATE}")

results = []
current = start_date
while current <= end_date:
    file_name = f"epa_{current.isoformat()}.json"
    destination = f"{RAW_EPA_ROOT}/{file_name}"
    try:
        payload, raw, http_code = fetch_epa_day(current)
        status = response_status(payload)
        data = payload.get("Data")

        header_text = json.dumps(payload.get("Header"), ensure_ascii=False)
        #write_notebook_log("INFO", f"{file_name}: HTTP {http_code}; EPA response: {header_text}")

        # EPA may return an error response with HTTP 200. Do not write that as
        # a normal input file unless the response has the expected Data array.
        if not isinstance(data, list):
            raise RuntimeError(
                f"EPA response for {current} has no Data array; "
                f"Header={payload.get('Header')}"
            )

        # AQS often has no data yet for the newest date. Do not create an
        # empty file in that case: an empty file would be recorded in the
        # Bronze manifest and could prevent a later retry from loading data.
        if len(data) == 0:
            results.append((current.isoformat(), 0, False, "no_data", "", ""))
            write_notebook_log(
                "WARNING",
                f"{file_name}: EPA returned no data; file not written. "
                f"Full API response: {json.dumps(payload, indent=2, ensure_ascii=False)}",
            )
            current += timedelta(days=1)
            continue

        changed, content_hash = write_if_changed(destination, raw)
        results.append((current.isoformat(), len(data), changed, status, content_hash[:12], ""))
        #write_notebook_log("INFO", f"{file_name}: {len(data):,} records; {'written' if changed else 'unchanged'}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        results.append((current.isoformat(), 0, False, "http_error", "", f"HTTP {exc.code}: {body}"))
        write_notebook_log("ERROR", f"{file_name}: HTTP {exc.code}; {body}")
    except urllib.error.URLError as exc:
        message = str(exc.reason)
        results.append((current.isoformat(), 0, False, "network_error", "", message))
        write_notebook_log("ERROR", f"{file_name}: network error: {message}")
    except Exception as exc:
        results.append((current.isoformat(), 0, False, "error", "", str(exc)))
        write_notebook_log("ERROR", f"{file_name}: error: {exc}")
    current += timedelta(days=1)

# COMMAND ----------

result_df = spark.createDataFrame(
    results,
    "date string, records long, changed boolean, status string, sha256_prefix string, error string",
)
errors = result_df.where(F.col("error") != "").count()
written_or_unchanged = result_df.where(F.col("error") == "").count()
source_file = SOURCE_FILE
if errors:
    log_severity = "ERROR"
    log_message = (
        f"{errors} EPA date(s) failed. If the error mentions DNS, connection, or "
        "access, Free Edition may be blocking the EPA domain. If it mentions the "
        "API key, register for an AQS key or try the documented test credentials."
    )
else:
    log_severity = "INFO"
    log_message = (
        f"Completed successfully: {written_or_unchanged} date(s) available in {RAW_EPA_ROOT}"
    )

store_log(spark, log_severity, source_file, log_message)
