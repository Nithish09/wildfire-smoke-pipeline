"""Reusable logging helpers for Databricks notebooks.

The default destination is the managed Delta table
``workspace.wildfire.log``.  The table is created automatically the first
time ``store_log`` is called.
"""

import re

from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_LOG_TABLE = "workspace.wildfire.log"
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_table_name(table_name):
    """Validate and quote a one-, two-, or three-part table identifier."""
    parts = table_name.split(".")
    if len(parts) not in {1, 2, 3} or not all(
        _IDENTIFIER_PATTERN.fullmatch(part) for part in parts
    ):
        raise ValueError(
            "table_name must be a valid one-, two-, or three-part identifier"
        )
    return ".".join(f"`{part}`" for part in parts)


def ensure_log_table(spark, table_name=DEFAULT_LOG_TABLE):
    """Create the logging table if it does not already exist."""
    quoted_table = _quote_table_name(table_name)
    parts = table_name.split(".")

    if len(parts) == 3:
        catalog, schema = parts[:2]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    elif len(parts) == 2:
        catalog, schema = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {quoted_table} (
            `timestamp` TIMESTAMP NOT NULL,
            `log_severity` STRING NOT NULL,
            `source_file` STRING NOT NULL,
            `log_message` STRING NOT NULL
        ) USING DELTA
        """
    )
    return table_name


def store_log(
    spark,
    log_severity,
    source_file,
    log_message,
    table_name=DEFAULT_LOG_TABLE,
):
    """Append one log record and create the destination table if necessary."""
    ensure_log_table(spark, table_name)

    severity = str(log_severity).strip().upper()
    source = str(source_file).strip()
    message = str(log_message).strip()

    if not severity:
        raise ValueError("log_severity cannot be blank")
    if not source:
        raise ValueError("source_file cannot be blank")
    if not message:
        raise ValueError("log_message cannot be blank")

    schema = T.StructType(
        [
            T.StructField("log_severity", T.StringType(), False),
            T.StructField("source_file", T.StringType(), False),
            T.StructField("log_message", T.StringType(), False),
        ]
    )
    log_row = spark.createDataFrame([(severity, source, message)], schema)
    log_row = log_row.select(
        F.current_timestamp().alias("timestamp"),
        F.col("log_severity"),
        F.col("source_file"),
        F.col("log_message"),
    )
    log_row.write.mode("append").saveAsTable(table_name)

