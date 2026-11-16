# Databricks notebook source
# MAGIC %md
# MAGIC # Gold data quality check
# MAGIC
# MAGIC This notebook rebuilds `workspace.wildfire.gold_data_check` from scratch.
# MAGIC
# MAGIC The output contains one row per year for every available non-null `date_local` in the source table. It summarizes date coverage, duplicate daily keys, null KPI cells, completeness, correctness-rule passes, and complete valid rows. The source Gold table is never modified.

# COMMAND ----------

from pyspark.sql import functions as F
from logging_utils import store_log

SOURCE_FILE = "08_gold_data_check"
SOURCE_TABLE = "workspace.wildfire.gold_kpi_state_day"
TARGET_TABLE = "workspace.wildfire.gold_data_check"

required_columns = {
    "date_local",
    "ca_fire_detections",
    "ca_fire_frp_mw",
    "avg_county_pm25_ugm3",
    "counties_over_standard",
    "counties_with_pm25",
    "counties_likely_smoke",
}

source_columns = set(spark.table(SOURCE_TABLE).columns)
missing_columns = sorted(required_columns - source_columns)

if missing_columns:
    raise ValueError(
        f"{SOURCE_TABLE} is missing required columns: {missing_columns}"
    )

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    f"Validated source schema for {SOURCE_TABLE}; rebuilding {TARGET_TABLE}.",
)

# COMMAND ----------

quality_query = f"""
WITH source_data AS (
    SELECT
        date_local,
        ca_fire_detections,
        ca_fire_frp_mw,
        avg_county_pm25_ugm3,
        counties_over_standard,
        counties_with_pm25,
        counties_likely_smoke
    FROM {SOURCE_TABLE}
),

dataset_bounds AS (
    SELECT
        MIN(date_local) AS start_date,
        MAX(date_local) AS end_date
    FROM source_data
    WHERE date_local IS NOT NULL
),

calendar_days AS (
    SELECT
        YEAR(date_local) AS calendar_year,
        COUNT(*) AS expected_days
    FROM (
        SELECT EXPLODE(
            SEQUENCE(
                (SELECT start_date FROM dataset_bounds),
                (SELECT end_date FROM dataset_bounds),
                INTERVAL 1 DAY
            )
        ) AS date_local
    )
    GROUP BY YEAR(date_local)
),

in_range AS (
    SELECT
        YEAR(date_local) AS calendar_year,
        date_local,
        ca_fire_detections,
        ca_fire_frp_mw,
        avg_county_pm25_ugm3,
        counties_over_standard,
        counties_with_pm25,
        counties_likely_smoke
    FROM source_data
    WHERE date_local IS NOT NULL
),

flagged_base AS (
    SELECT
        *,

        (
              CASE WHEN ca_fire_detections IS NULL THEN 1 ELSE 0 END
            + CASE WHEN ca_fire_frp_mw IS NULL THEN 1 ELSE 0 END
            + CASE WHEN avg_county_pm25_ugm3 IS NULL THEN 1 ELSE 0 END
            + CASE WHEN counties_over_standard IS NULL THEN 1 ELSE 0 END
            + CASE WHEN counties_with_pm25 IS NULL THEN 1 ELSE 0 END
            + CASE WHEN counties_likely_smoke IS NULL THEN 1 ELSE 0 END
        ) AS null_kpi_cells_row,

        CASE
            WHEN COALESCE(ca_fire_detections < 0, FALSE)
              OR COALESCE(ca_fire_frp_mw < 0, FALSE)
              OR COALESCE(avg_county_pm25_ugm3 < 0, FALSE)
              OR COALESCE(counties_over_standard < 0, FALSE)
              OR COALESCE(counties_with_pm25 < 0, FALSE)
              OR COALESCE(counties_likely_smoke < 0, FALSE)
            THEN 1
            ELSE 0
        END AS negative_row,

        CASE
            WHEN COALESCE(
                     counties_over_standard > counties_with_pm25
                  OR counties_likely_smoke > counties_over_standard
                  OR counties_likely_smoke > counties_with_pm25,
                  FALSE
                 )
            THEN 1
            ELSE 0
        END AS hierarchy_row

    FROM in_range
),

flagged AS (
    SELECT
        *,
        CASE
            WHEN null_kpi_cells_row = 0
             AND negative_row = 0
             AND hierarchy_row = 0
            THEN 1
            ELSE 0
        END AS complete_valid_row
    FROM flagged_base
),

date_counts AS (
    SELECT
        calendar_year,
        date_local,
        COUNT(*) AS rows_for_date
    FROM flagged
    GROUP BY calendar_year, date_local
),

duplicate_summary AS (
    SELECT
        calendar_year,
        COUNT(*) AS duplicate_date_groups
    FROM date_counts
    WHERE rows_for_date > 1
    GROUP BY calendar_year
),

year_summary AS (
    SELECT
        calendar_year,
        COUNT(*) AS row_count,
        COUNT(DISTINCT date_local) AS observed_days,
        SUM(null_kpi_cells_row) AS null_kpi_cells,
        SUM(
            CASE
                WHEN negative_row = 1 OR hierarchy_row = 1
                THEN 1
                ELSE 0
            END
        ) AS rule_violation_rows,
        SUM(complete_valid_row) AS complete_valid_rows
    FROM flagged
    GROUP BY calendar_year
)

SELECT
    c.calendar_year AS year,
    c.expected_days,
    COALESCE(y.observed_days, 0) AS observed_days,
    GREATEST(
        c.expected_days - COALESCE(y.observed_days, 0),
        0
    ) AS missing_days,
    ROUND(
        100.0 * COALESCE(y.observed_days, 0)
        / NULLIF(c.expected_days, 0),
        2
    ) AS date_coverage_pct,
    COALESCE(d.duplicate_date_groups, 0) AS duplicate_date_groups,
    COALESCE(y.null_kpi_cells, 0) AS null_kpi_cells,
    CASE
        WHEN COALESCE(y.row_count, 0) = 0 THEN NULL
        ELSE ROUND(
            100.0 * (y.row_count * 6 - y.null_kpi_cells)
            / (y.row_count * 6),
            2
        )
    END AS kpi_completeness_pct,
    CASE
        WHEN COALESCE(y.row_count, 0) = 0 THEN NULL
        ELSE ROUND(
            100.0 * (y.row_count - y.rule_violation_rows)
            / y.row_count,
            2
        )
    END AS correctness_rule_pass_pct,
    CASE
        WHEN COALESCE(y.row_count, 0) = 0 THEN NULL
        ELSE ROUND(
            100.0 * y.complete_valid_rows
            / y.row_count,
            2
        )
    END AS complete_and_valid_row_pct

FROM calendar_days c
LEFT JOIN year_summary y
    ON c.calendar_year = y.calendar_year
LEFT JOIN duplicate_summary d
    ON c.calendar_year = d.calendar_year
ORDER BY year
"""

quality_df = (
    spark.sql(quality_query)
    .withColumn("_check_built_at_utc", F.current_timestamp())
)
store_log(spark, "INFO", SOURCE_FILE, "Computed annual Gold coverage and correctness metrics.")

# Rebuild the derived table completely on every run.
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

(
    quality_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

rows_written = spark.table(TARGET_TABLE).count()
store_log(spark, "INFO", SOURCE_FILE, f"Recreated {TARGET_TABLE} with {rows_written:,} yearly row(s).")
