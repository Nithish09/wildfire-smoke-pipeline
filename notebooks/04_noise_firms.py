# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Row-level Laplace noise for FIRMS detections
# MAGIC
# MAGIC This notebook creates a separate noised copy of the FIRMS Bronze table. It preserves one output row per FIRMS detection and replaces selected sensitive or identifying numeric fields with pseudo-random Laplace perturbations. By default, each run uses a new random seed; supplying a fixed `noise_seed` makes a release reproducible.
# MAGIC
# MAGIC Noised fields:
# MAGIC - `latitude` and `longitude`: approximate spatial perturbation with a one-kilometre characteristic scale
# MAGIC - brightness field, when present: 2 K noise scale
# MAGIC - FRP field, when present: 5 MW noise scale
# MAGIC
# MAGIC The original `workspace.wildfire.bronze_firms` table is never modified. The output is written to `workspace.wildfire.firms_row_noised`. Blank, `auto`, or `random` `noise_seed` values produce a different release on every run; a fixed integer seed produces the same release again.
# MAGIC
# MAGIC This is an educational row-perturbation demonstration, not a complete formal differential-privacy mechanism. It does not provide privacy-budget accounting or a formal guarantee for arbitrary releases.

# COMMAND ----------

import secrets

from pyspark.sql import functions as F
from logging_utils import store_log


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

SOURCE_FILE = "04_noise_firms"

SOURCE_TABLE = "workspace.wildfire.bronze_firms"
TARGET_TABLE = "workspace.wildfire.firms_row_noised"

LOCATION_NOISE_KM = 1.0
BRIGHTNESS_NOISE_K = 2.0
FRP_NOISE_MW = 5.0


# ---------------------------------------------------------------------
# Noise seed
# ---------------------------------------------------------------------

dbutils.widgets.text(
    "noise_seed",
    "",
    "Noise seed (blank or auto = new random seed)",
)

NOISE_SEED_TEXT = dbutils.widgets.get("noise_seed").strip()

if NOISE_SEED_TEXT.lower() in {"", "auto", "random"}:
    NOISE_SEED = secrets.randbits(63)
else:
    try:
        NOISE_SEED = int(NOISE_SEED_TEXT)
    except ValueError as exc:
        raise ValueError(
            "noise_seed must be an integer, blank, auto, or random"
        ) from exc

if not 0 <= NOISE_SEED < 2**63:
    raise ValueError("noise_seed must be between 0 and 2^63 - 1")


# ---------------------------------------------------------------------
# Read source table
# ---------------------------------------------------------------------

source_df = spark.table(SOURCE_TABLE)


def choose_column(columns, candidates):
    lookup = {column.lower(): column for column in columns}

    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]

    return None


latitude_column = choose_column(
    source_df.columns,
    ["latitude", "lat"],
)

longitude_column = choose_column(
    source_df.columns,
    ["longitude", "lon", "long"],
)

brightness_column = choose_column(
    source_df.columns,
    [
        "brightness_k",
        "brightness",
        "bright_ti4",
        "bright_t31",
        "bright_ti11",
    ],
)

frp_column = choose_column(
    source_df.columns,
    [
        "frp_mw",
        "frp",
        "fire_radiative_power",
    ],
)

if latitude_column is None or longitude_column is None:
    raise ValueError(
        "Latitude/longitude columns were not found. "
        f"Available columns: {source_df.columns}"
    )


# ---------------------------------------------------------------------
# Build a faster row identity
# ---------------------------------------------------------------------

identity_candidates = [
    latitude_column,
    longitude_column,
    "acq_date",
    "acq_time",
    "satellite",
    "instrument",
    "version",
    "confidence",
    "daynight",
]

identity_columns = [
    column
    for column in identity_candidates
    if column is not None and column in source_df.columns
]

if not identity_columns:
    identity_columns = [
        column
        for column in source_df.columns
        if not column.startswith("_")
    ]

identity_expressions = [
    F.coalesce(
        F.col(column).cast("string"),
        F.lit("<NULL>"),
    )
    for column in identity_columns
]

# xxhash64 is considerably faster than SHA-256.
# Collision risk is extremely low for this dataset size.
working_df = (
    source_df
    .withColumn(
        "_privacy_row_key",
        F.xxhash64(
            F.lit("privacy-row-key-v1"),
            *identity_expressions,
        ).cast("string"),
    )
    # Faster than a Window + row_number global sort.
    # If duplicate rows exist, one copy is retained.
    .dropDuplicates(["_privacy_row_key"])
)


# ---------------------------------------------------------------------
# Deterministic random functions
# ---------------------------------------------------------------------

def stable_uniform(key_column, field_name):
    hashed = F.xxhash64(
        F.lit(NOISE_SEED),
        key_column,
        F.lit(field_name),
    )

    return (
        F.pmod(hashed, F.lit(1_000_000))
        .cast("double")
        + F.lit(0.5)
    ) / F.lit(1_000_000.0)


def laplace_noise(key_column, field_name, scale_expression):
    centered = (
        stable_uniform(key_column, field_name)
        - F.lit(0.5)
    )

    return (
        -scale_expression
        * F.signum(centered)
        * F.log(
            F.lit(1.0)
            - F.lit(2.0) * F.abs(centered)
        )
    )


# ---------------------------------------------------------------------
# Apply location noise
# ---------------------------------------------------------------------

noised_df = working_df

key_column = F.col("_privacy_row_key")

latitude_value = F.col(latitude_column).cast("double")
longitude_value = F.col(longitude_column).cast("double")

latitude_scale_degrees = F.lit(
    LOCATION_NOISE_KM / 111.32
)

latitude_noise = laplace_noise(
    key_column,
    "latitude",
    latitude_scale_degrees,
)

longitude_scale_degrees = (
    latitude_scale_degrees
    / F.greatest(
        F.abs(F.cos(F.radians(latitude_value))),
        F.lit(0.2),
    )
)

longitude_noise = laplace_noise(
    key_column,
    "longitude",
    longitude_scale_degrees,
)

noised_latitude = F.least(
    F.lit(90.0),
    F.greatest(
        F.lit(-90.0),
        latitude_value + latitude_noise,
    ),
)

raw_longitude = longitude_value + longitude_noise

noised_longitude = (
    F.pmod(
        raw_longitude + F.lit(180.0),
        F.lit(360.0),
    )
    - F.lit(180.0)
)

noised_df = (
    noised_df
    .withColumn(latitude_column, noised_latitude)
    .withColumn(longitude_column, noised_longitude)
)


# ---------------------------------------------------------------------
# Apply brightness noise
# ---------------------------------------------------------------------

noised_column_names = [
    latitude_column,
    longitude_column,
]

if brightness_column is not None:
    brightness_noise = laplace_noise(
        key_column,
        "brightness",
        F.lit(BRIGHTNESS_NOISE_K),
    )

    noised_df = noised_df.withColumn(
        brightness_column,
        F.col(brightness_column).cast("double")
        + brightness_noise,
    )

    noised_column_names.append(brightness_column)


# ---------------------------------------------------------------------
# Apply FRP noise
# ---------------------------------------------------------------------

if frp_column is not None:
    frp_noise = laplace_noise(
        key_column,
        "frp",
        F.lit(FRP_NOISE_MW),
    )

    noised_df = noised_df.withColumn(
        frp_column,
        F.greatest(
            F.lit(0.0),
            F.col(frp_column).cast("double")
            + frp_noise,
        ),
    )

    noised_column_names.append(frp_column)


# ---------------------------------------------------------------------
# Add audit metadata
# ---------------------------------------------------------------------

noised_df = (
    noised_df
    .withColumn(
        "_noise_method",
        F.lit("deterministic_laplace_row_perturbation"),
    )
    .withColumn(
        "_noise_seed",
        F.lit(NOISE_SEED).cast("long"),
    )
    .withColumn(
        "_location_noise_km",
        F.lit(LOCATION_NOISE_KM),
    )
    .withColumn(
        "_brightness_noise_k",
        F.lit(
            BRIGHTNESS_NOISE_K
            if brightness_column is not None
            else None
        ).cast("double"),
    )
    .withColumn(
        "_frp_noise_mw",
        F.lit(
            FRP_NOISE_MW
            if frp_column is not None
            else None
        ).cast("double"),
    )
)


# ---------------------------------------------------------------------
# Fast output write
# ---------------------------------------------------------------------
# Overwrite is faster than MERGE.
# A new random seed changes every row, so MERGE would rewrite the
# entire target table after scanning both source and target.

(
    noised_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)


# ---------------------------------------------------------------------
# One final log entry
# ---------------------------------------------------------------------

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    (
        f"Rebuilt {TARGET_TABLE}; "
        f"columns noised={noised_column_names}; "
        f"noise_seed={NOISE_SEED}; "
        f"identity_columns={identity_columns}."
    ),
)

print(f"Completed privacy release: {TARGET_TABLE}")
print(f"Noise seed: {NOISE_SEED}")
print(f"Noised columns: {noised_column_names}")

# COMMAND ----------

# Build a stable row identity from FIRMS business fields. Metadata such as
# ingestion timestamps is excluded so a rerun does not create a new noisy row.
identity_candidates = [
    latitude_column,
    longitude_column,
    "acq_date",
    "acq_time",
    "satellite",
    "instrument",
    "version",
    "confidence",
    "daynight"
]
identity_columns = [
    column for column in identity_candidates
    if column is not None and column in source_df.columns
]

if not identity_columns:
    identity_columns = [column for column in source_df.columns if not column.startswith("_")]

identity_struct = F.struct(*[F.col(column) for column in identity_columns])
working_df = source_df.withColumn(
    "_privacy_row_key",
    F.sha2(F.to_json(identity_struct), 256)
)

# Bronze is append-only and can contain revised versions. Keep the latest
# received copy for each FIRMS business identity.
if "_ingested_at_utc" in working_df.columns:
    latest_window = Window.partitionBy("_privacy_row_key").orderBy(
        F.col("_ingested_at_utc").desc_nulls_last()
    )
    working_df = (
        working_df
        .withColumn("_latest_row", F.row_number().over(latest_window))
        .where(F.col("_latest_row") == 1)
        .drop("_latest_row")
    )
else:
    working_df = working_df.dropDuplicates(["_privacy_row_key"])

def stable_uniform(key_column, field_name):
    hashed = F.xxhash64(F.lit(NOISE_SEED), key_column, F.lit(field_name))
    return (
        F.pmod(hashed, F.lit(1000000)).cast("double") + F.lit(0.5)
    ) / F.lit(1000000.0)

def laplace_noise(key_column, field_name, scale_expression):
    centered = stable_uniform(key_column, field_name) - F.lit(0.5)
    return (
        -scale_expression
        * F.signum(centered)
        * F.log(F.lit(1.0) - F.lit(2.0) * F.abs(centered))
    )

noised_df = working_df
key_column = F.col("_privacy_row_key")

latitude_value = F.col(latitude_column).cast("double")
longitude_value = F.col(longitude_column).cast("double")
latitude_scale_degrees = F.lit(LOCATION_NOISE_KM / 111.32)
latitude_noise = laplace_noise(key_column, "latitude", latitude_scale_degrees)

# Longitude degrees represent less distance as latitude increases.
longitude_scale_degrees = latitude_scale_degrees / F.greatest(
    F.abs(F.cos(F.radians(latitude_value))),
    F.lit(0.2)
)
longitude_noise = laplace_noise(key_column, "longitude", longitude_scale_degrees)

noised_latitude = F.least(
    F.lit(90.0),
    F.greatest(F.lit(-90.0), latitude_value + latitude_noise)
)
raw_longitude = longitude_value + longitude_noise
noised_longitude = F.pmod(raw_longitude + F.lit(180.0), F.lit(360.0)) - F.lit(180.0)

noised_df = (
    noised_df
    .withColumn(latitude_column, noised_latitude)
    .withColumn(longitude_column, noised_longitude)
)

noised_column_names = [latitude_column, longitude_column]

if brightness_column is not None:
    brightness_noise = laplace_noise(
        key_column,
        "brightness",
        F.lit(BRIGHTNESS_NOISE_K)
    )
    noised_df = noised_df.withColumn(
        brightness_column,
        F.col(brightness_column).cast("double") + brightness_noise
    )
    noised_column_names.append(brightness_column)

if frp_column is not None:
    frp_noise = laplace_noise(
        key_column,
        "frp",
        F.lit(FRP_NOISE_MW)
    )
    noised_df = noised_df.withColumn(
        frp_column,
        F.greatest(
            F.lit(0.0),
            F.col(frp_column).cast("double") + frp_noise
        )
    )
    noised_column_names.append(frp_column)

noised_df = (
    noised_df
    .withColumn("_noise_method", F.lit("deterministic_laplace_row_perturbation"))
    .withColumn("_noise_seed", F.lit(NOISE_SEED).cast("long"))
    .withColumn("_location_noise_km", F.lit(LOCATION_NOISE_KM))
    .withColumn("_brightness_noise_k", F.lit(BRIGHTNESS_NOISE_K if brightness_column else None).cast("double"))
    .withColumn("_frp_noise_mw", F.lit(FRP_NOISE_MW if frp_column else None).cast("double"))
)

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    f"Applying deterministic Laplace perturbation to columns: {noised_column_names}.",
)

# COMMAND ----------

# Create or incrementally update a separate Delta table.
# The source table remains unchanged.
if not spark.catalog.tableExists(TARGET_TABLE):
    (
        noised_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    store_log(spark, "INFO", SOURCE_FILE, f"Created noised table {TARGET_TABLE}.")
else:
    target = DeltaTable.forName(spark, TARGET_TABLE)
    update_columns = {
        column: f"s.`{column}`"
        for column in noised_df.columns
        if column != "_privacy_row_key"
    }
    insert_columns = {
        column: f"s.`{column}`"
        for column in noised_df.columns
    }
    (
        target.alias("t")
        .merge(
            noised_df.alias("s"),
            "t._privacy_row_key = s._privacy_row_key"
        )
        .whenMatchedUpdate(set=update_columns)
        .whenNotMatchedInsert(values=insert_columns)
        .execute()
    )
    store_log(spark, "INFO", SOURCE_FILE, f"Updated noised table {TARGET_TABLE}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation
# MAGIC
# MAGIC The noised table retains the FIRMS row structure but does not retain the original values for the selected fields. Latitude and longitude are perturbed together with fire-intensity measurements. The deterministic seed makes the release reproducible; changing the seed creates a different release.
# MAGIC
# MAGIC This table should be used only for privacy demonstrations or exploratory analysis. The original FIRMS Bronze table remains the authoritative source for the project pipeline.