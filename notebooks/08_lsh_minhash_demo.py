# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Locality-Sensitive Hashing demonstration
# MAGIC
# MAGIC This notebook demonstrates **MinHash + banding-based Locality-Sensitive Hashing (LSH)** on the Gold county-day table. It identifies county-days with similar fire, PM2.5, AQI, wind, and smoke profiles.
# MAGIC
# MAGIC The notebook is presentation-oriented: it reads the Gold table, creates one clearly labelled synthetic near-duplicate for a guaranteed demonstration, and does not modify any production table. Run it after `04_kpi_gold`.
# MAGIC
# MAGIC LSH is used to generate candidate pairs efficiently. Exact Jaccard similarity is calculated afterward to show how the candidates can be validated.

# COMMAND ----------

from collections import defaultdict
from itertools import combinations
from math import floor
import hashlib

from pyspark.sql import functions as F
from logging_utils import store_log

SOURCE_FILE = "08_lsh_minhash_demo"
SOURCE_TABLE = "workspace.wildfire.gold_fact_county_day"
NUM_PERMUTATIONS = 64
BANDS = 32
ROWS_PER_BAND = 2
SAMPLE_SIZE = 250
MIN_JACCARD = 0.40

assert BANDS * ROWS_PER_BAND == NUM_PERMUTATIONS

required_columns = {
    "county_fips",
    "date_local",
    "fire_detections_within_100km",
    "pm25_mean_ugm3",
    "aqi_max",
    "wind_mean_ms",
    "smoke_indicator"
}
available_columns = set(spark.table(SOURCE_TABLE).columns)
missing_columns = sorted(required_columns - available_columns)
if missing_columns:
    raise ValueError(f"Missing Gold columns: {missing_columns}")

source_df = (
    spark.table(SOURCE_TABLE)
    .select(
        "county_fips",
        "date_local",
        "fire_detections_within_100km",
        "pm25_mean_ugm3",
        "aqi_max",
        "wind_mean_ms",
        "smoke_indicator"
    )
    .where(F.col("date_local").isNotNull())
    .orderBy(F.rand(42))
    .limit(SAMPLE_SIZE)
)

store_log(spark, "INFO", SOURCE_FILE, f"Reading up to {SAMPLE_SIZE:,} county-day profiles from {SOURCE_TABLE}.")

# COMMAND ----------

def numeric_token(name, value, width):
    if value is None:
        return f"{name}=missing"
    return f"{name}={int(floor(float(value) / width))}"


def text_token(name, value):
    return f"{name}={value if value is not None else 'missing'}"


def build_tokens(row):
    return {
        numeric_token("fires", row["fire_detections_within_100km"], 5),
        numeric_token("pm25", row["pm25_mean_ugm3"], 5),
        numeric_token("aqi", row["aqi_max"], 25),
        numeric_token("wind", row["wind_mean_ms"], 2),
        text_token("smoke", row["smoke_indicator"]),
        text_token("month", row["date_local"].month)
    }


records = []

for row in source_df.collect():
    records.append({
        "record_id": f"{row['county_fips']}|{row['date_local']}",
        "tokens": build_tokens(row)
    })

# Presentation-only near-duplicate. This changes one feature token and
# does not write the synthetic record back to Gold.
if records:
    base = records[0]
    near_tokens = set(base["tokens"])
    changed = False

    for prefix in ("pm25=", "fires=", "aqi="):
        token = next((t for t in near_tokens if t.startswith(prefix) and not t.endswith("missing")), None)
        if token is not None:
            old_value = int(token.split("=", 1)[1])
            near_tokens.remove(token)
            near_tokens.add(f"{prefix}{old_value + 1}")
            changed = True
            break

    if not changed:
        near_tokens.remove(next(iter(near_tokens)))
        near_tokens.add("synthetic=adjacent_profile")

    records.append({
        "record_id": base["record_id"] + "|synthetic_near_duplicate",
        "tokens": near_tokens
    })

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    f"Tokenized {len(records):,} records; synthetic near-duplicate added={bool(records)}.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MinHash signatures and LSH banding
# MAGIC
# MAGIC Each MinHash signature approximates the Jaccard similarity between two token sets. LSH divides the signature into bands; records sharing a band become candidate pairs. Only those candidates receive the more expensive exact Jaccard comparison.

# COMMAND ----------

def seeded_hash(seed, token):
    payload = f"{seed}|{token}".encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big")


def minhash_signature(tokens):
    tokens = tokens or {"__EMPTY__"}
    return tuple(
        min(seeded_hash(seed, token) for token in tokens)
        for seed in range(NUM_PERMUTATIONS)
    )


for record in records:
    record["signature"] = minhash_signature(record["tokens"])

lsh_buckets = defaultdict(list)

for record in records:
    signature = record["signature"]

    for band in range(BANDS):
        start = band * ROWS_PER_BAND
        end = start + ROWS_PER_BAND
        band_key = (band, signature[start:end])
        lsh_buckets[band_key].append(record["record_id"])

candidate_pairs = set()
band_hits = defaultdict(int)

for ids in lsh_buckets.values():
    unique_ids = sorted(set(ids))

    for pair in combinations(unique_ids, 2):
        candidate_pairs.add(pair)
        band_hits[pair] += 1

record_map = {record["record_id"]: record for record in records}
results = []

for left_id, right_id in sorted(candidate_pairs):
    left = record_map[left_id]
    right = record_map[right_id]

    left_tokens = left["tokens"]
    right_tokens = right["tokens"]
    exact_jaccard = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)

    matching_permutations = sum(
        a == b
        for a, b in zip(left["signature"], right["signature"])
    )
    estimated_jaccard = matching_permutations / NUM_PERMUTATIONS

    if exact_jaccard >= MIN_JACCARD:
        results.append((
            left_id,
            right_id,
            float(exact_jaccard),
            float(estimated_jaccard),
            int(band_hits[(left_id, right_id)])
        ))

result_df = spark.createDataFrame(
    results,
    """
    left_record string,
    right_record string,
    exact_jaccard double,
    minhash_estimate double,
    shared_lsh_bands int
    """
)

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    f"LSH MinHash banding completed: records={len(records):,}, "
    f"candidate_pairs={len(candidate_pairs):,}, similar_pairs={len(results):,}.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation
# MAGIC
# MAGIC The LSH stage reduces the number of pairs requiring exact comparison. The synthetic pair is included only to make the demonstration reproducible; it is never written to a Delta table. In the presentation, this notebook can be described as an exploratory similarity-search extension for the Gold county-day data.
