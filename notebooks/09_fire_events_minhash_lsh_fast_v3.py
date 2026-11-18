# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 · Fire events + band-by-band MinHash/LSH graph (v3)
# MAGIC Turns individual FIRMS detections into **fire events** and links each event to the counties around it.
# MAGIC
# MAGIC **Algorithm.** Each detection receives coordinate/time tokens. MinHash signatures and LSH banding generate candidate
# MAGIC pairs; candidates are then checked with the exact **2 km and 24-hour** rules. Validated pairs are merged with
# MAGIC union–find. Because this version intentionally uses LSH candidate retrieval, a few valid links may be missed.
# MAGIC
# MAGIC **Outputs**
# MAGIC | Table | Grain | Use |
# MAGIC | --- | --- | --- |
# MAGIC | `silver_fire_event_members` | detection → event_id | which event every detection belongs to |
# MAGIC | `gold_fire_lsh_detection_edges` | validated detection pair | all exact links found through MinHash/LSH |
# MAGIC | `gold_fire_detection_edges` | fire detection → next detection | nearest-successor movement links for dashboards |
# MAGIC | `gold_fire_events` | event | start/end, size, fire power, centroid, counties burned |
# MAGIC | `gold_fire_event_day` | event × local date | daily growth of each event |
# MAGIC | `gold_fire_event_county_impact` | event × day × nearby county | which counties each event could smoke, with wind and PM2.5 |
# MAGIC
# MAGIC Run after `04_kpi_gold` (it reads `gold_dim_county` and `gold_fact_county_day`). Each run recomputes events from all
# MAGIC Silver; the hashing is seeded, so a rerun on unchanged data gives identical results and event IDs.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./lib_fire_events

# COMMAND ----------

## Imports used below (also in 00_config; repeated so this notebook works with any version of it)
import hashlib
import math
import random
import time
from bisect import bisect_right
from collections import defaultdict

try:
    import numpy as np
except ImportError:
    np = None

from pyspark.sql import Window
from pyspark.sql import functions as F
from logging_utils import store_log

SOURCE_FILE = "09_fire_events_fast_lsh_v3"

dbutils.widgets.text("link_km", "2", "Link distance (km)")
dbutils.widgets.text("link_hours", "24", "Link time window (hours)")
dbutils.widgets.text("impact_km", "150", "Max distance from event to county centre for impact (km)")
dbutils.widgets.text("minhash_permutations", "32", "Number of MinHash permutations")
dbutils.widgets.text("lsh_bands", "16", "Number of LSH bands")
dbutils.widgets.text("lsh_rows_per_band", "2", "Rows per LSH band")
dbutils.widgets.text("lsh_max_neighbors_per_record", "16", "Max LSH bucket neighbors per record")
dbutils.widgets.text("lsh_max_validated_links", "500000", "Maximum validated LSH links")

LINK_KM = float(dbutils.widgets.get("link_km"))
LINK_HOURS = float(dbutils.widgets.get("link_hours"))
IMPACT_KM = float(dbutils.widgets.get("impact_km"))
NUM_PERMUTATIONS = int(dbutils.widgets.get("minhash_permutations"))
BANDS = int(dbutils.widgets.get("lsh_bands"))
ROWS_PER_BAND = int(dbutils.widgets.get("lsh_rows_per_band"))
MAX_LSH_NEIGHBORS = int(dbutils.widgets.get("lsh_max_neighbors_per_record"))
MAX_VALIDATED_LINKS = int(dbutils.widgets.get("lsh_max_validated_links"))

if LINK_KM <= 0 or LINK_HOURS <= 0 or IMPACT_KM <= 0:
    raise ValueError("link_km, link_hours, and impact_km must be positive")
if NUM_PERMUTATIONS <= 0 or BANDS <= 0 or ROWS_PER_BAND <= 0:
    raise ValueError("MinHash and LSH parameters must be positive")
if MAX_LSH_NEIGHBORS <= 0 or MAX_VALIDATED_LINKS <= 0:
    raise ValueError("LSH neighbor and validated-link limits must be positive")
if BANDS * ROWS_PER_BAND != NUM_PERMUTATIONS:
    raise ValueError("lsh_bands * lsh_rows_per_band must equal minhash_permutations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Read and prepare detections
# MAGIC
# MAGIC The expensive work is split into separate cells. This version processes one LSH band at a time,
# MAGIC validates pairs immediately, and never stores the full candidate-pair set.

# COMMAND ----------

started = time.time()
KEY = ["latitude", "longitude", "acq_ts_utc", "satellite"]

det = (
    spark.table(t("silver_fire_detections"))
    .withColumn("satellite", F.coalesce("satellite", F.lit("")))
    .select(*KEY, "date_local", "frp_mw", "county_fips", "confidence_class")
    .where(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("acq_ts_utc").isNotNull()
    )
)

rows = (
    det.withColumn("_epoch_s", F.unix_timestamp("acq_ts_utc"))
    .orderBy("acq_ts_utc", "latitude", "longitude", "satellite")
    .collect()
)
if not rows:
    raise ValueError("No FIRMS detections with coordinates and acquisition time were found")

reference_latitude = (
    det.select(F.avg("latitude").alias("reference_latitude")).first()["reference_latitude"]
    or 38.5
)
spatial_cell_km = LINK_KM
time_bucket_hours = LINK_HOURS
lat_cell_deg = spatial_cell_km / 111.32
lon_cell_deg = spatial_cell_km / (
    111.32 * max(math.cos(math.radians(float(reference_latitude))), 0.1)
)
spatial_neighbor_radius = max(1, math.ceil(LINK_KM / spatial_cell_km))
time_neighbor_radius = 1

print(
    f"Read {len(rows):,} detections; "
    f"spatial cell={spatial_cell_km:g} km; time bucket={time_bucket_hours:g} hours."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Build MinHash signatures

# COMMAND ----------

def detection_key(row):
    return f"{row.latitude}|{row.longitude}|{row.acq_ts_utc}|{row.satellite}"


def coordinate_time_tokens(latitude, longitude, epoch_hours):
    cell_y = math.floor(float(latitude) / lat_cell_deg)
    cell_x = math.floor(float(longitude) / lon_cell_deg)
    time_bucket = math.floor(float(epoch_hours) / time_bucket_hours)
    tokens = set()
    for dy in range(-spatial_neighbor_radius, spatial_neighbor_radius + 1):
        for dx in range(-spatial_neighbor_radius, spatial_neighbor_radius + 1):
            for dt in range(-time_neighbor_radius, time_neighbor_radius + 1):
                tokens.add(f"st:{cell_y + dy}:{cell_x + dx}:{time_bucket + dt}")
    return tokens


def seeded_hash(seed, token):
    payload = f"{seed}|{token}".encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big")


def stable_token_hash(token):
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big")


HASH_MULTIPLIERS = None
HASH_OFFSETS = None
if np is not None:
    hash_rng = random.Random(20261117)
    HASH_MULTIPLIERS = np.array(
        [hash_rng.getrandbits(64) | 1 for _ in range(NUM_PERMUTATIONS)], dtype=np.uint64
    )
    HASH_OFFSETS = np.array(
        [hash_rng.getrandbits(64) for _ in range(NUM_PERMUTATIONS)], dtype=np.uint64
    )


def minhash_signature(tokens):
    tokens = tokens or {"__EMPTY__"}
    if np is not None:
        minimums = np.full(NUM_PERMUTATIONS, np.iinfo(np.uint64).max, dtype=np.uint64)
        for token in tokens:
            token_hash = np.uint64(stable_token_hash(token))
            values = (HASH_MULTIPLIERS * token_hash) + HASH_OFFSETS
            minimums = np.minimum(minimums, values)
        return tuple(int(value) for value in minimums)
    return tuple(
        min(seeded_hash(seed, token) for token in tokens)
        for seed in range(NUM_PERMUTATIONS)
    )


signature_phase_started = time.time()
records = []
for row in rows:
    epoch_hours = float(row["_epoch_s"]) / 3600.0
    records.append(
        {
            "detection_id": detection_key(row),
            "epoch_hours": epoch_hours,
            "latitude": float(row.latitude),
            "longitude": float(row.longitude),
            "signature": minhash_signature(
                coordinate_time_tokens(row.latitude, row.longitude, epoch_hours)
            ),
        }
    )

print(
    f"MinHash signatures built: {len(records):,}; "
    f"signature phase: {time.time() - signature_phase_started:.1f}s"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Initialize exact validation state

# COMMAND ----------

def haversine_km(latitude_1, longitude_1, latitude_2, longitude_2):
    lat_1 = math.radians(latitude_1)
    lat_2 = math.radians(latitude_2)
    d_lat = lat_2 - lat_1
    d_lon = math.radians(longitude_2 - longitude_1)
    value = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat_1) * math.cos(lat_2) * math.sin(d_lon / 2) ** 2
    )
    value = min(1.0, max(0.0, value))
    return 2.0 * 6371.0 * math.asin(math.sqrt(value))


class UnionFind:
    def __init__(self, size):
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, item):
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left, right):
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return False
        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1
        return True


max_latitude_delta = LINK_KM / 110.57
# California spans roughly 32–42 degrees north. 0.70 is deliberately broad.
max_longitude_delta = LINK_KM / (111.32 * 0.70)

union_find = UnionFind(len(records))
validated_edges = []
validated_pair_index = {}
lsh_cap_reached = False
total_lsh_stats = {
    "bands_processed": 0,
    "buckets": 0,
    "non_singleton_buckets": 0,
    "pairs_examined": 0,
    "pairs_within_bbox": 0,
    "validated_links": 0,
}


def process_lsh_band(band_index):
    global lsh_cap_reached
    if lsh_cap_reached:
        return {
            "band": band_index,
            "buckets": 0,
            "non_singleton_buckets": 0,
            "pairs_examined": 0,
            "pairs_within_bbox": 0,
            "validated_links": 0,
            "skipped": True,
        }

    band_started = time.time()
    start = band_index * ROWS_PER_BAND
    end = start + ROWS_PER_BAND
    band_buckets = defaultdict(list)

    for record_index, record in enumerate(records):
        band_buckets[record["signature"][start:end]].append(record_index)

    pairs_examined = 0
    pairs_within_bbox = 0
    validated_in_band = 0
    non_singleton_buckets = 0

    for bucket_ids in band_buckets.values():
        bucket_size = len(bucket_ids)
        if bucket_size < 2:
            continue

        non_singleton_buckets += 1
        for left_pos in range(bucket_size - 1):
            left_index = bucket_ids[left_pos]
            left = records[left_index]
            left_time = left["epoch_hours"]
            right_stop = min(bucket_size, left_pos + 1 + MAX_LSH_NEIGHBORS)

            for right_pos in range(left_pos + 1, right_stop):
                right_index = bucket_ids[right_pos]
                right = records[right_index]
                right_time = right["epoch_hours"]

                if right_time - left_time > LINK_HOURS:
                    break

                pairs_examined += 1
                if (
                    abs(left["latitude"] - right["latitude"]) > max_latitude_delta
                    or abs(left["longitude"] - right["longitude"]) > max_longitude_delta
                ):
                    continue

                pairs_within_bbox += 1
                distance_km = haversine_km(
                    left["latitude"],
                    left["longitude"],
                    right["latitude"],
                    right["longitude"],
                )
                if distance_km > LINK_KM:
                    continue

                pair_key = (left_index, right_index)
                edge_position = validated_pair_index.get(pair_key)
                if edge_position is None:
                    if len(validated_edges) >= MAX_VALIDATED_LINKS:
                        lsh_cap_reached = True
                        break

                    minhash_estimate = sum(
                        left_value == right_value
                        for left_value, right_value in zip(
                            left["signature"], right["signature"]
                        )
                    ) / NUM_PERMUTATIONS
                    union_find.union(left_index, right_index)
                    validated_pair_index[pair_key] = len(validated_edges)
                    validated_edges.append(
                        {
                            "left_index": left_index,
                            "right_index": right_index,
                            "distance_km": float(distance_km),
                            "time_gap_hours": float(right_time - left_time),
                            "minhash_estimate": float(minhash_estimate),
                            "shared_lsh_bands": 1,
                        }
                    )
                    validated_in_band += 1
                else:
                    validated_edges[edge_position]["shared_lsh_bands"] += 1

            if lsh_cap_reached:
                break
        if lsh_cap_reached:
            break

    stats = {
        "band": band_index,
        "buckets": len(band_buckets),
        "non_singleton_buckets": non_singleton_buckets,
        "pairs_examined": pairs_examined,
        "pairs_within_bbox": pairs_within_bbox,
        "validated_links": validated_in_band,
        "elapsed_seconds": round(time.time() - band_started, 1),
        "skipped": False,
    }
    del band_buckets
    return stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Process one LSH band
# MAGIC
# MAGIC Run the following band cells in order. Each cell reports its own bucket, pair, and validation counts.
# MAGIC The hard link cap prevents the driver from becoming unresponsive.

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 1 of 16

# COMMAND ----------

band_stats = process_lsh_band(0)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 2 of 16

# COMMAND ----------

band_stats = process_lsh_band(1)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 3 of 16

# COMMAND ----------

band_stats = process_lsh_band(2)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 4 of 16

# COMMAND ----------

band_stats = process_lsh_band(3)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 5 of 16

# COMMAND ----------

band_stats = process_lsh_band(4)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 6 of 16

# COMMAND ----------

band_stats = process_lsh_band(5)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 7 of 16

# COMMAND ----------

band_stats = process_lsh_band(6)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 8 of 16

# COMMAND ----------

band_stats = process_lsh_band(7)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 9 of 16

# COMMAND ----------

band_stats = process_lsh_band(8)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 10 of 16

# COMMAND ----------

band_stats = process_lsh_band(9)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 11 of 16

# COMMAND ----------

band_stats = process_lsh_band(10)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 12 of 16

# COMMAND ----------

band_stats = process_lsh_band(11)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 13 of 16

# COMMAND ----------

band_stats = process_lsh_band(12)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 14 of 16

# COMMAND ----------

band_stats = process_lsh_band(13)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 15 of 16

# COMMAND ----------

band_stats = process_lsh_band(14)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LSH band 16 of 16

# COMMAND ----------

band_stats = process_lsh_band(15)
total_lsh_stats["bands_processed"] += 0 if band_stats.get("skipped") else 1
total_lsh_stats["buckets"] += band_stats.get("buckets", 0)
total_lsh_stats["non_singleton_buckets"] += band_stats.get("non_singleton_buckets", 0)
total_lsh_stats["pairs_examined"] += band_stats.get("pairs_examined", 0)
total_lsh_stats["pairs_within_bbox"] += band_stats.get("pairs_within_bbox", 0)
total_lsh_stats["validated_links"] = len(validated_edges)
print(band_stats)
print(f"Total validated links: {len(validated_edges):,}; cap reached: {lsh_cap_reached}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Assign events and write LSH outputs

# COMMAND ----------

roots = [union_find.find(index) for index in range(len(records))]
first_index_by_root = {}
for index, root in enumerate(roots):
    first_index_by_root[root] = min(index, first_index_by_root.get(root, index))

event_id_by_root = {
    root: event_id(records[first_index]["detection_id"], rows[first_index].acq_ts_utc.date())
    for root, first_index in first_index_by_root.items()
}
event_labels = [event_id_by_root[root] for root in roots]
event_ids_by_index = event_labels
detection_ids = [record["detection_id"] for record in records]

member_rows = [
    (
        detection_ids[index],
        row.latitude,
        row.longitude,
        row.acq_ts_utc,
        row.satellite,
        event_labels[index],
    )
    for index, row in enumerate(rows)
]
members = spark.createDataFrame(
    member_rows,
    "detection_id string, latitude double, longitude double, acq_ts_utc timestamp, "
    "satellite string, event_id string",
)
(
    members.withColumn("_built_at_utc", F.current_timestamp())
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(t("silver_fire_event_members"))
)
log_step(
    "silver_fire_event_members",
    "silver",
    len(rows),
    started,
    {
        "bands_processed": total_lsh_stats["bands_processed"],
        "buckets": total_lsh_stats["buckets"],
        "pairs_examined": total_lsh_stats["pairs_examined"],
        "validated_links": len(validated_edges),
        "candidate_cap_reached": int(lsh_cap_reached),
        "max_lsh_neighbors_per_record": MAX_LSH_NEIGHBORS,
        "max_validated_links": MAX_VALIDATED_LINKS,
    },
)

lsh_edge_rows = []
for edge in validated_edges:
    left_index = edge["left_index"]
    right_index = edge["right_index"]
    left_row = rows[left_index]
    right_row = rows[right_index]
    source_id = records[left_index]["detection_id"]
    target_id = records[right_index]["detection_id"]
    lsh_edge_rows.append(
        (
            f"L:{source_id}->{target_id}",
            event_labels[left_index],
            source_id,
            target_id,
            float(left_row.latitude),
            float(left_row.longitude),
            float(right_row.latitude),
            float(right_row.longitude),
            left_row.acq_ts_utc,
            right_row.acq_ts_utc,
            left_row.date_local,
            right_row.date_local,
            float(left_row.frp_mw) if left_row.frp_mw is not None else None,
            float(right_row.frp_mw) if right_row.frp_mw is not None else None,
            edge["distance_km"],
            edge["time_gap_hours"],
            edge["minhash_estimate"],
            edge["shared_lsh_bands"],
            "fire_movement_lsh",
        )
    )

lsh_edge_schema = (
    "edge_id string, event_id string, source_detection_id string, target_detection_id string, "
    "source_latitude double, source_longitude double, target_latitude double, target_longitude double, "
    "source_time timestamp, target_time timestamp, source_date_local date, target_date_local date, "
    "source_frp_mw double, target_frp_mw double, distance_km double, elapsed_hours double, "
    "minhash_estimate double, shared_lsh_bands int, edge_type string"
)
(
    spark.createDataFrame(lsh_edge_rows, lsh_edge_schema)
    .withColumn("_built_at_utc", F.current_timestamp())
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(t("gold_fire_lsh_detection_edges"))
)
log_step(
    "gold_fire_lsh_detection_edges",
    "gold",
    len(lsh_edge_rows),
    started,
    {
        "validated_links": len(validated_edges),
        "candidate_cap_reached": int(lsh_cap_reached),
    },
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Persist chronological fire-movement links
# MAGIC
# MAGIC The LSH links above define the approximate event membership and are retained in
# MAGIC `gold_fire_lsh_detection_edges`. For the dashboard, also persist a separate
# MAGIC nearest-successor edge: each detection points to the closest later detection
# MAGIC within the same distance and time limits. This preserves the original movement view.

# COMMAND ----------

started = time.time()
fire_edge_rows = []

# For California-scale distances, a local equirectangular approximation is
# sufficiently accurate and avoids the sin/cos/asin work of Haversine for every
# candidate pair. Compare squared distances so sqrt is only used for the final
# selected edge.
latitudes = [float(row["latitude"]) for row in rows]
longitudes = [float(row["longitude"]) for row in rows]
epochs = [int(row["_epoch_s"]) for row in rows]
cos_latitudes = [math.cos(math.radians(latitude)) for latitude in latitudes]
event_ids_by_index = event_labels
detection_ids = [record["detection_id"] for record in records]

max_latitude = max(abs(CA_BBOX[1]), abs(CA_BBOX[3]))
longitude_km_per_degree = 111.32 * math.cos(math.radians(max_latitude))
cell_size = LINK_KM / min(110.57, longitude_km_per_degree)
distance_limit_sq = LINK_KM * LINK_KM
time_limit_seconds = int(LINK_HOURS * 3600)
future_grid = defaultdict(list)

for row_index in range(len(rows)):
    grid_key = (
        event_ids_by_index[row_index],
        int(math.floor(latitudes[row_index] / cell_size)),
        int(math.floor(longitudes[row_index] / cell_size)),
    )
    future_grid[grid_key].append((epochs[row_index], row_index))

for current_index in range(len(rows) - 1):
    best = None

    current_latitude = latitudes[current_index]
    current_longitude = longitudes[current_index]
    current_epoch = epochs[current_index]
    max_epoch = current_epoch + time_limit_seconds
    current_event_id = event_ids_by_index[current_index]
    current_grid_x = int(math.floor(current_latitude / cell_size))
    current_grid_y = int(math.floor(current_longitude / cell_size))

    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            bucket = future_grid.get(
                (current_event_id, current_grid_x + dx, current_grid_y + dy), []
            )
            start_pos = bisect_right(bucket, (current_epoch, current_index))
            end_pos = bisect_right(bucket, (max_epoch, len(rows)))

            for bucket_position in range(start_pos, end_pos):
                _, next_index = bucket[bucket_position]
                if next_index <= current_index:
                    continue

                delta_latitude = latitudes[next_index] - current_latitude
                delta_longitude = longitudes[next_index] - current_longitude
                delta_x = delta_longitude * 111.32 * (
                    cos_latitudes[current_index] + cos_latitudes[next_index]
                ) / 2.0
                delta_y = delta_latitude * 110.57
                distance_sq = delta_x * delta_x + delta_y * delta_y

                if distance_sq <= distance_limit_sq:
                    elapsed_seconds = epochs[next_index] - current_epoch
                    candidate_key = (distance_sq, elapsed_seconds, next_index)
                    if best is None or candidate_key < best[0]:
                        best = (candidate_key, next_index)

    if best is not None:
        (distance_sq, elapsed_seconds, _), next_index = best
        current = rows[current_index]
        candidate = rows[next_index]
        source_id = detection_ids[current_index]
        target_id = detection_ids[next_index]
        distance_km = math.sqrt(distance_sq)
        elapsed_hours = elapsed_seconds / 3600.0
        fire_edge_rows.append(
            (
                f"F:{source_id}->{target_id}",
                current_event_id,
                source_id,
                target_id,
                current_latitude,
                current_longitude,
                latitudes[next_index],
                longitudes[next_index],
                current["acq_ts_utc"],
                candidate["acq_ts_utc"],
                current["date_local"],
                candidate["date_local"],
                float(current["frp_mw"]) if current["frp_mw"] is not None else None,
                float(candidate["frp_mw"]) if candidate["frp_mw"] is not None else None,
                distance_km,
                elapsed_hours,
                "fire_progression",
            )
        )

fire_edge_schema = (
    "edge_id string, event_id string, source_detection_id string, target_detection_id string, "
    "source_latitude double, source_longitude double, target_latitude double, target_longitude double, "
    "source_time timestamp, target_time timestamp, source_date_local date, target_date_local date, "
    "source_frp_mw double, target_frp_mw double, distance_km double, elapsed_hours double, edge_type string"
)
(
    spark.createDataFrame(fire_edge_rows, fire_edge_schema)
    .withColumn("_built_at_utc", F.current_timestamp())
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(t("gold_fire_detection_edges"))
)
log_step("gold_fire_detection_edges", "gold", len(fire_edge_rows), started)

# COMMAND ----------

# MAGIC %md ## 3 · Event summaries and daily growth

# COMMAND ----------

started = time.time()
m = spark.table(t("silver_fire_event_members"))
d = (spark.table(t("silver_fire_detections")).withColumn("satellite", F.coalesce("satellite", F.lit("")))
     .select(*KEY, "date_local", "frp_mw", "county_fips", "confidence_class")
     .join(m.select(*KEY, "event_id"), KEY, "inner"))
w = F.greatest(F.coalesce("frp_mw", F.lit(0.0)), F.lit(0.1))          # FRP weight for the centroid

daily = (d.groupBy("event_id", "date_local").agg(
            F.count("*").alias("detections"), F.round(F.sum("frp_mw"), 1).alias("frp_total_mw"),
            F.round(F.max("frp_mw"), 1).alias("frp_max_mw"),
            (F.sum(F.col("latitude") * w) / F.sum(w)).alias("centroid_lat"),
            (F.sum(F.col("longitude") * w) / F.sum(w)).alias("centroid_lon")))
daily.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_fire_event_day"))

peak = (daily.withColumn("_r", F.row_number().over(Window.partitionBy("event_id").orderBy(F.desc("frp_total_mw"))))
             .where("_r = 1").select("event_id", F.col("date_local").alias("peak_day"), F.col("frp_total_mw").alias("peak_day_frp_mw")))
main_county = (d.groupBy("event_id", "county_fips").count()
                .withColumn("_r", F.row_number().over(Window.partitionBy("event_id").orderBy(F.desc("count"), "county_fips")))
                .where("_r = 1").select("event_id", F.col("county_fips").alias("primary_county_fips")))
events = (d.groupBy("event_id").agg(
            F.min("acq_ts_utc").alias("first_seen_utc"), F.max("acq_ts_utc").alias("last_seen_utc"),
            F.min("date_local").alias("first_day"), F.max("date_local").alias("last_day"),
            F.countDistinct("date_local").alias("active_days"), F.count("*").alias("detections"),
            F.sum(F.when(F.col("confidence_class") == "high", 1).otherwise(0)).alias("high_conf_detections"),
            F.round(F.sum("frp_mw"), 1).alias("frp_total_mw"), F.round(F.max("frp_mw"), 1).alias("frp_max_mw"),
            F.round(F.sum(F.col("latitude") * w) / F.sum(w), 4).alias("centroid_lat"),
            F.round(F.sum(F.col("longitude") * w) / F.sum(w), 4).alias("centroid_lon"),
            F.min("latitude").alias("min_lat"), F.max("latitude").alias("max_lat"),
            F.min("longitude").alias("min_lon"), F.max("longitude").alias("max_lon"),
            F.array_sort(F.collect_set("county_fips")).alias("counties_burned"))
          .withColumn("duration_hours", F.round((F.unix_timestamp("last_seen_utc") - F.unix_timestamp("first_seen_utc")) / 3600, 1))
          .withColumn("size_class", F.when(F.col("detections") >= 500, "large").when(F.col("detections") >= 10, "medium")
                                     .when(F.col("detections") >= 2, "small").otherwise("single"))
          .join(peak, "event_id").join(main_county, "event_id")
          .join(spark.table(t("gold_dim_county")).select(F.col("county_fips").alias("primary_county_fips"),
                                                          F.col("county_name").alias("primary_county")), "primary_county_fips", "left")
          .withColumn("event_label", F.concat_ws(" ", F.coalesce("primary_county", F.lit("Unknown")), F.lit("fire, from"),
                                                 F.date_format("first_day", "d MMM yyyy")))
          .withColumn("_built_at_utc", F.current_timestamp()))
events.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_fire_events"))
log_step("gold_fire_events", "gold", spark.table(t("gold_fire_events")).count(), started)

# COMMAND ----------

# MAGIC %md ## 4 · Which counties could each event smoke?
# MAGIC For each event-day, every county whose centre lies within `impact_km` of the event's daily centroid.
# MAGIC `upwind_alignment` = cos(bearing from county to event − wind-from direction at the county): +1 means the event is
# MAGIC directly upwind (its smoke blows toward the county), −1 directly downwind. NULL when the county had no wind data
# MAGIC that day (station wind coverage is sparse; see the data-quality notebook).

# COMMAND ----------

started = time.time()
cty = spark.table(t("gold_dim_county")).select("county_fips", "county_name", "c_lat", "c_lon")
fact = spark.table(t("gold_fact_county_day")).select(
    "county_fips", "date_local", "wind_dir_deg", "wind_mean_ms", "pm25_mean_ugm3"
)
nxt = fact.select("county_fips", F.date_sub("date_local", 1).alias("date_local"), F.col("pm25_mean_ugm3").alias("pm25_next_day_ugm3"))

def hav_km(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1); dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    return 2 * 6371.0 * F.asin(F.sqrt(a))

def bearing_deg(lat1, lon1, lat2, lon2):
    y = F.sin(F.radians(lon2 - lon1)) * F.cos(F.radians(lat2))
    x = F.cos(F.radians(lat1)) * F.sin(F.radians(lat2)) - F.sin(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.cos(F.radians(lon2 - lon1))
    return (F.degrees(F.atan2(y, x)) + 360) % 360

impact = (spark.table(t("gold_fire_event_day")).alias("e")
          .crossJoin(F.broadcast(cty).alias("c"))
          .withColumn("distance_km", F.round(hav_km(F.col("c.c_lat"), F.col("c.c_lon"), F.col("e.centroid_lat"), F.col("e.centroid_lon")), 1))
          .where(F.col("distance_km") <= IMPACT_KM)
          .withColumn("bearing_county_to_event", F.round(bearing_deg(F.col("c.c_lat"), F.col("c.c_lon"), F.col("e.centroid_lat"), F.col("e.centroid_lon")), 0))
          .select("e.event_id", "e.date_local", "c.county_fips", "c.county_name", "distance_km", "bearing_county_to_event",
                  F.col("e.frp_total_mw").alias("event_day_frp_mw"), F.col("e.detections").alias("event_day_detections"))
          .join(fact, ["county_fips", "date_local"], "left").join(nxt, ["county_fips", "date_local"], "left")
          .withColumn("upwind_alignment", F.round(F.cos(F.radians(F.col("bearing_county_to_event") - F.col("wind_dir_deg"))), 2))
          .withColumn("event_upwind", F.when(F.col("upwind_alignment").isNull(), F.lit(None).cast("boolean"))
                                       .otherwise(F.col("upwind_alignment") >= 0.5))
          .withColumn("_built_at_utc", F.current_timestamp()))
impact.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_fire_event_county_impact"))
log_step("gold_fire_event_county_impact", "gold", spark.table(t("gold_fire_event_county_impact")).count(), started)

# COMMAND ----------

# MAGIC %md ## 5 · Checks

# COMMAND ----------

n_det = (
    spark.table(t("silver_fire_detections"))
    .where(
        F.col("acq_ts_utc").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
    )
    .count()
)
n_mem = spark.table(t("silver_fire_event_members")).count()
n_dup = n_mem - spark.table(t("silver_fire_event_members")).select(*KEY).distinct().count()
ev = spark.table(t("gold_fire_events"))
event_count = ev.count()
size_classes = {r.size_class: r["count"] for r in ev.groupBy("size_class").count().collect()}
store_log(
    spark,
    "INFO" if n_det == n_mem and n_dup == 0 else "ERROR",
    SOURCE_FILE,
    f"Event assignment check: detections={n_det:,}, assigned={n_mem:,}, "
    f"duplicates={n_dup}, events={event_count:,}, size_classes={size_classes}.",
)
assert n_det == n_mem and n_dup == 0, "every detection must belong to exactly one event"

# COMMAND ----------

# MAGIC %md ## 6 · Results (dashboard tiles)

# COMMAND ----------

# Largest events
largest_events = spark.sql(f"""
SELECT event_id, event_label, first_day, last_day, active_days, detections, frp_total_mw, frp_max_mw,
       centroid_lat, centroid_lon, peak_day, size(counties_burned) AS counties_burned
FROM {t('gold_fire_events')} ORDER BY detections DESC LIMIT 15""")

# COMMAND ----------

# Counties around the largest event: distance, wind alignment, PM2.5 that day and the next
top_event_counties = spark.sql(f"""
WITH top AS (SELECT event_id FROM {t('gold_fire_events')} ORDER BY detections DESC LIMIT 1)
SELECT i.county_name, count(*) AS days_within_radius, round(min(i.distance_km), 0) AS closest_km,
       sum(CASE WHEN i.event_upwind THEN 1 ELSE 0 END) AS days_event_upwind,
       round(avg(i.pm25_mean_ugm3), 1) AS avg_pm25_same_day, round(avg(i.pm25_next_day_ugm3), 1) AS avg_pm25_next_day
FROM {t('gold_fire_event_county_impact')} i JOIN top USING (event_id)
GROUP BY i.county_name ORDER BY days_within_radius DESC, closest_km""")

# COMMAND ----------

# Daily number of active events and total fire power (timeline)
event_timeline = spark.sql(f"""
SELECT date_local, count(DISTINCT event_id) AS active_events,
       sum(CASE WHEN detections >= 50 THEN 1 ELSE 0 END) AS events_with_50plus_detections,
       round(sum(frp_total_mw), 0) AS total_frp_mw
FROM {t('gold_fire_event_day')} GROUP BY date_local ORDER BY date_local""")

# COMMAND ----------

# Does PM2.5 depend on whether an event is upwind? (county-days with wind data only)
upwind_summary = spark.sql(f"""
SELECT CASE WHEN event_upwind THEN 'event upwind' ELSE 'event not upwind' END AS wind_relation,
       count(*) AS county_event_days, round(avg(pm25_mean_ugm3), 1) AS avg_pm25_same_day,
       round(avg(pm25_next_day_ugm3), 1) AS avg_pm25_next_day
FROM {t('gold_fire_event_county_impact')}
WHERE event_upwind IS NOT NULL AND event_day_frp_mw >= 1000
GROUP BY 1""")

store_log(
    spark,
    "INFO",
    SOURCE_FILE,
    "Prepared dashboard result sets: "
    f"largest_events={largest_events.count():,}, "
    f"top_event_counties={top_event_counties.count():,}, "
    f"event_timeline={event_timeline.count():,}, "
    f"upwind_summary={upwind_summary.count():,}.",
)

