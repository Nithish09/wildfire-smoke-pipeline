# Databricks notebook source
# MAGIC %md
# MAGIC # Library · Fire events from the detection stream (graph-stream connected components)
# MAGIC Loaded by `08_fire_events` with `%run ./lib_fire_events`. Pure Python, so it can be tested outside Databricks.
# MAGIC
# MAGIC **Idea.** Treat every FIRMS detection as a node arriving in time order. When a detection arrives, add an edge to every
# MAGIC earlier detection within `link_km` and `link_hours` (one pass, looking only at nearby grid cells and a sliding time window).
# MAGIC A union–find structure keeps the connected components as edges stream in. Each component is a **fire event**.
# MAGIC
# MAGIC * Memory: union–find over nodes plus the detections of the last `link_hours` in the grid (old ones expire).
# MAGIC * Deterministic: input sorted by (time, lat, lon, satellite); event IDs come from each event's earliest detection,
# MAGIC   so reruns on unchanged data give identical IDs.

# COMMAND ----------

import hashlib
import math
from collections import defaultdict


class UnionFind:
    """Disjoint sets with union by size and path compression (near-constant time per operation)."""

    def __init__(self, n):
        self.parent = list(range(n))
        self.size = [1] * n

    def find(self, x):
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]
        return True


def km_between(lat1, lon1, lat2, lon2):
    """Equirectangular distance: accurate to well under 1% at the 2 km scale used here."""
    x = (lon2 - lon1) * 111.32 * math.cos(math.radians((lat1 + lat2) / 2))
    y = (lat2 - lat1) * 110.57
    return math.hypot(x, y)


def stream_edges(detections, link_km=2.0, link_hours=24.0):
    """Edge stream for detections given in time order as (index, lat, lon, hours_since_epoch).
    Yields (i, j) for each earlier detection j within link_km and link_hours of detection i."""
    cell = link_km / 110.0                       # grid cell ≥ link distance, so neighbours are in the 3x3 block
    grid = defaultdict(list)
    for i, lat, lon, th in detections:
        gx, gy = int(math.floor(lat / cell)), int(math.floor(lon / cell))
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                key = (gx + dx, gy + dy)
                bucket = grid.get(key)
                if not bucket:
                    continue
                fresh = [d for d in bucket if th - d[3] <= link_hours]      # sliding time window: drop expired
                if len(fresh) != len(bucket):
                    grid[key] = fresh
                for j, la, lo, tj in fresh:
                    if km_between(lat, lon, la, lo) <= link_km:
                        yield i, j
        grid[(gx, gy)].append((i, lat, lon, th))


def cluster_detections(detections, link_km=2.0, link_hours=24.0):
    """detections: list of (lat, lon, hours_since_epoch, key) SORTED by time (then lat, lon, key).
    Returns (event_index_per_detection, stats). event_index is 0..n_events-1, ordered by first detection."""
    n = len(detections)
    uf = UnionFind(n)
    edges = merges = 0
    for i, j in stream_edges(((k, d[0], d[1], d[2]) for k, d in enumerate(detections)), link_km, link_hours):
        edges += 1
        merges += uf.union(i, j)
    first_of_root, labels = {}, [0] * n
    for k in range(n):                            # detections are time-sorted, so the first seen is the earliest
        r = uf.find(k)
        if r not in first_of_root:
            first_of_root[r] = len(first_of_root)
        labels[k] = first_of_root[r]
    return labels, {"detections": n, "edges_streamed": edges, "merges": merges, "events": len(first_of_root)}


def event_id(first_detection_key, first_date):
    """Stable, readable ID: E<yyyymmdd>-<6 hex of the earliest detection's key>."""
    return f"E{first_date:%Y%m%d}-{hashlib.sha1(first_detection_key.encode()).hexdigest()[:6]}"
