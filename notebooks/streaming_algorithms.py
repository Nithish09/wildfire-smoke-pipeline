"""Streaming algorithms used by the wildfire batch/streaming extension.

The functions are deliberately independent of Spark where possible. The Spark
helpers use the exact canonical Silver keys documented in README.md.
"""

from __future__ import annotations

import hashlib
import math
import random
import statistics
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, Sequence


NATURAL_KEYS = {
    "firms": ("latitude", "longitude", "acq_ts_utc", "satellite"),
    "epa_aqs": ("site_id", "poc", "date_local", "sample_duration"),
    "noaa_weather": ("station", "date_local", "element"),
    "noaa_stations": ("id",),
}


def _canonical(value: Any) -> str:
    if value is None:
        return "<NULL>"
    return str(value).strip()


def canonical_key(record: Mapping[str, Any], source: str) -> str:
    """Serialize a key exactly once, with an unambiguous length prefix."""
    fields = NATURAL_KEYS[source]
    encoded = []
    for name in fields:
        value = _canonical(record.get(name))
        encoded.append(f"{len(value)}:{value}")
    return source + "|" + "|".join(encoded)


class BloomFilter:
    """A deterministic Bloom filter. Never use it as the final dedup rule."""

    def __init__(self, expected_items: int, false_positive_rate: float = 0.01):
        if expected_items < 1 or not 0 < false_positive_rate < 1:
            raise ValueError("expected_items >= 1 and 0 < false_positive_rate < 1 required")
        self.size = max(8, math.ceil(-expected_items * math.log(false_positive_rate) / math.log(2) ** 2))
        self.hash_count = max(1, round(self.size / expected_items * math.log(2)))
        self.bits = bytearray((self.size + 7) // 8)

    def _positions(self, value: str) -> Iterator[int]:
        digest = hashlib.sha256(value.encode("utf-8")).digest()
        h1 = int.from_bytes(digest[:8], "big")
        h2 = int.from_bytes(digest[8:16], "big") | 1
        for i in range(self.hash_count):
            yield (h1 + i * h2) % self.size

    def add(self, value: str) -> None:
        for pos in self._positions(value):
            self.bits[pos // 8] |= 1 << (pos % 8)

    def __contains__(self, value: str) -> bool:
        return all(self.bits[pos // 8] & (1 << (pos % 8)) for pos in self._positions(value))


def reservoir_sample(rows: Iterable[Any], sample_size: int, seed: int = 42) -> list[Any]:
    """Uniform one-pass reservoir sample of an iterable."""
    if sample_size < 0:
        raise ValueError("sample_size must be non-negative")
    rng = random.Random(seed)
    sample: list[Any] = []
    for index, row in enumerate(rows):
        if index < sample_size:
            sample.append(row)
        else:
            slot = rng.randint(0, index)
            if slot < sample_size:
                sample[slot] = row
    return sample


def flajolet_martin(
    values: Iterable[str],
    num_registers: int = 64,
    group_size: int = 4,
) -> int:
    """Estimate distinct values with grouped Flajolet-Martin registers.

    The old implementation used one register, which is too noisy for an
    evaluation notebook.  A value chooses one register from its hash and the
    remaining hash bits provide the trailing-zero rank.  We average registers
    inside groups and take the median of those group estimates, which is still
    a one-pass, bounded-memory algorithm but is much more stable.
    """
    if num_registers < 1 or group_size < 1 or num_registers % group_size:
        raise ValueError("num_registers must be a positive multiple of group_size")

    registers = [0] * num_registers
    for value in values:
        digest = hashlib.sha256(_canonical(value).encode("utf-8")).digest()
        register = int.from_bytes(digest[:4], "big") % num_registers
        remainder = int.from_bytes(digest[4:12], "big") or 1
        rank = 1
        while remainder & 1 == 0 and rank < 64:
            rank += 1
            remainder >>= 1
        registers[register] = max(registers[register], rank)

    if not any(registers):
        return 0

    group_estimates = []
    for start in range(0, num_registers, group_size):
        group = registers[start:start + group_size]
        group_estimates.append(sum(2.0 ** rank for rank in group) / len(group))
    estimate = statistics.median(group_estimates) / 0.77351
    return max(0, round(estimate))


def dgim_update(buckets: list[tuple[int, int]], bit: int, timestamp: int, max_buckets: int = 2) -> None:
    """Update DGIM buckets for a binary event stream in-place.

    Each tuple is (timestamp_of_newest_bit, bucket_size). The caller should
    discard buckets older than its window and use `dgim_query` for estimates.
    """
    if bit not in (0, 1):
        raise ValueError("DGIM input must be 0 or 1")
    if max_buckets < 2:
        raise ValueError("max_buckets must be at least 2")
    if buckets and timestamp < buckets[0][0]:
        raise ValueError("DGIM updates must arrive in non-decreasing timestamp order")
    if bit:
        buckets.insert(0, (timestamp, 1))
        while True:
            overfull_size = next(
                (size for size in sorted({size for _, size in buckets})
                 if sum(bucket_size == size for _, bucket_size in buckets) > max_buckets),
                None,
            )
            if overfull_size is None:
                break

            same = [i for i, (_, size) in enumerate(buckets) if size == overfull_size]
            newer_old = same[-2]
            oldest = same[-1]
            merged_timestamp = buckets[newer_old][0]
            del buckets[oldest]
            buckets[newer_old] = (merged_timestamp, overfull_size * 2)


def dgim_expire(buckets: list[tuple[int, int]], oldest_timestamp: int) -> None:
    """Discard buckets whose newest 1 is older than the query window."""
    buckets[:] = [(timestamp, size) for timestamp, size in buckets
                  if timestamp >= oldest_timestamp]


def dgim_query(buckets: Sequence[tuple[int, int]], oldest_timestamp: int) -> int:
    """Estimate the number of 1s since ``oldest_timestamp``.

    DGIM counts every in-window bucket, but only half of the oldest included
    bucket because that bucket may cross the window boundary. Buckets are
    stored newest first.
    """
    # Buckets are newest-first.  Stop at the first bucket whose newest 1 is
    # outside the window; do not subtract half of that outside bucket.  The
    # oldest bucket that remains inside is the only bucket that can cross the
    # left edge of the window, so only half of that bucket is counted.
    total = 0.0
    oldest_inside_size = None
    for timestamp, size in buckets:
        if timestamp < oldest_timestamp:
            break
        total += size
        oldest_inside_size = size
    if oldest_inside_size is None:
        return 0
    return max(0, math.ceil(total - oldest_inside_size / 2.0))


@dataclass
class Moments:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0
    m3: float = 0.0
    m4: float = 0.0

    def update(self, value: float) -> "Moments":
        n1, delta = self.count, value - self.mean
        self.count += 1
        delta_n = delta / self.count
        term1 = delta * delta_n * n1
        self.mean += delta_n
        self.m4 += term1 * delta_n * delta_n * (self.count * self.count - 3 * self.count + 3) + 6 * delta_n * delta_n * self.m2 - 4 * delta_n * self.m3
        self.m3 += term1 * delta_n * (self.count - 2) - 3 * delta_n * self.m2
        self.m2 += term1
        return self

    def as_dict(self) -> dict[str, float | int | None]:
        return {"count": self.count, "mean": self.mean if self.count else None,
                "variance": self.m2 / (self.count - 1) if self.count > 1 else None,
                "skewness": (math.sqrt(self.count) * self.m3 / self.m2 ** 1.5) if self.count > 2 and self.m2 else None,
                "kurtosis": (self.count * self.m4 / self.m2 ** 2 - 3) if self.count > 3 and self.m2 else None}


def compute_moments(values: Iterable[float]) -> dict[str, float | int | None]:
    state = Moments()
    for value in values:
        state.update(float(value))
    return state.as_dict()


def screen_batch_keys(rows: Iterable[Mapping[str, Any]], source: str, existing_keys: Iterable[str], false_positive_rate: float = 0.01) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """Return (candidate_new_rows, Bloom-hit_rows); exact MERGE still decides."""
    materialized = list(rows)
    existing = list(existing_keys)
    # Capacity must include every inserted key. Sizing only for the incoming
    # batch saturates the filter as the Silver key set grows.
    bloom = BloomFilter(max(len(existing) + len(materialized), 1), false_positive_rate)
    for key in existing:
        bloom.add(key)
    new_rows, hits = [], []
    for row in materialized:
        if canonical_key(row, source) in bloom:
            hits.append(row)
        else:
            new_rows.append(row)
            # This also screens repeated keys within the same incoming batch.
            # False positives are acceptable because Silver MERGE is final.
            bloom.add(canonical_key(row, source))
    return new_rows, hits
