# Databricks notebook source
# MAGIC %md
# MAGIC # Library · Streaming, LSH and privacy algorithms (from scratch)
# MAGIC Textbook versions (Leskovec, Rajaraman & Ullman, *Mining of Massive Datasets*, ch. 3–4, plus Dwork & Roth for
# MAGIC differential privacy and Sweeney / Machanavajjhala et al. for k-anonymity / l-diversity).
# MAGIC Pure Python + hashlib so each idea is visible. Loaded by `08_streaming` and `09_lsh_privacy` with `%run ./lib_stream_algorithms`.
# MAGIC
# MAGIC | Class / function | Answers | Memory |
# MAGIC | --- | --- | --- |
# MAGIC | `ReservoirSampler` | a uniform random sample of k items from a stream of unknown length | O(k) |
# MAGIC | `BloomFilter` | "have I seen this key before?" (no false negatives, tunable false positives) | m bits |
# MAGIC | `FlajoletMartin` | number of distinct items | O(#hashes) |
# MAGIC | `DGIM` | number of 1s in the last N bits of a bit stream (≤ 50% error) | O(log² N) |
# MAGIC | `AMSMoment` | second frequency moment (surprise number) Σ mᵢ² | O(#variables) |
# MAGIC | `StreamingComponents` | connected components of a graph arriving as an edge stream (union–find) | O(#nodes) |
# MAGIC | `MinHashLSH` | candidate pairs of similar sets (Jaccard) via minhash signatures + banding | O(#sets · #hashes) |
# MAGIC | `laplace_mechanism`, `k_anonymize_*`, `l_diversity` | privacy-preserving release of aggregates / records | — |

# COMMAND ----------

import hashlib
import math
import random
import struct
from collections import defaultdict


def h64(value, seed=0):
    """Deterministic 64-bit hash of any value (seeded), via BLAKE2b."""
    b = hashlib.blake2b(f"{seed}|{value}".encode(), digest_size=8).digest()
    return struct.unpack("<Q", b)[0]


MERSENNE = (1 << 61) - 1


class HashFamily:
    """Universal hash family h_i(x) = (a_i * x + b_i) mod p, p = 2^61 - 1, applied to one base hash of x.
    One expensive hash per item, many cheap independent-looking hashes (the standard MinHash construction)."""

    def __init__(self, n, seed=0):
        rng = random.Random(seed)
        self.ab = [(rng.randrange(1, MERSENNE), rng.randrange(0, MERSENNE)) for _ in range(n)]

    def __call__(self, item):
        x = h64(item, 7) % MERSENNE
        return [(a * x + b) % MERSENNE for a, b in self.ab]


# ---------------------------------------------------------------- Reservoir sampling (Algorithm R)
class ReservoirSampler:
    """Keep a uniform random sample of size k: item n (1-based) replaces a random slot with probability k/n."""

    def __init__(self, k, seed=0):
        self.k, self.n, self.sample, self.rng = k, 0, [], random.Random(seed)

    def add(self, item):
        self.n += 1
        if len(self.sample) < self.k:
            self.sample.append(item)
        else:
            j = self.rng.randrange(self.n)          # 0..n-1
            if j < self.k:
                self.sample[j] = item


# ---------------------------------------------------------------- Bloom filter
class BloomFilter:
    """m-bit array, k hash functions. Optimal k = (m/n) ln 2; false-positive rate ≈ (1 - e^{-kn/m})^k."""

    def __init__(self, expected_items, fp_rate=0.01):
        self.m = max(8, int(-expected_items * math.log(fp_rate) / (math.log(2) ** 2)))
        self.k = max(1, round(self.m / expected_items * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)
        self.n = 0

    def _positions(self, key):
        a, b = h64(key, 1), h64(key, 2) | 1          # Kirsch–Mitzenmacher double hashing
        return [(a + i * b) % self.m for i in range(self.k)]

    def add(self, key):
        for p in self._positions(key):
            self.bits[p >> 3] |= 1 << (p & 7)
        self.n += 1

    def __contains__(self, key):
        return all(self.bits[p >> 3] & (1 << (p & 7)) for p in self._positions(key))

    def check_and_add(self, key):
        """True if (probably) seen before; otherwise records it and returns False."""
        seen = key in self
        if not seen:
            self.add(key)
        return seen

    def theoretical_fp_rate(self):
        return (1 - math.exp(-self.k * self.n / self.m)) ** self.k


# ---------------------------------------------------------------- Flajolet–Martin
class FlajoletMartin:
    """Distinct-count estimate. For each hash h, R = max trailing zeros of h(x); estimate 2^R.
    Combine many hashes: split into groups, average within groups, take the median of the group averages."""

    def __init__(self, num_hashes=64, group_size=8, seed=100):
        self.num_hashes, self.group_size = num_hashes, group_size
        self.R = [0] * num_hashes
        self.hashes = HashFamily(num_hashes, seed)

    @staticmethod
    def _tz(x):
        return (x & -x).bit_length() - 1 if x else 61

    def add(self, item):
        for i, hv in enumerate(self.hashes(item)):
            r = self._tz(hv)
            if r > self.R[i]:
                self.R[i] = r

    def estimate_raw(self):
        """MMDS textbook combination: median of group means of 2^R (simple, but biased upward)."""
        ests = [2 ** r for r in self.R]
        groups = [ests[i:i + self.group_size] for i in range(0, len(ests), self.group_size)]
        return _median([sum(g) / len(g) for g in groups])

    def estimate(self):
        """Original Flajolet–Martin (1985) estimator: 2^(mean R) / φ with φ ≈ 0.77351 within each group, median across groups."""
        groups = [self.R[i:i + self.group_size] for i in range(0, len(self.R), self.group_size)]
        return _median([2 ** (sum(g) / len(g)) / 0.77351 for g in groups])


def _median(values):
    v = sorted(values); m = len(v) // 2
    return v[m] if len(v) % 2 else (v[m - 1] + v[m]) / 2


# ---------------------------------------------------------------- DGIM
class DGIM:
    """Count 1s in the last N bits. Buckets (timestamp of most recent 1, size = power of 2), at most two per size.
    Estimate = sum of all buckets' sizes except half of the oldest one."""

    def __init__(self, N):
        self.N, self.t, self.buckets = N, 0, []      # buckets: list of [timestamp, size], newest first

    def add(self, bit):
        self.t += 1
        while self.buckets and self.buckets[-1][0] <= self.t - self.N:   # oldest bucket left the window
            self.buckets.pop()
        if not bit:
            return
        self.buckets.insert(0, [self.t, 1])
        size = 1
        while True:                                   # merge when three buckets share a size
            idx = [i for i, b in enumerate(self.buckets) if b[1] == size]
            if len(idx) <= 2:
                break
            i1, i2 = idx[-2], idx[-1]                 # the two oldest of that size
            self.buckets[i1] = [self.buckets[i1][0], size * 2]
            del self.buckets[i2]
            size *= 2

    def estimate(self):
        if not self.buckets:
            return 0
        return sum(b[1] for b in self.buckets[:-1]) + self.buckets[-1][1] / 2

    def num_buckets(self):
        return len(self.buckets)


# ---------------------------------------------------------------- AMS second moment
class AMSMoment:
    """Alon–Matias–Szegedy estimate of F2 = Σ m_i². Each variable sits at a uniformly random position of the stream
    (re-chosen reservoir-style, so the stream length need not be known) and counts later occurrences of that element (c);
    estimate n(2c − 1), averaged over variables. Each variable's next re-sampling time is drawn directly
    (P(no re-sample up to m | at t) = t/m), so an update costs O(1) amortised instead of O(#variables)."""

    def __init__(self, num_vars=200, seed=0):
        import heapq
        self._heapq, self.rng, self.n = heapq, random.Random(seed), 0
        self.elem = [None] * num_vars
        self.count = [0] * num_vars
        self.by_elem = defaultdict(set)
        self.next_at = [(1, i) for i in range(num_vars)]          # every variable takes position 1
        heapq.heapify(self.next_at)

    def add(self, item):
        self.n += 1
        for i in self.by_elem.get(item, ()):
            self.count[i] += 1
        while self.next_at and self.next_at[0][0] == self.n:
            _, i = self._heapq.heappop(self.next_at)
            if self.elem[i] is not None:
                self.by_elem[self.elem[i]].discard(i)
            self.elem[i], self.count[i] = item, 1
            self.by_elem[item].add(i)
            u = 1.0 - self.rng.random()
            self._heapq.heappush(self.next_at, (max(self.n + 1, int(self.n / u) + 1), i))

    def estimate(self):
        vals = [self.n * (2 * c - 1) for e, c in zip(self.elem, self.count) if e is not None]
        return sum(vals) / len(vals) if vals else 0.0


# ---------------------------------------------------------------- Graph stream: connected components
class StreamingComponents:
    """Union–find over an edge stream: one pass, memory proportional to the number of nodes."""

    def __init__(self):
        self.parent, self.size = {}, {}

    def find(self, x):
        self.parent.setdefault(x, x); self.size.setdefault(x, 1)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:                 # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra; self.size[ra] += self.size[rb]

    def components(self):
        out = defaultdict(list)
        for x in list(self.parent):
            out[self.find(x)].append(x)
        return out


def fire_event_edges(detections, link_km=2.0, link_hours=24.0):
    """Stream edges between detections close in space and time. detections: iterable of
    (id, lat, lon, epoch_hours) in arrival order. Uses a grid so each point only checks nearby cells."""
    cell = link_km / 111.0
    grid = defaultdict(list)                          # (gx, gy) -> recent detections
    for did, lat, lon, th in detections:
        gx, gy = int(lat / cell), int(lon / cell)
        yield (did, did)                              # every detection is a node
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                keep = []
                for oid, olat, olon, oth in grid.get((gx + dx, gy + dy), []):
                    if th - oth > link_hours:
                        continue                      # expired: falls out of the time window
                    keep.append((oid, olat, olon, oth))
                    dkm = math.hypot((lat - olat) * 111.0, (lon - olon) * 111.0 * math.cos(math.radians(lat)))
                    if dkm <= link_km:
                        yield (did, oid)
                if (gx + dx, gy + dy) in grid:
                    grid[(gx + dx, gy + dy)] = keep
        grid[(gx, gy)].append((did, lat, lon, th))


# ---------------------------------------------------------------- MinHash + LSH banding
class MinHashLSH:
    """Signatures of num_perm minhashes; split into b bands of r rows. Pairs sharing any band bucket are candidates.
    A pair with Jaccard s becomes a candidate with probability 1 - (1 - s^r)^b (threshold ≈ (1/b)^(1/r))."""

    def __init__(self, num_perm=120, bands=30, seed=1000):
        assert num_perm % bands == 0
        self.num_perm, self.b, self.r = num_perm, bands, num_perm // bands
        self.hashes = HashFamily(num_perm, seed)

    def signature(self, items):
        sig = [MERSENNE] * self.num_perm
        for x in items:
            sig = [min(a, b) for a, b in zip(sig, self.hashes(x))]
        return sig

    def candidate_pairs(self, sets):
        sigs = {k: self.signature(v) for k, v in sets.items()}
        buckets = defaultdict(set)
        for k, s in sigs.items():
            for band in range(self.b):
                buckets[(band, tuple(s[band * self.r:(band + 1) * self.r]))].add(k)
        pairs = set()
        for members in buckets.values():
            m = sorted(members)
            for i in range(len(m)):
                for j in range(i + 1, len(m)):
                    pairs.add((m[i], m[j]))
        return pairs, sigs

    @staticmethod
    def estimated_jaccard(sig_a, sig_b):
        return sum(a == b for a, b in zip(sig_a, sig_b)) / len(sig_a)

    def threshold(self):
        return (1 / self.b) ** (1 / self.r)


def jaccard(a, b):
    a, b = set(a), set(b)
    return len(a & b) / len(a | b) if a | b else 0.0


# ---------------------------------------------------------------- Privacy
def laplace_mechanism(true_values, sensitivity, epsilon, seed=0):
    """ε-differential privacy: add Laplace(0, sensitivity/ε) noise to each released count."""
    rng = random.Random(seed)
    scale = sensitivity / epsilon
    out = []
    for v in true_values:
        u = rng.random() - 0.5
        noise = -scale * math.copysign(1, u) * math.log(1 - 2 * abs(u))
        out.append(v + noise)
    return out


def geohash(lat, lon, precision):
    """Standard base-32 geohash; shorter = coarser cell (generalisation hierarchy for k-anonymity)."""
    alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_r, lon_r, bits, even, out, ch, bit = [-90.0, 90.0], [-180.0, 180.0], [16, 8, 4, 2, 1], True, [], 0, 0
    while len(out) < precision:
        rng_, v = (lon_r, lon) if even else (lat_r, lat)
        mid = (rng_[0] + rng_[1]) / 2
        if v >= mid:
            ch |= bits[bit]; rng_[0] = mid
        else:
            rng_[1] = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            out.append(alphabet[ch]); bit, ch = 0, 0
    return "".join(out)


def k_anonymize_locations(records, k, max_precision=7, min_precision=1, max_suppression=0.05):
    """Generalise (lat, lon) to the finest geohash precision at which every kept cell holds >= k records,
    suppressing at most `max_suppression` of the records (isolated outliers) — the usual generalisation +
    suppression approach. records: list of dicts with 'lat', 'lon'. Returns (precision, kept_cells, suppressed_count)."""
    for p in range(max_precision, min_precision - 1, -1):
        cells = defaultdict(list)
        for r in records:
            cells[geohash(r["lat"], r["lon"], p)].append(r)
        small = sum(len(v) for v in cells.values() if len(v) < k)
        if small <= max_suppression * len(records):
            return p, {c: v for c, v in cells.items() if len(v) >= k}, small
    cells = defaultdict(list)
    for r in records:
        cells[geohash(r["lat"], r["lon"], min_precision)].append(r)
    return min_precision, {c: v for c, v in cells.items() if len(v) >= k}, sum(len(v) for v in cells.values() if len(v) < k)


def l_diversity(cells, sensitive_key):
    """Distinct l-diversity: the smallest number of distinct sensitive values in any equivalence class."""
    return min(len({r[sensitive_key] for r in v}) for v in cells.values()) if cells else 0
