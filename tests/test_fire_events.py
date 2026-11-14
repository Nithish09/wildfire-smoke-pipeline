"""Test notebooks/lib_fire_events.py on the REAL first-week FIRMS files (data/raw/firms, 15-22 Aug 2020).

Checks: every detection gets exactly one event; the streaming grid finds exactly the same events as a brute-force
all-pairs version (on a sample); order of equal-time detections does not change events; reruns give identical IDs;
the largest events are where the big August 2020 lightning complexes burned.
Run: python3 tests/test_fire_events.py
"""
import csv, datetime as dt, glob, importlib.util, os, random, sys, time
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("lib", os.path.join(ROOT, "notebooks", "lib_fire_events.py"))
L = importlib.util.module_from_spec(spec); spec.loader.exec_module(L)
results = []


def check(name, ok, detail):
    results.append((name, ok)); print(("PASS " if ok else "FAIL ") + f"{name}: {detail}")


# Same essentials as Silver: valid coords inside the CA box, valid confidence, dedup on (lat, lon, time, satellite)
W, S, E, N = -124.5, 32.5, -114.1, 42.0
seen, dets = set(), []
for f in sorted(glob.glob(os.path.join(ROOT, "data", "raw", "firms", "firms_*.csv"))):
    for r in csv.DictReader(open(f)):
        try:
            lat, lon = float(r["latitude"]), float(r["longitude"])
        except ValueError:
            continue
        if not (S <= lat <= N and W <= lon <= E) or r["confidence"].strip().lower() not in ("l", "n", "h"):
            continue
        ts = dt.datetime.strptime(r["acq_date"] + r["acq_time"].strip().zfill(4), "%Y-%m-%d%H%M")
        k = (lat, lon, ts, r["satellite"])
        if k in seen:
            continue
        seen.add(k)
        dets.append((lat, lon, ts.timestamp() / 3600.0, f"{lat}|{lon}|{ts:%Y-%m-%dT%H:%M}|{r['satellite']}", ts, float(r["frp"] or 0)))
dets.sort(key=lambda d: (d[2], d[0], d[1], d[3]))
print(f"{len(dets):,} detections after Silver-style filtering")

t0 = time.time()
labels, stats = L.cluster_detections([d[:4] for d in dets], 2.0, 24.0)
print(f"clustered in {time.time() - t0:.1f}s: {stats}")
check("one_event_per_detection", len(labels) == len(dets) and stats["events"] == len(set(labels)),
      f"{len(dets):,} detections -> {stats['events']:,} events")

# brute force on a spatial sample (all pairs) must give the same partition
rng = random.Random(0)
box = [d for d in dets if 37.0 <= d[0] <= 38.5 and -122.5 <= d[1] <= -120.5]     # SCU / LNU area
sample = sorted(rng.sample(box, min(2500, len(box))), key=lambda d: (d[2], d[0], d[1], d[3]))
lab_s, _ = L.cluster_detections([d[:4] for d in sample], 2.0, 24.0)
uf = L.UnionFind(len(sample))
for i in range(len(sample)):
    for j in range(i):
        if abs(sample[i][2] - sample[j][2]) <= 24.0 and L.km_between(sample[i][0], sample[i][1], sample[j][0], sample[j][1]) <= 2.0:
            uf.union(i, j)
part_grid = sorted(sorted(i for i in range(len(sample)) if lab_s[i] == e) for e in set(lab_s))
groups = defaultdict(list)
for i in range(len(sample)):
    groups[uf.find(i)].append(i)
part_brute = sorted(sorted(g) for g in groups.values())
check("matches_brute_force", part_grid == part_brute, f"{len(sample)} sampled detections: {len(part_grid)} events either way")

# rerun determinism and stable IDs
labels2, _ = L.cluster_detections([d[:4] for d in dets], 2.0, 24.0)
check("rerun_identical", labels == labels2, "same events on rerun")
first = {}
for k, e in enumerate(labels):
    first.setdefault(e, k)
ids = {e: L.event_id(dets[k][3], dets[k][4].date()) for e, k in first.items()}
check("unique_event_ids", len(set(ids.values())) == len(ids), f"e.g. {list(ids.values())[:3]}")

# largest events: where are they? (FRP-weighted centroid)
agg = defaultdict(lambda: [0, 0.0, 0.0, 0.0, None, None])
for (lat, lon, th, key, ts, frp), e in zip(dets, labels):
    a = agg[e]; w = max(frp, 0.1)
    a[0] += 1; a[1] += frp; a[2] += lat * w; a[3] += lon * w
    a[4] = ts if a[4] is None or ts < a[4] else a[4]; a[5] = ts if a[5] is None or ts > a[5] else a[5]
wsum = defaultdict(float)
for (lat, lon, th, key, ts, frp), e in zip(dets, labels):
    wsum[e] += max(frp, 0.1)
top = sorted(agg.items(), key=lambda kv: -kv[1][0])[:8]
print("\nLargest events (first week):")
print(f"{'event_id':18s} {'detections':>10s} {'total FRP MW':>12s} {'centroid':>18s}  first seen -> last seen (UTC)")
for e, (n, frp, la, lo, t1, t2) in top:
    print(f"{ids[e]:18s} {n:10,d} {frp:12,.0f} {la / wsum[e]:8.2f},{lo / wsum[e]:8.2f}  {t1:%m-%d %H:%M} -> {t2:%m-%d %H:%M}")
sizes = Counter("1 detection" if a[0] == 1 else "2-9" if a[0] < 10 else "10-499" if a[0] < 500 else "500+" for a in agg.values())
print("size classes:", dict(sizes))
big_in_ca = sum(1 for e, a in top[:5] if 32.5 <= a[2] / wsum[e] <= 42 and -124.5 <= a[3] / wsum[e] <= -114.1)
check("largest_events_in_california", big_in_ca == 5, "top-5 event centroids inside California")

passed = sum(ok for _, ok in results)
print(f"\n{passed}/{len(results)} checks passed")
sys.exit(0 if passed == len(results) else 1)
