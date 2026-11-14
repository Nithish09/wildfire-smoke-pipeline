"""Check every algorithm in notebooks/lib_stream_algorithms.py against exact answers on the REAL first-week FIRMS
and EPA files in data/raw (15-21 Aug 2020). Run: python3 tests/test_stream_algorithms.py"""
import csv, datetime as dt, glob, importlib.util, json, math, os, random, sys, time
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("lib", os.path.join(ROOT, "notebooks", "lib_stream_algorithms.py"))
L = importlib.util.module_from_spec(spec); spec.loader.exec_module(L)
results = []

def check(name, ok, detail):
    results.append((name, ok, detail)); print(("PASS " if ok else "FAIL ") + f"{name}: {detail}")

# ---- the stream: real FIRMS detections in arrival order (file by file, then acquisition time)
rows = []
for f in sorted(glob.glob(os.path.join(ROOT, "data", "raw", "firms", "firms_*.csv"))):
    rows += sorted(csv.DictReader(open(f)), key=lambda r: (r["acq_date"], r["acq_time"].zfill(4)))
print(f"stream length: {len(rows):,} detections")
key = lambda r: f'{r["latitude"]}|{r["longitude"]}|{r["acq_date"]}|{r["acq_time"]}|{r["satellite"]}'
cell = lambda r: f'{round(float(r["latitude"]), 1)}|{round(float(r["longitude"]), 1)}'   # ~10 km grid cell

# Reservoir: sample distribution over dates should match the stream's
rs = L.ReservoirSampler(1000, seed=1)
for r in rows: rs.add(r)
pop = Counter(r["acq_date"] for r in rows); smp = Counter(r["acq_date"] for r in rs.sample)
tv = 0.5 * sum(abs(pop[d] / len(rows) - smp[d] / len(rs.sample)) for d in pop)
check("reservoir_uniform", len(rs.sample) == 1000 and tv < 0.06, f"k=1000, total-variation distance vs stream = {tv:.3f}")

# Bloom: dedup the stream; measure false-positive rate on keys never inserted
bf = L.BloomFilter(expected_items=len(rows), fp_rate=0.01)
flagged = sum(bf.check_and_add(key(r)) for r in rows)
exact_dups = len(rows) - len({key(r) for r in rows})
probe = [f"absent-{i}" for i in range(20000)]
fp = sum(p in bf for p in probe) / len(probe)
check("bloom_no_false_negatives", all(key(r) in bf for r in rows[:5000]), "every inserted key is found")
check("bloom_fp_rate", fp < 0.02, f"m={bf.m:,} bits, k={bf.k}, measured FP {fp:.4f} vs theory {bf.theoretical_fp_rate():.4f}; "
      f"flagged {flagged} as seen vs {exact_dups} true duplicates (= {flagged - exact_dups} false positives)")

# Flajolet-Martin: distinct ~10 km cells
fm = L.FlajoletMartin(num_hashes=96, group_size=8)
t0 = time.time()
for r in rows: fm.add(cell(r))
exact = len({cell(r) for r in rows}); est = fm.estimate()
check("flajolet_martin", 0.5 <= est / exact <= 2.0, f"estimate {est:.0f} vs exact {exact} distinct cells (ratio {est/exact:.2f}); "
      f"uncorrected textbook estimate {fm.estimate_raw():.0f} ({time.time()-t0:.1f}s)")

# DGIM: intense detections (FRP > 50 MW) among the last N
N = 5000; dg = L.DGIM(N); bits = [1 if float(r["frp"] or 0) > 50 else 0 for r in rows]
worst = 0; maxb = 0
for i, b in enumerate(bits, 1):
    dg.add(b); maxb = max(maxb, dg.num_buckets())
    if i % 1000 == 0 and i >= N:
        true = sum(bits[i - N:i]); e = dg.estimate()
        if true: worst = max(worst, abs(e - true) / true)
check("dgim_error_bound", worst <= 0.5, f"N={N}: worst relative error {worst:.3f} (bound 0.5), max {maxb} buckets vs {N} bits")

# AMS second moment of the cell stream
ams = L.AMSMoment(num_vars=400, seed=3)
t0 = time.time()
for r in rows: ams.add(cell(r))
f2 = sum(c * c for c in Counter(cell(r) for r in rows).values())
check("ams_second_moment", 0.6 <= ams.estimate() / f2 <= 1.4, f"estimate {ams.estimate():,.0f} vs exact {f2:,} (ratio {ams.estimate()/f2:.2f}, {time.time()-t0:.1f}s)")

# Graph stream: fire events = connected components of detections within 2 km and 24 h
def epoch_h(r):
    t = dt.datetime.strptime(r["acq_date"] + r["acq_time"].zfill(4), "%Y-%m-%d%H%M")
    return t.timestamp() / 3600
dets = [(i, float(r["latitude"]), float(r["longitude"]), epoch_h(r)) for i, r in enumerate(rows)]
sc = L.StreamingComponents(); ne = 0
t0 = time.time()
for a, b in L.fire_event_edges(dets, 2.0, 24.0):
    sc.union(a, b); ne += 1
comps = sc.components(); sizes = sorted((len(v) for v in comps.values()), reverse=True)
check("graph_components", len(comps) < len(rows) and sum(sizes) == len(rows),
      f"{len(rows):,} detections -> {len(comps):,} fire events; largest {sizes[:5]}; {ne:,} edges streamed ({time.time()-t0:.1f}s)")

# MinHash LSH on sets with known Jaccard similarity
rng = random.Random(0); base = list(range(1000)); sets = {}
for j, s in enumerate([0.9, 0.7, 0.5, 0.2]):
    a = set(rng.sample(base, 200)); keep = int(200 * 2 * s / (1 + s))     # |A∩B| giving Jaccard s
    b = set(list(a)[:keep]) | set(rng.sample([x for x in range(1000, 5000)], 200 - keep))
    sets[f"A{j}"], sets[f"B{j}"] = a, b
lsh = L.MinHashLSH(num_perm=120, bands=24)
pairs, sigs = lsh.candidate_pairs(sets)
errs = [abs(lsh.estimated_jaccard(sigs[f"A{j}"], sigs[f"B{j}"]) - L.jaccard(sets[f"A{j}"], sets[f"B{j}"])) for j in range(4)]
found = [(f"A{j}", f"B{j}") in pairs for j in range(4)]
check("minhash_estimates", max(errs) < 0.12, f"max |estimated - true Jaccard| = {max(errs):.3f}")
check("lsh_banding", found[0] and found[1] and not found[3], f"threshold≈{lsh.threshold():.2f}; candidates found for J=0.9,0.7,0.5,0.2: {found}")

# Privacy on the real EPA week: county exceedance site-days, k-anonymity of site locations
sites, county_exceed = {}, Counter()
for f in sorted(glob.glob(os.path.join(ROOT, "data", "raw", "epa", "epa_*.json"))):
    for r in json.load(open(f)).get("Data", []):
        sid = f'{r["county_code"]}-{r["site_number"]}'
        sites[sid] = {"lat": float(r["latitude"]), "lon": float(r["longitude"]), "county": r["county"]}
        if r["pollutant_standard"] == "PM25 24-hour 2012" and float(r["arithmetic_mean"]) > 35:
            county_exceed[r["county"]] += 1
counties = sorted(county_exceed); true = [county_exceed[c] for c in counties]
for eps in [0.1, 1.0, 10.0]:
    noisy = L.laplace_mechanism(true, sensitivity=1, epsilon=eps, seed=4)
    mae = sum(abs(a - b) for a, b in zip(noisy, true)) / len(true)
    check(f"dp_laplace_eps_{eps}", abs(mae - 1 / eps) < 1.5 / eps, f"{len(true)} counties, MAE {mae:.2f} (expected ≈ {1/eps:.2f})")
recs = list(sites.values())
for r in recs:  # sensitive attribute for l-diversity: whether the site had any exceedance this week
    r["exceeded"] = "yes" if county_exceed.get(r["county"], 0) > 0 else "no"
p, cells, sup = L.k_anonymize_locations(recs, k=5)
check("k_anonymity", all(len(v) >= 5 for v in cells.values()),
      f"{len(recs)} sites -> geohash precision {p}, {len(cells)} cells, min cell size {min(len(v) for v in cells.values())}, "
      f"suppressed {sup}, distinct l-diversity {L.l_diversity(cells, 'exceeded')}")

passed = sum(ok for _, ok, _ in results)
print(f"\n{passed}/{len(results)} checks passed")
sys.exit(0 if passed == len(results) else 1)
