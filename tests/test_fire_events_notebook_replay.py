"""
Replay of notebooks/09_fire_events.py in pandas on the REAL first-week data (data/raw), step by step,
with the same formulas, joins and filters as the Spark code. Use it to check the notebook's LOGIC and outputs
where Spark is not available. (It does not test Spark syntax; run the notebook in Databricks for that.)

Inputs are rebuilt the way the group's pipeline builds them:
  silver_fire_detections  <- raw FIRMS + Silver rules (CA box, confidence, FRP range, dedup, UTC->local date, nearest county)
  gold_dim_county         <- Census gazetteer (county_fips, county_name, c_lat, c_lon)
  gold_fact_county_day    <- EPA PM2.5 (site mean -> county mean) + NOAA WDF2 wind direction (vector mean per county)
Run: python3 tests/test_fire_events_notebook_replay.py
"""
import csv, glob, importlib.util, json, math, os, sys
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "data", "raw")
spec = importlib.util.spec_from_file_location("lib", os.path.join(ROOT, "notebooks", "lib_fire_events.py"))
L = importlib.util.module_from_spec(spec); spec.loader.exec_module(L)
LINK_KM, LINK_HOURS, IMPACT_KM = 2.0, 24.0, 150.0                     # the notebook's default widget values
results = []


def check(name, ok, detail=""):
    results.append((name, bool(ok))); print(("PASS " if ok else "FAIL ") + name + (f": {detail}" if detail else ""))


def hav(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(a))


def bearing(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    y = np.sin(lon2 - lon1) * np.cos(lat2)
    x = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(lon2 - lon1)
    return (np.degrees(np.arctan2(y, x)) + 360) % 360


# ------------------------------------------------------------------ inputs (as the group's pipeline builds them)
g = pd.read_csv(os.path.join(RAW, "reference", "ca_counties.csv"), dtype=str)
dim = pd.DataFrame({"county_fips": g.GEOID, "county_name": g.NAME.str.replace(" County$", "", regex=True),
                    "c_lat": g.INTPTLAT.astype(float), "c_lon": g.INTPTLONG.astype(float)})

f = pd.concat([pd.read_csv(p, dtype=str) for p in sorted(glob.glob(os.path.join(RAW, "firms", "*.csv")))], ignore_index=True)
f["latitude"] = pd.to_numeric(f.latitude, errors="coerce"); f["longitude"] = pd.to_numeric(f.longitude, errors="coerce")
f["acq_ts_utc"] = pd.to_datetime(f.acq_date + " " + f.acq_time.str.strip().str.zfill(4), format="%Y-%m-%d %H%M", errors="coerce")
f["frp_mw"] = pd.to_numeric(f.frp, errors="coerce")
f["confidence_class"] = f.confidence.str.strip().str.lower().map({"l": "low", "n": "nominal", "h": "high"})
f = f[f.latitude.between(32.5, 42.0) & f.longitude.between(-124.5, -114.1) & f.acq_ts_utc.notna()
      & (f.frp_mw.isna() | f.frp_mw.between(0, 20000)) & f.confidence_class.notna()]
f = f.drop_duplicates(["latitude", "longitude", "acq_ts_utc", "satellite"])
f["date_local"] = f.acq_ts_utc.dt.tz_localize("UTC").dt.tz_convert("America/Los_Angeles").dt.date
d = hav(f.latitude.values[:, None], f.longitude.values[:, None], dim.c_lat.values[None, :], dim.c_lon.values[None, :])
f["county_fips"] = dim.county_fips.values[d.argmin(1)]
silver = f[["latitude", "longitude", "acq_ts_utc", "satellite", "date_local", "frp_mw", "county_fips", "confidence_class"]].reset_index(drop=True)

pm = []
for p in sorted(glob.glob(os.path.join(RAW, "epa", "*.json"))):
    for r in json.load(open(p)).get("Data", []):
        if r["parameter_code"] == "88101" and float(r.get("observation_percent") or 100) >= 75 and r.get("event_type") != "Excluded":
            pm.append(("06" + str(r["county_code"]).zfill(3), str(r["site_number"]), pd.to_datetime(r["date_local"]).date(), float(r["arithmetic_mean"])))
pm = pd.DataFrame(pm, columns=["county_fips", "site", "date_local", "pm"]).query("0 <= pm <= 1000")
pm = pm.groupby(["county_fips", "site", "date_local"]).pm.mean().reset_index().groupby(["county_fips", "date_local"]).pm.mean().round(2)
st = pd.DataFrame(json.load(open(os.path.join(RAW, "reference", "noaa_stations.json")))["results"])[["id", "latitude", "longitude"]]
wx = []
for p in sorted(glob.glob(os.path.join(RAW, "noaa", "*.json"))):
    for r in json.load(open(p)).get("results", []):
        if r["datatype"] == "WDF2":
            wx.append((r["station"], pd.to_datetime(r["date"][:10]).date(), float(r["value"])))
wx = pd.DataFrame(wx, columns=["id", "date_local", "wdf2"]).merge(st, on="id")
dd = hav(wx.latitude.values[:, None], wx.longitude.values[:, None], dim.c_lat.values[None, :], dim.c_lon.values[None, :])
wx["county_fips"] = dim.county_fips.values[dd.argmin(1)]
wx["_s"], wx["_c"] = np.sin(np.radians(wx.wdf2)), np.cos(np.radians(wx.wdf2))
wind = wx.groupby(["county_fips", "date_local"])[["_s", "_c"]].mean()
wind = ((np.degrees(np.arctan2(wind._s, wind._c)) + 360) % 360).round(0).rename("wind_dir_deg")
days = sorted(silver.date_local.unique())
fact = pd.MultiIndex.from_product([dim.county_fips, days], names=["county_fips", "date_local"]).to_frame(index=False)
fact = fact.merge(wind.reset_index(), how="left").merge(pm.rename("pm25_mean_ugm3").reset_index(), how="left")
print(f"inputs: {len(silver):,} Silver detections, {len(dim)} counties, {len(fact):,} county-days "
      f"({fact.pm25_mean_ugm3.notna().mean():.0%} with PM2.5, {fact.wind_dir_deg.notna().mean():.0%} with wind)")

# ------------------------------------------------------------------ notebook section 1: stream + members
det = silver.assign(satellite=silver.satellite.fillna("")).dropna(subset=["latitude", "longitude", "acq_ts_utc"])
det = det.sort_values(["acq_ts_utc", "latitude", "longitude", "satellite"]).reset_index(drop=True)
epoch_s = (det.acq_ts_utc - pd.Timestamp("1970-01-01")).dt.total_seconds()
detections = [(la, lo, e / 3600.0, f"{la}|{lo}|{ts}|{s}") for la, lo, e, ts, s in
              zip(det.latitude, det.longitude, epoch_s, det.acq_ts_utc, det.satellite)]
labels, stats = L.cluster_detections(detections, LINK_KM, LINK_HOURS)
first_index = {}
for k, e in enumerate(labels):
    first_index.setdefault(e, k)
ids = {e: L.event_id(detections[k][3], det.acq_ts_utc[k].date()) for e, k in first_index.items()}
members = det[["latitude", "longitude", "acq_ts_utc", "satellite"]].assign(event_id=[ids[x] for x in labels])
print("stream stats:", stats)

# ------------------------------------------------------------------ section 2: events + daily
KEY = ["latitude", "longitude", "acq_ts_utc", "satellite"]
dj = det.merge(members, on=KEY, how="inner")
dj["w"] = dj.frp_mw.fillna(0).clip(lower=0.1)
daily = dj.groupby(["event_id", "date_local"]).apply(lambda x: pd.Series({
    "detections": len(x), "frp_total_mw": round(x.frp_mw.sum(), 1), "frp_max_mw": round(x.frp_mw.max(), 1),
    "centroid_lat": (x.latitude * x.w).sum() / x.w.sum(), "centroid_lon": (x.longitude * x.w).sum() / x.w.sum()}),
    include_groups=False).reset_index()
peak = daily.sort_values(["event_id", "frp_total_mw"], ascending=[True, False]).drop_duplicates("event_id")[["event_id", "date_local", "frp_total_mw"]] \
    .rename(columns={"date_local": "peak_day", "frp_total_mw": "peak_day_frp_mw"})
mc = dj.groupby(["event_id", "county_fips"]).size().rename("n").reset_index().sort_values(["event_id", "n", "county_fips"], ascending=[True, False, True]) \
    .drop_duplicates("event_id").rename(columns={"county_fips": "primary_county_fips"})[["event_id", "primary_county_fips"]]
ev = dj.groupby("event_id").apply(lambda x: pd.Series({
    "first_seen_utc": x.acq_ts_utc.min(), "last_seen_utc": x.acq_ts_utc.max(), "first_day": x.date_local.min(),
    "last_day": x.date_local.max(), "active_days": x.date_local.nunique(), "detections": len(x),
    "high_conf_detections": int((x.confidence_class == "high").sum()), "frp_total_mw": round(x.frp_mw.sum(), 1),
    "frp_max_mw": round(x.frp_mw.max(), 1), "centroid_lat": round((x.latitude * x.w).sum() / x.w.sum(), 4),
    "centroid_lon": round((x.longitude * x.w).sum() / x.w.sum(), 4), "counties_burned": sorted(set(x.county_fips))}),
    include_groups=False).reset_index()
ev["duration_hours"] = ((ev.last_seen_utc - ev.first_seen_utc).dt.total_seconds() / 3600).round(1)
ev["size_class"] = np.select([ev.detections >= 500, ev.detections >= 10, ev.detections >= 2], ["large", "medium", "small"], "single")
ev = ev.merge(peak, on="event_id").merge(mc, on="event_id").merge(
    dim[["county_fips", "county_name"]].rename(columns={"county_fips": "primary_county_fips", "county_name": "primary_county"}),
    on="primary_county_fips", how="left")
ev["event_label"] = ev.primary_county.fillna("Unknown") + " fire, from " + pd.to_datetime(ev.first_day).dt.strftime("%-d %b %Y")

# ------------------------------------------------------------------ section 3: county impact
imp = daily.merge(dim, how="cross")
imp["distance_km"] = hav(imp.c_lat, imp.c_lon, imp.centroid_lat, imp.centroid_lon).round(1)
imp = imp[imp.distance_km <= IMPACT_KM].copy()
imp["bearing_county_to_event"] = bearing(imp.c_lat, imp.c_lon, imp.centroid_lat, imp.centroid_lon).round(0)
imp = imp[["event_id", "date_local", "county_fips", "county_name", "distance_km", "bearing_county_to_event", "frp_total_mw", "detections"]] \
    .rename(columns={"frp_total_mw": "event_day_frp_mw", "detections": "event_day_detections"})
nxt = fact[["county_fips", "date_local", "pm25_mean_ugm3"]].copy()
nxt["date_local"] = [x - pd.Timedelta(days=1) for x in pd.to_datetime(nxt.date_local)]
nxt["date_local"] = pd.to_datetime(nxt.date_local).dt.date
imp = imp.merge(fact, on=["county_fips", "date_local"], how="left").merge(
    nxt.rename(columns={"pm25_mean_ugm3": "pm25_next_day_ugm3"}), on=["county_fips", "date_local"], how="left")
imp["upwind_alignment"] = np.cos(np.radians(imp.bearing_county_to_event - imp.wind_dir_deg)).round(2)
imp["event_upwind"] = np.where(imp.upwind_alignment.isna(), None, imp.upwind_alignment >= 0.5)

# ------------------------------------------------------------------ section 4 checks + extra sanity checks
check("every_detection_in_one_event", len(members) == len(det) and not members.duplicated(KEY).any(),
      f"detections {len(det):,} | assigned {len(members):,} | assigned twice {int(members.duplicated(KEY).sum())} | events {ev.event_id.nunique():,}")
check("events_match_algorithm", ev.event_id.nunique() == stats["events"] and ev.detections.sum() == len(det))
check("event_ids_unique", ev.event_id.is_unique)
check("daily_adds_up", daily.detections.sum() == len(det) and set(daily.event_id) == set(ev.event_id))
check("peak_day_within_event", ((ev.peak_day >= ev.first_day) & (ev.peak_day <= ev.last_day)).all())
check("impact_within_radius", imp.distance_km.le(IMPACT_KM).all() and imp.bearing_county_to_event.between(0, 360).all())
check("upwind_alignment_valid", imp.upwind_alignment.dropna().between(-1, 1).all(),
      f"{imp.upwind_alignment.notna().mean():.0%} of impact rows have wind")
s = imp.dropna(subset=["pm25_next_day_ugm3"]).iloc[0]
manual = fact[(fact.county_fips == s.county_fips) & (fact.date_local == s.date_local + pd.Timedelta(days=1))].pm25_mean_ugm3.iloc[0]
check("next_day_pm25_join", manual == s.pm25_next_day_ugm3, f"{s.county_name} {s.date_local}: next-day {manual}")
labels2, _ = L.cluster_detections(detections, LINK_KM, LINK_HOURS)
check("rerun_identical", labels2 == labels)

print("\nLargest events (what the notebook's first results table should look like):")
cols = ["event_id", "event_label", "first_day", "last_day", "active_days", "detections", "frp_total_mw", "centroid_lat", "centroid_lon", "peak_day"]
print(ev.sort_values("detections", ascending=False)[cols].head(6).to_string(index=False))
top = ev.sort_values("detections", ascending=False).event_id.iloc[0]
t = imp[imp.event_id == top].groupby("county_name").agg(days_within_radius=("date_local", "size"), closest_km=("distance_km", "min"),
                                                       avg_pm25_same_day=("pm25_mean_ugm3", "mean"), avg_pm25_next_day=("pm25_next_day_ugm3", "mean"))
print(f"\nCounties around the largest event ({top}):")
print(t.sort_values(["days_within_radius", "closest_km"], ascending=[False, True]).round(1).head(10).to_string())
print(f"\noutput table sizes: members {len(members):,} | events {len(ev):,} | event-days {len(daily):,} | impact rows {len(imp):,}")

passed = sum(ok for _, ok in results)
print(f"\n{passed}/{len(results)} checks passed")
sys.exit(0 if passed == len(results) else 1)
