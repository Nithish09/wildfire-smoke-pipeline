"""
End-to-end logic test on SYNTHETIC data (no Spark needed):
  1. first load: Bronze -> Silver -> Gold, check every planted defect is quarantined or deduplicated
  2. rerun with no new files: nothing may change (RQ3, part 1)
  3. late revision: EPA corrects one value months later -> only that county-day changes in Gold (RQ3, part 2)
  4. new day arrives: only the new date is built (incremental velocity)
Writes tests/local_test_report.md.

Usage:  python tests/run_local_test.py
"""
import datetime as dt, json, os, shutil, subprocess, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from reference_pipeline import Pipeline  # noqa: E402

RAW = os.path.join(ROOT, "data", "synthetic_raw")
STATE = os.path.join(HERE, "_state")
checks, lines = [], []


def check(name, ok, detail=""):
    checks.append((name, bool(ok), detail))
    print(("PASS " if ok else "FAIL ") + name + (f" – {detail}" if detail else ""))


def fingerprint(p, table):
    df = p.load(table).drop(columns=[c for c in ["_ingested_at"] if c in p.load(table).columns])
    df = df.sort_values(list(df.columns[:3])).reset_index(drop=True)
    return len(df), int(pd.util.hash_pandas_object(df.astype(str), index=False).sum())


shutil.rmtree(STATE, ignore_errors=True)
shutil.rmtree(RAW, ignore_errors=True)
subprocess.run([sys.executable, os.path.join(ROOT, "tools", "make_synthetic_data.py"), "--out", RAW, "--days", "14"], check=True)
# hold back the last day to test incremental arrival later
held = {s: os.path.join(RAW, s, f"{s}_2020-08-28.{'csv' if s == 'firms' else 'json'}") for s in ["firms", "epa", "noaa"]}
for s, f in held.items():
    shutil.move(f, f + ".held")

p = Pipeline(RAW, STATE, dt.date(2020, 8, 15), dt.date(2020, 8, 28))

# ---- 1. first load ----
b = p.bronze(); s = p.silver(); g = p.gold()
q = p.load("silver_quarantine")
reasons = q.groupby(["source", "reason"]).size().to_dict()
lines.append("## 1. First load (13 synthetic days)\n")
lines.append(f"Bronze rows loaded: {b}\n")
lines.append("| Source | Quarantine reason | Records |\n| --- | --- | --- |")
lines += [f"| {k[0]} | {k[1]} | {v} |" for k, v in sorted(reasons.items())]
for src, reason in [("nasa_firms", "outside_study_area"), ("nasa_firms", "missing_coordinates"), ("nasa_firms", "frp_out_of_range"),
                    ("epa_aqs", "impossible_value"), ("epa_aqs", "low_completeness"), ("epa_aqs", "wrong_parameter"),
                    ("noaa_cdo", "failed_source_qc"), ("noaa_cdo", "impossible_value"), ("noaa_cdo", "unmatched_station"),
                    ("noaa_cdo", "unneeded_element")]:
    check(f"quarantine catches {src}/{reason}", reasons.get((src, reason), 0) > 0)

fires_b = p.load("bronze_firms"); fires_s = p.load("silver_fire_detections")
check("FIRMS exact duplicates removed", len(fires_s) == len(fires_s.drop_duplicates(["latitude", "longitude", "acq_ts_utc", "satellite"])))
pm = p.load("silver_pm25_daily")
check("EPA: one row per site-day after pollutant-standard copies", len(pm) == pm[["site_id", "date_local"]].drop_duplicates().shape[0],
      f"{len(p.load('bronze_epa_pm25'))} bronze rows -> {len(pm)} silver rows")
check("EPA: wildfire 'Excluded' rows lose to 'Included'", (pm.event_type != "Excluded").all())
fx = p.load("gold_fact_county_day")
check("Gold has one row per county per day", len(fx) == 8 * 13 and not fx.duplicated(["county_fips", "date_local"]).any(), f"{len(fx)} rows")
check("County without monitor has NULL PM2.5 (not 0)", fx[fx.county_fips == "06087"].pm25_mean_ugm3.isna().all())
check("acq_time '52' read as 00:52 UTC", (fires_s.acq_ts_utc.dt.strftime("%H%M") == "0052").any())
early = fires_s[fires_s.acq_ts_utc.dt.date.astype(str) == "2020-08-15"]
shifted = int((early.date_local.astype(str) == "2020-08-14").sum())
check("UTC detections before 07:00 fall on the previous local date", shifted > 0,
      f"{shifted} detections with UTC date 2020-08-15 belong to local 2020-08-14 and are outside the window")
lines.append("\nSmoke indicator mix (county-days): " + json.dumps(fx.smoke_indicator.value_counts().to_dict()))

# ---- 2. rerun: no change ----
before = {t: fingerprint(p, t) for t in ["silver_fire_detections", "silver_pm25_daily", "silver_weather_obs", "gold_fact_county_day"]}
b2 = p.bronze(); s2 = p.silver(); g2 = p.gold(rebuild_all=True)
after = {t: fingerprint(p, t) for t in before}
check("Rerun: Bronze skips every file", sum(b2.values()) == 0, str(b2))
check("Rerun with full Gold rebuild: 0 inserted, 0 updated", g2 == {"inserted": 0, "updated": 0}, str(g2))
check("Rerun: Silver and Gold fingerprints identical", before == after)
lines.append(f"\n## 2. Rerun on the same files\nBronze: {b2}; Gold MERGE: {g2}; fingerprints unchanged: {before == after}\n")

# ---- 3. late revision ----
f = os.path.join(RAW, "epa", "epa_2020-08-20.json")
doc = json.load(open(f))
rec = next(r for r in doc["Data"] if r["county_code"] == "037" and r["pollutant_standard"] == "PM25 24-hour 2012")
old = rec["arithmetic_mean"]
for r in doc["Data"]:
    if r["county_code"] == "037" and r["site_number"] == rec["site_number"] and r["event_type"] != "Excluded":
        r["arithmetic_mean"] = round(old + 10, 1); r["date_of_last_change"] = "2021-06-01"
json.dump(doc, open(f, "w"))
g_before = p.load("gold_fact_county_day")[["county_fips", "date_local", "pm25_mean_ugm3"]]
b3 = p.bronze(); p.silver(); g3 = p.gold()
g_after = p.load("gold_fact_county_day")[["county_fips", "date_local", "pm25_mean_ugm3"]]
diff = g_before.merge(g_after, on=["county_fips", "date_local"], suffixes=("_before", "_after"))
both_null = diff.pm25_mean_ugm3_before.isna() & diff.pm25_mean_ugm3_after.isna()
diff = diff[~both_null & (diff.pm25_mean_ugm3_before.round(2) != diff.pm25_mean_ugm3_after.round(2))]
check("Late revision: revised file re-ingested", b3["epa_aqs"] > 0, str(b3))
check("Late revision: exactly one Gold county-day changed", len(diff) == 1 and diff.iloc[0].county_fips == "06037",
      diff.to_dict("records").__str__())
lines.append(f"## 3. Late revision (Los Angeles, 2020-08-20: {old} -> {round(old + 10, 1)})\nGold MERGE: {g3}\n")
lines.append(diff.to_markdown(index=False) if hasattr(diff, "to_markdown") else str(diff))

# ---- 4. new day ----
for s_, f_ in held.items():
    shutil.move(f_ + ".held", f_)
b4 = p.bronze(); p.silver(); g4 = p.gold()
check("New day: only the new date is built (8 inserts, 0 updates)", g4 == {"inserted": 8, "updated": 0}, str(g4))
lines.append(f"\n## 4. New day arrives (2020-08-28)\nBronze: {b4}; Gold MERGE: {g4}\n")

passed = sum(ok for _, ok, _ in checks)
report = ["# Local logic test report (SYNTHETIC data)\n",
          "Generated by `tests/run_local_test.py` with the pandas reference implementation. "
          "These numbers come from invented test data and show only that the rules behave as designed.\n",
          f"**{passed}/{len(checks)} checks passed.**\n", "| Check | Result | Detail |", "| --- | --- | --- |"]
report += [f"| {n} | {'PASS' if ok else 'FAIL'} | {d.replace('|', '/')[:120]} |" for n, ok, d in checks]
open(os.path.join(HERE, "local_test_report.md"), "w").write("\n".join(report + [""] + lines) + "\n")
print(f"\n{passed}/{len(checks)} checks passed. Report: tests/local_test_report.md")
sys.exit(0 if passed == len(checks) else 1)
