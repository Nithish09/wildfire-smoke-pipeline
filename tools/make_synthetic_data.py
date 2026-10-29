"""
Generate a SMALL SYNTHETIC test dataset in exactly the same file formats as the real sources.
!!! These numbers are made up. Use them only to test the code. Never report them as results. !!!

It deliberately includes the defects the pipeline must handle, so the tests can check each rule:
  FIRMS: exact duplicate rows, a point outside California, a missing coordinate, a negative FRP, an acq_time with dropped zeros
  EPA:   the same measurement repeated per pollutant standard, an Included/Excluded event pair, a negative PM2.5,
         a day with low completeness, a wrong parameter code
  NOAA:  a value that failed NOAA QC (qflag), an impossible TMAX, an unknown station, an element we do not use

Usage:  python tools/make_synthetic_data.py --out data/synthetic_raw --days 14
"""
import argparse, csv, datetime as dt, json, math, os, random

# 8 counties, internal points approximated to ~0.1 degree (synthetic reference file, not the Census gazetteer)
COUNTIES = [
    ("06037", "Los Angeles County", 34.32, -118.22, 4058), ("06075", "San Francisco County", 37.75, -122.44, 47),
    ("06067", "Sacramento County", 38.45, -121.34, 965), ("06019", "Fresno County", 36.76, -119.65, 5958),
    ("06089", "Shasta County", 40.76, -122.04, 3775), ("06045", "Mendocino County", 39.44, -123.39, 3506),
    ("06073", "San Diego County", 33.03, -116.74, 4207), ("06087", "Santa Cruz County", 37.06, -122.00, 445),
]
FIRE_CENTRE = (40.10, -122.90)       # a made-up fire between Shasta and Mendocino


def write_counties(ref):
    with open(os.path.join(ref, "ca_counties.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["USPS", "GEOID", "ANSICODE", "NAME", "ALAND", "AWATER", "ALAND_SQMI", "AWATER_SQMI", "INTPTLAT", "INTPTLONG"])
        for fips, name, lat, lon, sq in COUNTIES:
            w.writerow(["CA", fips, "0", name, sq * 2589988, 0, sq, 0, lat, lon])


def stations():
    st = []
    for i, (fips, name, lat, lon, _) in enumerate(COUNTIES):
        for k in range(2):
            st.append({"id": f"GHCND:USW000{i}{k}999", "name": f"SYNTHETIC {name.split()[0].upper()} {k}",
                       "latitude": round(lat + 0.05 * k, 3), "longitude": round(lon - 0.05 * k, 3), "elevation": 50})
    return st


def fire_size(day_idx, n_days):
    # grows, peaks in the middle, then dies down
    return max(0, int(60 * math.sin(math.pi * (day_idx + 1) / (n_days + 1)) ** 2))


def firms_file(path, day, day_idx, n_days, rnd):
    cols = ["latitude", "longitude", "bright_ti4", "scan", "track", "acq_date", "acq_time", "satellite",
            "instrument", "confidence", "version", "bright_ti5", "frp", "daynight"]
    rows = []
    for i in range(fire_size(day_idx, n_days)):
        lat = FIRE_CENTRE[0] + rnd.gauss(0, 0.15); lon = FIRE_CENTRE[1] + rnd.gauss(0, 0.15)
        hhmm = rnd.choice([952, 1018, 2054, 2112])        # UTC; written without leading zeros like FIRMS
        rows.append([f"{lat:.5f}", f"{lon:.5f}", f"{rnd.uniform(300, 367):.2f}", "0.39", "0.36", str(day), str(hhmm),
                     "N", "VIIRS", rnd.choice(["n", "n", "h", "l"]), "1", f"{rnd.uniform(280, 300):.2f}",
                     f"{rnd.uniform(1, 80):.2f}", "D" if hhmm > 1500 else "N"])
    # a small separate fire near San Diego on a few days
    if day_idx % 3 == 0:
        rows.append(["32.95000", "-116.70000", "330.1", "0.4", "0.4", str(day), "52", "N", "VIIRS", "n", "1", "290", "12.5", "N"])
    if rows:
        rows.append(list(rows[0]))                                           # exact duplicate
    rows.append(["45.50000", "-122.60000", "320", "0.4", "0.4", str(day), "1000", "N", "VIIRS", "n", "1", "290", "5", "N"])  # Oregon
    if day_idx == 2:
        rows.append(["", "-121.0", "320", "0.4", "0.4", str(day), "1000", "N", "VIIRS", "n", "1", "290", "5", "N"])     # no lat
        rows.append(["38.0", "-121.0", "320", "0.4", "0.4", str(day), "1000", "N", "VIIRS", "n", "1", "290", "-4", "N"])  # FRP < 0
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(cols); w.writerows(rows)


def dist_km(a, b):
    return math.hypot((a[0] - b[0]) * 111, (a[1] - b[1]) * 88)


def epa_file(path, day, day_idx, n_days, rnd):
    data = []
    smoke = fire_size(day_idx, n_days) / 60.0
    for ci, (fips, name, lat, lon, _) in enumerate(COUNTIES):
        if fips == "06087":
            continue                                       # Santa Cruz: no monitor -> Gold must show NULL PM2.5
        d = dist_km((lat, lon), FIRE_CENTRE)
        base = 8 + rnd.uniform(-2, 3)
        pm = round(base + smoke * 140 * math.exp(-d / 180), 1)
        rec = {"state_code": "06", "county_code": fips[2:], "site_number": f"{ci + 1:04d}", "parameter_code": "88101",
               "poc": 1, "latitude": lat, "longitude": lon, "datum": "WGS84", "parameter": "PM2.5 - Local Conditions",
               "sample_duration": "24 HOUR", "pollutant_standard": "PM25 24-hour 2012", "date_local": str(day),
               "units_of_measure": "Micrograms/cubic meter (LC)", "event_type": "None", "observation_count": 1,
               "observation_percent": 100.0, "validity_indicator": "Y", "arithmetic_mean": pm, "first_max_value": pm,
               "first_max_hour": 0, "aqi": int(min(500, pm * 4.2)), "method_code": "145", "method": "SYNTHETIC",
               "local_site_name": f"SYNTHETIC {name}", "site_address": "n/a", "state": "California",
               "county": name.replace(" County", ""), "city": "n/a", "cbsa_code": "", "cbsa": "",
               "date_of_last_change": "2021-03-01"}
        data.append(rec)
        data.append(dict(rec, pollutant_standard="PM25 Annual 2012"))           # same measurement, other standard
        if pm > 35:                                                               # exceptional-event pair
            data.append(dict(rec, event_type="Excluded", arithmetic_mean=round(base, 1), aqi=int(base * 4.2)))
            for r in data[-3:-1]:
                r["event_type"] = "Included"
    data.append(dict(data[0], site_number="0099", arithmetic_mean=-3.0))                # impossible value
    if day_idx == 4:
        data.append(dict(data[0], site_number="0098", observation_percent=40.0))        # low completeness
    data.append(dict(data[0], parameter_code="81102"))                                  # wrong parameter (PM10)
    json.dump({"Header": [{"status": "Success", "rows": len(data)}], "Data": data}, open(path, "w"))


def noaa_file(path, day, day_idx, n_days, st, rnd):
    res = []
    for s in st:
        hot = 30 + rnd.uniform(-4, 6) - (s["latitude"] - 34) * 1.5
        vals = {"TMAX": round(hot, 1), "TMIN": round(hot - 14, 1), "PRCP": 0.0,
                "AWND": round(rnd.uniform(1, 6), 1), "WDF2": rnd.choice([270, 290, 310, 330]), "WSF2": round(rnd.uniform(5, 12), 1)}
        for e, v in vals.items():
            res.append({"date": f"{day}T00:00:00", "datatype": e, "station": s["id"], "attributes": ",,W,2400", "value": v})
    def find(station, element):
        return next(r for r in res if r["station"] == station and r["datatype"] == element)
    find(st[3]["id"], "TMIN")["attributes"] = ",I,W,2400"               # failed NOAA's own QC (qflag = I)
    find(st[6]["id"], "TMAX")["value"] = 99.0                            # impossible temperature
    res.append({"date": f"{day}T00:00:00", "datatype": "SNOW", "station": st[0]["id"], "attributes": ",,W,", "value": 0})  # unused
    res.append({"date": f"{day}T00:00:00", "datatype": "TMAX", "station": "GHCND:USW00000BAD", "attributes": ",,W,2400", "value": 25.0})  # unknown station
    res.append(dict(res[0]))                                             # exact duplicate
    json.dump({"results": res}, open(path, "w"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/synthetic_raw"); ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--start", default="2020-08-15"); ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    for sub in ["firms", "epa", "noaa", "reference"]:
        os.makedirs(os.path.join(a.out, sub), exist_ok=True)
    write_counties(os.path.join(a.out, "reference"))
    st = stations()
    json.dump({"results": st}, open(os.path.join(a.out, "reference", "noaa_stations.json"), "w"))
    start = dt.date.fromisoformat(a.start)
    for i in range(a.days):
        d = start + dt.timedelta(days=i)
        firms_file(os.path.join(a.out, "firms", f"firms_{d}.csv"), d, i, a.days, rnd)
        epa_file(os.path.join(a.out, "epa", f"epa_{d}.json"), d, i, a.days, rnd)
        noaa_file(os.path.join(a.out, "noaa", f"noaa_{d}.json"), d, i, a.days, st, rnd)
    with open(os.path.join(a.out, "SYNTHETIC_DATA_README.txt"), "w") as f:
        f.write("SYNTHETIC TEST DATA - generated by tools/make_synthetic_data.py. Values are invented. "
                "Use only to test the pipeline code; never report these numbers as findings.\n")
    print(f"Synthetic data written to {a.out} ({a.days} days)")
