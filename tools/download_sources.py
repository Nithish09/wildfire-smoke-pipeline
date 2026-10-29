"""
Download the raw source files for the study window onto your own computer, one file per source per day.
Then upload the `raw/` folder to the Databricks volume /Volumes/workspace/wildfire/raw (Catalog → volume → Upload).

Why outside Databricks? Free Edition limits outbound internet to trusted domains, so API calls from a notebook may be blocked.

Usage:
    pip install requests
    export FIRMS_MAP_KEY=...        # https://firms.modaps.eosdis.nasa.gov/api/map_key/
    export AQS_EMAIL=you@uni.edu    # sign up: https://aqs.epa.gov/data/api/signup?email=you@uni.edu
    export AQS_KEY=...
    export NOAA_TOKEN=...           # https://www.ncdc.noaa.gov/cdo-web/token
    python tools/download_sources.py --start 2020-08-15 --end 2020-09-30 --out raw
    python tools/download_sources.py --start 2020-08-15 --end 2020-08-21 --out raw   # a quick first week

Re-running skips files that already exist (use --force to refetch, e.g. to test late revisions).
"""
import argparse, csv, datetime as dt, io, json, os, sys, time, zipfile
import requests

CA_BBOX = "-124.5,32.5,-114.1,42.0"
FIRMS_SOURCE = "VIIRS_SNPP_SP"          # standard (science-quality) product for historical dates; NRT only covers recent months
NOAA_ELEMENTS = ["TMAX", "TMIN", "PRCP", "AWND", "WDF2", "WSF2"]
GAZETTEER = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_counties_national.zip"
EPA_BULK = "https://aqs.epa.gov/aqsweb/airdata/daily_88101_2020.zip"


def days(start, end):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def get(url, params=None, headers=None, tries=5):
    for i in range(tries):
        r = requests.get(url, params=params, headers=headers, timeout=120)
        if r.status_code == 200:
            return r
        if 400 <= r.status_code < 500 and r.status_code != 429:
            # a request error (bad key, bad email, bad parameter): retrying will not help, so show the reason and stop
            reason = r.text
            try:
                j = r.json()
                reason = j.get("Header", [{}])[0].get("error", j) if "Header" in j else j
            except ValueError:
                pass
            safe_url = r.url.split("?")[0]
            sys.exit(f"\nHTTP {r.status_code} from {safe_url}\nReason: {reason}\n"
                     "Check the key/email environment variables for this source, then run the script again "
                     "(files already downloaded are skipped).")
        wait = 2 ** i * 3
        print(f"  HTTP {r.status_code} – retry in {wait}s: {r.text[:120]}")
        time.sleep(wait)
    r.raise_for_status()


def firms(day, out, force):
    path = os.path.join(out, "firms", f"firms_{day}.csv")
    if os.path.exists(path) and not force:
        return
    key = os.environ["FIRMS_MAP_KEY"]
    r = get(f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{key}/{FIRMS_SOURCE}/{CA_BBOX}/1/{day}")
    open(path, "w", encoding="utf-8").write(r.text)
    print(f"firms {day}: {max(r.text.count(chr(10)) - 1, 0)} rows")


def epa(day, out, force):
    path = os.path.join(out, "epa", f"epa_{day}.json")
    if os.path.exists(path) and not force:
        return
    ymd = day.strftime("%Y%m%d")
    r = get("https://aqs.epa.gov/data/api/dailyData/byState", params={
        "email": os.environ["AQS_EMAIL"], "key": os.environ["AQS_KEY"],
        "param": "88101", "bdate": ymd, "edate": ymd, "state": "06"})
    open(path, "w", encoding="utf-8").write(r.text)
    print(f"epa   {day}: {len(r.json().get('Data', []))} rows")
    time.sleep(5)                      # AQS asks callers to pause between requests


def noaa_pages(endpoint, params):
    """params: list of (name, value) pairs, so datatypeid can repeat."""
    token = os.environ["NOAA_TOKEN"]
    results, offset = [], 1
    while True:
        r = get(f"https://www.ncei.noaa.gov/cdo-web/api/v2/{endpoint}",
                params=list(params) + [("limit", 1000), ("offset", offset)], headers={"token": token}).json()
        page = r.get("results", [])
        results += page
        total = r.get("metadata", {}).get("resultset", {}).get("count", 0)
        offset += 1000
        time.sleep(0.25)               # stay under 5 requests/second
        if not page or offset > total:
            return results


def noaa(day, out, force):
    path = os.path.join(out, "noaa", f"noaa_{day}.json")
    if os.path.exists(path) and not force:
        return
    params = [("datasetid", "GHCND"), ("locationid", "FIPS:06"), ("startdate", str(day)), ("enddate", str(day)),
              ("units", "metric")] + [("datatypeid", e) for e in NOAA_ELEMENTS]
    res = noaa_pages("data", params)
    json.dump({"results": res}, open(path, "w"))
    print(f"noaa  {day}: {len(res)} rows")


def reference(start, end, out, force):
    ref = os.path.join(out, "reference")
    p = os.path.join(ref, "noaa_stations.json")
    if force or not os.path.exists(p):
        st = noaa_pages("stations", [("datasetid", "GHCND"), ("locationid", "FIPS:06"),
                                     ("startdate", str(start)), ("enddate", str(end))])
        json.dump({"results": st}, open(p, "w"))
        print(f"stations: {len(st)}")
    p = os.path.join(ref, "ca_counties.csv")
    if force or not os.path.exists(p):
        z = zipfile.ZipFile(io.BytesIO(get(GAZETTEER).content))
        txt = z.read(z.namelist()[0]).decode("latin-1")
        rows = list(csv.reader(io.StringIO(txt), delimiter="\t"))
        header = [h.strip() for h in rows[0]]
        ca = [[c.strip() for c in r] for r in rows[1:] if r and r[0] == "CA"]
        with open(p, "w", newline="") as f:
            w = csv.writer(f); w.writerow(header); w.writerows(ca)
        print(f"counties: {len(ca)} (expect 58)")


def epa_bulk(out, force):
    p = os.path.join(out, "epa_bulk", "daily_88101_2020.csv")
    if os.path.exists(p) and not force:
        return
    z = zipfile.ZipFile(io.BytesIO(get(EPA_BULK).content))
    open(p, "wb").write(z.read(z.namelist()[0]))
    print(f"epa bulk csv: {os.path.getsize(p)/1e6:.0f} MB")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2020-08-15"); ap.add_argument("--end", default="2020-09-30")
    ap.add_argument("--out", default="raw"); ap.add_argument("--force", action="store_true")
    ap.add_argument("--skip-bulk", action="store_true", help="skip the ~70 MB EPA bulk CSV (only needed for the benchmark)")
    a = ap.parse_args()
    start, end = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
    for sub in ["firms", "epa", "noaa", "reference", "epa_bulk"]:
        os.makedirs(os.path.join(a.out, sub), exist_ok=True)
    missing = [k for k in ["FIRMS_MAP_KEY", "AQS_EMAIL", "AQS_KEY", "NOAA_TOKEN"] if not os.environ.get(k)]
    if missing:
        sys.exit(f"Set these environment variables first: {', '.join(missing)}")
    reference(start, end, a.out, a.force)
    for d in days(start, end):
        firms(d, a.out, a.force); epa(d, a.out, a.force); noaa(d, a.out, a.force)
    # FIRMS dates are UTC. 00:00-07:00 UTC on the day after END is still the evening of END in California,
    # so fetch one extra UTC day; Silver assigns every detection to its local date.
    firms(end + dt.timedelta(days=1), a.out, a.force)
    if not a.skip_bulk:
        epa_bulk(a.out, a.force)
    print("Done. Upload the folder contents to /Volumes/workspace/wildfire/raw/")
