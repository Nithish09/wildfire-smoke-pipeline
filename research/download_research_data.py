"""
Download the multi-season research dataset onto your own computer (Databricks Free Edition cannot reach these sites).

Sources (all free; only FIRMS needs a key):
  * EPA AirData pre-generated daily PM2.5 files (daily_88101_<year>.zip), keyless
  * NASA FIRMS VIIRS S-NPP standard product (VIIRS_SNPP_SP), one request per day, needs FIRMS_MAP_KEY
  * NOAA GHCN-Daily per-station CSV files for California first-order (USW) stations, keyless
  * NOAA Hazard Mapping System (HMS) daily smoke polygons, keyless
  * Census 2020 county gazetteer, keyless

Usage (from the project folder; FIRMS_MAP_KEY is read from .env like tools/download_sources.py):
    pip3 install requests
    python3 research/download_research_data.py                  # all seasons 2018-2021
    python3 research/download_research_data.py --seasons 2020   # one season first (recommended)

Re-running skips files that already exist. Expect ~1.5 GB and 1-3 hours for all four seasons (FIRMS is the slow part).
"""
import argparse, csv, datetime as dt, io, os, sys, time, zipfile

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as C  # noqa: E402

GAZ = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_counties_national.zip"
EPA = "https://aqs.epa.gov/aqsweb/airdata/daily_88101_{y}.zip"
FIRMS = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{key}/VIIRS_SNPP_SP/{bbox}/1/{day}"
GHCN_STATIONS = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
GHCN_ACCESS = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/{sid}.csv"
HMS = "https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile/{y}/{m:02d}/hms_smoke{ymd}.zip"


def load_env():
    p = os.path.join(C.ROOT, ".env")
    if os.path.exists(p):
        for line in open(p, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip().removeprefix("export ").strip(), v.strip().strip('"').strip("'")
                if v and not os.environ.get(k):
                    os.environ[k] = v


def get(url, tries=5, ok404=False):
    for i in range(tries):
        try:
            r = requests.get(url, timeout=180)
        except requests.RequestException as e:
            print(f"  network error ({e.__class__.__name__}), retry"); time.sleep(3 * 2 ** i); continue
        if r.status_code == 200:
            return r
        if r.status_code == 404 and ok404:
            return None
        if 400 <= r.status_code < 500 and r.status_code != 429:
            sys.exit(f"HTTP {r.status_code} for {url.split('?')[0]}: {r.text[:300]}")
        time.sleep(3 * 2 ** i)
    sys.exit(f"Giving up on {url.split('?')[0]}")


def counties():
    out = C.path(C.DATA, "reference", "ca_counties.csv")
    if os.path.exists(out):
        return
    z = zipfile.ZipFile(io.BytesIO(get(GAZ).content))
    rows = list(csv.reader(io.StringIO(z.read(z.namelist()[0]).decode("latin-1")), delimiter="\t"))
    hdr = [h.strip() for h in rows[0]]
    ca = [[c.strip() for c in r] for r in rows[1:] if r and r[0] == "CA"]
    with open(out, "w", newline="") as f:
        w = csv.writer(f); w.writerow(hdr); w.writerows(ca)
    print(f"counties: {len(ca)}")


def epa(year):
    out = C.path(C.DATA, "epa", f"daily_88101_{year}_CA.csv")
    if os.path.exists(out):
        return
    z = zipfile.ZipFile(io.BytesIO(get(EPA.format(y=year)).content))
    with z.open(z.namelist()[0]) as f, open(out, "w", newline="") as g:
        rd = csv.reader(io.TextIOWrapper(f, encoding="utf-8")); w = csv.writer(g)
        hdr = next(rd); w.writerow(hdr); i = hdr.index("State Code"); n = 0
        for row in rd:
            if row[i].zfill(2) == "06":
                w.writerow(row); n += 1
    print(f"epa {year}: {n} California rows")


def firms(day, key):
    out = C.path(C.DATA, "firms", str(day.year), f"firms_{day}.csv")
    if os.path.exists(out):
        return
    bbox = ",".join(str(v) for v in C.FIRE_BBOX)
    r = get(FIRMS.format(key=key, bbox=bbox, day=day))
    open(out, "w").write(r.text)
    print(f"firms {day}: {max(r.text.count(chr(10)) - 1, 0)} detections")


def ghcn():
    st_path = C.path(C.DATA, "weather", "ghcnd-stations.txt")
    if not os.path.exists(st_path):
        open(st_path, "w").write(get(GHCN_STATIONS).text)
    ids = []
    for line in open(st_path):
        sid, state = line[0:11], line[38:40]
        if state == "CA" and sid.startswith("USW"):     # first-order stations: these report daily wind (AWND, WDF2)
            ids.append(sid)
    print(f"weather stations to fetch: {len(ids)}")
    for sid in ids:
        out = C.path(C.DATA, "weather", "stations", f"{sid}.csv")
        if os.path.exists(out):
            continue
        r = get(GHCN_ACCESS.format(sid=sid), ok404=True)
        if r is None:
            continue
        # keep only the study years to save space
        lines = r.text.splitlines()
        keep = [lines[0]] + [l for l in lines[1:] if any(f'"{y}-' in l for y in C.SEASONS)]
        open(out, "w").write("\n".join(keep) + "\n")
        time.sleep(0.2)


def hms(day):
    out = C.path(C.DATA, "hms", str(day.year), f"hms_smoke{day:%Y%m%d}.zip")
    if os.path.exists(out) or os.path.exists(out + ".missing"):
        return
    r = get(HMS.format(y=day.year, m=day.month, ymd=f"{day:%Y%m%d}"), ok404=True)
    if r is None:
        open(out + ".missing", "w").write("404")      # some days have no analysis; remember so we don't retry
        return
    open(out, "wb").write(r.content)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seasons", type=int, nargs="*", default=C.SEASONS)
    ap.add_argument("--skip", nargs="*", default=[], choices=["epa", "firms", "weather", "hms"])
    a = ap.parse_args()
    load_env()
    key = os.environ.get("FIRMS_MAP_KEY")
    if "firms" not in a.skip and not key:
        sys.exit("FIRMS_MAP_KEY missing: put it in .env (see README) or use --skip firms")
    counties()
    if "weather" not in a.skip:
        ghcn()
    for y in a.seasons:
        if "epa" not in a.skip:
            epa(y)
        for d in C.season_days(y):
            if "hms" not in a.skip:
                hms(d)
            if "firms" not in a.skip:
                firms(d, key)
        # one extra UTC day: early-morning UTC detections of 30 Nov local time
        if "firms" not in a.skip:
            firms(dt.date(y, *C.SEASON_END) + dt.timedelta(days=1), key)
    print(f"Done. Data in {C.DATA}")
