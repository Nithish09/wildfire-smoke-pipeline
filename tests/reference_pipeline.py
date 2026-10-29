"""
Plain-Python (pandas) re-implementation of the Bronze → Silver → Gold rules in notebooks/02–04.
Purpose: test the pipeline LOGIC (validation, dedup, MERGE/incremental behaviour, Gold aggregation) on a laptop,
without Spark or Databricks. Tables are kept as pickled DataFrames in a state folder.

It mirrors the notebooks rule for rule; if you change a rule in a notebook, change it here too.
"""
import datetime as dt, hashlib, json, math, os, re
import numpy as np
import pandas as pd

CA_BBOX = (-124.5, 32.5, -114.1, 42.0)
PM25_MIN, PM25_MAX = 0.0, 1000.0
TEMP_MIN_C, TEMP_MAX_C = -40.0, 60.0
WIND_MAX_MS, FRP_MAX_MW = 75.0, 20000.0
NOAA_ELEMENTS = ["TMAX", "TMIN", "PRCP", "AWND", "WDF2", "WSF2"]
NEAR_KM, PM25_STD = 100.0, 35.0
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(a))


class Pipeline:
    def __init__(self, raw_dir, state_dir, start_date, end_date):
        self.raw, self.state = raw_dir, state_dir
        self.start, self.end = start_date, end_date          # study window in LOCAL dates, as in 00_config
        os.makedirs(state_dir, exist_ok=True)
        self.clock = 0                      # monotonically increasing "ingestion timestamp"

    # ---------- table storage ----------
    def load(self, name):
        p = os.path.join(self.state, name + ".pkl")
        return pd.read_pickle(p) if os.path.exists(p) else None

    def save(self, name, obj):
        pd.to_pickle(obj, os.path.join(self.state, name + ".pkl"))

    def append(self, name, df):
        old = self.load(name)
        self.save(name, df if old is None else pd.concat([old, df], ignore_index=True))

    # ---------- BRONZE ----------
    def bronze(self):
        meta = self.load("meta") or {"clock": 0, "watermark": {}}
        self.clock = meta["clock"]
        manifest = self.load("ops_ingest_manifest")
        seen = set() if manifest is None else set(zip(manifest.file_name, manifest.content_hash))
        loaded = {}
        for source, folder, ext, table in [("nasa_firms", "firms", ".csv", "bronze_firms"),
                                           ("epa_aqs", "epa", ".json", "bronze_epa_pm25"),
                                           ("noaa_cdo", "noaa", ".json", "bronze_noaa_daily")]:
            n_total = 0
            for name in sorted(os.listdir(os.path.join(self.raw, folder))):
                if not name.endswith(ext) or not DATE_RE.search(name):
                    continue
                path = os.path.join(self.raw, folder, name)
                h = hashlib.sha256(open(path, "rb").read()).hexdigest()
                if (name, h) in seen:
                    continue
                if ext == ".csv":
                    df = pd.read_csv(path, dtype=str, keep_default_na=False).replace("", None)
                else:
                    doc = json.load(open(path))
                    df = pd.DataFrame(doc.get("Data" if source == "epa_aqs" else "results", []))
                    df = df.astype(object).where(df.notna(), None).map(lambda v: None if v is None else str(v))
                self.clock += 1
                df["_source_file"] = name
                df["_data_date"] = DATE_RE.search(name).group(1)
                df["_ingested_at"] = self.clock
                df["_content_hash"] = h
                self.append(table, df)
                self.append("ops_ingest_manifest", pd.DataFrame([{"source": source, "file_name": name,
                                                                   "content_hash": h, "rows": len(df), "ingested_at": self.clock}]))
                n_total += len(df)
            loaded[source] = n_total
        meta["clock"] = self.clock
        self.save("meta", meta)
        return loaded

    # ---------- SILVER helpers ----------
    def incoming(self, table, source, meta):
        df = self.load(table)
        if df is None:
            return pd.DataFrame()
        wm = meta["watermark"].get(source)
        return df if wm is None else df[df._ingested_at > wm].copy()

    @staticmethod
    def split(df, rules):
        reason = pd.Series([None] * len(df), index=df.index, dtype=object)
        for name, ok in rules:
            ok = ok.fillna(False).astype(bool)
            reason = reason.where(reason.notna() | ok, name)
        return df[reason.isna()].copy(), df[reason.notna()].assign(reason=reason[reason.notna()])

    def quarantine(self, bad, source, key):
        if bad.empty:
            return
        q = pd.DataFrame({"source": source, "reason": bad.reason,
                          "record_key": bad[key].apply(lambda c: c.map(str)).agg("|".join, axis=1), "_source_file": bad._source_file})
        old = self.load("silver_quarantine")
        allq = q if old is None else pd.concat([old, q])
        self.save("silver_quarantine", allq.drop_duplicates(["source", "record_key", "reason", "_source_file"]))

    @staticmethod
    def latest(df, key, order, ascending):
        return df.sort_values(order, ascending=ascending).drop_duplicates(key, keep="first")

    def merge(self, table, new, key, is_newer):
        """Delta-style MERGE: insert new keys; update matched rows only where is_newer(new_row, old_row)."""
        old = self.load(table)
        if old is None:
            self.save(table, new.reset_index(drop=True)); return {"inserted": len(new), "updated": 0}
        idx_old = old.set_index(key); ins = upd = 0
        rows = []
        for k, n in new.set_index(key).iterrows():
            if k in idx_old.index:
                if is_newer(n, idx_old.loc[k]):
                    idx_old.loc[k] = n; upd += 1
            else:
                rows.append((k, n))
        out = idx_old.reset_index()
        if rows:
            add = pd.DataFrame([n for _, n in rows]); add.index = pd.MultiIndex.from_tuples([k for k, _ in rows], names=key)
            out = pd.concat([out, add.reset_index()], ignore_index=True)
        self.save(table, out)
        return {"inserted": ins + len(rows), "updated": upd}

    def assign_county(self, df, lat, lon):
        c = self.load("silver_dim_county")
        d = haversine_km(df[lat].values[:, None], df[lon].values[:, None], c.c_lat.values[None, :], c.c_lon.values[None, :])
        df = df.copy()
        df["county_fips"] = c.county_fips.values[d.argmin(axis=1)]
        df["county_distance_km"] = d.min(axis=1).round(1)
        return df

    def queue(self, dates):
        old = self.load("ops_gold_pending_dates") or set()
        self.save("ops_gold_pending_dates", set(old) | set(dates))

    # ---------- SILVER ----------
    def silver(self):
        meta = self.load("meta")
        stats = {}
        ref = pd.read_csv(os.path.join(self.raw, "reference", "ca_counties.csv"), dtype=str)
        dim = pd.DataFrame({"county_fips": ref.GEOID, "county_name": ref.NAME.str.replace(" County$", "", regex=True),
                            "c_lat": ref.INTPTLAT.astype(float), "c_lon": ref.INTPTLONG.astype(float)})[ref.USPS == "CA"]
        self.save("silver_dim_county", dim)

        # FIRMS
        b = self.incoming("bronze_firms", "nasa_firms", meta)
        if not b.empty:
            f = b.copy()
            f["latitude"] = pd.to_numeric(f.latitude, errors="coerce"); f["longitude"] = pd.to_numeric(f.longitude, errors="coerce")
            f["acq_ts_utc"] = pd.to_datetime(f.acq_date + " " + f.acq_time.str.strip().str.zfill(4), format="%Y-%m-%d %H%M", errors="coerce")
            f["date_local"] = (f.acq_ts_utc.dt.tz_localize("UTC").dt.tz_convert("America/Los_Angeles").dt.date)
            f["frp_mw"] = pd.to_numeric(f.frp, errors="coerce")
            conf = f.confidence.str.lower().str.strip()
            f["confidence_class"] = conf.map({"l": "low", "low": "low", "n": "nominal", "nominal": "nominal", "h": "high", "high": "high"})
            W, S, E, N = CA_BBOX
            key = ["latitude", "longitude", "acq_ts_utc", "satellite"]
            good, bad = self.split(f, [
                ("missing_coordinates", f.latitude.notna() & f.longitude.notna()),
                ("outside_study_area", f.latitude.between(S, N) & f.longitude.between(W, E)),
                ("bad_timestamp", f.acq_ts_utc.notna()),
                ("frp_out_of_range", f.frp_mw.isna() | f.frp_mw.between(0, FRP_MAX_MW)),
                ("unknown_confidence", f.confidence_class.notna())])
            good = self.latest(good, key, ["_ingested_at"], [False])
            good = self.assign_county(good[key + ["date_local", "frp_mw", "confidence_class", "_ingested_at"]], "latitude", "longitude")
            self.quarantine(bad, "nasa_firms", key)
            stats["firms_merge"] = self.merge("silver_fire_detections", good, key, lambda n, o: n._ingested_at > o._ingested_at)
            self.queue(good.date_local)
            meta["watermark"]["nasa_firms"] = int(b._ingested_at.max())

        # EPA
        b = self.incoming("bronze_epa_pm25", "epa_aqs", meta)
        if not b.empty:
            e = b.copy()
            e["county_fips"] = e.state_code.str.zfill(2) + e.county_code.str.zfill(3)
            e["site_id"] = e.county_fips + e.site_number.str.zfill(4)
            e["date_local"] = pd.to_datetime(e.date_local, errors="coerce").dt.date
            e["pm25_ugm3"] = pd.to_numeric(e.arithmetic_mean, errors="coerce")
            e["aqi"] = pd.to_numeric(e.aqi, errors="coerce")
            e["observation_percent"] = pd.to_numeric(e.observation_percent, errors="coerce")
            e["date_of_last_change"] = pd.to_datetime(e.date_of_last_change, errors="coerce")
            e["event_rank"] = (e.event_type == "Excluded").astype(int)
            key = ["site_id", "poc", "date_local", "sample_duration"]
            good, bad = self.split(e, [
                ("wrong_parameter", e.parameter_code == "88101"),
                ("wrong_state", e.state_code.str.zfill(2) == "06"),
                ("bad_date", e.date_local.notna()),
                ("missing_value", e.pm25_ugm3.notna()),
                ("impossible_value", e.pm25_ugm3.between(PM25_MIN, PM25_MAX)),
                ("low_completeness", e.observation_percent.isna() | (e.observation_percent >= 75))])
            good = self.latest(good, key, ["event_rank", "date_of_last_change", "_ingested_at"], [True, False, False])
            good = good[key + ["county_fips", "pm25_ugm3", "aqi", "event_type", "date_of_last_change", "_ingested_at"]]
            self.quarantine(bad, "epa_aqs", key)

            def newer(n, o):
                if n.event_type == "Excluded" and o.event_type != "Excluded":
                    return False
                if n.date_of_last_change > o.date_of_last_change:
                    return True
                return n.date_of_last_change == o.date_of_last_change and n._ingested_at > o._ingested_at and \
                    not (n.pm25_ugm3 == o.pm25_ugm3 and n.aqi == o.aqi)
            stats["epa_merge"] = self.merge("silver_pm25_daily", good, key, newer)
            self.queue(good.date_local)
            meta["watermark"]["epa_aqs"] = int(b._ingested_at.max())

        # NOAA
        b = self.incoming("bronze_noaa_daily", "noaa_cdo", meta)
        if not b.empty:
            st = pd.DataFrame(json.load(open(os.path.join(self.raw, "reference", "noaa_stations.json")))["results"])
            st = st.rename(columns={"id": "station", "latitude": "st_lat", "longitude": "st_lon"})[["station", "st_lat", "st_lon"]]
            w = b.merge(st, on="station", how="left")
            w["date_local"] = pd.to_datetime(w.date.str[:10], errors="coerce").dt.date
            w["element"] = w.datatype.str.upper()
            w["value"] = pd.to_numeric(w.value, errors="coerce")
            w["qflag"] = w.attributes.fillna("").str.split(",").str[1].fillna("")
            el, v = w.element, w.value
            rng = np.select([el.isin(["TMAX", "TMIN"]), el == "PRCP", el.isin(["AWND", "WSF2"]), el == "WDF2"],
                            [v.between(TEMP_MIN_C, TEMP_MAX_C), v.between(0, 1000), v.between(0, WIND_MAX_MS), v.between(0, 360)], True)
            key = ["station", "date_local", "element"]
            good, bad = self.split(w, [
                ("unneeded_element", el.isin(NOAA_ELEMENTS)),
                ("bad_date", w.date_local.notna()),
                ("missing_value", v.notna()),
                ("failed_source_qc", w.qflag.str.strip() == ""),
                ("impossible_value", pd.Series(rng, index=w.index).astype(bool)),
                ("unmatched_station", w.st_lat.notna())])
            good = self.latest(good, key, ["_ingested_at"], [False])[key + ["value", "st_lat", "st_lon", "_ingested_at"]]
            self.quarantine(bad, "noaa_cdo", key)
            stats["noaa_merge"] = self.merge("silver_weather_obs", good, key,
                                             lambda n, o: n._ingested_at > o._ingested_at and n.value != o.value)
            obs = self.load("silver_weather_obs")
            wide = obs.pivot_table(index=["station", "date_local", "st_lat", "st_lon"], columns="element", values="value", aggfunc="first").reset_index()
            wide = wide.rename(columns={"TMAX": "tmax_c", "TMIN": "tmin_c", "PRCP": "prcp_mm", "AWND": "awnd_ms", "WDF2": "wdf2_deg", "WSF2": "wsf2_ms"})
            self.save("silver_weather_daily", self.assign_county(wide, "st_lat", "st_lon"))
            self.queue(good.date_local)
            meta["watermark"]["noaa_cdo"] = int(b._ingested_at.max())
        self.save("meta", meta)
        return stats

    # ---------- GOLD ----------
    def gold(self, rebuild_all=False):
        pending = self.load("ops_gold_pending_dates") or set()
        fires = self.load("silver_fire_detections")
        if rebuild_all:
            pending = set(fires.date_local)
        days = sorted(d for d in pending if self.start <= d <= self.end)   # same window filter as 04_gold
        if not days:
            return {"inserted": 0, "updated": 0}
        dim = self.load("silver_dim_county")
        grid = dim[["county_fips"]].merge(pd.DataFrame({"date_local": days}), how="cross")
        keys = ["county_fips", "date_local"]
        fd = fires[fires.date_local.isin(days)]
        fire_in = fd.groupby(keys).agg(fire_detections=("frp_mw", "size"),
                                       fire_frp_total_mw=("frp_mw", "sum")).reset_index()
        near = fd.merge(dim[["county_fips", "c_lat", "c_lon"]], how="cross", suffixes=("_pt", ""))
        near = near[haversine_km(near.latitude, near.longitude, near.c_lat, near.c_lon) <= NEAR_KM]
        fire_near = near.groupby(keys).agg(fire_detections_within_100km=("frp_mw", "size"),
                                           fire_frp_within_100km_mw=("frp_mw", "sum")).reset_index()
        pm = self.load("silver_pm25_daily"); pm = pm[pm.date_local.isin(days)]
        site = pm.groupby(["county_fips", "site_id", "date_local"]).pm25_ugm3.mean().reset_index()
        pmc = site.groupby(keys).agg(pm25_mean_ugm3=("pm25_ugm3", "mean"), pm25_sites=("site_id", "nunique")).reset_index()
        wx = self.load("silver_weather_daily"); wx = wx[wx.date_local.isin(days)]
        wxc = wx.groupby(keys).agg(tmax_c=("tmax_c", "mean"), wind_mean_ms=("awnd_ms", "mean"),
                                   weather_stations=("station", "nunique")).reset_index()
        fact = grid
        for part in [fire_in, fire_near, pmc, wxc]:
            fact = fact.merge(part, on=keys, how="left")
        for c in ["fire_detections", "fire_frp_total_mw", "fire_detections_within_100km", "fire_frp_within_100km_mw"]:
            fact[c] = fact[c].fillna(0)
        fact["fire_frp_total_mw"] = fact.fire_frp_total_mw.round(1)
        fact["fire_frp_within_100km_mw"] = fact.fire_frp_within_100km_mw.round(1)
        fact["pm25_mean_ugm3"] = fact.pm25_mean_ugm3.round(2)
        fact["smoke_indicator"] = np.select(
            [fact.pm25_mean_ugm3.isna(), (fact.pm25_mean_ugm3 > PM25_STD) & (fact.fire_detections_within_100km > 0),
             fact.pm25_mean_ugm3 > PM25_STD, fact.fire_detections_within_100km > 0],
            ["no_pm25_data", "likely_smoke", "high_pm25_no_nearby_fire", "nearby_fire_pm25_ok"], "clear")
        measures = [c for c in fact.columns if c not in keys]

        def changed(n, o):
            return any(not (pd.isna(n[c]) and pd.isna(o[c])) and n[c] != o[c] for c in measures)
        res = self.merge("gold_fact_county_day", fact, keys, changed)
        self.save("ops_gold_pending_dates", set())
        return res
