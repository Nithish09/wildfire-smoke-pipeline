"""
Build the county-day modelling table from the raw research downloads.

One row = (California county c, local date t) inside a fire season. Features use information up to and
including day t; targets are day t+1 (next-day forecast):
    y_pm25_next   county-mean PM2.5 on t+1 (ug/m3)          regression target
    y_exceed_next y_pm25_next > 35 ug/m3 (EPA 24-h standard)  classification target

Feature groups (used for the ablation study, see FEATURE_GROUPS):
    base       PM2.5 persistence (t, t-1, t-2, 3-day mean), neighbouring-county PM2.5, weather, calendar, location
    fire       fire counts and fire radiative power (FRP) within 50/100/200/400 km at t and t-1, nearest-fire distance
    transport  wind-aware upwind FRP index (this study's main feature) at two decay lengths, t and t-1
    placebo    the same index computed DOWNWIND (should not help; a control for the transport hypothesis)
    hms        NOAA HMS analyst smoke density over the county centre on day t

Usage:  python3 research/build_dataset.py            -> research_out/features.csv.gz
"""
import glob, json, os, sys
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as C  # noqa: E402
import hms as H  # noqa: E402

R_EARTH = 6371.0


def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * R_EARTH * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def bearing(lat1, lon1, lat2, lon2):
    """Initial bearing (degrees from north, clockwise) from point 1 to point 2."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    x = np.sin(lon2 - lon1) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(lon2 - lon1)
    return (np.degrees(np.arctan2(x, y)) + 360) % 360


# ------------------------------------------------------------------ loaders
def load_counties():
    c = pd.read_csv(os.path.join(C.DATA, "reference", "ca_counties.csv"), dtype=str)
    c = c[c.USPS == "CA"]
    return pd.DataFrame({"county_fips": c.GEOID, "county_name": c.NAME.str.replace(" County$", "", regex=True),
                         "lat": c.INTPTLAT.astype(float), "lon": c.INTPTLONG.astype(float),
                         "land_sqmi": c.ALAND_SQMI.astype(float)}).reset_index(drop=True)


def load_pm25(years):
    frames = []
    for y in years:
        p = os.path.join(C.DATA, "epa", f"daily_88101_{y}_CA.csv")
        if os.path.exists(p):
            frames.append(pd.read_csv(p, dtype=str))
    if not frames:
        raise SystemExit("No EPA files found; run research/download_research_data.py first")
    e = pd.concat(frames, ignore_index=True)
    e = e[(e["State Code"].str.zfill(2) == "06") & (e["Parameter Code"] == C.PM25_PARAM)].copy()
    e["county_fips"] = "06" + e["County Code"].str.zfill(3)
    e["site"] = e.county_fips + e["Site Num"].str.zfill(4)
    e["date"] = pd.to_datetime(e["Date Local"]).dt.date
    e["pm"] = pd.to_numeric(e["Arithmetic Mean"], errors="coerce")
    e["obs_pct"] = pd.to_numeric(e["Observation Percent"], errors="coerce")
    e = e[e.pm.between(0, 1000) & (e.obs_pct.isna() | (e.obs_pct >= 75))]
    # same rules as the Silver layer: one row per site/POC/date/duration, keep wildfire-'Included' over 'Excluded'
    e["excl"] = (e["Event Type"] == "Excluded").astype(int)
    e = e.sort_values("excl").drop_duplicates(["site", "POC", "date", "Sample Duration"])
    site = e.groupby(["county_fips", "site", "date"]).pm.mean().reset_index()
    county = site.groupby(["county_fips", "date"]).agg(pm25=("pm", "mean"), pm25_sites=("site", "nunique")).reset_index()
    return county


def load_fires(year):
    files = sorted(glob.glob(os.path.join(C.DATA, "firms", str(year), "firms_*.csv")))
    frames = [pd.read_csv(f, dtype=str) for f in files if os.path.getsize(f) > 0]
    frames = [f for f in frames if "latitude" in f.columns and len(f)]
    if not frames:
        return pd.DataFrame(columns=["date", "lat", "lon", "frp"])
    f = pd.concat(frames, ignore_index=True)
    f["lat"] = pd.to_numeric(f.latitude, errors="coerce"); f["lon"] = pd.to_numeric(f.longitude, errors="coerce")
    f["frp"] = pd.to_numeric(f.frp, errors="coerce").clip(lower=0)
    if "type" in f.columns:                          # SP files: 0 = presumed vegetation fire
        f = f[pd.to_numeric(f["type"], errors="coerce").fillna(0) == 0]
    f = f[f.confidence.str.lower().str.strip().isin(["n", "h", "nominal", "high"])]
    ts = pd.to_datetime(f.acq_date + " " + f.acq_time.str.strip().str.zfill(4), format="%Y-%m-%d %H%M", errors="coerce")
    f["date"] = ts.dt.tz_localize("UTC").dt.tz_convert(C.LOCAL_TZ).dt.date
    f = f.dropna(subset=["lat", "lon", "date"]).drop_duplicates(["lat", "lon", "acq_date", "acq_time", "satellite"])
    W, S, E, N = C.FIRE_BBOX
    f = f[f.lat.between(S, N) & f.lon.between(W, E)]
    return f[["date", "lat", "lon", "frp"]].reset_index(drop=True)


def load_weather(years):
    rows = []
    for p in sorted(glob.glob(os.path.join(C.DATA, "weather", "stations", "*.csv"))):
        w = pd.read_csv(p, dtype=str)
        if w.empty or "DATE" not in w.columns:
            continue
        w["date"] = pd.to_datetime(w.DATE, errors="coerce").dt.date
        w = w[pd.to_datetime(w.DATE, errors="coerce").dt.year.isin(years)]
        if w.empty:
            continue
        out = pd.DataFrame({"station": w.STATION, "date": w.date,
                            "slat": pd.to_numeric(w.LATITUDE, errors="coerce"), "slon": pd.to_numeric(w.LONGITUDE, errors="coerce")})
        for col in ["TMAX", "TMIN", "PRCP", "AWND", "WDF2"]:
            out[col] = pd.to_numeric(w[col], errors="coerce") if col in w.columns else np.nan
        # GHCN-Daily raw units are tenths (TMAX/TMIN 0.1 C, PRCP 0.1 mm, AWND 0.1 m/s); detect and convert
        if out.TMAX.median(skipna=True) > 60:
            for col in ["TMAX", "TMIN", "PRCP", "AWND"]:
                out[col] = out[col] / 10.0
        out.loc[~out.TMAX.between(-40, 60), "TMAX"] = np.nan
        out.loc[~out.TMIN.between(-40, 60), "TMIN"] = np.nan
        out.loc[~out.PRCP.between(0, 1000), "PRCP"] = np.nan
        out.loc[~out.AWND.between(0, 75), "AWND"] = np.nan
        out.loc[~out.WDF2.between(0, 360), "WDF2"] = np.nan
        rows.append(out)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def load_hms(day):
    p = os.path.join(C.DATA, "hms", str(day.year), f"hms_smoke{day:%Y%m%d}.zip")
    if not os.path.exists(p):
        return None
    try:
        return H.read_hms_zip(p)
    except Exception as ex:                          # corrupt or unexpected file: treat day as missing
        print(f"  HMS {day}: unreadable ({ex})")
        return None


# ------------------------------------------------------------------ per-day features
def weather_for_counties(wx_day, cty):
    """Inverse-distance mean of the N nearest stations that report each variable."""
    out = {}
    for var in ["TMAX", "TMIN", "PRCP", "AWND"]:
        s = wx_day.dropna(subset=[var])
        if s.empty:
            out[var] = np.full(len(cty), np.nan); continue
        d = haversine(cty.lat.values[:, None], cty.lon.values[:, None], s.slat.values[None, :], s.slon.values[None, :])
        k = min(C.N_WEATHER_STATIONS, d.shape[1])
        idx = np.argsort(d, axis=1)[:, :k]
        dd = np.take_along_axis(d, idx, 1); w = 1 / np.maximum(dd, 5.0)
        out[var] = (s[var].values[idx] * w).sum(1) / w.sum(1)
    s = wx_day.dropna(subset=["WDF2"])
    if s.empty:
        out["u"] = out["v"] = np.full(len(cty), np.nan)
    else:
        d = haversine(cty.lat.values[:, None], cty.lon.values[:, None], s.slat.values[None, :], s.slon.values[None, :])
        k = min(C.N_WEATHER_STATIONS, d.shape[1])
        idx = np.argsort(d, axis=1)[:, :k]
        w = 1 / np.maximum(np.take_along_axis(d, idx, 1), 5.0)
        th = np.radians(s.WDF2.values[idx])                       # direction the wind blows FROM
        out["u"] = (np.sin(th) * w).sum(1) / w.sum(1)              # unit vector pointing to the source side
        out["v"] = (np.cos(th) * w).sum(1) / w.sum(1)
    return out


def fire_features(fd, cty, wind_from_deg):
    n = len(cty)
    feats = {}
    if fd.empty:
        for r in C.FIRE_RINGS_KM:
            feats[f"fire_n_{r}km"] = np.zeros(n); feats[f"fire_frp_{r}km"] = np.zeros(n)
        feats["fire_nearest_km"] = np.full(n, 1000.0)
        for L in C.TRANSPORT_DECAY_KM:
            feats[f"transport_L{int(L)}"] = np.zeros(n); feats[f"placebo_L{int(L)}"] = np.zeros(n)
        return feats
    d = haversine(cty.lat.values[:, None], cty.lon.values[:, None], fd.lat.values[None, :], fd.lon.values[None, :])
    frp = fd.frp.fillna(0).values[None, :]
    for r in C.FIRE_RINGS_KM:
        m = d <= r
        feats[f"fire_n_{r}km"] = m.sum(1).astype(float)
        feats[f"fire_frp_{r}km"] = (frp * m).sum(1)
    feats["fire_nearest_km"] = np.minimum(d.min(1), 1000.0)
    b = bearing(cty.lat.values[:, None], cty.lon.values[:, None], fd.lat.values[None, :], fd.lon.values[None, :])
    phi = np.nan_to_num(wind_from_deg, nan=270.0)[:, None]             # missing wind -> prevailing westerly
    align_up = np.cos(np.radians(b - phi))                             # +1 = fire lies exactly upwind
    within = d <= C.TRANSPORT_MAX_KM
    for L in C.TRANSPORT_DECAY_KM:
        decay = np.exp(-d / L) * within
        feats[f"transport_L{int(L)}"] = (frp * np.clip(align_up, 0, None) * decay).sum(1)
        feats[f"placebo_L{int(L)}"] = (frp * np.clip(-align_up, 0, None) * decay).sum(1)   # fires downwind
    return feats


FEATURE_GROUPS = {
    "base": ["pm25_t", "pm25_lag1", "pm25_lag2", "pm25_mean3", "pm25_neigh_t", "pm25_missing_t",
             "tmax_c", "tmin_c", "prcp_mm", "wind_ms", "wind_from_u", "wind_from_v",
             "doy_sin", "doy_cos", "lat", "lon"],
    "fire": [f"fire_n_{r}km" for r in C.FIRE_RINGS_KM] + [f"fire_frp_{r}km" for r in C.FIRE_RINGS_KM]
            + [f"fire_frp_{r}km_lag1" for r in C.FIRE_RINGS_KM] + ["fire_nearest_km"],
    "transport": [f"transport_L{int(L)}" for L in C.TRANSPORT_DECAY_KM] + [f"transport_L{int(L)}_lag1" for L in C.TRANSPORT_DECAY_KM],
    "placebo": [f"placebo_L{int(L)}" for L in C.TRANSPORT_DECAY_KM] + [f"placebo_L{int(L)}_lag1" for L in C.TRANSPORT_DECAY_KM],
    "hms": ["hms_density_t"],
}


def build(years=None):
    years = years or C.SEASONS
    cty = load_counties()
    pm = load_pm25(years)
    wx = load_weather(years)
    rows = []
    for y in years:
        fires = load_fires(y)
        print(f"{y}: {len(fires):,} fire detections")
        fires_by_day = {d: g for d, g in fires.groupby("date")}
        wx_by_day = {d: g for d, g in wx.groupby("date")} if not wx.empty else {}
        for day in C.season_days(y):
            wd = wx_by_day.get(day, pd.DataFrame(columns=["slat", "slon", "TMAX", "TMIN", "PRCP", "AWND", "WDF2"]))
            w = weather_for_counties(wd, cty)
            wind_from = (np.degrees(np.arctan2(w["u"], w["v"])) + 360) % 360
            ff = fire_features(fires_by_day.get(day, pd.DataFrame(columns=["lat", "lon", "frp"])), cty, wind_from)
            polys = load_hms(day)
            hms_d = H.max_density_at(cty.lon.values, cty.lat.values, polys) if polys is not None else np.full(len(cty), np.nan)
            frame = pd.DataFrame({"county_fips": cty.county_fips, "date": day, "season": y,
                                  "tmax_c": w["TMAX"], "tmin_c": w["TMIN"], "prcp_mm": w["PRCP"], "wind_ms": w["AWND"],
                                  "wind_from_u": w["u"], "wind_from_v": w["v"], "hms_density_t": hms_d, **ff})
            rows.append(frame)
    df = pd.concat(rows, ignore_index=True)
    df = df.merge(cty[["county_fips", "lat", "lon"]], on="county_fips").merge(
        pm.rename(columns={"pm25": "pm25_t"}), on=["county_fips", "date"], how="left")
    df = df.sort_values(["county_fips", "date"]).reset_index(drop=True)
    g = df.groupby(["county_fips", "season"])
    df["pm25_lag1"] = g.pm25_t.shift(1); df["pm25_lag2"] = g.pm25_t.shift(2)
    df["pm25_mean3"] = df[["pm25_t", "pm25_lag1", "pm25_lag2"]].mean(axis=1)
    df["pm25_missing_t"] = df.pm25_t.isna().astype(int)
    for c in [f"fire_frp_{r}km" for r in C.FIRE_RINGS_KM] + [f"transport_L{int(L)}" for L in C.TRANSPORT_DECAY_KM] \
            + [f"placebo_L{int(L)}" for L in C.TRANSPORT_DECAY_KM]:
        df[f"{c}_lag1"] = g[c].shift(1)
    df["y_pm25_next"] = g.pm25_t.shift(-1)
    # neighbouring counties' PM2.5 on day t (inverse-distance, within NEIGHBOUR_RADIUS_KM, excluding itself)
    D = haversine(cty.lat.values[:, None], cty.lon.values[:, None], cty.lat.values[None, :], cty.lon.values[None, :])
    Wn = np.where((D > 0) & (D <= C.NEIGHBOUR_RADIUS_KM), 1 / np.maximum(D, 5.0), 0.0)
    order = {f: i for i, f in enumerate(cty.county_fips)}
    neigh = np.full(len(df), np.nan)
    for day, idx in df.groupby("date").groups.items():
        sub = df.loc[idx]
        v = np.full(len(cty), np.nan); v[[order[f] for f in sub.county_fips]] = sub.pm25_t.values
        have = ~np.isnan(v); num = Wn[:, have] @ v[have]; den = Wn[:, have].sum(1)
        res = np.where(den > 0, num / np.where(den > 0, den, 1), np.nan)
        neigh[np.asarray(idx)] = res[[order[f] for f in sub.county_fips]]
    df["pm25_neigh_t"] = neigh
    doy = pd.to_datetime(df.date).dt.dayofyear
    df["doy_sin"] = np.sin(2 * np.pi * doy / 365.25); df["doy_cos"] = np.cos(2 * np.pi * doy / 365.25)
    df = df[df.y_pm25_next.notna()].copy()               # only rows with a measured next-day target
    df["y_exceed_next"] = (df.y_pm25_next > C.EXCEEDANCE).astype(int)
    df["smoke_context"] = ((df.pm25_t > C.EXCEEDANCE) | (df.y_pm25_next > C.EXCEEDANCE) | (df.hms_density_t >= 2)).astype(int)
    return df


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(); ap.add_argument("--seasons", type=int, nargs="*", default=C.SEASONS)
    a = ap.parse_args()
    df = build(a.seasons)
    out = C.path(C.OUT, "features.csv.gz")
    df.to_csv(out, index=False)
    json.dump(FEATURE_GROUPS, open(C.path(C.OUT, "feature_groups.json"), "w"), indent=1)
    print(f"{len(df):,} county-day rows, {df.county_fips.nunique()} counties, "
          f"{df.y_exceed_next.mean():.1%} exceedance days -> {out}")
