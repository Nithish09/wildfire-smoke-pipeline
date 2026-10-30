"""
Panel tensors and wind-aware graph for the deep-learning models (numpy only, so it can be unit-tested without torch).

Panel: X[T, N, F] features, Y[T, N] next-day log1p(PM2.5), M[T, N] target mask, for one or more seasons.
Graph: for every day t, A[t, i, j] = weight of county j as a smoke SOURCE for county i
       = max(0, cos(bearing(i -> j) - wind_from_i(t))) * exp(-d_ij / L), d_ij <= max_km, row-normalised,
       plus a static distance kernel A0[i, j] = exp(-d_ij / L) (j != i). Inspired by PM2.5-GNN (Wang et al., 2020).
"""
import numpy as np
import pandas as pd

from build_dataset import bearing, haversine


def build_panel(df, feats, seasons):
    sub = df[df.season.isin(seasons)]
    counties = sorted(df.county_fips.unique())
    dates = sorted(sub.date.unique())
    ci = {c: i for i, c in enumerate(counties)}; ti = {d: i for i, d in enumerate(dates)}
    T, N, F = len(dates), len(counties), len(feats)
    X = np.full((T, N, F), np.nan, dtype=np.float32)
    Y = np.zeros((T, N), dtype=np.float32); M = np.zeros((T, N), dtype=np.float32)
    WF = np.full((T, N), np.nan, dtype=np.float32)          # wind-from direction, degrees
    S = np.zeros(T, dtype=int)
    t_idx = sub.date.map(ti).values; n_idx = sub.county_fips.map(ci).values
    X[t_idx, n_idx] = sub[feats].values.astype(np.float32)
    Y[t_idx, n_idx] = np.log1p(sub.y_pm25_next.values); M[t_idx, n_idx] = 1.0
    WF[t_idx, n_idx] = (np.degrees(np.arctan2(sub.wind_from_u.values, sub.wind_from_v.values)) + 360) % 360
    S[t_idx] = sub.season.values
    return {"X": X, "Y": Y, "M": M, "WF": WF, "season": S, "dates": dates, "counties": counties}


def county_coords(df, counties):
    g = df.groupby("county_fips")[["lat", "lon"]].first()
    return g.loc[counties, "lat"].values, g.loc[counties, "lon"].values


def wind_graph(lat, lon, WF, L=300.0, max_km=600.0):
    D = haversine(lat[:, None], lon[:, None], lat[None, :], lon[None, :])
    B = bearing(lat[:, None], lon[:, None], lat[None, :], lon[None, :])          # bearing i -> j
    K = np.exp(-D / L) * (D <= max_km) * (D > 0)
    phi = np.nan_to_num(WF, nan=270.0)                                            # T x N
    align = np.clip(np.cos(np.radians(B[None, :, :] - phi[:, :, None])), 0, None)  # T x N x N
    A = (align * K[None]).astype(np.float32)
    A = A / np.maximum(A.sum(-1, keepdims=True), 1e-6)
    A0 = (K / np.maximum(K.sum(-1, keepdims=True), 1e-6)).astype(np.float32)
    return A, A0


def standardise(X, train_mask_t):
    """Feature-wise median impute + z-score using training days only; adds a missing-indicator channel per feature group."""
    Xtr = X[train_mask_t].reshape(-1, X.shape[-1])
    med = np.nanmedian(Xtr, axis=0); med = np.where(np.isnan(med), 0, med)
    miss = np.isnan(X).astype(np.float32)
    Xf = np.where(np.isnan(X), med, X)
    mu = Xf[train_mask_t].reshape(-1, X.shape[-1]).mean(0); sd = Xf[train_mask_t].reshape(-1, X.shape[-1]).std(0) + 1e-6
    Xs = (Xf - mu) / sd
    return np.concatenate([Xs, miss.any(-1, keepdims=True).astype(np.float32)], -1).astype(np.float32)


def windows(T, W, valid_t):
    """End indices t (t >= W-1) whose window [t-W+1, t] lies within one season and t is in valid_t."""
    return [t for t in range(W - 1, T) if valid_t[t]]
