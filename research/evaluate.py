"""Evaluation protocol shared by the ML and DL scripts: splits, metrics, bootstrap CIs, Diebold-Mariano test."""
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.cluster import KMeans
from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score, roc_auc_score

EXCEED = 35.0


HEAVY_TAILED = ("fire_n_", "fire_frp_", "transport_", "placebo_")


def prepare_features(df):
    """log1p-compress heavy-tailed fire/transport counts and power (helps linear and neural models; trees unaffected)."""
    df = df.copy()
    for c in df.columns:
        if c.startswith(HEAVY_TAILED):
            df[c] = np.log1p(df[c].clip(lower=0))
    return df


def season_splits(df):
    """Leave-one-season-out: train on the other fire seasons, test on the held-out one (temporal generalisation)."""
    for s in sorted(df.season.unique()):
        yield f"season_{s}", df.season != s, df.season == s


def region_splits(df, k=5, seed=0):
    """Leave-one-region-out over counties clustered by location (spatial generalisation to unseen counties)."""
    cty = df.groupby("county_fips")[["lat", "lon"]].first()
    lab = KMeans(n_clusters=min(k, len(cty)), n_init=10, random_state=seed).fit_predict(cty.values)
    region = df.county_fips.map(dict(zip(cty.index, lab)))
    for r in sorted(set(lab)):
        yield f"region_{r}", region != r, region == r


def point_metrics(y, p):
    y, p = np.asarray(y, float), np.asarray(p, float)
    err = p - y
    out = {"n": len(y), "rmse": float(np.sqrt(np.mean(err ** 2))), "mae": float(np.mean(np.abs(err))),
           "r2": float(1 - np.sum(err ** 2) / np.sum((y - y.mean()) ** 2)) if len(y) > 1 else np.nan}
    yb, pb = y > EXCEED, p > EXCEED
    out.update({"exceed_precision": precision_score(yb, pb, zero_division=0),
                "exceed_recall": recall_score(yb, pb, zero_division=0),
                "exceed_f1": f1_score(yb, pb, zero_division=0)})
    if 0 < yb.sum() < len(yb):
        out["exceed_auprc"] = average_precision_score(yb, p); out["exceed_auroc"] = roc_auc_score(yb, p)
    else:
        out["exceed_auprc"] = out["exceed_auroc"] = np.nan
    return out


def bootstrap_ci(df, pred_col, metric="rmse", n=500, seed=0):
    """95% CI by resampling whole days (keeps same-day spatial correlation together)."""
    rng = np.random.default_rng(seed)
    days = df.date.unique(); groups = {d: g for d, g in df.groupby("date")}
    vals = []
    for _ in range(n):
        samp = pd.concat([groups[d] for d in rng.choice(days, len(days))])
        vals.append(point_metrics(samp.y_pm25_next, samp[pred_col])[metric])
    return float(np.nanpercentile(vals, 2.5)), float(np.nanpercentile(vals, 97.5))


def diebold_mariano(df, pred_a, pred_b, power=2):
    """DM test on daily mean loss differential (a vs b). Negative statistic => a has lower loss. Newey-West lag 1."""
    d = (np.abs(df[pred_a] - df.y_pm25_next) ** power - np.abs(df[pred_b] - df.y_pm25_next) ** power)
    dd = d.groupby(df.date).mean().sort_index().values
    n = len(dd)
    if n < 10:
        return np.nan, np.nan
    m = dd.mean(); g0 = np.mean((dd - m) ** 2); g1 = np.mean((dd[1:] - m) * (dd[:-1] - m))
    var = (g0 + g1) / n                      # Newey-West, Bartlett weight 1/2 at lag 1 (always >= 0)
    stat = m / np.sqrt(var) if var > 0 else np.nan
    return float(stat), float(2 * (1 - stats.norm.cdf(abs(stat))))
