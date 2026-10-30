"""
Classical ML experiments (scikit-learn only).

  1. Model comparison under two protocols: leave-one-season-out (temporal) and leave-one-region-out (spatial)
     models: persistence, climatology, ridge, random forest, gradient boosting (HistGB), MLP
  2. Ablation of feature groups with gradient boosting (does the wind-aware transport index add skill?
     does the downwind placebo NOT add skill?)
  3. Significance: day-block bootstrap 95% CIs and Diebold-Mariano tests vs persistence
  4. Permutation importance of the best model
  5. Independent check against NOAA HMS analyst smoke polygons

Usage:  python3 research/train_ml.py            (reads research_out/features.csv.gz, writes research_out/results/)
        python3 research/train_ml.py --quick    (fewer trees / repeats; for testing)
"""
import argparse, json, os, sys, time
import numpy as np
import pandas as pd
from sklearn.compose import TransformedTargetRegressor
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import Ridge
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config as C  # noqa: E402
import evaluate as E  # noqa: E402

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


def LogTarget(est):
    """Fit on log1p(PM2.5), predict back on the original scale (PM2.5 is heavily right-skewed)."""
    return TransformedTargetRegressor(regressor=est, func=np.log1p, inverse_func=lambda z: np.expm1(np.clip(z, 0, np.log1p(1000.0))),   # 1000 = validity cap
                                      check_inverse=False)


def make_models(quick, seed=0):
    imp = lambda: SimpleImputer(strategy="median", add_indicator=True)   # noqa: E731
    return {
        "ridge": LogTarget(make_pipeline(imp(), StandardScaler(), Ridge(alpha=1.0))),
        "random_forest": LogTarget(make_pipeline(imp(), RandomForestRegressor(
            n_estimators=60 if quick else 300, min_samples_leaf=3, max_features=0.5, n_jobs=-1, random_state=seed))),
        "gradient_boosting": LogTarget(HistGradientBoostingRegressor(
            max_iter=150 if quick else 600, learning_rate=0.05, max_leaf_nodes=31, l2_regularization=1.0,
            early_stopping=True, validation_fraction=0.15, random_state=seed)),
        "mlp": LogTarget(make_pipeline(imp(), StandardScaler(), MLPRegressor(
            hidden_layer_sizes=(128, 64), alpha=1e-3, early_stopping=True, max_iter=200 if quick else 600, random_state=seed))),
    }


def persistence(train, test):
    p = test.pm25_t.copy()
    p = p.fillna(test.pm25_neigh_t).fillna(train.y_pm25_next.median())
    return p.values


def climatology(train, test):
    m = pd.to_datetime(train.date).dt.month; mt = pd.to_datetime(test.date).dt.month
    clim = train.assign(m=m).groupby(["county_fips", "m"]).y_pm25_next.median()
    p = pd.Series([clim.get((c, mm), np.nan) for c, mm in zip(test.county_fips, mt)], index=test.index)
    return p.fillna(train.y_pm25_next.median()).values


def run_protocol(df, splits, feats, quick):
    preds = df[["county_fips", "date", "season", "y_pm25_next", "y_exceed_next", "smoke_context", "hms_density_t"]].copy()
    preds["fold"] = ""
    for name, tr, te in splits:
        train, test = df[tr], df[te]
        if test.empty or train.empty:
            continue
        preds.loc[te, "fold"] = name
        preds.loc[te, "pred_persistence"] = persistence(train, test)
        preds.loc[te, "pred_climatology"] = climatology(train, test)
        for mname, model in make_models(quick).items():
            t0 = time.time()
            model.fit(train[feats].values, train.y_pm25_next.values)
            preds.loc[te, f"pred_{mname}"] = model.predict(test[feats].values)
            print(f"  {name:10s} {mname:18s} {time.time() - t0:5.1f}s")
    return preds[preds.fold != ""]


def summarise(preds, protocol):
    rows = []
    for col in [c for c in preds.columns if c.startswith("pred_")]:
        for subset, sub in [("all", preds), ("smoke_context", preds[preds.smoke_context == 1])]:
            if len(sub) < 2:
                continue
            m = E.point_metrics(sub.y_pm25_next, sub[col])
            rows.append({"protocol": protocol, "model": col[5:], "subset": subset, **m})
    return pd.DataFrame(rows)


def main(quick=False):
    t_start = time.time()
    df = E.prepare_features(pd.read_csv(os.path.join(C.OUT, "features.csv.gz"), dtype={"county_fips": str}))
    groups = json.load(open(os.path.join(C.OUT, "feature_groups.json")))
    res_dir = os.path.join(C.OUT, "results"); fig_dir = os.path.join(C.OUT, "figures")
    os.makedirs(res_dir, exist_ok=True); os.makedirs(fig_dir, exist_ok=True)
    full = groups["base"] + groups["fire"] + groups["transport"] + groups["hms"]
    print(f"{len(df):,} rows, {df.season.nunique()} seasons, {df.county_fips.nunique()} counties, {len(full)} features")

    # 1. model comparison
    all_metrics, all_preds = [], {}
    for protocol, splits in [("leave_season_out", E.season_splits(df)), ("leave_region_out", E.region_splits(df))]:
        print(f"[{protocol}]")
        p = run_protocol(df, splits, full, quick)
        all_preds[protocol] = p
        p.to_csv(os.path.join(res_dir, f"predictions_{protocol}.csv.gz"), index=False)
        all_metrics.append(summarise(p, protocol))
    metrics = pd.concat(all_metrics, ignore_index=True)

    # 3. significance (on the temporal protocol)
    p = all_preds["leave_season_out"]
    sig = []
    for col in [c for c in p.columns if c.startswith("pred_") and c != "pred_persistence"]:
        lo, hi = E.bootstrap_ci(p, col, "rmse", n=100 if quick else 1000)
        dm, pv = E.diebold_mariano(p, col, "pred_persistence")
        sig.append({"model": col[5:], "rmse_ci_low": lo, "rmse_ci_high": hi, "dm_vs_persistence": dm, "dm_p_value": pv})
    lo, hi = E.bootstrap_ci(p, "pred_persistence", "rmse", n=100 if quick else 1000)
    sig.append({"model": "persistence", "rmse_ci_low": lo, "rmse_ci_high": hi, "dm_vs_persistence": 0.0, "dm_p_value": 1.0})
    sig = pd.DataFrame(sig)
    metrics = metrics.merge(sig.assign(protocol="leave_season_out", subset="all"), on=["protocol", "model", "subset"], how="left")
    metrics.to_csv(os.path.join(res_dir, "model_comparison.csv"), index=False)

    # 2. ablation (gradient boosting, temporal protocol)
    ablations = {
        "A0 base (persistence + weather)": groups["base"],
        "A1 + fire rings": groups["base"] + groups["fire"],
        "A2 + fire + transport (ours)": groups["base"] + groups["fire"] + groups["transport"],
        "A3 + fire + downwind placebo": groups["base"] + groups["fire"] + groups["placebo"],
        "A4 + fire + transport + HMS": full,
        "A5 base + HMS only": groups["base"] + groups["hms"],
    }
    abl_rows, abl_preds = [], df[["date", "y_pm25_next", "smoke_context"]].copy()
    for aname, feats in ablations.items():
        for name, tr, te in E.season_splits(df):
            m = make_models(quick)["gradient_boosting"].fit(df.loc[tr, feats].values, df.loc[tr, "y_pm25_next"].values)
            abl_preds.loc[te, aname] = m.predict(df.loc[te, feats].values)
        for subset, sub in [("all", abl_preds), ("smoke_context", abl_preds[abl_preds.smoke_context == 1])]:
            abl_rows.append({"ablation": aname, "subset": subset, **E.point_metrics(sub.y_pm25_next, sub[aname])})
        print(f"  ablation {aname}")
    abl = pd.DataFrame(abl_rows)
    base_name = "A1 + fire rings"
    for aname in ablations:
        if aname != base_name:
            dm, pv = E.diebold_mariano(abl_preds, aname, base_name)
            abl.loc[(abl.ablation == aname) & (abl.subset == "all"), ["dm_vs_A1", "dm_p_value"]] = [dm, pv]
    # the key contrast: upwind transport vs the same index computed downwind
    dm, pv = E.diebold_mariano(abl_preds, "A2 + fire + transport (ours)", "A3 + fire + downwind placebo")
    abl.loc[(abl.ablation == "A2 + fire + transport (ours)") & (abl.subset == "all"), ["dm_vs_placebo", "dm_vs_placebo_p"]] = [dm, pv]
    abl.to_csv(os.path.join(res_dir, "ablation.csv"), index=False)

    # 4. permutation importance (gradient boosting, held-out season with the most exceedances)
    ex_season = df.groupby("season").y_exceed_next.sum().idxmax()
    tr, te = df.season != ex_season, df.season == ex_season
    gb = make_models(quick)["gradient_boosting"].fit(df.loc[tr, full].values, df.loc[tr, "y_pm25_next"].values)
    pi = permutation_importance(gb, df.loc[te, full].values, df.loc[te, "y_pm25_next"].values,
                                scoring="neg_root_mean_squared_error", n_repeats=3 if quick else 10, random_state=0, n_jobs=-1)
    imp = pd.DataFrame({"feature": full, "importance_rmse_increase": pi.importances_mean, "std": pi.importances_std}) \
            .sort_values("importance_rmse_increase", ascending=False)
    imp.to_csv(os.path.join(res_dir, f"permutation_importance_season_{ex_season}.csv"), index=False)

    # 5. HMS cross-check: is observed exceedance more frequent under denser analyst-drawn smoke?
    h = df.dropna(subset=["hms_density_t"])
    hms_tab = h.groupby("hms_density_t").agg(county_days=("y_pm25_next", "size"), mean_pm25_t=("pm25_t", "mean"),
                                             exceed_rate_t=("pm25_t", lambda s: (s > C.EXCEEDANCE).mean())).reset_index()
    hms_tab.to_csv(os.path.join(res_dir, "hms_crosscheck.csv"), index=False)

    # figures
    m = metrics[(metrics.protocol == "leave_season_out") & (metrics.subset == "all")].sort_values("rmse")
    fig, ax = plt.subplots(figsize=(6, 3.2))
    err = np.array([m.rmse - m.rmse_ci_low, m.rmse_ci_high - m.rmse]).clip(0)
    ax.barh(m.model, m.rmse, xerr=err, color="#3b6ea5"); ax.invert_yaxis()
    ax.set_xlabel("RMSE, next-day county PM2.5 (µg/m³)"); ax.set_title("Leave-one-season-out, 95% day-bootstrap CI")
    fig.tight_layout(); fig.savefig(os.path.join(fig_dir, "model_comparison.png"), dpi=200); plt.close(fig)

    a = abl[abl.subset == "smoke_context"]
    fig, ax = plt.subplots(figsize=(6, 3.2))
    ax.barh(a.ablation, a.rmse, color=["#9aa5b1", "#9aa5b1", "#d1603d", "#9aa5b1", "#d1603d", "#9aa5b1"][:len(a)]); ax.invert_yaxis()
    ax.set_xlabel("RMSE on smoke-context days (µg/m³)"); ax.set_title("Feature ablation (gradient boosting)")
    fig.tight_layout(); fig.savefig(os.path.join(fig_dir, "ablation.png"), dpi=200); plt.close(fig)

    top = imp.head(15)
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.barh(top.feature, top.importance_rmse_increase, xerr=top["std"], color="#3b6ea5"); ax.invert_yaxis()
    ax.set_xlabel("RMSE increase when permuted (µg/m³)"); ax.set_title(f"Permutation importance, held-out {ex_season}")
    fig.tight_layout(); fig.savefig(os.path.join(fig_dir, "permutation_importance.png"), dpi=200); plt.close(fig)

    pp = p[p.season == ex_season]
    worst = pp.groupby("county_fips").y_pm25_next.max().idxmax()
    s = pp[pp.county_fips == worst].sort_values("date")
    fig, ax = plt.subplots(figsize=(7, 3))
    ax.plot(pd.to_datetime(s.date), s.y_pm25_next, color="black", lw=1.5, label="observed")
    ax.plot(pd.to_datetime(s.date), s.pred_persistence, color="#9aa5b1", lw=1, label="persistence")
    ax.plot(pd.to_datetime(s.date), s.pred_gradient_boosting, color="#d1603d", lw=1.2, label="gradient boosting")
    ax.axhline(C.EXCEEDANCE, ls="--", color="grey", lw=0.8); ax.set_ylabel("PM2.5 (µg/m³)")
    ax.set_title(f"County {worst}, held-out season {ex_season}"); ax.legend(frameon=False, fontsize=8)
    fig.tight_layout(); fig.savefig(os.path.join(fig_dir, "timeseries_worst_county.png"), dpi=200); plt.close(fig)

    print("\nModel comparison (leave-one-season-out, all days):")
    print(m[["model", "rmse", "rmse_ci_low", "rmse_ci_high", "mae", "r2", "exceed_f1", "exceed_auprc", "dm_p_value"]].round(3).to_string(index=False))
    print("\nAblation:"); print(abl[["ablation", "subset", "rmse", "exceed_f1", "dm_vs_A1", "dm_p_value", "dm_vs_placebo", "dm_vs_placebo_p"]].round(3).to_string(index=False))
    print("\nTop features:"); print(imp.head(8).round(3).to_string(index=False))
    print(f"\nDone in {time.time() - t_start:.0f}s -> {res_dir}, {fig_dir}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--quick", action="store_true")
    main(ap.parse_args().quick)
