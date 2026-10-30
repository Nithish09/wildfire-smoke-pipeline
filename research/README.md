# Research extension: next-day wildfire-smoke PM2.5 forecasting (ML + DL)

**Question.** Can freely available satellite fire detections plus a simple *wind-aware fire-transport index* forecast
next-day county PM2.5 and exceedances of the 35 µg/m³ standard in California? Does this skill hold for an **unseen fire season**
and for **unseen regions**? Does the index work *because of* transport, which we test by also computing it **downwind** as a placebo?

Data: 2018–2021 fire seasons (1 Jul – 30 Nov): EPA AirData PM2.5, NASA FIRMS VIIRS, NOAA GHCN-Daily wind and weather,
NOAA HMS analyst smoke polygons, Census counties. Models: persistence, climatology, ridge, random forest,
gradient boosting, MLP, GRU, and a wind-graph spatio-temporal GNN.

> **Status.** Code tested end to end on *synthetic* data in the real file formats (`make_synthetic_research_data.py`)
> and the fire loader on the real FIRMS week in `data/raw/`. No real-data results yet. Do not report synthetic numbers.
> `train_dl.py` has not been executed yet (PyTorch was not available where it was written): run `--smoke-test` first.

## Run it (on your laptop; ~2–4 h total, mostly downloads)

```bash
pip3 install requests pandas numpy scipy scikit-learn matplotlib torch
# FIRMS_MAP_KEY must be in .env (see the main README)

python3 research/download_research_data.py --seasons 2020   # 1. one season first (~30-45 min)
python3 research/download_research_data.py                  #    then all four seasons (re-run skips existing files)
python3 research/build_dataset.py                           # 2. county-day feature table   (~5-15 min)
python3 research/train_ml.py --quick                        # 3a. fast check                (~2 min)
python3 research/train_ml.py                                # 3b. full ML experiments        (~15-40 min)
python3 research/train_dl.py --smoke-test                   # 4a. check the DL code runs     (~5 min)
python3 research/train_dl.py                                # 4b. full DL experiments        (~30-90 min on CPU, faster on Apple MPS/GPU)
```

Downloads go to `research_data/` and results to `research_out/` (both git-ignored; override with `RESEARCH_DATA` / `RESEARCH_OUT`).

## Outputs → paper

| File (`research_out/`) | Paper item |
| --- | --- |
| `results/model_comparison.csv`, `figures/model_comparison.png` | Table 2, Fig. 2: models under leave-one-season-out and leave-one-region-out |
| `results/dl_comparison.csv` | Table 2 (DL rows) |
| `results/ablation.csv`, `figures/ablation.png` | Table 3, Fig. 3: feature ablation incl. downwind placebo |
| `results/permutation_importance_season_*.csv`, `figures/permutation_importance.png` | Fig. 4 |
| `figures/timeseries_worst_county.png` | Fig. 5: case study |
| `results/hms_crosscheck.csv` | Table 4: PM2.5 vs independent HMS smoke density |

## Files

| File | What it does |
| --- | --- |
| `config.py` | Seasons, bounding box, thresholds, paths |
| `download_research_data.py` | Downloads all sources (keyless except FIRMS) |
| `hms.py` | Dependency-free HMS shapefile reader + point-in-polygon |
| `build_dataset.py` | County-day features and next-day targets (no look-ahead: features ≤ day t, target t+1) |
| `evaluate.py` | Splits, metrics, day-block bootstrap CIs, Diebold–Mariano test |
| `train_ml.py` | Classical ML, ablations, importance, HMS cross-check, figures |
| `dl_data.py`, `train_dl.py` | Panel tensors, wind-aware county graph, GRU and ST-GNN |
| `make_synthetic_research_data.py` | Synthetic data in the real formats, for testing only |
