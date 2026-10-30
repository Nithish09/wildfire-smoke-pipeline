# California Wildfire Smoke Data Pipeline – Group 8

PySpark + Delta Lake pipeline on Databricks that turns NASA FIRMS fire detections, EPA AQS PM2.5 and NOAA GHCN-Daily
weather into a county-day Gold table (Bronze → Silver → Gold, incremental, with quality checks).

> **Status of this repository:** the code is written and its logic is tested on *synthetic* data (`tests/`).
> It has **not yet been run on real data in Databricks**. Every number in a report must come from your own Databricks runs.

## What is in here

| Path | What it is | Week |
| --- | --- | --- |
| `docs/data_contract.md` | Sources, endpoints, imported columns, keys, validation rules | 1 |
| `tools/download_sources.py` | Downloads the real raw files (one per source per day) to your laptop | 1–2 |
| `notebooks/00_config.py` | Shared settings (catalog, paths, study window, thresholds) | 2 |
| `notebooks/01_setup.py` | Creates schema, volume, manifest and log tables (run once) | 2 |
| `notebooks/02_bronze.py` | Lands raw files as Delta with metadata; skips files already loaded | 2 |
| `notebooks/03_silver.py` | Cast, validate, quarantine, deduplicate, MERGE; county assignment | 3 |
| `notebooks/04_gold.py` | Star schema + `gold_fact_county_day`, incremental MERGE | 4 |
| `notebooks/05_data_quality.py` | Numbers for the data-quality report | 5 |
| `notebooks/06_benchmark_and_rerun.py` | Step timings, CSV vs API, rerun test, late-data test | 5 |
| `notebooks/07_dashboard_queries.sql` | Dashboard tiles | 5 |
| `tools/make_synthetic_data.py`, `tests/` | Synthetic test data + pandas reference test (23 checks) | any |
| `docs/*_template.md` | Report, benchmark, data-quality and slide templates with `[FILL]` slots | 5–6 |

## How to run it (step by step)

> **Just want to run the pipeline?** The first week of real data (15–21 Aug 2020) is already in `data/raw/`.
> Skip to step 4 and upload those folders. Steps 1–2 are only needed to download more days.

**1. Get your own API keys (10 minutes, free; each person uses their own)**

| Variable | What it is | Where to get it |
| --- | --- | --- |
| `FIRMS_MAP_KEY` | NASA FIRMS map key (32 characters) | https://firms.modaps.eosdis.nasa.gov/api/map_key/ |
| `AQS_EMAIL` | The email you signed up to EPA AQS with | Your email address |
| `AQS_KEY` | EPA AQS key (arrives by email) | Open `https://aqs.epa.gov/data/api/signup?email=YOUR_EMAIL` in a browser |
| `NOAA_TOKEN` | NOAA Climate Data Online token (arrives by email) | https://www.ncdc.noaa.gov/cdo-web/token |

`AQS_EMAIL` must be exactly the address you signed up with, or EPA answers `HTTP 400`. Only the newest EPA key works.

**2. Put the keys in a `.env` file (one time)**

From the project folder:
```bash
cp .env.example .env     # make your private copy of the template
open -e .env             # Mac (TextEdit). Windows: notepad .env
```
Fill in the values (no quotes, no spaces) and save:
```
FIRMS_MAP_KEY=your_nasa_key
AQS_EMAIL=you@sjsu.edu
AQS_KEY=your_epa_key
NOAA_TOKEN=your_noaa_token
```
`.env` is in `.gitignore`, so it is never committed. **Never paste keys into code, notebooks, issues or the report.**
The downloader reads `.env` automatically; a variable you `export` in the terminal takes priority over the file.

**3. Download the raw data on a laptop**
```bash
pip3 install requests      # Mac: use pip3 / python3
python3 tools/download_sources.py --start 2020-08-15 --end 2020-08-21 --out raw --skip-bulk   # first week, quick test
python3 tools/download_sources.py --start 2020-08-15 --end 2020-09-30 --out raw               # full window + EPA bulk CSV
```
The EPA API is slow on purpose (5 s pause per call), so the full window takes roughly 10–20 minutes.
Re-running skips files you already have. The downloader removes your email and key from the EPA files before saving them.

| Error | Cause | Fix |
| --- | --- | --- |
| `Missing: ...` | A key is empty in `.env` | Fill it in and save `.env` |
| EPA `HTTP 400` | Email and key don't match | Check `AQS_EMAIL`; request a new key if needed |
| `command not found: python` (Mac) | Mac uses `python3` | Use `python3` / `pip3` |
| `No matching distribution found for request` | Typo | The package is `requests` |

**4. Databricks Free Edition**
1. Workspace → *Import* → upload the files in `notebooks/` (they are Databricks source format).
2. Run `01_setup`.
3. Catalog → `workspace` → `wildfire` → volume `raw` → *Upload* the contents of your local `raw/` folder (or the repo's `data/raw/`)
   (`firms/`, `epa/`, `noaa/`, `reference/`, `epa_bulk/`).
4. Run `02_bronze` → `03_silver` → `04_gold`. Then `05_data_quality` and `06_benchmark_and_rerun`.
5. To simulate *daily velocity*, upload one day of files at a time and rerun 02 → 04 after each upload,
   or run 02 with the widget `only_date = 2020-08-15`, then `2020-08-16`, …
6. Optional: *Workflows → Create job* with tasks 02 → 03 → 04 to show a scheduled daily pipeline.

**5. Test the logic without Databricks**
```bash
pip install pandas numpy tabulate
python tests/run_local_test.py        # builds synthetic data, runs 23 checks, writes tests/local_test_report.md
```

## Research extension (ML / DL, toward publication)
`research/` forecasts next-day county PM2.5 and exceedances over four fire seasons (2018–2021) with a wind-aware fire-transport
index, classical ML and deep learning (GRU, wind-graph ST-GNN). See `research/README.md` to run it and `docs/paper_draft.md` for the
paper draft. Status: tested on synthetic data only; real-data results pending.

## Tables produced

| Layer | Table | Grain |
| --- | --- | --- |
| Ops | `ops_ingest_manifest`, `ops_step_log`, `ops_watermark`, `ops_gold_pending_dates` | file / step / source / date |
| Bronze | `bronze_firms`, `bronze_epa_pm25`, `bronze_noaa_daily`, `bronze_ref_*` | raw record (all strings + metadata) |
| Silver | `silver_fire_detections`, `silver_pm25_daily`, `silver_weather_obs`, `silver_weather_daily`, `silver_dim_county`, `silver_quarantine` | detection / site-day / station-day-element |
| Gold | `gold_dim_county`, `gold_dim_date`, `gold_fact_county_day`, `gold_kpi_state_day` | county-day / state-day |
| DQ / bench | `dq_summary`, `bench_results` | metric / test |

## Known limitations (state them in the report)
* Points are assigned to the county with the **nearest internal point**, not by polygon; near borders this can be wrong
  (`05_data_quality` counts detections > 60 km from their county centre).
* `smoke_indicator` is descriptive (PM2.5 > 35 µg/m³ with fire within 100 km). It is not a causal or regulatory attribution.
* PM2.5 monitors are sparse; many county-days have no PM2.5 (shown as NULL, not 0).
* EPA AQS has no real-time data; "daily velocity" is a replay of 2020.

## AI disclosure
Parts of this code and documentation were drafted with an AI assistant (Claude) and reviewed by the team.
