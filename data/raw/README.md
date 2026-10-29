# Real raw data: first week (15–21 Aug 2020)

Downloaded on 29 Oct 2026 with `tools/download_sources.py`. These are the original source responses, one file per source per day.

| Folder | Source | Files | Records |
| --- | --- | --- | --- |
| firms/ | NASA FIRMS VIIRS S-NPP (SP), CSV, UTC days 15–22 Aug | 8 | 33,714 fire detections |
| epa/ | EPA AQS dailyData PM2.5 (88101), JSON | 7 | 7,457 records (includes repeats per pollutant standard) |
| noaa/ | NOAA CDO GHCN-Daily, JSON | 7 | 15,184 observations |
| reference/ | Census 2020 county gazetteer (CA) + NOAA GHCND stations | 2 | 58 counties, 1,423 stations |

The email and API key that EPA echoes in each file's `Header.url` were replaced with `REDACTED`. The `Data` records are unchanged.
To load into Databricks, upload each folder to `/Volumes/workspace/wildfire/raw/<folder>/`.
