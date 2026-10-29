# What to do each week (checklist)

| Week | Owner | Tasks | Done when |
| --- | --- | --- | --- |
| 1 | Nikhil | Get FIRMS, AQS, NOAA keys; run `download_sources.py` for 15–21 Aug; check files open | 3 keys + 7 days of files per source |
| 2 | Nino | Import notebooks; run `01_setup`, upload files, run `02_bronze`; check `ops_ingest_manifest` | Bronze tables with metadata; rerun skips files |
| 3 | Nino | Run `03_silver`; read quarantine reasons; tune thresholds if needed | Silver + quarantine tables; dedup works |
| 4 | Nithish | Download the full window; daily replay 02 → 04; run `04_gold` | `gold_fact_county_day` with 58 × 47 rows |
| 5 | Nithish, Ragavedhni | Run 05 + 06; build the dashboard from 07; fill DQ + benchmark templates | Numbers in `dq_summary`, `bench_results`; dashboard screenshot |
| 6 | Ragavedhni, all | Final report from the template, slides, code clean-up, rehearse | Submission |

Before the intermediate report deadline, the minimum to show real progress: steps for Weeks 1–2 on the first 7 days
(a few hours of work), then paste real row counts into Section 2 of the intermediate report.
