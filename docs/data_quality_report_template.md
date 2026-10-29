# Data-Quality Report – Group 8
Source of every number: `notebooks/05_data_quality.py` (table `dq_summary`, run at [FILL: run_at_utc]).

## 1 Completeness
| Source | Bronze rows | Quarantined | % passing | Duplicates / superseded | Silver rows |
| --- | --- | --- | --- | --- | --- |
| NASA FIRMS | [FILL] | [FILL] | [FILL] | [FILL] | [FILL] |
| EPA AQS | [FILL] | [FILL] | [FILL] | [FILL] | [FILL] |
| NOAA GHCN-Daily | [FILL] | [FILL] | [FILL] | [FILL] | [FILL] |

Note for EPA: AQS repeats each site-day once per pollutant standard (and twice more on exceptional-event days),
so a large "duplicates" count is expected and correct.

## 2 Quarantine reasons
| Source | Reason | Records | % of source quarantine |
| --- | --- | --- | --- |
| [FILL from the "Quarantine reasons" cell] | | | |

## 3 Null rates in Silver (%)
| Table | Column | % null |
| --- | --- | --- |
| silver_fire_detections | frp_mw, confidence_class, county_fips | [FILL] |
| silver_pm25_daily | aqi, observation_percent | [FILL] |
| silver_weather_daily | tmax_c, tmin_c, prcp_mm, awnd_ms, wdf2_deg | [FILL] |

## 4 Identifiers, spatial assignment and late data
* EPA county codes not in the county dimension: [FILL]
* Fire detections > 60 km from their assigned county centre: [FILL] (border-assignment risk)
* Files revised at the source (same name, new hash): [FILL]
* EPA values last changed > 30 days after measurement: [FILL]

## 5 Gold coverage
* Gold rows: [FILL]; duplicate keys: [FILL: must be 0]
* County-days with PM2.5: [FILL] %; with weather: [FILL] %

## 6 Sample-based correctness
Paste the 5-row check from the last cell of 05 (Gold fire count vs recount from Silver): [FILL: n/5 matched].
Manually compare 2 county-days against the source website (FIRMS map / AirNow / AQS) and describe the result: [FILL].
