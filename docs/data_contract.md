# Data contract (Week 1 deliverable)

Study area: California (state FIPS 06, bounding box -124.5, 32.5, -114.1, 42.0).
Study window: 15 Aug – 30 Sep 2020 (local California dates), replayed one day per batch.
Time standard: all timestamps stored in UTC; analytical key uses the local date (America/Los_Angeles).
Spatial key: 5-digit county FIPS (`06xxx`), 58 counties.

## Sources

| Source | Endpoint | Format | One file = | Key needed |
| --- | --- | --- | --- | --- |
| NASA FIRMS VIIRS S-NPP (standard product) | `firms.modaps.eosdis.nasa.gov/api/area/csv/{KEY}/VIIRS_SNPP_SP/{bbox}/1/{date}` | CSV | 1 UTC day | MAP_KEY |
| EPA AQS daily PM2.5 (param 88101) | `aqs.epa.gov/data/api/dailyData/byState?param=88101&state=06&bdate=&edate=` | JSON | 1 local day | email + key |
| EPA AQS bulk file (benchmark only) | `aqs.epa.gov/aqsweb/airdata/daily_88101_2020.zip` | CSV | 1 year, US | none |
| NOAA CDO GHCN-Daily | `ncei.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&locationid=FIPS:06&units=metric` | JSON (paged, 1,000 rows) | 1 day | token |
| NOAA stations | `.../v2/stations?datasetid=GHCND&locationid=FIPS:06` | JSON | reference | token |
| Census 2020 county gazetteer | `www2.census.gov/.../2020_Gaz_counties_national.zip` | TSV → CSV | reference | none |

## Imported columns

**FIRMS:** latitude, longitude, bright_ti4, scan, track, acq_date, acq_time (HHMM UTC, leading zeros dropped), satellite,
instrument, confidence (l / n / h), version, bright_ti5, frp (MW), daynight.

**EPA AQS dailyData:** state_code, county_code, site_number, parameter_code, poc, latitude, longitude, sample_duration,
pollutant_standard, date_local, event_type, observation_count, observation_percent, validity_indicator, arithmetic_mean (µg/m³),
first_max_value, aqi, method_code, local_site_name, date_of_last_change (+ the remaining fields, kept in Bronze).

**NOAA GHCN-Daily:** date, datatype (TMAX, TMIN °C; PRCP mm; AWND, WSF2 m/s; WDF2 degrees), station, attributes
(`mflag,qflag,sflag,time`), value. Stations: id, name, latitude, longitude, elevation.

**Gazetteer:** USPS, GEOID, NAME, ALAND_SQMI, INTPTLAT, INTPTLONG.

## Metadata added in Bronze
`_source`, `_source_file`, `_data_date` (day the data is for), `_ingested_at_utc`, `_batch_id`, `_content_hash` (SHA-256 of the file).

## Business keys (deduplication and MERGE)

| Silver table | Key | Which version wins |
| --- | --- | --- |
| silver_fire_detections | latitude, longitude, acq_ts_utc, satellite | latest ingestion |
| silver_pm25_daily | site_id, poc, date_local, sample_duration | not "Excluded", then newest `date_of_last_change`, then latest ingestion |
| silver_weather_obs | station, date_local, element | latest ingestion, only if the value changed |
| gold_fact_county_day | county_fips, date_local | updated only if a measure changed |

## Validation rules (failing rows → `silver_quarantine` with the reason)

| Source | Reason code | Rule |
| --- | --- | --- |
| FIRMS | missing_coordinates | latitude and longitude present |
| FIRMS | outside_study_area | inside the California bounding box |
| FIRMS | bad_timestamp | acq_date + acq_time parse to a timestamp |
| FIRMS | frp_out_of_range | 0 ≤ FRP ≤ 20,000 MW (or missing) |
| FIRMS | unknown_confidence | confidence maps to low / nominal / high |
| EPA | wrong_parameter | parameter_code = 88101 |
| EPA | wrong_state | state_code = 06 |
| EPA | bad_date, missing_value | date_local parses; arithmetic_mean present |
| EPA | impossible_value | 0 ≤ PM2.5 ≤ 1,000 µg/m³ |
| EPA | low_completeness | observation_percent ≥ 75 % |
| NOAA | unneeded_element | element in TMAX, TMIN, PRCP, AWND, WDF2, WSF2 |
| NOAA | failed_source_qc | NOAA quality flag (qflag) is empty |
| NOAA | impossible_value | −40…60 °C; 0…1,000 mm; wind 0…75 m/s; direction 0…360° |
| NOAA | unmatched_station | station exists in the stations reference |

## Gold fact table columns (`gold_fact_county_day`)
county_fips, date_local, date_key, fire_detections, fire_detections_high_conf, fire_frp_total_mw, fire_frp_max_mw,
fire_detections_within_100km, fire_frp_within_100km_mw, pm25_mean_ugm3, pm25_max_site_ugm3, aqi_max, pm25_sites,
tmax_c, tmin_c, prcp_mm, wind_mean_ms, wind_dir_deg, weather_stations, pm25_exceeds_daily_standard, smoke_indicator,
has_pm25, has_weather, sources_present, _gold_built_at_utc.
