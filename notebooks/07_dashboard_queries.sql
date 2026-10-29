-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 · Dashboard queries (Week 5 demo)
-- MAGIC Each cell is one dashboard tile. In Databricks: run a cell, click **+ → Visualization** on the result,
-- MAGIC then **Add to dashboard**. Or paste the queries into a Databricks SQL / AI-BI dashboard dataset.

-- COMMAND ----------

USE workspace.wildfire;

-- COMMAND ----------

-- Tile 1 · Statewide trend: fire activity vs average county PM2.5 (line chart, two y-axes)
SELECT date_local, ca_fire_detections, ca_fire_frp_mw, avg_county_pm25_ugm3, counties_over_standard
FROM gold_kpi_state_day ORDER BY date_local;

-- COMMAND ----------

-- Tile 2 · Worst 15 counties by days over the 24-hour PM2.5 standard (bar chart)
SELECT c.county_name,
       sum(CAST(f.pm25_exceeds_daily_standard AS INT)) AS days_over_35,
       round(max(f.pm25_mean_ugm3), 1) AS worst_day_pm25,
       sum(f.fire_detections) AS fires_in_county
FROM gold_fact_county_day f JOIN gold_dim_county c USING (county_fips)
GROUP BY c.county_name ORDER BY days_over_35 DESC, worst_day_pm25 DESC LIMIT 15;

-- COMMAND ----------

-- Tile 3 · Smoke indicator mix per study week (stacked bar)
SELECT d.study_week, f.smoke_indicator, count(*) AS county_days
FROM gold_fact_county_day f JOIN gold_dim_date d USING (date_local)
GROUP BY d.study_week, f.smoke_indicator ORDER BY d.study_week;

-- COMMAND ----------

-- Tile 4 · Does PM2.5 rise with nearby fire activity? (scatter, county-days with PM2.5 data)
SELECT fire_frp_within_100km_mw, pm25_mean_ugm3, wind_mean_ms, county_fips, date_local
FROM gold_fact_county_day WHERE has_pm25;

-- COMMAND ----------

-- Tile 5 · Lagged view: PM2.5 today vs nearby fire radiative power yesterday (smoke takes time to arrive)
SELECT f.county_fips, f.date_local, f.pm25_mean_ugm3,
       lag(f.fire_frp_within_100km_mw) OVER (PARTITION BY f.county_fips ORDER BY f.date_local) AS frp_100km_prev_day
FROM gold_fact_county_day f WHERE f.has_pm25;

-- COMMAND ----------

-- Tile 6 · Correlation summary for the report (same-day and previous-day)
WITH x AS (
  SELECT pm25_mean_ugm3 AS pm, fire_frp_within_100km_mw AS frp,
         lag(fire_frp_within_100km_mw) OVER (PARTITION BY county_fips ORDER BY date_local) AS frp_lag1, wind_mean_ms
  FROM gold_fact_county_day WHERE has_pm25)
SELECT round(corr(pm, frp), 3) AS corr_same_day, round(corr(pm, frp_lag1), 3) AS corr_prev_day,
       round(corr(pm, wind_mean_ms), 3) AS corr_wind, count(*) AS n_county_days FROM x;

-- COMMAND ----------

-- Tile 7 · Data coverage (quality panel)
SELECT date_local, sum(CAST(has_pm25 AS INT)) AS counties_with_pm25, sum(CAST(has_weather AS INT)) AS counties_with_weather,
       count(*) AS counties
FROM gold_fact_county_day GROUP BY date_local ORDER BY date_local;
