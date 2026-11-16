-- Run once after 01_setup.py and before 02_load_bronze_v2.py or
-- 03s_silver_stream.py. These operational tables are intentionally created
-- in SQL, not by the Python notebooks.

CREATE TABLE IF NOT EXISTS workspace.wildfire.log (
  `timestamp` TIMESTAMP NOT NULL,
  log_severity STRING NOT NULL,
  source_file STRING NOT NULL,
  log_message STRING NOT NULL
) USING DELTA;

INSERT INTO workspace.wildfire.log
SELECT current_timestamp(), 'INFO', '08_streaming_algorithm_tables',
       'Streaming algorithm table setup started.';

CREATE TABLE IF NOT EXISTS workspace.wildfire.ops_stream_bronze_metrics (
  metric_ts_utc TIMESTAMP,
  source STRING,
  batch_id STRING,
  rows_written LONG,
  seconds DOUBLE,
  files_seen LONG,
  details_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.wildfire.ops_stream_metrics (
  metric_ts_utc TIMESTAMP,
  source STRING,
  batch_id STRING,
  algorithm STRING,
  metric_name STRING,
  metric_value DOUBLE,
  details_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.wildfire.ops_stream_state (
  state_key STRING,
  algorithm STRING,
  buckets_json STRING,
  last_event_ts BIGINT,
  window_seconds BIGINT,
  max_buckets INT,
  updated_at_utc TIMESTAMP
) USING DELTA;

INSERT INTO workspace.wildfire.log
SELECT current_timestamp(), 'INFO', '08_streaming_algorithm_tables',
       'Streaming algorithm metric and state tables are ready.';

-- Reporting view: preserve append-only history above, but return only the
-- latest final-design metrics per source and algorithm. Legacy source labels
-- are mapped to the current labels for clean reporting.
CREATE OR REPLACE VIEW workspace.wildfire.v_ops_stream_metrics_latest AS
WITH normalized AS (
  SELECT
    CASE source
      WHEN 'firms' THEN 'nasa_firms'
      WHEN 'noaa_weather' THEN 'noaa_cdo'
      ELSE source
    END AS source,
    batch_id,
    algorithm,
    metric_name,
    metric_value,
    metric_ts_utc,
    details_json
  FROM workspace.wildfire.ops_stream_metrics
  WHERE algorithm IN ('bloom_filter', 'dgim')
    AND metric_name IN (
      'incoming_rows', 'existing_silver_keys', 'candidate_duplicate_hits',
      'exact_existing_key_hits', 'exact_new_rows', 'false_positive_count',
      'false_negative_count', 'measured_false_positive_rate',
      'estimated_alerts_24h', 'exact_alerts_24h', 'absolute_error',
      'relative_error', 'bucket_count', 'late_events_in_batch', 'state_rebuilt'
    )
), latest AS (
  SELECT source, algorithm, max(metric_ts_utc) AS latest_ts
  FROM normalized
  GROUP BY source, algorithm
)
SELECT n.source, n.algorithm, n.batch_id, n.metric_name, n.metric_value,
       n.metric_ts_utc, n.details_json
FROM normalized n
JOIN latest l
  ON n.source = l.source
 AND n.algorithm = l.algorithm
 AND n.metric_ts_utc = l.latest_ts;

INSERT INTO workspace.wildfire.log
SELECT current_timestamp(), 'INFO', '08_streaming_algorithm_tables',
       'Latest streaming metrics view is ready.';
