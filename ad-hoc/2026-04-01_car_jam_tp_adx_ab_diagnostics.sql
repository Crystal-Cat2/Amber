-- execution_path = local_bigquery
-- Diagnostics:
-- 1. overlap users between A/B windows
-- 2. MAX rows that match multiple windows
-- 3. TradPlus unit ids not covered by current mapping

DECLARE start_date DATE DEFAULT DATE '2026-01-27';
DECLARE end_date DATE DEFAULT CURRENT_DATE('UTC');

WITH experiment_events AS (
  SELECT
    'ios_car_jam' AS product,
    user_id,
    event_timestamp,
    (
      SELECT ep.value.string_value
      FROM UNNEST(event_params.array) AS ep
      WHERE ep.key = 'group'
      LIMIT 1
    ) AS raw_group
  FROM `transferred.hudi_ods.ios_car_jam`
  WHERE event_date BETWEEN FORMAT_DATE('%Y-%m-%d', start_date) AND FORMAT_DATE('%Y-%m-%d', end_date)
    AND event_name = 'lib_tpx_group'
    AND user_id IS NOT NULL

  UNION ALL

  SELECT
    'car_jam' AS product,
    user_id,
    event_timestamp,
    (
      SELECT ep.value.string_value
      FROM UNNEST(event_params.array) AS ep
      WHERE ep.key = 'group'
      LIMIT 1
    ) AS raw_group
  FROM `transferred.hudi_ods.car_jam`
  WHERE event_date BETWEEN FORMAT_DATE('%Y-%m-%d', start_date) AND FORMAT_DATE('%Y-%m-%d', end_date)
    AND event_name = 'lib_tpx_group'
    AND user_id IS NOT NULL
),
experiment_windows AS (
  SELECT
    product,
    user_id,
    CASE raw_group
      WHEN 'A' THEN 'no_tp_adx'
      WHEN 'B' THEN 'have_tp_adx'
      ELSE NULL
    END AS experiment_group,
    MIN(event_timestamp) AS min_timestamp,
    MAX(event_timestamp) AS max_timestamp
  FROM experiment_events
  WHERE raw_group IN ('A', 'B')
  GROUP BY 1, 2, 3
),
window_overlap AS (
  SELECT
    a.product,
    COUNT(DISTINCT a.user_id) AS overlap_user_cnt,
    COUNT(*) AS overlap_pair_cnt
  FROM experiment_windows AS a
  JOIN experiment_windows AS b
    ON a.product = b.product
   AND a.user_id = b.user_id
   AND a.experiment_group < b.experiment_group
   AND a.min_timestamp <= b.max_timestamp
   AND b.min_timestamp <= a.max_timestamp
  GROUP BY 1
),
max_source AS (
  SELECT
    'ios_car_jam' AS product,
    ROW_NUMBER() OVER () AS source_row_id,
    User_ID AS user_id,
    UNIX_MICROS(`Date`) AS event_timestamp
  FROM `gpdata-224001.applovin_max.ios_car_jam_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
    AND User_ID IS NOT NULL
    AND `Date` >= TIMESTAMP(start_date)
    AND `Date` < TIMESTAMP(DATE_ADD(end_date, INTERVAL 1 DAY))

  UNION ALL

  SELECT
    'car_jam' AS product,
    ROW_NUMBER() OVER () AS source_row_id,
    User_ID AS user_id,
    UNIX_MICROS(`Date`) AS event_timestamp
  FROM `gpdata-224001.applovin_max.car_jam_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
    AND User_ID IS NOT NULL
    AND `Date` >= TIMESTAMP(start_date)
    AND `Date` < TIMESTAMP(DATE_ADD(end_date, INTERVAL 1 DAY))
),
max_multi_match AS (
  SELECT
    s.product,
    COUNT(*) AS multi_match_event_cnt
  FROM (
    SELECT
      s.product,
      s.source_row_id,
      COUNT(*) AS matched_window_cnt
    FROM max_source AS s
    JOIN experiment_windows AS w
      ON s.product = w.product
     AND s.user_id = w.user_id
     AND s.event_timestamp BETWEEN w.min_timestamp AND w.max_timestamp
    GROUP BY 1, 2
  ) AS s
  WHERE s.matched_window_cnt > 1
  GROUP BY 1
),
tradplus_unmapped AS (
  SELECT
    product,
    unit_id,
    COUNT(*) AS row_cnt
  FROM (
    SELECT
      'ios_car_jam' AS product,
      log.unit_id AS unit_id
    FROM `transferred.dw_tridplus.ios_car_jam`
    WHERE SAFE_CAST(log.date AS DATE) BETWEEN start_date AND end_date

    UNION ALL

    SELECT
      'car_jam' AS product,
      log.unit_id AS unit_id
    FROM `transferred.dw_tridplus.car_jam`
    WHERE SAFE_CAST(log.date AS DATE) BETWEEN start_date AND end_date
  )
  WHERE unit_id NOT IN (
    '27CA56AF5AFB21D0423F222AE5EC917',
    '1B6BA61AE8908E9301F02397B8ABB034',
    'F87628C46300295E9DBC69D7253C9F66',
    '141AAA94861AAAD60646407B7E2AC3DE',
    '6F2643A90A53A1DAF244609655F8C8B1',
    '39CF840361B8BCFD305B8F5B1B15873C',
    'E0814496DC2C85045C260802234F9C72',
    '039C8CE8B25200C0A6831DCF11626050',
    '8092A3EE0CE75B60801F9B1A7375F626',
    'B5E74925A701E44E898C80344446EAE9',
    '6AA798CDB3034C2A04FCD480D02C1AF4',
    '436AAF3C5A73773954258B0F47720703',
    'DF12D193E468FCB5A167C9E1A7381584',
    'DD50A085140081615F536633B74F4E96',
    '99E8CD4D880F2D54EB30D41EEF559BA8',
    'B05BC48C4A5B745DA7C3A35147A1457E',
    'EF12CA4404CAD5B4F1EB4026546DB845',
    '346B68A2377DC03C39E4F5B4056C3BE8'
  )
  GROUP BY 1, 2
)
SELECT
  'window_overlap' AS check_type,
  product,
  CAST(NULL AS STRING) AS unit_id,
  overlap_user_cnt AS metric_1,
  overlap_pair_cnt AS metric_2
FROM window_overlap

UNION ALL

SELECT
  'max_multi_match' AS check_type,
  product,
  CAST(NULL AS STRING) AS unit_id,
  multi_match_event_cnt AS metric_1,
  CAST(NULL AS INT64) AS metric_2
FROM max_multi_match

UNION ALL

SELECT
  'tradplus_unmapped_unit' AS check_type,
  product,
  unit_id,
  row_cnt AS metric_1,
  CAST(NULL AS INT64) AS metric_2
FROM tradplus_unmapped
ORDER BY check_type, product, unit_id;
