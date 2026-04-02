-- execution_path = local_bigquery
-- Query goal:
-- 1. Use lib_tpx_group.group to split car_jam AB on both platforms.
-- 2. Attribution uses cycle-level min/max windows, not date equality.
-- 3. Output base table: date, product, ad_format, experiment_group, network, source, impression, revenue, ecpm.
-- 4. Keep both MAX-derived TradPlus_ADX and external TradPlus rows; final markdown keeps only external TradPlus.

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
    MAX(event_timestamp) AS max_timestamp,
    MAX(event_timestamp) - MIN(event_timestamp) AS window_span_us
  FROM experiment_events
  WHERE raw_group IN ('A', 'B')
  GROUP BY 1, 2, 3
),
max_source AS (
  SELECT
    'ios_car_jam' AS product,
    ROW_NUMBER() OVER () AS source_row_id,
    DATE(`Date`, 'UTC') AS date,
    User_ID AS user_id,
    UNIX_MICROS(`Date`) AS event_timestamp,
    UPPER(Ad_Format) AS ad_format,
    CASE
      WHEN LOWER(Ad_placement) LIKE 'tradplusadx%' THEN 'TradPlus_ADX'
      ELSE Network
    END AS network,
    1 AS impression,
    Revenue AS revenue,
    'max' AS source
  FROM `gpdata-224001.applovin_max.ios_car_jam_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
    AND User_ID IS NOT NULL
    AND `Date` >= TIMESTAMP(start_date)
    AND `Date` < TIMESTAMP(DATE_ADD(end_date, INTERVAL 1 DAY))
    AND Revenue IS NOT NULL

  UNION ALL

  SELECT
    'car_jam' AS product,
    ROW_NUMBER() OVER () AS source_row_id,
    DATE(`Date`, 'UTC') AS date,
    User_ID AS user_id,
    UNIX_MICROS(`Date`) AS event_timestamp,
    UPPER(Ad_Format) AS ad_format,
    CASE
      WHEN LOWER(Ad_placement) LIKE 'tradplusadx%' THEN 'TradPlus_ADX'
      ELSE Network
    END AS network,
    1 AS impression,
    Revenue AS revenue,
    'max' AS source
  FROM `gpdata-224001.applovin_max.car_jam_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
    AND User_ID IS NOT NULL
    AND `Date` >= TIMESTAMP(start_date)
    AND `Date` < TIMESTAMP(DATE_ADD(end_date, INTERVAL 1 DAY))
    AND Revenue IS NOT NULL
),
matched_max AS (
  SELECT
    s.date,
    s.product,
    s.ad_format,
    w.experiment_group,
    s.network,
    s.source,
    s.impression,
    s.revenue
  FROM max_source AS s
  JOIN experiment_windows AS w
    ON s.product = w.product
   AND s.user_id = w.user_id
   AND s.event_timestamp BETWEEN w.min_timestamp AND w.max_timestamp
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.product, s.source, s.source_row_id
    ORDER BY w.window_span_us ASC, w.min_timestamp DESC, w.max_timestamp DESC, w.experiment_group DESC
  ) = 1
),
tradplus_external AS (
  SELECT
    date,
    product,
    ad_format,
    'have_tp_adx' AS experiment_group,
    'TradPlus_ADX' AS network,
    'tradplus_external' AS source,
    SUM(impression) AS impression,
    SUM(revenue) AS revenue
  FROM (
    SELECT
      date,
      CASE
        WHEN appId = 'BEC3799A1CB6EE4D97BF451CB4EB8408' THEN 'ios_car_jam'
        WHEN appId = '4925CFAA3FA0144A611342E2076552BA' THEN 'car_jam'
        ELSE NULL
      END AS product,
      CASE
        WHEN placementId IN (
          '27CA56AF5AFB21D0423F222AE5EC917',
          '1B6BA61AE8908E9301F02397B8ABB034',
          'F87628C46300295E9DBC69D7253C9F66',
          'B5E74925A701E44E898C80344446EAE9',
          '6AA798CDB3034C2A04FCD480D02C1AF4',
          '436AAF3C5A73773954258B0F47720703'
        ) THEN 'BANNER'
        WHEN placementId IN (
          '141AAA94861AAAD60646407B7E2AC3DE',
          '6F2643A90A53A1DAF244609655F8C8B1',
          '39CF840361B8BCFD305B8F5B1B15873C',
          'DF12D193E468FCB5A167C9E1A7381584',
          'DD50A085140081615F536633B74F4E96',
          '99E8CD4D880F2D54EB30D41EEF559BA8'
        ) THEN 'INTER'
        WHEN placementId IN (
          'E0814496DC2C85045C260802234F9C72',
          '039C8CE8B25200C0A6831DCF11626050',
          '8092A3EE0CE75B60801F9B1A7375F626',
          'B05BC48C4A5B745DA7C3A35147A1457E',
          'EF12CA4404CAD5B4F1EB4026546DB845',
          '346B68A2377DC03C39E4F5B4056C3BE8'
        ) THEN 'REWARD'
        ELSE 'UNKNOWN'
      END AS ad_format,
      COALESCE(SAFE_CAST(impressionApi AS INT64), SAFE_CAST(impression AS INT64), 0) AS impression,
      COALESCE(SAFE_CAST(Revenue AS FLOAT64), 0.0) AS revenue
    FROM `transferred.dw_external_data.external_ad_tradplus_data`
    WHERE date BETWEEN start_date AND end_date
      AND appId IN ('BEC3799A1CB6EE4D97BF451CB4EB8408', '4925CFAA3FA0144A611342E2076552BA')
  )
  WHERE product IS NOT NULL
    AND ad_format != 'UNKNOWN'
  GROUP BY 1, 2, 3, 4, 5, 6
),
combined AS (
  SELECT * FROM matched_max
  UNION ALL
  SELECT * FROM tradplus_external
)

SELECT
  date,
  product,
  ad_format,
  experiment_group,
  network,
  source,
  SUM(impression) AS impression,
  SUM(revenue) AS revenue,
  SAFE_DIVIDE(SUM(revenue) * 1000.0, NULLIF(SUM(impression), 0)) AS ecpm
FROM combined
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY date, product, ad_format, experiment_group, network, source;
