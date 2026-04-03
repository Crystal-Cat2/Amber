-- 查询目的：
-- 统计 2026-01-05 到 2026-01-12 期间，Hudi adslog_filled 的时长分布，
-- 仅输出 experiment_group + product + ad_format + max_unit_id + 0.01 秒粒度的聚合结果。
--
-- 输出字段：
-- experiment_group, product, ad_format, max_unit_id, duration_sec_2dp, filled_pv, denominator_filled_pv

WITH app_base AS (
  SELECT
    'screw_puzzle' AS product,
    user_pseudo_id,
    event_timestamp,
    event_name,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      WHEN event_name LIKE 'inter%' THEN 'interstitial'
      WHEN event_name LIKE 'reward%' THEN 'rewarded'
      WHEN event_name LIKE 'banner%' THEN 'banner'
      ELSE 'unknown'
    END AS ad_format,
    CASE
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group') = 'A' THEN 'no_is_adx'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group') = 'B' THEN 'have_is_adx'
      ELSE NULL
    END AS experiment_group,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('max_unit_id', 'unit_id', 'sdk_unit_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('max_unit_id', 'unit_id', 'sdk_unit_id')) AS STRING)
    ) AS max_unit_id,
    ROUND(
      COALESCE(
        SAFE_CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'duration') AS FLOAT64),
        SAFE_CAST((SELECT value.double_value FROM UNNEST(event_params.array) WHERE key = 'duration') AS FLOAT64),
        SAFE_CAST((SELECT value.float_value FROM UNNEST(event_params.array) WHERE key = 'duration') AS FLOAT64),
        SAFE_CAST((SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'duration') AS FLOAT64)
      ) / 1000.0,
      2
    ) AS duration_sec_2dp
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('adslog_filled', 'lib_isx_group')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    user_pseudo_id,
    event_timestamp,
    event_name,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      WHEN event_name LIKE 'inter%' THEN 'interstitial'
      WHEN event_name LIKE 'reward%' THEN 'rewarded'
      WHEN event_name LIKE 'banner%' THEN 'banner'
      ELSE 'unknown'
    END AS ad_format,
    CASE
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group') = 'A' THEN 'no_is_adx'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group') = 'B' THEN 'have_is_adx'
      ELSE NULL
    END AS experiment_group,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('max_unit_id', 'unit_id', 'sdk_unit_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('max_unit_id', 'unit_id', 'sdk_unit_id')) AS STRING)
    ) AS max_unit_id,
    ROUND(
      COALESCE(
        SAFE_CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'duration') AS FLOAT64),
        SAFE_CAST((SELECT value.double_value FROM UNNEST(event_params.array) WHERE key = 'duration') AS FLOAT64),
        SAFE_CAST((SELECT value.float_value FROM UNNEST(event_params.array) WHERE key = 'duration') AS FLOAT64),
        SAFE_CAST((SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'duration') AS FLOAT64)
      ) / 1000.0,
      2
    ) AS duration_sec_2dp
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('adslog_filled', 'lib_isx_group')
),

group_ranges AS (
  SELECT
    product,
    user_pseudo_id,
    experiment_group,
    MIN(event_timestamp) AS group_start_ts,
    MAX(event_timestamp) AS group_end_ts
  FROM app_base
  WHERE event_name = 'lib_isx_group'
    AND experiment_group IN ('no_is_adx', 'have_is_adx')
  GROUP BY product, user_pseudo_id, experiment_group
),

filled_events AS (
  SELECT
    f.product,
    g.experiment_group,
    LOWER(COALESCE(f.ad_format, 'unknown')) AS ad_format,
    NULLIF(TRIM(f.max_unit_id), '') AS max_unit_id,
    f.duration_sec_2dp
  FROM app_base f
  JOIN group_ranges g
    ON f.product = g.product
   AND f.user_pseudo_id = g.user_pseudo_id
   AND f.event_timestamp BETWEEN g.group_start_ts AND g.group_end_ts
  WHERE f.event_name = 'adslog_filled'
    AND LOWER(COALESCE(f.ad_format, 'unknown')) IN ('interstitial', 'rewarded')
    AND f.request_id IS NOT NULL
    AND NULLIF(TRIM(f.max_unit_id), '') IS NOT NULL
    AND f.duration_sec_2dp IS NOT NULL
),

duration_agg AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    duration_sec_2dp,
    COUNT(*) AS filled_pv
  FROM filled_events
  GROUP BY experiment_group, product, ad_format, max_unit_id, duration_sec_2dp
)

SELECT
  experiment_group,
  product,
  ad_format,
  max_unit_id,
  duration_sec_2dp,
  filled_pv,
  SUM(filled_pv) OVER (
    PARTITION BY experiment_group, product, ad_format, max_unit_id
  ) AS denominator_filled_pv
FROM duration_agg
ORDER BY product, ad_format, max_unit_id, experiment_group, duration_sec_2dp;
