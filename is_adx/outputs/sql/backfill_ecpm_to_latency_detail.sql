-- 查询目的：给 commercial-adx.lmh.isadx_adslog_latency_detail 新增并回填 ecpm 列。
-- 关键口径：
-- 1. ecpm 来源于 Hudi adslog_filled 的 filled_value。
-- 2. 关联键使用 product + user_pseudo_id + request_id，避免跨产品/跨用户误匹配。
-- 3. 只给 latency 中 status = 'AD_LOADED' 的行回填 ecpm，其他状态保留 NULL。
-- 4. 若同一请求有多条 filled，优先取 filled_value 非空且最早发生的事件。

CREATE TEMP TABLE filled_request_map AS
WITH raw_filled_events AS (
  SELECT
    'com.takeoffbolts.screw.puzzle' AS product,
    user_pseudo_id,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    (SELECT value.double_value FROM UNNEST(event_params.array) WHERE key = 'filled_value') AS filled_value,
    event_timestamp
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_filled'

  UNION ALL

  SELECT
    'ios.takeoffbolts.screw.puzzle' AS product,
    user_pseudo_id,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    (SELECT value.double_value FROM UNNEST(event_params.array) WHERE key = 'filled_value') AS filled_value,
    event_timestamp
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_filled'
),
ranked_filled_events AS (
  SELECT
    product,
    user_pseudo_id,
    ad_format,
    request_id,
    filled_value,
    event_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY product, user_pseudo_id, request_id
      ORDER BY IF(filled_value IS NULL, 1, 0), event_timestamp
    ) AS filled_rank
  FROM raw_filled_events
  WHERE user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
)
SELECT
  product,
  user_pseudo_id,
  ad_format,
  request_id,
  filled_value
FROM ranked_filled_events
WHERE filled_rank = 1;

ALTER TABLE `commercial-adx.lmh.isadx_adslog_latency_detail`
ADD COLUMN IF NOT EXISTS ecpm FLOAT64;

UPDATE `commercial-adx.lmh.isadx_adslog_latency_detail` AS latency
SET ecpm = filled.filled_value
FROM filled_request_map AS filled
WHERE latency.product = filled.product
  AND latency.user_pseudo_id = filled.user_pseudo_id
  AND latency.request_id = filled.request_id
  AND latency.status = 'AD_LOADED'
  AND DATE(TIMESTAMP_MICROS(latency.event_timestamp), 'UTC') BETWEEN '2026-01-05' AND '2026-01-12';

WITH raw_filled_events AS (
  SELECT
    'com.takeoffbolts.screw.puzzle' AS product,
    user_pseudo_id,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    (SELECT value.double_value FROM UNNEST(event_params.array) WHERE key = 'filled_value') AS filled_value
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_filled'

  UNION ALL

  SELECT
    'ios.takeoffbolts.screw.puzzle' AS product,
    user_pseudo_id,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    (SELECT value.double_value FROM UNNEST(event_params.array) WHERE key = 'filled_value') AS filled_value
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_filled'
),
filled_duplicates AS (
  SELECT
    product,
    user_pseudo_id,
    request_id,
    COUNT(*) AS filled_event_cnt,
    COUNT(DISTINCT CAST(filled_value AS STRING)) AS distinct_filled_value_cnt
  FROM raw_filled_events
  WHERE user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
  HAVING COUNT(*) > 1
)
SELECT
  COUNT(*) AS duplicate_filled_key_cnt,
  COUNTIF(distinct_filled_value_cnt > 1) AS duplicate_filled_value_conflict_cnt
FROM filled_duplicates;
