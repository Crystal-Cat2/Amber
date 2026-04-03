-- 查询目的：统计 adslog_filled / adslog_request 的请求级比例，只按 product + ad_format 汇总。
-- 输出字段：product, ad_format, adslog_request_cnt, adslog_filled_cnt, filled_ratio
-- 关键口径：
-- 1. request 粒度按 user_pseudo_id + request_id 去重。
-- 2. 分母是 adslog_request 的去重请求数。
-- 3. 分子是 adslog_filled 的去重请求数。
-- 4. 时间范围固定为 2026-01-05 到 2026-01-12。

WITH app_events AS (
  SELECT
    'screw_puzzle' AS product,
    user_pseudo_id,
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
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('adslog_request', 'adslog_filled')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    user_pseudo_id,
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
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('adslog_request', 'adslog_filled')
),

adslog_request_dedup AS (
  SELECT
    product,
    ad_format,
    user_pseudo_id,
    request_id
  FROM app_events
  WHERE event_name = 'adslog_request'
    AND ad_format IN ('banner', 'interstitial', 'rewarded')
    AND request_id IS NOT NULL
  GROUP BY
    product,
    ad_format,
    user_pseudo_id,
    request_id
),

adslog_filled_dedup AS (
  SELECT
    product,
    ad_format,
    user_pseudo_id,
    request_id
  FROM app_events
  WHERE event_name = 'adslog_filled'
    AND ad_format IN ('banner', 'interstitial', 'rewarded')
    AND request_id IS NOT NULL
  GROUP BY
    product,
    ad_format,
    user_pseudo_id,
    request_id
),

request_counts AS (
  SELECT
    product,
    ad_format,
    COUNT(*) AS adslog_request_cnt
  FROM adslog_request_dedup
  GROUP BY
    product,
    ad_format
),

filled_counts AS (
  SELECT
    product,
    ad_format,
    COUNT(*) AS adslog_filled_cnt
  FROM adslog_filled_dedup
  GROUP BY
    product,
    ad_format
)

SELECT
  r.product,
  r.ad_format,
  r.adslog_request_cnt,
  COALESCE(f.adslog_filled_cnt, 0) AS adslog_filled_cnt,
  SAFE_DIVIDE(COALESCE(f.adslog_filled_cnt, 0), r.adslog_request_cnt) AS filled_ratio
FROM request_counts r
LEFT JOIN filled_counts f
  ON r.product = f.product
  AND r.ad_format = f.ad_format
ORDER BY
  r.product,
  r.ad_format;
