-- 查询目的：验证 loaded latency request 中，约 99% 能匹配到 Hudi adslog_filled。
-- 输出字段：
-- product, ad_format, loaded_request_cnt, matched_filled_request_cnt, unmatched_request_cnt,
-- matched_ratio, loaded_row_cnt, loaded_row_with_ecpm_source_cnt, non_null_filled_value_ratio

WITH loaded_latency_rows AS (
  SELECT
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    event_timestamp
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE product IN ('com.takeoffbolts.screw.puzzle', 'ios.takeoffbolts.screw.puzzle')
    AND status = 'AD_LOADED'
    AND request_id IS NOT NULL
    AND user_pseudo_id IS NOT NULL
    AND DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') BETWEEN '2026-01-05' AND '2026-01-12'
),
loaded_latency_requests AS (
  SELECT
    product,
    LOWER(COALESCE(ad_format, 'unknown')) AS ad_format,
    user_pseudo_id,
    request_id,
    COUNT(*) AS loaded_row_cnt
  FROM loaded_latency_rows
  GROUP BY product, ad_format, user_pseudo_id, request_id
),
hudi_filled_events AS (
  SELECT
    'com.takeoffbolts.screw.puzzle' AS product,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    user_pseudo_id,
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
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    user_pseudo_id,
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
hudi_filled_dedup AS (
  SELECT
    product,
    LOWER(COALESCE(ad_format, 'unknown')) AS ad_format,
    user_pseudo_id,
    request_id,
    ARRAY_AGG(
      STRUCT(filled_value, event_timestamp)
      ORDER BY IF(filled_value IS NULL, 1, 0), event_timestamp
      LIMIT 1
    )[OFFSET(0)].filled_value AS filled_value
  FROM hudi_filled_events
  WHERE user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, ad_format, user_pseudo_id, request_id
),
filled_match_flags AS (
  SELECT
    req.product,
    req.ad_format,
    req.user_pseudo_id,
    req.request_id,
    req.loaded_row_cnt,
    IF(filled.request_id IS NOT NULL, 1, 0) AS has_filled,
    IF(filled.filled_value IS NOT NULL, 1, 0) AS has_non_null_filled_value
  FROM loaded_latency_requests req
  LEFT JOIN hudi_filled_dedup filled
    ON req.product = filled.product
   AND req.ad_format = filled.ad_format
   AND req.user_pseudo_id = filled.user_pseudo_id
   AND req.request_id = filled.request_id
)
SELECT
  product,
  ad_format,
  COUNT(*) AS loaded_request_cnt,
  SUM(has_filled) AS matched_filled_request_cnt,
  COUNTIF(has_filled = 0) AS unmatched_request_cnt,
  SAFE_DIVIDE(SUM(has_filled), COUNT(*)) AS matched_ratio,
  SUM(loaded_row_cnt) AS loaded_row_cnt,
  SUM(IF(has_filled = 1, loaded_row_cnt, 0)) AS loaded_row_with_ecpm_source_cnt,
  SAFE_DIVIDE(SUM(IF(has_non_null_filled_value = 1, loaded_row_cnt, 0)), SUM(loaded_row_cnt)) AS non_null_filled_value_ratio
FROM filled_match_flags
GROUP BY product, ad_format
ORDER BY product, ad_format;
