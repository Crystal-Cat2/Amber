-- 查询目的：
-- 统计拧螺丝双端 Hudi 表中，adslog_request 与其下一个 adslog_request 间隔 <= 50ms 的请求事件，
-- 其中有多少匹配到 adslog_filled，有多少匹配到 adslog_error。
-- 输出字段：
-- 1. stat_level: overall / by_product_ad_format
-- 2. product
-- 3. ad_format
-- 4. qualified_request_event_cnt
-- 5. matched_adslog_filled_cnt
-- 6. matched_adslog_error_cnt
-- 关键口径：
-- 1. 时间范围：2026-01-05 到 2026-01-12。
-- 2. 双端各扫一次 Hudi，统一沉淀 app_events 再派生 request / filled / error。
-- 3. 双端分别构造 user_key，按 product + user_key + ad_format + event_timestamp 排序。
-- 4. 命中事件定义为“当前 adslog_request 到下一个 adslog_request 的间隔 <= 50ms”。
-- 5. filled / error 按 product + user_pseudo_id + request_id 请求级匹配。
-- 6. adslog_filled 与 adslog_error 分别统计，互不排斥。

WITH app_events AS (
  SELECT
    'screw_puzzle' AS product,
    event_name,
    user_pseudo_id,
    CONCAT(
      NULLIF(device.advertising_id, ''),
      '-',
      CAST(user_first_touch_timestamp AS STRING)
    ) AS user_key,
    event_timestamp,
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
    AND event_name IN ('adslog_request', 'adslog_filled', 'adslog_error')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    event_name,
    user_pseudo_id,
    CONCAT(
      NULLIF(COALESCE(user_id, device.vendor_id), ''),
      '_',
      CAST(user_first_touch_timestamp AS STRING)
    ) AS user_key,
    event_timestamp,
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
    AND event_name IN ('adslog_request', 'adslog_filled', 'adslog_error')
),

qualified_request_events AS (
  SELECT
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    event_timestamp
  FROM (
    SELECT
      product,
      ad_format,
      user_pseudo_id,
      request_id,
      event_timestamp,
      LEAD(event_timestamp) OVER (
        PARTITION BY product, user_key, ad_format
        ORDER BY event_timestamp
      ) AS next_event_timestamp
    FROM app_events
    WHERE event_name = 'adslog_request'
      AND user_key IS NOT NULL
      AND user_pseudo_id IS NOT NULL
      AND request_id IS NOT NULL
      AND event_timestamp IS NOT NULL
  ) t
  WHERE next_event_timestamp IS NOT NULL
    AND next_event_timestamp >= event_timestamp
    AND next_event_timestamp - event_timestamp <= 50000
),

filled_request_keys AS (
  SELECT
    product,
    user_pseudo_id,
    request_id
  FROM app_events
  WHERE event_name = 'adslog_filled'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

error_request_keys AS (
  SELECT
    product,
    user_pseudo_id,
    request_id
  FROM app_events
  WHERE event_name = 'adslog_error'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

matched_base AS (
  SELECT
    q.product,
    q.ad_format,
    q.user_pseudo_id,
    q.request_id,
    IF(f.request_id IS NOT NULL, 1, 0) AS has_adslog_filled,
    IF(e.request_id IS NOT NULL, 1, 0) AS has_adslog_error
  FROM qualified_request_events q
  LEFT JOIN filled_request_keys f
    ON q.product = f.product
   AND q.user_pseudo_id = f.user_pseudo_id
   AND q.request_id = f.request_id
  LEFT JOIN error_request_keys e
    ON q.product = e.product
   AND q.user_pseudo_id = e.user_pseudo_id
   AND q.request_id = e.request_id
)

SELECT
  'overall' AS stat_level,
  'all' AS product,
  'all' AS ad_format,
  COUNT(*) AS qualified_request_event_cnt,
  SUM(has_adslog_filled) AS matched_adslog_filled_cnt,
  SUM(has_adslog_error) AS matched_adslog_error_cnt
FROM matched_base

UNION ALL

SELECT
  'by_product_ad_format' AS stat_level,
  product,
  ad_format,
  COUNT(*) AS qualified_request_event_cnt,
  SUM(has_adslog_filled) AS matched_adslog_filled_cnt,
  SUM(has_adslog_error) AS matched_adslog_error_cnt
FROM matched_base
GROUP BY product, ad_format

ORDER BY
  stat_level,
  product,
  ad_format;
