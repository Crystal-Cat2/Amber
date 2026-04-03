-- 查询目的：
-- 对拧螺丝双端 Hudi 中全量 rewarded adslog_request 的 request PV 做两级分类：
-- 1. 一级分类：filled / error / neither
-- 2. 二级分类：with_latency / without_latency
-- 输出字段：
-- 1. stat_level
-- 2. product
-- 3. primary_class
-- 4. latency_class
-- 5. request_pv
-- 6. total_request_pv
-- 7. ratio_in_total_request
-- 8. primary_class_total_pv
-- 9. ratio_in_primary_class
-- 关键口径：
-- 1. 只使用 Hudi 事件：adslog_request / adslog_filled / adslog_error / adslog_load_latency。
-- 2. 只看 ad_format = rewarded。
-- 3. request PV 粒度按 product + user_pseudo_id + request_id 去重。
-- 4. 一级分类优先级：filled > error > neither。
-- 5. 二级分类仅按是否存在 adslog_load_latency 判定。

WITH all_events AS (
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
    AND event_name IN ('adslog_request', 'adslog_filled', 'adslog_error', 'adslog_load_latency')

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
    AND event_name IN ('adslog_request', 'adslog_filled', 'adslog_error', 'adslog_load_latency')
),

reward_request_pv AS (
  SELECT
    product,
    user_pseudo_id,
    user_key,
    request_id,
    MIN(event_timestamp) AS request_ts
  FROM all_events
  WHERE event_name = 'adslog_request'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND user_key IS NOT NULL
    AND request_id IS NOT NULL
    AND event_timestamp IS NOT NULL
  GROUP BY product, user_pseudo_id, user_key, request_id
),

reward_filled_pv AS (
  SELECT
    product,
    user_pseudo_id,
    request_id
  FROM all_events
  WHERE event_name = 'adslog_filled'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

reward_error_pv AS (
  SELECT
    product,
    user_pseudo_id,
    request_id
  FROM all_events
  WHERE event_name = 'adslog_error'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

reward_latency_pv AS (
  SELECT
    product,
    user_pseudo_id,
    request_id
  FROM all_events
  WHERE event_name = 'adslog_load_latency'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

classified_request_pv AS (
  SELECT
    r.product,
    r.user_pseudo_id,
    r.request_id,
    CASE
      WHEN f.request_id IS NOT NULL THEN 'filled'
      WHEN e.request_id IS NOT NULL THEN 'error'
      ELSE 'neither'
    END AS primary_class,
    CASE
      WHEN l.request_id IS NOT NULL THEN 'with_latency'
      ELSE 'without_latency'
    END AS latency_class
  FROM reward_request_pv r
  LEFT JOIN reward_filled_pv f
    ON r.product = f.product
   AND r.user_pseudo_id = f.user_pseudo_id
   AND r.request_id = f.request_id
  LEFT JOIN reward_error_pv e
    ON r.product = e.product
   AND r.user_pseudo_id = e.user_pseudo_id
   AND r.request_id = e.request_id
  LEFT JOIN reward_latency_pv l
    ON r.product = l.product
   AND r.user_pseudo_id = l.user_pseudo_id
   AND r.request_id = l.request_id
),

product_total AS (
  SELECT
    product,
    COUNT(*) AS total_request_pv
  FROM reward_request_pv
  GROUP BY product
),

product_primary_total AS (
  SELECT
    product,
    primary_class,
    COUNT(*) AS primary_class_total_pv
  FROM classified_request_pv
  GROUP BY product, primary_class
),

product_summary AS (
  SELECT
    c.product,
    c.primary_class,
    c.latency_class,
    COUNT(*) AS request_pv
  FROM classified_request_pv c
  GROUP BY c.product, c.primary_class, c.latency_class
),

overall_total AS (
  SELECT COUNT(*) AS total_request_pv
  FROM reward_request_pv
),

overall_primary_total AS (
  SELECT
    primary_class,
    COUNT(*) AS primary_class_total_pv
  FROM classified_request_pv
  GROUP BY primary_class
),

overall_summary AS (
  SELECT
    primary_class,
    latency_class,
    COUNT(*) AS request_pv
  FROM classified_request_pv
  GROUP BY primary_class, latency_class
)

SELECT
  'overall' AS stat_level,
  'all' AS product,
  s.primary_class,
  s.latency_class,
  s.request_pv,
  t.total_request_pv,
  SAFE_DIVIDE(s.request_pv, t.total_request_pv) AS ratio_in_total_request,
  p.primary_class_total_pv,
  SAFE_DIVIDE(s.request_pv, p.primary_class_total_pv) AS ratio_in_primary_class
FROM overall_summary s
CROSS JOIN overall_total t
JOIN overall_primary_total p
  ON s.primary_class = p.primary_class

UNION ALL

SELECT
  'by_product' AS stat_level,
  s.product,
  s.primary_class,
  s.latency_class,
  s.request_pv,
  t.total_request_pv,
  SAFE_DIVIDE(s.request_pv, t.total_request_pv) AS ratio_in_total_request,
  p.primary_class_total_pv,
  SAFE_DIVIDE(s.request_pv, p.primary_class_total_pv) AS ratio_in_primary_class
FROM product_summary s
JOIN product_total t
  ON s.product = t.product
JOIN product_primary_total p
  ON s.product = p.product
 AND s.primary_class = p.primary_class

ORDER BY
  stat_level,
  product,
  CASE primary_class
    WHEN 'filled' THEN 0
    WHEN 'error' THEN 1
    ELSE 2
  END,
  CASE latency_class
    WHEN 'with_latency' THEN 0
    ELSE 1
  END;
