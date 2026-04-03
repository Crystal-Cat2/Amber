-- 查询目的：
-- 统计拧螺丝双端 Hudi 中 rewarded adslog_request 的 request PV，
-- 其中没有对应 adslog_load_latency 的 request PV 数量与比例，
-- 并按 duplicate_retained_first -> adslog_error -> unknown 的优先级归因。
-- 输出字段：
-- 1. stat_level
-- 2. product
-- 3. total_request_pv
-- 4. missing_latency_request_pv
-- 5. missing_latency_ratio
-- 6. duplicate_retained_first_pv
-- 7. duplicate_retained_first_ratio_in_missing
-- 8. adslog_error_pv
-- 9. adslog_error_ratio_in_missing
-- 10. unknown_pv
-- 11. unknown_ratio_in_missing
-- 关键口径：
-- 1. 只使用 Hudi 事件：adslog_request / adslog_filled / adslog_load_latency / adslog_error。
-- 2. 只看 ad_format = rewarded。
-- 3. request PV 粒度按 product + user_pseudo_id + request_id 去重。
-- 4. duplicate 判定改为在全量 reward filled 序列上计算，而不是看 request 序列。
-- 5. duplicate 条件：当前 request 必须有 filled，前一个 filled 存在且 request_id 不同，前一个 filled 对应 request 有 latency，filled 间隔 <= 50ms。
-- 6. 主因优先级：duplicate_retained_first > adslog_error > unknown。

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
    AND event_name IN ('adslog_request', 'adslog_filled', 'adslog_load_latency', 'adslog_error')

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
    AND event_name IN ('adslog_request', 'adslog_filled', 'adslog_load_latency', 'adslog_error')
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
    user_key,
    request_id AS filled_request_id,
    MIN(event_timestamp) AS filled_ts
  FROM all_events
  WHERE event_name = 'adslog_filled'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND user_key IS NOT NULL
    AND request_id IS NOT NULL
    AND event_timestamp IS NOT NULL
  GROUP BY product, user_pseudo_id, user_key, filled_request_id
),

reward_latency_pv AS (
  SELECT
    product,
    user_pseudo_id,
    request_id,
    MIN(event_timestamp) AS latency_ts
  FROM all_events
  WHERE event_name = 'adslog_load_latency'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

reward_error_pv AS (
  SELECT
    product,
    user_pseudo_id,
    request_id,
    MIN(event_timestamp) AS error_ts
  FROM all_events
  WHERE event_name = 'adslog_error'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

reward_filled_sequence AS (
  SELECT
    f.product,
    f.user_pseudo_id,
    f.user_key,
    f.filled_request_id,
    f.filled_ts,
    LAG(f.filled_request_id) OVER (
      PARTITION BY f.product, f.user_key
      ORDER BY f.filled_ts, f.filled_request_id
    ) AS prev_filled_request_id,
    LAG(f.filled_ts) OVER (
      PARTITION BY f.product, f.user_key
      ORDER BY f.filled_ts, f.filled_request_id
    ) AS prev_filled_ts
  FROM reward_filled_pv f
),

reward_filled_sequence_with_prev_latency AS (
  SELECT
    s.product,
    s.user_pseudo_id,
    s.user_key,
    s.filled_request_id,
    s.filled_ts,
    s.prev_filled_request_id,
    s.prev_filled_ts,
    IF(p.request_id IS NOT NULL, 1, 0) AS prev_filled_has_latency
  FROM reward_filled_sequence s
  LEFT JOIN reward_latency_pv p
    ON s.product = p.product
   AND s.user_pseudo_id = p.user_pseudo_id
   AND s.prev_filled_request_id = p.request_id
),

missing_latency_pv AS (
  SELECT
    r.product,
    r.user_pseudo_id,
    r.user_key,
    r.request_id,
    r.request_ts
  FROM reward_request_pv r
  LEFT JOIN reward_latency_pv l
    ON r.product = l.product
   AND r.user_pseudo_id = l.user_pseudo_id
   AND r.request_id = l.request_id
  WHERE l.request_id IS NULL
),

missing_with_flags AS (
  SELECT
    m.product,
    m.user_pseudo_id,
    m.request_id,
    IF(f.filled_request_id IS NOT NULL, 1, 0) AS current_has_filled,
    CASE
      WHEN f.filled_request_id IS NOT NULL
       AND f.prev_filled_request_id IS NOT NULL
       AND f.prev_filled_request_id != m.request_id
       AND f.prev_filled_has_latency = 1
       AND f.prev_filled_ts IS NOT NULL
       AND TIMESTAMP_DIFF(
         TIMESTAMP_MICROS(f.filled_ts),
         TIMESTAMP_MICROS(f.prev_filled_ts),
         MICROSECOND
       ) <= 50000 -- <= 50ms
      THEN 1
      ELSE 0
    END AS is_duplicate_retained_first,
    IF(e.request_id IS NOT NULL, 1, 0) AS has_adslog_error
  FROM missing_latency_pv m
  LEFT JOIN reward_filled_sequence_with_prev_latency f
    ON m.product = f.product
   AND m.user_pseudo_id = f.user_pseudo_id
   AND m.request_id = f.filled_request_id
  LEFT JOIN reward_error_pv e
    ON m.product = e.product
   AND m.user_pseudo_id = e.user_pseudo_id
   AND m.request_id = e.request_id
),

missing_with_root_cause AS (
  SELECT
    product,
    user_pseudo_id,
    request_id,
    CASE
      WHEN is_duplicate_retained_first = 1 THEN 'duplicate_retained_first'
      WHEN has_adslog_error = 1 THEN 'adslog_error'
      ELSE 'unknown'
    END AS root_cause
  FROM missing_with_flags
),

product_summary AS (
  SELECT
    r.product,
    COUNT(*) AS total_request_pv,
    COUNT(m.request_id) AS missing_latency_request_pv,
    COUNTIF(c.root_cause = 'duplicate_retained_first') AS duplicate_retained_first_pv,
    COUNTIF(c.root_cause = 'adslog_error') AS adslog_error_pv,
    COUNTIF(c.root_cause = 'unknown') AS unknown_pv
  FROM reward_request_pv r
  LEFT JOIN missing_latency_pv m
    ON r.product = m.product
   AND r.user_pseudo_id = m.user_pseudo_id
   AND r.request_id = m.request_id
  LEFT JOIN missing_with_root_cause c
    ON m.product = c.product
   AND m.user_pseudo_id = c.user_pseudo_id
   AND m.request_id = c.request_id
  GROUP BY r.product
),

overall_summary AS (
  SELECT
    'all' AS product,
    SUM(total_request_pv) AS total_request_pv,
    SUM(missing_latency_request_pv) AS missing_latency_request_pv,
    SUM(duplicate_retained_first_pv) AS duplicate_retained_first_pv,
    SUM(adslog_error_pv) AS adslog_error_pv,
    SUM(unknown_pv) AS unknown_pv
  FROM product_summary
)

SELECT
  'overall' AS stat_level,
  product,
  total_request_pv,
  missing_latency_request_pv,
  SAFE_DIVIDE(missing_latency_request_pv, total_request_pv) AS missing_latency_ratio,
  duplicate_retained_first_pv,
  SAFE_DIVIDE(duplicate_retained_first_pv, NULLIF(missing_latency_request_pv, 0)) AS duplicate_retained_first_ratio_in_missing,
  adslog_error_pv,
  SAFE_DIVIDE(adslog_error_pv, NULLIF(missing_latency_request_pv, 0)) AS adslog_error_ratio_in_missing,
  unknown_pv,
  SAFE_DIVIDE(unknown_pv, NULLIF(missing_latency_request_pv, 0)) AS unknown_ratio_in_missing
FROM overall_summary

UNION ALL

SELECT
  'by_product' AS stat_level,
  product,
  total_request_pv,
  missing_latency_request_pv,
  SAFE_DIVIDE(missing_latency_request_pv, total_request_pv) AS missing_latency_ratio,
  duplicate_retained_first_pv,
  SAFE_DIVIDE(duplicate_retained_first_pv, NULLIF(missing_latency_request_pv, 0)) AS duplicate_retained_first_ratio_in_missing,
  adslog_error_pv,
  SAFE_DIVIDE(adslog_error_pv, NULLIF(missing_latency_request_pv, 0)) AS adslog_error_ratio_in_missing,
  unknown_pv,
  SAFE_DIVIDE(unknown_pv, NULLIF(missing_latency_request_pv, 0)) AS unknown_ratio_in_missing
FROM product_summary

ORDER BY stat_level, product;
