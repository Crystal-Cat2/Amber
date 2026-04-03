-- 查询目的：
-- 统计拧螺丝双端 Hudi 中 root_cause = 'unknown' 的 rewarded request，
-- 在全量 rewarded request 序列里相对前一个 request 的时间间隔分布。
-- 输出分两层：
-- 1. fine_grained：精细原值毫秒分布
-- 2. report_bucket：汇报用区间桶分布（0-10 / 10-20 / 20-50 / 50-100 / 100-500 / 500ms+）
-- 输出字段：
-- 1. report_section
-- 2. product
-- 3. interval_bucket
-- 4. diff_ms
-- 5. bucket_pv
-- 6. total_unknown_valid_pv
-- 7. pv_ratio
-- 关键口径：
-- 1. 只使用 Hudi 事件：adslog_request / adslog_filled / adslog_load_latency / adslog_error。
-- 2. 只看 ad_format = rewarded。
-- 3. unknown 样本沿用现有根因优先级：duplicate_retained_first > adslog_error > unknown，其中 duplicate 基于 filled 序列判定。
-- 4. 时间间隔本身仍在全量 reward request 序列上计算，而不是只在 unknown 子集里计算。
-- 5. 时间差定义为当前 unknown request 到前一个 reward request 的间隔。
-- 6. 没有前一个 request 的 unknown 样本不纳入间隔分布。

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
    request_id
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
    request_id
  FROM all_events
  WHERE event_name = 'adslog_error'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

reward_request_sequence AS (
  SELECT
    r.product,
    r.user_pseudo_id,
    r.user_key,
    r.request_id,
    r.request_ts,
    LAG(r.request_id) OVER (
      PARTITION BY r.product, r.user_key
      ORDER BY r.request_ts, r.request_id
    ) AS prev_request_id,
    LAG(r.request_ts) OVER (
      PARTITION BY r.product, r.user_key
      ORDER BY r.request_ts, r.request_id
    ) AS prev_request_ts
  FROM reward_request_pv r
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
       ) <= 50000
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

unknown_interval_base AS (
  SELECT
    s.product,
    ROUND(
      TIMESTAMP_DIFF(
        TIMESTAMP_MICROS(s.request_ts),
        TIMESTAMP_MICROS(s.prev_request_ts),
        MICROSECOND
      ) / 1000.0,
      3
    ) AS diff_ms
  FROM reward_request_sequence s
  JOIN missing_with_root_cause c
    ON s.product = c.product
   AND s.user_pseudo_id = c.user_pseudo_id
   AND s.request_id = c.request_id
  WHERE c.root_cause = 'unknown'
    AND s.prev_request_ts IS NOT NULL
    AND s.prev_request_ts <= s.request_ts
),

fine_grained_counts AS (
  SELECT
    product,
    FORMAT('%.3fms', ROUND(diff_ms, 3)) AS interval_bucket,
    ROUND(diff_ms, 3) AS diff_ms,
    COUNT(*) AS bucket_pv
  FROM unknown_interval_base
  GROUP BY product, interval_bucket, diff_ms
),

report_bucket_counts AS (
  SELECT
    product,
    CASE
      WHEN diff_ms < 10 THEN '0-10ms'
      WHEN diff_ms < 20 THEN '10-20ms'
      WHEN diff_ms < 50 THEN '20-50ms'
      WHEN diff_ms < 100 THEN '50-100ms'
      WHEN diff_ms < 500 THEN '100-500ms'
      ELSE '500ms+'
    END AS interval_bucket,
    COUNT(*) AS bucket_pv
  FROM unknown_interval_base
  GROUP BY product, interval_bucket
)

SELECT
  'fine_grained' AS report_section,
  product,
  interval_bucket,
  diff_ms,
  bucket_pv,
  SUM(bucket_pv) OVER (PARTITION BY product) AS total_unknown_valid_pv,
  SAFE_DIVIDE(bucket_pv, SUM(bucket_pv) OVER (PARTITION BY product)) AS pv_ratio
FROM fine_grained_counts

UNION ALL

SELECT
  'report_bucket' AS report_section,
  product,
  interval_bucket,
  NULL AS diff_ms,
  bucket_pv,
  SUM(bucket_pv) OVER (PARTITION BY product) AS total_unknown_valid_pv,
  SAFE_DIVIDE(bucket_pv, SUM(bucket_pv) OVER (PARTITION BY product)) AS pv_ratio
FROM report_bucket_counts

ORDER BY
  report_section,
  product,
  CASE
    WHEN interval_bucket = '0-10ms' THEN 0
    WHEN interval_bucket = '10-20ms' THEN 1
    WHEN interval_bucket = '20-50ms' THEN 2
    WHEN interval_bucket = '50-100ms' THEN 3
    WHEN interval_bucket = '100-500ms' THEN 4
    WHEN interval_bucket = '500ms+' THEN 5
    ELSE 6
  END,
  diff_ms;
