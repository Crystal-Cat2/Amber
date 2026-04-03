-- 查询目的：
-- 统计拧螺丝双端 Hudi 中 rewarded adslog_filled 事件里，
-- 没有对应 adslog_load_latency 的 filled 事件，
-- 其中有多少可以被“前序 50ms 内、同 user_pseudo_id + max_unit_id 的 filled 且前序 filled 有 latency”解释为重复请求。
-- 输出字段：
-- 1. product
-- 2. filled_without_latency_event_cnt
-- 3. duplicate_explained_event_cnt
-- 4. duplicate_explained_ratio
-- 5. remaining_unexplained_event_cnt
-- 6. remaining_unexplained_ratio
-- 关键口径：
-- 1. 只使用 Hudi 事件：adslog_filled / adslog_load_latency。
-- 2. 只看 ad_format = rewarded。
-- 3. 统计单位是 raw filled 事件，不回连请求事件。
-- 4. max_unit_id 取 event_params 中 ('max_unit_id','unit_id','sdk_unit_id')。
-- 5. 重复解释按存在性判断：当前 filled 无 latency，往前 50ms 内只要存在任意一条同 product + user_pseudo_id + max_unit_id 的前序 filled，
--    且该前序 filled 对应 request_id 有 latency，就记为 duplicate_explained。

WITH all_events AS (
  SELECT
    'screw_puzzle' AS product,
    event_name,
    user_pseudo_id,
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
    ) AS request_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('max_unit_id', 'unit_id', 'sdk_unit_id')) AS max_unit_id
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('adslog_filled', 'adslog_load_latency')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    event_name,
    user_pseudo_id,
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
    ) AS request_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('max_unit_id', 'unit_id', 'sdk_unit_id')) AS max_unit_id
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('adslog_filled', 'adslog_load_latency')
),

raw_reward_filled_events AS (
  SELECT
    product,
    user_pseudo_id,
    request_id,
    event_timestamp AS filled_ts,
    NULLIF(max_unit_id, '') AS max_unit_id,
    ROW_NUMBER() OVER (
      PARTITION BY product, user_pseudo_id, request_id, event_timestamp, COALESCE(NULLIF(max_unit_id, ''), '__NULL__')
      ORDER BY event_timestamp
    ) AS filled_event_seq
  FROM all_events
  WHERE event_name = 'adslog_filled'
    AND ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
    AND event_timestamp IS NOT NULL
),

reward_latency_keys AS (
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
    AND event_timestamp IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
),

filled_events_with_latency_flag AS (
  SELECT
    f.product,
    f.user_pseudo_id,
    f.request_id,
    f.filled_ts,
    f.max_unit_id,
    f.filled_event_seq,
    IF(l.request_id IS NOT NULL, 1, 0) AS current_has_latency
  FROM raw_reward_filled_events f
  LEFT JOIN reward_latency_keys l
    ON f.product = l.product
   AND f.user_pseudo_id = l.user_pseudo_id
   AND f.request_id = l.request_id
),

filled_without_latency_events AS (
  SELECT
    product,
    user_pseudo_id,
    request_id,
    filled_ts,
    max_unit_id,
    filled_event_seq
  FROM filled_events_with_latency_flag
  WHERE current_has_latency = 0
),

duplicate_explained_flags AS (
  SELECT
    c.product,
    c.user_pseudo_id,
    c.request_id,
    c.filled_ts,
    c.max_unit_id,
    c.filled_event_seq,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM filled_events_with_latency_flag p
        WHERE p.product = c.product
          AND p.user_pseudo_id = c.user_pseudo_id
          AND p.max_unit_id = c.max_unit_id
          AND p.current_has_latency = 1
          AND p.filled_ts < c.filled_ts
          AND TIMESTAMP_DIFF(
            TIMESTAMP_MICROS(c.filled_ts),
            TIMESTAMP_MICROS(p.filled_ts),
            MICROSECOND
          ) <= 50000
      ) THEN 1
      ELSE 0
    END AS is_duplicate_explained
  FROM filled_without_latency_events c
),

product_summary AS (
  SELECT
    product,
    COUNT(*) AS filled_without_latency_event_cnt,
    COUNTIF(is_duplicate_explained = 1) AS duplicate_explained_event_cnt,
    COUNTIF(is_duplicate_explained = 0) AS remaining_unexplained_event_cnt
  FROM duplicate_explained_flags
  GROUP BY product
)

SELECT
  product,
  filled_without_latency_event_cnt,
  duplicate_explained_event_cnt,
  SAFE_DIVIDE(duplicate_explained_event_cnt, filled_without_latency_event_cnt) AS duplicate_explained_ratio,
  remaining_unexplained_event_cnt,
  SAFE_DIVIDE(remaining_unexplained_event_cnt, filled_without_latency_event_cnt) AS remaining_unexplained_ratio
FROM product_summary
ORDER BY product;
