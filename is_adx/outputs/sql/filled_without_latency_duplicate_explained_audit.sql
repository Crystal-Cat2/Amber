-- 查询目的：
-- 输出 rewarded 场景下，filled 但无 latency 的 raw filled 事件中，
-- 被“前序 50ms 内同 user_pseudo_id + max_unit_id 的 filled 且前序 filled 有 latency”解释为重复请求的匹配明细。
-- 输出字段：
-- 1. product
-- 2. user_pseudo_id
-- 3. current_request_id
-- 4. current_filled_ts
-- 5. max_unit_id
-- 6. prev_request_id
-- 7. prev_filled_ts
-- 8. prev_latency_ts
-- 9. diff_ms

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
    l.latency_ts,
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

prev_filled_candidates AS (
  SELECT
    c.product,
    c.user_pseudo_id,
    c.request_id AS current_request_id,
    c.filled_ts AS current_filled_ts,
    c.max_unit_id,
    c.filled_event_seq AS current_filled_event_seq,
    p.request_id AS prev_request_id,
    p.filled_ts AS prev_filled_ts,
    p.latency_ts AS prev_latency_ts,
    p.filled_event_seq AS prev_filled_event_seq,
    ROUND(
      TIMESTAMP_DIFF(
        TIMESTAMP_MICROS(c.filled_ts),
        TIMESTAMP_MICROS(p.filled_ts),
        MICROSECOND
      ) / 1000.0,
      3
    ) AS diff_ms,
    ROW_NUMBER() OVER (
      PARTITION BY c.product, c.user_pseudo_id, c.request_id, c.filled_ts, c.filled_event_seq
      ORDER BY p.filled_ts DESC, p.request_id, p.filled_event_seq
    ) AS match_rank
  FROM filled_without_latency_events c
  INNER JOIN filled_events_with_latency_flag p
    ON c.product = p.product
   AND c.user_pseudo_id = p.user_pseudo_id
   AND c.max_unit_id = p.max_unit_id
   AND p.current_has_latency = 1
   AND p.filled_ts < c.filled_ts
   AND TIMESTAMP_DIFF(
     TIMESTAMP_MICROS(c.filled_ts),
     TIMESTAMP_MICROS(p.filled_ts),
     MICROSECOND
   ) <= 50000
)

SELECT
  product,
  user_pseudo_id,
  current_request_id,
  current_filled_ts,
  max_unit_id,
  prev_request_id,
  prev_filled_ts,
  prev_latency_ts,
  diff_ms
FROM prev_filled_candidates
WHERE match_rank = 1
ORDER BY product, user_pseudo_id, current_filled_ts, current_request_id;
