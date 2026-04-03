CREATE TEMP FUNCTION mark_greedy_filled_events(
  events ARRAY<STRUCT<request_id STRING, filled_ts INT64, filled_event_seq INT64>>
)
RETURNS ARRAY<STRUCT<
  request_id STRING,
  filled_ts INT64,
  filled_event_seq INT64,
  anchor_request_id STRING,
  anchor_filled_ts INT64,
  anchor_filled_event_seq INT64,
  is_duplicate INT64
>>
LANGUAGE js AS """
  const results = [];
  let anchor = null;
  for (const ev of events) {
    const filledTs = Number(ev.filled_ts);
    if (anchor === null || filledTs - Number(anchor.filled_ts) > 50000) {
      anchor = ev;
      results.push({
        request_id: ev.request_id,
        filled_ts: ev.filled_ts,
        filled_event_seq: ev.filled_event_seq,
        anchor_request_id: ev.request_id,
        anchor_filled_ts: ev.filled_ts,
        anchor_filled_event_seq: ev.filled_event_seq,
        is_duplicate: 0
      });
    } else {
      results.push({
        request_id: ev.request_id,
        filled_ts: ev.filled_ts,
        filled_event_seq: ev.filled_event_seq,
        anchor_request_id: anchor.request_id,
        anchor_filled_ts: anchor.filled_ts,
        anchor_filled_event_seq: anchor.filled_event_seq,
        is_duplicate: 1
      });
    }
  }
  return results;
""";

-- 查询目的：
-- 统计拧螺丝双端 Hudi 中 rewarded adslog_filled 事件里，
-- 按同 product + user_pseudo_id + max_unit_id 的 50ms 锚点窗口做贪心分段后，
-- 有多少 filled 事件被标记为 duplicate。
-- 输出字段：
-- 1. product
-- 2. total_filled_event_cnt
-- 3. duplicate_filled_event_cnt
-- 4. duplicate_ratio
-- 5. anchor_filled_event_cnt
-- 6. anchor_ratio

WITH all_events AS (
  SELECT
    'screw_puzzle' AS product,
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
    AND event_name = 'adslog_filled'

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
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
    AND event_name = 'adslog_filled'
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
  WHERE ad_format = 'rewarded'
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
    AND event_timestamp IS NOT NULL
),

ordered_filled_events AS (
  SELECT
    product,
    user_pseudo_id,
    request_id,
    filled_ts,
    max_unit_id,
    filled_event_seq,
    COALESCE(
      max_unit_id,
      CONCAT('__NULL__:', request_id, ':', CAST(filled_ts AS STRING), ':', CAST(filled_event_seq AS STRING))
    ) AS partition_max_unit_id
  FROM raw_reward_filled_events
),

greedy_marked_filled_events AS (
  SELECT
    product,
    user_pseudo_id,
    max_unit_id,
    marked.request_id,
    marked.filled_ts,
    marked.filled_event_seq,
    marked.anchor_request_id,
    marked.anchor_filled_ts,
    marked.anchor_filled_event_seq,
    marked.is_duplicate
  FROM (
    SELECT
      product,
      user_pseudo_id,
      partition_max_unit_id,
      max_unit_id,
      mark_greedy_filled_events(
        ARRAY_AGG(
          STRUCT(
            request_id,
            filled_ts,
            filled_event_seq
          )
          ORDER BY filled_ts, request_id, filled_event_seq
        )
      ) AS marked_events
    FROM ordered_filled_events
    GROUP BY product, user_pseudo_id, partition_max_unit_id, max_unit_id
  ) grouped
  CROSS JOIN UNNEST(grouped.marked_events) AS marked
),

product_summary AS (
  SELECT
    product,
    COUNT(*) AS total_filled_event_cnt,
    COUNTIF(is_duplicate = 1) AS duplicate_filled_event_cnt,
    COUNTIF(is_duplicate = 0) AS anchor_filled_event_cnt
  FROM greedy_marked_filled_events
  GROUP BY product
)

SELECT
  product,
  total_filled_event_cnt,
  duplicate_filled_event_cnt,
  SAFE_DIVIDE(duplicate_filled_event_cnt, total_filled_event_cnt) AS duplicate_ratio,
  anchor_filled_event_cnt,
  SAFE_DIVIDE(anchor_filled_event_cnt, total_filled_event_cnt) AS anchor_ratio
FROM product_summary
ORDER BY product;
