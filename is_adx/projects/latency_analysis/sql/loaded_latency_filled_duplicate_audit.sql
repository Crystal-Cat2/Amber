-- 查询目的：审计 Hudi adslog_filled 在回填键上的重复情况。
-- 输出字段：duplicate_filled_key_cnt, duplicate_filled_value_conflict_cnt

WITH hudi_filled_events AS (
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
duplicate_keys AS (
  SELECT
    product,
    user_pseudo_id,
    request_id,
    COUNT(*) AS filled_event_cnt,
    COUNT(DISTINCT CAST(filled_value AS STRING)) AS distinct_filled_value_cnt
  FROM hudi_filled_events
  WHERE user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
  GROUP BY product, user_pseudo_id, request_id
  HAVING COUNT(*) > 1
)
SELECT
  COUNT(*) AS duplicate_filled_key_cnt,
  COUNTIF(distinct_filled_value_cnt > 1) AS duplicate_filled_value_conflict_cnt
FROM duplicate_keys;
