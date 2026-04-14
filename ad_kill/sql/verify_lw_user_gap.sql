-- 验证：scene_distribution 与 long_watch_kill 的用户差集分析
-- 对比两种 ROW_NUMBER 过滤顺序下，long_watch_kill 用户集合的差异

-- 方式A（scene_distribution）：先过滤 ab_group IS NOT NULL，再取最后一条
-- 方式B（long_watch_kill）：先取最后一条，再过滤 ab_group IS NOT NULL

WITH all_events AS (
  SELECT
    'ball_sort' AS product,
    user_pseudo_id,
    event_timestamp,
    CASE
      WHEN REGEXP_CONTAINS(
        LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
        r'(^|[^a-z0-9])(12a)([^a-z0-9]|$)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
        r'(^|[^a-z0-9])(12b)([^a-z0-9]|$)'
      ) THEN 'B'
    END AS ab_group,
    (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name IN ('game_new_start', 'game_win')
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
),

-- 方式A：先过滤再排序
method_a AS (
  SELECT product, user_pseudo_id, ab_group, ad_kill_scene
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product, user_pseudo_id ORDER BY event_timestamp DESC) AS rn
    FROM all_events
    WHERE ab_group IS NOT NULL
  )
  WHERE rn = 1
),

-- 方式B：先排序再过滤
method_b AS (
  SELECT product, user_pseudo_id, ab_group, ad_kill_scene
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS rn
    FROM all_events
  )
  WHERE rn = 1 AND ab_group IS NOT NULL
)

SELECT
  'method_a_lw' AS metric,
  COUNT(*) AS user_count
FROM method_a WHERE ad_kill_scene = 'long_watch_kill'

UNION ALL

SELECT
  'method_b_lw' AS metric,
  COUNT(*) AS user_count
FROM method_b WHERE ad_kill_scene = 'long_watch_kill'

UNION ALL

-- 在A中是long_watch_kill但在B中不存在的用户数
SELECT
  'a_only_lw' AS metric,
  COUNT(*) AS user_count
FROM method_a a
WHERE a.ad_kill_scene = 'long_watch_kill'
  AND NOT EXISTS (
    SELECT 1 FROM method_b b
    WHERE b.user_pseudo_id = a.user_pseudo_id
    AND b.ad_kill_scene = 'long_watch_kill'
  )

UNION ALL

-- 在A中是long_watch_kill但在B中被完全丢弃（ab_group=NULL）的用户数
SELECT
  'a_lw_b_dropped' AS metric,
  COUNT(*) AS user_count
FROM method_a a
WHERE a.ad_kill_scene = 'long_watch_kill'
  AND NOT EXISTS (
    SELECT 1 FROM method_b b
    WHERE b.user_pseudo_id = a.user_pseudo_id
  )

ORDER BY metric