-- 第5关 AB 对比：1) 开局次数分布  2) time_to_kill 分布
-- 数据源：transferred.hudi_ods.*

-- AB 分组映射
WITH user_ab AS (
  SELECT user_pseudo_id, 'ball_sort' AS product, ab_group
  FROM (
    SELECT
      user_pseudo_id,
      CASE
        WHEN REGEXP_CONTAINS(
          LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
          r'(^|[^a-z0-9])(12a)([^a-z0-9]|$)'
        ) THEN 'A'
        WHEN REGEXP_CONTAINS(
          LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
          r'(^|[^a-z0-9])(12b)([^a-z0-9]|$)'
        ) THEN 'B'
      END AS ab_group
    FROM `transferred.hudi_ods.ball_sort`
    WHERE event_date BETWEEN '2026-01-30' AND '2026-04-07'
      AND event_name = 'user_engagement'
      AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  )
  WHERE ab_group IS NOT NULL
  GROUP BY user_pseudo_id, ab_group

  UNION ALL

  SELECT user_pseudo_id, 'ios_nuts_sort' AS product, ab_group
  FROM (
    SELECT
      user_pseudo_id,
      CASE
        WHEN REGEXP_CONTAINS(
          LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
          r'(^|[^a-z0-9])(2a)([^a-z0-9]|$)'
        ) THEN 'A'
        WHEN REGEXP_CONTAINS(
          LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
          r'(^|[^a-z0-9])(2b)([^a-z0-9]|$)'
        ) THEN 'B'
      END AS ab_group
    FROM `transferred.hudi_ods.ios_nuts_sort`
    WHERE event_date BETWEEN '2026-02-02' AND '2026-04-07'
      AND event_name = 'user_engagement'
      AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  )
  WHERE ab_group IS NOT NULL
  GROUP BY user_pseudo_id, ab_group
),

-- Part 1: 每用户第5关 game_new_start 次数
user_start_count AS (
  SELECT ua.product, ua.ab_group, e.user_pseudo_id, COUNT(*) AS start_count
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  GROUP BY ua.product, ua.ab_group, e.user_pseudo_id

  UNION ALL

  SELECT ua.product, ua.ab_group, e.user_pseudo_id, COUNT(*) AS start_count
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  GROUP BY ua.product, ua.ab_group, e.user_pseudo_id
),

-- B 组幽灵事件用户：第5关 game_new_start 带有触发跳关的 ad_kill_scene
ghost_users AS (
  SELECT DISTINCT ua.product, e.user_pseudo_id
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND ua.ab_group = 'B'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene')
        IN ('long_watch_kill', 'short_watch_repeat_kill')

  UNION ALL

  SELECT DISTINCT ua.product, e.user_pseudo_id
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND ua.ab_group = 'B'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene')
        IN ('long_watch_kill', 'short_watch_repeat_kill')
),

-- 修正后开局次数：B 组幽灵用户 start_count - 1
adjusted_start AS (
  SELECT
    sc.product, sc.ab_group, sc.user_pseudo_id,
    sc.start_count AS original_start_count,
    CASE
      WHEN sc.ab_group = 'B' AND g.user_pseudo_id IS NOT NULL THEN sc.start_count - 1
      ELSE sc.start_count
    END AS adj_start_count
  FROM user_start_count sc
  LEFT JOIN ghost_users g ON sc.product = g.product AND sc.user_pseudo_id = g.user_pseudo_id
),

start_distribution AS (
  SELECT
    product, ab_group, start_count,
    COUNT(*) AS user_count,
    ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY product, ab_group)), 4) AS user_ratio
  FROM user_start_count
  GROUP BY product, ab_group, start_count
),

-- Part 2: 每次杀广告的 time_to_kill 分桶
kill_ttk AS (
  SELECT
    ua.product, ua.ab_group,
    (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'time_to_kill') AS time_to_kill
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5

  UNION ALL

  SELECT
    ua.product, ua.ab_group,
    (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'time_to_kill') AS time_to_kill
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
),

ttk_distribution AS (
  SELECT
    product, ab_group,
    CASE
      WHEN time_to_kill IS NULL THEN 'null'
      WHEN time_to_kill <= 5 THEN '0-5s'
      WHEN time_to_kill <= 10 THEN '6-10s'
      WHEN time_to_kill <= 20 THEN '11-20s'
      WHEN time_to_kill <= 30 THEN '21-30s'
      ELSE '30s+'
    END AS ttk_bucket,
    COUNT(*) AS event_count,
    ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY product, ab_group)), 4) AS event_ratio
  FROM kill_ttk
  GROUP BY product, ab_group, ttk_bucket
)

-- 输出两部分
SELECT 'start_count' AS metric, product, ab_group,
  CAST(start_count AS STRING) AS bucket, user_count AS cnt, user_ratio AS ratio
FROM start_distribution
WHERE start_count <= 10

UNION ALL

SELECT 'time_to_kill' AS metric, product, ab_group,
  ttk_bucket AS bucket, event_count AS cnt, event_ratio AS ratio
FROM ttk_distribution

ORDER BY metric, product, ab_group, bucket;
