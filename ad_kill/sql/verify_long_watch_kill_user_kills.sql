-- long_watch_kill 用户在第5关的杀广告次数分布（AB 对比）
-- 验证：B 组跳关是否导致 long_watch_kill 用户杀广告次数更少

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

-- long_watch_kill 用户：第5关 game_new_start 上 ad_kill_scene = 'long_watch_kill'
long_watch_users AS (
  SELECT DISTINCT ua.product, ua.ab_group, e.user_pseudo_id
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') = 'long_watch_kill'

  UNION ALL

  SELECT DISTINCT ua.product, ua.ab_group, e.user_pseudo_id
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') = 'long_watch_kill'
),

-- 这些用户在第5关的杀广告次数
user_kill_count AS (
  SELECT lw.product, lw.ab_group, lw.user_pseudo_id, COUNT(*) AS kill_count
  FROM long_watch_users lw
  INNER JOIN `transferred.hudi_ods.ball_sort` e
    ON lw.user_pseudo_id = e.user_pseudo_id AND lw.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  GROUP BY lw.product, lw.ab_group, lw.user_pseudo_id

  UNION ALL

  SELECT lw.product, lw.ab_group, lw.user_pseudo_id, COUNT(*) AS kill_count
  FROM long_watch_users lw
  INNER JOIN `transferred.hudi_ods.ios_nuts_sort` e
    ON lw.user_pseudo_id = e.user_pseudo_id AND lw.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  GROUP BY lw.product, lw.ab_group, lw.user_pseudo_id
)

SELECT
  product,
  ab_group,
  COUNT(*) AS user_count,
  ROUND(AVG(kill_count), 4) AS avg_kill_count,
  SUM(CASE WHEN kill_count = 1 THEN 1 ELSE 0 END) AS kill_1,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN kill_count = 1 THEN 1 ELSE 0 END), COUNT(*)), 4) AS kill_1_ratio,
  SUM(CASE WHEN kill_count = 2 THEN 1 ELSE 0 END) AS kill_2,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN kill_count = 2 THEN 1 ELSE 0 END), COUNT(*)), 4) AS kill_2_ratio,
  SUM(CASE WHEN kill_count >= 3 THEN 1 ELSE 0 END) AS kill_3plus,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN kill_count >= 3 THEN 1 ELSE 0 END), COUNT(*)), 4) AS kill_3plus_ratio
FROM user_kill_count
GROUP BY product, ab_group
ORDER BY product, ab_group;
