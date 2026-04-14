-- long_watch_kill 用户在第5关的杀广告次数分布（AB 对比）
-- 用最后一条事件的 ad_kill_scene 分类（与 dashboard 口径一致）
-- AB 分组从同事件的 user_properties 提取，不额外扫 user_engagement

WITH level5_last_scene AS (
  SELECT product, user_pseudo_id, ab_group, ad_kill_scene
  FROM (
    SELECT
      'ball_sort' AS product,
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
      END AS ab_group,
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS rn
    FROM `transferred.hudi_ods.ball_sort`
    WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND event_name IN ('game_new_start', 'game_win')
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
      AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  )
  WHERE rn = 1 AND ab_group IS NOT NULL

  UNION ALL

  SELECT product, user_pseudo_id, ab_group, ad_kill_scene
  FROM (
    SELECT
      'ios_nuts_sort' AS product,
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
      END AS ab_group,
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS rn
    FROM `transferred.hudi_ods.ios_nuts_sort`
    WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND event_name IN ('game_new_start', 'game_win')
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
      AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  )
  WHERE rn = 1 AND ab_group IS NOT NULL
),

long_watch_users AS (
  SELECT product, ab_group, user_pseudo_id
  FROM level5_last_scene
  WHERE ad_kill_scene = 'long_watch_kill'
),

user_kill_count AS (
  SELECT lw.product, lw.ab_group, lw.user_pseudo_id, COUNT(e.event_name) AS kill_count
  FROM long_watch_users lw
  LEFT JOIN (
    SELECT user_pseudo_id, event_name
    FROM `transferred.hudi_ods.ball_sort`
    WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND event_name = 'lib_fullscreen_ad_killed'
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  ) e ON lw.user_pseudo_id = e.user_pseudo_id
  WHERE lw.product = 'ball_sort'
  GROUP BY lw.product, lw.ab_group, lw.user_pseudo_id

  UNION ALL

  SELECT lw.product, lw.ab_group, lw.user_pseudo_id, COUNT(e.event_name) AS kill_count
  FROM long_watch_users lw
  LEFT JOIN (
    SELECT user_pseudo_id, event_name
    FROM `transferred.hudi_ods.ios_nuts_sort`
    WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND event_name = 'lib_fullscreen_ad_killed'
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  ) e ON lw.user_pseudo_id = e.user_pseudo_id
  WHERE lw.product = 'ios_nuts_sort'
  GROUP BY lw.product, lw.ab_group, lw.user_pseudo_id
)

SELECT
  product,
  ab_group,
  kill_count,
  COUNT(*) AS user_count,
  ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY product, ab_group)), 4) AS user_ratio
FROM user_kill_count
GROUP BY product, ab_group, kill_count
ORDER BY product, ab_group, kill_count