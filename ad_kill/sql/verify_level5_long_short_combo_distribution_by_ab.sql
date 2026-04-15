-- 第5关 game_new_start 的 long/short 联合出现次数分布（按 product + AB）
-- 口径：只看 game_new_start，不看 game_win
-- 输出示例：long1 + short0

WITH level5_events AS (
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
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
      'none'
    ) AS ad_kill_scene
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL
    AND user_pseudo_id != ''

  UNION ALL

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
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
      'none'
    ) AS ad_kill_scene
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL
    AND user_pseudo_id != ''
),

user_scene_stats AS (
  SELECT
    product,
    ab_group,
    user_pseudo_id,
    COUNTIF(ad_kill_scene = 'long_watch_kill') AS long_cnt,
    COUNTIF(ad_kill_scene = 'short_watch_repeat_kill') AS short_cnt
  FROM level5_events
  WHERE ab_group IS NOT NULL
  GROUP BY product, ab_group, user_pseudo_id
),

combo_distribution AS (
  SELECT
    product,
    ab_group,
    long_cnt,
    short_cnt,
    COUNT(*) AS user_cnt
  FROM user_scene_stats
  GROUP BY product, ab_group, long_cnt, short_cnt
),

group_total AS (
  SELECT
    product,
    ab_group,
    COUNT(*) AS total_users
  FROM user_scene_stats
  GROUP BY product, ab_group
)

SELECT
  d.product,
  d.ab_group,
  d.long_cnt,
  d.short_cnt,
  CONCAT('long', CAST(d.long_cnt AS STRING), '+short', CAST(d.short_cnt AS STRING)) AS combo_label,
  d.user_cnt,
  ROUND(SAFE_DIVIDE(d.user_cnt, t.total_users), 6) AS user_ratio
FROM combo_distribution d
JOIN group_total t
  ON d.product = t.product
 AND d.ab_group = t.ab_group
ORDER BY d.product, d.ab_group, d.user_cnt DESC, d.long_cnt, d.short_cnt;
