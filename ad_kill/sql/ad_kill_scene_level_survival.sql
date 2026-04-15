-- ad_kill 实验：按第5关 scene 分组的关卡留存曲线
-- 口径：
-- 1. 只看第5关 game_new_start（levelid=5, activity_id=0）
-- 2. long / short 按首次出现时间判定，谁先出现归谁
-- 3. 若两者都未出现，则按第5关杀广告次数分为 other_0/1/2/3plus
-- 4. 留存仍按 game_new_start 的 max_level >= level 计算
-- 输出：
--   product, ab_group, scene_group, level, retained_users, total_users, retention_rate

WITH level5_gns_events AS (
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
    event_timestamp,
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
    event_timestamp,
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

level5_users AS (
  SELECT
    product,
    user_pseudo_id,
    ARRAY_AGG(ab_group IGNORE NULLS ORDER BY event_timestamp ASC LIMIT 1)[SAFE_OFFSET(0)] AS ab_group
  FROM level5_gns_events
  GROUP BY product, user_pseudo_id
  HAVING ab_group IS NOT NULL
),

scene_first_seen AS (
  SELECT
    product,
    user_pseudo_id,
    MIN(IF(ad_kill_scene = 'long_watch_kill', event_timestamp, NULL)) AS first_long_ts,
    MIN(IF(ad_kill_scene = 'short_watch_repeat_kill', event_timestamp, NULL)) AS first_short_ts
  FROM level5_gns_events
  GROUP BY product, user_pseudo_id
),

kill_counts AS (
  SELECT
    'ball_sort' AS product,
    user_pseudo_id,
    COUNT(*) AS kill_count
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL
    AND user_pseudo_id != ''
  GROUP BY user_pseudo_id

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product,
    user_pseudo_id,
    COUNT(*) AS kill_count
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL
    AND user_pseudo_id != ''
  GROUP BY user_pseudo_id
),

user_scene AS (
  SELECT
    u.product,
    u.user_pseudo_id,
    u.ab_group,
    CASE
      WHEN first_long_ts IS NOT NULL AND first_short_ts IS NOT NULL THEN
        CASE
          WHEN first_long_ts < first_short_ts THEN 'long'
          WHEN first_short_ts < first_long_ts THEN 'short'
          ELSE 'long'
        END
      WHEN first_long_ts IS NOT NULL THEN 'long'
      WHEN first_short_ts IS NOT NULL THEN 'short'
      WHEN COALESCE(k.kill_count, 0) = 0 THEN 'other_0'
      WHEN COALESCE(k.kill_count, 0) = 1 THEN 'other_1'
      WHEN COALESCE(k.kill_count, 0) = 2 THEN 'other_2'
      ELSE 'other_3plus'
    END AS scene_group
  FROM level5_users AS u
  LEFT JOIN scene_first_seen AS s
    ON u.product = s.product
   AND u.user_pseudo_id = s.user_pseudo_id
  LEFT JOIN kill_counts AS k
    ON u.product = k.product
   AND u.user_pseudo_id = k.user_pseudo_id
),

max_level_data AS (
  SELECT
    'ball_sort' AS product,
    us.user_pseudo_id,
    us.ab_group,
    us.scene_group,
    MAX((SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid')) AS max_level
  FROM `transferred.hudi_ods.ball_sort` AS e
  INNER JOIN user_scene AS us
    ON e.user_pseudo_id = us.user_pseudo_id
   AND us.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  GROUP BY us.user_pseudo_id, us.ab_group, us.scene_group

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product,
    us.user_pseudo_id,
    us.ab_group,
    us.scene_group,
    MAX((SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid')) AS max_level
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN user_scene AS us
    ON e.user_pseudo_id = us.user_pseudo_id
   AND us.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  GROUP BY us.user_pseudo_id, us.ab_group, us.scene_group
),

scene_total AS (
  SELECT
    product,
    ab_group,
    scene_group,
    COUNT(*) AS total_users
  FROM max_level_data
  GROUP BY product, ab_group, scene_group
),

levels AS (
  SELECT level
  FROM UNNEST(GENERATE_ARRAY(5, 500)) AS level
)

SELECT
  ml.product,
  ml.ab_group,
  ml.scene_group,
  lv.level,
  COUNT(*) AS retained_users,
  st.total_users,
  SAFE_DIVIDE(COUNT(*), st.total_users) AS retention_rate
FROM max_level_data AS ml
CROSS JOIN levels AS lv
INNER JOIN scene_total AS st
  ON ml.product = st.product
 AND ml.ab_group = st.ab_group
 AND ml.scene_group = st.scene_group
WHERE ml.max_level >= lv.level
GROUP BY ml.product, ml.ab_group, ml.scene_group, lv.level, st.total_users
HAVING COUNT(*) > 0
ORDER BY ml.product, ml.ab_group, ml.scene_group, lv.level;
