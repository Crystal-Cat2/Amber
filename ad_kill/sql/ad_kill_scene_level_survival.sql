-- ad_kill 实验：按第5关 ad_kill_scene 分组的关卡留存曲线
-- 数据源：transferred.hudi_ods.ball_sort / ios_nuts_sort
-- scene 判定：每用户第5关最晚的 game_new_start 或 game_win 事件的 ad_kill_scene
-- 对 scene=none 的用户二次判定：有第5关杀广告事件的拆分为 other_kill1 / other_kill2+
-- AB 分组：从同事件的 user_properties 提取，不额外扫 user_engagement
-- 对象：所有到达第5关的用户（不区分新老）
-- 输出：product, ab_group, scene_group, level, retained_users, total_users, retention_rate

WITH level5_all_events AS (
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
    AND event_name IN ('game_new_start', 'game_win')
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''

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
    AND event_name IN ('game_new_start', 'game_win')
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
),

user_scene_raw AS (
  SELECT product, user_pseudo_id, ab_group, ad_kill_scene
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY product, user_pseudo_id ORDER BY event_timestamp DESC) AS rn
    FROM level5_all_events
    WHERE ab_group IS NOT NULL
  )
  WHERE rn = 1
),

-- 对 scene=none 的用户，统计第5关杀广告次数
none_user_kills AS (
  SELECT us.product, us.user_pseudo_id, COUNT(e.event_name) AS kill_count
  FROM user_scene_raw us
  LEFT JOIN (
    SELECT user_pseudo_id, event_name
    FROM `transferred.hudi_ods.ball_sort`
    WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND event_name = 'lib_fullscreen_ad_killed'
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  ) e ON us.user_pseudo_id = e.user_pseudo_id AND us.product = 'ball_sort'
  WHERE us.ad_kill_scene = 'none' AND us.product = 'ball_sort'
  GROUP BY us.product, us.user_pseudo_id

  UNION ALL

  SELECT us.product, us.user_pseudo_id, COUNT(e.event_name) AS kill_count
  FROM user_scene_raw us
  LEFT JOIN (
    SELECT user_pseudo_id, event_name
    FROM `transferred.hudi_ods.ios_nuts_sort`
    WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND event_name = 'lib_fullscreen_ad_killed'
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  ) e ON us.user_pseudo_id = e.user_pseudo_id AND us.product = 'ios_nuts_sort'
  WHERE us.ad_kill_scene = 'none' AND us.product = 'ios_nuts_sort'
  GROUP BY us.product, us.user_pseudo_id
),

-- 最终 scene
user_scene AS (
  SELECT product, user_pseudo_id, ab_group,
    CASE
      WHEN ad_kill_scene != 'none' THEN ad_kill_scene
      WHEN nk.kill_count = 1 THEN 'other_kill1'
      WHEN nk.kill_count >= 2 THEN 'other_kill2+'
      ELSE 'none'
    END AS scene_group
  FROM user_scene_raw us
  LEFT JOIN none_user_kills nk USING (product, user_pseudo_id)
),

-- ============================================================
-- 每用户最高关卡（game_new_start, activity_id=0）
-- ============================================================
max_level_data AS (
  SELECT
    'ball_sort' AS product, us.user_pseudo_id, us.ab_group, us.scene_group,
    MAX((SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid')) AS max_level
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN user_scene us ON e.user_pseudo_id = us.user_pseudo_id AND us.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  GROUP BY us.user_pseudo_id, us.ab_group, us.scene_group

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product, us.user_pseudo_id, us.ab_group, us.scene_group,
    MAX((SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid')) AS max_level
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN user_scene us ON e.user_pseudo_id = us.user_pseudo_id AND us.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  GROUP BY us.user_pseudo_id, us.ab_group, us.scene_group
),

-- 每 scene 组总用户数
scene_total AS (
  SELECT product, ab_group, scene_group, COUNT(*) AS total_users
  FROM max_level_data
  GROUP BY product, ab_group, scene_group
),

-- 关卡序列
levels AS (
  SELECT level FROM UNNEST(GENERATE_ARRAY(5, 500)) AS level
)

-- ============================================================
-- 留存计算
-- ============================================================
SELECT
  ml.product,
  ml.ab_group,
  ml.scene_group,
  lv.level,
  COUNT(*) AS retained_users,
  st.total_users,
  SAFE_DIVIDE(COUNT(*), st.total_users) AS retention_rate
FROM max_level_data ml
CROSS JOIN levels lv
INNER JOIN scene_total st
  ON ml.product = st.product AND ml.ab_group = st.ab_group AND ml.scene_group = st.scene_group
WHERE ml.max_level >= lv.level
GROUP BY ml.product, ml.ab_group, ml.scene_group, lv.level, st.total_users
HAVING COUNT(*) > 0
ORDER BY ml.product, ml.ab_group, ml.scene_group, lv.level;