-- ad_kill 实验：按第5关 ad_kill_scene 分组的关卡留存曲线
-- 数据源：transferred.hudi_ods.ball_sort / ios_nuts_sort
-- scene 判定：每用户第5关最晚的 game_new_start 或 game_win 事件的 ad_kill_scene
-- 对象：所有到达第5关的用户（不区分新老）
-- 输出：product, ab_group, scene_group, level, retained_users, total_users, retention_rate

-- ============================================================
-- Ball Sort: AB 分组
-- ============================================================
WITH bs_user_ab AS (
  SELECT
    user_pseudo_id,
    CASE
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)12a($|_)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)12b($|_)'
      ) THEN 'B'
    END AS ab_group
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'user_engagement'
    AND (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1) IS NOT NULL
  GROUP BY user_pseudo_id, ab_group
  HAVING ab_group IS NOT NULL
),

-- iOS Nuts Sort: AB 分组
ns_user_ab AS (
  SELECT
    user_pseudo_id,
    CASE
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)2a($|_)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)2b($|_)'
      ) THEN 'B'
    END AS ab_group
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'user_engagement'
    AND (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1) IS NOT NULL
  GROUP BY user_pseudo_id, ab_group
  HAVING ab_group IS NOT NULL
),

-- ============================================================
-- 第5关所有 game_new_start / game_win 事件，取每用户最晚事件的 scene
-- ============================================================
level5_all_events AS (
  SELECT
    'ball_sort' AS product, e.user_pseudo_id, ua.ab_group, e.event_timestamp,
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
      'no_scene'
    ) AS ad_kill_scene
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN bs_user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name IN ('game_new_start', 'game_win')
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product, e.user_pseudo_id, ua.ab_group, e.event_timestamp,
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
      'no_scene'
    ) AS ad_kill_scene
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN ns_user_ab ua ON e.user_pseudo_id = ua.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name IN ('game_new_start', 'game_win')
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
),

user_scene AS (
  SELECT product, user_pseudo_id, ab_group, ad_kill_scene AS scene_group
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product, user_pseudo_id, ab_group ORDER BY event_timestamp DESC) AS rn
    FROM level5_all_events
  )
  WHERE rn = 1
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
