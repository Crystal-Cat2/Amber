-- ad_kill 实验：第5关 DAU 及 ad_kill_scene 分布
-- 数据源：transferred.hudi_ods.ball_sort / ios_nuts_sort
-- scene 判定：每用户第5关最晚的 game_new_start 或 game_win 事件的 ad_kill_scene
-- 输出：product, ab_group, ad_kill_scene, level5_dau, pv, uv, uv_ratio

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
-- 第5关所有 game_new_start / game_win 事件（两产品合并）
-- 每用户取最晚事件的 ad_kill_scene 作为该用户的 scene 判定
-- ============================================================
level5_all_events AS (
  SELECT
    'ball_sort' AS product,
    e.user_pseudo_id,
    ua.ab_group,
    e.event_timestamp,
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
    'ios_nuts_sort' AS product,
    e.user_pseudo_id,
    ua.ab_group,
    e.event_timestamp,
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

-- 每用户取最晚事件确定唯一 scene
user_scene AS (
  SELECT product, user_pseudo_id, ab_group, ad_kill_scene
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY product, user_pseudo_id, ab_group ORDER BY event_timestamp DESC) AS rn
    FROM level5_all_events
  )
  WHERE rn = 1
),

-- 第5关 DAU（分母）
level5_dau AS (
  SELECT product, ab_group, COUNT(DISTINCT user_pseudo_id) AS dau
  FROM user_scene
  GROUP BY product, ab_group
),

-- 每个 scene 的 UV
scene_uv AS (
  SELECT product, ab_group, ad_kill_scene, COUNT(DISTINCT user_pseudo_id) AS uv
  FROM user_scene
  GROUP BY product, ab_group, ad_kill_scene
),

-- 每个 scene 的 PV（该 scene 用户在第5关的总事件次数）
scene_pv AS (
  SELECT us.product, us.ab_group, us.ad_kill_scene, COUNT(*) AS pv
  FROM level5_all_events ev
  INNER JOIN user_scene us
    ON ev.product = us.product AND ev.user_pseudo_id = us.user_pseudo_id AND ev.ab_group = us.ab_group
  GROUP BY us.product, us.ab_group, us.ad_kill_scene
)

-- ============================================================
-- 输出
-- ============================================================
SELECT
  s.product,
  s.ab_group,
  s.ad_kill_scene,
  d.dau AS level5_dau,
  COALESCE(p.pv, 0) AS pv,
  s.uv,
  ROUND(SAFE_DIVIDE(s.uv, d.dau), 4) AS uv_ratio
FROM scene_uv s
JOIN level5_dau d ON s.product = d.product AND s.ab_group = d.ab_group
LEFT JOIN scene_pv p ON s.product = p.product AND s.ab_group = p.ab_group AND s.ad_kill_scene = p.ad_kill_scene
ORDER BY s.product, s.ab_group, s.ad_kill_scene;
