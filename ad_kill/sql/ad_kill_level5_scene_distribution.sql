-- ad_kill 实验：第5关 DAU 及 ad_kill_scene 分布
-- 数据源：transferred.hudi_ods.ball_sort / ios_nuts_sort
-- scene 判定：每用户第5关最晚的 game_new_start 或 game_win 事件的 ad_kill_scene
-- 对 scene=none 的用户二次判定：有第5关杀广告事件的拆分为 other_kill1 / other_kill2+
-- AB 分组：从同事件的 user_properties 提取，不额外扫 user_engagement
-- 输出：product, ab_group, ad_kill_scene, level5_dau, pv, uv, uv_ratio

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

-- 每用户取最晚事件确定唯一 scene
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

-- 最终 scene：对 none 用户二次判定
user_scene AS (
  SELECT product, user_pseudo_id, ab_group,
    CASE
      WHEN ad_kill_scene != 'none' THEN ad_kill_scene
      WHEN nk.kill_count = 1 THEN 'other_kill1'
      WHEN nk.kill_count >= 2 THEN 'other_kill2+'
      ELSE 'none'
    END AS ad_kill_scene
  FROM user_scene_raw us
  LEFT JOIN none_user_kills nk USING (product, user_pseudo_id)
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
    ON ev.product = us.product AND ev.user_pseudo_id = us.user_pseudo_id
  WHERE ev.ab_group IS NOT NULL
  GROUP BY us.product, us.ab_group, us.ad_kill_scene
)

-- 输出
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
ORDER BY s.product, s.ab_group, s.ad_kill_scene