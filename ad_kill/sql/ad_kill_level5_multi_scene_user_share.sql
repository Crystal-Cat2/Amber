-- ad_kill 实验：第5关多 scene 用户占比
-- 数据源：transferred.hudi_ods.ball_sort / ios_nuts_sort
-- 目的：
--   1) 统计第5关用户里，命中过多个 scene 的用户占比
--   2) 为后续 scene 优先级归因补充辅助信息
-- 口径说明：
--   - 范围：event_name = 'game_new_start' AND levelid = 5 AND activity_id = 0
--   - scene：直接读取 game_new_start 事件上的 ad_kill_scene
--   - ad_kill_scene 缺失统一记为 'no_scene'
--   - ad_kill_scene = 'none' 视为“没有杀广告”
--   - 其他 scene 视为“有明确 kill scene”
--   - lib_fullscreen_ad_killed 用于辅助确认无值用户是否发生过杀广告
-- 输出：
--   product, ab_group 粒度的一行汇总

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

level5_events AS (
  SELECT
    'ball_sort' AS product,
    e.user_pseudo_id,
    ua.ab_group,
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
      'no_scene'
    ) AS normalized_scene
  FROM `transferred.hudi_ods.ball_sort` AS e
  INNER JOIN bs_user_ab AS ua
    ON e.user_pseudo_id = ua.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product,
    e.user_pseudo_id,
    ua.ab_group,
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
      'no_scene'
    ) AS normalized_scene
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN ns_user_ab AS ua
    ON e.user_pseudo_id = ua.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
),

kill_event_users AS (
  SELECT
    'ball_sort' AS product,
    e.user_pseudo_id,
    ua.ab_group,
    COUNT(*) AS kill_event_count
  FROM `transferred.hudi_ods.ball_sort` AS e
  INNER JOIN bs_user_ab AS ua
    ON e.user_pseudo_id = ua.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
  GROUP BY product, e.user_pseudo_id, ua.ab_group

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product,
    e.user_pseudo_id,
    ua.ab_group,
    COUNT(*) AS kill_event_count
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN ns_user_ab AS ua
    ON e.user_pseudo_id = ua.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
  GROUP BY product, e.user_pseudo_id, ua.ab_group
),

level5_user_scene_profile AS (
  SELECT
    product,
    ab_group,
    user_pseudo_id,
    COUNT(DISTINCT normalized_scene) AS scene_count,
    STRING_AGG(DISTINCT normalized_scene, ' | ' ORDER BY normalized_scene) AS scene_set,
    MAX(CASE WHEN normalized_scene = 'none' THEN 1 ELSE 0 END) AS has_none_scene,
    MAX(CASE WHEN normalized_scene = 'no_scene' THEN 1 ELSE 0 END) AS has_no_scene,
    MAX(CASE WHEN normalized_scene NOT IN ('none', 'no_scene') THEN 1 ELSE 0 END) AS has_named_kill_scene
  FROM level5_events
  GROUP BY product, ab_group, user_pseudo_id
)

SELECT
  p.product,
  p.ab_group,
  COUNT(*) AS level5_users,
  COUNTIF(p.scene_count > 1) AS multi_scene_users,
  ROUND(SAFE_DIVIDE(COUNTIF(p.scene_count > 1), COUNT(*)), 4) AS multi_scene_user_ratio,
  COUNTIF(p.scene_count = 1) AS single_scene_users,
  COUNTIF(p.has_none_scene = 1) AS users_with_none_scene,
  COUNTIF(p.has_no_scene = 1) AS users_with_no_scene,
  COUNTIF(p.has_named_kill_scene = 1) AS users_with_named_kill_scene,
  COUNTIF(p.has_no_scene = 1 AND COALESCE(k.kill_event_count, 0) > 0) AS no_scene_users_with_kill_event,
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(p.has_no_scene = 1 AND COALESCE(k.kill_event_count, 0) > 0),
      COUNTIF(p.has_no_scene = 1)
    ),
    4
  ) AS no_scene_users_with_kill_event_ratio,
  COUNTIF(p.scene_set = 'none') AS pure_none_only_users,
  COUNTIF(p.scene_set = 'no_scene') AS pure_no_scene_only_users
FROM level5_user_scene_profile AS p
LEFT JOIN kill_event_users AS k
  ON p.product = k.product
  AND p.ab_group = k.ab_group
  AND p.user_pseudo_id = k.user_pseudo_id
GROUP BY p.product, p.ab_group
ORDER BY p.product, p.ab_group;
