-- ad_kill 实验：第5关 no_scene 用户的杀广告次数分布
-- 数据源：transferred.hudi_ods.ball_sort / ios_nuts_sort
-- 目的：
--   1) 统计第5关 ad_kill_scene 缺失（no_scene）用户的 lib_fullscreen_ad_killed 次数分布
--   2) 输出 no_scene 用户总量，以及 A/B 组在产品内的占比
-- 输出：
--   product, ab_group, kill_count, users, total_no_scene_users, product_no_scene_users,
--   ratio_within_ab, ab_share_within_product

WITH no_scene_users AS (
  SELECT DISTINCT
    base.product,
    base.user_pseudo_id,
    base.ab_group
  FROM (
    SELECT
      'ball_sort' AS product,
      e.user_pseudo_id,
      CASE
        WHEN REGEXP_CONTAINS(
          (SELECT up.value.string_value FROM UNNEST(e.user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
          r'(^|_)12a($|_)'
        ) THEN 'A'
        WHEN REGEXP_CONTAINS(
          (SELECT up.value.string_value FROM UNNEST(e.user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
          r'(^|_)12b($|_)'
        ) THEN 'B'
      END AS ab_group
    FROM `transferred.hudi_ods.ball_sort` AS e
    WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND e.event_name = 'game_new_start'
      AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
      AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
      AND (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') IS NULL
      AND (SELECT up.value.string_value FROM UNNEST(e.user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1) IS NOT NULL

    UNION ALL

    SELECT
      'ios_nuts_sort' AS product,
      e.user_pseudo_id,
      CASE
        WHEN REGEXP_CONTAINS(
          (SELECT up.value.string_value FROM UNNEST(e.user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
          r'(^|_)2a($|_)'
        ) THEN 'A'
        WHEN REGEXP_CONTAINS(
          (SELECT up.value.string_value FROM UNNEST(e.user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
          r'(^|_)2b($|_)'
        ) THEN 'B'
      END AS ab_group
    FROM `transferred.hudi_ods.ios_nuts_sort` AS e
    WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND e.event_name = 'game_new_start'
      AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
      AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
      AND (SELECT ep.value.string_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') IS NULL
      AND (SELECT up.value.string_value FROM UNNEST(e.user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1) IS NOT NULL
  ) AS base
  WHERE base.ab_group IS NOT NULL
),

user_kill_counts AS (
  SELECT
    ns.product,
    ns.user_pseudo_id,
    ns.ab_group,
    COUNT(e.event_name) AS kill_count
  FROM no_scene_users AS ns
  LEFT JOIN `transferred.hudi_ods.ball_sort` AS e
    ON ns.product = 'ball_sort'
    AND ns.user_pseudo_id = e.user_pseudo_id
    AND e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
  WHERE ns.product = 'ball_sort'
  GROUP BY ns.product, ns.user_pseudo_id, ns.ab_group

  UNION ALL

  SELECT
    ns.product,
    ns.user_pseudo_id,
    ns.ab_group,
    COUNT(e.event_name) AS kill_count
  FROM no_scene_users AS ns
  LEFT JOIN `transferred.hudi_ods.ios_nuts_sort` AS e
    ON ns.product = 'ios_nuts_sort'
    AND ns.user_pseudo_id = e.user_pseudo_id
    AND e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
  WHERE ns.product = 'ios_nuts_sort'
  GROUP BY ns.product, ns.user_pseudo_id, ns.ab_group
),

ab_totals AS (
  SELECT
    product,
    ab_group,
    COUNT(*) AS total_no_scene_users
  FROM user_kill_counts
  WHERE ab_group IS NOT NULL
  GROUP BY product, ab_group
),

product_totals AS (
  SELECT
    product,
    COUNT(*) AS product_no_scene_users
  FROM user_kill_counts
  WHERE ab_group IS NOT NULL
  GROUP BY product
),

histogram AS (
  SELECT
    product,
    ab_group,
    kill_count,
    COUNT(*) AS users
  FROM user_kill_counts
  WHERE ab_group IS NOT NULL
  GROUP BY product, ab_group, kill_count
)

SELECT
  h.product,
  h.ab_group,
  h.kill_count,
  h.users,
  a.total_no_scene_users,
  p.product_no_scene_users,
  ROUND(SAFE_DIVIDE(h.users, a.total_no_scene_users), 4) AS ratio_within_ab,
  ROUND(SAFE_DIVIDE(a.total_no_scene_users, p.product_no_scene_users), 4) AS ab_share_within_product
FROM histogram AS h
INNER JOIN ab_totals AS a
  ON h.product = a.product
  AND h.ab_group = a.ab_group
INNER JOIN product_totals AS p
  ON h.product = p.product
ORDER BY h.product, h.ab_group, h.kill_count;
