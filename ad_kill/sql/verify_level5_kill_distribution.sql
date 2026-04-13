-- 第5关用户按杀广告次数分布（直接用 hudi 表 + lib_fullscreen_ad_killed 事件）
-- 用户范围：与 ad_kill_detail_rebuild 一致的 AB 分组用户
-- 逻辑：统计每用户在第5关的 lib_fullscreen_ad_killed 次数，按次数分桶看分布

-- 1. AB 分组映射（复用 ad_kill_detail_rebuild 逻辑）
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

-- 2. 第5关有 game_new_start 的用户（第5关 DAU 分母）
level5_users AS (
  SELECT DISTINCT
    ua.product,
    ua.ab_group,
    e.user_pseudo_id
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN user_ab ua
    ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5

  UNION ALL

  SELECT DISTINCT
    ua.product,
    ua.ab_group,
    e.user_pseudo_id
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN user_ab ua
    ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
),

-- 3. 每用户第5关的杀广告次数
user_kill_count AS (
  SELECT
    ua.product,
    ua.ab_group,
    e.user_pseudo_id,
    COUNT(*) AS kill_count
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN user_ab ua
    ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  GROUP BY ua.product, ua.ab_group, e.user_pseudo_id

  UNION ALL

  SELECT
    ua.product,
    ua.ab_group,
    e.user_pseudo_id,
    COUNT(*) AS kill_count
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN user_ab ua
    ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND e.event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  GROUP BY ua.product, ua.ab_group, e.user_pseudo_id
),

-- 4. 合并：每个第5关用户的杀广告次数（无杀广告为0）
user_with_kill AS (
  SELECT
    l.product,
    l.ab_group,
    l.user_pseudo_id,
    IFNULL(k.kill_count, 0) AS kill_count
  FROM level5_users l
  LEFT JOIN user_kill_count k
    ON l.product = k.product AND l.user_pseudo_id = k.user_pseudo_id
)

-- 5. 按杀广告次数分桶统计
SELECT
  product,
  ab_group,
  kill_count,
  COUNT(*) AS user_count,
  ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY product, ab_group)), 4) AS user_ratio
FROM user_with_kill
GROUP BY product, ab_group, kill_count
ORDER BY product, ab_group, kill_count;
