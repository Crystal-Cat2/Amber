-- ad_kill 实验 ad_kill_scene 用户占比分析
-- 数据源：commercial-adx.lmh.ad_kill_detail
-- 目的：分析 long_watch_kill / short_watch_repeat_kill 用户在 AB 组的占比差异
--        以及在新老用户中的占比差异

-- 从 game_new_start 事件提取第5关的 ad_kill_scene（限制 level_id=5 取唯一值）
WITH scene_events AS (
  SELECT
    product,
    user_pseudo_id,
    ab_group,
    user_type,
    (SELECT ep.value.string_value
     FROM UNNEST(event_params.array) AS ep
     WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE event_name = 'game_new_start'
    AND (SELECT ep.value.int_value
         FROM UNNEST(event_params.array) AS ep
         WHERE ep.key = 'levelid') = 5
),

-- 用户级别：第5关的 ad_kill_scene 唯一分类
user_scene AS (
  SELECT
    product,
    user_pseudo_id,
    ab_group,
    user_type,
    ad_kill_scene
  FROM (
    SELECT
      product,
      user_pseudo_id,
      ab_group,
      user_type,
      ad_kill_scene,
      ROW_NUMBER() OVER (
        PARTITION BY product, user_pseudo_id
        ORDER BY ad_kill_scene DESC  -- 优先取有值的记录
      ) AS rn
    FROM scene_events
    WHERE ad_kill_scene IS NOT NULL
  )
  WHERE rn = 1
),

-- 用户标记
user_scene_flags AS (
  SELECT
    product,
    user_pseudo_id,
    ab_group,
    user_type,
    CASE WHEN ad_kill_scene = 'long_watch_kill' THEN 1 ELSE 0 END AS has_long,
    CASE WHEN ad_kill_scene = 'short_watch_repeat_kill' THEN 1 ELSE 0 END AS has_short,
    CASE WHEN ad_kill_scene IN ('long_watch_kill', 'short_watch_repeat_kill') THEN 1 ELSE 0 END AS has_any
  FROM user_scene
),

-- 按 product/ab_group/user_type 汇总
scene_summary AS (
  SELECT
    product,
    ab_group,
    user_type,
    COUNT(*) AS total_users,
    SUM(has_long) AS long_kill_users,
    SUM(has_short) AS short_kill_users,
    SUM(has_any) AS any_kill_users
  FROM user_scene_flags
  GROUP BY product, ab_group, user_type
)

-- Part 1: 整体占比（不分新老）
SELECT
  'overall' AS view_type,
  product,
  ab_group,
  CAST(NULL AS STRING) AS user_type,
  SUM(total_users) AS total_users,
  SUM(long_kill_users) AS long_kill_users,
  ROUND(SAFE_DIVIDE(SUM(long_kill_users), SUM(total_users)), 4) AS long_kill_ratio,
  SUM(short_kill_users) AS short_kill_users,
  ROUND(SAFE_DIVIDE(SUM(short_kill_users), SUM(total_users)), 4) AS short_kill_ratio,
  SUM(any_kill_users) AS any_kill_users,
  ROUND(SAFE_DIVIDE(SUM(any_kill_users), SUM(total_users)), 4) AS any_kill_ratio
FROM scene_summary
GROUP BY product, ab_group

UNION ALL

-- Part 2: 按新老用户拆分
SELECT
  'by_user_type' AS view_type,
  product,
  ab_group,
  user_type,
  total_users,
  long_kill_users,
  ROUND(SAFE_DIVIDE(long_kill_users, total_users), 4) AS long_kill_ratio,
  short_kill_users,
  ROUND(SAFE_DIVIDE(short_kill_users, total_users), 4) AS short_kill_ratio,
  any_kill_users,
  ROUND(SAFE_DIVIDE(any_kill_users, total_users), 4) AS any_kill_ratio
FROM scene_summary

ORDER BY product, view_type, ab_group, user_type;
