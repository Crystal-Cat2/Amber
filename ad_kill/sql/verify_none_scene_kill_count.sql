-- 验证：第5关最后一条事件 scene=none 的用户，在该关是否有杀广告行为
-- 逻辑：取每用户第5关最晚一条 game_new_start/game_win 的 scene，
--       筛出 scene=none 的用户，统计其第5关 lib_fullscreen_ad_killed 次数
-- 数据源：commercial-adx.lmh.ad_kill_detail

-- 1. 每用户第5关最后一条事件的 scene
WITH level5_last_scene AS (
  SELECT
    product,
    user_pseudo_id,
    ab_group,
    ad_kill_scene
  FROM (
    SELECT
      product,
      user_pseudo_id,
      ab_group,
      (SELECT ep.value.string_value
       FROM UNNEST(event_params.array) AS ep
       WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene,
      ROW_NUMBER() OVER (
        PARTITION BY product, user_pseudo_id
        ORDER BY event_timestamp DESC
      ) AS rn
    FROM `commercial-adx.lmh.ad_kill_detail`
    WHERE event_name IN ('game_new_start', 'game_win')
      AND (SELECT ep.value.int_value
           FROM UNNEST(event_params.array) AS ep
           WHERE ep.key = 'levelid') = 5
  )
  WHERE rn = 1
),

-- 2. 筛出最后一条 scene=none 的用户
level5_none_users AS (
  SELECT product, user_pseudo_id, ab_group
  FROM level5_last_scene
  WHERE ad_kill_scene = 'none'
),

-- 3. 这些用户在第5关的 lib_fullscreen_ad_killed 次数
kill_events AS (
  SELECT
    d.product,
    d.ab_group,
    d.user_pseudo_id,
    COUNT(*) AS kill_count
  FROM `commercial-adx.lmh.ad_kill_detail` d
  INNER JOIN level5_none_users u
    ON d.product = u.product AND d.user_pseudo_id = u.user_pseudo_id
  WHERE d.event_name = 'lib_fullscreen_ad_killed'
    AND (SELECT ep.value.int_value
         FROM UNNEST(d.event_params.array) AS ep
         WHERE ep.key = 'levelid') = 5
  GROUP BY d.product, d.ab_group, d.user_pseudo_id
)

-- 4. 汇总
SELECT
  u.product,
  u.ab_group,
  COUNT(DISTINCT u.user_pseudo_id) AS none_users,
  COUNT(DISTINCT k.user_pseudo_id) AS users_with_kill,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT k.user_pseudo_id), COUNT(DISTINCT u.user_pseudo_id)), 4) AS kill_ratio,
  ROUND(AVG(IFNULL(k.kill_count, 0)), 2) AS avg_kill_count
FROM level5_none_users u
LEFT JOIN kill_events k
  ON u.product = k.product AND u.user_pseudo_id = k.user_pseudo_id
GROUP BY u.product, u.ab_group
ORDER BY u.product, u.ab_group;
