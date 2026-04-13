-- 验证：scene=none 的用户在第5关是否有杀广告行为
-- 目的：确认 none 用户的 lib_fullscreen_ad_killed 次数是否为零
-- 数据源：commercial-adx.lmh.ad_kill_detail

-- 1. 找出第5关 scene=none 的用户
WITH level5_none_users AS (
  SELECT DISTINCT
    product,
    user_pseudo_id,
    ab_group
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE event_name = 'game_new_start'
    AND (SELECT ep.value.int_value
         FROM UNNEST(event_params.array) AS ep
         WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.string_value
         FROM UNNEST(event_params.array) AS ep
         WHERE ep.key = 'ad_kill_scene') = 'none'
),

-- 2. 统计这些用户在第5关的 lib_fullscreen_ad_killed 事件次数
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

-- 3. 汇总：none 用户中有多少人有杀广告事件，平均杀了几次
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
