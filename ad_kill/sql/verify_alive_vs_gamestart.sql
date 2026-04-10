-- 无 engagement 用户中，universal_alive 和 game_new_start 的交叉分布
WITH users_with_engagement AS (
  SELECT DISTINCT user_pseudo_id
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE product = 'ball_sort'
    AND event_date = '2026-02-20'
    AND event_name = 'user_engagement'
),

no_engagement_users AS (
  SELECT DISTINCT d.user_pseudo_id,
    MAX(CASE WHEN d.event_name = 'universal_alive' THEN 1 ELSE 0 END) AS has_alive,
    MAX(CASE WHEN d.event_name = 'game_new_start' THEN 1 ELSE 0 END) AS has_game_start
  FROM `commercial-adx.lmh.ad_kill_detail` d
  WHERE d.product = 'ball_sort'
    AND d.event_date = '2026-02-20'
    AND d.user_pseudo_id NOT IN (SELECT user_pseudo_id FROM users_with_engagement)
  GROUP BY d.user_pseudo_id
)

SELECT
  CASE
    WHEN has_alive = 1 AND has_game_start = 1 THEN 'both'
    WHEN has_alive = 1 AND has_game_start = 0 THEN 'alive_only'
    WHEN has_alive = 0 AND has_game_start = 1 THEN 'game_start_only'
    ELSE 'neither'
  END AS user_group,
  COUNT(*) AS user_count
FROM no_engagement_users
GROUP BY 1
ORDER BY user_count DESC
