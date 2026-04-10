-- 无 engagement 用户的新老用户分布
WITH users_with_engagement AS (
  SELECT DISTINCT user_pseudo_id
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE product = 'ball_sort'
    AND event_date = '2026-02-20'
    AND event_name = 'user_engagement'
),

no_engagement_users AS (
  SELECT DISTINCT user_pseudo_id
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE product = 'ball_sort'
    AND event_date = '2026-02-20'
    AND user_pseudo_id NOT IN (SELECT user_pseudo_id FROM users_with_engagement)
)

SELECT
  d.user_type,
  COUNT(DISTINCT d.user_pseudo_id) AS user_count
FROM `commercial-adx.lmh.ad_kill_detail` d
INNER JOIN no_engagement_users n ON d.user_pseudo_id = n.user_pseudo_id
WHERE d.product = 'ball_sort'
  AND d.event_date = '2026-02-20'
GROUP BY d.user_type
